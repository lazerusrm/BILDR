//! Explicit, controller-owned one-shot evaluation for the observer bound case.
//!
//! This deliberately has no timer, queue, API route, or candidate-provided
//! command. It is a narrow materialization/execution/grading adapter with
//! explicit typed Store custody rather than a general evaluation scheduler.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use harness_eval::{
    IsolationCapability, OBSERVER_SNAPSHOT_BOUNDED_FIX, OBSERVER_SNAPSHOT_COMMAND,
    OBSERVER_SNAPSHOT_HISTORICAL_BASE, OBSERVER_SNAPSHOT_TARGET_PATH, ObserverSnapshotArm,
    ObserverSnapshotControllerReceiptV1, ObserverSnapshotGrade, ObserverSnapshotIsolationPinV1,
    ObserverSnapshotMaterializationReceiptV1, ObserverSnapshotSignal,
    canonical_digest_without_self, grade_observer_snapshot_controller_receipt,
    observer_snapshot_command_digest, observer_snapshot_fixture_digest,
    observer_snapshot_setup_digest,
};
use harness_git::{GitManager, WorktreeSpec};
use harness_runner::{
    CandidateIsolationSpec, CargoBuildCacheAdmission, CommandRunner, CommandSpec,
    EvaluationIsolationRunner, GraderIsolationSpec,
};
use harness_store::{
    EvaluationArm, EvaluationRunStatus, NewArtifact, NewCommandRecord, NewEvaluationRun,
    NewEvaluationRunStatus, NewEvaluationSample, NewEvidenceRecord, NewImprovementRevision, NewRun,
    NewTasksetMembership, NewValidationRecord, NewWorktree, Store,
};
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverSnapshotArmPlan {
    pub arm: ObserverSnapshotArm,
    pub base_sha: &'static str,
    pub relative_worktree: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverSnapshotPlan {
    pub repository: PathBuf,
    pub arms: [ObserverSnapshotArmPlan; 2],
}

impl ObserverSnapshotPlan {
    pub fn new(repository: PathBuf) -> Self {
        Self {
            repository,
            arms: [
                ObserverSnapshotArmPlan {
                    arm: ObserverSnapshotArm::Historical,
                    base_sha: OBSERVER_SNAPSHOT_HISTORICAL_BASE,
                    relative_worktree: PathBuf::from("evaluation/observer-snapshot/historical"),
                },
                ObserverSnapshotArmPlan {
                    arm: ObserverSnapshotArm::Fixed,
                    base_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX,
                    relative_worktree: PathBuf::from("evaluation/observer-snapshot/fixed"),
                },
            ],
        }
    }
}

pub struct EvaluationService {
    store: Store,
    git: GitManager,
    worktree_root: PathBuf,
    spool_root: PathBuf,
}

struct MaterializedOverlay {
    receipt: ObserverSnapshotMaterializationReceiptV1,
    artifact_path: PathBuf,
    artifact_sha256: String,
}

struct ArmExecution {
    receipt: ObserverSnapshotControllerReceiptV1,
    materialization: MaterializedOverlay,
    candidate: harness_runner::EvaluationIsolationOutcome,
    grader: harness_runner::EvaluationIsolationOutcome,
    isolated: EvaluationIsolationRunner,
}

struct PersistedArm {
    receipt: ObserverSnapshotControllerReceiptV1,
    controller_evidence: PersistedEvidence,
    grader_evidence: PersistedEvidence,
    materialization_artifact_digest: String,
    candidate_command_digest: String,
}

struct PersistedEvidence {
    evidence_id: harness_domain::EvidenceId,
    evidence_sha256: String,
}

struct CargoAdmission {
    admission: CargoBuildCacheAdmission,
    snapshot_root: PathBuf,
    target_dir: PathBuf,
    registry_manifest_digest: String,
    git_manifest_digest: String,
    toolchain_manifest_digest: String,
    target_scope_digest: String,
}

struct CustodyCleanup {
    paths: Vec<PathBuf>,
    armed: bool,
}

impl CustodyCleanup {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths, armed: true }
    }

    fn cleanup(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        for path in &self.paths {
            if let Err(error) = remove_custody_tree(path) {
                errors.push(format!("{}: {error}", path.display()));
            }
        }
        if errors.is_empty() {
            self.armed = false;
            Ok(())
        } else {
            bail!("evaluation custody cleanup failed: {}", errors.join("; "))
        }
    }
}

impl Drop for CustodyCleanup {
    fn drop(&mut self) {
        if self.armed {
            for path in &self.paths {
                let _ = remove_custody_tree(path);
            }
        }
    }
}

impl EvaluationService {
    pub fn new(store: Store, worktree_root: PathBuf, spool_root: PathBuf) -> Result<Self> {
        Ok(Self {
            store,
            git: GitManager::new(&worktree_root)?,
            worktree_root,
            spool_root,
        })
    }

    pub async fn run_observer_snapshot_once(&self, repository: PathBuf) -> Result<String> {
        let (repository, repository_id) = self.resolve_registered_repository(&repository)?;
        match self
            .store
            .evaluation_sample("m2-observer-snapshot-fixed-sample")
        {
            Ok(existing)
                if existing.evaluation_run_id == "m2-observer-snapshot-fixed-evaluation"
                    && existing.arm == EvaluationArm::Champion
                    && existing.eval_case_revision_id == "m2-observer-snapshot-case-r1"
                    && existing.classification == harness_eval::SampleClassification::Pass
                    && !existing.invalidated =>
            {
                // The Store record is immutable and its idempotency checks bind all
                // launch pins/evidence. Re-running controller commands would only
                // create duplicate custody records.
                self.complete_from_sample(&existing, &repository_id)?;
                return Ok(
                    "m2-observer-snapshot-fixed-evaluation:m2-observer-snapshot-fixed-sample"
                        .to_owned(),
                );
            }
            Ok(_) => bail!("fixed evaluation sample id exists with different immutable pins"),
            Err(harness_store::StoreError::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let plan = ObserverSnapshotPlan::new(repository);
        let historical_plan = &plan.arms[0];
        let historical_run = self.ensure_controller_run(&repository_id, historical_plan)?;
        let historical = self
            .run_arm(&plan.repository, historical_plan, historical_run)
            .await?;
        if grade_observer_snapshot_controller_receipt(&historical.receipt)
            != ObserverSnapshotGrade::Pass
        {
            bail!("historical observer snapshot reproduction was not trusted");
        }
        let (taskset_revision, grader_revision, case_revision) = self.ensure_fixed_eval_wires(
            &repository_id,
            &historical.controller_evidence.evidence_sha256,
        )?;
        self.store
            .append_taskset_membership(&NewTasksetMembership {
                taskset_revision_id: taskset_revision.clone(),
                eval_case_revision_id: case_revision.clone(),
                ordinal: 0,
            })?;
        let fixed_plan = &plan.arms[1];
        let fixed_run = self.ensure_controller_run(&repository_id, fixed_plan)?;
        let fixed = self
            .run_arm(&plan.repository, fixed_plan, fixed_run.clone())
            .await?;
        if [historical.receipt.clone(), fixed.receipt.clone()]
            .iter()
            .any(|receipt| {
                grade_observer_snapshot_controller_receipt(receipt) != ObserverSnapshotGrade::Pass
            })
        {
            bail!("observer snapshot evaluation did not produce both trusted passing arm receipts");
        }
        let runtime_digest = digest_text("harness.m2.observer.runtime.v1");
        let evaluation = self.store.start_evaluation_run(&NewEvaluationRun {
            id: "m2-observer-snapshot-fixed-evaluation".to_owned(),
            controller_run_id: fixed_run,
            taskset_revision_id: taskset_revision.clone(),
            grader_bundle_revision_id: grader_revision.clone(),
            base_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
            fixture_digest: fixed.receipt.fixture_digest.clone(),
            runtime_digest: runtime_digest.clone(),
            seed_policy_digest: digest_text("harness.m2.observer.seed.v1:1"),
            champion_policy_digest: digest_text("harness.m2.observer.current-fixed.v1"),
            challenger_policy_digest: None,
            idempotency_key: "m2-observer-snapshot-fixed-evaluation-v1".to_owned(),
        })?;
        let pins = self
            .store
            .evaluation_launch_pins(&taskset_revision, &grader_revision)?;
        let case = pins
            .eval_cases
            .iter()
            .find(|case| case.id == case_revision)
            .context("fixed eval case is absent from immutable launch pins")?;
        let mut sample = harness_eval::EvalSampleV1 {
            schema: "harness.eval-sample.v1".to_owned(),
            sample_id: "m2-observer-snapshot-fixed-sample".to_owned(),
            case_id: case.wire.case_id.clone(),
            case_revision: case.wire.revision,
            case_digest: case.wire.sha256.clone(),
            taskset_digest: pins.taskset.payload_sha256,
            grader_bundle_digest: pins.grader_bundle.payload_sha256,
            policy_digest: digest_text("harness.m2.observer.current-fixed.v1"),
            base_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
            fixture_digest: fixed.receipt.fixture_digest.clone(),
            setup_digest: fixed.receipt.setup_digest.clone(),
            runtime_digest,
            isolation: fixed.receipt.isolation,
            command_digest: fixed.candidate_command_digest,
            classification: harness_eval::SampleClassification::Pass,
            trace_digest: harness_eval::NullableDigest(None),
            evidence_digest: harness_eval::NullableDigest(Some(
                fixed.grader_evidence.evidence_sha256.clone(),
            )),
            artifact_digest: harness_eval::NullableDigest(Some(
                fixed.materialization_artifact_digest,
            )),
            cost_receipt_digest: harness_eval::NullableDigest(None),
            seed: 1,
            sha256: String::new(),
        };
        sample.sha256 = canonical_digest_without_self(&sample)?;
        let sample_receipt = self.store.record_evaluation_sample(&NewEvaluationSample {
            id: sample.sample_id.clone(),
            evaluation_run_id: evaluation.id.clone(),
            controller_evidence_id: fixed.controller_evidence.evidence_id,
            grader_evidence_id: fixed.grader_evidence.evidence_id,
            eval_case_revision_id: case_revision,
            arm: EvaluationArm::Champion,
            sample,
            idempotency_key: "m2-observer-snapshot-fixed-sample-v1".to_owned(),
        })?;
        self.complete_from_sample(&sample_receipt, &repository_id)?;
        Ok(format!(
            "{}:{}",
            evaluation.id, "m2-observer-snapshot-fixed-sample"
        ))
    }

    fn complete_from_sample(
        &self,
        sample: &harness_store::EvaluationSampleReceipt,
        repository_id: &harness_domain::RepositoryId,
    ) -> Result<()> {
        if sample.evaluation_run_id != "m2-observer-snapshot-fixed-evaluation"
            || sample.arm != EvaluationArm::Champion
            || sample.classification != harness_eval::SampleClassification::Pass
            || sample.invalidated
        {
            bail!("existing sample cannot authoritatively complete the fixed evaluation");
        }
        let run = self.store.evaluation_run(&sample.evaluation_run_id)?;
        if run.invalidated
            || run.controller_run_id.as_str() != "m2-observer-snapshot-fixed"
            || !matches!(
                run.status,
                EvaluationRunStatus::Recording | EvaluationRunStatus::Completed
            )
        {
            bail!("existing fixed evaluation is terminally incompatible with completion replay");
        }
        let controller_run = self.store.run(&run.controller_run_id)?;
        if controller_run.repository_id != *repository_id
            || controller_run.base_sha != OBSERVER_SNAPSHOT_BOUNDED_FIX
        {
            bail!("existing fixed evaluation belongs to a different repository or base");
        }
        self.store
            .append_evaluation_run_status(&NewEvaluationRunStatus {
                id: "m2-observer-snapshot-fixed-evaluation-completed".to_owned(),
                evaluation_run_id: sample.evaluation_run_id.clone(),
                status: EvaluationRunStatus::Completed,
                receipt_digest: sample.sample_digest.clone(),
                idempotency_key: "m2-observer-snapshot-fixed-evaluation-completed-v1".to_owned(),
            })?;
        Ok(())
    }

    fn resolve_registered_repository(
        &self,
        requested: &Path,
    ) -> Result<(PathBuf, harness_domain::RepositoryId)> {
        let requested = std::fs::canonicalize(requested)
            .context("evaluation repository path must be an existing registered checkout")?;
        let registered = self
            .store
            .list_repositories()?
            .into_iter()
            .filter_map(|repository| {
                std::fs::canonicalize(&repository.root_path)
                    .ok()
                    .filter(|path| path == &requested)
                    .map(|_| repository.id)
            })
            .collect::<Vec<_>>();
        if registered.len() != 1 {
            bail!("evaluation repository path must resolve to exactly one registered repository");
        }
        Ok((
            requested,
            registered.into_iter().next().expect("checked exact count"),
        ))
    }

    fn ensure_controller_run(
        &self,
        repository_id: &harness_domain::RepositoryId,
        arm: &ObserverSnapshotArmPlan,
    ) -> Result<harness_domain::RunId> {
        let suffix = match arm.arm {
            ObserverSnapshotArm::Historical => "historical",
            ObserverSnapshotArm::Fixed => "fixed",
        };
        let run_id = harness_domain::RunId::from(format!("m2-observer-snapshot-{suffix}"));
        match self.store.run(&run_id) {
            Ok(existing) => {
                if existing.repository_id == *repository_id && existing.base_sha == arm.base_sha {
                    return Ok(run_id);
                }
                bail!("controller evaluation run id exists with different repository/base pin");
            }
            Err(harness_store::StoreError::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let digest = hex::encode(Sha256::digest(
            format!(
                "harness.m2.observer.authority.v1\0{repository_id}\0{}",
                arm.base_sha
            )
            .as_bytes(),
        ));
        self.store.create_run(&NewRun {
            id: run_id.clone(),
            repository_id: repository_id.clone(),
            title: format!("M2 observer snapshot {suffix}"),
            objective: "Controller-owned historical observer snapshot regression".to_owned(),
            mode: "evaluation".to_owned(),
            publication_mode: "none".to_owned(),
            state: "CREATED".to_owned(),
            phase: "evaluation_controller".to_owned(),
            base_ref: arm.base_sha.to_owned(),
            base_sha: arm.base_sha.to_owned(),
            authority_digest: digest.clone(),
            profile_digest: digest,
            codex_version: None,
            protocol_schema_sha256: None,
            requested_by: "evaluation-controller".to_owned(),
            token_budget: None,
        })?;
        Ok(run_id)
    }

    /// Append the three immutable controller wires in dependency order. The
    /// caller supplies no manifest: all identifiers and contents are closed
    /// constants derived from the historical reproduction receipt.
    fn ensure_fixed_eval_wires(
        &self,
        repository_id: &harness_domain::RepositoryId,
        historical_source_digest: &str,
    ) -> Result<(String, String, String)> {
        use harness_domain::{ImprovementEventId, ImprovementRecordKind, ImprovementSchema};
        use harness_eval::{
            AcceptanceClaim, AcceptanceKind, CaseCustody, CasePin, CasePrivacy, CaseRuntime,
            CaseSource, CaseSourceKind, GraderBundleV1, GraderIsolation, GraderKind, GraderRuntime,
            GraderSignal, NegativeControl, NegativeControlAction, PrivacyClass, SignalDirection,
            Split, TasksetV1,
        };
        let grader_id = "m2-observer-snapshot-grader".to_owned();
        let mut grader = GraderBundleV1 {
            schema: "harness.grader-bundle.v1".to_owned(),
            grader_bundle_id: grader_id.clone(),
            revision: 1,
            signals: vec![GraderSignal {
                id: "snapshot-bound".to_owned(),
                kind: GraderKind::Deterministic,
                direction: SignalDirection::BooleanPass,
                weight: 1.0,
                required: true,
                definition_digest: observer_snapshot_fixture_digest(ObserverSnapshotArm::Fixed),
                calibration_set_digest: None,
            }],
            hard_gates: vec!["snapshot-bound".to_owned()],
            negative_controls: vec![NegativeControl {
                id: "historical-admission".to_owned(),
                signal_id: "snapshot-bound".to_owned(),
                expected_relationship: "historical_reproduction_fails_target_behavior".to_owned(),
                failure_action: NegativeControlAction::Block,
            }],
            reward_integrity_required: true,
            isolation: GraderIsolation {
                candidate_write_access: false,
                holdout_answer_access: false,
                grader_runtime: GraderRuntime::SeparateProcess,
            },
            sha256: String::new(),
        };
        grader.sha256 = canonical_digest_without_self(&grader)?;
        let grader_revision = append_wire(
            &self.store,
            "m2-observer-snapshot-grader-r1",
            &grader_id,
            ImprovementRecordKind::GraderBundle,
            ImprovementSchema::GraderBundleV1,
            serde_json::to_value(&grader)?,
            &grader.sha256,
            ImprovementEventId::from("m2-observer-snapshot-grader-event".to_owned()),
        )?;
        let case_id = "m2-observer-snapshot-case".to_owned();
        let mut case = harness_eval::EvalCaseV1 {
            schema: "harness.eval-case.v1".to_owned(),
            case_id: case_id.clone(),
            revision: 1,
            title: "Observer snapshot bound".to_owned(),
            task_family: "trace_projection".to_owned(),
            objective: "Reject 10001 domain receipts while admitting 10000".to_owned(),
            source: CaseSource {
                kind: CaseSourceKind::Regression,
                locator: "controller:historical-observer-snapshot".to_owned(),
                digest: historical_source_digest.to_owned(),
            },
            split: Split::Development,
            runtime: CaseRuntime {
                repository_id: repository_id.to_string(),
                repository_fixture: "controller-owned-detached-worktree".to_owned(),
                base_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
                setup_digest: observer_snapshot_setup_digest(
                    ObserverSnapshotArm::Fixed,
                    OBSERVER_SNAPSHOT_BOUNDED_FIX,
                ),
                resource_class: "heavy".to_owned(),
                timeout_seconds: 120,
                token_budget: 1,
                seeds: Some(vec![1]),
            },
            custody: CaseCustody {
                owned_paths: vec![OBSERVER_SNAPSHOT_TARGET_PATH.to_owned()],
                forbidden_paths: vec![
                    "bins/harnessd/evaluation-assets".to_owned(),
                    "grader".to_owned(),
                    "holdout".to_owned(),
                ],
                grader_isolated: true,
            },
            acceptance: vec![AcceptanceClaim {
                claim_id: "snapshot-bound".to_owned(),
                kind: AcceptanceKind::Command,
                required: true,
                spec: Some(json!({"command_digest": observer_snapshot_command_digest()})),
            }],
            grader_bundle_id: grader_id,
            grader_bundle_revision: 1,
            grader_bundle_digest: grader.sha256.clone(),
            privacy: CasePrivacy {
                classification: PrivacyClass::Internal,
                export_allowed: false,
                license: None,
            },
            leakage_status: Some(harness_eval::LeakageStatus::Clean),
            sha256: String::new(),
        };
        case.sha256 = canonical_digest_without_self(&case)?;
        let case_revision = append_wire(
            &self.store,
            "m2-observer-snapshot-case-r1",
            &case_id,
            ImprovementRecordKind::EvalCase,
            ImprovementSchema::EvalCaseV1,
            serde_json::to_value(&case)?,
            &case.sha256,
            ImprovementEventId::from("m2-observer-snapshot-case-event".to_owned()),
        )?;
        let taskset_id = "m2-observer-snapshot-taskset".to_owned();
        let mut taskset = TasksetV1 {
            schema: "harness.taskset.v1".to_owned(),
            taskset_id: taskset_id.clone(),
            revision: 1,
            cases: vec![CasePin {
                case_id,
                revision: 1,
                split: Split::Development,
                case_digest: case.sha256,
            }],
            sha256: String::new(),
        };
        taskset.sha256 = canonical_digest_without_self(&taskset)?;
        let taskset_revision = append_wire(
            &self.store,
            "m2-observer-snapshot-taskset-r1",
            &taskset_id,
            ImprovementRecordKind::Taskset,
            ImprovementSchema::TasksetV1,
            serde_json::to_value(&taskset)?,
            &taskset.sha256,
            ImprovementEventId::from("m2-observer-snapshot-taskset-event".to_owned()),
        )?;
        Ok((taskset_revision, grader_revision, case_revision))
    }

    async fn run_arm(
        &self,
        repository: &Path,
        arm: &ObserverSnapshotArmPlan,
        run_id: harness_domain::RunId,
    ) -> Result<PersistedArm> {
        let attempt = execution_attempt_suffix()?;
        let expected_path = self.worktree_root.join(&arm.relative_worktree);
        let managed_root = std::fs::canonicalize(&self.worktree_root)
            .context("managed evaluation worktree root is unavailable")?;
        if expected_path.exists() {
            let canonical_path = std::fs::canonicalize(&expected_path)
                .context("managed evaluation worktree path cannot be canonicalized")?;
            if !canonical_path.starts_with(&managed_root) {
                bail!("managed evaluation worktree path escapes controller root");
            }
            let prior = self
                .store
                .list_worktrees(Some(&run_id))?
                .into_iter()
                .find(|worktree| worktree.path == expected_path.to_string_lossy());
            let Some(prior) = prior.filter(|worktree| {
                worktree.kind == "evaluation_controller"
                    && (worktree.state == "ACTIVE" || worktree.state == "REMOVED")
                    && worktree.base_sha == arm.base_sha
                    && worktree.head_sha.as_deref() == Some(arm.base_sha)
            }) else {
                bail!(
                    "managed evaluation worktree path exists without an exact controller custody record"
                );
            };
            if self.git.head_sha(&expected_path).await? != arm.base_sha {
                bail!("stale managed evaluation worktree head differs from pinned base");
            }
            self.git
                .remove_worktree(repository, &expected_path, true)
                .await
                .context("could not remove stale managed evaluation worktree")?;
            self.store.mark_worktree_removed(&prior.id)?;
        }
        let worktree = self
            .git
            .create_worktree(&WorktreeSpec {
                repository_root: repository.to_path_buf(),
                relative_path: arm.relative_worktree.clone(),
                base_sha: arm.base_sha.to_owned(),
                branch: None,
            })
            .await
            .context("could not create exact detached evaluation worktree")?;
        if worktree.head_sha != arm.base_sha {
            let _ = self
                .git
                .remove_worktree(repository, &worktree.path, true)
                .await;
            bail!("evaluation worktree head did not match its pinned base");
        }
        let worktree_id = harness_domain::WorktreeId::from(format!(
            "m2-observer-snapshot-{}-{attempt}-worktree",
            match arm.arm {
                ObserverSnapshotArm::Historical => "historical",
                ObserverSnapshotArm::Fixed => "fixed",
            }
        ));
        if let Err(error) = self
            .store
            .create_or_validate_evaluation_worktree(&NewWorktree {
                id: worktree_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                kind: "evaluation_controller".to_owned(),
                path: worktree.path.clone(),
                branch: None,
                base_sha: arm.base_sha.to_owned(),
                head_sha: Some(worktree.head_sha.clone()),
                state: "ACTIVE".to_owned(),
            })
        {
            match self
                .git
                .remove_worktree(repository, &worktree.path, true)
                .await
            {
                Ok(()) => return Err(error.into()),
                Err(cleanup_error) => bail!(
                    "could not register evaluation worktree ({error}); cleanup also failed: {cleanup_error}"
                ),
            }
        }
        let result = self
            .run_materialized_arm(arm.arm, arm.base_sha, &attempt, &worktree.path)
            .await;
        let persisted = match result {
            Ok(execution) => {
                let recorded = if !receipt_is_persistable(&execution.receipt) {
                    Err(anyhow::anyhow!(
                        "isolated evaluation arm did not produce a successful closed receipt; refusing success evidence"
                    ))
                } else {
                    self.persist_arm(&run_id, &worktree_id, arm.base_sha, &attempt, &execution)
                };
                // Command spools are transient. Output was either placed in
                // immutable artifact custody before persistence, or there is
                // no authority to retain it; in both cases discard safely.
                let discard = match execution.isolated.discard(&execution.candidate).await {
                    Ok(()) => execution.isolated.discard(&execution.grader).await,
                    Err(error) => Err(error),
                };
                match (recorded, discard) {
                    (Ok(receipt), Ok(())) => Ok(receipt),
                    (Ok(_), Err(error)) => Err(error.into()),
                    (Err(error), Ok(())) => Err(error),
                    (Err(error), Err(discard_error)) => Err(anyhow::anyhow!(
                        "evaluation persistence failed ({error}); spool cleanup also failed: {discard_error}"
                    )),
                }
            }
            Err(error) => Err(error),
        };
        let cleanup = self
            .git
            .remove_worktree(repository, &worktree.path, true)
            .await;
        if cleanup.is_ok() {
            self.store.mark_worktree_removed(&worktree_id)?;
        }
        match (persisted, cleanup) {
            (Ok(receipt), Ok(())) => Ok(receipt),
            (_, Err(error)) => bail!("evaluation cleanup failed: {error}"),
            (Err(error), Ok(())) => Err(error),
        }
    }

    async fn run_materialized_arm(
        &self,
        arm: ObserverSnapshotArm,
        base_sha: &str,
        attempt: &str,
        worktree: &Path,
    ) -> Result<ArmExecution> {
        let spool = self.spool_root.join(match arm {
            ObserverSnapshotArm::Historical => "historical",
            ObserverSnapshotArm::Fixed => "fixed",
        });
        let runner = CommandRunner::new(&spool, Default::default()).await?;
        let trusted = self.worktree_root.join("evaluation/observer-snapshot");
        let grader = trusted.join("grader");
        let holdout = trusted.join("holdout");
        let artifacts = trusted.join("artifacts");
        for path in [&grader, &holdout, &artifacts] {
            std::fs::create_dir_all(path)?;
        }
        let materialization = materialize_overlay(arm, base_sha, worktree, &artifacts)?;
        write_grader(&grader)?;
        let admission = self.cargo_cache_admission(arm, attempt, worktree)?;
        let mut custody_cleanup = CustodyCleanup::new(vec![
            admission.target_dir.clone(),
            admission.snapshot_root.clone(),
        ]);
        let registry_cache = admission.admission.registry_cache.clone();
        let git_cache = admission.admission.git_cache.clone();
        let toolchain_dir = admission.admission.toolchain_dir.clone();
        let isolated = EvaluationIsolationRunner::new(
            runner,
            &trusted,
            &trusted,
            &trusted,
            &trusted,
            self.spool_root.join("staging"),
        )?
        .with_cargo_build_cache(admission.admission)?;
        let outcome = isolated
            .run_candidate(CandidateIsolationSpec {
                command: CommandSpec {
                    program: OBSERVER_SNAPSHOT_COMMAND[0].to_owned(),
                    args: OBSERVER_SNAPSHOT_COMMAND[1..]
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                    cwd: worktree.to_path_buf(),
                    resource_class: harness_domain::ResourceClass::Heavy,
                    timeout_ms: 120_000,
                    inherited_environment: Vec::new(),
                    environment: Default::default(),
                    stdin: None,
                },
                grader_paths: Vec::new(),
                ground_truth_paths: Vec::new(),
            })
            .await?;
        let (post_registry, post_git, post_toolchain) =
            match snapshot_manifest_digests(&registry_cache, &git_cache, &toolchain_dir) {
                Ok(digests) => digests,
                Err(error) => {
                    return Err(cleanup_spools_after_error(&isolated, [&outcome], error).await);
                }
            };
        if post_registry != admission.registry_manifest_digest
            || post_git != admission.git_manifest_digest
            || post_toolchain != admission.toolchain_manifest_digest
        {
            let _ = isolated.discard(&outcome).await;
            bail!("controller cache/toolchain snapshot drifted during candidate execution");
        }
        let grader_outcome = match isolated
            .run_grader(GraderIsolationSpec {
                command: CommandSpec {
                    program: "/usr/bin/python3".to_owned(),
                    args: vec!["verify_materialization.py".to_owned()],
                    cwd: grader.clone(),
                    resource_class: harness_domain::ResourceClass::Control,
                    timeout_ms: 10_000,
                    inherited_environment: Vec::new(),
                    environment: Default::default(),
                    stdin: None,
                },
                grader_root: grader,
                ground_truth_paths: Vec::new(),
                artifact_path: materialization.artifact_path.clone(),
                artifact_sha256: materialization.artifact_sha256.clone(),
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = isolated.discard(&outcome).await;
                return Err(error.into());
            }
        };
        let after_grader =
            match snapshot_manifest_digests(&registry_cache, &git_cache, &toolchain_dir) {
                Ok(digests) => digests,
                Err(error) => {
                    return Err(cleanup_spools_after_error(
                        &isolated,
                        [&outcome, &grader_outcome],
                        error,
                    )
                    .await);
                }
            };
        if after_grader
            != (
                admission.registry_manifest_digest.clone(),
                admission.git_manifest_digest.clone(),
                admission.toolchain_manifest_digest.clone(),
            )
        {
            let _ = isolated.discard(&outcome).await;
            let _ = isolated.discard(&grader_outcome).await;
            bail!("controller cache/toolchain snapshot drifted during grader execution");
        }
        let isolation = if outcome.receipt.available {
            IsolationCapability::Available
        } else {
            IsolationCapability::InfrastructureUnavailable
        };
        let success = outcome
            .command
            .as_ref()
            .is_some_and(|command| command.succeeded())
            && grader_outcome
                .command
                .as_ref()
                .is_some_and(|command| command.succeeded());
        let classification = if !outcome.receipt.available {
            harness_eval::SampleClassification::InfrastructureUnavailable
        } else if success {
            match arm {
                ObserverSnapshotArm::Historical => harness_eval::SampleClassification::Fail,
                ObserverSnapshotArm::Fixed => harness_eval::SampleClassification::Pass,
            }
        } else {
            harness_eval::SampleClassification::Invalidated
        };
        let mut receipt = ObserverSnapshotControllerReceiptV1 {
            schema: "harness.eval.observer-snapshot-receipt.v2".to_owned(),
            arm,
            historical_base_sha: OBSERVER_SNAPSHOT_HISTORICAL_BASE.to_owned(),
            fixed_source_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
            signal: match arm {
                ObserverSnapshotArm::Historical => ObserverSnapshotSignal::HistoricalBugReproduced,
                ObserverSnapshotArm::Fixed => ObserverSnapshotSignal::FixedBoundEnforced,
            },
            materialization: materialization.receipt.clone(),
            fixture_digest: observer_snapshot_fixture_digest(arm),
            setup_digest: observer_snapshot_setup_digest(arm, base_sha),
            command_digest: observer_snapshot_command_digest(),
            candidate_isolation: isolation_pin(&outcome.receipt),
            grader_isolation: isolation_pin(&grader_outcome.receipt),
            registry_manifest_digest: admission.registry_manifest_digest,
            git_manifest_digest: admission.git_manifest_digest,
            toolchain_manifest_digest: admission.toolchain_manifest_digest,
            target_scope_digest: admission.target_scope_digest,
            isolation,
            classification,
            controller_exit_success: success,
            sha256: String::new(),
        };
        receipt.sha256 = canonical_digest_without_self(&receipt)?;
        let execution = ArmExecution {
            receipt,
            materialization,
            candidate: outcome,
            grader: grader_outcome,
            isolated,
        };
        if let Err(error) = custody_cleanup.cleanup() {
            return Err(cleanup_spools_after_error(
                &execution.isolated,
                [&execution.candidate, &execution.grader],
                error,
            )
            .await);
        }
        Ok(execution)
    }

    fn persist_arm(
        &self,
        run_id: &harness_domain::RunId,
        worktree_id: &harness_domain::WorktreeId,
        base_sha: &str,
        attempt: &str,
        execution: &ArmExecution,
    ) -> Result<PersistedArm> {
        let arm_label = arm_name(execution.receipt.arm);
        let arm_name = format!("{arm_label}-{attempt}");
        let candidate = execution
            .candidate
            .command
            .as_ref()
            .context("candidate isolation did not produce a durable command receipt")?;
        let grader = execution
            .grader
            .command
            .as_ref()
            .context("grader isolation did not produce a durable command receipt")?;
        let candidate_stdout =
            self.register_capture(run_id, arm_label, "candidate-stdout", &candidate.stdout)?;
        let candidate_stderr =
            self.register_capture(run_id, arm_label, "candidate-stderr", &candidate.stderr)?;
        let grader_stdout =
            self.register_capture(run_id, arm_label, "grader-stdout", &grader.stdout)?;
        let grader_stderr =
            self.register_capture(run_id, arm_label, "grader-stderr", &grader.stderr)?;
        let candidate_command = command_wire(&OBSERVER_SNAPSHOT_COMMAND);
        let candidate_command_digest = digest_json(&candidate_command)?;
        let candidate_id = harness_domain::CommandRunId::from(format!(
            "m2-observer-snapshot-{arm_name}-candidate"
        ));
        self.store
            .record_or_validate_evaluation_command(&NewCommandRecord {
                id: candidate_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                agent_session_id: None,
                worktree_id: Some(worktree_id.clone()),
                command: candidate_command,
                cwd: candidate.cwd.clone(),
                source_sha_before: Some(base_sha.to_owned()),
                source_sha_after: Some(base_sha.to_owned()),
                resource_class: "heavy".to_owned(),
                host_identity: None,
                target_profile: Some("offline-locked".to_owned()),
                started_at: candidate.started_at_ms,
                completed_at: candidate.started_at_ms + candidate.duration_ms as i64,
                exit_code: candidate.exit_code,
                signal: candidate.signal,
                timed_out: candidate.timed_out,
                result_class: candidate.result_class,
                stdout_artifact_id: Some(candidate_stdout),
                stderr_artifact_id: Some(candidate_stderr),
                error: None,
            })?;
        let grader_id =
            harness_domain::CommandRunId::from(format!("m2-observer-snapshot-{arm_name}-grader"));
        self.store
            .record_or_validate_evaluation_command(&NewCommandRecord {
                id: grader_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                agent_session_id: None,
                worktree_id: Some(worktree_id.clone()),
                command: command_wire(&["/usr/bin/python3", "verify_materialization.py"]),
                cwd: PathBuf::from("grader"),
                source_sha_before: Some(base_sha.to_owned()),
                source_sha_after: Some(base_sha.to_owned()),
                resource_class: "control".to_owned(),
                host_identity: None,
                target_profile: Some("separate-grader".to_owned()),
                started_at: grader.started_at_ms,
                completed_at: grader.started_at_ms + grader.duration_ms as i64,
                exit_code: grader.exit_code,
                signal: grader.signal,
                timed_out: grader.timed_out,
                result_class: grader.result_class,
                stdout_artifact_id: Some(grader_stdout),
                stderr_artifact_id: Some(grader_stderr),
                error: None,
            })?;
        let validation_id = harness_domain::ValidationId::from(format!(
            "m2-observer-snapshot-{arm_name}-validation"
        ));
        self.store
            .record_or_validate_evaluation_validation(&NewValidationRecord {
                id: validation_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                worktree_id: worktree_id.clone(),
                validator_id: "controller-observer-snapshot".to_owned(),
                proof_tier: harness_domain::ProofTier::T3,
                source_sha: base_sha.to_owned(),
                selector_reason: "controller_owned_observer_snapshot_bound".to_owned(),
                result_class: harness_domain::ResultClass::Success,
                command_run_id: Some(candidate_id),
                started_at: candidate.started_at_ms,
                completed_at: candidate.started_at_ms + candidate.duration_ms as i64,
            })?;
        let grader_validation_id = harness_domain::ValidationId::from(format!(
            "m2-observer-snapshot-{arm_name}-grader-validation"
        ));
        self.store
            .record_or_validate_evaluation_validation(&NewValidationRecord {
                id: grader_validation_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                worktree_id: worktree_id.clone(),
                validator_id: "controller-observer-snapshot-grader".to_owned(),
                proof_tier: harness_domain::ProofTier::T3,
                source_sha: base_sha.to_owned(),
                selector_reason: "separate_controller_owned_materialization_grader".to_owned(),
                result_class: harness_domain::ResultClass::Success,
                command_run_id: Some(grader_id.clone()),
                started_at: grader.started_at_ms,
                completed_at: grader.started_at_ms + grader.duration_ms as i64,
            })?;
        let materialization_artifact = self.register_file(
            run_id,
            arm_label,
            "materialization",
            &execution.materialization.artifact_path,
            "application/json",
        )?;
        let materialization_artifact_digest =
            self.store.artifact(&materialization_artifact)?.sha256;
        let controller_evidence = self.record_evaluation_evidence(
            harness_domain::EvidenceId::from(format!("m2-observer-snapshot-{arm_name}-controller-evidence")), run_id,
            Some(validation_id), "controller_materialized_observer_snapshot", base_sha,
            vec!["exact_base".to_owned(), "closed_overlay".to_owned(), "candidate_command".to_owned()],
            json!({"receipt": execution.receipt, "candidate_command_digest": candidate_command_digest, "fixture_command_digest": observer_snapshot_command_digest()}),
            vec![(materialization_artifact.clone(), "controller_materialization_receipt".to_owned())],
        )?;
        let grader_evidence = self.record_evaluation_evidence(
            harness_domain::EvidenceId::from(format!("m2-observer-snapshot-{arm_name}-grader-evidence")), run_id,
            Some(grader_validation_id), "separate_grader_verified_materialization", base_sha,
            vec!["separate_process".to_owned(), "materialization_bound".to_owned(), "closed_signal".to_owned()],
            json!({"receipt_digest": execution.receipt.sha256, "grader_command": grader_id.to_string(), "materialization_artifact_digest": materialization_artifact_digest}),
            vec![(materialization_artifact, "grader_verified_materialization".to_owned())],
        )?;
        // Both runner spools are discarded only after their command output is
        // registered and both typed evidence chains have committed.
        // The underlying files remain in artifact custody.
        Ok(PersistedArm {
            receipt: execution.receipt.clone(),
            controller_evidence,
            grader_evidence,
            materialization_artifact_digest,
            candidate_command_digest,
        })
    }

    fn register_capture(
        &self,
        run_id: &harness_domain::RunId,
        arm: &str,
        name: &str,
        capture: &harness_runner::StreamCapture,
    ) -> Result<harness_domain::ArtifactId> {
        let stored = self.store.artifacts().put_file(&capture.path)?;
        self.store
            .register_or_validate_evaluation_artifact(&NewArtifact {
                id: harness_domain::ArtifactId::from(format!("m2-observer-snapshot-{arm}-{name}")),
                run_id: Some(run_id.clone()),
                task_attempt_id: None,
                kind: "command_stream".to_owned(),
                logical_name: format!("{arm}-{name}.log"),
                storage_path: stored.path,
                sha256: stored.digest,
                media_type: "text/plain".to_owned(),
                compression: None,
                sensitivity: "internal".to_owned(),
                byte_length: stored.byte_length,
                retention_class: "evaluation".to_owned(),
                pinned: true,
            })
            .map_err(Into::into)
    }

    fn register_file(
        &self,
        run_id: &harness_domain::RunId,
        arm: &str,
        name: &str,
        path: &Path,
        media_type: &str,
    ) -> Result<harness_domain::ArtifactId> {
        let stored = self.store.artifacts().put_file(path)?;
        self.store
            .register_or_validate_evaluation_artifact(&NewArtifact {
                id: harness_domain::ArtifactId::from(format!("m2-observer-snapshot-{arm}-{name}")),
                run_id: Some(run_id.clone()),
                task_attempt_id: None,
                kind: "evaluation_materialization".to_owned(),
                logical_name: format!("{arm}-{name}"),
                storage_path: stored.path,
                sha256: stored.digest,
                media_type: media_type.to_owned(),
                compression: None,
                sensitivity: "internal".to_owned(),
                byte_length: stored.byte_length,
                retention_class: "evaluation".to_owned(),
                pinned: true,
            })
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_evaluation_evidence(
        &self,
        id: harness_domain::EvidenceId,
        run_id: &harness_domain::RunId,
        validation_id: Option<harness_domain::ValidationId>,
        claim_id: &str,
        source_sha: &str,
        checklist_rows: Vec<String>,
        details: serde_json::Value,
        artifact_links: Vec<(harness_domain::ArtifactId, String)>,
    ) -> Result<PersistedEvidence> {
        let artifact_ids = artifact_links.iter().map(|(id, _)| id).collect::<Vec<_>>();
        let evidence = json!({
            "schema": "harness-evidence/v1", "claim_id": claim_id, "source_sha": source_sha,
            "proof_tier": harness_domain::ProofTier::T3, "result_class": harness_domain::ResultClass::Success,
            "details": details, "artifact_ids": artifact_ids,
        });
        let evidence_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&evidence)?));
        self.store.record_or_validate_evaluation_evidence(
            &NewEvidenceRecord {
                id: id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                validation_id,
                claim_id: claim_id.to_owned(),
                checklist_rows,
                source_sha: source_sha.to_owned(),
                proof_tier: harness_domain::ProofTier::T3,
                result_class: harness_domain::ResultClass::Success,
                evidence,
                unproved_claims: Vec::new(),
            },
            &artifact_links,
        )?;
        Ok(PersistedEvidence {
            evidence_id: id,
            evidence_sha256,
        })
    }

    /// Only daemon-controlled cache roots are admitted. The candidate gets
    /// mounted registry/git snapshots, never Cargo config or credentials, and
    /// receives a fresh writable target child per pinned arm.
    fn cargo_cache_admission(
        &self,
        arm: ObserverSnapshotArm,
        attempt: &str,
        worktree: &Path,
    ) -> Result<CargoAdmission> {
        let cargo_home = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
            .context("controller Cargo home is unavailable; fail-closed evaluation preflight")?;
        let registry = cargo_home.join("registry");
        let git = cargo_home.join("git");
        if !registry.join("index").is_dir()
            || !registry.join("cache").is_dir()
            || !registry.join("src").is_dir()
            || !git.is_dir()
        {
            bail!(
                "controller offline Cargo registry/git snapshot is unavailable; fail-closed evaluation preflight"
            );
        }
        let lock = std::fs::read_to_string(worktree.join("Cargo.lock"))
            .context("pinned evaluation worktree has no readable Cargo.lock")?;
        if lock
            .lines()
            .any(|line| line.trim_start().starts_with("source = \"git+"))
        {
            bail!("sealed evaluation does not admit Git-sourced Cargo dependencies");
        }
        let toolchain = discover_controller_toolchain()?;
        let snapshot_root = self
            .spool_root
            .join(format!("sealed-input-{}-{attempt}", arm_name(arm)));
        std::fs::create_dir(&snapshot_root)?;
        let registry_snapshot = snapshot_root.join("registry");
        let git_snapshot = snapshot_root.join("git");
        let toolchain_snapshot = snapshot_root.join("toolchain");
        let target_root = self.spool_root.join("cargo-target");
        let target = target_root.join(format!("{}-{attempt}", arm_name(arm)));
        let result = (|| {
            sealed_copy_registry(&lock, &registry, &registry_snapshot)?;
            sealed_empty_git_snapshot(&git_snapshot)?;
            sealed_copy_tree(&toolchain, &toolchain_snapshot)?;
            std::fs::create_dir_all(&target_root)?;
            std::fs::create_dir(&target)?;
            let registry_manifest_digest = content_manifest_digest(&registry_snapshot)?;
            let git_manifest_digest = content_manifest_digest(&git_snapshot)?;
            let toolchain_manifest_digest = content_manifest_digest(&toolchain_snapshot)?;
            let target_scope_digest = digest_text(&format!(
                "harness.m2.empty-target-scope.v1\0{}\0{}\0{attempt}",
                target.display(),
                arm_name(arm)
            ));
            Ok(CargoAdmission {
                admission: CargoBuildCacheAdmission {
                    trusted_registry_root: snapshot_root.clone(),
                    registry_cache: registry_snapshot,
                    registry_receipt_digest: registry_manifest_digest.clone(),
                    trusted_git_root: snapshot_root.clone(),
                    git_cache: git_snapshot,
                    git_receipt_digest: git_manifest_digest.clone(),
                    trusted_target_root: target_root,
                    target_dir: target.clone(),
                    target_receipt_digest: target_scope_digest.clone(),
                    trusted_toolchain_root: snapshot_root.clone(),
                    toolchain_dir: toolchain_snapshot,
                    toolchain_receipt_digest: toolchain_manifest_digest.clone(),
                },
                snapshot_root: snapshot_root.clone(),
                target_dir: target.clone(),
                registry_manifest_digest,
                git_manifest_digest,
                toolchain_manifest_digest,
                target_scope_digest,
            })
        })();
        match result {
            Ok(admission) => Ok(admission),
            Err(error) => {
                let mut cleanup = CustodyCleanup::new(vec![target, snapshot_root]);
                match cleanup.cleanup() {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => bail!(
                        "sealed input admission failed ({error}); partial custody cleanup also failed: {cleanup_error}"
                    ),
                }
            }
        }
    }
}

fn command_wire(parts: &[&str]) -> serde_json::Value {
    json!({"program": parts[0], "args": parts[1..]})
}

fn digest_json(value: &serde_json::Value) -> Result<String> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_string(value)?.as_bytes(),
    )))
}

fn digest_text(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn receipt_is_persistable(receipt: &ObserverSnapshotControllerReceiptV1) -> bool {
    grade_observer_snapshot_controller_receipt(receipt) == ObserverSnapshotGrade::Pass
}

fn snapshot_manifest_digests(
    registry: &Path,
    git: &Path,
    toolchain: &Path,
) -> Result<(String, String, String)> {
    Ok((
        content_manifest_digest(registry)?,
        content_manifest_digest(git)?,
        content_manifest_digest(toolchain)?,
    ))
}

async fn cleanup_spools_after_error<'a>(
    isolated: &EvaluationIsolationRunner,
    outcomes: impl IntoIterator<Item = &'a harness_runner::EvaluationIsolationOutcome>,
    error: anyhow::Error,
) -> anyhow::Error {
    let mut cleanup_errors = Vec::new();
    for outcome in outcomes {
        if let Err(cleanup_error) = isolated.discard(outcome).await {
            cleanup_errors.push(cleanup_error.to_string());
        }
    }
    if cleanup_errors.is_empty() {
        error
    } else {
        anyhow::anyhow!(
            "evaluation failed ({error}); command spool cleanup also failed: {}",
            cleanup_errors.join("; ")
        )
    }
}

const SNAPSHOT_MAX_ENTRIES: usize = 1_000_000;
const SNAPSHOT_MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const SNAPSHOT_MAX_FILE_BYTES: u64 = 1024 * 1024 * 1024;
const SNAPSHOT_MAX_PATH_BYTES: usize = 1024;
const SNAPSHOT_MAX_DEPTH: usize = 64;

#[derive(Default)]
struct SnapshotBudget {
    entries: usize,
    bytes: u64,
}

impl SnapshotBudget {
    fn admit(&mut self, relative: &str, file_bytes: u64) -> Result<()> {
        if relative.len() > SNAPSHOT_MAX_PATH_BYTES {
            bail!("controller snapshot exceeds path bound");
        }
        self.entries = self
            .entries
            .checked_add(1)
            .context("controller snapshot entry overflow")?;
        if self.entries > SNAPSHOT_MAX_ENTRIES {
            bail!("controller snapshot exceeds entry bound");
        }
        if file_bytes > SNAPSHOT_MAX_FILE_BYTES {
            bail!("controller snapshot exceeds file bound");
        }
        self.bytes = self
            .bytes
            .checked_add(file_bytes)
            .context("controller snapshot byte overflow")?;
        if self.bytes > SNAPSHOT_MAX_TOTAL_BYTES {
            bail!("controller snapshot exceeds byte bound");
        }
        Ok(())
    }
}

fn content_manifest_digest(root: &Path) -> Result<String> {
    fn collect(
        root: &Path,
        path: &Path,
        depth: usize,
        rows: &mut Vec<String>,
        budget: &mut SnapshotBudget,
    ) -> Result<()> {
        if depth > SNAPSHOT_MAX_DEPTH {
            bail!("controller cache manifest exceeds depth bound");
        }
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let entry_path = entry.path();
            let relative = entry_path
                .strip_prefix(root)?
                .to_str()
                .context("controller cache manifest rejects non-UTF8 path")?;
            if file_type.is_symlink() {
                bail!("controller cache manifest rejects symlink");
            }
            if file_type.is_dir() {
                budget.admit(relative, 0)?;
                collect(root, &entry_path, depth + 1, rows, budget)?;
            } else if file_type.is_file() {
                let metadata = entry.metadata()?;
                budget.admit(relative, metadata.len())?;
                let mut file = std::fs::File::open(&entry_path)?;
                let mut hasher = Sha256::new();
                let mut buffer = [0_u8; 64 * 1024];
                loop {
                    use std::io::Read;
                    let read = file.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                }
                let digest = hasher.finalize();
                rows.push(format!(
                    "{relative}\0{}\0{}",
                    metadata.len(),
                    hex::encode(digest)
                ));
            } else {
                bail!("controller cache manifest rejects special file");
            }
        }
        Ok(())
    }
    let mut rows = Vec::new();
    let mut budget = SnapshotBudget::default();
    collect(root, root, 0, &mut rows, &mut budget)?;
    rows.sort();
    Ok(digest_text(&format!(
        "harness.m2.cache-manifest.v1\0{}",
        rows.join("\0")
    )))
}

fn sealed_copy_tree_with_budget(
    source: &Path,
    target: &Path,
    budget: &mut SnapshotBudget,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    fn copy(
        root: &Path,
        source: &Path,
        target: &Path,
        depth: usize,
        budget: &mut SnapshotBudget,
    ) -> Result<()> {
        if depth > SNAPSHOT_MAX_DEPTH {
            bail!("sealed snapshot exceeds depth bound");
        }
        let ty = std::fs::symlink_metadata(source)?.file_type();
        if ty.is_symlink() || !ty.is_dir() {
            bail!("sealed snapshot source must be a real directory");
        }
        std::fs::create_dir(target)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            let name = entry.file_name();
            let from = entry.path();
            let to = target.join(name);
            let relative = from
                .strip_prefix(root)?
                .to_str()
                .context("sealed snapshot rejects non-UTF8 path")?;
            let before = std::fs::symlink_metadata(&from)?;
            let ty = before.file_type();
            if ty.is_symlink() {
                bail!("sealed snapshot rejects symlink");
            }
            if ty.is_dir() {
                budget.admit(relative, 0)?;
                copy(root, &from, &to, depth + 1, budget)?;
            } else if ty.is_file() {
                budget.admit(relative, before.len())?;
                let mut input = std::fs::File::open(&from)?;
                let opened = input.metadata()?;
                if !opened.is_file() || opened.dev() != before.dev() || opened.ino() != before.ino()
                {
                    bail!("sealed snapshot source changed during admission");
                }
                let mut output = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&to)?;
                let copied = std::io::copy(&mut input, &mut output)?;
                if copied != before.len() {
                    bail!("sealed snapshot source changed size during admission");
                }
                output.sync_all()?;
                let sealed_mode = 0o444 | (opened.permissions().mode() & 0o111);
                std::fs::set_permissions(&to, std::fs::Permissions::from_mode(sealed_mode))?;
            } else {
                bail!("sealed snapshot rejects special file");
            }
        }
        std::fs::File::open(target)?.sync_all()?;
        std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o555))?;
        Ok(())
    }
    copy(source, source, target, 0, budget)
}

fn sealed_copy_tree(source: &Path, target: &Path) -> Result<()> {
    let result = sealed_copy_tree_with_budget(source, target, &mut SnapshotBudget::default());
    match result {
        Ok(()) => Ok(()),
        Err(error) => match remove_custody_tree(target) {
            Ok(()) => Err(error),
            Err(cleanup_error) => bail!(
                "sealed snapshot copy failed ({error}); partial copy cleanup also failed: {cleanup_error}"
            ),
        },
    }
}

fn sealed_copy_file_with_budget(
    source_root: &Path,
    source: &Path,
    target: &Path,
    expected_sha256: Option<&str>,
    budget: &mut SnapshotBudget,
) -> Result<()> {
    use std::io::{Read, Write};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let relative = source
        .strip_prefix(source_root)?
        .to_str()
        .context("sealed snapshot rejects non-UTF8 path")?;
    let before = std::fs::symlink_metadata(source)?;
    if before.file_type().is_symlink() || !before.is_file() {
        bail!("sealed snapshot source must be a real file");
    }
    budget.admit(relative, before.len())?;
    let mut input = std::fs::File::open(source)?;
    let opened = input.metadata()?;
    if !opened.is_file() || opened.dev() != before.dev() || opened.ino() != before.ino() {
        bail!("sealed snapshot source changed during admission");
    }
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    let mut hasher = Sha256::new();
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        copied = copied
            .checked_add(u64::try_from(read)?)
            .context("sealed snapshot copy size overflow")?;
    }
    if copied != before.len() {
        bail!("sealed snapshot source changed size during admission");
    }
    let after = input.metadata()?;
    if after.dev() != before.dev() || after.ino() != before.ino() || after.len() != before.len() {
        bail!("sealed snapshot source changed during admission");
    }
    let digest = hex::encode(hasher.finalize());
    if expected_sha256.is_some_and(|expected| digest != expected) {
        bail!("sealed Cargo archive does not match Cargo.lock checksum");
    }
    output.sync_all()?;
    let sealed_mode = 0o444 | (opened.permissions().mode() & 0o111);
    std::fs::set_permissions(target, std::fs::Permissions::from_mode(sealed_mode))?;
    Ok(())
}

fn seal_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::File::open(path)?.sync_all()?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555))?;
    Ok(())
}

fn safe_registry_component(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 200
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'+'))
    {
        bail!("Cargo.lock contains invalid {label}");
    }
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sealed_copy_registry(lock: &str, source: &Path, target: &Path) -> Result<()> {
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct LockedPackage {
        name: String,
        version: String,
        checksum: String,
    }

    let result = (|| {
        let document: toml::Value =
            toml::from_str(lock).context("pinned Cargo.lock is not valid TOML")?;
        let package_rows = document
            .get("package")
            .and_then(toml::Value::as_array)
            .context("pinned Cargo.lock has no package records")?;
        let mut packages = BTreeSet::new();
        for row in package_rows {
            let Some(source_kind) = row.get("source").and_then(toml::Value::as_str) else {
                continue;
            };
            if !source_kind.starts_with("registry+") && !source_kind.starts_with("sparse+") {
                bail!("sealed evaluation does not admit non-registry Cargo dependencies");
            }
            let name = row
                .get("name")
                .and_then(toml::Value::as_str)
                .context("registry Cargo.lock package has no name")?;
            let version = row
                .get("version")
                .and_then(toml::Value::as_str)
                .context("registry Cargo.lock package has no version")?;
            let checksum = row
                .get("checksum")
                .and_then(toml::Value::as_str)
                .context("registry Cargo.lock package has no checksum")?;
            safe_registry_component(name, "registry package name")?;
            safe_registry_component(version, "registry package version")?;
            if !is_lower_sha256(checksum) {
                bail!("Cargo.lock contains invalid registry package checksum");
            }
            packages.insert(LockedPackage {
                name: name.to_owned(),
                version: version.to_owned(),
                checksum: checksum.to_owned(),
            });
        }
        if packages.is_empty() {
            bail!("pinned Cargo.lock has no registry packages");
        }

        let cache_root = source.join("cache");
        let src_root = source.join("src");
        let index_root = source.join("index");
        let mut cache_hashes = BTreeMap::new();
        for entry in std::fs::read_dir(&cache_root)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("Cargo registry cache rejects non-UTF8 path"))?;
            safe_registry_component(&name, "registry cache identifier")?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("Cargo registry cache contains unexpected entry");
            }
            cache_hashes.insert(name, entry.path());
        }
        if cache_hashes.is_empty() {
            bail!("controller Cargo registry cache is empty");
        }

        let mut resolved = Vec::with_capacity(packages.len());
        let mut used_hashes = BTreeSet::new();
        for package in packages {
            let archive_name = format!("{}-{}.crate", package.name, package.version);
            safe_registry_component(&archive_name, "registry archive name")?;
            let source_name = format!("{}-{}", package.name, package.version);
            safe_registry_component(&source_name, "registry source directory")?;
            let mut matches = Vec::new();
            for (registry_hash, cache_dir) in &cache_hashes {
                let archive = cache_dir.join(&archive_name);
                if archive.is_file() {
                    let actual = sha256_file(&archive)?;
                    if actual == package.checksum {
                        let unpacked = src_root.join(registry_hash).join(&source_name);
                        if !unpacked.is_dir() {
                            bail!("locked Cargo package has no unpacked offline source");
                        }
                        matches.push((registry_hash.clone(), archive, unpacked));
                    }
                }
            }
            let [(registry_hash, archive, unpacked)] = matches.as_slice() else {
                bail!("locked Cargo package archive is missing, ambiguous, or has wrong checksum");
            };
            used_hashes.insert(registry_hash.clone());
            resolved.push((
                package,
                registry_hash.clone(),
                archive.clone(),
                unpacked.clone(),
            ));
        }

        std::fs::create_dir(target)?;
        let target_index = target.join("index");
        let target_cache = target.join("cache");
        let target_src = target.join("src");
        for directory in [&target_index, &target_cache, &target_src] {
            std::fs::create_dir(directory)?;
        }
        let mut budget = SnapshotBudget::default();
        for registry_hash in &used_hashes {
            budget.admit(&format!("index/{registry_hash}"), 0)?;
            let index = index_root.join(registry_hash);
            if !index.is_dir() {
                bail!("locked Cargo registry has no offline index");
            }
            sealed_copy_tree_with_budget(&index, &target_index.join(registry_hash), &mut budget)?;
            for parent in [
                target_cache.join(registry_hash),
                target_src.join(registry_hash),
            ] {
                budget.admit(
                    parent
                        .strip_prefix(target)?
                        .to_str()
                        .context("sealed registry rejects non-UTF8 target")?,
                    0,
                )?;
                std::fs::create_dir(parent)?;
            }
        }
        for (package, registry_hash, archive, unpacked) in resolved {
            let archive_target = target_cache.join(&registry_hash).join(
                archive
                    .file_name()
                    .context("locked Cargo archive has no file name")?,
            );
            sealed_copy_file_with_budget(
                source,
                &archive,
                &archive_target,
                Some(&package.checksum),
                &mut budget,
            )?;
            let unpacked_target = target_src.join(&registry_hash).join(
                unpacked
                    .file_name()
                    .context("locked Cargo source has no directory name")?,
            );
            budget.admit(
                unpacked_target
                    .strip_prefix(target)?
                    .to_str()
                    .context("sealed registry rejects non-UTF8 target")?,
                0,
            )?;
            sealed_copy_tree_with_budget(&unpacked, &unpacked_target, &mut budget)?;
        }
        for registry_hash in used_hashes {
            seal_directory(&target_cache.join(&registry_hash))?;
            seal_directory(&target_src.join(&registry_hash))?;
        }
        for directory in [&target_index, &target_cache, &target_src, target] {
            seal_directory(directory)?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok(()),
        Err(error) => match remove_custody_tree(target) {
            Ok(()) => Err(error),
            Err(cleanup_error) => bail!(
                "sealed Cargo registry copy failed ({error}); partial copy cleanup also failed: {cleanup_error}"
            ),
        },
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn sealed_empty_git_snapshot(target: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir(target)?;
    for child in ["db", "checkouts"] {
        let child = target.join(child);
        std::fs::create_dir(&child)?;
        std::fs::File::open(&child)?.sync_all()?;
        std::fs::set_permissions(child, std::fs::Permissions::from_mode(0o555))?;
    }
    std::fs::File::open(target)?.sync_all()?;
    std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o555))?;
    Ok(())
}

fn remove_custody_tree(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || metadata.is_file() {
        std::fs::remove_file(path)?;
        return Ok(());
    }
    if !metadata.is_dir() {
        bail!("refusing to remove unexpected custody object");
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    for entry in std::fs::read_dir(path)? {
        remove_custody_tree(&entry?.path())?;
    }
    std::fs::remove_dir(path)?;
    Ok(())
}

fn arm_name(arm: ObserverSnapshotArm) -> &'static str {
    match arm {
        ObserverSnapshotArm::Historical => "historical",
        ObserverSnapshotArm::Fixed => "fixed",
    }
}

fn execution_attempt_suffix() -> Result<String> {
    Ok(format!("a{}", ulid::Ulid::generate()))
}

fn isolation_pin(
    receipt: &harness_runner::EvaluationIsolationReceipt,
) -> ObserverSnapshotIsolationPinV1 {
    ObserverSnapshotIsolationPinV1 {
        backend: receipt.backend.clone(),
        backend_version: receipt.backend_version.clone(),
        namespaces: receipt.namespaces.clone(),
        candidate_access: receipt.candidate_access.clone(),
        grader_access: receipt.grader_access.clone(),
        artifact_access: receipt.artifact_access.clone(),
        available: receipt.available,
        policy_digest: receipt.policy_digest.clone(),
        receipt_digest: receipt.digest.clone(),
    }
}

fn discover_controller_toolchain() -> Result<PathBuf> {
    let output = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .context("controller Rust toolchain probe failed; fail-closed evaluation preflight")?;
    if !output.status.success() {
        bail!("controller Rust toolchain probe failed; fail-closed evaluation preflight");
    }
    let sysroot = std::str::from_utf8(&output.stdout)
        .context("controller Rust toolchain probe returned invalid UTF-8")?
        .trim();
    let toolchain = std::fs::canonicalize(sysroot)
        .context("controller Rust sysroot is unavailable; fail-closed evaluation preflight")?;
    if !toolchain.join("bin/cargo").is_file() || !toolchain.join("bin/rustc").is_file() {
        bail!("controller Rust sysroot lacks cargo/rustc; fail-closed evaluation preflight");
    }
    Ok(toolchain)
}

fn materialize_overlay(
    arm: ObserverSnapshotArm,
    base_sha: &str,
    worktree: &Path,
    artifacts: &Path,
) -> Result<MaterializedOverlay> {
    let target = worktree.join(OBSERVER_SNAPSHOT_TARGET_PATH);
    let original = std::fs::read_to_string(&target)?;
    let closing = original
        .rfind("\n}")
        .context("test module closing brace is missing")?;
    let overlay = match arm {
        ObserverSnapshotArm::Historical => include_str!(
            "../../../crates/harness-eval/evaluation-fixtures/observer_snapshot_bound_historical_overlay.rs"
        ),
        ObserverSnapshotArm::Fixed => include_str!(
            "../../../crates/harness-eval/evaluation-fixtures/observer_snapshot_bound_fixed_overlay.rs"
        ),
    };
    let materialized = format!("{}\n\n{}\n}}\n", &original[..closing], overlay);
    std::fs::write(&target, materialized.as_bytes())?;
    let artifact = json!({
        "schema": "harness.eval.materialization-artifact.v1",
        "arm": match arm { ObserverSnapshotArm::Historical => "historical", ObserverSnapshotArm::Fixed => "fixed" },
        "signal": match arm { ObserverSnapshotArm::Historical => "historical_bug_reproduced", ObserverSnapshotArm::Fixed => "fixed_bound_enforced" },
        "base_checkout_sha": base_sha,
        "target_path": OBSERVER_SNAPSHOT_TARGET_PATH,
        "original_target_hex": hex::encode(original.as_bytes()),
        "overlay_hex": hex::encode(overlay.as_bytes()),
        "overlay_digest": observer_snapshot_fixture_digest(arm),
        "resulting_target_digest": hex::encode(Sha256::digest(materialized.as_bytes())),
    });
    let artifact_bytes = serde_json::to_vec(&artifact)?;
    let artifact_path = artifacts.join(match arm {
        ObserverSnapshotArm::Historical => "historical-materialization.json",
        ObserverSnapshotArm::Fixed => "fixed-materialization.json",
    });
    std::fs::write(&artifact_path, &artifact_bytes)?;
    let mut receipt = ObserverSnapshotMaterializationReceiptV1 {
        schema: "harness.eval.observer-snapshot-materialization.v1".to_owned(),
        base_checkout_sha: base_sha.to_owned(),
        target_path: OBSERVER_SNAPSHOT_TARGET_PATH.to_owned(),
        arm,
        overlay_digest: observer_snapshot_fixture_digest(arm),
        resulting_target_digest: hex::encode(Sha256::digest(materialized.as_bytes())),
        command_digest: observer_snapshot_command_digest(),
        sha256: String::new(),
    };
    receipt.sha256 = canonical_digest_without_self(&receipt)?;
    Ok(MaterializedOverlay {
        receipt,
        artifact_path,
        artifact_sha256: hex::encode(Sha256::digest(&artifact_bytes)),
    })
}

fn write_grader(root: &Path) -> Result<()> {
    const SCRIPT: &str = include_str!("../evaluation-assets/observer_snapshot_grader.py");
    std::fs::write(root.join("verify_materialization.py"), SCRIPT)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_wire(
    store: &Store,
    revision_id: &str,
    aggregate_id: &str,
    kind: harness_domain::ImprovementRecordKind,
    schema: harness_domain::ImprovementSchema,
    payload: serde_json::Value,
    _wire_digest: &str,
    event_id: harness_domain::ImprovementEventId,
) -> Result<String> {
    let envelope_digest = hex::encode(Sha256::digest(serde_json::to_string(&payload)?.as_bytes()));
    let (record, _) = store.append_improvement_revision(&NewImprovementRevision {
        id: revision_id.to_owned(),
        aggregate_kind: kind,
        aggregate_id: aggregate_id.to_owned(),
        schema,
        state: harness_domain::ImprovementState::Proposed,
        payload,
        payload_sha256: envelope_digest,
        sensitivity: harness_domain::SensitivityClass::Internal,
        retention_class: harness_domain::RetentionClass::Evaluation,
        export_allowed: false,
        idempotency_key: format!("{revision_id}:idempotent"),
        event_id,
        source_raw_event_id: None,
        source_domain_event_id: None,
    })?;
    Ok(record.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_plan_is_closed_to_the_two_controller_owned_pins() {
        let plan = ObserverSnapshotPlan::new(PathBuf::from("/repo"));
        assert_eq!(plan.arms[0].base_sha, OBSERVER_SNAPSHOT_HISTORICAL_BASE);
        assert_eq!(plan.arms[1].base_sha, OBSERVER_SNAPSHOT_BOUNDED_FIX);
        assert_ne!(
            observer_snapshot_fixture_digest(plan.arms[0].arm),
            observer_snapshot_fixture_digest(plan.arms[1].arm)
        );
    }

    #[test]
    fn fixed_wires_are_persisted_as_proposed_immutable_records() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let service = EvaluationService::new(
            store.clone(),
            temp.path().join("worktrees"),
            temp.path().join("spool"),
        )
        .unwrap();
        let source = "a".repeat(64);
        let repository_id = harness_domain::RepositoryId::from("test-repository");
        let _ = service
            .ensure_fixed_eval_wires(&repository_id, &source)
            .unwrap();
        for (kind, id) in [
            (
                harness_domain::ImprovementRecordKind::Taskset,
                "m2-observer-snapshot-taskset".to_owned(),
            ),
            (
                harness_domain::ImprovementRecordKind::GraderBundle,
                "m2-observer-snapshot-grader".to_owned(),
            ),
            (
                harness_domain::ImprovementRecordKind::EvalCase,
                "m2-observer-snapshot-case".to_owned(),
            ),
        ] {
            let record = store
                .improvement_current_revision(kind, &id)
                .unwrap()
                .unwrap();
            assert_eq!(record.state, harness_domain::ImprovementState::Proposed);
        }
    }

    #[test]
    fn cargo_cache_manifest_digest_is_stable_for_the_same_content() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = temp.path().join("registry");
        std::fs::create_dir_all(&cache).unwrap();
        let other = temp.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("closed-receipt"), b"different").unwrap();
        assert_eq!(
            content_manifest_digest(&cache).unwrap(),
            content_manifest_digest(&cache).unwrap()
        );
        assert_ne!(
            content_manifest_digest(&cache).unwrap(),
            content_manifest_digest(&other).unwrap()
        );
    }

    #[test]
    fn sealed_snapshot_copy_is_bounded_and_cleans_partial_output() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let temp = tempfile::TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let executable = source.join("tool");
        std::fs::write(&executable, b"tool").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let sealed = temp.path().join("sealed");
        sealed_copy_tree(&source, &sealed).unwrap();
        assert_eq!(
            std::fs::metadata(sealed.join("tool"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o555
        );
        remove_custody_tree(&sealed).unwrap();
        assert!(!sealed.exists());

        let unsafe_source = temp.path().join("unsafe-source");
        std::fs::create_dir(&unsafe_source).unwrap();
        symlink("/etc/passwd", unsafe_source.join("escape")).unwrap();
        let unsafe_target = temp.path().join("unsafe-target");
        assert!(sealed_copy_tree(&unsafe_source, &unsafe_target).is_err());
        assert!(!unsafe_target.exists());

        let deep_source = temp.path().join("deep-source");
        std::fs::create_dir(&deep_source).unwrap();
        let mut deep = deep_source.clone();
        for index in 0..6 {
            deep = deep.join(format!("{index}-{}", "x".repeat(210)));
            std::fs::create_dir(&deep).unwrap();
        }
        let deep_target = temp.path().join("deep-target");
        assert!(sealed_copy_tree(&deep_source, &deep_target).is_err());
        assert!(!deep_target.exists());
    }

    #[test]
    fn sealed_registry_contains_only_lockfile_packages_and_checks_archives() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let registry_hash = "index.example-0123456789abcdef";
        let index = registry.join("index").join(registry_hash);
        let cache = registry.join("cache").join(registry_hash);
        let source = registry.join("src").join(registry_hash);
        std::fs::create_dir_all(index.join(".cache")).unwrap();
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::create_dir_all(source.join("needed-1.0.0")).unwrap();
        std::fs::create_dir_all(source.join("unrelated-9.9.9")).unwrap();
        std::fs::write(index.join("config.json"), b"{}").unwrap();
        std::fs::write(index.join(".cache").join("needed"), b"index-row").unwrap();
        let archive = b"controller-attested-crate-archive";
        let checksum = hex::encode(Sha256::digest(archive));
        std::fs::write(cache.join("needed-1.0.0.crate"), archive).unwrap();
        std::fs::write(cache.join("unrelated-9.9.9.crate"), b"unrelated").unwrap();
        std::fs::write(source.join("needed-1.0.0").join("Cargo.toml"), b"[package]").unwrap();
        std::fs::write(
            source.join("unrelated-9.9.9").join("Cargo.toml"),
            b"[package]",
        )
        .unwrap();
        let lock = format!(
            r#"version = 4

[[package]]
name = "workspace-local"
version = "0.1.0"

[[package]]
name = "needed"
version = "1.0.0"
source = "registry+https://example.invalid/index"
checksum = "{checksum}"
"#
        );
        let sealed = temp.path().join("sealed");
        sealed_copy_registry(&lock, &registry, &sealed).unwrap();
        assert!(
            sealed
                .join("cache")
                .join(registry_hash)
                .join("needed-1.0.0.crate")
                .is_file()
        );
        assert!(
            sealed
                .join("src")
                .join(registry_hash)
                .join("needed-1.0.0")
                .is_dir()
        );
        assert!(
            !sealed
                .join("cache")
                .join(registry_hash)
                .join("unrelated-9.9.9.crate")
                .exists()
        );
        assert!(
            !sealed
                .join("src")
                .join(registry_hash)
                .join("unrelated-9.9.9")
                .exists()
        );
        assert_eq!(
            std::fs::metadata(&sealed).unwrap().permissions().mode() & 0o777,
            0o555
        );
        assert_eq!(
            content_manifest_digest(&sealed).unwrap(),
            content_manifest_digest(&sealed).unwrap()
        );
        remove_custody_tree(&sealed).unwrap();

        let bad_lock = lock.replace(&checksum, &"0".repeat(64));
        let bad_target = temp.path().join("bad-sealed");
        assert!(sealed_copy_registry(&bad_lock, &registry, &bad_target).is_err());
        assert!(!bad_target.exists());
    }

    #[test]
    fn persisted_command_wire_has_its_own_store_digest() {
        let command = command_wire(&OBSERVER_SNAPSHOT_COMMAND);
        assert_eq!(
            digest_json(&command).unwrap(),
            hex::encode(Sha256::digest(
                serde_json::to_string(&command).unwrap().as_bytes()
            ))
        );
        assert_ne!(
            digest_json(&command).unwrap(),
            observer_snapshot_command_digest()
        );
    }

    #[test]
    fn failed_or_unavailable_receipts_are_not_persistable_as_success_authority() {
        let receipt = ObserverSnapshotControllerReceiptV1 {
            schema: "harness.eval.observer-snapshot-receipt.v2".to_owned(),
            arm: ObserverSnapshotArm::Fixed,
            historical_base_sha: OBSERVER_SNAPSHOT_HISTORICAL_BASE.to_owned(),
            fixed_source_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
            signal: ObserverSnapshotSignal::FixedBoundEnforced,
            materialization: ObserverSnapshotMaterializationReceiptV1 {
                schema: "harness.eval.observer-snapshot-materialization.v1".to_owned(),
                base_checkout_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
                target_path: OBSERVER_SNAPSHOT_TARGET_PATH.to_owned(),
                arm: ObserverSnapshotArm::Fixed,
                overlay_digest: observer_snapshot_fixture_digest(ObserverSnapshotArm::Fixed),
                resulting_target_digest: digest_text("target"),
                command_digest: observer_snapshot_command_digest(),
                sha256: String::new(),
            },
            fixture_digest: observer_snapshot_fixture_digest(ObserverSnapshotArm::Fixed),
            setup_digest: observer_snapshot_setup_digest(
                ObserverSnapshotArm::Fixed,
                OBSERVER_SNAPSHOT_BOUNDED_FIX,
            ),
            command_digest: observer_snapshot_command_digest(),
            candidate_isolation: ObserverSnapshotIsolationPinV1 {
                backend: "none".to_owned(),
                backend_version: "none".to_owned(),
                namespaces: Vec::new(),
                candidate_access: "none".to_owned(),
                grader_access: "none".to_owned(),
                artifact_access: "none".to_owned(),
                available: false,
                policy_digest: digest_text("policy"),
                receipt_digest: digest_text("receipt"),
            },
            grader_isolation: ObserverSnapshotIsolationPinV1 {
                backend: "none".to_owned(),
                backend_version: "none".to_owned(),
                namespaces: Vec::new(),
                candidate_access: "none".to_owned(),
                grader_access: "none".to_owned(),
                artifact_access: "none".to_owned(),
                available: false,
                policy_digest: digest_text("policy"),
                receipt_digest: digest_text("receipt"),
            },
            registry_manifest_digest: digest_text("registry"),
            git_manifest_digest: digest_text("git"),
            toolchain_manifest_digest: digest_text("toolchain"),
            target_scope_digest: digest_text("target"),
            isolation: IsolationCapability::InfrastructureUnavailable,
            classification: harness_eval::SampleClassification::InfrastructureUnavailable,
            controller_exit_success: false,
            sha256: String::new(),
        };
        assert!(!receipt_is_persistable(&receipt));
    }

    #[tokio::test]
    #[ignore = "runs two real isolated pinned-worktree Cargo evaluations"]
    async fn isolated_controller_smoke_persists_and_replays() {
        fn contains_transient_custody(path: &Path) -> bool {
            std::fs::read_dir(path).is_ok_and(|entries| {
                entries.filter_map(Result::ok).any(|entry| {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    name.starts_with("sealed-input-")
                        || name.starts_with("cargo-target-")
                        || (entry.path().is_dir() && contains_transient_custody(&entry.path()))
                })
            })
        }

        let temp = tempfile::TempDir::new().unwrap();
        let git_dir = std::process::Command::new("git")
            .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
            .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .output()
            .unwrap();
        assert!(git_dir.status.success());
        let repository = std::fs::canonicalize(
            PathBuf::from(std::str::from_utf8(&git_dir.stdout).unwrap().trim())
                .parent()
                .unwrap(),
        )
        .unwrap();
        let store = Store::open(
            &temp.path().join("harness.sqlite3"),
            &temp.path().join("artifacts"),
        )
        .unwrap();
        let repository_id = harness_domain::RepositoryId::from("m2-smoke-repository");
        store
            .create_repository(&harness_store::NewRepository {
                id: repository_id.clone(),
                profile_id: "general".to_owned(),
                profile_version: 1,
                display_name: "M2 smoke repository".to_owned(),
                root_path: repository.clone(),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "ACTIVE".to_owned(),
            })
            .unwrap();
        let spool_root = temp.path().join("spool");
        let service = EvaluationService::new(
            store.clone(),
            temp.path().join("worktrees"),
            spool_root.clone(),
        )
        .unwrap();
        let first = service
            .run_observer_snapshot_once(repository.clone())
            .await
            .unwrap();
        assert_eq!(
            first,
            "m2-observer-snapshot-fixed-evaluation:m2-observer-snapshot-fixed-sample"
        );
        let sample = store
            .evaluation_sample("m2-observer-snapshot-fixed-sample")
            .unwrap();
        assert_ne!(sample.controller_evidence_id, sample.grader_evidence_id);
        assert_eq!(
            store
                .evaluation_run("m2-observer-snapshot-fixed-evaluation")
                .unwrap()
                .status,
            EvaluationRunStatus::Completed
        );
        for run_id in [
            harness_domain::RunId::from("m2-observer-snapshot-historical"),
            harness_domain::RunId::from("m2-observer-snapshot-fixed"),
        ] {
            let evidence = store.evidence_snapshot(&run_id).unwrap();
            assert_eq!(evidence["evidence"].as_array().unwrap().len(), 2);
            assert_eq!(evidence["artifacts"].as_array().unwrap().len(), 1);
            assert!(
                store
                    .list_worktrees(Some(&run_id))
                    .unwrap()
                    .iter()
                    .all(|worktree| worktree.state == "REMOVED")
            );
        }
        assert!(!contains_transient_custody(&spool_root));
        let fixed_evidence_before = store
            .evidence_snapshot(&harness_domain::RunId::from("m2-observer-snapshot-fixed"))
            .unwrap();
        assert_eq!(
            service
                .run_observer_snapshot_once(repository)
                .await
                .unwrap(),
            first
        );
        let fixed_evidence_after = store
            .evidence_snapshot(&harness_domain::RunId::from("m2-observer-snapshot-fixed"))
            .unwrap();
        assert_eq!(
            fixed_evidence_after["evidence"].as_array().unwrap().len(),
            fixed_evidence_before["evidence"].as_array().unwrap().len()
        );
        assert_eq!(
            fixed_evidence_after["artifacts"].as_array().unwrap().len(),
            fixed_evidence_before["artifacts"].as_array().unwrap().len()
        );
        assert!(!contains_transient_custody(&spool_root));
        let unregistered = temp.path().join("unregistered-repository");
        std::fs::create_dir(&unregistered).unwrap();
        assert!(
            service
                .run_observer_snapshot_once(unregistered)
                .await
                .is_err()
        );
    }
}
