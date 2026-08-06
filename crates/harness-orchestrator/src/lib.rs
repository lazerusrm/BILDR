//! Deterministic orchestration service for controller-owned Codex work.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use harness_codex::{CodexEvent, CodexRuntime, EventDirection, EventKind, StartThread, StartTurn};
use harness_context::{ContextCompiler, ContextPacket};
use harness_domain::{
    AgentRole, AgentSessionId, ApprovalId, ApprovalSummary, ArtifactId, CodexRuntimeStatus,
    CommandRunId, ComponentStatus, DiffBudget, EvidenceId, ProofTier, RepositoryId,
    RepositorySummary, ResourceClass, ResultClass, RiskLevel, RunId, RunPlan, RunState, RunSummary,
    RuntimeStatus, SandboxMode, SchedulerStatus, TaskId, TaskPacket, TaskState, TaskSummary,
    ValidationId, WorktreeId,
};
use harness_evidence::{EvidenceArtifactInput, EvidenceClaim, EvidenceService};
use harness_git::{DiffPolicy, GitManager, WorktreeSpec};
use harness_profile::{HarnessConfig, LoadedProfile, ModelRoute, RepositoryProfile, ResolvedPaths};
use harness_runner::{CommandOutcome, CommandRunner, CommandSpec, ResourceManager};
use harness_store::{
    ContextSourceRecord, NewAgentSession, NewApproval, NewArtifact, NewCommandRecord,
    NewContextPacket, NewRepository, NewRun, NewTaskAttempt, NewValidationRecord, NewWorktree,
    ProtocolProjection, RepositoryHealthInput, Store, packet_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::warn;

const RUN_PLAN_SCHEMA: &str = include_str!("../../../schemas/nm.orchestration.plan.v1.schema.json");

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterRepositoryRequest {
    pub profile_id: Option<String>,
    #[serde(alias = "path")]
    pub root_path: PathBuf,
    pub expected_origin: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunRequest {
    pub repository_id: RepositoryId,
    pub objective: String,
    #[serde(default = "default_run_mode")]
    pub mode: String,
    #[serde(default = "default_publication_mode", alias = "publication_mode")]
    pub publication: String,
    pub base_ref: Option<String>,
    pub title: Option<String>,
    #[serde(alias = "token_budget")]
    pub run_token_budget: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunDetail {
    pub run: RunSummary,
    pub tasks: Vec<TaskSummary>,
    pub agents: Vec<harness_domain::AgentSummary>,
    pub worktrees: Vec<harness_domain::WorktreeSummary>,
    pub approvals: Vec<ApprovalSummary>,
    pub plan: Option<RunPlan>,
    pub plan_digest: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OperationAccepted {
    pub operation_id: String,
    pub state: String,
    pub target_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecisionRequest {
    pub decision: String,
    pub note: Option<String>,
    pub expected_version: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryTaskRequest {
    pub reason: String,
    pub revised_objective: Option<String>,
    #[serde(default = "default_retry_route")]
    pub model_route: String,
    #[serde(default)]
    pub additional_token_budget: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishDraftPrRequest {
    pub expected_head_sha: String,
    pub title: String,
    pub body_appendix: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ValidationOutcome {
    pub validation_id: ValidationId,
    pub command_id: CommandRunId,
    pub validator_id: String,
    pub source_sha: String,
    pub proof_tier: ProofTier,
    pub result: CommandOutcome,
}

#[derive(Clone)]
pub struct Orchestrator {
    config: Arc<HarnessConfig>,
    paths: Arc<ResolvedPaths>,
    profile: Arc<LoadedProfile>,
    store: Store,
    git: GitManager,
    runner: CommandRunner,
    evidence: EvidenceService,
    projection: ProtocolProjection,
    context: Arc<ContextCompiler>,
    runtime: Arc<RwLock<Option<Arc<dyn CodexRuntime>>>>,
    operation_lock: Arc<Mutex<()>>,
}

impl Orchestrator {
    pub async fn new(
        config: HarnessConfig,
        paths: ResolvedPaths,
        profile: LoadedProfile,
        store: Store,
        runtime: Option<Arc<dyn CodexRuntime>>,
    ) -> Result<Self, OrchestratorError> {
        let git = GitManager::new(&paths.worktree_root)?;
        let runner = CommandRunner::new(
            paths.state_dir.join("command-spool"),
            ResourceManager::new(
                config.orchestration.max_total_agent_threads as usize,
                config.orchestration.max_mutable_tasks as usize,
                1,
            ),
        )
        .await?;
        let pricing = config
            .pricing
            .snapshots
            .iter()
            .map(harness_profile::PriceSnapshotConfig::to_domain)
            .collect::<Result<Vec<_>, _>>()?;
        let projection =
            ProtocolProjection::new(store.clone(), pricing, config.security.store_raw_reasoning);
        let orchestrator = Self {
            config: Arc::new(config),
            paths: Arc::new(paths),
            profile: Arc::new(profile),
            git,
            runner,
            evidence: EvidenceService::new(store.clone()),
            projection,
            context: Arc::new(ContextCompiler::default()),
            store,
            runtime: Arc::new(RwLock::new(runtime)),
            operation_lock: Arc::new(Mutex::new(())),
        };
        orchestrator.reconcile_orphaned_sessions("daemon restarted")?;
        Ok(orchestrator)
    }

    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }

    #[must_use]
    pub fn profile(&self) -> &LoadedProfile {
        &self.profile
    }

    pub async fn set_runtime(&self, runtime: Arc<dyn CodexRuntime>) {
        *self.runtime.write().await = Some(runtime);
    }

    #[must_use]
    pub fn maintenance_interval_seconds(&self) -> u64 {
        self.config.orchestration.heartbeat_interval_seconds
    }

    #[must_use]
    pub fn ui_event_replay_limit(&self) -> u32 {
        self.config.server.ui_event_replay_limit
    }

    pub async fn maintenance_tick(&self) -> Result<(), OrchestratorError> {
        let runs = self.store.list_runs(None, false)?;
        for run in &runs {
            self.store
                .heartbeat_run_path_leases(&run.id, self.config.orchestration.lease_ttl_seconds)?;
        }
        let runtime_ready = match self.runtime.read().await.as_ref() {
            Some(runtime) => {
                let status = runtime.runtime_status().await;
                status.state == "ready" && status.schema_match
            }
            None => false,
        };
        if runtime_ready {
            for run in runs
                .into_iter()
                .filter(|run| run.state == RunState::Executing && !run.scheduler_paused)
            {
                self.tick(&run.id).await?;
            }
        }
        Ok(())
    }

    pub async fn runtime_status(&self) -> RuntimeStatus {
        let database = match self.store.check() {
            Ok(health) => ComponentStatus {
                state: if health.ready { "ready" } else { "degraded" }.to_owned(),
                detail: Some(format!(
                    "SQLite {} · schema {} · raw events {} · projection lag {}",
                    health.journal_mode,
                    health.schema_version,
                    health.raw_event_count,
                    health.projection_lag
                )),
            },
            Err(error) => ComponentStatus {
                state: "unavailable".to_owned(),
                detail: Some(error.to_string()),
            },
        };
        let codex = match self.runtime.read().await.as_ref() {
            Some(runtime) => runtime.runtime_status().await,
            None => CodexRuntimeStatus {
                state: "unavailable".to_owned(),
                detail: Some("Codex App Server is not connected".to_owned()),
                version: None,
                required_version: nonempty(&self.config.codex.required_version),
                protocol_schema_sha256: nonempty(
                    &self.config.codex.required_protocol_schema_sha256,
                ),
                schema_match: false,
                pid: None,
                restart_count: 0,
            },
        };
        let (active_total, active_mutable, active_verifiers, queued_tasks, paused) =
            self.scheduler_totals();
        RuntimeStatus {
            daemon: ComponentStatus {
                state: "ready".to_owned(),
                detail: Some(format!("Harness Console {}", env!("CARGO_PKG_VERSION"))),
            },
            codex,
            database,
            scheduler: SchedulerStatus {
                paused,
                active_total,
                max_total: self.config.orchestration.max_total_agent_threads,
                active_mutable,
                max_mutable: self.config.orchestration.max_mutable_tasks,
                active_verifiers,
                max_verifiers: self.config.orchestration.max_independent_verifiers,
                queued_tasks,
            },
        }
    }

    fn scheduler_totals(&self) -> (u32, u32, u32, u32, bool) {
        let mut active_total = 0_u32;
        let mut active_mutable = 0_u32;
        let mut active_verifiers = 0_u32;
        let mut queued = 0_u32;
        let mut paused = false;
        if let Ok(runs) = self.store.list_runs(None, false) {
            for run in runs {
                paused |= run.scheduler_paused;
                if let Ok(agents) = self.store.list_agents(&run.id) {
                    for agent in agents
                        .iter()
                        .filter(|agent| agent_state_consumes_capacity(&agent.state))
                    {
                        active_total += 1;
                        if matches!(
                            agent.role,
                            AgentRole::Worker | AgentRole::HighRiskWorker | AgentRole::Integrator
                        ) {
                            active_mutable += 1;
                        }
                        if agent.role == AgentRole::Verifier {
                            active_verifiers += 1;
                        }
                    }
                }
                if let Ok(tasks) = self.store.list_tasks(&run.id) {
                    queued += tasks
                        .iter()
                        .filter(|task| {
                            matches!(
                                task.state,
                                TaskState::Ready
                                    | TaskState::ReviewReady
                                    | TaskState::WaitingDependency
                                    | TaskState::WaitingResource
                            )
                        })
                        .count() as u32;
                }
            }
        }
        (
            active_total,
            active_mutable,
            active_verifiers,
            queued,
            paused,
        )
    }

    fn active_agent_counts(&self) -> Result<(u32, u32, u32), OrchestratorError> {
        let mut total = 0_u32;
        let mut mutable = 0_u32;
        let mut verifiers = 0_u32;
        for run in self.store.list_runs(None, false)? {
            for agent in self
                .store
                .list_agents(&run.id)?
                .into_iter()
                .filter(|agent| agent_state_consumes_capacity(&agent.state))
            {
                total = total.saturating_add(1);
                if matches!(
                    agent.role,
                    AgentRole::Worker | AgentRole::HighRiskWorker | AgentRole::Integrator
                ) {
                    mutable = mutable.saturating_add(1);
                }
                if agent.role == AgentRole::Verifier {
                    verifiers = verifiers.saturating_add(1);
                }
            }
        }
        Ok((total, mutable, verifiers))
    }

    pub async fn register_repository(
        &self,
        request: RegisterRepositoryRequest,
    ) -> Result<RepositorySummary, OrchestratorError> {
        if request
            .profile_id
            .as_deref()
            .is_some_and(|id| id != self.profile.profile.profile_id)
        {
            return Err(OrchestratorError::Validation(format!(
                "loaded profile is {}, not {}",
                self.profile.profile.profile_id,
                request.profile_id.as_deref().unwrap_or_default()
            )));
        }
        let inspection = self
            .git
            .inspect(&request.root_path, &self.profile.profile)
            .await?;
        if request
            .expected_origin
            .as_deref()
            .is_some_and(|origin| inspection.origin_url.as_deref() != Some(origin))
        {
            return Err(OrchestratorError::Blocked(
                "repository origin does not match expected_origin".to_owned(),
            ));
        }
        if request.expected_origin.is_none()
            && !inspection.origin_url.as_deref().is_some_and(|origin| {
                origin_matches_repository(origin, &self.profile.profile.repository)
            })
        {
            return Err(OrchestratorError::Blocked(format!(
                "repository origin does not identify {} (pass an exact expected_origin only for an intentional mirror)",
                self.profile.profile.repository
            )));
        }
        let repository_id = RepositoryId::new();
        self.store.create_repository(&NewRepository {
            id: repository_id.clone(),
            profile_id: self.profile.profile.profile_id.clone(),
            profile_version: self.profile.profile.schema_version,
            display_name: self.profile.profile.display_name.clone(),
            root_path: inspection.root.clone(),
            origin_url: inspection.origin_url.clone(),
            default_branch: self.profile.profile.default_branch.clone(),
            expected_coordination_branch: Some(self.profile.profile.default_branch.clone()),
            state: if inspection.blockers.is_empty() {
                "READY".to_owned()
            } else {
                "BLOCKED".to_owned()
            },
        })?;
        self.record_inspection(&repository_id, &inspection, None)?;
        self.store.emit_domain_event(
            None,
            "repository",
            repository_id.as_str(),
            "repository.registered",
            &serde_json::to_value(&inspection)?,
            None,
        )?;
        self.store.repository(&repository_id).map_err(Into::into)
    }

    pub async fn inspect_repository(
        &self,
        repository_id: &RepositoryId,
    ) -> Result<RepositorySummary, OrchestratorError> {
        let repository = self.store.repository(repository_id)?;
        let inspection = self
            .git
            .inspect(Path::new(&repository.root_path), &self.profile.profile)
            .await?;
        self.record_inspection(repository_id, &inspection, repository.authority_digest)?;
        self.store.repository(repository_id).map_err(Into::into)
    }

    fn record_inspection(
        &self,
        repository_id: &RepositoryId,
        inspection: &harness_git::RepositoryInspection,
        authority_digest: Option<String>,
    ) -> Result<(), OrchestratorError> {
        self.store
            .record_repository_health(&RepositoryHealthInput {
                repository_id: repository_id.clone(),
                primary_branch: inspection.current_branch.clone(),
                primary_head_sha: Some(inspection.head_sha.clone()),
                primary_clean: inspection.clean,
                origin_head_sha: None,
                git_identity_name_present: inspection.git_identity_name_present,
                git_identity_email_present: inspection.git_identity_email_present,
                authority_digest,
                blockers: inspection.blockers.clone(),
                details: serde_json::to_value(inspection)?,
            })?;
        Ok(())
    }

    pub async fn create_run(
        &self,
        request: CreateRunRequest,
    ) -> Result<RunSummary, OrchestratorError> {
        if request.objective.trim().is_empty() {
            return Err(OrchestratorError::Validation(
                "run objective must not be empty".to_owned(),
            ));
        }
        if request.objective.chars().count() > 50_000 {
            return Err(OrchestratorError::Validation(
                "run objective exceeds 50,000 characters".to_owned(),
            ));
        }
        if request
            .title
            .as_ref()
            .is_some_and(|title| title.chars().count() > 240)
        {
            return Err(OrchestratorError::Validation(
                "run title exceeds 240 characters".to_owned(),
            ));
        }
        if !matches!(request.mode.as_str(), "plan_only" | "plan_and_implement") {
            return Err(OrchestratorError::Validation(
                "mode must be plan_only or plan_and_implement".to_owned(),
            ));
        }
        if !matches!(
            request.publication.as_str(),
            "local_only" | "draft_pr_after_approval"
        ) {
            return Err(OrchestratorError::Validation(
                "publication must be local_only or draft_pr_after_approval".to_owned(),
            ));
        }
        if request
            .run_token_budget
            .is_some_and(|budget| budget < 1_000)
        {
            return Err(OrchestratorError::Validation(
                "run token budget must be at least 1,000 tokens".to_owned(),
            ));
        }
        let repository = self.store.repository(&request.repository_id)?;
        let fresh = self
            .git
            .inspect(Path::new(&repository.root_path), &self.profile.profile)
            .await?;
        self.record_inspection(&request.repository_id, &fresh, repository.authority_digest)?;
        if !fresh.blockers.is_empty() {
            return Err(OrchestratorError::Blocked(fresh.blockers.join("; ")));
        }
        let base_ref = request
            .base_ref
            .clone()
            .unwrap_or_else(|| self.config.git.base_ref.clone());
        let base_sha = self
            .git
            .fetch_and_pin(
                Path::new(&repository.root_path),
                &base_ref,
                self.config.git.fetch_before_run,
            )
            .await?;
        let run_id = RunId::new();
        let inspection_worktree = self
            .git
            .create_worktree(&WorktreeSpec {
                repository_root: PathBuf::from(&repository.root_path),
                relative_path: PathBuf::from(run_id.as_str()).join("inspection"),
                base_sha: base_sha.clone(),
                branch: None,
            })
            .await?;
        let authority_digest =
            match authority_digest(&inspection_worktree.path, &self.profile.profile) {
                Ok(digest) => digest,
                Err(error) => {
                    if let Err(cleanup_error) = self
                        .git
                        .remove_worktree(
                            Path::new(&repository.root_path),
                            &inspection_worktree.path,
                            true,
                        )
                        .await
                    {
                        warn!(%cleanup_error, "could not clean up rejected inspection worktree");
                    }
                    return Err(error);
                }
            };
        let runtime_status = self.runtime_status().await.codex;
        let title = request
            .title
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| compact_title(&request.objective));
        if let Err(error) = self.store.create_run(&NewRun {
            id: run_id.clone(),
            repository_id: request.repository_id,
            title,
            objective: request.objective,
            mode: request.mode,
            publication_mode: request.publication,
            state: RunState::Created.to_string(),
            phase: "created".to_owned(),
            base_ref,
            base_sha: base_sha.clone(),
            authority_digest,
            profile_digest: self.profile.digest.clone(),
            codex_version: runtime_status.version,
            protocol_schema_sha256: runtime_status.protocol_schema_sha256,
            requested_by: "local-user".to_owned(),
            token_budget: request.run_token_budget,
        }) {
            if let Err(cleanup_error) = self
                .git
                .remove_worktree(
                    Path::new(&repository.root_path),
                    &inspection_worktree.path,
                    true,
                )
                .await
            {
                warn!(%cleanup_error, "could not clean up unregistered inspection worktree");
            }
            return Err(error.into());
        }
        self.store
            .transition_run(&run_id, RunState::Preparing, "preparing", None, None)?;
        self.store.create_worktree(&NewWorktree {
            id: WorktreeId::new(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            kind: "inspection".to_owned(),
            path: inspection_worktree.path,
            branch: None,
            base_sha,
            head_sha: Some(inspection_worktree.head_sha),
            state: "READY".to_owned(),
        })?;
        let run = self.store.transition_run(
            &run_id,
            RunState::ReadyForArchitecture,
            "ready_for_architecture",
            None,
            None,
        )?;
        self.emit_run_event(&run, "run.prepared", json!({"base_sha": run.base_sha}))?;
        Ok(run)
    }

    pub fn run_detail(&self, run_id: &RunId) -> Result<RunDetail, OrchestratorError> {
        let run = self.store.run(run_id)?;
        let plan = self.store.latest_plan(run_id)?.map(|(_, plan, _, _)| plan);
        let plan_digest = plan.as_ref().map(packet_digest).transpose()?;
        Ok(RunDetail {
            run,
            tasks: self.store.list_tasks(run_id)?,
            agents: self.store.list_agents(run_id)?,
            worktrees: self.store.list_worktrees(Some(run_id))?,
            approvals: self.store.list_approvals(Some(run_id), None)?,
            plan,
            plan_digest,
        })
    }

    pub fn evidence_snapshot(&self, run_id: &RunId) -> Result<Value, OrchestratorError> {
        self.store.evidence_snapshot(run_id).map_err(Into::into)
    }

    pub fn usage_summary(
        &self,
        run_id: &RunId,
    ) -> Result<harness_domain::UsageSummary, OrchestratorError> {
        self.store.run_usage(run_id).map_err(Into::into)
    }

    pub fn default_export_path(&self, run_id: &RunId) -> PathBuf {
        self.paths
            .data_dir
            .join("exports")
            .join(format!("harness-evidence-{run_id}.tar.zst"))
    }

    pub fn preserve_worktree(
        &self,
        worktree_id: &WorktreeId,
        reason: Option<&str>,
    ) -> Result<harness_domain::WorktreeSummary, OrchestratorError> {
        self.store.update_worktree(
            worktree_id,
            "PRESERVED",
            None,
            Some(reason.unwrap_or("preserved by operator")),
        )?;
        self.store
            .list_worktrees(None)?
            .into_iter()
            .find(|worktree| &worktree.id == worktree_id)
            .ok_or_else(|| OrchestratorError::Protocol("updated worktree disappeared".to_owned()))
    }

    pub async fn stop_run(
        &self,
        run_id: &RunId,
        interrupt_turns: bool,
        actor: &str,
    ) -> Result<RunSummary, OrchestratorError> {
        let mut run = self.store.run(run_id)?;
        if run.state.is_terminal() {
            return Ok(run);
        }
        self.store.transition_run(
            run_id,
            RunState::Stopping,
            "stopping",
            Some(run.version),
            None,
        )?;
        run = self.store.set_scheduler_paused(run_id, true)?;
        for approval in self.store.list_approvals(Some(run_id), Some("pending"))? {
            if let Err(error) = self
                .decide_approval(
                    &approval.id,
                    ApprovalDecisionRequest {
                        decision: "cancel".to_owned(),
                        note: Some("run stop requested".to_owned()),
                        expected_version: Some(approval.version),
                    },
                    actor,
                )
                .await
            {
                warn!(approval_id = %approval.id, %error, "run stop could not cancel approval");
            }
        }
        if interrupt_turns {
            for agent in self.store.list_agents(run_id)?.into_iter().filter(|agent| {
                agent.active_turn_id.is_some()
                    && !matches!(
                        agent.state.as_str(),
                        "COMPLETED" | "FAILED" | "INTERRUPTED" | "CANCELED"
                    )
            }) {
                if let Err(error) = self.interrupt_agent(&agent.id, actor).await {
                    warn!(agent_id = %agent.id, %error, "run stop could not interrupt agent");
                }
            }
        }
        self.store.record_human_action(
            Some(run_id),
            None,
            actor,
            "stop_run",
            "run",
            run_id.as_str(),
            &json!({"interrupt_turns": interrupt_turns}),
        )?;
        if interrupt_turns {
            self.cancel_run_work(run_id, "run interrupted by operator")?;
            self.store
                .transition_run(
                    run_id,
                    RunState::Canceled,
                    "canceled",
                    Some(run.version),
                    None,
                )
                .map_err(Into::into)
        } else {
            self.finish_stopping_run_if_idle(run_id)?;
            self.store.run(run_id).map_err(Into::into)
        }
    }

    pub async fn start_architecture(
        &self,
        run_id: &RunId,
    ) -> Result<OperationAccepted, OrchestratorError> {
        let _guard = self.operation_lock.lock().await;
        let run = self.store.run(run_id)?;
        if run.state != RunState::ReadyForArchitecture {
            return Err(OrchestratorError::Conflict(format!(
                "run {} is {}, not READY_FOR_ARCHITECTURE",
                run.id, run.state
            )));
        }
        self.require_runtime_ready().await?;
        let (active_total, _, _) = self.active_agent_counts()?;
        if active_total >= self.config.orchestration.max_total_agent_threads {
            return Err(OrchestratorError::Blocked(format!(
                "all {} Codex thread slots are active",
                self.config.orchestration.max_total_agent_threads
            )));
        }
        let inspection = self
            .store
            .list_worktrees(Some(run_id))?
            .into_iter()
            .find(|worktree| worktree.kind == "inspection" && worktree.state == "READY")
            .ok_or_else(|| {
                OrchestratorError::Blocked("inspection worktree is unavailable".to_owned())
            })?;
        let packet = architecture_packet(&run, &self.profile.profile, &self.config);
        let context = self.context.compile(
            Path::new(&inspection.path),
            &run.base_sha,
            &packet,
            &self.profile.profile,
            &self.profile.digest,
        )?;
        self.persist_context(run_id, None, "architect", &context)?;
        let route = &self.profile.profile.models.architect;
        let agent_id = AgentSessionId::new();
        self.store.create_agent_session(&NewAgentSession {
            id: agent_id.clone(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            parent_agent_session_id: None,
            runtime_kind: "codex_controller".to_owned(),
            role: AgentRole::Architect,
            nickname: Some("architect".to_owned()),
            requested_model: route.model.clone(),
            requested_reasoning_effort: route.reasoning_effort.clone(),
            sandbox_mode: SandboxMode::ReadOnly,
            approval_policy: "never".to_owned(),
            cwd: PathBuf::from(&inspection.path),
            state: "STARTING".to_owned(),
            current_goal: Some(run.objective.clone()),
            token_budget: Some(self.config.orchestration.default_task_token_budget),
        })?;
        self.store.transition_run(
            run_id,
            RunState::Architecting,
            "architecting",
            Some(run.version),
            None,
        )?;
        let prompt = format!(
            "{}\n\nYou are the read-only architecture agent. Produce only a JSON value matching the supplied run-plan schema. Every task must use base SHA {}, cite active authorities, define disjoint owned paths, explicit negative tests, evidence, proof limits, and realistic budgets. Do not modify files.",
            context.prompt_prefix(),
            run.base_sha
        );
        if let Err(error) = self
            .start_agent(
                &agent_id,
                run_id,
                None,
                Path::new(&inspection.path),
                route,
                SandboxMode::ReadOnly,
                &run.objective,
                Some(self.config.orchestration.default_task_token_budget),
                prompt,
                Some(serde_json::from_str(RUN_PLAN_SCHEMA)?),
            )
            .await
        {
            let reason = error.to_string();
            let current = self.store.run(run_id)?;
            self.store.transition_run(
                run_id,
                RunState::ReadyForArchitecture,
                "architect_start_failed",
                Some(current.version),
                Some(("infrastructure_unavailable", &reason)),
            )?;
            self.store.update_agent_state(
                &agent_id,
                "FAILED",
                Some("Architecture agent could not start"),
                None,
                None,
                Some(("infrastructure_unavailable", &reason)),
            )?;
            return Err(error);
        }
        self.emit_agent_event(run_id, &agent_id, "agent.architect.started", json!({}))?;
        Ok(operation("start_architecture", run_id.as_str()))
    }

    pub fn submit_plan(
        &self,
        run_id: &RunId,
        architect_id: &AgentSessionId,
        plan: RunPlan,
    ) -> Result<String, OrchestratorError> {
        let run = self.store.run(run_id)?;
        if run.state != RunState::Architecting {
            return Err(OrchestratorError::Conflict(format!(
                "run is {}, not ARCHITECTING",
                run.state
            )));
        }
        validate_plan(&run, &plan, &self.profile.profile)?;
        let digest = packet_digest(&plan)?;
        self.store.store_plan(run_id, architect_id, &plan)?;
        self.store.transition_run(
            run_id,
            RunState::PlanReviewRequired,
            "plan_review",
            Some(run.version),
            None,
        )?;
        self.emit_run_event(
            &self.store.run(run_id)?,
            "run.plan.proposed",
            json!({"digest": digest, "tasks": plan.tasks.len()}),
        )?;
        Ok(digest)
    }

    pub async fn approve_plan(
        &self,
        run_id: &RunId,
        expected_digest: &str,
        actor: &str,
    ) -> Result<OperationAccepted, OrchestratorError> {
        let Some((_, plan, state, _)) = self.store.latest_plan(run_id)? else {
            return Err(OrchestratorError::Blocked(
                "run has no proposed plan".to_owned(),
            ));
        };
        if state != "PROPOSED" {
            return Err(OrchestratorError::Conflict(format!("plan is {state}")));
        }
        let digest = packet_digest(&plan)?;
        if digest != expected_digest {
            return Err(OrchestratorError::Conflict(
                "plan digest changed before approval".to_owned(),
            ));
        }
        let run = self.store.run(run_id)?;
        self.store.approve_latest_plan(run_id, actor)?;
        self.store.record_human_action(
            Some(run_id),
            None,
            actor,
            "approve_plan",
            "run_plan",
            expected_digest,
            &json!({"digest": expected_digest}),
        )?;
        if run.mode == "plan_only" {
            for task in self.store.list_tasks(run_id)? {
                self.store
                    .transition_task(&task.id, TaskState::Canceled, None)?;
            }
            self.store.transition_run(
                run_id,
                RunState::Completed,
                "plan_approved",
                Some(run.version),
                None,
            )?;
            return Ok(operation("approve_plan", run_id.as_str()));
        }
        self.store.transition_run(
            run_id,
            RunState::ReadyToExecute,
            "ready_to_execute",
            Some(run.version),
            None,
        )?;
        self.tick(run_id).await?;
        Ok(operation("approve_plan", run_id.as_str()))
    }

    pub async fn tick(&self, run_id: &RunId) -> Result<u32, OrchestratorError> {
        let _guard = self.operation_lock.lock().await;
        let mut run = self.store.run(run_id)?;
        if self.enforce_run_budget(&run)? {
            return Ok(0);
        }
        if run.scheduler_paused {
            return Ok(0);
        }
        if run.state == RunState::ReadyToExecute {
            run = self.store.transition_run(
                run_id,
                RunState::Executing,
                "executing",
                Some(run.version),
                None,
            )?;
        }
        if run.state != RunState::Executing {
            return Ok(0);
        }
        self.require_runtime_ready().await?;
        self.store.mark_unblocked_tasks_ready(run_id)?;
        let (mut active_total, mut active_mutable, mut active_verifiers) =
            self.active_agent_counts()?;
        let mut started = 0_u32;
        for task in self.store.list_tasks(run_id)? {
            if task.state != TaskState::ReviewReady
                || active_total >= self.config.orchestration.max_total_agent_threads
                || active_verifiers >= self.config.orchestration.max_independent_verifiers
            {
                continue;
            }
            if self.launch_review_ready_verifier(&task).await? {
                active_total = active_total.saturating_add(1);
                active_verifiers = active_verifiers.saturating_add(1);
                started = started.saturating_add(1);
            }
        }
        for task in self.store.list_tasks(run_id)? {
            if active_mutable >= self.config.orchestration.max_mutable_tasks
                || active_total >= self.config.orchestration.max_total_agent_threads
            {
                break;
            }
            if task.state != TaskState::Ready {
                continue;
            }
            match self.start_task(&run, &task).await {
                Ok(()) => {
                    started += 1;
                    active_mutable += 1;
                    active_total += 1;
                }
                Err(error) => {
                    warn!(task_id = %task.id, %error, "task start failed");
                    let current = self.store.task(&task.id)?;
                    if !current.state.is_terminal() {
                        let _ = self
                            .store
                            .transition_task(&task.id, TaskState::NeedsHelp, None);
                    }
                    self.emit_run_event(
                        &run,
                        "task.start_failed",
                        json!({"task_id": task.id, "error": error.to_string()}),
                    )?;
                }
            }
        }
        Ok(started)
    }

    async fn start_task(
        &self,
        run: &RunSummary,
        task: &TaskSummary,
    ) -> Result<(), OrchestratorError> {
        let (_, plan, state, _) = self
            .store
            .latest_plan(&run.id)?
            .ok_or_else(|| OrchestratorError::Blocked("approved plan disappeared".to_owned()))?;
        if state != "APPROVED" {
            return Err(OrchestratorError::Blocked(
                "plan is not approved".to_owned(),
            ));
        }
        let planned_packet = plan
            .tasks
            .into_iter()
            .find(|packet| packet.task_id == task.external_task_id)
            .ok_or_else(|| OrchestratorError::Blocked("task packet disappeared".to_owned()))?;
        let retry_key = format!("retry:{}", task.id);
        let mut packet = self
            .store
            .runtime_metadata(&retry_key)?
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(planned_packet);
        if packet.base_sha != run.base_sha {
            return Err(OrchestratorError::Blocked(format!(
                "task {} base {} differs from pinned run base {}",
                packet.task_id, packet.base_sha, run.base_sha
            )));
        }
        let dependency_commits = dependency_task_commits(
            task,
            &self.store.list_tasks(&run.id)?,
            self.store.verified_task_commits(&run.id)?,
        )?;
        let dependency_sha_by_external = dependency_commits
            .iter()
            .map(|(external_id, _, sha)| (external_id.clone(), sha.clone()))
            .collect::<BTreeMap<_, _>>();
        packet.dependency_shas = task
            .dependencies
            .iter()
            .map(|dependency| {
                dependency_sha_by_external
                    .get(dependency)
                    .cloned()
                    .map(|sha| (dependency.clone(), sha))
                    .ok_or_else(|| {
                        OrchestratorError::Blocked(format!(
                            "dependency {dependency} has no verified commit"
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        let attempt_id = harness_domain::AttemptId::new();
        let route = if packet.is_high_risk() || packet.owner_profile == "worker_escalation" {
            &self.profile.profile.models.worker_escalation
        } else {
            &self.profile.profile.models.worker
        };
        self.store.create_task_attempt(&NewTaskAttempt {
            id: attempt_id.clone(),
            task_id: task.id.clone(),
            attempt_number: task.attempt.saturating_add(1),
            state: "LEASED".to_owned(),
            packet: packet.clone(),
            packet_sha256: packet_digest(&packet)?,
            base_sha: run.base_sha.clone(),
            requested_model_route: route.model.clone(),
        })?;
        let repository = self.store.repository(&run.repository_id)?;
        let branch = format!(
            "harness/{}/{}/{}",
            short_id(run.id.as_str()),
            sanitize_ref(&packet.task_id),
            task.attempt.saturating_add(1)
        );
        let worktree = match self
            .git
            .create_worktree(&WorktreeSpec {
                repository_root: PathBuf::from(&repository.root_path),
                relative_path: PathBuf::from(run.id.as_str()).join("tasks").join(format!(
                    "{}-{}",
                    sanitize_ref(&packet.task_id),
                    task.attempt + 1
                )),
                base_sha: run.base_sha.clone(),
                branch: Some(branch.clone()),
            })
            .await
        {
            Ok(worktree) => worktree,
            Err(error) => {
                let reason = error.to_string();
                self.store.set_attempt_result(
                    &attempt_id,
                    "FAILED",
                    None,
                    Some("infrastructure_unavailable"),
                    Some(&reason),
                )?;
                return Err(error.into());
            }
        };
        let worktree_id = WorktreeId::new();
        if let Err(error) = self.store.create_worktree(&NewWorktree {
            id: worktree_id.clone(),
            run_id: run.id.clone(),
            task_attempt_id: Some(attempt_id.clone()),
            kind: "task".to_owned(),
            path: worktree.path.clone(),
            branch: Some(branch),
            base_sha: run.base_sha.clone(),
            head_sha: Some(worktree.head_sha.clone()),
            state: if dependency_commits.is_empty() {
                "ACTIVE".to_owned()
            } else {
                "COMPOSING".to_owned()
            },
        }) {
            let reason = error.to_string();
            if let Err(cleanup_error) = self
                .git
                .remove_worktree(Path::new(&repository.root_path), &worktree.path, true)
                .await
            {
                warn!(%cleanup_error, "could not clean up unregistered task worktree");
            }
            self.store.set_attempt_result(
                &attempt_id,
                "FAILED",
                None,
                Some("infrastructure_unavailable"),
                Some(&reason),
            )?;
            return Err(error.into());
        }
        let composed_base = if dependency_commits.is_empty() {
            worktree.head_sha.clone()
        } else {
            match self
                .git
                .cherry_pick(
                    &worktree.path,
                    &dependency_commits
                        .iter()
                        .map(|(_, _, sha)| sha.clone())
                        .collect::<Vec<_>>(),
                )
                .await
            {
                Ok(head) => head,
                Err(error) => {
                    let reason = error.to_string();
                    self.store.update_worktree(
                        &worktree_id,
                        "CONFLICTED",
                        None,
                        Some("dependency composition conflict"),
                    )?;
                    self.store.set_attempt_result(
                        &attempt_id,
                        "FAILED",
                        None,
                        Some("integration_conflict"),
                        Some(&reason),
                    )?;
                    return Err(error.into());
                }
            }
        };
        self.store
            .set_attempt_composed_base(&attempt_id, &packet, &composed_base)?;
        self.store
            .set_worktree_composed_base(&worktree_id, &composed_base)?;
        let lease_paths = packet
            .owned_paths
            .iter()
            .chain(packet.reserved_serial_paths.iter())
            .cloned()
            .collect::<Vec<_>>();
        if let Err(error) = self.store.acquire_path_leases(
            &run.id,
            &attempt_id,
            &composed_base,
            &lease_paths,
            self.config.orchestration.lease_ttl_seconds,
        ) {
            let reason = error.to_string();
            self.store.update_worktree(
                &worktree_id,
                "PRESERVED",
                Some(&composed_base),
                Some("path lease acquisition failed"),
            )?;
            self.store.set_attempt_result(
                &attempt_id,
                "FAILED",
                Some(&composed_base),
                Some("policy_blocked"),
                Some(&reason),
            )?;
            return Err(error.into());
        }
        let launch = async {
            let context = self.context.compile(
                &worktree.path,
                &composed_base,
                &packet,
                &self.profile.profile,
                &self.profile.digest,
            )?;
            self.persist_context(&run.id, Some(&attempt_id), "worker", &context)?;
            let role = if packet.is_high_risk() || packet.owner_profile == "worker_escalation" {
                AgentRole::HighRiskWorker
            } else {
                AgentRole::Worker
            };
            let agent_id = AgentSessionId::new();
            self.store.create_agent_session(&NewAgentSession {
                id: agent_id.clone(),
                run_id: run.id.clone(),
                task_attempt_id: Some(attempt_id.clone()),
                parent_agent_session_id: None,
                runtime_kind: "codex_controller".to_owned(),
                role,
                nickname: Some(packet.task_id.clone()),
                requested_model: route.model.clone(),
                requested_reasoning_effort: route.reasoning_effort.clone(),
                sandbox_mode: SandboxMode::WorkspaceWrite,
                approval_policy: self.config.security.approval_policy.clone(),
                cwd: worktree.path.clone(),
                state: "STARTING".to_owned(),
                current_goal: Some(packet.objective.clone()),
                token_budget: Some(packet.token_budget),
            })?;
            self.store
                .transition_task(&task.id, TaskState::Starting, None)?;
            self.store
                .transition_task(&task.id, TaskState::Implementing, None)?;
            let prompt = worker_prompt(&packet, &context)?;
            self.start_agent(
                &agent_id,
                &run.id,
                Some(&attempt_id),
                &worktree.path,
                route,
                SandboxMode::WorkspaceWrite,
                &packet.objective,
                Some(packet.token_budget),
                prompt,
                None,
            )
            .await?;
            Ok::<AgentSessionId, OrchestratorError>(agent_id)
        }
        .await;
        let agent_id = match launch {
            Ok(agent_id) => agent_id,
            Err(error) => {
                self.store
                    .release_path_leases(&attempt_id, "task launch failed")?;
                self.store.update_worktree(
                    &worktree_id,
                    "PRESERVED",
                    None,
                    Some("task launch failed"),
                )?;
                self.store.set_attempt_result(
                    &attempt_id,
                    "FAILED",
                    None,
                    Some("infrastructure_unavailable"),
                    Some(&error.to_string()),
                )?;
                return Err(error);
            }
        };
        self.store.delete_runtime_metadata(&retry_key)?;
        self.emit_agent_event(
            &run.id,
            &agent_id,
            "agent.worker.started",
            json!({"task_id": task.id, "attempt_id": attempt_id}),
        )?;
        Ok(())
    }

    // App Server thread startup intentionally keeps each protocol/custody
    // field explicit at this single internal boundary.
    #[allow(clippy::too_many_arguments)]
    async fn start_agent(
        &self,
        agent_id: &AgentSessionId,
        _run_id: &RunId,
        _attempt_id: Option<&harness_domain::AttemptId>,
        cwd: &Path,
        route: &ModelRoute,
        sandbox: SandboxMode,
        goal: &str,
        token_budget: Option<u64>,
        prompt: String,
        output_schema: Option<Value>,
    ) -> Result<(), OrchestratorError> {
        let runtime = self.runtime().await?;
        let approval_policy = if sandbox == SandboxMode::ReadOnly {
            "never"
        } else {
            self.config.security.approval_policy.as_str()
        };
        let result = runtime
            .start_thread(StartThread {
                cwd: cwd.to_path_buf(),
                model: route.model.clone(),
                sandbox: sandbox_text(sandbox).to_owned(),
                approval_policy: approval_policy.to_owned(),
                developer_instructions: format!(
                    "You are controlled by Harness Console. Obey the supplied task packet and path custody. Do not commit, push, create PRs, alter completion ledgers, or write outside the exact worktree. Controller-owned validation and Git operations are authoritative. Native subagents count against a global limit of {} live threads and a per-run discovery limit of {}; create only bounded read-only children that are necessary for this goal, and wait for them before completing your turn.\n\n{prompt}",
                    self.config.orchestration.max_total_agent_threads,
                    self.config.orchestration.max_read_only_discovery,
                ),
                service_name: self.config.codex.service_name.clone(),
                ephemeral: false,
            })
            .await;
        let thread_result = match result {
            Ok(value) => value,
            Err(error) => {
                self.store.update_agent_state(
                    agent_id,
                    "FAILED",
                    Some("App Server thread start failed"),
                    None,
                    None,
                    Some(("infrastructure_unavailable", &error.to_string())),
                )?;
                return Err(error.into());
            }
        };
        let Some(thread_id) =
            value_text(&thread_result, &[&["thread", "id"], &["threadId"], &["id"]])
                .map(ToOwned::to_owned)
        else {
            let error =
                OrchestratorError::Protocol("thread/start response lacks thread id".to_owned());
            self.store.update_agent_state(
                agent_id,
                "FAILED",
                Some("App Server thread response was invalid"),
                None,
                None,
                Some(("protocol_error", &error.to_string())),
            )?;
            return Err(error);
        };
        self.store.attach_codex_thread(
            agent_id,
            &thread_id,
            value_text(&thread_result, &[&["thread", "parentThreadId"]]),
            &self.config.codex.service_name,
            value_text(&thread_result, &[&["thread", "gitInfo", "branch"]]),
            value_text(&thread_result, &[&["thread", "gitInfo", "sha"]]),
        )?;
        if let Err(error) = runtime.set_goal(&thread_id, goal, token_budget).await {
            self.store.update_agent_state(
                agent_id,
                "FAILED",
                Some("App Server goal setup failed"),
                None,
                None,
                Some(("infrastructure_unavailable", &error.to_string())),
            )?;
            return Err(error.into());
        }
        let turn = match runtime
            .start_turn(StartTurn {
                thread_id: thread_id.clone(),
                input: prompt,
                model: route.model.clone(),
                effort: route.reasoning_effort.clone(),
                cwd: cwd.to_path_buf(),
                sandbox_policy: sandbox_policy(sandbox, cwd),
                approval_policy: approval_policy.to_owned(),
                output_schema,
                reasoning_summary: self.config.codex.reasoning_summary.clone(),
            })
            .await
        {
            Ok(turn) => turn,
            Err(error) => {
                self.store.update_agent_state(
                    agent_id,
                    "FAILED",
                    Some("App Server turn start failed"),
                    None,
                    None,
                    Some(("infrastructure_unavailable", &error.to_string())),
                )?;
                return Err(error.into());
            }
        };
        let Some(turn_id) = value_text(&turn, &[&["turn", "id"], &["turnId"], &["id"]]) else {
            let error = OrchestratorError::Protocol("turn/start response lacks turn id".to_owned());
            self.store.update_agent_state(
                agent_id,
                "FAILED",
                Some("App Server turn response was invalid"),
                None,
                None,
                Some(("protocol_error", &error.to_string())),
            )?;
            return Err(error);
        };
        self.store.attach_codex_turn(
            agent_id,
            &thread_id,
            turn_id,
            Some(&route.model),
            Some(&route.reasoning_effort),
        )?;
        Ok(())
    }

    pub async fn ingest_codex_event(&self, event: CodexEvent) -> Result<(), OrchestratorError> {
        let payload = event.message.get("params").unwrap_or(&event.message);
        let thread_id = value_text(payload, &[&["threadId"], &["thread", "id"], &["thread_id"]])
            .map(ToOwned::to_owned);
        let mut agent_id = thread_id
            .as_deref()
            .map(|thread| self.store.agent_by_thread(thread))
            .transpose()?
            .flatten();
        if event.direction != EventDirection::Outbound
            && agent_id.is_none()
            && event.method == "thread/started"
        {
            agent_id = self.project_native_subagent(payload)?;
        }
        let (run_id, attempt_id) = match agent_id.as_ref() {
            Some(agent) => {
                let (run, attempt) = self.store.agent_context(agent)?;
                (Some(run), attempt)
            }
            None => (None, None),
        };
        if event.direction != EventDirection::Outbound
            && let Some(attempt_id) = attempt_id.as_ref()
        {
            self.store
                .heartbeat_path_leases(attempt_id, self.config.orchestration.lease_ttl_seconds)?;
        }
        let context = harness_store::ProjectionContext {
            run_id: run_id.clone(),
            agent_session_id: agent_id.clone(),
        };
        if event.direction == EventDirection::Outbound {
            self.projection.ingest_outbound(
                &context,
                &event.method,
                event.request_id.as_ref().map(Value::to_string),
                &event.message,
            )?;
            return Ok(());
        }
        match event.kind {
            EventKind::Notification => {
                self.projection
                    .ingest_notification(&context, &event.method, payload)?;
                if event.method == "thread/tokenUsage/updated"
                    && let Some(run_id) = run_id.as_ref()
                {
                    self.enforce_run_budget(&self.store.run(run_id)?)?;
                }
            }
            EventKind::ServerRequest => {
                self.handle_server_request(&event, payload, agent_id.as_ref(), run_id.as_ref())
                    .await?;
                return Ok(());
            }
            EventKind::Stderr => {
                self.projection
                    .ingest_diagnostic(&event.method, &event.message)?;
                return Ok(());
            }
            EventKind::ProcessExit => {
                self.projection
                    .ingest_diagnostic(&event.method, &event.message)?;
                if event.message.get("stale").and_then(Value::as_bool) != Some(true) {
                    self.reconcile_orphaned_sessions("Codex App Server exited")?;
                }
                return Ok(());
            }
            EventKind::Request | EventKind::Response => return Ok(()),
        }

        if event.method == "item/completed"
            && let (Some(agent_id), Some(text)) =
                (agent_id.as_ref(), extract_agent_message(payload))
        {
            self.handle_structured_agent_message(agent_id, text).await?;
        }
        if event.method == "turn/completed"
            && let Some(agent_id) = agent_id.as_ref()
        {
            self.handle_turn_completed(agent_id, payload).await?;
        }
        Ok(())
    }

    fn project_native_subagent(
        &self,
        payload: &Value,
    ) -> Result<Option<AgentSessionId>, OrchestratorError> {
        let (Some(thread_id), Some(parent_thread_id)) = (
            value_text(payload, &[&["thread", "id"]]),
            value_text(payload, &[&["thread", "parentThreadId"]]),
        ) else {
            return Ok(None);
        };
        let Some(parent_id) = self.store.agent_by_thread(parent_thread_id)? else {
            return Ok(None);
        };
        let parent = self.store.agent(&parent_id)?;
        let (run_id, attempt_id) = self.store.agent_context(&parent_id)?;
        let (active_total, _, _) = self.active_agent_counts()?;
        let active_discovery = self
            .store
            .list_agents(&run_id)?
            .into_iter()
            .filter(|agent| {
                agent.role == AgentRole::Explorer && agent_state_consumes_capacity(&agent.state)
            })
            .count() as u32;
        let capacity_exceeded = active_total >= self.config.orchestration.max_total_agent_threads
            || active_discovery >= self.config.orchestration.max_read_only_discovery;
        let agent_id = AgentSessionId::new();
        self.store.create_agent_session(&NewAgentSession {
            id: agent_id.clone(),
            run_id: run_id.clone(),
            task_attempt_id: attempt_id,
            parent_agent_session_id: Some(parent_id),
            runtime_kind: "codex_native_subagent".to_owned(),
            role: AgentRole::Explorer,
            nickname: Some(format!("native-{}", short_id(thread_id))),
            requested_model: parent
                .effective_model
                .clone()
                .unwrap_or(parent.requested_model),
            requested_reasoning_effort: parent
                .effective_reasoning_effort
                .clone()
                .unwrap_or(parent.requested_reasoning_effort),
            sandbox_mode: parent.sandbox_mode,
            approval_policy: self.config.security.approval_policy.clone(),
            cwd: value_text(payload, &[&["thread", "cwd"]])
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(parent.cwd)),
            state: "RUNNING".to_owned(),
            current_goal: value_text(payload, &[&["thread", "preview"]]).map(ToOwned::to_owned),
            token_budget: None,
        })?;
        self.store.attach_codex_thread(
            &agent_id,
            thread_id,
            Some(parent_thread_id),
            &self.config.codex.service_name,
            value_text(payload, &[&["thread", "gitInfo", "branch"]]),
            value_text(payload, &[&["thread", "gitInfo", "sha"]]),
        )?;
        self.emit_agent_event(
            &run_id,
            &agent_id,
            "agent.native_subagent.started",
            json!({
                "thread_id": thread_id,
                "parent_thread_id": parent_thread_id,
                "capacity_exceeded": capacity_exceeded,
            }),
        )?;
        if capacity_exceeded {
            let run = self.store.set_scheduler_paused(&run_id, true)?;
            self.emit_run_event(
                &run,
                "scheduler.native_subagent_capacity_exceeded",
                json!({
                    "active_total_before_child": active_total,
                    "max_total": self.config.orchestration.max_total_agent_threads,
                    "active_discovery_before_child": active_discovery,
                    "max_discovery": self.config.orchestration.max_read_only_discovery,
                    "child_agent_id": agent_id,
                }),
            )?;
        }
        Ok(Some(agent_id))
    }

    async fn handle_server_request(
        &self,
        event: &CodexEvent,
        payload: &Value,
        agent_id: Option<&AgentSessionId>,
        run_id: Option<&RunId>,
    ) -> Result<(), OrchestratorError> {
        if !matches!(
            event.method.as_str(),
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
        ) {
            if let Some(rpc_id) = event.request_id.clone() {
                self.runtime()
                    .await?
                    .respond_rpc_error(
                        rpc_id,
                        -32601,
                        "Harness Console v1 does not broker this server-request class",
                    )
                    .await?;
            }
            return Ok(());
        }
        let (Some(run_id), Some(thread_id), Some(rpc_id)) = (
            run_id,
            value_text(payload, &[&["threadId"]]),
            event.request_id.as_ref(),
        ) else {
            return Err(OrchestratorError::Protocol(
                "unmapped App Server request cannot be approved".to_owned(),
            ));
        };
        let attempt_id = agent_id
            .map(|agent| self.store.task_attempt_for_agent(agent))
            .transpose()?
            .flatten();
        let (expected_head_sha, expected_worktree_fingerprint) = match attempt_id.as_ref() {
            Some(attempt_id) => {
                let (_, worktree, _, _) = self.store.worktree_for_attempt(attempt_id)?;
                (
                    Some(self.git.head_sha(&worktree).await?),
                    Some(self.git.worktree_fingerprint(Path::new(&worktree)).await?),
                )
            }
            None => (None, None),
        };
        let approval_id = ApprovalId::new();
        self.store.create_approval(
            &NewApproval {
                id: approval_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: attempt_id,
                agent_session_id: agent_id.cloned(),
                thread_id: thread_id.to_owned(),
                turn_id: value_text(payload, &[&["turnId"]]).map(ToOwned::to_owned),
                item_id: value_text(payload, &[&["itemId"]]).map(ToOwned::to_owned),
                approval_type: event.method.clone(),
                risk_level: approval_risk(&event.method, payload),
                request: payload.clone(),
                expected_head_sha,
                expected_worktree_fingerprint,
            },
            rpc_id,
        )?;
        if let Some(agent) = agent_id {
            self.store.update_agent_state(
                agent,
                "WAITING_APPROVAL",
                Some("Waiting for operator approval"),
                None,
                None,
                None,
            )?;
        }
        self.store.emit_domain_event(
            Some(run_id),
            "approval",
            approval_id.as_str(),
            "approval.requested",
            payload,
            None,
        )?;
        Ok(())
    }

    async fn handle_structured_agent_message(
        &self,
        agent_id: &AgentSessionId,
        text: &str,
    ) -> Result<(), OrchestratorError> {
        let agent = self.store.agent(agent_id)?;
        let (run_id, attempt_id) = self.store.agent_context(agent_id)?;
        match agent.role {
            AgentRole::Architect => {
                if self.store.run(&run_id)?.state == RunState::Architecting
                    && let Ok(plan) = parse_json_text::<RunPlan>(text)
                {
                    self.submit_plan(&run_id, agent_id, plan)?;
                    self.store.update_agent_state(
                        agent_id,
                        "COMPLETED",
                        Some("Plan proposed for human review"),
                        None,
                        None,
                        None,
                    )?;
                }
            }
            AgentRole::Verifier => {
                let Some(attempt_id) = attempt_id else {
                    return Ok(());
                };
                let verdict = parse_json_text::<VerifierVerdict>(text)?;
                self.apply_verifier_verdict(&run_id, &attempt_id, agent_id, verdict)
                    .await?;
            }
            AgentRole::FinalAuditor => {
                let verdict = parse_json_text::<VerifierVerdict>(text)?;
                self.apply_final_audit_verdict(&run_id, agent_id, verdict)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_turn_completed(
        &self,
        agent_id: &AgentSessionId,
        payload: &Value,
    ) -> Result<(), OrchestratorError> {
        let status = value_text(payload, &[&["turn", "status"], &["status"]]).unwrap_or("failed");
        let agent = self.store.agent(agent_id)?;
        let (run_id, attempt_id) = self.store.agent_context(agent_id)?;
        if self.store.run(&run_id)?.state == RunState::Stopping {
            if let Some(attempt_id) = attempt_id.as_ref() {
                let task_id = self.store.task_for_attempt(attempt_id)?;
                let _ = self
                    .store
                    .transition_task(&task_id, TaskState::Canceled, None);
                self.store.release_path_leases(attempt_id, "run stopping")?;
                if let Ok((worktree_id, _, _, head)) = self.store.worktree_for_attempt(attempt_id) {
                    self.store.update_worktree(
                        &worktree_id,
                        "PRESERVED",
                        head.as_deref(),
                        Some("run stopped while turn was active"),
                    )?;
                }
                self.store.set_attempt_result(
                    attempt_id,
                    "CANCELED",
                    None,
                    Some("cancelled_superseded"),
                    Some("run stopped by operator"),
                )?;
            }
            self.store.update_agent_state(
                agent_id,
                "CANCELED",
                Some("Run stopped by operator"),
                None,
                None,
                None,
            )?;
            self.finish_stopping_run_if_idle(&run_id)?;
            return Ok(());
        }
        if status != "completed" {
            self.store.update_agent_state(
                agent_id,
                "FAILED",
                Some("Codex turn did not complete"),
                None,
                None,
                Some(("infrastructure_unavailable", status)),
            )?;
            if let Some(attempt_id) = attempt_id.as_ref() {
                let task_id = self.store.task_for_attempt(attempt_id)?;
                let _ = self
                    .store
                    .transition_task(&task_id, TaskState::NeedsHelp, None);
                self.store
                    .release_path_leases(attempt_id, "Codex turn failed")?;
                if let Ok((worktree_id, _, _, head)) = self.store.worktree_for_attempt(attempt_id) {
                    self.store.update_worktree(
                        &worktree_id,
                        "PRESERVED",
                        head.as_deref(),
                        Some("Codex turn failed before a safe handoff"),
                    )?;
                }
                self.store.set_attempt_result(
                    attempt_id,
                    "FAILED",
                    None,
                    Some("infrastructure_unavailable"),
                    Some(status),
                )?;
            } else if agent.role == AgentRole::Architect {
                let run = self.store.run(&run_id)?;
                if run.state == RunState::Architecting {
                    self.store.transition_run(
                        &run_id,
                        RunState::ReadyForArchitecture,
                        "architecture_turn_failed",
                        Some(run.version),
                        Some(("infrastructure_unavailable", status)),
                    )?;
                }
            } else if agent.role == AgentRole::FinalAuditor {
                let run = self.store.run(&run_id)?;
                if run.state == RunState::FinalAudit {
                    self.store.transition_run(
                        &run_id,
                        RunState::Blocked,
                        "final_audit_turn_failed",
                        Some(run.version),
                        Some(("infrastructure_unavailable", status)),
                    )?;
                }
            }
            return Ok(());
        }
        if matches!(agent.role, AgentRole::Worker | AgentRole::HighRiskWorker) {
            self.finalize_worker(agent_id).await?;
        } else if agent.role == AgentRole::Verifier {
            if let Some(attempt_id) = self.store.task_attempt_for_agent(agent_id)? {
                let task_id = self.store.task_for_attempt(&attempt_id)?;
                if self.store.task(&task_id)?.state == TaskState::Verifying {
                    self.store
                        .transition_task(&task_id, TaskState::NeedsHelp, None)?;
                    self.store.release_path_leases(
                        &attempt_id,
                        "verifier returned no schema-valid verdict",
                    )?;
                    self.store.set_attempt_result(
                        &attempt_id,
                        "FAILED",
                        None,
                        Some("inconclusive"),
                        Some("missing verifier verdict"),
                    )?;
                    self.store.update_agent_state(
                        agent_id,
                        "FAILED",
                        Some("Verifier returned no schema-valid verdict"),
                        None,
                        None,
                        Some(("inconclusive", "missing verifier verdict")),
                    )?;
                }
            }
        } else if agent.role == AgentRole::Architect {
            let run = self.store.run(&run_id)?;
            if run.state == RunState::Architecting {
                self.store.transition_run(
                    &run_id,
                    RunState::ReadyForArchitecture,
                    "architecture_response_invalid",
                    Some(run.version),
                    Some(("protocol_error", "architect returned no schema-valid plan")),
                )?;
                self.store.update_agent_state(
                    agent_id,
                    "FAILED",
                    Some("Architect returned no schema-valid plan"),
                    None,
                    None,
                    Some(("protocol_error", "missing architecture plan")),
                )?;
            }
        } else if agent.role == AgentRole::FinalAuditor {
            let run = self.store.run(&run_id)?;
            if run.state == RunState::FinalAudit {
                self.store.transition_run(
                    &run_id,
                    RunState::Blocked,
                    "final_audit_response_invalid",
                    Some(run.version),
                    Some((
                        "inconclusive",
                        "final auditor returned no schema-valid verdict",
                    )),
                )?;
                self.store.update_agent_state(
                    agent_id,
                    "FAILED",
                    Some("Final auditor returned no schema-valid verdict"),
                    None,
                    None,
                    Some(("inconclusive", "missing final audit verdict")),
                )?;
            }
        }
        Ok(())
    }

    fn finish_stopping_run_if_idle(&self, run_id: &RunId) -> Result<(), OrchestratorError> {
        let run = self.store.run(run_id)?;
        if run.state != RunState::Stopping
            || self
                .store
                .list_agents(run_id)?
                .iter()
                .any(|agent| agent.active_turn_id.is_some())
        {
            return Ok(());
        }
        self.cancel_run_work(run_id, "active turns reached a safe boundary")?;
        self.store.transition_run(
            run_id,
            RunState::Canceled,
            "canceled",
            Some(run.version),
            None,
        )?;
        Ok(())
    }

    fn cancel_run_work(&self, run_id: &RunId, reason: &str) -> Result<(), OrchestratorError> {
        for task in self.store.list_tasks(run_id)? {
            if !task.state.is_terminal() {
                self.store
                    .transition_task(&task.id, TaskState::Canceled, None)?;
            }
        }
        self.store.release_run_path_leases(run_id, reason)?;
        for worktree in self.store.list_worktrees(Some(run_id))? {
            if worktree.state != "REMOVED" {
                self.store.update_worktree(
                    &worktree.id,
                    "PRESERVED",
                    worktree.head_sha.as_deref(),
                    Some(reason),
                )?;
            }
        }
        Ok(())
    }

    fn reconcile_orphaned_sessions(&self, reason: &str) -> Result<(), OrchestratorError> {
        for run in self.store.list_runs(None, false)? {
            let mut affected = 0_u32;
            for agent in self
                .store
                .list_agents(&run.id)?
                .into_iter()
                .filter(|agent| {
                    !matches!(
                        agent.state.as_str(),
                        "COMPLETED"
                            | "TURN_COMPLETE"
                            | "FAILED"
                            | "INTERRUPTED"
                            | "CANCELED"
                            | "STALLED"
                    )
                })
            {
                affected = affected.saturating_add(1);
                self.store.clear_agent_active_turn(&agent.id)?;
                self.store.update_agent_state(
                    &agent.id,
                    "STALLED",
                    Some(reason),
                    None,
                    None,
                    Some(("infrastructure_unavailable", reason)),
                )?;
                let Some(attempt_id) = self.store.task_attempt_for_agent(&agent.id)? else {
                    continue;
                };
                let task_id = self.store.task_for_attempt(&attempt_id)?;
                let task = self.store.task(&task_id)?;
                if matches!(
                    task.state,
                    TaskState::Leased
                        | TaskState::Starting
                        | TaskState::Implementing
                        | TaskState::ReviewReady
                        | TaskState::Verifying
                        | TaskState::WaitingApproval
                        | TaskState::WaitingResource
                        | TaskState::Blocked
                        | TaskState::NeedsHelp
                ) {
                    self.store
                        .transition_task(&task_id, TaskState::Stalled, None)?;
                    self.store
                        .release_path_leases(&attempt_id, "runtime session lost")?;
                    self.store.set_attempt_result(
                        &attempt_id,
                        "STALLED",
                        task.head_sha.as_deref(),
                        Some("infrastructure_unavailable"),
                        Some(reason),
                    )?;
                    if let Ok((worktree_id, _, _, head)) =
                        self.store.worktree_for_attempt(&attempt_id)
                    {
                        self.store.update_worktree(
                            &worktree_id,
                            "PRESERVED",
                            head.as_deref(),
                            Some(reason),
                        )?;
                    }
                }
            }
            self.store.expire_pending_approvals(&run.id, reason)?;
            let current = self.store.run(&run.id)?;
            let reconciled = if current.state == RunState::Architecting {
                self.store.transition_run(
                    &run.id,
                    RunState::ReadyForArchitecture,
                    "architecture_session_lost",
                    Some(current.version),
                    Some(("infrastructure_unavailable", reason)),
                )?
            } else if current.state == RunState::FinalAudit && affected > 0 {
                self.store.transition_run(
                    &run.id,
                    RunState::Blocked,
                    "final_audit_session_lost",
                    Some(current.version),
                    Some(("infrastructure_unavailable", reason)),
                )?
            } else if current.state == RunState::Stopping {
                self.cancel_run_work(&run.id, reason)?;
                self.store.transition_run(
                    &run.id,
                    RunState::Canceled,
                    "canceled_after_recovery",
                    Some(current.version),
                    None,
                )?
            } else {
                current
            };
            if affected > 0 {
                self.emit_run_event(
                    &reconciled,
                    "runtime.sessions.reconciled",
                    json!({"affected_agents": affected, "reason": reason}),
                )?;
            }
        }
        Ok(())
    }

    fn enforce_run_budget(&self, run: &RunSummary) -> Result<bool, OrchestratorError> {
        let Some(budget) = run.run_token_budget else {
            return Ok(false);
        };
        let used = self.store.run_usage(&run.id)?.total_tokens;
        if used < budget {
            return Ok(false);
        }
        if !run.scheduler_paused {
            let paused = self.store.set_scheduler_paused(&run.id, true)?;
            self.emit_run_event(
                &paused,
                "run.token_budget.reached",
                json!({"used": used, "budget": budget}),
            )?;
        }
        Ok(true)
    }

    async fn finalize_worker(&self, agent_id: &AgentSessionId) -> Result<(), OrchestratorError> {
        let (run_id, Some(attempt_id)) = self.store.agent_context(agent_id)? else {
            return Err(OrchestratorError::Protocol(
                "worker lacks task attempt".to_owned(),
            ));
        };
        let task_id = self.store.task_for_attempt(&attempt_id)?;
        let task = self.store.task(&task_id)?;
        if task.state != TaskState::Implementing {
            return Ok(());
        }
        let (_, packet) = self
            .store
            .task_packet(&task_id)?
            .ok_or_else(|| OrchestratorError::Protocol("worker task packet missing".to_owned()))?;
        let (worktree_id, worktree, base_sha, _) = self.store.worktree_for_attempt(&attempt_id)?;
        let diff = match self
            .git
            .verify_diff(
                &worktree,
                &base_sha,
                &DiffPolicy {
                    owned_paths: packet.owned_paths.clone(),
                    forbidden_paths: packet
                        .forbidden_paths
                        .iter()
                        .chain(
                            self.profile
                                .profile
                                .forbidden_generated_runtime_paths
                                .iter(),
                        )
                        .cloned()
                        .collect(),
                    serial_paths: self.profile.profile.serial_paths.clone(),
                    reserved_serial_paths: packet.reserved_serial_paths.clone(),
                    max_files: packet.diff_budget.files,
                    max_lines: packet.diff_budget.lines,
                },
            )
            .await
        {
            Ok(diff) => diff,
            Err(error) => {
                let reason = format!("Git custody inspection failed: {error}");
                self.store
                    .transition_task(&task_id, TaskState::NeedsHelp, None)?;
                self.store
                    .release_path_leases(&attempt_id, "Git custody inspection failed")?;
                self.store
                    .update_worktree(&worktree_id, "PRESERVED", None, Some(&reason))?;
                self.store.set_attempt_result(
                    &attempt_id,
                    "FAILED",
                    None,
                    Some("policy_blocked"),
                    Some(&reason),
                )?;
                self.store.update_agent_state(
                    agent_id,
                    "FAILED",
                    Some(&reason),
                    None,
                    None,
                    Some(("policy_blocked", &reason)),
                )?;
                return Ok(());
            }
        };
        if !diff.acceptable() || diff.changed_paths.is_empty() {
            self.store.set_task_diff_result(
                &attempt_id,
                None,
                diff.files_changed(),
                diff.additions,
                diff.deletions,
                &diff.unexpected_paths,
            )?;
            self.store
                .transition_task(&task_id, TaskState::NeedsHelp, None)?;
            self.store
                .release_path_leases(&attempt_id, "diff custody check failed")?;
            self.store.update_worktree(
                &worktree_id,
                "PRESERVED",
                Some(&diff.head_sha),
                Some("diff custody check failed or diff was empty"),
            )?;
            self.store.set_attempt_result(
                &attempt_id,
                "FAILED",
                Some(&diff.head_sha),
                Some("policy_blocked"),
                Some("diff custody check failed or diff was empty"),
            )?;
            self.store.update_agent_state(
                agent_id,
                "FAILED",
                Some("Diff custody check failed"),
                None,
                None,
                Some((
                    "policy_blocked",
                    "diff custody check failed or diff was empty",
                )),
            )?;
            return Ok(());
        }
        if diff.head_sha != base_sha {
            self.store
                .transition_task(&task_id, TaskState::NeedsHelp, None)?;
            self.store
                .release_path_leases(&attempt_id, "agent-created commit detected")?;
            self.store.update_worktree(
                &worktree_id,
                "PRESERVED",
                Some(&diff.head_sha),
                Some("agent-created commit detected"),
            )?;
            self.store.set_attempt_result(
                &attempt_id,
                "FAILED",
                Some(&diff.head_sha),
                Some("policy_blocked"),
                Some("agent-created commit detected"),
            )?;
            self.store.update_agent_state(
                agent_id,
                "FAILED",
                Some("Agent created a commit; controller custody requires an uncommitted diff"),
                None,
                None,
                Some(("policy_blocked", "agent-created commit detected")),
            )?;
            return Ok(());
        }
        let commit = match self
            .git
            .commit(
                &worktree,
                &format!("{}: {}", packet.task_id, packet.title),
                &diff,
            )
            .await
        {
            Ok(commit) => commit,
            Err(error) => {
                let reason = format!("controller could not commit the verified diff: {error}");
                self.store
                    .transition_task(&task_id, TaskState::NeedsHelp, None)?;
                self.store
                    .release_path_leases(&attempt_id, "controller commit failed")?;
                self.store.update_worktree(
                    &worktree_id,
                    "PRESERVED",
                    Some(&diff.head_sha),
                    Some(&reason),
                )?;
                self.store.set_attempt_result(
                    &attempt_id,
                    "FAILED",
                    Some(&diff.head_sha),
                    Some("infrastructure_unavailable"),
                    Some(&reason),
                )?;
                self.store.update_agent_state(
                    agent_id,
                    "FAILED",
                    Some(&reason),
                    None,
                    None,
                    Some(("infrastructure_unavailable", &reason)),
                )?;
                return Ok(());
            }
        };
        self.store.set_task_diff_result(
            &attempt_id,
            Some(&commit),
            diff.files_changed(),
            diff.additions,
            diff.deletions,
            &diff.unexpected_paths,
        )?;
        self.store
            .update_worktree(&worktree_id, "REVIEW_READY", Some(&commit), None)?;
        self.store
            .set_attempt_result(&attempt_id, "REVIEW_READY", Some(&commit), None, None)?;
        self.store
            .transition_task(&task_id, TaskState::ReviewReady, None)?;
        self.store.update_agent_state(
            agent_id,
            "COMPLETED",
            Some("Controller committed custody-verified diff"),
            None,
            None,
            None,
        )?;
        self.launch_verifier(&run_id, &task_id, &attempt_id, &packet, &worktree, &commit)
            .await?;
        Ok(())
    }

    async fn launch_review_ready_verifier(
        &self,
        task: &TaskSummary,
    ) -> Result<bool, OrchestratorError> {
        if task.state != TaskState::ReviewReady {
            return Ok(false);
        }
        if self.store.list_agents(&task.run_id)?.iter().any(|agent| {
            agent.task_id.as_ref() == Some(&task.id)
                && agent.role == AgentRole::Verifier
                && agent_state_consumes_capacity(&agent.state)
        }) {
            return Ok(false);
        }
        let (attempt_id, packet) = self
            .store
            .task_packet(&task.id)?
            .ok_or_else(|| OrchestratorError::Blocked("task packet is missing".to_owned()))?;
        let (_, worktree, _, head) = self.store.worktree_for_attempt(&attempt_id)?;
        let head =
            head.ok_or_else(|| OrchestratorError::Blocked("review head is missing".to_owned()))?;
        self.launch_verifier(
            &task.run_id,
            &task.id,
            &attempt_id,
            &packet,
            &worktree,
            &head,
        )
        .await
    }

    async fn launch_verifier(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        attempt_id: &harness_domain::AttemptId,
        packet: &TaskPacket,
        worktree: &Path,
        commit: &str,
    ) -> Result<bool, OrchestratorError> {
        let (active_total, _, active_verifiers) = self.active_agent_counts()?;
        if active_total >= self.config.orchestration.max_total_agent_threads
            || active_verifiers >= self.config.orchestration.max_independent_verifiers
        {
            self.store
                .heartbeat_path_leases(attempt_id, self.config.orchestration.lease_ttl_seconds)?;
            self.store.emit_domain_event(
                Some(run_id),
                "task",
                task_id.as_str(),
                "task.verifier.queued",
                &json!({
                    "active_total": active_total,
                    "max_total": self.config.orchestration.max_total_agent_threads,
                    "active_verifiers": active_verifiers,
                    "max_verifiers": self.config.orchestration.max_independent_verifiers,
                }),
                None,
            )?;
            return Ok(false);
        }
        self.store
            .transition_task(task_id, TaskState::Verifying, None)?;
        let (_, _, review_base, _) = self.store.worktree_for_attempt(attempt_id)?;
        let route = &self.profile.profile.models.verifier;
        let agent_id = AgentSessionId::new();
        self.store.create_agent_session(&NewAgentSession {
            id: agent_id.clone(),
            run_id: run_id.clone(),
            task_attempt_id: Some(attempt_id.clone()),
            parent_agent_session_id: None,
            runtime_kind: "codex_controller".to_owned(),
            role: AgentRole::Verifier,
            nickname: Some(format!("verify-{}", packet.task_id)),
            requested_model: route.model.clone(),
            requested_reasoning_effort: route.reasoning_effort.clone(),
            sandbox_mode: SandboxMode::ReadOnly,
            approval_policy: "never".to_owned(),
            cwd: worktree.to_path_buf(),
            state: "STARTING".to_owned(),
            current_goal: Some(format!(
                "Independently verify {} at {commit}",
                packet.task_id
            )),
            token_budget: Some(packet.token_budget / 2),
        })?;
        let prompt = format!(
            "Independently verify task {} at exact commit {} against this packet:\n{}\n\nInspect the complete diff against {} and the cited authorities. Do not modify files. Return only JSON: {{\"verdict\":\"accept\"|\"changes_requested\",\"summary\":string,\"findings\":[{{\"severity\":string,\"file\":string|null,\"line\":number|null,\"description\":string,\"required_correction\":string}}]}}. Accept only when success criteria and required positive/negative tests are credibly met; a worker response is not proof.",
            packet.task_id,
            commit,
            serde_json::to_string_pretty(packet)?,
            review_base
        );
        if let Err(error) = self
            .start_agent(
                &agent_id,
                run_id,
                Some(attempt_id),
                worktree,
                route,
                SandboxMode::ReadOnly,
                &format!("Verify {}", packet.objective),
                Some(packet.token_budget / 2),
                prompt,
                Some(verifier_schema()),
            )
            .await
        {
            let reason = error.to_string();
            self.store
                .transition_task(task_id, TaskState::NeedsHelp, None)?;
            self.store
                .release_path_leases(attempt_id, "independent verifier could not start")?;
            if let Ok((worktree_id, _, _, head)) = self.store.worktree_for_attempt(attempt_id) {
                self.store.update_worktree(
                    &worktree_id,
                    "PRESERVED",
                    head.as_deref(),
                    Some("independent verifier could not start"),
                )?;
            }
            self.store.set_attempt_result(
                attempt_id,
                "FAILED",
                Some(commit),
                Some("infrastructure_unavailable"),
                Some(&reason),
            )?;
            self.store.update_agent_state(
                &agent_id,
                "FAILED",
                Some("Independent verifier could not start"),
                None,
                None,
                Some(("infrastructure_unavailable", &reason)),
            )?;
            return Err(error);
        }
        Ok(true)
    }

    async fn apply_verifier_verdict(
        &self,
        run_id: &RunId,
        attempt_id: &harness_domain::AttemptId,
        agent_id: &AgentSessionId,
        verdict: VerifierVerdict,
    ) -> Result<(), OrchestratorError> {
        let task_id = self.store.task_for_attempt(attempt_id)?;
        if self.store.task(&task_id)?.state != TaskState::Verifying {
            return Ok(());
        }
        if verdict.verdict == "accept" && verdict.findings.is_empty() {
            let (_, _, _, head) = self.store.worktree_for_attempt(attempt_id)?;
            let head = head
                .ok_or_else(|| OrchestratorError::Protocol("verified head missing".to_owned()))?;
            self.store
                .set_attempt_result(attempt_id, "COMPLETED", Some(&head), None, None)?;
            self.store
                .transition_task(&task_id, TaskState::Verified, None)?;
            self.store
                .release_path_leases(attempt_id, "independent verifier accepted")?;
            self.store.update_agent_state(
                agent_id,
                "COMPLETED",
                Some(&verdict.summary),
                None,
                None,
                None,
            )?;
            self.store.emit_domain_event(
                Some(run_id),
                "task",
                task_id.as_str(),
                "task.verified",
                &serde_json::to_value(&verdict)?,
                None,
            )?;
            self.store.mark_unblocked_tasks_ready(run_id)?;
            let tasks = self.store.list_tasks(run_id)?;
            if tasks.iter().all(|task| task.state == TaskState::Verified) {
                let run = self.store.run(run_id)?;
                if run.state == RunState::Executing {
                    self.store.transition_run(
                        run_id,
                        RunState::TaskVerification,
                        "tasks_verified",
                        Some(run.version),
                        None,
                    )?;
                    let run = self.store.run(run_id)?;
                    self.store.transition_run(
                        run_id,
                        RunState::IntegrationReady,
                        "integration_ready",
                        Some(run.version),
                        None,
                    )?;
                    self.prepare_integration(run_id).await?;
                }
            } else {
                self.tick(run_id).await?;
            }
        } else {
            self.store
                .transition_task(&task_id, TaskState::ChangesRequested, None)?;
            self.store.set_attempt_result(
                attempt_id,
                "CHANGES_REQUESTED",
                None,
                Some("source_failure"),
                Some(&verdict.summary),
            )?;
            self.store
                .release_path_leases(attempt_id, "verifier requested changes")?;
            self.store.update_agent_state(
                agent_id,
                "COMPLETED",
                Some(&verdict.summary),
                None,
                None,
                None,
            )?;
        }
        Ok(())
    }

    pub async fn decide_approval(
        &self,
        approval_id: &ApprovalId,
        request: ApprovalDecisionRequest,
        actor: &str,
    ) -> Result<ApprovalSummary, OrchestratorError> {
        if !matches!(request.decision.as_str(), "accept" | "decline" | "cancel") {
            return Err(OrchestratorError::Validation(
                "approval decision must be accept, decline, or cancel; session-wide approval is forbidden by v1 policy"
                    .to_owned(),
            ));
        }
        if request
            .note
            .as_ref()
            .is_some_and(|note| note.chars().count() > 4_000)
        {
            return Err(OrchestratorError::Validation(
                "approval note exceeds 4,000 characters".to_owned(),
            ));
        }
        if request.decision == "accept" {
            let (expected_head, expected_fingerprint) =
                self.store.approval_expected_custody(approval_id)?;
            if expected_head.is_some() || expected_fingerprint.is_some() {
                let approval = self.store.approval(approval_id)?;
                let agent_id = approval.agent_id.ok_or_else(|| {
                    OrchestratorError::Protocol(
                        "custody-bound approval has no agent session".to_owned(),
                    )
                })?;
                let attempt_id =
                    self.store
                        .task_attempt_for_agent(&agent_id)?
                        .ok_or_else(|| {
                            OrchestratorError::Protocol(
                                "custody-bound approval has no task attempt".to_owned(),
                            )
                        })?;
                let (_, worktree, _, _) = self.store.worktree_for_attempt(&attempt_id)?;
                if let Some(expected_head) = expected_head
                    && self.git.head_sha(&worktree).await? != expected_head
                {
                    return Err(OrchestratorError::Conflict(
                        "worktree head changed while approval was pending".to_owned(),
                    ));
                }
                if let Some(expected_fingerprint) = expected_fingerprint {
                    let current = self.git.worktree_fingerprint(Path::new(&worktree)).await?;
                    if current != expected_fingerprint {
                        return Err(OrchestratorError::Conflict(
                            "worktree contents changed while approval was pending".to_owned(),
                        ));
                    }
                }
            }
        }
        let (approval, rpc_id) = self.store.decide_approval(
            approval_id,
            &request.decision,
            request.note.as_deref(),
            actor,
            request.expected_version,
        )?;
        let runtime = self.runtime().await?;
        let delivery = runtime
            .respond_rpc(rpc_id, json!({"decision": request.decision}))
            .await;
        match delivery {
            Ok(()) => {
                self.store.mark_approval_delivered(approval_id, None)?;
                if let Some(agent) = approval.agent_id.as_ref() {
                    self.store.update_agent_state(
                        agent,
                        "RUNNING",
                        Some("Approval decision delivered"),
                        None,
                        None,
                        None,
                    )?;
                }
            }
            Err(error) => {
                self.store
                    .mark_approval_delivered(approval_id, Some(&error.to_string()))?;
                return Err(error.into());
            }
        }
        self.store.record_human_action(
            Some(&approval.run_id),
            None,
            actor,
            "decide_approval",
            "approval",
            approval_id.as_str(),
            &json!({"decision": request.decision, "note": request.note}),
        )?;
        self.store.approval(approval_id).map_err(Into::into)
    }

    pub async fn steer_agent(
        &self,
        agent_id: &AgentSessionId,
        message: &str,
        actor: &str,
    ) -> Result<Value, OrchestratorError> {
        if message.trim().is_empty() {
            return Err(OrchestratorError::Validation(
                "steer message is empty".to_owned(),
            ));
        }
        if message.chars().count() > 12_000 {
            return Err(OrchestratorError::Validation(
                "steer message exceeds 12,000 characters".to_owned(),
            ));
        }
        let agent = self.store.agent(agent_id)?;
        let (thread, turn) = (
            agent
                .thread_id
                .as_deref()
                .ok_or_else(|| OrchestratorError::Conflict("agent has no thread".to_owned()))?,
            agent.active_turn_id.as_deref().ok_or_else(|| {
                OrchestratorError::Conflict("agent has no active turn".to_owned())
            })?,
        );
        let result = self
            .runtime()
            .await?
            .steer_turn(thread, turn, message)
            .await?;
        let (run_id, attempt) = self.store.agent_context(agent_id)?;
        self.store.record_human_action(
            Some(&run_id),
            attempt.as_ref(),
            actor,
            "steer_agent",
            "agent",
            agent_id.as_str(),
            &json!({"message": message}),
        )?;
        self.store.update_agent_state(
            agent_id,
            "STEERED",
            Some("Operator steering delivered"),
            None,
            None,
            None,
        )?;
        Ok(result)
    }

    pub async fn interrupt_agent(
        &self,
        agent_id: &AgentSessionId,
        actor: &str,
    ) -> Result<Value, OrchestratorError> {
        let agent = self.store.agent(agent_id)?;
        let thread = agent
            .thread_id
            .as_deref()
            .ok_or_else(|| OrchestratorError::Conflict("agent has no thread".to_owned()))?;
        let turn = agent
            .active_turn_id
            .as_deref()
            .ok_or_else(|| OrchestratorError::Conflict("agent has no active turn".to_owned()))?;
        let result = self.runtime().await?.interrupt_turn(thread, turn).await?;
        let (run_id, attempt) = self.store.agent_context(agent_id)?;
        self.store.update_agent_state(
            agent_id,
            "INTERRUPTED",
            Some("Interrupted by operator"),
            None,
            None,
            None,
        )?;
        if let Some(attempt) = attempt.as_ref() {
            let task = self.store.task_for_attempt(attempt)?;
            let _ = self
                .store
                .transition_task(&task, TaskState::Interrupted, None);
            self.store.set_attempt_result(
                attempt,
                "INTERRUPTED",
                None,
                Some("cancelled_superseded"),
                Some("operator interrupted turn"),
            )?;
        }
        self.store.record_human_action(
            Some(&run_id),
            attempt.as_ref(),
            actor,
            "interrupt_agent",
            "agent",
            agent_id.as_str(),
            &json!({}),
        )?;
        Ok(result)
    }

    pub fn set_scheduler_paused(
        &self,
        run_id: &RunId,
        paused: bool,
        actor: &str,
    ) -> Result<RunSummary, OrchestratorError> {
        let run = self.store.set_scheduler_paused(run_id, paused)?;
        self.store.record_human_action(
            Some(run_id),
            None,
            actor,
            if paused {
                "pause_scheduler"
            } else {
                "resume_scheduler"
            },
            "run",
            run_id.as_str(),
            &json!({"paused": paused}),
        )?;
        Ok(run)
    }

    async fn prepare_integration(&self, run_id: &RunId) -> Result<(), OrchestratorError> {
        let run = self.store.run(run_id)?;
        if run.state != RunState::IntegrationReady {
            return Err(OrchestratorError::Conflict(format!(
                "run is {}, not INTEGRATION_READY",
                run.state
            )));
        }
        if self
            .store
            .list_worktrees(Some(run_id))?
            .iter()
            .any(|worktree| worktree.kind == "integration")
        {
            return Ok(());
        }
        let repository = self.store.repository(&run.repository_id)?;
        let tasks = self.store.list_tasks(run_id)?;
        let commits = ordered_task_commits(&tasks, self.store.verified_task_commits(run_id)?)?;
        if commits.is_empty() {
            return Err(OrchestratorError::Blocked(
                "no independently verified commits are available for integration".to_owned(),
            ));
        }
        let branch = format!(
            "harness/run-{}",
            short_id(run.id.as_str()).to_ascii_lowercase()
        );
        let managed = self
            .git
            .create_worktree(&WorktreeSpec {
                repository_root: PathBuf::from(&repository.root_path),
                relative_path: PathBuf::from(run.id.as_str()).join("integration"),
                base_sha: run.base_sha.clone(),
                branch: Some(branch.clone()),
            })
            .await?;
        let worktree_id = WorktreeId::new();
        self.store.create_worktree(&NewWorktree {
            id: worktree_id.clone(),
            run_id: run.id.clone(),
            task_attempt_id: None,
            kind: "integration".to_owned(),
            path: managed.path.clone(),
            branch: Some(branch.clone()),
            base_sha: run.base_sha.clone(),
            head_sha: Some(managed.head_sha),
            state: "INTEGRATING".to_owned(),
        })?;
        for (task_id, _) in &commits {
            self.store
                .transition_task(task_id, TaskState::IntegrationQueued, None)?;
            self.store
                .transition_task(task_id, TaskState::Integrating, None)?;
        }
        let commit_shas = commits
            .iter()
            .map(|(_, sha)| sha.clone())
            .collect::<Vec<_>>();
        let head = match self.git.cherry_pick(&managed.path, &commit_shas).await {
            Ok(head) => head,
            Err(error) => {
                self.store.update_worktree(
                    &worktree_id,
                    "CONFLICTED",
                    None,
                    Some("semantic integration conflict; operator review required"),
                )?;
                let current = self.store.run(run_id)?;
                self.store.transition_run(
                    run_id,
                    RunState::Blocked,
                    "integration_conflict",
                    Some(current.version),
                    Some(("integration_conflict", &error.to_string())),
                )?;
                return Err(error.into());
            }
        };
        for (task_id, _) in &commits {
            self.store
                .transition_task(task_id, TaskState::Integrated, None)?;
        }
        self.store
            .update_worktree(&worktree_id, "REVIEW_READY", Some(&head), None)?;
        self.store.set_run_integration(run_id, &branch, &head)?;
        self.store.emit_domain_event(
            Some(run_id),
            "run",
            run_id.as_str(),
            "run.integration.prepared",
            &json!({
                "branch": branch,
                "head_sha": head,
                "commits": commit_shas,
                "worktree_id": worktree_id,
            }),
            None,
        )?;
        Ok(())
    }

    pub async fn approve_integration(
        &self,
        run_id: &RunId,
        expected_head_sha: &str,
        actor: &str,
    ) -> Result<OperationAccepted, OrchestratorError> {
        require_exact_sha(expected_head_sha)?;
        let mut run = self.store.run(run_id)?;
        if run.state != RunState::IntegrationReady {
            return Err(OrchestratorError::Conflict(format!(
                "run is {}, not INTEGRATION_READY",
                run.state
            )));
        }
        if run.integration_sha.as_deref() != Some(expected_head_sha) {
            return Err(OrchestratorError::Conflict(
                "integration head changed before approval".to_owned(),
            ));
        }
        let worktree = self
            .store
            .list_worktrees(Some(run_id))?
            .into_iter()
            .find(|worktree| worktree.kind == "integration")
            .ok_or_else(|| {
                OrchestratorError::Blocked("integration worktree is missing".to_owned())
            })?;
        if self.git.head_sha(Path::new(&worktree.path)).await? != expected_head_sha {
            return Err(OrchestratorError::Conflict(
                "integration worktree no longer matches the reviewed head".to_owned(),
            ));
        }
        self.store.record_human_action(
            Some(run_id),
            None,
            actor,
            "approve_integration",
            "run",
            run_id.as_str(),
            &json!({"expected_head_sha": expected_head_sha}),
        )?;
        run = self.store.transition_run(
            run_id,
            RunState::Integrating,
            "integration_approved",
            Some(run.version),
            None,
        )?;
        run = self.store.transition_run(
            run_id,
            RunState::IntegrationVerification,
            "integration_verification",
            Some(run.version),
            None,
        )?;
        let result = self
            .runner
            .run(CommandSpec {
                program: "git".to_owned(),
                args: vec![
                    "diff".to_owned(),
                    "--check".to_owned(),
                    format!("{}..{}", run.base_sha, expected_head_sha),
                    "--".to_owned(),
                ],
                cwd: PathBuf::from(&worktree.path),
                resource_class: ResourceClass::Control,
                timeout_ms: 120_000,
                inherited_environment: vec![
                    "PATH".to_owned(),
                    "LANG".to_owned(),
                    "LC_ALL".to_owned(),
                ],
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await?;
        let stdout = self.register_command_artifact(
            run_id,
            None,
            "integration_stdout",
            &format!("{}-stdout.log", result.command_id),
            &result.stdout.path,
        )?;
        let stderr = self.register_command_artifact(
            run_id,
            None,
            "integration_stderr",
            &format!("{}-stderr.log", result.command_id),
            &result.stderr.path,
        )?;
        let command_id = CommandRunId::from(result.command_id.clone());
        self.store.record_command(&NewCommandRecord {
            id: command_id.clone(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            agent_session_id: None,
            worktree_id: Some(worktree.id.clone()),
            command: json!({"program": "git", "args": ["diff", "--check", format!("{}..{}", run.base_sha, expected_head_sha), "--"]}),
            cwd: PathBuf::from(&worktree.path),
            source_sha_before: Some(expected_head_sha.to_owned()),
            source_sha_after: Some(expected_head_sha.to_owned()),
            resource_class: "control".to_owned(),
            host_identity: std::env::var("HOSTNAME").ok(),
            target_profile: Some(self.profile.profile.profile_id.clone()),
            started_at: result.started_at_ms,
            completed_at: result.started_at_ms.saturating_add(result.duration_ms as i64),
            exit_code: result.exit_code,
            signal: result.signal,
            timed_out: result.timed_out,
            result_class: result.result_class,
            stdout_artifact_id: Some(stdout),
            stderr_artifact_id: Some(stderr),
            error: None,
        })?;
        let validation_id = ValidationId::new();
        self.store.record_validation(&NewValidationRecord {
            id: validation_id.clone(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            worktree_id: worktree.id.clone(),
            validator_id: "integration-diff-check".to_owned(),
            proof_tier: ProofTier::T1,
            source_sha: expected_head_sha.to_owned(),
            selector_reason: "mandatory integration custody check".to_owned(),
            result_class: result.result_class,
            command_run_id: Some(command_id),
            started_at: result.started_at_ms,
            completed_at: result
                .started_at_ms
                .saturating_add(result.duration_ms as i64),
        })?;
        self.evidence.record(EvidenceClaim {
            id: EvidenceId::new(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            validation_id: Some(validation_id),
            claim_id: "integration-diff-check".to_owned(),
            checklist_rows: vec!["Integrated commits preserve a clean Git patch".to_owned()],
            source_sha: expected_head_sha.to_owned(),
            proof_tier: ProofTier::T1,
            result_class: result.result_class,
            details: json!({"exit_code": result.exit_code, "timed_out": result.timed_out}),
            unproved_claims: if result.succeeded() {
                Vec::new()
            } else {
                vec!["integration patch formatting remains unproved".to_owned()]
            },
            artifacts: Vec::new(),
        })?;
        if !result.succeeded() {
            self.store.transition_run(
                run_id,
                RunState::Blocked,
                "integration_validation_failed",
                Some(run.version),
                Some((
                    "source_failure",
                    "git diff --check failed on integration head",
                )),
            )?;
            return Err(OrchestratorError::Blocked(
                "integration validation failed; inspect the retained command artifacts".to_owned(),
            ));
        }
        run = self.store.transition_run(
            run_id,
            RunState::FinalAudit,
            "final_audit",
            Some(run.version),
            None,
        )?;
        let tasks = self.store.list_tasks(run_id)?;
        if tasks.iter().any(|task| task.state != TaskState::Integrated) {
            return Err(OrchestratorError::Blocked(
                "final audit found a task that was not integrated".to_owned(),
            ));
        }
        if let Err(error) = self
            .launch_final_auditor(run_id, Path::new(&worktree.path), expected_head_sha)
            .await
        {
            let reason = error.to_string();
            let current = self.store.run(run_id)?;
            self.store.transition_run(
                run_id,
                RunState::Blocked,
                "final_audit_unavailable",
                Some(current.version),
                Some(("infrastructure_unavailable", &reason)),
            )?;
            return Err(error);
        }
        self.emit_run_event(
            &run,
            "run.final_audit.started",
            json!({"head_sha": expected_head_sha, "integration_check": result.result_class}),
        )?;
        Ok(operation("approve_integration", run_id.as_str()))
    }

    async fn launch_final_auditor(
        &self,
        run_id: &RunId,
        worktree: &Path,
        integration_sha: &str,
    ) -> Result<(), OrchestratorError> {
        require_exact_sha(integration_sha)?;
        let (active_total, _, _) = self.active_agent_counts()?;
        if active_total >= self.config.orchestration.max_total_agent_threads {
            return Err(OrchestratorError::Blocked(format!(
                "final audit requires a free Codex thread slot; {active_total}/{} are active",
                self.config.orchestration.max_total_agent_threads
            )));
        }
        let run = self.store.run(run_id)?;
        if run.state != RunState::FinalAudit
            || run.integration_sha.as_deref() != Some(integration_sha)
        {
            return Err(OrchestratorError::Conflict(
                "final audit target no longer matches the run's reviewed integration head"
                    .to_owned(),
            ));
        }
        if self.git.head_sha(worktree).await? != integration_sha {
            return Err(OrchestratorError::Conflict(
                "integration worktree changed before final audit".to_owned(),
            ));
        }

        let mut packet = architecture_packet(&run, &self.profile.profile, &self.config);
        packet.task_id = "FINAL_AUDIT".to_owned();
        packet.title = "Adversarial audit of the integrated result".to_owned();
        packet.owner_profile = "final_auditor".to_owned();
        packet.reviewer_profile = "human".to_owned();
        packet.base_sha = integration_sha.to_owned();
        packet.objective = format!(
            "Independently audit integrated head {integration_sha} for run objective: {}",
            run.objective
        );
        packet.success_criteria = vec![
            "Every approved task is present at the reviewed integration head".to_owned(),
            "The integrated diff respects active authorities and protected semantics".to_owned(),
            "Evidence claims do not outrun their recorded proof tier".to_owned(),
        ];
        let context = self.context.compile(
            worktree,
            integration_sha,
            &packet,
            &self.profile.profile,
            &self.profile.digest,
        )?;
        self.persist_context(run_id, None, "final_auditor", &context)?;

        let route = &self.profile.profile.models.final_auditor;
        let agent_id = AgentSessionId::new();
        self.store.create_agent_session(&NewAgentSession {
            id: agent_id.clone(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            parent_agent_session_id: None,
            runtime_kind: "codex_controller".to_owned(),
            role: AgentRole::FinalAuditor,
            nickname: Some("final-auditor".to_owned()),
            requested_model: route.model.clone(),
            requested_reasoning_effort: route.reasoning_effort.clone(),
            sandbox_mode: SandboxMode::ReadOnly,
            approval_policy: "never".to_owned(),
            cwd: worktree.to_path_buf(),
            state: "STARTING".to_owned(),
            current_goal: Some(packet.objective.clone()),
            token_budget: Some(self.config.orchestration.default_task_token_budget),
        })?;
        let plan = self
            .store
            .latest_plan(run_id)?
            .map(|(_, plan, _, _)| plan)
            .ok_or_else(|| OrchestratorError::Blocked("approved plan is missing".to_owned()))?;
        let evidence = self.store.evidence_snapshot(run_id)?;
        let prompt = format!(
            "{}\n\nAudit exact integrated head {} against base {} and this approved plan:\n{}\n\nController evidence snapshot:\n{}\n\nInspect the actual repository and complete diff. Do not modify files. Return only JSON matching the supplied schema. Use verdict `accept` only when the integrated result, task coverage, authority compliance, and recorded proof are all credible; otherwise return `changes_requested` with concrete findings.",
            context.prompt_prefix(),
            integration_sha,
            run.base_sha,
            serde_json::to_string_pretty(&plan)?,
            serde_json::to_string_pretty(&evidence)?,
        );
        self.start_agent(
            &agent_id,
            run_id,
            None,
            worktree,
            route,
            SandboxMode::ReadOnly,
            &packet.objective,
            Some(self.config.orchestration.default_task_token_budget),
            prompt,
            Some(verifier_schema()),
        )
        .await?;
        self.emit_agent_event(
            run_id,
            &agent_id,
            "agent.final_auditor.started",
            json!({"head_sha": integration_sha}),
        )?;
        Ok(())
    }

    async fn apply_final_audit_verdict(
        &self,
        run_id: &RunId,
        agent_id: &AgentSessionId,
        verdict: VerifierVerdict,
    ) -> Result<(), OrchestratorError> {
        let mut run = self.store.run(run_id)?;
        if run.state != RunState::FinalAudit {
            return Ok(());
        }
        let integration_sha = run.integration_sha.clone().ok_or_else(|| {
            OrchestratorError::Protocol("final audit run has no integration SHA".to_owned())
        })?;
        require_exact_sha(&integration_sha)?;
        let worktree = self
            .store
            .list_worktrees(Some(run_id))?
            .into_iter()
            .find(|worktree| worktree.kind == "integration")
            .ok_or_else(|| {
                OrchestratorError::Blocked("integration worktree is missing".to_owned())
            })?;
        if self.git.head_sha(Path::new(&worktree.path)).await? != integration_sha {
            return Err(OrchestratorError::Conflict(
                "integration head changed during final audit".to_owned(),
            ));
        }
        let accepted = verdict.verdict == "accept" && verdict.findings.is_empty();
        let tasks = self.store.list_tasks(run_id)?;
        self.evidence.record(EvidenceClaim {
            id: EvidenceId::new(),
            run_id: run_id.clone(),
            task_attempt_id: None,
            validation_id: None,
            claim_id: "independent-final-audit".to_owned(),
            checklist_rows: tasks.iter().map(|task| task.title.clone()).collect(),
            source_sha: integration_sha.clone(),
            proof_tier: ProofTier::T2,
            result_class: if accepted {
                ResultClass::Success
            } else {
                ResultClass::SourceFailure
            },
            details: serde_json::to_value(&verdict)?,
            unproved_claims: if accepted {
                Vec::new()
            } else {
                verdict
                    .findings
                    .iter()
                    .map(|finding| finding.description.clone())
                    .collect()
            },
            artifacts: Vec::new(),
        })?;
        self.store.update_agent_state(
            agent_id,
            "COMPLETED",
            Some(&verdict.summary),
            None,
            None,
            None,
        )?;
        if !accepted {
            run = self.store.transition_run(
                run_id,
                RunState::Blocked,
                "final_audit_changes_requested",
                Some(run.version),
                Some(("source_failure", &verdict.summary)),
            )?;
            self.emit_run_event(
                &run,
                "run.final_audit.rejected",
                serde_json::to_value(&verdict)?,
            )?;
            return Ok(());
        }

        run = self.store.transition_run(
            run_id,
            RunState::HumanReview,
            "human_review",
            Some(run.version),
            None,
        )?;
        run = self.store.transition_run(
            run_id,
            RunState::PublicationReady,
            "publication_ready",
            Some(run.version),
            None,
        )?;
        if run.publication_mode == "local_only" {
            run = self.store.transition_run(
                run_id,
                RunState::Completed,
                "completed_local",
                Some(run.version),
                None,
            )?;
            for task in tasks {
                self.store
                    .transition_task(&task.id, TaskState::Closed, None)?;
            }
        }
        self.emit_run_event(
            &run,
            "run.final_audit.accepted",
            json!({"head_sha": integration_sha, "summary": verdict.summary}),
        )?;
        Ok(())
    }

    pub async fn retry_task(
        &self,
        task_id: &TaskId,
        request: RetryTaskRequest,
        actor: &str,
    ) -> Result<OperationAccepted, OrchestratorError> {
        if request.reason.trim().is_empty() {
            return Err(OrchestratorError::Validation(
                "retry reason must not be empty".to_owned(),
            ));
        }
        if request.reason.chars().count() > 4_000
            || request
                .revised_objective
                .as_ref()
                .is_some_and(|objective| objective.chars().count() > 4_000)
        {
            return Err(OrchestratorError::Validation(
                "retry reason and revised objective are limited to 4,000 characters".to_owned(),
            ));
        }
        if !matches!(request.model_route.as_str(), "same" | "escalate_terra") {
            return Err(OrchestratorError::Validation(
                "retry model_route must be same or escalate_terra".to_owned(),
            ));
        }
        let task = self.store.task(task_id)?;
        if !matches!(
            task.state,
            TaskState::NeedsHelp
                | TaskState::ChangesRequested
                | TaskState::Interrupted
                | TaskState::Stalled
                | TaskState::Blocked
                | TaskState::Failed
        ) {
            return Err(OrchestratorError::Conflict(format!(
                "task is {}, not retryable",
                task.state
            )));
        }
        let (attempt_id, mut packet) = self
            .store
            .task_packet(task_id)?
            .ok_or_else(|| OrchestratorError::Blocked("task has no prior packet".to_owned()))?;
        if let Some(objective) = request
            .revised_objective
            .filter(|objective| !objective.trim().is_empty())
        {
            packet.objective = objective;
        }
        packet.token_budget = packet
            .token_budget
            .saturating_add(request.additional_token_budget);
        if request.model_route == "escalate_terra" {
            packet.owner_profile = "worker_escalation".to_owned();
        }
        self.store
            .release_path_leases(&attempt_id, "task retry requested")?;
        if let Ok((worktree_id, _, _, head)) = self.store.worktree_for_attempt(&attempt_id) {
            self.store.update_worktree(
                &worktree_id,
                "PRESERVED",
                head.as_deref(),
                Some("superseded by a new immutable retry attempt"),
            )?;
        }
        self.store
            .put_runtime_metadata(&format!("retry:{task_id}"), &serde_json::to_value(&packet)?)?;
        self.store.record_human_action(
            Some(&task.run_id),
            Some(&attempt_id),
            actor,
            "retry_task",
            "task",
            task_id.as_str(),
            &json!({
                "reason": request.reason,
                "model_route": request.model_route,
                "additional_token_budget": request.additional_token_budget,
                "packet_sha256": packet_digest(&packet)?,
            }),
        )?;
        self.store
            .transition_task(task_id, TaskState::Ready, None)?;
        self.tick(&task.run_id).await?;
        Ok(operation("retry_task", task_id.as_str()))
    }

    pub async fn request_task_review(
        &self,
        task_id: &TaskId,
        actor: &str,
    ) -> Result<OperationAccepted, OrchestratorError> {
        let task = self.store.task(task_id)?;
        if task.state != TaskState::ReviewReady {
            return Err(OrchestratorError::Conflict(format!(
                "task is {}, not REVIEW_READY",
                task.state
            )));
        }
        if self.store.list_agents(&task.run_id)?.iter().any(|agent| {
            agent.task_id.as_ref() == Some(task_id)
                && agent.role == AgentRole::Verifier
                && agent_state_consumes_capacity(&agent.state)
        }) {
            return Err(OrchestratorError::Conflict(
                "an independent verifier is already active".to_owned(),
            ));
        }
        let (attempt_id, packet) = self
            .store
            .task_packet(task_id)?
            .ok_or_else(|| OrchestratorError::Blocked("task packet is missing".to_owned()))?;
        let (_, worktree, _, head) = self.store.worktree_for_attempt(&attempt_id)?;
        let head =
            head.ok_or_else(|| OrchestratorError::Blocked("review head is missing".to_owned()))?;
        self.store.record_human_action(
            Some(&task.run_id),
            Some(&attempt_id),
            actor,
            "request_task_review",
            "task",
            task_id.as_str(),
            &json!({"head_sha": head}),
        )?;
        if !self
            .launch_verifier(
                &task.run_id,
                task_id,
                &attempt_id,
                &packet,
                &worktree,
                &head,
            )
            .await?
        {
            return Err(OrchestratorError::Conflict(
                "independent verifier capacity is currently exhausted".to_owned(),
            ));
        }
        Ok(operation("request_task_review", task_id.as_str()))
    }

    pub async fn publish_draft_pr(
        &self,
        run_id: &RunId,
        request: PublishDraftPrRequest,
        actor: &str,
    ) -> Result<OperationAccepted, OrchestratorError> {
        require_exact_sha(&request.expected_head_sha)?;
        if request.title.trim().is_empty() || request.title.chars().count() > 240 {
            return Err(OrchestratorError::Validation(
                "draft PR title must contain 1-240 characters".to_owned(),
            ));
        }
        let run = self.store.run(run_id)?;
        if run.state != RunState::PublicationReady {
            return Err(OrchestratorError::Conflict(format!(
                "run is {}, not PUBLICATION_READY",
                run.state
            )));
        }
        if run.publication_mode != "draft_pr_after_approval" {
            return Err(OrchestratorError::Conflict(
                "run was not configured for draft PR publication".to_owned(),
            ));
        }
        if run.integration_sha.as_deref() != Some(request.expected_head_sha.as_str()) {
            return Err(OrchestratorError::Conflict(
                "publication head differs from reviewed integration head".to_owned(),
            ));
        }
        let branch = run.integration_branch.as_deref().ok_or_else(|| {
            OrchestratorError::Blocked("integration branch is missing".to_owned())
        })?;
        let worktree = self
            .store
            .list_worktrees(Some(run_id))?
            .into_iter()
            .find(|worktree| worktree.kind == "integration")
            .ok_or_else(|| {
                OrchestratorError::Blocked("integration worktree is missing".to_owned())
            })?;
        self.git
            .push_exact(
                Path::new(&worktree.path),
                "origin",
                branch,
                &request.expected_head_sha,
            )
            .await?;
        let repository = self.store.repository(&run.repository_id)?;
        let mut body = format!(
            "Harness Console run `{}`\n\nBase: `{}`\nHead: `{}`\n\nEvidence remains local until explicitly exported.",
            run.id, run.base_sha, request.expected_head_sha
        );
        if let Some(appendix) = request.body_appendix {
            if appendix.chars().count() > 20_000 {
                return Err(OrchestratorError::Validation(
                    "draft PR appendix exceeds 20,000 characters".to_owned(),
                ));
            }
            body.push_str("\n\n");
            body.push_str(&appendix);
        }
        let result = self
            .runner
            .run(CommandSpec {
                program: "gh".to_owned(),
                args: vec![
                    "pr".to_owned(),
                    "create".to_owned(),
                    "--draft".to_owned(),
                    "--title".to_owned(),
                    request.title.clone(),
                    "--body".to_owned(),
                    body,
                    "--head".to_owned(),
                    branch.to_owned(),
                    "--base".to_owned(),
                    repository.default_branch,
                ],
                cwd: PathBuf::from(&worktree.path),
                resource_class: ResourceClass::Control,
                timeout_ms: 120_000,
                inherited_environment: vec![
                    "PATH".to_owned(),
                    "HOME".to_owned(),
                    "GH_HOST".to_owned(),
                    "GH_TOKEN".to_owned(),
                    "GITHUB_TOKEN".to_owned(),
                    "LANG".to_owned(),
                ],
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await?;
        let stdout = self.register_command_artifact(
            run_id,
            None,
            "publication_stdout",
            &format!("{}-stdout.log", result.command_id),
            &result.stdout.path,
        )?;
        let stderr = self.register_command_artifact(
            run_id,
            None,
            "publication_stderr",
            &format!("{}-stderr.log", result.command_id),
            &result.stderr.path,
        )?;
        self.store.record_command(&NewCommandRecord {
            id: CommandRunId::from(result.command_id.clone()),
            run_id: run_id.clone(),
            task_attempt_id: None,
            agent_session_id: None,
            worktree_id: Some(worktree.id),
            command: json!({"program": "gh", "args": ["pr", "create", "--draft", "--title", request.title, "--head", branch]}),
            cwd: PathBuf::from(&worktree.path),
            source_sha_before: Some(request.expected_head_sha.clone()),
            source_sha_after: Some(request.expected_head_sha.clone()),
            resource_class: "control".to_owned(),
            host_identity: std::env::var("HOSTNAME").ok(),
            target_profile: Some(self.profile.profile.profile_id.clone()),
            started_at: result.started_at_ms,
            completed_at: result.started_at_ms.saturating_add(result.duration_ms as i64),
            exit_code: result.exit_code,
            signal: result.signal,
            timed_out: result.timed_out,
            result_class: result.result_class,
            stdout_artifact_id: Some(stdout),
            stderr_artifact_id: Some(stderr),
            error: None,
        })?;
        if !result.succeeded() {
            return Err(OrchestratorError::Blocked(
                "gh could not create the draft PR; publication logs were retained".to_owned(),
            ));
        }
        let url = result.stdout.preview.lines().last().unwrap_or_default();
        self.store.record_human_action(
            Some(run_id),
            None,
            actor,
            "publish_draft_pr",
            "run",
            run_id.as_str(),
            &json!({"head_sha": request.expected_head_sha, "branch": branch, "url": url}),
        )?;
        let updated = self.store.transition_run(
            run_id,
            RunState::DraftPrCreated,
            "draft_pr_created",
            Some(run.version),
            None,
        )?;
        self.emit_run_event(
            &updated,
            "run.draft_pr.created",
            json!({"head_sha": request.expected_head_sha, "branch": branch, "url": url}),
        )?;
        Ok(operation("publish_draft_pr", run_id.as_str()))
    }

    pub async fn run_validator(
        &self,
        task_id: &TaskId,
        validator_id: &str,
    ) -> Result<ValidationOutcome, OrchestratorError> {
        let task = self.store.task(task_id)?;
        let (attempt_id, packet) = self
            .store
            .task_packet(task_id)?
            .ok_or_else(|| OrchestratorError::Blocked("task has no current attempt".to_owned()))?;
        let (worktree_id, worktree, base_sha, stored_head) =
            self.store.worktree_for_attempt(&attempt_id)?;
        let validator = self
            .profile
            .profile
            .validators
            .iter()
            .find(|validator| validator.id == validator_id)
            .cloned()
            .ok_or_else(|| {
                OrchestratorError::Validation(format!("unknown validator {validator_id}"))
            })?;
        if validator.command.is_empty() {
            return Err(OrchestratorError::Validation(format!(
                "validator {validator_id} has no command"
            )));
        }
        let source_before = self.git.head_sha(&worktree).await?;
        if stored_head
            .as_deref()
            .is_some_and(|head| head != source_before)
        {
            return Err(OrchestratorError::Conflict(
                "worktree head differs from the controller-recorded head".to_owned(),
            ));
        }
        let result = self
            .runner
            .run(CommandSpec {
                program: validator.command[0].clone(),
                args: validator.command[1..].to_vec(),
                cwd: worktree.clone(),
                resource_class: validator.class(),
                timeout_ms: self
                    .config
                    .orchestration
                    .default_turn_timeout_seconds
                    .saturating_mul(1_000),
                inherited_environment: vec![
                    "PATH".to_owned(),
                    "CARGO_HOME".to_owned(),
                    "RUSTUP_HOME".to_owned(),
                    "LANG".to_owned(),
                    "LC_ALL".to_owned(),
                    "TMPDIR".to_owned(),
                ],
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await?;
        let source_after = self.git.head_sha(&worktree).await?;
        let stdout_id = self.register_command_artifact(
            &task.run_id,
            Some(&attempt_id),
            "command_stdout",
            &format!("{}-stdout.log", result.command_id),
            &result.stdout.path,
        )?;
        let stderr_id = self.register_command_artifact(
            &task.run_id,
            Some(&attempt_id),
            "command_stderr",
            &format!("{}-stderr.log", result.command_id),
            &result.stderr.path,
        )?;
        let command_id = CommandRunId::from(result.command_id.clone());
        let effective_result = if source_before == source_after {
            result.result_class
        } else {
            ResultClass::SourceFailure
        };
        self.store.record_command(&NewCommandRecord {
            id: command_id.clone(),
            run_id: task.run_id.clone(),
            task_attempt_id: Some(attempt_id.clone()),
            agent_session_id: None,
            worktree_id: Some(worktree_id.clone()),
            command: json!({"program": validator.command[0], "args": validator.command[1..]}),
            cwd: worktree.clone(),
            source_sha_before: Some(source_before.clone()),
            source_sha_after: Some(source_after.clone()),
            resource_class: serde_json::to_value(validator.class())?
                .as_str()
                .unwrap_or("hardware")
                .to_owned(),
            host_identity: std::env::var("HOSTNAME").ok(),
            target_profile: Some(self.profile.profile.profile_id.clone()),
            started_at: result.started_at_ms,
            completed_at: result
                .started_at_ms
                .saturating_add(result.duration_ms as i64),
            exit_code: result.exit_code,
            signal: result.signal,
            timed_out: result.timed_out,
            result_class: effective_result,
            stdout_artifact_id: Some(stdout_id.clone()),
            stderr_artifact_id: Some(stderr_id.clone()),
            error: (source_before != source_after)
                .then(|| json!({"reason": "validator changed source HEAD"})),
        })?;
        let validation_id = ValidationId::new();
        let proof_tier = parse_proof_tier(&validator.proof_tier)?;
        self.store.record_validation(&NewValidationRecord {
            id: validation_id.clone(),
            run_id: task.run_id.clone(),
            task_attempt_id: Some(attempt_id.clone()),
            worktree_id,
            validator_id: validator.id.clone(),
            proof_tier,
            source_sha: source_before.clone(),
            selector_reason: format!("selected for task {}", packet.task_id),
            result_class: effective_result,
            command_run_id: Some(command_id.clone()),
            started_at: result.started_at_ms,
            completed_at: result
                .started_at_ms
                .saturating_add(result.duration_ms as i64),
        })?;
        let unproved = if effective_result == ResultClass::Success {
            Vec::new()
        } else {
            packet.required_evidence.clone()
        };
        self.evidence.record(EvidenceClaim {
            id: EvidenceId::new(),
            run_id: task.run_id.clone(),
            task_attempt_id: Some(attempt_id),
            validation_id: Some(validation_id.clone()),
            claim_id: validator.id.clone(),
            checklist_rows: packet.checklist_rows,
            source_sha: source_before.clone(),
            proof_tier,
            result_class: effective_result,
            details: json!({
                "command_id": command_id,
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "base_sha": base_sha,
            }),
            unproved_claims: unproved,
            artifacts: vec![
                EvidenceArtifactInput {
                    path: result.stdout.path.clone(),
                    kind: "command_stdout".to_owned(),
                    logical_name: format!("{}-stdout.log", result.command_id),
                    media_type: "text/plain; charset=utf-8".to_owned(),
                    sensitivity: "internal".to_owned(),
                    purpose: "validator stdout".to_owned(),
                    retention_class: "validation".to_owned(),
                },
                EvidenceArtifactInput {
                    path: result.stderr.path.clone(),
                    kind: "command_stderr".to_owned(),
                    logical_name: format!("{}-stderr.log", result.command_id),
                    media_type: "text/plain; charset=utf-8".to_owned(),
                    sensitivity: "internal".to_owned(),
                    purpose: "validator stderr".to_owned(),
                    retention_class: "validation".to_owned(),
                },
            ],
        })?;
        Ok(ValidationOutcome {
            validation_id,
            command_id,
            validator_id: validator.id,
            source_sha: source_before,
            proof_tier,
            result,
        })
    }

    fn register_command_artifact(
        &self,
        run_id: &RunId,
        attempt_id: Option<&harness_domain::AttemptId>,
        kind: &str,
        logical_name: &str,
        path: &Path,
    ) -> Result<ArtifactId, OrchestratorError> {
        let stored = self.store.artifacts().put_file(path)?;
        self.store
            .register_artifact(&NewArtifact {
                id: ArtifactId::new(),
                run_id: Some(run_id.clone()),
                task_attempt_id: attempt_id.cloned(),
                kind: kind.to_owned(),
                logical_name: logical_name.to_owned(),
                storage_path: stored.path,
                sha256: stored.digest,
                media_type: "text/plain; charset=utf-8".to_owned(),
                compression: None,
                sensitivity: "internal".to_owned(),
                byte_length: stored.byte_length,
                retention_class: "validation".to_owned(),
                pinned: false,
            })
            .map_err(Into::into)
    }

    pub fn export_evidence(
        &self,
        run_id: &RunId,
        output: &Path,
    ) -> Result<harness_evidence::BundleExport, OrchestratorError> {
        self.evidence
            .export_bundle(run_id, output)
            .map_err(Into::into)
    }

    fn persist_context(
        &self,
        run_id: &RunId,
        attempt_id: Option<&harness_domain::AttemptId>,
        role: &str,
        packet: &ContextPacket,
    ) -> Result<(), OrchestratorError> {
        self.store.record_context_packet(&NewContextPacket {
            id: ulid::Ulid::generate().to_string(),
            run_id: run_id.clone(),
            task_attempt_id: attempt_id.cloned(),
            role: role.to_owned(),
            base_sha: packet.base_sha.clone(),
            profile_digest: packet.profile_digest.clone(),
            packet: serde_json::to_value(packet)?,
            packet_sha256: packet.digest.clone(),
            estimated_tokens: packet.estimated_tokens,
            sources: packet
                .sources
                .iter()
                .map(|source| ContextSourceRecord {
                    path: source.path.clone(),
                    source_class: source.kind.clone(),
                    content_sha256: source
                        .sha256
                        .clone()
                        .unwrap_or_else(|| "unavailable".to_owned()),
                    included: source.included,
                    reason: source.reason.clone(),
                    estimated_tokens: source.bytes.div_ceil(4),
                })
                .collect(),
        })?;
        Ok(())
    }

    async fn require_runtime_ready(&self) -> Result<(), OrchestratorError> {
        let runtime = self.runtime().await?;
        let status = runtime.runtime_status().await;
        if status.state != "ready" || !status.schema_match {
            return Err(OrchestratorError::Blocked(status.detail.unwrap_or_else(
                || "Codex App Server is not execution-ready".to_owned(),
            )));
        }
        Ok(())
    }

    async fn runtime(&self) -> Result<Arc<dyn CodexRuntime>, OrchestratorError> {
        self.runtime
            .read()
            .await
            .clone()
            .ok_or_else(|| OrchestratorError::Blocked("Codex App Server is unavailable".to_owned()))
    }

    fn emit_run_event(
        &self,
        run: &RunSummary,
        event_type: &str,
        payload: Value,
    ) -> Result<(), OrchestratorError> {
        self.store.emit_domain_event(
            Some(&run.id),
            "run",
            run.id.as_str(),
            event_type,
            &payload,
            None,
        )?;
        Ok(())
    }

    fn emit_agent_event(
        &self,
        run_id: &RunId,
        agent_id: &AgentSessionId,
        event_type: &str,
        payload: Value,
    ) -> Result<(), OrchestratorError> {
        self.store.emit_domain_event(
            Some(run_id),
            "agent",
            agent_id.as_str(),
            event_type,
            &payload,
            None,
        )?;
        Ok(())
    }
}

fn architecture_packet(
    run: &RunSummary,
    profile: &RepositoryProfile,
    config: &HarnessConfig,
) -> TaskPacket {
    TaskPacket {
        schema: "nm.orchestration.task.v1".to_owned(),
        program_id: run.id.to_string(),
        task_id: "ARCHITECTURE".to_owned(),
        title: "Create implementation task graph".to_owned(),
        state: "ready".to_owned(),
        priority: "P0".to_owned(),
        execution_mode: "controller".to_owned(),
        owner_profile: "architect".to_owned(),
        reviewer_profile: "human".to_owned(),
        checklist_rows: vec![],
        authority_refs: profile.required_global_authorities.clone(),
        base_sha: run.base_sha.clone(),
        dependency_shas: BTreeMap::new(),
        depends_on: vec![],
        owned_paths: vec!["**".to_owned()],
        forbidden_paths: profile.forbidden_generated_runtime_paths.clone(),
        reserved_serial_paths: vec![],
        objective: run.objective.clone(),
        non_goals: vec!["Do not modify repository files".to_owned()],
        success_criteria: vec![
            "Schema-valid, acyclic, independently verifiable task graph".to_owned(),
        ],
        required_positive_tests: vec![],
        required_negative_tests: vec![],
        required_metrics: vec![],
        required_evidence: vec!["authority-linked plan".to_owned()],
        proof_limits: vec!["Architecture is a proposal until operator approval".to_owned()],
        diff_budget: DiffBudget { files: 0, lines: 0 },
        token_budget: config.orchestration.default_task_token_budget,
        tool_budget: None,
        lease_expires_at: "controller-managed".to_owned(),
        stop_conditions: vec!["Missing canonical authority".to_owned()],
        handoff_path: "controller://run-plan".to_owned(),
        risk_flags: vec![],
    }
}

fn worker_prompt(
    packet: &TaskPacket,
    context: &ContextPacket,
) -> Result<String, OrchestratorError> {
    Ok(format!(
        "{}\n\nAuthoritative task packet:\n{}\n\nImplement only this task. Work only in owned paths, stop on forbidden or serial-path ambiguity, run focused checks for feedback, and leave the final diff uncommitted for controller custody. Finish with a concise handoff naming changes, tests attempted, residual risks, and anything unproved.",
        context.prompt_prefix(),
        serde_json::to_string_pretty(packet)?
    ))
}

fn validate_plan(
    run: &RunSummary,
    plan: &RunPlan,
    profile: &RepositoryProfile,
) -> Result<(), OrchestratorError> {
    if plan.schema != "nm.orchestration.plan.v1" || plan.tasks.is_empty() {
        return Err(OrchestratorError::Validation(
            "plan schema must be nm.orchestration.plan.v1 and contain tasks".to_owned(),
        ));
    }
    let mut ids = BTreeSet::new();
    for packet in &plan.tasks {
        if !ids.insert(packet.task_id.clone()) {
            return Err(OrchestratorError::Validation(format!(
                "duplicate task id {}",
                packet.task_id
            )));
        }
        if packet.base_sha != run.base_sha {
            return Err(OrchestratorError::Validation(format!(
                "task {} does not use pinned base {}",
                packet.task_id, run.base_sha
            )));
        }
        if packet.owned_paths.is_empty()
            || packet.success_criteria.is_empty()
            || packet.required_evidence.is_empty()
            || packet.proof_limits.is_empty()
            || packet.token_budget == 0
            || packet.diff_budget.files == 0
            || packet.diff_budget.lines == 0
        {
            return Err(OrchestratorError::Validation(format!(
                "task {} lacks custody, criteria, evidence, proof limits, or budgets",
                packet.task_id
            )));
        }
        if packet
            .forbidden_paths
            .iter()
            .any(|path| packet.owned_paths.contains(path))
        {
            return Err(OrchestratorError::Validation(format!(
                "task {} owns an exactly forbidden path",
                packet.task_id
            )));
        }
        for path in packet
            .owned_paths
            .iter()
            .chain(packet.forbidden_paths.iter())
            .chain(packet.reserved_serial_paths.iter())
        {
            validate_repo_glob(path).map_err(OrchestratorError::Validation)?;
        }
        for reserved in &packet.reserved_serial_paths {
            if !profile.serial_paths.contains(reserved) {
                return Err(OrchestratorError::Validation(format!(
                    "task {} reserves serial path {reserved}, which is not an exact profile serial path",
                    packet.task_id
                )));
            }
            if !packet.owned_paths.contains(reserved) {
                return Err(OrchestratorError::Validation(format!(
                    "task {} reserves serial path {reserved} without owning the same bounded path",
                    packet.task_id
                )));
            }
        }
        if packet.is_high_risk() && packet.owner_profile == "worker" {
            return Err(OrchestratorError::Validation(format!(
                "high-risk task {} must use an escalated owner profile",
                packet.task_id
            )));
        }
        for authority in &packet.authority_refs {
            if authority.starts_with(".omx/") || authority.starts_with(".harness-runtime/") {
                return Err(OrchestratorError::Validation(format!(
                    "task {} treats runtime state as authority",
                    packet.task_id
                )));
            }
        }
        for (dependency, sha) in &packet.dependency_shas {
            if !packet.depends_on.contains(dependency) {
                return Err(OrchestratorError::Validation(format!(
                    "task {} supplies a SHA for non-dependency {dependency}",
                    packet.task_id
                )));
            }
            require_exact_sha(sha)?;
        }
    }
    let lookup = plan
        .tasks
        .iter()
        .map(|task| (task.task_id.as_str(), task))
        .collect::<BTreeMap<_, _>>();
    for task in &plan.tasks {
        for dependency in &task.depends_on {
            if !lookup.contains_key(dependency.as_str()) {
                return Err(OrchestratorError::Validation(format!(
                    "task {} depends on missing task {}",
                    task.task_id, dependency
                )));
            }
        }
    }
    for (index, left) in plan.tasks.iter().enumerate() {
        for right in plan.tasks.iter().skip(index + 1) {
            for left_path in &left.owned_paths {
                for right_path in &right.owned_paths {
                    if repo_globs_may_overlap(left_path, right_path) {
                        return Err(OrchestratorError::Validation(format!(
                            "task custody overlaps: {} owns {left_path}, {} owns {right_path}",
                            left.task_id, right.task_id
                        )));
                    }
                }
            }
        }
    }
    for task in &plan.tasks {
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        detect_cycle(task.task_id.as_str(), &lookup, &mut visiting, &mut visited)?;
    }
    Ok(())
}

fn detect_cycle<'a>(
    task_id: &'a str,
    tasks: &BTreeMap<&'a str, &'a TaskPacket>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> Result<(), OrchestratorError> {
    if visited.contains(task_id) {
        return Ok(());
    }
    if !visiting.insert(task_id) {
        return Err(OrchestratorError::Validation(format!(
            "task graph contains a dependency cycle at {task_id}"
        )));
    }
    if let Some(task) = tasks.get(task_id) {
        for dependency in &task.depends_on {
            detect_cycle(dependency, tasks, visiting, visited)?;
        }
    }
    visiting.remove(task_id);
    visited.insert(task_id);
    Ok(())
}

fn validate_repo_glob(value: &str) -> Result<(), String> {
    if value.trim().is_empty()
        || value.starts_with('/')
        || value.starts_with('-')
        || value.contains(['\0', '\n', '\r'])
        || value
            .split('/')
            .any(|component| component == ".." || component.is_empty())
    {
        return Err(format!("unsafe repository custody pattern: {value}"));
    }
    Ok(())
}

fn repo_globs_may_overlap(left: &str, right: &str) -> bool {
    fn prefix(value: &str) -> &str {
        value
            .split(['*', '?', '[', ']', '{', '}'])
            .next()
            .unwrap_or(value)
            .trim_end_matches('/')
    }
    let left = prefix(left);
    let right = prefix(right);
    left.is_empty()
        || right.is_empty()
        || left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn authority_digest(
    repository: &Path,
    profile: &RepositoryProfile,
) -> Result<String, OrchestratorError> {
    let mut hasher = Sha256::new();
    for path in profile
        .instruction_sources
        .iter()
        .chain(profile.required_global_authorities.iter())
    {
        let bytes = std::fs::read(repository.join(path)).map_err(|error| {
            OrchestratorError::Blocked(format!("required authority {path} is unavailable: {error}"))
        })?;
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&bytes));
        hasher.update([0]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn value_text<'a>(value: &'a Value, paths: &[&[&str]]) -> Option<&'a str> {
    paths.iter().find_map(|path| {
        path.iter()
            .try_fold(value, |current, key| current.get(*key))?
            .as_str()
    })
}

fn extract_agent_message(payload: &Value) -> Option<&str> {
    let item = payload.get("item")?;
    (item.get("type")?.as_str()? == "agentMessage")
        .then(|| item.get("text").and_then(Value::as_str))
        .flatten()
}

fn parse_json_text<T: for<'de> Deserialize<'de>>(text: &str) -> Result<T, OrchestratorError> {
    if let Ok(value) = serde_json::from_str(text.trim()) {
        return Ok(value);
    }
    let start = text.find('{').ok_or_else(|| {
        OrchestratorError::Protocol("structured response has no JSON object".to_owned())
    })?;
    let end = text.rfind('}').ok_or_else(|| {
        OrchestratorError::Protocol("structured response has no closing brace".to_owned())
    })?;
    serde_json::from_str(&text[start..=end]).map_err(Into::into)
}

fn sandbox_text(sandbox: SandboxMode) -> &'static str {
    match sandbox {
        SandboxMode::ReadOnly => "read-only",
        SandboxMode::WorkspaceWrite => "workspace-write",
    }
}

fn sandbox_policy(sandbox: SandboxMode, cwd: &Path) -> Value {
    match sandbox {
        SandboxMode::ReadOnly => json!({"type": "readOnly", "networkAccess": false}),
        SandboxMode::WorkspaceWrite => json!({
            "type": "workspaceWrite",
            "writableRoots": [cwd],
            "networkAccess": false,
            "excludeSlashTmp": true,
            "excludeTmpdirEnvVar": true
        }),
    }
}

fn approval_risk(method: &str, payload: &Value) -> RiskLevel {
    let raw = payload.to_string().to_ascii_lowercase();
    if method.contains("permissions") || raw.contains("dangerfullaccess") {
        RiskLevel::Critical
    } else if method.contains("fileChange") || raw.contains("network") {
        RiskLevel::High
    } else {
        RiskLevel::Medium
    }
}

fn verifier_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["verdict", "summary", "findings"],
        "properties": {
            "verdict": {"enum": ["accept", "changes_requested"]},
            "summary": {"type": "string"},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["severity", "file", "line", "description", "required_correction"],
                    "properties": {
                        "severity": {"type": "string"},
                        "file": {"type": ["string", "null"]},
                        "line": {"type": ["integer", "null"]},
                        "description": {"type": "string"},
                        "required_correction": {"type": "string"}
                    }
                }
            }
        }
    })
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct VerifierVerdict {
    verdict: String,
    summary: String,
    findings: Vec<VerifierFinding>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct VerifierFinding {
    severity: String,
    file: Option<String>,
    line: Option<u64>,
    description: String,
    required_correction: String,
}

fn parse_proof_tier(value: &str) -> Result<ProofTier, OrchestratorError> {
    match value {
        "T0" => Ok(ProofTier::T0),
        "T1" => Ok(ProofTier::T1),
        "T2" => Ok(ProofTier::T2),
        "T3" => Ok(ProofTier::T3),
        "T4" => Ok(ProofTier::T4),
        "T5" => Ok(ProofTier::T5),
        "T6" => Ok(ProofTier::T6),
        _ => Err(OrchestratorError::Validation(format!(
            "unknown proof tier {value}"
        ))),
    }
}

fn compact_title(objective: &str) -> String {
    let mut title = objective
        .split_whitespace()
        .take(12)
        .collect::<Vec<_>>()
        .join(" ");
    if title.chars().count() > 96 {
        title = title.chars().take(95).collect();
        title.push('…');
    }
    title
}

fn sanitize_ref(value: &str) -> String {
    let result = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    result
        .trim_matches('-')
        .to_owned()
        .chars()
        .take(48)
        .collect()
}

fn origin_matches_repository(origin: &str, repository: &str) -> bool {
    let origin = origin
        .trim()
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .to_ascii_lowercase();
    let repository = repository
        .trim_matches('/')
        .trim_end_matches(".git")
        .to_ascii_lowercase();
    origin == repository
        || origin.ends_with(&format!("/{repository}"))
        || origin.ends_with(&format!(":{repository}"))
}

fn short_id(value: &str) -> &str {
    value.get(..8).unwrap_or(value)
}

fn operation(kind: &str, target: &str) -> OperationAccepted {
    OperationAccepted {
        operation_id: format!("{}-{}", kind, ulid::Ulid::generate()),
        state: "accepted".to_owned(),
        target_id: target.to_owned(),
    }
}

fn nonempty(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_owned())
}

fn agent_state_consumes_capacity(state: &str) -> bool {
    !matches!(
        state,
        "COMPLETED" | "TURN_COMPLETE" | "FAILED" | "INTERRUPTED" | "CANCELED" | "STALLED"
    )
}

fn require_exact_sha(value: &str) -> Result<(), OrchestratorError> {
    if value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(OrchestratorError::Validation(format!(
            "expected an exact lowercase 40-character Git SHA, observed {value}"
        )))
    }
}

fn ordered_task_commits(
    tasks: &[TaskSummary],
    commits: Vec<(TaskId, String)>,
) -> Result<Vec<(TaskId, String)>, OrchestratorError> {
    let commit_by_id = commits.into_iter().collect::<BTreeMap<_, _>>();
    let mut pending = tasks
        .iter()
        .map(|task| {
            let sha = commit_by_id.get(&task.id).cloned().ok_or_else(|| {
                OrchestratorError::Blocked(format!(
                    "verified task {} has no verified commit",
                    task.external_task_id
                ))
            })?;
            Ok((
                task.external_task_id.clone(),
                (task.id.clone(), task.dependencies.clone(), sha),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, OrchestratorError>>()?;
    let mut integrated = BTreeSet::new();
    let mut ordered = Vec::with_capacity(pending.len());
    while !pending.is_empty() {
        let next = pending
            .iter()
            .find(|(_, (_, dependencies, _))| {
                dependencies
                    .iter()
                    .all(|dependency| integrated.contains(dependency))
            })
            .map(|(external_id, _)| external_id.clone())
            .ok_or_else(|| {
                OrchestratorError::Protocol(
                    "approved task dependencies could not be topologically ordered".to_owned(),
                )
            })?;
        let (task_id, _, sha) = pending.remove(&next).ok_or_else(|| {
            OrchestratorError::Protocol("integration task disappeared".to_owned())
        })?;
        require_exact_sha(&sha)?;
        integrated.insert(next);
        ordered.push((task_id, sha));
    }
    Ok(ordered)
}

fn dependency_task_commits(
    task: &TaskSummary,
    tasks: &[TaskSummary],
    commits: Vec<(TaskId, String)>,
) -> Result<Vec<(String, TaskId, String)>, OrchestratorError> {
    let task_by_external = tasks
        .iter()
        .map(|task| (task.external_task_id.clone(), task))
        .collect::<BTreeMap<_, _>>();
    let commit_by_id = commits.into_iter().collect::<BTreeMap<_, _>>();
    let mut needed = task.dependencies.iter().cloned().collect::<BTreeSet<_>>();
    let mut queue = task.dependencies.clone();
    while let Some(external_id) = queue.pop() {
        let dependency = task_by_external.get(&external_id).ok_or_else(|| {
            OrchestratorError::Protocol(format!(
                "task {} depends on missing task {external_id}",
                task.external_task_id
            ))
        })?;
        for transitive in &dependency.dependencies {
            if needed.insert(transitive.clone()) {
                queue.push(transitive.clone());
            }
        }
    }
    let mut pending = needed
        .into_iter()
        .map(|external_id| {
            let dependency = task_by_external.get(&external_id).ok_or_else(|| {
                OrchestratorError::Protocol(format!("missing dependency task {external_id}"))
            })?;
            let sha = commit_by_id.get(&dependency.id).cloned().ok_or_else(|| {
                OrchestratorError::Blocked(format!(
                    "dependency {external_id} has not produced a verified commit"
                ))
            })?;
            require_exact_sha(&sha)?;
            Ok((
                external_id,
                (dependency.id.clone(), dependency.dependencies.clone(), sha),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, OrchestratorError>>()?;
    let mut completed = BTreeSet::new();
    let mut ordered = Vec::with_capacity(pending.len());
    while !pending.is_empty() {
        let next = pending
            .iter()
            .find(|(_, (_, dependencies, _))| {
                dependencies
                    .iter()
                    .all(|dependency| completed.contains(dependency))
            })
            .map(|(external_id, _)| external_id.clone())
            .ok_or_else(|| {
                OrchestratorError::Protocol(
                    "dependency commits could not be topologically ordered".to_owned(),
                )
            })?;
        let (task_id, _, sha) = pending
            .remove(&next)
            .ok_or_else(|| OrchestratorError::Protocol("dependency disappeared".to_owned()))?;
        completed.insert(next.clone());
        ordered.push((next, task_id, sha));
    }
    Ok(ordered)
}

fn default_run_mode() -> String {
    "plan_and_implement".to_owned()
}

fn default_retry_route() -> String {
    "same".to_owned()
}

fn default_publication_mode() -> String {
    "local_only".to_owned()
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("store error: {0}")]
    Store(#[from] harness_store::StoreError),
    #[error("Git error: {0}")]
    Git(#[from] harness_git::GitError),
    #[error("Codex runtime error: {0}")]
    Codex(#[from] harness_codex::CodexError),
    #[error("context error: {0}")]
    Context(#[from] harness_context::ContextError),
    #[error("command runner error: {0}")]
    Runner(#[from] harness_runner::RunnerError),
    #[error("evidence error: {0}")]
    Evidence(#[from] harness_evidence::EvidenceError),
    #[error("profile error: {0}")]
    Profile(#[from] harness_profile::ProfileError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("state conflict: {0}")]
    Conflict(String),
    #[error("operation blocked: {0}")]
    Blocked(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_json_can_be_unwrapped_from_fence() {
        let value: Value = parse_json_text("```json\n{\"ok\":true}\n```").unwrap();
        assert_eq!(value["ok"], true);
    }

    #[test]
    fn refs_are_sanitized() {
        assert_eq!(sanitize_ref("MEDIA/001 weird"), "media-001-weird");
    }

    #[test]
    fn verifier_schema_forbids_extra_fields() {
        assert_eq!(verifier_schema()["additionalProperties"], false);
    }

    #[test]
    fn completed_and_stalled_agents_release_scheduler_capacity() {
        for state in [
            "COMPLETED",
            "TURN_COMPLETE",
            "FAILED",
            "INTERRUPTED",
            "CANCELED",
            "STALLED",
        ] {
            assert!(!agent_state_consumes_capacity(state), "state {state}");
        }
        for state in ["STARTING", "RUNNING", "WAITING_APPROVAL", "STEERED"] {
            assert!(agent_state_consumes_capacity(state), "state {state}");
        }
    }
}
