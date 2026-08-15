//! Immutable investigation-artifact repository.
//!
//! Artifacts are controller-produced evidence records, never browser input and
//! never a route to task creation, publication, or mutable worktree custody.

use harness_domain::{
    ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState,
    InvestigationArtifact, InvestigationArtifactId, InvestigationArtifactSummary,
    InvestigationFindingClassification, InvestigationSensitivity, RetentionClass, SensitivityClass,
    now_ms,
};
use harness_learning::{
    CustodyState, KnowledgeFreshness, KnowledgeItemV1, KnowledgeKind, KnowledgeReview,
    KnowledgeScope, KnowledgeState, ReceiptKind, ReviewState, SourceReceipt,
};
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{NewImprovementRevision, NewInvestigationKnowledgeCandidate, Store, StoreError};

const MAX_INVESTIGATION_PAGE_SIZE: u32 = 200;
const MAX_KNOWLEDGE_STATEMENT_BYTES: usize = 4_096;
const KNOWLEDGE_REVALIDATE_AFTER_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const KNOWLEDGE_EXPIRES_AFTER_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

impl Store {
    /// Records one fully validated immutable investigation artifact. Retrying
    /// the exact same artifact is safe; changing bytes for an existing ID is a
    /// custody conflict rather than an implicit replacement.
    pub fn record_investigation_artifact(
        &self,
        artifact: &InvestigationArtifact,
    ) -> Result<InvestigationArtifact, StoreError> {
        artifact
            .validate_new_record()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let raw = serde_json::to_string(artifact)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM investigation_artifacts WHERE id=?1",
                [artifact.artifact_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_artifact_row(existing_raw, existing_digest)?;
            if existing == *artifact {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict(format!(
                "investigation artifact {} already exists with different immutable content",
                artifact.artifact_id
            )));
        }
        transaction.execute(
            "INSERT INTO investigation_artifacts(id,run_id,task_id,base_sha,repository_state_digest,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                artifact.artifact_id.as_str(),
                artifact.run_id,
                artifact.task_id,
                artifact.base_sha,
                artifact.repository_state_digest,
                raw,
                payload_sha256,
                artifact.created_at_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(artifact.clone())
    }

    /// Creates one unreviewed, display-only knowledge candidate from a fresh
    /// confirmed investigation finding.  The caller cannot provide prose,
    /// evidence, classification, sensitivity, or a knowledge identity: those
    /// all derive from the immutable artifact.  This method never activates
    /// knowledge or writes it into task context.
    pub fn propose_knowledge_from_investigation(
        &self,
        input: &NewInvestigationKnowledgeCandidate,
    ) -> Result<crate::ImprovementRevisionRecord, StoreError> {
        let artifact = self
            .investigation_artifact(&input.artifact_id)?
            .ok_or_else(|| StoreError::NotFound(input.artifact_id.to_string()))?;
        artifact
            .validate_new_record()
            .map_err(|error| StoreError::Conflict(error.to_string()))?;
        if artifact.sha256 != input.expected_artifact_sha256 {
            return Err(StoreError::Conflict(
                "knowledge proposal artifact digest is stale".to_owned(),
            ));
        }
        let finding = artifact
            .findings
            .iter()
            .find(|finding| finding.finding_id == input.finding_id)
            .ok_or_else(|| StoreError::NotFound(input.finding_id.clone()))?;
        if finding.classification != InvestigationFindingClassification::Confirmed {
            return Err(StoreError::Conflict(
                "only confirmed investigation findings may seed knowledge candidates".to_owned(),
            ));
        }
        let statement = format!(
            "Confirmed investigation finding {}: {}",
            finding.finding_id, finding.summary
        );
        if statement.len() > MAX_KNOWLEDGE_STATEMENT_BYTES {
            return Err(StoreError::Validation(
                "derived knowledge statement exceeds the bounded candidate limit".to_owned(),
            ));
        }

        let (repository_id, run_base_sha): (String, String) = self
            .connection()?
            .query_row(
                "SELECT repository_id,base_sha FROM runs WHERE id=?1",
                [&artifact.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(artifact.run_id.clone()))?;
        if artifact.base_sha != run_base_sha {
            return Err(StoreError::Conflict(
                "investigation artifact base SHA is not the owning run base".to_owned(),
            ));
        }

        let created_at = u64::try_from(artifact.created_at_ms).map_err(|_| {
            StoreError::Validation("investigation artifact creation time is invalid".to_owned())
        })?;
        let revalidate_after = created_at
            .checked_add(KNOWLEDGE_REVALIDATE_AFTER_MS)
            .ok_or_else(|| StoreError::Validation("knowledge freshness overflow".to_owned()))?;
        let expires_at = created_at
            .checked_add(KNOWLEDGE_EXPIRES_AFTER_MS)
            .ok_or_else(|| StoreError::Validation("knowledge freshness overflow".to_owned()))?;
        let now = u64::try_from(now_ms()).unwrap_or(u64::MAX);
        if created_at > now || now >= revalidate_after {
            return Err(StoreError::Conflict(
                "investigation artifact is no longer fresh enough to seed knowledge".to_owned(),
            ));
        }

        let scope = KnowledgeScope {
            repository_id,
            task_family: input.task_family.clone(),
            model_family: input.model_family.clone(),
            runtime_class: input.runtime_class.clone(),
        };
        let identity = digest(&serde_json::to_string(&json!({
            "schema": "harness.investigation-knowledge-proposal.v1",
            "artifact_id": artifact.artifact_id.to_string(),
            "artifact_sha256": artifact.sha256.clone(),
            "finding_id": finding.finding_id.clone(),
            "scope": scope.clone(),
        }))?);
        let knowledge_id = format!("knowledge-investigation-{}", &identity[..32]);
        let sensitivity = match artifact.sensitivity {
            InvestigationSensitivity::Public => SensitivityClass::Public,
            InvestigationSensitivity::Internal => SensitivityClass::Internal,
            InvestigationSensitivity::Restricted => SensitivityClass::Restricted,
        };
        let mut item = KnowledgeItemV1 {
            schema: "harness.knowledge-item.v1".to_owned(),
            knowledge_id: knowledge_id.clone(),
            kind: KnowledgeKind::Fact,
            statement,
            scope,
            evidence: vec![SourceReceipt {
                kind: ReceiptKind::InvestigationArtifact,
                revision_id: artifact.artifact_id.to_string(),
                digest: artifact.sha256.clone(),
                split: None,
                custody: Some(CustodyState::Clean),
            }],
            confidence_milli: finding.confidence_milli,
            review: KnowledgeReview {
                state: ReviewState::Unreviewed,
                reviewer_id: None,
                reviewed_at: None,
                receipt: None,
            },
            freshness: KnowledgeFreshness {
                created_at,
                revalidate_after,
                expires_at,
            },
            contradicts: Vec::new(),
            supersedes: Vec::new(),
            state: KnowledgeState::Candidate,
            sha256: String::new(),
        };
        item.sha256 = item
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let payload = serde_json::to_value(&item)?;
        let (record, _) = self.append_improvement_revision(&NewImprovementRevision {
            id: format!("knowledge-revision-{identity}"),
            aggregate_kind: ImprovementRecordKind::Knowledge,
            aggregate_id: knowledge_id,
            schema: ImprovementSchema::KnowledgeItemV1,
            state: ImprovementState::Candidate,
            payload_sha256: digest(&serde_json::to_string(&payload)?),
            payload,
            sensitivity,
            retention_class: RetentionClass::Governance,
            export_allowed: false,
            idempotency_key: format!("knowledge:investigation:{identity}"),
            event_id: ImprovementEventId::from(format!("knowledge-event-{identity}")),
            source_raw_event_id: None,
            source_domain_event_id: None,
        })?;
        Ok(record)
    }

    pub fn investigation_artifact(
        &self,
        artifact_id: &InvestigationArtifactId,
    ) -> Result<Option<InvestigationArtifact>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT payload_json,payload_sha256 FROM investigation_artifacts WHERE id=?1",
                [artifact_id.as_str()],
                |row| checked_artifact_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Lists immutable artifacts in stable most-recent-first order. This is a
    /// read model only; recording stays behind controller/evidence authority.
    pub fn list_investigation_artifacts(
        &self,
        run_id: Option<&str>,
        task_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<InvestigationArtifact>, StoreError> {
        if limit == 0 || limit > MAX_INVESTIGATION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "investigation page limit must be 1..={MAX_INVESTIGATION_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let limit = i64::from(limit);
        let rows = match (run_id, task_id) {
            (Some(run_id), Some(task_id)) => connection
                .prepare("SELECT payload_json,payload_sha256 FROM investigation_artifacts WHERE run_id=?1 AND task_id=?2 ORDER BY created_at DESC,id DESC LIMIT ?3")?
                .query_map(params![run_id, task_id, limit], |row| checked_artifact_row(row.get(0)?, row.get(1)?))?
                .collect::<Result<Vec<_>, _>>()?,
            (Some(run_id), None) => connection
                .prepare("SELECT payload_json,payload_sha256 FROM investigation_artifacts WHERE run_id=?1 ORDER BY created_at DESC,id DESC LIMIT ?2")?
                .query_map(params![run_id, limit], |row| checked_artifact_row(row.get(0)?, row.get(1)?))?
                .collect::<Result<Vec<_>, _>>()?,
            (None, Some(task_id)) => connection
                .prepare("SELECT payload_json,payload_sha256 FROM investigation_artifacts WHERE task_id=?1 ORDER BY created_at DESC,id DESC LIMIT ?2")?
                .query_map(params![task_id, limit], |row| checked_artifact_row(row.get(0)?, row.get(1)?))?
                .collect::<Result<Vec<_>, _>>()?,
            (None, None) => connection
                .prepare("SELECT payload_json,payload_sha256 FROM investigation_artifacts ORDER BY created_at DESC,id DESC LIMIT ?1")?
                .query_map([limit], |row| checked_artifact_row(row.get(0)?, row.get(1)?))?
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }

    /// Lists compact, integrity-checked summaries for browser and snapshot
    /// projections. Callers must explicitly read one artifact by ID before
    /// receiving findings, recommendations, or other bounded evidence prose.
    pub fn list_investigation_artifact_summaries(
        &self,
        run_id: Option<&str>,
        task_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<InvestigationArtifactSummary>, StoreError> {
        self.list_investigation_artifacts(run_id, task_id, limit)
            .map(|artifacts| {
                artifacts
                    .iter()
                    .map(InvestigationArtifactSummary::from)
                    .collect()
            })
    }
}

pub(crate) fn checked_artifact_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<InvestigationArtifact> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "investigation artifact payload integrity check failed".into(),
        ));
    }
    let artifact: InvestigationArtifact = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    artifact.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(artifact)
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use harness_domain::{
        AttentionSeverity, DecisionInventoryItem, InvestigationArtifact, InvestigationArtifactId,
        InvestigationFinding, InvestigationFindingClassification, InvestigationRecommendation,
        InvestigationScope, InvestigationSensitivity, RepositoryId, RunId, now_ms,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{NewInvestigationKnowledgeCandidate, NewRepository, NewRun};

    fn artifact() -> InvestigationArtifact {
        let mut artifact = InvestigationArtifact {
            schema: "harness.investigation-artifact.v1".to_owned(),
            artifact_id: InvestigationArtifactId::new(),
            run_id: "run_a".to_owned(),
            task_id: "task_a".to_owned(),
            attempt_id: "attempt_a".to_owned(),
            question: "Why is the contract rejected?".to_owned(),
            scope: InvestigationScope {
                owned_read_paths: vec!["crates/harness-store/**".to_owned()],
                forbidden_paths: vec![".git/objects/**".to_owned()],
                time_budget_ms: 60_000,
                token_budget: 8_000,
            },
            base_sha: "a".repeat(40),
            repository_state_digest: "b".repeat(64),
            methods: vec!["read source and focused tests".to_owned()],
            sources: vec![format!("context:{}", "b".repeat(64))],
            findings: vec![InvestigationFinding {
                finding_id: "finding_a".to_owned(),
                classification: InvestigationFindingClassification::Confirmed,
                summary: "The source and schema use different revisions.".to_owned(),
                confidence_milli: 950,
                evidence_refs: vec![format!("context:{}", "b".repeat(64))],
                affected_refs: vec!["task:task_a".to_owned()],
                risk: AttentionSeverity::High,
                limitations: vec![],
            }],
            recommendations: vec![InvestigationRecommendation {
                recommendation_id: "recommendation_a".to_owned(),
                summary: "Use the controller-owned schema revision.".to_owned(),
                required_authority: "controller".to_owned(),
                evidence_refs: vec![format!("context:{}", "b".repeat(64))],
                alternatives: vec!["Preserve the rejected revision".to_owned()],
                risk: AttentionSeverity::High,
                next_verification: "Run the exact schema check.".to_owned(),
            }],
            decision_inventory: vec![DecisionInventoryItem {
                decision_id: "decision_a".to_owned(),
                question: "Which revision is authoritative?".to_owned(),
                state: "open".to_owned(),
                options: vec!["controller".to_owned(), "legacy".to_owned()],
                evidence_refs: vec![format!("context:{}", "b".repeat(64))],
                impact: "Blocks schema publication.".to_owned(),
                recommended_option: Some("controller".to_owned()),
                required_actor: "operator".to_owned(),
                blocking_refs: vec!["task:task_a".to_owned()],
                independent_work_can_continue: false,
            }],
            limitations: vec!["No hosted replay was available.".to_owned()],
            rejected_hypotheses: vec!["The diff changed the schema.".to_owned()],
            sensitivity: InvestigationSensitivity::Internal,
            artifact_refs: vec![],
            created_at_ms: 1,
            sha256: String::new(),
        };
        artifact.sha256 = artifact.digest().expect("digest");
        artifact
    }

    #[test]
    fn artifacts_are_idempotent_immutable_and_digest_checked() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let artifact = artifact();
        assert_eq!(
            store
                .record_investigation_artifact(&artifact)
                .expect("first"),
            artifact
        );
        assert_eq!(
            store
                .record_investigation_artifact(&artifact)
                .expect("retry"),
            artifact
        );
        let mut changed = artifact.clone();
        changed.question = "Different question".to_owned();
        changed.sha256 = changed.digest().expect("digest");
        assert!(matches!(
            store.record_investigation_artifact(&changed),
            Err(StoreError::Conflict(_))
        ));
        assert_eq!(
            store
                .list_investigation_artifacts(Some("run_a"), Some("task_a"), 10)
                .expect("list"),
            vec![artifact]
        );
        assert_eq!(
            store
                .list_investigation_artifact_summaries(Some("run_a"), Some("task_a"), 10)
                .expect("summary list")[0]
                .finding_count,
            1
        );
        let snapshot = store.control_plane_snapshot().expect("snapshot");
        assert_eq!(
            snapshot.investigations.state,
            harness_domain::SnapshotSectionState::Current
        );
        assert_eq!(snapshot.investigations.rows.len(), 1);
        assert_eq!(
            snapshot.investigations.rows[0]["schema"],
            "harness.investigation-artifact-summary.v1"
        );
        assert!(snapshot.investigations.rows[0].get("findings").is_none());
        assert_eq!(snapshot.source_cursors["investigation_artifacts"], 1);
    }

    #[test]
    fn v1_artifact_reads_preserve_pre_ceiling_scope_and_empty_evidence() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let mut artifact = artifact();
        artifact.scope.time_budget_ms = 48 * 60 * 60 * 1_000;
        artifact.scope.token_budget = 200_000_000;
        artifact.findings[0].evidence_refs.clear();
        artifact.recommendations[0].evidence_refs.clear();
        artifact.decision_inventory[0].evidence_refs.clear();
        artifact.sha256 = artifact.digest().expect("legacy artifact digest");

        assert!(
            store.record_investigation_artifact(&artifact).is_err(),
            "new artifact intake must not weaken its current evidence/scope contract"
        );
        let raw = serde_json::to_string(&artifact).expect("legacy artifact serializes");
        let raw_digest = digest(&raw);
        store
            .connection()
            .expect("connection")
            .execute(
                "INSERT INTO investigation_artifacts(id,run_id,task_id,base_sha,repository_state_digest,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    artifact.artifact_id.as_str(),
                    artifact.run_id,
                    artifact.task_id,
                    artifact.base_sha,
                    artifact.repository_state_digest,
                    raw,
                    raw_digest,
                    artifact.created_at_ms,
                ],
            )
            .expect("immutable legacy migration row inserts");
        assert_eq!(
            store
                .investigation_artifact(&artifact.artifact_id)
                .expect("artifact rereads"),
            Some(artifact),
            "integrity-checked immutable reads retain the v1 contract"
        );
    }

    #[test]
    fn confirmed_fresh_investigation_finding_proposes_candidate_only() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let repository_id = RepositoryId::from("repository_a");
        store
            .create_repository(&NewRepository {
                id: repository_id.clone(),
                profile_id: "fixture".to_owned(),
                profile_version: 1,
                display_name: "Investigation knowledge fixture".to_owned(),
                root_path: temp.path().join("checkout"),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "READY".to_owned(),
            })
            .expect("repository");
        store
            .create_run(&NewRun {
                id: RunId::from("run_a"),
                repository_id: repository_id.clone(),
                title: "Investigation knowledge fixture".to_owned(),
                objective: "Prove knowledge remains an unreviewed suggestion".to_owned(),
                mode: "observe_only".to_owned(),
                publication_mode: "none".to_owned(),
                state: "CREATED".to_owned(),
                phase: "created".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: "a".repeat(40),
                authority_digest: "c".repeat(64),
                profile_digest: "d".repeat(64),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".to_owned(),
                token_budget: None,
            })
            .expect("run");
        let mut artifact = artifact();
        artifact.created_at_ms = now_ms();
        artifact.sha256 = artifact.digest().expect("fresh artifact digest");
        store
            .record_investigation_artifact(&artifact)
            .expect("artifact");
        let input = NewInvestigationKnowledgeCandidate {
            artifact_id: artifact.artifact_id.clone(),
            expected_artifact_sha256: artifact.sha256.clone(),
            finding_id: "finding_a".to_owned(),
            task_family: "operator_control".to_owned(),
            model_family: None,
            runtime_class: None,
        };
        let record = store
            .propose_knowledge_from_investigation(&input)
            .expect("candidate");
        let item: KnowledgeItemV1 = serde_json::from_value(record.payload.clone()).expect("wire");
        assert_eq!(record.state, ImprovementState::Candidate);
        assert_eq!(item.state, KnowledgeState::Candidate);
        assert_eq!(item.review.state, ReviewState::Unreviewed);
        assert_eq!(item.kind, KnowledgeKind::Fact);
        assert_eq!(item.scope.repository_id, repository_id.as_str());
        assert_eq!(item.evidence.len(), 1);
        assert_eq!(item.evidence[0].kind, ReceiptKind::InvestigationArtifact);
        assert_eq!(item.evidence[0].revision_id, artifact.artifact_id.as_str());
        assert_eq!(item.evidence[0].digest, artifact.sha256);
        assert!(
            store
                .resolved_active_knowledge(&repository_id, "operator_control", 0)
                .expect("display projection")
                .is_empty()
        );
        assert_eq!(
            store
                .propose_knowledge_from_investigation(&input)
                .expect("idempotent candidate")
                .id,
            record.id
        );

        let mut stale = input.clone();
        stale.expected_artifact_sha256 = "e".repeat(64);
        assert!(matches!(
            store.propose_knowledge_from_investigation(&stale),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn nonconfirmed_or_legacy_investigation_cannot_seed_knowledge() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let repository_id = RepositoryId::from("repository_a");
        store
            .create_repository(&NewRepository {
                id: repository_id.clone(),
                profile_id: "fixture".to_owned(),
                profile_version: 1,
                display_name: "Investigation knowledge fixture".to_owned(),
                root_path: temp.path().join("checkout"),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "READY".to_owned(),
            })
            .expect("repository");
        store
            .create_run(&NewRun {
                id: RunId::from("run_a"),
                repository_id,
                title: "Investigation knowledge fixture".to_owned(),
                objective: "Reject unadmitted sources".to_owned(),
                mode: "observe_only".to_owned(),
                publication_mode: "none".to_owned(),
                state: "CREATED".to_owned(),
                phase: "created".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: "a".repeat(40),
                authority_digest: "c".repeat(64),
                profile_digest: "d".repeat(64),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".to_owned(),
                token_budget: None,
            })
            .expect("run");

        let mut unconfirmed = artifact();
        unconfirmed.created_at_ms = now_ms();
        unconfirmed.findings[0].classification = InvestigationFindingClassification::Supported;
        unconfirmed.sha256 = unconfirmed.digest().expect("unconfirmed digest");
        store
            .record_investigation_artifact(&unconfirmed)
            .expect("artifact");
        let candidate = NewInvestigationKnowledgeCandidate {
            artifact_id: unconfirmed.artifact_id.clone(),
            expected_artifact_sha256: unconfirmed.sha256.clone(),
            finding_id: "finding_a".to_owned(),
            task_family: "operator_control".to_owned(),
            model_family: None,
            runtime_class: None,
        };
        assert!(matches!(
            store.propose_knowledge_from_investigation(&candidate),
            Err(StoreError::Conflict(_))
        ));

        let mut legacy = artifact();
        legacy.created_at_ms = now_ms();
        legacy.scope.time_budget_ms = 48 * 60 * 60 * 1_000;
        legacy.sha256 = legacy.digest().expect("legacy digest");
        let raw = serde_json::to_string(&legacy).expect("legacy wire");
        store
            .connection()
            .expect("connection")
            .execute(
                "INSERT INTO investigation_artifacts(id,run_id,task_id,base_sha,repository_state_digest,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    legacy.artifact_id.as_str(),
                    legacy.run_id,
                    legacy.task_id,
                    legacy.base_sha,
                    legacy.repository_state_digest,
                    raw.clone(),
                    digest(&raw),
                    legacy.created_at_ms,
                ],
            )
            .expect("legacy artifact row");
        assert!(matches!(
            store.propose_knowledge_from_investigation(&NewInvestigationKnowledgeCandidate {
                artifact_id: legacy.artifact_id,
                expected_artifact_sha256: legacy.sha256,
                finding_id: "finding_a".to_owned(),
                task_family: "operator_control".to_owned(),
                model_family: None,
                runtime_class: None,
            }),
            Err(StoreError::Conflict(_))
        ));
    }
}
