//! Immutable investigation-artifact repository.
//!
//! Artifacts are controller-produced evidence records, never browser input and
//! never a route to task creation, publication, or mutable worktree custody.

use harness_domain::{InvestigationArtifact, InvestigationArtifactId};
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const MAX_INVESTIGATION_PAGE_SIZE: u32 = 200;

impl Store {
    /// Records one fully validated immutable investigation artifact. Retrying
    /// the exact same artifact is safe; changing bytes for an existing ID is a
    /// custody conflict rather than an implicit replacement.
    pub fn record_investigation_artifact(
        &self,
        artifact: &InvestigationArtifact,
    ) -> Result<InvestigationArtifact, StoreError> {
        artifact
            .validate()
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
        InvestigationScope, InvestigationSensitivity,
    };
    use tempfile::TempDir;

    use super::*;

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
            sources: vec!["validation:fixture".to_owned()],
            findings: vec![InvestigationFinding {
                finding_id: "finding_a".to_owned(),
                classification: InvestigationFindingClassification::Confirmed,
                summary: "The source and schema use different revisions.".to_owned(),
                confidence_milli: 950,
                evidence_refs: vec!["validation:fixture".to_owned()],
                affected_refs: vec!["task:task_a".to_owned()],
                risk: AttentionSeverity::High,
                limitations: vec![],
            }],
            recommendations: vec![InvestigationRecommendation {
                recommendation_id: "recommendation_a".to_owned(),
                summary: "Use the controller-owned schema revision.".to_owned(),
                required_authority: "controller".to_owned(),
                evidence_refs: vec!["validation:fixture".to_owned()],
                alternatives: vec!["Preserve the rejected revision".to_owned()],
                risk: AttentionSeverity::High,
                next_verification: "Run the exact schema check.".to_owned(),
            }],
            decision_inventory: vec![DecisionInventoryItem {
                decision_id: "decision_a".to_owned(),
                question: "Which revision is authoritative?".to_owned(),
                state: "open".to_owned(),
                options: vec!["controller".to_owned(), "legacy".to_owned()],
                evidence_refs: vec!["validation:fixture".to_owned()],
                impact: "Blocks schema publication.".to_owned(),
                recommended_option: Some("controller".to_owned()),
                required_actor: "operator".to_owned(),
                blocking_refs: vec!["task:task_a".to_owned()],
                independent_work_can_continue: false,
            }],
            limitations: vec!["No hosted replay was available.".to_owned()],
            rejected_hypotheses: vec!["The diff changed the schema.".to_owned()],
            sensitivity: InvestigationSensitivity::Internal,
            artifact_refs: vec!["artifact:source-snapshot".to_owned()],
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
        let snapshot = store.control_plane_snapshot().expect("snapshot");
        assert_eq!(
            snapshot.investigations.state,
            harness_domain::SnapshotSectionState::Current
        );
        assert_eq!(snapshot.investigations.rows.len(), 1);
        assert_eq!(snapshot.source_cursors["investigation_artifacts"], 1);
    }
}
