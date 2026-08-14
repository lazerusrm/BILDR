//! Closed reconciliation and ownership-proof custody.
//!
//! This repository records exact observed inventory and exclusive-ownership
//! proofs. It does not launch, resume, delete, reset, release, or authorize an
//! attempt; those controller actions need a later source-specific consumer.

use harness_domain::{
    OwnershipProof, ReconciliationEpisode, ReconciliationEpisodeId, ReconciliationState,
};
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const MAX_RECONCILIATION_PAGE_SIZE: u32 = 200;

impl Store {
    pub fn open_reconciliation_episode(
        &self,
        episode: &ReconciliationEpisode,
    ) -> Result<ReconciliationEpisode, StoreError> {
        episode.validate().map_err(control_error)?;
        if episode.state != ReconciliationState::Open || episode.version != 1 {
            return Err(StoreError::Validation(
                "a new reconciliation episode must be open at version one".to_owned(),
            ));
        }
        let raw = serde_json::to_string(episode)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes WHERE id=?1",
                [episode.episode_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_reconciliation_row(existing_raw, existing_digest)?;
            if existing == *episode {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict(format!(
                "reconciliation episode {} already has different content",
                episode.episode_id
            )));
        }
        let existing_active: Option<String> = transaction
            .query_row(
                "SELECT id FROM reconciliation_episodes WHERE run_id IS ?1 AND trigger_kind=?2 AND state IN ('open','claimed','awaiting_evidence') LIMIT 1",
                params![episode.run_id, trigger_name(episode.trigger_kind)],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing_active {
            return Err(StoreError::Conflict(format!(
                "an active reconciliation episode already owns this trigger: {existing}"
            )));
        }
        transaction.execute(
            "INSERT INTO reconciliation_episodes(id,run_id,trigger_kind,state,version,opened_at,updated_at,current_payload_json,current_payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                episode.episode_id.as_str(),
                episode.run_id,
                trigger_name(episode.trigger_kind),
                state_name(episode.state),
                to_i64(episode.version, "reconciliation version")?,
                episode.opened_at_ms,
                episode.updated_at_ms,
                raw,
                payload_sha256,
            ],
        )?;
        transaction.commit()?;
        Ok(episode.clone())
    }

    /// Stores a fully exclusive proof append-only. The proof is evidence only:
    /// a caller must separately consume it in a future transactional controller
    /// action before it can authorize replacement work.
    pub fn record_ownership_proof(
        &self,
        proof: &OwnershipProof,
    ) -> Result<OwnershipProof, StoreError> {
        proof.validate().map_err(control_error)?;
        let raw = serde_json::to_string(proof)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM ownership_proofs WHERE source_event_id=?1",
                [proof.source_event_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_ownership_row(existing_raw, existing_digest)?;
            if existing == *proof {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict(
                "ownership proof source event already has different content".to_owned(),
            ));
        }
        transaction.execute(
            "INSERT INTO ownership_proofs(id,run_id,task_id,attempt_id,source_event_id,payload_json,payload_sha256,observed_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                proof.proof_id.as_str(), proof.run_id, proof.task_id, proof.prior_attempt_id,
                proof.source_event_id, raw, payload_sha256, proof.expires_at_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(proof.clone())
    }

    pub fn reconciliation_episode(
        &self,
        episode_id: &ReconciliationEpisodeId,
    ) -> Result<Option<ReconciliationEpisode>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes WHERE id=?1",
                [episode_id.as_str()],
                |row| checked_reconciliation_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_reconciliation_episodes(
        &self,
        run_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ReconciliationEpisode>, StoreError> {
        if limit == 0 || limit > MAX_RECONCILIATION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "reconciliation page limit must be 1..={MAX_RECONCILIATION_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let mut statement = if run_id.is_some() {
            connection.prepare("SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes WHERE run_id=?1 ORDER BY updated_at DESC,id DESC LIMIT ?2")?
        } else {
            connection.prepare("SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes ORDER BY updated_at DESC,id DESC LIMIT ?1")?
        };
        if let Some(run_id) = run_id {
            let rows = statement.query_map(params![run_id, i64::from(limit)], |row| {
                checked_reconciliation_row(row.get(0)?, row.get(1)?)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        } else {
            let rows = statement.query_map([i64::from(limit)], |row| {
                checked_reconciliation_row(row.get(0)?, row.get(1)?)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }
    }
}

pub(crate) fn checked_reconciliation_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<ReconciliationEpisode> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "reconciliation payload integrity check failed".into(),
        ));
    }
    let episode: ReconciliationEpisode = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    episode.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(episode)
}

fn checked_ownership_row(raw: String, payload_sha256: String) -> rusqlite::Result<OwnershipProof> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "ownership proof payload integrity check failed".into(),
        ));
    }
    let proof: OwnershipProof = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    proof.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(proof)
}

fn control_error(error: harness_domain::OperatorControlError) -> StoreError {
    StoreError::Validation(error.to_string())
}
fn to_i64(value: u64, field: &str) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::Validation(format!("{field} exceeds SQLite integer range")))
}
fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

fn trigger_name(trigger: harness_domain::ReconciliationTrigger) -> &'static str {
    match trigger {
        harness_domain::ReconciliationTrigger::DaemonRestart => "daemon_restart",
        harness_domain::ReconciliationTrigger::AppServerLoss => "app_server_loss",
        harness_domain::ReconciliationTrigger::ProcessLoss => "process_loss",
        harness_domain::ReconciliationTrigger::VersionTransition => "version_transition",
        harness_domain::ReconciliationTrigger::AccountHandoff => "account_handoff",
        harness_domain::ReconciliationTrigger::WorktreeMismatch => "worktree_mismatch",
        harness_domain::ReconciliationTrigger::UncertainCommandCompletion => {
            "uncertain_command_completion"
        }
    }
}
fn state_name(state: ReconciliationState) -> &'static str {
    match state {
        ReconciliationState::Open => "open",
        ReconciliationState::Claimed => "claimed",
        ReconciliationState::AwaitingEvidence => "awaiting_evidence",
        ReconciliationState::Resolved => "resolved",
        ReconciliationState::Refused => "refused",
    }
}

#[cfg(test)]
mod tests {
    use harness_domain::{ReconciliationEpisodeId, ReconciliationTrigger};
    use tempfile::TempDir;

    use super::*;

    fn episode() -> ReconciliationEpisode {
        let mut episode = ReconciliationEpisode {
            schema: "harness.reconciliation-episode.v1".to_owned(),
            episode_id: ReconciliationEpisodeId::new(),
            run_id: None,
            trigger_kind: ReconciliationTrigger::AppServerLoss,
            state: ReconciliationState::Open,
            version: 1,
            opened_at_ms: 1_000,
            updated_at_ms: 1_000,
            source_event_id: "event-app-server-loss".to_owned(),
            inventory_sha256: "a".repeat(64),
            finding_count: 0,
            action_count: 0,
            report: None,
            sha256: String::new(),
        };
        episode.sha256 = episode.digest().expect("digest");
        episode
    }

    #[test]
    fn open_reconciliation_is_idempotent_and_exclusive_per_active_trigger() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let first = episode();
        assert_eq!(store.open_reconciliation_episode(&first).unwrap(), first);
        assert_eq!(store.open_reconciliation_episode(&first).unwrap(), first);
        let second = episode();
        assert!(matches!(
            store.open_reconciliation_episode(&second),
            Err(StoreError::Conflict(_))
        ));
    }
}
