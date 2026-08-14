//! Closed reconciliation and ownership-proof custody.
//!
//! This repository records exact observed inventory and exclusive-ownership
//! proofs. It does not launch, resume, delete, reset, release, or authorize an
//! attempt; those controller actions need a later source-specific consumer.

use harness_domain::{
    OwnershipProof, ReconciliationActionKind, ReconciliationActionReceipt, ReconciliationEpisode,
    ReconciliationEpisodeId, ReconciliationFinding, ReconciliationState,
};
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const MAX_RECONCILIATION_PAGE_SIZE: u32 = 200;

impl Store {
    pub fn open_reconciliation_episode(
        &self,
        episode: &ReconciliationEpisode,
    ) -> Result<ReconciliationEpisode, StoreError> {
        episode.validate().map_err(control_error)?;
        if episode.state != ReconciliationState::Open
            || episode.version != 1
            || episode.finding_count != 0
            || episode.action_count != 0
        {
            return Err(StoreError::Validation(
                "a new reconciliation episode must be open at version one with no findings or actions"
                    .to_owned(),
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

    /// Appends a controller-observed inventory finding and advances the exact
    /// reconciliation episode revision in the same transaction. A finding is
    /// evidence only: this method cannot release a lease, reset a worktree,
    /// resume a session, or authorize replacement work.
    pub fn record_reconciliation_finding(
        &self,
        finding: &ReconciliationFinding,
        expected_version: u64,
    ) -> Result<ReconciliationEpisode, StoreError> {
        finding.validate().map_err(control_error)?;
        let raw = serde_json::to_string(finding)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM reconciliation_findings WHERE episode_id=?1 AND kind=?2 AND source_event_id=?3",
                params![
                    finding.episode_id.as_str(),
                    finding_kind_name(finding.kind),
                    finding.source_event_id,
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_finding_row(existing_raw, existing_digest)?;
            if existing != *finding {
                return Err(StoreError::Conflict(
                    "reconciliation finding source event already has different content".to_owned(),
                ));
            }
            let episode = reconciliation_episode_in_transaction(&transaction, &finding.episode_id)?;
            transaction.commit()?;
            return Ok(episode);
        }
        let episode = advance_reconciliation_episode(
            &transaction,
            &finding.episode_id,
            expected_version,
            finding.observed_at_ms,
            ReconciliationCounter::Finding,
        )?;
        transaction.execute(
            "INSERT INTO reconciliation_findings(episode_id,kind,source_event_id,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                finding.episode_id.as_str(),
                finding_kind_name(finding.kind),
                finding.source_event_id,
                raw,
                payload_sha256,
                finding.observed_at_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(episode)
    }

    /// Appends an immutable reconciliation authorization/receipt. It is not
    /// an action executor. Preservation callers record this before their
    /// authority-neutral state change; the target record supplies effect
    /// proof. Fresh-attempt receipts remain rejected until a controller-owned
    /// implementation can consume exclusive ownership proof while creating
    /// the exact replacement attempt in the same transaction.
    pub fn record_reconciliation_action_receipt(
        &self,
        receipt: &ReconciliationActionReceipt,
        expected_version: u64,
    ) -> Result<ReconciliationEpisode, StoreError> {
        receipt.validate().map_err(control_error)?;
        if receipt.kind == ReconciliationActionKind::AuthorizeFreshAttempt {
            return Err(StoreError::Validation(
                "fresh-attempt reconciliation receipts require the unavailable transactional attempt-creation consumer"
                    .to_owned(),
            ));
        }
        let raw = serde_json::to_string(receipt)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM reconciliation_actions WHERE episode_id=?1 AND kind=?2 AND source_event_id=?3",
                params![
                    receipt.episode_id.as_str(),
                    action_kind_name(receipt.kind),
                    receipt.source_event_id,
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_action_receipt_row(existing_raw, existing_digest)?;
            if existing != *receipt {
                return Err(StoreError::Conflict(
                    "reconciliation action source event already has different content".to_owned(),
                ));
            }
            let episode = reconciliation_episode_in_transaction(&transaction, &receipt.episode_id)?;
            transaction.commit()?;
            return Ok(episode);
        }
        let episode = advance_reconciliation_episode(
            &transaction,
            &receipt.episode_id,
            expected_version,
            receipt.created_at_ms,
            ReconciliationCounter::Action,
        )?;
        transaction.execute(
            "INSERT INTO reconciliation_actions(episode_id,kind,source_event_id,authority_event_id,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                receipt.episode_id.as_str(),
                action_kind_name(receipt.kind),
                receipt.source_event_id,
                receipt.authority_event_id,
                raw,
                payload_sha256,
                receipt.created_at_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(episode)
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

    pub fn list_reconciliation_findings(
        &self,
        episode_id: &ReconciliationEpisodeId,
        limit: u32,
    ) -> Result<Vec<ReconciliationFinding>, StoreError> {
        self.ensure_reconciliation_page(episode_id, limit)?;
        let connection = self.connection()?;
        Ok(connection
            .prepare(
                "SELECT payload_json,payload_sha256 FROM reconciliation_findings WHERE episode_id=?1 ORDER BY created_at DESC,id DESC LIMIT ?2",
            )?
            .query_map(params![episode_id.as_str(), i64::from(limit)], |row| {
                checked_finding_row(row.get(0)?, row.get(1)?)
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_reconciliation_action_receipts(
        &self,
        episode_id: &ReconciliationEpisodeId,
        limit: u32,
    ) -> Result<Vec<ReconciliationActionReceipt>, StoreError> {
        self.ensure_reconciliation_page(episode_id, limit)?;
        let connection = self.connection()?;
        Ok(connection
            .prepare(
                "SELECT payload_json,payload_sha256 FROM reconciliation_actions WHERE episode_id=?1 ORDER BY created_at DESC,id DESC LIMIT ?2",
            )?
            .query_map(params![episode_id.as_str(), i64::from(limit)], |row| {
                checked_action_receipt_row(row.get(0)?, row.get(1)?)
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn ensure_reconciliation_page(
        &self,
        episode_id: &ReconciliationEpisodeId,
        limit: u32,
    ) -> Result<(), StoreError> {
        if limit == 0 || limit > MAX_RECONCILIATION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "reconciliation page limit must be 1..={MAX_RECONCILIATION_PAGE_SIZE}"
            )));
        }
        if self.reconciliation_episode(episode_id)?.is_none() {
            return Err(StoreError::NotFound(format!(
                "reconciliation episode {episode_id}"
            )));
        }
        Ok(())
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

fn checked_finding_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<ReconciliationFinding> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "reconciliation finding payload integrity check failed".into(),
        ));
    }
    let finding: ReconciliationFinding = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    finding.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(finding)
}

fn checked_action_receipt_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<ReconciliationActionReceipt> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "reconciliation action receipt payload integrity check failed".into(),
        ));
    }
    let receipt: ReconciliationActionReceipt = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    receipt.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(receipt)
}

#[derive(Clone, Copy)]
enum ReconciliationCounter {
    Finding,
    Action,
}

fn reconciliation_episode_in_transaction(
    transaction: &Transaction<'_>,
    episode_id: &ReconciliationEpisodeId,
) -> Result<ReconciliationEpisode, StoreError> {
    transaction
        .query_row(
            "SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes WHERE id=?1",
            [episode_id.as_str()],
            |row| checked_reconciliation_row(row.get(0)?, row.get(1)?),
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound(format!("reconciliation episode {episode_id}")))
}

fn advance_reconciliation_episode(
    transaction: &Transaction<'_>,
    episode_id: &ReconciliationEpisodeId,
    expected_version: u64,
    occurred_at_ms: i64,
    counter: ReconciliationCounter,
) -> Result<ReconciliationEpisode, StoreError> {
    let mut episode = reconciliation_episode_in_transaction(transaction, episode_id)?;
    if episode.version != expected_version {
        return Err(StoreError::Conflict(format!(
            "reconciliation episode {episode_id} has version {}, expected {expected_version}",
            episode.version
        )));
    }
    if matches!(
        episode.state,
        ReconciliationState::Resolved | ReconciliationState::Refused
    ) {
        return Err(StoreError::Conflict(
            "a terminal reconciliation episode cannot accept another finding or action receipt"
                .to_owned(),
        ));
    }
    if occurred_at_ms < episode.opened_at_ms {
        return Err(StoreError::Validation(
            "reconciliation evidence predates the episode".to_owned(),
        ));
    }
    match counter {
        ReconciliationCounter::Finding => {
            episode.finding_count = episode.finding_count.checked_add(1).ok_or_else(|| {
                StoreError::Validation("reconciliation finding count overflow".to_owned())
            })?;
        }
        ReconciliationCounter::Action => {
            episode.action_count = episode.action_count.checked_add(1).ok_or_else(|| {
                StoreError::Validation("reconciliation action count overflow".to_owned())
            })?;
        }
    }
    episode.version = episode
        .version
        .checked_add(1)
        .ok_or_else(|| StoreError::Validation("reconciliation version overflow".to_owned()))?;
    episode.updated_at_ms = episode.updated_at_ms.max(occurred_at_ms);
    episode.sha256 = episode.digest().map_err(control_error)?;
    episode.validate().map_err(control_error)?;
    let raw = serde_json::to_string(&episode)?;
    let payload_sha256 = digest(&raw);
    let changed = transaction.execute(
        "UPDATE reconciliation_episodes SET state=?1,version=?2,updated_at=?3,current_payload_json=?4,current_payload_sha256=?5 WHERE id=?6 AND version=?7",
        params![
            state_name(episode.state),
            to_i64(episode.version, "reconciliation version")?,
            episode.updated_at_ms,
            raw,
            payload_sha256,
            episode_id.as_str(),
            to_i64(expected_version, "reconciliation expected version")?,
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::Conflict(format!(
            "reconciliation episode {episode_id} changed during evidence recording"
        )));
    }
    Ok(episode)
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

fn finding_kind_name(kind: harness_domain::ReconciliationFindingKind) -> &'static str {
    match kind {
        harness_domain::ReconciliationFindingKind::LiveOwner => "live_owner",
        harness_domain::ReconciliationFindingKind::UnknownOwner => "unknown_owner",
        harness_domain::ReconciliationFindingKind::PreservedCandidate => "preserved_candidate",
        harness_domain::ReconciliationFindingKind::StaleApproval => "stale_approval",
        harness_domain::ReconciliationFindingKind::AmbiguousExternalEffect => {
            "ambiguous_external_effect"
        }
    }
}

fn action_kind_name(kind: ReconciliationActionKind) -> &'static str {
    match kind {
        ReconciliationActionKind::Preserve => "preserve",
        ReconciliationActionKind::ResumeProvenOwner => "resume_proven_owner",
        ReconciliationActionKind::InvalidateStaleApproval => "invalidate_stale_approval",
        ReconciliationActionKind::ReleaseProvenDeadLease => "release_proven_dead_lease",
        ReconciliationActionKind::AuthorizeFreshAttempt => "authorize_fresh_attempt",
        ReconciliationActionKind::OpenAttention => "open_attention",
    }
}

#[cfg(test)]
mod tests {
    use harness_domain::{
        ReconciliationActionKind, ReconciliationActionReceipt, ReconciliationEpisodeId,
        ReconciliationFinding, ReconciliationFindingKind, ReconciliationTrigger,
    };
    use serde_json::json;
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

    fn finding(episode_id: ReconciliationEpisodeId) -> ReconciliationFinding {
        let mut finding = ReconciliationFinding {
            schema: "harness.reconciliation-finding.v1".to_owned(),
            episode_id,
            kind: ReconciliationFindingKind::UnknownOwner,
            source_event_id: "event-owner-unknown".to_owned(),
            observed_at_ms: 1_100,
            payload: json!({
                "attempt_id": "attempt-01",
                "reason": "runtime session is unavailable",
            }),
            sha256: String::new(),
        };
        finding.sha256 = finding.digest().expect("finding digest");
        finding
    }

    fn preserve_receipt(episode_id: ReconciliationEpisodeId) -> ReconciliationActionReceipt {
        let mut receipt = ReconciliationActionReceipt {
            schema: "harness.reconciliation-action-receipt.v1".to_owned(),
            episode_id,
            kind: ReconciliationActionKind::Preserve,
            source_event_id: "event-preserved-worktree".to_owned(),
            authority_event_id: None,
            created_at_ms: 1_200,
            payload: json!({
                "worktree_id": "worktree-01",
                "result": "preserved",
            }),
            sha256: String::new(),
        };
        receipt.sha256 = receipt.digest().expect("receipt digest");
        receipt
    }

    #[test]
    fn reconciliation_findings_and_receipts_are_immutable_and_revision_bound() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let opened = store.open_reconciliation_episode(&episode()).expect("open");
        let finding = finding(opened.episode_id.clone());
        let after_finding = store
            .record_reconciliation_finding(&finding, opened.version)
            .expect("record finding");
        assert_eq!(after_finding.version, 2);
        assert_eq!(after_finding.finding_count, 1);
        assert_eq!(after_finding.action_count, 0);
        assert_eq!(
            store
                .record_reconciliation_finding(&finding, opened.version)
                .expect("idempotent finding"),
            after_finding
        );
        assert_eq!(
            store
                .list_reconciliation_findings(&opened.episode_id, 10)
                .expect("findings"),
            vec![finding]
        );

        let receipt = preserve_receipt(opened.episode_id.clone());
        let after_action = store
            .record_reconciliation_action_receipt(&receipt, after_finding.version)
            .expect("record receipt");
        assert_eq!(after_action.version, 3);
        assert_eq!(after_action.finding_count, 1);
        assert_eq!(after_action.action_count, 1);
        assert_eq!(
            store
                .list_reconciliation_action_receipts(&opened.episode_id, 10)
                .expect("receipts"),
            vec![receipt]
        );

        let mut conflicting = preserve_receipt(opened.episode_id.clone());
        conflicting.payload = json!({"worktree_id":"worktree-02", "result":"preserved"});
        conflicting.sha256 = conflicting.digest().expect("conflicting digest");
        assert!(matches!(
            store.record_reconciliation_action_receipt(&conflicting, after_finding.version),
            Err(StoreError::Conflict(_))
        ));
        assert_eq!(
            store
                .reconciliation_episode(&opened.episode_id)
                .expect("episode")
                .expect("present"),
            after_action
        );
    }

    #[test]
    fn fresh_attempt_receipt_is_unavailable_without_transactional_attempt_creation() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let opened = store.open_reconciliation_episode(&episode()).expect("open");
        let mut receipt = ReconciliationActionReceipt {
            schema: "harness.reconciliation-action-receipt.v1".to_owned(),
            episode_id: opened.episode_id.clone(),
            kind: ReconciliationActionKind::AuthorizeFreshAttempt,
            source_event_id: "event-fresh-attempt".to_owned(),
            authority_event_id: Some("ownership-proof-event".to_owned()),
            created_at_ms: 1_200,
            payload: json!({
                "run_id": "run-01",
                "task_id": "task-01",
                "prior_attempt_id": "attempt-01",
                "worktree_id": "worktree-01",
                "head_sha": "a".repeat(40),
                "worktree_fingerprint": "b".repeat(64),
                "lease_generation": 1,
            }),
            sha256: String::new(),
        };
        receipt.sha256 = receipt.digest().expect("receipt digest");
        assert!(matches!(
            store.record_reconciliation_action_receipt(&receipt, opened.version),
            Err(StoreError::Validation(_))
        ));
        assert_eq!(
            store
                .reconciliation_episode(&opened.episode_id)
                .expect("episode")
                .expect("present"),
            opened
        );
    }
}
