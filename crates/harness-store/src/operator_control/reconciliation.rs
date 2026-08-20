//! Closed reconciliation and ownership-proof custody.
//!
//! This repository records exact observed inventory and exclusive-ownership
//! proofs. It can atomically consume an already-recorded proof while
//! authorizing one exact replacement attempt; it does not itself launch,
//! resume, delete, reset, or release mutable controller resources.

use harness_domain::{
    CorrelationLink, CorrelationLinkId, OwnershipProof, OwnershipProofId, ReconciliationActionKind,
    ReconciliationActionReceipt, ReconciliationEpisode, ReconciliationEpisodeId,
    ReconciliationFinding, ReconciliationState, ReconciliationTrigger, TaskId, TaskState,
    TraceContext, now_ms,
};
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

use crate::{NewTaskAttempt, Store, StoreError};

use super::correlation::record_correlation_link_in_transaction;

const MAX_RECONCILIATION_PAGE_SIZE: u32 = 200;
const FRESH_ATTEMPT_AUTHORIZED_STATE: &str = "AUTHORIZED";

impl Store {
    /// Returns the one replacement attempt whose exclusive-ownership proof
    /// was consumed for this task but which the scheduler has not yet leased.
    /// The packet is revalidated before it can cross the scheduler boundary.
    pub fn authorized_fresh_attempt(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<NewTaskAttempt>, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT a.id,a.task_id,a.attempt_number,a.state,a.task_packet_json,a.task_packet_sha256,a.base_sha,a.requested_model_route FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN reconciliation_proof_consumptions c ON c.replacement_attempt_id=a.id WHERE a.task_id=?1 AND a.state='AUTHORIZED' AND t.state='READY' AND t.current_attempt_number=a.attempt_number",
                [task_id.as_str()],
                |row| {
                    let packet_raw: String = row.get(4)?;
                    let packet: harness_domain::TaskPacket = serde_json::from_str(&packet_raw)
                        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(
                            packet_raw.len(),
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        ))?;
                    let packet_sha256: String = row.get(5)?;
                    if digest(&packet_raw) != packet_sha256 {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            "authorized fresh-attempt packet integrity check failed".into(),
                        ));
                    }
                    packet.validate_execution_contract().map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok(NewTaskAttempt {
                        id: harness_domain::AttemptId::from(row.get::<_, String>(0)?),
                        task_id: TaskId::from(row.get::<_, String>(1)?),
                        attempt_number: row.get::<_, i64>(2)? as u32,
                        state: row.get(3)?,
                        packet,
                        packet_sha256,
                        base_sha: row.get(6)?,
                        requested_model_route: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Leases exactly an authorization materialized by
    /// [`Self::consume_ownership_proof_for_fresh_attempt`]. This is separate
    /// from proof consumption because the scheduler must still perform local
    /// runtime preflight before it creates a mutable worktree.
    pub fn lease_authorized_fresh_attempt(
        &self,
        task_id: &TaskId,
        attempt_id: &harness_domain::AttemptId,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let changed_attempt = transaction.execute(
            "UPDATE task_attempts SET state='LEASED',updated_at=?3,version=version+1 WHERE id=?1 AND task_id=?2 AND state='AUTHORIZED' AND EXISTS(SELECT 1 FROM reconciliation_proof_consumptions WHERE replacement_attempt_id=?1)",
            params![attempt_id.as_str(), task_id.as_str(), now_ms()],
        )?;
        if changed_attempt != 1 {
            return Err(StoreError::Conflict(format!(
                "fresh attempt {attempt_id} is no longer authorized for leasing"
            )));
        }
        let changed_task = transaction.execute(
            "UPDATE tasks SET state='LEASED',failure_reason=NULL,updated_at=?3,version=version+1 WHERE id=?1 AND state='READY' AND current_attempt_number=(SELECT attempt_number FROM task_attempts WHERE id=?2)",
            params![task_id.as_str(), attempt_id.as_str(), now_ms()],
        )?;
        if changed_task != 1 {
            return Err(StoreError::Conflict(format!(
                "task {task_id} is no longer ready for its authorized fresh attempt"
            )));
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn open_reconciliation_episode(
        &self,
        episode: &ReconciliationEpisode,
    ) -> Result<ReconciliationEpisode, StoreError> {
        episode.validate().map_err(control_error)?;
        let correlation = reconciliation_episode_correlation_link(episode)?;
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
                record_correlation_link_in_transaction(&transaction, &correlation)?;
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
        record_correlation_link_in_transaction(&transaction, &correlation)?;
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
        let correlation = ownership_proof_correlation_link(proof)?;
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
                record_correlation_link_in_transaction(&transaction, &correlation)?;
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
                proof.source_event_id, raw, payload_sha256, proof.observed_at_ms,
            ],
        )?;
        record_correlation_link_in_transaction(&transaction, &correlation)?;
        transaction.commit()?;
        Ok(proof.clone())
    }

    /// Atomically consumes one exclusive ownership proof and materializes the
    /// exact next attempt that the scheduler may launch.  The attempt starts
    /// in `AUTHORIZED`, not `LEASED`: no worktree, agent, or mutable lease is
    /// created by this method.  The scheduler can only consume the attempt
    /// after the task is returned to `READY` in the same transaction.
    ///
    /// This is intentionally the only store boundary that accepts
    /// `AuthorizeFreshAttempt`.  It prevents a receipt, proof, task state, and
    /// replacement attempt from becoming durable in different crash windows.
    pub fn consume_ownership_proof_for_fresh_attempt(
        &self,
        proof_id: &OwnershipProofId,
        receipt: &ReconciliationActionReceipt,
        expected_episode_version: u64,
        replacement: &NewTaskAttempt,
        expected_task_version: u64,
    ) -> Result<(), StoreError> {
        receipt.validate().map_err(control_error)?;
        let correlation = reconciliation_action_correlation_link(receipt)?;
        if receipt.kind != ReconciliationActionKind::AuthorizeFreshAttempt {
            return Err(StoreError::Validation(
                "ownership-proof consumption requires an authorize_fresh_attempt receipt"
                    .to_owned(),
            ));
        }
        if replacement.state != FRESH_ATTEMPT_AUTHORIZED_STATE {
            return Err(StoreError::Validation(format!(
                "fresh replacement attempt must begin in {FRESH_ATTEMPT_AUTHORIZED_STATE}"
            )));
        }
        replacement
            .packet
            .validate_execution_contract()
            .map_err(|error| {
                StoreError::Validation(format!("fresh replacement packet is invalid: {error}"))
            })?;
        let packet_json = serde_json::to_string(&replacement.packet)?;
        if digest(&packet_json) != replacement.packet_sha256 {
            return Err(StoreError::Validation(
                "fresh replacement packet sha256 does not match its canonical payload".to_owned(),
            ));
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let (proof_raw, proof_digest) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM ownership_proofs WHERE id=?1",
                [proof_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("ownership proof {proof_id}")))?;
        let proof = checked_ownership_row(proof_raw, proof_digest)?;
        let existing_consumption: Option<(
            String,
            String,
            String,
            String,
            i64,
            String,
            String,
            String,
            String,
            String,
        )> = transaction
            .query_row(
                "SELECT c.replacement_attempt_id,c.task_id,a.payload_json,a.payload_sha256,ta.attempt_number,ta.state,ta.task_packet_json,ta.task_packet_sha256,ta.base_sha,ta.requested_model_route FROM reconciliation_proof_consumptions c JOIN reconciliation_actions a ON a.id=c.action_id JOIN task_attempts ta ON ta.id=c.replacement_attempt_id AND ta.task_id=c.task_id WHERE c.proof_id=?1",
                [proof_id.as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                    ))
                },
            )
            .optional()?;
        if let Some((
            existing_attempt_id,
            existing_task_id,
            existing_raw,
            existing_digest,
            existing_number,
            _existing_state,
            existing_packet_json,
            existing_packet_sha256,
            existing_base_sha,
            existing_model_route,
        )) = existing_consumption
        {
            if existing_attempt_id == replacement.id.as_str()
                && existing_task_id == replacement.task_id.as_str()
                && existing_number == i64::from(replacement.attempt_number)
                && existing_packet_json == packet_json
                && existing_packet_sha256 == replacement.packet_sha256
                && existing_base_sha == replacement.base_sha
                && existing_model_route == replacement.requested_model_route
                && checked_action_receipt_row(existing_raw, existing_digest)? == *receipt
            {
                record_correlation_link_in_transaction(&transaction, &correlation)?;
                transaction.commit()?;
                return Ok(());
            }
            return Err(StoreError::Conflict(format!(
                "ownership proof {proof_id} was already consumed by replacement attempt {existing_attempt_id}"
            )));
        }
        let now = now_ms();
        if ownership_proof_expired(&proof, now) {
            return Err(StoreError::Conflict(format!(
                "ownership proof {proof_id} expired before replacement authorization"
            )));
        }

        let (task_run_id, task_external_id, task_state_raw, task_attempt_number, task_version) =
            transaction
            .query_row(
                "SELECT run_id,external_task_id,state,current_attempt_number,version FROM tasks WHERE id=?1",
                [replacement.task_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("task {}", replacement.task_id)))?;
        let task_state: TaskState = task_state_raw.parse().map_err(|error| {
            StoreError::Validation(format!(
                "task {} has invalid state: {error}",
                replacement.task_id
            ))
        })?;
        if task_version != i64::try_from(expected_task_version).unwrap_or(i64::MAX) {
            return Err(StoreError::Conflict(format!(
                "task {} changed before ownership-proof consumption",
                replacement.task_id
            )));
        }
        if !task_state.can_transition_to(TaskState::Ready) {
            return Err(StoreError::Conflict(format!(
                "task {} in {task_state} cannot accept a fresh authorized attempt",
                replacement.task_id
            )));
        }
        if proof.run_id != task_run_id
            || proof.task_id != replacement.task_id.as_str()
            || replacement.packet.task_id.is_empty()
            || replacement.packet.task_id != task_external_id
        {
            return Err(StoreError::Validation(
                "ownership proof, replacement task, and replacement packet must bind the same task/run identity"
                    .to_owned(),
            ));
        }
        if replacement.base_sha != proof.head_sha
            || replacement.packet.base_sha != replacement.base_sha
        {
            return Err(StoreError::Validation(
                "replacement packet/base must be pinned to the proven preserved head".to_owned(),
            ));
        }
        let expected_attempt_number = task_attempt_number
            .checked_add(1)
            .ok_or_else(|| StoreError::Validation("task attempt number overflow".to_owned()))?;
        if replacement.attempt_number != u32::try_from(expected_attempt_number).unwrap_or(u32::MAX)
        {
            return Err(StoreError::Conflict(format!(
                "replacement attempt number {} does not follow task attempt {}",
                replacement.attempt_number, task_attempt_number
            )));
        }

        let (prior_number, prior_state, prior_head): (i64, String, Option<String>) = transaction
            .query_row(
                "SELECT attempt_number,state,head_sha FROM task_attempts WHERE id=?1 AND task_id=?2",
                params![proof.prior_attempt_id, replacement.task_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("prior attempt {}", proof.prior_attempt_id)))?;
        if prior_number != task_attempt_number
            || !matches!(prior_state.as_str(), "FAILED" | "INTERRUPTED" | "STALLED")
            || prior_head.as_deref() != Some(proof.head_sha.as_str())
        {
            return Err(StoreError::Conflict(
                "ownership proof no longer matches a terminal preserved prior attempt".to_owned(),
            ));
        }
        let (worktree_attempt_id, worktree_run_id, worktree_head, worktree_state): (
            Option<String>,
            String,
            Option<String>,
            String,
        ) = transaction
            .query_row(
                "SELECT task_attempt_id,run_id,head_sha,state FROM worktrees WHERE id=?1",
                [proof.worktree_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::NotFound(format!("proven worktree {}", proof.worktree_id))
            })?;
        if worktree_attempt_id.as_deref() != Some(proof.prior_attempt_id.as_str())
            || worktree_run_id != proof.run_id
            || worktree_head.as_deref() != Some(proof.head_sha.as_str())
            || worktree_state != "PRESERVED"
        {
            return Err(StoreError::Conflict(
                "ownership proof no longer matches its preserved worktree".to_owned(),
            ));
        }
        let active_path_lease: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM path_leases WHERE task_attempt_id=?1 AND released_at IS NULL)",
            [proof.prior_attempt_id.as_str()],
            |row| row.get(0),
        )?;
        if active_path_lease {
            return Err(StoreError::Conflict(
                "ownership proof cannot authorize a replacement while the prior path lease remains active"
                    .to_owned(),
            ));
        }
        let active_agent: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_sessions WHERE task_attempt_id=?1 AND state NOT IN ('COMPLETED','FAILED','CANCELED','CANCELLED','SHUTDOWN','TERMINATED','TURN_COMPLETE','STALLED','INTERRUPTED'))",
            [proof.prior_attempt_id.as_str()],
            |row| row.get(0),
        )?;
        if active_agent {
            return Err(StoreError::Conflict(
                "ownership proof cannot authorize a replacement while the prior agent remains nonterminal"
                    .to_owned(),
            ));
        }
        let recorded_command: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM command_runs WHERE task_attempt_id=?1)",
            [proof.prior_attempt_id.as_str()],
            |row| row.get(0),
        )?;
        if recorded_command {
            return Err(StoreError::Conflict(
                "ownership proof cannot authorize a replacement while prior command effects remain outside the reconciled proof boundary"
                    .to_owned(),
            ));
        }

        let episode = reconciliation_episode_in_transaction(&transaction, &receipt.episode_id)?;
        if episode.run_id.as_deref() != Some(proof.run_id.as_str())
            || matches!(
                episode.state,
                ReconciliationState::Resolved | ReconciliationState::Refused
            )
        {
            return Err(StoreError::Conflict(
                "reconciliation episode is not active for the ownership proof run".to_owned(),
            ));
        }
        if receipt.authority_event_id.as_deref() != Some(proof.source_event_id.as_str())
            || receipt_payload_string(&receipt.payload, "proof_id")? != proof.proof_id.as_str()
            || receipt_payload_string(&receipt.payload, "run_id")? != proof.run_id
            || receipt_payload_string(&receipt.payload, "task_id")? != proof.task_id
            || receipt_payload_string(&receipt.payload, "prior_attempt_id")?
                != proof.prior_attempt_id
            || receipt_payload_string(&receipt.payload, "worktree_id")? != proof.worktree_id
            || receipt_payload_string(&receipt.payload, "head_sha")? != proof.head_sha
            || receipt_payload_string(&receipt.payload, "worktree_fingerprint")?
                != proof.worktree_fingerprint
            || receipt_payload_u64(&receipt.payload, "lease_generation")? != proof.lease_generation
            || receipt_payload_string(&receipt.payload, "replacement_attempt_id")?
                != replacement.id.as_str()
        {
            return Err(StoreError::Validation(
                "fresh-attempt receipt must bind the exact stored proof and replacement identity"
                    .to_owned(),
            ));
        }
        let human_action_id = receipt_payload_i64(&receipt.payload, "human_action_id")?;
        let human_authorized: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM human_actions WHERE id=?1 AND run_id=?2 AND task_attempt_id=?3 AND action_type='retry_task' AND target_type='task' AND target_id=?4)",
            params![
                human_action_id,
                proof.run_id,
                proof.prior_attempt_id,
                replacement.task_id.as_str(),
            ],
            |row| row.get(0),
        )?;
        if !human_authorized {
            return Err(StoreError::Conflict(
                "fresh-attempt receipt does not bind a durable operator retry action".to_owned(),
            ));
        }

        let advanced_episode = advance_reconciliation_episode(
            &transaction,
            &receipt.episode_id,
            expected_episode_version,
            receipt.created_at_ms,
            ReconciliationCounter::Action,
        )?;
        let receipt_raw = serde_json::to_string(receipt)?;
        let receipt_digest = digest(&receipt_raw);
        transaction.execute(
            "INSERT INTO reconciliation_actions(episode_id,kind,source_event_id,authority_event_id,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                receipt.episode_id.as_str(),
                action_kind_name(receipt.kind),
                receipt.source_event_id,
                receipt.authority_event_id,
                receipt_raw,
                receipt_digest,
                receipt.created_at_ms,
            ],
        )?;
        // A correlation receipt has its own SQLite row id, so capture the
        // action id before recording that separate immutable fact.
        let action_id = transaction.last_insert_rowid();
        record_correlation_link_in_transaction(&transaction, &correlation)?;
        transaction.execute(
            "INSERT INTO task_attempts(id,task_id,attempt_number,state,task_packet_json,task_packet_sha256,base_sha,requested_model_route,token_budget,tool_budget,diff_file_budget,diff_line_budget,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13,1)",
            params![
                replacement.id.as_str(),
                replacement.task_id.as_str(),
                replacement.attempt_number,
                FRESH_ATTEMPT_AUTHORIZED_STATE,
                packet_json,
                replacement.packet_sha256,
                replacement.base_sha,
                replacement.requested_model_route,
                replacement.packet.token_budget as i64,
                replacement.packet.tool_budget.map(|value| value as i64),
                replacement.packet.diff_budget.files,
                replacement.packet.diff_budget.lines,
                now,
            ],
        )?;
        let changed = transaction.execute(
            "UPDATE tasks SET current_attempt_number=?2,state='READY',failure_reason=NULL,updated_at=?3,version=version+1 WHERE id=?1 AND version=?4",
            params![
                replacement.task_id.as_str(),
                replacement.attempt_number,
                now,
                i64::try_from(expected_task_version).unwrap_or(i64::MAX),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "task {} changed during ownership-proof consumption",
                replacement.task_id
            )));
        }
        transaction.execute(
            "INSERT INTO task_results(task_attempt_id,updated_at) VALUES(?1,?2)",
            params![replacement.id.as_str(), now],
        )?;
        transaction.execute(
            "INSERT INTO reconciliation_proof_consumptions(proof_id,episode_id,action_id,task_id,prior_attempt_id,replacement_attempt_id,consumed_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                proof.proof_id.as_str(),
                receipt.episode_id.as_str(),
                action_id,
                replacement.task_id.as_str(),
                proof.prior_attempt_id,
                replacement.id.as_str(),
                now,
            ],
        )?;
        transaction.commit()?;
        debug_assert_eq!(
            advanced_episode.version,
            expected_episode_version.saturating_add(1)
        );
        Ok(())
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
        let correlation = reconciliation_finding_correlation_link(finding)?;
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
            record_correlation_link_in_transaction(&transaction, &correlation)?;
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
        record_correlation_link_in_transaction(&transaction, &correlation)?;
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
        let correlation = reconciliation_action_correlation_link(receipt)?;
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
            record_correlation_link_in_transaction(&transaction, &correlation)?;
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
        record_correlation_link_in_transaction(&transaction, &correlation)?;
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

    /// Finds an unresolved episode by its complete controller identity. This
    /// is intentionally not a presentation-page query: recovery must keep
    /// finding an active episode even after arbitrary newer history exists.
    pub fn active_reconciliation_episode_for_run_trigger(
        &self,
        run_id: &str,
        trigger: ReconciliationTrigger,
    ) -> Result<Option<ReconciliationEpisode>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes WHERE run_id=?1 AND trigger_kind=?2 AND state IN ('open','claimed','awaiting_evidence') ORDER BY updated_at DESC,id DESC LIMIT 1",
                params![run_id, trigger_name(trigger)],
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

/// Reconciliation records share one controller-owned trace per episode. The
/// inventory adapter cannot supply a context: a deterministic causal receipt
/// ties each durable finding back to the exact reconciliation episode.
fn reconciliation_episode_correlation_link(
    episode: &ReconciliationEpisode,
) -> Result<CorrelationLink, StoreError> {
    let trace_id = digest(&format!(
        "harness.reconciliation.trace.v1:{}",
        episode.episode_id
    ));
    let span_id = digest(&format!(
        "harness.reconciliation.episode.span.v1:{}",
        episode.episode_id
    ));
    let link_id = CorrelationLinkId::parse(format!(
        "correlation-{}",
        &digest(&format!(
            "harness.reconciliation.episode.link.v1:{}",
            episode.episode_id
        ))[..48]
    ))
    .map_err(|error| StoreError::Validation(error.to_string()))?;
    Ok(CorrelationLink {
        schema: "harness.correlation-link.v1".to_owned(),
        link_id,
        trace: TraceContext {
            trace_id: trace_id[..32].to_owned(),
            span_id: span_id[..16].to_owned(),
            parent_span_id: None,
        },
        from_kind: "source_event".to_owned(),
        from_id: episode.source_event_id.clone(),
        to_kind: "reconciliation_episode".to_owned(),
        to_id: episode.episode_id.to_string(),
        relation: "opens_reconciliation".to_owned(),
        created_at_ms: episode.opened_at_ms,
    })
}

/// An ownership proof is controller evidence rather than an action. Its source
/// event gets its own immutable root trace so a future proof-consuming action
/// cannot be mistaken for the act of establishing exclusive custody.
fn ownership_proof_correlation_link(proof: &OwnershipProof) -> Result<CorrelationLink, StoreError> {
    let trace_id = digest(&format!(
        "harness.ownership-proof.trace.v1:{}",
        proof.proof_id
    ));
    let span_id = digest(&format!(
        "harness.ownership-proof.span.v1:{}",
        proof.proof_id
    ));
    let link_id = CorrelationLinkId::parse(format!(
        "correlation-{}",
        &digest(&format!(
            "harness.ownership-proof.link.v1:{}",
            proof.proof_id
        ))[..48]
    ))
    .map_err(|error| StoreError::Validation(error.to_string()))?;
    Ok(CorrelationLink {
        schema: "harness.correlation-link.v1".to_owned(),
        link_id,
        trace: TraceContext {
            trace_id: trace_id[..32].to_owned(),
            span_id: span_id[..16].to_owned(),
            parent_span_id: None,
        },
        from_kind: "source_event".to_owned(),
        from_id: proof.source_event_id.clone(),
        to_kind: "ownership_proof".to_owned(),
        to_id: proof.proof_id.to_string(),
        relation: "establishes_ownership_proof".to_owned(),
        created_at_ms: proof.observed_at_ms,
    })
}

fn ownership_proof_expired(proof: &OwnershipProof, now: i64) -> bool {
    proof.expires_at_ms <= now
}

fn reconciliation_finding_correlation_link(
    finding: &ReconciliationFinding,
) -> Result<CorrelationLink, StoreError> {
    reconciliation_correlation_link(
        &finding.episode_id,
        "finding",
        finding_kind_name(finding.kind),
        &finding.source_event_id,
        "reconciliation_finding",
        "has_finding",
        finding.observed_at_ms,
    )
}

/// A reconciliation action receipt is a causal child of its exact inventory
/// episode. The link is recorded in the same transaction as the receipt and,
/// for fresh attempts, the proof-consumption/attempt materialization.
fn reconciliation_action_correlation_link(
    receipt: &ReconciliationActionReceipt,
) -> Result<CorrelationLink, StoreError> {
    reconciliation_correlation_link(
        &receipt.episode_id,
        "action",
        action_kind_name(receipt.kind),
        &receipt.source_event_id,
        "reconciliation_action",
        "has_action",
        receipt.created_at_ms,
    )
}

fn reconciliation_correlation_link(
    episode_id: &ReconciliationEpisodeId,
    record_class: &str,
    record_kind: &str,
    source_event_id: &str,
    to_kind: &str,
    relation: &str,
    created_at_ms: i64,
) -> Result<CorrelationLink, StoreError> {
    let trace_id = digest(&format!("harness.reconciliation.trace.v1:{episode_id}"));
    let span_id = digest(&format!(
        "harness.reconciliation.{record_class}.span.v1:{episode_id}:{record_kind}:{source_event_id}"
    ));
    let link_id = CorrelationLinkId::parse(format!(
        "correlation-{}",
        &digest(&format!(
            "harness.reconciliation.{record_class}.link.v1:{episode_id}:{record_kind}:{source_event_id}"
        ))[..48]
    ))
    .map_err(|error| StoreError::Validation(error.to_string()))?;
    Ok(CorrelationLink {
        schema: "harness.correlation-link.v1".to_owned(),
        link_id,
        trace: TraceContext {
            trace_id: trace_id[..32].to_owned(),
            span_id: span_id[..16].to_owned(),
            parent_span_id: None,
        },
        from_kind: "reconciliation_episode".to_owned(),
        from_id: episode_id.to_string(),
        to_kind: to_kind.to_owned(),
        to_id: source_event_id.to_owned(),
        relation: relation.to_owned(),
        created_at_ms,
    })
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

pub(crate) fn checked_finding_row(
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

pub(crate) fn checked_action_receipt_row(
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

fn receipt_payload_string<'a>(
    payload: &'a serde_json::Value,
    field: &str,
) -> Result<&'a str, StoreError> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            StoreError::Validation(format!(
                "fresh-attempt receipt payload must contain non-empty string {field}"
            ))
        })
}

fn receipt_payload_u64(payload: &serde_json::Value, field: &str) -> Result<u64, StoreError> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            StoreError::Validation(format!(
                "fresh-attempt receipt payload must contain unsigned integer {field}"
            ))
        })
}

fn receipt_payload_i64(payload: &serde_json::Value, field: &str) -> Result<i64, StoreError> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            StoreError::Validation(format!(
                "fresh-attempt receipt payload must contain positive integer {field}"
            ))
        })
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
    use std::path::PathBuf;

    use harness_domain::{
        AttemptId, DiffBudget, OwnershipProof, ReconciliationActionKind,
        ReconciliationActionReceipt, ReconciliationEpisodeId, ReconciliationFinding,
        ReconciliationFindingKind, ReconciliationTrigger, RepositoryId, RunId, TaskId, TaskPacket,
        WorktreeId, now_ms,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{NewRepository, NewRun, NewWorktree};

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
        let correlation =
            reconciliation_episode_correlation_link(&first).expect("opening correlation");
        assert_eq!(
            store
                .correlation_links(&correlation.trace.trace_id, 10)
                .expect("opening trace"),
            vec![correlation]
        );
        let second = episode();
        assert!(matches!(
            store.open_reconciliation_episode(&second),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn active_reconciliation_lookup_is_not_lost_behind_a_history_page() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let run_id = "run-reconciliation-page-fixture";
        let mut active = episode();
        active.run_id = Some(run_id.to_owned());
        active.trigger_kind = ReconciliationTrigger::DaemonRestart;
        active.source_event_id = "event-active-daemon-restart".to_owned();
        active.sha256 = active.digest().expect("active digest");
        let active = store
            .open_reconciliation_episode(&active)
            .expect("active episode opens");

        // These immutable terminal rows model arbitrary newer history. The
        // old presentation-page query returned only 200 of them and would
        // never see the still-open episode above.
        for index in 0..201_i64 {
            let mut historical = episode();
            historical.run_id = Some(run_id.to_owned());
            historical.trigger_kind = ReconciliationTrigger::DaemonRestart;
            historical.state = ReconciliationState::Resolved;
            historical.opened_at_ms = 10_000 + index;
            historical.updated_at_ms = historical.opened_at_ms;
            historical.source_event_id = format!("event-resolved-daemon-restart-{index}");
            historical.sha256 = historical.digest().expect("historical digest");
            let raw = serde_json::to_string(&historical).expect("historical serializes");
            let raw_digest = digest(&raw);
            store
                .connection()
                .expect("connection")
                .execute(
                    "INSERT INTO reconciliation_episodes(id,run_id,trigger_kind,state,version,opened_at,updated_at,current_payload_json,current_payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    rusqlite::params![
                        historical.episode_id.as_str(),
                        run_id,
                        trigger_name(historical.trigger_kind),
                        state_name(historical.state),
                        historical.version as i64,
                        historical.opened_at_ms,
                        historical.updated_at_ms,
                        raw,
                        raw_digest,
                    ],
                )
                .expect("historical terminal episode persists");
        }

        assert!(
            !store
                .list_reconciliation_episodes(Some(run_id), 200)
                .expect("history page")
                .into_iter()
                .any(|episode| episode.episode_id == active.episode_id),
            "the regression setup places the active episode outside the newest page"
        );
        assert_eq!(
            store
                .active_reconciliation_episode_for_run_trigger(
                    run_id,
                    ReconciliationTrigger::DaemonRestart,
                )
                .expect("active lookup"),
            Some(active),
            "recovery must query the exact active controller identity"
        );
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

    fn packet(task_id: &str, base_sha: &str) -> TaskPacket {
        TaskPacket {
            schema: "harness.orchestration.task.v1".to_owned(),
            program_id: "run-fresh-attempt".to_owned(),
            task_id: task_id.to_owned(),
            title: "Fresh attempt fixture".to_owned(),
            state: "ready".to_owned(),
            priority: "P1".to_owned(),
            execution_mode: "controller".to_owned(),
            execution_kind: harness_domain::TaskExecutionKind::Implementation,
            investigation_scope: None,
            owner_profile: "fixture".to_owned(),
            reviewer_profile: "fixture".to_owned(),
            checklist_rows: vec![],
            authority_refs: vec![],
            base_sha: base_sha.to_owned(),
            dependency_shas: Default::default(),
            depends_on: vec![],
            owned_paths: vec!["crates/fresh-attempt/**".to_owned()],
            forbidden_paths: vec![],
            reserved_serial_paths: vec![],
            objective: "Exercise exclusive fresh-attempt custody".to_owned(),
            milestones: vec![],
            non_goals: vec![],
            success_criteria: vec!["An exact proof is consumed once".to_owned()],
            required_positive_tests: vec![],
            required_negative_tests: vec![],
            required_metrics: vec![],
            required_evidence: vec![],
            proof_limits: vec![],
            diff_budget: DiffBudget {
                files: 4,
                lines: 400,
            },
            token_budget: 4_000,
            tool_budget: None,
            lease_expires_at: "controller-managed".to_owned(),
            stop_conditions: vec![],
            handoff_path: "controller://attempt-handoff".to_owned(),
            risk_flags: vec![],
        }
    }

    fn fresh_attempt_fixture(
        temp: &TempDir,
    ) -> (
        Store,
        ReconciliationEpisode,
        OwnershipProof,
        TaskId,
        NewTaskAttempt,
        i64,
    ) {
        fresh_attempt_fixture_with_proof_ttl(temp, 60_000)
    }

    fn fresh_attempt_fixture_with_proof_ttl(
        temp: &TempDir,
        proof_ttl_ms: i64,
    ) -> (
        Store,
        ReconciliationEpisode,
        OwnershipProof,
        TaskId,
        NewTaskAttempt,
        i64,
    ) {
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let repository_id = RepositoryId::from("repository-fresh-attempt");
        store
            .create_repository(&NewRepository {
                id: repository_id.clone(),
                profile_id: "fixture".to_owned(),
                profile_version: 1,
                display_name: "Fresh attempt fixture".to_owned(),
                root_path: PathBuf::from("/tmp/fresh-attempt-fixture"),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "READY".to_owned(),
            })
            .expect("repository");
        let run_id = RunId::from("run-fresh-attempt");
        let head_sha = "a".repeat(40);
        store
            .create_run(&NewRun {
                id: run_id.clone(),
                repository_id,
                title: "Fresh attempt fixture".to_owned(),
                objective: "Exercise exclusive fresh-attempt custody".to_owned(),
                mode: "standard".to_owned(),
                publication_mode: "none".to_owned(),
                state: "EXECUTING".to_owned(),
                phase: "execution".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: head_sha.clone(),
                authority_digest: "fixture".to_owned(),
                profile_digest: "fixture".to_owned(),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".to_owned(),
                token_budget: None,
            })
            .expect("run");
        let task_id = TaskId::from("task-fresh-attempt");
        {
            let connection = store.connection().expect("connection");
            connection
                .pragma_update(None, "foreign_keys", false)
                .expect("disable task plan foreign key");
            connection
                .execute(
                    "INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at,version) VALUES(?1,?2,'plan-fixture','fresh-attempt','Fresh attempt fixture','Exercise exclusive fresh-attempt custody','P1','fixture','fixture','FAILED',1,1,1)",
                    rusqlite::params![task_id.as_str(), run_id.as_str()],
                )
                .expect("task");
            connection
                .pragma_update(None, "foreign_keys", true)
                .expect("restore task plan foreign key");
        }
        let prior_packet = packet("fresh-attempt", &head_sha);
        let prior_attempt = NewTaskAttempt {
            id: AttemptId::from("attempt-fresh-prior"),
            task_id: task_id.clone(),
            attempt_number: 1,
            state: "LEASED".to_owned(),
            packet: prior_packet.clone(),
            packet_sha256: digest(&serde_json::to_string(&prior_packet).expect("packet")),
            base_sha: head_sha.clone(),
            requested_model_route: "fixture-model".to_owned(),
        };
        store
            .create_task_attempt(&prior_attempt)
            .expect("prior attempt");
        store
            .set_attempt_result(
                &prior_attempt.id,
                "FAILED",
                Some(&head_sha),
                Some("infrastructure_unavailable"),
                Some("fixture failure"),
            )
            .expect("terminal prior attempt");
        {
            let connection = store.connection().expect("connection");
            connection
                .execute(
                    "UPDATE tasks SET state='FAILED',failure_reason='fixture failure' WHERE id=?1",
                    [task_id.as_str()],
                )
                .expect("failed task");
        }
        let worktree_id = WorktreeId::from("worktree-fresh-prior");
        store
            .create_worktree(&NewWorktree {
                id: worktree_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: Some(prior_attempt.id.clone()),
                kind: "task".to_owned(),
                path: temp.path().join("preserved-worktree"),
                branch: Some("harness/fresh-attempt".to_owned()),
                base_sha: head_sha.clone(),
                head_sha: Some(head_sha.clone()),
                state: "PRESERVED".to_owned(),
            })
            .expect("preserved worktree");
        let opened_at = now_ms();
        let mut episode = ReconciliationEpisode {
            schema: "harness.reconciliation-episode.v1".to_owned(),
            episode_id: "episode-fresh-attempt".parse().expect("episode id"),
            run_id: Some(run_id.to_string()),
            trigger_kind: ReconciliationTrigger::AppServerLoss,
            state: ReconciliationState::Open,
            version: 1,
            opened_at_ms: opened_at,
            updated_at_ms: opened_at,
            source_event_id: "event-fresh-attempt-recovery".to_owned(),
            inventory_sha256: "c".repeat(64),
            finding_count: 0,
            action_count: 0,
            report: None,
            sha256: String::new(),
        };
        episode.sha256 = episode.digest().expect("episode digest");
        let episode = store
            .open_reconciliation_episode(&episode)
            .expect("episode");
        let human_action_id = store
            .record_human_action(
                Some(&run_id),
                Some(&prior_attempt.id),
                "test-operator",
                "retry_task",
                "task",
                task_id.as_str(),
                &json!({"reason": "fixture authorization"}),
            )
            .expect("operator action");
        let replacement_packet = packet("fresh-attempt", &head_sha);
        let replacement = NewTaskAttempt {
            id: AttemptId::from("attempt-fresh-replacement"),
            task_id: task_id.clone(),
            attempt_number: 2,
            state: FRESH_ATTEMPT_AUTHORIZED_STATE.to_owned(),
            packet: replacement_packet.clone(),
            packet_sha256: digest(&serde_json::to_string(&replacement_packet).expect("packet")),
            base_sha: head_sha,
            requested_model_route: "fixture-model".to_owned(),
        };
        let observed_at_ms = now_ms();
        let mut proof = OwnershipProof {
            schema: "harness.exclusive-ownership-proof.v1".to_owned(),
            proof_id: "proof-fresh-attempt".parse().expect("proof id"),
            run_id: run_id.to_string(),
            task_id: task_id.to_string(),
            prior_attempt_id: prior_attempt.id.to_string(),
            worktree_id: worktree_id.to_string(),
            source_event_id: "event-fresh-attempt-proof".to_owned(),
            head_sha: replacement.base_sha.clone(),
            worktree_fingerprint: "b".repeat(64),
            lease_generation: 3,
            process_state: "proven_absent".to_owned(),
            session_state: "proven_closed".to_owned(),
            command_state: "terminal_or_none".to_owned(),
            external_effect_state: "none_or_reconciled".to_owned(),
            candidate_state: "preserved".to_owned(),
            approved_actions: vec!["authorize_fresh_attempt".to_owned()],
            observed_at_ms,
            expires_at_ms: observed_at_ms.saturating_add(proof_ttl_ms),
            sha256: String::new(),
        };
        proof.sha256 = proof.digest().expect("proof digest");
        let proof = store.record_ownership_proof(&proof).expect("proof");
        (store, episode, proof, task_id, replacement, human_action_id)
    }

    fn fresh_attempt_receipt(
        episode_id: ReconciliationEpisodeId,
        proof: &OwnershipProof,
        replacement: &NewTaskAttempt,
        human_action_id: i64,
    ) -> ReconciliationActionReceipt {
        let mut receipt = ReconciliationActionReceipt {
            schema: "harness.reconciliation-action-receipt.v1".to_owned(),
            episode_id,
            kind: ReconciliationActionKind::AuthorizeFreshAttempt,
            source_event_id: "event-fresh-attempt-authorized".to_owned(),
            authority_event_id: Some(proof.source_event_id.clone()),
            created_at_ms: now_ms(),
            payload: json!({
                "proof_id": proof.proof_id,
                "run_id": proof.run_id,
                "task_id": proof.task_id,
                "prior_attempt_id": proof.prior_attempt_id,
                "worktree_id": proof.worktree_id,
                "head_sha": proof.head_sha,
                "worktree_fingerprint": proof.worktree_fingerprint,
                "lease_generation": proof.lease_generation,
                "replacement_attempt_id": replacement.id,
                "human_action_id": human_action_id,
            }),
            sha256: String::new(),
        };
        receipt.sha256 = receipt.digest().expect("receipt digest");
        receipt
    }

    #[test]
    fn ownership_proof_uses_observation_time_and_expires_at_the_boundary() {
        let temp = TempDir::new().expect("temp");
        let (_store, _episode, proof, _task_id, _replacement, _human_action_id) =
            fresh_attempt_fixture(&temp);
        assert!(!ownership_proof_expired(&proof, proof.observed_at_ms));
        assert!(ownership_proof_expired(&proof, proof.expires_at_ms));
        assert_eq!(
            ownership_proof_correlation_link(&proof)
                .expect("proof correlation")
                .created_at_ms,
            proof.observed_at_ms
        );

        let mut invalid = proof;
        invalid.observed_at_ms = invalid.expires_at_ms;
        invalid.sha256 = invalid.digest().expect("invalid proof digest");
        assert!(invalid.validate().is_err());
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
            vec![finding.clone()]
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
            vec![receipt.clone()]
        );
        let finding_correlation =
            reconciliation_finding_correlation_link(&finding).expect("finding correlation");
        let action_correlation =
            reconciliation_action_correlation_link(&receipt).expect("action correlation");
        let opening_correlation =
            reconciliation_episode_correlation_link(&opened).expect("opening correlation");
        assert_eq!(
            finding_correlation.trace.trace_id,
            action_correlation.trace.trace_id
        );
        assert_eq!(
            store
                .correlation_links(&finding_correlation.trace.trace_id, 10)
                .expect("reconciliation trace"),
            vec![opening_correlation, finding_correlation, action_correlation]
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

    #[test]
    fn proof_consumption_authorizes_exactly_one_replacement_and_scheduler_lease() {
        let temp = TempDir::new().expect("temp");
        let (store, episode, proof, task_id, replacement, human_action_id) =
            fresh_attempt_fixture_with_proof_ttl(&temp, 250);
        let receipt = fresh_attempt_receipt(
            episode.episode_id.clone(),
            &proof,
            &replacement,
            human_action_id,
        );
        let task_version = store.task(&task_id).expect("task").version;

        store
            .consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &replacement,
                task_version,
            )
            .expect("consume proof");
        assert_eq!(
            store
                .task(&task_id)
                .expect("task after authorization")
                .state,
            TaskState::Ready
        );
        assert_eq!(
            store
                .authorized_fresh_attempt(&task_id)
                .expect("authorized attempt")
                .expect("one attempt")
                .id,
            replacement.id
        );
        assert_eq!(
            store
                .list_reconciliation_action_receipts(&episode.episode_id, 10)
                .expect("receipt"),
            vec![receipt.clone()]
        );
        let correlation =
            reconciliation_action_correlation_link(&receipt).expect("fresh-attempt correlation");
        let opening_correlation =
            reconciliation_episode_correlation_link(&episode).expect("opening correlation");
        let proof_correlation =
            ownership_proof_correlation_link(&proof).expect("proof correlation");
        assert_eq!(
            store
                .correlation_links(&correlation.trace.trace_id, 10)
                .expect("fresh-attempt trace"),
            vec![opening_correlation, correlation]
        );
        assert_eq!(
            store
                .correlation_links(&proof_correlation.trace.trace_id, 10)
                .expect("proof trace"),
            vec![proof_correlation]
        );

        store
            .consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &replacement,
                task_version,
            )
            .expect("idempotent replay");
        let conflicting_replacement = NewTaskAttempt {
            id: AttemptId::from("attempt-fresh-conflict"),
            ..replacement.clone()
        };
        assert!(matches!(
            store.consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &conflicting_replacement,
                task_version,
            ),
            Err(StoreError::Conflict(_))
        ));

        let mut conflicting_route = replacement.clone();
        conflicting_route.requested_model_route = "different-route".to_owned();
        assert!(matches!(
            store.consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &conflicting_route,
                task_version,
            ),
            Err(StoreError::Conflict(_))
        ));

        while now_ms() <= proof.expires_at_ms {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(ownership_proof_expired(&proof, now_ms()));
        store
            .consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &replacement,
                task_version,
            )
            .expect("exact committed replay survives later proof expiry");

        store
            .lease_authorized_fresh_attempt(&task_id, &replacement.id)
            .expect("lease authorized attempt");
        assert_eq!(
            store.task(&task_id).expect("leased task").state,
            TaskState::Leased
        );
        assert!(
            store
                .authorized_fresh_attempt(&task_id)
                .expect("no longer authorized")
                .is_none()
        );
    }

    #[test]
    fn proof_consumption_refuses_receipt_that_omits_proven_custody() {
        let temp = TempDir::new().expect("temp");
        let (store, episode, proof, task_id, replacement, human_action_id) =
            fresh_attempt_fixture(&temp);
        let mut receipt = fresh_attempt_receipt(
            episode.episode_id.clone(),
            &proof,
            &replacement,
            human_action_id,
        );
        receipt.payload["worktree_fingerprint"] = json!("c".repeat(64));
        receipt.sha256 = receipt.digest().expect("receipt digest");

        assert!(matches!(
            store.consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &replacement,
                store.task(&task_id).expect("task").version,
            ),
            Err(StoreError::Validation(_))
        ));
        assert!(
            store
                .authorized_fresh_attempt(&task_id)
                .expect("no replacement")
                .is_none()
        );
    }

    #[test]
    fn proof_consumption_refuses_any_unreconciled_command_history() {
        let temp = TempDir::new().expect("temp");
        let (store, episode, proof, task_id, replacement, human_action_id) =
            fresh_attempt_fixture(&temp);
        let command_json = r#"{"program":"fixture-command"}"#;
        store
            .connection()
            .expect("connection")
            .execute(
                "INSERT INTO command_runs(id,run_id,task_attempt_id,command_json,command_sha256,cwd,resource_class,started_at,timed_out,version) VALUES('command-fresh-prior',?1,?2,?3,?4,'/tmp/fresh-attempt-fixture','control',1,0,1)",
                rusqlite::params![
                    &proof.run_id,
                    &proof.prior_attempt_id,
                    command_json,
                    digest(command_json),
                ],
            )
            .expect("recorded command");
        let receipt = fresh_attempt_receipt(
            episode.episode_id.clone(),
            &proof,
            &replacement,
            human_action_id,
        );
        assert!(matches!(
            store.consume_ownership_proof_for_fresh_attempt(
                &proof.proof_id,
                &receipt,
                episode.version,
                &replacement,
                store.task(&task_id).expect("task").version,
            ),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn correlation_conflict_rolls_back_reconciliation_open_and_ownership_proof() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let opening = episode();
        let mut conflicting_opening =
            reconciliation_episode_correlation_link(&opening).expect("opening correlation");
        conflicting_opening.relation = "different_relation".to_owned();
        store
            .record_correlation_link(&conflicting_opening)
            .expect("preload conflicting opening correlation");
        assert!(matches!(
            store.open_reconciliation_episode(&opening),
            Err(StoreError::Conflict(_))
        ));
        assert!(
            store
                .reconciliation_episode(&opening.episode_id)
                .expect("episode read")
                .is_none()
        );

        let (store, _episode, proof, _task_id, _replacement, _human_action_id) =
            fresh_attempt_fixture(&temp);
        let mut attempted_proof = proof.clone();
        attempted_proof.proof_id = "proof-correlation-conflict".parse().expect("proof id");
        attempted_proof.source_event_id = "event-proof-correlation-conflict".to_owned();
        attempted_proof.sha256 = attempted_proof.digest().expect("proof digest");
        let mut conflicting_proof =
            ownership_proof_correlation_link(&attempted_proof).expect("proof correlation");
        conflicting_proof.relation = "different_relation".to_owned();
        store
            .record_correlation_link(&conflicting_proof)
            .expect("preload conflicting proof correlation");
        assert!(matches!(
            store.record_ownership_proof(&attempted_proof),
            Err(StoreError::Conflict(_))
        ));
        let attempted_count: i64 = store
            .connection()
            .expect("connection")
            .query_row(
                "SELECT count(*) FROM ownership_proofs WHERE id=?1",
                [attempted_proof.proof_id.as_str()],
                |row| row.get(0),
            )
            .expect("proof count");
        assert_eq!(attempted_count, 0);
    }
}
