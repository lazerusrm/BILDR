//! Observe-only, stateful liveness episode repository.
//!
//! The reducer consumes explicit deterministic observations and persists every
//! observation before its current episode view. It schedules no work and emits
//! no recovery action; an operator/control-plane consumer may inspect the
//! resulting state but cannot clear it with model prose.

use harness_domain::{
    CorrelationLink, CorrelationLinkId, InterventionId, InterventionKind, InterventionReceipt,
    LivenessEpisode, LivenessEpisodeId, LivenessObservation, LivenessObservationKind,
    LivenessState, TraceContext, now_ms,
};
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

use super::correlation::record_correlation_link_in_transaction;

const MAX_LIVENESS_PAGE_SIZE: u32 = 200;
const OBSERVE_REVIEW_DELAY_MS: i64 = 5 * 60 * 1_000;
const WAIT_INTERVENTION_POLICY_VERSION: &str = "operator_control_wait_v1";

impl Store {
    /// Opens exactly one nonterminal liveness episode for an exact attempt.
    /// Existing identical content is a safe replay; a different concurrent
    /// episode for the same attempt is a custody conflict.
    pub fn open_liveness_episode(
        &self,
        episode: &LivenessEpisode,
    ) -> Result<LivenessEpisode, StoreError> {
        episode
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if episode.run_id.is_none() || episode.task_id.is_none() || episode.attempt_id.is_none() {
            return Err(StoreError::Validation(
                "a liveness episode requires exact run, task, and attempt identity".to_owned(),
            ));
        }
        if episode.version != 1 || episode.state == LivenessState::Terminal {
            return Err(StoreError::Validation(
                "a new liveness episode must start at version one in a nonterminal state"
                    .to_owned(),
            ));
        }
        let raw = serde_json::to_string(episode)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                [episode.episode_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_episode_row(existing_raw, existing_digest)?;
            if existing == *episode {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict(format!(
                "liveness episode {} already has different content",
                episode.episode_id
            )));
        }
        let existing_for_attempt: Option<(String, String)> = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE attempt_id=?1 AND state!='terminal' ORDER BY updated_at DESC,id DESC LIMIT 1",
                [episode.attempt_id.as_deref().expect("required above")],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        if let Some((existing_raw, existing_digest)) = existing_for_attempt {
            let existing = checked_episode_row(existing_raw, existing_digest)?;
            return Err(StoreError::Conflict(format!(
                "attempt {} already has active liveness episode {}",
                episode.attempt_id.as_deref().expect("required above"),
                existing.episode_id
            )));
        }
        transaction.execute(
            "INSERT INTO liveness_episodes(id,run_id,task_id,attempt_id,state,version,opened_at,updated_at,current_payload_json,current_payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                episode.episode_id.as_str(),
                episode.run_id,
                episode.task_id,
                episode.attempt_id,
                state_name(episode.state),
                to_i64(episode.version, "liveness episode version")?,
                episode.opened_at_ms,
                episode.updated_at_ms,
                raw,
                payload_sha256,
            ],
        )?;
        transaction.commit()?;
        Ok(episode.clone())
    }

    /// Records one immutable typed observation and atomically advances the
    /// deterministic episode projection. A heartbeat or command activity does
    /// not clear degraded/stalled state; only material progress can recover it.
    pub fn record_liveness_observation(
        &self,
        episode_id: &LivenessEpisodeId,
        expected_version: u64,
        observation: &LivenessObservation,
    ) -> Result<LivenessEpisode, StoreError> {
        observation
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if observation.episode_id != *episode_id {
            return Err(StoreError::Validation(
                "liveness observation must bind the requested episode".to_owned(),
            ));
        }
        let observation_raw = serde_json::to_string(observation)?;
        let observation_digest = digest(&observation_raw);
        let expected_version = to_i64(expected_version, "liveness expected version")?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM liveness_observations WHERE episode_id=?1 AND observation_kind=?2 AND source_event_id=?3",
                params![
                    episode_id.as_str(),
                    observation_kind_name(observation.observation_kind),
                    observation.source_event_id,
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_observation_row(existing_raw, existing_digest)?;
            if existing != *observation {
                return Err(StoreError::Conflict(
                    "liveness observation source event already has different content".to_owned(),
                ));
            }
            let episode = transaction.query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                [episode_id.as_str()],
                |row| checked_episode_row(row.get(0)?, row.get(1)?),
            )?;
            transaction.commit()?;
            return Ok(episode);
        }
        let (raw, stored_digest): (String, String) = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                [episode_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("liveness episode {episode_id}")))?;
        let mut episode = checked_episode_row(raw, stored_digest)?;
        if to_i64(episode.version, "liveness episode version")? != expected_version {
            return Err(StoreError::Conflict(format!(
                "liveness episode {episode_id} has version {}, expected {expected_version}",
                episode.version
            )));
        }
        if episode.state == LivenessState::Terminal {
            return Err(StoreError::Conflict(
                "a terminal liveness episode cannot accept another observation".to_owned(),
            ));
        }
        if observation.observed_at_ms < episode.opened_at_ms {
            return Err(StoreError::Validation(
                "liveness observation predates the episode".to_owned(),
            ));
        }
        reduce_episode(&mut episode, observation)?;
        episode.version = episode.version.checked_add(1).ok_or_else(|| {
            StoreError::Validation("liveness episode version overflow".to_owned())
        })?;
        episode.updated_at_ms = observation.observed_at_ms.max(episode.updated_at_ms);
        episode.sha256 = episode
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        episode
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let episode_raw = serde_json::to_string(&episode)?;
        let episode_digest = digest(&episode_raw);
        let changed = transaction.execute(
            "UPDATE liveness_episodes SET state=?1,version=?2,updated_at=?3,current_payload_json=?4,current_payload_sha256=?5 WHERE id=?6 AND version=?7",
            params![
                state_name(episode.state),
                to_i64(episode.version, "liveness episode version")?,
                episode.updated_at_ms,
                episode_raw,
                episode_digest,
                episode_id.as_str(),
                expected_version,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "liveness episode {episode_id} changed during observation reduction"
            )));
        }
        transaction.execute(
            "INSERT INTO liveness_observations(id,episode_id,observation_kind,source_event_id,observed_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                observation.observation_id.as_str(),
                episode_id.as_str(),
                observation_kind_name(observation.observation_kind),
                observation.source_event_id,
                observation.observed_at_ms,
                observation_raw,
                observation_digest,
            ],
        )?;
        transaction.commit()?;
        Ok(episode)
    }

    pub fn liveness_episode(
        &self,
        episode_id: &LivenessEpisodeId,
    ) -> Result<Option<LivenessEpisode>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                [episode_id.as_str()],
                |row| checked_episode_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_liveness_episodes(
        &self,
        run_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<LivenessEpisode>, StoreError> {
        if limit == 0 || limit > MAX_LIVENESS_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "liveness page limit must be 1..={MAX_LIVENESS_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let mut statement = if run_id.is_some() {
            connection.prepare(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE run_id=?1 ORDER BY updated_at DESC,id DESC LIMIT ?2",
            )?
        } else {
            connection.prepare(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes ORDER BY updated_at DESC,id DESC LIMIT ?1",
            )?
        };
        if let Some(run_id) = run_id {
            let rows = statement.query_map(params![run_id, i64::from(limit)], |row| {
                checked_episode_row(row.get(0)?, row.get(1)?)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        } else {
            let rows = statement.query_map([i64::from(limit)], |row| {
                checked_episode_row(row.get(0)?, row.get(1)?)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }
    }

    /// Executes the one active-low-risk intervention: observe and wait on the
    /// exact liveness revision. It cannot alter a task, owner, worktree,
    /// process, approval, external condition, or episode state. Its only
    /// effect is the immutable receipt and incremented intervention counter.
    ///
    /// The deterministic identity makes a browser retry of the same
    /// `(episode, revision, requester)` operation return the original receipt
    /// rather than creating another wait decision.
    pub fn execute_wait_intervention(
        &self,
        episode_id: &LivenessEpisodeId,
        expected_version: u64,
        requested_by: &str,
    ) -> Result<LivenessEpisode, StoreError> {
        let identity = digest(&format!(
            "harness.liveness-wait.v1\0{episode_id}\0{expected_version}\0{requested_by}"
        ));
        let intervention_id = format!("intervention-wait-{}", &identity[..32]);
        let source_event_id = format!("liveness-wait-{}", &identity[..32]);
        let existing = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT payload_json,payload_sha256 FROM intervention_receipts WHERE id=?1",
                    [&intervention_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
        };
        if let Some((raw, payload_sha256)) = existing {
            let receipt = checked_intervention_receipt(raw, payload_sha256)?;
            if receipt.episode_id != *episode_id
                || receipt.kind != InterventionKind::Wait
                || receipt.source_event_id != source_event_id
                || receipt.target_version != expected_version
                || receipt.policy_version != WAIT_INTERVENTION_POLICY_VERSION
                || receipt.requested_by != requested_by
            {
                return Err(StoreError::Conflict(
                    "wait intervention identity already has different content".to_owned(),
                ));
            }
            return self.record_intervention_receipt(&receipt);
        }
        let episode = self
            .liveness_episode(episode_id)?
            .ok_or_else(|| StoreError::NotFound(format!("liveness episode {episode_id}")))?;
        if episode.version != expected_version {
            return Err(StoreError::Conflict(format!(
                "liveness episode {episode_id} has version {}, wait requires {expected_version}",
                episode.version
            )));
        }
        let mut receipt = InterventionReceipt {
            schema: "harness.intervention-receipt.v1".to_owned(),
            intervention_id: InterventionId::parse(&intervention_id)
                .map_err(|error| StoreError::Validation(error.to_string()))?,
            episode_id: episode_id.clone(),
            kind: InterventionKind::Wait,
            source_event_id,
            target_version: expected_version,
            policy_version: WAIT_INTERVENTION_POLICY_VERSION.to_owned(),
            requested_by: requested_by.to_owned(),
            created_at_ms: now_ms().max(episode.updated_at_ms),
            sha256: String::new(),
        };
        receipt.sha256 = receipt
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        self.record_intervention_receipt(&receipt)
    }

    /// Records controller-owned evidence for one legal liveness intervention.
    ///
    /// This is intentionally not an intervention executor: the caller must
    /// have used an existing controller path to perform any work. The receipt
    /// can only bind the current episode revision, so a stale recommendation
    /// cannot be presented as an action on a newer custody state.
    pub fn record_intervention_receipt(
        &self,
        receipt: &InterventionReceipt,
    ) -> Result<LivenessEpisode, StoreError> {
        receipt
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let correlation = intervention_correlation_link(receipt)?;
        let receipt_raw = serde_json::to_string(receipt)?;
        let receipt_digest = digest(&receipt_raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;

        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM intervention_receipts WHERE id=?1",
                [receipt.intervention_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_intervention_receipt(existing_raw, existing_digest)?;
            if existing != *receipt {
                return Err(StoreError::Conflict(
                    "intervention receipt id already has different content".to_owned(),
                ));
            }
            let episode = transaction
                .query_row(
                    "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                    [receipt.episode_id.as_str()],
                    |row| checked_episode_row(row.get(0)?, row.get(1)?),
                )
                .optional()?
                .ok_or_else(|| StoreError::NotFound(format!("liveness episode {}", receipt.episode_id)))?;
            record_correlation_link_in_transaction(&transaction, &correlation)?;
            transaction.commit()?;
            return Ok(episode);
        }
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM intervention_receipts WHERE episode_id=?1 AND kind=?2 AND source_event_id=?3",
                params![
                    receipt.episode_id.as_str(),
                    intervention_kind_name(receipt.kind),
                    receipt.source_event_id.as_str(),
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_intervention_receipt(existing_raw, existing_digest)?;
            if existing != *receipt {
                return Err(StoreError::Conflict(
                    "intervention source event already has different content".to_owned(),
                ));
            }
            let episode = transaction
                .query_row(
                    "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                    [receipt.episode_id.as_str()],
                    |row| checked_episode_row(row.get(0)?, row.get(1)?),
                )
                .optional()?
                .ok_or_else(|| StoreError::NotFound(format!("liveness episode {}", receipt.episode_id)))?;
            record_correlation_link_in_transaction(&transaction, &correlation)?;
            transaction.commit()?;
            return Ok(episode);
        }

        let (raw, stored_digest): (String, String) = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                [receipt.episode_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("liveness episode {}", receipt.episode_id)))?;
        let mut episode = checked_episode_row(raw, stored_digest)?;
        if episode.version != receipt.target_version {
            return Err(StoreError::Conflict(format!(
                "liveness episode {} has version {}, intervention requires {}",
                receipt.episode_id, episode.version, receipt.target_version
            )));
        }
        if episode.state == LivenessState::Terminal {
            return Err(StoreError::Conflict(
                "a terminal liveness episode cannot accept an intervention receipt".to_owned(),
            ));
        }
        if receipt.created_at_ms < episode.opened_at_ms {
            return Err(StoreError::Validation(
                "intervention receipt predates the liveness episode".to_owned(),
            ));
        }
        episode.intervention_count =
            episode.intervention_count.checked_add(1).ok_or_else(|| {
                StoreError::Validation("liveness intervention count overflow".to_owned())
            })?;
        episode.version = episode.version.checked_add(1).ok_or_else(|| {
            StoreError::Validation("liveness episode version overflow".to_owned())
        })?;
        episode.updated_at_ms = episode.updated_at_ms.max(receipt.created_at_ms);
        if episode
            .next_review_at_ms
            .is_some_and(|next_review_at_ms| next_review_at_ms < episode.updated_at_ms)
        {
            // The due review is now immediately eligible. This updates the
            // factual projection without scheduling, waking, or executing
            // anything on behalf of the receipt.
            episode.next_review_at_ms = Some(episode.updated_at_ms);
        }
        episode.sha256 = episode
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        episode
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let episode_raw = serde_json::to_string(&episode)?;
        let episode_digest = digest(&episode_raw);
        let changed = transaction.execute(
            "UPDATE liveness_episodes SET version=?1,updated_at=?2,current_payload_json=?3,current_payload_sha256=?4 WHERE id=?5 AND version=?6",
            params![
                to_i64(episode.version, "liveness episode version")?,
                episode.updated_at_ms,
                episode_raw,
                episode_digest,
                receipt.episode_id.as_str(),
                to_i64(receipt.target_version, "intervention target version")?,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "liveness episode {} changed during intervention recording",
                receipt.episode_id
            )));
        }
        transaction.execute(
            "INSERT INTO intervention_receipts(id,episode_id,kind,source_event_id,created_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                receipt.intervention_id.as_str(),
                receipt.episode_id.as_str(),
                intervention_kind_name(receipt.kind),
                receipt.source_event_id.as_str(),
                receipt.created_at_ms,
                receipt_raw,
                receipt_digest,
            ],
        )?;
        record_correlation_link_in_transaction(&transaction, &correlation)?;
        transaction.commit()?;
        Ok(episode)
    }

    pub fn list_intervention_receipts(
        &self,
        episode_id: &LivenessEpisodeId,
        limit: u32,
    ) -> Result<Vec<InterventionReceipt>, StoreError> {
        if limit == 0 || limit > MAX_LIVENESS_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "intervention receipt page limit must be 1..={MAX_LIVENESS_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let exists = connection
            .query_row(
                "SELECT 1 FROM liveness_episodes WHERE id=?1",
                [episode_id.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Err(StoreError::NotFound(format!(
                "liveness episode {episode_id}"
            )));
        }
        let mut statement = connection.prepare(
            "SELECT payload_json,payload_sha256 FROM intervention_receipts WHERE episode_id=?1 ORDER BY created_at DESC,id DESC LIMIT ?2",
        )?;
        Ok(statement
            .query_map(params![episode_id.as_str(), i64::from(limit)], |row| {
                checked_intervention_receipt(row.get(0)?, row.get(1)?)
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }
}

/// Derives one controller-owned causal link for an immutable intervention
/// receipt. Requests cannot supply trace context: the receipt's exact episode,
/// intervention identity, and policy record form the sole causal claim.
fn intervention_correlation_link(
    receipt: &InterventionReceipt,
) -> Result<CorrelationLink, StoreError> {
    let trace_id = digest(&format!(
        "harness.intervention.trace.v1:{}",
        receipt.intervention_id
    ));
    let span_id = digest(&format!(
        "harness.intervention.span.v1:{}",
        receipt.intervention_id
    ));
    let link_id = CorrelationLinkId::parse(format!(
        "correlation-{}",
        &digest(&format!(
            "harness.intervention.link.v1:{}",
            receipt.intervention_id
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
        from_kind: "liveness_episode".to_owned(),
        from_id: receipt.episode_id.to_string(),
        to_kind: "intervention_receipt".to_owned(),
        to_id: receipt.intervention_id.to_string(),
        relation: "has_intervention".to_owned(),
        created_at_ms: receipt.created_at_ms,
    })
}

fn reduce_episode(
    episode: &mut LivenessEpisode,
    observation: &LivenessObservation,
) -> Result<(), StoreError> {
    let active_external_wait = observation
        .value
        .get("active_external_wait")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let ownership_uncertain = observation
        .value
        .get("ownership")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "uncertain");
    let no_progress_boundary = observation
        .value
        .get("no_material_progress_boundary")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let repeated_failures = observation
        .value
        .get("repeated_semantic_failures")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let fresh_inspection = observation
        .value
        .get("fresh_inspection")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let (state, reasons, next_review): (LivenessState, Vec<String>, Option<i64>) = match observation
        .observation_kind
    {
        LivenessObservationKind::MaterialProgress => {
            episode.last_material_progress_at_ms = Some(observation.observed_at_ms);
            (
                LivenessState::Healthy,
                vec!["material_progress".to_owned()],
                None,
            )
        }
        LivenessObservationKind::OwnershipEvidence if ownership_uncertain => (
            LivenessState::OwnershipUncertain,
            vec!["ownership_uncertain".to_owned()],
            Some(observation.observed_at_ms + OBSERVE_REVIEW_DELAY_MS),
        ),
        LivenessObservationKind::ExternalWait if active_external_wait => (
            LivenessState::WaitingExternal,
            vec!["active_external_wait".to_owned()],
            Some(observation.observed_at_ms + OBSERVE_REVIEW_DELAY_MS),
        ),
        _ if no_progress_boundary && !active_external_wait && !ownership_uncertain => {
            if repeated_failures >= 2 && fresh_inspection {
                (
                    LivenessState::ConfirmedStall,
                    vec![
                        "no_material_progress".to_owned(),
                        "repeated_semantic_failure".to_owned(),
                        "fresh_inspection".to_owned(),
                    ],
                    Some(observation.observed_at_ms + OBSERVE_REVIEW_DELAY_MS),
                )
            } else {
                (
                    LivenessState::SuspectedStall,
                    vec!["no_material_progress_boundary".to_owned()],
                    Some(observation.observed_at_ms + OBSERVE_REVIEW_DELAY_MS),
                )
            }
        }
        LivenessObservationKind::RuntimeHeartbeat | LivenessObservationKind::CommandActivity
            if matches!(
                episode.state,
                LivenessState::Healthy | LivenessState::QuietActive
            ) =>
        {
            (
                LivenessState::QuietActive,
                vec!["activity_without_material_progress".to_owned()],
                Some(observation.observed_at_ms + OBSERVE_REVIEW_DELAY_MS),
            )
        }
        _ => (
            episode.state,
            episode.state_reason_codes.clone(),
            episode.next_review_at_ms,
        ),
    };
    episode.state = state;
    episode.state_reason_codes = reasons;
    episode.next_review_at_ms = next_review;
    Ok(())
}

pub(crate) fn checked_episode_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<LivenessEpisode> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "liveness episode payload integrity check failed".into(),
        ));
    }
    let episode: LivenessEpisode = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    episode.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(episode)
}

fn checked_observation_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<LivenessObservation> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "liveness observation payload integrity check failed".into(),
        ));
    }
    let observation: LivenessObservation = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    observation.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(observation)
}

fn checked_intervention_receipt(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<InterventionReceipt> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "intervention receipt payload integrity check failed".into(),
        ));
    }
    let receipt: InterventionReceipt = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    receipt.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(receipt)
}

fn state_name(state: LivenessState) -> &'static str {
    match state {
        LivenessState::Healthy => "healthy",
        LivenessState::QuietActive => "quiet_active",
        LivenessState::WaitingExternal => "waiting_external",
        LivenessState::Degraded => "degraded",
        LivenessState::SuspectedStall => "suspected_stall",
        LivenessState::ConfirmedStall => "confirmed_stall",
        LivenessState::OwnershipUncertain => "ownership_uncertain",
        LivenessState::RecoveryRequired => "recovery_required",
        LivenessState::Terminal => "terminal",
    }
}

fn observation_kind_name(kind: LivenessObservationKind) -> &'static str {
    match kind {
        LivenessObservationKind::MaterialProgress => "material_progress",
        LivenessObservationKind::RuntimeHeartbeat => "runtime_heartbeat",
        LivenessObservationKind::CommandActivity => "command_activity",
        LivenessObservationKind::ExternalWait => "external_wait",
        LivenessObservationKind::OwnershipEvidence => "ownership_evidence",
    }
}

fn intervention_kind_name(kind: InterventionKind) -> &'static str {
    match kind {
        InterventionKind::Wait => "wait",
        InterventionKind::RequestOperatorDecision => "request_operator_decision",
        InterventionKind::RequestReconciliation => "request_reconciliation",
        InterventionKind::QueueReadOnlyReview => "queue_read_only_review",
    }
}

fn to_i64(value: u64, field: &str) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::Validation(format!("{field} exceeds SQLite integer range")))
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use harness_domain::{InterventionId, LivenessObservationId};
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    fn episode() -> LivenessEpisode {
        let mut episode = LivenessEpisode {
            schema: "harness.liveness-episode.v1".to_owned(),
            episode_id: LivenessEpisodeId::new(),
            run_id: Some("run-a".to_owned()),
            task_id: Some("task-a".to_owned()),
            attempt_id: Some("attempt-a".to_owned()),
            state: LivenessState::Healthy,
            version: 1,
            opened_at_ms: 1_000,
            updated_at_ms: 1_000,
            state_reason_codes: vec!["opened".to_owned()],
            last_material_progress_at_ms: None,
            next_review_at_ms: None,
            intervention_count: 0,
            outcome: None,
            sha256: String::new(),
        };
        episode.sha256 = episode.digest().expect("digest");
        episode
    }

    fn observation(
        episode_id: LivenessEpisodeId,
        kind: LivenessObservationKind,
        source_event_id: &str,
        at_ms: i64,
        value: Value,
    ) -> LivenessObservation {
        let mut observation = LivenessObservation {
            schema: "harness.liveness-observation.v1".to_owned(),
            observation_id: LivenessObservationId::new(),
            episode_id,
            observation_kind: kind,
            source_event_id: source_event_id.to_owned(),
            observed_at_ms: at_ms,
            value,
            classifier_version: "liveness-v1".to_owned(),
            sha256: String::new(),
        };
        observation.sha256 = observation.digest().expect("digest");
        observation
    }

    fn intervention(
        episode_id: LivenessEpisodeId,
        target_version: u64,
        source_event_id: &str,
    ) -> InterventionReceipt {
        let mut receipt = InterventionReceipt {
            schema: "harness.intervention-receipt.v1".to_owned(),
            intervention_id: InterventionId::new(),
            episode_id,
            kind: InterventionKind::RequestReconciliation,
            source_event_id: source_event_id.to_owned(),
            target_version,
            policy_version: "liveness-policy-v1".to_owned(),
            requested_by: "controller".to_owned(),
            created_at_ms: 2_000,
            sha256: String::new(),
        };
        receipt.sha256 = receipt.digest().expect("digest");
        receipt
    }

    #[test]
    fn noisy_activity_does_not_clear_a_confirmed_stall_but_progress_does() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let opened = store.open_liveness_episode(&episode()).expect("open");
        let stalled = store
            .record_liveness_observation(
                &opened.episode_id,
                opened.version,
                &observation(
                    opened.episode_id.clone(),
                    LivenessObservationKind::RuntimeHeartbeat,
                    "event-stall",
                    2_000,
                    json!({
                        "no_material_progress_boundary": true,
                        "repeated_semantic_failures": 2,
                        "fresh_inspection": true,
                    }),
                ),
            )
            .expect("stall");
        assert_eq!(stalled.state, LivenessState::ConfirmedStall);
        let noisy = store
            .record_liveness_observation(
                &stalled.episode_id,
                stalled.version,
                &observation(
                    stalled.episode_id.clone(),
                    LivenessObservationKind::CommandActivity,
                    "event-noise",
                    3_000,
                    json!({"bounded_command_active": true}),
                ),
            )
            .expect("noise");
        assert_eq!(noisy.state, LivenessState::ConfirmedStall);
        let recovered = store
            .record_liveness_observation(
                &noisy.episode_id,
                noisy.version,
                &observation(
                    noisy.episode_id.clone(),
                    LivenessObservationKind::MaterialProgress,
                    "event-progress",
                    4_000,
                    json!({"progress_id": "progress-a"}),
                ),
            )
            .expect("progress");
        assert_eq!(recovered.state, LivenessState::Healthy);
        assert_eq!(recovered.last_material_progress_at_ms, Some(4_000));
        assert!(matches!(
            store.record_liveness_observation(
                &recovered.episode_id,
                recovered.version,
                &observation(
                    recovered.episode_id.clone(),
                    LivenessObservationKind::MaterialProgress,
                    "event-progress",
                    4_000,
                    json!({"progress_id": "different"}),
                ),
            ),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn intervention_receipt_is_exact_revisioned_and_replay_safe() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let opened = store.open_liveness_episode(&episode()).expect("open");
        let receipt = intervention(opened.episode_id.clone(), opened.version, "event-action-a");

        let applied = store
            .record_intervention_receipt(&receipt)
            .expect("record receipt");
        assert_eq!(applied.version, opened.version + 1);
        assert_eq!(applied.intervention_count, 1);
        assert_eq!(
            store.record_intervention_receipt(&receipt).expect("replay"),
            applied
        );
        assert_eq!(
            store
                .list_intervention_receipts(&opened.episode_id, 10)
                .expect("receipts"),
            vec![receipt.clone()]
        );
        assert!(matches!(
            store.list_intervention_receipts(&LivenessEpisodeId::new(), 10),
            Err(StoreError::NotFound(_))
        ));

        let stale = intervention(opened.episode_id.clone(), opened.version, "event-action-b");
        assert!(matches!(
            store.record_intervention_receipt(&stale),
            Err(StoreError::Conflict(_))
        ));
        let mut conflicting = receipt;
        conflicting.requested_by = "other-controller".to_owned();
        conflicting.sha256 = conflicting.digest().expect("digest");
        assert!(matches!(
            store.record_intervention_receipt(&conflicting),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn wait_intervention_is_idempotent_and_cannot_apply_to_a_stale_episode() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let opened = store.open_liveness_episode(&episode()).expect("open");

        let waited = store
            .execute_wait_intervention(&opened.episode_id, opened.version, "local_session")
            .expect("wait");
        assert_eq!(waited.version, opened.version + 1);
        assert_eq!(waited.state, opened.state);
        assert_eq!(waited.intervention_count, 1);
        assert_eq!(
            store
                .execute_wait_intervention(&opened.episode_id, opened.version, "local_session")
                .expect("idempotent replay"),
            waited
        );
        let receipts = store
            .list_intervention_receipts(&opened.episode_id, 10)
            .expect("receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].kind, InterventionKind::Wait);
        assert_eq!(receipts[0].target_version, opened.version);
        let correlation = intervention_correlation_link(&receipts[0]).expect("correlation");
        assert_eq!(
            store
                .correlation_links(&correlation.trace.trace_id, 10)
                .expect("stored correlation"),
            vec![correlation]
        );
        assert!(matches!(
            store.execute_wait_intervention(&opened.episode_id, opened.version, "other_session"),
            Err(StoreError::Conflict(_))
        ));
    }
}
