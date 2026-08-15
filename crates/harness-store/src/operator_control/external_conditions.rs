//! Source-owned external-condition registry.
//!
//! This repository captures observations and exposes read models. It does not
//! schedule adapters, poll a provider, wake work, or execute any result. A
//! controller may atomically pair one already-evaluated local fact with a
//! material event through the explicit custody method below.

use harness_domain::{
    ConditionObservation, CorrelationLink, CorrelationLinkId, ExternalCondition,
    ExternalConditionAdapter, ExternalConditionId, ExternalConditionOwnerType,
    ExternalConditionState, ExternalConditionSummary, RunId, TraceContext,
};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

use super::correlation::record_correlation_link_in_transaction;

const MAX_EXTERNAL_CONDITION_PAGE_SIZE: u32 = 200;
const MAX_CONDITION_OBSERVATION_PAGE_SIZE: u32 = 200;

impl Store {
    /// Registers an exact source-owned condition. Registration is idempotent
    /// only for identical bytes; a changed adapter/source pair must be
    /// explicitly retired before a replacement can be introduced.
    pub fn register_external_condition(
        &self,
        condition: &ExternalCondition,
    ) -> Result<ExternalCondition, StoreError> {
        condition
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if condition.state != ExternalConditionState::Open
            || condition.sequence != 0
            || condition.version != 1
            || condition.last_observation.is_some()
        {
            return Err(StoreError::Validation(
                "a new external condition must be open at sequence zero, version one, without an observation".to_owned(),
            ));
        }
        let raw = serde_json::to_string(condition)?;
        let payload_sha256 = digest(&raw);
        let adapter = enum_name(&condition.adapter)?;
        let state = enum_name(&condition.state)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM external_conditions WHERE adapter=?1 AND source_id=?2",
                params![adapter, condition.source_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_condition_row(existing_raw, existing_digest)?;
            if existing == *condition {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict(format!(
                "external condition source {}:{} already has different current content",
                adapter, condition.source_id
            )));
        }
        transaction.execute(
            "INSERT INTO external_conditions(id,adapter,source_id,state,version,current_payload_json,current_payload_sha256,opened_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                condition.condition_id.as_str(),
                adapter,
                condition.source_id,
                state,
                to_i64(condition.version, "external condition version")?,
                raw,
                payload_sha256,
                condition.opened_at_ms,
                condition.updated_at_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(condition.clone())
    }

    /// Records one source observation and advances the in-store condition
    /// atomically. No domain event, scheduler wake, polling, or consequent
    /// action is produced here.
    pub fn record_external_condition_observation(
        &self,
        condition_id: &ExternalConditionId,
        expected_version: u64,
        observation: &ConditionObservation,
    ) -> Result<ExternalCondition, StoreError> {
        self.record_external_condition_observation_inner(
            condition_id,
            expected_version,
            observation,
            None,
        )
    }

    /// Records a controller-observed external fact and its material event in
    /// one transaction. The caller supplies an exact owning run, while this
    /// method re-derives that ownership from the condition before accepting
    /// it. A successful observation therefore cannot survive without its
    /// scheduler-visible controller event.
    pub fn record_external_condition_observation_and_emit(
        &self,
        condition_id: &ExternalConditionId,
        expected_version: u64,
        observation: &ConditionObservation,
        run_id: &RunId,
        event_type: &str,
        event_payload: &Value,
    ) -> Result<ExternalCondition, StoreError> {
        validate_domain_event_type(event_type)?;
        if !event_payload.is_object() {
            return Err(StoreError::Validation(
                "external condition event payload must be an object".to_owned(),
            ));
        }
        self.record_external_condition_observation_inner(
            condition_id,
            expected_version,
            observation,
            Some(ConditionMaterialEvent {
                run_id,
                event_type,
                payload: event_payload,
            }),
        )
    }

    fn record_external_condition_observation_inner(
        &self,
        condition_id: &ExternalConditionId,
        expected_version: u64,
        observation: &ConditionObservation,
        material_event: Option<ConditionMaterialEvent<'_>>,
    ) -> Result<ExternalCondition, StoreError> {
        observation
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if observation.condition_id != *condition_id {
            return Err(StoreError::Validation(
                "condition observation must bind the requested condition".to_owned(),
            ));
        }
        let correlation = condition_observation_correlation_link(observation)?;
        let observation_raw = serde_json::to_string(observation)?;
        let observation_digest = digest(&observation_raw);
        let expected_version = to_i64(expected_version, "external condition expected version")?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM condition_observations WHERE condition_id=?1 AND source_event_id=?2",
                params![condition_id.as_str(), observation.source_event_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let existing = checked_observation_row(existing_raw, existing_digest)?;
            if existing == *observation {
                let current = transaction.query_row(
                    "SELECT current_payload_json,current_payload_sha256 FROM external_conditions WHERE id=?1",
                    [condition_id.as_str()],
                    |row| checked_condition_row(row.get(0)?, row.get(1)?),
                )?;
                if let Some(event) = material_event {
                    ensure_material_event_binding(&transaction, &current, observation, &event)?;
                    let present: i64 = transaction.query_row(
                        "SELECT count(*) FROM domain_events WHERE run_id=?1 AND aggregate_type='external_condition' AND aggregate_id=?2 AND event_type=?3 AND json_extract(payload_json,'$.source_event_id')=?4",
                        params![
                            event.run_id.as_str(),
                            condition_id.as_str(),
                            event.event_type,
                            observation.source_event_id,
                        ],
                        |row| row.get(0),
                    )?;
                    if present != 1 {
                        return Err(StoreError::Conflict(
                            "an existing external-condition observation is missing its material event"
                                .to_owned(),
                        ));
                    }
                }
                record_correlation_link_in_transaction(&transaction, &correlation)?;
                transaction.commit()?;
                return Ok(current);
            }
            return Err(StoreError::Conflict(
                "external condition observation source event already has different content".to_owned(),
            ));
        }
        let raw = transaction
            .query_row(
                "SELECT current_payload_json FROM external_conditions WHERE id=?1",
                [condition_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("external condition {condition_id}")))?;
        let mut condition: ExternalCondition = serde_json::from_str(&raw)?;
        condition
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if let Some(event) = material_event.as_ref() {
            ensure_material_event_binding(&transaction, &condition, observation, event)?;
        }
        if to_i64(condition.version, "external condition version")? != expected_version {
            return Err(StoreError::Conflict(format!(
                "external condition {condition_id} has version {}, expected {expected_version}",
                condition.version
            )));
        }
        if condition.state.is_terminal() {
            return Err(StoreError::Conflict(
                "a terminal external condition cannot accept another observation".to_owned(),
            ));
        }
        let expected_sequence = condition.sequence.checked_add(1).ok_or_else(|| {
            StoreError::Validation("external condition sequence overflow".to_owned())
        })?;
        if observation.sequence != expected_sequence {
            return Err(StoreError::Conflict(format!(
                "external condition observation sequence {} does not follow current sequence {}",
                observation.sequence, condition.sequence
            )));
        }
        condition.sequence = observation.sequence;
        condition.state = observation.state;
        condition.last_observation = Some(observation.clone());
        condition.version = condition.version.checked_add(1).ok_or_else(|| {
            StoreError::Validation("external condition version overflow".to_owned())
        })?;
        condition.updated_at_ms = observation.observed_at_ms;
        condition.sha256 = condition
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        condition
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let condition_raw = serde_json::to_string(&condition)?;
        let condition_digest = digest(&condition_raw);
        let updated = transaction.execute(
            "UPDATE external_conditions SET state=?1,version=?2,current_payload_json=?3,current_payload_sha256=?4,updated_at=?5 WHERE id=?6 AND version=?7",
            params![
                enum_name(&condition.state)?,
                to_i64(condition.version, "external condition version")?,
                condition_raw,
                condition_digest,
                condition.updated_at_ms,
                condition_id.as_str(),
                expected_version,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::Conflict(
                "external condition changed while recording its observation".to_owned(),
            ));
        }
        transaction.execute(
            "INSERT INTO condition_observations(id,condition_id,source_event_id,observed_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                observation.observation_id.as_str(),
                condition_id.as_str(),
                observation.source_event_id,
                observation.observed_at_ms,
                observation_raw,
                observation_digest,
            ],
        )?;
        if let Some(event) = material_event {
            let mut payload = event.payload.as_object().cloned().ok_or_else(|| {
                StoreError::Validation(
                    "external condition event payload must be an object".to_owned(),
                )
            })?;
            // These fields are controller-owned joins, not adapter-provided
            // text. They let a replayed event prove the exact observation and
            // resulting immutable condition revision that it represents.
            payload.insert(
                "source_event_id".to_owned(),
                Value::String(observation.source_event_id.clone()),
            );
            payload.insert(
                "condition_id".to_owned(),
                Value::String(condition.condition_id.to_string()),
            );
            payload.insert(
                "condition_version".to_owned(),
                Value::from(condition.version),
            );
            payload.insert(
                "condition_sha256".to_owned(),
                Value::String(condition.sha256.clone()),
            );
            payload.insert(
                "observation_id".to_owned(),
                Value::String(observation.observation_id.to_string()),
            );
            transaction.execute(
                "INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json,source_raw_event_id) VALUES(?1,'external_condition',?2,?3,?4,?5,NULL)",
                params![
                    event.run_id.as_str(),
                    condition.condition_id.as_str(),
                    event.event_type,
                    harness_domain::now_ms(),
                    serde_json::to_string(&Value::Object(payload))?,
                ],
            )?;
        }
        record_correlation_link_in_transaction(&transaction, &correlation)?;
        transaction.commit()?;
        Ok(condition)
    }

    pub fn external_condition(
        &self,
        condition_id: &ExternalConditionId,
    ) -> Result<Option<ExternalCondition>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM external_conditions WHERE id=?1",
                [condition_id.as_str()],
                |row| checked_condition_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_external_conditions(
        &self,
        include_terminal: bool,
        limit: u32,
    ) -> Result<Vec<ExternalCondition>, StoreError> {
        if limit == 0 || limit > MAX_EXTERNAL_CONDITION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "external condition page limit must be 1..={MAX_EXTERNAL_CONDITION_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let limit = i64::from(limit);
        let rows = if include_terminal {
            connection
                .prepare("SELECT current_payload_json,current_payload_sha256 FROM external_conditions ORDER BY updated_at DESC,id DESC LIMIT ?1")?
                .query_map([limit], |row| checked_condition_row(row.get(0)?, row.get(1)?))?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            connection
                .prepare("SELECT current_payload_json,current_payload_sha256 FROM external_conditions WHERE state IN ('open','unknown') ORDER BY updated_at DESC,id DESC LIMIT ?1")?
                .query_map([limit], |row| checked_condition_row(row.get(0)?, row.get(1)?))?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(rows)
    }

    /// Lists open conditions only after applying the durable owner and adapter
    /// predicates. Applying a global page limit before those predicates can
    /// leave an older due condition unobserved forever.
    pub fn list_open_external_conditions_for_owner_adapter(
        &self,
        owner_type: ExternalConditionOwnerType,
        owner_id: &str,
        adapter: ExternalConditionAdapter,
        limit: u32,
    ) -> Result<Vec<ExternalCondition>, StoreError> {
        self.list_open_external_conditions_for_owner_adapter_before(
            owner_type, owner_id, adapter, None, limit,
        )
    }

    /// Lists one stable newest-first page of an owner's open conditions. The
    /// cursor is the final `(updated_at_ms, condition_id)` tuple from the
    /// preceding page. It makes reconciliation exhaustive even when an owner
    /// has more than the page-size ceiling.
    pub fn list_open_external_conditions_for_owner_adapter_before(
        &self,
        owner_type: ExternalConditionOwnerType,
        owner_id: &str,
        adapter: ExternalConditionAdapter,
        before: Option<(i64, &ExternalConditionId)>,
        limit: u32,
    ) -> Result<Vec<ExternalCondition>, StoreError> {
        if limit == 0 || limit > MAX_EXTERNAL_CONDITION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "external condition page limit must be 1..={MAX_EXTERNAL_CONDITION_PAGE_SIZE}"
            )));
        }
        let owner_type = enum_name(&owner_type)?;
        let adapter = enum_name(&adapter)?;
        let before_updated_at = before.map(|(updated_at_ms, _)| updated_at_ms);
        let before_id = before.map(|(_, condition_id)| condition_id.as_str());
        let connection = self.connection()?;
        connection
            .prepare(
                "SELECT current_payload_json,current_payload_sha256 FROM external_conditions \
                 WHERE adapter=?1 AND state='open' \
                   AND json_extract(current_payload_json, '$.owner_type')=?2 \
                   AND json_extract(current_payload_json, '$.owner_id')=?3 \
                   AND (?4 IS NULL OR updated_at < ?4 OR (updated_at = ?4 AND id < ?5)) \
                 ORDER BY updated_at DESC,id DESC LIMIT ?6",
            )?
            .query_map(
                params![
                    adapter,
                    owner_type,
                    owner_id,
                    before_updated_at,
                    before_id,
                    i64::from(limit)
                ],
                |row| checked_condition_row(row.get(0)?, row.get(1)?),
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Lists compact, integrity-checked summaries for browser and snapshot
    /// projections. Adapter specifications and observation payloads require an
    /// explicit read of the selected condition or its observation history.
    pub fn list_external_condition_summaries(
        &self,
        include_terminal: bool,
        limit: u32,
    ) -> Result<Vec<ExternalConditionSummary>, StoreError> {
        self.list_external_conditions(include_terminal, limit)
            .map(|conditions| {
                conditions
                    .iter()
                    .map(ExternalConditionSummary::from)
                    .collect()
            })
    }

    pub fn list_condition_observations(
        &self,
        condition_id: &ExternalConditionId,
        limit: u32,
    ) -> Result<Vec<ConditionObservation>, StoreError> {
        if limit == 0 || limit > MAX_CONDITION_OBSERVATION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "condition observation page limit must be 1..={MAX_CONDITION_OBSERVATION_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT payload_json,payload_sha256 FROM condition_observations WHERE condition_id=?1 ORDER BY observed_at DESC,id DESC LIMIT ?2",
        )?;
        statement
            .query_map(params![condition_id.as_str(), i64::from(limit)], |row| {
                checked_observation_row(row.get(0)?, row.get(1)?)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

/// Derives the controller-owned causal edge from one exact condition to its
/// immutable observation. Adapter payload cannot supply trace context, and an
/// observation retry repairs a missing old link without creating new facts.
fn condition_observation_correlation_link(
    observation: &ConditionObservation,
) -> Result<CorrelationLink, StoreError> {
    let trace_id = digest(&format!(
        "harness.condition-observation.trace.v1:{}",
        observation.observation_id
    ));
    let span_id = digest(&format!(
        "harness.condition-observation.span.v1:{}",
        observation.observation_id
    ));
    let link_id = CorrelationLinkId::parse(format!(
        "correlation-{}",
        &digest(&format!(
            "harness.condition-observation.link.v1:{}",
            observation.observation_id
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
        from_kind: "external_condition".to_owned(),
        from_id: observation.condition_id.to_string(),
        to_kind: "condition_observation".to_owned(),
        to_id: observation.observation_id.to_string(),
        relation: "observed_as".to_owned(),
        created_at_ms: observation.observed_at_ms,
    })
}

struct ConditionMaterialEvent<'a> {
    run_id: &'a RunId,
    event_type: &'a str,
    payload: &'a Value,
}

fn ensure_material_event_binding(
    transaction: &rusqlite::Transaction<'_>,
    condition: &ExternalCondition,
    observation: &ConditionObservation,
    event: &ConditionMaterialEvent<'_>,
) -> Result<(), StoreError> {
    if observation.condition_id != condition.condition_id {
        return Err(StoreError::Validation(
            "external condition event observation must bind the current condition".to_owned(),
        ));
    }
    let owner_run_id = condition_owner_run_id(transaction, condition)?;
    if owner_run_id != *event.run_id {
        return Err(StoreError::Conflict(
            "external condition event run does not match the condition owner".to_owned(),
        ));
    }
    Ok(())
}

fn condition_owner_run_id(
    transaction: &rusqlite::Transaction<'_>,
    condition: &ExternalCondition,
) -> Result<RunId, StoreError> {
    let run_id = match condition.owner_type {
        ExternalConditionOwnerType::Run => transaction
            .query_row(
                "SELECT id FROM runs WHERE id=?1",
                [condition.owner_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?,
        ExternalConditionOwnerType::Task => transaction
            .query_row(
                "SELECT run_id FROM tasks WHERE id=?1",
                [condition.owner_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?,
        ExternalConditionOwnerType::Attempt => transaction
            .query_row(
                "SELECT t.run_id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE a.id=?1",
                [condition.owner_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?,
    }
    .ok_or_else(|| {
        StoreError::NotFound(format!(
            "external condition owner {} is not present",
            condition.owner_id
        ))
    })?;
    Ok(RunId::from(run_id))
}

fn validate_domain_event_type(value: &str) -> Result<(), StoreError> {
    if value.is_empty()
        || value.len() > 160
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(StoreError::Validation(
            "external condition event type must be a bounded controller identifier".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn checked_condition_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<ExternalCondition> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "external condition payload integrity check failed".into(),
        ));
    }
    let condition: ExternalCondition = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    condition.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(condition)
}

fn checked_observation_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<ConditionObservation> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "external condition observation payload integrity check failed".into(),
        ));
    }
    let observation: ConditionObservation = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    observation.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(observation)
}

fn enum_name(value: &impl Serialize) -> Result<String, StoreError> {
    let encoded = serde_json::to_string(value)?;
    encoded
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            StoreError::Validation("external condition enum must serialize as a string".to_owned())
        })
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
    use std::path::Path;

    use harness_domain::{
        ConditionObservationId, ExternalConditionAdapter, ExternalConditionId,
        ExternalConditionOwnerType, ExternalConditionPollPolicy,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    fn condition() -> ExternalCondition {
        let mut condition = ExternalCondition {
            schema: "harness.external-condition.v1".to_owned(),
            condition_id: ExternalConditionId::new(),
            owner_type: ExternalConditionOwnerType::Task,
            owner_id: "task_a".to_owned(),
            adapter: ExternalConditionAdapter::CiCheck,
            source_id: "check:run_a:required".to_owned(),
            spec: json!({"check_name":"required"}),
            state: ExternalConditionState::Open,
            sequence: 0,
            poll_policy: ExternalConditionPollPolicy {
                initial_ms: 15_000,
                maximum_ms: 300_000,
                deadline_ms: Some(1_000_000),
            },
            source_identity_digest: "a".repeat(64),
            last_observation: None,
            version: 1,
            opened_at_ms: 1,
            updated_at_ms: 1,
            sha256: String::new(),
        };
        condition.sha256 = condition.digest().expect("digest");
        condition
    }

    fn observation(condition: &ExternalCondition) -> ConditionObservation {
        let mut observation = ConditionObservation {
            schema: "harness.condition-observation.v1".to_owned(),
            observation_id: ConditionObservationId::new(),
            condition_id: condition.condition_id.clone(),
            source_event_id: "ci:event:1".to_owned(),
            sequence: 1,
            observed_at_ms: 2,
            state: ExternalConditionState::Satisfied,
            payload: json!({"conclusion":"success"}),
            sha256: String::new(),
        };
        observation.sha256 = observation.digest().expect("digest");
        observation
    }

    #[test]
    fn observation_is_atomic_idempotent_and_never_schedules_work() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let condition = condition();
        store
            .register_external_condition(&condition)
            .expect("registration");
        let registered_snapshot = store.control_plane_snapshot().expect("registered snapshot");
        assert_eq!(
            registered_snapshot.external_conditions.state,
            harness_domain::SnapshotSectionState::Current
        );
        assert_eq!(registered_snapshot.external_conditions.rows.len(), 1);
        assert_eq!(
            registered_snapshot.external_conditions.rows[0]["schema"],
            "harness.external-condition-summary.v1"
        );
        assert!(
            registered_snapshot.external_conditions.rows[0]
                .get("spec")
                .is_none()
        );
        let observation = observation(&condition);
        let updated = store
            .record_external_condition_observation(&condition.condition_id, 1, &observation)
            .expect("observation");
        assert_eq!(updated.state, ExternalConditionState::Satisfied);
        assert_eq!(updated.sequence, 1);
        assert_eq!(updated.version, 2);
        let observed_snapshot = store.control_plane_snapshot().expect("observed snapshot");
        assert!(observed_snapshot.revision > registered_snapshot.revision);
        assert_eq!(observed_snapshot.external_conditions.rows.len(), 0);
        assert_eq!(
            observed_snapshot.source_cursors["condition_observations"],
            1
        );
        assert_eq!(
            store
                .record_external_condition_observation(&condition.condition_id, 1, &observation)
                .expect("idempotent observation"),
            updated
        );
        assert!(
            store
                .list_external_conditions(false, 10)
                .expect("open list")
                .is_empty()
        );
        assert_eq!(
            store
                .list_external_condition_summaries(true, 10)
                .expect("summary list")[0]
                .condition_sha256,
            updated.sha256
        );
        assert_eq!(
            store
                .list_condition_observations(&condition.condition_id, 10)
                .expect("observation list"),
            vec![observation.clone()]
        );
        let correlation =
            condition_observation_correlation_link(&observation).expect("observation correlation");
        assert_eq!(
            store
                .correlation_links(&correlation.trace.trace_id, 10)
                .expect("stored observation correlation"),
            vec![correlation]
        );
        assert_eq!(
            store
                .connection()
                .expect("connection")
                .query_row("SELECT count(*) FROM domain_events", [], |row| row
                    .get::<_, i64>(0))
                .expect("event count"),
            0,
            "the passive registry must not emit a scheduler-visible event"
        );
        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute(
                    "UPDATE condition_observations SET observed_at = 3 WHERE id = ?1",
                    params![observation.observation_id.as_str()],
                )
                .is_err(),
            "condition observations must remain source-history receipts"
        );
        assert!(
            connection
                .execute(
                    "DELETE FROM condition_observations WHERE id = ?1",
                    params![observation.observation_id.as_str()],
                )
                .is_err(),
            "condition observations must remain append-only"
        );
    }

    #[test]
    fn observation_and_causal_link_roll_back_together_on_a_conflicting_trace() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let condition = condition();
        store
            .register_external_condition(&condition)
            .expect("registration");
        let observation = observation(&condition);
        let mut conflicting =
            condition_observation_correlation_link(&observation).expect("expected link");
        conflicting.relation = "different_relation".to_owned();
        store
            .record_correlation_link(&conflicting)
            .expect("preexisting conflicting link");

        assert!(matches!(
            store.record_external_condition_observation(&condition.condition_id, 1, &observation),
            Err(StoreError::Conflict(_))
        ));
        assert_eq!(
            store
                .external_condition(&condition.condition_id)
                .expect("condition reads")
                .expect("condition remains"),
            condition
        );
        assert!(
            store
                .list_condition_observations(&condition.condition_id, 10)
                .expect("no observation written")
                .is_empty()
        );
    }

    #[test]
    fn owner_filtered_open_page_cannot_starve_an_older_condition() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let mut target = condition();
        target.owner_type = ExternalConditionOwnerType::Run;
        target.owner_id = "run-target".to_owned();
        target.adapter = ExternalConditionAdapter::TimeGate;
        target.source_id = "time-gate:target".to_owned();
        target.sha256 = target.digest().expect("target digest");
        store
            .register_external_condition(&target)
            .expect("older target registers");

        for index in 0..MAX_EXTERNAL_CONDITION_PAGE_SIZE {
            let mut unrelated = target.clone();
            unrelated.condition_id = ExternalConditionId::new();
            unrelated.owner_id = "run-other".to_owned();
            unrelated.source_id = format!("time-gate:other:{index}");
            unrelated.opened_at_ms = i64::from(index) + 2;
            unrelated.updated_at_ms = unrelated.opened_at_ms;
            unrelated.sha256 = unrelated.digest().expect("unrelated digest");
            store
                .register_external_condition(&unrelated)
                .expect("newer unrelated condition registers");
        }

        assert!(
            !store
                .list_external_conditions(false, MAX_EXTERNAL_CONDITION_PAGE_SIZE)
                .expect("global page reads")
                .iter()
                .any(|condition| condition.condition_id == target.condition_id),
            "the legacy global page demonstrates the starvation boundary"
        );
        assert_eq!(
            store
                .list_open_external_conditions_for_owner_adapter(
                    ExternalConditionOwnerType::Run,
                    "run-target",
                    ExternalConditionAdapter::TimeGate,
                    1,
                )
                .expect("owner page reads"),
            vec![target],
            "owner and adapter predicates must apply before LIMIT"
        );
    }

    #[test]
    fn owner_adapter_cursor_reaches_the_201st_open_condition() {
        let temp = TempDir::new().expect("temp");
        let store =
            Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store");
        let mut oldest = condition();
        oldest.owner_type = ExternalConditionOwnerType::Run;
        oldest.owner_id = "run-target".to_owned();
        oldest.adapter = ExternalConditionAdapter::TimeGate;
        oldest.source_id = "time-gate:oldest".to_owned();
        oldest.sha256 = oldest.digest().expect("oldest digest");
        store
            .register_external_condition(&oldest)
            .expect("oldest condition registers");

        for index in 0..MAX_EXTERNAL_CONDITION_PAGE_SIZE {
            let mut newer = oldest.clone();
            newer.condition_id = ExternalConditionId::new();
            newer.source_id = format!("time-gate:newer:{index}");
            newer.opened_at_ms = i64::from(index) + 2;
            newer.updated_at_ms = newer.opened_at_ms;
            newer.sha256 = newer.digest().expect("newer digest");
            store
                .register_external_condition(&newer)
                .expect("newer same-owner condition registers");
        }

        let first = store
            .list_open_external_conditions_for_owner_adapter(
                ExternalConditionOwnerType::Run,
                "run-target",
                ExternalConditionAdapter::TimeGate,
                MAX_EXTERNAL_CONDITION_PAGE_SIZE,
            )
            .expect("first page reads");
        assert_eq!(first.len(), MAX_EXTERNAL_CONDITION_PAGE_SIZE as usize);
        assert!(
            first
                .iter()
                .all(|condition| condition.condition_id != oldest.condition_id),
            "the oldest record is beyond the first same-owner page"
        );
        let cursor = first.last().expect("full page has a final cursor");
        assert_eq!(
            store
                .list_open_external_conditions_for_owner_adapter_before(
                    ExternalConditionOwnerType::Run,
                    "run-target",
                    ExternalConditionAdapter::TimeGate,
                    Some((cursor.updated_at_ms, &cursor.condition_id)),
                    MAX_EXTERNAL_CONDITION_PAGE_SIZE,
                )
                .expect("second page reads"),
            vec![oldest],
            "the stable cursor reaches the 201st same-owner condition"
        );
    }
}
