//! Source-owned passive external-condition registry.
//!
//! This repository captures observations and exposes read models only. It does
//! not schedule adapters, poll a provider, wake work, or execute any result.

use harness_domain::{
    ConditionObservation, ExternalCondition, ExternalConditionId, ExternalConditionState,
};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

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
        observation
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if observation.condition_id != *condition_id {
            return Err(StoreError::Validation(
                "condition observation must bind the requested condition".to_owned(),
            ));
        }
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
                .list_condition_observations(&condition.condition_id, 10)
                .expect("observation list"),
            vec![observation.clone()]
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
}
