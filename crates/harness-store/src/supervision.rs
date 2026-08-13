use harness_domain::{RunId, SupervisorSnapshotId, format_timestamp, now_ms};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

pub const SUPERVISOR_SNAPSHOT_SCHEMA: &str = "harness.supervisor-snapshot.v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SupervisorSnapshotRecord {
    pub id: SupervisorSnapshotId,
    pub run_id: RunId,
    pub revision: u64,
    pub event_cursor: i64,
    pub trigger_kind: String,
    pub payload: Value,
    pub payload_sha256: String,
    pub byte_length: u64,
    pub created_at: String,
}

impl Store {
    pub fn supervisor_observation_cursor(&self, run_id: &RunId) -> Result<i64, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT last_event_cursor FROM supervisor_observation_cursors WHERE run_id=?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn advance_supervisor_observation_cursor(
        &self,
        run_id: &RunId,
        event_cursor: i64,
    ) -> Result<(), StoreError> {
        if event_cursor < 0 {
            return Err(StoreError::Validation(
                "supervisor observation cursor must be non-negative".to_owned(),
            ));
        }
        self.connection()?.execute(
            "INSERT INTO supervisor_observation_cursors(run_id,last_event_cursor,updated_at) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET last_event_cursor=max(last_event_cursor,excluded.last_event_cursor),updated_at=excluded.updated_at",
            params![run_id.as_str(), event_cursor, now_ms()],
        )?;
        Ok(())
    }

    pub fn record_supervisor_snapshot<F>(
        &self,
        run_id: &RunId,
        event_cursor: i64,
        trigger_kind: &str,
        build_payload: F,
    ) -> Result<SupervisorSnapshotRecord, StoreError>
    where
        F: FnOnce(&SupervisorSnapshotId, u64) -> Result<Value, StoreError>,
    {
        if event_cursor < 0 || trigger_kind.is_empty() || trigger_kind.len() > 128 {
            return Err(StoreError::Validation(
                "supervisor snapshot cursor or trigger is invalid".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE run_id=?1 AND event_cursor=?2",
                params![run_id.as_str(), event_cursor],
                map_supervisor_snapshot,
            )
            .optional()?
        {
            transaction.execute(
                "INSERT INTO supervisor_observation_cursors(run_id,last_event_cursor,updated_at) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET last_event_cursor=max(last_event_cursor,excluded.last_event_cursor),updated_at=excluded.updated_at",
                params![run_id.as_str(), event_cursor, now_ms()],
            )?;
            transaction.commit()?;
            return Ok(existing);
        }
        let revision_raw: i64 = transaction.query_row(
            "SELECT coalesce(max(revision),0)+1 FROM supervisor_snapshots WHERE run_id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        let revision = u64::try_from(revision_raw).map_err(|_| {
            StoreError::Validation("supervisor snapshot revision is invalid".to_owned())
        })?;
        let id = SupervisorSnapshotId::new();
        let payload = build_payload(&id, revision)?;
        validate_snapshot_binding(&payload, &id, run_id, revision, event_cursor)?;
        let raw = serde_json::to_string(&payload)?;
        let byte_length = i64::try_from(raw.len()).map_err(|_| {
            StoreError::Validation("supervisor snapshot exceeds supported size".to_owned())
        })?;
        let digest = hex::encode(Sha256::digest(raw.as_bytes()));
        let created_at = now_ms();
        transaction.execute(
            "INSERT INTO supervisor_snapshots(id,run_id,revision,schema_name,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![id.as_str(),run_id.as_str(),revision_raw,SUPERVISOR_SNAPSHOT_SCHEMA,event_cursor,trigger_kind,raw,digest,byte_length,created_at],
        )?;
        transaction.execute(
            "INSERT INTO supervisor_observation_cursors(run_id,last_event_cursor,updated_at) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET last_event_cursor=max(last_event_cursor,excluded.last_event_cursor),updated_at=excluded.updated_at",
            params![run_id.as_str(), event_cursor, created_at],
        )?;
        let record = transaction.query_row(
            "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE id=?1",
            [id.as_str()],
            map_supervisor_snapshot,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn latest_supervisor_snapshot(
        &self,
        run_id: &RunId,
    ) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
        self.connection()?.query_row(
            "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE run_id=?1 ORDER BY revision DESC LIMIT 1",
            [run_id.as_str()],
            map_supervisor_snapshot,
        ).optional().map_err(Into::into)
    }
}

fn validate_snapshot_binding(
    payload: &Value,
    id: &SupervisorSnapshotId,
    run_id: &RunId,
    revision: u64,
    event_cursor: i64,
) -> Result<(), StoreError> {
    let valid = payload.get("schema").and_then(Value::as_str) == Some(SUPERVISOR_SNAPSHOT_SCHEMA)
        && payload.get("snapshot_id").and_then(Value::as_str) == Some(id.as_str())
        && payload.get("run_id").and_then(Value::as_str) == Some(run_id.as_str())
        && payload.get("revision").and_then(Value::as_u64) == Some(revision)
        && payload.get("event_cursor").and_then(Value::as_i64) == Some(event_cursor);
    if valid {
        Ok(())
    } else {
        Err(StoreError::Validation(
            "supervisor snapshot payload does not match its immutable envelope".to_owned(),
        ))
    }
}

fn positive_u64(column: usize, value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            format!("invalid non-negative integer in supervisor snapshot column {column}").into(),
        )
    })
}

fn map_supervisor_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<SupervisorSnapshotRecord> {
    let revision: i64 = row.get(2)?;
    let byte_length: i64 = row.get(7)?;
    let created_at: i64 = row.get(8)?;
    let raw: String = row.get(5)?;
    let payload_sha256: String = row.get(6)?;
    let byte_length = positive_u64(7, byte_length)?;
    let encoded_length = u64::try_from(raw.len()).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Text,
            "supervisor snapshot payload exceeds supported size".into(),
        )
    })?;
    let calculated_sha256 = hex::encode(Sha256::digest(raw.as_bytes()));
    if byte_length != encoded_length || payload_sha256 != calculated_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Text,
            "supervisor snapshot payload integrity check failed".into(),
        ));
    }
    Ok(SupervisorSnapshotRecord {
        id: SupervisorSnapshotId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        revision: positive_u64(2, revision)?,
        event_cursor: row.get(3)?,
        trigger_kind: row.get(4)?,
        payload: serde_json::from_str(&raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        payload_sha256,
        byte_length,
        created_at: format_timestamp(created_at),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NewRepository, NewRun};
    use tempfile::TempDir;

    fn fixture() -> (TempDir, Store, RunId) {
        let temp = TempDir::new().expect("temporary fixture");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store opens");
        let repository = harness_domain::RepositoryId::from("supervision-repository");
        let run = RunId::from("supervision-run");
        store
            .create_repository(&NewRepository {
                id: repository.clone(),
                profile_id: "fixture".into(),
                profile_version: 1,
                display_name: "fixture".into(),
                root_path: temp.path().join("checkout"),
                origin_url: None,
                default_branch: "main".into(),
                expected_coordination_branch: None,
                state: "READY".into(),
            })
            .expect("repository persists");
        store
            .create_run(&NewRun {
                id: run.clone(),
                repository_id: repository,
                title: "supervision fixture".into(),
                objective: "exercise immutable snapshot custody".into(),
                mode: "plan_and_implement".into(),
                publication_mode: "none".into(),
                state: "CREATED".into(),
                phase: "created".into(),
                base_ref: "main".into(),
                base_sha: "a".repeat(40),
                authority_digest: "b".repeat(64),
                profile_digest: "c".repeat(64),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".into(),
                token_budget: None,
            })
            .expect("run persists");
        (temp, store, run)
    }

    #[test]
    fn snapshot_custody_is_immutable_and_idempotent_at_an_event_cursor() {
        let (_temp, store, run) = fixture();
        let first = store
            .record_supervisor_snapshot(&run, 41, "attempt_failed", |id, revision| {
                Ok(serde_json::json!({
                    "schema": SUPERVISOR_SNAPSHOT_SCHEMA,
                    "snapshot_id": id,
                    "run_id": run,
                    "revision": revision,
                    "event_cursor": 41,
                    "value": "first",
                }))
            })
            .expect("snapshot persists");
        assert_eq!(first.revision, 1);
        assert_eq!(store.supervisor_observation_cursor(&run).unwrap(), 41);
        assert_eq!(store.check().unwrap().schema_version, "12");

        let duplicate = store
            .record_supervisor_snapshot(&run, 41, "task_stalled", |_, _| {
                panic!("idempotent replay must not rebuild an immutable snapshot")
            })
            .expect("same cursor reads existing snapshot");
        assert_eq!(duplicate.id, first.id);
        assert_eq!(duplicate.revision, 1);
        assert_eq!(duplicate.trigger_kind, "attempt_failed");

        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute(
                    "UPDATE supervisor_snapshots SET trigger_kind='rewritten' WHERE id=?1",
                    [first.id.as_str()],
                )
                .is_err()
        );
        assert!(
            connection
                .execute(
                    "DELETE FROM supervisor_snapshots WHERE id=?1",
                    [first.id.as_str()],
                )
                .is_err()
        );
        // Simulate off-path database tampering after explicitly removing the
        // production immutability guard. Reads still must fail closed on the
        // independent byte/digest receipt.
        connection
            .execute_batch("DROP TRIGGER supervisor_snapshots_no_update;")
            .expect("test can remove guard");
        connection
            .execute(
                "UPDATE supervisor_snapshots SET payload_json='{}' WHERE id=?1",
                [first.id.as_str()],
            )
            .expect("test corruption writes");
        drop(connection);
        assert!(store.latest_supervisor_snapshot(&run).is_err());
    }

    #[test]
    fn malformed_snapshot_binding_is_rejected_without_advancing_the_cursor() {
        let (_temp, store, run) = fixture();
        let error = store
            .record_supervisor_snapshot(&run, 7, "attempt_failed", |_, _| {
                Ok(serde_json::json!({"schema": SUPERVISOR_SNAPSHOT_SCHEMA}))
            })
            .expect_err("binding mismatch must fail closed");
        assert!(error.to_string().contains("immutable envelope"));
        assert_eq!(store.supervisor_observation_cursor(&run).unwrap(), 0);
        assert!(store.latest_supervisor_snapshot(&run).unwrap().is_none());
    }
}
