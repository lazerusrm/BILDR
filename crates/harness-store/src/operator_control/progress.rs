//! Deterministic, append-only material-progress classifier.
//!
//! This repository deliberately recognizes a small allow-list of controller
//! events. Routine agent activity, output, tokens, and unrecognized event
//! names cannot become progress by accident.

use harness_domain::{MaterialProgressEvent, MaterialProgressEventId, MaterialProgressKind};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const CLASSIFIER_VERSION: &str = "material-progress-v1";
const MAX_PROGRESS_PAGE_SIZE: u32 = 200;

#[derive(Debug)]
struct SourceEvent {
    id: i64,
    run_id: Option<String>,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    occurred_at_ms: i64,
    payload: Value,
}

impl Store {
    /// Advances the closed material-progress classifier over previously
    /// unclassified controller events. A durable cursor makes a normal read
    /// path bounded by the new event suffix, not the lifetime event ledger.
    /// The source event id and kind remain the immutable idempotency key, so a
    /// crash before commit simply retries the same suffix.
    pub fn classify_material_progress(&self) -> Result<Vec<MaterialProgressEvent>, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cursor: i64 = transaction.query_row(
            "SELECT event_cursor FROM material_progress_classifier_state WHERE id=1",
            [],
            |row| row.get(0),
        )?;
        if cursor < 0 {
            return Err(StoreError::Validation(
                "material progress classifier cursor is negative".to_owned(),
            ));
        }
        let events = {
            let mut statement = transaction.prepare(
                "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json FROM domain_events WHERE id>?1 ORDER BY id ASC",
            )?;
            statement
                .query_map([cursor], |row| {
                    Ok(SourceEvent {
                        id: row.get(0)?,
                        run_id: row.get(1)?,
                        aggregate_type: row.get(2)?,
                        aggregate_id: row.get(3)?,
                        event_type: row.get(4)?,
                        occurred_at_ms: row.get(5)?,
                        payload: serde_json::from_str(&row.get::<_, String>(6)?).map_err(
                            |error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    6,
                                    rusqlite::types::Type::Text,
                                    Box::new(error),
                                )
                            },
                        )?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut classified = Vec::new();
        let mut advanced_cursor = cursor;
        for event in events {
            advanced_cursor = event.id;
            let Some((kind, summary)) = classify_source_event(&event) else {
                continue;
            };
            let progress = material_progress_from_source(&event, kind, summary)?;
            let raw = serde_json::to_string(&progress)?;
            let existing: Option<(String, String)> = transaction
                .query_row(
                    "SELECT payload_json,payload_sha256 FROM material_progress_events WHERE kind=?1 AND source_event_id=?2",
                    params![kind_name(kind), progress.source_event_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((existing_raw, existing_digest)) = existing {
                let existing = checked_progress_row(existing_raw, existing_digest)?;
                if existing != progress {
                    return Err(StoreError::Conflict(format!(
                        "material progress classifier produced divergent content for {}",
                        progress.source_event_id
                    )));
                }
                classified.push(existing);
                continue;
            }
            transaction.execute(
                "INSERT INTO material_progress_events(id,run_id,task_id,attempt_id,kind,source_event_id,occurred_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    progress.event_id.as_str(),
                    progress.run_id,
                    progress.task_id,
                    progress.attempt_id,
                    kind_name(kind),
                    progress.source_event_id,
                    progress.occurred_at_ms,
                    raw,
                    digest(&serde_json::to_string(&progress)?),
                ],
            )?;
            classified.push(progress);
        }
        if advanced_cursor != cursor {
            transaction.execute(
                "UPDATE material_progress_classifier_state SET event_cursor=?1 WHERE id=1",
                [advanced_cursor],
            )?;
        }
        transaction.commit()?;
        Ok(classified)
    }

    pub fn list_material_progress(
        &self,
        run_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<MaterialProgressEvent>, StoreError> {
        if limit == 0 || limit > MAX_PROGRESS_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "material progress page limit must be 1..={MAX_PROGRESS_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let mut statement = if run_id.is_some() {
            connection.prepare(
                "SELECT payload_json,payload_sha256 FROM material_progress_events WHERE run_id=?1 ORDER BY occurred_at DESC,id DESC LIMIT ?2",
            )?
        } else {
            connection.prepare(
                "SELECT payload_json,payload_sha256 FROM material_progress_events ORDER BY occurred_at DESC,id DESC LIMIT ?1",
            )?
        };
        if let Some(run_id) = run_id {
            let rows = statement.query_map(params![run_id, i64::from(limit)], |row| {
                checked_progress_row(row.get(0)?, row.get(1)?)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        } else {
            let rows = statement.query_map([i64::from(limit)], |row| {
                checked_progress_row(row.get(0)?, row.get(1)?)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }
    }
}

fn classify_source_event(event: &SourceEvent) -> Option<(MaterialProgressKind, &'static str)> {
    match event.event_type.as_str() {
        "task.governor.candidate_materialized" => Some((
            MaterialProgressKind::CandidateChanged,
            "Controller materialized a candidate from the exact task worktree.",
        )),
        "task.verified" => Some((
            MaterialProgressKind::ValidationAdvanced,
            "Independent verifier recorded a task validation outcome.",
        )),
        "run.integration.prepared" => Some((
            MaterialProgressKind::EvidenceRecorded,
            "Controller prepared an exact integration candidate with custody evidence.",
        )),
        "task.governor.progress_updated"
            if event
                .payload
                .get("completed_milestones")
                .and_then(Value::as_u64)
                .is_some_and(|completed| completed > 0) =>
        {
            Some((
                MaterialProgressKind::EvidenceRecorded,
                "Controller checkpoint records one or more completed planned milestones.",
            ))
        }
        _ => None,
    }
}

fn material_progress_from_source(
    source: &SourceEvent,
    kind: MaterialProgressKind,
    summary: &'static str,
) -> Result<MaterialProgressEvent, StoreError> {
    let source_event_id = format!("domain-event-{}", source.id);
    let event_id =
        MaterialProgressEventId::parse(format!("progress-{}-{}", kind_name(kind), source.id))
            .map_err(|error| StoreError::Validation(error.to_string()))?;
    let task_id = (source.aggregate_type == "task").then(|| source.aggregate_id.clone());
    let attempt_id = source
        .payload
        .get("attempt_id")
        .and_then(Value::as_str)
        .filter(|value| is_identifier(value))
        .map(str::to_owned);
    let candidate_sha = source
        .payload
        .get("tree_sha")
        .or_else(|| source.payload.get("head_sha"))
        .and_then(Value::as_str)
        .filter(|value| is_sha(value))
        .map(str::to_owned);
    let milestone_refs = source
        .payload
        .get("current_milestone_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| vec![format!("milestone:{value}")])
        .unwrap_or_default();
    let mut progress = MaterialProgressEvent {
        schema: "harness.material-progress.v1".to_owned(),
        event_id,
        run_id: source.run_id.clone(),
        task_id,
        attempt_id,
        kind,
        source_event_id: source_event_id.clone(),
        occurred_at_ms: source.occurred_at_ms,
        classifier_version: CLASSIFIER_VERSION.to_owned(),
        summary: summary.to_owned(),
        evidence_refs: vec![source_event_id],
        candidate_sha,
        milestone_refs,
        sha256: String::new(),
    };
    progress.sha256 = progress
        .digest()
        .map_err(|error| StoreError::Validation(error.to_string()))?;
    progress
        .validate()
        .map_err(|error| StoreError::Validation(error.to_string()))?;
    Ok(progress)
}

pub(crate) fn checked_progress_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<MaterialProgressEvent> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "material progress payload integrity check failed".into(),
        ));
    }
    let progress: MaterialProgressEvent = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    progress.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(progress)
}

fn kind_name(kind: MaterialProgressKind) -> &'static str {
    match kind {
        MaterialProgressKind::CandidateChanged => "candidate_changed",
        MaterialProgressKind::ValidationAdvanced => "validation_advanced",
        MaterialProgressKind::EvidenceRecorded => "evidence_recorded",
        MaterialProgressKind::ExternalConditionChanged => "external_condition_changed",
        MaterialProgressKind::ReconciliationAdvanced => "reconciliation_advanced",
        MaterialProgressKind::AttentionChanged => "attention_changed",
    }
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn is_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn classifier_is_deterministic_and_rejects_activity_noise() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        store
            .emit_domain_event(
                None,
                "agent",
                "agent-a",
                "agent.output.delta",
                &json!({"tokens": 1000}),
                None,
            )
            .expect("noise");
        store
            .emit_domain_event(
                None,
                "task",
                "task-a",
                "task.governor.candidate_materialized",
                &json!({"attempt_id": "attempt-a", "tree_sha": "a".repeat(40)}),
                None,
            )
            .expect("candidate");
        store
            .emit_domain_event(
                None,
                "task",
                "task-a",
                "task.governor.progress_updated",
                &json!({"completed_milestones": 0}),
                None,
            )
            .expect("empty checkpoint");
        let first = store.classify_material_progress().expect("classify");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].kind, MaterialProgressKind::CandidateChanged);
        let replay = store.classify_material_progress().expect("replay");
        assert!(
            replay.is_empty(),
            "a fully classified ledger is not replayed"
        );
        assert_eq!(store.list_material_progress(None, 10).unwrap(), first);
    }

    #[test]
    fn classifier_advances_a_durable_suffix_cursor() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        for index in 0..3 {
            store
                .emit_domain_event(
                    None,
                    "agent",
                    format!("agent-{index}"),
                    "agent.output.delta",
                    &json!({"tokens": index}),
                    None,
                )
                .expect("noise");
        }
        store
            .emit_domain_event(
                None,
                "task",
                "task-first",
                "task.governor.candidate_materialized",
                &json!({"tree_sha": "a".repeat(40)}),
                None,
            )
            .expect("first material event");
        let first = store.classify_material_progress().expect("first suffix");
        assert_eq!(first.len(), 1);
        store
            .emit_domain_event(
                None,
                "task",
                "task-second",
                "task.verified",
                &json!({}),
                None,
            )
            .expect("second material event");
        let second = store.classify_material_progress().expect("second suffix");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].kind, MaterialProgressKind::ValidationAdvanced);
        let connection = store.connection().expect("connection");
        let cursor: i64 = connection
            .query_row(
                "SELECT event_cursor FROM material_progress_classifier_state WHERE id=1",
                [],
                |row| row.get(0),
            )
            .expect("cursor");
        let max_event: i64 = connection
            .query_row("SELECT max(id) FROM domain_events", [], |row| row.get(0))
            .expect("event cursor");
        assert_eq!(cursor, max_event);
        drop(connection);
        assert_eq!(store.list_material_progress(None, 10).unwrap().len(), 2);
    }

    #[test]
    fn classifier_cursor_survives_restart_and_serializes_concurrent_readers() {
        use std::sync::{Arc, Barrier};

        let temp = TempDir::new().expect("temp");
        let database = temp.path().join("harness.sqlite3");
        let store = Store::open(&database, &temp.path().join("artifacts-a")).expect("store");
        store
            .emit_domain_event(
                None,
                "task",
                "task-restart",
                "task.verified",
                &json!({}),
                None,
            )
            .expect("material event");
        assert_eq!(store.classify_material_progress().unwrap().len(), 1);
        drop(store);

        let restarted =
            Store::open(&database, &temp.path().join("artifacts-b")).expect("restarted store");
        assert!(restarted.classify_material_progress().unwrap().is_empty());
        restarted
            .emit_domain_event(
                None,
                "task",
                "task-concurrent",
                "task.verified",
                &json!({}),
                None,
            )
            .expect("new material event");
        let second =
            Store::open(&database, &temp.path().join("artifacts-c")).expect("second reader");
        let barrier = Arc::new(Barrier::new(2));
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            restarted
                .classify_material_progress()
                .expect("first classifier")
        });
        barrier.wait();
        let second_result = second
            .classify_material_progress()
            .expect("second classifier");
        let first_result = first.join().expect("classifier thread joins");
        assert_eq!(first_result.len() + second_result.len(), 1);
    }
}
