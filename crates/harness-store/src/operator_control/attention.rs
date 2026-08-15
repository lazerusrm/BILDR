use harness_domain::{
    AttentionItem, AttentionItemId, AttentionResolution, AttentionState, OperatorControlError,
    now_ms,
};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const MAX_ATTENTION_PAGE_SIZE: u32 = 200;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AttentionPage {
    pub items: Vec<AttentionItem>,
    pub includes_terminal: bool,
    pub next_cursor: Option<String>,
}

impl Store {
    /// Persists a newly opened source-owned attention item. The source identity
    /// and revision are the idempotency key; an identical retry is safe, while
    /// a changed payload at the same revision fails closed.
    pub fn upsert_source_attention(
        &self,
        item: &AttentionItem,
    ) -> Result<AttentionItem, StoreError> {
        item.validate().map_err(control_error)?;
        if item.state != AttentionState::Open
            || item.version != 1
            || item.acknowledged_at_ms.is_some()
            || item.resolution.is_some()
        {
            return Err(StoreError::Validation(
                "a new source attention item must be open at version one without acknowledgement or resolution".to_owned(),
            ));
        }
        let raw = serde_json::to_string(item)?;
        let digest = digest(&raw);
        let source_type = enum_name(&item.source.source_type)?;
        let category = enum_name(&item.category)?;
        let severity = enum_name(&item.severity)?;
        let state = enum_name(&item.state)?;
        let source_revision = to_i64(item.source.source_revision, "attention source revision")?;
        let version = to_i64(item.version, "attention version")?;
        let now = now_ms();

        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some(existing_raw) = transaction
            .query_row(
                "SELECT payload_json FROM attention_items WHERE source_type=?1 AND source_id=?2 AND source_revision=?3",
                params![source_type, item.source.source_id, source_revision],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            let existing = attention_from_raw(&existing_raw)?;
            if existing_raw == raw {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict(format!(
                "attention source {}:{} revision {} already has a different payload",
                item.source.source_type as u8, item.source.source_id, item.source.source_revision
            )));
        }
        let active_for_source: i64 = transaction.query_row(
            "SELECT count(*) FROM attention_items WHERE source_type=?1 AND source_id=?2 AND state IN ('open','acknowledged','waiting_external')",
            params![source_type, item.source.source_id],
            |row| row.get(0),
        )?;
        if active_for_source != 0 {
            return Err(StoreError::Conflict(
                "a source revision cannot replace an active attention item; the source adapter must record a terminal authority receipt first".to_owned(),
            ));
        }
        transaction.execute(
            "INSERT INTO attention_items(id,source_type,source_id,source_revision,repository_id,run_id,task_id,category,severity,state,title,summary,dedupe_key,opened_event_id,opened_at,acknowledged_at,due_at,resurfacing_json,resolution_json,payload_json,payload_sha256,version,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,NULL,?16,?17,NULL,?18,?19,?20,?21,?21)",
            params![
                item.attention_id.as_str(), source_type, item.source.source_id, source_revision,
                item.repository_id, item.run_id, item.task_id, category, severity, state,
                item.title, item.summary, item.dedupe_key, item.opened_event_id, item.opened_at_ms,
                item.due_at_ms, serde_json::to_string(&item.resurfacing)?, raw, digest, version, now,
            ],
        )?;
        insert_attention_event(&transaction, item, "opened", None, &raw, &digest, now)?;
        transaction.commit()?;
        Ok(item.clone())
    }

    /// Acknowledgement is presentation state only: it cannot close, resolve,
    /// approve, or otherwise satisfy the attention source.
    pub fn acknowledge_attention(
        &self,
        attention_id: &AttentionItemId,
        expected_version: u64,
        acknowledged_at_ms: Option<i64>,
    ) -> Result<AttentionItem, StoreError> {
        let expected_version = to_i64(expected_version, "attention expected version")?;
        let now = acknowledged_at_ms.unwrap_or_else(now_ms);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let raw = transaction
            .query_row(
                "SELECT payload_json FROM attention_items WHERE id=?1",
                [attention_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("attention item {attention_id}")))?;
        let mut item = attention_from_raw(&raw)?;
        if to_i64(item.version, "attention version")? != expected_version {
            return Err(StoreError::Conflict(format!(
                "attention item {attention_id} has version {}, expected {expected_version}",
                item.version
            )));
        }
        item.state
            .validate_transition(AttentionState::Acknowledged, false)
            .map_err(control_error)?;
        item.state = AttentionState::Acknowledged;
        item.acknowledged_at_ms = Some(now);
        item.version = item
            .version
            .checked_add(1)
            .ok_or_else(|| StoreError::Validation("attention version overflow".to_owned()))?;
        item.validate().map_err(control_error)?;
        let updated_raw = serde_json::to_string(&item)?;
        let updated_digest = digest(&updated_raw);
        let changed = transaction.execute(
            "UPDATE attention_items SET state='acknowledged',acknowledged_at=?1,payload_json=?2,payload_sha256=?3,version=?4,updated_at=?1 WHERE id=?5 AND version=?6",
            params![
                now,
                updated_raw,
                updated_digest,
                to_i64(item.version, "attention version")?,
                attention_id.as_str(),
                expected_version,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "attention item {attention_id} changed during acknowledgement"
            )));
        }
        insert_attention_event(
            &transaction,
            &item,
            "acknowledged",
            Some(expected_version),
            &updated_raw,
            &updated_digest,
            now,
        )?;
        transaction.commit()?;
        Ok(item)
    }

    /// Only a source adapter with an explicit authority receipt may close an
    /// item. There is deliberately no generic resolve/close store API.
    pub fn resolve_attention_from_source(
        &self,
        attention_id: &AttentionItemId,
        expected_version: u64,
        next_state: AttentionState,
        resolution: AttentionResolution,
    ) -> Result<AttentionItem, StoreError> {
        if !next_state.is_terminal() {
            return Err(StoreError::Validation(
                "source attention closure requires a terminal state".to_owned(),
            ));
        }
        resolution.validate().map_err(control_error)?;
        let expected_version = to_i64(expected_version, "attention expected version")?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let raw = transaction
            .query_row(
                "SELECT payload_json FROM attention_items WHERE id=?1",
                [attention_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("attention item {attention_id}")))?;
        let mut item = attention_from_raw(&raw)?;
        if to_i64(item.version, "attention version")? != expected_version {
            return Err(StoreError::Conflict(format!(
                "attention item {attention_id} has changed since the source adapter observed it"
            )));
        }
        item.state
            .validate_transition(next_state, false)
            .map_err(control_error)?;
        item.state = next_state;
        item.resolution = Some(resolution);
        item.version = item
            .version
            .checked_add(1)
            .ok_or_else(|| StoreError::Validation("attention version overflow".to_owned()))?;
        item.validate().map_err(control_error)?;
        let updated_raw = serde_json::to_string(&item)?;
        let updated_digest = digest(&updated_raw);
        let state = enum_name(&item.state)?;
        let changed = transaction.execute(
            "UPDATE attention_items SET state=?1,resolution_json=?2,payload_json=?3,payload_sha256=?4,version=?5,updated_at=?6 WHERE id=?7 AND version=?8",
            params![
                state,
                serde_json::to_string(&item.resolution)?,
                updated_raw,
                updated_digest,
                to_i64(item.version, "attention version")?,
                now,
                attention_id.as_str(),
                expected_version,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "attention item {attention_id} changed during source closure"
            )));
        }
        let event_kind = match item.state {
            AttentionState::Resolved => "resolved",
            AttentionState::Declined => "declined",
            AttentionState::Superseded => "superseded",
            AttentionState::Invalidated => "invalidated",
            AttentionState::Open
            | AttentionState::Acknowledged
            | AttentionState::WaitingExternal => {
                unreachable!("terminal state was checked above")
            }
        };
        insert_attention_event(
            &transaction,
            &item,
            event_kind,
            Some(expected_version),
            &updated_raw,
            &updated_digest,
            now,
        )?;
        transaction.commit()?;
        Ok(item)
    }

    pub fn attention_item(
        &self,
        attention_id: &AttentionItemId,
    ) -> Result<Option<AttentionItem>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT payload_json,payload_sha256 FROM attention_items WHERE id=?1",
                [attention_id.as_str()],
                |row| checked_attention_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    pub(crate) fn attention_by_source(
        &self,
        source_type: &harness_domain::AttentionSourceType,
        source_id: &str,
    ) -> Result<Option<AttentionItem>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT payload_json,payload_sha256 FROM attention_items WHERE source_type=?1 AND source_id=?2 ORDER BY source_revision DESC LIMIT 1",
                params![enum_name(source_type)?, source_id],
                |row| checked_attention_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_attention(
        &self,
        include_terminal: bool,
        limit: u32,
    ) -> Result<AttentionPage, StoreError> {
        self.list_attention_page(include_terminal, limit, None)
    }

    pub fn list_attention_page(
        &self,
        include_terminal: bool,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<AttentionPage, StoreError> {
        if limit == 0 || limit > MAX_ATTENTION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "attention page limit must be 1..={MAX_ATTENTION_PAGE_SIZE}"
            )));
        }
        let cursor = cursor.map(parse_cursor).transpose()?;
        let connection = self.connection()?;
        let state_filter = if include_terminal {
            ""
        } else {
            "WHERE state IN ('open','acknowledged','waiting_external')"
        };
        let cursor_filter = if cursor.is_some() {
            if include_terminal {
                "WHERE (CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END > ?1 OR (CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END = ?1 AND (opened_at < ?2 OR (opened_at = ?2 AND id < ?3))))"
            } else {
                "WHERE state IN ('open','acknowledged','waiting_external') AND (CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END > ?1 OR (CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END = ?1 AND (opened_at < ?2 OR (opened_at = ?2 AND id < ?3))))"
            }
        } else {
            state_filter
        };
        let sql = format!(
            "SELECT payload_json,payload_sha256 FROM attention_items {cursor_filter} ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END, opened_at DESC, id DESC LIMIT ?{}",
            if cursor.is_some() { 4 } else { 1 }
        );
        let mut statement = connection.prepare(&sql)?;
        let requested = i64::from(limit) + 1;
        let mut items = if let Some(cursor) = &cursor {
            statement
                .query_map(
                    params![
                        cursor.severity_rank,
                        cursor.opened_at_ms,
                        cursor.attention_id,
                        requested
                    ],
                    |row| checked_attention_row(row.get(0)?, row.get(1)?),
                )?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map([requested], |row| {
                    checked_attention_row(row.get(0)?, row.get(1)?)
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let next_cursor = if items.len() > limit as usize {
            let next = items.pop().expect("page length exceeded limit");
            items
                .last()
                .map(format_cursor)
                .or_else(|| Some(format_cursor(&next)))
        } else {
            None
        };
        Ok(AttentionPage {
            items,
            includes_terminal: include_terminal,
            next_cursor,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AttentionCursor {
    severity_rank: i64,
    opened_at_ms: i64,
    attention_id: String,
}

fn parse_cursor(value: &str) -> Result<AttentionCursor, StoreError> {
    let mut parts = value.splitn(3, ':');
    let severity_rank = parts
        .next()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| (0..=3).contains(value));
    let opened_at_ms = parts.next().and_then(|value| value.parse::<i64>().ok());
    let attention_id = parts
        .next()
        .and_then(|value| AttentionItemId::parse(value).ok());
    match (severity_rank, opened_at_ms, attention_id) {
        (Some(severity_rank), Some(opened_at_ms), Some(attention_id)) => Ok(AttentionCursor {
            severity_rank,
            opened_at_ms,
            attention_id: attention_id.to_string(),
        }),
        _ => Err(StoreError::Validation(
            "attention cursor is malformed".to_owned(),
        )),
    }
}

fn format_cursor(item: &AttentionItem) -> String {
    format!(
        "{}:{}:{}",
        severity_rank(item),
        item.opened_at_ms,
        item.attention_id
    )
}

fn severity_rank(item: &AttentionItem) -> i64 {
    match item.severity {
        harness_domain::AttentionSeverity::Critical => 0,
        harness_domain::AttentionSeverity::High => 1,
        harness_domain::AttentionSeverity::Normal => 2,
        harness_domain::AttentionSeverity::Info => 3,
    }
}

fn insert_attention_event(
    transaction: &rusqlite::Transaction<'_>,
    item: &AttentionItem,
    event_kind: &str,
    expected_version: Option<i64>,
    raw: &str,
    payload_sha256: &str,
    created_at: i64,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO attention_events(attention_id,source_type,source_id,source_revision,event_kind,expected_version,resulting_version,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            item.attention_id.as_str(),
            enum_name(&item.source.source_type)?,
            item.source.source_id,
            to_i64(item.source.source_revision, "attention source revision")?,
            event_kind,
            expected_version,
            to_i64(item.version, "attention version")?,
            raw,
            payload_sha256,
            created_at,
        ],
    )?;
    Ok(())
}

fn attention_from_raw(raw: &str) -> Result<AttentionItem, StoreError> {
    let item: AttentionItem = serde_json::from_str(raw)?;
    item.validate().map_err(control_error)?;
    Ok(item)
}

pub(crate) fn checked_attention_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<AttentionItem> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "attention payload integrity check failed".into(),
        ));
    }
    attention_from_raw(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn enum_name(value: &impl Serialize) -> Result<String, StoreError> {
    let encoded = serde_json::to_string(value)?;
    encoded
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            StoreError::Validation("operator-control enum did not serialize as a string".to_owned())
        })
}

fn to_i64(value: u64, field: &str) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::Validation(format!("{field} exceeds SQLite integer range")))
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

fn control_error(error: OperatorControlError) -> StoreError {
    StoreError::Validation(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use harness_domain::{
        AttentionCategory, AttentionResurfacingPolicy, AttentionSeverity, AttentionSourceRef,
        AttentionSourceType,
    };
    use tempfile::TempDir;

    use super::*;

    fn item(source_revision: u64) -> AttentionItem {
        AttentionItem {
            schema: "harness.attention-item.v1".to_owned(),
            attention_id: AttentionItemId::new(),
            repository_id: Some("repo_a".to_owned()),
            run_id: Some("run_a".to_owned()),
            task_id: Some("task_a".to_owned()),
            source: AttentionSourceRef {
                source_type: AttentionSourceType::Approval,
                source_id: "approval_a".to_owned(),
                source_revision,
            },
            category: AttentionCategory::Approval,
            severity: AttentionSeverity::High,
            state: AttentionState::Open,
            title: "Approval required".to_owned(),
            summary: "A bound approval is required before execution.".to_owned(),
            option_refs: vec![],
            evidence_refs: vec![],
            blocked_refs: vec!["task_a".to_owned()],
            dedupe_key: "approval_task_a".to_owned(),
            opened_event_id: "event_a".to_owned(),
            opened_at_ms: 1,
            acknowledged_at_ms: None,
            due_at_ms: None,
            resurfacing: AttentionResurfacingPolicy {
                policy: "until_authority_receipt".to_owned(),
                maximum_defer_ms: 60_000,
            },
            resolution: None,
            version: 1,
        }
    }

    fn store(temp: &TempDir) -> Store {
        Store::in_memory(Path::new(temp.path()).join("artifacts").as_path()).expect("store")
    }

    #[test]
    fn source_open_is_idempotent_and_acknowledgement_is_versioned_only() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let attention = item(1);
        assert_eq!(
            store.upsert_source_attention(&attention).unwrap(),
            attention
        );
        assert_eq!(
            store.upsert_source_attention(&attention).unwrap(),
            attention
        );
        let acknowledged = store
            .acknowledge_attention(&attention.attention_id, 1, Some(7))
            .unwrap();
        assert_eq!(acknowledged.state, AttentionState::Acknowledged);
        assert_eq!(acknowledged.version, 2);
        assert!(acknowledged.resolution.is_none());
        assert!(matches!(
            store.acknowledge_attention(&attention.attention_id, 1, Some(8)),
            Err(StoreError::Conflict(_))
        ));
        assert_eq!(
            store.list_attention(false, 10).unwrap().items,
            vec![acknowledged]
        );
    }

    #[test]
    fn a_source_revision_cannot_replace_an_active_attention_item() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let first = item(1);
        store.upsert_source_attention(&first).unwrap();
        let mut next = item(2);
        next.dedupe_key = "approval_task_a_new".to_owned();
        assert!(matches!(
            store.upsert_source_attention(&next),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn attention_event_history_is_immutable() {
        let temp = TempDir::new().expect("temp");
        let store = store(&temp);
        let attention = item(1);
        store
            .upsert_source_attention(&attention)
            .expect("open attention");
        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute("UPDATE attention_events SET event_kind='resolved'", [])
                .is_err()
        );
        assert!(
            connection
                .execute("DELETE FROM attention_events", [])
                .is_err()
        );
    }
}
