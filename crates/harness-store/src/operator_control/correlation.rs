use harness_domain::CorrelationLink;
use harness_trace::validate_causal_links;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

impl Store {
    /// Inserts a correlation receipt only after checking the entire bounded
    /// per-trace causal graph. The database uniqueness key makes identical
    /// event replays harmless and rejects divergent duplicate causality.
    pub fn record_correlation_link(
        &self,
        link: &CorrelationLink,
    ) -> Result<CorrelationLink, StoreError> {
        link.validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let raw = serde_json::to_string(link)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some(existing_raw) = transaction
            .query_row(
                "SELECT payload_json FROM correlation_links WHERE id=?1",
                [link.link_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            if existing_raw == raw {
                transaction.commit()?;
                return Ok(link.clone());
            }
            return Err(StoreError::Conflict(
                "correlation link id already has a different immutable payload".to_owned(),
            ));
        }
        let mut links = {
            let mut statement = transaction.prepare(
                "SELECT payload_json,payload_sha256 FROM correlation_links WHERE trace_id=?1 ORDER BY created_at,id LIMIT 8192",
            )?;
            let rows = statement.query_map([link.trace.trace_id.as_str()], |row| {
                checked_link_row(row.get(0)?, row.get(1)?)
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        links.push(link.clone());
        validate_causal_links(&links).map_err(|error| StoreError::Validation(error.to_string()))?;
        let relation = safe_enum_name(&link.relation)?;
        transaction.execute(
            "INSERT INTO correlation_links(id,trace_id,span_id,parent_span_id,from_kind,from_id,to_kind,to_id,relation,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                link.link_id.as_str(), link.trace.trace_id, link.trace.span_id,
                link.trace.parent_span_id, link.from_kind, link.from_id, link.to_kind,
                link.to_id, relation, raw, payload_sha256, link.created_at_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(link.clone())
    }

    pub fn correlation_links(
        &self,
        trace_id: &str,
        limit: u32,
    ) -> Result<Vec<CorrelationLink>, StoreError> {
        if !is_trace_id(trace_id) || limit == 0 || limit > 8_192 {
            return Err(StoreError::Validation(
                "correlation trace id or page limit is invalid".to_owned(),
            ));
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT payload_json,payload_sha256 FROM correlation_links WHERE trace_id=?1 ORDER BY created_at,id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![trace_id, i64::from(limit)], |row| {
            checked_link_row(row.get(0)?, row.get(1)?)
        })?;
        let links = rows.collect::<Result<Vec<_>, _>>()?;
        validate_causal_links(&links).map_err(|error| StoreError::Validation(error.to_string()))?;
        Ok(links)
    }
}

fn checked_link_row(raw: String, payload_sha256: String) -> rusqlite::Result<CorrelationLink> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "correlation payload integrity check failed".into(),
        ));
    }
    let link: CorrelationLink = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    link.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(link)
}

fn safe_enum_name(value: &str) -> Result<&str, StoreError> {
    if value.is_empty()
        || value.len() > 160
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(StoreError::Validation(
            "correlation relation must be a bounded path-safe identifier".to_owned(),
        ));
    }
    Ok(value)
}

fn is_trace_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use harness_domain::{CorrelationLinkId, TraceContext};
    use tempfile::TempDir;

    use super::*;

    fn link(id: CorrelationLinkId, from: &str, to: &str) -> CorrelationLink {
        CorrelationLink {
            schema: "harness.correlation-link.v1".to_owned(),
            link_id: id,
            trace: TraceContext {
                trace_id: "a".repeat(32),
                span_id: "b".repeat(16),
                parent_span_id: None,
            },
            from_kind: "event".to_owned(),
            from_id: from.to_owned(),
            to_kind: "attention".to_owned(),
            to_id: to.to_owned(),
            relation: "derived_from".to_owned(),
            created_at_ms: 1,
        }
    }

    #[test]
    fn correlation_receipts_are_immutable_and_cycle_checked() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let first = link(CorrelationLinkId::new(), "event_a", "item_b");
        assert_eq!(store.record_correlation_link(&first).unwrap(), first);
        assert_eq!(store.record_correlation_link(&first).unwrap(), first);
        let cycle = CorrelationLink {
            from_kind: "attention".to_owned(),
            from_id: "item_b".to_owned(),
            to_kind: "event".to_owned(),
            to_id: "event_a".to_owned(),
            ..link(CorrelationLinkId::new(), "unused", "unused")
        };
        assert!(matches!(
            store.record_correlation_link(&cycle),
            Err(StoreError::Validation(_))
        ));
    }
}
