//! Presence and notification-mirror custody.
//!
//! This first delivery slice records what the product has presented in its
//! local control plane. It does not suppress an attention item, send desktop
//! notifications, or update a source-owned lifecycle.

use harness_domain::{
    AttentionItem, AttentionSeverity, CorrelationLink, CorrelationLinkId, NotificationClass,
    NotificationDelivery, NotificationDeliveryHealth, NotificationDeliveryId, NotificationState,
    OperatorPresence, OperatorPresenceMode, TraceContext, now_ms,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

use super::correlation::record_correlation_link_in_transaction;

const MAX_NOTIFICATION_PAGE_SIZE: u32 = 200;
const MAX_NOTIFICATION_HEALTH_ROWS: u32 = 200;

impl Store {
    pub fn operator_presence(&self, operator_id: &str) -> Result<OperatorPresence, StoreError> {
        validate_operator(operator_id)?;
        let existing = self
            .connection()?
            .query_row(
                "SELECT payload_json,payload_sha256 FROM operator_presence WHERE operator_id=?1",
                [operator_id],
                |row| checked_presence_row(row.get(0)?, row.get(1)?),
            )
            .optional()?;
        Ok(existing.unwrap_or_else(|| default_presence(operator_id)))
    }

    /// Presence changes are optimistic-concurrency checked presentation
    /// preferences. Mirror delivery has no behavior change for any mode.
    pub fn set_operator_presence(
        &self,
        operator_id: &str,
        mode: OperatorPresenceMode,
        expected_version: u64,
    ) -> Result<OperatorPresence, StoreError> {
        validate_operator(operator_id)?;
        // Read and write under one immediate transaction. A missing row still
        // has the canonical version-one default, so competing first writes
        // serialize into a success and a normal optimistic-version conflict.
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM operator_presence WHERE operator_id=?1",
                [operator_id],
                |row| checked_presence_row(row.get(0)?, row.get(1)?),
            )
            .optional()?
            .unwrap_or_else(|| default_presence(operator_id));
        if current.version != expected_version {
            return Err(StoreError::Conflict(format!(
                "operator presence has version {}, expected {expected_version}",
                current.version
            )));
        }
        let mut next = OperatorPresence {
            mode,
            version: current.version.checked_add(1).ok_or_else(|| {
                StoreError::Validation("operator presence version overflow".to_owned())
            })?,
            updated_at_ms: now_ms(),
            ..current
        };
        next.sha256 = next.digest().map_err(control_error)?;
        next.validate().map_err(control_error)?;
        let raw = serde_json::to_string(&next)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM operator_presence WHERE operator_id=?1",
                [operator_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            transaction.execute("INSERT INTO operator_presence(operator_id,mode,version,updated_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6)", params![operator_id, mode_name(mode), to_i64(next.version)?, next.updated_at_ms, raw, digest(&serde_json::to_string(&next)?)])?;
        } else {
            let changed = transaction.execute("UPDATE operator_presence SET mode=?1,version=?2,updated_at=?3,payload_json=?4,payload_sha256=?5 WHERE operator_id=?6 AND version=?7", params![mode_name(mode), to_i64(next.version)?, next.updated_at_ms, raw, digest(&serde_json::to_string(&next)?), operator_id, to_i64(expected_version)?])?;
            if changed != 1 {
                return Err(StoreError::Conflict(
                    "operator presence changed during update".to_owned(),
                ));
            }
        }
        transaction.commit()?;
        Ok(next)
    }

    /// Mirrors one bounded oldest-first batch of as-yet-undelivered
    /// source-owned attention revisions into append-only product delivery
    /// receipts. Selecting only pending revisions prevents a stable newest
    /// page from starving an older item forever; the deterministic receipt ID
    /// still makes retry and restart safe.
    pub fn refresh_notification_mirror(&self) -> Result<Vec<NotificationDelivery>, StoreError> {
        let attention = self.pending_notification_attention(MAX_NOTIFICATION_PAGE_SIZE)?;
        let mut written = Vec::new();
        for item in attention {
            let receipt = notification_from_attention(&item)?;
            written.push(self.record_notification_delivery(&receipt)?);
        }
        Ok(written)
    }

    fn pending_notification_attention(&self, limit: u32) -> Result<Vec<AttentionItem>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT attention.payload_json,attention.payload_sha256
             FROM attention_items AS attention
             WHERE attention.state IN ('open','acknowledged','waiting_external')
               AND NOT EXISTS (
                 SELECT 1
                 FROM notification_deliveries AS delivery
                 WHERE delivery.source_event_id =
                   ('attention-' || attention.id || '-' || attention.version)
               )
             ORDER BY
               CASE attention.severity
                 WHEN 'critical' THEN 0
                 WHEN 'high' THEN 1
                 WHEN 'normal' THEN 2
                 ELSE 3
               END,
               attention.opened_at ASC,
               attention.id ASC
             LIMIT ?1",
        )?;
        Ok(statement
            .query_map([i64::from(limit)], |row| {
                super::attention::checked_attention_row(row.get(0)?, row.get(1)?)
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn record_notification_delivery(
        &self,
        delivery: &NotificationDelivery,
    ) -> Result<NotificationDelivery, StoreError> {
        delivery.validate().map_err(control_error)?;
        let raw = serde_json::to_string(delivery)?;
        let payload_sha256 = digest(&raw);
        let correlation = notification_correlation_link(delivery)?;
        let mut connection = self.connection()?;
        // A refresh first reads a bounded pending page, so two processes may
        // reach this receipt write for the same revision. Acquire the writer
        // slot before the idempotency read rather than attempting a deferred
        // read-to-write upgrade, which SQLite can reject as `database is
        // locked` despite the immutable replay being safe.
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some((existing_raw, existing_digest)) = transaction.query_row(
            "SELECT payload_json,payload_sha256 FROM notification_deliveries WHERE source_event_id=?1",
            [delivery.source_event_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        ).optional()? {
            let existing = checked_delivery_row(existing_raw, existing_digest)?;
            if existing == *delivery {
                record_correlation_link_in_transaction(&transaction, &correlation)?;
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::Conflict("notification source event already has different content".to_owned()));
        }
        transaction.execute("INSERT INTO notification_deliveries(id,attention_id,class,state,source_event_id,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", params![delivery.delivery_id.as_str(), delivery.attention_id.as_ref().map(|id| id.as_str()), class_name(delivery.class), state_name(delivery.state), delivery.source_event_id, raw, payload_sha256, delivery.created_at_ms])?;
        record_correlation_link_in_transaction(&transaction, &correlation)?;
        transaction.commit()?;
        Ok(delivery.clone())
    }

    pub fn list_notification_deliveries(
        &self,
        limit: u32,
    ) -> Result<Vec<NotificationDelivery>, StoreError> {
        if limit == 0 || limit > MAX_NOTIFICATION_PAGE_SIZE {
            return Err(StoreError::Validation(format!(
                "notification page limit must be 1..={MAX_NOTIFICATION_PAGE_SIZE}"
            )));
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT payload_json,payload_sha256 FROM notification_deliveries ORDER BY created_at DESC,id DESC LIMIT ?1")?;
        Ok(statement
            .query_map([i64::from(limit)], |row| {
                checked_delivery_row(row.get(0)?, row.get(1)?)
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Computes a bounded health projection for the current source-owned
    /// attention revisions without refreshing the mirror or changing any
    /// delivery state. A caller must not interpret a truncated result as a
    /// whole-system delivery guarantee.
    pub fn notification_delivery_health(&self) -> Result<NotificationDeliveryHealth, StoreError> {
        // The count and bounded rows must share one SQLite read snapshot. A
        // concurrent source update between two autocommit reads could otherwise
        // make the health counters contradict their own truncation flag.
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let current_attention_revisions = non_negative_u64(
            transaction.query_row(
                "SELECT count(*) FROM attention_items WHERE state IN ('open','acknowledged','waiting_external')",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            "current notification attention count",
        )?;
        let rows = {
            let mut statement = transaction.prepare(
                "SELECT attention.payload_json,attention.payload_sha256,delivery.payload_json,delivery.payload_sha256
                 FROM attention_items AS attention
                 LEFT JOIN notification_deliveries AS delivery
                   ON delivery.source_event_id = ('attention-' || attention.id || '-' || attention.version)
                 WHERE attention.state IN ('open','acknowledged','waiting_external')
                 ORDER BY
                   CASE attention.severity
                     WHEN 'critical' THEN 0
                     WHEN 'high' THEN 1
                     WHEN 'normal' THEN 2
                     ELSE 3
                   END,
                   attention.opened_at ASC,
                   attention.id ASC
                 LIMIT ?1",
            )?;
            statement
                .query_map([i64::from(MAX_NOTIFICATION_HEALTH_ROWS)], |row| {
                    let attention =
                        super::attention::checked_attention_row(row.get(0)?, row.get(1)?)?;
                    let delivery_raw: Option<String> = row.get(2)?;
                    let delivery_digest: Option<String> = row.get(3)?;
                    let delivery = match (delivery_raw, delivery_digest) {
                        (None, None) => None,
                        (Some(raw), Some(digest)) => Some(checked_delivery_row(raw, digest)?),
                        _ => {
                            return Err(rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                "notification delivery join has incomplete immutable payload"
                                    .into(),
                            ));
                        }
                    };
                    Ok((attention, delivery))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        transaction.commit()?;
        let mut health = NotificationDeliveryHealth {
            schema: "harness.notification-delivery-health.v1".to_owned(),
            channel: "in_product_mirror".to_owned(),
            current_attention_revisions,
            examined_current_revisions: rows.len() as u64,
            delivered_examined_revisions: 0,
            undelivered_examined_revisions: 0,
            undelivered_critical_examined_revisions: 0,
            undelivered_action_required_examined_revisions: 0,
            failed_examined_revisions: 0,
            unverified_delivery_examined_revisions: 0,
            oldest_undelivered_opened_at_ms: None,
            latest_verified_mirror_receipt_at_ms: None,
            truncated: (rows.len() as u64) < current_attention_revisions,
            desktop_delivery_enabled: false,
            batching_enabled: false,
            suppression_enabled: false,
        };
        for (attention, delivery) in rows {
            let verified = delivery.as_ref().is_some_and(|delivery| {
                notification_delivery_matches_attention(delivery, &attention)
            });
            if verified {
                health.delivered_examined_revisions += 1;
                let delivery = delivery.expect("verified delivery is present");
                health.latest_verified_mirror_receipt_at_ms = Some(
                    health
                        .latest_verified_mirror_receipt_at_ms
                        .map_or(delivery.created_at_ms, |current| {
                            current.max(delivery.created_at_ms)
                        }),
                );
                continue;
            }
            health.undelivered_examined_revisions += 1;
            health.oldest_undelivered_opened_at_ms = Some(
                health
                    .oldest_undelivered_opened_at_ms
                    .map_or(attention.opened_at_ms, |current| {
                        current.min(attention.opened_at_ms)
                    }),
            );
            match attention.severity {
                AttentionSeverity::Critical => health.undelivered_critical_examined_revisions += 1,
                AttentionSeverity::High => {
                    health.undelivered_action_required_examined_revisions += 1
                }
                AttentionSeverity::Normal | AttentionSeverity::Info => {}
            }
            if let Some(delivery) = delivery {
                health.unverified_delivery_examined_revisions += 1;
                if delivery.state == NotificationState::Failed {
                    health.failed_examined_revisions += 1;
                }
            }
        }
        health.validate().map_err(control_error)?;
        Ok(health)
    }
}

fn default_presence(operator_id: &str) -> OperatorPresence {
    let mut presence = OperatorPresence {
        schema: "harness.operator-presence.v1".to_owned(),
        operator_id: operator_id.to_owned(),
        mode: OperatorPresenceMode::Interactive,
        version: 1,
        updated_at_ms: 0,
        sha256: String::new(),
    };
    presence.sha256 = presence.digest().expect("presence serialization");
    presence
}
fn notification_from_attention(item: &AttentionItem) -> Result<NotificationDelivery, StoreError> {
    let source_event_id = format!("attention-{}-{}", item.attention_id, item.version);
    let class = match item.severity {
        AttentionSeverity::Critical => NotificationClass::Critical,
        AttentionSeverity::High => NotificationClass::ActionRequired,
        AttentionSeverity::Normal | AttentionSeverity::Info => NotificationClass::Routine,
    };
    let mut delivery = NotificationDelivery {
        schema: "harness.notification-delivery.v1".to_owned(),
        delivery_id: NotificationDeliveryId::parse(format!(
            "notification-{}-{}",
            item.attention_id, item.version
        ))
        .map_err(control_error)?,
        attention_id: Some(item.attention_id.clone()),
        class,
        state: NotificationState::Delivered,
        channel: "in_product_mirror".to_owned(),
        source_event_id,
        // A delivery is an immutable mirror of one exact attention revision.
        // Its source event ID is deterministic, so every field must be too:
        // using the local refresh clock would turn a safe retry into a custody
        // conflict after a restart or a second snapshot read.
        created_at_ms: item.opened_at_ms,
        payload_sha256: digest(&serde_json::to_string(item)?),
        sha256: String::new(),
    };
    delivery.sha256 = delivery.digest().map_err(control_error)?;
    delivery.validate().map_err(control_error)?;
    Ok(delivery)
}

fn notification_delivery_matches_attention(
    delivery: &NotificationDelivery,
    attention: &AttentionItem,
) -> bool {
    notification_from_attention(attention).is_ok_and(|expected| *delivery == expected)
}

/// Derives a controller-owned correlation root from the immutable receipt ID.
/// No request/client-provided trace context crosses this local projection
/// boundary. The deterministic IDs make a retry repair a missing link without
/// creating a second causal claim.
fn notification_correlation_link(
    delivery: &NotificationDelivery,
) -> Result<CorrelationLink, StoreError> {
    let trace_id = digest(&format!(
        "harness.notification-delivery.trace.v1:{}",
        delivery.delivery_id
    ));
    let span_id = digest(&format!(
        "harness.notification-delivery.span.v1:{}",
        delivery.delivery_id
    ));
    let link_id = CorrelationLinkId::parse(format!(
        "correlation-{}",
        &digest(&format!(
            "harness.notification-delivery.link.v1:{}",
            delivery.delivery_id
        ))[..48]
    ))
    .map_err(control_error)?;
    let (from_kind, from_id) = delivery.attention_id.as_ref().map_or_else(
        || {
            (
                "notification_source".to_owned(),
                delivery.source_event_id.clone(),
            )
        },
        |attention_id| ("attention".to_owned(), attention_id.to_string()),
    );
    Ok(CorrelationLink {
        schema: "harness.correlation-link.v1".to_owned(),
        link_id,
        trace: TraceContext {
            trace_id: trace_id[..32].to_owned(),
            span_id: span_id[..16].to_owned(),
            parent_span_id: None,
        },
        from_kind,
        from_id,
        to_kind: "notification_delivery".to_owned(),
        to_id: delivery.delivery_id.to_string(),
        relation: "presented_as".to_owned(),
        created_at_ms: delivery.created_at_ms,
    })
}
pub(crate) fn checked_delivery_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<NotificationDelivery> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "notification payload integrity check failed".into(),
        ));
    }
    let delivery: NotificationDelivery = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    delivery.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(delivery)
}
fn checked_presence_row(raw: String, payload_sha256: String) -> rusqlite::Result<OperatorPresence> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "presence payload integrity check failed".into(),
        ));
    }
    let presence: OperatorPresence = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    presence.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(presence)
}
fn validate_operator(value: &str) -> Result<(), StoreError> {
    if value.is_empty()
        || value.len() > 160
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(StoreError::Validation(
            "operator id must be a bounded path-safe identifier".to_owned(),
        ));
    }
    Ok(())
}
fn control_error(error: harness_domain::OperatorControlError) -> StoreError {
    StoreError::Validation(error.to_string())
}
fn to_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| {
        StoreError::Validation("operator presence version exceeds SQLite integer range".to_owned())
    })
}

fn non_negative_u64(value: i64, field: &str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Validation(format!("{field} is negative")))
}
fn mode_name(mode: OperatorPresenceMode) -> &'static str {
    match mode {
        OperatorPresenceMode::Interactive => "interactive",
        OperatorPresenceMode::Focus => "focus",
        OperatorPresenceMode::Unattended => "unattended",
    }
}
fn class_name(class: NotificationClass) -> &'static str {
    match class {
        NotificationClass::Critical => "critical",
        NotificationClass::ActionRequired => "action_required",
        NotificationClass::Routine => "routine",
    }
}
fn state_name(state: NotificationState) -> &'static str {
    match state {
        NotificationState::Pending => "pending",
        NotificationState::Deferred => "deferred",
        NotificationState::Delivered => "delivered",
        NotificationState::Failed => "failed",
    }
}
fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use harness_domain::{
        AttentionCategory, AttentionItemId, AttentionResurfacingPolicy, AttentionSourceRef,
        AttentionSourceType, AttentionState,
    };
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn presence_is_versioned_and_never_changes_notification_authority() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let initial = store.operator_presence("operator-a").expect("presence");
        assert_eq!(initial.mode, OperatorPresenceMode::Interactive);
        let updated = store
            .set_operator_presence("operator-a", OperatorPresenceMode::Focus, initial.version)
            .expect("set presence");
        assert_eq!(updated.mode, OperatorPresenceMode::Focus);
        assert!(matches!(
            store.set_operator_presence(
                "operator-a",
                OperatorPresenceMode::Unattended,
                initial.version
            ),
            Err(StoreError::Conflict(_))
        ));
        assert!(store.list_notification_deliveries(10).unwrap().is_empty());
    }

    #[test]
    fn concurrent_first_presence_updates_return_a_version_conflict() {
        let temp = TempDir::new().expect("temp");
        let store = Arc::new(Store::in_memory(&temp.path().join("artifacts")).expect("store"));
        let barrier = Arc::new(Barrier::new(2));
        let first_store = Arc::clone(&store);
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first_store.set_operator_presence("operator-a", OperatorPresenceMode::Focus, 1)
        });
        barrier.wait();
        let second = store.set_operator_presence("operator-a", OperatorPresenceMode::Unattended, 1);
        let first = first.join().expect("join");
        let outcomes = [first, second];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(StoreError::Conflict(_))))
                .count(),
            1
        );
    }

    #[test]
    fn concurrent_notification_refreshes_persist_one_delivery_and_one_causal_link() {
        let temp = TempDir::new().expect("temp");
        let database = temp.path().join("harness.sqlite3");
        let first_store =
            Store::open(&database, &temp.path().join("artifacts-a")).expect("first store");
        let attention = test_attention();
        first_store
            .upsert_source_attention(&attention)
            .expect("open attention");
        let second_store =
            Store::open(&database, &temp.path().join("artifacts-b")).expect("second store");
        let barrier = Arc::new(Barrier::new(2));
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first_store
                .refresh_notification_mirror()
                .expect("first refresh")
        });
        barrier.wait();
        let second = second_store
            .refresh_notification_mirror()
            .expect("second refresh");
        let first = first.join().expect("refresh thread joins");
        assert!((first.len() + second.len()) >= 1);
        let deliveries = second_store
            .list_notification_deliveries(10)
            .expect("stored deliveries");
        assert_eq!(deliveries.len(), 1);
        let correlation = notification_correlation_link(&deliveries[0]).expect("correlation");
        assert_eq!(
            second_store
                .correlation_links(&correlation.trace.trace_id, 10)
                .expect("stored correlation"),
            vec![correlation]
        );
    }

    #[test]
    fn notification_mirror_processes_each_attention_revision_once() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let attention = AttentionItem {
            schema: "harness.attention-item.v1".to_owned(),
            attention_id: AttentionItemId::new(),
            repository_id: Some("repo-a".to_owned()),
            run_id: Some("run-a".to_owned()),
            task_id: Some("task-a".to_owned()),
            source: AttentionSourceRef {
                source_type: AttentionSourceType::Approval,
                source_id: "approval-a".to_owned(),
                source_revision: 1,
            },
            category: AttentionCategory::Approval,
            severity: AttentionSeverity::High,
            state: AttentionState::Open,
            title: "Approval required".to_owned(),
            summary: "A bound approval is required before execution.".to_owned(),
            option_refs: vec![],
            evidence_refs: vec![],
            blocked_refs: vec!["task-a".to_owned()],
            dedupe_key: "approval-task-a".to_owned(),
            opened_event_id: "event-a".to_owned(),
            opened_at_ms: 1_000,
            acknowledged_at_ms: None,
            due_at_ms: None,
            resurfacing: AttentionResurfacingPolicy {
                policy: "until_authority_receipt".to_owned(),
                maximum_defer_ms: 60_000,
            },
            resolution: None,
            version: 1,
        };
        store
            .upsert_source_attention(&attention)
            .expect("open attention");

        let first = store.refresh_notification_mirror().expect("first mirror");
        let replay = store.refresh_notification_mirror().expect("replay mirror");
        assert!(replay.is_empty());
        assert_eq!(store.list_notification_deliveries(10).unwrap(), first);
        assert_eq!(first[0].created_at_ms, attention.opened_at_ms);
        assert_eq!(
            store
                .notification_delivery_health()
                .expect("delivery health"),
            NotificationDeliveryHealth {
                schema: "harness.notification-delivery-health.v1".to_owned(),
                channel: "in_product_mirror".to_owned(),
                current_attention_revisions: 1,
                examined_current_revisions: 1,
                delivered_examined_revisions: 1,
                undelivered_examined_revisions: 0,
                undelivered_critical_examined_revisions: 0,
                undelivered_action_required_examined_revisions: 0,
                failed_examined_revisions: 0,
                unverified_delivery_examined_revisions: 0,
                oldest_undelivered_opened_at_ms: None,
                latest_verified_mirror_receipt_at_ms: Some(attention.opened_at_ms),
                truncated: false,
                desktop_delivery_enabled: false,
                batching_enabled: false,
                suppression_enabled: false,
            }
        );
        let correlation = notification_correlation_link(&first[0]).expect("correlation");
        assert_eq!(
            store
                .correlation_links(&correlation.trace.trace_id, 10)
                .expect("stored correlation"),
            vec![correlation]
        );
        assert_eq!(
            store.list_attention(false, 10).unwrap().items,
            vec![attention]
        );
    }

    #[test]
    fn notification_refresh_processes_an_older_pending_revision_after_a_full_batch() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        for index in 0..=MAX_NOTIFICATION_PAGE_SIZE {
            let mut attention = test_attention();
            attention.source.source_id = format!("approval-{index}");
            attention.dedupe_key = format!("approval-task-{index}");
            attention.opened_event_id = format!("event-{index}");
            attention.opened_at_ms = i64::from(index);
            attention.validate().expect("valid unique attention");
            store
                .upsert_source_attention(&attention)
                .expect("open attention");
        }

        let first = store.refresh_notification_mirror().expect("first batch");
        assert_eq!(first.len(), MAX_NOTIFICATION_PAGE_SIZE as usize);
        let second = store.refresh_notification_mirror().expect("remaining item");
        assert_eq!(second.len(), 1);
        assert_eq!(
            second[0].created_at_ms,
            i64::from(MAX_NOTIFICATION_PAGE_SIZE)
        );
        assert!(
            store
                .refresh_notification_mirror()
                .expect("no pending revisions")
                .is_empty()
        );
    }

    #[test]
    fn notification_delivery_health_is_bounded_and_does_not_refresh_the_mirror() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        for index in 0..=MAX_NOTIFICATION_HEALTH_ROWS {
            let mut attention = test_attention();
            attention.source.source_id = format!("health-approval-{index}");
            attention.dedupe_key = format!("health-approval-task-{index}");
            attention.opened_event_id = format!("health-event-{index}");
            attention.opened_at_ms = i64::from(index);
            attention.validate().expect("valid unique attention");
            store
                .upsert_source_attention(&attention)
                .expect("open attention");
        }

        let health = store
            .notification_delivery_health()
            .expect("delivery health");
        assert_eq!(health.current_attention_revisions, 201);
        assert_eq!(health.examined_current_revisions, 200);
        assert_eq!(health.delivered_examined_revisions, 0);
        assert_eq!(health.undelivered_examined_revisions, 200);
        assert_eq!(health.undelivered_action_required_examined_revisions, 200);
        assert!(health.truncated);
        assert_eq!(health.oldest_undelivered_opened_at_ms, Some(0));
        assert!(store.list_notification_deliveries(200).unwrap().is_empty());
    }

    #[test]
    fn notification_delivery_health_does_not_treat_a_failed_receipt_as_delivered() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let attention = test_attention();
        store
            .upsert_source_attention(&attention)
            .expect("open attention");
        let mut failed = notification_from_attention(&attention).expect("delivery");
        failed.state = NotificationState::Failed;
        failed.sha256 = failed.digest().expect("failed delivery digest");
        store
            .record_notification_delivery(&failed)
            .expect("record failed receipt");

        let health = store
            .notification_delivery_health()
            .expect("delivery health");
        assert_eq!(health.delivered_examined_revisions, 0);
        assert_eq!(health.undelivered_examined_revisions, 1);
        assert_eq!(health.undelivered_action_required_examined_revisions, 1);
        assert_eq!(health.failed_examined_revisions, 1);
        assert_eq!(health.unverified_delivery_examined_revisions, 1);
        assert_eq!(
            health.oldest_undelivered_opened_at_ms,
            Some(attention.opened_at_ms)
        );
    }

    #[test]
    fn delivery_and_correlation_commit_together() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let attention = test_attention();
        store
            .upsert_source_attention(&attention)
            .expect("open attention");
        let delivery = notification_from_attention(&attention).expect("delivery");
        let mut conflicting = notification_correlation_link(&delivery).expect("link");
        conflicting.relation = "different_relation".to_owned();
        store
            .record_correlation_link(&conflicting)
            .expect("preexisting incompatible immutable link");

        assert!(matches!(
            store.refresh_notification_mirror(),
            Err(StoreError::Conflict(_))
        ));
        assert!(store.list_notification_deliveries(10).unwrap().is_empty());
    }

    fn test_attention() -> AttentionItem {
        let attention = AttentionItem {
            schema: "harness.attention-item.v1".to_owned(),
            attention_id: AttentionItemId::new(),
            repository_id: Some("repo-a".to_owned()),
            run_id: Some("run-a".to_owned()),
            task_id: Some("task-a".to_owned()),
            source: AttentionSourceRef {
                source_type: AttentionSourceType::Approval,
                source_id: "approval-a".to_owned(),
                source_revision: 1,
            },
            category: AttentionCategory::Approval,
            severity: AttentionSeverity::High,
            state: AttentionState::Open,
            title: "Approval required".to_owned(),
            summary: "A bound approval is required before execution.".to_owned(),
            option_refs: vec![],
            evidence_refs: vec![],
            blocked_refs: vec!["task-a".to_owned()],
            dedupe_key: "approval-task-a".to_owned(),
            opened_event_id: "event-a".to_owned(),
            opened_at_ms: 1_000,
            acknowledged_at_ms: None,
            due_at_ms: None,
            resurfacing: AttentionResurfacingPolicy {
                policy: "until_authority_receipt".to_owned(),
                maximum_defer_ms: 60_000,
            },
            resolution: None,
            version: 1,
        };
        attention.validate().expect("valid attention");
        attention
    }
}
