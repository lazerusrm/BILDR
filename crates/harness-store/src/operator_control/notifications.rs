//! Presence and notification-mirror custody.
//!
//! This first delivery slice records what the product has presented in its
//! local control plane. It does not suppress an attention item, send desktop
//! notifications, or update a source-owned lifecycle.

use harness_domain::{
    AttentionItem, AttentionSeverity, NotificationClass, NotificationDelivery,
    NotificationDeliveryId, NotificationState, OperatorPresence, OperatorPresenceMode, now_ms,
};
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const MAX_NOTIFICATION_PAGE_SIZE: u32 = 200;

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
        let current = self.operator_presence(operator_id)?;
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
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if expected_version == 1
            && transaction.query_row(
                "SELECT count(*) FROM operator_presence WHERE operator_id=?1",
                [operator_id],
                |row| row.get::<_, i64>(0),
            )? == 0
        {
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

    /// Mirrors visible source-owned attention into append-only product delivery
    /// receipts. The id is deterministic per attention version, so retry and
    /// restart cannot produce duplicate presentation claims.
    pub fn refresh_notification_mirror(&self) -> Result<Vec<NotificationDelivery>, StoreError> {
        let attention = self
            .list_attention(false, MAX_NOTIFICATION_PAGE_SIZE)?
            .items;
        let mut written = Vec::new();
        for item in attention {
            let receipt = notification_from_attention(&item)?;
            written.push(self.record_notification_delivery(&receipt)?);
        }
        Ok(written)
    }

    pub fn record_notification_delivery(
        &self,
        delivery: &NotificationDelivery,
    ) -> Result<NotificationDelivery, StoreError> {
        delivery.validate().map_err(control_error)?;
        let raw = serde_json::to_string(delivery)?;
        let payload_sha256 = digest(&raw);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((existing_raw, existing_digest)) = transaction.query_row(
            "SELECT payload_json,payload_sha256 FROM notification_deliveries WHERE source_event_id=?1",
            [delivery.source_event_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        ).optional()? {
            let existing = checked_delivery_row(existing_raw, existing_digest)?;
            if existing == *delivery { transaction.commit()?; return Ok(existing); }
            return Err(StoreError::Conflict("notification source event already has different content".to_owned()));
        }
        transaction.execute("INSERT INTO notification_deliveries(id,attention_id,class,state,source_event_id,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", params![delivery.delivery_id.as_str(), delivery.attention_id.as_ref().map(|id| id.as_str()), class_name(delivery.class), state_name(delivery.state), delivery.source_event_id, raw, payload_sha256, delivery.created_at_ms])?;
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
        created_at_ms: now_ms(),
        payload_sha256: digest(&serde_json::to_string(item)?),
        sha256: String::new(),
    };
    delivery.sha256 = delivery.digest().map_err(control_error)?;
    delivery.validate().map_err(control_error)?;
    Ok(delivery)
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
}
