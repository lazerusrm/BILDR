//! Read-first operator-control CLI commands.
//!
//! This module deliberately exposes only bounded snapshots, source-owned
//! attention, and presentation acknowledgement. It cannot resolve a source,
//! approve work, or resume a run.

use anyhow::Result;
use serde_json::{Value, json};

use super::{ApiClient, AttentionCommand};

pub(super) async fn return_view(api: &ApiClient, operator_id: String) -> Result<Value> {
    let operator_id: String =
        url::form_urlencoded::byte_serialize(operator_id.as_bytes()).collect();
    api.get(&format!(
        "/api/v1/control-plane/return-view?operator_id={operator_id}"
    ))
    .await
}

pub(super) async fn attention(api: &ApiClient, command: AttentionCommand) -> Result<Value> {
    match command {
        AttentionCommand::List {
            include_terminal,
            cursor,
        } => {
            let mut query = vec!["limit=50".to_owned()];
            if include_terminal {
                query.push("include_terminal=true".to_owned());
            }
            if let Some(cursor) = cursor {
                let cursor: String =
                    url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect();
                query.push(format!("cursor={cursor}"));
            }
            api.get(&format!("/api/v1/attention?{}", query.join("&")))
                .await
        }
        AttentionCommand::Show { attention_id } => {
            let attention_id: String =
                url::form_urlencoded::byte_serialize(attention_id.as_bytes()).collect();
            api.get(&format!("/api/v1/attention/{attention_id}")).await
        }
        AttentionCommand::Acknowledge {
            attention_id,
            expected_version,
        } => {
            let attention_id: String =
                url::form_urlencoded::byte_serialize(attention_id.as_bytes()).collect();
            api.post(
                &format!("/api/v1/attention/{attention_id}/acknowledge"),
                json!({"expected_version": expected_version}),
            )
            .await
        }
    }
}
