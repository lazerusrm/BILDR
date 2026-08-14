//! Read-first operator-control CLI commands.
//!
//! This module deliberately exposes bounded snapshots, source-owned attention,
//! and immutable controller receipts. It cannot resolve a source, approve work,
//! request an intervention, or resume a run.

use anyhow::Result;
use serde_json::{Value, json};

use super::{ApiClient, AttentionCommand, ConditionCommand, InvestigationCommand, RecoveryCommand};

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

pub(super) async fn investigation(api: &ApiClient, command: InvestigationCommand) -> Result<Value> {
    match command {
        InvestigationCommand::List { run_id, task_id } => {
            let mut query = vec!["limit=50".to_owned()];
            if let Some(run_id) = run_id {
                let run_id: String =
                    url::form_urlencoded::byte_serialize(run_id.as_bytes()).collect();
                query.push(format!("run_id={run_id}"));
            }
            if let Some(task_id) = task_id {
                let task_id: String =
                    url::form_urlencoded::byte_serialize(task_id.as_bytes()).collect();
                query.push(format!("task_id={task_id}"));
            }
            api.get(&format!("/api/v1/investigations?{}", query.join("&")))
                .await
        }
        InvestigationCommand::Show { artifact_id } => {
            let artifact_id: String =
                url::form_urlencoded::byte_serialize(artifact_id.as_bytes()).collect();
            api.get(&format!("/api/v1/investigations/{artifact_id}"))
                .await
        }
    }
}

pub(super) async fn condition(api: &ApiClient, command: ConditionCommand) -> Result<Value> {
    match command {
        ConditionCommand::List { include_terminal } => {
            let query = if include_terminal {
                "limit=50&include_terminal=true"
            } else {
                "limit=50"
            };
            api.get(&format!("/api/v1/external-conditions?{query}"))
                .await
        }
        ConditionCommand::Show { condition_id } => {
            let condition_id: String =
                url::form_urlencoded::byte_serialize(condition_id.as_bytes()).collect();
            api.get(&format!("/api/v1/external-conditions/{condition_id}"))
                .await
        }
        ConditionCommand::Observations { condition_id } => {
            let condition_id: String =
                url::form_urlencoded::byte_serialize(condition_id.as_bytes()).collect();
            api.get(&format!(
                "/api/v1/external-conditions/{condition_id}/observations?limit=50"
            ))
            .await
        }
    }
}

pub(super) async fn progress(api: &ApiClient, run_id: Option<String>) -> Result<Value> {
    api.get(&bounded_run_query("/api/v1/material-progress", run_id))
        .await
}

pub(super) async fn liveness(api: &ApiClient, run_id: Option<String>) -> Result<Value> {
    api.get(&bounded_run_query("/api/v1/liveness", run_id))
        .await
}

pub(super) async fn intervention_receipts(api: &ApiClient, episode_id: String) -> Result<Value> {
    let episode_id: String = url::form_urlencoded::byte_serialize(episode_id.as_bytes()).collect();
    api.get(&format!(
        "/api/v1/liveness/{episode_id}/interventions?limit=50"
    ))
    .await
}

pub(super) async fn trace(api: &ApiClient, trace_id: String) -> Result<Value> {
    let trace_id: String = url::form_urlencoded::byte_serialize(trace_id.as_bytes()).collect();
    api.get(&format!("/api/v1/traces/{trace_id}?limit=50"))
        .await
}

pub(super) async fn topology(api: &ApiClient, run_id: String) -> Result<Value> {
    let run_id: String = url::form_urlencoded::byte_serialize(run_id.as_bytes()).collect();
    api.get(&format!("/api/v1/runs/{run_id}/topology")).await
}

pub(super) async fn recovery(api: &ApiClient, command: RecoveryCommand) -> Result<Value> {
    match command {
        RecoveryCommand::List { run_id } => {
            api.get(&bounded_run_query("/api/v1/reconciliations", run_id))
                .await
        }
        RecoveryCommand::Show { episode_id } => {
            let episode_id: String =
                url::form_urlencoded::byte_serialize(episode_id.as_bytes()).collect();
            api.get(&format!("/api/v1/reconciliations/{episode_id}"))
                .await
        }
    }
}

fn bounded_run_query(path: &str, run_id: Option<String>) -> String {
    let mut query = vec!["limit=50".to_owned()];
    if let Some(run_id) = run_id {
        let run_id: String = url::form_urlencoded::byte_serialize(run_id.as_bytes()).collect();
        query.push(format!("run_id={run_id}"));
    }
    format!("{path}?{}", query.join("&"))
}
