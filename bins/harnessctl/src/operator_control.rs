//! Read-first operator-control CLI commands.
//!
//! This module deliberately exposes bounded snapshots, source-owned attention,
//! immutable controller receipts, and exact governed-knowledge reviews. It
//! cannot resolve a source, approve work, request an intervention, inject task
//! context, or resume a run.

use anyhow::Result;
use serde_json::{Value, json};

use super::{
    ApiClient, AttentionCommand, ConditionCommand, InvestigationCommand, KnowledgeCommand,
    PresenceCommand, RecoveryCommand,
};

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

/// Reads exact governed knowledge or records an explicit local-session review
/// against the current candidate SHA. The server derives reviewer identity from
/// the session and rejects stale, unclean, or non-candidate records.
pub(super) async fn knowledge(api: &ApiClient, command: KnowledgeCommand) -> Result<Value> {
    match command {
        KnowledgeCommand::List { repository_id } => {
            let repository_id: String =
                url::form_urlencoded::byte_serialize(repository_id.as_bytes()).collect();
            api.get(&format!(
                "/api/v1/improvement/knowledge?repository_id={repository_id}&limit=50"
            ))
            .await
        }
        KnowledgeCommand::Show { knowledge_id } => {
            let knowledge_id: String =
                url::form_urlencoded::byte_serialize(knowledge_id.as_bytes()).collect();
            api.get(&format!("/api/v1/improvement/knowledge/{knowledge_id}"))
                .await
        }
        KnowledgeCommand::Review {
            knowledge_id,
            expected_knowledge_sha256,
            decision,
        } => {
            let knowledge_id: String =
                url::form_urlencoded::byte_serialize(knowledge_id.as_bytes()).collect();
            api.post(
                &format!("/api/v1/improvement/knowledge/{knowledge_id}/review"),
                json!({
                    "expected_knowledge_sha256": expected_knowledge_sha256,
                    "decision": decision,
                }),
            )
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
        ConditionCommand::RegisterTimeGate {
            run_id,
            not_before_ms,
            deadline_ms,
        } => {
            let run_id: String = url::form_urlencoded::byte_serialize(run_id.as_bytes()).collect();
            api.post(
                &format!("/api/v1/runs/{run_id}/external-conditions/time-gates"),
                time_gate_request(not_before_ms, deadline_ms),
            )
            .await
        }
        ConditionCommand::RegisterLocalCapacity {
            run_id,
            minimum_available_bytes,
            deadline_ms,
        } => {
            let run_id: String = url::form_urlencoded::byte_serialize(run_id.as_bytes()).collect();
            api.post(
                &format!("/api/v1/runs/{run_id}/external-conditions/local-capacity"),
                local_capacity_request(minimum_available_bytes, deadline_ms),
            )
            .await
        }
    }
}

fn time_gate_request(not_before_ms: i64, deadline_ms: Option<i64>) -> Value {
    json!({
        "not_before_ms": not_before_ms,
        "deadline_ms": deadline_ms,
    })
}

fn local_capacity_request(minimum_available_bytes: u64, deadline_ms: Option<i64>) -> Value {
    json!({
        "minimum_available_bytes": minimum_available_bytes,
        "deadline_ms": deadline_ms,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{local_capacity_request, time_gate_request};

    #[test]
    fn time_gate_request_keeps_an_omitted_deadline_explicitly_null() {
        assert_eq!(
            time_gate_request(1_786_809_600_000, None),
            json!({
                "not_before_ms": 1_786_809_600_000_i64,
                "deadline_ms": null,
            })
        );
    }

    #[test]
    fn local_capacity_request_has_only_the_closed_capacity_shape() {
        assert_eq!(
            local_capacity_request(1_048_576, None),
            json!({
                "minimum_available_bytes": 1_048_576_u64,
                "deadline_ms": null,
            })
        );
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

/// Presentation-only local preference. The expected version prevents the CLI
/// from overwriting a more recent operator update and the API retains all
/// session, CSRF, and same-origin protections.
pub(super) async fn presence(api: &ApiClient, command: PresenceCommand) -> Result<Value> {
    match command {
        PresenceCommand::Show { operator_id } => {
            let operator_id: String =
                url::form_urlencoded::byte_serialize(operator_id.as_bytes()).collect();
            api.get(&format!(
                "/api/v1/operator-presence?operator_id={operator_id}"
            ))
            .await
        }
        PresenceCommand::Set {
            operator_id,
            mode,
            expected_version,
        } => {
            api.post(
                "/api/v1/operator-presence",
                json!({
                    "operator_id": operator_id,
                    "mode": mode,
                    "expected_version": expected_version,
                }),
            )
            .await
        }
    }
}

pub(super) async fn notification_deliveries(api: &ApiClient) -> Result<Value> {
    api.get("/api/v1/notification-deliveries?limit=50").await
}

/// Read-only bounded health for the in-product mirror. The server computes
/// integrity status from immutable receipts; this CLI command never refreshes
/// a receipt or affects a source-owned attention lifecycle.
pub(super) async fn notification_delivery_health(api: &ApiClient) -> Result<Value> {
    api.get("/api/v1/notification-delivery-health").await
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
        RecoveryCommand::Findings { episode_id } => {
            let episode_id: String =
                url::form_urlencoded::byte_serialize(episode_id.as_bytes()).collect();
            api.get(&format!(
                "/api/v1/reconciliations/{episode_id}/findings?limit=50"
            ))
            .await
        }
        RecoveryCommand::Actions { episode_id } => {
            let episode_id: String =
                url::form_urlencoded::byte_serialize(episode_id.as_bytes()).collect();
            api.get(&format!(
                "/api/v1/reconciliations/{episode_id}/actions?limit=50"
            ))
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
