//! Read-first operator-control API surface.
//!
//! The only mutations are version-checked presentation acknowledgement and
//! authority-neutral local presence. They deliberately cannot resolve,
//! approve, or force recovery; source-specific controllers retain that
//! authority.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use harness_domain::{
    AttentionItemId, ExternalConditionId, InvestigationArtifactId, LivenessEpisodeId,
    OperatorPresenceMode, ReconciliationEpisodeId,
};
use serde::Deserialize;

use super::{ApiError, ApiState, authenticate};

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/v1/control-plane/snapshot",
            get(control_plane_snapshot),
        )
        .route("/api/v1/control-plane/return-view", get(return_view))
        .route(
            "/api/v1/control-plane/return-view/cursor",
            post(advance_return_view_cursor),
        )
        .route("/api/v1/attention", get(list_attention))
        .route("/api/v1/attention/{attention_id}", get(get_attention))
        .route(
            "/api/v1/attention/{attention_id}/acknowledge",
            post(acknowledge_attention),
        )
        .route("/api/v1/investigations", get(list_investigations))
        .route(
            "/api/v1/investigations/{artifact_id}",
            get(get_investigation),
        )
        .route("/api/v1/external-conditions", get(list_external_conditions))
        .route(
            "/api/v1/external-conditions/{condition_id}",
            get(get_external_condition),
        )
        .route(
            "/api/v1/external-conditions/{condition_id}/observations",
            get(list_condition_observations),
        )
        .route("/api/v1/material-progress", get(list_material_progress))
        .route("/api/v1/liveness", get(list_liveness))
        .route("/api/v1/runs/{run_id}/liveness", get(list_run_liveness))
        .route(
            "/api/v1/liveness/{episode_id}/interventions",
            get(list_intervention_receipts),
        )
        .route("/api/v1/traces/{trace_id}", get(list_correlation_links))
        .route("/api/v1/runs/{run_id}/topology", get(run_topology))
        .route("/api/v1/reconciliations", get(list_reconciliations))
        .route(
            "/api/v1/reconciliations/{episode_id}",
            get(get_reconciliation),
        )
        .route(
            "/api/v1/operator-presence",
            get(get_operator_presence).post(set_operator_presence),
        )
        .route(
            "/api/v1/notification-deliveries",
            get(list_notification_deliveries),
        )
}

async fn control_plane_snapshot(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<harness_domain::ControlPlaneSnapshot>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.store().control_plane_snapshot()?))
}

#[derive(Debug, Deserialize)]
struct ReturnViewQuery {
    operator_id: Option<String>,
}

async fn return_view(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ReturnViewQuery>,
) -> Result<Json<harness_domain::ReturnView>, ApiError> {
    authenticate(&state, &headers, false)?;
    let operator_id = query.operator_id.as_deref().unwrap_or("local_operator");
    Ok(Json(
        state
            .orchestrator
            .store()
            .control_plane_return_view(operator_id)?,
    ))
}

#[derive(Debug, Deserialize)]
struct AttentionQuery {
    include_terminal: Option<bool>,
    limit: Option<u32>,
    cursor: Option<String>,
}

async fn list_attention(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<AttentionQuery>,
) -> Result<Json<harness_store::AttentionPage>, ApiError> {
    authenticate(&state, &headers, false)?;
    state.orchestrator.store().refresh_approval_attention()?;
    Ok(Json(state.orchestrator.store().list_attention_page(
        query.include_terminal.unwrap_or(false),
        query.limit.unwrap_or(50),
        query.cursor.as_deref(),
    )?))
}

async fn get_attention(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(attention_id): Path<String>,
) -> Result<Json<harness_domain::AttentionItem>, ApiError> {
    authenticate(&state, &headers, false)?;
    let attention_id = parse_attention_id(&attention_id)?;
    let item = state
        .orchestrator
        .store()
        .attention_item(&attention_id)?
        .ok_or_else(|| {
            ApiError::from(harness_store::StoreError::NotFound(format!(
                "attention item {attention_id}"
            )))
        })?;
    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcknowledgeAttentionRequest {
    expected_version: u64,
    acknowledged_at_ms: Option<i64>,
}

async fn acknowledge_attention(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(attention_id): Path<String>,
    Json(request): Json<AcknowledgeAttentionRequest>,
) -> Result<Json<harness_domain::AttentionItem>, ApiError> {
    authenticate(&state, &headers, true)?;
    let attention_id = parse_attention_id(&attention_id)?;
    Ok(Json(state.orchestrator.store().acknowledge_attention(
        &attention_id,
        request.expected_version,
        request.acknowledged_at_ms,
    )?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdvanceReturnViewCursorRequest {
    operator_id: String,
    expected_snapshot_revision: u64,
    acknowledged_cursor: u64,
}

async fn advance_return_view_cursor(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<AdvanceReturnViewCursorRequest>,
) -> Result<Json<harness_store::ReturnViewCursor>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state.orchestrator.store().advance_return_view_cursor(
            &request.operator_id,
            request.expected_snapshot_revision,
            request.acknowledged_cursor,
        )?,
    ))
}

fn parse_attention_id(value: &str) -> Result<AttentionItemId, ApiError> {
    AttentionItemId::parse(value)
        .map_err(|error| ApiError::from(harness_store::StoreError::Validation(error.to_string())))
}

#[derive(Debug, Deserialize)]
struct InvestigationQuery {
    run_id: Option<String>,
    task_id: Option<String>,
    limit: Option<u32>,
}

async fn list_investigations(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<InvestigationQuery>,
) -> Result<Json<Vec<harness_domain::InvestigationArtifactSummary>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_investigation_artifact_summaries(
                query.run_id.as_deref(),
                query.task_id.as_deref(),
                query.limit.unwrap_or(50),
            )?,
    ))
}

async fn get_investigation(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(artifact_id): Path<String>,
) -> Result<Json<harness_domain::InvestigationArtifact>, ApiError> {
    authenticate(&state, &headers, false)?;
    let artifact_id = InvestigationArtifactId::parse(&artifact_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    state
        .orchestrator
        .store()
        .investigation_artifact(&artifact_id)?
        .map(Json)
        .ok_or_else(|| {
            ApiError::from(harness_store::StoreError::NotFound(format!(
                "investigation artifact {artifact_id}"
            )))
        })
}

#[derive(Debug, Deserialize)]
struct ExternalConditionQuery {
    include_terminal: Option<bool>,
    limit: Option<u32>,
}

async fn list_external_conditions(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ExternalConditionQuery>,
) -> Result<Json<Vec<harness_domain::ExternalConditionSummary>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_external_condition_summaries(
                query.include_terminal.unwrap_or(false),
                query.limit.unwrap_or(50),
            )?,
    ))
}

async fn get_external_condition(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(condition_id): Path<String>,
) -> Result<Json<harness_domain::ExternalCondition>, ApiError> {
    authenticate(&state, &headers, false)?;
    let condition_id = parse_condition_id(&condition_id)?;
    state
        .orchestrator
        .store()
        .external_condition(&condition_id)?
        .map(Json)
        .ok_or_else(|| {
            ApiError::from(harness_store::StoreError::NotFound(format!(
                "external condition {condition_id}"
            )))
        })
}

#[derive(Debug, Deserialize)]
struct ConditionObservationQuery {
    limit: Option<u32>,
}

async fn list_condition_observations(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(condition_id): Path<String>,
    Query(query): Query<ConditionObservationQuery>,
) -> Result<Json<Vec<harness_domain::ConditionObservation>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let condition_id = parse_condition_id(&condition_id)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_condition_observations(&condition_id, query.limit.unwrap_or(50))?,
    ))
}

fn parse_condition_id(value: &str) -> Result<ExternalConditionId, ApiError> {
    ExternalConditionId::parse(value)
        .map_err(|error| ApiError::from(harness_store::StoreError::Validation(error.to_string())))
}

#[derive(Debug, Deserialize)]
struct BoundedReadQuery {
    run_id: Option<String>,
    limit: Option<u32>,
}

/// Read-only material progress records produced by the deterministic
/// classifier. This endpoint cannot submit classifications or change a run.
async fn list_material_progress(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<BoundedReadQuery>,
) -> Result<Json<Vec<harness_domain::MaterialProgressEvent>>, ApiError> {
    authenticate(&state, &headers, false)?;
    state.orchestrator.store().classify_material_progress()?;
    Ok(Json(state.orchestrator.store().list_material_progress(
        query.run_id.as_deref(),
        query.limit.unwrap_or(50),
    )?))
}

/// Observe-only liveness episodes. Listing does not collect a fresh
/// observation, start recovery, or apply an intervention.
async fn list_liveness(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<BoundedReadQuery>,
) -> Result<Json<Vec<harness_domain::LivenessEpisode>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.store().list_liveness_episodes(
        query.run_id.as_deref(),
        query.limit.unwrap_or(50),
    )?))
}

async fn list_run_liveness(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<BoundedReadQuery>,
) -> Result<Json<Vec<harness_domain::LivenessEpisode>>, ApiError> {
    authenticate(&state, &headers, false)?;
    if query.run_id.is_some() {
        return Err(ApiError::from(harness_store::StoreError::Validation(
            "run liveness route does not accept a second run_id filter".to_owned(),
        )));
    }
    Ok(Json(state.orchestrator.store().list_liveness_episodes(
        Some(&run_id),
        query.limit.unwrap_or(50),
    )?))
}

/// Immutable receipts for interventions already performed through an existing
/// controller path. This route cannot request, replay, or apply an action.
async fn list_intervention_receipts(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<harness_domain::InterventionReceipt>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let episode_id = LivenessEpisodeId::parse(episode_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_intervention_receipts(&episode_id, query.limit.unwrap_or(50))?,
    ))
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

/// A bounded, immutable causal trace. Trace links are receipts produced by
/// controller-owned paths; looking them up cannot add a link, resume a run,
/// or disclose unbounded event payloads.
async fn list_correlation_links(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<harness_domain::CorrelationLink>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .correlation_links(&trace_id, query.limit.unwrap_or(50))?,
    ))
}

async fn run_topology(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<harness_domain::TopologySnapshot>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.store().run_topology(&run_id)?))
}

async fn list_reconciliations(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<BoundedReadQuery>,
) -> Result<Json<Vec<harness_domain::ReconciliationEpisode>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_reconciliation_episodes(query.run_id.as_deref(), query.limit.unwrap_or(50))?,
    ))
}

async fn get_reconciliation(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
) -> Result<Json<harness_domain::ReconciliationEpisode>, ApiError> {
    authenticate(&state, &headers, false)?;
    let episode_id = ReconciliationEpisodeId::parse(episode_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    state
        .orchestrator
        .store()
        .reconciliation_episode(&episode_id)?
        .map(Json)
        .ok_or_else(|| {
            ApiError::from(harness_store::StoreError::NotFound(format!(
                "reconciliation episode {episode_id}"
            )))
        })
}

#[derive(Debug, Deserialize)]
struct PresenceQuery {
    operator_id: Option<String>,
}

async fn get_operator_presence(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<PresenceQuery>,
) -> Result<Json<harness_domain::OperatorPresence>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.store().operator_presence(
        query.operator_id.as_deref().unwrap_or("local_operator"),
    )?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetPresenceRequest {
    operator_id: String,
    mode: OperatorPresenceMode,
    expected_version: u64,
}

async fn set_operator_presence(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<SetPresenceRequest>,
) -> Result<Json<harness_domain::OperatorPresence>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(state.orchestrator.store().set_operator_presence(
        &request.operator_id,
        request.mode,
        request.expected_version,
    )?))
}

#[derive(Debug, Deserialize)]
struct NotificationQuery {
    limit: Option<u32>,
}

async fn list_notification_deliveries(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<NotificationQuery>,
) -> Result<Json<Vec<harness_domain::NotificationDelivery>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_notification_deliveries(query.limit.unwrap_or(50))?,
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // Axum's Json extractor maps these serde failures to a 422 response before
    // the mutation handler/authentication path runs. Keeping the request
    // structs closed here makes that HTTP behavior match the checked-in
    // OpenAPI `additionalProperties: false` contract.
    #[test]
    fn operator_control_mutation_bodies_reject_unknown_http_json_fields() {
        assert!(
            serde_json::from_value::<AcknowledgeAttentionRequest>(json!({
                "expected_version": 1,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AdvanceReturnViewCursorRequest>(json!({
                "operator_id": "local_operator",
                "expected_snapshot_revision": 1,
                "acknowledged_cursor": 0,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<SetPresenceRequest>(json!({
                "operator_id": "local_operator",
                "mode": "interactive",
                "expected_version": 0,
                "unexpected": true,
            }))
            .is_err()
        );
    }
}
