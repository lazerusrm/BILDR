//! Read-first operator-control API surface.
//!
//! The only mutation in this first vertical slice is a version-checked
//! acknowledgement. It deliberately cannot resolve, approve, or force a
//! recovery; source-specific controllers retain that authority.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use harness_domain::{AttentionItemId, ExternalConditionId, InvestigationArtifactId};
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
) -> Result<Json<Vec<harness_domain::InvestigationArtifact>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state.orchestrator.store().list_investigation_artifacts(
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
) -> Result<Json<Vec<harness_domain::ExternalCondition>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.store().list_external_conditions(
        query.include_terminal.unwrap_or(false),
        query.limit.unwrap_or(50),
    )?))
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
