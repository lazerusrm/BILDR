//! Read-first operator-control API surface.
//!
//! Mutations stay deliberately narrow: version-checked presentation
//! acknowledgement, authority-neutral local presence, and creation of an
//! unreviewed knowledge candidates from exact investigation, liveness, and
//! reconciliation evidence, one exact human knowledge review, and an
//! exact-revision liveness wait receipt. They cannot resolve source attention,
//! alter task custody, inject knowledge into task context, or force recovery.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use harness_domain::{
    AttentionItemId, ExternalConditionId, InvestigationArtifactId, KnowledgeReviewDecision,
    LivenessEpisodeId, OperatorPresenceMode, ReconciliationEpisodeId, RunId,
};
use serde::Deserialize;

use super::{ApiError, ApiState, authenticate, authenticated_session_id};

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
        .route(
            "/api/v1/investigations/{artifact_id}/knowledge-candidates",
            post(propose_knowledge_from_investigation),
        )
        .route(
            "/api/v1/improvement/knowledge",
            get(list_current_knowledge_items),
        )
        .route(
            "/api/v1/improvement/knowledge/{knowledge_id}/review",
            post(review_knowledge_candidate),
        )
        .route(
            "/api/v1/improvement/knowledge/{knowledge_id}",
            get(get_current_knowledge_item),
        )
        .route("/api/v1/external-conditions", get(list_external_conditions))
        .route(
            "/api/v1/runs/{run_id}/external-conditions/time-gates",
            post(register_run_time_gate),
        )
        .route(
            "/api/v1/runs/{run_id}/external-conditions/local-capacity",
            post(register_run_local_capacity_gate),
        )
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
            get(list_intervention_receipts).post(execute_wait_intervention),
        )
        .route(
            "/api/v1/liveness/{episode_id}/knowledge-candidates",
            post(propose_knowledge_from_repeated_liveness),
        )
        .route("/api/v1/traces/{trace_id}", get(list_correlation_links))
        .route("/api/v1/runs/{run_id}/topology", get(run_topology))
        .route("/api/v1/reconciliations", get(list_reconciliations))
        .route(
            "/api/v1/reconciliations/{episode_id}/findings",
            get(list_reconciliation_findings),
        )
        .route(
            "/api/v1/reconciliations/{episode_id}/actions",
            get(list_reconciliation_action_receipts),
        )
        .route(
            "/api/v1/reconciliations/{episode_id}/knowledge-candidates",
            post(propose_knowledge_from_repeated_reconciliation),
        )
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
        .route(
            "/api/v1/notification-shadow-batches",
            get(list_notification_shadow_batches).post(create_notification_shadow_batch),
        )
        .route(
            "/api/v1/notification-delivery-health",
            get(notification_delivery_health),
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
#[serde(deny_unknown_fields)]
struct ProposeKnowledgeFromInvestigationRequest {
    expected_artifact_sha256: String,
    finding_id: String,
    task_family: String,
    model_family: Option<String>,
    runtime_class: Option<String>,
}

/// Writes a suggestion only. The Store derives the statement, evidence,
/// sensitivity, retention, scope repository, freshness, and immutable
/// identity from the selected controller-admitted artifact. This route cannot
/// accept free-text knowledge or activate it for context use.
async fn propose_knowledge_from_investigation(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(artifact_id): Path<String>,
    Json(request): Json<ProposeKnowledgeFromInvestigationRequest>,
) -> Result<Json<harness_learning::KnowledgeItemV1>, ApiError> {
    authenticate(&state, &headers, true)?;
    let artifact_id = InvestigationArtifactId::parse(&artifact_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    let revision = state
        .orchestrator
        .store()
        .propose_knowledge_from_investigation(
            &harness_store::NewInvestigationKnowledgeCandidate {
                artifact_id,
                expected_artifact_sha256: request.expected_artifact_sha256,
                finding_id: request.finding_id,
                task_family: request.task_family,
                model_family: request.model_family,
                runtime_class: request.runtime_class,
            },
        )?;
    let item = serde_json::from_value(revision.payload).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(format!(
            "stored knowledge candidate has an invalid wire contract: {error}"
        )))
    })?;
    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProposeKnowledgeFromLivenessRequest {
    expected_episode_sha256: String,
    task_family: String,
    model_family: Option<String>,
    runtime_class: Option<String>,
}

/// Writes a suggestion only from two independently recovered, immutable
/// liveness episodes. The store derives all evidence, factual statement,
/// freshness, identity, and sensitivity; this route cannot accept prose,
/// activate knowledge, or alter task context.
async fn propose_knowledge_from_repeated_liveness(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
    Json(request): Json<ProposeKnowledgeFromLivenessRequest>,
) -> Result<Json<harness_learning::KnowledgeItemV1>, ApiError> {
    authenticate(&state, &headers, true)?;
    let episode_id = LivenessEpisodeId::parse(episode_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    let revision = state
        .orchestrator
        .store()
        .propose_knowledge_from_repeated_liveness(
            &harness_store::NewLivenessKnowledgeCandidate {
                episode_id,
                expected_episode_sha256: request.expected_episode_sha256,
                task_family: request.task_family,
                model_family: request.model_family,
                runtime_class: request.runtime_class,
            },
        )?;
    let item = serde_json::from_value(revision.payload).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(format!(
            "stored liveness knowledge candidate has an invalid wire contract: {error}"
        )))
    })?;
    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProposeKnowledgeFromReconciliationRequest {
    expected_episode_sha256: String,
    task_family: String,
    model_family: Option<String>,
    runtime_class: Option<String>,
}

/// Writes a suggestion only from two independently preserved reconciliation
/// episodes. The store derives all evidence, statement, freshness, identity,
/// and sensitivity; this route cannot treat preservation as recovery, retry a
/// task, activate knowledge, or alter task context.
async fn propose_knowledge_from_repeated_reconciliation(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
    Json(request): Json<ProposeKnowledgeFromReconciliationRequest>,
) -> Result<Json<harness_learning::KnowledgeItemV1>, ApiError> {
    authenticate(&state, &headers, true)?;
    let episode_id = ReconciliationEpisodeId::parse(episode_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    let revision = state
        .orchestrator
        .store()
        .propose_knowledge_from_repeated_reconciliation(
            &harness_store::NewReconciliationKnowledgeCandidate {
                episode_id,
                expected_episode_sha256: request.expected_episode_sha256,
                task_family: request.task_family,
                model_family: request.model_family,
                runtime_class: request.runtime_class,
            },
        )?;
    let item = serde_json::from_value(revision.payload).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(format!(
            "stored reconciliation knowledge candidate has an invalid wire contract: {error}"
        )))
    })?;
    Ok(Json(item))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewKnowledgeCandidateRequest {
    expected_knowledge_sha256: String,
    decision: KnowledgeReviewDecision,
}

/// Records an explicit local-session decision over the exact current candidate.
/// The Store binds the immutable pre-review revision/hash and rejects stale,
/// expired, or unclean acceptance evidence. This cannot inject task context or
/// change task/worktree/controller authority.
async fn review_knowledge_candidate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(knowledge_id): Path<String>,
    Json(request): Json<ReviewKnowledgeCandidateRequest>,
) -> Result<Json<harness_learning::KnowledgeItemV1>, ApiError> {
    let reviewer_id = authenticated_session_id(&state, &headers, true)?;
    let revision = state.orchestrator.store().review_knowledge_candidate(
        &harness_store::ReviewKnowledgeCandidate {
            knowledge_id,
            expected_knowledge_sha256: request.expected_knowledge_sha256,
            decision: request.decision,
            reviewer_id,
        },
    )?;
    let item = serde_json::from_value(revision.payload).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(format!(
            "stored reviewed knowledge has an invalid wire contract: {error}"
        )))
    })?;
    Ok(Json(item))
}

/// Reads one exact current knowledge wire for display or independent review.
/// Reading cannot alter task context or use the item as execution authority.
async fn get_current_knowledge_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(knowledge_id): Path<String>,
) -> Result<Json<harness_learning::KnowledgeItemV1>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .current_knowledge_item(&knowledge_id)?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KnowledgeQuery {
    repository_id: String,
    limit: Option<u32>,
}

/// Lists the current immutable knowledge records for one exact repository.
/// This read cannot inject or use an item as execution authority.
async fn list_current_knowledge_items(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<KnowledgeQuery>,
) -> Result<Json<Vec<harness_learning::KnowledgeItemV1>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_current_knowledge_items(&query.repository_id, query.limit.unwrap_or(50))?,
    ))
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterRunTimeGateRequest {
    not_before_ms: i64,
    #[serde(deserialize_with = "required_nullable_i64")]
    deadline_ms: Option<i64>,
}

fn required_nullable_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<i64>::deserialize(deserializer)
}

/// Registers the one operator-facing external adapter that is entirely local:
/// a controller-clock time gate. It contains no provider endpoint, command,
/// credential, or result-to-action mapping. Terminal observations remain
/// non-authorizing controller facts and cannot resume or otherwise change work.
async fn register_run_time_gate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(request): Json<RegisterRunTimeGateRequest>,
) -> Result<Json<harness_domain::ExternalCondition>, ApiError> {
    authenticate(&state, &headers, true)?;
    let run_id = RunId::from(run_id);
    Ok(Json(state.orchestrator.register_run_time_gate(
        &run_id,
        request.not_before_ms,
        request.deadline_ms,
    )?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterRunLocalCapacityGateRequest {
    minimum_available_bytes: u64,
    #[serde(deserialize_with = "required_nullable_i64")]
    deadline_ms: Option<i64>,
}

/// Registers a wake-only repository-root capacity condition. The controller
/// resolves the path from the run's durable repository custody; this request
/// therefore accepts no path, command, provider URL, credential, or action
/// mapping. Observations cannot resume or mutate the run.
async fn register_run_local_capacity_gate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(request): Json<RegisterRunLocalCapacityGateRequest>,
) -> Result<Json<harness_domain::ExternalCondition>, ApiError> {
    authenticate(&state, &headers, true)?;
    let run_id = RunId::from(run_id);
    Ok(Json(state.orchestrator.register_run_local_capacity_gate(
        &run_id,
        request.minimum_available_bytes,
        request.deadline_ms,
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
#[serde(deny_unknown_fields)]
struct ExecuteWaitInterventionRequest {
    expected_version: u64,
}

/// The only active intervention route is a bounded wait decision. It binds
/// the current episode revision and records no work beyond its receipt; it
/// cannot clear a stall or change any run/task/attempt custody.
async fn execute_wait_intervention(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
    Json(request): Json<ExecuteWaitInterventionRequest>,
) -> Result<Json<harness_domain::LivenessEpisode>, ApiError> {
    authenticate(&state, &headers, true)?;
    let episode_id = LivenessEpisodeId::parse(episode_id).map_err(|error| {
        ApiError::from(harness_store::StoreError::Validation(error.to_string()))
    })?;
    Ok(Json(state.orchestrator.store().execute_wait_intervention(
        &episode_id,
        request.expected_version,
        "local_session",
    )?))
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

/// Immutable reconciliation inventory facts. This route only exposes what the
/// controller recorded; it cannot apply, retry, release, or reset anything.
async fn list_reconciliation_findings(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
    Query(query): Query<BoundedReadQuery>,
) -> Result<Json<Vec<harness_domain::ReconciliationFinding>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let episode_id = reconciliation_episode_id(episode_id)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_reconciliation_findings(&episode_id, query.limit.unwrap_or(50))?,
    ))
}

/// Immutable receipts for controller actions already performed while
/// reconciling. The route has no action or recovery side effect.
async fn list_reconciliation_action_receipts(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(episode_id): Path<String>,
    Query(query): Query<BoundedReadQuery>,
) -> Result<Json<Vec<harness_domain::ReconciliationActionReceipt>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let episode_id = reconciliation_episode_id(episode_id)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_reconciliation_action_receipts(&episode_id, query.limit.unwrap_or(50))?,
    ))
}

fn reconciliation_episode_id(episode_id: String) -> Result<ReconciliationEpisodeId, ApiError> {
    ReconciliationEpisodeId::parse(episode_id)
        .map_err(|error| ApiError::from(harness_store::StoreError::Validation(error.to_string())))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PresenceQuery {
    operator_id: String,
}

async fn get_operator_presence(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<PresenceQuery>,
) -> Result<Json<harness_domain::OperatorPresence>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .operator_presence(&query.operator_id)?,
    ))
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotificationShadowBatchQuery {
    operator_id: String,
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

/// Reads immutable shadow-only notification plans. They compare a presence
/// policy against immediate mirror receipts but cannot change delivery timing,
/// suppress a source, or trigger an external notification channel.
async fn list_notification_shadow_batches(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<NotificationShadowBatchQuery>,
) -> Result<Json<Vec<harness_domain::NotificationShadowBatch>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_notification_shadow_batches(
                Some(&query.operator_id),
                query.limit.unwrap_or(50),
            )?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateNotificationShadowBatchRequest {
    operator_id: String,
    expected_presence_version: u64,
}

/// Records a complete snapshot-bound shadow plan only. The Store requires the
/// existing immediate mirror receipt for every source and refuses truncated
/// attention snapshots, so this route never creates a hidden defer or
/// suppression path.
async fn create_notification_shadow_batch(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<CreateNotificationShadowBatchRequest>,
) -> Result<Json<harness_domain::NotificationShadowBatch>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .create_notification_shadow_batch(
                &request.operator_id,
                request.expected_presence_version,
            )?,
    ))
}

/// Bounded, integrity-checked health for current attention revisions. This
/// read does not refresh the mirror, send or batch a notification, suppress a
/// source item, or turn a delivery receipt into execution authority.
async fn notification_delivery_health(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<harness_domain::NotificationDeliveryHealth>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state.orchestrator.store().notification_delivery_health()?,
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
        assert!(
            serde_json::from_value::<ProposeKnowledgeFromInvestigationRequest>(json!({
                "expected_artifact_sha256": "a".repeat(64),
                "finding_id": "finding_a",
                "task_family": "operator_control",
                "model_family": null,
                "runtime_class": null,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ProposeKnowledgeFromLivenessRequest>(json!({
                "expected_episode_sha256": "a".repeat(64),
                "task_family": "operator_control",
                "model_family": null,
                "runtime_class": null,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ProposeKnowledgeFromReconciliationRequest>(json!({
                "expected_episode_sha256": "a".repeat(64),
                "task_family": "operator_control",
                "model_family": null,
                "runtime_class": null,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<CreateNotificationShadowBatchRequest>(json!({
                "operator_id": "local_operator",
                "expected_presence_version": 1,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ReviewKnowledgeCandidateRequest>(json!({
                "expected_knowledge_sha256": "a".repeat(64),
                "decision": "accept",
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ReviewKnowledgeCandidateRequest>(json!({
                "expected_knowledge_sha256": "a".repeat(64),
                "decision": "defer",
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ExecuteWaitInterventionRequest>(json!({
                "expected_version": 1,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<RegisterRunTimeGateRequest>(json!({
                "not_before_ms": 1,
                "deadline_ms": null,
                "unexpected": true,
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<RegisterRunTimeGateRequest>(json!({
                "not_before_ms": 1,
            }))
            .is_err(),
            "the explicit nullable deadline field cannot silently default"
        );
        assert!(
            serde_json::from_value::<RegisterRunLocalCapacityGateRequest>(json!({
                "minimum_available_bytes": 1,
                "deadline_ms": null,
                "path": "/not-accepted",
            }))
            .is_err(),
            "the local capacity route must never accept a caller-selected path"
        );
        assert!(
            serde_json::from_value::<RegisterRunLocalCapacityGateRequest>(json!({
                "minimum_available_bytes": 1,
            }))
            .is_err(),
            "the explicit nullable deadline field cannot silently default"
        );
    }

    #[test]
    fn knowledge_collection_query_requires_an_exact_closed_repository_scope() {
        assert!(
            serde_json::from_value::<KnowledgeQuery>(json!({
                "repository_id": "repository_1",
                "limit": 50,
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<KnowledgeQuery>(json!({
                "limit": 50,
            }))
            .is_err(),
            "the collection has no cross-repository default scope"
        );
        assert!(
            serde_json::from_value::<KnowledgeQuery>(json!({
                "repository_id": "repository_1",
                "unexpected": true,
            }))
            .is_err(),
            "unknown query fields are not silently accepted"
        );
    }

    #[test]
    fn notification_reads_require_an_exact_operator_scope() {
        assert!(
            serde_json::from_value::<PresenceQuery>(json!({
                "operator_id": "local_operator",
            }))
            .is_ok()
        );
        assert!(serde_json::from_value::<PresenceQuery>(json!({})).is_err());
        assert!(
            serde_json::from_value::<PresenceQuery>(json!({
                "operator_id": "local_operator",
                "unexpected": true,
            }))
            .is_err()
        );

        assert!(
            serde_json::from_value::<NotificationShadowBatchQuery>(json!({
                "operator_id": "local_operator",
                "limit": 50,
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<NotificationShadowBatchQuery>(json!({
                "limit": 50,
            }))
            .is_err()
        );
    }
}
