//! Same-origin localhost REST API and durable SSE stream.

use std::{convert::Infallible, sync::Arc, time::Duration};

use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use harness_domain::{
    AgentRole, AgentSessionId, ApprovalId, ArtifactId, OutcomeClassification, OutcomeDimension,
    OutcomeId, OutcomeSubject, RepositoryId, RunId, TaskId, WorktreeId, is_safe_outcome_identifier,
    is_safe_outcome_reason_code,
};
use harness_orchestrator::{
    ApprovalDecisionRequest, ApproveSignoffRequest, AttestAcceptanceRequest, CreateRunRequest,
    OperatorSettings, Orchestrator, OrchestratorError, PlanReviewFinding,
    PrepareCoordinationCheckoutRequest, PublishDraftPrRequest, RegisterRepositoryRequest,
    RenameCodexAccountRequest, RepositoryDiscovery, RequestSignoffChanges, RetryTaskRequest,
    StartCodexAccountLoginRequest, UpdateOperatorSettingsRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::warn;
use uuid::Uuid;

const SESSION_COOKIE: &str = "harness_session";
const CSRF_HEADER: &str = "x-harness-csrf";
const SESSION_TTL_SECONDS: u64 = 12 * 60 * 60;
const EVENT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const EVENT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct ApiState {
    orchestrator: Arc<Orchestrator>,
    started: Instant,
    event_replay_limit: u32,
}

pub fn router(orchestrator: Arc<Orchestrator>) -> Router {
    let state = ApiState {
        event_replay_limit: orchestrator.ui_event_replay_limit(),
        orchestrator,
        started: Instant::now(),
    };
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/session", post(create_session))
        .route("/api/v1/runtime", get(runtime))
        .route("/api/v1/codex/accounts", get(codex_accounts))
        .route(
            "/api/v1/codex/accounts/{account_id}/select",
            post(select_codex_account),
        )
        .route(
            "/api/v1/codex/accounts/login",
            post(start_codex_account_login),
        )
        .route(
            "/api/v1/codex/accounts/login/{login_id}",
            get(codex_account_login_status),
        )
        .route(
            "/api/v1/codex/accounts/login/{login_id}/cancel",
            post(cancel_codex_account_login),
        )
        .route(
            "/api/v1/codex/accounts/{account_id}/rename",
            post(rename_codex_account),
        )
        .route(
            "/api/v1/codex/accounts/{account_id}/remove",
            post(remove_codex_account),
        )
        .route(
            "/api/v1/repositories",
            get(list_repositories).post(register_repository),
        )
        .route("/api/v1/repositories/discover", get(discover_repositories))
        .route("/api/v1/repositories/{repository_id}", get(get_repository))
        .route(
            "/api/v1/repositories/{repository_id}/inspect",
            post(inspect_repository),
        )
        .route(
            "/api/v1/repositories/{repository_id}/prepare-clean-checkout",
            post(prepare_coordination_checkout),
        )
        .route("/api/v1/runs", get(list_runs).post(create_run))
        .route("/api/v1/runs/{run_id}", get(get_run))
        .route(
            "/api/v1/runs/{run_id}/start-architecture",
            post(start_architecture),
        )
        .route(
            "/api/v1/runs/{run_id}/interview/start",
            post(start_intent_interview),
        )
        .route(
            "/api/v1/runs/{run_id}/interview/respond",
            post(respond_to_intent_interview),
        )
        .route(
            "/api/v1/runs/{run_id}/interview/confirm",
            post(confirm_intent_interview),
        )
        .route(
            "/api/v1/runs/{run_id}/interview/skip",
            post(skip_intent_interview),
        )
        .route("/api/v1/runs/{run_id}/plan/approve", post(approve_plan))
        .route(
            "/api/v1/runs/{run_id}/plan/request_changes",
            post(request_plan_changes),
        )
        .route(
            "/api/v1/runs/{run_id}/plan/resume-review",
            post(resume_blocked_plan_review),
        )
        .route(
            "/api/v1/runs/{run_id}/scheduler/pause",
            post(pause_scheduler),
        )
        .route(
            "/api/v1/runs/{run_id}/scheduler/resume",
            post(resume_scheduler),
        )
        .route(
            "/api/v1/runs/{run_id}/supervision/review",
            post(request_supervisor_review),
        )
        .route("/api/v1/runs/{run_id}/stop", post(stop_run))
        .route("/api/v1/runs/{run_id}/archive", post(archive_run))
        .route(
            "/api/v1/runs/{run_id}/approve-integration",
            post(approve_integration),
        )
        .route(
            "/api/v1/runs/{run_id}/signoff/approve",
            post(approve_signoff),
        )
        .route(
            "/api/v1/runs/{run_id}/signoff/request_changes",
            post(request_signoff_changes),
        )
        .route(
            "/api/v1/runs/{run_id}/signoff/acceptance/{acceptance_id}/attest",
            post(attest_acceptance),
        )
        .route(
            "/api/v1/runs/{run_id}/publish-draft-pr",
            post(publish_draft_pr),
        )
        .route(
            "/api/v1/runs/{run_id}/draft-pr/refresh-ci",
            post(refresh_draft_pr_ci),
        )
        .route("/api/v1/runs/{run_id}/tasks", get(list_tasks))
        .route("/api/v1/tasks/{task_id}", get(get_task))
        .route("/api/v1/tasks/{task_id}/retry", post(retry_task))
        .route(
            "/api/v1/tasks/{task_id}/request-review",
            post(request_task_review),
        )
        .route(
            "/api/v1/tasks/{task_id}/validate/{validator_id}",
            post(run_validator),
        )
        .route("/api/v1/agents/{agent_id}", get(get_agent))
        .route("/api/v1/agents/{agent_id}/steer", post(steer_agent))
        .route("/api/v1/agents/{agent_id}/interrupt", post(interrupt_agent))
        .route("/api/v1/agents/{agent_id}/activity", get(agent_activity))
        .route("/api/v1/approvals", get(list_approvals))
        .route(
            "/api/v1/approvals/{approval_id}/decision",
            post(decide_approval),
        )
        .route("/api/v1/worktrees", get(list_worktrees))
        .route("/api/v1/worktrees/{worktree_id}/diff", get(worktree_diff))
        .route(
            "/api/v1/worktrees/{worktree_id}/preserve",
            post(preserve_worktree),
        )
        .route("/api/v1/runs/{run_id}/evidence", get(run_evidence))
        .route(
            "/api/v1/improvement/outcomes",
            get(list_outcomes).post(record_operator_outcome),
        )
        .route(
            "/api/v1/improvement/outcomes/{outcome_id}",
            get(outcome_history),
        )
        .route("/api/v1/improvement/failures", get(list_failure_overview))
        .route(
            "/api/v1/improvement/traces/{trace_id}",
            get(get_failure_trace),
        )
        .route(
            "/api/v1/improvement/evaluations/runs/{evaluation_run_id}",
            get(get_evaluation_run),
        )
        .route(
            "/api/v1/improvement/evaluations/samples/{sample_id}",
            get(get_evaluation_sample),
        )
        .route(
            "/api/v1/improvement/evaluations/cases/{case_revision_id}",
            get(get_evaluation_case),
        )
        .route(
            "/api/v1/improvement/evaluations/occurrences/{occurrence_id}",
            get(get_evaluation_occurrence_source),
        )
        .route(
            "/api/v1/runs/{run_id}/evidence/export",
            post(export_evidence),
        )
        .route("/api/v1/runs/{run_id}/usage", get(run_usage))
        .route("/api/v1/usage", get(usage_breakdown))
        .route("/api/v1/events", get(events))
        .route(
            "/api/v1/settings",
            get(operator_settings).post(update_operator_settings),
        )
        .with_state(state)
}

async fn health(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.started.elapsed().as_secs(),
        "time": OffsetDateTime::now_utc().unix_timestamp(),
    }))
}

#[derive(Serialize)]
struct SessionResponse {
    csrf_token: String,
    expires_at_ms: i64,
}

async fn create_session(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    validate_origin(&headers)?;
    let session_id = Uuid::new_v4().simple().to_string();
    let csrf_token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let csrf_hash = sha256(csrf_token.as_bytes());
    let session = state.orchestrator.store().create_api_session(
        &session_id,
        &csrf_hash,
        SESSION_TTL_SECONDS,
    )?;
    let cookie = format!(
        "{SESSION_COOKIE}={session_id}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECONDS}"
    );
    let mut response = (
        StatusCode::CREATED,
        Json(SessionResponse {
            csrf_token,
            expires_at_ms: session.expires_at,
        }),
    )
        .into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie)
            .map_err(|_| ApiError::internal("failed to construct session cookie"))?,
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

async fn runtime(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<harness_domain::RuntimeStatus>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.runtime_status().await))
}

#[derive(Default, Deserialize)]
struct CodexAccountsQuery {
    #[serde(default)]
    force: bool,
}

async fn codex_accounts(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<CodexAccountsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .refresh_codex_accounts(query.force)
            .await?,
    ))
}

async fn select_codex_account(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state.orchestrator.select_codex_account(&account_id).await?,
    ))
}

async fn start_codex_account_login(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<StartCodexAccountLoginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .start_codex_account_login(request)
            .await?,
    ))
}

async fn codex_account_login_status(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(login_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .codex_account_login_status(&login_id)
            .await?,
    ))
}

async fn cancel_codex_account_login(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(login_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .cancel_codex_account_login(&login_id)
            .await?,
    ))
}

async fn rename_codex_account(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Json(request): Json<RenameCodexAccountRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .rename_codex_account(&account_id, request)
            .await?,
    ))
}

async fn remove_codex_account(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state.orchestrator.remove_codex_account(&account_id).await?,
    ))
}

async fn list_repositories(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<harness_domain::RepositorySummary>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.store().list_repositories()?))
}

async fn register_repository(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<RegisterRepositoryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    let repository = state.orchestrator.register_repository(request).await?;
    Ok((StatusCode::CREATED, Json(repository)))
}

async fn discover_repositories(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<RepositoryDiscovery>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.discover_repositories().await?))
}

async fn operator_settings(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<OperatorSettings>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.operator_settings()))
}

async fn update_operator_settings(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<UpdateOperatorSettingsRequest>,
) -> Result<Json<OperatorSettings>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(state.orchestrator.update_operator_settings(request)?))
}

async fn get_repository(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<harness_domain::RepositorySummary>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .repository(&RepositoryId::from(repository_id))?,
    ))
}

async fn inspect_repository(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<harness_domain::RepositorySummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .inspect_repository(&RepositoryId::from(repository_id))
            .await?,
    ))
}

async fn prepare_coordination_checkout(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(repository_id): Path<String>,
    Json(request): Json<PrepareCoordinationCheckoutRequest>,
) -> Result<Json<harness_domain::RepositorySummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .prepare_coordination_checkout(&RepositoryId::from(repository_id), request)
            .await?,
    ))
}

#[derive(Default, Deserialize)]
struct RunsQuery {
    repository_id: Option<String>,
    #[allow(dead_code)]
    state: Option<String>,
    #[allow(dead_code)]
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn list_runs(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Value>, ApiError> {
    authenticate(&state, &headers, false)?;
    let repository_id = query.repository_id.as_deref().map(RepositoryId::from);
    let mut runs = state
        .orchestrator
        .store()
        .list_runs(repository_id.as_ref(), true)?;
    if let Some(filter) = query.state {
        runs.retain(|run| run.state.to_string() == filter);
    }
    runs.truncate(query.limit.unwrap_or(50).clamp(1, 200));
    Ok(Json(json!({"items": runs, "next_cursor": null})))
}

async fn create_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<CreateRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    let run = state.orchestrator.create_run(request).await?;
    Ok((StatusCode::CREATED, Json(run)))
}

async fn get_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<harness_orchestrator::RunDetail>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.run_detail(&RunId::from(run_id))?))
}

async fn start_architecture(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .start_architecture(&RunId::from(run_id))
                .await?,
        ),
    ))
}

async fn start_intent_interview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .start_intent_interview(&RunId::from(run_id))
                .await?,
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentInterviewResponseBody {
    message: String,
}

async fn respond_to_intent_interview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<IntentInterviewResponseBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .respond_to_intent_interview(&RunId::from(run_id), &body.message, "local-user")
                .await?,
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmIntentInterviewBody {
    brief_digest: String,
}

async fn confirm_intent_interview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<ConfirmIntentInterviewBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .confirm_intent_interview(&RunId::from(run_id), &body.brief_digest, "local-user")
                .await?,
        ),
    ))
}

async fn skip_intent_interview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .skip_intent_interview(&RunId::from(run_id), "local-user")
                .await?,
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovePlanBody {
    task_graph_digest: String,
    note: Option<String>,
    #[serde(default)]
    allow_budget_override: bool,
}

async fn approve_plan(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<ApprovePlanBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    reject_long_note(body.note.as_deref())?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .approve_plan(
                    &RunId::from(run_id),
                    &body.task_graph_digest,
                    body.allow_budget_override,
                    body.note.as_deref(),
                    "local-user",
                )
                .await?,
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestPlanChangesBody {
    task_graph_digest: String,
    summary: Option<String>,
    findings: Vec<PlanReviewFinding>,
}

async fn request_plan_changes(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<RequestPlanChangesBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    reject_long_note(body.summary.as_deref())?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .request_plan_changes(
                    &RunId::from(run_id),
                    &body.task_graph_digest,
                    body.summary.as_deref(),
                    body.findings,
                    "local-user",
                )
                .await?,
        ),
    ))
}

async fn resume_blocked_plan_review(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .resume_blocked_plan_review(&RunId::from(run_id), "local-user")
                .await?,
        ),
    ))
}

async fn pause_scheduler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<harness_domain::RunSummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(state.orchestrator.set_scheduler_paused(
        &RunId::from(run_id),
        true,
        "local-user",
    )?))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeSchedulerBody {
    #[serde(default)]
    additional_token_budget: u64,
}

async fn resume_scheduler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    body: Option<Json<ResumeSchedulerBody>>,
) -> Result<Json<harness_domain::RunSummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    let run_id = RunId::from(run_id);
    let additional_token_budget = body.unwrap_or_default().0.additional_token_budget;
    state
        .orchestrator
        .resume_scheduler(&run_id, additional_token_budget, "local-user")?;
    let _ = state.orchestrator.tick(&run_id).await?;
    Ok(Json(state.orchestrator.store().run(&run_id)?))
}

async fn request_supervisor_review(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .request_supervisor_review(&RunId::from(run_id), "local-user")
                .await?,
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StopBody {
    mode: String,
    preserve_all_worktrees: Option<bool>,
}

async fn stop_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<StopBody>,
) -> Result<Json<harness_domain::RunSummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    if !matches!(
        body.mode.as_str(),
        "after_current_commands" | "interrupt_turns" | "cancel"
    ) {
        return Err(OrchestratorError::Validation(
            "stop mode must be after_current_commands, interrupt_turns, or cancel".to_owned(),
        )
        .into());
    }
    if body.preserve_all_worktrees == Some(false) {
        return Err(OrchestratorError::Validation(
            "failed and stopped worktrees are always preserved in v1".to_owned(),
        )
        .into());
    }
    let interrupt = matches!(body.mode.as_str(), "interrupt_turns" | "cancel");
    Ok(Json(
        state
            .orchestrator
            .stop_run(&RunId::from(run_id), interrupt, "local-user")
            .await?,
    ))
}

async fn archive_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<harness_domain::RunSummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok(Json(
        state
            .orchestrator
            .archive_run(&RunId::from(run_id), "local-user")?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApproveIntegrationBody {
    expected_head_sha: String,
    note: Option<String>,
}

async fn approve_integration(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<ApproveIntegrationBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    reject_long_note(body.note.as_deref())?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .approve_integration(&RunId::from(run_id), &body.expected_head_sha, "local-user")
                .await?,
        ),
    ))
}

async fn approve_signoff(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<ApproveSignoffRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    reject_long_note(body.note.as_deref())?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .approve_signoff(&RunId::from(run_id), body, "local-user")
                .await?,
        ),
    ))
}

async fn request_signoff_changes(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<RequestSignoffChanges>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    reject_long_note(Some(&body.summary))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .request_signoff_changes(&RunId::from(run_id), body, "local-user")
                .await?,
        ),
    ))
}

async fn attest_acceptance(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path((run_id, acceptance_id)): Path<(String, String)>,
    Json(body): Json<AttestAcceptanceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .attest_acceptance(&RunId::from(run_id), &acceptance_id, body, "local-user")
                .await?,
        ),
    ))
}

async fn publish_draft_pr(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(body): Json<PublishDraftPrRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .publish_draft_pr(&RunId::from(run_id), body, "local-user")
                .await?,
        ),
    ))
}

async fn refresh_draft_pr_ci(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .refresh_draft_pr_ci(&RunId::from(run_id), "local-user")
                .await?,
        ),
    ))
}

async fn list_tasks(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Vec<harness_domain::TaskSummary>>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_tasks(&RunId::from(run_id))?,
    ))
}

async fn get_task(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authenticate(&state, &headers, false)?;
    let task_id = TaskId::from(task_id);
    let task = state.orchestrator.store().task(&task_id)?;
    let packet = state.orchestrator.store().task_packet(&task_id)?;
    Ok(Json(json!({"task": task, "attempt_packet": packet})))
}

async fn retry_task(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
    Json(body): Json<RetryTaskRequest>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    let task_id = TaskId::from(task_id);
    let run_id = state.orchestrator.store().task(&task_id)?.run_id;
    let accepted = state
        .orchestrator
        .retry_task(&task_id, body, "local-user")
        .await?;
    let orchestrator = Arc::clone(&state.orchestrator);
    tokio::spawn(async move {
        if let Err(error) = orchestrator.tick(&run_id).await {
            warn!(%run_id, %error, "background task scheduling failed");
        }
    });
    Ok((StatusCode::ACCEPTED, Json(accepted)))
}

async fn request_task_review(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .request_task_review(&TaskId::from(task_id), "local-user")
                .await?,
        ),
    ))
}

async fn run_validator(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path((task_id, validator_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .orchestrator
                .run_validator(&TaskId::from(task_id), &validator_id)
                .await?,
        ),
    ))
}

async fn get_agent(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<Json<harness_domain::AgentSummary>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .store()
            .agent(&AgentSessionId::from(agent_id))?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SteerBody {
    message: String,
    #[allow(dead_code)]
    update_goal: Option<bool>,
}

async fn steer_agent(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
    Json(body): Json<SteerBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    state
        .orchestrator
        .steer_agent(
            &AgentSessionId::from(agent_id.clone()),
            &body.message,
            "local-user",
        )
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"state": "accepted", "target_id": agent_id})),
    ))
}

async fn interrupt_agent(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    state
        .orchestrator
        .interrupt_agent(&AgentSessionId::from(agent_id.clone()), "local-user")
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"state": "accepted", "target_id": agent_id})),
    ))
}

#[derive(Default, Deserialize)]
struct ActivityQuery {
    after: Option<i64>,
    limit: Option<u32>,
}

async fn agent_activity(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
    Query(query): Query<ActivityQuery>,
) -> Result<Json<Value>, ApiError> {
    authenticate(&state, &headers, false)?;
    let agent_id = AgentSessionId::from(agent_id);
    let limit = query.limit.unwrap_or(200).clamp(1, 1_000);
    let items = if let Some(after) = query.after {
        state
            .orchestrator
            .store()
            .list_activity(&agent_id, after.max(0), limit)?
    } else {
        state
            .orchestrator
            .store()
            .list_recent_activity(&agent_id, limit)?
    };
    let agent = state.orchestrator.store().agent(&agent_id)?;
    let messages = if agent.role == AgentRole::Governor {
        if let Some(task_id) = agent.task_id {
            state
                .orchestrator
                .store()
                .list_task_governor_messages(&task_id, 100)?
        } else {
            state
                .orchestrator
                .store()
                .list_agent_messages(&agent_id, 100)?
        }
    } else {
        state
            .orchestrator
            .store()
            .list_agent_messages(&agent_id, 100)?
    };
    let latest_message = messages.last();
    let next = items.last().map(|item| item.sequence);
    Ok(Json(
        json!({"items": items, "next_cursor": next, "latest_message": latest_message, "messages": messages}),
    ))
}

#[derive(Default, Deserialize)]
struct ApprovalQuery {
    state: Option<String>,
    run_id: Option<String>,
}

async fn list_approvals(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ApprovalQuery>,
) -> Result<Json<Vec<harness_domain::ApprovalSummary>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let run_id = query.run_id.as_deref().map(RunId::from);
    Ok(Json(
        state
            .orchestrator
            .store()
            .list_approvals(run_id.as_ref(), query.state.as_deref())?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiApprovalDecision {
    decision: String,
    note: Option<String>,
    expected_version: Option<u64>,
}

async fn decide_approval(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
    Json(body): Json<ApiApprovalDecision>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    let decision = match body.decision.as_str() {
        "approve_once" => "accept",
        "deny" => "decline",
        other => other,
    };
    let approval = state
        .orchestrator
        .decide_approval(
            &ApprovalId::from(approval_id),
            ApprovalDecisionRequest {
                decision: decision.to_owned(),
                note: body.note,
                expected_version: body.expected_version,
            },
            "local-user",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(approval)))
}

#[derive(Default, Deserialize)]
struct WorktreeQuery {
    run_id: Option<String>,
    state: Option<String>,
}

async fn list_worktrees(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<WorktreeQuery>,
) -> Result<Json<Vec<harness_domain::WorktreeSummary>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let run_id = query.run_id.as_deref().map(RunId::from);
    let mut worktrees = state.orchestrator.store().list_worktrees(run_id.as_ref())?;
    if let Some(filter) = query.state {
        worktrees.retain(|worktree| worktree.state == filter);
    }
    Ok(Json(worktrees))
}

async fn worktree_diff(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(worktree_id): Path<String>,
) -> Result<Json<harness_orchestrator::WorktreeDiffSummary>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .orchestrator
            .worktree_diff_summary(&WorktreeId::from(worktree_id))
            .await?,
    ))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreserveBody {
    reason: Option<String>,
}

async fn preserve_worktree(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(worktree_id): Path<String>,
    Json(body): Json<PreserveBody>,
) -> Result<Json<harness_domain::WorktreeSummary>, ApiError> {
    authenticate(&state, &headers, true)?;
    reject_long_note(body.reason.as_deref())?;
    Ok(Json(
        state
            .orchestrator
            .preserve_worktree(
                &WorktreeId::from(worktree_id),
                body.reason.as_deref(),
                "local-user",
            )
            .await?,
    ))
}

async fn run_evidence(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state.orchestrator.evidence_snapshot(&RunId::from(run_id))?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeVectorQuery {
    run_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureOverviewQuery {
    repository_id: String,
}

#[derive(Serialize)]
struct FailureOverviewResponse {
    taxonomy_version: &'static str,
    classified_occurrences: u64,
    unknown_occurrences: u64,
    clusters: Vec<FailureClusterResponse>,
}

#[derive(Serialize)]
struct FailureClusterResponse {
    id: String,
    failure_class: String,
    frequency: u64,
    severity: String,
    cost_upper_microusd: Option<u64>,
    unknown_cost_occurrences: u64,
    representative_occurrence_id: Option<String>,
    representative_run_id: Option<String>,
    representative_trace_id: Option<String>,
}

#[derive(Serialize)]
struct FailureTraceResponse {
    trace_id: String,
    run_id: String,
    rows: Vec<FailureTraceRowResponse>,
    outcomes: harness_domain::OutcomeVector,
}

#[derive(Serialize)]
struct FailureTraceRowResponse {
    id: String,
    kind: String,
    timestamp_ms: Option<i64>,
    redaction_class: String,
    source_receipt_count: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordOperatorOutcomeBody {
    run_id: String,
    subject: OutcomeSubject,
    dimension: OutcomeDimension,
    classification: OutcomeClassification,
    code: String,
    #[serde(default)]
    reason_code: Option<String>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    correction_artifact_id: Option<String>,
    supersedes: Vec<String>,
    idempotency_key: String,
}

async fn list_outcomes(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<OutcomeVectorQuery>,
) -> Result<Json<harness_domain::OutcomeVector>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_outcome_read_identifier(&query.run_id, "run_id")?;
    let vector = state
        .orchestrator
        .store()
        .outcome_vector(&RunId::from(query.run_id))?;
    validate_outcome_vector_response(&vector)?;
    Ok(Json(vector))
}

async fn list_failure_overview(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<FailureOverviewQuery>,
) -> Result<Json<FailureOverviewResponse>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_failure_read_identifier(&query.repository_id, "repository_id")?;
    let clusters = state
        .orchestrator
        .store()
        .failure_cluster_overview(&RepositoryId::from(query.repository_id))?;
    let mut classified_occurrences = 0;
    let mut unknown_occurrences = 0;
    let clusters = clusters
        .into_iter()
        .map(|cluster| -> Result<FailureClusterResponse, ApiError> {
            let failure_class = cluster
                .effective_class
                .filter(|value| closed_failure_class(value))
                .unwrap_or_else(|| "unknown".to_owned());
            if failure_class == "unknown" {
                unknown_occurrences += cluster.occurrences;
            } else {
                classified_occurrences += cluster.occurrences;
            }
            Ok(FailureClusterResponse {
                id: checked_failure_identifier(cluster.cluster_id, "cluster id")?,
                failure_class,
                frequency: cluster.occurrences,
                severity: cluster
                    .severity
                    .filter(|value| closed_severity(value))
                    .unwrap_or_else(|| "unknown".to_owned()),
                cost_upper_microusd: (cluster.cost_upper_microusd > 0
                    || cluster.unknown_cost_occurrences == 0)
                    .then_some(cluster.cost_upper_microusd),
                unknown_cost_occurrences: cluster.unknown_cost_occurrences,
                representative_occurrence_id: cluster
                    .representative_occurrence_id
                    .map(|id| checked_failure_identifier(id, "occurrence id"))
                    .transpose()?,
                representative_run_id: cluster
                    .representative_run_id
                    .map(|id| checked_failure_identifier(id.to_string(), "run id"))
                    .transpose()?,
                representative_trace_id: cluster
                    .representative_trace_id
                    .map(|id| checked_failure_identifier(id, "trace id"))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(FailureOverviewResponse {
        taxonomy_version: "harness.failure-taxonomy.v1",
        classified_occurrences,
        unknown_occurrences,
        clusters,
    }))
}

async fn get_failure_trace(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
) -> Result<Json<FailureTraceResponse>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_failure_read_identifier(&trace_id, "trace_id")?;
    let composition = state
        .orchestrator
        .store()
        .failure_trace_composition(&trace_id)?;
    validate_outcome_vector_response(&composition.outcomes)?;
    let rows = closed_trace_rows(&composition.trace_manifest)?;
    Ok(Json(FailureTraceResponse {
        trace_id: checked_failure_identifier(composition.trace_id, "trace id")?,
        run_id: checked_failure_identifier(composition.run_id.to_string(), "run id")?,
        rows,
        outcomes: composition.outcomes,
    }))
}

/// Receipt-only M2 read models.  These intentionally omit fixture locators,
/// commands, evidence bodies, artifacts, and any executor controls.
#[derive(Serialize)]
struct EvaluationRunResponse {
    id: String,
    controller_run_id: String,
    taskset_revision_id: String,
    grader_bundle_revision_id: String,
    split: String,
    status: String,
    invalidated: bool,
}

#[derive(Serialize)]
struct EvaluationSampleResponse {
    id: String,
    evaluation_run_id: String,
    eval_case_revision_id: String,
    arm: String,
    seed: u64,
    classification: String,
    sample_digest: String,
    invalidated: bool,
}

#[derive(Serialize)]
struct EvaluationCaseResponse {
    revision_id: String,
    case_id: String,
    revision: u64,
    payload_sha256: String,
    case_sha256: String,
    split: String,
    task_family: String,
    base_sha: String,
    setup_digest: String,
    grader_bundle_id: String,
    grader_bundle_revision: u64,
    grader_bundle_digest: String,
}

#[derive(Serialize)]
struct EvaluationOccurrenceSourceResponse {
    occurrence_id: String,
    repository_id: String,
    run_id: String,
    base_sha: String,
    source_receipt_sha256: String,
    source_kind: String,
    trace_revision_id: Option<String>,
    trace_digest: Option<String>,
    outcome_revision_id: Option<String>,
    outcome_digest: Option<String>,
}

async fn get_evaluation_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(evaluation_run_id): Path<String>,
) -> Result<Json<EvaluationRunResponse>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_failure_read_identifier(&evaluation_run_id, "run id")?;
    let value = state
        .orchestrator
        .store()
        .evaluation_run(&evaluation_run_id)?;
    Ok(Json(EvaluationRunResponse {
        id: checked_failure_identifier(value.id, "evaluation run id")?,
        controller_run_id: checked_failure_identifier(
            value.controller_run_id.to_string(),
            "controller run id",
        )?,
        taskset_revision_id: checked_failure_identifier(
            value.taskset_revision_id,
            "taskset revision id",
        )?,
        grader_bundle_revision_id: checked_failure_identifier(
            value.grader_bundle_revision_id,
            "grader revision id",
        )?,
        split: closed_eval_split(value.split)?,
        status: closed_evaluation_run_status(value.status)?,
        invalidated: value.invalidated,
    }))
}

async fn get_evaluation_sample(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(sample_id): Path<String>,
) -> Result<Json<EvaluationSampleResponse>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_failure_read_identifier(&sample_id, "sample id")?;
    let value = state.orchestrator.store().evaluation_sample(&sample_id)?;
    Ok(Json(EvaluationSampleResponse {
        id: checked_failure_identifier(value.id, "sample id")?,
        evaluation_run_id: checked_failure_identifier(
            value.evaluation_run_id,
            "evaluation run id",
        )?,
        eval_case_revision_id: checked_failure_identifier(
            value.eval_case_revision_id,
            "case revision id",
        )?,
        arm: closed_evaluation_arm(value.arm)?,
        seed: value.seed,
        classification: closed_sample_classification(value.classification)?,
        sample_digest: checked_digest(value.sample_digest, "sample digest")?,
        invalidated: value.invalidated,
    }))
}

async fn get_evaluation_case(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(case_revision_id): Path<String>,
) -> Result<Json<EvaluationCaseResponse>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_failure_read_identifier(&case_revision_id, "case revision id")?;
    let value = state
        .orchestrator
        .store()
        .immutable_eval_case_revision(&case_revision_id)?;
    let wire = value.wire;
    Ok(Json(EvaluationCaseResponse {
        revision_id: checked_failure_identifier(value.id, "case revision id")?,
        case_id: checked_failure_identifier(wire.case_id, "case id")?,
        revision: wire.revision,
        payload_sha256: checked_digest(value.payload_sha256, "case payload digest")?,
        case_sha256: checked_digest(wire.sha256, "case digest")?,
        split: closed_eval_split(wire.split)?,
        task_family: checked_failure_identifier(wire.task_family, "task family")?,
        base_sha: checked_base_sha(wire.runtime.base_sha)?,
        setup_digest: checked_digest(wire.runtime.setup_digest, "setup digest")?,
        grader_bundle_id: checked_failure_identifier(wire.grader_bundle_id, "grader bundle id")?,
        grader_bundle_revision: wire.grader_bundle_revision,
        grader_bundle_digest: checked_digest(wire.grader_bundle_digest, "grader bundle digest")?,
    }))
}

async fn get_evaluation_occurrence_source(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(occurrence_id): Path<String>,
) -> Result<Json<EvaluationOccurrenceSourceResponse>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_failure_read_identifier(&occurrence_id, "occurrence id")?;
    let value = state
        .orchestrator
        .store()
        .failure_development_case_source(&occurrence_id)?;
    Ok(Json(EvaluationOccurrenceSourceResponse {
        occurrence_id: checked_failure_identifier(value.occurrence_id, "occurrence id")?,
        repository_id: checked_failure_identifier(
            value.repository_id.to_string(),
            "repository id",
        )?,
        run_id: checked_failure_identifier(value.run_id.to_string(), "run id")?,
        base_sha: checked_base_sha(value.base_sha)?,
        source_receipt_sha256: checked_digest(
            value.source_receipt_sha256,
            "source receipt digest",
        )?,
        source_kind: closed_failure_source_kind(value.source_kind)?,
        trace_revision_id: value
            .trace_revision_id
            .map(|id| checked_failure_identifier(id, "trace revision id"))
            .transpose()?,
        trace_digest: value
            .trace_digest
            .map(|digest| checked_digest(digest, "trace digest"))
            .transpose()?,
        outcome_revision_id: value
            .outcome_revision_id
            .map(|id| checked_failure_identifier(id, "outcome revision id"))
            .transpose()?,
        outcome_digest: value
            .outcome_digest
            .map(|digest| checked_digest(digest, "outcome digest"))
            .transpose()?,
    }))
}

fn closed_eval_split(value: harness_eval::Split) -> Result<String, ApiError> {
    Ok(match value {
        harness_eval::Split::Training => "training",
        harness_eval::Split::Development => "development",
        harness_eval::Split::Holdout => "holdout",
        harness_eval::Split::Canary => "canary",
        harness_eval::Split::Quarantine => "quarantine",
    }
    .to_owned())
}

fn closed_evaluation_arm(value: harness_store::EvaluationArm) -> Result<String, ApiError> {
    Ok(match value {
        harness_store::EvaluationArm::Champion => "champion",
        harness_store::EvaluationArm::Challenger => "challenger",
    }
    .to_owned())
}

fn closed_evaluation_run_status(
    value: harness_store::EvaluationRunStatus,
) -> Result<String, ApiError> {
    Ok(match value {
        harness_store::EvaluationRunStatus::Recording => "recording",
        harness_store::EvaluationRunStatus::Completed => "completed",
        harness_store::EvaluationRunStatus::InfrastructureUnavailable => {
            "infrastructure_unavailable"
        }
        harness_store::EvaluationRunStatus::Invalidated => "invalidated",
    }
    .to_owned())
}

fn closed_sample_classification(
    value: harness_eval::SampleClassification,
) -> Result<String, ApiError> {
    Ok(match value {
        harness_eval::SampleClassification::Pass => "pass",
        harness_eval::SampleClassification::Fail => "fail",
        harness_eval::SampleClassification::InfrastructureUnavailable => {
            "infrastructure_unavailable"
        }
        harness_eval::SampleClassification::Invalidated => "invalidated",
    }
    .to_owned())
}

fn closed_failure_source_kind(value: String) -> Result<String, ApiError> {
    matches!(
        value.as_str(),
        "attempt_terminal" | "run_terminal" | "typed_outcome"
    )
    .then_some(value)
    .ok_or_else(|| ApiError::internal("persisted failure source kind is invalid"))
}

fn checked_digest(value: String, field: &str) -> Result<String, ApiError> {
    (value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    .then_some(value)
    .ok_or_else(|| ApiError::internal(&format!("persisted {field} is invalid")))
}

fn checked_base_sha(value: String) -> Result<String, ApiError> {
    (value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    .then_some(value)
    .ok_or_else(|| ApiError::internal("persisted base SHA is invalid"))
}

fn closed_trace_rows(manifest: &Value) -> Result<Vec<FailureTraceRowResponse>, ApiError> {
    let nodes = manifest
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::internal("persisted trace nodes are invalid"))?;
    nodes
        .iter()
        .map(|node| {
            let node = node
                .as_object()
                .ok_or_else(|| ApiError::internal("persisted trace node is invalid"))?;
            let id = node
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| is_safe_failure_identifier(value, 128))
                .ok_or_else(|| ApiError::internal("persisted trace node id is invalid"))?;
            let kind = node
                .get("kind")
                .and_then(Value::as_str)
                .filter(|value| closed_trace_kind(value))
                .ok_or_else(|| ApiError::internal("persisted trace node kind is invalid"))?;
            let redaction_class = node
                .get("redaction_class")
                .and_then(Value::as_str)
                .filter(|value| closed_redaction_class(value))
                .ok_or_else(|| ApiError::internal("persisted trace redaction class is invalid"))?;
            let timestamp_ms = match node.get("timestamp_ms") {
                Some(Value::Null) => None,
                Some(value) => Some(value.as_i64().ok_or_else(|| {
                    ApiError::internal("persisted trace node timestamp is invalid")
                })?),
                None => {
                    return Err(ApiError::internal(
                        "persisted trace node timestamp is missing",
                    ));
                }
            };
            let source_receipt_count = node
                .get("source_receipts")
                .and_then(Value::as_array)
                .filter(|receipts| !receipts.is_empty())
                .ok_or_else(|| ApiError::internal("persisted trace node receipts are invalid"))?
                .len()
                .try_into()
                .map_err(|_| ApiError::internal("persisted trace node receipt count is invalid"))?;
            Ok(FailureTraceRowResponse {
                id: id.to_owned(),
                kind: kind.to_owned(),
                timestamp_ms,
                redaction_class: redaction_class.to_owned(),
                source_receipt_count,
            })
        })
        .collect()
}

fn closed_failure_class(value: &str) -> bool {
    matches!(
        value,
        "unknown"
            | "policy_blocked"
            | "budget_exhausted"
            | "infrastructure_unavailable"
            | "protocol_error"
            | "integration_conflict"
            | "source_failure"
            | "inconclusive"
            | "cancelled_superseded"
    )
}

fn closed_severity(value: &str) -> bool {
    matches!(value, "unknown" | "low" | "medium" | "high" | "critical")
}

fn closed_redaction_class(value: &str) -> bool {
    matches!(
        value,
        "none"
            | "secret_removed"
            | "private_reasoning_removed"
            | "customer_data_removed"
            | "content_withheld"
    )
}

fn closed_trace_kind(value: &str) -> bool {
    matches!(
        value,
        "system_message"
            | "developer_message"
            | "user_message"
            | "model_message"
            | "reasoning_summary"
            | "tool_request"
            | "tool_result"
            | "command"
            | "file_read"
            | "file_change"
            | "approval_request"
            | "approval_decision"
            | "compaction"
            | "subagent_spawn"
            | "subagent_join"
            | "validation"
            | "finding"
            | "operator_feedback"
            | "outcome"
            | "unknown_protocol"
            | "run_lifecycle"
            | "attempt_boundary"
            | "runtime_restart"
    )
}

fn is_safe_failure_identifier(value: &str, maximum_length: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

fn validate_failure_read_identifier(value: &str, field: &str) -> Result<(), ApiError> {
    if is_safe_failure_identifier(value, 128) {
        Ok(())
    } else {
        Err(OrchestratorError::Validation(format!("invalid failure {field}")).into())
    }
}

fn checked_failure_identifier(value: String, field: &str) -> Result<String, ApiError> {
    validate_failure_read_identifier(&value, field)?;
    Ok(value)
}

async fn outcome_history(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(outcome_id): Path<String>,
) -> Result<Json<harness_domain::OutcomeHistory>, ApiError> {
    authenticate(&state, &headers, false)?;
    validate_outcome_read_identifier(&outcome_id, "outcome_id")?;
    let history = state
        .orchestrator
        .store()
        .outcome_history(&OutcomeId::from(outcome_id))?;
    validate_outcome_history_response(&history)?;
    Ok(Json(history))
}

async fn record_operator_outcome(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<RecordOperatorOutcomeBody>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    validate_operator_outcome_request(&body)?;
    let receipt =
        state
            .orchestrator
            .store()
            .record_operator_outcome(&harness_store::NewOperatorOutcome {
                run_id: RunId::from(body.run_id),
                subject: body.subject,
                dimension: body.dimension,
                classification: body.classification,
                code: body.code,
                reason_code: body.reason_code,
                note: body.note,
                correction_artifact_id: body.correction_artifact_id.map(ArtifactId::from),
                supersedes: body.supersedes,
                actor: "local-user".to_owned(),
                idempotency_key: body.idempotency_key,
            })?;
    Ok((StatusCode::CREATED, Json(receipt)))
}

fn validate_operator_outcome_request(body: &RecordOperatorOutcomeBody) -> Result<(), ApiError> {
    if !is_safe_outcome_identifier(&body.run_id, 128)
        || !is_safe_outcome_identifier(&body.subject.id, 128)
        || body.code.trim().is_empty()
        || body.code.chars().count() > 80
        || body
            .reason_code
            .as_ref()
            .is_some_and(|code| !is_safe_outcome_reason_code(code))
        || body
            .note
            .as_ref()
            .is_some_and(|note| note.chars().count() > 1_000)
        || !is_safe_outcome_identifier(&body.idempotency_key, 200)
        || body
            .supersedes
            .iter()
            .any(|id| !is_safe_outcome_identifier(id, 128))
    {
        return Err(OrchestratorError::Validation(
            "invalid bounded operator outcome request".to_owned(),
        )
        .into());
    }
    if !matches!(
        body.dimension,
        OutcomeDimension::OperatorAcceptance
            | OutcomeDimension::OperatorCorrection
            | OutcomeDimension::ReviewRegression
            | OutcomeDimension::PrReopened
            | OutcomeDimension::Rollback
            | OutcomeDimension::DownstreamRegression
    ) {
        return Err(OrchestratorError::Validation(
            "clients may record only operator outcome dimensions".to_owned(),
        )
        .into());
    }
    harness_domain::validate_operator_outcome_label(
        body.dimension,
        body.classification,
        &body.code,
    )
    .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
    Ok(())
}

fn validate_outcome_vector_response(
    vector: &harness_domain::OutcomeVector,
) -> Result<(), ApiError> {
    if !is_safe_outcome_identifier(vector.run_id.as_str(), 128)
        || vector.items.iter().any(|item| {
            !is_safe_outcome_identifier(item.outcome_id.as_str(), 128)
                || item.revisions.is_empty()
                || item.revisions.iter().any(|revision| {
                    revision.revision == 0
                        || !is_safe_outcome_identifier(&revision.revision_id, 128)
                        || revision.outcome.outcome_id != item.outcome_id
                        || revision.outcome.run_id != vector.run_id
                        || revision.outcome.subject != item.subject
                        || revision.outcome.dimension != item.dimension
                        || revision.outcome.validate().is_err()
                })
        })
    {
        return Err(OrchestratorError::Validation(
            "stored outcome response violates the closed OutcomeV1 contract".to_owned(),
        )
        .into());
    }
    Ok(())
}

fn validate_outcome_read_identifier(value: &str, field: &str) -> Result<(), ApiError> {
    if is_safe_outcome_identifier(value, 128) {
        Ok(())
    } else {
        Err(OrchestratorError::Validation(format!("invalid outcome {field}")).into())
    }
}

fn validate_outcome_history_response(
    history: &harness_domain::OutcomeHistory,
) -> Result<(), ApiError> {
    validate_outcome_vector_response(&harness_domain::OutcomeVector {
        run_id: history.run_id.clone(),
        items: vec![harness_domain::OutcomeVectorItem {
            outcome_id: history.outcome_id.clone(),
            subject: history
                .revisions
                .first()
                .map(|revision| revision.outcome.subject.clone())
                .ok_or_else(|| OrchestratorError::Validation("empty outcome history".to_owned()))?,
            dimension: history
                .revisions
                .first()
                .map(|revision| revision.outcome.dimension)
                .ok_or_else(|| OrchestratorError::Validation("empty outcome history".to_owned()))?,
            revisions: history.revisions.clone(),
            conflicted: history.conflicted,
        }],
    })
}

async fn export_evidence(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    authenticate(&state, &headers, true)?;
    let run_id = RunId::from(run_id);
    let output = state.orchestrator.default_export_path(&run_id);
    let export = state.orchestrator.export_evidence(&run_id, &output)?;
    Ok((StatusCode::ACCEPTED, Json(export)))
}

async fn run_usage(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<harness_domain::UsageSummary>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state.orchestrator.usage_summary(&RunId::from(run_id))?,
    ))
}

async fn usage_breakdown(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<harness_domain::UsageBreakdown>, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.orchestrator.usage_breakdown()?))
}

#[derive(Default, Deserialize)]
struct EventsQuery {
    cursor: Option<i64>,
    run_id: Option<String>,
}

async fn events(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let header_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok());
    let mut cursor = header_cursor.or(query.cursor).unwrap_or_default().max(0);
    let run_id = query.run_id.map(RunId::from);
    let store = state.orchestrator.store().clone();
    let replay_limit = state.event_replay_limit;
    let stream = stream! {
        let mut poll = interval(EVENT_POLL_INTERVAL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        poll.tick().await;
        let mut last_heartbeat = Instant::now();
        loop {
            match store.list_domain_events(cursor, run_id.as_ref(), replay_limit) {
                Ok(events) if !events.is_empty() => {
                    for item in events {
                        cursor = item.id;
                        let event = Event::default()
                            .id(item.id.to_string())
                            .event("domain")
                            .json_data(&item)
                            .unwrap_or_else(|_| Event::default().event("serialization_error").data("{}"));
                        yield Ok(event);
                    }
                }
                Ok(_) => {
                    poll.tick().await;
                    if last_heartbeat.elapsed() >= EVENT_HEARTBEAT_INTERVAL {
                        last_heartbeat = Instant::now();
                        yield Ok(Event::default().event("heartbeat").data(cursor.to_string()));
                    }
                }
                Err(error) => {
                    yield Ok(Event::default().event("stream_error").data(error.to_string()));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    };
    Ok(Sse::new(stream))
}

fn authenticate(state: &ApiState, headers: &HeaderMap, mutation: bool) -> Result<(), ApiError> {
    if mutation {
        validate_origin(headers)?;
    }
    let session_id = cookie(headers, SESSION_COOKIE)
        .ok_or_else(|| ApiError::unauthorized("local session cookie is missing"))?;
    let session = state
        .orchestrator
        .store()
        .api_session(&session_id)?
        .filter(|session| !session.revoked)
        .ok_or_else(|| ApiError::unauthorized("local session expired or was revoked"))?;
    if mutation {
        let supplied = headers
            .get(CSRF_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| ApiError::forbidden("CSRF token is missing"))?;
        let supplied_hash = sha256(supplied.as_bytes());
        if supplied_hash
            .as_bytes()
            .ct_eq(session.csrf_secret_hash.as_bytes())
            .unwrap_u8()
            != 1
        {
            return Err(ApiError::forbidden("CSRF token is invalid"));
        }
    }
    Ok(())
}

fn validate_origin(headers: &HeaderMap) -> Result<(), ApiError> {
    let origin = headers
        .get(header::ORIGIN)
        .ok_or_else(|| ApiError::forbidden("Origin header is required"))?
        .to_str()
        .map_err(|_| ApiError::forbidden("Origin header is invalid"))?;
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::forbidden("Host header is missing"))?;
    let authority = host
        .parse::<http::uri::Authority>()
        .map_err(|_| ApiError::forbidden("Host header is invalid"))?;
    let hostname = authority.host().trim_matches(['[', ']']);
    if !hostname.eq_ignore_ascii_case("localhost") && hostname != "127.0.0.1" && hostname != "::1" {
        return Err(ApiError::forbidden(
            "Host must identify the local loopback listener",
        ));
    }
    if origin != format!("http://{host}") && origin != format!("https://{host}") {
        return Err(ApiError::forbidden("cross-origin request rejected"));
    }
    if headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !matches!(value, "same-origin" | "same-site" | "none"))
    {
        return Err(ApiError::forbidden("cross-site request rejected"));
    }
    Ok(())
}

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|pair| pair.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then(|| value.to_owned()))
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn reject_long_note(value: Option<&str>) -> Result<(), ApiError> {
    if value.is_some_and(|value| value.chars().count() > 4_000) {
        return Err(OrchestratorError::Validation(
            "operator note exceeds 4,000 characters".to_owned(),
        )
        .into());
    }
    Ok(())
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn unauthorized(message: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.to_owned(),
        }
    }

    fn forbidden(message: &str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: message.to_owned(),
        }
    }

    fn internal(message: &str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal",
            message: message.to_owned(),
        }
    }
}

impl From<OrchestratorError> for ApiError {
    fn from(error: OrchestratorError) -> Self {
        let (status, code) = match &error {
            OrchestratorError::Validation(_) => (StatusCode::UNPROCESSABLE_ENTITY, "validation"),
            OrchestratorError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            OrchestratorError::Blocked(_) => (StatusCode::CONFLICT, "blocked"),
            OrchestratorError::Store(harness_store::StoreError::NotFound(_)) => {
                (StatusCode::NOT_FOUND, "not_found")
            }
            OrchestratorError::Store(harness_store::StoreError::Conflict(_)) => {
                (StatusCode::CONFLICT, "conflict")
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        Self {
            status,
            code,
            message: error.to_string(),
        }
    }
}

impl From<harness_store::StoreError> for ApiError {
    fn from(error: harness_store::StoreError) -> Self {
        Self::from(OrchestratorError::from(error))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": {
                    "code": self.code,
                    "message": self.message,
                    "request_id": Uuid::new_v4().to_string()
                }
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cookie_parser_uses_exact_name() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("other=x; harness_session=abc"),
        );
        assert_eq!(cookie(&headers, SESSION_COOKIE).as_deref(), Some("abc"));
    }

    #[test]
    fn csrf_digest_comparison_is_stable() {
        assert_eq!(sha256(b"token"), sha256(b"token"));
        assert_ne!(sha256(b"token"), sha256(b"other"));
    }

    #[test]
    fn mutation_origin_is_required_and_must_match_host() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:7310"));
        assert!(validate_origin(&headers).is_err());
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:7310"),
        );
        assert!(validate_origin(&headers).is_ok());
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://attacker.invalid"),
        );
        assert!(validate_origin(&headers).is_err());

        headers.insert(
            header::HOST,
            HeaderValue::from_static("attacker.invalid:7310"),
        );
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://attacker.invalid:7310"),
        );
        assert!(validate_origin(&headers).is_err());
    }

    #[test]
    fn operator_notes_are_bounded() {
        assert!(reject_long_note(Some(&"x".repeat(4_000))).is_ok());
        assert!(reject_long_note(Some(&"x".repeat(4_001))).is_err());
    }

    #[test]
    fn operator_outcomes_are_closed_and_identifier_safe() {
        let body = RecordOperatorOutcomeBody {
            run_id: "01JOUTCOME".to_owned(),
            subject: OutcomeSubject {
                kind: harness_domain::OutcomeSubjectKind::Run,
                id: "01JOUTCOME".to_owned(),
            },
            dimension: OutcomeDimension::OperatorAcceptance,
            classification: OutcomeClassification::Positive,
            code: "accepted_after_correction".to_owned(),
            reason_code: Some("verification_gap_corrected".to_owned()),
            note: Some("operator note".to_owned()),
            correction_artifact_id: None,
            supersedes: vec!["revision_1".to_owned()],
            idempotency_key: "outcome-1".to_owned(),
        };
        assert!(validate_operator_outcome_request(&body).is_ok());

        let mut automated = body;
        automated.dimension = OutcomeDimension::CiRequiredChecks;
        automated.code = "passed".to_owned();
        assert!(validate_operator_outcome_request(&automated).is_err());
        automated.dimension = OutcomeDimension::OperatorAcceptance;
        automated.code = "accepted_after_correction".to_owned();
        automated.reason_code = Some("free text is unsafe".to_owned());
        assert!(validate_operator_outcome_request(&automated).is_err());

        assert!(validate_outcome_read_identifier("run_01", "run_id").is_ok());
        assert!(validate_outcome_read_identifier("run/../01", "run_id").is_err());
        assert!(validate_outcome_read_identifier("outcome space", "outcome_id").is_err());
    }

    #[test]
    fn failure_trace_rows_are_closed_and_payload_free() {
        let manifest = json!({
            "nodes": [{
                "id": "n_01",
                "kind": "tool_result",
                "timestamp_ms": 42,
                "redaction_class": "content_withheld",
                "source_receipts": ["r_1", "r_2"],
                "payload": {"token": "not returned"}
            }]
        });
        let rows = closed_trace_rows(&manifest).expect("closed trace rows");
        let response = serde_json::to_value(rows).expect("serialize rows");
        assert_eq!(response[0]["source_receipt_count"], 2);
        assert!(response.to_string().contains("tool_result"));
        assert!(!response.to_string().contains("not returned"));
        assert!(
            closed_trace_rows(
                &json!({"nodes": [{"id": "n_01", "kind": "untrusted", "redaction_class": "none"}]})
            )
            .is_err()
        );
        assert!(closed_trace_rows(
            &json!({"nodes": [{"id": "n_01", "kind": "tool_result", "timestamp_ms": "never", "redaction_class": "none", "source_receipts": ["r_1"]}]})
        )
        .is_err());
        assert!(closed_trace_rows(
            &json!({"nodes": [{"id": "n_01", "kind": "tool_result", "timestamp_ms": null, "redaction_class": "none", "source_receipts": []}]})
        )
        .is_err());
        assert!(validate_failure_read_identifier("trace:01J", "trace_id").is_ok());
        assert!(validate_failure_read_identifier("trace/01J", "trace_id").is_err());
    }

    #[test]
    fn evaluation_read_models_are_receipt_only_and_closed() {
        let run = serde_json::to_value(EvaluationRunResponse {
            id: "evaluation-run-1".into(),
            controller_run_id: "run-1".into(),
            taskset_revision_id: "taskset-revision-1".into(),
            grader_bundle_revision_id: "grader-revision-1".into(),
            split: "development".into(),
            status: "completed".into(),
            invalidated: false,
        })
        .unwrap();
        assert_eq!(run["status"], "completed");
        assert_eq!(run.as_object().unwrap().len(), 7);
        let sample = serde_json::to_value(EvaluationSampleResponse {
            id: "sample-1".into(),
            evaluation_run_id: "evaluation-run-1".into(),
            eval_case_revision_id: "case-revision-1".into(),
            arm: "challenger".into(),
            seed: 7,
            classification: "pass".into(),
            sample_digest: "f".repeat(64),
            invalidated: false,
        })
        .unwrap();
        assert_eq!(sample["classification"], "pass");
        assert_eq!(sample.as_object().unwrap().len(), 8);
        let response = EvaluationCaseResponse {
            revision_id: "case-revision-1".into(),
            case_id: "case-1".into(),
            revision: 1,
            payload_sha256: "a".repeat(64),
            case_sha256: "b".repeat(64),
            split: "development".into(),
            task_family: "context".into(),
            base_sha: "c".repeat(40),
            setup_digest: "d".repeat(64),
            grader_bundle_id: "grader-1".into(),
            grader_bundle_revision: 1,
            grader_bundle_digest: "e".repeat(64),
        };
        let value = serde_json::to_value(response).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(object.len(), 12);
        for forbidden in ["fixture", "command", "evidence", "artifact", "objective"] {
            assert!(!object.contains_key(forbidden));
        }
        let occurrence = serde_json::to_value(EvaluationOccurrenceSourceResponse {
            occurrence_id: "occurrence-1".into(),
            repository_id: "repository-1".into(),
            run_id: "run-1".into(),
            base_sha: "f".repeat(40),
            source_receipt_sha256: "a".repeat(64),
            source_kind: "run_terminal".into(),
            trace_revision_id: Some("trace-1".into()),
            trace_digest: Some("b".repeat(64)),
            outcome_revision_id: None,
            outcome_digest: None,
        })
        .unwrap();
        let occurrence = occurrence.as_object().unwrap();
        assert_eq!(occurrence.len(), 10);
        assert!(!occurrence.contains_key("source_domain_event_id"));
        assert!(checked_digest("A".repeat(64), "digest").is_err());
        assert!(checked_base_sha("a".repeat(39)).is_err());
        assert_eq!(
            closed_evaluation_run_status(
                harness_store::EvaluationRunStatus::InfrastructureUnavailable
            )
            .unwrap(),
            "infrastructure_unavailable"
        );
        assert_eq!(
            closed_sample_classification(harness_eval::SampleClassification::Fail).unwrap(),
            "fail"
        );
    }
}
