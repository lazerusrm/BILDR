//! Read-only supervisory observation.
//!
//! This first slice deliberately compiles immutable snapshots only. It never
//! starts a model turn, proposes an action, or mutates controller run/task
//! state. Later runtime slices must consume the stored snapshot rather than
//! reconstructing one from mutable live state.

use harness_domain::{
    AgentSummary, DomainEvent, RunId, RunPlan, RunState, RunSummary, SupervisorMode,
    SupervisorSnapshotId, TaskPacket, TaskState, TaskSummary, format_timestamp, now_ms,
};
use harness_profile::SupervisionConfig;
use harness_store::{Store, StoreError, SupervisorSnapshotRecord, packet_digest};
use serde_json::{Value, json};
use std::sync::LazyLock;

// A 10k telemetry backlog must not postpone a later material event for hours
// at the normal maintenance cadence. The snapshot itself still includes at
// most the schema's 100 evidence event references.
const MAX_EVENTS_PER_OBSERVATION: u32 = 10_000;
const MAX_MATERIAL_EVENTS_PER_SNAPSHOT: usize = 100;
const MAX_TASKS_PER_SNAPSHOT: usize = 50;
const MAX_AGENTS_PER_SNAPSHOT: usize = 50;
const EFFICIENCY_POLICY_VERSION: &str = "supervision-efficiency.v1";
const SUPERVISOR_SNAPSHOT_SCHEMA: &str =
    include_str!("../../../schemas/harness.supervisor-snapshot.v1.schema.json");

static SUPERVISOR_SNAPSHOT_VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    let schema = serde_json::from_str(SUPERVISOR_SNAPSHOT_SCHEMA)
        .expect("checked-in supervisor snapshot schema parses");
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&schema)
        .expect("checked-in supervisor snapshot schema compiles")
});

pub(crate) fn observe_run(
    store: &Store,
    config: &SupervisionConfig,
    max_thread_count: u32,
    run_id: &RunId,
) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
    observe_run_with_force(store, config, max_thread_count, run_id, false)
}

/// Capture an operator-requested review snapshot immediately.  This is still
/// subject to the exact same bounded immutable envelope as scheduled reviews;
/// `force` only bypasses the short event coalescing delay so a person can
/// analyze an already-blocked run without waiting for a timer tick.
pub(crate) fn observe_run_now(
    store: &Store,
    config: &SupervisionConfig,
    max_thread_count: u32,
    run_id: &RunId,
) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
    observe_run_with_force(store, config, max_thread_count, run_id, true)
}

fn observe_run_with_force(
    store: &Store,
    config: &SupervisionConfig,
    max_thread_count: u32,
    run_id: &RunId,
    force: bool,
) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
    if config.mode == SupervisorMode::Disabled {
        return Ok(None);
    }
    if !matches!(
        config.mode,
        SupervisorMode::ObserveOnly | SupervisorMode::Advisory
    ) {
        return Err(StoreError::Validation(
            "supervision mode is not enabled by the advisory runtime".to_owned(),
        ));
    }
    // Run/task evidence and operator-control custody must come from one
    // SQLite transaction cut. Capturing them through separate Store calls can
    // otherwise construct a formally immutable but internally inconsistent
    // supervisor snapshot under a concurrent controller update.
    let (observation, control_plane) = store
        .capture_supervisor_observation_with_control_plane(run_id, MAX_EVENTS_PER_OBSERVATION)?;
    let Some(last_event) = observation.events.last() else {
        return Ok(None);
    };
    let material = observation
        .events
        .iter()
        .filter(|event| material_trigger(event).is_some())
        .collect::<Vec<_>>();
    if material.is_empty() {
        store.advance_supervisor_observation_cursor(run_id, last_event.id)?;
        return Ok(None);
    }
    let latest_material = material.last().expect("material events are non-empty");
    let coalesce_ms =
        i64::try_from(config.event_coalesce_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX);
    if !force && now_ms().saturating_sub(latest_material.occurred_at) < coalesce_ms {
        return Ok(None);
    }
    let Some((plan, plan_revision)) = observation.latest_plan else {
        // A snapshot contract binds a plan digest. Preserve a precise cursor
        // boundary while no plan exists; the next material plan event gets a
        // new snapshot instead of inventing a digest.
        store.advance_supervisor_observation_cursor(run_id, last_event.id)?;
        return Ok(None);
    };
    let trigger = material_trigger(latest_material).expect("material trigger checked");
    let event_cursor = last_event.id;
    let material_events = material
        .iter()
        .rev()
        .take(MAX_MATERIAL_EVENTS_PER_SNAPSHOT)
        .rev()
        .map(|event| (*event).clone())
        .collect::<Vec<_>>();
    let snapshot = store.record_supervisor_snapshot(
        run_id,
        event_cursor,
        trigger,
        |snapshot_id, revision| {
            let payload = build_snapshot(
                snapshot_id,
                revision,
                &observation.run,
                &plan,
                plan_revision,
                &observation.tasks,
                &observation.agents,
                &observation.expert_consultations,
                observation.run_tokens_used,
                &observation.repository_profile_id,
                &material_events,
                &control_plane,
                event_cursor,
                config,
                max_thread_count,
            );
            validate_snapshot_contract(&payload)?;
            let bytes = serde_json::to_vec(&payload)?;
            if bytes.len() > usize::try_from(config.max_snapshot_bytes).unwrap_or(usize::MAX) {
                return Err(StoreError::Validation(format!(
                    "supervisor snapshot is {} bytes, over configured {} byte bound",
                    bytes.len(),
                    config.max_snapshot_bytes
                )));
            }
            Ok(payload)
        },
    )?;
    Ok(Some(snapshot))
}

pub(crate) fn material_trigger(event: &DomainEvent) -> Option<&'static str> {
    // This deliberately is an allow-list.  Observe-only mode must not wake
    // on a new telemetry event simply because its spelling happens to include
    // a word such as "failed" or "budget".
    match event.event_type.as_str() {
        "run.lifecycle.transitioned"
            if event.payload.get("next_state").and_then(Value::as_str) == Some("EXECUTING") =>
        {
            Some("run_execution_started")
        }
        "task.start_failed" | "agent.governor.warm_continuation_failed" => Some("attempt_failed"),
        "task.governor.candidate_recovery_deferred" => Some("task_needs_help"),
        "agent.native_subagent.terminal" => Some("attempt_interrupted"),
        "task.verified" => Some("verifier_completed"),
        "run.integration.prepared" => Some("validation_completed"),
        "run.final_audit.rejected" => Some("attempt_failed"),
        "task.github_resource_recovered" => Some("dependency_unblocked"),
        "run.token_budget.reached"
        | "agent.governor.budget_hard_stop"
        | "agent.native_subagent.budget_hard_stop"
        | "agent.run_budget.hard_stop"
        | "agent.session_budget.hard_stop" => Some("budget_boundary_crossed"),
        "run.plan.revision_requested"
        | "run.plan.review_resume_requested"
        | "run.supervision.operator_review_requested" => Some("operator_steered"),
        "run.supervision.expert_completed" => Some("expert_completed"),
        "run.supervision.expert_failed" => Some("expert_failed"),
        "external_condition.time_gate_satisfied"
        | "external_condition.time_gate_deadline_elapsed"
        | "external_condition.time_gate_invalid_spec"
        | "external_condition.local_capacity_satisfied"
        | "external_condition.local_capacity_deadline_elapsed"
        | "external_condition.local_capacity_source_unavailable"
        | "external_condition.local_capacity_continuity_break" => {
            Some("external_condition_changed")
        }
        "task.governor.candidate_materialized" | "run.final_audit.accepted" => {
            Some("agent_completed")
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_snapshot(
    snapshot_id: &SupervisorSnapshotId,
    revision: u64,
    run: &RunSummary,
    plan: &RunPlan,
    plan_revision: u64,
    tasks: &[TaskSummary],
    agents: &[AgentSummary],
    expert_consultations: &[harness_store::ExpertConsultationObservation],
    run_tokens_used: u64,
    profile_id: &str,
    material_events: &[DomainEvent],
    control_plane: &harness_domain::ControlPlaneSnapshot,
    event_cursor: i64,
    config: &SupervisionConfig,
    max_thread_count: u32,
) -> Value {
    let trigger = material_trigger(material_events.last().expect("events are non-empty"))
        .expect("material trigger checked");
    let plan_digest = packet_digest(plan).expect("serializable run plan has a digest");
    let terminal_states = [
        TaskState::Verified,
        TaskState::Integrated,
        TaskState::CiProven,
        TaskState::LiveProven,
        TaskState::Closed,
    ];
    let blocked_states = [
        TaskState::Blocked,
        TaskState::NeedsHelp,
        TaskState::ChangesRequested,
        TaskState::Stalled,
    ];
    let active_states = [
        TaskState::Leased,
        TaskState::Starting,
        TaskState::Implementing,
        TaskState::Verifying,
        TaskState::ReviewReady,
    ];
    let task_payloads = tasks
        .iter()
        .take(MAX_TASKS_PER_SNAPSHOT)
        .map(|task| {
            let packet = plan
                .tasks
                .iter()
                .find(|packet| packet.task_id == task.external_task_id);
            task_snapshot(task, terminal_states.contains(&task.state), packet)
        })
        .collect::<Vec<_>>();
    let agent_payloads = agents
        .iter()
        .take(MAX_AGENTS_PER_SNAPSHOT)
        .map(|agent| agent_snapshot(agent, &run.objective))
        .collect::<Vec<_>>();
    let supervisor_tokens_used = agents
        .iter()
        .filter(|agent| agent.role == harness_domain::AgentRole::Supervisor)
        .fold(0_u64, |total, agent| {
            total.saturating_add(agent.tokens_used)
        });
    let expert_tokens_used = agents
        .iter()
        .filter(|agent| agent.role == harness_domain::AgentRole::Expert)
        .fold(0_u64, |total, agent| {
            total.saturating_add(agent.tokens_used)
        });
    let active_expert_requests = expert_consultations
        .iter()
        .filter(|consultation| matches!(consultation.request.state.as_str(), "QUEUED" | "RUNNING"))
        .count();
    let critical_path_task_ids = plan
        .tasks
        .iter()
        .take(MAX_TASKS_PER_SNAPSHOT)
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let milestones = plan
        .tasks
        .iter()
        .take(MAX_TASKS_PER_SNAPSHOT)
        .map(|task| {
            let runtime = tasks
                .iter()
                .find(|candidate| candidate.external_task_id == task.task_id);
            let state = runtime.map_or("pending", |item| {
                if terminal_states.contains(&item.state) {
                    "completed"
                } else if blocked_states.contains(&item.state) {
                    "blocked"
                } else if active_states.contains(&item.state) {
                    "in_progress"
                } else {
                    "pending"
                }
            });
            json!({
                "id": bounded(&format!("plan-{}", task.task_id), 128),
                "title": bounded(&task.title, 500),
                "state": state,
                "critical_path": true,
                "task_ids": [task.task_id],
            })
        })
        .collect::<Vec<_>>();
    let completed = tasks
        .iter()
        .filter(|task| terminal_states.contains(&task.state))
        .count();
    let elapsed_seconds = run
        .started_at
        .as_deref()
        .and_then(parse_timestamp_millis)
        .map(|started| now_ms().saturating_sub(started) / 1_000)
        .unwrap_or(0);
    let last_material = material_events
        .last()
        .map(|event| format_timestamp(event.occurred_at));
    let last_progress = material_events
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event.event_type.as_str(),
                "task.governor.candidate_materialized"
                    | "task.verified"
                    | "run.final_audit.accepted"
            )
        })
        .map(|event| format_timestamp(event.occurred_at));
    let operator_control = control_plane_summary(control_plane, run.id.as_str(), tasks);
    let custody_uncertain = operator_control
        .get("liveness")
        .and_then(Value::as_array)
        .is_some_and(|episodes| {
            episodes.iter().any(|episode| {
                matches!(
                    episode.get("state").and_then(Value::as_str),
                    Some("ownership_uncertain" | "recovery_required")
                )
            })
        })
        || operator_control
            .get("reconciliation")
            .and_then(Value::as_array)
            .is_some_and(|episodes| {
                episodes.iter().any(|episode| {
                    !matches!(
                        episode.get("state").and_then(Value::as_str),
                        Some("resolved" | "refused")
                    )
                })
            })
        || operator_control
            .get("truncated_sections")
            .and_then(Value::as_array)
            .is_some_and(|sections| !sections.is_empty());
    json!({
        "schema": "harness.supervisor-snapshot.v1",
        "snapshot_id": snapshot_id,
        "run_id": run.id,
        "revision": revision,
        "generated_at": format_timestamp(now_ms()),
        "event_cursor": event_cursor,
        "base_sha": run.base_sha,
        "profile_id": bounded(profile_id, 128),
        "plan_digest": plan_digest,
        "goal_revision": plan_revision,
        "trigger": {
            "kind": trigger,
            "occurred_at": last_material,
            "event_ids": material_events.iter().map(|event| format!("event-{}", event.id)).collect::<Vec<_>>(),
            "coalesced_count": material_events.len(),
        },
        "goal": {
            "original_objective": bounded(&run.objective, 12_000),
            "refined_objective": bounded(&plan.summary, 12_000),
            "status": goal_status(run.state),
            "hard_constraints": [],
            "non_goals": [],
            "success_criteria": [{
                "id": "controller-objective",
                "statement": bounded(&run.objective, 2_000),
                "state": if completed == tasks.len() && !tasks.is_empty() { "proven" } else { "in_progress" },
                "evidence_refs": [],
            }],
            "milestones": milestones,
            "critical_path_task_ids": critical_path_task_ids,
        },
        "run": {
            "state": run.state.to_string().to_ascii_lowercase(),
            "phase": bounded(&run.phase, 128),
            "elapsed_seconds": elapsed_seconds,
            "last_material_progress_at": last_progress,
            "ready_task_ids": tasks.iter().filter(|task| task.state == TaskState::Ready).map(|task| task.external_task_id.clone()).collect::<Vec<_>>(),
            "blocked_task_ids": tasks.iter().filter(|task| blocked_states.contains(&task.state)).map(|task| task.external_task_id.clone()).collect::<Vec<_>>(),
            "active_task_ids": tasks.iter().filter(|task| active_states.contains(&task.state)).map(|task| task.external_task_id.clone()).collect::<Vec<_>>(),
            "completion_candidate": run.state == RunState::FinalAudit,
        },
        "tasks": task_payloads,
        "agents": agent_payloads,
        "budgets": {
            "run_tokens_used": run_tokens_used,
            "run_tokens_remaining": run.run_token_budget.unwrap_or(run_tokens_used).saturating_sub(run_tokens_used),
            "supervisor_tokens_used": supervisor_tokens_used,
            "supervisor_token_budget_per_request": config.supervisor.token_budget,
            "expert_tokens_used": expert_tokens_used,
            "expert_token_budget_per_request": config.expert.token_budget,
            "expert_completed_cap_per_signature": config.expert.max_completed_per_signature,
            "active_expert_requests": active_expert_requests,
            "active_thread_count": agents.iter().filter(|agent| agent.active_turn_id.is_some()).count(),
            "max_thread_count": max_thread_count,
        },
        "allowed_actions": allowed_actions(run, tasks, plan, config.mode, custody_uncertain),
        "evidence_refs": material_events.iter().map(|event| json!({
            "kind": "event",
            "id": format!("event-{}", event.id),
            "summary": bounded(&event.event_type, 1_000),
        })).collect::<Vec<_>>(),
        "prior_decision": null,
        "expert_consultations": expert_consultations.iter().map(|consultation| {
            let request = &consultation.request;
            let response = consultation.response.as_ref();
            json!({
                "request_id": request.id,
                "action_id": request.action_id,
                "state": request.state,
                "category": request.payload.get("category").and_then(Value::as_str).unwrap_or("other"),
                "requested_model": request.requested_model,
                "requested_effort": request.requested_effort,
                "escalation_signature": request.signature,
                "response": response.map(|response| json!({
                    "id": response.id,
                    "payload_sha256": response.payload_sha256,
                    "verdict": response.payload.get("verdict").and_then(Value::as_str).unwrap_or("insufficient_evidence"),
                    "summary": bounded(response.payload.get("summary").and_then(Value::as_str).unwrap_or("No expert summary was retained."), 4_000),
                    "recommendation": bounded(response.payload.get("recommendation").and_then(Value::as_str).unwrap_or("No expert recommendation was retained."), 6_000),
                    "evidence_refs": response.payload.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
                })),
            })
        }).collect::<Vec<_>>(),
        "operator_control": operator_control,
    })
}

pub(crate) fn control_plane_summary(
    snapshot: &harness_domain::ControlPlaneSnapshot,
    run_id: &str,
    tasks: &[TaskSummary],
) -> Value {
    let facts = control_plane_fact_payload(snapshot, run_id, tasks);
    let summary = json!({
        "schema": "harness.supervisor-control-facts.v1",
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_revision": snapshot.revision,
        "snapshot_sha256": snapshot.sha256,
        "event_cursor": snapshot.event_cursor,
        "attention": facts["attention"],
        "material_progress": facts["material_progress"],
        "liveness": facts["liveness"],
        "reconciliation": facts["reconciliation"],
        "investigations": facts["investigations"],
        "external_conditions": facts["external_conditions"],
        "truncated_sections": facts["truncated_sections"],
        "limitations": [
            "operator-control facts are bounded read-only evidence",
            "the supervisor cannot close attention, modify liveness, reconcile ownership, or deliver notifications",
        ],
    });
    summary
}

const OPERATOR_CONTROL_FACT_KEYS: [&str; 7] = [
    "attention",
    "material_progress",
    "liveness",
    "reconciliation",
    "investigations",
    "external_conditions",
    "truncated_sections",
];

/// Canonical, run-scoped controller facts used both in a supervisor snapshot
/// and during action freshness validation. Metadata such as a global snapshot
/// revision is intentionally excluded: recording an advisory decision itself
/// must not stale its own proposal.
pub(crate) fn control_plane_fact_payload(
    snapshot: &harness_domain::ControlPlaneSnapshot,
    run_id: &str,
    tasks: &[TaskSummary],
) -> Value {
    let run_rows = |section: &harness_domain::SnapshotSection| {
        section
            .rows
            .iter()
            .filter(|row| row.get("run_id").and_then(Value::as_str) == Some(run_id))
            .take(50)
            .cloned()
            .collect::<Vec<_>>()
    };
    let task_owner_ids = tasks
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    let attempt_owner_ids = snapshot
        .attempts
        .rows
        .iter()
        .filter(|row| row.get("run_id").and_then(Value::as_str) == Some(run_id))
        .filter_map(|row| row.get("attempt_id").and_then(Value::as_str))
        .collect::<Vec<_>>();
    let external_conditions = snapshot
        .external_conditions
        .rows
        .iter()
        .filter(|row| {
            let owner_id = row.get("owner_id").and_then(Value::as_str);
            match row.get("owner_type").and_then(Value::as_str) {
                Some("run") => owner_id == Some(run_id),
                Some("task") => owner_id.is_some_and(|id| task_owner_ids.contains(&id)),
                Some("attempt") => owner_id.is_some_and(|id| attempt_owner_ids.contains(&id)),
                _ => false,
            }
        })
        .take(50)
        .cloned()
        .collect::<Vec<_>>();
    let truncated_sections = snapshot
        .truncation
        .iter()
        .filter(|entry| {
            matches!(
                entry.section.as_str(),
                "attention"
                    | "attempts"
                    | "investigations"
                    | "external_conditions"
                    | "progress"
                    | "progress_classifier"
                    | "liveness"
                    | "reconciliation"
            )
        })
        .map(|entry| {
            json!({
                "section": entry.section,
                "omitted_rows": entry.omitted_rows,
                "limit": entry.limit,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "attention": run_rows(&snapshot.attention),
        "material_progress": run_rows(&snapshot.progress),
        "liveness": run_rows(&snapshot.liveness),
        "reconciliation": run_rows(&snapshot.reconciliation),
        "investigations": run_rows(&snapshot.investigations),
        "external_conditions": external_conditions,
        "truncated_sections": truncated_sections,
    })
}

pub(crate) fn operator_control_fact_payload_from_summary(summary: &Value) -> Result<Value, String> {
    let object = summary
        .as_object()
        .ok_or_else(|| "operator-control binding is not an object".to_owned())?;
    if object.get("schema").and_then(Value::as_str) != Some("harness.supervisor-control-facts.v1") {
        return Err("operator-control binding has an unknown schema".to_owned());
    }
    let mut facts = serde_json::Map::new();
    for key in OPERATOR_CONTROL_FACT_KEYS {
        let value = object
            .get(key)
            .ok_or_else(|| format!("operator-control binding is missing {key}"))?;
        if !value.is_array() {
            return Err(format!("operator-control binding {key} is not an array"));
        }
        facts.insert(key.to_owned(), value.clone());
    }
    Ok(Value::Object(facts))
}

fn validate_snapshot_contract(payload: &Value) -> Result<(), StoreError> {
    SUPERVISOR_SNAPSHOT_VALIDATOR
        .validate(payload)
        .map_err(|error| {
            StoreError::Validation(format!(
                "generated supervisor snapshot does not conform to its strict contract: {error}"
            ))
        })
}

fn allowed_actions(
    run: &RunSummary,
    tasks: &[TaskSummary],
    plan: &RunPlan,
    mode: SupervisorMode,
    custody_uncertain: bool,
) -> Vec<&'static str> {
    if mode != SupervisorMode::Advisory {
        return vec!["wait"];
    }
    let mut actions = vec!["wait"];
    if custody_uncertain {
        // An unresolved ownership/recovery record is a hard controller
        // boundary: the supervisor may only ask the human to pause and inspect
        // it. It must never propose a fresh attempt, continuation, replan, or
        // any action that could create a competing writer.
        actions.push("pause_for_human");
        return actions;
    }
    // A supervisor snapshot is advisory evidence, not exclusive ownership
    // proof. It must not propose a fresh attempt until a transactional
    // proof-consuming recovery controller exists.
    if run.scheduler_paused
        && !matches!(
            run.state,
            RunState::Completed | RunState::Canceled | RunState::Failed | RunState::Archived
        )
    {
        actions.push("continue_attempt");
    }
    if matches!(
        run.state,
        RunState::Blocked | RunState::PlanRevisionRequired | RunState::PlanReviewRequired
    ) {
        actions.push("request_replan");
    }
    if run.state == RunState::Blocked && run.phase.starts_with("plan_review_") {
        actions.push("start_followup_turn");
    }
    // A model-declared impact label is never enough to start Sol. The
    // controller must have certified at least one still-blocked task with a
    // high-risk plan packet. The decision/action broker repeats this exact
    // lookup immediately before launch.
    if run.state == RunState::Blocked
        && tasks.iter().any(|task| {
            matches!(
                task.state,
                TaskState::NeedsHelp
                    | TaskState::ChangesRequested
                    | TaskState::Interrupted
                    | TaskState::Stalled
                    | TaskState::Blocked
                    | TaskState::Failed
            ) && plan
                .tasks
                .iter()
                .any(|packet| packet.task_id == task.external_task_id && packet.is_high_risk())
        })
    {
        actions.push("request_expert");
    }
    actions
}

fn task_snapshot(task: &TaskSummary, completed: bool, packet: Option<&TaskPacket>) -> Value {
    json!({
        // Decision targets must be the controller's exact opaque ID, never a
        // human-facing external label that could be duplicated or remapped.
        "task_id": task.id,
        "title": bounded(&task.title, 500),
        "state": task.state.to_string().to_ascii_lowercase(),
        "priority": priority_number(&task.priority),
        "risk_flags": packet.map_or_else(Vec::new, |packet| {
            packet.risk_flags.iter().map(|flag| bounded(flag, 128)).collect()
        }),
        "objective": bounded(&task.objective, 4_000),
        "depends_on": task.dependencies,
        "current_attempt_id": null,
        "retry_count": task.attempt.saturating_sub(1),
        "same_failure_repetitions": 0,
        "progress": progress_vector(completed),
        "efficiency": efficiency_vector(0),
        "blockers": task.failure_reason.iter().map(|reason| bounded(reason, 1_000)).collect::<Vec<_>>(),
        "evidence_refs": [],
    })
}

fn agent_snapshot(agent: &AgentSummary, fallback_goal: &str) -> Value {
    json!({
        "session_id": agent.id,
        "role": agent_role_name(agent.role),
        "task_id": agent.task_id,
        "attempt_id": null,
        "state": agent_state(&agent.state),
        "requested_model": bounded(&agent.requested_model, 128),
        "effective_model": bounded(agent.effective_model.as_deref().unwrap_or(&agent.requested_model), 128),
        "requested_effort": known_effort(&agent.requested_reasoning_effort),
        "effective_effort": known_effort(agent.effective_reasoning_effort.as_deref().unwrap_or(&agent.requested_reasoning_effort)),
        "current_goal": bounded(agent.current_goal.as_deref().unwrap_or(fallback_goal), 4_000),
        "help_requested": matches!(agent.state.as_str(), "FAILED" | "STALLED" | "BLOCKED" | "INTERRUPTED"),
        "efficiency": efficiency_vector(agent.tokens_used),
    })
}

fn progress_vector(completed: bool) -> Value {
    json!({
        "milestones_completed": u64::from(completed),
        "milestones_total": 1,
        "criteria_proven": u64::from(completed),
        "criteria_total": 1,
        "candidate_materialized": completed,
        "validations_passed": 0,
        "validations_failed": 0,
        "blocking_findings": 0,
        "material_progress_sequence": u64::from(completed),
        "last_material_progress_at": null,
    })
}

fn efficiency_vector(tokens_total: u64) -> Value {
    json!({
        "class": "unknown",
        "tokens_total": tokens_total,
        "tokens_since_material_progress": tokens_total,
        "material_progress_events": 0,
        "semantic_repeat_count": 0,
        "tool_calls": 0,
        "tool_failures": 0,
        "active_seconds": 0,
        "externally_blocked_seconds": 0,
        "baseline_sample_size": 0,
        "policy_version": EFFICIENCY_POLICY_VERSION,
        "reason_codes": ["cold_start"],
    })
}

fn goal_status(state: RunState) -> &'static str {
    match state {
        RunState::Blocked => "blocked",
        RunState::TaskVerification | RunState::IntegrationVerification | RunState::FinalAudit => {
            "verification"
        }
        RunState::HumanReview | RunState::PublicationReady | RunState::DraftPrCreated => {
            "human_review"
        }
        RunState::Completed | RunState::Canceled | RunState::Failed | RunState::Archived => {
            "terminal"
        }
        _ => "active",
    }
}

fn priority_number(priority: &str) -> u64 {
    priority
        .trim_start_matches(['P', 'p'])
        .parse::<u64>()
        .ok()
        .filter(|value| *value <= 1_000)
        .unwrap_or(1_000)
}

fn agent_state(state: &str) -> &'static str {
    match state {
        "STARTING" | "RUNNING" | "STEERED" | "WAITING_APPROVAL" => "active",
        "PAUSED" => "waiting",
        "COMPLETED" | "TURN_COMPLETE" => "completed",
        "INTERRUPTED" => "interrupted",
        "FAILED" | "STALLED" | "BLOCKED" => "failed",
        "QUEUED" => "queued",
        _ => "idle",
    }
}

fn agent_role_name(role: harness_domain::AgentRole) -> &'static str {
    use harness_domain::AgentRole;
    match role {
        AgentRole::Interviewer => "interviewer",
        AgentRole::Architect => "architect",
        AgentRole::PlanReviewer => "plan_reviewer",
        AgentRole::Explorer => "explorer",
        AgentRole::Governor => "governor",
        AgentRole::Worker => "worker",
        AgentRole::HighRiskWorker => "high_risk_worker",
        AgentRole::Integrator => "integrator",
        AgentRole::Verifier => "verifier",
        AgentRole::FinalAuditor => "final_auditor",
        AgentRole::CiTriage => "ci_triage",
        AgentRole::Supervisor => "supervisor",
        AgentRole::Expert => "expert",
        AgentRole::Investigator => "investigator",
    }
}

fn known_effort(value: &str) -> &'static str {
    match value {
        "none" => "none",
        "low" => "low",
        "medium" => "medium",
        "high" => "high",
        "xhigh" => "xhigh",
        "max" => "max",
        _ => "none",
    }
}

fn bounded(value: &str, maximum: usize) -> String {
    if value.is_empty() {
        return "unspecified".to_owned();
    }
    value.chars().take(maximum).collect()
}

pub(crate) fn parse_timestamp_millis(value: &str) -> Option<i64> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .ok()
        .and_then(|timestamp| i64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000).ok())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_EVENTS_PER_OBSERVATION, MAX_MATERIAL_EVENTS_PER_SNAPSHOT, build_snapshot,
        material_trigger, validate_snapshot_contract,
    };
    use harness_domain::{
        AgentRole, AgentSessionId, AgentSummary, DiffBudget, DomainEvent, ExpertRequestId,
        ExpertResponseId, RepositoryId, RunId, RunPlan, RunState, RunSummary, SandboxMode,
        SupervisorActionId, SupervisorDecisionId, SupervisorSnapshotId, TaskId, TaskMilestone,
        TaskPacket, TaskState, TaskSummary,
    };
    use harness_profile::SupervisionConfig;
    use harness_store::{ExpertConsultationObservation, ExpertRequestRecord, ExpertResponseRecord};
    use serde_json::json;

    fn event(event_type: &str, payload: serde_json::Value) -> DomainEvent {
        DomainEvent {
            id: 7,
            run_id: Some(RunId::from("run-1")),
            event_type: event_type.to_owned(),
            aggregate_type: "agent".to_owned(),
            aggregate_id: "agent-1".to_owned(),
            occurred_at: 1,
            payload,
        }
    }

    #[test]
    fn telemetry_only_events_never_wake_the_observer() {
        assert_eq!(material_trigger(&event("agent.heartbeat", json!({}))), None);
        assert_eq!(
            material_trigger(&event("telemetry.future_failed_counter", json!({}))),
            None
        );
    }

    #[test]
    fn material_event_allowlist_maps_only_defined_controller_events() {
        assert_eq!(
            material_trigger(&event(
                "run.lifecycle.transitioned",
                json!({"next_state": "EXECUTING"}),
            )),
            Some("run_execution_started")
        );
        assert_eq!(
            material_trigger(&event(
                "run.lifecycle.transitioned",
                json!({"next_state": "PREPARING"}),
            )),
            None
        );
        assert_eq!(
            material_trigger(&event("task.verified", json!({}))),
            Some("verifier_completed")
        );
        assert_eq!(
            material_trigger(&event(
                "external_condition.local_capacity_satisfied",
                json!({"consequential_action": "none"}),
            )),
            Some("external_condition_changed")
        );
        assert_eq!(
            material_trigger(&event("run.supervision.expert_completed", json!({}))),
            Some("expert_completed")
        );
        assert_eq!(
            material_trigger(&event("run.supervision.expert_failed", json!({}))),
            Some("expert_failed")
        );
    }

    #[test]
    fn post_consultation_snapshot_is_schema_valid_and_reports_real_expert_usage() {
        let run_id = RunId::from("run-1");
        let task_id = TaskId::from("task-internal-1");
        let run = RunSummary {
            id: run_id.clone(),
            repository_id: RepositoryId::from("repository-1"),
            title: "Expert snapshot fixture".to_owned(),
            objective: "Preserve a bounded expert consultation in the next review.".to_owned(),
            mode: "plan_and_implement".to_owned(),
            publication_mode: "local_only".to_owned(),
            state: RunState::Blocked,
            phase: "integration_conflict".to_owned(),
            base_ref: "main".to_owned(),
            base_sha: "a".repeat(40),
            integration_branch: None,
            integration_sha: None,
            authority_digest: "fixture".to_owned(),
            created_at: "2026-08-14T00:00:00Z".to_owned(),
            started_at: Some("2026-08-14T00:00:00Z".to_owned()),
            completed_at: None,
            failure_reason: Some("The integration contract remains disputed.".to_owned()),
            scheduler_paused: false,
            run_token_budget: Some(1_000_000),
            version: 1,
        };
        let plan = RunPlan {
            schema: "harness.orchestration.plan.v1".to_owned(),
            summary: "Resolve the blocked high-impact integration contract.".to_owned(),
            tasks: vec![TaskPacket {
                schema: "harness.orchestration.task.v1".to_owned(),
                program_id: "SUPERVISION".to_owned(),
                task_id: "task-1".to_owned(),
                title: "Resolve integration contract".to_owned(),
                state: "blocked".to_owned(),
                priority: "P1".to_owned(),
                execution_mode: "controller".to_owned(),
                execution_kind: harness_domain::TaskExecutionKind::Implementation,
                investigation_scope: None,
                owner_profile: "general".to_owned(),
                reviewer_profile: "general".to_owned(),
                checklist_rows: vec!["Keep the consultation advisory.".to_owned()],
                authority_refs: vec!["CONTRIBUTING.md".to_owned()],
                base_sha: "a".repeat(40),
                dependency_shas: Default::default(),
                depends_on: Vec::new(),
                owned_paths: vec!["crates/example.rs".to_owned()],
                forbidden_paths: Vec::new(),
                reserved_serial_paths: Vec::new(),
                objective: "Resolve the conflicting integration contract.".to_owned(),
                milestones: vec![TaskMilestone {
                    id: "integration-contract".to_owned(),
                    title: "Resolve the integration contract".to_owned(),
                    objective: "Record a verified compatibility decision.".to_owned(),
                    success_criteria: vec!["A human can inspect the expert advice.".to_owned()],
                }],
                non_goals: vec!["Do not automatically execute advice.".to_owned()],
                success_criteria: vec!["The integration blocker is explicit.".to_owned()],
                required_positive_tests: Vec::new(),
                required_negative_tests: Vec::new(),
                required_metrics: Vec::new(),
                required_evidence: vec!["CONTRIBUTING.md".to_owned()],
                proof_limits: vec!["Fixture only.".to_owned()],
                diff_budget: DiffBudget { files: 1, lines: 1 },
                token_budget: 80_000,
                tool_budget: Some(1),
                lease_expires_at: "controller-managed".to_owned(),
                stop_conditions: vec!["A human must apply any recovery.".to_owned()],
                handoff_path: "CONTRIBUTING.md".to_owned(),
                risk_flags: vec!["canonical_contract".to_owned()],
            }],
        };
        let task = TaskSummary {
            id: task_id,
            run_id: run_id.clone(),
            external_task_id: "task-1".to_owned(),
            title: "Resolve integration contract".to_owned(),
            objective: "Resolve the conflicting integration contract.".to_owned(),
            state: TaskState::Blocked,
            priority: "P1".to_owned(),
            owner_profile: "general".to_owned(),
            reviewer_profile: "general".to_owned(),
            attempt: 1,
            base_sha: "a".repeat(40),
            head_sha: None,
            token_budget: Some(80_000),
            dependencies: Vec::new(),
            failure_reason: Some("The canonical integration contract is disputed.".to_owned()),
            version: 1,
        };
        let expert_agent = AgentSummary {
            id: AgentSessionId::from("expert-1"),
            parent_agent_id: None,
            task_id: None,
            role: AgentRole::Expert,
            codex_account_id: None,
            nickname: Some("expert".to_owned()),
            state: "COMPLETED".to_owned(),
            requested_model: "gpt-5.6-sol".to_owned(),
            effective_model: Some("gpt-5.6-sol".to_owned()),
            requested_reasoning_effort: "xhigh".to_owned(),
            effective_reasoning_effort: Some("xhigh".to_owned()),
            sandbox_mode: SandboxMode::ReadOnly,
            cwd: "/tmp/expert-snapshot".to_owned(),
            current_goal: Some("Provide advisory analysis only.".to_owned()),
            current_action: None,
            failure_reason: None,
            started_at: "2026-08-14T00:00:00Z".to_owned(),
            completed_at: Some("2026-08-14T00:01:00Z".to_owned()),
            token_budget: Some(80_000),
            tokens_used: 12_345,
            budget_tokens_used: 12_345,
            estimated_cost_lower: "0".to_owned(),
            estimated_cost_upper: "0".to_owned(),
            heartbeat_at: None,
            thread_id: Some("thread-1".to_owned()),
            active_turn_id: None,
            active_turn_started_at: None,
            active_turn_usage: None,
            context_strategy: "fresh_independent".to_owned(),
            context_source_attempt_id: None,
            context_reuse_reason: None,
            version: 1,
        };
        let consultation = ExpertConsultationObservation {
            request: ExpertRequestRecord {
                id: ExpertRequestId::from("expert-request-1"),
                action_id: SupervisorActionId::from("expert-action-1"),
                decision_id: SupervisorDecisionId::from("expert-decision-1"),
                run_id: run_id.clone(),
                snapshot_id: SupervisorSnapshotId::from("expert-source-snapshot"),
                signature: "e".repeat(64),
                state: "COMPLETED".to_owned(),
                payload: json!({"category": "integration"}),
                payload_sha256: "f".repeat(64),
                requested_model: "gpt-5.6-sol".to_owned(),
                requested_effort: "xhigh".to_owned(),
                expires_at: "2026-08-14T01:00:00Z".to_owned(),
                created_at: "2026-08-14T00:00:00Z".to_owned(),
                started_at: Some("2026-08-14T00:00:01Z".to_owned()),
                completed_at: Some("2026-08-14T00:01:00Z".to_owned()),
                failure_reason: None,
                agent_session_id: Some(AgentSessionId::from("expert-1")),
            },
            response: Some(ExpertResponseRecord {
                id: ExpertResponseId::from("expert-response-1"),
                request_id: ExpertRequestId::from("expert-request-1"),
                run_id: run_id.clone(),
                snapshot_id: SupervisorSnapshotId::from("expert-source-snapshot"),
                payload: json!({
                    "verdict": "recommendation",
                    "summary": "The invariant must be preserved.",
                    "recommendation": "Require an exact compatibility validation before continuing.",
                    "evidence_refs": ["event-7"],
                }),
                payload_sha256: "d".repeat(64),
                byte_length: 1,
                created_at: "2026-08-14T00:01:00Z".to_owned(),
            }),
        };
        let config = SupervisionConfig {
            mode: harness_domain::SupervisorMode::Advisory,
            ..SupervisionConfig::default()
        };
        let current_section = harness_domain::SnapshotSection {
            state: harness_domain::SnapshotSectionState::Current,
            rows: Vec::new(),
            source_cursor: 0,
            truncated: false,
            detail: None,
        };
        // The control-plane fixture retains one run-scoped condition through
        // the full canonical projection, preventing the supervisor snapshot
        // contract from drifting from the PR4 custody integration.
        let control_plane = harness_domain::ControlPlaneSnapshot {
            schema: "harness.control-plane-snapshot.v1".to_owned(),
            snapshot_id: harness_domain::ControlPlaneSnapshotId::new(),
            revision: 1,
            compiled_at_ms: 0,
            event_cursor: 0,
            consistency: "fixture".to_owned(),
            system: current_section.clone(),
            accounts: current_section.clone(),
            scheduler: current_section.clone(),
            runs: current_section.clone(),
            attention: current_section.clone(),
            attempts: harness_domain::SnapshotSection {
                rows: vec![json!({
                    "run_id": run_id,
                    "attempt_id": "attempt-1",
                })],
                ..current_section.clone()
            },
            investigations: current_section.clone(),
            progress: current_section.clone(),
            liveness: current_section.clone(),
            reconciliation: current_section.clone(),
            external_conditions: harness_domain::SnapshotSection {
                rows: vec![
                    json!({
                        "schema": "harness.external-condition-summary.v1",
                        "owner_type": "run",
                        "owner_id": run_id,
                    }),
                    json!({
                        "schema": "harness.external-condition-summary.v1",
                        "owner_type": "task",
                        "owner_id": "task-internal-1",
                    }),
                    // This value deliberately matches the external label of
                    // the target task but not its controller TaskId. A second
                    // run may reuse that external label, so it must not enter
                    // this run-scoped custody binding.
                    json!({
                        "schema": "harness.external-condition-summary.v1",
                        "owner_type": "task",
                        "owner_id": "task-1",
                    }),
                    json!({
                        "schema": "harness.external-condition-summary.v1",
                        "owner_type": "attempt",
                        "owner_id": "attempt-1",
                    }),
                    json!({
                        "schema": "harness.external-condition-summary.v1",
                        "owner_type": "run",
                        "owner_id": "other-run",
                    }),
                ],
                ..current_section.clone()
            },
            cost: current_section.clone(),
            notifications: current_section.clone(),
            limits: current_section,
            // These omitted rows may belong to a different run, but the
            // bounded global projection cannot prove that. Surface the gap
            // and restrict action selection instead of treating absence as
            // healthy custody.
            truncation: vec![harness_domain::SnapshotTruncation {
                section: "liveness".to_owned(),
                omitted_rows: 1,
                limit: 100,
            }],
            source_cursors: Default::default(),
            sha256: "c".repeat(64),
        };
        let snapshot = build_snapshot(
            &SupervisorSnapshotId::from("snapshot-1"),
            1,
            &run,
            &plan,
            1,
            &[task],
            &[expert_agent],
            &[consultation],
            12_345,
            "general",
            &[event("run.supervision.expert_completed", json!({}))],
            &control_plane,
            7,
            &config,
            4,
        );

        validate_snapshot_contract(&snapshot).expect("post-consultation snapshot is schema valid");
        assert_eq!(snapshot["trigger"]["kind"], "expert_completed");
        assert_eq!(
            snapshot["operator_control"]["truncated_sections"][0]["section"],
            "liveness"
        );
        assert_eq!(
            snapshot["allowed_actions"],
            json!(["wait", "pause_for_human"])
        );
        assert_eq!(snapshot["budgets"]["expert_tokens_used"], 12_345);
        assert_eq!(
            snapshot["budgets"]["expert_token_budget_per_request"],
            80_000
        );
        assert_eq!(snapshot["budgets"]["expert_completed_cap_per_signature"], 2);
        assert_eq!(snapshot["budgets"]["active_expert_requests"], 0);
        assert_eq!(
            snapshot["operator_control"]["external_conditions"]
                .as_array()
                .expect("external conditions are an array")
                .len(),
            3
        );
        assert_eq!(
            snapshot["expert_consultations"][0]["response"]["verdict"],
            "recommendation"
        );
    }

    #[test]
    fn telemetry_backlog_catch_up_remains_bounded_without_deferring_material_events() {
        assert_eq!(MAX_EVENTS_PER_OBSERVATION, 10_000);
        assert_eq!(MAX_MATERIAL_EVENTS_PER_SNAPSHOT, 100);
    }
}
