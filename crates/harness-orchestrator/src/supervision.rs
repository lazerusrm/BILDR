//! Read-only supervisory observation.
//!
//! This first slice deliberately compiles immutable snapshots only. It never
//! starts a model turn, proposes an action, or mutates controller run/task
//! state. Later runtime slices must consume the stored snapshot rather than
//! reconstructing one from mutable live state.

use harness_domain::{
    AgentSummary, DomainEvent, RunId, RunPlan, RunState, RunSummary, SupervisorMode,
    SupervisorSnapshotId, TaskState, TaskSummary, format_timestamp, now_ms,
};
use harness_profile::SupervisionConfig;
use harness_store::{Store, StoreError, SupervisorSnapshotRecord, packet_digest};
use serde_json::{Value, json};

// A 10k telemetry backlog must not postpone a later material event for hours
// at the normal maintenance cadence. The snapshot itself still includes at
// most the schema's 100 evidence event references.
const MAX_EVENTS_PER_OBSERVATION: u32 = 10_000;
const MAX_MATERIAL_EVENTS_PER_SNAPSHOT: usize = 100;
const MAX_TASKS_PER_SNAPSHOT: usize = 50;
const MAX_AGENTS_PER_SNAPSHOT: usize = 50;
const EFFICIENCY_POLICY_VERSION: &str = "supervision-efficiency.v1";

pub(crate) fn observe_run(
    store: &Store,
    config: &SupervisionConfig,
    max_thread_count: u32,
    run_id: &RunId,
) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
    if config.mode == SupervisorMode::Disabled {
        return Ok(None);
    }
    if config.mode != SupervisorMode::ObserveOnly {
        return Err(StoreError::Validation(
            "supervision mode is not enabled by the observation-only runtime".to_owned(),
        ));
    }
    let observation = store.capture_supervisor_observation(run_id, MAX_EVENTS_PER_OBSERVATION)?;
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
    if now_ms().saturating_sub(latest_material.occurred_at) < coalesce_ms {
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
                observation.run_tokens_used,
                &observation.repository_profile_id,
                &material_events,
                event_cursor,
                config,
                max_thread_count,
            );
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

fn material_trigger(event: &DomainEvent) -> Option<&'static str> {
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
        "run.plan.revision_requested" | "run.plan.review_resume_requested" => {
            Some("operator_steered")
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
    run_tokens_used: u64,
    profile_id: &str,
    material_events: &[DomainEvent],
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
        .map(|task| task_snapshot(task, terminal_states.contains(&task.state)))
        .collect::<Vec<_>>();
    let agent_payloads = agents
        .iter()
        .take(MAX_AGENTS_PER_SNAPSHOT)
        .map(|agent| agent_snapshot(agent, &run.objective))
        .collect::<Vec<_>>();
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
            "supervisor_tokens_used": 0,
            "supervisor_tokens_remaining": config.supervisor.token_budget,
            "expert_tokens_used": 0,
            "expert_tokens_remaining": config.expert.token_budget,
            "expert_requests_remaining": 0,
            "active_thread_count": agents.iter().filter(|agent| agent.active_turn_id.is_some()).count(),
            "max_thread_count": max_thread_count,
        },
        "allowed_actions": ["wait"],
        "evidence_refs": material_events.iter().map(|event| json!({
            "kind": "event",
            "id": format!("event-{}", event.id),
            "summary": bounded(&event.event_type, 1_000),
        })).collect::<Vec<_>>(),
        "prior_decision": null,
        "expert_consultations": [],
    })
}

fn task_snapshot(task: &TaskSummary, completed: bool) -> Value {
    json!({
        "task_id": task.external_task_id,
        "title": bounded(&task.title, 500),
        "state": task.state.to_string().to_ascii_lowercase(),
        "priority": priority_number(&task.priority),
        "risk_flags": [],
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

fn parse_timestamp_millis(value: &str) -> Option<i64> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .ok()
        .and_then(|timestamp| i64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000).ok())
}

#[cfg(test)]
mod tests {
    use super::{MAX_EVENTS_PER_OBSERVATION, MAX_MATERIAL_EVENTS_PER_SNAPSHOT, material_trigger};
    use harness_domain::{DomainEvent, RunId};
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
    }

    #[test]
    fn telemetry_backlog_catch_up_remains_bounded_without_deferring_material_events() {
        assert_eq!(MAX_EVENTS_PER_OBSERVATION, 10_000);
        assert_eq!(MAX_MATERIAL_EVENTS_PER_SNAPSHOT, 100);
    }
}
