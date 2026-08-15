use harness_domain::{
    AgentSessionId, AgentSummary, DomainEvent, ExpertRequestId, ExpertResponseId, RunId, RunPlan,
    RunSummary, SupervisorActionId, SupervisorDecisionId, SupervisorReviewId, SupervisorSnapshotId,
    TaskSummary, format_timestamp, now_ms,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError, queries};

pub const SUPERVISOR_SNAPSHOT_SCHEMA: &str = "harness.supervisor-snapshot.v1";
const EXPERT_REQUEST_SELECT: &str = "SELECT id,action_id,decision_id,run_id,snapshot_id,signature,state,payload_json,payload_sha256,requested_model,requested_effort,expires_at,created_at,started_at,completed_at,failure_reason,agent_session_id FROM expert_requests";
const MAX_EXPERT_FRESHNESS_EVENTS: usize = 10_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SupervisorSnapshotRecord {
    pub id: SupervisorSnapshotId,
    pub run_id: RunId,
    pub revision: u64,
    pub event_cursor: i64,
    pub trigger_kind: String,
    pub payload: Value,
    pub payload_sha256: String,
    pub byte_length: u64,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SupervisorReviewRecord {
    pub id: SupervisorReviewId,
    pub run_id: RunId,
    pub snapshot_id: SupervisorSnapshotId,
    pub agent_session_id: AgentSessionId,
    pub expected_decision_id: SupervisorDecisionId,
    pub state: String,
    pub trigger_kind: String,
    pub requested_model: String,
    pub requested_effort: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub failure_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NewSupervisorReview {
    pub id: SupervisorReviewId,
    pub run_id: RunId,
    pub snapshot_id: SupervisorSnapshotId,
    pub agent_session_id: AgentSessionId,
    pub expected_decision_id: SupervisorDecisionId,
    pub trigger_kind: String,
    pub requested_model: String,
    pub requested_effort: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SupervisorDecisionRecord {
    pub id: SupervisorDecisionId,
    pub review_id: SupervisorReviewId,
    pub run_id: RunId,
    pub snapshot_id: SupervisorSnapshotId,
    pub agent_session_id: AgentSessionId,
    /// `ADVISORY` decisions are display-only suggestions. `STALE` decisions
    /// remain visible for audit but are never offered for recovery.
    pub policy_state: String,
    pub payload: Value,
    pub payload_sha256: String,
    pub byte_length: u64,
    pub created_at: String,
}

/// One immutable action proposal projected from a hash-bound supervisor
/// decision. Lifecycle transitions are deliberately separate from the model
/// payload so policy and execution cannot rewrite the original proposal.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SupervisorActionRecord {
    pub id: SupervisorActionId,
    pub decision_id: SupervisorDecisionId,
    pub run_id: RunId,
    pub snapshot_id: SupervisorSnapshotId,
    pub proposal_action_id: String,
    pub kind: String,
    pub target: Value,
    pub proposal: Value,
    pub proposal_sha256: String,
    pub dedupe_key: String,
    pub state: String,
    pub policy_reason: Option<String>,
    pub execution_receipt: Option<Value>,
    pub execution_receipt_sha256: Option<String>,
    pub created_at: String,
    pub evaluated_at: Option<String>,
    pub execution_started_at: Option<String>,
    pub completed_at: Option<String>,
}

/// A controller-constructed, bounded expert request. It is never a direct
/// authority grant: only an already policy-accepted `request_expert` action
/// may own one of these records.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExpertRequestRecord {
    pub id: ExpertRequestId,
    pub action_id: SupervisorActionId,
    pub decision_id: SupervisorDecisionId,
    pub run_id: RunId,
    pub snapshot_id: SupervisorSnapshotId,
    pub signature: String,
    pub state: String,
    pub payload: Value,
    pub payload_sha256: String,
    pub requested_model: String,
    pub requested_effort: String,
    pub expires_at: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub failure_reason: Option<String>,
    /// The one read-only advisory session permitted to answer this request.
    /// This binding is append-only in SQLite and lets the event consumer reject
    /// replies from any other agent after restarts or retries.
    pub agent_session_id: Option<AgentSessionId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExpertResponseRecord {
    pub id: ExpertResponseId,
    pub request_id: ExpertRequestId,
    pub run_id: RunId,
    pub snapshot_id: SupervisorSnapshotId,
    pub payload: Value,
    pub payload_sha256: String,
    pub byte_length: u64,
    pub created_at: String,
}

#[derive(Clone, Debug)]
pub struct NewExpertRequest {
    pub id: ExpertRequestId,
    pub action_id: SupervisorActionId,
    /// The immutable snapshot's controller-event cursor. A request is only
    /// durable when no newer event can make that snapshot stale.
    pub event_cursor: i64,
    pub signature: String,
    pub payload: Value,
    pub requested_model: String,
    pub requested_effort: String,
    pub expires_at_ms: i64,
    /// Controller configuration, not model output. Limits reusable answers to
    /// one exact escalation signature while preserving the durable history.
    pub max_completed_per_signature: u8,
}

/// The bounded expert evidence visible to a later supervisory snapshot. The
/// response remains independently immutable; this projection merely keeps the
/// following Terra review from treating a completed consultation as invisible.
#[derive(Clone, Debug)]
pub struct ExpertConsultationObservation {
    pub request: ExpertRequestRecord,
    pub response: Option<ExpertResponseRecord>,
}

/// A single, internally consistent controller projection captured for the
/// observe-only supervisor.  The event cursor and every value represented in
/// a resulting receipt come from one SQLite read transaction.
#[derive(Clone, Debug)]
pub struct SupervisorObservationInput {
    pub run: RunSummary,
    pub cursor: i64,
    pub events: Vec<DomainEvent>,
    pub latest_plan: Option<(RunPlan, u64)>,
    pub tasks: Vec<TaskSummary>,
    pub agents: Vec<AgentSummary>,
    pub expert_consultations: Vec<ExpertConsultationObservation>,
    pub run_tokens_used: u64,
    pub repository_profile_id: String,
}

impl Store {
    pub fn capture_supervisor_observation(
        &self,
        run_id: &RunId,
        max_events: u32,
    ) -> Result<SupervisorObservationInput, StoreError> {
        self.capture_supervisor_observation_with(run_id, max_events, || Ok(()))
    }

    /// Reads the complete supervisory projection through one transaction. The
    /// callback is only used by the concurrency regression test to mutate the
    /// same database from another connection after this transaction has
    /// established its read snapshot.
    fn capture_supervisor_observation_with<F>(
        &self,
        run_id: &RunId,
        max_events: u32,
        after_run_read: F,
    ) -> Result<SupervisorObservationInput, StoreError>
    where
        F: FnOnce() -> Result<(), StoreError>,
    {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let observation = Self::capture_supervisor_observation_in_transaction(
            &transaction,
            run_id,
            max_events,
            after_run_read,
        )?;
        transaction.commit()?;
        Ok(observation)
    }

    pub(crate) fn capture_supervisor_observation_in_transaction<F>(
        transaction: &Transaction<'_>,
        run_id: &RunId,
        max_events: u32,
        after_run_read: F,
    ) -> Result<SupervisorObservationInput, StoreError>
    where
        F: FnOnce() -> Result<(), StoreError>,
    {
        let run = transaction
            .query_row(
                &format!("{} WHERE r.id=?1", queries::run_select()),
                [run_id.as_str()],
                queries::map_run,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("run {run_id}")))?;
        after_run_read()?;
        let cursor = transaction
            .query_row(
                "SELECT last_event_cursor FROM supervisor_observation_cursors WHERE run_id=?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let events = {
            let mut statement = transaction.prepare(
                "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json FROM domain_events WHERE id>?1 AND run_id=?2 ORDER BY id LIMIT ?3",
            )?;
            let rows = statement.query_map(
                params![cursor, run_id.as_str(), max_events],
                queries::map_domain_event,
            )?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let latest_plan = transaction
            .query_row(
                "SELECT plan_json,revision FROM run_plan_revisions WHERE run_id=?1 ORDER BY revision DESC LIMIT 1",
                [run_id.as_str()],
                |row| {
                    let raw: String = row.get(0)?;
                    let plan: RunPlan = serde_json::from_str(&raw).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok((plan, row.get::<_, i64>(1)?))
                },
            )
            .optional()?
            .map(|(plan, revision)| {
                u64::try_from(revision)
                    .map(|revision| (plan, revision))
                    .map_err(|_| StoreError::Validation("plan revision is invalid".to_owned()))
            })
            .transpose()?;
        let tasks = {
            let mut statement = transaction.prepare(
                "SELECT t.id,t.run_id,t.external_task_id,t.title,t.objective,t.state,t.priority,t.owner_profile,t.reviewer_profile,t.current_attempt_number,coalesce(a.base_sha,r.base_sha),coalesce(a.head_sha,tr.verified_commit_sha),a.token_budget,t.version,(SELECT json_group_array(dt.external_task_id) FROM task_dependencies d JOIN tasks dt ON dt.id=d.depends_on_task_id WHERE d.task_id=t.id),coalesce(a.failure_reason,t.failure_reason) FROM tasks t JOIN runs r ON r.id=t.run_id LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number LEFT JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.run_id=?1 ORDER BY CASE t.priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,t.created_at",
            )?;
            let rows = statement.query_map([run_id.as_str()], queries::map_task)?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let agents = {
            let mut statement = transaction.prepare(&format!(
                "{} WHERE a.run_id=?1 ORDER BY a.started_at,a.id",
                queries::agent_select()
            ))?;
            let rows = statement.query_map([run_id.as_str()], queries::map_agent)?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let expert_consultations = {
            let mut statement = transaction.prepare(&format!(
                "{EXPERT_REQUEST_SELECT} WHERE run_id=?1 ORDER BY created_at DESC,id DESC LIMIT 3"
            ))?;
            let requests = statement
                .query_map([run_id.as_str()], map_expert_request)?
                .collect::<Result<Vec<_>, _>>()?;
            requests
                .into_iter()
                .map(|request| {
                    let response = transaction
                        .query_row(
                            "SELECT id,request_id,run_id,snapshot_id,payload_json,payload_sha256,byte_length,created_at FROM expert_responses WHERE request_id=?1 ORDER BY created_at DESC,id DESC LIMIT 1",
                            [request.id.as_str()],
                            map_expert_response,
                        )
                        .optional()?;
                    Ok(ExpertConsultationObservation { request, response })
                })
                .collect::<Result<Vec<_>, rusqlite::Error>>()?
        };
        let run_tokens_used: i64 = transaction.query_row(
            "SELECT coalesce(sum(ts.total_tokens),0) FROM token_samples ts JOIN codex_threads ct ON ct.thread_id=ts.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id WHERE a.run_id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        let run_tokens_used = u64::try_from(run_tokens_used)
            .map_err(|_| StoreError::Validation("run token total is invalid".to_owned()))?;
        let repository_profile_id: String = transaction.query_row(
            "SELECT profile_id FROM repositories WHERE id=?1",
            [run.repository_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(SupervisorObservationInput {
            run,
            cursor,
            events,
            latest_plan,
            tasks,
            agents,
            expert_consultations,
            run_tokens_used,
            repository_profile_id,
        })
    }

    pub fn supervisor_observation_cursor(&self, run_id: &RunId) -> Result<i64, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT last_event_cursor FROM supervisor_observation_cursors WHERE run_id=?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    pub fn advance_supervisor_observation_cursor(
        &self,
        run_id: &RunId,
        event_cursor: i64,
    ) -> Result<(), StoreError> {
        if event_cursor < 0 {
            return Err(StoreError::Validation(
                "supervisor observation cursor must be non-negative".to_owned(),
            ));
        }
        self.connection()?.execute(
            "INSERT INTO supervisor_observation_cursors(run_id,last_event_cursor,updated_at) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET last_event_cursor=max(last_event_cursor,excluded.last_event_cursor),updated_at=excluded.updated_at",
            params![run_id.as_str(), event_cursor, now_ms()],
        )?;
        Ok(())
    }

    pub fn record_supervisor_snapshot<F>(
        &self,
        run_id: &RunId,
        event_cursor: i64,
        trigger_kind: &str,
        build_payload: F,
    ) -> Result<SupervisorSnapshotRecord, StoreError>
    where
        F: FnOnce(&SupervisorSnapshotId, u64) -> Result<Value, StoreError>,
    {
        if event_cursor < 0 || trigger_kind.is_empty() || trigger_kind.len() > 128 {
            return Err(StoreError::Validation(
                "supervisor snapshot cursor or trigger is invalid".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE run_id=?1 AND event_cursor=?2",
                params![run_id.as_str(), event_cursor],
                map_supervisor_snapshot,
            )
            .optional()?
        {
            transaction.execute(
                "INSERT INTO supervisor_observation_cursors(run_id,last_event_cursor,updated_at) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET last_event_cursor=max(last_event_cursor,excluded.last_event_cursor),updated_at=excluded.updated_at",
                params![run_id.as_str(), event_cursor, now_ms()],
            )?;
            transaction.commit()?;
            return Ok(existing);
        }
        let revision_raw: i64 = transaction.query_row(
            "SELECT coalesce(max(revision),0)+1 FROM supervisor_snapshots WHERE run_id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        let revision = u64::try_from(revision_raw).map_err(|_| {
            StoreError::Validation("supervisor snapshot revision is invalid".to_owned())
        })?;
        let id = SupervisorSnapshotId::new();
        let payload = build_payload(&id, revision)?;
        validate_snapshot_binding(&payload, &id, run_id, revision, event_cursor)?;
        let raw = serde_json::to_string(&payload)?;
        let byte_length = i64::try_from(raw.len()).map_err(|_| {
            StoreError::Validation("supervisor snapshot exceeds supported size".to_owned())
        })?;
        let digest = hex::encode(Sha256::digest(raw.as_bytes()));
        let created_at = now_ms();
        transaction.execute(
            "INSERT INTO supervisor_snapshots(id,run_id,revision,schema_name,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![id.as_str(),run_id.as_str(),revision_raw,SUPERVISOR_SNAPSHOT_SCHEMA,event_cursor,trigger_kind,raw,digest,byte_length,created_at],
        )?;
        transaction.execute(
            "INSERT INTO supervisor_observation_cursors(run_id,last_event_cursor,updated_at) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET last_event_cursor=max(last_event_cursor,excluded.last_event_cursor),updated_at=excluded.updated_at",
            params![run_id.as_str(), event_cursor, created_at],
        )?;
        let record = transaction.query_row(
            "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE id=?1",
            [id.as_str()],
            map_supervisor_snapshot,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn latest_supervisor_snapshot(
        &self,
        run_id: &RunId,
    ) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
        self.connection()?.query_row(
            "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE run_id=?1 ORDER BY revision DESC LIMIT 1",
            [run_id.as_str()],
            map_supervisor_snapshot,
        ).optional().map_err(Into::into)
    }

    pub fn supervisor_snapshot(
        &self,
        snapshot_id: &SupervisorSnapshotId,
    ) -> Result<Option<SupervisorSnapshotRecord>, StoreError> {
        self.connection()?.query_row(
            "SELECT id,run_id,revision,event_cursor,trigger_kind,payload_json,payload_sha256,byte_length,created_at FROM supervisor_snapshots WHERE id=?1",
            [snapshot_id.as_str()],
            map_supervisor_snapshot,
        ).optional().map_err(Into::into)
    }

    pub fn create_supervisor_review(
        &self,
        input: &NewSupervisorReview,
    ) -> Result<SupervisorReviewRecord, StoreError> {
        if input.trigger_kind.is_empty()
            || input.trigger_kind.len() > 128
            || input.requested_model.is_empty()
            || input.requested_model.len() > 128
            || input.requested_effort.is_empty()
            || input.requested_effort.len() > 32
        {
            return Err(StoreError::Validation(
                "supervisor review metadata is invalid".to_owned(),
            ));
        }
        let now = now_ms();
        {
            let connection = self.connection()?;
            connection.execute(
                "INSERT INTO supervisor_reviews(id,run_id,snapshot_id,agent_session_id,expected_decision_id,state,trigger_kind,requested_model,requested_effort,created_at) VALUES(?1,?2,?3,?4,?5,'STARTING',?6,?7,?8,?9)",
                params![
                    input.id.as_str(),
                    input.run_id.as_str(),
                    input.snapshot_id.as_str(),
                    input.agent_session_id.as_str(),
                    input.expected_decision_id.as_str(),
                    input.trigger_kind,
                    input.requested_model,
                    input.requested_effort,
                    now,
                ],
            )?;
        }
        self.supervisor_review(&input.id)
    }

    pub fn mark_supervisor_review_running(
        &self,
        review_id: &SupervisorReviewId,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE supervisor_reviews SET state='RUNNING' WHERE id=?1 AND state='STARTING'",
            [review_id.as_str()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "supervisor review {review_id} is not startable"
            )));
        }
        Ok(())
    }

    pub fn fail_supervisor_review(
        &self,
        review_id: &SupervisorReviewId,
        reason: &str,
    ) -> Result<(), StoreError> {
        let reason = reason.trim();
        if reason.is_empty() || reason.chars().count() > 4_000 {
            return Err(StoreError::Validation(
                "supervisor review failure reason is invalid".to_owned(),
            ));
        }
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE supervisor_reviews SET state='FAILED',completed_at=?2,failure_reason=?3 WHERE id=?1 AND state IN ('STARTING','RUNNING')",
            params![review_id.as_str(), now, reason],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "supervisor review {review_id} is not active"
            )));
        }
        Ok(())
    }

    /// Atomically records a model decision only if no later material event
    /// exists for the immutable source cursor.  The material-event predicate
    /// is controller-owned, while this transaction owns the race-free
    /// boundary between that check and the immutable decision receipt.
    pub fn record_current_supervisor_decision<F>(
        &self,
        review_id: &SupervisorReviewId,
        event_cursor: i64,
        payload: &Value,
        is_material_event: F,
    ) -> Result<SupervisorDecisionRecord, StoreError>
    where
        F: Fn(&DomainEvent) -> bool,
    {
        self.record_supervisor_decision_with_freshness(
            review_id,
            event_cursor,
            payload,
            is_material_event,
        )
    }

    fn record_supervisor_decision_with_freshness<F>(
        &self,
        review_id: &SupervisorReviewId,
        event_cursor: i64,
        payload: &Value,
        is_material_event: F,
    ) -> Result<SupervisorDecisionRecord, StoreError>
    where
        F: Fn(&DomainEvent) -> bool,
    {
        let raw = serde_json::to_string(payload)?;
        let byte_length = u64::try_from(raw.len()).map_err(|_| {
            StoreError::Validation("supervisor decision exceeds supported size".to_owned())
        })?;
        if byte_length == 0 || byte_length > 262_144 {
            return Err(StoreError::Validation(
                "supervisor decision byte length is invalid".to_owned(),
            ));
        }
        let digest = hex::encode(Sha256::digest(raw.as_bytes()));
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let review = transaction
            .query_row(
                "SELECT id,run_id,snapshot_id,agent_session_id,expected_decision_id,state,trigger_kind,requested_model,requested_effort,created_at,completed_at,failure_reason FROM supervisor_reviews WHERE id=?1",
                [review_id.as_str()],
                map_supervisor_review,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("supervisor review {review_id}")))?;
        if !matches!(review.state.as_str(), "STARTING" | "RUNNING") {
            return Err(StoreError::Conflict(format!(
                "supervisor review {review_id} is not active"
            )));
        }
        let bound = payload.get("schema").and_then(Value::as_str)
            == Some("harness.supervisor-decision.v1")
            && payload.get("decision_id").and_then(Value::as_str)
                == Some(review.expected_decision_id.as_str())
            && payload.get("snapshot_id").and_then(Value::as_str)
                == Some(review.snapshot_id.as_str())
            && payload.get("run_id").and_then(Value::as_str) == Some(review.run_id.as_str());
        if !bound {
            return Err(StoreError::Validation(
                "supervisor decision does not match its immutable review envelope".to_owned(),
            ));
        }
        // Filter in SQLite first so an arbitrary telemetry backlog cannot
        // turn this strict receipt into an unbounded read. The controller
        // still evaluates the small allowlisted candidate set, including
        // payload-sensitive lifecycle transitions.
        let policy_state = {
            let mut statement = transaction.prepare(
                "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json
                 FROM domain_events
                 WHERE run_id=?1 AND id>?2 AND event_type IN (
                    'run.lifecycle.transitioned',
                    'task.start_failed',
                    'agent.governor.warm_continuation_failed',
                    'task.governor.candidate_recovery_deferred',
                    'agent.native_subagent.terminal',
                    'task.verified',
                    'run.integration.prepared',
                    'run.final_audit.rejected',
                    'task.github_resource_recovered',
                    'run.token_budget.reached',
                    'agent.governor.budget_hard_stop',
                    'agent.native_subagent.budget_hard_stop',
                    'agent.run_budget.hard_stop',
                    'agent.session_budget.hard_stop',
                    'run.plan.revision_requested',
                    'run.plan.review_resume_requested',
                    'run.supervision.operator_review_requested',
                    'task.governor.candidate_materialized',
                    'run.final_audit.accepted'
                 ) ORDER BY id",
            )?;
            let events = statement.query_map(
                params![review.run_id.as_str(), event_cursor],
                queries::map_domain_event,
            )?;
            if events
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .any(is_material_event)
            {
                "STALE"
            } else {
                "ADVISORY"
            }
        };
        transaction.execute(
            "INSERT INTO supervisor_decisions(id,review_id,run_id,snapshot_id,agent_session_id,policy_state,payload_json,payload_sha256,byte_length,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                review.expected_decision_id.as_str(),
                review.id.as_str(),
                review.run_id.as_str(),
                review.snapshot_id.as_str(),
                review.agent_session_id.as_str(),
                policy_state,
                raw,
                digest,
                i64::try_from(byte_length).map_err(|_| StoreError::Validation("supervisor decision exceeds SQLite limits".to_owned()))?,
                now,
            ],
        )?;
        insert_supervisor_action_proposals(
            &transaction,
            &review.expected_decision_id,
            &review.run_id,
            &review.snapshot_id,
            payload,
            policy_state,
            now,
        )?;
        transaction.execute(
            "UPDATE supervisor_reviews SET state=?2,completed_at=?3,failure_reason=NULL WHERE id=?1",
            params![review.id.as_str(), if policy_state == "STALE" { "STALE" } else { "COMPLETED" }, now],
        )?;
        let record = transaction.query_row(
            "SELECT id,review_id,run_id,snapshot_id,agent_session_id,policy_state,payload_json,payload_sha256,byte_length,created_at FROM supervisor_decisions WHERE review_id=?1",
            [review.id.as_str()],
            map_supervisor_decision,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn supervisor_review(
        &self,
        review_id: &SupervisorReviewId,
    ) -> Result<SupervisorReviewRecord, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,run_id,snapshot_id,agent_session_id,expected_decision_id,state,trigger_kind,requested_model,requested_effort,created_at,completed_at,failure_reason FROM supervisor_reviews WHERE id=?1",
                [review_id.as_str()],
                map_supervisor_review,
            )
            .map_err(Into::into)
    }

    pub fn supervisor_review_for_agent(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<SupervisorReviewRecord>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,run_id,snapshot_id,agent_session_id,expected_decision_id,state,trigger_kind,requested_model,requested_effort,created_at,completed_at,failure_reason FROM supervisor_reviews WHERE agent_session_id=?1",
                [agent_id.as_str()],
                map_supervisor_review,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_supervisor_review(
        &self,
        run_id: &RunId,
    ) -> Result<Option<SupervisorReviewRecord>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,run_id,snapshot_id,agent_session_id,expected_decision_id,state,trigger_kind,requested_model,requested_effort,created_at,completed_at,failure_reason FROM supervisor_reviews WHERE run_id=?1 ORDER BY created_at DESC,id DESC LIMIT 1",
                [run_id.as_str()],
                map_supervisor_review,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn supervisor_review_for_snapshot(
        &self,
        snapshot_id: &SupervisorSnapshotId,
    ) -> Result<Option<SupervisorReviewRecord>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,run_id,snapshot_id,agent_session_id,expected_decision_id,state,trigger_kind,requested_model,requested_effort,created_at,completed_at,failure_reason FROM supervisor_reviews WHERE snapshot_id=?1",
                [snapshot_id.as_str()],
                map_supervisor_review,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_supervisor_decision(
        &self,
        run_id: &RunId,
    ) -> Result<Option<SupervisorDecisionRecord>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,review_id,run_id,snapshot_id,agent_session_id,policy_state,payload_json,payload_sha256,byte_length,created_at FROM supervisor_decisions WHERE run_id=?1 ORDER BY created_at DESC,id DESC LIMIT 1",
                [run_id.as_str()],
                map_supervisor_decision,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn supervisor_actions_for_decision(
        &self,
        decision_id: &SupervisorDecisionId,
    ) -> Result<Vec<SupervisorActionRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id,decision_id,run_id,snapshot_id,proposal_action_id,kind,target_json,proposal_json,proposal_sha256,dedupe_key,state,policy_reason,execution_receipt_json,execution_receipt_sha256,created_at,evaluated_at,execution_started_at,completed_at FROM supervisor_actions WHERE decision_id=?1 ORDER BY created_at,id",
        )?;
        statement
            .query_map([decision_id.as_str()], map_supervisor_action)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn supervisor_actions_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<SupervisorActionRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id,decision_id,run_id,snapshot_id,proposal_action_id,kind,target_json,proposal_json,proposal_sha256,dedupe_key,state,policy_reason,execution_receipt_json,execution_receipt_sha256,created_at,evaluated_at,execution_started_at,completed_at FROM supervisor_actions WHERE run_id=?1 ORDER BY created_at DESC,id DESC LIMIT 100",
        )?;
        statement
            .query_map([run_id.as_str()], map_supervisor_action)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn supervisor_action(
        &self,
        action_id: &SupervisorActionId,
    ) -> Result<SupervisorActionRecord, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,decision_id,run_id,snapshot_id,proposal_action_id,kind,target_json,proposal_json,proposal_sha256,dedupe_key,state,policy_reason,execution_receipt_json,execution_receipt_sha256,created_at,evaluated_at,execution_started_at,completed_at FROM supervisor_actions WHERE id=?1",
                [action_id.as_str()],
                map_supervisor_action,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("supervisor action {action_id}")))
    }

    /// Records exactly one closed policy outcome. The caller cannot move an
    /// action straight into execution or overwrite a prior decision.
    pub fn evaluate_supervisor_action(
        &self,
        action_id: &SupervisorActionId,
        accepted: bool,
        reason: &str,
    ) -> Result<SupervisorActionRecord, StoreError> {
        let reason = bounded_text(reason, 4_000, "supervisor action policy reason")?;
        let state = if accepted {
            "POLICY_ACCEPTED"
        } else {
            "POLICY_REJECTED"
        };
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE supervisor_actions SET state=?2,policy_reason=?3,evaluated_at=?4 WHERE id=?1 AND state='PROPOSED'",
            params![action_id.as_str(), state, reason, now],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "supervisor action {action_id} is not pending policy evaluation"
            )));
        }
        self.supervisor_action(action_id)
    }

    /// Marks an unexecuted proposal stale when its snapshot/target no longer
    /// matches live controller state. A stale receipt is preserved for the
    /// operator; it cannot later be revived or applied.
    pub fn stale_supervisor_action(
        &self,
        action_id: &SupervisorActionId,
        reason: &str,
    ) -> Result<SupervisorActionRecord, StoreError> {
        let reason = bounded_text(reason, 4_000, "supervisor action stale reason")?;
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE supervisor_actions SET state='STALE',policy_reason=?2,evaluated_at=?3 WHERE id=?1 AND state IN ('PROPOSED','POLICY_ACCEPTED')",
            params![action_id.as_str(), reason, now],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "supervisor action {action_id} is not eligible to become stale"
            )));
        }
        self.supervisor_action(action_id)
    }

    pub fn begin_supervisor_action(
        &self,
        action_id: &SupervisorActionId,
    ) -> Result<SupervisorActionRecord, StoreError> {
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE supervisor_actions SET state='EXECUTING',execution_started_at=?2 WHERE id=?1 AND state='POLICY_ACCEPTED'",
            params![action_id.as_str(), now],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "supervisor action {action_id} is not policy-accepted"
            )));
        }
        self.supervisor_action(action_id)
    }

    /// Persists a bounded, hash-verifiable controller receipt. The receipt is
    /// written only from `EXECUTING`, making retries and late callbacks unable
    /// to change a terminal action.
    pub fn complete_supervisor_action(
        &self,
        action_id: &SupervisorActionId,
        succeeded: bool,
        receipt: &Value,
    ) -> Result<SupervisorActionRecord, StoreError> {
        let raw = serde_json::to_string(receipt)?;
        if raw.is_empty() || raw.len() > 65_536 {
            return Err(StoreError::Validation(
                "supervisor action receipt exceeds its bounded custody limit".to_owned(),
            ));
        }
        let state = if succeeded { "SUCCEEDED" } else { "FAILED" };
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE supervisor_actions SET state=?2,execution_receipt_json=?3,execution_receipt_sha256=?4,completed_at=?5 WHERE id=?1 AND state='EXECUTING'",
            params![action_id.as_str(), state, raw, hex::encode(Sha256::digest(raw.as_bytes())), now],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "supervisor action {action_id} is not executing"
            )));
        }
        self.supervisor_action(action_id)
    }

    pub fn create_expert_request(
        &self,
        input: &NewExpertRequest,
    ) -> Result<ExpertRequestRecord, StoreError> {
        self.create_expert_request_if_materially_current(input, |_| true)
    }

    /// Queues an expert request only if the action snapshot has not been
    /// superseded by a later *material* controller event. Benign receipt and
    /// transport events necessarily occur while a human reviews a supervisor
    /// proposal; treating those as stale would make a valid expert action
    /// impossible to apply after its originating review completes.
    pub fn create_expert_request_if_materially_current<F>(
        &self,
        input: &NewExpertRequest,
        is_material_event: F,
    ) -> Result<ExpertRequestRecord, StoreError>
    where
        F: Fn(&DomainEvent) -> bool,
    {
        let signature = exact_sha256(&input.signature, "expert escalation signature")?;
        let requested_model = bounded_text(&input.requested_model, 128, "expert model")?;
        let requested_effort = bounded_text(&input.requested_effort, 32, "expert effort")?;
        if input.expires_at_ms <= now_ms() {
            return Err(StoreError::Validation(
                "expert request expiry must be in the future".to_owned(),
            ));
        }
        if !(1..=2).contains(&input.max_completed_per_signature) {
            return Err(StoreError::Validation(
                "expert completed-signature cap must be one or two".to_owned(),
            ));
        }
        let raw = serde_json::to_string(&input.payload)?;
        if raw.is_empty() || raw.len() > 131_072 {
            return Err(StoreError::Validation(
                "expert request payload exceeds its bounded custody limit".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let action = transaction
            .query_row(
                "SELECT decision_id,run_id,snapshot_id,kind,state FROM supervisor_actions WHERE id=?1",
                [input.action_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("supervisor action {}", input.action_id)))?;
        if action.3 != "request_expert"
            || !matches!(action.4.as_str(), "POLICY_ACCEPTED" | "EXECUTING")
        {
            return Err(StoreError::Conflict(
                "only a policy-accepted request_expert action may create an expert request"
                    .to_owned(),
            ));
        }
        if later_material_event_exists(
            &transaction,
            &RunId::from(action.1.as_str()),
            input.event_cursor,
            &is_material_event,
        )? {
            return Err(StoreError::Conflict(
                "a newer material controller event superseded the expert request snapshot"
                    .to_owned(),
            ));
        }
        let active: i64 = transaction.query_row(
            "SELECT count(*) FROM expert_requests WHERE run_id=?1 AND state IN ('QUEUED','RUNNING')",
            [action.1.as_str()],
            |row| row.get(0),
        )?;
        if active != 0 {
            return Err(StoreError::Conflict(
                "an expert consultation is already active for this run".to_owned(),
            ));
        }
        let completed: i64 = transaction.query_row(
            "SELECT count(*) FROM expert_requests WHERE run_id=?1 AND signature=?2 AND state IN ('COMPLETED','INCONCLUSIVE')",
            params![action.1.as_str(), &signature],
            |row| row.get(0),
        )?;
        if completed >= i64::from(input.max_completed_per_signature) {
            return Err(StoreError::Conflict(
                "the exact expert escalation signature has reached its completed-response cap"
                    .to_owned(),
            ));
        }
        let now = now_ms();
        transaction.execute(
            "INSERT INTO expert_requests(id,action_id,decision_id,run_id,snapshot_id,signature,state,payload_json,payload_sha256,requested_model,requested_effort,expires_at,created_at) VALUES(?1,?2,?3,?4,?5,?6,'QUEUED',?7,?8,?9,?10,?11,?12)",
            params![
                input.id.as_str(),
                input.action_id.as_str(),
                action.0,
                action.1,
                action.2,
                signature,
                raw,
                hex::encode(Sha256::digest(raw.as_bytes())),
                requested_model,
                requested_effort,
                input.expires_at_ms,
                now,
            ],
        )?;
        let record = transaction.query_row(
            &format!("{EXPERT_REQUEST_SELECT} WHERE id=?1"),
            [input.id.as_str()],
            map_expert_request,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn begin_expert_request(
        &self,
        request_id: &ExpertRequestId,
    ) -> Result<ExpertRequestRecord, StoreError> {
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE expert_requests SET state='RUNNING',started_at=?2 WHERE id=?1 AND state='QUEUED' AND expires_at>?2 AND agent_session_id IS NOT NULL",
            params![request_id.as_str(), now],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "expert request {request_id} is not queueable or has expired"
            )));
        }
        self.expert_request(request_id)
    }

    /// Atomically marks a bound request runnable only when its immutable
    /// snapshot cursor is still the newest controller observation. The
    /// orchestrator calls this immediately before the App Server start RPC;
    /// accepting a benign-but-new event as fresh would be a fail-open expert
    /// escalation boundary.
    pub fn begin_expert_request_if_current(
        &self,
        request_id: &ExpertRequestId,
        run_id: &RunId,
        event_cursor: i64,
    ) -> Result<ExpertRequestRecord, StoreError> {
        self.begin_expert_request_if_materially_current(request_id, run_id, event_cursor, |_| true)
    }

    /// Binds and starts a queued expert request while preserving the same
    /// material-event freshness contract used at request creation.
    pub fn begin_expert_request_if_materially_current<F>(
        &self,
        request_id: &ExpertRequestId,
        run_id: &RunId,
        event_cursor: i64,
        is_material_event: F,
    ) -> Result<ExpertRequestRecord, StoreError>
    where
        F: Fn(&DomainEvent) -> bool,
    {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let request_run_id: String = transaction
            .query_row(
                "SELECT run_id FROM expert_requests WHERE id=?1",
                [request_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("expert request {request_id}")))?;
        if request_run_id != run_id.as_str() {
            return Err(StoreError::Validation(
                "expert request run does not match its current snapshot run".to_owned(),
            ));
        }
        if later_material_event_exists(&transaction, run_id, event_cursor, &is_material_event)? {
            return Err(StoreError::Conflict(
                "a newer material controller event superseded the expert request before runtime launch"
                    .to_owned(),
            ));
        }
        let now = now_ms();
        let changed = transaction.execute(
            "UPDATE expert_requests SET state='RUNNING',started_at=?2 WHERE id=?1 AND state='QUEUED' AND expires_at>?2 AND agent_session_id IS NOT NULL",
            params![request_id.as_str(), now],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "expert request {request_id} is not queueable or has expired"
            )));
        }
        let record = transaction.query_row(
            &format!("{EXPERT_REQUEST_SELECT} WHERE id=?1"),
            [request_id.as_str()],
            map_expert_request,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn expert_request(
        &self,
        request_id: &ExpertRequestId,
    ) -> Result<ExpertRequestRecord, StoreError> {
        self.connection()?
            .query_row(
                &format!("{EXPERT_REQUEST_SELECT} WHERE id=?1"),
                [request_id.as_str()],
                map_expert_request,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("expert request {request_id}")))
    }

    pub fn expert_requests_for_run(
        &self,
        run_id: &RunId,
        limit: u32,
    ) -> Result<Vec<ExpertRequestRecord>, StoreError> {
        if limit == 0 || limit > 100 {
            return Err(StoreError::Validation(
                "expert request page limit must be 1..=100".to_owned(),
            ));
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "{EXPERT_REQUEST_SELECT} WHERE run_id=?1 ORDER BY created_at DESC,id DESC LIMIT ?2"
        ))?;
        statement
            .query_map(
                params![run_id.as_str(), i64::from(limit)],
                map_expert_request,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Atomically attaches the one expert session to its queued request.  The
    /// session must belong to the same run and have the exact `Expert` role;
    /// callers cannot bind a governor or a session from another run and then
    /// feed its output through the expert result path.
    pub fn attach_expert_agent(
        &self,
        request_id: &ExpertRequestId,
        agent_session_id: &AgentSessionId,
    ) -> Result<ExpertRequestRecord, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let request_run_id: String = transaction
            .query_row(
                "SELECT run_id FROM expert_requests WHERE id=?1",
                [request_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("expert request {request_id}")))?;
        let agent = transaction
            .query_row(
                "SELECT run_id,role FROM agent_sessions WHERE id=?1",
                [agent_session_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("agent session {agent_session_id}")))?;
        if agent.0 != request_run_id || agent.1 != "expert" {
            return Err(StoreError::Validation(
                "expert request agent must be an Expert session on the same run".to_owned(),
            ));
        }
        let changed = transaction.execute(
            "UPDATE expert_requests SET agent_session_id=?2 WHERE id=?1 AND state='QUEUED' AND agent_session_id IS NULL",
            params![request_id.as_str(), agent_session_id.as_str()],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "expert request {request_id} is not an unbound queued request"
            )));
        }
        let record = transaction.query_row(
            &format!("{EXPERT_REQUEST_SELECT} WHERE id=?1"),
            [request_id.as_str()],
            map_expert_request,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn expert_request_for_agent(
        &self,
        agent_session_id: &AgentSessionId,
    ) -> Result<Option<ExpertRequestRecord>, StoreError> {
        self.connection()?
            .query_row(
                &format!("{EXPERT_REQUEST_SELECT} WHERE agent_session_id=?1"),
                [agent_session_id.as_str()],
                map_expert_request,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn finish_expert_request(
        &self,
        request_id: &ExpertRequestId,
        state: &str,
        failure_reason: Option<&str>,
    ) -> Result<ExpertRequestRecord, StoreError> {
        if !matches!(
            state,
            "COMPLETED" | "FAILED" | "INCONCLUSIVE" | "CANCELED" | "STALE"
        ) {
            return Err(StoreError::Validation(
                "expert request terminal state is invalid".to_owned(),
            ));
        }
        let failure_reason = failure_reason
            .map(|value| bounded_text(value, 4_000, "expert request failure reason"))
            .transpose()?;
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE expert_requests SET state=?2,completed_at=?3,failure_reason=?4 WHERE id=?1 AND state IN ('QUEUED','RUNNING')",
            params![request_id.as_str(), state, now, failure_reason],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "expert request {request_id} is not active"
            )));
        }
        self.expert_request(request_id)
    }

    pub fn record_expert_response(
        &self,
        request_id: &ExpertRequestId,
        response_id: &ExpertResponseId,
        payload: &Value,
    ) -> Result<ExpertResponseRecord, StoreError> {
        let raw = serde_json::to_string(payload)?;
        if raw.is_empty() || raw.len() > 131_072 {
            return Err(StoreError::Validation(
                "expert response payload exceeds its bounded custody limit".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let request = transaction
            .query_row(
                "SELECT run_id,snapshot_id,state,agent_session_id,expires_at FROM expert_requests WHERE id=?1",
                [request_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("expert request {request_id}")))?;
        if request.2 != "RUNNING" || request.3.is_none() {
            return Err(StoreError::Conflict(format!(
                "expert request {request_id} is not a bound running request"
            )));
        }
        let now = now_ms();
        if request.4 <= now {
            transaction.execute(
                "UPDATE expert_requests SET state='STALE',completed_at=?2,failure_reason='expert request expired before its response was accepted' WHERE id=?1 AND state='RUNNING'",
                params![request_id.as_str(), now],
            )?;
            transaction.commit()?;
            return Err(StoreError::Conflict(format!(
                "expert request {request_id} expired before response intake"
            )));
        }
        transaction.execute(
            "INSERT INTO expert_responses(id,request_id,run_id,snapshot_id,payload_json,payload_sha256,byte_length,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                response_id.as_str(),
                request_id.as_str(),
                request.0,
                request.1,
                raw,
                hex::encode(Sha256::digest(raw.as_bytes())),
                i64::try_from(raw.len()).map_err(|_| StoreError::Validation("expert response payload is too large".to_owned()))?,
                now,
            ],
        )?;
        transaction.execute(
            "UPDATE expert_requests SET state='COMPLETED',completed_at=?2,failure_reason=NULL WHERE id=?1 AND state='RUNNING'",
            params![request_id.as_str(), now],
        )?;
        let record = transaction.query_row(
            "SELECT id,request_id,run_id,snapshot_id,payload_json,payload_sha256,byte_length,created_at FROM expert_responses WHERE id=?1",
            [response_id.as_str()],
            map_expert_response,
        )?;
        transaction.commit()?;
        Ok(record)
    }
}

fn later_material_event_exists<F>(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    event_cursor: i64,
    is_material_event: &F,
) -> Result<bool, StoreError>
where
    F: Fn(&DomainEvent) -> bool,
{
    let mut statement = transaction.prepare(
        "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json
         FROM domain_events WHERE run_id=?1 AND id>?2 ORDER BY id LIMIT ?3",
    )?;
    let events = statement
        .query_map(
            params![
                run_id.as_str(),
                event_cursor,
                i64::try_from(MAX_EXPERT_FRESHNESS_EVENTS + 1)
                    .expect("bounded event limit fits i64"),
            ],
            queries::map_domain_event,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(events.len() > MAX_EXPERT_FRESHNESS_EVENTS || events.iter().any(is_material_event))
}

fn validate_snapshot_binding(
    payload: &Value,
    id: &SupervisorSnapshotId,
    run_id: &RunId,
    revision: u64,
    event_cursor: i64,
) -> Result<(), StoreError> {
    let valid = payload.get("schema").and_then(Value::as_str) == Some(SUPERVISOR_SNAPSHOT_SCHEMA)
        && payload.get("snapshot_id").and_then(Value::as_str) == Some(id.as_str())
        && payload.get("run_id").and_then(Value::as_str) == Some(run_id.as_str())
        && payload.get("revision").and_then(Value::as_u64) == Some(revision)
        && payload.get("event_cursor").and_then(Value::as_i64) == Some(event_cursor);
    if valid {
        Ok(())
    } else {
        Err(StoreError::Validation(
            "supervisor snapshot payload does not match its immutable envelope".to_owned(),
        ))
    }
}

fn positive_u64(column: usize, value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            format!("invalid non-negative integer in supervisor snapshot column {column}").into(),
        )
    })
}

fn map_supervisor_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<SupervisorSnapshotRecord> {
    let revision: i64 = row.get(2)?;
    let byte_length: i64 = row.get(7)?;
    let created_at: i64 = row.get(8)?;
    let raw: String = row.get(5)?;
    let payload_sha256: String = row.get(6)?;
    let byte_length = positive_u64(7, byte_length)?;
    let encoded_length = u64::try_from(raw.len()).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Text,
            "supervisor snapshot payload exceeds supported size".into(),
        )
    })?;
    let calculated_sha256 = hex::encode(Sha256::digest(raw.as_bytes()));
    if byte_length != encoded_length || payload_sha256 != calculated_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Text,
            "supervisor snapshot payload integrity check failed".into(),
        ));
    }
    Ok(SupervisorSnapshotRecord {
        id: SupervisorSnapshotId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        revision: positive_u64(2, revision)?,
        event_cursor: row.get(3)?,
        trigger_kind: row.get(4)?,
        payload: serde_json::from_str(&raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        payload_sha256,
        byte_length,
        created_at: format_timestamp(created_at),
    })
}

fn map_supervisor_review(row: &rusqlite::Row<'_>) -> rusqlite::Result<SupervisorReviewRecord> {
    let created_at: i64 = row.get(9)?;
    let completed_at: Option<i64> = row.get(10)?;
    Ok(SupervisorReviewRecord {
        id: SupervisorReviewId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        snapshot_id: SupervisorSnapshotId::from(row.get::<_, String>(2)?),
        agent_session_id: AgentSessionId::from(row.get::<_, String>(3)?),
        expected_decision_id: SupervisorDecisionId::from(row.get::<_, String>(4)?),
        state: row.get(5)?,
        trigger_kind: row.get(6)?,
        requested_model: row.get(7)?,
        requested_effort: row.get(8)?,
        created_at: format_timestamp(created_at),
        completed_at: completed_at.map(format_timestamp),
        failure_reason: row.get(11)?,
    })
}

fn map_supervisor_decision(row: &rusqlite::Row<'_>) -> rusqlite::Result<SupervisorDecisionRecord> {
    let raw: String = row.get(6)?;
    let payload_sha256: String = row.get(7)?;
    let byte_length = positive_u64(8, row.get(8)?)?;
    let encoded_length = u64::try_from(raw.len()).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Text,
            "supervisor decision payload exceeds supported size".into(),
        )
    })?;
    let calculated_sha256 = hex::encode(Sha256::digest(raw.as_bytes()));
    if encoded_length != byte_length || payload_sha256 != calculated_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Text,
            "supervisor decision payload integrity check failed".into(),
        ));
    }
    let created_at: i64 = row.get(9)?;
    Ok(SupervisorDecisionRecord {
        id: SupervisorDecisionId::from(row.get::<_, String>(0)?),
        review_id: SupervisorReviewId::from(row.get::<_, String>(1)?),
        run_id: RunId::from(row.get::<_, String>(2)?),
        snapshot_id: SupervisorSnapshotId::from(row.get::<_, String>(3)?),
        agent_session_id: AgentSessionId::from(row.get::<_, String>(4)?),
        policy_state: row.get(5)?,
        payload: serde_json::from_str(&raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        payload_sha256,
        byte_length,
        created_at: format_timestamp(created_at),
    })
}

fn insert_supervisor_action_proposals(
    transaction: &Transaction<'_>,
    decision_id: &SupervisorDecisionId,
    run_id: &RunId,
    snapshot_id: &SupervisorSnapshotId,
    payload: &Value,
    policy_state: &str,
    created_at: i64,
) -> Result<(), StoreError> {
    let Some(actions) = payload.get("actions") else {
        // Store-level custody fixtures intentionally test only the immutable
        // envelope. The orchestrator schema validator requires actions before
        // any real decision can arrive here.
        return Ok(());
    };
    let actions = actions.as_array().ok_or_else(|| {
        StoreError::Validation("supervisor decision actions must be an array".to_owned())
    })?;
    if actions.is_empty() || actions.len() > 50 {
        return Err(StoreError::Validation(
            "supervisor decision action count is invalid".to_owned(),
        ));
    }
    let state = if policy_state == "STALE" {
        "STALE"
    } else {
        "PROPOSED"
    };
    for action in actions {
        let object = action.as_object().ok_or_else(|| {
            StoreError::Validation("supervisor decision action must be an object".to_owned())
        })?;
        let proposal_action_id =
            object
                .get("action_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    StoreError::Validation("supervisor action lacks action_id".to_owned())
                })?;
        let proposal_action_id = bounded_text(proposal_action_id, 128, "supervisor action id")?;
        let kind = object
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| StoreError::Validation("supervisor action lacks kind".to_owned()))?;
        if !SUPERVISOR_ACTION_KINDS.contains(&kind) {
            return Err(StoreError::Validation(format!(
                "supervisor action {proposal_action_id} has an unknown kind"
            )));
        }
        let target = object
            .get("target")
            .cloned()
            .ok_or_else(|| StoreError::Validation("supervisor action lacks target".to_owned()))?;
        let dedupe_key = object
            .get("dedupe_key")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                StoreError::Validation("supervisor action lacks dedupe key".to_owned())
            })?;
        let dedupe_key = bounded_text(dedupe_key, 256, "supervisor action dedupe key")?;
        let target_raw = serde_json::to_string(&target)?;
        let proposal_raw = serde_json::to_string(action)?;
        if target_raw.len() > 16_384 || proposal_raw.len() > 65_536 {
            return Err(StoreError::Validation(
                "supervisor action proposal exceeds its bounded custody limit".to_owned(),
            ));
        }
        transaction.execute(
            "INSERT INTO supervisor_actions(id,decision_id,run_id,snapshot_id,proposal_action_id,kind,target_json,proposal_json,proposal_sha256,dedupe_key,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                SupervisorActionId::new().as_str(),
                decision_id.as_str(),
                run_id.as_str(),
                snapshot_id.as_str(),
                proposal_action_id,
                kind,
                target_raw,
                proposal_raw,
                hex::encode(Sha256::digest(proposal_raw.as_bytes())),
                dedupe_key,
                state,
                created_at,
            ],
        )?;
    }
    Ok(())
}

const SUPERVISOR_ACTION_KINDS: &[&str] = &[
    "wait",
    "continue_attempt",
    "steer_active_turn",
    "start_followup_turn",
    "retry_fresh_attempt",
    "spawn_explorer",
    "spawn_reviewer",
    "reroute_attempt",
    "request_expert",
    "request_replan",
    "request_verification",
    "queue_integration",
    "cancel_attempt",
    "pause_for_human",
    "stop_run",
];

fn bounded_text(value: &str, maximum: usize, field: &str) -> Result<String, StoreError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > maximum {
        return Err(StoreError::Validation(format!("{field} is invalid")));
    }
    Ok(value.to_owned())
}

fn exact_sha256(value: &str, field: &str) -> Result<String, StoreError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(StoreError::Validation(format!(
            "{field} must be lowercase SHA-256"
        )));
    }
    Ok(value.to_owned())
}

fn map_supervisor_action(row: &rusqlite::Row<'_>) -> rusqlite::Result<SupervisorActionRecord> {
    let target_raw: String = row.get(6)?;
    let proposal_raw: String = row.get(7)?;
    let proposal_sha256: String = row.get(8)?;
    let receipt_raw: Option<String> = row.get(12)?;
    let receipt_sha256: Option<String> = row.get(13)?;
    if proposal_sha256 != hex::encode(Sha256::digest(proposal_raw.as_bytes())) {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Text,
            "supervisor action proposal integrity check failed".into(),
        ));
    }
    if let (Some(raw), Some(digest)) = (&receipt_raw, &receipt_sha256)
        && digest != &hex::encode(Sha256::digest(raw.as_bytes()))
    {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Text,
            "supervisor action receipt integrity check failed".into(),
        ));
    }
    let created_at: i64 = row.get(14)?;
    let evaluated_at: Option<i64> = row.get(15)?;
    let execution_started_at: Option<i64> = row.get(16)?;
    let completed_at: Option<i64> = row.get(17)?;
    Ok(SupervisorActionRecord {
        id: SupervisorActionId::from(row.get::<_, String>(0)?),
        decision_id: SupervisorDecisionId::from(row.get::<_, String>(1)?),
        run_id: RunId::from(row.get::<_, String>(2)?),
        snapshot_id: SupervisorSnapshotId::from(row.get::<_, String>(3)?),
        proposal_action_id: row.get(4)?,
        kind: row.get(5)?,
        target: serde_json::from_str(&target_raw).map_err(json_column_error(6))?,
        proposal: serde_json::from_str(&proposal_raw).map_err(json_column_error(7))?,
        proposal_sha256,
        dedupe_key: row.get(9)?,
        state: row.get(10)?,
        policy_reason: row.get(11)?,
        execution_receipt: receipt_raw
            .map(|raw| serde_json::from_str(&raw).map_err(json_column_error(12)))
            .transpose()?,
        execution_receipt_sha256: receipt_sha256,
        created_at: format_timestamp(created_at),
        evaluated_at: evaluated_at.map(format_timestamp),
        execution_started_at: execution_started_at.map(format_timestamp),
        completed_at: completed_at.map(format_timestamp),
    })
}

fn map_expert_request(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExpertRequestRecord> {
    let payload_raw: String = row.get(7)?;
    let payload_sha256: String = row.get(8)?;
    if payload_sha256 != hex::encode(Sha256::digest(payload_raw.as_bytes())) {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Text,
            "expert request payload integrity check failed".into(),
        ));
    }
    let expires_at: i64 = row.get(11)?;
    let created_at: i64 = row.get(12)?;
    let started_at: Option<i64> = row.get(13)?;
    let completed_at: Option<i64> = row.get(14)?;
    Ok(ExpertRequestRecord {
        id: ExpertRequestId::from(row.get::<_, String>(0)?),
        action_id: SupervisorActionId::from(row.get::<_, String>(1)?),
        decision_id: SupervisorDecisionId::from(row.get::<_, String>(2)?),
        run_id: RunId::from(row.get::<_, String>(3)?),
        snapshot_id: SupervisorSnapshotId::from(row.get::<_, String>(4)?),
        signature: row.get(5)?,
        state: row.get(6)?,
        payload: serde_json::from_str(&payload_raw).map_err(json_column_error(7))?,
        payload_sha256,
        requested_model: row.get(9)?,
        requested_effort: row.get(10)?,
        expires_at: format_timestamp(expires_at),
        created_at: format_timestamp(created_at),
        started_at: started_at.map(format_timestamp),
        completed_at: completed_at.map(format_timestamp),
        failure_reason: row.get(15)?,
        agent_session_id: row.get::<_, Option<String>>(16)?.map(AgentSessionId::from),
    })
}

fn map_expert_response(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExpertResponseRecord> {
    let payload_raw: String = row.get(4)?;
    let payload_sha256: String = row.get(5)?;
    let byte_length = positive_u64(6, row.get(6)?)?;
    if byte_length != payload_raw.len() as u64
        || payload_sha256 != hex::encode(Sha256::digest(payload_raw.as_bytes()))
    {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            "expert response payload integrity check failed".into(),
        ));
    }
    let created_at: i64 = row.get(7)?;
    Ok(ExpertResponseRecord {
        id: ExpertResponseId::from(row.get::<_, String>(0)?),
        request_id: ExpertRequestId::from(row.get::<_, String>(1)?),
        run_id: RunId::from(row.get::<_, String>(2)?),
        snapshot_id: SupervisorSnapshotId::from(row.get::<_, String>(3)?),
        payload: serde_json::from_str(&payload_raw).map_err(json_column_error(4))?,
        payload_sha256,
        byte_length,
        created_at: format_timestamp(created_at),
    })
}

fn json_column_error(column: usize) -> impl FnOnce(serde_json::Error) -> rusqlite::Error {
    move |error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NewAgentSession, NewRepository, NewRun};
    use harness_domain::{AgentRole, SandboxMode};
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn fixture() -> (TempDir, Store, RunId) {
        let temp = TempDir::new().expect("temporary fixture");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store opens");
        let (store, run) = populate_fixture(&temp, store);
        (temp, store, run)
    }

    fn shared_fixture() -> (TempDir, Store, Store, RunId) {
        let temp = TempDir::new().expect("temporary fixture");
        let database = temp.path().join("supervision.sqlite3");
        let store =
            Store::open(&database, &temp.path().join("artifacts")).expect("first store opens");
        let (store, run) = populate_fixture(&temp, store);
        let writer = Store::open(&database, &temp.path().join("writer-artifacts"))
            .expect("second store opens");
        (temp, store, writer, run)
    }

    fn populate_fixture(temp: &TempDir, store: Store) -> (Store, RunId) {
        let repository = harness_domain::RepositoryId::from("supervision-repository");
        let run = RunId::from("supervision-run");
        store
            .create_repository(&NewRepository {
                id: repository.clone(),
                profile_id: "fixture".into(),
                profile_version: 1,
                display_name: "fixture".into(),
                root_path: temp.path().join("checkout"),
                origin_url: None,
                default_branch: "main".into(),
                expected_coordination_branch: None,
                state: "READY".into(),
            })
            .expect("repository persists");
        store
            .create_run(&NewRun {
                id: run.clone(),
                repository_id: repository,
                title: "supervision fixture".into(),
                objective: "exercise immutable snapshot custody".into(),
                mode: "plan_and_implement".into(),
                publication_mode: "none".into(),
                state: "CREATED".into(),
                phase: "created".into(),
                base_ref: "main".into(),
                base_sha: "a".repeat(40),
                authority_digest: "b".repeat(64),
                profile_digest: "c".repeat(64),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".into(),
                token_budget: None,
            })
            .expect("run persists");
        (store, run)
    }

    fn decision_with_action(store: &Store, run: &RunId, kind: &str) -> SupervisorDecisionRecord {
        let snapshot = store
            .record_supervisor_snapshot(run, 1, "operator_steered", |id, revision| {
                Ok(serde_json::json!({
                    "schema": SUPERVISOR_SNAPSHOT_SCHEMA,
                    "snapshot_id": id,
                    "run_id": run,
                    "revision": revision,
                    "event_cursor": 1,
                }))
            })
            .expect("snapshot persists");
        let agent_id = AgentSessionId::from("supervisor-action-agent");
        store
            .create_agent_session(&NewAgentSession {
                id: agent_id.clone(),
                run_id: run.clone(),
                task_attempt_id: None,
                parent_agent_session_id: None,
                runtime_kind: "test".to_owned(),
                codex_account_id: None,
                role: AgentRole::Supervisor,
                nickname: Some("supervisor".to_owned()),
                requested_model: "gpt-5.6-terra".to_owned(),
                requested_reasoning_effort: "high".to_owned(),
                sandbox_mode: SandboxMode::ReadOnly,
                approval_policy: "never".to_owned(),
                cwd: PathBuf::from("/tmp"),
                state: "STARTING".to_owned(),
                current_goal: Some("read-only review".to_owned()),
                token_budget: Some(24_000),
            })
            .expect("agent persists");
        let review = store
            .create_supervisor_review(&NewSupervisorReview {
                id: SupervisorReviewId::from("supervisor-action-review"),
                run_id: run.clone(),
                snapshot_id: snapshot.id.clone(),
                agent_session_id: agent_id,
                expected_decision_id: SupervisorDecisionId::from("supervisor-action-decision"),
                trigger_kind: "operator_steered".to_owned(),
                requested_model: "gpt-5.6-terra".to_owned(),
                requested_effort: "high".to_owned(),
            })
            .expect("review persists");
        store
            .mark_supervisor_review_running(&review.id)
            .expect("review starts");
        let payload = serde_json::json!({
            "schema": "harness.supervisor-decision.v1",
            "decision_id": review.expected_decision_id,
            "snapshot_id": snapshot.id,
            "run_id": run,
            "summary": "A bounded controller action was proposed.",
            "actions": [{
                "action_id": "action-one",
                "kind": kind,
                "target": {"kind": "run", "id": run, "task_id": null, "attempt_id": null, "session_id": null},
                "dedupe_key": format!("fixture-{kind}"),
            }],
        });
        store
            .record_current_supervisor_decision(&review.id, 1, &payload, |_| false)
            .expect("decision and its action proposal persist together")
    }

    #[test]
    fn snapshot_custody_is_immutable_and_idempotent_at_an_event_cursor() {
        let (_temp, store, run) = fixture();
        let first = store
            .record_supervisor_snapshot(&run, 41, "attempt_failed", |id, revision| {
                Ok(serde_json::json!({
                    "schema": SUPERVISOR_SNAPSHOT_SCHEMA,
                    "snapshot_id": id,
                    "run_id": run,
                    "revision": revision,
                    "event_cursor": 41,
                    "value": "first",
                }))
            })
            .expect("snapshot persists");
        assert_eq!(first.revision, 1);
        assert_eq!(store.supervisor_observation_cursor(&run).unwrap(), 41);
        assert_eq!(store.check().unwrap().schema_version, "18");

        let duplicate = store
            .record_supervisor_snapshot(&run, 41, "task_stalled", |_, _| {
                panic!("idempotent replay must not rebuild an immutable snapshot")
            })
            .expect("same cursor reads existing snapshot");
        assert_eq!(duplicate.id, first.id);
        assert_eq!(duplicate.revision, 1);
        assert_eq!(duplicate.trigger_kind, "attempt_failed");

        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute(
                    "UPDATE supervisor_snapshots SET trigger_kind='rewritten' WHERE id=?1",
                    [first.id.as_str()],
                )
                .is_err()
        );
        assert!(
            connection
                .execute(
                    "DELETE FROM supervisor_snapshots WHERE id=?1",
                    [first.id.as_str()],
                )
                .is_err()
        );
        // Simulate off-path database tampering after explicitly removing the
        // production immutability guard. Reads still must fail closed on the
        // independent byte/digest receipt.
        connection
            .execute_batch("DROP TRIGGER supervisor_snapshots_no_update;")
            .expect("test can remove guard");
        connection
            .execute(
                "UPDATE supervisor_snapshots SET payload_json='{}' WHERE id=?1",
                [first.id.as_str()],
            )
            .expect("test corruption writes");
        drop(connection);
        assert!(store.latest_supervisor_snapshot(&run).is_err());
    }

    #[test]
    fn malformed_snapshot_binding_is_rejected_without_advancing_the_cursor() {
        let (_temp, store, run) = fixture();
        let error = store
            .record_supervisor_snapshot(&run, 7, "attempt_failed", |_, _| {
                Ok(serde_json::json!({"schema": SUPERVISOR_SNAPSHOT_SCHEMA}))
            })
            .expect_err("binding mismatch must fail closed");
        assert!(error.to_string().contains("immutable envelope"));
        assert_eq!(store.supervisor_observation_cursor(&run).unwrap(), 0);
        assert!(store.latest_supervisor_snapshot(&run).unwrap().is_none());
    }

    #[test]
    fn advisory_decision_is_hash_bound_immutable_and_single_use() {
        let (_temp, store, run) = fixture();
        let snapshot = store
            .record_supervisor_snapshot(&run, 9, "operator_steered", |id, revision| {
                Ok(serde_json::json!({
                    "schema": SUPERVISOR_SNAPSHOT_SCHEMA,
                    "snapshot_id": id,
                    "run_id": run,
                    "revision": revision,
                    "event_cursor": 9,
                }))
            })
            .expect("snapshot persists");
        let agent_id = AgentSessionId::from("supervisor-agent");
        store
            .create_agent_session(&NewAgentSession {
                id: agent_id.clone(),
                run_id: run.clone(),
                task_attempt_id: None,
                parent_agent_session_id: None,
                runtime_kind: "test".to_owned(),
                codex_account_id: None,
                role: AgentRole::Supervisor,
                nickname: Some("supervisor".to_owned()),
                requested_model: "gpt-5.6-terra".to_owned(),
                requested_reasoning_effort: "high".to_owned(),
                sandbox_mode: SandboxMode::ReadOnly,
                approval_policy: "never".to_owned(),
                cwd: PathBuf::from("/tmp"),
                state: "STARTING".to_owned(),
                current_goal: Some("read-only review".to_owned()),
                token_budget: Some(24_000),
            })
            .expect("agent persists");
        let review = store
            .create_supervisor_review(&NewSupervisorReview {
                id: SupervisorReviewId::from("supervisor-review"),
                run_id: run.clone(),
                snapshot_id: snapshot.id.clone(),
                agent_session_id: agent_id.clone(),
                expected_decision_id: SupervisorDecisionId::from("supervisor-decision"),
                trigger_kind: "operator_steered".to_owned(),
                requested_model: "gpt-5.6-terra".to_owned(),
                requested_effort: "high".to_owned(),
            })
            .expect("review persists");
        store
            .mark_supervisor_review_running(&review.id)
            .expect("review starts");
        let payload = serde_json::json!({
            "schema": "harness.supervisor-decision.v1",
            "decision_id": "supervisor-decision",
            "snapshot_id": snapshot.id,
            "run_id": run,
            "summary": "A human decision is required.",
        });
        let decision = store
            .record_current_supervisor_decision(&review.id, 9, &payload, |_| false)
            .expect("decision persists");
        assert_eq!(decision.policy_state, "ADVISORY");
        assert_eq!(
            store.latest_supervisor_decision(&run).unwrap().unwrap().id,
            decision.id
        );
        assert!(
            store
                .record_current_supervisor_decision(&review.id, 9, &payload, |_| false)
                .is_err()
        );
        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute(
                    "UPDATE supervisor_decisions SET policy_state='STALE' WHERE id=?1",
                    [decision.id.as_str()],
                )
                .is_err()
        );
    }

    #[test]
    fn material_event_committed_before_decision_receipt_forces_stale_policy() {
        let (_temp, store, run) = fixture();
        let baseline = store
            .emit_domain_event(
                Some(&run),
                "agent",
                "agent-1",
                "agent.heartbeat",
                &serde_json::json!({}),
                None,
            )
            .expect("baseline telemetry persists");
        let snapshot = store
            .record_supervisor_snapshot(&run, baseline.id, "operator_steered", |id, revision| {
                Ok(serde_json::json!({
                    "schema": SUPERVISOR_SNAPSHOT_SCHEMA,
                    "snapshot_id": id,
                    "run_id": run,
                    "revision": revision,
                    "event_cursor": baseline.id,
                }))
            })
            .expect("snapshot persists");
        let agent_id = AgentSessionId::from("stale-supervisor-agent");
        store
            .create_agent_session(&NewAgentSession {
                id: agent_id.clone(),
                run_id: run.clone(),
                task_attempt_id: None,
                parent_agent_session_id: None,
                runtime_kind: "test".to_owned(),
                codex_account_id: None,
                role: AgentRole::Supervisor,
                nickname: Some("stale-supervisor".to_owned()),
                requested_model: "gpt-5.6-terra".to_owned(),
                requested_reasoning_effort: "high".to_owned(),
                sandbox_mode: SandboxMode::ReadOnly,
                approval_policy: "never".to_owned(),
                cwd: PathBuf::from("/tmp"),
                state: "STARTING".to_owned(),
                current_goal: Some("read-only review".to_owned()),
                token_budget: Some(24_000),
            })
            .expect("agent persists");
        let review = store
            .create_supervisor_review(&NewSupervisorReview {
                id: SupervisorReviewId::from("stale-supervisor-review"),
                run_id: run.clone(),
                snapshot_id: snapshot.id.clone(),
                agent_session_id: agent_id,
                expected_decision_id: SupervisorDecisionId::from("stale-supervisor-decision"),
                trigger_kind: "operator_steered".to_owned(),
                requested_model: "gpt-5.6-terra".to_owned(),
                requested_effort: "high".to_owned(),
            })
            .expect("review persists");
        store
            .mark_supervisor_review_running(&review.id)
            .expect("review starts");
        store
            .emit_domain_event(
                Some(&run),
                "task",
                "task-1",
                "task.start_failed",
                &serde_json::json!({"reason": "fixture failure"}),
                None,
            )
            .expect("later material event persists");
        let payload = serde_json::json!({
            "schema": "harness.supervisor-decision.v1",
            "decision_id": "stale-supervisor-decision",
            "snapshot_id": snapshot.id,
            "run_id": run,
            "summary": "The earlier snapshot cannot authorize a current recommendation.",
        });
        let decision = store
            .record_current_supervisor_decision(&review.id, baseline.id, &payload, |event| {
                event.event_type == "task.start_failed"
            })
            .expect("stale receipt persists");
        assert_eq!(decision.policy_state, "STALE");
        assert_eq!(store.supervisor_review(&review.id).unwrap().state, "STALE");
    }

    #[test]
    fn observation_input_is_captured_from_one_read_transaction() {
        let (_temp, store, writer, run) = shared_fixture();
        let captured = store
            .capture_supervisor_observation_with(&run, 10_000, || {
                {
                    let connection = writer.connection()?;
                    connection.execute(
                        "UPDATE runs SET phase='mutated-after-capture' WHERE id=?1",
                        [run.as_str()],
                    )?;
                }
                writer.emit_domain_event(
                    Some(&run),
                    "task",
                    "fixture-task",
                    "task.start_failed",
                    &serde_json::json!({"reason": "concurrent mutation"}),
                    None,
                )?;
                Ok(())
            })
            .expect("read transaction captures a coherent observation");

        assert_eq!(captured.run.phase, "created");
        assert!(captured.events.is_empty());
        assert_eq!(store.run(&run).unwrap().phase, "mutated-after-capture");
        assert_eq!(
            store.list_domain_events(0, Some(&run), 10).unwrap().len(),
            1,
            "the concurrent event exists, but was not mixed into the prior read view"
        );
    }

    #[test]
    fn action_proposals_are_hash_bound_and_follow_a_closed_lifecycle() {
        let (_temp, store, run) = fixture();
        let decision = decision_with_action(&store, &run, "wait");
        let action = store
            .supervisor_actions_for_decision(&decision.id)
            .expect("action projection reads")
            .pop()
            .expect("one action proposal is materialized with its decision");
        assert_eq!(action.state, "PROPOSED");
        assert!(store.begin_supervisor_action(&action.id).is_err());
        let accepted = store
            .evaluate_supervisor_action(&action.id, true, "current run still matches snapshot")
            .expect("policy accepts once");
        assert_eq!(accepted.state, "POLICY_ACCEPTED");
        assert!(
            store
                .evaluate_supervisor_action(&action.id, false, "late rejection")
                .is_err()
        );
        let executing = store
            .begin_supervisor_action(&action.id)
            .expect("accepted action begins once");
        assert_eq!(executing.state, "EXECUTING");
        let terminal = store
            .complete_supervisor_action(
                &action.id,
                true,
                &serde_json::json!({"schema": "harness.supervisor-action-receipt.v1", "outcome": "wait_scheduled"}),
            )
            .expect("terminal receipt persists");
        assert_eq!(terminal.state, "SUCCEEDED");
        assert!(
            store
                .complete_supervisor_action(&action.id, false, &serde_json::json!({"late": true}))
                .is_err()
        );
        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute(
                    "UPDATE supervisor_actions SET dedupe_key='rewritten' WHERE id=?1",
                    [action.id.as_str()],
                )
                .is_err()
        );
    }

    #[test]
    fn expert_request_requires_policy_and_response_is_immutable() {
        let (_temp, store, run) = fixture();
        let decision = decision_with_action(&store, &run, "request_expert");
        let action = store
            .supervisor_actions_for_decision(&decision.id)
            .unwrap()
            .pop()
            .unwrap();
        let input = NewExpertRequest {
            id: ExpertRequestId::from("expert-request-one"),
            action_id: action.id.clone(),
            event_cursor: 0,
            signature: "a".repeat(64),
            payload: serde_json::json!({"schema": "harness.expert-request.v1", "question": "resolve one bounded invariant"}),
            requested_model: "gpt-5.6-sol".to_owned(),
            requested_effort: "xhigh".to_owned(),
            expires_at_ms: now_ms() + 60_000,
            max_completed_per_signature: 2,
        };
        assert!(store.create_expert_request(&input).is_err());
        store
            .evaluate_supervisor_action(&action.id, true, "hard escalation gate satisfied")
            .unwrap();
        let request = store
            .create_expert_request(&input)
            .expect("accepted expert action creates one durable request");
        assert_eq!(request.state, "QUEUED");
        assert!(store.begin_expert_request(&request.id).is_err());
        let expert_id = AgentSessionId::from("expert-session-one");
        store
            .create_agent_session(&NewAgentSession {
                id: expert_id.clone(),
                run_id: run.clone(),
                task_attempt_id: None,
                parent_agent_session_id: None,
                runtime_kind: "test".to_owned(),
                codex_account_id: None,
                role: AgentRole::Expert,
                nickname: Some("expert".to_owned()),
                requested_model: "gpt-5.6-sol".to_owned(),
                requested_reasoning_effort: "xhigh".to_owned(),
                sandbox_mode: SandboxMode::ReadOnly,
                approval_policy: "never".to_owned(),
                cwd: PathBuf::from("/tmp"),
                state: "STARTING".to_owned(),
                current_goal: Some("bounded expert review".to_owned()),
                token_budget: Some(48_000),
            })
            .unwrap();
        let request = store
            .attach_expert_agent(&request.id, &expert_id)
            .expect("one expert session binds before execution");
        assert_eq!(request.agent_session_id.as_ref(), Some(&expert_id));
        assert_eq!(
            store
                .expert_request_for_agent(&expert_id)
                .unwrap()
                .expect("expert binding reads")
                .id,
            request.id
        );
        store.begin_expert_request(&request.id).unwrap();
        let response = store
            .record_expert_response(
                &request.id,
                &ExpertResponseId::from("expert-response-one"),
                &serde_json::json!({"schema": "harness.expert-response.v1", "recommendation": "preserve the stated invariant"}),
            )
            .expect("running expert request receives one immutable response");
        assert_eq!(
            store.expert_request(&request.id).unwrap().state,
            "COMPLETED"
        );
        let connection = store.connection().expect("connection");
        assert!(
            connection
                .execute(
                    "UPDATE expert_responses SET payload_json='{}' WHERE id=?1",
                    [response.id.as_str()],
                )
                .is_err()
        );
    }

    #[test]
    fn expert_freshness_ignores_receipts_but_rejects_a_later_material_event() {
        let (_temp, store, run) = fixture();
        let decision = decision_with_action(&store, &run, "request_expert");
        let action = store
            .supervisor_actions_for_decision(&decision.id)
            .unwrap()
            .pop()
            .unwrap();
        store
            .evaluate_supervisor_action(&action.id, true, "hard escalation gate satisfied")
            .unwrap();
        let request = |id: &str| NewExpertRequest {
            id: ExpertRequestId::from(id),
            action_id: action.id.clone(),
            event_cursor: 0,
            signature: "e".repeat(64),
            payload: serde_json::json!({"schema": "harness.expert-request.v1"}),
            requested_model: "gpt-5.6-sol".to_owned(),
            requested_effort: "xhigh".to_owned(),
            expires_at_ms: now_ms() + 60_000,
            max_completed_per_signature: 2,
        };
        store
            .emit_domain_event(
                Some(&run),
                "agent",
                "supervisor",
                "agent.supervisor.decision_recorded",
                &serde_json::json!({"automatic_action": false}),
                None,
            )
            .expect("non-material supervisor receipt persists");
        let queued = store
            .create_expert_request_if_materially_current(&request("expert-nonmaterial"), |event| {
                event.event_type == "task.start_failed"
            })
            .expect("a non-material receipt does not stale the expert snapshot");
        store
            .finish_expert_request(&queued.id, "FAILED", Some("fixture terminal"))
            .expect("fixture request finishes without consuming the completion cap");
        store
            .emit_domain_event(
                Some(&run),
                "task",
                "task-1",
                "task.start_failed",
                &serde_json::json!({"reason": "material fixture change"}),
                None,
            )
            .expect("material controller event persists");
        let error = store
            .create_expert_request_if_materially_current(&request("expert-material"), |event| {
                event.event_type == "task.start_failed"
            })
            .expect_err("a later material event must stale the expert snapshot");
        assert!(
            error
                .to_string()
                .contains("newer material controller event")
        );
    }

    #[test]
    fn expert_request_has_one_active_run_and_bounded_completed_signature_history() {
        let (_temp, store, run) = fixture();
        let decision = decision_with_action(&store, &run, "request_expert");
        let action = store
            .supervisor_actions_for_decision(&decision.id)
            .unwrap()
            .pop()
            .unwrap();
        store
            .evaluate_supervisor_action(&action.id, true, "hard escalation gate satisfied")
            .unwrap();

        let input = |id: &str, action_id: SupervisorActionId| NewExpertRequest {
            id: ExpertRequestId::from(id),
            action_id,
            event_cursor: 0,
            signature: "b".repeat(64),
            payload: serde_json::json!({
                "schema": "harness.expert-request.v1",
                "question": "resolve one bounded invariant"
            }),
            requested_model: "gpt-5.6-sol".to_owned(),
            requested_effort: "xhigh".to_owned(),
            expires_at_ms: now_ms() + 60_000,
            max_completed_per_signature: 2,
        };
        let request_one = store
            .create_expert_request(&input("expert-request-cap-one", action.id.clone()))
            .expect("first expert request queues");

        let insert_accepted_action = |id: &str, proposal_action_id: &str| {
            store
                .connection()
                .unwrap()
                .execute(
                    "INSERT INTO supervisor_actions(id,decision_id,run_id,snapshot_id,proposal_action_id,kind,target_json,proposal_json,proposal_sha256,dedupe_key,state,created_at) VALUES(?1,?2,?3,?4,?5,'request_expert','{}','{}',?6,?7,'POLICY_ACCEPTED',?8)",
                    params![
                        id,
                        action.decision_id.as_str(),
                        run.as_str(),
                        action.snapshot_id.as_str(),
                        proposal_action_id,
                        "c".repeat(64),
                        format!("cap-{proposal_action_id}"),
                        now_ms(),
                    ],
                )
                .unwrap();
        };
        insert_accepted_action("expert-cap-action-two", "expert-cap-two");
        let active_error = store
            .create_expert_request(&input(
                "expert-request-cap-two",
                SupervisorActionId::from("expert-cap-action-two"),
            ))
            .expect_err("only one queued or running expert request may exist per run");
        assert!(active_error.to_string().contains("already active"));

        store
            .finish_expert_request(&request_one.id, "INCONCLUSIVE", Some("bounded result"))
            .expect("the first bounded request finishes");
        let request_two = store
            .create_expert_request(&input(
                "expert-request-cap-two",
                SupervisorActionId::from("expert-cap-action-two"),
            ))
            .expect("second completed response remains within cap");
        store
            .finish_expert_request(&request_two.id, "INCONCLUSIVE", Some("bounded result"))
            .expect("the second bounded request finishes");

        insert_accepted_action("expert-cap-action-three", "expert-cap-three");
        let cap_error = store
            .create_expert_request(&input(
                "expert-request-cap-three",
                SupervisorActionId::from("expert-cap-action-three"),
            ))
            .expect_err("a third completed response with the same signature is prohibited");
        assert!(cap_error.to_string().contains("completed-response cap"));
    }

    #[test]
    fn expired_expert_response_is_not_persisted_as_current_advisory_evidence() {
        let (_temp, store, run) = fixture();
        let decision = decision_with_action(&store, &run, "request_expert");
        let action = store
            .supervisor_actions_for_decision(&decision.id)
            .unwrap()
            .pop()
            .unwrap();
        store
            .evaluate_supervisor_action(&action.id, true, "hard escalation gate satisfied")
            .unwrap();
        let request = store
            .create_expert_request(&NewExpertRequest {
                id: ExpertRequestId::from("expired-expert-request"),
                action_id: action.id.clone(),
                event_cursor: 0,
                signature: "d".repeat(64),
                payload: serde_json::json!({"schema": "harness.expert-request.v1"}),
                requested_model: "gpt-5.6-sol".to_owned(),
                requested_effort: "xhigh".to_owned(),
                expires_at_ms: now_ms() + 60_000,
                max_completed_per_signature: 2,
            })
            .expect("request queues before expiry");
        let expert_id = AgentSessionId::from("expired-expert-session");
        store
            .create_agent_session(&NewAgentSession {
                id: expert_id.clone(),
                run_id: run.clone(),
                task_attempt_id: None,
                parent_agent_session_id: None,
                runtime_kind: "test".to_owned(),
                codex_account_id: None,
                role: AgentRole::Expert,
                nickname: Some("expired expert".to_owned()),
                requested_model: "gpt-5.6-sol".to_owned(),
                requested_reasoning_effort: "xhigh".to_owned(),
                sandbox_mode: SandboxMode::ReadOnly,
                approval_policy: "never".to_owned(),
                cwd: PathBuf::from("/tmp"),
                state: "STARTING".to_owned(),
                current_goal: Some("bounded expert review".to_owned()),
                token_budget: Some(48_000),
            })
            .unwrap();
        store.attach_expert_agent(&request.id, &expert_id).unwrap();
        store.begin_expert_request(&request.id).unwrap();

        let connection = store.connection().unwrap();
        connection
            .execute_batch("DROP TRIGGER expert_requests_custody_immutable;")
            .unwrap();
        connection
            .execute(
                "UPDATE expert_requests SET expires_at=0 WHERE id=?1",
                [request.id.as_str()],
            )
            .unwrap();
        drop(connection);

        let error = store
            .record_expert_response(
                &request.id,
                &ExpertResponseId::from("expired-expert-response"),
                &serde_json::json!({"schema": "harness.expert-response.v1"}),
            )
            .expect_err("a response after durable expiry must not become evidence");
        assert!(error.to_string().contains("expired before response intake"));
        assert_eq!(store.expert_request(&request.id).unwrap().state, "STALE");
        let response_count: i64 = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM expert_responses WHERE request_id=?1",
                [request.id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(response_count, 0);
    }
}
