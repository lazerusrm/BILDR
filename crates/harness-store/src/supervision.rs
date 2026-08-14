use harness_domain::{
    AgentSessionId, AgentSummary, DomainEvent, RunId, RunPlan, RunSummary, SupervisorDecisionId,
    SupervisorReviewId, SupervisorSnapshotId, TaskSummary, format_timestamp, now_ms,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Store, StoreError, queries};

pub const SUPERVISOR_SNAPSHOT_SCHEMA: &str = "harness.supervisor-snapshot.v1";

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
        transaction.commit()?;
        Ok(SupervisorObservationInput {
            run,
            cursor,
            events,
            latest_plan,
            tasks,
            agents,
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

    /// Persists the only model output that can influence the advisory surface.
    /// It binds the preallocated decision id, review, snapshot, and agent in a
    /// single transaction.  `STALE` is durable audit evidence, never a
    /// recoverable recommendation.
    pub fn record_supervisor_decision(
        &self,
        review_id: &SupervisorReviewId,
        policy_state: &str,
        payload: &Value,
    ) -> Result<SupervisorDecisionRecord, StoreError> {
        self.record_supervisor_decision_with_freshness(
            review_id,
            policy_state,
            None,
            payload,
            |_| false,
        )
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
            "ADVISORY",
            Some(event_cursor),
            payload,
            is_material_event,
        )
    }

    fn record_supervisor_decision_with_freshness<F>(
        &self,
        review_id: &SupervisorReviewId,
        policy_state: &str,
        event_cursor: Option<i64>,
        payload: &Value,
        is_material_event: F,
    ) -> Result<SupervisorDecisionRecord, StoreError>
    where
        F: Fn(&DomainEvent) -> bool,
    {
        if !matches!(policy_state, "ADVISORY" | "STALE") {
            return Err(StoreError::Validation(
                "supervisor decision policy state is invalid".to_owned(),
            ));
        }
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
        let policy_state = if let Some(event_cursor) = event_cursor {
            // Filter in SQLite first so an arbitrary telemetry backlog cannot
            // turn this strict receipt into an unbounded read. The controller
            // still evaluates the small allowlisted candidate set, including
            // payload-sensitive lifecycle transitions.
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
                policy_state
            }
        } else {
            policy_state
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
        assert_eq!(store.check().unwrap().schema_version, "13");

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
            .record_supervisor_decision(&review.id, "ADVISORY", &payload)
            .expect("decision persists");
        assert_eq!(decision.policy_state, "ADVISORY");
        assert_eq!(
            store.latest_supervisor_decision(&run).unwrap().unwrap().id,
            decision.id
        );
        assert!(
            store
                .record_supervisor_decision(&review.id, "ADVISORY", &payload)
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
}
