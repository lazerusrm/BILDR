use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use harness_domain::{AgentSessionId, CostEstimate, PricingSnapshot, RunId, TokenUsage, now_ms};
use harness_usage::{add_sample, estimate};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

use crate::{RawEventInput, Store, StoreError};

#[derive(Clone)]
pub struct ProtocolProjection {
    store: Store,
    pricing: Arc<BTreeMap<String, PricingSnapshot>>,
    store_raw_reasoning: Arc<AtomicBool>,
    store_reasoning_summaries: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
pub struct ProjectionContext {
    pub run_id: Option<RunId>,
    pub agent_session_id: Option<AgentSessionId>,
}

impl ProtocolProjection {
    #[must_use]
    pub fn new(
        store: Store,
        pricing: impl IntoIterator<Item = PricingSnapshot>,
        store_raw_reasoning: bool,
        store_reasoning_summaries: bool,
    ) -> Self {
        Self {
            store,
            pricing: Arc::new(
                pricing
                    .into_iter()
                    .map(|snapshot| (snapshot.model.clone(), snapshot))
                    .collect(),
            ),
            store_raw_reasoning: Arc::new(AtomicBool::new(store_raw_reasoning)),
            store_reasoning_summaries: Arc::new(AtomicBool::new(store_reasoning_summaries)),
        }
    }

    #[must_use]
    pub fn store_raw_reasoning(&self) -> bool {
        self.store_raw_reasoning.load(Ordering::Acquire)
    }

    pub fn set_store_raw_reasoning(&self, value: bool) {
        self.store_raw_reasoning.store(value, Ordering::Release);
    }

    #[must_use]
    pub fn store_reasoning_summaries(&self) -> bool {
        self.store_reasoning_summaries.load(Ordering::Acquire)
    }

    pub fn set_store_reasoning_summaries(&self, value: bool) {
        self.store_reasoning_summaries
            .store(value, Ordering::Release);
    }

    /// Rebuild the usage ledger once when upgrading from the legacy projection
    /// that retained only the final model call in each Codex turn. Raw App
    /// Server events are the authority, so this is lossless and resumable.
    pub fn rebuild_usage_projection_if_needed(&self) -> Result<(), StoreError> {
        const PROJECTION_KEY: &str = "usage-projection-version";
        const PROJECTION_VERSION: u64 = 3;
        if self
            .store
            .runtime_metadata(PROJECTION_KEY)?
            .and_then(|value| value.as_u64())
            == Some(PROJECTION_VERSION)
        {
            return Ok(());
        }

        let turns = {
            let connection = self.store.connection()?;
            let mut statement = connection.prepare(
                "SELECT thread_id,turn_id,max(id) FROM raw_events WHERE method='thread/tokenUsage/updated' AND thread_id IS NOT NULL AND turn_id IS NOT NULL GROUP BY thread_id,turn_id ORDER BY max(id)",
            )?;
            let turns = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            connection.execute("DELETE FROM cost_entries", [])?;
            connection.execute("DELETE FROM token_samples", [])?;
            connection.execute("UPDATE agent_sessions SET goal_tokens_used=0", [])?;
            turns
        };

        for (thread_id, turn_id, source_raw_id) in turns {
            self.rebuild_turn_usage(&thread_id, &turn_id, source_raw_id, None)?;
        }
        self.store
            .put_runtime_metadata(PROJECTION_KEY, &json!(PROJECTION_VERSION))?;
        Ok(())
    }

    pub fn ingest_notification(
        &self,
        context: &ProjectionContext,
        method: &str,
        payload: &Value,
    ) -> Result<i64, StoreError> {
        let (stored_payload, redaction_class) = redact_reasoning(
            method,
            payload,
            self.store_raw_reasoning(),
            self.store_reasoning_summaries(),
        );
        let thread_id = find_text(&stored_payload, &["threadId"])
            .or_else(|| find_text(&stored_payload, &["thread", "id"]))
            .map(ToOwned::to_owned);
        let turn_id = find_text(&stored_payload, &["turnId"])
            .or_else(|| find_text(&stored_payload, &["turn", "id"]))
            .map(ToOwned::to_owned);
        let raw_id = self.store.append_raw_event(&RawEventInput {
            run_id: context.run_id.clone(),
            agent_session_id: context.agent_session_id.clone(),
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            direction: "inbound".to_owned(),
            method: method.to_owned(),
            request_id: None,
            payload: stored_payload.clone(),
            source_sequence: find_text(&stored_payload, &["sequence"]).map(ToOwned::to_owned),
            redaction_class,
        })?;

        match method {
            "thread/started" => self.project_thread_started(context, &stored_payload)?,
            "turn/started" => self.project_turn_started(context, &stored_payload)?,
            "turn/completed" => self.project_turn_completed(context, &stored_payload)?,
            "item/started" | "item/completed" => {
                self.project_item(method, raw_id, &stored_payload)?;
            }
            "turn/plan/updated" => self.project_plan(raw_id, &stored_payload)?,
            "thread/goal/updated" => self.project_goal(context, &stored_payload)?,
            "thread/goal/cleared" => self.clear_goal(context)?,
            "thread/tokenUsage/updated" => {
                self.project_usage(context, raw_id, &stored_payload)?;
            }
            "model/rerouted" => self.project_model_reroute(context, &stored_payload)?,
            _ => {}
        }
        self.store.touch_projector("codex-v2", raw_id)?;

        // Token and message deltas are immutable raw evidence, but they are
        // not domain state transitions. Mirroring each character fragment to
        // the UI event stream makes a long local-model turn generate tens of
        // thousands of SSE messages and can starve the command-approval path.
        // The completed item still publishes the consolidated result.
        if Self::is_transient_stream_delta(method) {
            return Ok(raw_id);
        }

        let aggregate_id = context
            .agent_session_id
            .as_ref()
            .map(ToString::to_string)
            .or(thread_id)
            .unwrap_or_else(|| "runtime".to_owned());
        let event_type = method.replace('/', ".");
        self.store.emit_domain_event(
            context.run_id.as_ref(),
            if context.agent_session_id.is_some() {
                "agent"
            } else {
                "runtime"
            },
            &aggregate_id,
            &event_type,
            &stored_payload,
            Some(raw_id),
        )?;
        Ok(raw_id)
    }

    fn is_transient_stream_delta(method: &str) -> bool {
        matches!(
            method,
            "item/reasoning/textDelta" | "item/agentMessage/delta"
        )
    }

    /// Build the redacted raw receipt for an inbound notification that the
    /// controller deliberately refuses to project.  The caller commits this
    /// with its terminal containment state in one transaction, so an unbound
    /// child can never acquire a parent session binding through projection.
    pub fn unprojected_notification_input(
        &self,
        context: &ProjectionContext,
        method: &str,
        payload: &Value,
        source_sequence: String,
    ) -> RawEventInput {
        let (stored_payload, redaction_class) = redact_reasoning(
            method,
            payload,
            self.store_raw_reasoning(),
            self.store_reasoning_summaries(),
        );
        RawEventInput {
            run_id: context.run_id.clone(),
            agent_session_id: context.agent_session_id.clone(),
            thread_id: find_text(&stored_payload, &["threadId"])
                .or_else(|| find_text(&stored_payload, &["thread", "id"]))
                .map(ToOwned::to_owned),
            turn_id: find_text(&stored_payload, &["turnId"])
                .or_else(|| find_text(&stored_payload, &["turn", "id"]))
                .map(ToOwned::to_owned),
            direction: "inbound".to_owned(),
            method: method.to_owned(),
            request_id: None,
            payload: stored_payload,
            source_sequence: Some(source_sequence),
            redaction_class,
        }
    }

    pub fn ingest_outbound(
        &self,
        context: &ProjectionContext,
        method: &str,
        request_id: Option<String>,
        payload: &Value,
    ) -> Result<i64, StoreError> {
        let raw_id = self.store.append_raw_event(&RawEventInput {
            run_id: context.run_id.clone(),
            agent_session_id: context.agent_session_id.clone(),
            thread_id: find_text(payload, &["params", "threadId"])
                .or_else(|| find_text(payload, &["threadId"]))
                .map(ToOwned::to_owned),
            turn_id: find_text(payload, &["params", "turnId"])
                .or_else(|| find_text(payload, &["turnId"]))
                .map(ToOwned::to_owned),
            direction: "outbound".to_owned(),
            method: method.to_owned(),
            request_id,
            payload: payload.clone(),
            source_sequence: None,
            redaction_class: "none".to_owned(),
        })?;
        self.store.touch_projector("codex-v2", raw_id)?;
        Ok(raw_id)
    }

    pub fn ingest_diagnostic(&self, method: &str, payload: &Value) -> Result<i64, StoreError> {
        let raw_id = self.store.append_raw_event(&RawEventInput {
            run_id: None,
            agent_session_id: None,
            thread_id: None,
            turn_id: None,
            direction: "diagnostic".to_owned(),
            method: method.to_owned(),
            request_id: None,
            payload: payload.clone(),
            source_sequence: None,
            redaction_class: "probable_secrets_redacted".to_owned(),
        })?;
        self.store.touch_projector("codex-v2", raw_id)?;
        Ok(raw_id)
    }

    fn project_thread_started(
        &self,
        context: &ProjectionContext,
        payload: &Value,
    ) -> Result<(), StoreError> {
        let (Some(agent_id), Some(thread_id)) = (
            context.agent_session_id.as_ref(),
            find_text(payload, &["thread", "id"]),
        ) else {
            return Ok(());
        };
        let parent = find_text(payload, &["thread", "parentThreadId"]);
        self.store.attach_codex_thread(
            agent_id,
            thread_id,
            parent,
            "harness_console",
            find_text(payload, &["thread", "gitInfo", "branch"]),
            find_text(payload, &["thread", "gitInfo", "sha"]),
        )
    }

    fn project_turn_started(
        &self,
        context: &ProjectionContext,
        payload: &Value,
    ) -> Result<(), StoreError> {
        let (Some(agent_id), Some(thread_id), Some(turn_id)) = (
            context.agent_session_id.as_ref(),
            find_text(payload, &["threadId"]),
            find_text(payload, &["turn", "id"]),
        ) else {
            return Ok(());
        };
        self.store.attach_codex_turn(
            agent_id,
            thread_id,
            turn_id,
            find_text(payload, &["turn", "model"]),
            find_text(payload, &["turn", "effort"]),
            true,
        )
    }

    fn project_turn_completed(
        &self,
        context: &ProjectionContext,
        payload: &Value,
    ) -> Result<(), StoreError> {
        let Some(turn_id) = find_text(payload, &["turn", "id"]) else {
            return Ok(());
        };
        let status = find_text(payload, &["turn", "status"]).unwrap_or("failed");
        let now = now_ms();
        let connection = self.store.connection()?;
        connection.execute(
            "UPDATE codex_turns SET status=?2,completed_at=?3,duration_ms=CASE WHEN started_at IS NULL THEN NULL ELSE ?3-started_at END,error_json=?4,version=version+1 WHERE turn_id=?1",
            params![turn_id,status,now,payload.pointer("/turn/error").map(serde_json::to_string).transpose()?],
        )?;
        if let Some(agent_id) = context.agent_session_id.as_ref() {
            connection.execute(
                "UPDATE agent_runtime_details SET active_turn_id=NULL,current_action=?2,last_activity_kind='turn',last_activity_at=?3,updated_at=?3 WHERE agent_session_id=?1",
                params![agent_id.as_str(),format!("Turn {status}"),now],
            )?;
            connection.execute(
                "UPDATE agent_sessions SET state=?2,last_heartbeat_at=?3,version=version+1 WHERE id=?1",
                params![agent_id.as_str(),if status == "completed" { "TURN_COMPLETE" } else if status == "interrupted" { "INTERRUPTED" } else { "FAILED" },now],
            )?;
        }
        Ok(())
    }

    fn project_item(&self, method: &str, raw_id: i64, payload: &Value) -> Result<(), StoreError> {
        let Some(item) = payload.get("item") else {
            return Ok(());
        };
        let Some(item_id) = item.get("id").and_then(Value::as_str) else {
            return Ok(());
        };
        let item_type = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let thread_id = find_text(payload, &["threadId"]).unwrap_or("unknown");
        let turn_id = find_text(payload, &["turnId"]);
        let state = if method == "item/completed" {
            "completed"
        } else {
            "in_progress"
        };
        let summary = summarize_item(item);
        let now = now_ms();
        self.store.connection()?.execute(
            "INSERT INTO projected_items(item_id,thread_id,turn_id,item_type,state,summary,payload_json,source_raw_event_id,started_at,completed_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,1) ON CONFLICT(item_id) DO UPDATE SET state=excluded.state,summary=excluded.summary,payload_json=excluded.payload_json,source_raw_event_id=excluded.source_raw_event_id,completed_at=excluded.completed_at,version=version+1",
            params![item_id,thread_id,turn_id,item_type,state,summary,serde_json::to_string(item)?,raw_id,now,(method == "item/completed").then_some(now)],
        )?;
        if let Some(agent_id) = self.store.agent_by_thread(thread_id)? {
            let agent = self.store.agent(&agent_id)?;
            if agent.active_turn_id.is_some()
                && !matches!(
                    agent.state.as_str(),
                    "COMPLETED"
                        | "TURN_COMPLETE"
                        | "FAILED"
                        | "INTERRUPTED"
                        | "CANCELED"
                        | "STALLED"
                )
            {
                self.store.update_agent_state(
                    &agent_id,
                    "RUNNING",
                    summary.as_deref(),
                    None,
                    None,
                    None,
                )?;
            }
        }
        Ok(())
    }

    fn project_plan(&self, raw_id: i64, payload: &Value) -> Result<(), StoreError> {
        let turn_id = find_text(payload, &["turnId"]).unwrap_or("unknown");
        let thread_id: Option<String> = self
            .store
            .connection()?
            .query_row(
                "SELECT thread_id FROM codex_turns WHERE turn_id=?1",
                [turn_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(thread_id) = thread_id else {
            return Ok(());
        };
        let item_id = format!("plan-{turn_id}");
        let summary = payload
            .get("plan")
            .and_then(Value::as_array)
            .map(|steps| format!("{} plan steps", steps.len()));
        self.store.connection()?.execute(
            "INSERT INTO projected_items(item_id,thread_id,turn_id,item_type,state,summary,payload_json,source_raw_event_id,started_at,version) VALUES(?1,?2,?3,'plan','in_progress',?4,?5,?6,?7,1) ON CONFLICT(item_id) DO UPDATE SET summary=excluded.summary,payload_json=excluded.payload_json,source_raw_event_id=excluded.source_raw_event_id,version=version+1",
            params![item_id,thread_id,turn_id,summary,serde_json::to_string(payload)?,raw_id,now_ms()],
        )?;
        Ok(())
    }

    fn project_goal(&self, context: &ProjectionContext, payload: &Value) -> Result<(), StoreError> {
        let Some(agent_id) = context.agent_session_id.as_ref() else {
            return Ok(());
        };
        let goal = payload.get("goal").unwrap_or(payload);
        // `goal.tokensUsed` is an App Server goal/context counter, not the
        // cumulative billable usage for this Harness agent. The authoritative
        // attempt ledger is rebuilt from token samples in `rebuild_turn_usage`.
        // Never let a later goal notification replace that ledger with a
        // smaller, semantically different value.
        self.store.connection()?.execute(
            "UPDATE agent_sessions SET current_goal=?2,goal_status=?3,token_budget=coalesce(?4,token_budget),goal_time_used_seconds=coalesce(?5,goal_time_used_seconds),last_heartbeat_at=?6,version=version+1 WHERE id=?1",
            params![agent_id.as_str(),find_text(goal,&["objective"]),find_text(goal,&["status"]),find_u64(goal,&["tokenBudget"]).map(|v| v as i64),find_u64(goal,&["timeUsedSeconds"]).map(|v| v as i64),now_ms()],
        )?;
        Ok(())
    }

    fn clear_goal(&self, context: &ProjectionContext) -> Result<(), StoreError> {
        let Some(agent_id) = context.agent_session_id.as_ref() else {
            return Ok(());
        };
        self.store.connection()?.execute(
            "UPDATE agent_sessions SET current_goal=NULL,goal_status=NULL,version=version+1 WHERE id=?1",
            [agent_id.as_str()],
        )?;
        Ok(())
    }

    fn project_model_reroute(
        &self,
        context: &ProjectionContext,
        payload: &Value,
    ) -> Result<(), StoreError> {
        if let Some(agent_id) = context.agent_session_id.as_ref() {
            self.store.update_agent_state(
                agent_id,
                "RUNNING",
                Some("Runtime model rerouted"),
                find_text(payload, &["toModel"]),
                None,
                None,
            )?;
        }
        if let (Some(turn_id), Some(model)) = (
            find_text(payload, &["turnId"]),
            find_text(payload, &["toModel"]),
        ) {
            self.store.connection()?.execute(
                "UPDATE codex_turns SET effective_model=?2,version=version+1 WHERE turn_id=?1",
                params![turn_id, model],
            )?;
        }
        Ok(())
    }

    fn project_usage(
        &self,
        context: &ProjectionContext,
        raw_id: i64,
        payload: &Value,
    ) -> Result<(), StoreError> {
        let (Some(thread_id), Some(turn_id)) = (
            find_text(payload, &["threadId"]),
            find_text(payload, &["turnId"]),
        ) else {
            return Ok(());
        };
        self.rebuild_turn_usage(
            thread_id,
            turn_id,
            raw_id,
            context.agent_session_id.as_ref(),
        )
    }

    fn rebuild_turn_usage(
        &self,
        thread_id: &str,
        turn_id: &str,
        source_raw_id: i64,
        fallback_agent_id: Option<&AgentSessionId>,
    ) -> Result<(), StoreError> {
        let connection = self.store.connection()?;
        let mapped_agent_id = connection
            .query_row(
                "SELECT agent_session_id FROM codex_threads WHERE thread_id=?1",
                [thread_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let agent_id = mapped_agent_id
            .as_deref()
            .or_else(|| fallback_agent_id.map(AgentSessionId::as_str));
        let model = connection
            .query_row(
                "SELECT coalesce(ct.effective_model,a.effective_model,a.requested_model) FROM agent_sessions a LEFT JOIN codex_turns ct ON ct.turn_id=?2 WHERE a.id=?1",
                params![agent_id, turn_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .unwrap_or_else(|| "unknown".to_owned());

        let mut statement = connection.prepare(
            "SELECT id,received_at,payload_json FROM raw_events WHERE thread_id=?1 AND turn_id=?2 AND method='thread/tokenUsage/updated' ORDER BY id",
        )?;
        let rows = statement
            .query_map(params![thread_id, turn_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        let mut summary = harness_domain::UsageSummary::default();
        let mut last_cumulative_total = None;
        let mut observed_at = now_ms();
        let mut last_source_raw_id = source_raw_id;
        let mut context_window = None;
        for (raw_id, received_at, payload_json) in rows {
            let payload: Value = serde_json::from_str(&payload_json)?;
            let Some(last) = payload.pointer("/tokenUsage/last") else {
                continue;
            };
            let cumulative_total =
                find_u64(&payload, &["tokenUsage", "total", "totalTokens"]).unwrap_or_default();
            if last_cumulative_total == Some(cumulative_total) {
                continue;
            }
            last_cumulative_total = Some(cumulative_total);
            let usage = parse_token_usage(last, &payload)?;
            let cost = if let Some(pricing) = self.pricing.get(&model) {
                estimate(&usage, pricing)
                    .map_err(|error| StoreError::Validation(error.to_string()))?
            } else {
                CostEstimate {
                    explanation: "No matching price snapshot".to_owned(),
                    ..CostEstimate::default()
                }
            };
            add_sample(&mut summary, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            context_window = usage.model_context_window;
            observed_at = received_at;
            last_source_raw_id = raw_id;
        }
        if summary.total_tokens == 0 {
            return Ok(());
        }
        summary.cost.explanation = if self.pricing.contains_key(&model) {
            "Sum of distinct App Server model-call usage updates in this Codex turn; reasoning output is included in output and is not charged twice."
                .to_owned()
        } else {
            "No matching price snapshot".to_owned()
        };

        let existing_sample_id = connection
            .query_row(
                "SELECT id FROM token_samples WHERE turn_id=?1 AND sample_kind='turn_total'",
                [turn_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let sample_id = existing_sample_id.unwrap_or_else(|| ulid::Ulid::generate().to_string());
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "DELETE FROM token_samples WHERE turn_id=?1 AND sample_kind<>'turn_total'",
            [turn_id],
        )?;
        transaction.execute(
            "INSERT INTO token_samples(id,thread_id,turn_id,effective_model,observed_at,input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,model_context_window,sample_kind,source_event_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'turn_total',?13) ON CONFLICT(turn_id,sample_kind) WHERE turn_id IS NOT NULL DO UPDATE SET effective_model=excluded.effective_model,observed_at=excluded.observed_at,input_tokens=excluded.input_tokens,cached_input_tokens=excluded.cached_input_tokens,cache_write_input_tokens=excluded.cache_write_input_tokens,output_tokens=excluded.output_tokens,reasoning_output_tokens=excluded.reasoning_output_tokens,total_tokens=excluded.total_tokens,model_context_window=excluded.model_context_window,source_event_id=excluded.source_event_id",
            params![sample_id,thread_id,turn_id,model,observed_at,summary.input_tokens as i64,summary.cached_input_tokens as i64,summary.cache_write_input_tokens.map(|v| v as i64),summary.output_tokens as i64,summary.reasoning_output_tokens as i64,summary.total_tokens as i64,context_window.map(|v| v as i64),last_source_raw_id],
        )?;
        transaction.execute(
            "DELETE FROM cost_entries WHERE token_sample_id=?1",
            [&sample_id],
        )?;
        if let Some(pricing) = self.pricing.get(&model) {
            insert_cost(&transaction, &sample_id, pricing, &summary.cost)?;
        }
        if let Some(agent_id) = agent_id {
            transaction.execute(
                "UPDATE agent_sessions SET goal_tokens_used=coalesce((SELECT sum(ts.total_tokens) FROM codex_threads ct JOIN token_samples ts ON ts.thread_id=ct.thread_id WHERE ct.agent_session_id=?1),0),last_heartbeat_at=?2,version=version+1 WHERE id=?1",
                params![agent_id, now_ms()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn parse_token_usage(last: &Value, payload: &Value) -> Result<TokenUsage, StoreError> {
    let cache_write_present = last.get("cacheWriteInputTokens").is_some();
    let usage = TokenUsage {
        input_tokens: find_u64(last, &["inputTokens"]).unwrap_or_default(),
        cached_input_tokens: find_u64(last, &["cachedInputTokens"]).unwrap_or_default(),
        cache_write_input_tokens: cache_write_present
            .then(|| find_u64(last, &["cacheWriteInputTokens"]).unwrap_or_default()),
        output_tokens: find_u64(last, &["outputTokens"]).unwrap_or_default(),
        reasoning_output_tokens: find_u64(last, &["reasoningOutputTokens"]).unwrap_or_default(),
        total_tokens: find_u64(last, &["totalTokens"]).unwrap_or_default(),
        model_context_window: find_u64(payload, &["tokenUsage", "modelContextWindow"]),
    };
    usage
        .validate()
        .map_err(|error| StoreError::Validation(error.to_string()))?;
    Ok(usage)
}

fn insert_cost(
    connection: &rusqlite::Connection,
    sample_id: &str,
    pricing: &PricingSnapshot,
    cost: &CostEstimate,
) -> Result<(), StoreError> {
    connection.execute(
        "INSERT OR IGNORE INTO pricing_snapshots(id,model,currency,effective_at,input_microusd_per_million,cached_input_microusd_per_million,output_microusd_per_million,cache_write_multiplier_numerator,cache_write_multiplier_denominator,long_context_threshold_tokens,long_context_input_multiplier_numerator,long_context_input_multiplier_denominator,long_context_output_multiplier_numerator,long_context_output_multiplier_denominator,source_label,created_at) VALUES(?1,?2,'USD',0,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'config',?13)",
        params![pricing.id,pricing.model,pricing.input_microusd_per_million as i64,pricing.cached_input_microusd_per_million as i64,pricing.output_microusd_per_million as i64,pricing.cache_write_multiplier_numerator as i64,pricing.cache_write_multiplier_denominator as i64,pricing.long_context_threshold_tokens.map(|v| v as i64),pricing.long_context_input_multiplier_numerator.map(|v| v as i64),pricing.long_context_input_multiplier_denominator.map(|v| v as i64),pricing.long_context_output_multiplier_numerator.map(|v| v as i64),pricing.long_context_output_multiplier_denominator.map(|v| v as i64),now_ms()],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO cost_entries(id,token_sample_id,pricing_snapshot_id,lower_microusd,upper_microusd,confidence,explanation,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![ulid::Ulid::generate().to_string(),sample_id,pricing.id,cost.lower_microusd as i64,cost.upper_microusd as i64,serde_json::to_value(cost.confidence)?.as_str().unwrap_or("unknown"),cost.explanation,now_ms()],
    )?;
    Ok(())
}

fn find_text<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))?
        .as_str()
}

fn find_u64(value: &Value, path: &[&str]) -> Option<u64> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))?
        .as_u64()
}

fn redact_reasoning(
    method: &str,
    payload: &Value,
    retain_raw: bool,
    retain_summaries: bool,
) -> (Value, String) {
    if retain_raw && retain_summaries {
        return (payload.clone(), "none".to_owned());
    }
    let mut result = payload.clone();
    let mut raw_dropped = false;
    let mut summary_dropped = false;
    if !retain_raw && method == "item/reasoning/textDelta" {
        if let Some(object) = result.as_object_mut() {
            object.insert(
                "delta".to_owned(),
                Value::String("[not retained]".to_owned()),
            );
        }
        raw_dropped = true;
    }
    if !retain_summaries && method == "item/reasoning/summaryTextDelta" {
        if let Some(object) = result.as_object_mut() {
            object.insert(
                "delta".to_owned(),
                Value::String("[not retained]".to_owned()),
            );
        }
        summary_dropped = true;
    }
    let reasoning_item = result
        .get("item")
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
        == Some("reasoning");
    if reasoning_item && let Some(object) = result.get_mut("item").and_then(Value::as_object_mut) {
        if !retain_raw {
            object.insert("content".to_owned(), json!([]));
            raw_dropped = true;
        }
        if !retain_summaries {
            object.insert("summary".to_owned(), json!([]));
            summary_dropped = true;
        }
    }
    let redaction_class = match (raw_dropped, summary_dropped) {
        (true, true) => "reasoning_dropped",
        (true, false) => "raw_reasoning_dropped",
        (false, true) => "reasoning_summary_dropped",
        (false, false) => "none",
    };
    (result, redaction_class.to_owned())
}

fn summarize_item(item: &Value) -> Option<String> {
    let kind = item.get("type")?.as_str()?;
    match kind {
        "agentMessage" => item
            .get("text")
            .and_then(Value::as_str)
            .map(|text| truncate(text, 240)),
        "reasoning" => item
            .get("summary")
            .and_then(Value::as_array)
            .and_then(|parts| parts.first())
            .and_then(|part| part.get("text").or(Some(part)))
            .and_then(Value::as_str)
            .map(|text| truncate(text, 240))
            .or_else(|| Some("Reasoning summary updated".to_owned())),
        "commandExecution" => item
            .get("command")
            .and_then(Value::as_str)
            .map(|command| truncate(command, 240)),
        "fileChange" => Some(format!(
            "{} file changes",
            item.get("changes")
                .and_then(Value::as_array)
                .map_or(0, Vec::len)
        )),
        "collabAgentToolCall" => Some(summarize_collaboration(item)),
        "subAgentActivity" => Some(format!(
            "Subagent {}",
            item.get("kind")
                .and_then(Value::as_str)
                .unwrap_or("activity")
        )),
        "contextCompaction" => Some("Context compacted".to_owned()),
        other => Some(other.to_owned()),
    }
}

fn summarize_collaboration(item: &Value) -> String {
    let receivers = item
        .get("receiverThreadIds")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let targets = match receivers {
        0 => "delegated threads".to_owned(),
        1 => "1 delegated thread".to_owned(),
        count => format!("{count} delegated threads"),
    };
    let tool = item.get("tool").and_then(Value::as_str);
    let raw_status = item.get("status").and_then(Value::as_str);
    let action = match (tool, raw_status) {
        (Some("wait"), Some("inProgress")) => "Waiting for delegated threads".to_owned(),
        (Some("wait"), Some("completed")) => "Delegated-thread wait completed".to_owned(),
        (Some("wait"), Some("failed")) => "Delegated-thread wait failed".to_owned(),
        (Some("spawnAgent"), _) => "Started a delegated thread".to_owned(),
        (Some("sendInput"), _) => format!("Sent direction to {targets}"),
        (Some("resumeAgent"), _) => format!("Continued {targets}"),
        (Some("wait"), _) => format!("Waited for {targets}"),
        (Some("closeAgent"), _) => format!("Stopped {targets}"),
        (Some(tool), _) => format!("Collaboration action {tool}"),
        (None, _) => "Collaboration activity".to_owned(),
    };
    let status = match raw_status {
        Some("inProgress") => "in progress",
        Some("completed") => "complete",
        Some("failed") => "failed",
        _ => "updated",
    };
    let prompt = item
        .get("prompt")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(|value| truncate(value.trim(), 120));
    prompt.map_or_else(
        || format!("{action} · {status}"),
        |prompt| format!("{action} · {status} · {prompt}"),
    )
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_owned();
    }
    let mut result = value
        .chars()
        .take(max.saturating_sub(1))
        .collect::<String>();
    result.push('…');
    result
}

#[cfg(test)]
mod tests {
    use super::{ProjectionContext, ProtocolProjection, summarize_collaboration};
    use crate::{NewAgentSession, Store};
    use harness_domain::{AgentRole, AgentSessionId, RunId, SandboxMode};
    use serde_json::json;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn collaboration_summary_uses_human_action_names() {
        assert_eq!(
            summarize_collaboration(&json!({
                "tool": "resumeAgent",
                "status": "completed",
                "receiverThreadIds": ["child-1"],
                "prompt": "Re-check the focused test after the fix"
            })),
            "Continued 1 delegated thread · complete · Re-check the focused test after the fix"
        );
        assert_eq!(
            summarize_collaboration(&json!({
                "tool": "wait",
                "status": "inProgress",
                "receiverThreadIds": []
            })),
            "Waiting for delegated threads · in progress"
        );
    }

    #[test]
    fn stream_deltas_remain_raw_evidence_without_domain_event_fanout() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let projection = ProtocolProjection::new(store.clone(), [], false, true);
        let context = ProjectionContext {
            run_id: None,
            agent_session_id: None,
        };

        projection
            .ingest_notification(
                &context,
                "item/reasoning/textDelta",
                &json!({"threadId": "thread-1", "delta": "consider"}),
            )
            .unwrap();
        projection
            .ingest_notification(
                &context,
                "item/agentMessage/delta",
                &json!({"threadId": "thread-1", "delta": "done"}),
            )
            .unwrap();

        let connection = store.connection().unwrap();
        let raw_count: i64 = connection
            .query_row("SELECT count(*) FROM raw_events", [], |row| row.get(0))
            .unwrap();
        let domain_count: i64 = connection
            .query_row("SELECT count(*) FROM domain_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(raw_count, 2);
        assert_eq!(domain_count, 0);
    }

    #[test]
    fn usage_projection_sums_distinct_model_calls_within_a_turn() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let projection = ProtocolProjection::new(store.clone(), [], false, true);
        let context = ProjectionContext {
            run_id: None,
            agent_session_id: None,
        };
        let first = json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "tokenUsage": {
                "last": {
                    "inputTokens": 100,
                    "cachedInputTokens": 80,
                    "cacheWriteInputTokens": 0,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 4,
                    "totalTokens": 110
                },
                "total": {
                    "inputTokens": 100,
                    "cachedInputTokens": 80,
                    "cacheWriteInputTokens": 0,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 4,
                    "totalTokens": 110
                },
                "modelContextWindow": 258400
            }
        });
        projection
            .ingest_notification(&context, "thread/tokenUsage/updated", &first)
            .unwrap();
        projection
            .ingest_notification(&context, "thread/tokenUsage/updated", &first)
            .unwrap();
        projection
            .ingest_notification(
                &context,
                "thread/tokenUsage/updated",
                &json!({
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "tokenUsage": {
                        "last": {
                            "inputTokens": 150,
                            "cachedInputTokens": 120,
                            "cacheWriteInputTokens": 0,
                            "outputTokens": 20,
                            "reasoningOutputTokens": 6,
                            "totalTokens": 170
                        },
                        "total": {
                            "inputTokens": 250,
                            "cachedInputTokens": 200,
                            "cacheWriteInputTokens": 0,
                            "outputTokens": 30,
                            "reasoningOutputTokens": 10,
                            "totalTokens": 280
                        },
                        "modelContextWindow": 258400
                    }
                }),
            )
            .unwrap();

        let (input, cached, output, reasoning, total, samples):
            (i64, i64, i64, i64, i64, i64) = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,(SELECT count(*) FROM token_samples) FROM token_samples WHERE turn_id='turn-1' AND sample_kind='turn_total'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )
            .unwrap();
        assert_eq!(
            (input, cached, output, reasoning, total),
            (250, 200, 30, 10, 280)
        );
        assert_eq!(samples, 1);
    }

    #[test]
    fn active_agent_summary_exposes_authoritative_turn_usage() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,default_branch,state,created_at,updated_at,version) VALUES('repo','general',1,'repo','/repo','main','READY',1,1,1);
                 INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at,version) VALUES('run','repo','run','goal','plan_and_implement','local_only','INTERVIEWING','interviewing','main','0000000000000000000000000000000000000000','a','p','test',1,1,1);",
            )
            .unwrap();
        let agent_id = AgentSessionId::from("agent");
        let run_id = RunId::from("run");
        store
            .create_agent_session(&NewAgentSession {
                id: agent_id.clone(),
                run_id: run_id.clone(),
                task_attempt_id: None,
                parent_agent_session_id: None,
                runtime_kind: "codex_controller".to_owned(),
                codex_account_id: None,
                role: AgentRole::Interviewer,
                nickname: Some("intent-interviewer".to_owned()),
                requested_model: "gpt-test".to_owned(),
                requested_reasoning_effort: "high".to_owned(),
                sandbox_mode: SandboxMode::ReadOnly,
                approval_policy: "never".to_owned(),
                cwd: PathBuf::from("/repo"),
                state: "STARTING".to_owned(),
                current_goal: Some("goal".to_owned()),
                token_budget: Some(1_000),
            })
            .unwrap();
        store
            .attach_codex_thread(&agent_id, "thread-1", None, "codex", None, None)
            .unwrap();
        store
            .attach_codex_turn(
                &agent_id,
                "thread-1",
                "turn-1",
                Some("gpt-test"),
                Some("high"),
                true,
            )
            .unwrap();
        let projection = ProtocolProjection::new(store.clone(), [], false, true);
        let context = ProjectionContext {
            run_id: Some(run_id),
            agent_session_id: Some(agent_id.clone()),
        };
        projection
            .ingest_notification(
                &context,
                "thread/tokenUsage/updated",
                &json!({
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "tokenUsage": {
                        "last": {
                            "inputTokens": 100,
                            "cachedInputTokens": 80,
                            "cacheWriteInputTokens": 0,
                            "outputTokens": 10,
                            "reasoningOutputTokens": 4,
                            "totalTokens": 110
                        },
                        "total": {
                            "inputTokens": 100,
                            "cachedInputTokens": 80,
                            "cacheWriteInputTokens": 0,
                            "outputTokens": 10,
                            "reasoningOutputTokens": 4,
                            "totalTokens": 110
                        },
                        "modelContextWindow": 258400
                    }
                }),
            )
            .unwrap();

        let active = store.agent(&agent_id).unwrap();
        assert!(active.active_turn_started_at.is_some());
        assert_eq!(active.active_turn_usage.unwrap().total_tokens, 110);

        projection
            .ingest_notification(
                &context,
                "turn/completed",
                &json!({
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1", "status": "completed"}
                }),
            )
            .unwrap();
        let completed = store.agent(&agent_id).unwrap();
        assert!(completed.active_turn_started_at.is_none());
        assert!(completed.active_turn_usage.is_none());
    }

    #[test]
    fn goal_updates_do_not_overwrite_authoritative_usage() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,default_branch,state,created_at,updated_at,version) VALUES('repo','general',1,'repo','/repo','main','READY',1,1,1);
                 INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at,version) VALUES('run','repo','run','goal','plan_and_implement','local_only','EXECUTING','executing','main','0000000000000000000000000000000000000000','a','p','test',1,1,1);
                 INSERT INTO agent_sessions(id,run_id,runtime_kind,role,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state,goal_tokens_used,started_at,last_heartbeat_at,version) VALUES('agent','run','codex_controller','governor','gpt-test','high','workspace_write','never','/repo','RUNNING',777,1,1,1);",
            )
            .unwrap();
        let projection = ProtocolProjection::new(store.clone(), [], false, true);
        let context = ProjectionContext {
            run_id: None,
            agent_session_id: Some("agent".into()),
        };
        projection
            .project_goal(
                &context,
                &json!({
                    "goal": {
                        "objective": "continue",
                        "status": "active",
                        "tokenBudget": 1_000,
                        "tokensUsed": 12
                    }
                }),
            )
            .unwrap();
        let observed: i64 = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT goal_tokens_used FROM agent_sessions WHERE id='agent'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(observed, 777);
    }
}
