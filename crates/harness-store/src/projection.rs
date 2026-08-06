use std::{collections::BTreeMap, sync::Arc};

use harness_domain::{
    AgentSessionId, CostEstimate, DomainEvent, PricingSnapshot, RunId, TokenUsage, now_ms,
};
use harness_usage::estimate;
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

use crate::{RawEventInput, Store, StoreError};

#[derive(Clone)]
pub struct ProtocolProjection {
    store: Store,
    pricing: Arc<BTreeMap<String, PricingSnapshot>>,
    store_raw_reasoning: bool,
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
    ) -> Self {
        Self {
            store,
            pricing: Arc::new(
                pricing
                    .into_iter()
                    .map(|snapshot| (snapshot.model.clone(), snapshot))
                    .collect(),
            ),
            store_raw_reasoning,
        }
    }

    pub fn ingest_notification(
        &self,
        context: &ProjectionContext,
        method: &str,
        payload: &Value,
    ) -> Result<(i64, DomainEvent), StoreError> {
        let (stored_payload, redaction_class) =
            redact_reasoning(method, payload, self.store_raw_reasoning);
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

        let aggregate_id = context
            .agent_session_id
            .as_ref()
            .map(ToString::to_string)
            .or(thread_id)
            .unwrap_or_else(|| "runtime".to_owned());
        let event_type = method.replace('/', ".");
        let event = self.store.emit_domain_event(
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
        Ok((raw_id, event))
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
            self.store.update_agent_state(
                &agent_id,
                "RUNNING",
                summary.as_deref(),
                None,
                None,
                None,
            )?;
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
        self.store.connection()?.execute(
            "UPDATE agent_sessions SET current_goal=?2,goal_status=?3,token_budget=coalesce(?4,token_budget),goal_tokens_used=coalesce(?5,goal_tokens_used),goal_time_used_seconds=coalesce(?6,goal_time_used_seconds),last_heartbeat_at=?7,version=version+1 WHERE id=?1",
            params![agent_id.as_str(),find_text(goal,&["objective"]),find_text(goal,&["status"]),find_u64(goal,&["tokenBudget"]).map(|v| v as i64),find_u64(goal,&["tokensUsed"]).map(|v| v as i64),find_u64(goal,&["timeUsedSeconds"]).map(|v| v as i64),now_ms()],
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
        let (Some(thread_id), Some(turn_id), Some(last)) = (
            find_text(payload, &["threadId"]),
            find_text(payload, &["turnId"]),
            payload.pointer("/tokenUsage/last"),
        ) else {
            return Ok(());
        };
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
        let fallback_model = context
            .agent_session_id
            .as_ref()
            .and_then(|agent| self.store.agent(agent).ok())
            .map(|agent| agent.effective_model.unwrap_or(agent.requested_model));
        let connection = self.store.connection()?;
        let model: Option<String> = connection
            .query_row(
                "SELECT coalesce(effective_model,requested_model) FROM codex_turns WHERE turn_id=?1",
                [turn_id],
                |row| row.get(0),
            )
            .optional()?;
        let model = model
            .or(fallback_model)
            .unwrap_or_else(|| "unknown".to_owned());
        let existing: Option<(String, i64)> = connection
            .query_row(
                "SELECT id,total_tokens FROM token_samples WHERE turn_id=?1 AND sample_kind='last_turn'",
                [turn_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let prior_total = existing.as_ref().map_or(0, |(_, total)| *total);
        let sample_id = existing
            .map(|(sample_id, _)| sample_id)
            .unwrap_or_else(|| ulid::Ulid::generate().to_string());
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO token_samples(id,thread_id,turn_id,effective_model,observed_at,input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,model_context_window,sample_kind,source_event_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'last_turn',?13) ON CONFLICT(turn_id,sample_kind) WHERE turn_id IS NOT NULL DO UPDATE SET effective_model=excluded.effective_model,observed_at=excluded.observed_at,input_tokens=excluded.input_tokens,cached_input_tokens=excluded.cached_input_tokens,cache_write_input_tokens=excluded.cache_write_input_tokens,output_tokens=excluded.output_tokens,reasoning_output_tokens=excluded.reasoning_output_tokens,total_tokens=excluded.total_tokens,model_context_window=excluded.model_context_window,source_event_id=excluded.source_event_id",
            params![sample_id,thread_id,turn_id,model,now_ms(),usage.input_tokens as i64,usage.cached_input_tokens as i64,usage.cache_write_input_tokens.map(|v| v as i64),usage.output_tokens as i64,usage.reasoning_output_tokens as i64,usage.total_tokens as i64,usage.model_context_window.map(|v| v as i64),raw_id],
        )?;
        transaction.execute(
            "DELETE FROM cost_entries WHERE token_sample_id=?1",
            [&sample_id],
        )?;
        if let Some(pricing) = self.pricing.get(&model) {
            let cost = estimate(&usage, pricing)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            insert_cost(&transaction, &sample_id, pricing, &cost)?;
        }
        if let Some(agent_id) = context.agent_session_id.as_ref() {
            let delta = (usage.total_tokens as i128 - i128::from(prior_total))
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
            transaction.execute(
                "UPDATE agent_sessions SET goal_tokens_used=max(0,coalesce(goal_tokens_used,0)+?2),last_heartbeat_at=?3,version=version+1 WHERE id=?1",
                params![agent_id.as_str(),delta,now_ms()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
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

fn redact_reasoning(method: &str, payload: &Value, retain: bool) -> (Value, String) {
    if retain {
        return (payload.clone(), "none".to_owned());
    }
    let mut result = payload.clone();
    if method == "item/reasoning/textDelta" {
        if let Some(object) = result.as_object_mut() {
            object.insert(
                "delta".to_owned(),
                Value::String("[not retained]".to_owned()),
            );
        }
        return (result, "raw_reasoning_dropped".to_owned());
    }
    if result
        .get("item")
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
        == Some("reasoning")
    {
        if let Some(object) = result.get_mut("item").and_then(Value::as_object_mut) {
            object.insert("content".to_owned(), json!([]));
        }
        return (result, "raw_reasoning_dropped".to_owned());
    }
    (result, "none".to_owned())
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
        "collabAgentToolCall" => Some(format!(
            "Subagent {}",
            item.get("tool")
                .and_then(Value::as_str)
                .unwrap_or("activity")
        )),
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
