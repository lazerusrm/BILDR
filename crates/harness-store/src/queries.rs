use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use harness_domain::{
    ActivityItem, AgentSessionId, AgentSummary, ApprovalId, ApprovalSummary, ArtifactId, AttemptId,
    CostConfidence, CostEstimate, DomainEvent, LatestAgentMessage, ModelUsageSummary,
    PlanRevisionId, RepositoryId, RepositorySummary, RunId, RunPlan, RunState, RunSummary, TaskId,
    TaskState, TaskSummary, TokenUsage, UsageBreakdown, UsageGroup, UsageSummary, WorktreeId,
    WorktreeSummary, format_timestamp, now_ms,
};
use rusqlite::{OptionalExtension, Row, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    ArtifactRecord, NativeSubagentActivityRecord, NewAgentSession, NewApproval, NewArtifact,
    NewCommandRecord, NewContextPacket, NewEvidenceRecord, NewRepository, NewRun, NewTaskAttempt,
    NewValidationRecord, NewWorktree, PriorAttemptContext, RawEventInput, RepositoryHealthInput,
    Store, StoreError, StoredSession,
};

impl Store {
    pub fn put_runtime_metadata(&self, key: &str, value: &Value) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO runtime_metadata(key,value_json,updated_at) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            params![key, serde_json::to_string(value)?, now_ms()],
        )?;
        Ok(())
    }

    pub fn runtime_metadata(&self, key: &str) -> Result<Option<Value>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT value_json FROM runtime_metadata WHERE key=?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    pub fn delete_runtime_metadata(&self, key: &str) -> Result<(), StoreError> {
        self.connection()?
            .execute("DELETE FROM runtime_metadata WHERE key=?1", [key])?;
        Ok(())
    }

    pub fn governor_token_samples(
        &self,
        limit: u32,
        model: &str,
        reasoning_effort: &str,
        owner_profile: Option<&str>,
    ) -> Result<Vec<u64>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT a.goal_tokens_used FROM agent_sessions a JOIN task_attempts ta ON ta.id=a.task_attempt_id JOIN tasks t ON t.id=ta.task_id WHERE a.role='governor' AND a.parent_agent_session_id IS NULL AND a.completed_at IS NOT NULL AND a.goal_tokens_used > 0 AND coalesce(a.failure_class,'') NOT IN ('infrastructure_unavailable','authentication_rejected') AND coalesce(a.effective_model,a.requested_model)=?1 AND coalesce(a.effective_reasoning_effort,a.requested_reasoning_effort)=?2 AND (?3 IS NULL OR t.owner_profile=?3) ORDER BY a.completed_at DESC LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![
                model,
                reasoning_effort,
                owner_profile,
                i64::from(limit.clamp(1, 100)),
            ],
            |row| row.get::<_, i64>(0),
        )?;
        rows.map(|row| {
            row.map(|value| u64::try_from(value).unwrap_or_default())
                .map_err(StoreError::from)
        })
        .collect()
    }

    /// Counts the task's root governors and their delegated descendants across
    /// attempts, excluding independent verifier trees.
    pub fn task_governor_usage(&self, task_id: &TaskId) -> Result<u64, StoreError> {
        let connection = self.connection()?;
        let total = connection.query_row(
            "WITH RECURSIVE governor_tree(id) AS (
                SELECT a.id
                FROM agent_sessions a
                JOIN task_attempts ta ON ta.id=a.task_attempt_id
                WHERE ta.task_id=?1
                  AND a.role='governor'
                  AND a.parent_agent_session_id IS NULL
                UNION ALL
                SELECT child.id
                FROM agent_sessions child
                JOIN governor_tree parent ON child.parent_agent_session_id=parent.id
             )
             SELECT coalesce(sum(a.goal_tokens_used),0)
             FROM agent_sessions a
             JOIN governor_tree tree ON tree.id=a.id",
            [task_id.as_str()],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(u64::try_from(total).unwrap_or_default())
    }

    pub fn agent_goal_status(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<String>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT goal_status FROM agent_sessions WHERE id=?1",
                [agent_id.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    pub fn record_handoff(
        &self,
        attempt_id: &AttemptId,
        agent_id: &AgentSessionId,
        handoff: &Value,
        schema_valid: bool,
    ) -> Result<(), StoreError> {
        let raw = serde_json::to_string(handoff)?;
        let digest = sha256(raw.as_bytes());
        self.connection()?.execute(
            "INSERT INTO handoffs(id,task_attempt_id,agent_session_id,handoff_json,handoff_sha256,schema_valid,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(task_attempt_id) DO UPDATE SET agent_session_id=excluded.agent_session_id,handoff_json=excluded.handoff_json,handoff_sha256=excluded.handoff_sha256,schema_valid=excluded.schema_valid,created_at=excluded.created_at",
            params![
                format!("handoff-{}", attempt_id.as_str()),
                attempt_id.as_str(),
                agent_id.as_str(),
                raw,
                digest,
                schema_valid,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn attempt_handoff(&self, attempt_id: &AttemptId) -> Result<Option<String>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT handoff_json FROM handoffs WHERE task_attempt_id=?1",
                [attempt_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn recent_task_handoffs(
        &self,
        task_id: &TaskId,
        limit: u32,
    ) -> Result<Vec<String>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT h.handoff_json FROM handoffs h JOIN task_attempts a ON a.id=h.task_attempt_id WHERE a.task_id=?1 AND h.schema_valid=1 ORDER BY a.attempt_number DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![task_id.as_str(), i64::from(limit.clamp(1, 20))],
            |row| row.get::<_, String>(0),
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_repository(
        &self,
        input: &NewRepository,
    ) -> Result<RepositorySummary, StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,origin_url,default_branch,expected_coordination_branch,state,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10,1)",
            params![
                input.id.as_str(),
                input.profile_id,
                input.profile_version,
                input.display_name,
                input.root_path.to_string_lossy(),
                input.origin_url,
                input.default_branch,
                input.expected_coordination_branch,
                input.state,
                now,
            ],
        )?;
        self.repository(&input.id)
    }

    pub fn replace_repository_checkout(
        &self,
        repository_id: &RepositoryId,
        expected_root: &Path,
        replacement_root: &Path,
        origin_url: Option<&str>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE repositories SET root_path=?3,origin_url=?4,updated_at=?5,version=version+1 WHERE id=?1 AND root_path=?2",
            params![
                repository_id.as_str(),
                expected_root.to_string_lossy(),
                replacement_root.to_string_lossy(),
                origin_url,
                now_ms(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "repository {repository_id} checkout changed while preparing its replacement"
            )));
        }
        Ok(())
    }

    pub fn record_repository_health(
        &self,
        input: &RepositoryHealthInput,
    ) -> Result<(), StoreError> {
        let id = ulid::Ulid::generate().to_string();
        let now = now_ms();
        let blockers = serde_json::to_string(&input.blockers)?;
        let details = serde_json::to_string(&input.details)?;
        let state = if input.blockers.is_empty() {
            "READY"
        } else {
            "BLOCKED"
        };
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO repository_health_snapshots(id,repository_id,observed_at,primary_branch,primary_head_sha,primary_clean,origin_head_sha,git_identity_name_present,git_identity_email_present,authority_digest,blockers_json,details_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                id,
                input.repository_id.as_str(),
                now,
                input.primary_branch,
                input.primary_head_sha,
                input.primary_clean,
                input.origin_head_sha,
                input.git_identity_name_present,
                input.git_identity_email_present,
                input.authority_digest,
                blockers,
                details,
            ],
        )?;
        transaction.execute(
            "UPDATE repositories SET state=?2,updated_at=?3,version=version+1 WHERE id=?1",
            params![input.repository_id.as_str(), state, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn repository(&self, id: &RepositoryId) -> Result<RepositorySummary, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(repository_select(true), [id.as_str()], map_repository)
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("repository {id}")))
    }

    pub fn list_repositories(&self) -> Result<Vec<RepositorySummary>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(repository_select(false))?;
        let rows = statement.query_map([], map_repository)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_run(&self, input: &NewRun) -> Result<RunSummary, StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,codex_version,protocol_schema_sha256,requested_by,run_token_budget,created_at,updated_at,started_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?17,?17,1)",
            params![
                input.id.as_str(),
                input.repository_id.as_str(),
                input.title,
                input.objective,
                input.mode,
                input.publication_mode,
                input.state,
                input.phase,
                input.base_ref,
                input.base_sha,
                input.authority_digest,
                input.profile_digest,
                input.codex_version,
                input.protocol_schema_sha256,
                input.requested_by,
                input.token_budget.map(|value| value as i64),
                now,
            ],
        )?;
        self.run(&input.id)
    }

    pub fn run(&self, id: &RunId) -> Result<RunSummary, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                &format!("{} WHERE r.id=?1", run_select()),
                [id.as_str()],
                map_run,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("run {id}")))
    }

    pub fn list_runs(
        &self,
        repository_id: Option<&RepositoryId>,
        include_terminal: bool,
    ) -> Result<Vec<RunSummary>, StoreError> {
        let connection = self.connection()?;
        let terminal_filter = if include_terminal {
            ""
        } else {
            " AND r.state NOT IN ('COMPLETED','CANCELED','FAILED','ARCHIVED')"
        };
        let sql = if repository_id.is_some() {
            format!(
                "{} WHERE r.repository_id=?1{} ORDER BY r.created_at DESC",
                run_select(),
                terminal_filter
            )
        } else {
            format!(
                "{} WHERE 1=1{} ORDER BY r.created_at DESC",
                run_select(),
                terminal_filter
            )
        };
        let mut statement = connection.prepare(&sql)?;
        let rows = if let Some(repository_id) = repository_id {
            statement.query_map([repository_id.as_str()], map_run)?
        } else {
            statement.query_map([], map_run)?
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn transition_run(
        &self,
        id: &RunId,
        next: RunState,
        phase: &str,
        expected_version: Option<u64>,
        failure: Option<(&str, &str)>,
    ) -> Result<RunSummary, StoreError> {
        let current = self.run(id)?;
        if expected_version.is_some_and(|version| version != current.version) {
            return Err(StoreError::Conflict(format!(
                "run {id} is version {}, not {}",
                current.version,
                expected_version.unwrap_or_default()
            )));
        }
        if !current.state.can_transition_to(next) {
            return Err(StoreError::Conflict(format!(
                "illegal run transition {} -> {}",
                current.state, next
            )));
        }
        let now = now_ms();
        let completed = next.is_terminal().then_some(now);
        let (failure_class, failure_reason) = failure.unzip();
        let current_version = sqlite_version(current.version)?;
        let changed = self.connection()?.execute(
            "UPDATE runs SET state=?2,phase=?3,updated_at=?4,completed_at=coalesce(?5,completed_at),failure_class=?6,failure_reason=?7,version=version+1 WHERE id=?1 AND version=?8",
            params![
                id.as_str(),
                next.to_string(),
                phase,
                now,
                completed,
                failure_class,
                failure_reason,
                current_version,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "run {id} changed concurrently"
            )));
        }
        self.run(id)
    }

    pub fn set_scheduler_paused(&self, id: &RunId, paused: bool) -> Result<RunSummary, StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE runs SET scheduler_paused=?2,updated_at=?3,version=version+1 WHERE id=?1",
            params![id.as_str(), paused, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("run {id}")));
        }
        self.run(id)
    }

    pub fn set_run_token_budget_and_resume(
        &self,
        id: &RunId,
        token_budget: u64,
    ) -> Result<RunSummary, StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE runs SET run_token_budget=?2,scheduler_paused=0,updated_at=?3,version=version+1 WHERE id=?1",
            params![id.as_str(), token_budget as i64, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("run {id}")));
        }
        self.run(id)
    }

    pub fn set_run_integration(
        &self,
        id: &RunId,
        branch: &str,
        sha: &str,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE runs SET integration_branch=?2,integration_sha=?3,updated_at=?4,version=version+1 WHERE id=?1",
            params![id.as_str(), branch, sha, now_ms()],
        )?;
        Ok(())
    }

    pub fn clear_run_integration(&self, id: &RunId) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE runs SET integration_branch=NULL,integration_sha=NULL,updated_at=?2,version=version+1 WHERE id=?1",
            params![id.as_str(), now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("run {id}")));
        }
        Ok(())
    }

    pub fn create_worktree(&self, input: &NewWorktree) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO worktrees(id,run_id,task_attempt_id,kind,path,branch,base_sha,head_sha,state,created_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.kind,
                input.path.to_string_lossy(),
                input.branch,
                input.base_sha,
                input.head_sha,
                input.state,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn update_worktree(
        &self,
        id: &WorktreeId,
        state: &str,
        head_sha: Option<&str>,
        preserved_reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE worktrees SET state=?2,head_sha=coalesce(?3,head_sha),preserved_reason=coalesce(?4,preserved_reason),reconciled_at=?5,version=version+1 WHERE id=?1",
            params![id.as_str(), state, head_sha, preserved_reason, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("worktree {id}")));
        }
        Ok(())
    }

    pub fn mark_worktree_removed(&self, id: &WorktreeId) -> Result<(), StoreError> {
        let timestamp = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE worktrees SET state='REMOVED',removed_at=coalesce(removed_at,?2),reconciled_at=?2,version=version+1 WHERE id=?1",
            params![id.as_str(), timestamp],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("worktree {id}")));
        }
        Ok(())
    }

    pub fn worktree_has_active_path_lease(&self, id: &WorktreeId) -> Result<bool, StoreError> {
        self.connection()?
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM worktrees w JOIN path_leases p ON p.task_attempt_id=w.task_attempt_id WHERE w.id=?1 AND p.released_at IS NULL AND p.expires_at>?2)",
                params![id.as_str(), now_ms()],
                |row| row.get::<_, bool>(0),
            )
            .map_err(Into::into)
    }

    pub fn set_worktree_composed_base(
        &self,
        id: &WorktreeId,
        base_sha: &str,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE worktrees SET base_sha=?2,head_sha=?2,state='ACTIVE',reconciled_at=?3,version=version+1 WHERE id=?1",
            params![id.as_str(), base_sha, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("worktree {id}")));
        }
        Ok(())
    }

    pub fn list_worktrees(
        &self,
        run_id: Option<&RunId>,
    ) -> Result<Vec<WorktreeSummary>, StoreError> {
        let connection = self.connection()?;
        let base = "SELECT w.id,w.run_id,t.id,w.kind,w.path,w.branch,w.base_sha,w.head_sha,w.state,w.preserved_reason,coalesce(tr.diff_files,0),coalesce(tr.diff_additions,0),coalesce(tr.diff_deletions,0),w.version FROM worktrees w LEFT JOIN task_attempts a ON a.id=w.task_attempt_id LEFT JOIN tasks t ON t.id=a.task_id LEFT JOIN task_results tr ON tr.task_attempt_id=a.id";
        let sql = if run_id.is_some() {
            format!("{base} WHERE w.run_id=?1 ORDER BY w.created_at DESC")
        } else {
            format!("{base} ORDER BY w.created_at DESC")
        };
        let mut statement = connection.prepare(&sql)?;
        let rows = if let Some(run_id) = run_id {
            statement.query_map([run_id.as_str()], map_worktree)?
        } else {
            statement.query_map([], map_worktree)?
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_agent_session(&self, input: &NewAgentSession) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO agent_sessions(id,run_id,task_attempt_id,parent_agent_session_id,runtime_kind,codex_account_id,role,nickname,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state,current_goal,goal_status,token_budget,started_at,last_heartbeat_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,'active',?16,?17,?17,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.parent_agent_session_id.as_ref().map(AgentSessionId::as_str),
                input.runtime_kind,
                input.codex_account_id,
                enum_text(&input.role)?,
                input.nickname,
                input.requested_model,
                input.requested_reasoning_effort,
                enum_text(&input.sandbox_mode)?,
                input.approval_policy,
                input.cwd.to_string_lossy(),
                input.state,
                input.current_goal,
                input.token_budget.map(|value| value as i64),
                now_ms(),
            ],
        )?;
        self.connection()?.execute(
            "INSERT INTO agent_runtime_details(agent_session_id,updated_at) VALUES(?1,?2)",
            params![input.id.as_str(), now_ms()],
        )?;
        Ok(())
    }

    pub fn set_agent_context_strategy(
        &self,
        agent_id: &AgentSessionId,
        strategy: &str,
        source_attempt_id: Option<&AttemptId>,
        reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE agent_runtime_details SET context_strategy=?2,context_source_attempt_id=?3,context_reuse_reason=?4,updated_at=?5 WHERE agent_session_id=?1",
            params![
                agent_id.as_str(),
                strategy,
                source_attempt_id.map(AttemptId::as_str),
                reason,
                now_ms(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("agent {agent_id}")));
        }
        Ok(())
    }

    pub fn prepare_agent_continuation(
        &self,
        agent_id: &AgentSessionId,
        token_budget: u64,
        current_action: &str,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "UPDATE agent_sessions SET token_budget=?2,state='STARTING',completed_at=NULL,failure_class=NULL,failure_reason=NULL,last_heartbeat_at=?3,version=version+1 WHERE id=?1",
            params![agent_id.as_str(), token_budget as i64, now],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("agent {agent_id}")));
        }
        transaction.execute(
            "UPDATE agent_runtime_details SET active_turn_id=NULL,current_action=?2,last_activity_kind='turn',last_activity_at=?3,updated_at=?3 WHERE agent_session_id=?1",
            params![agent_id.as_str(), current_action, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn attach_codex_thread(
        &self,
        agent_id: &AgentSessionId,
        thread_id: &str,
        parent_thread_id: Option<&str>,
        service_name: &str,
        branch: Option<&str>,
        sha: Option<&str>,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO codex_threads(thread_id,agent_session_id,parent_thread_id,service_name,git_branch,git_sha,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?7) ON CONFLICT(thread_id) DO UPDATE SET agent_session_id=excluded.agent_session_id,parent_thread_id=excluded.parent_thread_id,updated_at=excluded.updated_at",
            params![thread_id, agent_id.as_str(), parent_thread_id, service_name, branch, sha, now],
        )?;
        Ok(())
    }

    pub fn attach_codex_turn(
        &self,
        agent_id: &AgentSessionId,
        thread_id: &str,
        turn_id: &str,
        requested_model: Option<&str>,
        requested_effort: Option<&str>,
        authoritative_notification: bool,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO codex_turns(turn_id,thread_id,status,requested_model,requested_reasoning_effort,started_at,version) VALUES(?1,?2,'inProgress',?3,?4,?5,1) ON CONFLICT(turn_id) DO UPDATE SET status='inProgress',started_at=coalesce(started_at,excluded.started_at),version=version+1",
            params![turn_id, thread_id, requested_model, requested_effort, now],
        )?;
        transaction.execute(
            "UPDATE agent_runtime_details SET active_turn_id=CASE WHEN ?3 THEN ?2 ELSE coalesce(active_turn_id,?2) END,last_activity_kind='turn',last_activity_at=?4,updated_at=?4 WHERE agent_session_id=?1",
            params![agent_id.as_str(), turn_id, authoritative_notification, now],
        )?;
        transaction.execute(
            "UPDATE agent_sessions SET state='RUNNING',last_heartbeat_at=?2,version=version+1 WHERE id=?1",
            params![agent_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn update_agent_state(
        &self,
        id: &AgentSessionId,
        state: &str,
        current_action: Option<&str>,
        effective_model: Option<&str>,
        effective_effort: Option<&str>,
        terminal_failure: Option<(&str, &str)>,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let (failure_class, failure_reason) = terminal_failure.unzip();
        let terminal = matches!(state, "COMPLETED" | "FAILED" | "INTERRUPTED" | "CANCELED");
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "UPDATE agent_sessions SET state=?2,effective_model=coalesce(?3,effective_model),effective_reasoning_effort=coalesce(?4,effective_reasoning_effort),last_heartbeat_at=?5,completed_at=CASE WHEN ?6 THEN ?5 ELSE completed_at END,failure_class=?7,failure_reason=?8,version=version+1 WHERE id=?1",
            params![id.as_str(), state, effective_model, effective_effort, now, terminal, failure_class, failure_reason],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("agent {id}")));
        }
        transaction.execute(
            "UPDATE agent_runtime_details SET current_action=coalesce(?2,current_action),last_activity_at=?3,updated_at=?3 WHERE agent_session_id=?1",
            params![id.as_str(), current_action, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn clear_agent_active_turn(&self, id: &AgentSessionId) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE agent_runtime_details SET active_turn_id=NULL,last_activity_kind='runtime',last_activity_at=?2,updated_at=?2 WHERE agent_session_id=?1",
            params![id.as_str(), now_ms()],
        )?;
        Ok(())
    }

    pub fn agent_by_thread(&self, thread_id: &str) -> Result<Option<AgentSessionId>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT agent_session_id FROM codex_threads WHERE thread_id=?1",
                [thread_id],
                |row| row.get::<_, String>(0).map(AgentSessionId::from),
            )
            .optional()?)
    }

    pub fn native_subagent_activities(
        &self,
    ) -> Result<Vec<NativeSubagentActivityRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT re.agent_session_id,re.thread_id,pi.payload_json FROM projected_items pi JOIN raw_events re ON re.id=pi.source_raw_event_id WHERE pi.item_type='subAgentActivity' AND pi.summary='Subagent started' AND re.agent_session_id IS NOT NULL AND re.thread_id IS NOT NULL ORDER BY re.id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (parent_agent_session_id, parent_thread_id, payload) = row?;
            Ok(NativeSubagentActivityRecord {
                parent_agent_session_id: AgentSessionId::from(parent_agent_session_id),
                parent_thread_id,
                payload: serde_json::from_str(&payload)?,
            })
        })
        .collect()
    }

    pub fn latest_thread_turn_status(&self, thread_id: &str) -> Result<Option<String>, StoreError> {
        let event = self
            .connection()?
            .query_row(
                "SELECT method,payload_json FROM raw_events WHERE thread_id=?1 AND method IN ('turn/started','turn/completed') ORDER BY id DESC LIMIT 1",
                [thread_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        event
            .map(|(method, payload)| {
                if method == "turn/started" {
                    return Ok(None);
                }
                let payload: Value = serde_json::from_str(&payload)?;
                Ok(Some(
                    payload
                        .pointer("/turn/status")
                        .and_then(Value::as_str)
                        .unwrap_or("failed")
                        .to_owned(),
                ))
            })
            .transpose()
            .map(Option::flatten)
    }

    pub fn agent(&self, id: &AgentSessionId) -> Result<AgentSummary, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                &format!("{} WHERE a.id=?1", agent_select()),
                [id.as_str()],
                map_agent,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("agent {id}")))
    }

    pub fn list_agents(&self, run_id: &RunId) -> Result<Vec<AgentSummary>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "{} WHERE a.run_id=?1 ORDER BY a.started_at,a.id",
            agent_select()
        ))?;
        let rows = statement.query_map([run_id.as_str()], map_agent)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn store_plan(
        &self,
        run_id: &RunId,
        architect_agent_id: &AgentSessionId,
        plan: &RunPlan,
    ) -> Result<PlanRevisionId, StoreError> {
        let plan_json = serde_json::to_string(plan)?;
        let plan_sha = sha256(plan_json.as_bytes());
        let revision_id = PlanRevisionId::new();
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let revision: i64 = transaction.query_row(
            "SELECT coalesce(max(revision),0)+1 FROM run_plan_revisions WHERE run_id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "DELETE FROM tasks WHERE run_id=?1 AND plan_revision_id IN (SELECT id FROM run_plan_revisions WHERE run_id=?1 AND state IN ('PROPOSED','CERTIFIED','REVISION_REQUIRED'))",
            [run_id.as_str()],
        )?;
        transaction.execute(
            "UPDATE run_plan_revisions SET state='SUPERSEDED' WHERE run_id=?1 AND state IN ('PROPOSED','CERTIFIED','REVISION_REQUIRED')",
            [run_id.as_str()],
        )?;
        transaction.execute(
            "INSERT INTO run_plan_revisions(id,run_id,revision,architect_agent_session_id,plan_json,plan_sha256,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,'PROPOSED',?7)",
            params![revision_id.as_str(),run_id.as_str(),revision,architect_agent_id.as_str(),plan_json,plan_sha,now],
        )?;

        let mut ids = BTreeMap::new();
        for packet in &plan.tasks {
            let task_id = TaskId::new();
            ids.insert(packet.task_id.clone(), task_id.clone());
            transaction.execute(
                "INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'PROPOSED',?10,?10,1)",
                params![task_id.as_str(),run_id.as_str(),revision_id.as_str(),packet.task_id,packet.title,packet.objective,packet.priority,packet.owner_profile,packet.reviewer_profile,now],
            )?;
        }
        for packet in &plan.tasks {
            let task_id = ids.get(&packet.task_id).ok_or_else(|| {
                StoreError::Validation(format!(
                    "task {} disappeared while storing the plan",
                    packet.task_id
                ))
            })?;
            for external_dependency in &packet.depends_on {
                let dependency_id = ids.get(external_dependency).ok_or_else(|| {
                    StoreError::Validation(format!(
                        "task {} depends on missing task {external_dependency}",
                        packet.task_id
                    ))
                })?;
                transaction.execute(
                    "INSERT INTO task_dependencies(task_id,depends_on_task_id,expected_dependency_sha) VALUES(?1,?2,?3)",
                    params![task_id.as_str(),dependency_id.as_str(),packet.dependency_shas.get(external_dependency)],
                )?;
            }
        }
        transaction.commit()?;
        Ok(revision_id)
    }

    pub fn latest_plan(
        &self,
        run_id: &RunId,
    ) -> Result<Option<(PlanRevisionId, RunPlan, String, u64)>, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id,plan_json,state,revision FROM run_plan_revisions WHERE run_id=?1 ORDER BY revision DESC LIMIT 1",
                [run_id.as_str()],
                |row| {
                    let json: String = row.get(1)?;
                    let plan: RunPlan = serde_json::from_str(&json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            json.len(),
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok((
                        PlanRevisionId::from(row.get::<_, String>(0)?),
                        plan,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn approve_latest_plan(&self, run_id: &RunId, actor: &str) -> Result<(), StoreError> {
        let Some((revision_id, _, state, _)) = self.latest_plan(run_id)? else {
            return Err(StoreError::NotFound(format!("plan for run {run_id}")));
        };
        if state != "CERTIFIED" {
            return Err(StoreError::Conflict(format!(
                "plan is {state}, not CERTIFIED"
            )));
        }
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE run_plan_revisions SET state='APPROVED',approved_at=?2,approved_by=?3 WHERE id=?1",
            params![revision_id.as_str(), now, actor],
        )?;
        transaction.execute(
            "UPDATE tasks SET state=CASE WHEN EXISTS(SELECT 1 FROM task_dependencies d WHERE d.task_id=tasks.id) THEN 'WAITING_DEPENDENCY' ELSE 'READY' END,updated_at=?2,version=version+1 WHERE plan_revision_id=?1",
            params![revision_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn certify_latest_plan(&self, run_id: &RunId) -> Result<(), StoreError> {
        self.transition_latest_plan_state(run_id, "PROPOSED", "CERTIFIED")
    }

    pub fn mark_latest_plan_revision_required(&self, run_id: &RunId) -> Result<(), StoreError> {
        self.transition_latest_plan_state(run_id, "PROPOSED", "REVISION_REQUIRED")
    }

    pub fn request_latest_plan_revision(&self, run_id: &RunId) -> Result<(), StoreError> {
        let Some((_, _, state, _)) = self.latest_plan(run_id)? else {
            return Err(StoreError::NotFound(format!("plan for run {run_id}")));
        };
        match state.as_str() {
            "CERTIFIED" => {
                self.transition_latest_plan_state(run_id, "CERTIFIED", "REVISION_REQUIRED")
            }
            "REVISION_REQUIRED" => Ok(()),
            _ => Err(StoreError::Conflict(format!(
                "plan is {state}, not CERTIFIED or REVISION_REQUIRED"
            ))),
        }
    }

    pub fn reopen_latest_plan_for_review(&self, run_id: &RunId) -> Result<(), StoreError> {
        self.transition_latest_plan_state(run_id, "CERTIFIED", "PROPOSED")
    }

    fn transition_latest_plan_state(
        &self,
        run_id: &RunId,
        expected: &str,
        next: &str,
    ) -> Result<(), StoreError> {
        let Some((revision_id, _, state, _)) = self.latest_plan(run_id)? else {
            return Err(StoreError::NotFound(format!("plan for run {run_id}")));
        };
        if state != expected {
            return Err(StoreError::Conflict(format!(
                "plan is {state}, not {expected}"
            )));
        }
        let changed = self.connection()?.execute(
            "UPDATE run_plan_revisions SET state=?2 WHERE id=?1 AND state=?3",
            params![revision_id.as_str(), next, expected],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "plan changed during state transition".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn list_tasks(&self, run_id: &RunId) -> Result<Vec<TaskSummary>, StoreError> {
        let connection = self.connection()?;
        let sql = "SELECT t.id,t.run_id,t.external_task_id,t.title,t.objective,t.state,t.priority,t.owner_profile,t.reviewer_profile,t.current_attempt_number,coalesce(a.base_sha,r.base_sha),coalesce(a.head_sha,tr.verified_commit_sha),a.token_budget,t.version,(SELECT json_group_array(dt.external_task_id) FROM task_dependencies d JOIN tasks dt ON dt.id=d.depends_on_task_id WHERE d.task_id=t.id) FROM tasks t JOIN runs r ON r.id=t.run_id LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number LEFT JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.run_id=?1 ORDER BY CASE t.priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,t.created_at";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map([run_id.as_str()], map_task)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn task(&self, id: &TaskId) -> Result<TaskSummary, StoreError> {
        let connection = self.connection()?;
        let sql = "SELECT t.id,t.run_id,t.external_task_id,t.title,t.objective,t.state,t.priority,t.owner_profile,t.reviewer_profile,t.current_attempt_number,coalesce(a.base_sha,r.base_sha),coalesce(a.head_sha,tr.verified_commit_sha),a.token_budget,t.version,(SELECT json_group_array(dt.external_task_id) FROM task_dependencies d JOIN tasks dt ON dt.id=d.depends_on_task_id WHERE d.task_id=t.id) FROM tasks t JOIN runs r ON r.id=t.run_id LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number LEFT JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.id=?1";
        connection
            .query_row(sql, [id.as_str()], map_task)
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("task {id}")))
    }

    pub fn task_packet(
        &self,
        id: &TaskId,
    ) -> Result<Option<(AttemptId, harness_domain::TaskPacket)>, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT a.id,a.task_packet_json FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.id=?1 ORDER BY a.attempt_number DESC LIMIT 1",
                [id.as_str()],
                |row| {
                    let raw: String = row.get(1)?;
                    let packet = serde_json::from_str(&raw).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(raw.len(), rusqlite::types::Type::Text, Box::new(error))
                    })?;
                    Ok((AttemptId::from(row.get::<_, String>(0)?), packet))
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_attempt_context(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<PriorAttemptContext>, StoreError> {
        let connection = self.connection()?;
        let attempt = connection
            .query_row(
                "SELECT id,attempt_number,state,terminal_class,failure_reason FROM task_attempts WHERE task_id=?1 ORDER BY attempt_number DESC LIMIT 1",
                [task_id.as_str()],
                |row| {
                    Ok((
                        AttemptId::from(row.get::<_, String>(0)?),
                        row.get::<_, i64>(1)? as u32,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((attempt_id, attempt_number, state, terminal_class, failure_reason)) = attempt
        else {
            return Ok(None);
        };
        let worktree_path = connection
            .query_row(
                "SELECT path FROM worktrees WHERE task_attempt_id=?1 ORDER BY created_at DESC LIMIT 1",
                [attempt_id.as_str()],
                |row| row.get::<_, String>(0).map(PathBuf::from),
            )
            .optional()?;
        let agent = connection
            .query_row(
                "SELECT id,role,requested_model,effective_model,requested_reasoning_effort,effective_reasoning_effort,coalesce(goal_tokens_used,0) FROM agent_sessions WHERE task_attempt_id=?1 AND parent_agent_session_id IS NULL AND role IN ('governor','worker','high_risk_worker') ORDER BY started_at DESC,id DESC LIMIT 1",
                [attempt_id.as_str()],
                |row| {
                    Ok((
                        AgentSessionId::from(row.get::<_, String>(0)?),
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)? as u64,
                    ))
                },
            )
            .optional()?;
        let verifier_verdict = connection
            .query_row(
                "SELECT verifier_verdict FROM task_results WHERE task_attempt_id=?1",
                [attempt_id.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        drop(connection);
        let last_agent_message = if let Some((agent_id, ..)) = agent.as_ref() {
            self.latest_agent_message(agent_id)?
                .map(|message| message.text)
        } else {
            None
        };
        let (
            agent_id,
            role,
            requested_model,
            effective_model,
            requested_reasoning_effort,
            effective_reasoning_effort,
            tokens_used,
        ) = agent.map_or((None, None, None, None, None, None, 0), |agent| {
            (
                Some(agent.0),
                Some(agent.1),
                Some(agent.2),
                agent.3,
                Some(agent.4),
                agent.5,
                agent.6,
            )
        });
        Ok(Some(PriorAttemptContext {
            attempt_id,
            attempt_number,
            state,
            terminal_class,
            failure_reason,
            worktree_path,
            agent_id,
            role,
            requested_model,
            effective_model,
            requested_reasoning_effort,
            effective_reasoning_effort,
            tokens_used,
            verifier_verdict,
            last_agent_message,
        }))
    }

    pub fn create_task_attempt(&self, input: &NewTaskAttempt) -> Result<(), StoreError> {
        let now = now_ms();
        let packet_json = serde_json::to_string(&input.packet)?;
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO task_attempts(id,task_id,attempt_number,state,task_packet_json,task_packet_sha256,base_sha,requested_model_route,token_budget,tool_budget,diff_file_budget,diff_line_budget,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13,1)",
            params![input.id.as_str(),input.task_id.as_str(),input.attempt_number,input.state,packet_json,input.packet_sha256,input.base_sha,input.requested_model_route,input.packet.token_budget as i64,input.packet.tool_budget.map(|value| value as i64),input.packet.diff_budget.files,input.packet.diff_budget.lines,now],
        )?;
        transaction.execute(
            "UPDATE tasks SET current_attempt_number=?2,state='LEASED',updated_at=?3,version=version+1 WHERE id=?1",
            params![input.task_id.as_str(), input.attempt_number, now],
        )?;
        transaction.execute(
            "INSERT INTO task_results(task_attempt_id,updated_at) VALUES(?1,?2)",
            params![input.id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn set_attempt_composed_base(
        &self,
        attempt_id: &AttemptId,
        packet: &harness_domain::TaskPacket,
        base_sha: &str,
    ) -> Result<(), StoreError> {
        let packet_json = serde_json::to_string(packet)?;
        let packet_sha = sha256(packet_json.as_bytes());
        let changed = self.connection()?.execute(
            "UPDATE task_attempts SET task_packet_json=?2,task_packet_sha256=?3,base_sha=?4,token_budget=?5,tool_budget=?6,updated_at=?7,version=version+1 WHERE id=?1",
            params![
                attempt_id.as_str(),
                packet_json,
                packet_sha,
                base_sha,
                packet.token_budget as i64,
                packet.tool_budget.map(|value| value as i64),
                now_ms(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("attempt {attempt_id}")));
        }
        Ok(())
    }

    pub fn transition_task(
        &self,
        id: &TaskId,
        next: TaskState,
        expected_version: Option<u64>,
    ) -> Result<TaskSummary, StoreError> {
        let current = self.task(id)?;
        if expected_version.is_some_and(|version| version != current.version) {
            return Err(StoreError::Conflict(format!("stale task version for {id}")));
        }
        if !current.state.can_transition_to(next) {
            return Err(StoreError::Conflict(format!(
                "illegal task transition {} -> {}",
                current.state, next
            )));
        }
        let current_version = sqlite_version(current.version)?;
        let changed = self.connection()?.execute(
            "UPDATE tasks SET state=?2,updated_at=?3,version=version+1 WHERE id=?1 AND version=?4",
            params![id.as_str(), next.to_string(), now_ms(), current_version],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "task {id} changed concurrently"
            )));
        }
        self.task(id)
    }

    pub fn task_attempt_for_agent(
        &self,
        id: &AgentSessionId,
    ) -> Result<Option<AttemptId>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT task_attempt_id FROM agent_sessions WHERE id=?1",
                [id.as_str()],
                |row| {
                    row.get::<_, Option<String>>(0)
                        .map(|value| value.map(AttemptId::from))
                },
            )
            .optional()?
            .flatten())
    }

    pub fn task_for_attempt(&self, id: &AttemptId) -> Result<TaskId, StoreError> {
        self.connection()?
            .query_row(
                "SELECT task_id FROM task_attempts WHERE id=?1",
                [id.as_str()],
                |row| row.get::<_, String>(0).map(TaskId::from),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("attempt {id}")))
    }

    pub fn agent_context(
        &self,
        id: &AgentSessionId,
    ) -> Result<(RunId, Option<AttemptId>), StoreError> {
        self.connection()?
            .query_row(
                "SELECT run_id,task_attempt_id FROM agent_sessions WHERE id=?1",
                [id.as_str()],
                |row| {
                    Ok((
                        RunId::from(row.get::<_, String>(0)?),
                        row.get::<_, Option<String>>(1)?.map(AttemptId::from),
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("agent {id}")))
    }

    pub fn worktree_for_attempt(
        &self,
        id: &AttemptId,
    ) -> Result<
        (
            harness_domain::WorktreeId,
            std::path::PathBuf,
            String,
            Option<String>,
        ),
        StoreError,
    > {
        self.connection()?
            .query_row(
                "SELECT id,path,base_sha,head_sha FROM worktrees WHERE task_attempt_id=?1 AND removed_at IS NULL ORDER BY created_at DESC LIMIT 1",
                [id.as_str()],
                |row| {
                    Ok((
                        harness_domain::WorktreeId::from(row.get::<_, String>(0)?),
                        std::path::PathBuf::from(row.get::<_, String>(1)?),
                        row.get(2)?,
                        row.get(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("worktree for attempt {id}")))
    }

    pub fn record_context_packet(&self, input: &NewContextPacket) -> Result<(), StoreError> {
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO context_packets(id,run_id,task_attempt_id,role,base_sha,profile_digest,packet_json,packet_sha256,estimated_tokens,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                input.id,
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.role,
                input.base_sha,
                input.profile_digest,
                serde_json::to_string(&input.packet)?,
                input.packet_sha256,
                i64::try_from(input.estimated_tokens).unwrap_or(i64::MAX),
                now_ms(),
            ],
        )?;
        for source in &input.sources {
            transaction.execute(
                "INSERT INTO context_sources(context_packet_id,path,source_class,content_sha256,included,reason,estimated_tokens) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    input.id,
                    source.path,
                    source.source_class,
                    source.content_sha256,
                    source.included,
                    source.reason,
                    i64::try_from(source.estimated_tokens).unwrap_or(i64::MAX),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_unblocked_tasks_ready(&self, run_id: &RunId) -> Result<u64, StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE tasks SET state='READY',updated_at=?2,version=version+1 WHERE run_id=?1 AND state='WAITING_DEPENDENCY' AND NOT EXISTS (SELECT 1 FROM task_dependencies d JOIN tasks dependency ON dependency.id=d.depends_on_task_id WHERE d.task_id=tasks.id AND dependency.state NOT IN ('VERIFIED','INTEGRATION_QUEUED','INTEGRATING','INTEGRATED','CI_PROVEN','LIVE_PROVEN','CLOSED'))",
            params![run_id.as_str(), now_ms()],
        )?;
        Ok(changed as u64)
    }

    pub fn verified_task_commits(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<(TaskId, String)>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT t.id,tr.verified_commit_sha FROM tasks t JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.run_id=?1 AND t.state='VERIFIED' AND tr.verified_commit_sha IS NOT NULL ORDER BY t.created_at,t.id",
        )?;
        let rows = statement.query_map([run_id.as_str()], |row| {
            Ok((TaskId::from(row.get::<_, String>(0)?), row.get(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn set_attempt_result(
        &self,
        attempt_id: &AttemptId,
        state: &str,
        head_sha: Option<&str>,
        terminal_class: Option<&str>,
        failure_reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let terminal = matches!(state, "COMPLETED" | "FAILED" | "INTERRUPTED" | "STALLED");
        self.connection()?.execute(
            "UPDATE task_attempts SET state=?2,head_sha=coalesce(?3,head_sha),terminal_class=?4,failure_reason=?5,started_at=coalesce(started_at,?6),completed_at=CASE WHEN ?7 THEN ?6 ELSE completed_at END,updated_at=?6,version=version+1 WHERE id=?1",
            params![attempt_id.as_str(),state,head_sha,terminal_class,failure_reason,now_ms(),terminal],
        )?;
        Ok(())
    }

    pub fn set_task_diff_result(
        &self,
        attempt_id: &AttemptId,
        head_sha: Option<&str>,
        files: u32,
        additions: u64,
        deletions: u64,
        unexpected_paths: &[String],
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE task_results SET verified_commit_sha=coalesce(?2,verified_commit_sha),diff_files=?3,diff_additions=?4,diff_deletions=?5,unexpected_paths_json=?6,updated_at=?7 WHERE task_attempt_id=?1",
            params![attempt_id.as_str(),head_sha,files,additions as i64,deletions as i64,serde_json::to_string(unexpected_paths)?,now_ms()],
        )?;
        Ok(())
    }

    pub fn acquire_path_leases(
        &self,
        run_id: &RunId,
        attempt_id: &AttemptId,
        base_sha: &str,
        paths: &[String],
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let expires = now.saturating_add((ttl_seconds as i64).saturating_mul(1_000));
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        for path in paths {
            let prefix = normalized_prefix(path);
            let conflict: Option<String> = transaction
                .query_row(
                    "SELECT path_glob FROM path_leases WHERE run_id=?1 AND task_attempt_id<>?4 AND released_at IS NULL AND expires_at>?2 AND (?3 LIKE normalized_prefix || '%' OR normalized_prefix LIKE ?3 || '%') LIMIT 1",
                    params![run_id.as_str(), now, prefix, attempt_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(conflict) = conflict {
                return Err(StoreError::Conflict(format!(
                    "path lease {path} overlaps active lease {conflict}"
                )));
            }
            transaction.execute(
                "INSERT INTO path_leases(id,run_id,task_attempt_id,path_glob,normalized_prefix,lease_kind,base_sha,acquired_at,heartbeat_at,expires_at) VALUES(?1,?2,?3,?4,?5,'write',?6,?7,?7,?8)",
                params![ulid::Ulid::generate().to_string(),run_id.as_str(),attempt_id.as_str(),path,prefix,base_sha,now,expires],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn release_path_leases(
        &self,
        attempt_id: &AttemptId,
        reason: &str,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE path_leases SET released_at=?2,release_reason=?3 WHERE task_attempt_id=?1 AND released_at IS NULL",
            params![attempt_id.as_str(), now_ms(), reason],
        )?;
        Ok(())
    }

    pub fn heartbeat_path_leases(
        &self,
        attempt_id: &AttemptId,
        ttl_seconds: u64,
    ) -> Result<u64, StoreError> {
        let now = now_ms();
        let expires = now.saturating_add(
            i64::try_from(ttl_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        let changed = self.connection()?.execute(
            "UPDATE path_leases SET heartbeat_at=?2,expires_at=?3 WHERE task_attempt_id=?1 AND released_at IS NULL",
            params![attempt_id.as_str(), now, expires],
        )?;
        Ok(changed as u64)
    }

    pub fn heartbeat_run_path_leases(
        &self,
        run_id: &RunId,
        ttl_seconds: u64,
    ) -> Result<u64, StoreError> {
        let now = now_ms();
        let expires = now.saturating_add(
            i64::try_from(ttl_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        let changed = self.connection()?.execute(
            "UPDATE path_leases SET heartbeat_at=?2,expires_at=?3 WHERE run_id=?1 AND released_at IS NULL AND task_attempt_id IN (SELECT a.id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.run_id=?1 AND a.attempt_number=t.current_attempt_number AND t.state IN ('LEASED','STARTING','IMPLEMENTING','REVIEW_READY','VERIFYING','WAITING_APPROVAL'))",
            params![run_id.as_str(), now, expires],
        )?;
        Ok(changed as u64)
    }

    pub fn release_run_path_leases(&self, run_id: &RunId, reason: &str) -> Result<(), StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "UPDATE path_leases SET released_at=?2,release_reason=?3 WHERE run_id=?1 AND released_at IS NULL",
            params![run_id.as_str(), now, reason],
        )?;
        Ok(())
    }

    pub fn append_raw_event(&self, input: &RawEventInput) -> Result<i64, StoreError> {
        let payload = serde_json::to_string(&input.payload)?;
        let digest = sha256(payload.as_bytes());
        let connection = self.connection()?;
        let result = connection.execute(
            "INSERT INTO raw_events(run_id,agent_session_id,thread_id,turn_id,direction,method,request_id,received_at,payload_json,payload_sha256,source_sequence,redaction_class) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![input.run_id.as_ref().map(RunId::as_str),input.agent_session_id.as_ref().map(AgentSessionId::as_str),input.thread_id,input.turn_id,input.direction,input.method,input.request_id,now_ms(),payload,digest,input.source_sequence,input.redaction_class],
        );
        match result {
            Ok(_) => Ok(connection.last_insert_rowid()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                if let (Some(thread), Some(sequence)) = (&input.thread_id, &input.source_sequence) {
                    connection
                        .query_row(
                            "SELECT id FROM raw_events WHERE thread_id=?1 AND source_sequence=?2",
                            params![thread, sequence],
                            |row| row.get(0),
                        )
                        .map_err(Into::into)
                } else {
                    Err(StoreError::Conflict("duplicate raw event".to_owned()))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn emit_domain_event(
        &self,
        run_id: Option<&RunId>,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: &Value,
        source_raw_event_id: Option<i64>,
    ) -> Result<DomainEvent, StoreError> {
        let occurred_at = now_ms();
        let raw = serde_json::to_string(payload)?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT OR IGNORE INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json,source_raw_event_id) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![run_id.map(RunId::as_str),aggregate_type,aggregate_id,event_type,occurred_at,raw,source_raw_event_id],
        )?;
        let id = if connection.changes() == 1 {
            connection.last_insert_rowid()
        } else {
            connection.query_row(
                "SELECT id FROM domain_events WHERE aggregate_type=?1 AND aggregate_id=?2 AND event_type=?3 AND source_raw_event_id IS ?4",
                params![aggregate_type,aggregate_id,event_type,source_raw_event_id],
                |row| row.get(0),
            )?
        };
        Ok(DomainEvent {
            id,
            run_id: run_id.cloned(),
            event_type: event_type.to_owned(),
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: aggregate_id.to_owned(),
            occurred_at,
            payload: payload.clone(),
        })
    }

    pub fn list_domain_events(
        &self,
        after: i64,
        run_id: Option<&RunId>,
        limit: u32,
    ) -> Result<Vec<DomainEvent>, StoreError> {
        let connection = self.connection()?;
        let sql = if run_id.is_some() {
            "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json FROM domain_events WHERE id>?1 AND run_id=?2 ORDER BY id LIMIT ?3"
        } else {
            "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json FROM domain_events WHERE id>?1 ORDER BY id LIMIT ?2"
        };
        let mut statement = connection.prepare(sql)?;
        let rows = if let Some(run_id) = run_id {
            statement.query_map(params![after, run_id.as_str(), limit], map_domain_event)?
        } else {
            statement.query_map(params![after, limit], map_domain_event)?
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn latest_domain_cursor(&self) -> Result<i64, StoreError> {
        Ok(self.connection()?.query_row(
            "SELECT coalesce(max(id),0) FROM domain_events",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn list_activity(
        &self,
        agent_id: &AgentSessionId,
        after: i64,
        limit: u32,
    ) -> Result<Vec<ActivityItem>, StoreError> {
        let connection = self.connection()?;
        let thread: Option<String> = connection
            .query_row(
                "SELECT thread_id FROM codex_threads WHERE agent_session_id=?1",
                [agent_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(thread) = thread else {
            return Ok(Vec::new());
        };
        let mut statement = connection.prepare(
            "SELECT re.id,re.method,coalesce(pi.state,'completed'),coalesce(pi.summary,re.method),re.payload_json,re.received_at FROM raw_events re LEFT JOIN projected_items pi ON pi.source_raw_event_id=re.id WHERE re.thread_id=?1 AND re.id>?2 ORDER BY re.id LIMIT ?3",
        )?;
        let rows = statement.query_map(params![thread, after, limit], |row| {
            let raw: String = row.get(4)?;
            Ok(ActivityItem {
                id: row.get::<_, i64>(0)?.to_string(),
                sequence: row.get(0)?,
                kind: row.get(1)?,
                state: row.get(2)?,
                summary: row.get(3)?,
                payload: serde_json::from_str(&raw).unwrap_or(Value::Null),
                occurred_at: format_timestamp(row.get(5)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn list_recent_activity(
        &self,
        agent_id: &AgentSessionId,
        limit: u32,
    ) -> Result<Vec<ActivityItem>, StoreError> {
        let connection = self.connection()?;
        let thread: Option<String> = connection
            .query_row(
                "SELECT thread_id FROM codex_threads WHERE agent_session_id=?1",
                [agent_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(thread) = thread else {
            return Ok(Vec::new());
        };
        let mut statement = connection.prepare(
            "SELECT id,method,state,summary,payload_json,received_at FROM (SELECT re.id,re.method,coalesce(pi.state,'completed') AS state,coalesce(pi.summary,re.method) AS summary,re.payload_json,re.received_at FROM raw_events re LEFT JOIN projected_items pi ON pi.source_raw_event_id=re.id WHERE re.thread_id=?1 ORDER BY re.id DESC LIMIT ?2) ORDER BY id",
        )?;
        let rows = statement.query_map(params![thread, limit], |row| {
            let raw: String = row.get(4)?;
            Ok(ActivityItem {
                id: row.get::<_, i64>(0)?.to_string(),
                sequence: row.get(0)?,
                kind: row.get(1)?,
                state: row.get(2)?,
                summary: row.get(3)?,
                payload: serde_json::from_str(&raw).unwrap_or(Value::Null),
                occurred_at: format_timestamp(row.get(5)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn latest_agent_message(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<LatestAgentMessage>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT pi.item_id,json_extract(pi.payload_json,'$.text'),json_extract(pi.payload_json,'$.phase'),coalesce(pi.completed_at,pi.started_at) FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id WHERE ct.agent_session_id=?1 AND pi.item_type='agentMessage' AND pi.state='completed' AND json_extract(pi.payload_json,'$.text') IS NOT NULL ORDER BY (json_extract(pi.payload_json,'$.phase')='final_answer') DESC,coalesce(pi.completed_at,pi.started_at) DESC LIMIT 1",
                [agent_id.as_str()],
                |row| {
                    Ok(LatestAgentMessage {
                        id: row.get(0)?,
                        text: row.get(1)?,
                        phase: row.get(2)?,
                        occurred_at: format_timestamp(row.get(3)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_agent_messages(
        &self,
        agent_id: &AgentSessionId,
        limit: u32,
    ) -> Result<Vec<LatestAgentMessage>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT item_id,text,phase,occurred_at FROM (SELECT pi.item_id AS item_id,json_extract(pi.payload_json,'$.text') AS text,json_extract(pi.payload_json,'$.phase') AS phase,coalesce(pi.completed_at,pi.started_at) AS occurred_at,pi.rowid AS sort_id FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id WHERE ct.agent_session_id=?1 AND pi.item_type='agentMessage' AND pi.state='completed' AND json_extract(pi.payload_json,'$.text') IS NOT NULL ORDER BY occurred_at DESC,sort_id DESC LIMIT ?2) ORDER BY occurred_at,sort_id",
        )?;
        let rows = statement.query_map(params![agent_id.as_str(), limit], |row| {
            Ok(LatestAgentMessage {
                id: row.get(0)?,
                text: row.get(1)?,
                phase: row.get(2)?,
                occurred_at: format_timestamp(row.get(3)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn latest_agent_plan(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<Value>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT pi.payload_json FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id WHERE ct.agent_session_id=?1 AND pi.item_type='plan' ORDER BY pi.rowid DESC LIMIT 1",
                [agent_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|raw| serde_json::from_str(&raw).map_err(StoreError::from))
            .transpose()
    }

    pub fn list_task_governor_messages(
        &self,
        task_id: &TaskId,
        limit: u32,
    ) -> Result<Vec<LatestAgentMessage>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT item_id,text,phase,occurred_at FROM (SELECT pi.item_id AS item_id,json_extract(pi.payload_json,'$.text') AS text,json_extract(pi.payload_json,'$.phase') AS phase,coalesce(pi.completed_at,pi.started_at) AS occurred_at,pi.rowid AS sort_id FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id JOIN task_attempts ta ON ta.id=a.task_attempt_id WHERE ta.task_id=?1 AND a.role='governor' AND pi.item_type='agentMessage' AND pi.state='completed' AND json_extract(pi.payload_json,'$.text') IS NOT NULL ORDER BY occurred_at DESC,sort_id DESC LIMIT ?2) ORDER BY occurred_at,sort_id",
        )?;
        let rows = statement.query_map(params![task_id.as_str(), limit], |row| {
            Ok(LatestAgentMessage {
                id: row.get(0)?,
                text: row.get(1)?,
                phase: row.get(2)?,
                occurred_at: format_timestamp(row.get(3)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_approval(&self, input: &NewApproval, rpc_id: &Value) -> Result<(), StoreError> {
        let now = now_ms();
        let request_json = serde_json::to_string(&input.request)?;
        let request_sha = sha256(request_json.as_bytes());
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO approvals(id,run_id,task_attempt_id,agent_session_id,thread_id,turn_id,item_id,approval_type,risk_level,request_json,request_sha256,expected_head_sha,expected_worktree_fingerprint,state,created_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,'pending',?14,1)",
            params![input.id.as_str(),input.run_id.as_str(),input.task_attempt_id.as_ref().map(AttemptId::as_str),input.agent_session_id.as_ref().map(AgentSessionId::as_str),input.thread_id,input.turn_id,input.item_id,input.approval_type,enum_text(&input.risk_level)?,request_json,request_sha,input.expected_head_sha,input.expected_worktree_fingerprint,now],
        )?;
        transaction.execute(
            "INSERT INTO runtime_rpc_requests(request_key,rpc_id_json,method,run_id,agent_session_id,thread_id,turn_id,item_id,payload_json,state,received_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'pending',?10)",
            params![input.id.as_str(),serde_json::to_string(rpc_id)?,input.approval_type,input.run_id.as_str(),input.agent_session_id.as_ref().map(AgentSessionId::as_str),input.thread_id,input.turn_id,input.item_id,request_json,now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn list_approvals(
        &self,
        run_id: Option<&RunId>,
        state: Option<&str>,
    ) -> Result<Vec<ApprovalSummary>, StoreError> {
        let connection = self.connection()?;
        let mut conditions = Vec::new();
        let mut values = Vec::<String>::new();
        if let Some(run_id) = run_id {
            conditions.push(format!("ap.run_id=?{}", values.len() + 1));
            values.push(run_id.to_string());
        }
        if let Some(state) = state {
            conditions.push(format!("ap.state=?{}", values.len() + 1));
            values.push(state.to_owned());
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT ap.id,ap.run_id,ap.agent_session_id,t.id,ap.thread_id,ap.turn_id,ap.approval_type,ap.risk_level,ap.request_json,ap.state,ap.decision,ap.created_at,ap.resolved_at,ap.version FROM approvals ap LEFT JOIN task_attempts a ON a.id=ap.task_attempt_id LEFT JOIN tasks t ON t.id=a.task_id{} ORDER BY CASE ap.risk_level WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,ap.created_at",
            where_clause
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values), map_approval)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn approval(&self, id: &ApprovalId) -> Result<ApprovalSummary, StoreError> {
        let connection = self.connection()?;
        connection.query_row(
            "SELECT ap.id,ap.run_id,ap.agent_session_id,t.id,ap.thread_id,ap.turn_id,ap.approval_type,ap.risk_level,ap.request_json,ap.state,ap.decision,ap.created_at,ap.resolved_at,ap.version FROM approvals ap LEFT JOIN task_attempts a ON a.id=ap.task_attempt_id LEFT JOIN tasks t ON t.id=a.task_id WHERE ap.id=?1",
            [id.as_str()],
            map_approval,
        ).optional()?.ok_or_else(|| StoreError::NotFound(format!("approval {id}")))
    }

    pub fn approval_expected_custody(
        &self,
        id: &ApprovalId,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        self.connection()?
            .query_row(
                "SELECT expected_head_sha,expected_worktree_fingerprint FROM approvals WHERE id=?1",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("approval {id}")))
    }

    pub fn decide_approval(
        &self,
        id: &ApprovalId,
        decision: &str,
        note: Option<&str>,
        actor: &str,
        expected_version: Option<u64>,
    ) -> Result<(ApprovalSummary, Value), StoreError> {
        let approval = self.approval(id)?;
        if approval.state != "pending" {
            return Err(StoreError::Conflict(format!(
                "approval {id} is already {}",
                approval.state
            )));
        }
        if expected_version.is_some_and(|version| version != approval.version) {
            return Err(StoreError::Conflict(format!(
                "approval {id} changed concurrently"
            )));
        }
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let rpc: String = transaction.query_row(
            "SELECT rpc_id_json FROM runtime_rpc_requests WHERE request_key=?1 AND state='pending'",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let now = now_ms();
        let current_version = sqlite_version(approval.version)?;
        transaction.execute(
            "UPDATE approvals SET state='delivering',decision=?2,decision_note=?3,decided_by=?4,resolved_at=?5,version=version+1 WHERE id=?1 AND version=?6",
            params![id.as_str(),decision,note,actor,now,current_version],
        )?;
        transaction.commit()?;
        Ok((self.approval(id)?, serde_json::from_str(&rpc)?))
    }

    pub fn mark_approval_delivered(
        &self,
        id: &ApprovalId,
        delivery_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let state = if delivery_error.is_some() {
            "pending"
        } else {
            "resolved"
        };
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE approvals SET state=?2,delivered_at=CASE WHEN ?3 IS NULL THEN ?4 ELSE delivered_at END,delivery_error=?3,version=version+1 WHERE id=?1",
            params![id.as_str(),state,delivery_error,now],
        )?;
        if delivery_error.is_none() {
            transaction.execute(
                "UPDATE runtime_rpc_requests SET state='resolved',resolved_at=?2 WHERE request_key=?1",
                params![id.as_str(), now],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn expire_pending_approvals(&self, run_id: &RunId, reason: &str) -> Result<(), StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE approvals SET state='expired',decision='cancel',decision_note=?2,decided_by='harnessd-recovery',resolved_at=?3,delivery_error='originating App Server process is unavailable',version=version+1 WHERE run_id=?1 AND state IN ('pending','delivering')",
            params![run_id.as_str(), reason, now],
        )?;
        transaction.execute(
            "UPDATE runtime_rpc_requests SET state='expired',resolved_at=?2 WHERE run_id=?1 AND state='pending'",
            params![run_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn create_api_session(
        &self,
        id: &str,
        csrf_secret_hash: &str,
        ttl_seconds: u64,
    ) -> Result<StoredSession, StoreError> {
        let now = now_ms();
        let expires = now.saturating_add((ttl_seconds as i64).saturating_mul(1_000));
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "DELETE FROM api_sessions WHERE expires_at<=?1 OR revoked_at IS NOT NULL",
            [now],
        )?;
        transaction.execute(
            "INSERT INTO api_sessions(id,created_at,expires_at,csrf_secret_hash,last_seen_at) VALUES(?1,?2,?3,?4,?2)",
            params![id,now,expires,csrf_secret_hash],
        )?;
        transaction.commit()?;
        Ok(StoredSession {
            id: id.to_owned(),
            expires_at: expires,
            csrf_secret_hash: csrf_secret_hash.to_owned(),
            revoked: false,
        })
    }

    pub fn api_session(&self, id: &str) -> Result<Option<StoredSession>, StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let session = connection
            .query_row(
                "SELECT id,expires_at,csrf_secret_hash,revoked_at IS NOT NULL FROM api_sessions WHERE id=?1 AND expires_at>?2",
                params![id,now],
                |row| Ok(StoredSession { id: row.get(0)?, expires_at: row.get(1)?, csrf_secret_hash: row.get(2)?, revoked: row.get(3)? }),
            )
            .optional()?;
        if session.is_some() {
            connection.execute(
                "UPDATE api_sessions SET last_seen_at=?2 WHERE id=?1",
                params![id, now],
            )?;
        }
        Ok(session)
    }

    // This mirrors the narrow immutable audit-row boundary; grouping these
    // columns would obscure which values are committed in one transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn record_human_action(
        &self,
        run_id: Option<&RunId>,
        attempt_id: Option<&AttemptId>,
        actor: &str,
        action_type: &str,
        target_type: &str,
        target_id: &str,
        payload: &Value,
    ) -> Result<i64, StoreError> {
        let raw = serde_json::to_string(payload)?;
        let digest = sha256(raw.as_bytes());
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO human_actions(run_id,task_attempt_id,actor,action_type,target_type,target_id,occurred_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![run_id.map(RunId::as_str),attempt_id.map(AttemptId::as_str),actor,action_type,target_type,target_id,now_ms(),raw,digest],
        )?;
        Ok(connection.last_insert_rowid())
    }

    pub fn register_artifact(&self, input: &NewArtifact) -> Result<ArtifactId, StoreError> {
        let connection = self.connection()?;
        let existing = connection
            .query_row(
                "SELECT id FROM artifacts WHERE sha256=?1",
                [input.sha256.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            return Ok(ArtifactId::from(existing));
        }
        connection.execute(
            "INSERT INTO artifacts(id,run_id,task_attempt_id,kind,logical_name,storage_path,sha256,media_type,compression,sensitivity,byte_length,retention_class,pinned,created_at,verified_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?14)",
            params![
                input.id.as_str(),
                input.run_id.as_ref().map(RunId::as_str),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.kind,
                input.logical_name,
                input.storage_path.to_string_lossy(),
                input.sha256,
                input.media_type,
                input.compression,
                input.sensitivity,
                i64::try_from(input.byte_length).unwrap_or(i64::MAX),
                input.retention_class,
                input.pinned,
                now_ms(),
            ],
        )?;
        Ok(input.id.clone())
    }

    pub fn artifact(&self, id: &ArtifactId) -> Result<ArtifactRecord, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,kind,logical_name,storage_path,sha256,media_type,byte_length,verified_at FROM artifacts WHERE id=?1",
                [id.as_str()],
                |row| {
                    Ok(ArtifactRecord {
                        id: ArtifactId::from(row.get::<_, String>(0)?),
                        kind: row.get(1)?,
                        logical_name: row.get(2)?,
                        storage_path: Path::new(&row.get::<_, String>(3)?).to_path_buf(),
                        sha256: row.get(4)?,
                        media_type: row.get(5)?,
                        byte_length: row.get::<_, i64>(6)? as u64,
                        verified_at: row.get(7)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("artifact {id}")))
    }

    pub fn record_command(&self, input: &NewCommandRecord) -> Result<(), StoreError> {
        let command_json = serde_json::to_string(&input.command)?;
        let command_sha = sha256(command_json.as_bytes());
        self.connection()?.execute(
            "INSERT INTO command_runs(id,run_id,task_attempt_id,agent_session_id,worktree_id,command_json,command_sha256,cwd,source_sha_before,source_sha_after,resource_class,host_identity,target_profile,started_at,completed_at,exit_code,signal,timed_out,result_class,stdout_artifact_id,stderr_artifact_id,error_json,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.agent_session_id.as_ref().map(AgentSessionId::as_str),
                input.worktree_id.as_ref().map(harness_domain::WorktreeId::as_str),
                command_json,
                command_sha,
                input.cwd.to_string_lossy(),
                input.source_sha_before,
                input.source_sha_after,
                input.resource_class,
                input.host_identity,
                input.target_profile,
                input.started_at,
                input.completed_at,
                input.exit_code,
                input.signal,
                input.timed_out,
                enum_text(&input.result_class)?,
                input.stdout_artifact_id.as_ref().map(ArtifactId::as_str),
                input.stderr_artifact_id.as_ref().map(ArtifactId::as_str),
                input.error.as_ref().map(serde_json::to_string).transpose()?,
            ],
        )?;
        Ok(())
    }

    pub fn record_validation(&self, input: &NewValidationRecord) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO validations(id,run_id,task_attempt_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,command_run_id,started_at,completed_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'completed',?9,?10,?11,?12,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.worktree_id.as_str(),
                input.validator_id,
                enum_text(&input.proof_tier)?,
                input.source_sha,
                input.selector_reason,
                enum_text(&input.result_class)?,
                input.command_run_id.as_ref().map(harness_domain::CommandRunId::as_str),
                input.started_at,
                input.completed_at,
            ],
        )?;
        Ok(())
    }

    pub fn record_evidence(&self, input: &NewEvidenceRecord) -> Result<(), StoreError> {
        let evidence_json = serde_json::to_string(&input.evidence)?;
        let evidence_sha = sha256(evidence_json.as_bytes());
        self.connection()?.execute(
            "INSERT INTO evidence_records(id,run_id,task_attempt_id,validation_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.validation_id.as_ref().map(harness_domain::ValidationId::as_str),
                input.claim_id,
                serde_json::to_string(&input.checklist_rows)?,
                input.source_sha,
                enum_text(&input.proof_tier)?,
                enum_text(&input.result_class)?,
                evidence_json,
                evidence_sha,
                serde_json::to_string(&input.unproved_claims)?,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn link_evidence_artifact(
        &self,
        evidence_id: &harness_domain::EvidenceId,
        artifact_id: &ArtifactId,
        purpose: &str,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT OR IGNORE INTO evidence_artifacts(evidence_id,artifact_id,purpose) VALUES(?1,?2,?3)",
            params![evidence_id.as_str(), artifact_id.as_str(), purpose],
        )?;
        Ok(())
    }

    pub fn evidence_snapshot(&self, run_id: &RunId) -> Result<Value, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT e.id,e.task_attempt_id,e.validation_id,e.claim_id,e.checklist_rows_json,e.source_sha,e.proof_tier,e.result_class,e.evidence_json,e.evidence_sha256,e.unproved_claims_json,e.created_at,e.invalidated_at,e.invalidated_reason FROM evidence_records e WHERE e.run_id=?1 ORDER BY e.created_at,e.id",
        )?;
        let evidence = statement
            .query_map([run_id.as_str()], |row| {
                let checklist: String = row.get(4)?;
                let evidence: String = row.get(8)?;
                let unproved: String = row.get(10)?;
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "task_attempt_id": row.get::<_, Option<String>>(1)?,
                    "validation_id": row.get::<_, Option<String>>(2)?,
                    "claim_id": row.get::<_, String>(3)?,
                    "checklist_rows": serde_json::from_str::<Value>(&checklist).unwrap_or(Value::Null),
                    "source_sha": row.get::<_, String>(5)?,
                    "proof_tier": row.get::<_, String>(6)?,
                    "result_class": row.get::<_, String>(7)?,
                    "evidence": serde_json::from_str::<Value>(&evidence).unwrap_or(Value::Null),
                    "evidence_sha256": row.get::<_, String>(9)?,
                    "unproved_claims": serde_json::from_str::<Value>(&unproved).unwrap_or(Value::Null),
                    "created_at": row.get::<_, i64>(11)?,
                    "invalidated_at": row.get::<_, Option<i64>>(12)?,
                    "invalidated_reason": row.get::<_, Option<String>>(13)?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        let mut artifacts_statement = connection.prepare(
            "SELECT DISTINCT a.id,a.kind,a.logical_name,a.storage_path,a.sha256,a.media_type,a.byte_length FROM artifacts a JOIN evidence_artifacts ea ON ea.artifact_id=a.id JOIN evidence_records e ON e.id=ea.evidence_id WHERE e.run_id=?1 ORDER BY a.id",
        )?;
        let artifacts = artifacts_statement
            .query_map([run_id.as_str()], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "kind": row.get::<_, String>(1)?,
                    "logical_name": row.get::<_, String>(2)?,
                    "storage_path": row.get::<_, String>(3)?,
                    "sha256": row.get::<_, String>(4)?,
                    "media_type": row.get::<_, String>(5)?,
                    "byte_length": row.get::<_, i64>(6)?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(artifacts_statement);
        drop(connection);
        let run = self.run(run_id)?;
        let tasks = self.list_tasks(run_id)?;
        let agents = self.list_agents(run_id)?;
        Ok(json!({
            "schema": "harness-evidence-snapshot/v1",
            "run": run,
            "tasks": tasks,
            "agents": agents,
            "evidence": evidence,
            "artifacts": artifacts,
        }))
    }

    pub fn run_usage(&self, run_id: &RunId) -> Result<UsageSummary, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT ts.effective_model,ts.input_tokens,ts.cached_input_tokens,ts.cache_write_input_tokens,ts.output_tokens,ts.reasoning_output_tokens,ts.total_tokens,ts.model_context_window,c.lower_microusd,c.upper_microusd,c.confidence,c.explanation,c.pricing_snapshot_id FROM token_samples ts JOIN codex_threads ct ON ct.thread_id=ts.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id LEFT JOIN cost_entries c ON c.token_sample_id=ts.id WHERE a.run_id=?1 ORDER BY ts.observed_at,ts.id",
        )?;
        let rows = statement.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                TokenUsage {
                    input_tokens: row.get::<_, i64>(1)? as u64,
                    cached_input_tokens: row.get::<_, i64>(2)? as u64,
                    cache_write_input_tokens: row
                        .get::<_, Option<i64>>(3)?
                        .map(|value| value as u64),
                    output_tokens: row.get::<_, i64>(4)? as u64,
                    reasoning_output_tokens: row.get::<_, i64>(5)? as u64,
                    total_tokens: row.get::<_, i64>(6)? as u64,
                    model_context_window: row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
                },
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
            ))
        })?;
        let mut summary = UsageSummary::default();
        let mut models = BTreeMap::<String, ModelUsageSummary>::new();
        for row in rows {
            let (model, usage, lower, upper, confidence, explanation, pricing_id) = row?;
            let cost = CostEstimate {
                lower_microusd: lower.unwrap_or_default().max(0) as u64,
                upper_microusd: upper.unwrap_or_default().max(0) as u64,
                confidence: match confidence.as_deref() {
                    Some("exact") => CostConfidence::Exact,
                    Some("bounded") => CostConfidence::Bounded,
                    _ => CostConfidence::Unknown,
                },
                pricing_snapshot_ids: pricing_id.into_iter().collect(),
                explanation: explanation.unwrap_or_else(|| "No matching price snapshot".to_owned()),
            };
            harness_usage::add_sample(&mut summary, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            let model_summary = models
                .entry(model.clone())
                .or_insert_with(|| ModelUsageSummary {
                    model,
                    ..ModelUsageSummary::default()
                });
            model_summary.turns = model_summary.turns.saturating_add(1);
            let mut temporary = UsageSummary {
                input_tokens: model_summary.usage.input_tokens,
                cached_input_tokens: model_summary.usage.cached_input_tokens,
                cache_write_input_tokens: model_summary.usage.cache_write_input_tokens,
                output_tokens: model_summary.usage.output_tokens,
                reasoning_output_tokens: model_summary.usage.reasoning_output_tokens,
                total_tokens: model_summary.usage.total_tokens,
                cost: model_summary.cost.clone(),
                by_model: vec![],
            };
            harness_usage::add_sample(&mut temporary, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            model_summary.usage = TokenUsage {
                input_tokens: temporary.input_tokens,
                cached_input_tokens: temporary.cached_input_tokens,
                cache_write_input_tokens: temporary.cache_write_input_tokens,
                output_tokens: temporary.output_tokens,
                reasoning_output_tokens: temporary.reasoning_output_tokens,
                total_tokens: temporary.total_tokens,
                model_context_window: usage.model_context_window,
            };
            model_summary.cost = temporary.cost;
        }
        summary.by_model = models.into_values().collect();
        Ok(summary)
    }

    pub fn usage_breakdown(&self) -> Result<UsageBreakdown, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT coalesce(a.codex_account_id,'legacy'),r.id,r.display_name,a.id,coalesce(a.nickname,a.role),a.role,ts.effective_model,ts.input_tokens,ts.cached_input_tokens,ts.cache_write_input_tokens,ts.output_tokens,ts.reasoning_output_tokens,ts.total_tokens,ts.model_context_window,c.lower_microusd,c.upper_microusd,c.confidence,c.explanation,c.pricing_snapshot_id FROM token_samples ts JOIN codex_threads ct ON ct.thread_id=ts.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id JOIN runs ru ON ru.id=a.run_id JOIN repositories r ON r.id=ru.repository_id LEFT JOIN cost_entries c ON c.token_sample_id=ts.id ORDER BY ts.observed_at,ts.id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                TokenUsage {
                    input_tokens: row.get::<_, i64>(7)? as u64,
                    cached_input_tokens: row.get::<_, i64>(8)? as u64,
                    cache_write_input_tokens: row
                        .get::<_, Option<i64>>(9)?
                        .map(|value| value as u64),
                    output_tokens: row.get::<_, i64>(10)? as u64,
                    reasoning_output_tokens: row.get::<_, i64>(11)? as u64,
                    total_tokens: row.get::<_, i64>(12)? as u64,
                    model_context_window: row.get::<_, Option<i64>>(13)?.map(|value| value as u64),
                },
                row.get::<_, Option<i64>>(14)?,
                row.get::<_, Option<i64>>(15)?,
                row.get::<_, Option<String>>(16)?,
                row.get::<_, Option<String>>(17)?,
                row.get::<_, Option<String>>(18)?,
            ))
        })?;
        let mut result = UsageBreakdown::default();
        let mut accounts = BTreeMap::<String, UsageGroup>::new();
        let mut repositories = BTreeMap::<String, UsageGroup>::new();
        let mut agents = BTreeMap::<String, UsageGroup>::new();
        for row in rows {
            let (
                account_id,
                repository_id,
                repository_label,
                agent_id,
                agent_label,
                role,
                model,
                usage,
                lower,
                upper,
                confidence,
                explanation,
                pricing_id,
            ) = row?;
            let cost = CostEstimate {
                lower_microusd: lower.unwrap_or_default().max(0) as u64,
                upper_microusd: upper.unwrap_or_default().max(0) as u64,
                confidence: match confidence.as_deref() {
                    Some("exact") => CostConfidence::Exact,
                    Some("bounded") => CostConfidence::Bounded,
                    _ => CostConfidence::Unknown,
                },
                pricing_snapshot_ids: pricing_id.into_iter().collect(),
                explanation: explanation.unwrap_or_else(|| "No matching price snapshot".to_owned()),
            };
            harness_usage::add_sample(&mut result.total, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            add_usage_group(
                &mut accounts,
                account_id.clone(),
                if account_id == "legacy" {
                    "Unattributed history".to_owned()
                } else {
                    account_id
                },
                "Codex account".to_owned(),
                &usage,
                &cost,
            )?;
            add_usage_group(
                &mut repositories,
                repository_id,
                repository_label,
                "Repository".to_owned(),
                &usage,
                &cost,
            )?;
            add_usage_group(
                &mut agents,
                agent_id,
                agent_label,
                format!("{role} · {model}"),
                &usage,
                &cost,
            )?;
        }
        result.by_account = accounts.into_values().collect();
        result.by_repository = repositories.into_values().collect();
        result.by_agent = agents.into_values().collect();
        for groups in [
            &mut result.by_account,
            &mut result.by_repository,
            &mut result.by_agent,
        ] {
            groups.sort_by_key(|group| std::cmp::Reverse(group.usage.total_tokens));
        }
        Ok(result)
    }

    pub fn record_run_export(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
        manifest_sha256: &str,
    ) -> Result<String, StoreError> {
        let id = ulid::Ulid::generate().to_string();
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO run_exports(id,run_id,artifact_id,state,manifest_sha256,created_at,completed_at) VALUES(?1,?2,?3,'completed',?4,?5,?5)",
            params![id, run_id.as_str(), artifact_id.as_str(), manifest_sha256, now],
        )?;
        Ok(id)
    }
}

fn add_usage_group(
    groups: &mut BTreeMap<String, UsageGroup>,
    id: String,
    label: String,
    detail: String,
    usage: &TokenUsage,
    cost: &CostEstimate,
) -> Result<(), StoreError> {
    let group = groups.entry(id.clone()).or_insert_with(|| UsageGroup {
        id,
        label,
        detail,
        ..UsageGroup::default()
    });
    group.turns = group.turns.saturating_add(1);
    let mut summary = UsageSummary {
        input_tokens: group.usage.input_tokens,
        cached_input_tokens: group.usage.cached_input_tokens,
        cache_write_input_tokens: group.usage.cache_write_input_tokens,
        output_tokens: group.usage.output_tokens,
        reasoning_output_tokens: group.usage.reasoning_output_tokens,
        total_tokens: group.usage.total_tokens,
        cost: group.cost.clone(),
        by_model: Vec::new(),
    };
    harness_usage::add_sample(&mut summary, usage, cost)
        .map_err(|error| StoreError::Validation(error.to_string()))?;
    group.usage = TokenUsage {
        input_tokens: summary.input_tokens,
        cached_input_tokens: summary.cached_input_tokens,
        cache_write_input_tokens: summary.cache_write_input_tokens,
        output_tokens: summary.output_tokens,
        reasoning_output_tokens: summary.reasoning_output_tokens,
        total_tokens: summary.total_tokens,
        model_context_window: usage.model_context_window,
    };
    group.cost = summary.cost;
    Ok(())
}

fn repository_select(by_id: bool) -> &'static str {
    if by_id {
        "SELECT r.id,r.profile_id,r.display_name,r.root_path,r.origin_url,r.default_branch,h.primary_branch,h.primary_head_sha,coalesce(h.primary_clean,0),r.state,coalesce(h.blockers_json,'[]'),(SELECT count(*) FROM worktrees w JOIN runs ru ON ru.id=w.run_id WHERE ru.repository_id=r.id AND w.removed_at IS NULL),h.authority_digest,r.version FROM repositories r LEFT JOIN repository_health_snapshots h ON h.id=(SELECT id FROM repository_health_snapshots WHERE repository_id=r.id ORDER BY observed_at DESC LIMIT 1) WHERE r.id=?1"
    } else {
        "SELECT r.id,r.profile_id,r.display_name,r.root_path,r.origin_url,r.default_branch,h.primary_branch,h.primary_head_sha,coalesce(h.primary_clean,0),r.state,coalesce(h.blockers_json,'[]'),(SELECT count(*) FROM worktrees w JOIN runs ru ON ru.id=w.run_id WHERE ru.repository_id=r.id AND w.removed_at IS NULL),h.authority_digest,r.version FROM repositories r LEFT JOIN repository_health_snapshots h ON h.id=(SELECT id FROM repository_health_snapshots WHERE repository_id=r.id ORDER BY observed_at DESC LIMIT 1) ORDER BY r.display_name"
    }
}

fn map_repository(row: &Row<'_>) -> rusqlite::Result<RepositorySummary> {
    let blockers: String = row.get(10)?;
    let state: String = row.get(9)?;
    Ok(RepositorySummary {
        id: RepositoryId::from(row.get::<_, String>(0)?),
        profile_id: row.get(1)?,
        display_name: row.get(2)?,
        root_path: row.get(3)?,
        origin_url: row.get(4)?,
        default_branch: row.get(5)?,
        primary_branch: row.get(6)?,
        primary_head: row.get(7)?,
        primary_clean: row.get(8)?,
        health: state.to_ascii_lowercase(),
        blockers: serde_json::from_str(&blockers).unwrap_or_default(),
        managed_worktree_count: row.get::<_, i64>(11)? as u32,
        authority_digest: row.get(12)?,
        version: row.get::<_, i64>(13)? as u64,
    })
}

fn run_select() -> &'static str {
    "SELECT r.id,r.repository_id,r.title,r.requested_objective,r.mode,r.publication_mode,r.state,r.phase,r.base_ref,r.base_sha,r.integration_branch,r.integration_sha,r.authority_digest,r.created_at,r.started_at,r.completed_at,r.scheduler_paused,r.run_token_budget,r.version FROM runs r"
}

fn map_run(row: &Row<'_>) -> rusqlite::Result<RunSummary> {
    let state: String = row.get(6)?;
    Ok(RunSummary {
        id: RunId::from(row.get::<_, String>(0)?),
        repository_id: RepositoryId::from(row.get::<_, String>(1)?),
        title: row.get(2)?,
        objective: row.get(3)?,
        mode: row.get(4)?,
        publication_mode: row.get(5)?,
        state: state
            .parse()
            .map_err(|error: harness_domain::DomainError| {
                rusqlite::Error::FromSqlConversionFailure(
                    state.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        phase: row.get(7)?,
        base_ref: row.get(8)?,
        base_sha: row.get(9)?,
        integration_branch: row.get(10)?,
        integration_sha: row.get(11)?,
        authority_digest: row.get(12)?,
        created_at: format_timestamp(row.get(13)?),
        started_at: row.get::<_, Option<i64>>(14)?.map(format_timestamp),
        completed_at: row.get::<_, Option<i64>>(15)?.map(format_timestamp),
        scheduler_paused: row.get(16)?,
        run_token_budget: row.get::<_, Option<i64>>(17)?.map(|value| value as u64),
        version: row.get::<_, i64>(18)? as u64,
    })
}

fn agent_select() -> &'static str {
    "SELECT a.id,a.parent_agent_session_id,t.id,a.role,a.codex_account_id,a.nickname,a.state,a.requested_model,a.effective_model,a.requested_reasoning_effort,a.effective_reasoning_effort,a.sandbox_mode,a.cwd,a.current_goal,d.current_action,a.token_budget,coalesce(a.goal_tokens_used,0),coalesce((SELECT sum(c.lower_microusd) FROM codex_threads ct JOIN token_samples ts ON ts.thread_id=ct.thread_id JOIN cost_entries c ON c.token_sample_id=ts.id WHERE ct.agent_session_id=a.id),0),coalesce((SELECT sum(c.upper_microusd) FROM codex_threads ct JOIN token_samples ts ON ts.thread_id=ct.thread_id JOIN cost_entries c ON c.token_sample_id=ts.id WHERE ct.agent_session_id=a.id),0),a.last_heartbeat_at,ct.thread_id,d.active_turn_id,coalesce(d.context_strategy,'fresh_independent'),d.context_source_attempt_id,d.context_reuse_reason,a.version FROM agent_sessions a LEFT JOIN task_attempts at ON at.id=a.task_attempt_id LEFT JOIN tasks t ON t.id=at.task_id LEFT JOIN agent_runtime_details d ON d.agent_session_id=a.id LEFT JOIN codex_threads ct ON ct.agent_session_id=a.id"
}

fn map_agent(row: &Row<'_>) -> rusqlite::Result<AgentSummary> {
    let role: String = row.get(3)?;
    let sandbox: String = row.get(11)?;
    Ok(AgentSummary {
        id: AgentSessionId::from(row.get::<_, String>(0)?),
        parent_agent_id: row.get::<_, Option<String>>(1)?.map(AgentSessionId::from),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId::from),
        role: parse_enum(&role)?,
        codex_account_id: row.get(4)?,
        nickname: row.get(5)?,
        state: row.get(6)?,
        requested_model: row.get(7)?,
        effective_model: row.get(8)?,
        requested_reasoning_effort: row.get(9)?,
        effective_reasoning_effort: row.get(10)?,
        sandbox_mode: parse_enum(&sandbox)?,
        cwd: row.get(12)?,
        current_goal: row.get(13)?,
        current_action: row.get(14)?,
        token_budget: row.get::<_, Option<i64>>(15)?.map(|value| value as u64),
        tokens_used: row.get::<_, i64>(16)? as u64,
        budget_tokens_used: row.get::<_, i64>(16)? as u64,
        estimated_cost_lower: format_microusd(row.get(17)?),
        estimated_cost_upper: format_microusd(row.get(18)?),
        heartbeat_at: row.get::<_, Option<i64>>(19)?.map(format_timestamp),
        thread_id: row.get(20)?,
        active_turn_id: row.get(21)?,
        context_strategy: row.get(22)?,
        context_source_attempt_id: row.get::<_, Option<String>>(23)?.map(AttemptId::from),
        context_reuse_reason: row.get(24)?,
        version: row.get::<_, i64>(25)? as u64,
    })
}

fn map_worktree(row: &Row<'_>) -> rusqlite::Result<WorktreeSummary> {
    Ok(WorktreeSummary {
        id: harness_domain::WorktreeId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId::from),
        kind: row.get(3)?,
        path: row.get(4)?,
        branch: row.get(5)?,
        base_sha: row.get(6)?,
        head_sha: row.get(7)?,
        state: row.get(8)?,
        preserved_reason: row.get(9)?,
        dirty: false,
        files_changed: row.get::<_, i64>(10)? as u32,
        additions: row.get::<_, i64>(11)? as u64,
        deletions: row.get::<_, i64>(12)? as u64,
        version: row.get::<_, i64>(13)? as u64,
    })
}

fn map_task(row: &Row<'_>) -> rusqlite::Result<TaskSummary> {
    let state: String = row.get(5)?;
    let dependencies: Option<String> = row.get(14)?;
    Ok(TaskSummary {
        id: TaskId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        external_task_id: row.get(2)?,
        title: row.get(3)?,
        objective: row.get(4)?,
        state: state
            .parse()
            .map_err(|error: harness_domain::DomainError| {
                rusqlite::Error::FromSqlConversionFailure(
                    state.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        priority: row.get(6)?,
        owner_profile: row.get(7)?,
        reviewer_profile: row.get(8)?,
        attempt: row.get::<_, i64>(9)? as u32,
        base_sha: row.get(10)?,
        head_sha: row.get(11)?,
        token_budget: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
        version: row.get::<_, i64>(13)? as u64,
        dependencies: dependencies
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok())
            .unwrap_or_default(),
    })
}

fn map_domain_event(row: &Row<'_>) -> rusqlite::Result<DomainEvent> {
    let payload: String = row.get(6)?;
    Ok(DomainEvent {
        id: row.get(0)?,
        run_id: row.get::<_, Option<String>>(1)?.map(RunId::from),
        aggregate_type: row.get(2)?,
        aggregate_id: row.get(3)?,
        event_type: row.get(4)?,
        occurred_at: row.get(5)?,
        payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
    })
}

fn map_approval(row: &Row<'_>) -> rusqlite::Result<ApprovalSummary> {
    let risk: String = row.get(7)?;
    let request: String = row.get(8)?;
    Ok(ApprovalSummary {
        id: ApprovalId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        agent_id: row.get::<_, Option<String>>(2)?.map(AgentSessionId::from),
        task_id: row.get::<_, Option<String>>(3)?.map(TaskId::from),
        thread_id: row.get(4)?,
        turn_id: row.get(5)?,
        approval_type: row.get(6)?,
        risk_level: parse_enum(&risk)?,
        request: serde_json::from_str(&request).unwrap_or(Value::Null),
        state: row.get(9)?,
        decision: row.get(10)?,
        created_at: format_timestamp(row.get(11)?),
        resolved_at: row.get::<_, Option<i64>>(12)?.map(format_timestamp),
        version: row.get::<_, i64>(13)? as u64,
    })
}

fn enum_text<T: Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_value(value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| StoreError::Validation("enum did not serialize as text".to_owned()))
}

fn sqlite_version(version: u64) -> Result<i64, StoreError> {
    i64::try_from(version)
        .map_err(|_| StoreError::Validation("resource version exceeds SQLite range".to_owned()))
}

fn parse_enum<T: serde::de::DeserializeOwned>(value: &str) -> rusqlite::Result<T> {
    serde_json::from_value(Value::String(value.to_owned())).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn normalized_prefix(path: &str) -> String {
    path.trim_start_matches("./")
        .split(['*', '?', '['])
        .next()
        .unwrap_or(path)
        .trim_end_matches('/')
        .to_owned()
}

fn format_microusd(value: i64) -> String {
    let value = value.max(0) as u64;
    format!("${}.{:02}", value / 1_000_000, (value % 1_000_000) / 10_000)
}

pub fn packet_digest<T: Serialize>(value: &T) -> Result<String, StoreError> {
    Ok(sha256(serde_json::to_string(value)?.as_bytes()))
}

pub fn operation_payload(kind: &str, target: &str) -> Value {
    json!({"kind": kind, "target": target})
}
