//! SQLite/WAL persistence, raw-first event journal, and content-addressed artifacts.

mod artifacts;
mod models;
mod projection;
mod queries;

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

pub use artifacts::ArtifactStore;
use harness_domain::now_ms;
pub use models::*;
pub use projection::{ProjectionContext, ProtocolProjection};
pub use queries::*;
use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;

const INITIAL_MIGRATION: &str = include_str!("../../../migrations/0001_initial.sql");
const RUNTIME_MIGRATION: &str = include_str!("../../../migrations/0002_runtime.sql");
const APPROVAL_WORKTREE_BINDING_MIGRATION: &str =
    include_str!("../../../migrations/0003_approval_worktree_binding.sql");
const ATTEMPT_CONTINUITY_MIGRATION: &str =
    include_str!("../../../migrations/0004_attempt_continuity.sql");
const ACCOUNT_ATTRIBUTION_MIGRATION: &str =
    include_str!("../../../migrations/0005_account_attribution.sql");

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
    database_path: Arc<PathBuf>,
    artifacts: ArtifactStore,
}

impl Store {
    pub fn open(database_path: &Path, artifact_root: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(database_path)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "busy_timeout", 5_000_i64)?;
        connection.pragma_update(None, "temp_store", "MEMORY")?;

        let has_schema: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations_meta')",
            [],
            |row| row.get(0),
        )?;
        if !has_schema {
            let transaction = connection.transaction()?;
            transaction.execute_batch(INITIAL_MIGRATION)?;
            transaction.commit()?;
        }
        apply_runtime_migrations(&mut connection)?;
        connection.pragma_update(None, "wal_autocheckpoint", 1_000_i64)?;
        fs::set_permissions(database_path, fs::Permissions::from_mode(0o600))?;
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = database_path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            if sidecar.exists() {
                fs::set_permissions(sidecar, fs::Permissions::from_mode(0o600))?;
            }
        }

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            database_path: Arc::new(database_path.to_path_buf()),
            artifacts: ArtifactStore::new(artifact_root)?,
        })
    }

    pub fn in_memory(artifact_root: &Path) -> Result<Self, StoreError> {
        let mut connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", true)?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(INITIAL_MIGRATION)?;
        transaction.commit()?;
        apply_runtime_migrations(&mut connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            database_path: Arc::new(PathBuf::from(":memory:")),
            artifacts: ArtifactStore::new(artifact_root)?,
        })
    }

    pub(crate) fn connection(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }

    #[must_use]
    pub fn artifacts(&self) -> &ArtifactStore {
        &self.artifacts
    }

    #[must_use]
    pub fn database_path(&self) -> &Path {
        self.database_path.as_ref()
    }

    pub fn check(&self) -> Result<DatabaseHealth, StoreError> {
        let connection = self.connection()?;
        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let schema_version: Option<String> = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let raw_event_count =
            connection.query_row("SELECT count(*) FROM raw_events", [], |row| row.get(0))?;
        let projection_lag = connection.query_row(
            "SELECT max((SELECT coalesce(max(id),0) FROM raw_events) - coalesce((SELECT min(last_raw_event_id) FROM projector_checkpoints), (SELECT coalesce(max(id),0) FROM raw_events)), 0)",
            [],
            |row| row.get(0),
        )?;
        Ok(DatabaseHealth {
            ready: integrity == "ok",
            integrity,
            journal_mode,
            schema_version: schema_version.unwrap_or_else(|| "1".to_owned()),
            raw_event_count,
            projection_lag,
        })
    }

    pub fn status(&self) -> Result<DatabaseHealth, StoreError> {
        let connection = self.connection()?;
        let _: i64 = connection.query_row("SELECT 1", [], |row| row.get(0))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let schema_version: Option<String> = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let raw_event_count =
            connection.query_row("SELECT coalesce(max(id),0) FROM raw_events", [], |row| {
                row.get(0)
            })?;
        let projection_lag = connection.query_row(
            "SELECT max((SELECT coalesce(max(id),0) FROM raw_events) - coalesce((SELECT min(last_raw_event_id) FROM projector_checkpoints), (SELECT coalesce(max(id),0) FROM raw_events)), 0)",
            [],
            |row| row.get(0),
        )?;
        Ok(DatabaseHealth {
            ready: true,
            integrity: "not checked during live status".to_owned(),
            journal_mode,
            schema_version: schema_version.unwrap_or_else(|| "1".to_owned()),
            raw_event_count,
            projection_lag,
        })
    }

    pub fn backup(&self, output: &Path) -> Result<(), StoreError> {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = self.connection()?;
        let mut destination = Connection::open(output)?;
        let backup = rusqlite::backup::Backup::new(&connection, &mut destination)?;
        backup.run_to_completion(64, std::time::Duration::from_millis(5), None)?;
        drop(backup);
        destination.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(destination);
        fs::set_permissions(output, fs::Permissions::from_mode(0o600))?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), StoreError> {
        self.connection()?
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }

    pub fn migration_version(&self) -> Result<String, StoreError> {
        Ok(self.connection()?.query_row(
            "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn touch_projector(&self, name: &str, raw_event_id: i64) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO projector_checkpoints(projector_name,last_raw_event_id,updated_at) VALUES(?1,?2,?3) ON CONFLICT(projector_name) DO UPDATE SET last_raw_event_id=max(last_raw_event_id,excluded.last_raw_event_id),updated_at=excluded.updated_at",
            (name, raw_event_id, now_ms()),
        )?;
        Ok(())
    }
}

fn apply_runtime_migrations(connection: &mut Connection) -> Result<(), StoreError> {
    connection.execute_batch(RUNTIME_MIGRATION)?;
    let has_worktree_fingerprint: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('approvals') WHERE name='expected_worktree_fingerprint')",
        [],
        |row| row.get(0),
    )?;
    if !has_worktree_fingerprint {
        let transaction = connection.transaction()?;
        transaction.execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)?;
        transaction.commit()?;
    }
    let has_context_strategy: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('agent_runtime_details') WHERE name='context_strategy')",
        [],
        |row| row.get(0),
    )?;
    if !has_context_strategy {
        let transaction = connection.transaction()?;
        transaction.execute_batch(ATTEMPT_CONTINUITY_MIGRATION)?;
        transaction.commit()?;
    }
    let has_codex_account_id: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('agent_sessions') WHERE name='codex_account_id')",
        [],
        |row| row.get(0),
    )?;
    if !has_codex_account_id {
        let transaction = connection.transaction()?;
        transaction.execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)?;
        transaction.commit()?;
    }
    connection.execute(
        "INSERT INTO schema_migrations_meta(key, value) VALUES('runtime_schema_version', '5') ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [],
    )?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("database connection mutex was poisoned")]
    Poisoned,
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("state conflict: {0}")]
    Conflict(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("artifact integrity failure: {0}")]
    ArtifactIntegrity(String),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn migration_and_restart_are_idempotent() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("harness.sqlite3");
        let artifacts = temp.path().join("artifacts");
        let store = Store::open(&database, &artifacts).unwrap();
        assert!(store.check().unwrap().ready);
        drop(store);
        let reopened = Store::open(&database, &artifacts).unwrap();
        assert_eq!(reopened.migration_version().unwrap(), "5");
        let has_worktree_fingerprint: bool = reopened
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('approvals') WHERE name='expected_worktree_fingerprint')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_worktree_fingerprint);
        let has_context_strategy: bool = reopened
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('agent_runtime_details') WHERE name='context_strategy')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_context_strategy);
        let has_codex_account_id: bool = reopened
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('agent_sessions') WHERE name='codex_account_id')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_codex_account_id);
    }

    #[test]
    fn online_backup_restores() {
        let temp = TempDir::new().unwrap();
        let store =
            Store::open(&temp.path().join("source.sqlite3"), &temp.path().join("a")).unwrap();
        let backup = temp.path().join("backup.sqlite3");
        store.backup(&backup).unwrap();
        let restored = Store::open(&backup, &temp.path().join("b")).unwrap();
        assert!(restored.check().unwrap().ready);
    }

    #[test]
    fn proposed_plan_cannot_be_approved_before_certification() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let plan = serde_json::to_string(&harness_domain::RunPlan {
            schema: "harness.orchestration.plan.v1".to_owned(),
            summary: "Focused plan-state fixture".to_owned(),
            tasks: vec![],
        })
        .unwrap();
        {
            let connection = store.connection().unwrap();
            connection
                .pragma_update(None, "foreign_keys", false)
                .unwrap();
            connection
                .execute(
                    "INSERT INTO run_plan_revisions(id,run_id,revision,plan_json,plan_sha256,state,created_at) VALUES('plan-1','run-1',1,?1,'digest','PROPOSED',1)",
                    [plan],
                )
                .unwrap();
        }
        let run_id = harness_domain::RunId::from("run-1");
        assert!(store.approve_latest_plan(&run_id, "automatic").is_err());

        store.certify_latest_plan(&run_id).unwrap();
        store.approve_latest_plan(&run_id, "automatic").unwrap();
        assert_eq!(store.latest_plan(&run_id).unwrap().unwrap().2, "APPROVED");
    }

    #[test]
    fn replacement_revision_removes_only_unapproved_task_rows() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let old_plan = serde_json::to_string(&harness_domain::RunPlan {
            schema: "harness.orchestration.plan.v1".to_owned(),
            summary: "Rejected plan".to_owned(),
            tasks: vec![],
        })
        .unwrap();
        {
            let connection = store.connection().unwrap();
            connection
                .pragma_update(None, "foreign_keys", false)
                .unwrap();
            connection
                .execute(
                    "INSERT INTO run_plan_revisions(id,run_id,revision,plan_json,plan_sha256,state,created_at) VALUES('old-plan','run-1',1,?1,'old-digest','REVISION_REQUIRED',1)",
                    [old_plan],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at,version) VALUES('old-task','run-1','old-plan','ROOT','Old task','Old objective','P0','governor','verifier','PROPOSED',1,1,1)",
                    [],
                )
                .unwrap();
        }
        let run_id = harness_domain::RunId::from("run-1");
        let replacement = harness_domain::RunPlan {
            schema: "harness.orchestration.plan.v1".to_owned(),
            summary: "Replacement plan".to_owned(),
            tasks: vec![],
        };
        store
            .store_plan(
                &run_id,
                &harness_domain::AgentSessionId::from("architect-2"),
                &replacement,
            )
            .unwrap();

        let connection = store.connection().unwrap();
        let old_tasks: i64 = connection
            .query_row(
                "SELECT count(*) FROM tasks WHERE plan_revision_id='old-plan'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let old_state: String = connection
            .query_row(
                "SELECT state FROM run_plan_revisions WHERE id='old-plan'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(old_tasks, 0);
        assert_eq!(old_state, "SUPERSEDED");
    }

    #[test]
    fn task_governor_usage_excludes_independent_review() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        {
            let connection = store.connection().unwrap();
            connection
                .pragma_update(None, "foreign_keys", false)
                .unwrap();
            connection
                .execute_batch(
                    "INSERT INTO task_attempts(id,task_id,attempt_number,state,task_packet_json,task_packet_sha256,base_sha,requested_model_route,created_at,updated_at) VALUES
                       ('attempt-1','task-a',1,'COMPLETED','{}','digest','base','governor',1,1),
                       ('attempt-2','task-a',2,'RUNNING','{}','digest','base','governor',2,2);
                     INSERT INTO agent_sessions(id,run_id,task_attempt_id,parent_agent_session_id,runtime_kind,role,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state,goal_tokens_used) VALUES
                       ('governor-1','run-a','attempt-1',NULL,'codex_controller','governor','gpt-test','high','workspace_write','never','/repo','COMPLETED',3000000),
                       ('governor-child','run-a','attempt-1','governor-1','codex_native_subagent','explorer','gpt-test','medium','read_only','never','/repo','COMPLETED',1000000),
                       ('verifier','run-a','attempt-1',NULL,'codex_controller','verifier','gpt-test','high','read_only','never','/repo','COMPLETED',7000000),
                       ('verifier-child','run-a','attempt-1','verifier','codex_native_subagent','explorer','gpt-test','medium','read_only','never','/repo','COMPLETED',9000000),
                       ('governor-2','run-a','attempt-2',NULL,'codex_controller','governor','gpt-test','high','workspace_write','never','/repo','RUNNING',2000000);",
                )
                .unwrap();
        }
        assert_eq!(
            store
                .task_governor_usage(&harness_domain::TaskId::from("task-a"))
                .unwrap(),
            6_000_000
        );
    }

    #[test]
    fn worktree_removal_state_waits_for_path_lease_release() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        {
            let connection = store.connection().unwrap();
            connection
                .pragma_update(None, "foreign_keys", false)
                .unwrap();
            connection
                .execute_batch(
                    "INSERT INTO worktrees(id,run_id,task_attempt_id,kind,path,base_sha,state,created_at,version)
                       VALUES('worktree-a','run-a','attempt-a','task','/tmp/worktree-a','base','PRESERVED',1,1);
                     INSERT INTO path_leases(id,run_id,task_attempt_id,path_glob,normalized_prefix,lease_kind,base_sha,acquired_at,heartbeat_at,expires_at)
                       VALUES('lease-a','run-a','attempt-a','src/**','src','write','base',1,1,9223372036854775807);",
                )
                .unwrap();
        }
        let worktree_id = harness_domain::WorktreeId::from("worktree-a");
        let attempt_id = harness_domain::AttemptId::from("attempt-a");
        assert!(store.worktree_has_active_path_lease(&worktree_id).unwrap());

        store
            .release_path_leases(&attempt_id, "fixture completed")
            .unwrap();
        assert!(!store.worktree_has_active_path_lease(&worktree_id).unwrap());
        store.mark_worktree_removed(&worktree_id).unwrap();

        let worktree = store
            .list_worktrees(None)
            .unwrap()
            .into_iter()
            .find(|worktree| worktree.id == worktree_id)
            .unwrap();
        assert_eq!(worktree.state, "REMOVED");
        let removed_at: Option<i64> = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT removed_at FROM worktrees WHERE id='worktree-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(removed_at.is_some());
    }
}
