//! SQLite/WAL persistence, raw-first event journal, and content-addressed artifacts.

mod artifacts;
mod models;
mod operator_control;
mod projection;
mod queries;
mod supervision;

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

pub use artifacts::ArtifactStore;
use harness_domain::now_ms;
pub use models::*;
pub use operator_control::*;
pub use projection::{ProjectionContext, ProtocolProjection};
pub use queries::*;
use rusqlite::{Connection, OptionalExtension};
pub use supervision::*;
use thiserror::Error;

const INITIAL_MIGRATION: &str = include_str!("../../../migrations/0001_initial.sql");
const RUNTIME_MIGRATION: &str = include_str!("../../../migrations/0002_runtime.sql");
const APPROVAL_WORKTREE_BINDING_MIGRATION: &str =
    include_str!("../../../migrations/0003_approval_worktree_binding.sql");
const ATTEMPT_CONTINUITY_MIGRATION: &str =
    include_str!("../../../migrations/0004_attempt_continuity.sql");
const ACCOUNT_ATTRIBUTION_MIGRATION: &str =
    include_str!("../../../migrations/0005_account_attribution.sql");
const SELF_IMPROVEMENT_MIGRATION: &str =
    include_str!("../../../migrations/0006_self_improvement.sql");
const FAILURE_OBSERVATION_MIGRATION: &str =
    include_str!("../../../migrations/0007_failure_observation.sql");
const EVALUATION_CUSTODY_MIGRATION: &str =
    include_str!("../../../migrations/0008_evaluation_custody.sql");
const LEARNING_POLICY_BINDING_MIGRATION: &str =
    include_str!("../../../migrations/0009_learning_policy_binding.sql");
const ARTIFACT_RUN_CUSTODY_MIGRATION: &str =
    include_str!("../../../migrations/0010_artifact_run_custody.sql");
const EVALUATION_CUSTODY_REPAIR_MIGRATION: &str =
    include_str!("../../../migrations/0011_evaluation_custody_repair.sql");
const TASK_FAILURE_REASON_MIGRATION: &str =
    include_str!("../../../migrations/0012_task_failure_reason.sql");
const SUPERVISION_OBSERVE_MIGRATION: &str =
    include_str!("../../../migrations/0013_supervision_observe.sql");
const SUPERVISION_ADVISORY_MIGRATION: &str =
    include_str!("../../../migrations/0014_supervision_advisory.sql");
const SUPERVISION_ACTIONS_MIGRATION: &str =
    include_str!("../../../migrations/0015_supervision_actions.sql");
const SUPERVISION_EXPERT_RUNTIME_MIGRATION: &str =
    include_str!("../../../migrations/0016_supervision_expert_runtime.sql");
const OPERATOR_CONTROL_MIGRATION: &str =
    include_str!("../../../migrations/0017_operator_control_plane.sql");
const RECONCILIATION_PROOF_CONSUMPTION_MIGRATION: &str =
    include_str!("../../../migrations/0018_reconciliation_proof_consumption.sql");
const NOTIFICATION_SHADOW_BATCHES_MIGRATION: &str =
    include_str!("../../../migrations/0019_notification_shadow_batches.sql");

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

    /// Narrow fault-injection seam for cross-crate recovery tests. It is
    /// compiled only when an explicit test-support feature is enabled and is
    /// unavailable to production controller code.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_set_agent_task_attempt(
        &self,
        agent_id: &harness_domain::AgentSessionId,
        attempt_id: Option<&harness_domain::AttemptId>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE agent_sessions SET task_attempt_id=?2 WHERE id=?1",
            (
                agent_id.as_str(),
                attempt_id.map(harness_domain::AttemptId::as_str),
            ),
        )?;
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::NotFound(format!("agent {agent_id}")))
        }
    }

    /// Narrow fault-injection seam for cross-crate terminal-replay tests.
    /// It deliberately removes only the run-scoped investigation completion
    /// receipt, so a test can prove that recovery fails closed instead of
    /// recreating a receipt after a simulated post-commit loss.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_delete_run_investigation_completion_receipt(
        &self,
        run_id: &harness_domain::RunId,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "DELETE FROM domain_events WHERE run_id=?1 AND aggregate_type='run' AND aggregate_id=?1 AND event_type='run.investigation.completed'",
            [run_id.as_str()],
        )?;
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::NotFound(format!(
                "run investigation completion receipt for {run_id}"
            )))
        }
    }

    /// Narrow fault-injection seam for a terminal lifecycle whose paired
    /// investigation receipts have both been lost. Production code cannot
    /// enable this feature.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_delete_investigation_completion_receipts(
        &self,
        run_id: &harness_domain::RunId,
        agent_id: &harness_domain::AgentSessionId,
    ) -> Result<(), StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let agent_changed = transaction.execute(
            "DELETE FROM domain_events WHERE run_id=?1 AND aggregate_type='agent' AND aggregate_id=?2 AND event_type='agent.investigation.artifact_recorded'",
            [run_id.as_str(), agent_id.as_str()],
        )?;
        let run_changed = transaction.execute(
            "DELETE FROM domain_events WHERE run_id=?1 AND aggregate_type='run' AND aggregate_id=?1 AND event_type='run.investigation.completed'",
            [run_id.as_str()],
        )?;
        if agent_changed != 1 || run_changed != 1 {
            return Err(StoreError::NotFound(format!(
                "paired investigation completion receipts for run {run_id} and agent {agent_id}"
            )));
        }
        transaction.commit()?;
        Ok(())
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
    // A populated v8 evaluation database has receipts that predate required
    // controller/evidence ownership. Refuse it before the generic runtime
    // migration can touch its version marker or any unrelated schema shape.
    if evaluation_custody_tables_exist(connection)?
        && !has_current_evaluation_custody(connection)?
        && legacy_evaluation_receipts_present(connection)?
    {
        return Err(StoreError::Migration(
            "legacy v8 evaluation custody contains receipts that lack controller/evidence ownership; restore a pre-v8 backup or retain this database with the matching binary".to_owned(),
        ));
    }
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
    let has_improvement_revisions: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='improvement_revisions')",
        [],
        |row| row.get(0),
    )?;
    if !has_improvement_revisions {
        let transaction = connection.transaction()?;
        transaction.execute_batch(SELF_IMPROVEMENT_MIGRATION)?;
        transaction.commit()?;
    }
    let has_failure_occurrences: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='failure_occurrences')",
        [],
        |row| row.get(0),
    )?;
    if !has_failure_occurrences {
        let transaction = connection.transaction()?;
        transaction.execute_batch(FAILURE_OBSERVATION_MIGRATION)?;
        transaction.commit()?;
    }
    let has_evaluation_runs: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='evaluation_runs')",
        [],
        |row| row.get(0),
    )?;
    if !has_evaluation_runs {
        let transaction = connection.transaction()?;
        transaction.execute_batch(EVALUATION_CUSTODY_MIGRATION)?;
        transaction.commit()?;
    }
    if !has_current_evaluation_custody(connection)? {
        let transaction = connection.transaction()?;
        transaction.execute_batch(EVALUATION_CUSTODY_REPAIR_MIGRATION)?;
        transaction.commit()?;
    }
    let has_policy_champion_bindings: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='policy_champion_bindings')",
        [],
        |row| row.get(0),
    )?;
    if !has_policy_champion_bindings {
        let transaction = connection.transaction()?;
        transaction.execute_batch(LEARNING_POLICY_BINDING_MIGRATION)?;
        transaction.commit()?;
    }
    let has_artifact_run_bindings: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='artifact_run_bindings')",
        [],
        |row| row.get(0),
    )?;
    if !has_artifact_run_bindings {
        let transaction = connection.transaction()?;
        transaction.execute_batch(ARTIFACT_RUN_CUSTODY_MIGRATION)?;
        transaction.commit()?;
    }
    if !table_has_column(connection, "tasks", "failure_reason")? {
        let transaction = connection.transaction()?;
        transaction.execute_batch(TASK_FAILURE_REASON_MIGRATION)?;
        transaction.commit()?;
    }
    let has_supervisor_snapshots: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_snapshots')",
        [],
        |row| row.get(0),
    )?;
    if !has_supervisor_snapshots {
        apply_supervision_observe_migration(connection, || Ok(()))?;
    }
    let has_supervisor_reviews: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_reviews')",
        [],
        |row| row.get(0),
    )?;
    if !has_supervisor_reviews {
        apply_supervision_advisory_migration(connection, || Ok(()))?;
    } else {
        set_runtime_schema_version(connection, "13")?;
    }
    let has_supervisor_actions: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_actions')",
        [],
        |row| row.get(0),
    )?;
    if !has_supervisor_actions {
        apply_supervision_actions_migration(connection, || Ok(()))?;
    } else {
        set_runtime_schema_version(connection, "14")?;
    }
    if !table_has_column(connection, "expert_requests", "agent_session_id")? {
        apply_supervision_expert_runtime_migration(connection, || Ok(()))?;
    } else {
        set_runtime_schema_version(connection, "15")?;
    }
    let has_attention_items: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='attention_items')",
        [],
        |row| row.get(0),
    )?;
    if !has_attention_items {
        apply_operator_control_migration(connection, || Ok(()))?;
    } else if !operator_control_schema_current(connection)? {
        return Err(StoreError::Migration(
            "operator-control storage is not the current greenfield schema; create a new database rather than applying a compatibility migration"
                .to_owned(),
        ));
    } else {
        set_runtime_schema_version(connection, "16")?;
    }
    let has_reconciliation_proof_consumptions: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='reconciliation_proof_consumptions')",
        [],
        |row| row.get(0),
    )?;
    if !has_reconciliation_proof_consumptions {
        let transaction = connection.transaction()?;
        transaction.execute_batch(RECONCILIATION_PROOF_CONSUMPTION_MIGRATION)?;
        set_runtime_schema_version(&transaction, "17")?;
        transaction.commit()?;
    } else {
        set_runtime_schema_version(connection, "17")?;
    }
    let notification_shadow_batches_table_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='notification_shadow_batches')",
        [],
        |row| row.get(0),
    )?;
    if !notification_shadow_batches_schema_current(connection)? {
        if notification_shadow_batches_table_exists {
            return Err(StoreError::Migration(
                "notification shadow batch schema is incomplete; restore a compatible backup or repair it before opening this database".to_owned(),
            ));
        }
        apply_notification_shadow_batches_migration(connection, || Ok(()))?;
    } else {
        set_runtime_schema_version(connection, "18")?;
    }
    Ok(())
}

/// Install the supervisory tables and their schema marker as one SQLite
/// transaction.  A database must never advertise v12 after only part of this
/// append-only schema has been installed.
fn apply_supervision_observe_migration<F>(
    connection: &mut Connection,
    after_schema: F,
) -> Result<(), StoreError>
where
    F: FnOnce() -> Result<(), StoreError>,
{
    let transaction = connection.transaction()?;
    transaction.execute_batch(SUPERVISION_OBSERVE_MIGRATION)?;
    // This seam deliberately permits a regression test to fail exactly after
    // DDL. Dropping the transaction then proves SQLite rolls back both the
    // schema objects and the v12 marker together.
    after_schema()?;
    set_runtime_schema_version(&transaction, "12")?;
    transaction.commit()?;
    Ok(())
}

/// Install the advisory review/decision custody tables with their schema
/// marker in one transaction.  A database must never advertise advisory
/// supervision before both the mutable review lifecycle and immutable decision
/// receipt exist.
fn apply_supervision_advisory_migration<F>(
    connection: &mut Connection,
    after_schema: F,
) -> Result<(), StoreError>
where
    F: FnOnce() -> Result<(), StoreError>,
{
    let transaction = connection.transaction()?;
    transaction.execute_batch(SUPERVISION_ADVISORY_MIGRATION)?;
    after_schema()?;
    set_runtime_schema_version(&transaction, "13")?;
    transaction.commit()?;
    Ok(())
}

/// Install the action and expert custody tables as one transaction. A running
/// binary must never advertise this schema before both action dedupe and the
/// immutable expert-response receipt exist.
fn apply_supervision_actions_migration<F>(
    connection: &mut Connection,
    after_schema: F,
) -> Result<(), StoreError>
where
    F: FnOnce() -> Result<(), StoreError>,
{
    let transaction = connection.transaction()?;
    transaction.execute_batch(SUPERVISION_ACTIONS_MIGRATION)?;
    after_schema()?;
    set_runtime_schema_version(&transaction, "14")?;
    transaction.commit()?;
    Ok(())
}

fn apply_supervision_expert_runtime_migration<F>(
    connection: &mut Connection,
    after_schema: F,
) -> Result<(), StoreError>
where
    F: FnOnce() -> Result<(), StoreError>,
{
    let transaction = connection.transaction()?;
    transaction.execute_batch(SUPERVISION_EXPERT_RUNTIME_MIGRATION)?;
    after_schema()?;
    set_runtime_schema_version(&transaction, "15")?;
    transaction.commit()?;
    Ok(())
}

/// Install the complete operator-control schema and v16 marker atomically.
/// Partial foundations are unsafe because a snapshot could otherwise claim
/// current state while the source-owned event histories do not exist.
fn apply_operator_control_migration<F>(
    connection: &mut Connection,
    after_schema: F,
) -> Result<(), StoreError>
where
    F: FnOnce() -> Result<(), StoreError>,
{
    let transaction = connection.transaction()?;
    transaction.execute_batch(OPERATOR_CONTROL_MIGRATION)?;
    after_schema()?;
    set_runtime_schema_version(&transaction, "16")?;
    transaction.commit()?;
    Ok(())
}

/// Install the immutable notification-shadow evidence table and v18 marker as
/// one transaction. A restart must not observe a schema marker for shadow
/// batching without the exact snapshot-bound evidence table and its immutable
/// guards.
fn apply_notification_shadow_batches_migration<F>(
    connection: &mut Connection,
    after_schema: F,
) -> Result<(), StoreError>
where
    F: FnOnce() -> Result<(), StoreError>,
{
    let transaction = connection.transaction()?;
    transaction.execute_batch(NOTIFICATION_SHADOW_BATCHES_MIGRATION)?;
    after_schema()?;
    set_runtime_schema_version(&transaction, "18")?;
    transaction.commit()?;
    Ok(())
}

fn notification_shadow_batches_schema_current(connection: &Connection) -> Result<bool, StoreError> {
    let required_columns = [
        "id",
        "operator_id",
        "snapshot_id",
        "snapshot_revision",
        "policy_id",
        "identity_sha256",
        "payload_json",
        "payload_sha256",
        "created_at",
    ];
    let required_triggers = [
        "notification_shadow_batches_no_update",
        "notification_shadow_batches_no_delete",
    ];
    let has_columns = required_columns
        .into_iter()
        .map(|column| table_has_column(connection, "notification_shadow_batches", column))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .all(|present| present);
    let has_triggers = required_triggers
        .into_iter()
        .map(|trigger| {
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?1)",
                [trigger],
                |row| row.get::<_, bool>(0),
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .all(|present| present);
    Ok(has_columns && has_triggers)
}

fn operator_control_schema_current(connection: &Connection) -> Result<bool, StoreError> {
    let required_columns = [
        "id",
        "delivery_id",
        "operator_id",
        "delivery_sha256",
        "presented_at",
        "payload_json",
        "payload_sha256",
    ];
    let required_triggers = [
        "notification_presentation_receipts_no_update",
        "notification_presentation_receipts_no_delete",
    ];
    let has_columns = required_columns
        .into_iter()
        .map(|column| table_has_column(connection, "notification_presentation_receipts", column))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .all(|present| present);
    let has_triggers = required_triggers
        .into_iter()
        .map(|trigger| {
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?1)",
                [trigger],
                |row| row.get::<_, bool>(0),
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .all(|present| present);
    Ok(has_columns && has_triggers)
}

fn set_runtime_schema_version(connection: &Connection, version: &str) -> Result<(), StoreError> {
    connection.execute(
        "INSERT INTO schema_migrations_meta(key, value) VALUES('runtime_schema_version', ?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [version],
    )?;
    Ok(())
}

fn has_current_evaluation_custody(connection: &Connection) -> Result<bool, StoreError> {
    [
        ("evaluation_runs", "controller_run_id"),
        ("evaluation_samples", "controller_evidence_id"),
        ("evaluation_samples", "grader_evidence_id"),
    ]
    .into_iter()
    .map(|(table, column)| table_has_column(connection, table, column))
    .collect::<Result<Vec<_>, _>>()
    .map(|columns| columns.into_iter().all(|present| present))
}

fn evaluation_custody_tables_exist(connection: &Connection) -> Result<bool, StoreError> {
    [
        "evaluation_runs",
        "evaluation_run_status_revisions",
        "evaluation_samples",
        "evaluation_stat_verdicts",
        "evaluation_invalidations",
    ]
    .into_iter()
    .map(|table| {
        connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            [table],
            |row| row.get::<_, bool>(0),
        )
    })
    .collect::<Result<Vec<_>, _>>()
    .map(|tables| tables.into_iter().all(|present| present))
    .map_err(Into::into)
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name=?2)",
            [table, column],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn legacy_evaluation_receipts_present(connection: &Connection) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM evaluation_runs
                UNION ALL SELECT 1 FROM evaluation_run_status_revisions
                UNION ALL SELECT 1 FROM evaluation_samples
                UNION ALL SELECT 1 FROM evaluation_stat_verdicts
                UNION ALL SELECT 1 FROM evaluation_invalidations
            )",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("migration safety error: {0}")]
    Migration(String),
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
    #[error(
        "trace snapshot exceeds projection bounds ({raw_receipts} raw receipts, {domain_receipts} domain receipts, {payload_bytes:?} payload bytes)"
    )]
    TraceProjectionBound {
        raw_receipts: i64,
        domain_receipts: i64,
        payload_bytes: Option<i64>,
    },
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
        assert_eq!(reopened.migration_version().unwrap(), "18");
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
        let has_failure_observations: bool = reopened
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='failure_occurrences')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_failure_observations);
        let has_operator_control: bool = reopened
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='attention_items')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_operator_control);
    }

    #[test]
    fn operator_control_schema_and_v16_marker_roll_back_together_on_failure() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        let error = apply_operator_control_migration(&mut connection, || {
            Err(StoreError::Migration(
                "injected failure after operator-control DDL".to_owned(),
            ))
        })
        .expect_err("a migration failure must roll back its schema marker");
        assert!(error.to_string().contains("injected failure"));
        let attention_table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='attention_items')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!attention_table_exists);
        assert_ne!(version, "16");
    }

    #[test]
    fn operator_control_schema_requires_current_presentation_receipts_without_a_compatibility_upgrade()
     {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(OPERATOR_CONTROL_MIGRATION)
            .unwrap();
        assert!(operator_control_schema_current(&connection).unwrap());
        connection
            .execute_batch(
                "DROP TRIGGER notification_presentation_receipts_no_update;
                 DROP TRIGGER notification_presentation_receipts_no_delete;
                 DROP TABLE notification_presentation_receipts;",
            )
            .unwrap();
        assert!(
            !operator_control_schema_current(&connection).unwrap(),
            "a partial historical shape must be refused, never altered in place"
        );
    }

    #[test]
    fn notification_shadow_schema_and_v18_marker_roll_back_together_on_failure() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(OPERATOR_CONTROL_MIGRATION)
            .unwrap();
        let error = apply_notification_shadow_batches_migration(&mut connection, || {
            Err(StoreError::Migration(
                "injected failure after notification shadow DDL".to_owned(),
            ))
        })
        .expect_err("a migration failure must roll back its schema marker");
        assert!(error.to_string().contains("injected failure"));
        let table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='notification_shadow_batches')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!table_exists);
        assert_ne!(version, "18");
    }

    #[test]
    fn incomplete_notification_shadow_schema_fails_closed_before_v18_marker() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("harness.sqlite3");
        let connection = Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection
            .execute_batch("CREATE TABLE notification_shadow_batches (id TEXT PRIMARY KEY);")
            .unwrap();
        drop(connection);
        let error = match Store::open(&database, &temp.path().join("artifacts")) {
            Ok(_) => panic!("incomplete shadow schema must not receive a v18 marker"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("notification shadow batch schema is incomplete")
        );
    }

    #[test]
    fn settings_metadata_and_receipt_roll_back_together_on_event_failure() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(
            &temp.path().join("harness.sqlite3"),
            &temp.path().join("artifacts"),
        )
        .unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_settings_receipt BEFORE INSERT ON domain_events
                 WHEN NEW.event_type='settings.updated'
                 BEGIN SELECT RAISE(FAIL, 'injected settings receipt failure'); END;",
            )
            .unwrap();

        let enabled = serde_json::json!(true);
        let result = store.update_runtime_metadata_with_settings_receipt(
            &[("settings.supervision_observe_only", &enabled)],
            &serde_json::json!({"supervision_observe_only": true}),
        );

        assert!(result.is_err());
        assert_eq!(
            store
                .runtime_metadata("settings.supervision_observe_only")
                .unwrap(),
            None
        );
        assert!(store.list_domain_events(0, None, 10).unwrap().is_empty());
    }

    #[test]
    fn supervision_schema_and_v12_marker_roll_back_together_on_failure() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let mut connection = store.connection().unwrap();
        connection
            .execute_batch(
                "DROP TRIGGER supervisor_snapshots_no_update;
                 DROP TRIGGER supervisor_snapshots_no_delete;
                 DROP TABLE supervisor_observation_cursors;
                 DROP TABLE supervisor_snapshots;
                 UPDATE schema_migrations_meta SET value='11' WHERE key='runtime_schema_version';",
            )
            .unwrap();

        let error = apply_supervision_observe_migration(&mut connection, || {
            Err(StoreError::Migration(
                "injected failure after supervisory DDL".to_owned(),
            ))
        })
        .expect_err("a migration failure must roll back its schema marker");
        assert!(error.to_string().contains("injected failure"));

        let snapshot_table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_snapshots')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!snapshot_table_exists);
        assert_eq!(version, "11");
    }

    #[test]
    fn advisory_supervision_schema_and_v13_marker_roll_back_together_on_failure() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let mut connection = store.connection().unwrap();
        connection
            .execute_batch(
                "DROP TRIGGER supervisor_decisions_no_update;
                 DROP TRIGGER supervisor_decisions_no_delete;
                 DROP TABLE supervisor_decisions;
                 DROP INDEX idx_supervisor_reviews_one_active_per_run;
                 DROP INDEX idx_supervisor_reviews_run_created;
                 DROP TABLE supervisor_reviews;
                 UPDATE schema_migrations_meta SET value='12' WHERE key='runtime_schema_version';",
            )
            .unwrap();

        let error = apply_supervision_advisory_migration(&mut connection, || {
            Err(StoreError::Migration(
                "injected failure after advisory supervisory DDL".to_owned(),
            ))
        })
        .expect_err("a migration failure must roll back advisory custody and its marker");
        assert!(error.to_string().contains("injected failure"));
        let review_table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_reviews')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!review_table_exists);
        assert_eq!(version, "12");
    }

    #[test]
    fn supervision_action_schema_and_v14_marker_roll_back_together_on_failure() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let mut connection = store.connection().unwrap();
        connection
            .execute_batch(
                "DROP TRIGGER expert_responses_no_update;
                 DROP TRIGGER expert_responses_no_delete;
                 DROP TRIGGER expert_requests_custody_immutable;
                 DROP TRIGGER supervisor_actions_proposal_immutable;
                 DROP TABLE expert_responses;
                 DROP INDEX idx_expert_requests_active_signature;
                 DROP INDEX idx_expert_requests_active_run;
                 DROP INDEX idx_expert_requests_run_created;
                 DROP TABLE expert_requests;
                 DROP INDEX idx_supervisor_actions_active_dedupe;
                 DROP INDEX idx_supervisor_actions_run_created;
                 DROP TABLE supervisor_actions;
                 UPDATE schema_migrations_meta SET value='13' WHERE key='runtime_schema_version';",
            )
            .unwrap();

        let error = apply_supervision_actions_migration(&mut connection, || {
            Err(StoreError::Migration(
                "injected failure after supervisory action DDL".to_owned(),
            ))
        })
        .expect_err("a migration failure must roll back action and expert custody");
        assert!(error.to_string().contains("injected failure"));
        let actions_table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_actions')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!actions_table_exists);
        assert_eq!(version, "13");
    }

    #[test]
    fn expert_runtime_schema_and_v15_marker_roll_back_together_on_failure() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let mut connection = store.connection().unwrap();
        connection
            .execute_batch(
                "DROP TRIGGER expert_requests_agent_binding_once;
                 DROP INDEX idx_expert_requests_agent_session;
                 ALTER TABLE expert_requests DROP COLUMN agent_session_id;
                 UPDATE schema_migrations_meta SET value='14' WHERE key='runtime_schema_version';",
            )
            .unwrap();

        let error = apply_supervision_expert_runtime_migration(&mut connection, || {
            Err(StoreError::Migration(
                "injected failure after expert runtime DDL".to_owned(),
            ))
        })
        .expect_err("a migration failure must roll back expert session custody");
        assert!(error.to_string().contains("injected failure"));
        assert!(!table_has_column(&connection, "expert_requests", "agent_session_id").unwrap());
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "14");
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

    fn improvement_input(id: &str, key: &str) -> NewImprovementRevision {
        use harness_domain::{
            ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState,
            RetentionClass, SensitivityClass,
        };
        let mut payload: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/policy-bundle.example.json"
        ))
        .unwrap();
        let mut without_self = payload.clone();
        without_self.as_object_mut().unwrap().remove("sha256");
        payload["sha256"] = serde_json::Value::String(crate::queries::sha256(
            serde_json::to_vec(&without_self).unwrap().as_slice(),
        ));
        let payload_sha256 = crate::queries::sha256(payload.to_string().as_bytes());
        NewImprovementRevision {
            id: id.to_owned(),
            aggregate_kind: ImprovementRecordKind::PolicyBundle,
            aggregate_id: "bundle-1".to_owned(),
            schema: ImprovementSchema::PolicyBundleV1,
            state: ImprovementState::Proposed,
            payload,
            payload_sha256,
            sensitivity: SensitivityClass::Internal,
            retention_class: RetentionClass::Governance,
            export_allowed: false,
            idempotency_key: key.to_owned(),
            event_id: ImprovementEventId::new(),
            source_raw_event_id: None,
            source_domain_event_id: None,
        }
    }

    #[test]
    fn improvement_append_is_idempotent_immutable_and_backupable() {
        let temp = TempDir::new().unwrap();
        let store =
            Store::open(&temp.path().join("source.sqlite3"), &temp.path().join("a")).unwrap();
        let input = improvement_input("candidate-revision-1", "candidate-key-1");
        let (revision, event) = store.append_improvement_revision(&input).unwrap();
        let (replay, replay_event) = store.append_improvement_revision(&input).unwrap();
        assert_eq!(revision.id, replay.id);
        assert_eq!(event.id, replay_event.id);
        let mut state_only = input.clone();
        state_only.id = "candidate-revision-2".to_owned();
        state_only.idempotency_key = "candidate-key-2".to_owned();
        state_only.event_id = harness_domain::ImprovementEventId::new();
        // PolicyBundle activation is deliberately unavailable through generic
        // revisions; this still proves a distinct immutable revision can
        // retain the same payload digest.
        state_only.state = harness_domain::ImprovementState::Proposed;
        let (second, _) = store.append_improvement_revision(&state_only).unwrap();
        assert_eq!(second.revision, 2);
        assert_eq!(second.payload_sha256, revision.payload_sha256);
        assert_eq!(
            store
                .list_improvement_events(revision.aggregate_kind, &revision.aggregate_id)
                .unwrap()
                .len(),
            2
        );
        assert!(
            store
                .connection()
                .unwrap()
                .execute("DELETE FROM improvement_revisions", [])
                .is_err()
        );
        let backup = temp.path().join("backup.sqlite3");
        store.backup(&backup).unwrap();
        let restored = Store::open(&backup, &temp.path().join("b")).unwrap();
        assert_eq!(
            restored
                .improvement_current_revision(revision.aggregate_kind, &revision.aggregate_id)
                .unwrap()
                .unwrap()
                .payload_sha256,
            revision.payload_sha256
        );
    }

    #[test]
    fn improvement_rejects_unknown_state_and_restricted_export() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("a")).unwrap();
        let mut input = improvement_input("candidate-revision-1", "candidate-key-1");
        input.sensitivity = harness_domain::SensitivityClass::Restricted;
        input.export_allowed = true;
        assert!(store.append_improvement_revision(&input).is_err());
        store.connection().unwrap().execute("INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES('bad','candidate','bad',1,'harness.improvement-candidate.v1','unknown','{}','0000000000000000000000000000000000000000000000000000000000000000','internal','governance',0,1)", []).unwrap();
        assert!(
            store
                .improvement_current_revision(
                    harness_domain::ImprovementRecordKind::Candidate,
                    "bad"
                )
                .is_err()
        );
    }

    #[test]
    fn promotion_wires_are_typed_on_append_and_corrupt_records_fail_closed() {
        use harness_domain::{
            ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState,
            RetentionClass, SensitivityClass,
        };

        fn canonicalize(value: &mut serde_json::Value) {
            let mut without_self = value.clone();
            without_self.as_object_mut().unwrap().remove("sha256");
            value["sha256"] = serde_json::Value::String(crate::queries::sha256(
                serde_json::to_vec(&without_self).unwrap().as_slice(),
            ));
        }
        fn input(
            id: &str,
            key: &str,
            kind: ImprovementRecordKind,
            schema: ImprovementSchema,
            state: ImprovementState,
            payload: serde_json::Value,
        ) -> NewImprovementRevision {
            NewImprovementRevision {
                id: id.into(),
                aggregate_kind: kind,
                aggregate_id: id.into(),
                schema,
                state,
                payload_sha256: crate::queries::sha256(payload.to_string().as_bytes()),
                payload,
                sensitivity: SensitivityClass::Internal,
                retention_class: RetentionClass::Governance,
                export_allowed: false,
                idempotency_key: key.into(),
                event_id: ImprovementEventId::new(),
                source_raw_event_id: None,
                source_domain_event_id: None,
            }
        }

        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let mut experiment: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/experiment.example.json"
        ))
        .unwrap();
        canonicalize(&mut experiment);
        // A self-consistent wire still cannot enter without the exact
        // candidate/champion/challenger immutable receipts.
        assert!(
            store
                .append_improvement_revision(&input(
                    "experiment-revision",
                    "experiment-key",
                    ImprovementRecordKind::Experiment,
                    ImprovementSchema::ExperimentV1,
                    ImprovementState::Running,
                    experiment.clone(),
                ))
                .is_err()
        );
        let mut malformed_experiment = experiment;
        malformed_experiment["stages"][0]["state"] = serde_json::json!("running");
        canonicalize(&mut malformed_experiment);
        assert!(
            store
                .append_improvement_revision(&input(
                    "bad-experiment-revision",
                    "bad-experiment-key",
                    ImprovementRecordKind::Experiment,
                    ImprovementSchema::ExperimentV1,
                    ImprovementState::Running,
                    malformed_experiment,
                ))
                .is_err()
        );

        let mut promotion: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/promotion-decision.example.json"
        ))
        .unwrap();
        canonicalize(&mut promotion);
        // Promotion is likewise persistence-only: it must name a stored,
        // digest-matching experiment before any later controller may act.
        assert!(
            store
                .append_improvement_revision(&input(
                    "promotion-revision",
                    "promotion-key",
                    ImprovementRecordKind::Promotion,
                    ImprovementSchema::PromotionDecisionV1,
                    ImprovementState::Decided,
                    promotion.clone(),
                ))
                .is_err()
        );
        let mut malformed_promotion = promotion;
        malformed_promotion["approvals"] = serde_json::json!([]);
        canonicalize(&mut malformed_promotion);
        assert!(
            store
                .append_improvement_revision(&input(
                    "bad-promotion-revision",
                    "bad-promotion-key",
                    ImprovementRecordKind::Promotion,
                    ImprovementSchema::PromotionDecisionV1,
                    ImprovementState::Decided,
                    malformed_promotion.clone(),
                ))
                .is_err()
        );

        // Bypass the API to prove readers do not trust an otherwise
        // discriminator/digest-consistent malformed promotion row.
        let corrupt_json = malformed_promotion.to_string();
        let corrupt_digest = crate::queries::sha256(corrupt_json.as_bytes());
        store
            .connection()
            .unwrap()
            .execute(
                "INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES(?1,'promotion','corrupt-promotion',1,'harness.promotion-decision.v1','decided',?2,?3,'internal','governance',0,1)",
                rusqlite::params!["corrupt-promotion-revision", corrupt_json, corrupt_digest],
            )
            .unwrap();
        assert!(store
            .improvement_current_revision(
                ImprovementRecordKind::Promotion,
                "corrupt-promotion",
            )
            .is_err());
    }

    #[test]
    fn v5_upgrade_creates_improvement_schema_and_accepts_records() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("v5.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute(
                "UPDATE schema_migrations_meta SET value='5' WHERE key='runtime_schema_version'",
                [],
            )
            .unwrap();
        drop(connection);
        let store = Store::open(&database, &temp.path().join("artifacts")).unwrap();
        assert_eq!(store.migration_version().unwrap(), "18");
        for name in [
            "improvement_revisions",
            "improvement_events",
            "improvement_current_revisions",
            "improvement_revisions_no_update",
        ] {
            let exists: bool = store
                .connection()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
                    [name],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing upgraded object {name}");
        }

        let failure_table: bool = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='failure_occurrences')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(failure_table);
        let input = improvement_input("v5-upgrade", "v5-upgrade-key");
        let (revision, _) = store.append_improvement_revision(&input).unwrap();
        assert_eq!(
            store
                .improvement_current_revision(revision.aggregate_kind, &revision.aggregate_id)
                .unwrap()
                .unwrap()
                .id,
            revision.id
        );
    }

    #[test]
    fn v6_upgrade_installs_failure_observation_schema_append_only_and_reopens() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("v6.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(SELF_IMPROVEMENT_MIGRATION)
            .unwrap();
        connection
            .execute(
                "UPDATE schema_migrations_meta SET value='6' WHERE key='runtime_schema_version'",
                [],
            )
            .unwrap();
        drop(connection);

        let artifacts = temp.path().join("artifacts");
        let store = Store::open(&database, &artifacts).unwrap();
        assert_eq!(store.migration_version().unwrap(), "18");
        for name in [
            "failure_occurrences",
            "failure_clusters",
            "failure_classification_revisions",
            "failure_cluster_membership_revisions",
            "failure_cluster_edits",
            "failure_occurrences_no_update",
            "failure_cluster_membership_revisions_no_delete",
            "taskset_revision_memberships",
            "evaluation_runs",
            "evaluation_run_status_revisions",
            "evaluation_samples",
            "evaluation_stat_verdicts",
            "holdout_access_log",
            "evaluation_invalidations",
            "evaluation_runs_no_update",
            "evaluation_invalidations_no_delete",
        ] {
            let exists: bool = store
                .connection()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
                    [name],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing v7 object {name}");
        }
        let repository = harness_domain::RepositoryId::from("repository-v6-upgrade");
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
            .unwrap();
        store.connection().unwrap().execute(
            "INSERT INTO failure_occurrences(id,repository_id,source_kind,source_id,terminal_code,automatic_class,severity,taxonomy_version,fingerprint_sha256,created_at) VALUES('failure-v6',?1,'run_terminal','run-v6','budget_exhausted','budget_exhausted','unknown','harness.failure-taxonomy.v1',?2,1)",
            rusqlite::params![repository.as_str(), "b".repeat(64)],
        ).unwrap();
        assert!(
            store
                .connection()
                .unwrap()
                .execute(
                    "UPDATE failure_occurrences SET severity='high' WHERE id='failure-v6'",
                    []
                )
                .is_err()
        );
        drop(store);
        let reopened = Store::open(&database, &artifacts).unwrap();
        assert_eq!(reopened.migration_version().unwrap(), "18");
        assert!(
            reopened
                .backup(&temp.path().join("v6-backup.sqlite3"))
                .is_ok()
        );
    }

    #[test]
    fn v7_upgrade_installs_evaluation_custody_schema_reopens_and_backups() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("v7.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(SELF_IMPROVEMENT_MIGRATION)
            .unwrap();
        connection
            .execute_batch(FAILURE_OBSERVATION_MIGRATION)
            .unwrap();
        connection
            .execute(
                "UPDATE schema_migrations_meta SET value='7' WHERE key='runtime_schema_version'",
                [],
            )
            .unwrap();
        drop(connection);
        let artifacts = temp.path().join("artifacts");
        let store = Store::open(&database, &artifacts).unwrap();
        assert_eq!(store.migration_version().unwrap(), "18");
        for name in [
            "taskset_revision_memberships",
            "evaluation_runs",
            "evaluation_run_status_revisions",
            "evaluation_samples",
            "evaluation_stat_verdicts",
            "holdout_access_log",
            "evaluation_invalidations",
            "evaluation_sample_membership",
            "evaluation_invalidation_target_exists",
        ] {
            let exists: bool = store
                .connection()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
                    [name],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing v8 object {name}");
        }
        assert!(store.connection().unwrap().execute(
            "INSERT INTO evaluation_invalidations(id,target_kind,target_id,reason,idempotency_key,created_at) VALUES('bad','evaluation_run','missing','fixture_drift','bad',1)", []
        ).is_err());
        let backup = temp.path().join("v8-backup.sqlite3");
        store.backup(&backup).unwrap();
        drop(store);
        assert_eq!(
            Store::open(&database, &artifacts)
                .unwrap()
                .migration_version()
                .unwrap(),
            "18"
        );
        assert!(
            Store::open(&backup, &temp.path().join("backup-artifacts"))
                .unwrap()
                .check()
                .unwrap()
                .ready
        );
    }

    #[test]
    fn v8_upgrade_installs_policy_binding_schema_reopens_and_backups() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("v8.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(SELF_IMPROVEMENT_MIGRATION)
            .unwrap();
        connection
            .execute_batch(FAILURE_OBSERVATION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(EVALUATION_CUSTODY_MIGRATION)
            .unwrap();
        connection
            .execute(
                "UPDATE schema_migrations_meta SET value='8' WHERE key='runtime_schema_version'",
                [],
            )
            .unwrap();
        drop(connection);

        let artifacts = temp.path().join("artifacts");
        let store = Store::open(&database, &artifacts).unwrap();
        assert_eq!(store.migration_version().unwrap(), "18");
        for name in [
            "policy_champion_bindings",
            "policy_current_champions",
            "policy_champion_binding_bundle",
            "policy_champion_bindings_no_update",
            "policy_champion_bindings_no_delete",
        ] {
            let exists: bool = store
                .connection()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
                    [name],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "missing v9 object {name}");
        }
        let repository = harness_domain::RepositoryId::from("repo");
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
            .unwrap();
        let mut payload: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/policy-bundle.example.json"
        ))
        .unwrap();
        let mut digest_payload = payload.clone();
        digest_payload.as_object_mut().unwrap().remove("sha256");
        payload["sha256"] = serde_json::Value::String(crate::queries::sha256(
            serde_json::to_vec(&digest_payload).unwrap().as_slice(),
        ));
        let bundle = NewImprovementRevision {
            id: "v9-policy-revision".into(),
            aggregate_kind: harness_domain::ImprovementRecordKind::PolicyBundle,
            aggregate_id: "bundle-1".into(),
            schema: harness_domain::ImprovementSchema::PolicyBundleV1,
            state: harness_domain::ImprovementState::Proposed,
            payload: payload.clone(),
            payload_sha256: crate::queries::sha256(payload.to_string().as_bytes()),
            sensitivity: harness_domain::SensitivityClass::Internal,
            retention_class: harness_domain::RetentionClass::Governance,
            export_allowed: false,
            idempotency_key: "v9-policy-key".into(),
            event_id: harness_domain::ImprovementEventId::new(),
            source_raw_event_id: None,
            source_domain_event_id: None,
        };
        store.append_improvement_revision(&bundle).unwrap();
        let binding = NewPolicyChampionBinding {
            id: "v9-binding-1".into(),
            repository_id: repository.clone(),
            task_family: "context".into(),
            model_family: None,
            runtime_class: None,
            policy_bundle_revision_id: bundle.id.clone(),
            expected_safety_anchor_digest: "a".repeat(64),
            expected_previous_binding_id: None,
            idempotency_key: "v9-binding-key".into(),
        };
        let receipt = store.bind_champion_policy(&binding).unwrap();
        assert_eq!(store.bind_champion_policy(&binding).unwrap(), receipt);
        let mut mutated_replay = binding.clone();
        mutated_replay.expected_previous_binding_id = Some("different-prior-binding".into());
        assert!(store.bind_champion_policy(&mutated_replay).is_err());
        assert_eq!(
            store
                .current_champion_policy(&repository, "context")
                .unwrap()
                .unwrap()
                .id,
            bundle.id
        );
        let scoped = NewPolicyChampionBinding {
            id: "v9-binding-scoped".into(),
            repository_id: repository.clone(),
            task_family: "context".into(),
            model_family: Some("model-a".into()),
            runtime_class: Some("runtime-a".into()),
            policy_bundle_revision_id: bundle.id.clone(),
            expected_safety_anchor_digest: "a".repeat(64),
            expected_previous_binding_id: None,
            idempotency_key: "v9-binding-scoped-key".into(),
        };
        store.bind_champion_policy(&scoped).unwrap();
        assert!(
            store
                .current_champion_policy_scoped(
                    &repository,
                    "context",
                    Some("model-a"),
                    Some("runtime-a"),
                )
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .current_champion_policy_scoped(
                    &repository,
                    "context",
                    Some("model-b"),
                    Some("runtime-a"),
                )
                .unwrap()
                .is_none()
        );
        let mut stale = binding.clone();
        stale.id = "v9-binding-stale".into();
        stale.idempotency_key = "v9-binding-stale-key".into();
        assert!(store.bind_champion_policy(&stale).is_err());
        assert!(
            store
                .connection()
                .unwrap()
                .execute(
                    "UPDATE policy_champion_bindings SET sequence=2 WHERE id='v9-binding-1'",
                    [],
                )
                .is_err()
        );
        assert!(
            store
                .connection()
                .unwrap()
                .execute(
                    "DELETE FROM policy_champion_bindings WHERE id='v9-binding-1'",
                    [],
                )
                .is_err()
        );
        let backup = temp.path().join("v9-backup.sqlite3");
        store.backup(&backup).unwrap();
        drop(store);
        assert_eq!(
            Store::open(&backup, &temp.path().join("backup-artifacts"))
                .unwrap()
                .migration_version()
                .unwrap(),
            "18"
        );
    }

    fn materialize_legacy_v8_evaluation_schema(connection: &rusqlite::Connection) {
        connection
            .execute_batch(
                "
                DROP TRIGGER IF EXISTS evaluation_run_revision_kinds;
                DROP TRIGGER IF EXISTS evaluation_sample_membership;
                DROP TABLE evaluation_samples;
                DROP TABLE evaluation_runs;
                CREATE TABLE evaluation_runs (
                    id TEXT PRIMARY KEY,
                    taskset_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
                    grader_bundle_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
                    split TEXT NOT NULL,
                    base_sha TEXT NOT NULL,
                    fixture_digest TEXT NOT NULL,
                    runtime_digest TEXT NOT NULL,
                    seed_policy_digest TEXT NOT NULL,
                    champion_policy_digest TEXT NOT NULL,
                    challenger_policy_digest TEXT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE evaluation_samples (
                    id TEXT PRIMARY KEY,
                    evaluation_run_id TEXT NOT NULL REFERENCES evaluation_runs(id),
                    eval_case_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
                    arm TEXT NOT NULL,
                    seed INTEGER NOT NULL,
                    classification TEXT NOT NULL,
                    sample_digest TEXT NOT NULL,
                    trace_digest TEXT,
                    evidence_digest TEXT,
                    artifact_digest TEXT,
                    cost_receipt_digest TEXT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL,
                    UNIQUE(evaluation_run_id, eval_case_revision_id, arm, seed)
                );
                CREATE TRIGGER evaluation_run_revision_kinds
                BEFORE INSERT ON evaluation_runs BEGIN
                    SELECT CASE WHEN (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'taskset'
                                      OR (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.grader_bundle_revision_id) <> 'grader_bundle'
                        THEN RAISE(ABORT, 'evaluation run revision kind mismatch') END;
                END;
                CREATE TRIGGER evaluation_sample_membership
                BEFORE INSERT ON evaluation_samples BEGIN
                    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM evaluation_runs r JOIN taskset_revision_memberships m ON m.taskset_revision_id=r.taskset_revision_id WHERE r.id=NEW.evaluation_run_id AND m.eval_case_revision_id=NEW.eval_case_revision_id)
                        THEN RAISE(ABORT, 'evaluation sample case is not taskset member') END;
                END;
                UPDATE schema_migrations_meta SET value='8' WHERE key='runtime_schema_version';
                ",
            )
            .unwrap();
    }

    #[test]
    fn empty_legacy_v8_evaluation_schema_is_rebuilt_before_version_is_advanced() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("legacy-v8-empty.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(SELF_IMPROVEMENT_MIGRATION)
            .unwrap();
        connection
            .execute_batch(FAILURE_OBSERVATION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(EVALUATION_CUSTODY_MIGRATION)
            .unwrap();
        materialize_legacy_v8_evaluation_schema(&connection);
        drop(connection);

        let artifacts = temp.path().join("artifacts");
        let store = Store::open(&database, &artifacts).unwrap();
        assert_eq!(store.migration_version().unwrap(), "18");
        for (table, column) in [
            ("evaluation_runs", "controller_run_id"),
            ("evaluation_samples", "controller_evidence_id"),
            ("evaluation_samples", "grader_evidence_id"),
        ] {
            assert!(table_has_column(&store.connection().unwrap(), table, column).unwrap());
        }
        for trigger in [
            "evaluation_runs_no_update",
            "evaluation_runs_no_delete",
            "evaluation_samples_no_update",
            "evaluation_samples_no_delete",
        ] {
            let present: bool = store
                .connection()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?1)",
                    [trigger],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(present, "missing repaired append-only trigger: {trigger}");
        }
        let backup = temp.path().join("legacy-v8-empty-backup.sqlite3");
        store.backup(&backup).unwrap();
        drop(store);
        assert!(
            Store::open(&backup, &temp.path().join("backup-artifacts"))
                .unwrap()
                .check()
                .unwrap()
                .ready
        );
    }

    #[test]
    fn populated_legacy_v8_evaluation_schema_fails_closed_before_version_is_advanced() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("legacy-v8-populated.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(SELF_IMPROVEMENT_MIGRATION)
            .unwrap();
        connection
            .execute_batch(FAILURE_OBSERVATION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(EVALUATION_CUSTODY_MIGRATION)
            .unwrap();
        materialize_legacy_v8_evaluation_schema(&connection);
        let legacy_version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy_version, "8");
        // The historical process had already accepted this row before the
        // controller/evidence foreign keys existed. Model that on-disk shape
        // directly; migration detection rejects the populated custody tables
        // before it considers a rebuild.
        connection
            .execute_batch("PRAGMA foreign_keys = OFF;")
            .unwrap();
        connection
            .execute(
                "INSERT INTO evaluation_runs(id,taskset_revision_id,grader_bundle_revision_id,split,base_sha,fixture_digest,runtime_digest,seed_policy_digest,champion_policy_digest,idempotency_key,created_at) VALUES('legacy-run','missing-taskset','missing-grader','development',?1,?2,?2,?2,?2,'legacy-run',1)",
                rusqlite::params!["a".repeat(40), "b".repeat(64)],
            )
            .unwrap();
        drop(connection);

        let error = match Store::open(&database, &temp.path().join("artifacts")) {
            Ok(_) => panic!("a populated legacy v8 evaluation database must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("legacy v8 evaluation custody"));
        let connection = rusqlite::Connection::open(&database).unwrap();
        let version: String = connection
            .query_row(
                "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "8");
        assert!(!table_has_column(&connection, "evaluation_runs", "controller_run_id").unwrap());
    }

    #[test]
    fn v9_upgrade_backfills_artifact_run_custody_and_reopens() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("v9.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection.execute_batch(RUNTIME_MIGRATION).unwrap();
        connection
            .execute_batch(APPROVAL_WORKTREE_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ATTEMPT_CONTINUITY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(ACCOUNT_ATTRIBUTION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(SELF_IMPROVEMENT_MIGRATION)
            .unwrap();
        connection
            .execute_batch(FAILURE_OBSERVATION_MIGRATION)
            .unwrap();
        connection
            .execute_batch(EVALUATION_CUSTODY_MIGRATION)
            .unwrap();
        connection
            .execute_batch(LEARNING_POLICY_BINDING_MIGRATION)
            .unwrap();
        connection
            .execute_batch(
                "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,default_branch,state,created_at,updated_at,version) VALUES('v9-repository','fixture',1,'fixture','/tmp','main','READY',1,1,1);
                 INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at,started_at,version) VALUES('v9-run','v9-repository','fixture','fixture','standard','none','CREATED','created','main','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','test',1,1,1,1);
                 INSERT INTO artifacts(id,run_id,kind,logical_name,storage_path,sha256,media_type,sensitivity,byte_length,retention_class,created_at,verified_at) VALUES('v9-artifact','v9-run','evaluation','fixture','/tmp/artifact','dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd','application/json','internal',1,'evaluation',1,1);
                 UPDATE schema_migrations_meta SET value='9' WHERE key='runtime_schema_version';",
            )
            .unwrap();
        drop(connection);

        let artifacts = temp.path().join("artifacts");
        let store = Store::open(&database, &artifacts).unwrap();
        assert_eq!(store.migration_version().unwrap(), "18");
        let backfilled: bool = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM artifact_run_bindings WHERE artifact_id='v9-artifact' AND run_id='v9-run')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(backfilled);
        assert!(store
            .connection()
            .unwrap()
            .execute(
                "DELETE FROM artifact_run_bindings WHERE artifact_id='v9-artifact' AND run_id='v9-run'",
                [],
            )
            .is_err());
        let backup = temp.path().join("v10-backup.sqlite3");
        store.backup(&backup).unwrap();
        drop(store);
        assert_eq!(
            Store::open(&backup, &temp.path().join("backup-artifacts"))
                .unwrap()
                .migration_version()
                .unwrap(),
            "18"
        );
    }

    #[test]
    fn evaluation_custody_partial_replay_is_exact_and_non_deadlocking() {
        use harness_domain::{
            ArtifactId, CommandRunId, ProofTier, ResultClass, ValidationId, WorktreeId,
        };

        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let repository = harness_domain::RepositoryId::from("replay-repository");
        let run = harness_domain::RunId::from("replay-run");
        store
            .create_repository(&NewRepository {
                id: repository.clone(),
                profile_id: "profile".into(),
                profile_version: 1,
                display_name: "replay".into(),
                root_path: temp.path().join("checkout"),
                origin_url: None,
                default_branch: "main".into(),
                expected_coordination_branch: None,
                state: "READY".into(),
            })
            .unwrap();
        store
            .create_run(&NewRun {
                id: run.clone(),
                repository_id: repository.clone(),
                title: "replay".into(),
                objective: "replay".into(),
                mode: "observe_only".into(),
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
            .unwrap();
        let worktree = NewWorktree {
            id: WorktreeId::from("replay-worktree"),
            run_id: run.clone(),
            task_attempt_id: None,
            kind: "evaluation_controller".into(),
            path: temp.path().join("worktree"),
            branch: None,
            base_sha: "a".repeat(40),
            head_sha: Some("a".repeat(40)),
            state: "ACTIVE".into(),
        };
        store
            .create_or_validate_evaluation_worktree(&worktree)
            .unwrap();
        store
            .create_or_validate_evaluation_worktree(&worktree)
            .unwrap();
        let mut changed_worktree = worktree.clone();
        changed_worktree.state = "DIRTY".into();
        assert!(
            store
                .create_or_validate_evaluation_worktree(&changed_worktree)
                .is_err()
        );
        store.mark_worktree_removed(&worktree.id).unwrap();
        store
            .create_or_validate_evaluation_worktree(&worktree)
            .unwrap();
        store.mark_worktree_removed(&worktree.id).unwrap();

        let command = NewCommandRecord {
            id: CommandRunId::from("replay-command"),
            run_id: run.clone(),
            task_attempt_id: None,
            agent_session_id: None,
            worktree_id: Some(worktree.id.clone()),
            command: serde_json::json!(["true"]),
            cwd: temp.path().join("worktree"),
            source_sha_before: Some("a".repeat(40)),
            source_sha_after: Some("a".repeat(40)),
            resource_class: "control".into(),
            host_identity: None,
            target_profile: None,
            started_at: 1,
            completed_at: 2,
            exit_code: Some(0),
            signal: None,
            timed_out: false,
            result_class: ResultClass::Success,
            stdout_artifact_id: None,
            stderr_artifact_id: None,
            error: None,
        };
        store
            .record_or_validate_evaluation_command(&command)
            .unwrap();
        store
            .record_or_validate_evaluation_command(&command)
            .unwrap();
        let mut changed_command = command.clone();
        changed_command.completed_at = 3;
        assert!(
            store
                .record_or_validate_evaluation_command(&changed_command)
                .is_err()
        );

        let validation = NewValidationRecord {
            id: ValidationId::from("replay-validation"),
            run_id: run.clone(),
            task_attempt_id: None,
            worktree_id: worktree.id.clone(),
            validator_id: "grader".into(),
            proof_tier: ProofTier::T2,
            source_sha: "a".repeat(40),
            selector_reason: "evaluation".into(),
            result_class: ResultClass::Success,
            command_run_id: Some(command.id.clone()),
            started_at: 1,
            completed_at: 2,
        };
        store
            .record_or_validate_evaluation_validation(&validation)
            .unwrap();
        store
            .record_or_validate_evaluation_validation(&validation)
            .unwrap();
        let mut changed_validation = validation.clone();
        changed_validation.selector_reason = "different".into();
        assert!(
            store
                .record_or_validate_evaluation_validation(&changed_validation)
                .is_err()
        );

        let stored = store.artifacts().put(b"replay artifact").unwrap();
        let artifact = ArtifactId::from("replay-artifact");
        let artifact_input = NewArtifact {
            id: artifact.clone(),
            run_id: Some(run.clone()),
            task_attempt_id: None,
            kind: "evaluation".into(),
            logical_name: "result".into(),
            storage_path: stored.path,
            sha256: stored.digest,
            media_type: "application/json".into(),
            compression: None,
            sensitivity: "internal".into(),
            byte_length: stored.byte_length,
            retention_class: "evaluation".into(),
            pinned: false,
        };
        store
            .register_or_validate_evaluation_artifact(&artifact_input)
            .unwrap();
        store
            .register_or_validate_evaluation_artifact(&artifact_input)
            .unwrap();
        let mut changed_artifact = artifact_input.clone();
        changed_artifact.logical_name = "other".into();
        assert!(
            store
                .register_or_validate_evaluation_artifact(&changed_artifact)
                .is_err()
        );
        let second_run = harness_domain::RunId::from("replay-run-second");
        store
            .create_run(&NewRun {
                id: second_run.clone(),
                repository_id: repository,
                title: "replay-two".into(),
                objective: "replay".into(),
                mode: "observe_only".into(),
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
            .unwrap();
        let mut shared_bytes = artifact_input.clone();
        shared_bytes.id = ArtifactId::from("replay-artifact-second");
        shared_bytes.run_id = Some(second_run);
        shared_bytes.logical_name = "same-bytes-other-run".into();
        assert_eq!(
            store
                .register_or_validate_evaluation_artifact(&shared_bytes)
                .unwrap(),
            artifact
        );
        let mut shared_mutation = shared_bytes.clone();
        shared_mutation.media_type = "text/plain".into();
        assert!(
            store
                .register_or_validate_evaluation_artifact(&shared_mutation)
                .is_err()
        );
        let evidence = NewEvidenceRecord {
            id: harness_domain::EvidenceId::from("replay-evidence"),
            run_id: run,
            task_attempt_id: None,
            validation_id: Some(validation.id),
            claim_id: "claim".into(),
            checklist_rows: vec!["row".into()],
            source_sha: "a".repeat(40),
            proof_tier: ProofTier::T2,
            result_class: ResultClass::Success,
            evidence: serde_json::json!({"result":"pass"}),
            unproved_claims: vec![],
        };
        let links = vec![(artifact, "grader_result".into())];
        store
            .record_or_validate_evaluation_evidence(&evidence, &links)
            .unwrap();
        store
            .record_or_validate_evaluation_evidence(&evidence, &links)
            .unwrap();
        let mut changed_evidence = evidence.clone();
        changed_evidence.claim_id = "other".into();
        assert!(
            store
                .record_or_validate_evaluation_evidence(&changed_evidence, &links)
                .is_err()
        );
        assert!(
            store
                .record_or_validate_evaluation_evidence(
                    &evidence,
                    &[(links[0].0.clone(), "different_purpose".into())],
                )
                .is_err()
        );
    }

    #[test]
    fn evaluation_custody_replays_exactly_and_fails_closed() {
        use harness_domain::{
            ImprovementEventId, ImprovementRecordKind as K, ImprovementSchema as S,
            ImprovementState as St, RetentionClass as R, SensitivityClass as C,
        };
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let stored_artifact = store.artifacts().put(b"").unwrap();
        assert_eq!(
            stored_artifact.digest,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        store.connection().unwrap().execute_batch(
            "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,default_branch,state,created_at,updated_at,version) VALUES('repo-control','fixture',1,'fixture','/tmp','main','READY',1,1,1);
             INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at,started_at,version) VALUES('run-control','repo-control','fixture','fixture','standard','none','CREATED','created','main','1111111111111111111111111111111111111111','fixture','fixture','test',1,1,1,1);
             INSERT INTO worktrees(id,run_id,kind,path,base_sha,state,created_at,version) VALUES('worktree-control','run-control','test','/tmp/eval-control','1111111111111111111111111111111111111111','READY',1,1);
             INSERT INTO artifacts(id,run_id,kind,logical_name,storage_path,sha256,media_type,sensitivity,byte_length,retention_class,created_at,verified_at) VALUES('artifact-control','run-control','test','test','/tmp/eval-artifact','e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','text/plain','internal',0,'evaluation',1,1);
             INSERT INTO command_runs(id,run_id,worktree_id,command_json,command_sha256,cwd,source_sha_before,source_sha_after,resource_class,started_at,completed_at,result_class,version) VALUES('command-control','run-control','worktree-control','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','/tmp','1111111111111111111111111111111111111111','1111111111111111111111111111111111111111','test',1,2,'success',1);
             INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,command_run_id,started_at,completed_at,version) VALUES('validation-control','run-control','worktree-control','validator','T1','1111111111111111111111111111111111111111','fixture','completed','success','command-control',1,2,1);
             INSERT INTO evidence_records(id,run_id,validation_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at) VALUES('evidence-control','run-control','validation-control','claim','[]','1111111111111111111111111111111111111111','T1','success','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','[]',1);
             INSERT INTO evidence_artifacts(evidence_id,artifact_id,purpose) VALUES('evidence-control','artifact-control','candidate-result');
             INSERT INTO command_runs(id,run_id,worktree_id,command_json,command_sha256,cwd,source_sha_before,source_sha_after,resource_class,started_at,completed_at,result_class,version) VALUES('command-grader','run-control','worktree-control','{}','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','/tmp','1111111111111111111111111111111111111111','1111111111111111111111111111111111111111','grader',1,2,'success',1);
             INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,command_run_id,started_at,completed_at,version) VALUES('validation-grader','run-control','worktree-control','grader','T1','1111111111111111111111111111111111111111','fixture','completed','success','command-grader',1,2,1);
             INSERT INTO evidence_records(id,run_id,validation_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at) VALUES('evidence-grader','run-control','validation-grader','claim','[]','1111111111111111111111111111111111111111','T1','success','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','[]',1);
             INSERT INTO evidence_artifacts(evidence_id,artifact_id,purpose) VALUES('evidence-grader','artifact-control','grader-result');",
        ).unwrap();
        let mut case: harness_eval::EvalCaseV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-case.example.json"
        ))
        .unwrap();
        case.case_id = "case-1".into();
        case.runtime.repository_id = "repo-control".into();
        let grader: harness_eval::GraderBundleV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/grader-bundle.example.json"
        ))
        .unwrap();
        case.grader_bundle_id = grader.grader_bundle_id.clone();
        case.grader_bundle_revision = grader.revision;
        case.grader_bundle_digest = grader.sha256.clone();
        case.sha256 = harness_eval::canonical_digest_without_self(&case).unwrap();
        let mut taskset: harness_eval::TasksetV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/taskset.example.json"
        ))
        .unwrap();
        taskset.taskset_id = "taskset-1".into();
        taskset.cases[0].case_id = case.case_id.clone();
        taskset.cases[0].case_digest = case.sha256.clone();
        taskset.sha256 = harness_eval::canonical_digest_without_self(&taskset).unwrap();
        let append = |id: &str, kind: K, schema: S, value: serde_json::Value| {
            let digest = crate::queries::sha256(serde_json::to_string(&value).unwrap().as_bytes());
            let aggregate_id = match &schema {
                S::EvalCaseV1 => value["case_id"].as_str().expect("case id").to_owned(),
                S::TasksetV1 => value["taskset_id"].as_str().expect("taskset id").to_owned(),
                S::GraderBundleV1 => value["grader_bundle_id"]
                    .as_str()
                    .expect("grader bundle id")
                    .to_owned(),
                S::OutcomeV1 => value["outcome_id"].as_str().expect("outcome id").to_owned(),
                _ => id.to_owned(),
            };
            store
                .append_improvement_revision(&NewImprovementRevision {
                    id: id.into(),
                    aggregate_kind: kind,
                    aggregate_id,
                    schema,
                    state: St::Proposed,
                    payload: value,
                    payload_sha256: digest,
                    sensitivity: C::Internal,
                    retention_class: R::Evaluation,
                    export_allowed: false,
                    idempotency_key: format!("key-{id}"),
                    event_id: ImprovementEventId::from(format!("event-{id}")),
                    source_raw_event_id: None,
                    source_domain_event_id: None,
                })
                .unwrap()
        };
        let (case_record, _) = append(
            "case-rev",
            K::EvalCase,
            S::EvalCaseV1,
            serde_json::to_value(&case).unwrap(),
        );
        let (taskset_record, _) = append(
            "taskset-rev",
            K::Taskset,
            S::TasksetV1,
            serde_json::to_value(&taskset).unwrap(),
        );
        let (grader_record, _) = append(
            "grader-rev",
            K::GraderBundle,
            S::GraderBundleV1,
            serde_json::to_value(&grader).unwrap(),
        );
        let immutable_case = store.immutable_eval_case_revision(&case_record.id).unwrap();
        assert_eq!(immutable_case.id, case_record.id);
        assert_eq!(immutable_case.aggregate_id, case_record.aggregate_id);
        assert_eq!(immutable_case.revision, case_record.revision);
        assert_eq!(immutable_case.payload_sha256, case_record.payload_sha256);
        assert_eq!(immutable_case.wire.sha256, case.sha256);
        store
            .append_taskset_membership(&NewTasksetMembership {
                taskset_revision_id: taskset_record.id.clone(),
                eval_case_revision_id: case_record.id.clone(),
                ordinal: 0,
            })
            .unwrap();
        let pins = store
            .evaluation_launch_pins(&taskset_record.id, &grader_record.id)
            .unwrap();
        assert_eq!(pins.taskset.id, taskset_record.id);
        assert_eq!(pins.taskset.payload_sha256, taskset_record.payload_sha256);
        assert_eq!(pins.grader_bundle.id, grader_record.id);
        assert_eq!(
            pins.grader_bundle.payload_sha256,
            grader_record.payload_sha256
        );
        assert_eq!(pins.eval_cases.len(), 1);
        assert_eq!(pins.eval_cases[0].id, case_record.id);
        assert_eq!(
            pins.eval_cases[0].payload_sha256,
            case_record.payload_sha256
        );
        assert!(
            store
                .append_taskset_membership(&NewTasksetMembership {
                    taskset_revision_id: taskset_record.id.clone(),
                    eval_case_revision_id: grader_record.id.clone(),
                    ordinal: 1
                })
                .is_err()
        );
        let run = NewEvaluationRun {
            id: "eval-1".into(),
            controller_run_id: harness_domain::RunId::from("run-control"),
            taskset_revision_id: taskset_record.id.clone(),
            grader_bundle_revision_id: grader_record.id.clone(),
            base_sha: "1".repeat(40),
            fixture_digest: "a".repeat(64),
            runtime_digest: "b".repeat(64),
            seed_policy_digest: "c".repeat(64),
            champion_policy_digest: "d".repeat(64),
            challenger_policy_digest: Some("e".repeat(64)),
            idempotency_key: "eval-key".into(),
        };
        for (suffix, repository_id, base_sha) in [
            ("foreign-repository", "other-repository", "1".repeat(40)),
            ("foreign-base", "repo-control", "2".repeat(40)),
        ] {
            let mut scoped_case = case.clone();
            scoped_case.case_id = format!("case-{suffix}");
            scoped_case.runtime.repository_id = repository_id.into();
            scoped_case.runtime.base_sha = base_sha;
            scoped_case.sha256 = harness_eval::canonical_digest_without_self(&scoped_case).unwrap();
            let (scoped_case_record, _) = append(
                &format!("case-{suffix}-rev"),
                K::EvalCase,
                S::EvalCaseV1,
                serde_json::to_value(&scoped_case).unwrap(),
            );
            let mut scoped_taskset = taskset.clone();
            scoped_taskset.taskset_id = format!("taskset-{suffix}");
            scoped_taskset.cases[0].case_id = scoped_case.case_id.clone();
            scoped_taskset.cases[0].case_digest = scoped_case.sha256.clone();
            scoped_taskset.sha256 =
                harness_eval::canonical_digest_without_self(&scoped_taskset).unwrap();
            let (scoped_taskset_record, _) = append(
                &format!("taskset-{suffix}-rev"),
                K::Taskset,
                S::TasksetV1,
                serde_json::to_value(&scoped_taskset).unwrap(),
            );
            store
                .append_taskset_membership(&NewTasksetMembership {
                    taskset_revision_id: scoped_taskset_record.id.clone(),
                    eval_case_revision_id: scoped_case_record.id,
                    ordinal: 0,
                })
                .unwrap();
            let mut scoped_run = run.clone();
            scoped_run.id = format!("eval-{suffix}");
            scoped_run.taskset_revision_id = scoped_taskset_record.id;
            scoped_run.idempotency_key = format!("eval-{suffix}-key");
            assert!(store.start_evaluation_run(&scoped_run).is_err());
        }
        let mut wrong_controller_base = run.clone();
        wrong_controller_base.id = "eval-wrong-controller-base".into();
        wrong_controller_base.base_sha = "2".repeat(40);
        wrong_controller_base.idempotency_key = "eval-wrong-controller-base".into();
        assert!(store.start_evaluation_run(&wrong_controller_base).is_err());
        assert_eq!(
            store.start_evaluation_run(&run).unwrap().split,
            harness_eval::Split::Development
        );
        assert_eq!(store.start_evaluation_run(&run).unwrap().id, "eval-1");
        // Exact replay remains legal after launch; mutations and new members do not.
        store
            .append_taskset_membership(&NewTasksetMembership {
                taskset_revision_id: taskset_record.id.clone(),
                eval_case_revision_id: case_record.id.clone(),
                ordinal: 0,
            })
            .unwrap();
        assert!(
            store
                .append_taskset_membership(&NewTasksetMembership {
                    taskset_revision_id: taskset_record.id.clone(),
                    eval_case_revision_id: case_record.id.clone(),
                    ordinal: 1
                })
                .is_err()
        );
        let mut changed = run.clone();
        changed.runtime_digest = "f".repeat(64);
        assert!(matches!(
            store.start_evaluation_run(&changed),
            Err(StoreError::Conflict(_))
        ));

        // Candidate custody is scoped by immutable controller repository and
        // base authorities, rather than by caller-supplied display fields.
        let runtime_digest = "9".repeat(64);
        let failure_digest = "8".repeat(64);
        store
            .connection()
            .unwrap()
            .execute(
                "UPDATE runs SET authority_digest=?2 WHERE id=?1",
                rusqlite::params!["run-control", runtime_digest],
            )
            .unwrap();
        store
            .connection()
            .unwrap()
            .execute(
                "INSERT INTO failure_occurrences(id,repository_id,source_kind,source_id,automatic_class,severity,taxonomy_version,fingerprint_sha256,created_at) VALUES(?1,'repo-control','run_terminal','run-control','unknown','unknown','harness.failure-taxonomy.v1',?2,1)",
                rusqlite::params!["candidate-failure", failure_digest],
            )
            .unwrap();
        let mut bundle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/policy-bundle.example.json"
        ))
        .unwrap();
        bundle["bundle_id"] = serde_json::json!("candidate-policy-revision");
        bundle["repository_id"] = serde_json::json!("repo-control");
        bundle["task_family"] = serde_json::json!(case.task_family.clone());
        let mut unsigned_bundle = bundle.clone();
        unsigned_bundle.as_object_mut().unwrap().remove("sha256");
        bundle["sha256"] = serde_json::Value::String(crate::queries::sha256(
            serde_json::to_vec(&unsigned_bundle).unwrap().as_slice(),
        ));
        let (bundle_record, _) = append(
            "candidate-policy-revision",
            K::PolicyBundle,
            S::PolicyBundleV1,
            bundle.clone(),
        );
        store
            .bind_champion_policy(&NewPolicyChampionBinding {
                id: "candidate-policy-binding".into(),
                repository_id: harness_domain::RepositoryId::from("repo-control"),
                task_family: case.task_family.clone(),
                model_family: None,
                runtime_class: None,
                policy_bundle_revision_id: bundle_record.id.clone(),
                expected_safety_anchor_digest: "a".repeat(64),
                expected_previous_binding_id: None,
                idempotency_key: "candidate-policy-binding".into(),
            })
            .unwrap();
        let bundle_wire: harness_learning::PolicyBundleV1 = serde_json::from_value(bundle).unwrap();
        let mut control_case = case.clone();
        control_case.case_id = "case-control".into();
        control_case.sha256 = harness_eval::canonical_digest_without_self(&control_case).unwrap();
        let (control_case_record, _) = append(
            "candidate-control-case-revision",
            K::EvalCase,
            S::EvalCaseV1,
            serde_json::to_value(&control_case).unwrap(),
        );
        let mut candidate_taskset = taskset.clone();
        candidate_taskset.taskset_id = "candidate-taskset".into();
        candidate_taskset.cases.push(harness_eval::CasePin {
            case_id: control_case.case_id.clone(),
            revision: control_case.revision,
            split: harness_eval::Split::Development,
            case_digest: control_case.sha256.clone(),
        });
        candidate_taskset.sha256 =
            harness_eval::canonical_digest_without_self(&candidate_taskset).unwrap();
        let (candidate_taskset_record, _) = append(
            "candidate-taskset-revision",
            K::Taskset,
            S::TasksetV1,
            serde_json::to_value(&candidate_taskset).unwrap(),
        );
        for (ordinal, eval_case_revision_id) in
            [case_record.id.clone(), control_case_record.id.clone()]
                .into_iter()
                .enumerate()
        {
            store
                .append_taskset_membership(&NewTasksetMembership {
                    taskset_revision_id: candidate_taskset_record.id.clone(),
                    eval_case_revision_id,
                    ordinal: ordinal as u64,
                })
                .unwrap();
        }
        let receipt = |kind, revision_id: String, digest: String| harness_learning::SourceReceipt {
            kind,
            revision_id,
            digest,
            split: None,
            custody: Some(harness_learning::CustodyState::Clean),
        };
        let development_receipt =
            |revision_id: String, digest: String| harness_learning::SourceReceipt {
                kind: harness_learning::ReceiptKind::EvalCase,
                revision_id,
                digest,
                split: Some(harness_learning::EvalSplit::Development),
                custody: Some(harness_learning::CustodyState::Clean),
            };
        let candidate_wire = |candidate_id: &str, repository_id: &str, base_sha: &str| {
            let mut value = harness_learning::CandidateV1 {
                schema: "harness.improvement-candidate.v1".into(),
                candidate_id: candidate_id.into(),
                scope: harness_learning::CandidateScope {
                    repository_id: repository_id.into(),
                    task_family: case.task_family.clone(),
                    model_family: None,
                    runtime_class: None,
                    base_sha: base_sha.into(),
                },
                parent_bundle: receipt(
                    harness_learning::ReceiptKind::PolicyBundle,
                    bundle_record.id.clone(),
                    bundle_wire.sha256.clone(),
                ),
                target_failure: receipt(
                    harness_learning::ReceiptKind::Failure,
                    "candidate-failure".into(),
                    failure_digest.clone(),
                ),
                development_case: development_receipt(case_record.id.clone(), case.sha256.clone()),
                no_change_control: development_receipt(
                    control_case_record.id.clone(),
                    control_case.sha256.clone(),
                ),
                taskset: receipt(
                    harness_learning::ReceiptKind::Taskset,
                    candidate_taskset_record.id.clone(),
                    candidate_taskset.sha256.clone(),
                ),
                grader_bundle: receipt(
                    harness_learning::ReceiptKind::GraderBundle,
                    grader_record.id.clone(),
                    grader.sha256.clone(),
                ),
                runtime: receipt(
                    harness_learning::ReceiptKind::Runtime,
                    "run-control".into(),
                    runtime_digest.clone(),
                ),
                hypothesis: "bounded scope".into(),
                edit: harness_learning::CandidateEdit {
                    dimension: harness_learning::ComponentDimension::TokenBudget,
                    risk_class: harness_learning::EditRisk::Green,
                    operation: harness_learning::EditOperation::Replace,
                    before_digest: "a".repeat(64),
                    after_digest: "b".repeat(64),
                },
                predictions: vec![harness_learning::Prediction {
                    signal_id: "quality".into(),
                    direction: harness_learning::PredictionDirection::Unchanged,
                    minimum_delta_milli: 0,
                }],
                evidence: vec![receipt(
                    harness_learning::ReceiptKind::Failure,
                    "candidate-failure".into(),
                    failure_digest.clone(),
                )],
                rollback_bundle: receipt(
                    harness_learning::ReceiptKind::PolicyBundle,
                    bundle_record.id.clone(),
                    bundle_wire.sha256.clone(),
                ),
                state: harness_learning::CandidateState::Proposed,
                sha256: String::new(),
            };
            let mut unsigned = serde_json::to_value(&value).unwrap();
            unsigned.as_object_mut().unwrap().remove("sha256");
            value.sha256 =
                crate::queries::sha256(serde_json::to_vec(&unsigned).unwrap().as_slice());
            value
        };
        let candidate = candidate_wire("candidate-scoped", "repo-control", &run.base_sha);
        let append_candidate = |id: &str, key: &str, candidate: harness_learning::CandidateV1| {
            let payload = serde_json::to_value(candidate).unwrap();
            store.append_improvement_revision(&NewImprovementRevision {
                id: id.into(),
                aggregate_kind: K::Candidate,
                aggregate_id: payload["candidate_id"].as_str().unwrap().into(),
                schema: S::ImprovementCandidateV1,
                state: St::Proposed,
                payload_sha256: crate::queries::sha256(
                    serde_json::to_string(&payload).unwrap().as_bytes(),
                ),
                payload,
                sensitivity: C::Internal,
                retention_class: R::Governance,
                export_allowed: false,
                idempotency_key: key.into(),
                event_id: ImprovementEventId::from(format!("event-{id}")),
                source_raw_event_id: None,
                source_domain_event_id: None,
            })
        };
        // A complete, exactly scoped candidate is accepted.
        append_candidate("candidate-scoped-rev", "candidate-scoped-key", candidate).unwrap();
        // Scope mutations cannot borrow the parent/cases/runtime from another
        // repository or base, even when their wire self-digests are valid.
        assert!(
            append_candidate(
                "candidate-cross-repo-rev",
                "candidate-cross-repo-key",
                candidate_wire("candidate-cross-repo", "other-repository", &run.base_sha),
            )
            .is_err()
        );
        assert!(
            append_candidate(
                "candidate-cross-base-rev",
                "candidate-cross-base-key",
                candidate_wire("candidate-cross-base", "repo-control", &"2".repeat(40)),
            )
            .is_err()
        );
        let mut sample: harness_eval::EvalSampleV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-sample.example.json"
        ))
        .unwrap();
        sample.case_id = case.case_id.clone();
        sample.case_revision = case.revision;
        sample.case_digest = case.sha256.clone();
        sample.taskset_digest = taskset_record.payload_sha256.clone();
        sample.grader_bundle_digest = grader_record.payload_sha256.clone();
        sample.policy_digest = "d".repeat(64);
        sample.artifact_digest.0 = Some(stored_artifact.digest);
        sample.base_sha = run.base_sha.clone();
        sample.fixture_digest = run.fixture_digest.clone();
        sample.setup_digest = case.runtime.setup_digest.clone();
        sample.runtime_digest = run.runtime_digest.clone();
        sample.sha256 = harness_eval::canonical_digest_without_self(&sample).unwrap();
        for mutation in [
            "case_id",
            "case_revision",
            "case_digest",
            "setup_digest",
            "policy_digest",
        ] {
            let mut bad = sample.clone();
            match mutation {
                "case_id" => bad.case_id = "wrong".into(),
                "case_revision" => bad.case_revision = 2,
                "case_digest" => bad.case_digest = "f".repeat(64),
                "setup_digest" => bad.setup_digest = "f".repeat(64),
                _ => bad.policy_digest = "e".repeat(64),
            };
            bad.sha256 = harness_eval::canonical_digest_without_self(&bad).unwrap();
            assert!(
                store
                    .record_evaluation_sample(&NewEvaluationSample {
                        id: format!("bad-{mutation}"),
                        evaluation_run_id: "eval-1".into(),
                        controller_evidence_id: harness_domain::EvidenceId::from(
                            "evidence-control"
                        ),
                        grader_evidence_id: harness_domain::EvidenceId::from("evidence-grader"),
                        eval_case_revision_id: case_record.id.clone(),
                        arm: EvaluationArm::Champion,
                        sample: bad,
                        idempotency_key: format!("bad-{mutation}")
                    })
                    .is_err(),
                "{mutation}"
            );
        }
        store.connection().unwrap().execute_batch(
            "INSERT INTO command_runs(id,run_id,worktree_id,command_json,command_sha256,cwd,source_sha_before,source_sha_after,resource_class,started_at,completed_at,result_class,version) VALUES('command-wrong-sha','run-control','worktree-control','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','/tmp','2222222222222222222222222222222222222222','2222222222222222222222222222222222222222','test',1,2,'success',1);
             INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,command_run_id,started_at,completed_at,version) VALUES('validation-wrong-sha','run-control','worktree-control','validator','T1','2222222222222222222222222222222222222222','fixture','completed','success','command-wrong-sha',1,2,1);
             INSERT INTO evidence_records(id,run_id,validation_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at) VALUES('evidence-wrong-sha','run-control','validation-wrong-sha','claim','[]','2222222222222222222222222222222222222222','T1','success','{}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','[]',1);
             INSERT INTO evidence_artifacts(evidence_id,artifact_id,purpose) VALUES('evidence-wrong-sha','artifact-control','result');",
        ).unwrap();
        assert!(
            store
                .record_evaluation_sample(&NewEvaluationSample {
                    id: "wrong-evidence-sha".into(),
                    evaluation_run_id: "eval-1".into(),
                    controller_evidence_id: harness_domain::EvidenceId::from("evidence-wrong-sha"),
                    grader_evidence_id: harness_domain::EvidenceId::from("evidence-grader"),
                    eval_case_revision_id: case_record.id.clone(),
                    arm: EvaluationArm::Champion,
                    sample: sample.clone(),
                    idempotency_key: "wrong-evidence-sha".into(),
                })
                .is_err()
        );
        // One chain cannot impersonate both candidate execution and grading.
        assert!(
            store
                .record_evaluation_sample(&NewEvaluationSample {
                    id: "candidate-only-pass".into(),
                    evaluation_run_id: "eval-1".into(),
                    controller_evidence_id: harness_domain::EvidenceId::from("evidence-control"),
                    grader_evidence_id: harness_domain::EvidenceId::from("evidence-control"),
                    eval_case_revision_id: case_record.id.clone(),
                    arm: EvaluationArm::Champion,
                    sample: sample.clone(),
                    idempotency_key: "candidate-only-pass".into(),
                })
                .is_err()
        );
        // A syntactically valid Pass wire is not authority: it must bind the
        // controller's completed validation/command/artifact evidence chain.
        assert!(
            store
                .record_evaluation_sample(&NewEvaluationSample {
                    id: "forged-pass".into(),
                    evaluation_run_id: "eval-1".into(),
                    controller_evidence_id: harness_domain::EvidenceId::from("forged-evidence"),
                    grader_evidence_id: harness_domain::EvidenceId::from("evidence-grader"),
                    eval_case_revision_id: case_record.id.clone(),
                    arm: EvaluationArm::Champion,
                    sample: sample.clone(),
                    idempotency_key: "forged-pass".into(),
                })
                .is_err()
        );
        let sample_receipt = store
            .record_evaluation_sample(&NewEvaluationSample {
                id: "sample-1".into(),
                evaluation_run_id: "eval-1".into(),
                controller_evidence_id: harness_domain::EvidenceId::from("evidence-control"),
                grader_evidence_id: harness_domain::EvidenceId::from("evidence-grader"),
                eval_case_revision_id: case_record.id.clone(),
                arm: EvaluationArm::Champion,
                sample: sample.clone(),
                idempotency_key: "sample-key".into(),
            })
            .unwrap();
        assert_eq!(
            store
                .record_evaluation_sample(&NewEvaluationSample {
                    id: "sample-1".into(),
                    evaluation_run_id: "eval-1".into(),
                    controller_evidence_id: harness_domain::EvidenceId::from("evidence-control"),
                    grader_evidence_id: harness_domain::EvidenceId::from("evidence-grader"),
                    eval_case_revision_id: case_record.id.clone(),
                    arm: EvaluationArm::Champion,
                    sample: sample.clone(),
                    idempotency_key: "sample-key".into(),
                })
                .unwrap()
                .id,
            sample_receipt.id
        );
        assert!(
            store
                .append_evaluation_run_status(&NewEvaluationRunStatus {
                    id: "bad-invalid".into(),
                    evaluation_run_id: "eval-1".into(),
                    status: EvaluationRunStatus::Invalidated,
                    receipt_digest: "a".repeat(64),
                    idempotency_key: "bad-invalid".into()
                })
                .is_err()
        );
        let mut holdout_case = case.clone();
        holdout_case.case_id = "case-holdout".into();
        holdout_case.split = harness_eval::Split::Holdout;
        holdout_case.sha256 = harness_eval::canonical_digest_without_self(&holdout_case).unwrap();
        let mut holdout_taskset = taskset.clone();
        holdout_taskset.taskset_id = "taskset-holdout".into();
        holdout_taskset.cases[0].case_id = holdout_case.case_id.clone();
        holdout_taskset.cases[0].case_digest = holdout_case.sha256.clone();
        holdout_taskset.cases[0].split = harness_eval::Split::Holdout;
        holdout_taskset.sha256 =
            harness_eval::canonical_digest_without_self(&holdout_taskset).unwrap();
        let (holdout_case_record, _) = append(
            "case-holdout-rev",
            K::EvalCase,
            S::EvalCaseV1,
            serde_json::to_value(&holdout_case).unwrap(),
        );
        let (holdout_taskset_record, _) = append(
            "taskset-holdout-rev",
            K::Taskset,
            S::TasksetV1,
            serde_json::to_value(&holdout_taskset).unwrap(),
        );
        store
            .append_taskset_membership(&NewTasksetMembership {
                taskset_revision_id: holdout_taskset_record.id.clone(),
                eval_case_revision_id: holdout_case_record.id,
                ordinal: 0,
            })
            .unwrap();
        let denied = store
            .record_holdout_access(&NewHoldoutAccess {
                id: "holdout-1".into(),
                taskset_revision_id: Some(holdout_taskset_record.id),
                eval_case_revision_id: None,
                principal: harness_eval::Principal::Optimizer,
                action: harness_eval::HoldoutAction::ReadAnswer,
                custody_digest: "a".repeat(64),
                idempotency_key: "holdout-key".into(),
            })
            .unwrap();
        assert!(!denied.granted);
        assert!(
            store
                .invalidate_evaluation(&NewEvaluationInvalidation {
                    id: "invalid-1".into(),
                    target: EvaluationInvalidationTarget::EvaluationRun,
                    target_id: "eval-1".into(),
                    reason: EvaluationInvalidationReason::HoldoutLeakage,
                    holdout_access_log_id: Some(denied.id),
                    idempotency_key: "invalid-key".into()
                })
                .is_ok()
        );
        assert!(matches!(
            store.evaluation_run("eval-1"),
            Ok(EvaluationRunReceipt {
                invalidated: true,
                ..
            })
        ));
        assert!(store.evaluation_sample(&sample_receipt.id).is_err());
        assert!(
            store
                .record_evaluation_sample(&NewEvaluationSample {
                    id: "sample-after-invalid".into(),
                    evaluation_run_id: "eval-1".into(),
                    controller_evidence_id: harness_domain::EvidenceId::from("evidence-control"),
                    grader_evidence_id: harness_domain::EvidenceId::from("evidence-grader"),
                    eval_case_revision_id: case_record.id.clone(),
                    arm: EvaluationArm::Champion,
                    sample: serde_json::from_str(include_str!(
                        "../../../examples/self-improvement/eval-sample.example.json"
                    ))
                    .unwrap(),
                    idempotency_key: "sample-after-invalid".into()
                })
                .is_err()
        );
        assert!(
            store
                .append_evaluation_run_status(&NewEvaluationRunStatus {
                    id: "status-after-invalid".into(),
                    evaluation_run_id: "eval-1".into(),
                    status: EvaluationRunStatus::Completed,
                    receipt_digest: "a".repeat(64),
                    idempotency_key: "status-after-invalid".into()
                })
                .is_err()
        );
        assert!(
            store
                .connection()
                .unwrap()
                .execute("UPDATE evaluation_runs SET base_sha='0'", [])
                .is_err()
        );
    }

    #[test]
    fn corrupt_improvement_rows_and_replay_provenance_fail_closed() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("a")).unwrap();
        let mut trace = harness_trace::project(&harness_trace::TraceInput {
            trace_id: "trace-self-hash".into(),
            run_id: "run-self-hash".into(),
            task_attempt_id: None,
            runtime_digest: "a".repeat(64),
            redaction_policy_digest: "b".repeat(64),
            sensitivity: "internal".into(),
            raw_events: Vec::new(),
            domain_events: Vec::new(),
            structural_receipts: vec![harness_trace::StructuralReceipt {
                id: "receipt-self-hash".into(),
                kind: "run_boundary".into(),
                occurred_at: Some(1),
                metadata: Default::default(),
            }],
            relations: Vec::new(),
        })
        .unwrap();
        trace.sha256 = "c".repeat(64);
        let payload = serde_json::to_value(trace).unwrap();
        assert!(
            store
                .append_improvement_revision(&NewImprovementRevision {
                    id: "trace-self-hash-revision".into(),
                    aggregate_kind: harness_domain::ImprovementRecordKind::Trace,
                    aggregate_id: "trace-self-hash".into(),
                    schema: harness_domain::ImprovementSchema::TraceV2,
                    state: harness_domain::ImprovementState::Captured,
                    payload: payload.clone(),
                    payload_sha256: crate::queries::sha256(
                        serde_json::to_string(&payload).unwrap().as_bytes(),
                    ),
                    sensitivity: harness_domain::SensitivityClass::Internal,
                    retention_class: harness_domain::RetentionClass::Operational,
                    export_allowed: false,
                    idempotency_key: "trace-self-hash-key".into(),
                    event_id: harness_domain::ImprovementEventId::from("trace-self-hash-event"),
                    source_raw_event_id: None,
                    source_domain_event_id: None,
                })
                .is_err()
        );
        // Bypass append validation to prove every stored TraceV2 decode also
        // rechecks the manifest self-hash before returning it to a reader.
        store.connection().unwrap().execute(
            "INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES(?1,'trace','trace-self-hash-corrupt',1,'harness.trace.v2','captured',?2,?3,'internal','operational',0,1)",
            rusqlite::params![
                "trace-self-hash-corrupt",
                serde_json::to_string(&payload).unwrap(),
                crate::queries::sha256(serde_json::to_string(&payload).unwrap().as_bytes()),
            ],
        ).unwrap();
        assert!(
            store
                .improvement_current_revision(
                    harness_domain::ImprovementRecordKind::Trace,
                    "trace-self-hash-corrupt",
                )
                .is_err()
        );
        let bad_rows = [
            (
                "kind",
                "trace",
                "harness.improvement-candidate.v1",
                "captured",
                r#"{"schema":"harness.improvement-candidate.v1"}"#,
                "internal",
                0,
            ),
            (
                "state",
                "candidate",
                "harness.improvement-candidate.v1",
                "active",
                r#"{"schema":"harness.improvement-candidate.v1"}"#,
                "internal",
                0,
            ),
            (
                "disc",
                "candidate",
                "harness.improvement-candidate.v1",
                "proposed",
                r#"{"schema":"harness.trace.v1"}"#,
                "internal",
                0,
            ),
            (
                "digest",
                "candidate",
                "harness.improvement-candidate.v1",
                "proposed",
                r#"{"schema":"harness.improvement-candidate.v1"}"#,
                "internal",
                0,
            ),
            (
                "restricted",
                "candidate",
                "harness.improvement-candidate.v1",
                "proposed",
                r#"{"schema":"harness.improvement-candidate.v1"}"#,
                "restricted",
                1,
            ),
        ];
        for (id, kind, schema, state, payload, sensitivity, export) in bad_rows {
            store.connection().unwrap().execute("INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES(?1,?2,?1,1,?3,?4,?5,?6,?7,'governance',?8,1)", rusqlite::params![id,kind,schema,state,payload, "0".repeat(64),sensitivity,export]).unwrap();
            assert!(
                store
                    .improvement_current_revision(
                        harness_domain::ImprovementRecordKind::Candidate,
                        id
                    )
                    .is_err()
                    || kind != "candidate"
            );
        }
        let input = improvement_input("replay", "replay-key");
        store.append_improvement_revision(&input).unwrap();
        let mut collision = input.clone();
        collision.event_id = harness_domain::ImprovementEventId::new();
        assert!(store.append_improvement_revision(&collision).is_err());
        let mut widened = input.clone();
        widened.id = "replay-second".to_owned();
        widened.idempotency_key = "replay-second-key".to_owned();
        widened.event_id = harness_domain::ImprovementEventId::new();
        widened.export_allowed = true;
        assert!(store.append_improvement_revision(&widened).is_err());
        for bad in [-1_i64, 0] {
            store.connection().unwrap().execute("INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES(?1,'candidate',?1,?2,'harness.improvement-candidate.v1','proposed','{\"schema\":\"harness.improvement-candidate.v1\"}',?3,'internal','governance',0,1)", rusqlite::params![format!("bad-revision-{bad}"), bad, crate::queries::sha256(b"{\"schema\":\"harness.improvement-candidate.v1\"}")]).unwrap_err();
        }
    }

    #[test]
    fn trace_snapshot_includes_domain_only_and_legacy_child_raw_rows() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("a")).unwrap();
        {
            let connection = store.connection().unwrap();
            connection
                .pragma_update(None, "foreign_keys", false)
                .unwrap();
            connection.execute_batch("INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at) VALUES('run-trace','repo','t','o','m','p','CREATED','p','main','0000000000000000000000000000000000000000','authority','profile','test',1,1);
                INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at) VALUES('run-other','repo','t','o','m','p','CREATED','p','main','0000000000000000000000000000000000000000','authority','profile','test',2,2);
                INSERT INTO agent_sessions(id,run_id,runtime_kind,role,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state) VALUES('child','run-trace','test','worker','model','low','read_only','never','/tmp','COMPLETED');
                INSERT INTO agent_sessions(id,run_id,runtime_kind,role,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state) VALUES('other-child','run-other','test','worker','model','low','read_only','never','/tmp','COMPLETED');
                INSERT INTO codex_threads(thread_id,agent_session_id,created_at,updated_at) VALUES('child-thread','child',1,1);
                INSERT INTO codex_threads(thread_id,agent_session_id,created_at,updated_at) VALUES('other-thread','other-child',1,1);").unwrap();
        }
        let payload = serde_json::json!({"value":"child"});
        store.connection().unwrap().execute("INSERT INTO raw_events(run_id,agent_session_id,thread_id,direction,method,received_at,payload_json,payload_sha256,redaction_class) VALUES(NULL,NULL,'child-thread','inbound','item/completed',1,?1,?2,'none')", rusqlite::params![payload.to_string(), crate::queries::sha256(payload.to_string().as_bytes())]).unwrap();
        let unrelated = serde_json::json!({"value":"other"});
        store.connection().unwrap().execute("INSERT INTO raw_events(run_id,agent_session_id,thread_id,direction,method,received_at,payload_json,payload_sha256,redaction_class) VALUES(NULL,NULL,'other-thread','inbound','item/completed',1,?1,?2,'none')", rusqlite::params![unrelated.to_string(), crate::queries::sha256(unrelated.to_string().as_bytes())]).unwrap();
        let stale_owner = serde_json::json!({"value":"stale-owner"});
        store.connection().unwrap().execute("INSERT INTO raw_events(run_id,agent_session_id,thread_id,direction,method,received_at,payload_json,payload_sha256,redaction_class) VALUES(NULL,'other-child','child-thread','inbound','item/completed',2,?1,?2,'none')", rusqlite::params![stale_owner.to_string(), crate::queries::sha256(stale_owner.to_string().as_bytes())]).unwrap();
        let direct = serde_json::json!({"value":"direct"});
        store.connection().unwrap().execute("INSERT INTO raw_events(run_id,agent_session_id,thread_id,direction,method,received_at,payload_json,payload_sha256,redaction_class) VALUES('run-trace','child','child-thread','inbound','item/completed',3,?1,?2,'none')", rusqlite::params![direct.to_string(), crate::queries::sha256(direct.to_string().as_bytes())]).unwrap();
        store
            .emit_domain_event(
                Some(&harness_domain::RunId::from("run-trace")),
                "run",
                "run-trace",
                "run.observed",
                &serde_json::json!({"ok":true}),
                Some(1),
            )
            .unwrap();
        let watermark = store
            .trace_projection_watermark(&harness_domain::RunId::from("run-trace"))
            .unwrap();
        let first = store
            .trace_projection_snapshot(&harness_domain::RunId::from("run-trace"))
            .unwrap();
        assert_eq!(first.raw_events.len(), 3);
        assert_eq!(watermark.base_sha, first.base_sha);
        assert_eq!(watermark.authority_digest, first.authority_digest);
        assert_eq!(watermark.profile_digest, first.profile_digest);
        assert_eq!(watermark.max_raw_event_id, first.max_raw_event_id);
        assert_eq!(watermark.max_domain_event_id, first.max_domain_event_id);
        assert_eq!(watermark.structural_digest, first.structural_digest);
        assert!(
            first
                .raw_events
                .iter()
                .all(|receipt| receipt.agent_session_id.as_deref() == Some("child"))
        );
        assert_eq!(first.domain_events.len(), 1);
        assert!(
            first
                .structural_receipts
                .iter()
                .any(|receipt| receipt.id == "agent:child")
        );
        assert!(
            first
                .relations
                .iter()
                .any(|relation| relation.from == "structural:agent:child"
                    && relation.to == "raw:1"
                    && relation.kind == "context_parent")
        );
        assert!(
            first
                .relations
                .iter()
                .any(|relation| relation.from == "raw:1"
                    && relation.to == "domain:1"
                    && relation.kind == "derived_from")
        );
        assert!(
            !first
                .relations
                .iter()
                .any(|relation| relation.kind == "next")
        );
        assert!(
            !first
                .relations
                .iter()
                .any(|relation| relation.from == "structural:agent:other-child")
        );
        assert_eq!(
            first.structural_digest,
            store
                .trace_projection_snapshot(&harness_domain::RunId::from("run-trace"))
                .unwrap()
                .structural_digest
        );
        store
            .connection()
            .unwrap()
            .execute(
                "UPDATE agent_sessions SET state='FAILED' WHERE id='child'",
                [],
            )
            .unwrap();
        let changed = store
            .trace_projection_watermark(&harness_domain::RunId::from("run-trace"))
            .unwrap();
        assert_eq!(changed.max_raw_event_id, watermark.max_raw_event_id);
        assert_eq!(changed.max_domain_event_id, watermark.max_domain_event_id);
        assert_ne!(changed.structural_digest, watermark.structural_digest);
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
