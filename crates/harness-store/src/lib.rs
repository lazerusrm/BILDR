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
    connection.execute(
        "INSERT INTO schema_migrations_meta(key, value) VALUES('runtime_schema_version', '3') ON CONFLICT(key) DO UPDATE SET value=excluded.value",
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
        assert_eq!(reopened.migration_version().unwrap(), "3");
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
}
