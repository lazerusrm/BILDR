PRAGMA foreign_keys = ON;

-- Runtime-only projections kept separate from the comprehensive v1 contract so
-- the original blueprint migration remains independently auditable.
CREATE TABLE IF NOT EXISTS runtime_metadata (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_rpc_requests (
    request_key TEXT PRIMARY KEY,
    rpc_id_json TEXT NOT NULL,
    method TEXT NOT NULL,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    agent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    thread_id TEXT,
    turn_id TEXT,
    item_id TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    received_at INTEGER NOT NULL,
    resolved_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_runtime_rpc_pending
    ON runtime_rpc_requests(state, received_at);

CREATE TABLE IF NOT EXISTS agent_runtime_details (
    agent_session_id TEXT PRIMARY KEY REFERENCES agent_sessions(id) ON DELETE CASCADE,
    active_turn_id TEXT,
    current_action TEXT,
    last_activity_kind TEXT,
    last_activity_at INTEGER,
    protocol_request_id TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_results (
    task_attempt_id TEXT PRIMARY KEY REFERENCES task_attempts(id) ON DELETE CASCADE,
    verified_commit_sha TEXT,
    verifier_verdict TEXT,
    diff_files INTEGER NOT NULL DEFAULT 0,
    diff_additions INTEGER NOT NULL DEFAULT 0,
    diff_deletions INTEGER NOT NULL DEFAULT 0,
    unexpected_paths_json TEXT NOT NULL DEFAULT '[]',
    validation_summary_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_exports (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    artifact_id TEXT REFERENCES artifacts(id),
    state TEXT NOT NULL,
    manifest_sha256 TEXT,
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    error_json TEXT
);

CREATE TABLE IF NOT EXISTS repository_locks (
    repository_id TEXT PRIMARY KEY REFERENCES repositories(id) ON DELETE CASCADE,
    owner_instance_id TEXT NOT NULL,
    acquired_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ui_preferences (
    session_id TEXT PRIMARY KEY,
    preferences_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO schema_migrations_meta(key, value)
VALUES ('runtime_schema_version', '2')
ON CONFLICT(key) DO UPDATE SET value=excluded.value;
