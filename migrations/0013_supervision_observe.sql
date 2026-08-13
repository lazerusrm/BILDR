-- The first supervisory slice is observation-only. Snapshots are immutable,
-- hash-bound receipts of existing controller state; no row in this migration
-- grants execution, worker ownership, or model authority.
CREATE TABLE IF NOT EXISTS supervisor_snapshots (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    schema_name TEXT NOT NULL CHECK(schema_name = 'harness.supervisor-snapshot.v1'),
    event_cursor INTEGER NOT NULL CHECK(event_cursor >= 0),
    trigger_kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    byte_length INTEGER NOT NULL CHECK(byte_length > 0),
    created_at INTEGER NOT NULL,
    UNIQUE(run_id, revision),
    UNIQUE(run_id, event_cursor)
);
CREATE INDEX IF NOT EXISTS idx_supervisor_snapshots_run_created
    ON supervisor_snapshots(run_id, created_at DESC);

-- This mutable watermark only records how far the observer has inspected the
-- immutable controller event stream. It is advanced after a snapshot write or
-- after explicitly classifying an event as telemetry-only.
CREATE TABLE IF NOT EXISTS supervisor_observation_cursors (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    last_event_cursor INTEGER NOT NULL CHECK(last_event_cursor >= 0),
    updated_at INTEGER NOT NULL
);

CREATE TRIGGER IF NOT EXISTS supervisor_snapshots_no_update
BEFORE UPDATE ON supervisor_snapshots BEGIN
 SELECT RAISE(ABORT, 'supervisor snapshots are immutable');
END;
CREATE TRIGGER IF NOT EXISTS supervisor_snapshots_no_delete
BEFORE DELETE ON supervisor_snapshots BEGIN
 SELECT RAISE(ABORT, 'supervisor snapshots are immutable');
END;
