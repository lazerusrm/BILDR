-- Advisory supervision keeps the model's bounded assessment separate from
-- controller state.  A review is mutable only for its lifecycle; the source
-- snapshot and completed decision are append-only, hash-bound receipts.
CREATE TABLE IF NOT EXISTS supervisor_reviews (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    snapshot_id TEXT NOT NULL REFERENCES supervisor_snapshots(id),
    agent_session_id TEXT NOT NULL UNIQUE REFERENCES agent_sessions(id),
    expected_decision_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('STARTING','RUNNING','COMPLETED','FAILED','STALE')),
    trigger_kind TEXT NOT NULL,
    requested_model TEXT NOT NULL,
    requested_effort TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    failure_reason TEXT,
    UNIQUE(run_id, snapshot_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_supervisor_reviews_one_active_per_run
    ON supervisor_reviews(run_id)
    WHERE state IN ('STARTING','RUNNING');
CREATE INDEX IF NOT EXISTS idx_supervisor_reviews_run_created
    ON supervisor_reviews(run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS supervisor_decisions (
    id TEXT PRIMARY KEY,
    review_id TEXT NOT NULL UNIQUE REFERENCES supervisor_reviews(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    snapshot_id TEXT NOT NULL REFERENCES supervisor_snapshots(id),
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    policy_state TEXT NOT NULL CHECK(policy_state IN ('ADVISORY','STALE')),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    byte_length INTEGER NOT NULL CHECK(byte_length > 0),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_supervisor_decisions_run_created
    ON supervisor_decisions(run_id, created_at DESC);

CREATE TRIGGER IF NOT EXISTS supervisor_decisions_no_update
BEFORE UPDATE ON supervisor_decisions BEGIN
 SELECT RAISE(ABORT, 'supervisor decisions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS supervisor_decisions_no_delete
BEFORE DELETE ON supervisor_decisions BEGIN
 SELECT RAISE(ABORT, 'supervisor decisions are immutable');
END;
