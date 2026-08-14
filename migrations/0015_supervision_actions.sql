-- Closed supervisory actions and bounded expert consultations.  Decisions are
-- immutable proposals; action lifecycle and expert request lifecycle are the
-- only mutable records below.  No row grants controller authority by itself.

CREATE TABLE IF NOT EXISTS supervisor_actions (
    id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL REFERENCES supervisor_decisions(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    snapshot_id TEXT NOT NULL REFERENCES supervisor_snapshots(id),
    proposal_action_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('wait','continue_attempt','steer_active_turn','start_followup_turn','retry_fresh_attempt','spawn_explorer','spawn_reviewer','reroute_attempt','request_expert','request_replan','request_verification','queue_integration','cancel_attempt','pause_for_human','stop_run')),
    target_json TEXT NOT NULL,
    proposal_json TEXT NOT NULL,
    proposal_sha256 TEXT NOT NULL CHECK(length(proposal_sha256) = 64 AND proposal_sha256 NOT GLOB '*[^0-9a-f]*'),
    dedupe_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PROPOSED','POLICY_ACCEPTED','POLICY_REJECTED','EXECUTING','SUCCEEDED','FAILED','STALE','CANCELED')),
    policy_reason TEXT,
    execution_receipt_json TEXT,
    execution_receipt_sha256 TEXT CHECK(execution_receipt_sha256 IS NULL OR (length(execution_receipt_sha256) = 64 AND execution_receipt_sha256 NOT GLOB '*[^0-9a-f]*')),
    created_at INTEGER NOT NULL,
    evaluated_at INTEGER,
    execution_started_at INTEGER,
    completed_at INTEGER,
    UNIQUE(decision_id, proposal_action_id)
);
CREATE INDEX IF NOT EXISTS idx_supervisor_actions_run_created
    ON supervisor_actions(run_id, created_at DESC, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_supervisor_actions_active_dedupe
    ON supervisor_actions(run_id, dedupe_key)
    WHERE state IN ('POLICY_ACCEPTED','EXECUTING');

CREATE TABLE IF NOT EXISTS expert_requests (
    id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL UNIQUE REFERENCES supervisor_actions(id),
    decision_id TEXT NOT NULL REFERENCES supervisor_decisions(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    snapshot_id TEXT NOT NULL REFERENCES supervisor_snapshots(id),
    signature TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PROPOSED','POLICY_ACCEPTED','POLICY_REJECTED','QUEUED','RUNNING','COMPLETED','FAILED','INCONCLUSIVE','CANCELED','STALE')),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    requested_model TEXT NOT NULL,
    requested_effort TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    failure_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_expert_requests_run_created
    ON expert_requests(run_id, created_at DESC, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_expert_requests_active_run
    ON expert_requests(run_id)
    WHERE state IN ('QUEUED','RUNNING');
CREATE UNIQUE INDEX IF NOT EXISTS idx_expert_requests_active_signature
    ON expert_requests(signature)
    WHERE state IN ('PROPOSED','POLICY_ACCEPTED','QUEUED','RUNNING');

CREATE TABLE IF NOT EXISTS expert_responses (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE REFERENCES expert_requests(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    snapshot_id TEXT NOT NULL REFERENCES supervisor_snapshots(id),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    byte_length INTEGER NOT NULL CHECK(byte_length > 0),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_expert_responses_run_created
    ON expert_responses(run_id, created_at DESC, id DESC);

CREATE TRIGGER IF NOT EXISTS supervisor_actions_proposal_immutable
BEFORE UPDATE OF decision_id,run_id,snapshot_id,proposal_action_id,kind,target_json,proposal_json,proposal_sha256,dedupe_key,created_at ON supervisor_actions
BEGIN
 SELECT RAISE(ABORT, 'supervisor action proposal is immutable');
END;
CREATE TRIGGER IF NOT EXISTS expert_requests_custody_immutable
BEFORE UPDATE OF action_id,decision_id,run_id,snapshot_id,signature,payload_json,payload_sha256,requested_model,requested_effort,expires_at,created_at ON expert_requests
BEGIN
 SELECT RAISE(ABORT, 'expert request custody is immutable');
END;
CREATE TRIGGER IF NOT EXISTS expert_responses_no_update
BEFORE UPDATE ON expert_responses BEGIN
 SELECT RAISE(ABORT, 'expert responses are immutable');
END;
CREATE TRIGGER IF NOT EXISTS expert_responses_no_delete
BEFORE DELETE ON expert_responses BEGIN
 SELECT RAISE(ABORT, 'expert responses are immutable');
END;
