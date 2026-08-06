PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;

-- Timestamps are Unix epoch milliseconds unless a field explicitly says otherwise.
-- Monetary values use integer micro-USD. Token price rates are micro-USD per 1M tokens.

CREATE TABLE repositories (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    profile_version INTEGER NOT NULL,
    display_name TEXT NOT NULL,
    root_path TEXT NOT NULL UNIQUE,
    origin_url TEXT,
    default_branch TEXT NOT NULL,
    expected_coordination_branch TEXT,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE repository_health_snapshots (
    id TEXT PRIMARY KEY,
    repository_id TEXT NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    observed_at INTEGER NOT NULL,
    primary_branch TEXT,
    primary_head_sha TEXT,
    primary_clean INTEGER NOT NULL,
    origin_head_sha TEXT,
    git_identity_name_present INTEGER NOT NULL,
    git_identity_email_present INTEGER NOT NULL,
    authority_digest TEXT,
    blockers_json TEXT NOT NULL,
    details_json TEXT NOT NULL
);
CREATE INDEX idx_repository_health_repo_time
    ON repository_health_snapshots(repository_id, observed_at DESC);

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    title TEXT NOT NULL,
    requested_objective TEXT NOT NULL,
    mode TEXT NOT NULL,
    publication_mode TEXT NOT NULL,
    state TEXT NOT NULL,
    phase TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    integration_branch TEXT,
    integration_sha TEXT,
    authority_digest TEXT NOT NULL,
    profile_digest TEXT NOT NULL,
    codex_version TEXT,
    protocol_schema_sha256 TEXT,
    requested_by TEXT NOT NULL,
    scheduler_paused INTEGER NOT NULL DEFAULT 0,
    run_token_budget INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    failure_class TEXT,
    failure_reason TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_runs_repository_created
    ON runs(repository_id, created_at DESC);
CREATE INDEX idx_runs_state
    ON runs(state, updated_at) WHERE state NOT IN ('COMPLETED', 'CANCELED', 'FAILED', 'ARCHIVED');

CREATE TABLE run_plan_revisions (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    architect_agent_session_id TEXT,
    plan_json TEXT NOT NULL,
    plan_sha256 TEXT NOT NULL,
    state TEXT NOT NULL,
    edited_by TEXT,
    created_at INTEGER NOT NULL,
    approved_at INTEGER,
    approved_by TEXT,
    UNIQUE(run_id, revision),
    UNIQUE(run_id, plan_sha256)
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    plan_revision_id TEXT NOT NULL REFERENCES run_plan_revisions(id),
    external_task_id TEXT NOT NULL,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    priority TEXT NOT NULL,
    owner_profile TEXT NOT NULL,
    reviewer_profile TEXT NOT NULL,
    state TEXT NOT NULL,
    current_attempt_number INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(run_id, external_task_id)
);
CREATE INDEX idx_tasks_run_state ON tasks(run_id, state, priority);

CREATE TABLE task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    expected_dependency_sha TEXT,
    PRIMARY KEY(task_id, depends_on_task_id),
    CHECK(task_id <> depends_on_task_id)
);

CREATE TABLE task_attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    task_packet_json TEXT NOT NULL,
    task_packet_sha256 TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    head_sha TEXT,
    requested_model_route TEXT NOT NULL,
    token_budget INTEGER,
    tool_budget INTEGER,
    diff_file_budget INTEGER,
    diff_line_budget INTEGER,
    started_at INTEGER,
    completed_at INTEGER,
    terminal_class TEXT,
    failure_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(task_id, attempt_number)
);
CREATE INDEX idx_attempts_task_time ON task_attempts(task_id, attempt_number DESC);
CREATE INDEX idx_attempts_state ON task_attempts(state);

CREATE TABLE worktrees (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    branch TEXT,
    base_sha TEXT NOT NULL,
    head_sha TEXT,
    state TEXT NOT NULL,
    preserved_reason TEXT,
    detected_external_mutation INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    reconciled_at INTEGER,
    removed_at INTEGER,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_worktrees_run_state ON worktrees(run_id, state);

CREATE TABLE path_leases (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    agent_session_id TEXT,
    path_glob TEXT NOT NULL,
    normalized_prefix TEXT NOT NULL,
    lease_kind TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    acquired_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    released_at INTEGER,
    release_reason TEXT
);
CREATE INDEX idx_path_leases_active
    ON path_leases(run_id, normalized_prefix, expires_at)
    WHERE released_at IS NULL;

CREATE TABLE resource_leases (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE CASCADE,
    command_run_id TEXT,
    resource_class TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    acquired_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    released_at INTEGER,
    release_reason TEXT
);
CREATE INDEX idx_resource_leases_active
    ON resource_leases(resource_class, resource_key, expires_at)
    WHERE released_at IS NULL;

CREATE TABLE agent_sessions (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    parent_agent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    runtime_kind TEXT NOT NULL,
    role TEXT NOT NULL,
    nickname TEXT,
    requested_model TEXT NOT NULL,
    effective_model TEXT,
    requested_reasoning_effort TEXT NOT NULL,
    effective_reasoning_effort TEXT,
    sandbox_mode TEXT NOT NULL,
    approval_policy TEXT NOT NULL,
    cwd TEXT NOT NULL,
    state TEXT NOT NULL,
    current_goal TEXT,
    goal_status TEXT,
    token_budget INTEGER,
    goal_tokens_used INTEGER,
    goal_time_used_seconds INTEGER,
    started_at INTEGER,
    completed_at INTEGER,
    last_heartbeat_at INTEGER,
    failure_class TEXT,
    failure_reason TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_agents_run_state ON agent_sessions(run_id, state);
CREATE INDEX idx_agents_parent ON agent_sessions(parent_agent_session_id);

-- Deferred reference from run_plan_revisions after agent_sessions exists.
CREATE INDEX idx_plan_architect ON run_plan_revisions(architect_agent_session_id);

CREATE TABLE codex_threads (
    thread_id TEXT PRIMARY KEY,
    agent_session_id TEXT NOT NULL UNIQUE REFERENCES agent_sessions(id) ON DELETE CASCADE,
    session_id TEXT,
    parent_thread_id TEXT,
    source_kind TEXT,
    service_name TEXT,
    git_branch TEXT,
    git_sha TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_codex_threads_parent ON codex_threads(parent_thread_id);

CREATE TABLE codex_turns (
    turn_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES codex_threads(thread_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    requested_model TEXT,
    effective_model TEXT,
    requested_reasoning_effort TEXT,
    effective_reasoning_effort TEXT,
    trace_id TEXT,
    started_at INTEGER,
    completed_at INTEGER,
    duration_ms INTEGER,
    time_to_first_token_ms INTEGER,
    error_json TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_codex_turns_thread_time ON codex_turns(thread_id, started_at);

CREATE TABLE raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    agent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    thread_id TEXT,
    turn_id TEXT,
    direction TEXT NOT NULL,
    method TEXT NOT NULL,
    request_id TEXT,
    received_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    source_sequence TEXT,
    redaction_class TEXT NOT NULL DEFAULT 'none',
    UNIQUE(thread_id, source_sequence)
);
CREATE INDEX idx_raw_events_run_id_id ON raw_events(run_id, id);
CREATE INDEX idx_raw_events_thread_id_id ON raw_events(thread_id, id);
CREATE INDEX idx_raw_events_method ON raw_events(method, received_at);

CREATE TABLE projector_checkpoints (
    projector_name TEXT PRIMARY KEY,
    last_raw_event_id INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE domain_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    source_raw_event_id INTEGER REFERENCES raw_events(id),
    UNIQUE(aggregate_type, aggregate_id, event_type, source_raw_event_id)
);
CREATE INDEX idx_domain_events_run_id ON domain_events(run_id, id);
CREATE INDEX idx_domain_events_aggregate ON domain_events(aggregate_type, aggregate_id, id);

CREATE TABLE projected_items (
    item_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    turn_id TEXT,
    item_type TEXT NOT NULL,
    state TEXT NOT NULL,
    summary TEXT,
    payload_json TEXT NOT NULL,
    source_raw_event_id INTEGER REFERENCES raw_events(id),
    started_at INTEGER,
    completed_at INTEGER,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_projected_items_thread ON projected_items(thread_id, started_at);

CREATE TABLE context_packets (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    profile_digest TEXT NOT NULL,
    packet_json TEXT NOT NULL,
    packet_sha256 TEXT NOT NULL,
    estimated_tokens INTEGER,
    created_at INTEGER NOT NULL,
    UNIQUE(task_attempt_id, role, packet_sha256)
);

CREATE TABLE context_sources (
    context_packet_id TEXT NOT NULL REFERENCES context_packets(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    source_class TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    included INTEGER NOT NULL,
    reason TEXT,
    estimated_tokens INTEGER,
    PRIMARY KEY(context_packet_id, path)
);

CREATE TABLE token_samples (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    turn_id TEXT,
    requested_model TEXT,
    effective_model TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL CHECK(input_tokens >= 0),
    cached_input_tokens INTEGER NOT NULL CHECK(cached_input_tokens >= 0),
    cache_write_input_tokens INTEGER CHECK(cache_write_input_tokens >= 0),
    output_tokens INTEGER NOT NULL CHECK(output_tokens >= 0),
    reasoning_output_tokens INTEGER NOT NULL CHECK(reasoning_output_tokens >= 0),
    total_tokens INTEGER NOT NULL CHECK(total_tokens >= 0),
    model_context_window INTEGER,
    sample_kind TEXT NOT NULL,
    source_event_id INTEGER REFERENCES raw_events(id),
    CHECK(cached_input_tokens <= input_tokens),
    CHECK(reasoning_output_tokens <= output_tokens)
);
CREATE INDEX idx_token_samples_thread ON token_samples(thread_id, observed_at);
CREATE UNIQUE INDEX idx_token_sample_turn_kind
    ON token_samples(turn_id, sample_kind)
    WHERE turn_id IS NOT NULL;

CREATE TABLE pricing_snapshots (
    id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    currency TEXT NOT NULL CHECK(currency = 'USD'),
    effective_at INTEGER NOT NULL,
    input_microusd_per_million INTEGER NOT NULL,
    cached_input_microusd_per_million INTEGER NOT NULL,
    output_microusd_per_million INTEGER NOT NULL,
    cache_write_multiplier_numerator INTEGER NOT NULL,
    cache_write_multiplier_denominator INTEGER NOT NULL,
    long_context_threshold_tokens INTEGER,
    long_context_input_multiplier_numerator INTEGER,
    long_context_input_multiplier_denominator INTEGER,
    long_context_output_multiplier_numerator INTEGER,
    long_context_output_multiplier_denominator INTEGER,
    source_label TEXT NOT NULL,
    source_digest TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_pricing_model_effective ON pricing_snapshots(model, effective_at DESC);

CREATE TABLE cost_entries (
    id TEXT PRIMARY KEY,
    token_sample_id TEXT NOT NULL REFERENCES token_samples(id) ON DELETE CASCADE,
    pricing_snapshot_id TEXT NOT NULL REFERENCES pricing_snapshots(id),
    lower_microusd INTEGER NOT NULL CHECK(lower_microusd >= 0),
    upper_microusd INTEGER NOT NULL CHECK(upper_microusd >= lower_microusd),
    confidence TEXT NOT NULL,
    explanation TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(token_sample_id, pricing_snapshot_id)
);

CREATE TABLE approvals (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    agent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT,
    item_id TEXT,
    approval_type TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    request_json TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    expected_head_sha TEXT,
    state TEXT NOT NULL,
    decision TEXT,
    decision_note TEXT,
    decided_by TEXT,
    created_at INTEGER NOT NULL,
    resolved_at INTEGER,
    delivered_at INTEGER,
    delivery_error TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_approvals_pending ON approvals(state, risk_level, created_at);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    logical_name TEXT NOT NULL,
    storage_path TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL UNIQUE,
    media_type TEXT NOT NULL,
    compression TEXT,
    sensitivity TEXT NOT NULL,
    byte_length INTEGER NOT NULL CHECK(byte_length >= 0),
    retention_class TEXT NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    verified_at INTEGER
);

CREATE TABLE command_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    agent_session_id TEXT REFERENCES agent_sessions(id) ON DELETE SET NULL,
    worktree_id TEXT REFERENCES worktrees(id) ON DELETE SET NULL,
    command_json TEXT NOT NULL,
    command_sha256 TEXT NOT NULL,
    cwd TEXT NOT NULL,
    source_sha_before TEXT,
    source_sha_after TEXT,
    resource_class TEXT NOT NULL,
    host_identity TEXT,
    target_profile TEXT,
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    exit_code INTEGER,
    signal INTEGER,
    timed_out INTEGER NOT NULL DEFAULT 0,
    result_class TEXT,
    stdout_artifact_id TEXT REFERENCES artifacts(id),
    stderr_artifact_id TEXT REFERENCES artifacts(id),
    parsed_report_artifact_id TEXT REFERENCES artifacts(id),
    error_json TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_command_runs_attempt ON command_runs(task_attempt_id, started_at);

CREATE TABLE validations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    worktree_id TEXT NOT NULL REFERENCES worktrees(id),
    validator_id TEXT NOT NULL,
    proof_tier TEXT NOT NULL,
    source_sha TEXT NOT NULL,
    selector_reason TEXT NOT NULL,
    state TEXT NOT NULL,
    result_class TEXT,
    command_run_id TEXT REFERENCES command_runs(id),
    started_at INTEGER,
    completed_at INTEGER,
    invalidated_at INTEGER,
    invalidated_reason TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_validations_attempt ON validations(task_attempt_id, state);

CREATE TABLE evidence_records (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    validation_id TEXT REFERENCES validations(id) ON DELETE SET NULL,
    claim_id TEXT NOT NULL,
    checklist_rows_json TEXT NOT NULL,
    source_sha TEXT NOT NULL,
    proof_tier TEXT NOT NULL,
    result_class TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    unproved_claims_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    invalidated_at INTEGER,
    invalidated_reason TEXT
);
CREATE INDEX idx_evidence_run_claim ON evidence_records(run_id, claim_id, proof_tier);

CREATE TABLE evidence_artifacts (
    evidence_id TEXT NOT NULL REFERENCES evidence_records(id) ON DELETE CASCADE,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id),
    purpose TEXT NOT NULL,
    PRIMARY KEY(evidence_id, artifact_id)
);

CREATE TABLE findings (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    verifier_agent_session_id TEXT REFERENCES agent_sessions(id),
    severity TEXT NOT NULL,
    category TEXT NOT NULL,
    invariant TEXT NOT NULL,
    authority_ref TEXT,
    file_path TEXT,
    line_start INTEGER,
    line_end INTEGER,
    description TEXT NOT NULL,
    required_correction TEXT NOT NULL,
    required_test TEXT,
    state TEXT NOT NULL,
    disposition_note TEXT,
    created_at INTEGER NOT NULL,
    resolved_at INTEGER,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_findings_open ON findings(run_id, state, severity);

CREATE TABLE handoffs (
    id TEXT PRIMARY KEY,
    task_attempt_id TEXT NOT NULL UNIQUE REFERENCES task_attempts(id) ON DELETE CASCADE,
    agent_session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    handoff_json TEXT NOT NULL,
    handoff_sha256 TEXT NOT NULL,
    schema_valid INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    operation_type TEXT NOT NULL,
    state TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    request_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    result_json TEXT,
    error_json TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_operations_state ON operations(state, created_at);

CREATE TABLE publications (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    publication_type TEXT NOT NULL,
    expected_head_sha TEXT NOT NULL,
    branch TEXT NOT NULL,
    remote TEXT NOT NULL,
    approval_id TEXT NOT NULL REFERENCES approvals(id),
    state TEXT NOT NULL,
    remote_url TEXT,
    external_id TEXT,
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    error_json TEXT,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE human_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    task_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL,
    actor TEXT NOT NULL,
    action_type TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
);
CREATE INDEX idx_human_actions_run ON human_actions(run_id, id);

CREATE TABLE api_sessions (
    id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    csrf_secret_hash TEXT NOT NULL,
    last_seen_at INTEGER NOT NULL,
    revoked_at INTEGER
);

CREATE TABLE schema_migrations_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO schema_migrations_meta(key, value)
VALUES
    ('schema_version', '1'),
    ('created_for', 'Harness Console blueprint 2026-08-05');
