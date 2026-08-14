-- Operator control plane foundation. Mutable tables below are explicitly
-- named current views/cursors; all observations, receipts, source events,
-- snapshots, artifacts, and correlations are append-only.

CREATE TABLE IF NOT EXISTS attention_items (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK(source_type IN ('approval','decision','credential_requirement','publication','policy_decision','evidence_gap','external_condition','reconciliation','infrastructure')),
    source_id TEXT NOT NULL,
    source_revision INTEGER NOT NULL CHECK(source_revision >= 0),
    repository_id TEXT,
    run_id TEXT,
    task_id TEXT,
    category TEXT NOT NULL CHECK(category IN ('decision','approval','credential','policy_exception','destructive_action','publication','missing_evidence','external_dependency','recovery_conflict','infrastructure')),
    severity TEXT NOT NULL CHECK(severity IN ('info','normal','high','critical')),
    state TEXT NOT NULL CHECK(state IN ('open','acknowledged','waiting_external','resolved','declined','superseded','invalidated')),
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    opened_event_id TEXT NOT NULL,
    opened_at INTEGER NOT NULL,
    acknowledged_at INTEGER,
    due_at INTEGER,
    resurfacing_json TEXT NOT NULL,
    resolution_json TEXT,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    version INTEGER NOT NULL CHECK(version >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(source_type, source_id, source_revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attention_items_active_dedupe
    ON attention_items(dedupe_key)
    WHERE state IN ('open','acknowledged','waiting_external');
CREATE INDEX IF NOT EXISTS idx_attention_items_state_severity ON attention_items(state, severity DESC, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_attention_items_run_state ON attention_items(run_id, state, opened_at DESC);

CREATE TABLE IF NOT EXISTS attention_events (
    id INTEGER PRIMARY KEY,
    attention_id TEXT NOT NULL REFERENCES attention_items(id),
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_revision INTEGER NOT NULL CHECK(source_revision >= 0),
    event_kind TEXT NOT NULL CHECK(event_kind IN ('opened','source_updated','acknowledged','waiting_external','resolved','declined','superseded','invalidated')),
    expected_version INTEGER,
    resulting_version INTEGER NOT NULL CHECK(resulting_version >= 0),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL,
    UNIQUE(source_type, source_id, source_revision, event_kind)
);
CREATE INDEX IF NOT EXISTS idx_attention_events_item_version ON attention_events(attention_id, resulting_version);

CREATE TABLE IF NOT EXISTS investigation_artifacts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    repository_state_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_investigation_artifacts_run_task_created ON investigation_artifacts(run_id, task_id, created_at DESC);

CREATE TABLE IF NOT EXISTS material_progress_events (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    task_id TEXT,
    attempt_id TEXT,
    kind TEXT NOT NULL CHECK(kind IN ('candidate_changed','validation_advanced','evidence_recorded','external_condition_changed','reconciliation_advanced','attention_changed')),
    source_event_id TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(kind, source_event_id)
);
CREATE INDEX IF NOT EXISTS idx_material_progress_run_occurred ON material_progress_events(run_id, occurred_at DESC);

-- One durable classifier checkpoint makes progress projection incremental.
-- It advances in the same immediate transaction as the immutable rows, so a
-- failed classification never skips a source event and a completed suffix is
-- never replayed on every operator refresh.
CREATE TABLE IF NOT EXISTS material_progress_classifier_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    event_cursor INTEGER NOT NULL CHECK(event_cursor >= 0)
);
INSERT OR IGNORE INTO material_progress_classifier_state(id, event_cursor) VALUES(1, 0);

CREATE TABLE IF NOT EXISTS liveness_episodes (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    task_id TEXT,
    attempt_id TEXT,
    state TEXT NOT NULL CHECK(state IN ('healthy','quiet_active','waiting_external','degraded','suspected_stall','confirmed_stall','ownership_uncertain','recovery_required','terminal')),
    version INTEGER NOT NULL CHECK(version >= 0),
    opened_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    current_payload_json TEXT NOT NULL,
    current_payload_sha256 TEXT NOT NULL CHECK(length(current_payload_sha256) = 64 AND current_payload_sha256 NOT GLOB '*[^0-9a-f]*')
);
CREATE INDEX IF NOT EXISTS idx_liveness_episodes_run_state ON liveness_episodes(run_id, state, updated_at DESC);

CREATE TABLE IF NOT EXISTS liveness_observations (
    id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES liveness_episodes(id),
    observation_kind TEXT NOT NULL CHECK(observation_kind IN ('material_progress','runtime_heartbeat','command_activity','external_wait','ownership_evidence')),
    source_event_id TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(episode_id, observation_kind, source_event_id)
);

CREATE TABLE IF NOT EXISTS intervention_receipts (
    id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES liveness_episodes(id),
    kind TEXT NOT NULL CHECK(kind IN ('wait','request_operator_decision','request_reconciliation','queue_read_only_review')),
    source_event_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(episode_id, kind, source_event_id)
);

CREATE TABLE IF NOT EXISTS reconciliation_episodes (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    trigger_kind TEXT NOT NULL CHECK(trigger_kind IN ('daemon_restart','app_server_loss','process_loss','version_transition','account_handoff','worktree_mismatch','uncertain_command_completion')),
    state TEXT NOT NULL CHECK(state IN ('open','claimed','awaiting_evidence','resolved','refused')),
    version INTEGER NOT NULL CHECK(version >= 0),
    opened_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    current_payload_json TEXT NOT NULL,
    current_payload_sha256 TEXT NOT NULL CHECK(length(current_payload_sha256) = 64 AND current_payload_sha256 NOT GLOB '*[^0-9a-f]*')
);
CREATE TABLE IF NOT EXISTS reconciliation_findings (
    id INTEGER PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES reconciliation_episodes(id),
    kind TEXT NOT NULL CHECK(kind IN ('live_owner','unknown_owner','preserved_candidate','stale_approval','ambiguous_external_effect')),
    source_event_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL,
    UNIQUE(episode_id, kind, source_event_id)
);
CREATE TABLE IF NOT EXISTS reconciliation_actions (
    id INTEGER PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES reconciliation_episodes(id),
    kind TEXT NOT NULL CHECK(kind IN ('preserve','resume_proven_owner','invalidate_stale_approval','release_proven_dead_lease','authorize_fresh_attempt','open_attention')),
    source_event_id TEXT NOT NULL,
    authority_event_id TEXT,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL,
    UNIQUE(episode_id, kind, source_event_id)
);

CREATE TABLE IF NOT EXISTS ownership_proofs (
    id TEXT PRIMARY KEY,
    run_id TEXT,
    task_id TEXT,
    attempt_id TEXT,
    source_event_id TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    observed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS external_conditions (
    id TEXT PRIMARY KEY,
    adapter TEXT NOT NULL CHECK(adapter IN ('ci_check','review_state','credential_availability','time_gate','hardware_capacity','service_availability')),
    source_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('open','satisfied','unsatisfied','unknown','cancelled')),
    version INTEGER NOT NULL CHECK(version >= 0),
    current_payload_json TEXT NOT NULL,
    current_payload_sha256 TEXT NOT NULL CHECK(length(current_payload_sha256) = 64 AND current_payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    opened_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(adapter, source_id)
);
CREATE TABLE IF NOT EXISTS condition_observations (
    id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL REFERENCES external_conditions(id),
    source_event_id TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    UNIQUE(condition_id, source_event_id)
);

CREATE TABLE IF NOT EXISTS control_plane_snapshots (
    id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL UNIQUE CHECK(revision > 0),
    event_cursor INTEGER NOT NULL CHECK(event_cursor >= 0),
    source_cursors_sha256 TEXT NOT NULL CHECK(length(source_cursors_sha256) = 64 AND source_cursors_sha256 NOT GLOB '*[^0-9a-f]*'),
    consistency TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL,
    UNIQUE(event_cursor, source_cursors_sha256)
);
CREATE TABLE IF NOT EXISTS snapshot_sections (
    snapshot_id TEXT NOT NULL REFERENCES control_plane_snapshots(id),
    section_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('current','stale','unknown','error')),
    source_cursor INTEGER NOT NULL CHECK(source_cursor >= 0),
    truncated INTEGER NOT NULL CHECK(truncated IN (0,1)),
    row_count INTEGER NOT NULL CHECK(row_count >= 0),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY(snapshot_id, section_name)
);

CREATE TABLE IF NOT EXISTS operator_presence (
    operator_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL CHECK(mode IN ('interactive','focus','unattended')),
    version INTEGER NOT NULL CHECK(version >= 0),
    updated_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*')
);
CREATE TABLE IF NOT EXISTS notification_deliveries (
    id TEXT PRIMARY KEY,
    attention_id TEXT REFERENCES attention_items(id),
    class TEXT NOT NULL CHECK(class IN ('critical','action_required','routine')),
    state TEXT NOT NULL CHECK(state IN ('pending','deferred','delivered','failed')),
    source_event_id TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS return_view_cursors (
    operator_id TEXT PRIMARY KEY,
    acknowledged_cursor INTEGER NOT NULL CHECK(acknowledged_cursor >= 0),
    expected_snapshot_revision INTEGER NOT NULL CHECK(expected_snapshot_revision >= 0),
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS topology_snapshots (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_cursor INTEGER NOT NULL CHECK(source_cursor >= 0),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL,
    UNIQUE(run_id, source_cursor)
);
CREATE TABLE IF NOT EXISTS correlation_links (
    id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL CHECK(length(trace_id) = 32 AND trace_id NOT GLOB '*[^0-9a-f]*'),
    span_id TEXT NOT NULL CHECK(length(span_id) = 16 AND span_id NOT GLOB '*[^0-9a-f]*'),
    parent_span_id TEXT CHECK(parent_span_id IS NULL OR (length(parent_span_id) = 16 AND parent_span_id NOT GLOB '*[^0-9a-f]*')),
    from_kind TEXT NOT NULL,
    from_id TEXT NOT NULL,
    to_kind TEXT NOT NULL,
    to_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL,
    UNIQUE(trace_id, from_kind, from_id, to_kind, to_id, relation)
);
CREATE INDEX IF NOT EXISTS idx_correlation_links_trace_created ON correlation_links(trace_id, created_at);

CREATE TRIGGER IF NOT EXISTS investigation_artifacts_no_update BEFORE UPDATE ON investigation_artifacts BEGIN SELECT RAISE(ABORT, 'investigation artifacts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS investigation_artifacts_no_delete BEFORE DELETE ON investigation_artifacts BEGIN SELECT RAISE(ABORT, 'investigation artifacts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS attention_events_no_update BEFORE UPDATE ON attention_events BEGIN SELECT RAISE(ABORT, 'attention events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS attention_events_no_delete BEFORE DELETE ON attention_events BEGIN SELECT RAISE(ABORT, 'attention events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS material_progress_events_no_update BEFORE UPDATE ON material_progress_events BEGIN SELECT RAISE(ABORT, 'material progress events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS material_progress_events_no_delete BEFORE DELETE ON material_progress_events BEGIN SELECT RAISE(ABORT, 'material progress events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS liveness_observations_no_update BEFORE UPDATE ON liveness_observations BEGIN SELECT RAISE(ABORT, 'liveness observations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS liveness_observations_no_delete BEFORE DELETE ON liveness_observations BEGIN SELECT RAISE(ABORT, 'liveness observations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS intervention_receipts_no_update BEFORE UPDATE ON intervention_receipts BEGIN SELECT RAISE(ABORT, 'intervention receipts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS intervention_receipts_no_delete BEFORE DELETE ON intervention_receipts BEGIN SELECT RAISE(ABORT, 'intervention receipts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_findings_no_update BEFORE UPDATE ON reconciliation_findings BEGIN SELECT RAISE(ABORT, 'reconciliation findings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_findings_no_delete BEFORE DELETE ON reconciliation_findings BEGIN SELECT RAISE(ABORT, 'reconciliation findings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_actions_no_update BEFORE UPDATE ON reconciliation_actions BEGIN SELECT RAISE(ABORT, 'reconciliation actions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS reconciliation_actions_no_delete BEFORE DELETE ON reconciliation_actions BEGIN SELECT RAISE(ABORT, 'reconciliation actions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS ownership_proofs_no_update BEFORE UPDATE ON ownership_proofs BEGIN SELECT RAISE(ABORT, 'ownership proofs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS ownership_proofs_no_delete BEFORE DELETE ON ownership_proofs BEGIN SELECT RAISE(ABORT, 'ownership proofs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS condition_observations_no_update BEFORE UPDATE ON condition_observations BEGIN SELECT RAISE(ABORT, 'condition observations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS condition_observations_no_delete BEFORE DELETE ON condition_observations BEGIN SELECT RAISE(ABORT, 'condition observations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS control_plane_snapshots_no_update BEFORE UPDATE ON control_plane_snapshots BEGIN SELECT RAISE(ABORT, 'control plane snapshots are immutable'); END;
CREATE TRIGGER IF NOT EXISTS control_plane_snapshots_no_delete BEFORE DELETE ON control_plane_snapshots BEGIN SELECT RAISE(ABORT, 'control plane snapshots are immutable'); END;
CREATE TRIGGER IF NOT EXISTS snapshot_sections_no_update BEFORE UPDATE ON snapshot_sections BEGIN SELECT RAISE(ABORT, 'snapshot sections are immutable'); END;
CREATE TRIGGER IF NOT EXISTS snapshot_sections_no_delete BEFORE DELETE ON snapshot_sections BEGIN SELECT RAISE(ABORT, 'snapshot sections are immutable'); END;
CREATE TRIGGER IF NOT EXISTS notification_deliveries_no_update BEFORE UPDATE ON notification_deliveries BEGIN SELECT RAISE(ABORT, 'notification deliveries are immutable'); END;
CREATE TRIGGER IF NOT EXISTS notification_deliveries_no_delete BEFORE DELETE ON notification_deliveries BEGIN SELECT RAISE(ABORT, 'notification deliveries are immutable'); END;
CREATE TRIGGER IF NOT EXISTS topology_snapshots_no_update BEFORE UPDATE ON topology_snapshots BEGIN SELECT RAISE(ABORT, 'topology snapshots are immutable'); END;
CREATE TRIGGER IF NOT EXISTS topology_snapshots_no_delete BEFORE DELETE ON topology_snapshots BEGIN SELECT RAISE(ABORT, 'topology snapshots are immutable'); END;
CREATE TRIGGER IF NOT EXISTS correlation_links_no_update BEFORE UPDATE ON correlation_links BEGIN SELECT RAISE(ABORT, 'correlation links are immutable'); END;
CREATE TRIGGER IF NOT EXISTS correlation_links_no_delete BEFORE DELETE ON correlation_links BEGIN SELECT RAISE(ABORT, 'correlation links are immutable'); END;
