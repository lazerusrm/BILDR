-- Whole-run purge without weakening record immutability.
--
-- The append-only triggers exist so no individual evidence record can be
-- quietly edited or removed while the work it describes still exists. They were
-- not meant to make a finished run immortal, and without a purge path the
-- database grows without bound.
--
-- A purge announces itself by inserting the run id into run_purges. Each
-- run-scoped trigger below aborts unless that exact run is mid-purge, so a
-- record can never be removed on its own, only as part of deleting the whole
-- run that owns it. The marker is removed in the same transaction, so the
-- guarantee is restored before the transaction is visible to anyone else.
--
-- The 26 global append-only tables (knowledge, taxonomy, holdout, evaluation
-- statistics) keep their unconditional triggers: a run purge never reaches them.

CREATE TABLE IF NOT EXISTS run_purges (
    run_id TEXT PRIMARY KEY,
    started_at INTEGER NOT NULL
);

DROP TRIGGER IF EXISTS evaluation_runs_no_delete;
CREATE TRIGGER evaluation_runs_no_delete BEFORE DELETE ON evaluation_runs
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.controller_run_id)
BEGIN SELECT RAISE(ABORT, 'evaluation runs are append-only'); END;

DROP TRIGGER IF EXISTS artifact_run_bindings_no_delete;
CREATE TRIGGER artifact_run_bindings_no_delete BEFORE DELETE ON artifact_run_bindings
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'artifact run bindings are append-only'); END;

DROP TRIGGER IF EXISTS supervisor_snapshots_no_delete;
CREATE TRIGGER supervisor_snapshots_no_delete BEFORE DELETE ON supervisor_snapshots
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'supervisor snapshots are immutable'); END;

DROP TRIGGER IF EXISTS supervisor_decisions_no_delete;
CREATE TRIGGER supervisor_decisions_no_delete BEFORE DELETE ON supervisor_decisions
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'supervisor decisions are immutable'); END;

DROP TRIGGER IF EXISTS expert_responses_no_delete;
CREATE TRIGGER expert_responses_no_delete BEFORE DELETE ON expert_responses
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'expert responses are immutable'); END;

DROP TRIGGER IF EXISTS investigation_artifacts_no_delete;
CREATE TRIGGER investigation_artifacts_no_delete BEFORE DELETE ON investigation_artifacts
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'investigation artifacts are immutable'); END;

DROP TRIGGER IF EXISTS material_progress_events_no_delete;
CREATE TRIGGER material_progress_events_no_delete BEFORE DELETE ON material_progress_events
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'material progress events are immutable'); END;

DROP TRIGGER IF EXISTS ownership_proofs_no_delete;
CREATE TRIGGER ownership_proofs_no_delete BEFORE DELETE ON ownership_proofs
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'ownership proofs are immutable'); END;

DROP TRIGGER IF EXISTS topology_snapshots_no_delete;
CREATE TRIGGER topology_snapshots_no_delete BEFORE DELETE ON topology_snapshots
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'topology snapshots are immutable'); END;

DROP TRIGGER IF EXISTS run_model_routes_no_delete;
CREATE TRIGGER run_model_routes_no_delete BEFORE DELETE ON run_model_routes
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'run model routes are immutable'); END;

DROP TRIGGER IF EXISTS agent_model_route_bindings_no_delete;
CREATE TRIGGER agent_model_route_bindings_no_delete BEFORE DELETE ON agent_model_route_bindings
WHEN NOT EXISTS (SELECT 1 FROM run_purges WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'agent model route bindings are immutable'); END;

-- Foreign keys whose child column was never indexed. SQLite enforces a parent
-- delete by scanning the child table once per row, so a cascade over tens of
-- thousands of raw_events became a full scan of domain_events each time. These
-- indexes make a run purge proportional to the run, and speed up ordinary
-- referential checks on every write path that touches these tables.
CREATE INDEX IF NOT EXISTS idx_domain_events_source_raw_event
    ON domain_events(source_raw_event_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_agent_session
    ON raw_events(agent_session_id);
CREATE INDEX IF NOT EXISTS idx_projected_items_source_raw_event
    ON projected_items(source_raw_event_id);
CREATE INDEX IF NOT EXISTS idx_improvement_revisions_source_domain_event
    ON improvement_revisions(source_domain_event_id);
CREATE INDEX IF NOT EXISTS idx_improvement_events_source_raw_event
    ON improvement_events(source_raw_event_id);
CREATE INDEX IF NOT EXISTS idx_token_samples_source_event
    ON token_samples(source_event_id);
CREATE INDEX IF NOT EXISTS idx_failure_occurrences_source_domain_event
    ON failure_occurrences(source_domain_event_id);

-- Learning records are global and outlive any single run, so they keep their
-- unconditional delete guard. They do carry a nullable pointer at the raw or
-- domain event that produced them, and that event goes away when its run is
-- purged. These update guards allow exactly one mutation: clearing that
-- provenance pointer, during a purge, with the record's identity and payload
-- digest unchanged. Every other update stays refused.

DROP TRIGGER IF EXISTS improvement_revisions_no_update;
CREATE TRIGGER improvement_revisions_no_update BEFORE UPDATE ON improvement_revisions
WHEN NOT (
    (SELECT count(*) FROM run_purges) > 0
    AND NEW.source_domain_event_id IS NULL
    AND OLD.source_domain_event_id IS NOT NULL
    AND NEW.id = OLD.id
    AND NEW.payload_sha256 = OLD.payload_sha256
    AND NEW.revision = OLD.revision
    AND NEW.lifecycle_state = OLD.lifecycle_state
)
BEGIN SELECT RAISE(ABORT, 'improvement revisions are append-only'); END;

DROP TRIGGER IF EXISTS improvement_events_no_update;
CREATE TRIGGER improvement_events_no_update BEFORE UPDATE ON improvement_events
WHEN NOT (
    (SELECT count(*) FROM run_purges) > 0
    AND NEW.source_raw_event_id IS NULL
    AND OLD.source_raw_event_id IS NOT NULL
    AND NEW.id = OLD.id
    AND NEW.payload_sha256 = OLD.payload_sha256
    AND NEW.sequence = OLD.sequence
)
BEGIN SELECT RAISE(ABORT, 'improvement events are append-only'); END;

DROP TRIGGER IF EXISTS failure_occurrences_no_update;
CREATE TRIGGER failure_occurrences_no_update BEFORE UPDATE ON failure_occurrences
WHEN NOT (
    (SELECT count(*) FROM run_purges) > 0
    AND NEW.source_domain_event_id IS NULL
    AND OLD.source_domain_event_id IS NOT NULL
    AND NEW.id = OLD.id
    AND NEW.fingerprint_sha256 = OLD.fingerprint_sha256
    AND NEW.terminal_code = OLD.terminal_code
)
BEGIN SELECT RAISE(ABORT, 'failure occurrences are append-only'); END;
