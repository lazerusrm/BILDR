CREATE TABLE IF NOT EXISTS improvement_revisions (
    id TEXT PRIMARY KEY,
    aggregate_kind TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    schema_name TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    sensitivity TEXT NOT NULL,
    retention_class TEXT NOT NULL,
    export_allowed INTEGER NOT NULL CHECK(export_allowed IN (0,1)),
    source_domain_event_id INTEGER REFERENCES domain_events(id),
    created_at INTEGER NOT NULL,
    UNIQUE(aggregate_kind, aggregate_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_improvement_revisions_current
    ON improvement_revisions(aggregate_kind, aggregate_id, revision DESC);
CREATE INDEX IF NOT EXISTS idx_improvement_revisions_retention
    ON improvement_revisions(sensitivity, retention_class, created_at);

CREATE TABLE IF NOT EXISTS improvement_events (
    id TEXT PRIMARY KEY,
    aggregate_kind TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    sequence INTEGER NOT NULL CHECK(sequence >= 1),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    source_raw_event_id INTEGER REFERENCES raw_events(id),
    occurred_at INTEGER NOT NULL,
    UNIQUE(aggregate_kind, aggregate_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_improvement_events_aggregate
    ON improvement_events(aggregate_kind, aggregate_id, sequence);

CREATE VIEW IF NOT EXISTS improvement_current_revisions AS
SELECT r.* FROM improvement_revisions r
JOIN (
    SELECT aggregate_kind, aggregate_id, max(revision) AS revision
    FROM improvement_revisions GROUP BY aggregate_kind, aggregate_id
) latest USING (aggregate_kind, aggregate_id, revision);

CREATE TRIGGER IF NOT EXISTS improvement_revisions_no_update
BEFORE UPDATE ON improvement_revisions BEGIN SELECT RAISE(ABORT, 'improvement revisions are append-only'); END;
CREATE TRIGGER IF NOT EXISTS improvement_revisions_no_delete
BEFORE DELETE ON improvement_revisions BEGIN SELECT RAISE(ABORT, 'improvement revisions are append-only'); END;
CREATE TRIGGER IF NOT EXISTS improvement_events_no_update
BEFORE UPDATE ON improvement_events BEGIN SELECT RAISE(ABORT, 'improvement events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS improvement_events_no_delete
BEFORE DELETE ON improvement_events BEGIN SELECT RAISE(ABORT, 'improvement events are append-only'); END;
