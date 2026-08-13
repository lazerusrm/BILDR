-- SI-007 typed failure observations and append-only human curation.
CREATE TABLE IF NOT EXISTS failure_occurrences (
    id TEXT PRIMARY KEY,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('attempt_terminal','run_terminal','typed_outcome')),
    source_id TEXT NOT NULL,
    terminal_code TEXT CHECK(terminal_code IN ('policy_blocked','budget_exhausted','infrastructure_unavailable','protocol_error','integration_conflict','source_failure','inconclusive','cancelled_superseded')),
    automatic_class TEXT NOT NULL CHECK(automatic_class IN ('unknown','policy_blocked','budget_exhausted','infrastructure_unavailable','protocol_error','integration_conflict','source_failure','inconclusive','cancelled_superseded')),
    severity TEXT NOT NULL CHECK(severity IN ('unknown','low','medium','high','critical')),
    taxonomy_version TEXT NOT NULL CHECK(taxonomy_version='harness.failure-taxonomy.v1'),
    fingerprint_sha256 TEXT NOT NULL CHECK(length(fingerprint_sha256)=64 AND fingerprint_sha256 NOT GLOB '*[^0-9a-f]*'),
    cost_scope_id TEXT,
    cost_lower_microusd INTEGER,
    cost_upper_microusd INTEGER,
    source_domain_event_id INTEGER REFERENCES domain_events(id),
    created_at INTEGER NOT NULL,
    UNIQUE(source_kind, source_id),
    CHECK((terminal_code IS NULL AND automatic_class='unknown') OR terminal_code=automatic_class),
    CHECK((cost_scope_id IS NULL AND cost_lower_microusd IS NULL AND cost_upper_microusd IS NULL)
       OR (cost_scope_id IS NOT NULL AND length(cost_scope_id)>0 AND cost_lower_microusd >= 0 AND cost_upper_microusd >= cost_lower_microusd))
);
CREATE INDEX IF NOT EXISTS idx_failure_occurrences_cluster_input
    ON failure_occurrences(repository_id, fingerprint_sha256, created_at);

CREATE TABLE IF NOT EXISTS failure_clusters (
    id TEXT PRIMARY KEY,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    version INTEGER NOT NULL CHECK(version >= 0),
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS failure_classification_revisions (
    occurrence_id TEXT NOT NULL REFERENCES failure_occurrences(id),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    class TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(occurrence_id, revision)
);

CREATE TABLE IF NOT EXISTS failure_cluster_membership_revisions (
    occurrence_id TEXT NOT NULL REFERENCES failure_occurrences(id),
    revision INTEGER PRIMARY KEY CHECK(revision >= 1),
    cluster_id TEXT NOT NULL REFERENCES failure_clusters(id),
    action TEXT NOT NULL CHECK(action IN ('assigned','merged','split')),
    actor TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_failure_membership_cluster_current
    ON failure_cluster_membership_revisions(cluster_id, occurrence_id, revision DESC);

CREATE TABLE IF NOT EXISTS failure_cluster_edits (
    id TEXT PRIMARY KEY,
    source_cluster_id TEXT NOT NULL REFERENCES failure_clusters(id),
    target_cluster_id TEXT REFERENCES failure_clusters(id),
    action TEXT NOT NULL CHECK(action IN ('merged','split')),
    actor TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    target_cluster_ids_json TEXT NOT NULL,
    target_cluster_ids_sha256 TEXT NOT NULL CHECK(length(target_cluster_ids_sha256)=64 AND target_cluster_ids_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL
);

CREATE TRIGGER IF NOT EXISTS failure_occurrences_no_update
BEFORE UPDATE ON failure_occurrences BEGIN SELECT RAISE(ABORT, 'failure occurrences are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_occurrences_no_delete
BEFORE DELETE ON failure_occurrences BEGIN SELECT RAISE(ABORT, 'failure occurrences are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_classification_revisions_no_update
BEFORE UPDATE ON failure_classification_revisions BEGIN SELECT RAISE(ABORT, 'failure classifications are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_classification_revisions_no_delete
BEFORE DELETE ON failure_classification_revisions BEGIN SELECT RAISE(ABORT, 'failure classifications are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_cluster_membership_revisions_no_update
BEFORE UPDATE ON failure_cluster_membership_revisions BEGIN SELECT RAISE(ABORT, 'failure memberships are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_cluster_membership_revisions_no_delete
BEFORE DELETE ON failure_cluster_membership_revisions BEGIN SELECT RAISE(ABORT, 'failure memberships are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_cluster_edits_no_update
BEFORE UPDATE ON failure_cluster_edits BEGIN SELECT RAISE(ABORT, 'failure cluster edits are append-only'); END;
CREATE TRIGGER IF NOT EXISTS failure_cluster_edits_no_delete
BEFORE DELETE ON failure_cluster_edits BEGIN SELECT RAISE(ABORT, 'failure cluster edits are append-only'); END;
