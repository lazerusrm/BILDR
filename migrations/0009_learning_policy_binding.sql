-- M3 keeps bundle/candidate/knowledge payloads in immutable improvement
-- revisions. This is the sole scope binding needed to resolve a champion.
CREATE TABLE IF NOT EXISTS policy_champion_bindings (
    id TEXT PRIMARY KEY,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    task_family TEXT NOT NULL,
    model_family TEXT NOT NULL DEFAULT '',
    runtime_class TEXT NOT NULL DEFAULT '',
    sequence INTEGER NOT NULL CHECK(sequence >= 1),
    policy_bundle_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    bundle_sha256 TEXT NOT NULL CHECK(length(bundle_sha256)=64 AND bundle_sha256 NOT GLOB '*[^0-9a-f]*'),
    previous_binding_id TEXT REFERENCES policy_champion_bindings(id),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    UNIQUE(repository_id, task_family, model_family, runtime_class, sequence)
);
CREATE INDEX IF NOT EXISTS idx_policy_champion_scope ON policy_champion_bindings(repository_id,task_family,model_family,runtime_class,created_at DESC);
CREATE VIEW IF NOT EXISTS policy_current_champions AS
SELECT b.* FROM policy_champion_bindings b WHERE b.sequence=(SELECT max(x.sequence) FROM policy_champion_bindings x WHERE x.repository_id=b.repository_id AND x.task_family=b.task_family AND x.model_family=b.model_family AND x.runtime_class=b.runtime_class);
CREATE TRIGGER IF NOT EXISTS policy_champion_binding_bundle BEFORE INSERT ON policy_champion_bindings BEGIN
 SELECT CASE WHEN (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.policy_bundle_revision_id) <> 'policy_bundle' OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.policy_bundle_revision_id) <> 'harness.policy-bundle.v1' THEN RAISE(ABORT,'policy champion requires policy bundle') END;
END;
CREATE TRIGGER IF NOT EXISTS policy_champion_bindings_no_update BEFORE UPDATE ON policy_champion_bindings BEGIN SELECT RAISE(ABORT,'policy champion bindings are append-only'); END;
CREATE TRIGGER IF NOT EXISTS policy_champion_bindings_no_delete BEFORE DELETE ON policy_champion_bindings BEGIN SELECT RAISE(ABORT,'policy champion bindings are append-only'); END;
