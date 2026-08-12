-- Content-addressed artifact bytes may be shared by independently replayed
-- controller runs.  The artifact row remains one immutable content record;
-- this table records each run's explicit custody relationship without
-- duplicating bytes or pretending the artifact has a second owner.
CREATE TABLE IF NOT EXISTS artifact_run_bindings (
    artifact_id TEXT NOT NULL REFERENCES artifacts(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY(artifact_id, run_id)
);

INSERT OR IGNORE INTO artifact_run_bindings(artifact_id, run_id, created_at)
SELECT id, run_id, created_at FROM artifacts WHERE run_id IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS artifact_run_bindings_no_update
BEFORE UPDATE ON artifact_run_bindings BEGIN
 SELECT RAISE(ABORT, 'artifact run bindings are append-only');
END;
CREATE TRIGGER IF NOT EXISTS artifact_run_bindings_no_delete
BEFORE DELETE ON artifact_run_bindings BEGIN
 SELECT RAISE(ABORT, 'artifact run bindings are append-only');
END;
