-- Repair the short-lived v8 schema emitted before controller-run and dual
-- evidence custody were added to the evaluation records. The caller verifies
-- that every dependent evaluation table is empty before this script runs: old
-- rows cannot be safely assigned a controller run or evidence owner after the
-- fact, so populated legacy databases are rejected rather than rewritten.

DROP TRIGGER IF EXISTS evaluation_run_revision_kinds;
DROP TRIGGER IF EXISTS evaluation_sample_membership;
DROP TABLE evaluation_samples;
DROP TABLE evaluation_runs;

CREATE TABLE evaluation_runs (
    id TEXT PRIMARY KEY,
    controller_run_id TEXT NOT NULL REFERENCES runs(id),
    taskset_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    grader_bundle_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    split TEXT NOT NULL CHECK(split IN ('training','development','holdout','canary','quarantine')),
    base_sha TEXT NOT NULL CHECK(length(base_sha)=40 AND base_sha NOT GLOB '*[^0-9a-f]*'),
    fixture_digest TEXT NOT NULL CHECK(length(fixture_digest)=64 AND fixture_digest NOT GLOB '*[^0-9a-f]*'),
    runtime_digest TEXT NOT NULL CHECK(length(runtime_digest)=64 AND runtime_digest NOT GLOB '*[^0-9a-f]*'),
    seed_policy_digest TEXT NOT NULL CHECK(length(seed_policy_digest)=64 AND seed_policy_digest NOT GLOB '*[^0-9a-f]*'),
    champion_policy_digest TEXT NOT NULL CHECK(length(champion_policy_digest)=64 AND champion_policy_digest NOT GLOB '*[^0-9a-f]*'),
    challenger_policy_digest TEXT CHECK(challenger_policy_digest IS NULL OR (length(challenger_policy_digest)=64 AND challenger_policy_digest NOT GLOB '*[^0-9a-f]*')),
    idempotency_key TEXT NOT NULL UNIQUE CHECK(length(idempotency_key) BETWEEN 1 AND 200),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_evaluation_runs_taskset
    ON evaluation_runs(taskset_revision_id, created_at);

CREATE TABLE evaluation_samples (
    id TEXT PRIMARY KEY,
    evaluation_run_id TEXT NOT NULL REFERENCES evaluation_runs(id),
    controller_evidence_id TEXT NOT NULL REFERENCES evidence_records(id),
    grader_evidence_id TEXT NOT NULL REFERENCES evidence_records(id),
    eval_case_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    arm TEXT NOT NULL CHECK(arm IN ('champion','challenger')),
    seed INTEGER NOT NULL CHECK(seed >= 0),
    classification TEXT NOT NULL CHECK(classification IN ('pass','fail','infrastructure_unavailable','invalidated')),
    sample_digest TEXT NOT NULL CHECK(length(sample_digest)=64 AND sample_digest NOT GLOB '*[^0-9a-f]*'),
    trace_digest TEXT CHECK(trace_digest IS NULL OR (length(trace_digest)=64 AND trace_digest NOT GLOB '*[^0-9a-f]*')),
    evidence_digest TEXT CHECK(evidence_digest IS NULL OR (length(evidence_digest)=64 AND evidence_digest NOT GLOB '*[^0-9a-f]*')),
    artifact_digest TEXT CHECK(artifact_digest IS NULL OR (length(artifact_digest)=64 AND artifact_digest NOT GLOB '*[^0-9a-f]*')),
    cost_receipt_digest TEXT CHECK(cost_receipt_digest IS NULL OR (length(cost_receipt_digest)=64 AND cost_receipt_digest NOT GLOB '*[^0-9a-f]*')),
    idempotency_key TEXT NOT NULL UNIQUE CHECK(length(idempotency_key) BETWEEN 1 AND 200),
    created_at INTEGER NOT NULL,
    UNIQUE(evaluation_run_id, eval_case_revision_id, arm, seed),
    CHECK(controller_evidence_id <> grader_evidence_id)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_samples_pair
    ON evaluation_samples(evaluation_run_id, eval_case_revision_id, seed);

CREATE TRIGGER evaluation_run_revision_kinds
BEFORE INSERT ON evaluation_runs BEGIN
    SELECT CASE WHEN (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'taskset'
                  OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'harness.taskset.v1'
                  OR (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.grader_bundle_revision_id) <> 'grader_bundle'
                  OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.grader_bundle_revision_id) <> 'harness.grader-bundle.v1'
        THEN RAISE(ABORT, 'evaluation run revision kind/schema mismatch') END;
END;
CREATE TRIGGER evaluation_sample_membership
BEFORE INSERT ON evaluation_samples BEGIN
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM evaluation_runs r JOIN taskset_revision_memberships m ON m.taskset_revision_id=r.taskset_revision_id WHERE r.id=NEW.evaluation_run_id AND m.eval_case_revision_id=NEW.eval_case_revision_id)
        THEN RAISE(ABORT, 'evaluation sample case is not taskset member') END;
END;

-- These were attached to the dropped tables, so SQLite removed them with the
-- legacy shape. Recreate the current append-only guards explicitly.
CREATE TRIGGER evaluation_runs_no_update
BEFORE UPDATE ON evaluation_runs BEGIN SELECT RAISE(ABORT, 'evaluation runs are append-only'); END;
CREATE TRIGGER evaluation_runs_no_delete
BEFORE DELETE ON evaluation_runs BEGIN SELECT RAISE(ABORT, 'evaluation runs are append-only'); END;
CREATE TRIGGER evaluation_samples_no_update
BEFORE UPDATE ON evaluation_samples BEGIN SELECT RAISE(ABORT, 'evaluation samples are append-only'); END;
CREATE TRIGGER evaluation_samples_no_delete
BEFORE DELETE ON evaluation_samples BEGIN SELECT RAISE(ABORT, 'evaluation samples are append-only'); END;
