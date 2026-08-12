-- SI-008–SI-012 evaluation custody.  Eval/taskset/grader manifests remain
-- immutable `improvement_revisions`; these tables only bind exact revisions to
-- reproducible receipts.  In particular, no fixture, answer, grader source,
-- command output, or free-text rationale is stored here.

CREATE TABLE IF NOT EXISTS taskset_revision_memberships (
    taskset_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    eval_case_revision_id TEXT NOT NULL REFERENCES improvement_revisions(id),
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    created_at INTEGER NOT NULL,
    PRIMARY KEY(taskset_revision_id, eval_case_revision_id),
    UNIQUE(taskset_revision_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_taskset_revision_memberships_case
    ON taskset_revision_memberships(eval_case_revision_id);

-- An evaluation run is a comparison receipt.  Its exact taskset, grader,
-- runtime/base, and policy identities are immutable; mutable progress is an
-- append-only status receipt below.
CREATE TABLE IF NOT EXISTS evaluation_runs (
    id TEXT PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS evaluation_run_status_revisions (
    id TEXT PRIMARY KEY,
    evaluation_run_id TEXT NOT NULL REFERENCES evaluation_runs(id),
    sequence INTEGER NOT NULL CHECK(sequence >= 1),
    status TEXT NOT NULL CHECK(status IN ('recording','completed','infrastructure_unavailable','invalidated')),
    receipt_digest TEXT NOT NULL CHECK(length(receipt_digest)=64 AND receipt_digest NOT GLOB '*[^0-9a-f]*'),
    idempotency_key TEXT NOT NULL UNIQUE CHECK(length(idempotency_key) BETWEEN 1 AND 200),
    created_at INTEGER NOT NULL,
    UNIQUE(evaluation_run_id, sequence)
);

-- One row per arm/case/seed.  Pairing is defined by the shared run, immutable
-- case revision, and seed; unavailable isolation is a distinct non-success.
CREATE TABLE IF NOT EXISTS evaluation_samples (
    id TEXT PRIMARY KEY,
    evaluation_run_id TEXT NOT NULL REFERENCES evaluation_runs(id),
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
    UNIQUE(evaluation_run_id, eval_case_revision_id, arm, seed)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_samples_pair
    ON evaluation_samples(evaluation_run_id, eval_case_revision_id, seed);

CREATE TABLE IF NOT EXISTS evaluation_stat_verdicts (
    id TEXT PRIMARY KEY,
    champion_evaluation_run_id TEXT NOT NULL REFERENCES evaluation_runs(id),
    challenger_evaluation_run_id TEXT NOT NULL REFERENCES evaluation_runs(id),
    method TEXT NOT NULL CHECK(method IN ('paired_exact_v1')),
    decision TEXT NOT NULL CHECK(decision IN ('better','worse','inconclusive','refused_critical_regression','refused_small_sample','invalid_reward_integrity')),
    input_digest TEXT NOT NULL CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
    successful_pairs INTEGER NOT NULL CHECK(successful_pairs >= 0),
    win_pairs INTEGER NOT NULL CHECK(win_pairs >= 0),
    loss_pairs INTEGER NOT NULL CHECK(loss_pairs >= 0),
    delta_milli INTEGER NOT NULL,
    critical_regression INTEGER NOT NULL CHECK(critical_regression IN (0,1)),
    reward_integrity_pass INTEGER NOT NULL CHECK(reward_integrity_pass IN (0,1)),
    idempotency_key TEXT NOT NULL UNIQUE CHECK(length(idempotency_key) BETWEEN 1 AND 200),
    created_at INTEGER NOT NULL,
    CHECK(champion_evaluation_run_id <> challenger_evaluation_run_id),
    CHECK(win_pairs + loss_pairs <= successful_pairs)
);

-- This is an audit of custody decisions, not a store of holdout content.
-- The DB itself forbids a successful read by optimizer/candidate principals.
CREATE TABLE IF NOT EXISTS holdout_access_log (
    id TEXT PRIMARY KEY,
    taskset_revision_id TEXT REFERENCES improvement_revisions(id),
    eval_case_revision_id TEXT REFERENCES improvement_revisions(id),
    principal TEXT NOT NULL CHECK(principal IN ('optimizer','candidate_runtime','evaluator','operator')),
    split TEXT NOT NULL CHECK(split IN ('training','development','holdout','canary','quarantine')),
    action TEXT NOT NULL CHECK(action IN ('read_metadata','read_answer','execute')),
    decision TEXT NOT NULL CHECK(decision IN ('granted','denied')),
    custody_digest TEXT NOT NULL CHECK(length(custody_digest)=64 AND custody_digest NOT GLOB '*[^0-9a-f]*'),
    idempotency_key TEXT NOT NULL UNIQUE CHECK(length(idempotency_key) BETWEEN 1 AND 200),
    created_at INTEGER NOT NULL,
    CHECK((taskset_revision_id IS NOT NULL) <> (eval_case_revision_id IS NOT NULL)),
    CHECK(split <> 'holdout' OR principal NOT IN ('optimizer','candidate_runtime') OR decision='denied')
);
CREATE INDEX IF NOT EXISTS idx_holdout_access_target
    ON holdout_access_log(taskset_revision_id, eval_case_revision_id, created_at);

-- An invalidation is append-only and makes all dependent Store readers fail
-- closed.  The target is polymorphic so exact target ownership is verified by
-- Store transaction code; leakage evidence is an access-log receipt only.
CREATE TABLE IF NOT EXISTS evaluation_invalidations (
    id TEXT PRIMARY KEY,
    target_kind TEXT NOT NULL CHECK(target_kind IN ('evaluation_run','evaluation_sample','stat_verdict')),
    target_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN ('holdout_leakage','grader_drift','fixture_drift','custody_violation')),
    holdout_access_log_id TEXT REFERENCES holdout_access_log(id),
    idempotency_key TEXT NOT NULL UNIQUE CHECK(length(idempotency_key) BETWEEN 1 AND 200),
    created_at INTEGER NOT NULL,
    UNIQUE(target_kind, target_id, reason, holdout_access_log_id)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_invalidations_target
    ON evaluation_invalidations(target_kind, target_id, created_at);

-- SQLite cannot express these cross-table type FKs directly.  Keep the local
-- guard here; Store additionally checks wire schemas/digests transactionally.
CREATE TRIGGER IF NOT EXISTS taskset_membership_kinds
BEFORE INSERT ON taskset_revision_memberships BEGIN
    SELECT CASE WHEN (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'taskset'
                  OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'harness.taskset.v1'
                  OR (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.eval_case_revision_id) <> 'eval_case'
                  OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.eval_case_revision_id) <> 'harness.eval-case.v1'
        THEN RAISE(ABORT, 'taskset membership revision kind/schema mismatch') END;
END;
CREATE TRIGGER IF NOT EXISTS evaluation_run_revision_kinds
BEFORE INSERT ON evaluation_runs BEGIN
    SELECT CASE WHEN (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'taskset'
                  OR (SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.grader_bundle_revision_id) <> 'grader_bundle'
                  OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.grader_bundle_revision_id) <> 'harness.grader-bundle.v1'
        THEN RAISE(ABORT, 'evaluation run revision kind/schema mismatch') END;
END;
CREATE TRIGGER IF NOT EXISTS evaluation_sample_membership
BEFORE INSERT ON evaluation_samples BEGIN
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM evaluation_runs r JOIN taskset_revision_memberships m ON m.taskset_revision_id=r.taskset_revision_id WHERE r.id=NEW.evaluation_run_id AND m.eval_case_revision_id=NEW.eval_case_revision_id)
        THEN RAISE(ABORT, 'evaluation sample case is not taskset member') END;
END;
CREATE TRIGGER IF NOT EXISTS holdout_access_target_kind
BEFORE INSERT ON holdout_access_log BEGIN
    SELECT CASE WHEN NEW.taskset_revision_id IS NOT NULL AND ((SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'taskset' OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.taskset_revision_id) <> 'harness.taskset.v1')
        THEN RAISE(ABORT, 'holdout taskset target kind/schema mismatch') END;
    SELECT CASE WHEN NEW.eval_case_revision_id IS NOT NULL AND ((SELECT aggregate_kind FROM improvement_revisions WHERE id=NEW.eval_case_revision_id) <> 'eval_case' OR (SELECT schema_name FROM improvement_revisions WHERE id=NEW.eval_case_revision_id) <> 'harness.eval-case.v1')
        THEN RAISE(ABORT, 'holdout eval-case target kind/schema mismatch') END;
END;
CREATE TRIGGER IF NOT EXISTS evaluation_invalidation_target_exists
BEFORE INSERT ON evaluation_invalidations BEGIN
    SELECT CASE WHEN NEW.target_kind='evaluation_run' AND NOT EXISTS(SELECT 1 FROM evaluation_runs WHERE id=NEW.target_id)
        THEN RAISE(ABORT, 'evaluation invalidation run target missing') END;
    SELECT CASE WHEN NEW.target_kind='evaluation_sample' AND NOT EXISTS(SELECT 1 FROM evaluation_samples WHERE id=NEW.target_id)
        THEN RAISE(ABORT, 'evaluation invalidation sample target missing') END;
    SELECT CASE WHEN NEW.target_kind='stat_verdict' AND NOT EXISTS(SELECT 1 FROM evaluation_stat_verdicts WHERE id=NEW.target_id)
        THEN RAISE(ABORT, 'evaluation invalidation verdict target missing') END;
    SELECT CASE WHEN NEW.reason='holdout_leakage' AND (NEW.holdout_access_log_id IS NULL OR NOT EXISTS(SELECT 1 FROM holdout_access_log WHERE id=NEW.holdout_access_log_id))
        THEN RAISE(ABORT, 'holdout leakage invalidation requires access receipt') END;
END;

CREATE TRIGGER IF NOT EXISTS taskset_revision_memberships_no_update
BEFORE UPDATE ON taskset_revision_memberships BEGIN SELECT RAISE(ABORT, 'taskset memberships are append-only'); END;
CREATE TRIGGER IF NOT EXISTS taskset_revision_memberships_no_delete
BEFORE DELETE ON taskset_revision_memberships BEGIN SELECT RAISE(ABORT, 'taskset memberships are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_runs_no_update
BEFORE UPDATE ON evaluation_runs BEGIN SELECT RAISE(ABORT, 'evaluation runs are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_runs_no_delete
BEFORE DELETE ON evaluation_runs BEGIN SELECT RAISE(ABORT, 'evaluation runs are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_run_status_revisions_no_update
BEFORE UPDATE ON evaluation_run_status_revisions BEGIN SELECT RAISE(ABORT, 'evaluation status revisions are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_run_status_revisions_no_delete
BEFORE DELETE ON evaluation_run_status_revisions BEGIN SELECT RAISE(ABORT, 'evaluation status revisions are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_samples_no_update
BEFORE UPDATE ON evaluation_samples BEGIN SELECT RAISE(ABORT, 'evaluation samples are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_samples_no_delete
BEFORE DELETE ON evaluation_samples BEGIN SELECT RAISE(ABORT, 'evaluation samples are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_stat_verdicts_no_update
BEFORE UPDATE ON evaluation_stat_verdicts BEGIN SELECT RAISE(ABORT, 'evaluation statistics are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_stat_verdicts_no_delete
BEFORE DELETE ON evaluation_stat_verdicts BEGIN SELECT RAISE(ABORT, 'evaluation statistics are append-only'); END;
CREATE TRIGGER IF NOT EXISTS holdout_access_log_no_update
BEFORE UPDATE ON holdout_access_log BEGIN SELECT RAISE(ABORT, 'holdout access is append-only'); END;
CREATE TRIGGER IF NOT EXISTS holdout_access_log_no_delete
BEFORE DELETE ON holdout_access_log BEGIN SELECT RAISE(ABORT, 'holdout access is append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_invalidations_no_update
BEFORE UPDATE ON evaluation_invalidations BEGIN SELECT RAISE(ABORT, 'evaluation invalidations are append-only'); END;
CREATE TRIGGER IF NOT EXISTS evaluation_invalidations_no_delete
BEFORE DELETE ON evaluation_invalidations BEGIN SELECT RAISE(ABORT, 'evaluation invalidations are append-only'); END;
