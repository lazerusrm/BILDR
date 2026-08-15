-- A fresh mutable attempt is legal only when one exact exclusive-ownership
-- proof is consumed alongside its reconciliation authorization receipt.  This
-- table is append-only: its primary key prevents the same proof from ever
-- authorizing a second replacement, even through a different episode.

CREATE TABLE IF NOT EXISTS reconciliation_proof_consumptions (
    proof_id TEXT PRIMARY KEY REFERENCES ownership_proofs(id),
    episode_id TEXT NOT NULL REFERENCES reconciliation_episodes(id),
    action_id INTEGER NOT NULL UNIQUE REFERENCES reconciliation_actions(id),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    prior_attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
    replacement_attempt_id TEXT NOT NULL UNIQUE REFERENCES task_attempts(id),
    consumed_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reconciliation_proof_consumptions_task
    ON reconciliation_proof_consumptions(task_id, consumed_at DESC);

CREATE TRIGGER IF NOT EXISTS reconciliation_proof_consumptions_no_update
BEFORE UPDATE ON reconciliation_proof_consumptions
BEGIN
    SELECT RAISE(ABORT, 'reconciliation proof consumptions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS reconciliation_proof_consumptions_no_delete
BEFORE DELETE ON reconciliation_proof_consumptions
BEGIN
    SELECT RAISE(ABORT, 'reconciliation proof consumptions are immutable');
END;
