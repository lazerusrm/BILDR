-- Operator-owned thread ordering. Pinning is presentation state: it never
-- changes run authority, scheduling, custody, or evidence.
ALTER TABLE runs ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_runs_pinned
    ON runs(pinned, updated_at DESC) WHERE pinned = 1;
