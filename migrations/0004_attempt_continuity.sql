ALTER TABLE agent_runtime_details
    ADD COLUMN context_strategy TEXT NOT NULL DEFAULT 'fresh_independent';

ALTER TABLE agent_runtime_details
    ADD COLUMN context_source_attempt_id TEXT REFERENCES task_attempts(id) ON DELETE SET NULL;

ALTER TABLE agent_runtime_details
    ADD COLUMN context_reuse_reason TEXT;
