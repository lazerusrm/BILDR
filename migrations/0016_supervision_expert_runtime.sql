-- A queued expert consultation becomes runnable only after the controller
-- binds one read-only agent session. The binding is written once and survives
-- restart, so a completed App Server item cannot be attributed to another
-- request or silently retried.

ALTER TABLE expert_requests ADD COLUMN agent_session_id TEXT REFERENCES agent_sessions(id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_expert_requests_agent_session
    ON expert_requests(agent_session_id)
    WHERE agent_session_id IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS expert_requests_agent_binding_once
BEFORE UPDATE OF agent_session_id ON expert_requests
WHEN OLD.agent_session_id IS NOT NULL OR NEW.agent_session_id IS NULL
BEGIN
 SELECT RAISE(ABORT, 'expert request agent binding is write-once');
END;
