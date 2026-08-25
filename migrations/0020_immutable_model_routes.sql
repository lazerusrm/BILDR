-- A model route is launch authority, not best-effort runtime metadata. The
-- run receipt and each normal controller-session binding are append-only.

CREATE TABLE IF NOT EXISTS run_model_routes (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    schema TEXT NOT NULL CHECK(schema = 'harness.run-model-route.v2'),
    provider TEXT NOT NULL CHECK(length(provider) BETWEEN 1 AND 160),
    model TEXT NOT NULL CHECK(length(model) BETWEEN 1 AND 160),
    reasoning_effort TEXT NOT NULL CHECK(length(reasoning_effort) BETWEEN 1 AND 32),
    model_profile_sha256 TEXT CHECK(model_profile_sha256 IS NULL OR (length(model_profile_sha256) = 64 AND model_profile_sha256 NOT GLOB '*[^0-9a-f]*')),
    route_sha256 TEXT NOT NULL CHECK(length(route_sha256) = 64 AND route_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL
);

CREATE TRIGGER IF NOT EXISTS run_model_routes_no_update
BEFORE UPDATE ON run_model_routes
BEGIN
    SELECT RAISE(ABORT, 'run model routes are immutable');
END;
CREATE TRIGGER IF NOT EXISTS run_model_routes_no_delete
BEFORE DELETE ON run_model_routes
BEGIN
    SELECT RAISE(ABORT, 'run model routes are immutable');
END;

CREATE TABLE IF NOT EXISTS agent_model_route_bindings (
    agent_session_id TEXT PRIMARY KEY REFERENCES agent_sessions(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    provider TEXT NOT NULL CHECK(length(provider) BETWEEN 1 AND 160),
    model TEXT NOT NULL CHECK(length(model) BETWEEN 1 AND 160),
    reasoning_effort TEXT NOT NULL CHECK(length(reasoning_effort) BETWEEN 1 AND 32),
    model_profile_sha256 TEXT CHECK(model_profile_sha256 IS NULL OR (length(model_profile_sha256) = 64 AND model_profile_sha256 NOT GLOB '*[^0-9a-f]*')),
    route_sha256 TEXT NOT NULL CHECK(length(route_sha256) = 64 AND route_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_model_route_bindings_run
    ON agent_model_route_bindings(run_id, agent_session_id);

-- Bind the durable session receipt to both its owning run and that run's
-- write-once authority receipt in the same insert that precedes thread/start.
CREATE TRIGGER IF NOT EXISTS agent_model_route_bindings_require_run_route
BEFORE INSERT ON agent_model_route_bindings
WHEN NOT EXISTS (
    SELECT 1
    FROM agent_sessions AS a
    JOIN run_model_routes AS r ON r.run_id = a.run_id
    WHERE a.id = NEW.agent_session_id
      AND a.run_id = NEW.run_id
      AND r.run_id = NEW.run_id
      AND r.provider = NEW.provider
      AND r.model = NEW.model
      AND r.reasoning_effort = NEW.reasoning_effort
      AND r.route_sha256 = NEW.route_sha256
      AND r.model_profile_sha256 IS NEW.model_profile_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'agent model route binding does not match owning run receipt');
END;
CREATE TRIGGER IF NOT EXISTS agent_model_route_bindings_no_update
BEFORE UPDATE ON agent_model_route_bindings
BEGIN
    SELECT RAISE(ABORT, 'agent model route bindings are immutable');
END;
CREATE TRIGGER IF NOT EXISTS agent_model_route_bindings_no_delete
BEFORE DELETE ON agent_model_route_bindings
BEGIN
    SELECT RAISE(ABORT, 'agent model route bindings are immutable');
END;
