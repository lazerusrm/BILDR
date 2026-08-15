-- Phase-two notification batching is evidence only. Every immutable shadow
-- plan is bound to an existing exact snapshot and cannot alter the immediate
-- in-product delivery receipts that remain the production baseline.

CREATE TABLE IF NOT EXISTS notification_shadow_batches (
    id TEXT PRIMARY KEY,
    operator_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL REFERENCES control_plane_snapshots(id),
    snapshot_revision INTEGER NOT NULL CHECK(snapshot_revision > 0),
    policy_id TEXT NOT NULL,
    identity_sha256 TEXT NOT NULL UNIQUE CHECK(length(identity_sha256) = 64 AND identity_sha256 NOT GLOB '*[^0-9a-f]*'),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notification_shadow_batches_operator_created
    ON notification_shadow_batches(operator_id, created_at DESC, id DESC);

CREATE TRIGGER IF NOT EXISTS notification_shadow_batches_no_update
BEFORE UPDATE ON notification_shadow_batches
BEGIN
    SELECT RAISE(ABORT, 'notification shadow batches are immutable');
END;
CREATE TRIGGER IF NOT EXISTS notification_shadow_batches_no_delete
BEFORE DELETE ON notification_shadow_batches
BEGIN
    SELECT RAISE(ABORT, 'notification shadow batches are immutable');
END;
