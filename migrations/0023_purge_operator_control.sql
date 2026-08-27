-- Operator-control records are run-scoped but reference runs by value, not by
-- foreign key, so a run purge left attention, liveness, and reconciliation
-- records behind. Orphaned attention is not harmless: it keeps asking the
-- operator to act on work that no longer exists.
--
-- These records and their children are now removed with their run. Their
-- immutability guards gain the same purge condition used in 0022: the child
-- tables have no run_id of their own, so they key off a purge being in flight.

DROP TRIGGER IF EXISTS attention_events_no_delete;
CREATE TRIGGER attention_events_no_delete BEFORE DELETE ON attention_events
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'attention events are immutable'); END;

DROP TRIGGER IF EXISTS notification_deliveries_no_delete;
CREATE TRIGGER notification_deliveries_no_delete BEFORE DELETE ON notification_deliveries
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'notification deliveries are immutable'); END;

DROP TRIGGER IF EXISTS notification_presentation_receipts_no_delete;
CREATE TRIGGER notification_presentation_receipts_no_delete BEFORE DELETE ON notification_presentation_receipts
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'notification presentation receipts are immutable'); END;

DROP TRIGGER IF EXISTS liveness_observations_no_delete;
CREATE TRIGGER liveness_observations_no_delete BEFORE DELETE ON liveness_observations
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'liveness observations are immutable'); END;

DROP TRIGGER IF EXISTS intervention_receipts_no_delete;
CREATE TRIGGER intervention_receipts_no_delete BEFORE DELETE ON intervention_receipts
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'intervention receipts are immutable'); END;

DROP TRIGGER IF EXISTS reconciliation_findings_no_delete;
CREATE TRIGGER reconciliation_findings_no_delete BEFORE DELETE ON reconciliation_findings
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'reconciliation findings are immutable'); END;

DROP TRIGGER IF EXISTS reconciliation_actions_no_delete;
CREATE TRIGGER reconciliation_actions_no_delete BEFORE DELETE ON reconciliation_actions
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'reconciliation actions are immutable'); END;

DROP TRIGGER IF EXISTS reconciliation_proof_consumptions_no_delete;
CREATE TRIGGER reconciliation_proof_consumptions_no_delete BEFORE DELETE ON reconciliation_proof_consumptions
WHEN NOT EXISTS (SELECT 1 FROM run_purges)
BEGIN SELECT RAISE(ABORT, 'reconciliation proof consumptions are immutable'); END;

-- Records already orphaned by an earlier purge. The marker below satisfies the
-- guards for the length of this migration and is removed immediately after.
INSERT OR REPLACE INTO run_purges(run_id, started_at) VALUES('__migration__', 0);

-- Records already orphaned by an earlier purge. They reference runs that no
-- longer exist, so nothing can act on them and nothing links back to them.
DELETE FROM attention_events WHERE attention_id IN
    (SELECT id FROM attention_items WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM notification_presentation_receipts WHERE delivery_id IN
    (SELECT delivery_id FROM notification_deliveries WHERE attention_id IN
        (SELECT id FROM attention_items WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs)));
DELETE FROM notification_deliveries WHERE attention_id IN
    (SELECT id FROM attention_items WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM attention_items WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs);
DELETE FROM reconciliation_findings WHERE episode_id IN
    (SELECT episode_id FROM reconciliation_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM reconciliation_actions WHERE episode_id IN
    (SELECT episode_id FROM reconciliation_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM reconciliation_proof_consumptions WHERE episode_id IN
    (SELECT episode_id FROM reconciliation_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM reconciliation_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs);
DELETE FROM liveness_observations WHERE episode_id IN
    (SELECT episode_id FROM liveness_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM intervention_receipts WHERE episode_id IN
    (SELECT episode_id FROM liveness_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs));
DELETE FROM liveness_episodes WHERE run_id IS NOT NULL AND run_id NOT IN (SELECT id FROM runs);

DELETE FROM run_purges WHERE run_id='__migration__';
