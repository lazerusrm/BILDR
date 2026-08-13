# Supervisory Runtime Delivery Status

This delivery intentionally implements the safe foundation, not the complete
supervisory roadmap described by PR #3.

## Included now

- SO-001 closed IDs, enums, configuration, and read-only role declarations;
- immutable, hash-verified snapshot custody and a per-run observation cursor;
- an exact controller-event allow-list, two-second coalescing, and bounded
  observe-only snapshot compiler;
- a run-detail receipt and UI panel that makes disabled versus observe-only
  status explicit.

`supervision.mode` defaults to `disabled`. In its only enabled mode,
`observe_only`, the runtime writes controller-state snapshots. It performs no
Terra or Sol call, creates no review/decision/action/expert row, changes no
run or task state, and offers only the displayed `wait` action.

## Deliberately deferred

SO-004 through SO-009 remain future work: Terra shadow decisions, policy
receipts, actions, Sol escalation, product controls, replay evaluation, and
canary activation. The `shadow`, `advisory`, `active_low_risk`, and `active`
configuration values are rejected at startup, rather than silently behaving as
observe-only or enabling partial authority.

The liveness setting is validated and retained as a policy contract for the
future scheduler slice; this delivery creates snapshots only from the explicit
material-event allow-list.
