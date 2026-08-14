# Supervisory Runtime Delivery Status

This implementation is progressing through the PR #3 supervision roadmap. The
installed production binary remains the reviewed advisory foundation until the
remaining slices have their own exact-head evidence and rollout gates.

## Included in the implementation branch

- SO-001 closed IDs, enums, configuration, schemas, and read-only role
  declarations;
- SO-002/003 immutable snapshot custody, bounded event projection, digest
  binding, and a material-event allow-list;
- SO-004 a human-approved, read-only Terra advisory review, immutable
  decision receipt, stale-decision invalidation, and missing-rollout recovery;
- SO-005/SO-006 durable action and expert custody: a decision atomically
  materializes hash-verified action proposals, each with a closed
  policy/execution lifecycle; an expert request can originate only from a
  policy-accepted `request_expert` action; expert responses are append-only
  and hash-verified. A human-applied, high/critical typed brief starts exactly
  one controller-owned Sol `xhigh`/read-only/no-network/no-child session;
  its immutable advisory response triggers a fresh Terra review and never an
  executor call.
- explicit advisory application for the currently controller-backed handlers:
  `wait`, paused-run continuation, bounded final-review follow-up, and exact
  retry. The API/UI re-read snapshot freshness and exact target state, leave
  later-material-event proposals stale, and record a success/failure receipt.

The current operator setting selects only `disabled`, `observe_only`, or the
human-approved `advisory` route. Advisory application always requires an
explicit operator click; it does not run on a model result or maintenance tick.
Unregistered action kinds are durably policy-rejected. The Sol broker itself
starts only from a human-applied, policy-valid `request_expert` proposal, and
the browser has no background model polling loop.

## Deliberately deferred

Additional action handlers, the remaining supervision UI/CLI, replay
evaluation, and canary activation remain unfinished. `shadow`,
`active_low_risk`, and `active` remain rejected at startup rather than silently
falling back or enabling partial authority.

SO-009 activation requires the documented shadow/advisory evidence window:
14 days or 100 representative runs, plus zero stale/duplicate action
execution and the other release gates. Implementing the remaining code does
not waive that empirical requirement.
