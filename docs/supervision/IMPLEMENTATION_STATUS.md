# Supervisory Runtime Delivery Status

This implementation is progressing through the PR #3 supervision roadmap.
The installed production binary remains the reviewed advisory foundation until
the remaining slices have their own exact-head evidence and rollout gates.

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
  and hash-verified.

The current operator setting selects only `disabled`, `observe_only`, or the
human-approved `advisory` route. Even in advisory mode, no action proposal is
executed and no Sol request starts until the later handler/broker slices are
complete. The browser has no background model polling loop.

## Deliberately deferred

Action handlers and explicit advisory application, the Sol turn broker, the
remaining supervision UI/CLI, replay evaluation, and canary activation remain
unfinished. `shadow`, `active_low_risk`, and `active` remain rejected at
startup rather than silently falling back or enabling partial authority.

SO-009 activation requires the documented shadow/advisory evidence window:
14 days or 100 representative runs, plus zero stale/duplicate action
execution and the other release gates. Implementing the remaining code does
not waive that empirical requirement.
