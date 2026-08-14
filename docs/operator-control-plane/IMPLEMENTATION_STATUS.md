# Operator control plane implementation status

**Current integrated branch:** `implement/pr4-operator-control-foundation`
(rebased onto deployed PR3 supervisor head `63d6cc3`). This status is intentionally a
capability boundary, not release evidence.

## Implemented, read-only/observe-only slices

- durable source-owned attention, canonical control-plane snapshots, and return
  cursor acknowledgement;
- immutable investigation artifacts and passive external-condition observations;
- deterministic allow-list material-progress classification;
- liveness episodes with immutable observations; ordinary activity cannot clear
  degraded/stalled state, plus exact-revision immutable receipts for completed
  controller-path interventions; the reducer and UI execute no intervention;
- reconciliation episode and exclusive-ownership proof custody; records cannot
  reset work, release leases, or authorize a replacement attempt;
- bounded run topology table over durable run/task/attempt/agent/worktree and
  dependency facts; no graph layout or inferred links;
- versioned local presence and deterministic, retry-safe in-product
  notification-mirror receipts; delivery cannot close attention or change
  controller authority;
- bounded supervisor snapshots carrying control-plane custody facts and
  restricting actions to wait/pause-for-human whenever ownership is uncertain;
- authenticated localhost API, CLI, and browser control-plane surfaces for the
  above read models.

## Explicitly not activated or complete

- controller-owned reconciliation actions and proof consumption;
- typed intervention execution;
- external-condition polling/wake adapters;
- notification batching, desktop delivery, and delivery-health rollout;
- adaptive supervisor policy or any independent control-plane mutation;
- governed-knowledge candidate integration;
- evaluation corpus, fault matrix, usability study, canary, rollback drill, and
  production activation.

These are not implied by the existing tables or projections. Empty, current
sections are not evidence that a run is healthy, reconciled, or safe to resume.

## Verification recorded at this head family

- `cargo test -p harness-store -p harness-api -p harnessctl --lib --bins -- --test-threads=1`
- `cargo test --workspace --all-targets -- --test-threads=1`
- `npm --prefix ui test -- --run`
- `npm --prefix ui run build`
- `cargo run -p xtask -- schema-check`
- `cargo run -p xtask -- openapi-check`

Temporary test output must use `/mnt/dev-fast`: `/mnt/bulk-fast` was at 100%
capacity during this implementation and must not be used for new build output.

## Promotion boundary

No deployment or supervisory activation may use this status as release proof.
The exact final artifact still needs complete integrated tests, an independent
SOL-xhigh review/signoff, isolated local HTTP/database smoke evidence, binary
provenance, and the activation evidence gates in
`EVALUATION_AND_ROLLOUT.md`.
