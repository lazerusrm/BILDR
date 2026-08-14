# Operator control plane implementation status

**Current integrated head:** `9dff73b` on
`implement/pr4-operator-control-foundation` (stacked on PR3 supervisor head
`f58874d`).

## Implemented, read-only/observe-only slices

- durable source-owned attention, canonical control-plane snapshots, and return
  cursor acknowledgement;
- immutable investigation artifacts and passive external-condition observations;
- deterministic allow-list material-progress classification;
- liveness episodes with immutable observations; ordinary activity cannot clear
  degraded/stalled state, and the reducer executes no intervention;
- reconciliation episode and exclusive-ownership proof custody; records cannot
  reset work, release leases, or authorize a replacement attempt;
- bounded run topology table over durable run/task/attempt/agent/worktree and
  dependency facts; no graph layout or inferred links;
- authenticated localhost API, CLI, and browser control-plane surfaces for the
  above read models.

## Explicitly not activated or complete

- controller-owned reconciliation actions and proof consumption;
- typed intervention execution;
- external-condition polling/wake adapters;
- presence/notification delivery and batching;
- supervisor v2 integration with control-plane facts;
- governed-knowledge candidate integration;
- evaluation corpus, fault matrix, usability study, canary, rollback drill, and
  production activation.

These are not implied by the existing tables or projections. Empty, current
sections are not evidence that a run is healthy, reconciled, or safe to resume.

## Verification recorded at this head family

- `cargo test -p harness-store -p harness-api -p harnessctl --lib --bins -- --test-threads=1`
- `npm --prefix ui test -- --run`
- `npm --prefix ui run build`
- `cargo run -p xtask -- openapi-check`

Temporary test output must use `/mnt/dev-fast`: `/mnt/bulk-fast` was at 100%
capacity during this implementation and must not be used for new build output.

## Promotion boundary

No deployment or supervisory activation may use this status as release proof.
The exact final artifact still needs complete integrated tests, an independent
SOL-xhigh review/signoff, isolated local HTTP/database smoke evidence, binary
provenance, and the activation evidence gates in
`EVALUATION_AND_ROLLOUT.md`.
