# Operator control plane implementation status

**Current implementation branch:** `implement/pr4-operator-control-completion`.
This status is intentionally a capability boundary, not release evidence.

## Implemented, read-only/observe-only slices

- durable source-owned attention, canonical control-plane snapshots, and return
  cursor acknowledgement;
- immutable investigation artifact contracts/reducer, passive
  external-condition observations, and authenticated registration of a
  run-owned local absolute-time gate; production scoped investigation dispatch is
  fail-closed until the App Server supports readable-root enforcement plus a
  controller-visible read-event stream;
- deterministic allow-list material-progress classification;
- liveness episodes with immutable observations; ordinary activity cannot clear
  degraded/stalled state, plus exact-revision immutable receipts for completed
  controller-path interventions. The only active executor is the authenticated
  `wait` receipt, which cannot alter custody or recovery; all other typed
  interventions remain inactive;
- reconciliation episode, immutable inventory finding/action-receipt, and
  exclusive-ownership proof custody, including one-use transactional
  proof-to-replacement authorization for authenticated retry. It requires a
  terminal attempt, explicitly preserved clean worktree, matching live HEAD,
  no active path lease/agent or prior command effect, and a durable operator action; it neither
  releases custody nor retries unknown effects implicitly;
- bounded run topology table over durable run/task/attempt/agent/worktree and
  dependency facts; no graph layout or inferred links;
- versioned local presence and deterministic, retry-safe in-product
  notification-mirror receipts; delivery cannot close attention or change
  controller authority;
- bounded supervisor snapshots carrying control-plane custody facts and
  restricting actions to wait/pause-for-human whenever ownership is uncertain;
- authenticated creation of an unreviewed, display-only knowledge candidate
  from one fresh confirmed investigation finding, with controller-derived
  evidence, repository scope, sensitivity, freshness, identity, and immutable
  readback by that identity;
- authenticated exact-revision `wait` liveness intervention receipts, which
  only record the bounded decision and increment the episode counter;
- authenticated localhost API, CLI, and browser control-plane surfaces for the
  above read models.

## Explicitly not activated or complete

- automatic reconciliation replacement, lease release, approval invalidation,
  and session resumption; restart loss records preservation receipts and retains
  uncertain custody, while only the authenticated proof-consuming retry path
  may authorize a clean replacement;
- all non-`wait` typed intervention execution;
- external-condition polling/wake adapters beyond the controller-clock time
  gate, whose terminal event remains non-authorizing;
- notification batching, desktop delivery, and delivery-health rollout;
- adaptive supervisor policy or any independent control-plane mutation;
- liveness/reconciliation-pattern knowledge candidates, knowledge review UI,
  and any context injection or activation beyond the existing governed
  pipeline;
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

Build output is isolated under `/mnt/bulk-fast/agent-builds/nmch-pr4`; do not
start workspace-wide builds when the controlled capacity policy is unmet.

## Promotion boundary

No deployment or supervisory activation may use this status as release proof.
The exact final artifact still needs complete integrated tests, an independent
SOL-xhigh review/signoff, isolated local HTTP/database smoke evidence, binary
provenance, and the activation evidence gates in
`EVALUATION_AND_ROLLOUT.md`.
