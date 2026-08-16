# Operator control plane implementation status

**Current implementation branch:** `implement/pr4-operator-control-completion`.
This status is intentionally a capability boundary, not release evidence.

## Implemented, read-mostly slices

- durable source-owned attention, canonical control-plane snapshots, and return
  cursor acknowledgement;
- immutable investigation artifact contracts/reducer, passive
  external-condition observations, and authenticated registration of a
  run-owned local absolute-time gate or repository-root capacity gate. Capacity
  observations use a closed no-path/no-command spec, deterministic bounded
  backoff, source-identity continuity detection, and wake-only terminal events;
  production scoped investigation dispatch is fail-closed until the App Server
  supports readable-root enforcement plus a controller-visible read-event stream;
- deterministic allow-list material-progress classification;
- liveness episodes with immutable observations; ordinary activity cannot clear
  degraded/stalled state, plus exact-revision immutable receipts for completed
  controller-path interventions. The active executors are authenticated `wait`
  and `pause_for_operator`: the latter is available only for an exact
  confirmed-stall or recovery-required episode and atomically records its
  local-session audit row, pauses that bound run's scheduler, and stores the
  immutable receipt. It cannot retry, resume, release, or change an attempt;
  all other typed interventions remain inactive;
- reconciliation episode, immutable inventory finding/action-receipt, and
  exclusive-ownership proof custody. The one-use transactional proof consumer
  exists, but fresh retry is disabled because no authoritative runtime issuer
  can yet prove process/session/command/external-effect closure; it neither
  releases custody nor retries unknown effects implicitly;
- bounded run topology table over durable run/task/attempt/agent/worktree and
  dependency facts; no graph layout or inferred links;
- versioned local presence, deterministic pending in-product notification
  claims, exact session-derived presentation receipts, and bounded
  integrity-checked current-revision presentation health; no receipt can close
  attention or change controller authority;
- authenticated, immutable notification shadow plans bound to one complete
  control-plane snapshot, exact local-presence revision, policy digest, and
  already-durable pending in-product claims. They prove critical bypass and theoretical
  bounded cadence only; they cannot defer, suppress, send, or resolve anything;
- bounded supervisor snapshots carrying control-plane custody facts and
  restricting actions to wait/pause-for-human whenever ownership is uncertain;
- authenticated creation of an unreviewed, display-only knowledge candidate
  from one fresh confirmed investigation finding, with controller-derived
  evidence, repository scope, sensitivity, freshness, identity, and immutable
  readback by that identity, plus a bounded integrity-checked current-record
  collection scoped to one exact repository and displayed in the Improvement
  Center. An authenticated local human may accept or reject only the exact
  current candidate SHA; the receipt binds its pre-review immutable revision
  and acceptance requires fresh controller-clean evidence. Reviewed knowledge
  is still not injected into task context or execution. The same exact-SHA
  review is available through the authenticated API, browser, and `harnessctl`;
- authenticated creation of an unreviewed, display-only heuristic from two
  independently recovered liveness episodes, with controller-derived exact
  observation receipts and no activation or context injection;
- authenticated creation of an unreviewed, display-only warning from two
  independently preserved reconciliation episodes with the same trigger, using
  exact episode evidence plus controller-verified preservation findings and
  receipts; preservation does not imply recovery or retry authority;
- authenticated exact-revision liveness interventions: `wait` records only
  the bounded decision, while `pause_for_operator` may atomically pause the
  bound run scheduler from a confirmed-stall or recovery-required episode;
- authenticated localhost API, CLI, and browser control-plane surfaces for the
  above read models.

## Explicitly not activated or complete

- automatic reconciliation replacement, lease release, approval invalidation,
  session resumption, and every retry-created replacement; restart loss records
  preservation receipts and retains uncertain custody. No route may create a
  clean replacement until a controller-owned transaction can consume an
  independently recorded ownership proof while creating that exact attempt;
- all typed intervention execution other than `wait` and the exact
  confirmed-stall/recovery-required `pause_for_operator` scheduler pause;
- external-condition polling/wake adapters beyond the controller-clock and
  controller-owned repository-capacity gates, whose terminal events remain
  non-authorizing;
- active opt-in/broader notification batching, desktop delivery, and
  delivery-health rollout;
- adaptive supervisor policy or any independent control-plane mutation;
- any knowledge context injection or execution activation beyond the existing
  governed display pipeline;
- held-out evaluation corpus, fault-matrix execution evidence, usability study,
  canary, rollback drill, and production activation. The closed OCP-018 fault
  receipt contract is implemented, but no receipt is promotion evidence until
  it pins this exact tested implementation SHA and all twelve results hold.

These are not implied by the existing tables or projections. Empty, current
sections are not evidence that a run is healthy, reconciled, or safe to resume.

## Historical validation commands (not current-head proof)

- `cargo test -p harness-store -p harness-api -p harnessctl --lib --bins -- --test-threads=1`
- `cargo test --workspace --all-targets -- --test-threads=1`
- `npm --prefix ui test -- --run`
- `npm --prefix ui run build`
- `cargo run -p xtask -- schema-check`
- `cargo run -p xtask -- openapi-check`

These are prior validation commands, not release evidence for the current
source. Build output must use an isolated `CARGO_TARGET_DIR`; the exact final
head needs fresh successful results before it can be signed or deployed.

## Promotion boundary

No production deployment or supervisory activation may use this status as
release proof. A controlled localhost test-harness cutover may exercise the
current shapes, but it does not promote any capability. The exact final
artifact still needs complete integrated tests, an independent SOL-xhigh
review/signoff, isolated local HTTP/database smoke evidence, binary provenance,
and the activation evidence gates in
`EVALUATION_AND_ROLLOUT.md`.
