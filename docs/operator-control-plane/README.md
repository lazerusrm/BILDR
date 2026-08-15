# Operator control plane

**Status:** partial runtime implementation; observe-only control projections are
available, while activation and the remaining rollout slices stay gated
**Prepared:** 2026-08-13
**Stack position:** extends the event-driven supervisor design

The operator control plane makes long-running BILDR work understandable,
recoverable, and safe to leave unattended. It adds controller-owned attention,
investigation, liveness, recovery, external-condition, projection, and
notification contracts without expanding model authority.

## Outcome

After implementation, an operator can answer these questions from one
authoritative product surface:

- What requires my action?
- What is currently progressing?
- What is waiting, blocked, or queued, and why?
- Which exact attempt and worktree own mutable work?
- What changed since I last looked?
- What evidence exists for claimed progress?
- What did recovery preserve, resume, invalidate, or refuse?
- What is the next legal action?
- What did the system spend to reach the current state?

## Design map

- [ADR-0012](../../adrs/ADR-0012-operator-control-plane.md) defines the decision,
  scope, invariants, adopted features, deferred work, and rejected alternatives.
- [PRODUCT_RESEARCH.md](PRODUCT_RESEARCH.md) records user problems, external
  evidence, comparative findings, and the feature-benefit decision matrix.
- [ARCHITECTURE.md](ARCHITECTURE.md) defines components, event flow,
  projections, reducers, recovery, notification, topology, and integration with
  supervision and knowledge governance.
- [CONTRACTS.md](CONTRACTS.md) defines domain types, states, persistence,
  events, API and CLI surfaces, authority ownership, and security boundaries.
- [UX_AND_OPERATIONS.md](UX_AND_OPERATIONS.md) defines the operator workflows,
  browser surfaces, accessibility behavior, presence modes, restart behavior,
  and operational controls.
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) provides implementation
  slices, exact repository areas, dependencies, tests, acceptance criteria,
  parallel lanes, and review order.
- [AGENT_EXECUTION_GUIDE.md](AGENT_EXECUTION_GUIDE.md) defines orchestration,
  path ownership, task packets, handoffs, stop conditions, conflict resolution,
  and review discipline for the implementation agents.
- [EVALUATION_AND_ROLLOUT.md](EVALUATION_AND_ROLLOUT.md) defines product
  hypotheses, countermetrics, replay and fault-injection suites, usability
  tests, release gates, canaries, and rollback.
- [TRACEABILITY_MATRIX.md](TRACEABILITY_MATRIX.md) maps every feature to its user
  problem, evidence, contract, implementation task, test, metric, and release
  gate.
- [REMOTE_EXECUTION_BOUNDARY.md](REMOTE_EXECUTION_BOUNDARY.md) defines the
  prerequisites and non-negotiable custody boundary for a later remote-node
  RFC. It authorizes no remote implementation.

## Relationship to existing architecture

This design does not replace the existing controller, governor, supervisor,
approval broker, evidence store, worktree isolation, self-improvement custody,
or final audit.

```text
operator goal
  -> existing plan and execution controller
  -> workers, verifiers, integration, and evidence
  -> operator-control records and projections
  -> event-driven supervisor receives a bounded run view
  -> browser, CLI, attention center, and return-to-work view
```

The controller remains authoritative. The operator control plane is a typed
read-and-reconciliation layer plus explicit controller commands.

## First credible vertical slice

Implement this before liveness automation or graphical topology:

```text
typed attention event
  -> durable attention projection
  -> canonical control-plane snapshot
  -> read-only API and CLI
  -> browser attention center
  -> task completion cannot erase unresolved attention
  -> restart reproduces the same open set
```

Then add investigation artifacts, liveness episodes, ownership-safe
reconciliation, presence-aware digests, return-to-work views, topology, and
controlled activation.

## Current implementation boundary

The current branch implements durable attention, investigation artifact
contracts/records, external-condition records, and authenticated run-owned
absolute-time or controller-owned repository-capacity gate registration, immutable
snapshots/return views, deterministic material progress, observe-only liveness episodes, exact-revision intervention receipts,
reconciliation/ownership evidence with immutable inventory findings and action
receipts, bounded run topology, a proof-consuming authenticated fresh-retry
path, and a retry-safe
in-product notification mirror. Each source-attention lifecycle transaction
atomically records its opening source, acknowledgement, or terminal authority
causal link; each mirror receipt atomically records its source-attention causal
link; each material-progress projection atomically
records its source-event causal link; each investigation artifact atomically
records its attempt-scoped causal link; each governed knowledge candidate
atomically records its evidence causal links; each liveness observation
atomically records its episode-scoped causal link; each reconciliation episode
opening and ownership proof atomically records its source-event causal link;
each reconciliation finding and action receipt, including proof-consuming
fresh-attempt authorization, atomically records its episode-scoped causal link;
and the API/CLI expose bounded immutable trace
lookup. This is not yet full trace propagation through every producer.
Supervisor snapshots receive bounded control-plane custody facts and restrict
recommendations on uncertain custody. The browser and CLI can read these
projections without acquiring work or changing controller state. The CLI can
version-check the local presentation-only presence preference; it cannot
deliver, suppress, retry, or resolve an item.

The pinned App Server protocol has no per-path readable-root allowlist or
controller-visible read-event stream. Therefore production investigation
dispatch is fail-closed: its artifact reducer is present, but no scoped
investigator may start until an enforcing runtime is supplied.

The authenticated fresh-retry path is narrow: it requires a terminal prior
attempt, explicitly preserved clean Git worktree, matching live/controller
HEAD, no active lease/agent or prior command effect, a durable operator action, and a
single-use proof/receipt/replacement-attempt transaction. Automatic retries
remain disabled because they do not yet have an equivalent proof issuer.

An authenticated operator may turn one fresh confirmed investigation finding
into an unreviewed, display-only knowledge candidate. The controller derives
the bounded statement, evidence receipt, repository scope, sensitivity,
freshness, and deterministic identity from the immutable artifact. This does
not activate knowledge, change task context, or write global memory.

The Improvement Center and `GET /api/v1/improvement/knowledge` expose a
bounded, repository-scoped collection of those immutable current records. Each
listed wire is integrity-checked and keeps candidate state, review, evidence,
scope, and freshness explicit. The collection remains display-only: it cannot
review, activate, inject, or otherwise use knowledge as execution authority.

An authenticated operator may also derive an unreviewed, display-only warning
from two immutable reconciliation episodes for the same trigger when each has
an exact preserved-candidate finding and authority-neutral preservation
receipt. The controller derives the statement and two episode receipts; this
does not call preservation a recovery, create a replacement attempt, activate
knowledge, change task context, or write global memory.

An authenticated operator may record an immutable notification shadow plan from
one complete control-plane snapshot and exact local-presence revision. It binds
every current attention revision to its already-durable immediate mirror receipt
and records the theoretical focus/unattended cadence, with critical attention
forced to immediate. A truncated snapshot is refused. The plan neither defers
nor suppresses delivery, sends no desktop notification, and cannot close its
source attention.

The sole active intervention is a version-bound `wait` receipt: it records a
bounded decision against one liveness episode and cannot clear a stall or
change any task, attempt, ownership, worktree, process, approval, or external
condition. Desktop notification delivery and active batching, all other
intervention execution, external provider wake adapters beyond the local clock
and repository-capacity adapters, adaptive supervisor
behavior, empirical evaluation, and rollout activation remain explicitly gated
until their own evidence passes.
Do not interpret an empty observe-only section as a healthy or automatically
recovered run.

## Critical implementation rules

- Do not add feature logic to the existing large
  `crates/harness-orchestrator/src/lib.rs`,
  `crates/harness-store/src/queries.rs`, or `ui/src/App.tsx`.
- Add focused modules with one contract owner.
- Treat projections as rebuildable views.
- Keep model-visible contracts narrower than operator/API contracts.
- Bind every mutable operation to exact run, task, attempt, worktree, HEAD,
  fingerprint, and policy state.
- Preserve unknown work and unknown external effects for reconciliation.
- Do not infer decisions, approvals, or ownership from prose.
- Make every release gate measurable and reversible.
