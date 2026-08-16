# Operator control plane implementation plan

## Outcome

Implement ADR-0012 as a sequence of independently reviewable slices. The first
useful release is deterministic attention plus a canonical current-state/return
view. Liveness automation, notification suppression, optional graph UI, routing
adaptation, and remote execution stay behind later evidence gates.

## Program constraints

### Required base

This plan is stacked on `design/event-driven-supervisor-20260813` and assumes the
contracts in ADR-0011. Rebase implementation branches when the stack changes;
do not copy parent changes into this program.

### Hard boundaries

- `harnessd` remains sole controller/Git mutation authority.
- No generic attention-resolve, recovery-force, arbitrary external-condition
  command, or raw worktree reset API.
- No model writes current state or closes attention/recovery/liveness.
- No replacement mutable attempt without exclusive ownership proof.
- No automatic retry of ambiguous external effects.
- No feature logic added to the existing large orchestrator/store/API/UI files;
  use focused modules with thin registration.
- No automatic push, publication, readiness change, or merge.
- No free-form global memory or remote runtime in this program.
- Every adaptive capability starts disabled, observe-only, shadow, or advisory.

### Migration number

The stacked supervision implementation now occupies migrations `0013` through
`0016`. The operator-control foundation therefore uses:

```text
migrations/0017_operator_control_plane.sql
```

Confirm the number again immediately before integration; a later migration must
never be renumbered to fit an obsolete design placeholder.

## Delivery strategy

```text
Foundation
  OCP-001 domain/events
  OCP-002 persistence/reducers
  OCP-017 correlation foundation

First product slice
  OCP-003 canonical snapshot
  OCP-004 attention/source adapters
  OCP-011 read API/CLI
  OCP-012 attention/return UI

Evidence and waits
  OCP-005 investigation
  OCP-006 external conditions
  OCP-016 governed knowledge integration

Liveness and recovery
  OCP-007 material progress
  OCP-008 liveness episodes
  OCP-009 reconciliation/ownership
  OCP-010 interventions

Presentation and integration
  OCP-013 topology
  OCP-014 presence/notifications
  OCP-015 supervisor integration

Assurance and activation
  OCP-018 invariant/fault suite
  OCP-019 product/usability evaluation
  OCP-020 rollout/operations
```

## OCP-001 — Domain types, event vocabulary, and module extraction

### Work

Create `crates/harness-domain/src/operator_control.rs` and thin exports from
`lib.rs`.

Add:

```text
new ID types
TaskExecutionKind
AttentionCategory/Severity/State/SourceType
InvestigationArtifact/Finding/Recommendation/DecisionInventory
MaterialProgressKind/Event
LivenessState/ObservationKind/InterventionKind
ReconciliationTrigger/State/FindingKind/ActionKind
ExternalConditionAdapter/State
OperatorPresenceMode
NotificationClass/State
snapshot/return/topology/correlation DTOs
transition validators and bounded identifier helpers
```

Require every `TaskPacket` to declare its execution kind and investigation
scope explicitly. Add event names and serialization fixtures. Do not add
generic state setters or omitted-field defaults.

### Required tests

- JSON round trip and snapshots for every enum/record;
- unknown enum rejection;
- legal/illegal transition tables;
- identifier/size bounds;
- missing task execution kind or investigation scope is rejected;
- investigation cannot request mutable sandbox/lease/candidate;
- terminal record idempotency with same receipt only.

### Exit criteria

All downstream lanes can compile against stable closed types and source-owner
semantics without editing the large domain file beyond module registration.

## OCP-002 — Persistence, repositories, and replay-safe reducers

### Schema

Create normalized tables for:

```text
attention_items / attention_events
investigation_artifacts
material_progress_events
liveness_episodes / liveness_observations / intervention_receipts
reconciliation_episodes / reconciliation_findings / reconciliation_actions
ownership_proofs
external_conditions / condition_observations
control_plane_snapshots / snapshot_sections
operator_presence
notification_deliveries
return_view_cursors
topology_snapshots
correlation_links
```

Use foreign keys where authoritative records are guaranteed to coexist; use
typed opaque source refs when subsystem retention differs. Add unique dedupe
keys, monotonic revision/sequence constraints, claim-generation fields, and
indexes for open attention, active episodes, due conditions, pending delivery,
run/task/attempt lookup, and event cursor.

Avoid unsafe cascading deletes. Active/audit records survive source archival as
redacted identity references where required.

### Store API

Create `crates/harness-store/src/operator_control/` with repositories per
aggregate. APIs require expected revision/generation and return typed conflict,
not-found, integrity, and policy-neutral errors.

Provide:

- source-idempotent attention open/update/close;
- append-only observation/progress/receipt writes;
- transactional ownership transfer/proof consume;
- reconciliation claim/heartbeat/release;
- external-condition sequence append and claim;
- notification claim/delivery receipt;
- bounded projection reads;
- deterministic snapshot persistence and digest verification;
- rebuild/replay tools.

### Required tests

- migrate fresh and realistic pre-0013 databases;
- migration rollback strategy and integrity check;
- duplicate source/event/sequence/intervention idempotency;
- concurrent attention/reconciliation/condition/delivery claims;
- expected revision conflicts;
- crash between intent/effect/receipt;
- snapshot digest mismatch;
- replay from event zero and mid-cursor;
- retention/archival without dangling authority;
- database busy/IO/full scenarios.

### Exit criteria

A deterministic replay produces identical open attention, episodes, external
conditions, notification state, source cursors, and snapshot digests.

## OCP-003 — Canonical control-plane projection

### Work

Implement `operator_control/snapshot.rs` and store projection queries.

Compile stable sections from normalized authoritative records at a captured
event cursor. Enforce:

```text
stable ordering
server-side classification
per-section row/byte limits
time budget
source cursors
unknown/stale/error states
truncation metadata
canonical digest
persisted revision
```

Use bounded batched queries; do not issue one query per run/task. Cache only by
exact source cursor/config/limit identity. Authority handlers never consume the
projection for permission.

### Tests and performance

- fixture for empty, normal, large, degraded, stale, mixed-version states;
- source state versus expected section classification;
- omitted data never rendered as healthy empty;
- deterministic ordering/truncation/digest;
- concurrent event arrival and consistency label;
- 100 active/recent runs p95 target 250 ms;
- payload/default return limits;
- corrupt section preserves prior visible stale snapshot.

### Exit criteria

Browser, CLI, return view, notification classifier, and supervision can share a
single bounded read model with explainable source references.

## OCP-004 — Durable attention ledger and source adapters

### Source adapters

Implement adapters for:

```text
approval broker
typed operator decisions
credential/runtime requirements
publication actions
policy decisions/exceptions
evidence gaps
external conditions
reconciliation conflicts
infrastructure/system health
```

Each adapter declares:

```text
source identity/revision
dedupe key
category/severity mapping
redacted title/summary
options/evidence/blocked refs
valid closure outcomes
staleness/invalidation behavior
resurfacing policy
```

Open/update/close transactionally with source outcome where practical. Where the
source already exists, reconstruct attention deterministically on replay.

### Completion gates

Run/task completion cannot advance to a state that falsely implies no pending
operator action. Completion may coexist with nonblocking informational attention
but must surface it in the final/return view. Define explicit blocking categories
by source/risk; do not use title text.

### API boundaries

Add acknowledgement only as presentation state. All substantive actions route
to source-specific commands. Do not expose generic severity/category mutation.

### Required tests

- decision followed by progress and task completion remains open;
- source-only typed closure;
- acknowledgement survives restart and does not close;
- stale approval invalidates item through authoritative source event;
- repeated projection creates no duplicate;
- source revision changes supersede/reopen correctly;
- credential value never stored;
- criticality matrix and blocked-critical-path behavior;
- source archive/retention behavior;
- notification delivery cannot close.

### Exit criteria

Every authoritative human/external obligation in supported sources appears in
exactly one current item and remains until valid outcome.

## OCP-005 — First-class investigation execution and artifact

### Task creation

Extend plan/task validation and scheduler role setup:

- execution kind `investigation`;
- read-only sandbox;
- no mutable path lease;
- bounded read/search/probe/tool allowlist;
- question/scope/non-goals/source/evidence/budget/stop conditions;
- artifact completion required;
- no candidate/integration/publication transition.

Use a dedicated artifact schema and prompts/templates. Keep investigation
separate from ADR-0011 expert consultation: an investigation gathers repository
facts; expert consultation answers a bounded hard technical question.

### Artifact validator

Validate:

```text
schema/version/limits
base SHA and repository-state digest
source references and allowed paths
finding evidence/classification
explicit limitations
rejected hypotheses
recommendation authority
unresolved decision inventory
sensitivity/export rules
digest/canonicalization
```

One bounded schema-repair attempt may be allowed. Preserve invalid raw bounded
output under retention policy but do not register it as accepted evidence.

### Evidence and later reuse

Register accepted artifact in existing evidence/artifact stores. Permit later
plans/tasks and knowledge candidates to reference its digest. Record reuse.

### Required tests

- investigation cannot mutate tracked/untracked files or Git;
- no mutable lease/candidate/integration state;
- accepted/invalid/oversize/stale-base/source-out-of-scope artifacts;
- malicious external content remains data;
- decision inventory required even when empty;
- restart during generation/validation;
- later task consumes artifact without copying unbounded prose;
- export/sensitivity/redaction.

### Exit criteria

A read-only investigation produces reusable exact evidence and cannot alter the
implementation lineage.

## OCP-006 — External condition registry

### Initial adapters

Implement typed adapters for:

```text
GitHub check/workflow status when connector/config permits
review/PR state
credential/account capability
absolute time gate
local hardware/resource availability
bounded HTTP/service health only when repository policy allows
```

Each adapter owns strict spec validation and typed result parsing. Do not accept
arbitrary shell commands, model-generated polling code, or generic JSONPath-to-
action configuration.

### Runner

Add bounded concurrency, claim generation, jittered backoff, deadlines, rate
limits, source identity digest, sequence persistence before publication, result
size/redaction, cancellation, continuity-break detection, and startup recovery.

### Wake-only boundary

Terminal condition emits material controller event and scheduler/supervisor wake.
No adapter executes a follow-up action. Result content is untrusted.

### Required tests

- duplicate/out-of-order observation;
- crash before/after observation write and event publish;
- source identity changes/shrinks/rewinds;
- rate limit/timeout/malformed/oversize response;
- credentials absent/expired;
- canceled/expired/deadline;
- duplicate terminal result emits one material wake;
- restart maintains sequence;
- no arbitrary command/external result authority.

### Exit criteria

Long waits consume no model turns and resume the controller exactly once per
new durable terminal sequence without granting authority.

## OCP-007 — Material progress classifier

### Work

Create deterministic rules over candidate, Git, artifact, validation, finding,
milestone, external condition, and integration events. Version the classifier.
Emit deduped `MaterialProgressEvent` with evidence.

Create labeled fixtures for:

```text
candidate first materialization
format-only/change-noise versus material change
validation regression/advance
same failure repeated
new/resolved blocking finding
milestone evidence
investigation acceptance
integration frontier
external dependency satisfaction
heartbeats/output/token/tool noise
```

### Evaluation

Use independent labels for disputed cases and preserve disagreement. Report
precision/recall by task/risk/role class and impact-weighted errors. Do not
collapse to one progress score.

### Exit criteria

The classifier is deterministic, replayable, useful for liveness/supervision,
and does not treat ordinary activity as progress.

## OCP-008 — Stateful liveness episodes

### Observation collector and reducer

Collect normalized evidence without model calls:

- process/session/turn state;
- bounded command state;
- worktree/candidate identity;
- material events;
- validator/finding trend;
- active external condition;
- repeated semantic action/failure signatures;
- budget/no-progress boundary;
- reconciliation/ownership state.

Reduce one episode per exact attempt generation. Persist reason codes, source
refs, state revision, last progress, next review, interventions, and outcome.

### Initial mode

Observe-only, then shadow. The product shows classifications and disagreement
with current behavior; it executes nothing.

### Required tests

- healthy quiet compile/test;
- external wait;
- active process with repeated unchanged failure;
- output activity without material progress;
- material progress after degradation;
- dead process with useful worktree;
- live process with identity mismatch;
- unknown command/external effect;
- stale observations and duplicate events;
- restart/replay;
- role/task-class thresholds;
- no state clearing from model prose.

### Exit criteria

Held-out traces demonstrate policy-defined precision/recall, and zero shadow
recommendations violate ownership/custody constraints.

## OCP-009 — Reconciliation controller and ownership proof

### Startup and runtime integration

Create a bounded startup pass after store/runtime checks and before mutable
scheduler dispatch. Also trigger targeted episodes for session/process loss,
lease expiry, account handoff, worktree mismatch, unknown command, version
transition, and operator request.

### Inventory and proof

Inspect exact:

```text
run/task/attempt/session/worktree IDs
base/candidate/HEAD/ancestry
mutable worktree fingerprint
lease owner/generation/expiry
process identity and command group
App Server thread/turn state
approvals and bound fingerprints
artifacts/evidence/candidate lineage
external effects and condition state
```

Exclusive ownership proof requires all previous mutable owner and ambiguous
effect fields to be terminal/reconciled. Consume proof transactionally when
authorizing a fresh attempt.

### Preservation rules

- never delete/reset/clean unknown work;
- preserve staged, unstaged, and untracked state in fingerprint/report;
- invalidate stale approvals through approval owner;
- requeue verification only on exact candidate identity;
- distinguish preserved-and-paused from resumed/recovered;
- cap work per pass and persist cursor/claim.

### Required fault tests

Kill/restart at every boundary around:

```text
attempt/worktree creation
lease claim
process spawn
thread/turn creation
command start/result projection
candidate commit/custody
approval open/decision
artifact write/register
validation start/result
integration action
external condition observation/event
reconciliation intent/effect/receipt
```

Verify zero duplicate writers, zero discarded work, no ambiguous effect retry,
idempotent restart, and accurate report.

### Exit criteria

All nonterminal work reaches a legal state: attached/resumed, preserved/paused,
reverification queued, fresh attempt authorized by proof, or explicit attention.

## OCP-010 — Typed intervention executor

### Work

Implement only closed actions with exact target revision, precondition digest,
policy version, dedupe key, budget, and receipt:

```text
wait
request targeted inspection
steer active turn
start follow-up turn
spawn read-only investigation
request verification
request reconciliation
pause for operator
stop run
```

Fresh attempt remains a reconciliation action. Reject stale/illegal/duplicate or
custody-incompatible requests. Capture later outcome window for evaluation.

### Activation

Start shadow, then advisory. Active-low-risk can include only reversible actions
that pass held-out and canary gates. No destructive process/worktree behavior.

### Exit criteria

Every accepted/rejected intervention is explainable and idempotent; no action
expands authority.

## OCP-011 — API, OpenAPI, SSE, and CLI

### Work

Update `openapi/harness-api.yaml`, add focused API/CLI modules and DTOs for routes
listed in `CONTRACTS.md`. Use stable cursor pagination, expected revisions,
source-specific mutations, bounded artifacts, redaction, and SSE replay.

### Security tests

- localhost Host/DNS-rebinding protections;
- session/CSRF/same-origin;
- expected revision conflict;
- source-owner authority;
- no generic resolve/force;
- malicious IDs/paths/payload sizes;
- sensitivity/export;
- SSE cursor/replay/redaction;
- CLI uses API only and stable exit codes.

### Exit criteria

Every product/automation surface can read canonical state and perform only typed
controller-legal actions.

## OCP-012 — Attention center and return view

### Work

Create `ui/src/operator-control/` modules from `UX_AND_OPERATIONS.md`. Implement
server-owned classifications, accessible list/detail/action workflows,
source/evidence links, stale conflict handling, unknown/stale/truncated states,
return cursor acknowledgement, cost/capacity and recovery sections.

Do not add feature logic to `App.tsx`; do not infer current state in TypeScript.

### Usability validation

Before broad release, run controlled tasks measuring decision discovery,
correctness/time of first action after interruption, navigation actions,
important omissions, accessibility, and payload/render performance.

### Exit criteria

Operators can resolve source actions and resume work without transcript
reconstruction; acknowledgement cannot be confused with resolution.

## OCP-013 — Run topology and evidence navigation

### Work

Compile typed nodes/edges with source refs and bounded ordering. Implement table
and detail panels first. Add graph behind disabled flag, lazy load, node/edge
caps, keyboard support, reduced motion, and no hidden facts.

### Exit criteria

Table view answers ownership/dependency/evidence questions accurately. Graph is
not activated without measured improvement.

## OCP-014 — Presence-aware notification delivery

### Phases

1. mirror classification with no suppression;
2. shadow batching and compare delivery;
3. opt-in focus/unattended batching;
4. canary broader opt-in only with evidence.

### Work

Implement presence persistence, deterministic classes, durable delivery claim,
critical bypass, maximum defer, chronological digest, receipts/retry,
delivery-health UI/CLI, desktop notification opt-in, and return digest.

### Hard gates

Zero omitted critical item, zero delivery-caused source closure, bounded high
latency, no secret payload, no authority change from mode.

### Exit criteria

Routine interruptions decrease without delayed/missed important action.

## OCP-015 — Supervisor integration

### Snapshot additions

Define the current `harness.supervisor-snapshot` contract after ADR-0011
implementation review. Include bounded refs/summaries
for open attention, liveness episodes, recovery conflicts, external waits,
investigation artifacts, and return cursor—not full artifacts or notification
preferences.

### Policy changes

- human-authority attention removes illegal automatic actions;
- ownership unknown exposes only wait/reconciliation/operator paths;
- investigation is read-only;
- external satisfaction is evidence, not action authority;
- supervisor cannot close/update operator-control records;
- decisions bind to exact snapshot revision/digest.

### Evaluation additions

Add cases for buried decisions, false stalls, useful dead worktrees, duplicate
ownership risk, unknown external effects, external waits, investigation reuse,
and notification irrelevance.

### Exit criteria

Supervision uses better facts without becoming a second controller.

## OCP-016 — Governed knowledge integration

Allow accepted investigation artifacts and repeated validated recovery/liveness
patterns to propose knowledge candidates to the existing pipeline. Require
scope, evidence, sensitivity, freshness/revalidation, contradiction,
supersession, review, and rollback. No single incident auto-activates knowledge.

Tests cover stale/contradicted exclusion, sensitivity, repository/runtime scope,
and no direct prompt/global memory write.

## OCP-017 — Correlation and causal trace graph

Propagate W3C-compatible trace/span IDs and BILDR domain IDs through API request,
run/task/attempt, App Server turn, command/tool, artifact/candidate,
validation/finding, supervisor/expert, intervention, recovery, external
condition, and notification delivery.

Use causal links for fan-out/fan-in. Add bounded redacted export and trace lookup.
Test parent/link validity, retries, restart continuity, duplicate IDs,
untrusted inbound context, redaction, and cost attribution.

## OCP-018 — Invariant, property, concurrency, and fault suite

### Hard invariants

```text
one mutable owner
unknown cannot authorize replacement
source-only attention closure
completion cannot hide blocking attention
acknowledgement/delivery cannot resolve
investigation cannot mutate/create candidate
ambiguous external effect never auto-retried
projection never authorizes
replay deterministic
stale version/digest rejected
critical notification not omitted
remote runtime absent
```

### Methods

Use table tests, state-machine/model-based tests, property generation,
concurrent claim tests, deterministic fake App Server/adapters, process fault
injection, SQLite failpoints, replay/golden fixtures, and bounded nightly fault
matrix. Keep a representative PR subset; do not bloat every CI path with
redundant end-to-end cases.

### Exit criteria

Every hard invariant maps to executable evidence and failure injection, with
seed/reproduction for generated cases.

## OCP-019 — Product and usability evaluation

Run studies defined in `EVALUATION_AND_ROLLOUT.md`:

- return view versus current UI;
- topology table versus table+graph;
- notification mirror versus batching;
- investigation artifact versus transcript-only handoff;
- liveness/recovery trace replay;
- attention coverage and duplicate/false-critical analysis.

Primary outcomes and countermetrics must both be reported. Preserve negative and
inconclusive results. No candidate feature self-grades its own promotion.

## OCP-020 — Rollout, operations, and activation

### Recommended activation order

```text
attention + snapshot: active after deterministic gates
return view: canary after usability result
investigation: active after custody/evidence tests
external conditions: wake-only active after continuity tests
liveness: observe -> shadow -> advisory -> active-low-risk
reconciliation: report/attention -> idempotent safe actions -> proof-gated replacement
notification: mirror -> shadow -> opt-in batching
optional graph: disabled until usability win
supervisor additions: shadow/advisory with ADR-0011 rollout
knowledge: candidate-only under existing governance
remote execution: absent
```

### Configuration and runbook

Add closed per-capability modes, thresholds/version IDs, bounded limits,
notification defer policy, reconciliation budgets, and emergency disable.
Document startup/recovery, attention source diagnosis, projection rebuild,
external adapter repair, notification degradation, trace export, rollback, and
integrity verification.

### Promotion gates

Require exact implementation SHA, schema/policy/config versions, datasets/splits,
hard invariant result, primary/countermetrics, security/privacy, performance,
known limitations, rollback drill, and independent review.

## Parallel implementation lanes

- **Lane A:** OCP-001/002 persistence and reducers.
- **Lane B:** OCP-003/004/011 projection, attention, API/CLI.
- **Lane C:** OCP-005/006/016 investigation, waits, knowledge.
- **Lane D:** OCP-007/008/009/010 progress, liveness, recovery, interventions.
- **Lane E:** OCP-012/013/014 UI and notifications.
- **Lane F:** OCP-017/018/019 trace, faults, product evaluation.

Integration order follows dependencies. Lanes may share domain contracts only
after OCP-001 review; they may not independently create competing enums/tables.
Recovery/ownership changes receive independent high-risk review before
integration.

## Review protocol

Review in this order:

1. controller authority and source ownership;
2. identity, revisions, digests, custody, and redaction;
3. state machines and uncertainty;
4. concurrency, claims, replay, and restart;
5. Git/worktree/command/external-effect safety;
6. source-only attention closure;
7. model and external-input boundaries;
8. API/CLI/UI explainability and accessibility;
9. performance/retention;
10. product benefit and rollout.

## Definition of done

The program is complete only when:

- every supported required action remains durable;
- all current-state surfaces consume one canonical projection;
- work survives tested failures without duplicate writer or silent discard;
- investigation output is reusable exact evidence;
- liveness interventions improve progress without destructive errors;
- notification changes reduce routine interruption without hiding important
  action;
- return/topology features demonstrate user benefit or remain disabled;
- every action is traceable to evidence, policy, identity, and receipt;
- adaptive behavior is measured, canaried, and reversible;
- operations and recovery drills pass;
- an independent final audit accepts the exact implementation head.
