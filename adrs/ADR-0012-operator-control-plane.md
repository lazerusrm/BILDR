# ADR-0012: Add an operator control plane for durable oversight and recovery

- **Status:** proposed architecture
- **Date:** 2026-08-13
- **Decision owner:** BILDR controller architecture
- **Depends on:** ADR-0011 event-driven supervisory orchestration
- **Extends:** ADR-0003 event-sourcing-lite, ADR-0004 worktree-per-mutable-task,
  ADR-0005 controller authority, ADR-0006 reasoning visibility, ADR-0007
  governed self-improvement, ADR-0010 branch-aware trace graph

## Context

BILDR already has the correct execution authority boundary. `harnessd`, not a
model, owns run and task state, path leases, exact-SHA Git custody, worktrees,
commands, validation, evidence, approvals, budgets, integration, and
publication. ADR-0011 adds an event-driven read-only supervisor that interprets
controller facts and proposes only closed controller actions.

The remaining product gap is operator control across long-running and
interrupted work. The current state model can represent blocked, stalled,
interrupted, or failed tasks, but those point states do not by themselves solve
these user problems:

1. A decision discovered during investigation can disappear from view when the
   task later reports progress or completes.
2. A dead process or App Server session does not prove that its worktree,
   attempt, command, or candidate is abandoned. Starting a replacement without
   reconciling ownership can create two writers or discard useful work.
3. A worker can emit activity while making no material progress. Conversely, a
   worker can be healthy while quiet, compiling, waiting for validation, or
   blocked on an exact external condition. A timeout alone is not a safe
   intervention policy.
4. Investigation currently risks becoming an informal worker mode whose useful
   output exists only in conversation. That loses evidence, repeats discovery,
   and makes later implementation depend on a lossy summary.
5. Frequent progress notifications create noise, while aggressive batching can
   hide a decision, credential request, security event, or custody conflict.
6. The browser, CLI, supervisor, recovery code, and future automation can drift
   if each reconstructs current state from different queries or event tails.
7. Restart and deployment recovery are operational facts, but the operator
   needs a durable explanation of what was preserved, resumed, invalidated,
   blocked, or refused.
8. Future remote or hardware-specific execution is valuable only if the same
   custody, identity, provenance, and unknown-completion rules survive a
   transport boundary.

These are not reasons to add another autonomous agent. They are reasons to add
typed controller state, deterministic projections, durable recovery, and
operator-focused product surfaces.

## Decision

Add an **operator control plane** above the existing controller records and
below every human-facing or model-facing view.

The operator control plane consists of the following controller-owned
capabilities.

### 1. Canonical control-plane projection

Add a bounded, revisioned `ControlPlaneSnapshot` compiled from authoritative
store state at an exact event cursor. It is a read model, not a new source of
truth.

The browser, CLI summary, return-to-work view, notification classifier, and
whole-system diagnostics consume this projection. The per-run supervisor
snapshot remains a separate, narrower model contract, but references the same
authoritative run, task, attention, liveness, recovery, and evidence records.

### 2. Durable attention ledger

Add `AttentionItem` as the normalized operator-facing projection for work that
requires a person or an external actor.

An attention item references, but does not replace, the subsystem that owns the
actual authority. Examples include an approval, goal decision, credential
request, recovery conflict, destructive operation, publication action, missing
evidence, or unavailable external dependency.

Activity, task completion, process death, or a model statement cannot close an
attention item. Only a typed outcome from the owning subsystem can resolve,
decline, supersede, or invalidate it.

### 3. First-class investigation work

Add a closed task execution kind for read-only investigation. An investigation
may write only to controller-managed artifact storage and must produce an
immutable `InvestigationArtifact` with findings, evidence references,
limitations, rejected hypotheses, recommended follow-up, and a complete
structured decision inventory.

An investigation cannot create a candidate commit, enter integration, push,
publish, or claim product completion.

### 4. Stateful liveness episodes

Add `LivenessEpisode` as a controller-derived sequence of observations and
interventions for one exact attempt identity. It distinguishes:

- healthy progress;
- quiet but provably active work;
- an exact external wait;
- degraded progress;
- suspected stall;
- confirmed stall;
- ownership uncertainty;
- recovery required;
- terminal completion.

The controller computes observations. The supervisor may interpret them, but
cannot rewrite them. An episode does not clear because an agent emits prose or
tool chatter.

### 5. Ownership-safe reconciliation

Add `ReconciliationEpisode` for daemon restart, App Server loss, process loss,
version transition, account handoff, worktree mismatch, and uncertain command
completion.

Reconciliation inventories the exact run, task, attempt, session, worktree,
HEAD, mutable-worktree fingerprint, lease, command, approval, artifact, and
candidate lineage before it authorizes resume or replacement.

A fresh attempt is forbidden while an earlier mutable owner is live or
ownership is unknown. Existing work is preserved and surfaced for recovery
rather than overwritten or silently discarded.

### 6. External condition registry

Add typed external conditions for waits such as CI checks, review results,
credentials, time gates, hardware capacity, or external service availability.

A condition observation is input, not authority. Satisfaction emits a material
controller event; any consequential action still passes the existing policy,
freshness, approval, and custody checks. Polling or long waits never require a
model turn.

### 7. Operator presence and notification policy

Add explicit `interactive`, `focus`, and `unattended` presence modes.

Presence changes delivery timing and presentation only. It never expands
model authority, action allowlists, budgets, retry counts, approval policy,
publication rights, or external-write permission.

Critical attention is immediate. Routine progress can be coalesced into a
bounded digest. Every deferred item has a maximum defer boundary and durable
delivery state. Returning to interactive mode creates a deterministic
return-to-work view from current state and chronological material events.

### 8. Explainable topology and correlation

Add a deterministic topology projection that relates goals, tasks, attempts,
agents, dependencies, worktrees, artifacts, validations, findings, attention
items, and integration state.

The topology is an explanation surface, not an animated representation of
activity. Every node and edge carries an authoritative source reference.

Propagate a stable trace identity across controller events, App Server turns,
commands, artifacts, validations, supervisor decisions, expert requests,
attention items, and recovery outcomes.

### 9. Existing knowledge governance integration

Do not add conversational global memory. Investigation and run-close artifacts
may propose candidates for the existing governed
`harness.knowledge-item.v1` pipeline. Knowledge remains evidence-bound,
reviewed, scoped, freshness-limited, and reversible under the existing
self-improvement architecture.

## Authority and safety invariants

The following rules are non-negotiable:

1. `harnessd` remains the sole mutation authority for controller state and Git.
2. Projections never become writable sources of truth.
3. Operator presence never changes execution authority.
4. An attention item cannot grant the authority it represents.
5. A model cannot close attention, assert ownership, or certify recovery.
6. A process identifier is presence evidence only; it is not task identity.
7. No replacement mutable attempt starts until exclusive ownership is proven.
8. Recovery preserves unknown or potentially useful work and fails closed on
   ambiguous external effects.
9. Investigation output is immutable evidence and never self-authorizes
   implementation.
10. External observations are untrusted input and never executable
    instructions.
11. Supervisor and expert roles remain read-only.
12. Completion, integration, publication, push, readiness, and merge authority
    remain unchanged.
13. Unknown enum values, stale revisions, digest mismatches, and ambiguous
    ownership fail closed.
14. No feature is activated automatically because it is implemented. Product
    evidence and rollout gates remain mandatory.

## Feature decisions

### Adopt in the first implementation program

- canonical control-plane projection;
- durable attention ledger;
- investigation task and artifact;
- liveness and intervention episodes;
- reconciliation and ownership protection;
- external condition registry;
- return-to-work view;
- presence-aware notification batching;
- correlated topology and trace views.

These capabilities directly address lost decisions, work loss, duplicate
ownership, poor resumption, false stall handling, and notification overload.

### Integrate, do not duplicate

- use the existing approval broker as the authority behind approval attention;
- use the existing evidence and artifact stores for investigation evidence;
- use ADR-0011 supervision rather than adding another coordinator;
- use existing account and usage telemetry rather than a second quota system;
- use the governed knowledge-item pipeline rather than free-form memory;
- use exact-SHA custody and existing worktree fingerprints for recovery.

### Prototype behind evidence gates

- graphical topology beyond the accessible table/list view;
- adaptive notification thresholds;
- automatic low-risk liveness interventions;
- resource-aware model routing within an approved capability class;
- scheduled recurring runs.

These need usability, false-positive, or cost evidence before activation.

### Defer to a separate RFC

Remote execution nodes are potentially valuable for macOS, GPU, hardware,
large-build, and isolated validation workloads. They are not part of the first
implementation. A future RFC must define content-addressed inputs, signed task
leases, node identity and capabilities, sandbox policy, authenticated transport,
unknown-completion reconciliation, output manifests, provenance, and a
controller-only publication boundary.

### Reject

Do not add:

- social-media or public-message execution;
- terminal-multiplexer injection as a control-plane primitive;
- broad multi-harness compatibility at the expense of the pinned Codex App
  Server contract;
- a metric that rewards worker count or concurrency by itself;
- continuous LLM polling;
- a single opaque progress, health, or efficiency score;
- automatic model downgrade based only on remaining quota;
- free-form global memory derived from conversation;
- automatic push, publication, readiness change, or merge;
- a replacement worker while previous ownership is unknown;
- product terminology copied from the comparison repository.

## Why this is one architecture

These capabilities share the same source-of-truth and failure boundary.

An unresolved decision affects the control-plane snapshot, notification
priority, supervisor snapshot, topology, return-to-work view, and recovery
behavior. A liveness intervention depends on exact ownership and can create
attention. An investigation can create findings and attention but not code. A
restart can invalidate an approval and must update attention and the return
view. Implementing these as unrelated UI features would create conflicting
truth and unsafe transitions.

The shared architecture is:

```text
immutable events and authoritative tables
  -> deterministic projections and episode reducers
  -> policy and custody checks
  -> controller commands
  -> durable outcomes
  -> browser, CLI, supervisor, and return-to-work views
```

## Consequences

### Positive

- decisions cannot disappear behind later activity;
- the operator can understand current state without reconstructing chat;
- crashes and restarts preserve useful work and prevent duplicate writers;
- stall handling becomes evidence-based and auditable;
- investigations become reusable, reviewable artifacts;
- notifications can be quieter without hiding critical work;
- UI, CLI, supervision, and recovery share one projection contract;
- future remote execution has a clear safety prerequisite;
- product value can be measured feature by feature.

### Costs

- new durable records, reducers, migrations, APIs, UI modules, fixtures, and
  fault-injection tests;
- explicit lifecycle ownership for attention and external conditions;
- calibration work for liveness and notification thresholds;
- a reconciliation pass on restart and selected lifecycle transitions;
- usability studies for return and topology views;
- migration away from adding more logic to existing large source files.

### Risks

- a bad attention classifier can create alert fatigue;
- a bad liveness classifier can interrupt healthy work;
- a stale projection can mislead the operator;
- a complex graph can look impressive while slowing comprehension;
- recovery code can duplicate an external effect if it treats unknown
  completion as failure;
- broad memory capture can preserve stale or sensitive content.

The evaluation and rollout plan treats each risk as a countermetric and blocks
activation when it is not controlled.

## Rejected alternatives

### Add more autonomous workers

Worker count does not solve lost decisions, ownership ambiguity, restart
recovery, or operator comprehension. It adds coordination and cost. BILDR
should create additional workers only for independent work justified by the
plan and measured benefit.

### Store everything in conversation history

Conversation is not a durable, typed, queryable, or authority-safe source of
truth. Context compaction, restart, and model replacement make it unsuitable
for decisions, ownership, or evidence.

### Use one generic notification stream

A flat event stream forces the operator either to monitor noise continuously or
risk missing critical work. Classification and durable attention are required.

### Treat process liveness as task liveness

A live process can be wedged, and a dead process can leave valid work. Process
state is one observation, not the lifecycle authority.

### Restart failed work from a clean checkout

This can discard uncommitted changes, duplicate an external effect, or split
ownership. Recovery must inspect and preserve the existing attempt before
authorizing replacement.

### Let the supervisor own the operator queue

The supervisor is probabilistic and read-only. The attention ledger is a
controller projection from typed subsystem events. The supervisor can recommend
a legal next action, not create authority or erase obligations.

### Copy the comparison repository as an execution layer

Its operational techniques provide useful failure cases, but its shell session,
multi-runtime, and terminology choices do not match BILDR's Rust controller,
Codex App Server protocol, evidence custody, and browser product. BILDR adopts
the validated user problems, not the implementation or vocabulary.
