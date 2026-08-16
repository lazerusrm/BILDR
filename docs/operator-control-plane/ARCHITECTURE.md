# Operator control plane architecture

## Purpose

The operator control plane makes long-running BILDR work safe to leave,
straightforward to resume, and explainable after failure or intervention.

It does not replace run, task, approval, Git, validation, evidence, usage, or
supervisory records. It adds the missing deterministic state machines and
bounded projections for attention, investigation, material progress, liveness,
reconciliation, external waits, notification delivery, and causal correlation.

```text
authoritative controller state and immutable events
  -> deterministic reducers and reconciliation
  -> bounded revisioned projections
  -> guarded controller commands
  -> durable outcomes and receipts
  -> browser, CLI, notifications, and read-only supervisor inputs
```

No presentation surface parses model prose, terminal output, or an arbitrary
event tail to infer current state.

## Architectural requirements

1. **One authority per fact.** Existing subsystem records remain authoritative.
   Operator-control records reference them rather than duplicating approvals,
   credentials, Git state, evidence, or publication authority.
2. **One mutable owner.** An exact task/worktree may have at most one
   controller-recognized mutable attempt owner. Process death is not ownership
   transfer.
3. **Explicit uncertainty.** Unknown process, transport, command, worktree, and
   external-effect outcomes stay unknown until reconciled.
4. **Typed closure.** Only a typed event from the owning subsystem closes an
   attention item, external wait, liveness episode, or recovery conflict.
5. **Material progress over activity.** Heartbeats, token deltas, output chunks,
   and tool calls are telemetry, not proof of progress.
6. **Immutable artifacts.** Investigation results, snapshots, recovery reports,
   intervention receipts, and exports are digest-bound.
7. **Bounded projections.** Every system-wide view has deterministic ordering,
   byte/row/event/time limits, truncation metadata, and an event cursor.
8. **Presence is authority-neutral.** Interactive or unattended delivery never
   changes approvals, budgets, publication rights, retry limits, or policy.
9. **Read-only judgment.** ADR-0011 supervision interprets controller facts and
   proposes closed actions; it never becomes the source of current state.
10. **Reversible activation.** Each adaptive capability can remain disabled,
    observe-only, shadow, or advisory without corrupting current runs.

## Component map

Implement the first version in focused modules. Do not expand the existing
large `lib.rs`, `queries.rs`, or `App.tsx` files with feature logic.

```text
crates/harness-domain/src/operator_control.rs
  IDs, closed enums, DTOs, transition validation, event payloads

crates/harness-store/src/operator_control/
  attention.rs
  investigations.rs
  liveness.rs
  reconciliation.rs
  external_conditions.rs
  snapshots.rs
  notifications.rs

crates/harness-orchestrator/src/operator_control/
  progress.rs
  liveness.rs
  reconcile.rs
  interventions.rs
  external_conditions.rs
  snapshot.rs
  notifications.rs

crates/harness-evidence/src/investigation.rs
  investigation artifact validation and evidence registration

crates/harness-trace/src/correlation.rs
  trace and causal-link construction

crates/harness-api/src/operator_control/
  versioned DTOs, reads, guarded mutations, SSE mapping

bins/harnessctl/src/operator_control.rs
  status, attention, return, recovery, investigation, condition commands

ui/src/operator-control/
  feature API/types, attention, return, run status, recovery, investigation,
  topology, tests, styles
```

## Canonical records

### Attention item

`AttentionItem` is a durable operator-facing projection for a required action or
choice that cannot safely disappear into task status.

It contains:

```text
attention_id
repository_id? / run_id? / task_id?
source_type and source_id
source_revision
dedupe_key
category and severity
state
title and redacted summary
option references
evidence references
blocked entity references
opened/acknowledged/due timestamps
resurfacing policy
resolution receipt reference
version
```

Categories are closed: decision, approval, credential, policy exception,
destructive action, publication, missing evidence, external dependency,
recovery conflict, infrastructure.

Acknowledgement means the operator saw the item; it is not resolution. Task
progress or completion does not close it. The source owner supplies the only
valid resolution event.

### Investigation artifact

Investigation is a first-class read-only task kind. It produces an immutable
`InvestigationArtifact` instead of a candidate commit.

Required content:

```text
question and scope
exact base SHA and repository-state digest
methods and tools used
sources and evidence references
findings with confidence and limitations
rejected hypotheses
recommended follow-up
unresolved decision inventory
sensitivity and export policy
artifact digest
```

An investigation has no mutable path lease, cannot enter integration, cannot
publish, and cannot create implementation work directly. A controller or
supervisor may later propose a separate implementation task using artifact
references.

### Material progress event

`MaterialProgressEvent` records a controller-verifiable change in task value.
Initial event kinds include:

```text
candidate materialized
candidate changed materially
required validation advanced
blocking finding resolved
new blocking finding discovered
milestone evidence satisfied
investigation artifact accepted
integration frontier advanced
external dependency satisfied
```

Token use, output volume, repeated reads, and ordinary commands do not create a
material event. Each event names its evidence and classifier version.

### Liveness episode

A `LivenessEpisode` is a stateful history for one exact attempt identity, not a
point-in-time timeout label.

States:

```text
observing
healthy
quiet_but_active
externally_waiting
degraded
suspected_stall
confirmed_stall
ownership_unknown
recovery_required
resolved
```

The episode stores observations, material-progress timestamps, repeated action
signatures, command/tool failure trends, validator trend, external-wait status,
intervention attempts, and resulting outcomes.

Only deterministic observations update the episode. A model may recommend a
legal intervention but cannot rewrite measurements or declare recovery.

### Reconciliation episode

`ReconciliationEpisode` records an ownership and recovery pass triggered by:

```text
daemon startup
App Server/session loss
worker process loss
account or model handoff boundary
lease expiry
worktree fingerprint mismatch
unknown command completion
runtime/schema version transition
operator-requested recovery
```

It inventories exact run/task/attempt/session/worktree IDs, HEAD and mutable
fingerprint, leases, command groups, approvals, artifacts, candidate lineage,
and external effects. It emits typed findings and a bounded set of safe actions.

Safe actions include preserve-and-pause, attach to a proven live owner, resume a
compatible session, resume from durable context, invalidate stale approval,
requeue verification, and release a proven-dead lease. Fresh-attempt
authorization remains unavailable until an authoritative runtime issuer records
exclusive ownership proof.

It never deletes or resets a worktree as part of automatic reconciliation.

### External condition

`ExternalCondition` represents an exact wait that should not consume model
turns. Initial adapters include CI/check status, review result, credential
availability, time gate, hardware/resource availability, and bounded external
service health.

The condition has a typed specification, owner, cadence/backoff, deadline,
sequence, source identity, last observation, and terminal result. Result bytes
are untrusted input. Satisfaction emits a material event; consequential actions
still pass normal policy and approval.

The first release is wake-only. It does not expose a generic condition-to-action
command surface.

### Presence and notification state

`OperatorPresence` is one of:

```text
interactive
focus
unattended
```

It changes delivery timing only. `NotificationDelivery` records classification,
dedupe key, defer deadline, attempts, receipt, and degradation.

Critical attention, security/custody failures, destructive-operation requests,
and recovery conflicts are never suppressed. Routine progress may be batched.

### Control-plane snapshot

`ControlPlaneSnapshot` is a bounded revisioned read model compiled at an exact
event cursor. Stable sections include:

```text
system health and runtime compatibility
account capacity and scheduler state
active/recent runs
open attention
critical-path and queue summary
active attempts and ownership state
material progress
liveness and recovery summaries
external waits
latest meaningful changes
cost and token summary
notification delivery health
truncation and source cursor metadata
```

The snapshot is persisted with a digest for audit and return-to-work views. It
is not used to authorize mutations; handlers reread authoritative records in a
transaction.

### Run topology projection

The topology relates goals, tasks, dependencies, attempts, agents, worktrees,
commits, artifacts, validations, findings, attention, and integration state.

The canonical output is an accessible table/list plus an edge list. An optional
graph is a presentation of the same projection and remains gated on usability
evidence.

### Correlation graph

Propagate W3C-compatible trace context plus BILDR domain identity across:

```text
run -> task -> attempt -> model turn -> command/tool
    -> artifact/candidate -> validation/finding
    -> supervisor/expert decision -> intervention
    -> recovery and notification delivery
```

Causal links support fan-out/fan-in where a strict parent-child span would be
misleading. Trace exports are bounded and redacted.

## Event flows

### Attention

```text
typed source event
  -> source adapter validates identity/revision
  -> attention open/update transaction
  -> snapshot revision and SSE event
  -> delivery classification
  -> operator action through source-specific command
  -> authoritative source outcome
  -> attention reducer closes with receipt
```

A generic `resolve attention` command is forbidden.

### Investigation

```text
approved read-only task packet
  -> bounded read-only context and tools
  -> structured artifact candidate
  -> schema, source, sensitivity, and digest validation
  -> immutable evidence registration
  -> task terminal state with artifact reference
  -> optional separate follow-up proposal
```

One bounded schema-repair attempt may occur if policy permits. Free-form output
is never silently converted into accepted evidence.

### Liveness

```text
telemetry/material events/external waits
  -> deterministic observations
  -> per-attempt episode reducer
  -> observe/shadow classification
  -> optional supervisor interpretation
  -> policy-valid intervention proposal
  -> exact-head/ownership/freshness gate
  -> intervention receipt and later outcome event
```

The intervention ladder starts with no action, targeted inspection, steering,
follow-up turn, reviewer/explorer, fresh attempt after proof, human pause, and
run stop. Destructive process or worktree actions are excluded.

### Restart and recovery

```text
startup lock
  -> inventory nonterminal runs and active claims
  -> reconcile processes, sessions, leases, worktrees, commands, approvals,
     artifacts, candidates, and external effects
  -> preserve uncertain state
  -> apply only idempotent proven-safe actions
  -> publish recovery reports and attention
  -> rebuild scheduler frontier
  -> resume normal processing
```

Recovery is bounded and resumable. A failed pass records its cursor and leaves
work paused rather than inventing a healthy state.

### Return-to-work

```text
presence becomes interactive or operator opens Return view
  -> compile snapshot at cursor
  -> compare with last acknowledged return cursor
  -> fold chronological material events and attention changes
  -> render: needs action, material changes, current work, recovery, cost,
     next legal actions, limitations
  -> acknowledge view cursor only after successful delivery
```

## Consistency and concurrency

Use transactions for source-event plus attention update where possible,
worktree/attempt ownership transfer, reconciliation claims and intents,
external-condition sequence capture, intervention dedupe, snapshot revision
allocation, and notification delivery claims.

Every claim records claimant instance, generation, claim/heartbeat/expiry time.
A replacement claim is legal only after the prior generation is proven gone and
its underlying effect has been reconciled.

Current-state snapshots may be boundedly eventually consistent. Authority checks
never use them; they reread source rows and exact Git/worktree identity.

## Failure behavior

- **Store corruption or digest mismatch:** fail closed for affected projection or
  artifact; preserve source records; open infrastructure attention; disable
  automatic intervention using the affected data.
- **Snapshot compiler failure:** keep the prior snapshot visibly stale, expose
  source cursor/error, continue authoritative scheduling, and do not fabricate
  an empty healthy view.
- **Notification failure:** retain delivery state, retry within policy, surface
  degradation in browser/CLI, and never close attention.
- **Investigation schema failure:** preserve bounded raw output under retention
  policy, mark artifact invalid, execute nothing, allow one policy-bounded repair.
- **Liveness uncertainty:** classify ownership unknown or remain observing;
  schedule reconciliation; do not kill, retry, or mark failed.
- **Version incompatibility during recovery:** preserve and pause the attempt,
  report the exact mismatch, require compatible resume or operator decision.
- **External-condition continuity break:** stop the adapter, preserve sequence and
  prior results, open attention, require explicit re-registration/rebase.

## Security and privacy

- Retain localhost listener, Host validation, session, CSRF, same-origin, and
  controller-policy checks for every mutation.
- Store redacted summaries and opaque references in attention/notifications.
- Never persist credential values in attention.
- Inherit repository sensitivity for investigation artifacts and exports.
- Treat all external content as data, never instructions.
- Do not persist hidden reasoning.
- Redact command arguments/environment in trace exports under existing policy.
- Validate IDs, paths, sizes, digests, and artifact ancestry.
- Keep future remote execution disabled until its separate trust boundary is
  reviewed and implemented.

## Initial performance budgets

These are benchmark hypotheses, not permanent constants:

| Surface | Initial target |
|---|---:|
| snapshot compile p95, 100 active/recent runs | 250 ms |
| per-run topology p95, 2,000 nodes/edges | 200 ms |
| attention projection lag | under 1 second |
| liveness reduction per changed attempt | under 50 ms, no model call |
| return-view payload default maximum | 256 KiB |
| investigation structured payload | 2 MiB plus referenced artifacts |
| trace export | streaming and bounded |

Fixtures must include stale records, long titles, many evidence references,
truncation, and mixed active/terminal states.

## Governed knowledge integration

Investigation, recovery, and repeated liveness evidence may propose candidates
to the existing `harness.knowledge-item.v1` system. An authenticated local
human may accept or reject exactly the immutable current candidate, with the
human action bound to its pre-review revision and wire digest. Acceptance
rechecks fresh controller-clean evidence. This produces governed display data,
not task context or execution authority. Existing evidence, review, scope,
freshness, contradiction, supersession, and rollback rules remain authoritative.

## Future remote execution boundary

The first implementation reserves node/correlation fields but runs locally.
A later RFC must add controller-issued immutable leases, content-addressed
inputs, node identity/capabilities, authenticated transport, result manifests,
provenance, unknown-completion handling, central re-verification, quarantine,
and no remote publication/merge authority.

## Architecture review checklist

Before accepting an implementation slice, verify:

- one source owner for each fact;
- closed and tested transitions;
- replay idempotency;
- explicit uncertainty;
- no presentation-to-authority path;
- inspectable evidence and policy;
- ownership survival across restart;
- unresolved attention survives task completion;
- automatic behavior has observe/shadow gates;
- deterministic bounds and truncation;
- redaction at persistence and presentation;
- rollback leaves authoritative work valid;
- focused modules replace monolith growth.
