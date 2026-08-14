# Operator control plane contracts

## Scope

This document defines the versioned contracts required to implement ADR-0012.
It describes data identity, lifecycle, ownership, persistence, API/CLI behavior,
replay, and verification. It does not grant runtime authority or activate any
adaptive behavior.

## Contract hierarchy

### Authoritative domain contracts

Stored controller records and immutable event envelopes are authoritative for:

- attention lifecycle;
- investigation artifacts;
- material progress;
- liveness episodes;
- reconciliation and mutable ownership proof;
- external conditions;
- presence and notification delivery;
- control-plane snapshots;
- return views, topology, and correlation.

### Model-visible contracts

ADR-0011 supervisor payloads receive bounded references and summaries. They do
not receive mutation methods or become current-state records.

### Presentation contracts

Browser and CLI DTOs are versioned projections. Client code must not classify
current state, close attention, infer ownership, or synthesize progress.

## Common envelope

Every immutable operator-control artifact or event uses a common envelope:

```json
{
  "schema": "harness.<name>.v1",
  "record_id": "opaque-id",
  "repository_id": "opaque-id-or-null",
  "run_id": "opaque-id-or-null",
  "task_id": "opaque-id-or-null",
  "attempt_id": "opaque-id-or-null",
  "source_event_id": "opaque-id",
  "source_revision": 7,
  "created_at_ms": 1786588800000,
  "trace_id": "32-hex",
  "causal_refs": [],
  "sensitivity": "internal",
  "sha256": "64-hex"
}
```

Rules:

- identifiers are opaque, bounded, and path-unsafe by default;
- timestamps are UTC epoch milliseconds;
- revisions are monotonic within the record identity;
- digest is computed over canonical serialized content excluding the digest;
- source event/revision identify the authoritative observation;
- causal references are typed and bounded;
- hidden reasoning and secret values are forbidden.

## Identifier types

Add distinct newtypes:

```text
AttentionItemId
InvestigationArtifactId
MaterialProgressEventId
LivenessEpisodeId
LivenessObservationId
InterventionId
ReconciliationEpisodeId
OwnershipProofId
ExternalConditionId
ConditionObservationId
ControlPlaneSnapshotId
ReturnViewId
NotificationDeliveryId
TopologySnapshotId
```

Do not reuse generic strings where identity affects authorization or dedupe.

## Task execution kind

Add a closed `TaskExecutionKind`:

```text
implementation
investigation
verification
review
integration
```

Compatibility:

- legacy stored task packets without the field deserialize as
  `implementation`;
- schema writers emit the explicit field after migration;
- unknown values fail closed;
- `investigation` requires an enforceable per-path read sandbox, no mutable
  path lease, and an investigation-artifact completion contract. A generic
  global read-only sandbox and prompt-only scope are insufficient; absent a
  readable-root allowlist plus controller-visible read-event custody, dispatch
  fails closed;
- only implementation tasks may produce a mutable candidate;
- verification/review/integration retain existing specialized authority.

## Attention item contract

### Shape

```json
{
  "schema": "harness.attention-item.v1",
  "attention_id": "attn-...",
  "repository_id": "repo-...",
  "run_id": "run-...",
  "task_id": "task-...",
  "source": {
    "type": "approval",
    "id": "approval-...",
    "revision": 3
  },
  "category": "approval",
  "severity": "high",
  "state": "open",
  "title": "Approve command on the current task state",
  "summary": "A bounded command requires operator approval.",
  "option_refs": [],
  "evidence_refs": [],
  "blocked_refs": ["task:task-..."],
  "dedupe_key": "sha256:...",
  "opened_event_id": "event-...",
  "opened_at_ms": 1786588800000,
  "acknowledged_at_ms": null,
  "due_at_ms": null,
  "resurfacing": {
    "policy": "until_resolved",
    "maximum_defer_ms": 900000
  },
  "resolution": null,
  "version": 1
}
```

### Source reference

Closed source types:

```text
approval
decision
credential_requirement
publication
policy_decision
evidence_gap
external_condition
reconciliation
infrastructure
```

The source adapter owns mapping and closure rules. Unknown source types cannot
open a generic mutable item.

### Categories and severity

Categories:

```text
decision
approval
credential
policy_exception
destructive_action
publication
missing_evidence
external_dependency
recovery_conflict
infrastructure
```

Severity:

```text
info
normal
high
critical
```

Severity is deterministic from source type, risk, blocked critical path, and
policy. Models and clients cannot set it.

### States

```text
open
acknowledged
waiting_external
resolved
declined
superseded
invalidated
```

Legal transitions:

```text
open -> acknowledged | waiting_external | resolved | declined | superseded | invalidated
acknowledged -> waiting_external | resolved | declined | superseded | invalidated
waiting_external -> open | resolved | declined | superseded | invalidated
terminal states -> terminal state only (idempotent same receipt)
```

Acknowledgement changes presentation state only.

### Decision options

A decision source supplies a versioned option set:

```json
{
  "option_set_id": "options-...",
  "revision": 2,
  "prompt": "Choose the compatible public contract behavior.",
  "options": [
    {"id": "preserve", "label": "Preserve current contract", "impact": "..."},
    {"id": "break", "label": "Accept breaking change", "impact": "..."}
  ],
  "allow_freeform": false,
  "expires_at_ms": 1786675200000
}
```

Answers bind to option-set revision and expected source revision.

### Resolution receipt

```json
{
  "source_type": "approval",
  "source_id": "approval-...",
  "source_revision": 4,
  "outcome": "approved",
  "actor_type": "operator",
  "actor_id": "local-session-...",
  "resolved_at_ms": 1786588900000,
  "authority_event_id": "event-...",
  "bound_head_sha": "40-hex-or-null",
  "worktree_fingerprint": "64-hex-or-null",
  "sha256": "64-hex"
}
```

A browser acknowledgement, task terminal state, model statement, or free-form
`resolved` string is not a valid receipt.

## Investigation artifact contract

```json
{
  "schema": "harness.investigation-artifact.v1",
  "investigation_id": "investigation-...",
  "run_id": "run-...",
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "question": "Why does the current verifier disagree with the worker?",
  "scope": {
    "owned_read_paths": ["crates/..."],
    "forbidden_paths": [".git/objects"],
    "time_budget_ms": 1800000,
    "token_budget": 40000
  },
  "base_sha": "40-hex",
  "repository_state_digest": "64-hex",
  "methods": [],
  "sources": [],
  "findings": [],
  "rejected_hypotheses": [],
  "recommendations": [],
  "unresolved_decisions": [],
  "limitations": [],
  "sensitivity": "internal",
  "artifact_refs": [],
  "created_at_ms": 1786588800000,
  "sha256": "64-hex"
}
```

### Finding

```json
{
  "finding_id": "finding-...",
  "statement": "The two components validate different schema revisions.",
  "classification": "confirmed",
  "confidence_milli": 930,
  "evidence_refs": ["artifact:...#L10-L40"],
  "affected_refs": ["task:..."],
  "risk": "high",
  "limitations": []
}
```

Classifications: confirmed, supported, hypothesis, disproven, inconclusive.
Confidence is descriptive, not authority.

### Recommendation

A recommendation includes proposed outcome, evidence, alternatives, risk,
required authority, and next verification. It cannot directly create a task or
execute a change.

### Unresolved decision inventory

Every discovered decision includes stable key, question, options, evidence,
impact, recommended option if any, required actor, blocking scope, and whether
independent work can continue. Accepted artifacts must explicitly provide an
empty list when no decision exists.

### Limits

Initial limits:

```text
structured payload <= 2 MiB
findings <= 200
recommendations <= 100
unresolved decisions <= 100
source refs <= 1,000
inline excerpt <= 8 KiB each
large output only by content-addressed artifact reference
```

## Material progress contract

```json
{
  "schema": "harness.material-progress.v1",
  "progress_id": "progress-...",
  "run_id": "run-...",
  "task_id": "task-...",
  "attempt_id": "attempt-...",
  "kind": "validation_advanced",
  "classifier_version": "material-progress-v1",
  "occurred_at_ms": 1786588800000,
  "summary": "Required unit suite changed from failing to passing on exact candidate.",
  "evidence_refs": ["validation:..."],
  "candidate_sha": "40-hex-or-null",
  "milestone_refs": [],
  "dedupe_key": "sha256:...",
  "sha256": "64-hex"
}
```

Closed kinds:

```text
candidate_materialized
candidate_materially_changed
validation_advanced
blocking_finding_resolved
blocking_finding_discovered
milestone_evidence_satisfied
investigation_artifact_accepted
integration_frontier_advanced
external_dependency_satisfied
```

The reducer must not emit an event for heartbeats, token deltas, ordinary output,
repeated reads, or unchanged command retries.

## Liveness contracts

### Observation

```json
{
  "schema": "harness.liveness-observation.v1",
  "observation_id": "observation-...",
  "episode_id": "liveness-...",
  "attempt_id": "attempt-...",
  "observed_at_ms": 1786588800000,
  "kind": "no_material_progress_boundary",
  "value": {"elapsed_ms": 900000},
  "source_refs": [],
  "classifier_version": "liveness-v1",
  "sha256": "64-hex"
}
```

Observation kinds include material progress, process/session state, command
state, worktree change, external wait, validator trend, repeated semantic action,
tool failure, budget boundary, operator steering, and reconciliation finding.

### Episode

The episode records exact attempt/worktree/head identity, opened/updated time,
state, state reason codes, last material progress, active external condition,
observation refs, intervention refs, repeated signature counts, and resolution.

Legal progression allows recovery to healthy after new material evidence; it
never clears solely on model prose. Ownership unknown is a high-priority state
that blocks replacement.

### Confirmed stall evidence

Confirmed stall requires policy-defined combinations such as:

- no material progress beyond a role/task-class boundary;
- no active exact external wait;
- process/session not demonstrably performing a bounded command;
- repeated unchanged semantic action or repeated typed failure;
- fresh worktree/candidate/validation inspection;
- no ownership conflict.

A single timeout is insufficient.

### Intervention receipt

```json
{
  "schema": "harness.intervention-receipt.v1",
  "intervention_id": "intervention-...",
  "episode_id": "liveness-...",
  "action": "request_targeted_inspection",
  "target_revision": 12,
  "precondition_digest": "64-hex",
  "policy_version": "operator-control-v1",
  "requested_by": "deterministic_policy",
  "executed_at_ms": 1786588800000,
  "result": "accepted",
  "effect_refs": [],
  "sha256": "64-hex"
}
```

Closed actions:

```text
wait
request_targeted_inspection
steer_active_turn
start_followup_turn
spawn_read_only_investigation
request_verification
request_reconciliation
pause_for_operator
stop_run
```

`retry_fresh_attempt` is owned by reconciliation and requires exclusive
ownership proof.

## Mutable ownership contract

### Exclusive ownership proof

```json
{
  "schema": "harness.exclusive-ownership-proof.v1",
  "proof_id": "ownership-...",
  "run_id": "run-...",
  "task_id": "task-...",
  "prior_attempt_id": "attempt-...",
  "worktree_id": "worktree-...",
  "head_sha": "40-hex",
  "worktree_fingerprint": "64-hex",
  "lease_generation": 9,
  "process_state": "proven_absent",
  "session_state": "proven_closed",
  "command_state": "terminal_or_none",
  "external_effect_state": "none_or_reconciled",
  "candidate_state": "preserved",
  "approved_actions": ["authorize_fresh_attempt"],
  "expires_at_ms": 1786589100000,
  "sha256": "64-hex"
}
```

Any unknown field invalidates the proof for replacement. Proof is short-lived
and consumed transactionally.

## Reconciliation contracts

### Episode and finding

A reconciliation episode has trigger, state, cursor, claimed-by/generation,
started/completed timestamps, exact inventory digest, findings, action intents,
action receipts, preserved refs, unresolved conflicts, and report digest.

Finding kinds:

```text
process_missing
process_identity_unknown
session_missing
session_incompatible
lease_expired
lease_owner_live
worktree_clean
worktree_changed
worktree_fingerprint_mismatch
candidate_present
approval_stale
command_terminal
command_unknown
external_effect_unknown
artifact_unregistered
version_incompatible
```

### Action kinds

```text
no_action
preserve_and_pause
attach_to_live_owner
resume_compatible_session
resume_from_durable_context
invalidate_stale_approval
register_existing_artifact
requeue_verification
release_proven_dead_lease
authorize_fresh_attempt
open_attention
```

Every action has exact preconditions and idempotency key. Worktree deletion,
reset, forced checkout, ambiguous command retry, and automatic local replacement
of unknown work are forbidden.

### Report

The report states what was observed, preserved, resumed, invalidated, refused,
and still requires attention. It distinguishes preserved state from recovered
execution.

## External condition contracts

### Specification

```json
{
  "schema": "harness.external-condition.v1",
  "condition_id": "condition-...",
  "owner_type": "task",
  "owner_id": "task-...",
  "adapter": "github_check",
  "spec": {},
  "state": "registered",
  "sequence": 0,
  "poll_policy": {"initial_ms": 15000, "maximum_ms": 300000, "deadline_ms": 1786675200000},
  "source_identity_digest": "64-hex",
  "last_observation": null,
  "terminal_result": null,
  "version": 1
}
```

States: registered, observing, satisfied, failed, expired, canceled,
continuity_broken, unknown_completion.

### Result and replay

Each observation is stored before its event is published and is keyed by exact
condition/sequence. Handled acknowledgement stops re-announcement; it does not
claim exactly-once external effect. Continuity mismatch stops the adapter and
requires explicit re-registration.

## Presence and notification contracts

Presence:

```json
{"mode":"interactive","changed_at_ms":1786588800000,"actor":"operator","version":4}
```

Notification delivery includes item/source refs, classification, channel,
created/eligible/defer-deadline times, state, attempt count, dedupe key,
redacted payload digest, receipts, and last error.

Delivery states: pending, deferred, claimed, delivered, failed_retryable,
failed_terminal, canceled, superseded.

Critical categories have zero configurable defer window. Delivery never resolves
the source item.

## Control-plane snapshot contract

```json
{
  "schema": "harness.control-plane-snapshot.v1",
  "snapshot_id": "snapshot-...",
  "revision": 42,
  "compiled_at_ms": 1786588800000,
  "event_cursor": 102938,
  "consistency": "transactional_projection",
  "system": {},
  "accounts": [],
  "scheduler": {},
  "runs": [],
  "attention": [],
  "attempts": [],
  "progress": [],
  "liveness": [],
  "reconciliation": [],
  "external_conditions": [],
  "cost": {},
  "notifications": {},
  "limits": {},
  "truncation": [],
  "source_cursors": {},
  "sha256": "64-hex"
}
```

Classification and ordering are server-owned. Limits are included in the
digest. Unknown or stale sections are marked explicitly; clients must not treat
missing data as empty healthy data.

## Return-view contract

The return view binds to a snapshot revision and the operator's last acknowledged
view cursor. Sections:

```text
needs_action
material_changes
current_work
waiting_and_blocked
recovery
cost_and_capacity
next_legal_actions
limitations_and_truncation
```

Every row links to source evidence. Acknowledging the view advances only the
presentation cursor.

## Run topology contract

The topology contains bounded typed nodes and edges. Node kinds include goal,
task, attempt, agent, worktree, commit, artifact, validation, finding, attention,
external condition, integration. Edge kinds include depends_on, owns, produced,
validates, found, blocks, resolves, derived_from, integrated_into, caused_by.

Every node/edge has source refs. The table/list is normative; graph coordinates
are presentation-only.

## Correlation contract

Use 16-byte trace IDs and 8-byte span IDs compatible with W3C Trace Context.
Domain IDs remain separate attributes. Propagation is allowlisted across child
process environment and App Server metadata; external inputs cannot choose an
existing trusted trace without validation.

## API contract

### Read routes

```text
GET /api/v1/control-plane/snapshot
GET /api/v1/control-plane/return-view
GET /api/v1/attention
GET /api/v1/attention/{id}
GET /api/v1/runs/{run_id}/topology
GET /api/v1/runs/{run_id}/liveness
GET /api/v1/reconciliations/{id}
GET /api/v1/reconciliations/{id}/findings
GET /api/v1/reconciliations/{id}/actions
GET /api/v1/investigations/{id}
GET /api/v1/external-conditions
GET /api/v1/traces/{trace_id}
```

All list routes use stable cursor pagination and explicit limits.

### Mutation routes

```text
POST /api/v1/attention/{id}/acknowledge
POST /api/v1/decisions/{source_id}/answer
POST /api/v1/approvals/{source_id}/decide
POST /api/v1/reconciliation/{id}/apply
POST /api/v1/external-conditions/{id}/cancel
PUT  /api/v1/operator-presence
POST /api/v1/runs/{run_id}/investigations
```

There is no generic attention-resolve or recovery-force route. Mutations require
expected version/revision, local session, CSRF, same-origin, controller policy,
and source-specific state checks.

### SSE events

```text
control_plane.snapshot.updated
attention.opened
attention.updated
attention.closed
investigation.accepted
material_progress.recorded
liveness.updated
reconciliation.updated
external_condition.updated
notification.delivery.updated
```

SSE data is replayable from the existing event cursor and does not carry secret
values or unbounded artifacts.

## CLI contract

```text
harnessctl status [--json]
harnessctl return [--since <cursor>] [--json]
harnessctl attention list|show|acknowledge
harnessctl decision answer
harnessctl recovery list|show|findings|actions
harnessctl investigation create|show|export
harnessctl condition list|show|cancel
harnessctl presence get|set
harnessctl topology show [--format table|json|dot]
harnessctl trace show
```

CLI uses authenticated API only. JSON mode returns versioned DTOs. Human mode
uses stable headings and explicit unknown/stale/truncated labels.

## Schema evolution

- V1 enums are closed.
- Additive optional presentation fields are allowed only with safe defaults.
- New authoritative states or actions require a new reviewed build and contract
  tests.
- Stored legacy task packets receive explicit compatibility fixtures.
- Snapshot compilers support current and immediately prior schema during rolling
  upgrade; incompatible active attempts are preserved and paused.
- Digest canonicalization is versioned.

## Retention and export

Attention and reconciliation receipts follow operational/audit retention.
Investigation artifacts inherit repository sensitivity. High-volume observations
may be compacted only after immutable episode summaries and source evidence
remain available. Exports are bounded, redacted, digest-bound, and record schema,
policy, and source cursors.

## Contract verification

Required property suites prove:

- only legal attention and episode transitions;
- source-only attention closure;
- acknowledgement never resolves;
- task completion cannot erase open attention;
- one mutable owner per task/worktree;
- unknown ownership cannot authorize replacement;
- replay produces the same projections and digests;
- duplicate source events/condition results/interventions are idempotent;
- stale versions and digests fail closed;
- snapshot truncation is deterministic;
- clients cannot create authority through presentation DTOs;
- ambiguous external effects are never automatically retried;
- restart/replay preserves open attention, work, and cursors;
- notification delivery cannot close attention;
- investigation tasks cannot create candidates or mutable leases.
