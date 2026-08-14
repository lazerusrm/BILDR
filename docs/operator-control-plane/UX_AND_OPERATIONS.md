# Operator experience and operations

## Product outcome

The product should let an operator leave a run, return later, understand the
current legal state, resolve required actions, and recover interrupted work
without reconstructing model conversation or terminal history.

Every operator-facing claim must be linked to controller evidence. Unknown,
stale, truncated, blocked, and preserved states must be explicit.

## Information architecture

### Home status

The default home view answers, in order:

1. What requires action now?
2. What changed materially since the last acknowledged view?
3. Which runs are active, waiting, blocked, recovering, or complete?
4. Is mutable ownership healthy and unambiguous?
5. What is the current cost/capacity posture?
6. Is any delivery, projection, runtime, or reconciliation subsystem degraded?

It does not lead with transcripts, agent count, or animated activity.

### Return view

The Return view is a deterministic chronological summary bound to the last
acknowledged return cursor and a current snapshot revision.

Sections:

```text
Needs action
Material changes
Current work
Waiting and blocked
Recovery and preserved work
Cost and capacity
Next legal actions
Limitations and truncation
```

Each row shows time, repository/run/task, human-readable summary, state, and a
source link. Chronology is preserved; grouping may not hide ordering.

### Run control view

A run view contains:

- objective and exact base/integration/candidate identity;
- current phase and critical-path frontier;
- open attention and source authority;
- task/attempt ownership;
- material progress timeline;
- liveness and intervention history;
- external waits;
- investigation artifacts;
- validations/findings/evidence;
- recovery reports;
- cost/token usage by role/account;
- supervisor decisions and action receipts;
- topology/evidence navigation.

## UI module layout

Implement under `ui/src/operator-control/`:

```text
api.ts
types.ts
OperatorStatusPage.tsx
ReturnView.tsx
AttentionCenter.tsx
AttentionDetail.tsx
RunStatus.tsx
RunTopologyTable.tsx
RunTopologyGraph.tsx          # optional/gated
LivenessPanel.tsx
RecoveryPanel.tsx
InvestigationPanel.tsx
ExternalConditionsPanel.tsx
PresenceControl.tsx
NotificationHealth.tsx
SourceEvidenceLink.tsx
operator-control.css
*.test.tsx
```

`ui/src/App.tsx` may only register routes and pass common shell dependencies.
Business classification remains server-side.

## Attention center

### List behavior

Default sort:

1. critical, high, normal, info;
2. blocks critical path;
3. overdue/due time;
4. opened time;
5. stable ID.

Filters:

```text
repository
run
task
category
severity
state
blocks critical path
requires operator
waiting external
```

The list shows source type, source revision, opened age, blocked entities,
latest evidence, and resolution owner.

Do not merge independent source items because titles look similar. Dedupe is
server-owned and identity-based.

### Decision UI

A decision detail shows:

- exact question and option-set revision;
- impact and evidence for each option;
- recommended option, when present, clearly labeled advisory;
- whether independent work can continue;
- affected tasks/contracts;
- required actor/authority;
- expiry/staleness;
- expected result of submission.

Submission binds to expected source and option-set revision. A stale decision
returns a visible conflict and reloads the current set.

### Acknowledgement

Acknowledge is visually and semantically separate from decide/approve/decline.
It advances presentation state only. Accessibility labels must state that the
item remains unresolved.

## Presence control

Modes:

- **Interactive:** immediate normal/high/critical delivery and live UI updates.
- **Focus:** critical/high immediate; routine items batched within a bounded
  window.
- **Unattended:** critical immediate; high delivered according to explicit
  maximum defer; routine progress summarized in durable digest.

The control states exactly what changes and what does not:

```text
Changes: notification timing and digest presentation.
Does not change: approvals, budgets, retries, policy, model route, write access,
publication, or merge authority.
```

Modes are local persisted preferences with actor/time/version. Switching to
interactive opens the Return view from the prior acknowledged cursor.

## Notification policy

### Classification

Deterministic classes:

```text
critical_attention
high_attention
normal_attention
material_progress
terminal_outcome
recovery_update
system_degradation
routine_capacity
```

Critical examples:

- custody/ownership conflict;
- security/integrity failure;
- destructive action approval;
- publication/merge-related required action;
- unknown external effect requiring human judgment;
- critical-path recovery conflict;
- critical delivery system degradation.

Models cannot choose class or defer boundary.

### Batching

Batch only items whose class permits it. Preserve chronological order and source
links. The digest states coverage window, generated time, snapshot revision,
open attention count, omitted/truncated count, and delivery health.

A new critical item bypasses the batch immediately. A routine item that becomes
high/critical is reclassified from authoritative state and delivered under the
new class.

### Delivery channels

Initial channels:

```text
browser/SSE
system desktop notification when explicitly enabled
CLI status/return view
```

Do not add public-message or social integrations. External channels require a
separate privacy/security review.

### Delivery degradation

When delivery fails:

- keep the durable delivery record;
- retry only within bounded policy;
- show degradation in browser and CLI;
- keep source attention open;
- do not mark delivered without a receipt;
- preserve a maximum defer deadline for important items.

## Topology and evidence view

### Purpose

Answer factual questions:

```text
Who owns this task?
What blocks this task or integration?
Which candidate did this validation inspect?
Which finding opened this attention item?
Why did the controller intervene?
What was preserved after restart?
```

### Default presentation

Use an accessible table and detail panels first. Columns include entity, state,
owner, exact identity, dependencies, produced evidence, blockers, and last
material change.

An optional graph uses the same server projection. It cannot infer new edges or
hide unknown relationships. It remains feature-gated until a controlled study
shows improved answer accuracy/time.

### Accessibility

- keyboard navigation for every node/row/action;
- semantic table/list fallback;
- visible focus;
- no color-only state;
- text labels for edge meaning;
- reduced-motion support;
- screen-reader summaries of current counts and selected relationships;
- zoom/pan not required for access to any fact.

## Liveness view

Show observation and episode facts separately from advisory interpretation:

```text
state and reason codes
last material progress
active command/session/process evidence
external wait
repeated action/failure signatures
validation trend
ownership state
prior interventions and outcomes
next review boundary
```

Avoid “stuck” labels without evidence. Use `suspected stall`, `confirmed stall`,
`ownership unknown`, and `recovery required` exactly as defined.

The operator can request inspection, reconciliation, pause, or a legal
intervention through existing controller paths. There is no raw kill/reset
button in the product surface.

## Recovery view

A recovery episode shows:

```text
trigger and scope
inventory identity and digest
what was observed
what was preserved
what resumed
what was invalidated
what action was refused and why
unknown/ambiguous effects
open recovery attention
safe actions and exact preconditions
receipts
```

Never use “recovered” when the system only preserved and paused work.

### Startup summary

After restart, display one bounded summary:

```text
Runs inspected: N
Attempts resumed: N
Attempts preserved and paused: N
Stale approvals invalidated: N
Verifications requeued: N
Ownership conflicts: N
Unknown external effects: N
Automatic actions refused: N
```

Each count links to exact records. The summary persists until acknowledged but
does not block normal operation unless source policy requires it.

## Investigation view

Show:

- question/scope/base SHA/state digest;
- methods/sources;
- findings and classifications;
- evidence/affected refs;
- rejected hypotheses;
- recommendations and required authority;
- unresolved decision inventory;
- limitations/sensitivity/export policy;
- downstream tasks/knowledge candidates that reference the artifact.

Do not display confidence as authority. Recommendations must visibly state that
another controller action is required.

## CLI experience

### `harnessctl status`

Stable sections match the control-plane snapshot. Human output is concise and
uses explicit `UNKNOWN`, `STALE`, and `TRUNCATED` labels. `--json` emits the
versioned DTO unchanged.

### `harnessctl return`

Renders the deterministic return view. `--since <cursor>` supports an explicit
presentation boundary. It exits nonzero only for API/contract failure, not
because attention exists.

### Exit codes

Suggested stable exit classes:

```text
0 success
2 invalid invocation
3 authentication/session failure
4 stale/conflict/expected revision mismatch
5 policy/authority denial
6 not found
7 unavailable/degraded
8 contract/integrity failure
```

Commands never access SQLite directly.

## Operational workflows

### Leave a run unattended

1. Review current critical/high attention.
2. Select unattended presence.
3. Review the exact maximum defer policy and budget/authority statement.
4. Leave normal controller work active.
5. Critical events bypass batching; routine events enter the durable digest.
6. On return, the product opens the Return view from the prior cursor.

Expected result: no authority expansion, no lost critical item, and a complete
bounded summary.

### Resolve a human decision

1. Open the attention item.
2. Inspect source revision, options, evidence, impact, and blocked entities.
3. Submit through the source-specific decision command.
4. The controller validates expected revision and authority.
5. The source emits a typed outcome.
6. The attention reducer closes with the source receipt.

Expected result: a stale or invalid answer fails without closing the item.

### Recover an interrupted worker

1. Open the recovery episode.
2. Inspect ownership, process/session, command, worktree, candidate, approvals,
   and external-effect state.
3. Apply only an offered typed safe action.
4. The controller revalidates all preconditions transactionally.
5. The recovery report records the result.

Expected result: replacement is unavailable while ownership or external effect
is unknown.

### Run a read-only investigation

1. Create an investigation with question, scope, budget, and evidence needs.
2. Controller creates a read-only task without mutable path lease.
3. Agent produces structured artifact candidate.
4. Controller validates schema, sources, sensitivity, limits, and digest.
5. Accepted artifact becomes immutable evidence.
6. Implementation requires a separate task/approval path.

Expected result: useful findings persist without changing code.

### Wait on an external condition

1. Register a typed adapter/specification.
2. Controller persists identity, cadence, deadline, and sequence.
3. Adapter records observations without model turns.
4. Satisfaction/failure emits a material event.
5. Normal scheduler/policy determines the next action.

Expected result: no arbitrary result-to-command path and no duplicate wake from
the same sequence.

## Diagnostics

Provide a read-only diagnostics bundle containing:

```text
schema/policy/runtime versions
snapshot health and cursor
attention reducer health
active claims/leases
reconciliation health
external adapter health
notification delivery health
bounded recent errors
redacted trace IDs
```

Never include credentials, hidden reasoning, unrestricted command environment,
or unbounded repository content.

## Product telemetry

Measure feature value, not activity:

- attention coverage, discovery time, duplicate/false-critical rate;
- return-view time to correct first action and comprehension;
- liveness precision/recall, healthy-work interruptions, progress after action;
- recovery work-preservation, duplicate ownership, unknown effects, latency;
- notifications/interruption count, critical response time, delayed action;
- investigation reuse, repeated work avoided, correctness, overhead;
- topology answer accuracy/time, accessibility, render latency;
- snapshot consistency, latency, size, and staleness.

No telemetry record should include secret values or raw hidden reasoning.

## User-value acceptance tests

1. **Attention:** create a decision, emit later progress and task completion, and
   verify the item remains open until the typed answer outcome.
2. **Resumption:** interrupt participants on representative runs; compare current
   UI with Return view for time and correctness of first action.
3. **Liveness:** replay healthy quiet, external wait, degraded, repeated failure,
   and ownership-unknown traces; measure classification and intervention safety.
4. **Topology:** answer ownership/dependency/evidence questions with table only
   versus optional graph.
5. **Notifications:** mirror then batch eligible events; verify critical bypass,
   maximum defer, response time, and missed/reopened rate.
6. **Investigation reuse:** give a later agent either raw transcript or accepted
   artifact; measure repeated reads/tokens and implementation correctness.

## Rollback and support

Each capability has an independent mode. Rollback disables new adaptive
behavior and presentation changes while preserving authoritative records and
active work. Do not roll back by deleting attention, artifacts, recovery reports,
or cursors.

Support documentation must distinguish:

```text
preserved
resumed
reconciled
verified
resolved
completed
```

Those terms are not interchangeable.

## Copy and terminology rules

Use literal product terms: operator, run, task, attempt, worker, supervisor,
reviewer, investigation, attention, recovery, external condition, notification,
artifact, evidence, candidate, integration.

Avoid decorative metaphors, role-play vocabulary, and claims that imply more
authority or certainty than the controller has proven.
