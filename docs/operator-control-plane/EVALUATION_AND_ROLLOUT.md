# Evaluation and rollout

## Purpose

This document prevents technically correct but performative features from
becoming product defaults. Each capability needs evidence that it improves a
user outcome without violating custody, authority, privacy, reliability,
performance, or cost constraints.

Implementation completion is not promotion evidence.

## Evaluation principles

1. Define the user problem, intervention, primary metric, and countermetric
   before collecting results.
2. Use deterministic invariants for safety; do not ask a model judge whether a
   duplicate writer or stale approval is acceptable.
3. Use held-out replay for classification and policy.
4. Use controlled operator tasks for resumption/comprehension claims.
5. Use production observe/shadow data before automatic action.
6. Bind every result to exact implementation SHA, schema, policy, model, prompt,
   configuration, dataset, split, and seed.
7. Preserve disagreement, negative outcomes, and inconclusive results.
8. Prevent feature authors/candidate policies from being the sole graders.
9. Promote the smallest mode supported by evidence.
10. Roll back behavior without deleting authoritative records.

## Evidence classes

### Deterministic conformance

State transitions, source closure, revisions, digests, idempotency, one-owner
rules, no forbidden action, redaction, and API authority.

### Trace replay

Historical/synthetic event windows with controller facts, expected current
state, acceptable actions, forbidden actions, later outcome window, risk/task
class, and leakage group.

### Controlled operator study

Representative users perform concrete tasks with randomized/counterbalanced
product conditions. Capture correctness, time, navigation, confidence, and
qualitative limitations.

### Production observe/shadow data

New classifier/projection/policy runs without changing current behavior. Compare
recommendations, delivery, latency, cost, and later outcomes.

### Canary

Explicit repository/task/risk/operator cohorts with emergency disable, bounded
budgets, and rollback drill.

## Versioned evaluation manifest

Every run records:

```text
evaluation ID and purpose
implementation/base SHA
schema/policy/prompt/model/runtime versions
configuration and rollout mode
dataset version, split, leakage group
random seed and repetitions
human label/adjudication version
primary metrics and countermetrics
hard gates
known limitations
artifact/result digests
```

## Dataset design

### Scenario families

```text
attention opened then buried by progress
attention source revised/superseded/invalidated
task completes with open blocking/nonblocking attention
healthy quiet work
long bounded compile/test
exact external wait
repeated semantic loop
repeated typed failure
material candidate/validation progress
process dead with clean worktree
process dead with useful uncommitted work
live process identity mismatch
stale lease/approval
unknown command/external effect
restart at every intent/effect/receipt boundary
investigation accepted/invalid/stale/oversize
external condition duplicate/out-of-order/continuity break
notification critical bypass/defer/degradation
large/truncated/stale snapshot
topology ownership/dependency/evidence questions
knowledge valid/stale/contradicted
```

### Splits

Group related incidents, repository families, repeated failures, and derived
mutations in one leakage group. Maintain development, calibration, held-out,
and untouched final holdout. Never tune thresholds on the final holdout.

### Labels

Use typed labels for current state, source authority, material event, liveness,
recovery safe actions, required attention, notification class, and human-only
authority. Two independent reviewers label high-impact/disputed cases. Preserve
raw disagreement and adjudication.

## Hard safety gates

All activated modes require zero observed violations in deterministic/fault
suites for:

```text
unresolved authoritative source missing from attention
duplicate mutable writer
unknown ownership authorizing replacement
unknown external effect automatically retried
unknown worktree content deleted/reset/overwritten
stale approval/decision accepted
presentation or model output closing authority record
investigation mutating or creating candidate
projection used as mutation authority
critical notification omitted by policy
secret/credential/hidden reasoning persisted or exported
unauthorized push/publication/readiness/merge
remote runtime enabled by this program
```

A hard-gate failure disables the affected capability regardless of aggregate
quality metrics.

## Capability 1: durable attention

### Hypothesis

Source-owned attention reduces lost required actions and decision-discovery time
without creating excessive duplicate or false-critical items.

### Test design

- replay all supported source lifecycles and interleaved unrelated activity;
- fault before/after source and attention writes;
- compare source current state with ledger after restart/rebuild;
- run operator search tasks in current UI versus attention center.

### Metrics

Primary:

```text
attention coverage recall
decision/approval discovery time
time from open to correct action
overdue blocking items
```

Counter:

```text
duplicate rate
false critical/high rate
stale/superseded item duration
operator reopen/correction
items per active run
```

### Activation

Deterministic source adapters may become active after hard gates. New source
adapters remain observe-only until their coverage/closure fixtures pass.

## Capability 2: return-to-work view

### Hypothesis

A chronological evidence-backed return view improves correct task resumption
versus the current run/transcript interface.

### Controlled study

Interrupt participants during representative implementation, investigation,
validation, blocked, and recovery scenarios. After a fixed delay, randomize:

```text
current BILDR surfaces
Return view plus current surfaces
```

Tasks:

- identify current owner and candidate;
- name blocking attention;
- explain material changes since interruption;
- choose the next legal action;
- identify preserved/recovery limitations.

### Metrics

Primary:

```text
time to correct first action
state-comprehension accuracy
attention discovery recall
source-navigation actions
```

Counter:

```text
incorrect first action
important omission
view reading time/payload
confusion between historical/current state
self-reported overload
```

### Initial promotion rule

Require statistically and practically meaningful improvement in correct first
action or comprehension, with no increase in serious incorrect action. Report
confidence intervals and participant/task distribution; do not set a permanent
threshold before pilot variance is known.

## Capability 3: investigation artifacts

### Hypothesis

A validated structured artifact reduces repeated discovery and improves later
implementation/review compared with transcript-only handoff.

### Paired evaluation

Give matched later agents either:

- accepted investigation artifact and referenced evidence; or
- bounded transcript/raw findings representing current behavior.

Measure:

```text
correct facts recovered
repeated reads/searches
tokens and elapsed time
implementation/review correctness
unresolved decision capture
evidence/source accuracy
```

Countermetrics:

```text
artifact generation/validation cost
unsupported/stale finding influence
schema repair rate
sensitive-data findings
artifact not reused
```

### Promotion

Activate only after mutation/candidate hard gates and evidence-quality targets
pass. Knowledge proposal remains separately governed.

## Capability 4: material progress classifier

### Hypothesis

The taxonomy distinguishes useful state change from activity sufficiently for
liveness and supervision.

### Evaluation

Measure precision/recall by event kind, task/risk/role class, impact-weighted
false positive/negative, repeated-run determinism, and correlation with later
milestone/validation/integration advance. Include explicit activity-only cases.

Do not compute a one-number progress score or treat correlation as causal proof.

## Capability 5: liveness episodes

### Hypothesis

Stateful evidence reduces both missed stalls and false interventions compared
with timeout/output heuristics.

### Trace corpus

Include quiet healthy commands, long tests, external waits, material progress,
loops, repeated failures, tool degradation, worktree changes, process/session
loss, identity mismatch, and ownership unknown.

### Metrics

```text
precision/recall for intervention-worthy state
time from true stall to classification
healthy-work interruption rate
no-progress tokens/time after classification
material progress within outcome window
repeat interventions per episode
operator acceptance/correction
cost and latency
```

### Hard gates and rollout

Zero destructive/ownership violations. Roll out observe -> shadow -> advisory ->
active-low-risk. `wait`, inspection, and reconciliation may be considered before
steering/follow-up; fresh attempt remains proof-gated reconciliation.

## Capability 6: reconciliation and recovery

### Hypothesis

Deterministic reconciliation preserves useful work and restores legal state
without duplicate ownership or ambiguous effect retry.

### Fault matrix

Inject failure before/after:

```text
run/task/attempt/worktree creation
lease claim/heartbeat/expiry
process spawn/session/turn creation
command intent/start/result
approval open/decision
candidate commit/custody
artifact write/register
validation/integration
external observation/event
reconciliation intent/action/receipt
snapshot publication/notification receipt
```

Test daemon and App Server restarts, process kill, database busy/full/IO failure,
version mismatch, corrupt digest, lost/duplicated/out-of-order signal, and
concurrent reconcilers.

### Metrics

Primary:

```text
work preserved
legal state restored
recovery latency
idempotent repeated pass
correct stale approval/verification handling
```

Counter:

```text
false pause
manual intervention
orphan claim/worktree
unbounded startup delay
incorrect recovered claim
```

### Activation

Start with report/attention only, then idempotent noncontroversial actions, then
proof-gated replacement. Maintain a startup time budget and resumable cursor.

## Capability 7: external condition registry

### Hypothesis

Typed durable waits avoid model polling and resume accurately.

Metrics:

```text
model turns/tokens avoided
terminal observation-to-wake latency
continuity/restart success
duplicate wake rate
poll/API volume
rate-limit/timeout behavior
false terminal classification
```

Gate: wake-only; result never executes action. Every adapter passes identity,
sequence, malformed/oversize, restart, and continuity-break tests.

The currently implemented local clock and repository-capacity adapters are
controller scoped and wake-only. Repository capacity accepts no path, command,
credential, or provider result; it records bounded backoff samples and marks a
source-identity change or read failure `unknown`. GitHub, review, credential,
and HTTP/service adapters remain disabled pending their separate connector,
rate-limit, redaction, and continuity evidence.

## Capability 8: presence and notification policy

### Hypothesis

Bounded batching reduces routine interruption without delaying important action.

### Replay and operator study

First mirror classifications. Replay current event histories through proposed
policy and measure theoretical delivery. Then run opt-in focus/unattended study.

Primary:

```text
routine interruptions per hour
time in uninterrupted work
critical/high response time
delivery success
return digest comprehension
```

Counter:

```text
important action delayed past policy
missed/reopened item
operator mode confusion
batch reading burden
alert fatigue
failed delivery duration
```

Activation: critical bypass and durable delivery first; suppression remains
opt-in until benefit and countermetrics pass.

## Capability 9: topology view

### Hypothesis

A graph improves ownership/dependency/evidence comprehension beyond the
normative table/list.

Study table only versus table+graph for factual questions. Measure answer
accuracy/time, navigation actions, accessibility completion, preference,
rendering, and incorrect layout inference. Keep graph disabled if benefit is not
meaningful or accessibility/performance regresses.

## Capability 10: correlation and trace graph

### Hypothesis

Correlation reduces time to root cause and explains action/cost attribution.

Test support/debug scenarios with ordinary logs/current views versus bounded
trace export. Measure root-cause accuracy/time, number of queries/logs opened,
causal chain validity, redaction, and cost attribution. Security/redaction is a
hard gate.

## Capability 11: governed knowledge reuse

Compare eligible tasks with no retrieval versus active in-scope reviewed
knowledge. Measure repeated failure/discovery, time/tokens, procedure use,
verification, stale/incorrect influence, and operator correction. No
single-incident auto-promotion.

## Deferred capability: remote execution

No canary in this program. A future RFC must first pass protocol/model tests for
content identity, signed/expiring leases, node capability/identity, transport
loss after unknown completion, duplicate dispatch/results, compromise/
quarantine, provenance, central re-verification, credential isolation, and no
remote publication authority.

## System performance evaluation

### Snapshot/store

Measure p50/p95/p99 compile latency, source-query time, bytes/allocations/CPU,
projection lag, replay/rebuild, attention/observation throughput, condition
polling, retention compaction, and startup reconciliation.

### UI

Measure initial render, incremental SSE update, large filters/lists, table and
optional graph, memory, keyboard/screen-reader completion, and degraded/stale
states.

Performance regression cannot be hidden by asynchronous delay or smaller test
fixtures. Use scale above expected initial use.

## Privacy and security evaluation

Test credential-value exclusion, attention/notification/trace redaction,
repository sensitivity, external untrusted content, artifact paths/digests,
malformed IDs and traversal, oversize denial, localhost/session/CSRF/same-origin,
export policy, hidden-reasoning exclusion, and future remote identity simulation.

## Rollout modes

```text
disabled
observe_only
shadow
advisory
active_low_risk
active
```

- **Observe:** compute facts/metrics; no new visible behavior unless explicitly
  marked diagnostic.
- **Shadow:** produce classification/proposal/delivery beside current behavior;
  execute/suppress nothing.
- **Advisory:** show operator proposed legal action or policy effect; operator
  chooses through normal source path.
- **Active low risk:** deterministic reversible proven actions only.
- **Active:** reviewed closed action set; human-only, publication, merge,
  destructive, credential, and evidence-acceptance authority remains unchanged.

## Canary design

Canary by repository profile, task/risk class, operator opt-in, capability,
implementation/policy version, and model route where applicable. Do not canary
storage integrity semantics inconsistently across repositories. Every canary has
bounded budget, live countermetrics, emergency disable, and rollback drill.

## Minimum observation windows

Initial planning assumptions, to be revised from event rates:

- deterministic invariant suites: every CI run;
- fault matrix: bounded PR subset plus full scheduled/pre-activation suite;
- liveness/notification shadow: at least 100 representative episodes/events
  across multiple task classes;
- usability studies: enough participants/tasks for stable median/error estimates;
- production canary: at least 14 calendar days and sufficient representative
  volume;
- rare critical paths: synthetic/fault coverage remains required regardless of
  production volume.

## Promotion review

The packet includes exact SHA, versions, dataset/splits, hard gates, primary and
countermetrics, disagreement, security/privacy, performance/cost, known
limitations, rollback result, recommended mode, and independent review.

Review in order: authority, custody/ownership, uncertainty, identity/digests,
replay/restart, security/privacy, user outcome, performance/cost,
maintainability.

## Rollback triggers

Immediately disable/roll back affected behavior for:

- any hard invariant violation;
- critical notification omission;
- source authority bypass;
- repeated false destructive or replacement intervention;
- work loss;
- secret/privacy leak;
- stale snapshot presented as current after integrity failure;
- unbounded storage/CPU/startup behavior;
- incompatible recovery corrupting active state.

Review/hold promotion for no meaningful user benefit, alert fatigue, graph not
improving comprehension, investigation overhead exceeding reuse, insufficient
liveness precision, delayed important action, or stale knowledge influence.

## Program success

The program succeeds when operators can safely leave/resume work, obligations
stay durable, useful work survives failures without duplicate writers, current
state is canonical, investigation evidence is reused, liveness actions improve
progress safely, routine interruption falls without critical omission, and every
controller action is explainable and reversible under measured rollout.

It does not succeed merely by adding more autonomy or more visible activity.
