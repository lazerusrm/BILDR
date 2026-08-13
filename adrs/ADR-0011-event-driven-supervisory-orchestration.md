# ADR-0011: Use an event-driven supervisory model cascade

- **Status:** proposed architecture
- **Date:** 2026-08-13
- **Decision owner:** BILDR controller architecture
- **Supersedes:** the assumption that every repository-wide goal requires a continuously active Sol governor
- **Extends:** ADR-0005 controller authority, ADR-0006 reasoning visibility, and ADR-0007 governed self-improvement

## Context

BILDR already has the correct hard control boundary: `harnessd`, not a model,
owns run and task state, dependency scheduling, path leases, Git custody,
validation, evidence classification, budgets, retries, approvals, and
publication. Models propose work under schema and policy.

The missing first-class capability is a strict supervisor whose only job is to
review the goal, measure material progress, evaluate the efficiency of active
agents, and choose the next legal orchestration action. This supervisor is not
an implementation worker. It must wake only when a material event or scheduled
liveness boundary requires judgment, and it must be able to request a stronger
expert when the evidence reveals a genuinely difficult problem.

Using the strongest model for every supervisory turn is wasteful. Using a small
or local model by default introduces another provider/runtime and lowers the
quality ceiling on the decisions that shape all downstream work. Allowing one
model to poll continuously, infer completion from prose, or directly mutate
controller state would also violate BILDR's custody model.

## Decision

Add a distinct, read-only `Supervisor` role and an event-driven supervisory
control loop.

The initial model cascade is:

| Stage | Model | Reasoning effort | Use |
|---|---|---:|---|
| routine supervision | `gpt-5.6-terra` | `high` | goal/progress review, agent-efficiency assessment, reprompting, routing, next-step selection |
| uncertainty retry | `gpt-5.6-terra` | `xhigh` | one bounded reconsideration of an ambiguous or high-impact routine decision |
| expert consultation | `gpt-5.6-sol` | `xhigh` | a concrete architecture, contract, security, data-integrity, or repeated-failure question |
| final independent audit | existing `gpt-5.6-sol` route | existing profile effort | exact-head completion assurance; unchanged by this ADR |

`max` is not an automatic supervisory tier. It remains available only through
an explicit operator/profile decision for exceptional quality-first work.

The supervisor is advisory. It consumes a controller-compiled immutable
snapshot and returns a schema-valid decision containing only closed action
kinds. The controller validates snapshot freshness, action legality, custody,
budgets, deduplication keys, and transactional preconditions before applying
any action.

A Sol expert response is also advisory. It cannot schedule work, alter a task,
approve evidence, close a goal, publish, or recursively request another expert.
The response returns to the Terra supervisor as a new material event. Terra
then proposes the next legal action under the normal controller gate.

## Why this model route

OpenAI describes Terra as the GPT-5.6 tier that balances intelligence and cost,
and recommends selecting reasoning effort intentionally on representative
workloads rather than assuming the largest setting is always optimal. Routine
supervision is structured and evidence-rich, but it has greater downstream
impact than high-volume exploration; `high` is therefore the quality-first
default, with exactly one `xhigh` retry when deterministic policy detects
uncertainty or impact. Sol is reserved for frontier capability on a bounded,
well-formed expert question.

This is a product default, not an unchangeable claim. BILDR must replay real
supervisory traces against Terra `medium`, `high`, and `xhigh`, plus a Sol
always-on baseline. Promotion remains eval-gated.

## Wake-up policy

The model is not a poller. A supervisor turn is eligible only after one or more
material triggers are coalesced:

- run execution starts or the goal revision changes;
- an agent completes, requests help, becomes stalled, is interrupted, or fails;
- a candidate diff, commit, durable artifact, or milestone is materialized;
- a validation, verifier, integration, or expert-consultation result arrives;
- a dependency becomes ready;
- a task or run crosses a configured budget/no-progress boundary;
- the operator steers the goal;
- a controller-scheduled liveness review becomes due.

Heartbeats, token deltas, command-output chunks, and ordinary tool chatter update
deterministic telemetry but do not wake the supervisor by themselves.

Events are coalesced per run, a run may have only one active supervisor turn,
and every decision is bound to the exact snapshot revision. A decision against
a stale revision is retained for audit but never executed.

## Progress and efficiency

The controller computes facts; the model interprets them.

Progress is represented by a vector rather than an opaque percentage:

- completed and active milestones;
- success criteria with exact evidence;
- critical-path frontier;
- candidate materialization;
- validator and verifier deltas;
- unresolved blocking findings;
- objective drift and missing proof.

Agent efficiency is also a vector, normalized by role, risk class, and task
class where enough history exists:

- tokens since last material progress;
- time to first candidate;
- material progress events per token window;
- repeated semantic action signatures;
- tool and command failure rate;
- validation trend;
- active versus externally blocked time;
- reuse of prior-attempt continuity;
- evidence produced for claimed progress.

The model may label an agent `healthy`, `watch`, `degraded`, `stalled`, or
`unknown`, but it cannot rewrite the underlying measurements. No single
self-reported confidence or composite efficiency score authorizes an action.

## Closed action surface

The first contract permits only:

- `wait`;
- `continue_attempt`;
- `steer_active_turn`;
- `start_followup_turn`;
- `retry_fresh_attempt`;
- `spawn_explorer`;
- `spawn_reviewer`;
- `reroute_attempt`;
- `request_expert`;
- `request_replan`;
- `request_verification`;
- `queue_integration`;
- `cancel_attempt`;
- `pause_for_human`;
- `stop_run`.

The policy engine may expose a smaller subset in each snapshot. Missing from the
allowlist means forbidden, even if the model emits a schema-valid action.

No model action means "complete", "merge", "push", "publish", "approve
evidence", "change custody", or "grant access".

## Sol escalation policy

An automatic Sol request requires an immutable expert brief and at least one
controller-recognized escalation gate:

- conflicting qualified-agent or verifier conclusions about a material
  invariant;
- the same typed failure signature after the configured bounded remediation
  rounds;
- a P0/P1 architecture, public-contract, authentication, authorization,
  tenancy, privacy, data-loss, unsafe-native, OTA, or integration conflict that
  lacks a direct mechanical resolution;
- low supervisory confidence combined with high or critical impact after the
  one Terra `xhigh` retry;
- an integration conflict whose resolution changes public or protected
  semantics;
- an explicit operator request.

Credentials, approvals, unavailable infrastructure, or policy authority that
only a human can supply route to the operator, not Sol. Ordinary CI
classification routes to CI triage. A first routine stall with a clear next
step routes to a reprompt or fresh attempt.

The controller deduplicates requests by escalation signature, allows one active
Sol consultation per run, caps consultations per task/failure signature, and
charges the request to explicit run and expert budgets.

## Persistent state and recovery

A run has at most one persistent supervisor thread. The thread receives stable
instructions and compact revisioned snapshots; it does not receive an
unbounded replay of raw events. Productive continuity may reuse the thread.
Daemon/App Server loss, model/effort change, account handoff, or stale custody
takes the existing cold-recovery path from the durable snapshot and prior
decision summary.

Supervisor snapshots, decisions, actions, expert requests, expert responses,
and action outcomes are immutable audit records. Raw hidden chain-of-thought is
not stored; concise reasoning summaries and evidence references are retained
under ADR-0006.

## Consequences

### Positive

- the strongest model is used only when its marginal capability has a concrete
  job;
- routine orchestration remains high quality without dedicating a local GPU or
  running a permanent inference server;
- thread supervision is event-driven and cheap at idle;
- goal and agent-health decisions are explainable from controller facts;
- expert calls are bounded, deduplicated, replayable, and measurable;
- completion and publication authority remain deterministic and independent.

### Costs

- four new durable contracts and a new supervisory state projection;
- an explicit event coalescer and snapshot compiler;
- trace replay/evaluation work before automatic actions are enabled;
- UI surfaces for goal health, decisions, efficiency, and expert consultations;
- calibration of thresholds by task class rather than one global heuristic.

## Rejected alternatives

### Sol `xhigh` for every supervisory turn

High quality, but it pays frontier cost and latency for routine decisions and
makes escalation indistinguishable from normal operation.

### Terra `xhigh` for every turn

Viable and retained as an eval baseline, but the sparse `high -> xhigh` retry
captures uncertainty explicitly and provides a cheaper default without lowering
the expert ceiling.

### Luna or a local open-weight model as the initial default

Potentially appropriate after replay evidence exists. It adds provider/runtime
surface now and risks routing errors before BILDR has a supervisory gold set.
The contracts intentionally permit a later eval-gated route change.

### Let the supervisor implement fixes

That recreates the existing governor/worker role, obscures custody, and makes
efficiency judgment self-interested. A strict supervisor is read-only.

### Continuous model polling

It burns tokens while agents are healthy, races active work, and converts
heartbeats into repeated judgment. Deterministic timers and event coalescing are
the correct idle path.

### Let workers call Sol directly

It produces uncontrolled fan-out and lets the least informed participant spend
the expert budget. Workers may request help; only the controller-mediated
supervisor policy may materialize an expert request.

### Treat model confidence as a probability

Self-reported confidence is advisory and not calibrated enough to be an
authorization gate. Impact, typed risk, repeated failure, disagreement, and
evidence gaps are controller facts and remain mandatory inputs.
