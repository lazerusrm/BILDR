# Supervisory Orchestrator: Runtime Architecture

## Components

Implement the feature as modules under `crates/harness-orchestrator/src/supervision/` first. This keeps controller authority in the existing composition boundary and prevents further growth of the current `lib.rs`. Split a crate only after the interfaces and dependency direction are proven.

### `SupervisorEventRouter`

Responsibilities:

- consume durable domain events after projection;
- classify material versus telemetry-only events;
- coalesce related events for the same run;
- maintain `next_review_at` without model polling;
- ensure one active supervisor turn per run;
- recover pending work after daemon/App Server restart;
- emit a durable `supervisor_review_requested` record.

It must not compile prompts, call models, or execute decisions.

```rust
pub trait SupervisorEventRouter {
    fn classify(&self, event: &DomainEvent) -> SupervisorEventClass;

    async fn enqueue(
        &self,
        run_id: &RunId,
        trigger: SupervisorTrigger,
    ) -> Result<SupervisorReviewRequest>;

    async fn claim_next(
        &self,
        now: Timestamp,
    ) -> Result<Option<ClaimedSupervisorReview>>;
}
```

### `SupervisorSnapshotCompiler`

Responsibilities:

- read one transactionally consistent run projection;
- bind goal revision, plan digest, profile, base SHA, and event cursor;
- compute the dependency and critical-path frontier;
- include changed tasks plus context needed to interpret the critical path;
- compile deterministic progress and efficiency measurements;
- include evidence references and compact summaries, not unbounded logs;
- expose only action kinds currently legal for the run mode and state;
- persist the immutable snapshot before the model call.

A snapshot is a receipt. The model cannot ask the controller to reinterpret the same revision after seeing its answer.

```rust
pub struct SnapshotRequest {
    pub run_id: RunId,
    pub trigger_event_ids: Vec<String>,
    pub previous_decision_id: Option<SupervisorDecisionId>,
}

pub trait SupervisorSnapshotCompiler {
    async fn compile(&self, request: SnapshotRequest) -> Result<SupervisorSnapshot>;
}
```

### `EfficiencyAnalyzer`

Responsibilities:

- derive vectors from durable usage, command, candidate, validation, and state events;
- separate productive active time from controller, approval, policy, and external blocking;
- calculate role/risk/task-class baselines only with a minimum sample count;
- classify measurements through deterministic versioned policy;
- retain raw inputs, cohort, sample size, and policy version.

It must not select a model or action.

### `SupervisorRuntime`

Responsibilities:

- start or resume one read-only supervisor thread per run;
- provide stable instructions and an immutable snapshot;
- request strict JSON matching the decision schema;
- allow at most one syntax/schema repair;
- optionally perform one Terra `xhigh` uncertainty retry;
- persist requested/effective model, effort, usage, and concise summary;
- return the decision to policy validation.

It must not invoke worker tools or controller methods directly.

### `DecisionPolicy`

Responsibilities:

- validate schema and identity bindings;
- reject a stale snapshot, goal revision, plan digest, or target;
- require the action kind to appear in `allowed_actions`;
- enforce target state, risk, custody, budget, approval, and action-specific preconditions;
- deduplicate action keys;
- determine whether an expert brief satisfies a hard escalation gate;
- persist accepted and rejected results with typed reasons.

### `ActionExecutor`

Responsibilities:

- translate one accepted proposal into one existing controller command;
- recheck preconditions in the transaction recording execution intent;
- call existing thread, turn, scheduler, retry, review, verification, or integration paths;
- record the outcome and resulting domain events;
- never infer success from the model's requested outcome.

Avoid compound actions such as “repair and verify.” Let the next material event select the next step.

### `ExpertRequestBroker`

Responsibilities:

- materialize a controller-bound request from an accepted expert brief;
- enforce `gpt-5.6-sol`, `xhigh`, read-only sandbox, advisory-only authority, and zero child agents;
- deduplicate by escalation signature;
- allow one active consultation per run;
- enforce run, task, signature, and token budgets;
- compile a narrow context packet;
- persist a schema-valid response;
- publish `expert_completed`, `expert_failed`, or `expert_needs_human`.

It must not execute the expert recommendation.

## Material events

Wake immediately, subject to short coalescing, on:

- `run_execution_started`;
- `goal_revision_changed`;
- `task_needs_help` or `task_stalled`;
- `attempt_failed` or `attempt_interrupted`;
- `agent_completed`;
- `candidate_materialized`;
- `validation_completed` or `verifier_completed`;
- `integration_conflict`;
- `dependency_unblocked`;
- `expert_completed` or `expert_failed`;
- `operator_steered`;
- `budget_boundary_crossed`;
- `no_progress_boundary_crossed`.

Heartbeats, token deltas, command-output chunks, reasoning summaries, ordinary file reads/searches, unchanged child status, and SSE activity update telemetry but do not invoke a model.

Recommended coalescing:

```text
first material event
 -> open 2-second per-run window
 -> append later event ids
 -> claim one review
 -> permit only one active supervisor turn
 -> events during the turn mark the result potentially stale
 -> compile a fresh snapshot before executing any stale result
```

Critical operator stop/cancel paths bypass model review.

## Scheduled liveness review

Timers detect semantic stagnation; the existing watchdog owns process death, missed heartbeats, and turn timeout.

A decision chooses `on_event`, `at_time`, or `none`. Policy clamps requested timing:

| Condition | Earliest | Default |
|---|---:|---:|
| healthy active turn/command | 10 min | 30 min |
| efficiency `watch` | 5 min | 15 min |
| `degraded` with no immediate action | 2 min | 5 min |
| waiting on approval/external input | none | on event |
| ready task and no active work | immediate | immediate |

The model cannot request sub-minute polling.

## Snapshot contract

The canonical contract is [`harness.supervisor-snapshot.v1`](../../schemas/harness.supervisor-snapshot.v1.schema.json).

Keep the stable prompt prefix limited to role/non-authority, controller boundaries, action vocabulary, escalation policy, and evaluation rubric.

The revision-bound payload includes:

- snapshot, run, revision, and trigger identities;
- exact base SHA, plan digest, profile id, event cursor, and goal revision;
- original/refined objective, constraints, non-goals, success criteria, and milestones;
- critical-path frontier;
- changed and critical-path tasks;
- active/recent agents;
- deterministic progress and efficiency vectors;
- evidence references;
- run, supervisor, and expert budgets;
- prior decision/action outcomes;
- currently allowed actions;
- active/completed expert consultations.

Use references and bounded summaries. Do not include unbounded logs, full transcripts, duplicate authority, or large diffs by default. Include unchanged non-critical work as aggregates. Allow targeted read-only lookup only for a concrete unresolved evidence seam.

Set an initial routine snapshot budget of 32K input tokens and measure it; this is a product budget, not a model context claim.
