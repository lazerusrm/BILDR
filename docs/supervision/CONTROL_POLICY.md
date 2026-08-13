# Supervisory Orchestrator: Goal, Efficiency, and Action Policy

## Goal review

The goal projection has immutable and versioned layers.

```text
immutable:
  original operator objective
  confirmed brief and hard constraints
  approval/publication boundaries

versioned:
  refined objective and intended final shape
  non-goals and success criteria
  plan digest and milestone strategy
```

The supervisor returns one goal status: `aligned`, `on_track`, `at_risk`,
`blocked`, `needs_replan`, or `completion_candidate`.
`completion_candidate` means independent verification should run; it never means
complete.

Every assessment names criteria with exact evidence, missing or inconclusive
criteria, the critical-path frontier, work that does not advance a criterion,
conflicts with authority, and plan assumptions that should change. Busy agents
do not make a goal `on_track` without material progress.

## Progress model

Store a vector, not an authoritative percentage:

```rust
pub struct ProgressVector {
    pub milestones_completed: u32,
    pub milestones_total: u32,
    pub critical_path_milestone: Option<String>,
    pub criteria_proven: u32,
    pub criteria_total: u32,
    pub candidate_materialized: bool,
    pub validations_passed: u32,
    pub validations_failed: u32,
    pub validations_inconclusive: u32,
    pub blocking_findings: u32,
    pub material_progress_sequence: u64,
    pub last_material_progress_at: Option<Timestamp>,
}
```

Material progress is a candidate diff/tree/commit created or improved, a
milestone completed with evidence, new exact criterion evidence, a previously
failing authoritative validation passing, a blocking finding resolved with
changed evidence, a dependency/authority decision resolved, or an expert answer
that removes a named ambiguity. File reads, prose, repeated tests, and agent
confidence do not count by themselves.

## Agent efficiency

For each attempt and active agent calculate:

```rust
pub struct EfficiencyVector {
    pub tokens_total: u64,
    pub tokens_since_material_progress: u64,
    pub material_progress_events: u32,
    pub time_to_first_candidate_seconds: Option<u64>,
    pub semantic_repeat_count: u32,
    pub tool_calls: u32,
    pub tool_failures: u32,
    pub validation_pass_delta: i32,
    pub validation_fail_delta: i32,
    pub active_seconds: u64,
    pub externally_blocked_seconds: u64,
    pub prior_continuity_reused: bool,
    pub baseline_sample_size: u32,
    pub policy_version: String,
}
```

Retain all inputs when deriving no-progress token ratio, tool failure rate, and
productive active ratio. Do not penalize time waiting on controller commands,
approvals, credentials, resources, or dependencies.

Normalize semantic action signatures, for example:

```text
search:<query-family>:<path-scope>
read:<path-set>
command:<program>:<normalized-args>
validation:<validator-id>:<head-sha>
reprompt:<reason-code>:<target>
failure:<typed-class>:<normalized-root-cause>
```

A repeat means the same strategy ran without new evidence or changed
preconditions; repeated prose is not enough.

Initial versioned cold-start classes:

- `unknown`: fewer than 5K attributable tokens and insufficient baseline;
- `healthy`: progress in the current window and no repeated failure/controller blocker;
- `watch`: 10K+ tokens since progress, one repeat, or declining validation;
- `degraded`: 20K+ tokens, two repeats, or tool failure above 25% after eight calls;
- `stalled`: 30K+ tokens or 40% of budget since progress plus two repeats, or the same typed failure past remediation policy.

Replay-tune these values. No single threshold may cancel work automatically;
`stalled` creates a material event for policy and model review.

Historical comparison requires compatible repository profile, role, risk,
execution mode, domain/language, and budget band, with at least ten usable
completed attempts. Show vector, class, reasons, cohort, and sample size; never
publish a one-number agent leaderboard.

## Decision contract

The canonical output is
[`harness.supervisor-decision.v1`](../../schemas/harness.supervisor-decision.v1.schema.json).
It binds exactly to the snapshot/run/revision, records requested/effective model
and effort, summarizes the outcome, assesses goal/tasks/agents, proposes ordered
closed actions, lists uncertainties, and chooses one next-review policy. A
schema-valid `wait` is required when no intervention is warranted.

## Action validation

Every action requires:

1. valid schema and exact identity binding;
2. current snapshot, goal, and plan revision;
3. action kind present in snapshot `allowed_actions`;
4. compatible current target state;
5. unexpired unique dedupe key;
6. resolvable evidence references;
7. remaining run/task/supervisor budgets;
8. no approval, custody, concurrency, or risk boundary violation;
9. transactional precondition recheck immediately before execution.

Action semantics:

- `wait`: healthy work or known event; one clamped liveness review is allowed.
- `continue_attempt`: release controller-held dispatch without modifying work.
- `steer_active_turn`: one targeted message naming a missing outcome,
  contradiction, or evidence gap; never broaden custody or approve a command.
- `start_followup_turn`: continue the same thread when model, sandbox, account,
  and custody remain unchanged.
- `retry_fresh_attempt`: new bounded attempt with prior evidence and typed
  strategy correction for poisoned context, repeats, runtime loss, route change,
  or rejected custody.
- `spawn_explorer`: bounded read-only investigation; no mutable ownership.
- `spawn_reviewer`: fresh independent read-only reviewer; never the worker.
- `reroute_attempt`: change the next attempt route, never an active turn.
- `request_expert`: materialize one controller-bound Sol request.
- `request_replan`: enter a supported planning/revision transition while
  preserving objective and immutable constraints.
- `request_verification`: use existing independent verification after controller
  preconditions pass.
- `queue_integration`: use existing dependency and serial-path rules.
- `cancel_attempt`: cancel only the named attempt and preserve failed custody.
- `pause_for_human`: ask one concrete intent, authority, approval, credential,
  or risk-acceptance question that no model may answer.
- `stop_run`: propose fail/stop only when constraints make the objective
  impossible, budget is exhausted, or continuation would violate policy.

## Terra uncertainty retry

Do not automatically execute the Terra `high` result when confidence is low;
a high/critical-impact action lacks direct evidence; actions conflict; Sol is
requested without a hard gate; assessment and action disagree; the proposal
would cancel, replan, reroute high-risk work, or stop the run; or policy detects
an unsupported assumption.

Start one fresh Terra `xhigh` turn with the same immutable snapshot, first
decision, and typed policy concerns. It must choose a conservative legal action,
ask the human, or produce a hard-gated expert brief. There is no second Terra
retry. Invalid retry output fails closed and is visible as
`supervision_failed`; deterministic-safe already-running work may continue.

## Sol expert consultation

Supported categories are `architecture_invariant`, `canonical_contract`,
`security_authorization`, `tenancy_privacy`, `data_integrity`,
`unsafe_native_or_hardware`, `integration_conflict`, `repeated_failure`,
`qualified_agent_disagreement`, and `other_high_impact`.

At least one controller fact is mandatory: high/critical risk flag, repeated
typed failure past remediation, two qualified conflicting conclusions,
integration/public-contract semantic conflict, Terra `xhigh` low confidence on
high impact, or explicit operator request.

The controller materializes
[`harness.expert-request.v1`](../../schemas/harness.expert-request.v1.schema.json)
with exact run/snapshot/goal/plan/base bindings, one crisp question, why ordinary
agents cannot resolve it, facts and disputed claims, constraints/non-goals,
bounded evidence, response requirements, token ceiling, expiry, escalation
signature, fixed `gpt-5.6-sol`/`xhigh`/read-only route, advisory authority, and
zero-child policy.

Sol returns
[`harness.expert-response.v1`](../../schemas/harness.expert-response.v1.schema.json)
with verdict, evidence-backed findings, recommended resolution, rejected
alternatives, missing evidence or human authority, unresolved risk, and
advisory confidence. The controller persists it and emits a material event;
Terra—not Sol—selects the next controller action.

Initial bounds: one active consultation per run; two completed consultations per
task/failure signature; active signature uniqueness; no expert-to-expert call;
no automatic `max`; no expert for credentials, approval, human authority, or
unavailable infrastructure; expert usage counts against run and expert budgets.

## Prompt contracts

Supervisor instruction:

```text
You are BILDR's read-only supervisor. Manage work; do not implement it.
Use only the immutable controller snapshot. Review the operator goal, exact
progress evidence, efficiency vectors, blockers, budgets, and legal actions.
Choose the smallest action that advances the critical path. Do not infer state
from agent claims, mark work complete, modify files, run implementation
commands, grant approvals, change custody, publish, or invent actions. Continue
healthy work. Reprompt only for a concrete outcome/evidence gap. Request Sol
only for a crisp high-impact question that meets a supplied gate. Return only
harness.supervisor-decision.v1.
```

Expert instruction:

```text
You are a fresh read-only Sol expert answering one bounded technical question.
Inspect only supplied evidence and the smallest targeted authority/source seam.
Resolve the disputed invariant or state the missing evidence/human authority.
Do not schedule agents, edit files, approve proof, change state, publish, or
request another expert. Return only harness.expert-response.v1.
```
