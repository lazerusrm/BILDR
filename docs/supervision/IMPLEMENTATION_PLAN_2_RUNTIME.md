# Supervisory Orchestrator Implementation Plan: Runtime and Expert Lane

## SO-004 — Terra supervisor runtime in shadow mode

**Depends on:** SO-003
**Mode after merge:** `shadow`

Create:

```text
crates/harness-orchestrator/src/supervision/
  runtime.rs
  prompt.rs
  decision.rs
  uncertainty.rs
```

Extend the Codex adapter only where existing primitives are insufficient:
start/resume one read-only supervisor thread per run, set its persistent goal,
start a bounded turn with requested model/effort and strict schema, record
effective route and usage by role, recover an interrupted supervisor, and
reject supervisor child-agent spawning unless a future accepted explorer action
creates a controller-owned read-only session.

Use stable developer instructions plus the immutable snapshot. Accept only the
decision schema, allow one syntax/schema repair, execute no action in shadow,
policy-evaluate every proposal, and persist all results. When uncertainty policy
fires, start exactly one fresh Terra `xhigh` turn with the same snapshot, first
decision, and typed concerns; retain both decisions and a superseding relation.
Invalid second output fails closed.

Do not send raw history or full previous conversations. Persistent thread
continuity may improve cache/model state, but each decision is independently
bound to the current snapshot.

Shadow `DecisionPolicy` classifies at least: `valid`, `stale`,
`action_not_allowed`, `target_missing`, `incompatible_state`,
`evidence_missing`, `budget_exceeded`, `custody_or_approval_boundary`,
`uncertainty_retry_required`, `expert_gate_missing`, and
`safe_in_current_mode`. No proposal calls a controller command.

Tests with `fake-app-server`:

- valid wait and targeted steer;
- unknown action/schema rejection;
- malformed output repair and invalid retry;
- requested/effective route attribution;
- low-confidence high result triggers one xhigh retry;
- clear decision avoids retry;
- event during turn makes result stale;
- writable sandbox and child spawn rejected;
- daemon/App Server restart;
- token/cost attribution;
- no scheduler/action call in shadow.

Exit: shadow mode supervises a complete fake run, persists decisions and policy
results, and executes zero actions.

## SO-005 — Closed action executor and low-risk activation

**Depends on:** SO-004
**Modes after merge:** `advisory`, then `active_low_risk`

Create:

```text
crates/harness-orchestrator/src/supervision/
  policy.rs
  actions.rs
  prompts.rs
```

Use one adapter per action kind, not a generic “execute JSON” path:

```rust
#[async_trait]
pub trait SupervisorActionHandler {
    fn kind(&self) -> SupervisorActionKind;

    async fn validate(
        &self,
        context: &ActionValidationContext,
        action: &SupervisorAction,
    ) -> Result<ActionPolicyVerdict>;

    async fn execute(
        &self,
        context: &ActionExecutionContext,
        accepted: AcceptedSupervisorAction,
    ) -> Result<ActionOutcome>;
}
```

Every handler re-reads target state, compares snapshot/goal/plan revisions,
claims the dedupe key, persists execution intent, invokes an existing controller
method, persists the outcome, and emits resulting domain events.

Initial automatic set:

- `wait`;
- `continue_attempt`;
- `steer_active_turn`;
- `start_followup_turn`;
- `spawn_explorer`.

`spawn_explorer` creates a controller-owned read-only primary session with a
bounded budget and capacity slot; it is not an invisible native child.
Advisory mode requires operator application. Active-low-risk automatically
executes only these five actions.

A steer/follow-up prompt includes task/attempt identity, current objective, one
reason code, exact missing outcome/evidence, controller evidence references,
prohibited scope changes, and the observable next result. Reject prompts that
ask self-approval, broaden paths, fabricate proof, request external writes,
contain secrets, merely restate the task without strategy correction, or exceed
the configured bound.

After canary evidence, add `retry_fresh_attempt`, `spawn_reviewer`,
`reroute_attempt`, `request_replan`, `request_verification`,
`queue_integration`, `cancel_attempt`, `pause_for_human`, and `stop_run`, each
mapped to existing state transitions and preconditions.

Tests:

- exactly one registered handler per enum variant;
- missing handler blocks active-mode startup;
- stale target and concurrent dedupe races;
- steer binds the exact active turn;
- follow-up reuse requires identical model/sandbox/account/custody;
- retry carries bounded continuity and typed correction;
- reviewer independence;
- reroute only between attempts;
- verification requires candidate/evidence;
- integration requires verified task;
- cancellation preserves worktree;
- human pause asks one concrete decision;
- stop cannot bypass state machine;
- prompt redaction and size bounds;
- advisory apply/reject flow;
- active-low-risk rejects every high-risk action.

Exit: canary traces reduce completion-to-next-dispatch delay and no-progress
tokens without a custody, state, approval, or completion-authority violation.

## SO-006 — Sol xhigh expert broker

**Depends on:** SO-004; automatic use also depends on SO-005
**Modes after merge:** expert `shadow`, then advisory

Create:

```text
crates/harness-orchestrator/src/supervision/
  expert.rs
  expert_policy.rs
  expert_context.rs
```

Accept only an expert brief embedded in a policy-valid `request_expert` action;
the controller constructs the final request. Hard gates are typed high/critical
risk, repeated failure at remediation limit, qualified conclusion conflict,
semantic integration/public-contract conflict, Terra xhigh low-confidence/high-
impact result, or explicit operator request.

Explicit non-expert routes:

- credentials, approval, or human authority -> operator;
- infrastructure/availability -> controller or CI triage;
- routine test failure -> current worker/CI triage;
- ordinary first stall -> steer/retry;
- missing evidence -> explorer/reviewer.

Compile a narrow authority/evidence context. Start a fresh read-only Sol `xhigh`
session with no child agents and no persistent goal. On response: validate,
persist, mark complete/inconclusive/failed, emit a material event, execute
nothing, and include the response reference in the next Terra snapshot.

Escalation signature hashes canonical:

```text
run_id + goal_revision + category + sorted task_ids
+ sorted typed failure/conflict ids + normalized question
+ relevant authority digests
```

Exclude timestamps and unstable summaries.

Tests:

- every hard-gate positive/negative;
- human/infrastructure cases never call Sol;
- one active request/run;
- duplicate signature and per-signature cap;
- expiry and cancellation;
- effective model/effort mismatch fails closed;
- writable sandbox/child spawn rejected;
- invalid and inconclusive response;
- response triggers a new Terra review;
- expert recommendation never reaches executor directly;
- restart produces one durable request;
- usage charges run and expert budgets.

Exit: disagreement/repeated-failure fixtures produce exactly one bounded Sol
consultation followed by a separate Terra decision; routine fixtures produce
none.
