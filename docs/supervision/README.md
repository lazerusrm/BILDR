# Event-Driven Supervisory Orchestrator

**Status:** implementation-ready design; runtime remains disabled
**Prepared:** 2026-08-13

## Decision

Add a strict read-only `Supervisor` role that reviews the operator goal,
material progress, agent efficiency, and the best legal next action.

Selected initial route:

```text
material controller event
 -> gpt-5.6-terra / high
 -> one gpt-5.6-terra / xhigh uncertainty retry when policy requires it
 -> gpt-5.6-sol / xhigh only for a bounded hard technical question
 -> advisory expert answer returns to Terra
 -> Terra proposes a closed controller action
```

`harnessd` remains authoritative for run/task state, dependencies, leases, Git,
commands, validation, evidence, budgets, retries, approvals, and publication.
The supervisor is event-driven and consumes no model tokens when no material
event or scheduled liveness boundary requires judgment.

## Design map

- [ADR-0011](../../adrs/ADR-0011-event-driven-supervisory-orchestration.md) — decision, model route, authority, triggers, and rejected alternatives.
- [ARCHITECTURE.md](ARCHITECTURE.md) — components, event flow, snapshot compiler, runtime, policy, action executor, and expert broker.
- [CONTROL_POLICY.md](CONTROL_POLICY.md) — goal review, progress/efficiency vectors, closed actions, uncertainty retry, and Sol gates.
- [CONTRACTS.md](CONTRACTS.md) — controller envelope versus model-visible payload and schema evolution.
- [OPERATIONS.md](OPERATIONS.md) — persistence, configuration, API/UI, recovery, security, and observability.
- [EVALUATION_AND_ROLLOUT.md](EVALUATION_AND_ROLLOUT.md) — replay matrix, release gates, and acceptance scenarios.
- [IMPLEMENTATION_PLAN_1_FOUNDATIONS.md](IMPLEMENTATION_PLAN_1_FOUNDATIONS.md) — SO-001 through SO-003.
- [IMPLEMENTATION_PLAN_2_RUNTIME.md](IMPLEMENTATION_PLAN_2_RUNTIME.md) — SO-004 through SO-006.
- [IMPLEMENTATION_PLAN_3_PRODUCTIZATION.md](IMPLEMENTATION_PLAN_3_PRODUCTIZATION.md) — SO-007 through SO-009.
- [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) — the exact safe foundation delivered now and deferred runtime authority.
- [RESEARCH.md](RESEARCH.md) — source-backed rationale and open empirical questions.

## Existing Governor compatibility

BILDR's existing `Governor` can own implementation and operate in a writable
leased worktree. The new `Supervisor` does not replace it.

| Capability | Governor | Supervisor |
|---|---:|---:|
| edit a leased worktree | yes | no |
| own implementation outcome | yes | no |
| review run-wide progress | bounded to task | yes |
| emit closed controller proposals | indirect | yes |
| request policy-gated expert help | through supervisor design | yes |
| mark complete or publish | no | no |
| default route | existing profile | Terra high |

A session must not act as both strict supervisor and implementation owner in the
same attempt.

## First credible vertical slice

```text
material event
 -> immutable snapshot
 -> Terra high structured decision
 -> shadow-only policy receipt
 -> UI goal/progress/efficiency/decision panel
```

Then add low-risk action execution, the one Terra xhigh retry, the Sol expert
broker, replay evaluation, and gated activation. Automatic expert calls and
broad active actions are intentionally late.

## Contracts included in this PR

- `harness.supervisor-snapshot.v1`;
- `harness.supervisor-decision.v1`;
- `harness.expert-request.v1`;
- `harness.expert-response.v1`.

The expert schemas cover the bounded model-visible question and answer. Fixed
model route, reasoning effort, read-only mode, zero-child policy, spend limits,
and advisory authority belong to the controller envelope and cannot be chosen
by a model.
