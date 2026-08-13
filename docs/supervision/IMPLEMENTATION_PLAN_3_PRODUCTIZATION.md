# Supervisory Orchestrator Implementation Plan: Productization

## SO-007 — API, CLI, and UI

**Depends on:** SO-004; may run in parallel with SO-005/006

Update `openapi/harness-api.yaml`, `crates/harness-api/src/lib.rs`, the existing
`harnessctl` command modules, and SSE/domain DTOs. Implement the read and
operator-mutation routes defined in `OPERATIONS.md`. All mutations retain the
existing local session, CSRF, same-origin, controller-policy, and state-machine
checks.

Keep the feature out of the large `ui/src/App.tsx`. Add:

```text
ui/src/supervision/
  api.ts
  types.ts
  SupervisionPanel.tsx
  GoalHealth.tsx
  ProgressMatrix.tsx
  AgentEfficiencyTable.tsx
  DecisionTimeline.tsx
  ActionReceipt.tsx
  ExpertConsultations.tsx
  supervision.css
```

Show supervision mode and route, last/next review, objective and revision,
critical-path frontier, criteria/evidence coverage, efficiency vector/class and
cohort, changed tasks, latest decision and policy result, action receipts,
model/effort/token/cost, expert question/advice/unresolved risk, stale/rejected
decisions, and operator controls.

Use explicit labels:

```text
SHADOW — no actions execute
ADVISORY — operator applies actions
ACTIVE LOW RISK
ACTIVE
EXPERT ADVISORY — recommendation not executed
```

Never derive product completion from supervisor output. Do not show a fabricated
percentage or one-number agent leaderboard.

Add CLI commands to show/review/pause/resume supervision, list decisions and
actions, and show/cancel an expert request.

Tests cover OpenAPI references, DTO serialization, local mutation protections,
SSE replay, mode labels, stale/rejected rendering, cost attribution, expert
advisory labeling, accessibility, long history, and Playwright apply/cancel
flows.

Exit: an operator can explain every displayed decision from evidence and control
the mode without direct database access.

## SO-008 — Replay harness and route evaluation

**Depends on:** SO-003–007
**Mode after merge:** shadow/advisory only

Extend the existing governed evaluation custody without making evaluation code
depend on the live orchestrator:

```text
crates/harness-eval/src/supervision.rs
fixtures/supervision/
examples/evals/supervision/
docs/SUPERVISORY_EVAL_PROTOCOL.md
```

Each case includes the immutable snapshot and digest, permitted actions, later
outcome-event window, acceptable plans, forbidden actions, expert-required and
human-authority labels, risk/task metadata, adjudicator evidence, and leakage
group. Related incidents remain in one data split.

Evaluate Terra medium, high, and xhigh; Terra high-to-xhigh; always-on Sol xhigh
as a reference; and the selected Terra-to-Sol cascade.

Measure legal/acceptable action rate, intervention quality, required and
unnecessary expert routing, human-authority routing, false completion/proof
claims, stale-target sensitivity, repeated-run consistency, next-window
material progress, tokens/time to progress and proof, repeat reduction, task
outcomes, supervisor cost/latency, Sol calls, coalescing, and cost per progress
event.

Use two independent reviewers for high-impact or disputed cases. A fresh Sol
judge may assist but is not sole label authority. Preserve disagreement.

Tests cover deterministic export, split integrity, grader versioning, no
candidate self-grading, repeated-run consistency, untouched holdout, and exact
model/prompt/policy/schema binding in promotion decisions.

Exit: held-out evidence supports or rejects the selected route. If Terra high
misses release gates, remain in shadow and change route only through a versioned
policy decision.

## SO-009 — Canary and activation

**Depends on:** SO-008

Roll out in six explicit gates:

1. observe: snapshots and metrics only;
2. shadow: Terra decisions, no action execution;
3. expert shadow: accepted briefs call Sol, then return to shadow Terra;
4. advisory: operator applies policy-valid actions;
5. active low risk: automatic wait/continue/steer/followup/explorer;
6. active: remaining non-external actions; expert stays advisory and publication stays manual.

Begin with fixture repositories, low-risk BILDR tasks, and deterministic
validators. Introduce higher-risk contract, persistence, privacy, native,
delivery, and serial integration classes only after the lower-risk gates pass.

Rollback is one configuration change to `disabled` or `observe_only`. Interrupt
active supervisor/expert turns while preserving worker/controller operations and
audit records.

Initial activation gates:

- 14 days or 100 representative shadow/advisory runs, whichever yields more cases;
- zero stale or duplicate action execution;
- zero unauthorized external operations or approval-policy violations;
- no increase in P0/P1 findings;
- improved median completion-to-useful-dispatch time;
- no-progress tokens improve or remain neutral;
- Sol usage remains within the configured budget;
- advisory acceptance is stable by task class;
- restart and recovery drills pass.

These are versioned product gates rather than permanent constants.

## Review order and definition of done

Review controller authority first, then identity/digest/custody, closed schema
behavior, stale/dedupe/recovery, model route/sandbox, action legality,
adversarial tests, operator explainability, and finally performance/cost.

The feature is done only when the cascade is measured on held-out traces; idle
supervision invokes no model; all decisions bind to truthful snapshots; progress
and efficiency derive from controller evidence; low-risk actions improve flow
without custody errors; hard questions produce bounded Sol advice that returns
through Terra; the UI explains every action and cost; failure/restart/rollback
paths pass; and an independent final audit accepts the exact implementation
head.
