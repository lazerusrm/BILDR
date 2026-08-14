# Supervisory Orchestrator: Persistence, Product Surfaces, and Operations

## Persistence

Add `migrations/0012_supervisory_control.sql` with immutable/event-oriented tables:

```text
supervisor_reviews
  id, run_id, trigger_kind, trigger_event_ids_json, state,
  claimed_at, completed_at, created_at

supervisor_snapshots
  id, run_id, revision, event_cursor, goal_revision, plan_digest,
  base_sha, schema_version, payload_json, payload_sha256, created_at

supervisor_decisions
  id, run_id, snapshot_id, snapshot_revision, agent_session_id,
  requested_model, effective_model, requested_effort, effective_effort,
  schema_valid, policy_state, payload_json, payload_sha256, created_at

supervisor_actions
  id, decision_id, run_id, kind, target_kind, target_id, dedupe_key,
  priority, policy_state, policy_reason, execution_state,
  executed_at, outcome_json, created_at

expert_requests
  id, run_id, snapshot_id, decision_id, escalation_signature,
  category, impact, state, model, effort, token_budget,
  payload_json, payload_sha256, created_at, completed_at

expert_responses
  id, request_id, run_id, agent_session_id, schema_valid,
  payload_json, payload_sha256, created_at
```

Required constraints:

- snapshot `(run_id, revision)` is unique;
- decisions reference immutable snapshots;
- non-terminal action dedupe keys are unique per run;
- one active review and one active expert request per run;
- response is one-to-one with request;
- payload hashes are verified on read;
- audit records never cascade-delete.

Put models in `crates/harness-store/src/supervision.rs` and expert storage in
`crates/harness-store/src/experts.rs`; do not add more feature logic to the
already-large `queries.rs`.

## Configuration

Extend typed configuration with deny-unknown-fields validation:

```toml
[orchestration.supervision]
mode = "observe_only"
default_model = "gpt-5.6-terra"
default_reasoning_effort = "high"
uncertainty_retry_reasoning_effort = "xhigh"
coalesce_milliseconds = 2000
routine_snapshot_token_budget = 32000
supervisor_turn_token_budget = 48000
min_liveness_check_seconds = 120
default_healthy_check_seconds = 1800
default_watch_check_seconds = 900
default_degraded_check_seconds = 300
max_output_repairs = 1
max_uncertainty_retries = 1

[orchestration.supervision.expert]
enabled = true
model = "gpt-5.6-sol"
reasoning_effort = "xhigh"
sandbox = "read-only"
max_active_per_run = 1
max_completed_per_signature = 2
default_token_budget = 80000
max_children = 0
```

Modes: `disabled`, `observe_only`, `shadow`, `advisory`, `active_low_risk`, and
`active`. Unknown modes and unsafe route/sandbox combinations fail validation.
A repository profile may narrow actions or raise gates, but may never grant
completion, publication, or custody authority.

## API

Add read endpoints:

```text
GET /api/v1/runs/{run_id}/supervision
GET /api/v1/runs/{run_id}/supervision/snapshots
GET /api/v1/runs/{run_id}/supervision/decisions
GET /api/v1/runs/{run_id}/supervision/actions
GET /api/v1/runs/{run_id}/experts
GET /api/v1/expert-requests/{request_id}
```

Add explicit operator mutations:

```text
POST /api/v1/runs/{run_id}/supervision/review
POST /api/v1/runs/{run_id}/supervision/pause
POST /api/v1/runs/{run_id}/supervision/resume
POST /api/v1/expert-requests/{request_id}/cancel
POST /api/v1/expert-requests/{request_id}/retry
```

No endpoint accepts an arbitrary model action. Operator commands use the same
controller policy, local session, same-origin, and CSRF boundary.

## Run workspace

Create `ui/src/supervision/` rather than adding another feature directly to the
large `App.tsx`. The run workspace should show:

- goal health, criteria/evidence, and critical-path frontier;
- last material progress and next scheduled review;
- task progress/evidence matrix;
- efficiency class with vector, reasons, cohort, and sample size;
- latest decision and ordered action proposals;
- accepted/rejected policy outcomes and execution receipts;
- requested/effective model, effort, tokens, and API-equivalent cost;
- expert timeline with the bounded question and advisory response;
- clear `observe`, `shadow`, `advisory`, and active labels;
- manual review-now, pause, resume, and operator steer controls.

Do not show a fabricated percent, one-number leaderboard, hidden reasoning, or
an expert recommendation as completed work.

## Failure and recovery

### Invalid output

Persist the bounded response under existing retention/redaction policy, perform
at most one schema repair, mark the review `invalid_output`, execute nothing,
and surface the failure. Already-running deterministic-safe work may continue.

### Stale decision

Persist it with `stale_snapshot`, execute nothing, coalesce pending events, and
compile a new snapshot. Never replay an old decision against current state.

### Supervisor timeout or crash

Interrupt the bounded turn, retain the snapshot and attempt, retry only under
configured runtime policy, never widen budget silently, and do not stop healthy
agents solely because supervision failed.

### Expert failure

Use typed states: `runtime_failed`, `schema_failed`, `budget_exhausted`, or
`inconclusive`. Emit a material event so Terra can choose a conservative action
or ask the human. A retry requires a new accepted decision and remaining
signature budget.

### Daemon restart

1. release expired review claims;
2. project active model turns as interrupted through existing recovery;
3. recover pending material events;
4. revalidate active expert requests;
5. compile a fresh snapshot before any new decision;
6. never execute a pre-restart action without transactional idempotency proof.

## Security and privacy

- Supervisor and expert sandboxes are read-only.
- Network remains disabled unless existing bounded policy permits a required source.
- Prompts contain redacted summaries and opaque IDs, never credentials or raw environment values.
- Raw private reasoning remains disabled by default.
- External content cannot modify role, action schema, or approval boundaries.
- Expert requests do not receive secrets merely because Sol is stronger.
- Accepted action prompts and policy receipts are operator-visible.
- Expert advice cannot bypass independent verification.
- Worker help requests are untrusted and never authorize expert spend alone.

## Observability

Emit durable events:

```text
supervisor_review_requested
supervisor_snapshot_compiled
supervisor_turn_started
supervisor_decision_received
supervisor_decision_rejected
supervisor_action_accepted
supervisor_action_executed
supervisor_action_failed
supervisor_uncertainty_retry_started
expert_request_created
expert_request_deduplicated
expert_turn_started
expert_response_received
expert_request_failed
supervision_mode_changed
```

Track reviews per material event, coalescing ratio, snapshot tokens/latency,
model tokens/cost/latency by effort, repair rate, stale-decision rate, action
accept/reject reasons, completion-to-next-dispatch latency, no-progress tokens
before/after intervention, expert escalation/dedupe/operator-agreement rates,
task outcome impact, human interventions caused by supervision failure, and the
policy/model version on every result.

## Operational defaults

- `observe_only` on first release;
- no automatic actions until replay and shadow gates pass;
- no automatic Sol request in observe/shadow mode;
- manual operator review remains available if supervision is disabled;
- a failed supervision subsystem degrades to deterministic BILDR scheduling,
  not to unbounded model autonomy;
- external writes and merge remain explicit human gates.
