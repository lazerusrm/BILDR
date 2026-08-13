# Supervisory Orchestrator Implementation Plan: Foundations

**Baseline:** `architecture/governed-self-improvement-20260811` at `c74629881b78916d2ece74c6c73a429f028328ff`

Implement nine reviewable slices:

```text
SO-001 contracts -> SO-002 store -> SO-003 snapshots/efficiency
 -> SO-004 Terra shadow -> SO-005 low-risk actions -> SO-006 Sol broker
 -> SO-007 API/UI -> SO-008 replay/evals -> SO-009 canary/activation
```

SO-007 may begin after SO-004, but activation waits for SO-008. Recommended
branch prefix: `supervision/SO-###-<slug>`.

Cross-cutting invariants: models remain proposal-only; snapshots/decisions are
immutable and hash-bound; stale decisions execute nothing; only allowlisted
actions pass; supervisor/expert are read-only; expert advice never executes
directly; no model closes, approves proof, changes custody, pushes, publishes,
or merges; external writes retain human approval; hidden reasoning is not
stored; all usage is attributable; unknown values fail closed; recovery never
replays an action without idempotency proof.

## SO-001 — Domain, schemas, and configuration

**Depends on:** none
**Mode after merge:** `disabled`

Add IDs and closed enums in `crates/harness-domain/src/lib.rs`:

```rust
id_type!(SupervisorReviewId);
id_type!(SupervisorSnapshotId);
id_type!(SupervisorDecisionId);
id_type!(SupervisorActionId);
id_type!(ExpertRequestId);
id_type!(ExpertResponseId);

pub enum SupervisorMode { Disabled, ObserveOnly, Shadow, Advisory, ActiveLowRisk, Active }
pub enum SupervisorTriggerKind { /* architecture allowlist */ }
pub enum SupervisorActionKind { /* decision schema allowlist */ }
pub enum SupervisorActionState { Proposed, PolicyAccepted, PolicyRejected, Executing, Succeeded, Failed, Stale, Canceled }
pub enum ExpertRequestState { Proposed, PolicyAccepted, PolicyRejected, Queued, Running, Completed, Failed, Inconclusive, Canceled, Stale }
pub enum EfficiencyClass { Unknown, Healthy, Watch, Degraded, Stalled }
```

Add `AgentRole::Supervisor` and `AgentRole::Expert`; do not change
`AgentRole::Governor`.

Add the four JSON Schemas from this design and conforming examples:

- `harness.supervisor-snapshot.v1`;
- `harness.supervisor-decision.v1`;
- `harness.expert-request.v1`;
- `harness.expert-response.v1`.

Extend `crates/harness-profile/src/lib.rs` with typed `SupervisionConfig` and
`ExpertConfig`, retaining `#[serde(deny_unknown_fields)]`. Add disabled defaults
to `config/harness.example.toml`. Add read-only role files:

```text
runtime/roles/supervisor.toml  # gpt-5.6-terra / high
runtime/roles/expert.toml      # gpt-5.6-sol / xhigh
```

Validation rejects writable supervisor/expert sandboxes, automatic expert route
other than Sol xhigh, more than one uncertainty retry, invalid budgets/timers,
expert children above zero, and active mode without compatible runtime/schema
support.

Tests:

- serde round trips and unknown-value rejection;
- supervisor/expert cannot become writable;
- safe default config parses and unsafe combinations fail;
- schema catalog and examples pass `cargo xtask schema-check`;
- existing Governor/worker/profile fixtures are unchanged;
- requested/effective model and effort fields serialize exactly.

Exit: closed, read-only, disabled-by-default contracts with zero model calls.

## SO-002 — Durable supervisory custody

**Depends on:** SO-001
**Mode after merge:** `disabled`

Add `migrations/0012_supervisory_control.sql` for reviews, snapshots, decisions,
actions, expert requests, and responses. Create unique indexes for snapshot
revision, one active review/run, one active expert/run, active escalation
signature, action dedupe, and one response/request.

Store canonical JSON plus SHA-256 and project query-critical columns. Use
foreign keys to runs, tasks, attempts, and sessions where singular. Audit
records never cascade-delete.

Add focused modules:

```text
crates/harness-store/src/supervision.rs
crates/harness-store/src/experts.rs
```

Expose them from `lib.rs`; keep feature logic out of the already-large
`queries.rs`.

Required operations:

- append, claim, complete, and release a review;
- allocate snapshot revision transactionally;
- insert/get/list snapshot and verify digest;
- insert decision and policy result;
- insert action, transition closed action state, enforce dedupe;
- insert/claim/cancel/complete expert request;
- insert/get expert response;
- recover incomplete reviews/requests;
- count expert usage by run/task/signature.

Payload/binding fields are immutable. Only closed store methods mutate lifecycle
state.

Tests:

- migrate from 0011 and empty DB;
- concurrent snapshot revision allocation and review claiming;
- stale-claim recovery;
- action dedupe race;
- one active expert/run and active-signature uniqueness;
- policy permits a later bounded consultation only after terminal state;
- hash corruption detection;
- no cascade deletion;
- restart with decision persisted/action unexecuted;
- backup/restore and concurrent SQLite behavior.

Exit: synthetic records survive restart without invoking Codex.

## SO-003 — Event router, snapshots, progress, and efficiency

**Depends on:** SO-001–002
**Mode after merge:** `observe_only`

Create:

```text
crates/harness-orchestrator/src/supervision/
  mod.rs
  event_router.rs
  snapshot.rs
  progress.rs
  efficiency.rs
  critical_path.rs
  fixtures.rs
```

Use the existing store/domain event pipeline; do not create a second bus.

### Event router

Implement material-event allowlist, telemetry-only denylist, per-run two-second
coalescing, one active claim, `next_review_at`, event-cursor binding, restart
recovery, and typed classification reasons. Unknown events are telemetry-only
with diagnostics until explicitly classified. Observe mode never calls a
model.

### Critical path

From the approved task DAG and current states, compute ready/blocked frontiers,
tasks gating remaining completion paths, newly unblocked dependencies,
completed/verified/integrated nodes, and evidence invalidated by integration.
This is deterministic graph logic.

### Progress

Add `MaterialProgressKind` values for candidate materialized/improved, milestone
completed, criterion proven, validation improved, blocking finding resolved,
dependency resolved, authority decision resolved, and expert ambiguity resolved.
Every event references concrete evidence. Agent messages alone cannot advance
the sequence.

### Efficiency

Implement the documented vector and cold-start classes in a versioned
`EfficiencyPolicyV1`. Persist raw counts, ratios, class, reason codes, cohort,
sample size, and policy version. Exclude controller/external blocked intervals
from productive-active time and normalize semantic action signatures.

### Snapshot compiler

Produce deterministic canonical JSON with exact goal, plan, profile, base SHA,
event cursor, and revision bindings; changed tasks plus critical-path context;
active/recent agents; progress and efficiency; evidence; prior action outcomes;
expert/budget state; and a controller-generated allowed-action list. Hash bytes
before insert. Use one consistent read transaction or captured projection
version. Exclude unbounded logs/diffs/transcripts.

Tests:

- DAG/frontier property tests;
- event classification table;
- 10K heartbeats create zero reviews;
- burst material events coalesce once;
- event during review schedules a fresh later review;
- prose cannot complete a milestone;
- external blocking does not reduce agent efficiency;
- repeat normalization and baseline behavior;
- deterministic snapshot bytes/digest;
- bounded snapshot omits large raw payloads;
- allowed actions match state/mode;
- restart replays a pending event once;
- virtual-time liveness scheduling/clamping.

Exit: observe mode emits truthful, bounded snapshots and metrics with zero model
calls and zero controller mutation.
