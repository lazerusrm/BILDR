# Harness Console
## Linux-First Codex Multi-Agent Control Plane for NeuralMatrix

**Status:** proposed architecture and implementation plan
**Initial repository:** `lazerusrm/NeuralMatrix`
**Target host:** Linux workstation/server
**Prepared:** 2026-08-05
**Primary binaries:** `harnessd`, `harnessctl`

---

## Executive decision

Build **Harness Console** as a local supervisory control plane around a version-pinned Codex App Server. It should feel like a focused Codex desktop command center, but its differentiator is not another chat UI. Its job is to make multi-agent engineering **observable, bounded, reproducible, and safe**:

- every agent and native subagent is visible with requested/effective model, reasoning effort, current goal, plan, current action, parent, worktree, branch, SHA, token use, and API-equivalent cost;
- every mutable task runs in a controller-created Git worktree with explicit path leases;
- the controller, not a model, owns state transitions, scheduling, Git, validation, evidence, budgets, retries, and publication;
- Sol plans and independently audits; Luna performs bounded high-volume exploration/implementation; Terra handles complex/high-risk implementation and serial integration;
- NeuralMatrix's active authority documents, completion checklist, proof tiers, no-fallback doctrine, exact-head rule, CI semantics, and worktree discipline are imported as profile policy rather than duplicated or weakened;
- state survives daemon or App Server restart;
- external writes stop at explicit human approval; v1 creates at most a draft PR and never auto-merges.

The recommended v1 is deliberately local and single-user:

```text
Browser/PWA on 127.0.0.1
        │ REST + durable SSE
        ▼
     harnessd (Rust)
        ├── Codex App Server child over JSONL stdio
        ├── deterministic orchestrator and policy engine
        ├── Git/worktree/process/validation managers
        ├── SQLite/WAL state + raw event journal
        └── content-addressed artifact store
```

Do **not** begin with a cloud service, Kubernetes, a general provider abstraction, multi-user authentication, a vector database, or twenty autonomous agents. Those multiply failure domains before the core custody model is proven.

---

## 1. Product definition

### 1.1 What Harness Console is

Harness Console is a durable local application that accepts an engineering objective scoped to a registered repository and exact base, turns it into a reviewed task graph, dispatches bounded Codex agents, records their runtime activity, verifies their changes, serially integrates accepted commits, and produces a reviewable exact-head result plus evidence.

It combines four product categories:

1. **Codex runtime console** — threads, turns, models, effort, plans, actions, approvals, diffs, reviews, goals, usage, and subagents.
2. **Engineering orchestrator** — task DAG, roles, leases, retries, escalation, integration, and stop conditions.
3. **Repository custody system** — exact base SHA, worktrees, branches, commits, path ownership, diff verification, and publication gates.
4. **Evidence ledger** — commands, proof tiers, result semantics, artifacts, findings, exact SHA, unproved claims, and estimated cost.

### 1.2 What it is not

Harness Console is not:

- a replacement for Codex's own sandbox, approvals, or agent loop;
- a second NeuralMatrix architecture or completion authority;
- a hidden chain-of-thought viewer;
- a general IDE or terminal multiplexer;
- a continuous background coding service that pushes changes without review;
- a production deployment controller;
- a fleet/hardware lab automation platform in v1;
- a mechanism for turning unavailable tools or hardware into green proof;
- a compatibility or fallback layer for agent failures.

### 1.3 Primary user workflow

```text
1. Select NeuralMatrix and state the objective.
2. Controller fetches and pins origin/main to an exact SHA.
3. Sol xhigh inspects active authority and emits a schema-valid task graph.
4. User reviews task boundaries, paths, models, budgets, tests, and proof limits.
5. Controller leases non-overlapping paths and creates task worktrees.
6. Luna/Terra workers implement bounded tasks; read-only native subagents assist.
7. Controller runs focused validations and captures evidence.
8. Fresh Sol verifier attempts to reject each task.
9. Terra integrates verified commits in dependency order.
10. Integration proof reruns; invalidated evidence is explicit.
11. Fresh Sol max final audit attempts to reject the complete result.
12. User reviews diff/evidence/cost and explicitly approves push/draft PR.
```

### 1.4 v1 success criteria

V1 is successful when it can run the NeuralMatrix pilot ladder and reliably answer, at any time:

- What is the exact objective and current phase?
- Which agent and model/effort is working on what?
- Which worktree, branch, base SHA, and head SHA does it own?
- What is it doing now, and when was its last heartbeat?
- Which subagents exist, what roles/models do they use, and where did their tokens go?
- What files and paths are leased, serial, forbidden, or unexpectedly changed?
- What commands ran, on what exact SHA, with what result class and artifact?
- What was verified independently and what remains unproved?
- What did the run cost at the configured API-equivalent rates?
- What approval is blocking progress?
- Can the daemon restart without losing or falsely advancing state?

---

## 2. Design basis and constraints

### 2.1 Codex runtime basis

The product is built on Codex App Server because the required GUI state is already represented as runtime primitives: threads, turns, settings, goals, plans, items, commands, file changes, diffs, approvals, reviews, token usage, model reroutes, and collaborative/subagent events. The server supports generated protocol schemas, so the correct engineering posture is to pin a release and adapt against its schema rather than scrape terminal output.

Use App Server's default JSONL stdio transport. Do not expose its experimental WebSocket listener. `harnessd` is the only client and owns the process lifetime.

### 2.2 NeuralMatrix basis

The NeuralMatrix profile must enforce the repository's current rules:

- start from active authority, not archived plans;
- keep the primary checkout clean and create worktrees from a freshly fetched exact base;
- one canonical producer/semantic owner and fail-closed behavior;
- no compatibility alias, accept-both decoder, broad normalization, stale/latest/raw-id/URL/binding/client repair, dual authority, or semantic fallback;
- exact-head evidence and result classes that distinguish source failure, unavailable infrastructure, and inconclusive proof;
- bounded mutable workers plus an independent verifier;
- no worker self-approval or automatic completion-checklist claim;
- source, integration, hardware/live, rollout, and default-on proof remain distinct.

Harness runtime state is disposable execution state relative to repository authority. It may export evidence and proposed checklist updates, but it must never silently become the completion ledger.

### 2.3 Repository scale

NeuralMatrix is a large multi-surface repository with multiple Rust workspaces, platform-specific build paths, clients, C2, edge, infrastructure, CI, and extensive documentation. The context engine must route and cache; it cannot pass the entire repository to every agent or assume one root `cargo test` establishes product truth.

### 2.4 Reasoning visibility

The UI should show:

- requested and effective model;
- requested and effective reasoning effort;
- current goal and goal budget;
- plan steps and status;
- concise reasoning summaries emitted by the runtime;
- commands, file/tool activity, subagent lifecycle, findings, and evidence;
- context packet and compaction history.

It should not promise or retain private hidden chain-of-thought. Raw reasoning storage is off by default. This is both the honest product contract and the lower-risk storage posture.

---

## 3. System architecture

### 3.1 Context diagram

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ Local Linux user                                                            │
│                                                                             │
│  Browser/PWA ───────────── REST/SSE ───────────────┐                        │
│  harnessctl ─────────────── local HTTP/Unix IPC ───┤                        │
│                                                    ▼                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ harnessd                                                               │  │
│  │                                                                         │  │
│  │ API/UI  Orchestrator  Scheduler  Policy  Usage  Evidence  Recovery     │  │
│  │   │          │           │        │       │       │          │          │  │
│  │   ├──────────┴───────────┴────────┴───────┴───────┴──────────┤          │  │
│  │   │                   durable domain event bus                │          │  │
│  │   └─────────────┬─────────────────┬───────────────────────────┘          │  │
│  │                 │                 │                                      │  │
│  │       JSONL stdio│                 │ Git/process/file APIs                │  │
│  │                 ▼                 ▼                                      │  │
│  │      codex app-server        Worktrees / validators / helpers            │  │
│  │                 │                                                        │  │
│  │                 ▼                                                        │  │
│  │           OpenAI/Codex auth                                               │  │
│  └───────────────┬─────────────────────────────┬───────────────────────────┘  │
│                  │                             │                              │
│          SQLite/WAL + artifacts       NeuralMatrix primary repo/object DB    │
│                                                                             │
│  Optional controlled external edges: origin/GitHub, container engine,       │
│  self-hosted CI, Jetson/C2/hardware validation targets                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component map

| Component | Responsibility | Must not own |
|---|---|---|
| `harness-api` | local REST/SSE, session/CSRF, DTOs | orchestration decisions |
| `harness-domain` | state machines, IDs, result classes, domain events | I/O |
| `harness-codex` | App Server process/protocol, raw events, requests | task truth or Git |
| `harness-store` | SQLite migrations/repositories, event journal, artifacts | business policy |
| `harness-profile` | repository policy, domain/risk/validator routing | runtime mutable state |
| `harness-git` | locks, fetch, worktrees, leases, diff, commit, integration | semantic conflict decisions |
| `harness-runner` | controller-owned commands, resources, logs, cancellation | model turns |
| `harness-context` | authority routing, repo map, context packet, probe helper | architecture authority |
| `harness-orchestrator` | run/task state, scheduler, retries, escalation | direct protocol/Git shell details |
| `harness-evidence` | validation, claims, artifact manifests, export | release promotion decision |
| `harness-usage` | token deltas, price snapshots, cost/budgets | billing claims beyond evidence |
| `harnessd` | composition root and lifecycle | domain logic embedded ad hoc |
| `harnessctl` | operator commands over the same API/domain services | an independent hidden control path |
| `ui` | observability and explicit human controls | source-of-truth state |

### 3.3 Proposed source repository layout

```text
harness-console/
├── Cargo.toml
├── rust-toolchain.toml
├── crates/
│   ├── harness-domain/
│   ├── harness-store/
│   ├── harness-codex/
│   ├── harness-profile/
│   ├── harness-git/
│   ├── harness-runner/
│   ├── harness-context/
│   ├── harness-orchestrator/
│   ├── harness-evidence/
│   ├── harness-usage/
│   └── harness-api/
├── bins/
│   ├── harnessd/
│   ├── harnessctl/
│   ├── harness-probe/
│   └── fake-app-server/
├── ui/
│   ├── src/
│   ├── tests/
│   └── package.json
├── migrations/
├── schemas/
├── openapi/
├── profiles/
│   └── neuralmatrix/
├── codex/
│   ├── agents/
│   └── skills/
├── packaging/
│   └── systemd/
├── fixtures/
│   ├── app-server-traces/
│   └── git-repositories/
├── docs/
├── adrs/
└── xtask/
```

Keep crate boundaries real and small enough to test, but do not create forwarding crates or an abstract provider layer merely for symmetry.

---

## 4. Runtime and protocol architecture

### 4.1 App Server supervisor

`AppServerSupervisor` owns one child process and exposes a typed asynchronous client. It performs:

1. resolve and execute the configured `codex` binary;
2. capture `--version` and compare to `required_version`;
3. start `codex app-server --listen stdio://` or the exact equivalent for the pinned release;
4. send initialize and await success;
5. verify generated protocol-schema digest;
6. start stdout frame reader, stdin request writer, stderr collector, and child watcher;
7. publish runtime readiness;
8. restart only under bounded compatible failure policy.

Recommended Rust interface:

```rust
#[async_trait]
pub trait CodexRuntime: Send + Sync {
    async fn status(&self) -> RuntimeStatus;
    async fn start_thread(&self, request: StartThread) -> Result<ThreadHandle>;
    async fn resume_thread(&self, id: ThreadId) -> Result<ThreadHandle>;
    async fn start_turn(&self, request: StartTurn) -> Result<TurnHandle>;
    async fn steer_turn(&self, turn: TurnId, message: String) -> Result<()>;
    async fn interrupt_turn(&self, turn: TurnId) -> Result<()>;
    async fn set_goal(&self, thread: ThreadId, goal: GoalSpec) -> Result<()>;
    async fn decide_approval(&self, request: ApprovalDecision) -> Result<()>;
    async fn start_review(&self, request: ReviewRequest) -> Result<ReviewHandle>;
}
```

The exact wire types are generated from the pinned schema; domain types remain stable and explicitly converted.

### 4.2 Raw-first event pipeline

Every inbound notification is persisted before projection:

```text
read JSONL frame
  -> envelope and size validation
  -> append raw_events + payload hash
  -> commit and assign durable cursor
  -> typed projectors
  -> update relational state/aggregates
  -> publish domain event through SSE
```

Why raw-first:

- daemon restart can replay state;
- App Server additive fields are not discarded;
- projector bugs are repairable;
- the protocol/debug view can explain runtime behavior;
- usage and approval races can be audited.

This is “event sourcing lite,” not a requirement that every human-entered setting be reconstructed from events. Normal relational tables remain authoritative for controller decisions.

### 4.3 Backpressure

- protocol reader writes bounded frames to a single ordered ingestion channel;
- raw event append is the acknowledgement boundary;
- UI subscribers never block protocol ingestion;
- SSE uses per-client bounded queues and disconnects lagging clients with a resumable cursor;
- command output streams use bounded previews and spill full output to artifacts;
- large file diffs are summarized and fetched on demand.

### 4.4 Protocol compatibility

Each Harness release records:

```text
harness_version
codex_required_version
app_server_schema_sha256
adapter_trace_fixture_version
```

On mismatch, the UI and historical database remain available but new execution is disabled. There is no silent terminal-scraping fallback.

### 4.5 Thread and turn ownership

- one controller `agent_session` maps to one primary Codex thread;
- each task attempt starts a fresh primary thread by default;
- resumed threads retain their task/attempt identity;
- native subagents appear as child sessions linked to the parent thread/task;
- an active turn is not the same as an active command; both are tracked;
- `thread/goal/set` initializes the objective/token budget and updates on material remediation or phase change.

### 4.6 Activity projection

Stable internal item types:

```rust
pub enum ActivityKind {
    AgentMessage,
    ReasoningSummary,
    Plan,
    Command,
    FileRead,
    FileChange,
    Search,
    ToolCall,
    Subagent,
    ReviewFinding,
    ContextCompaction,
    Approval,
    Usage,
    Unknown,
}
```

Every item retains raw event linkage and start/complete timestamps. The UI can show a concise summary while the protocol tab exposes sanitized payload metadata.

---

## 5. Durable state model

### 5.1 Core entities

```text
Repository
  └─ Run
      ├─ Worktree (inspection, task, verifier, integration)
      ├─ Task
      │   └─ Attempt
      │       ├─ PathLease
      │       ├─ AgentSession
      │       │   └─ child AgentSessions
      │       ├─ CodexThread
      │       │   └─ Turns / Activity / Usage
      │       ├─ CommandRuns
      │       ├─ Validations / Evidence
      │       └─ Findings / Handoff
      ├─ Integration
      ├─ Approvals
      ├─ Pricing snapshots / Cost entries
      └─ Export / Publication record
```

The supplied migration is the starting relational design. Add optimistic `version` columns and projector checkpoints before exposing mutation APIs.

### 5.2 Run state machine

```text
CREATED
  -> PREPARING
  -> READY_FOR_ARCHITECTURE
  -> ARCHITECTING
  -> PLAN_REVIEW_REQUIRED
  -> READY_TO_EXECUTE
  -> EXECUTING
  -> TASK_VERIFICATION
  -> INTEGRATION_READY
  -> INTEGRATING
  -> INTEGRATION_VERIFICATION
  -> FINAL_AUDIT
  -> HUMAN_REVIEW
  -> PUBLICATION_READY
  -> DRAFT_PR_CREATED
  -> COMPLETED
```

Cross-cutting terminal/suspension states:

```text
PAUSED
BLOCKED
STOPPING
CANCELED
FAILED
ARCHIVED
```

No state is inferred from UI counts. The controller executes an explicit transition command in a transaction, validates preconditions, and emits a domain event.

### 5.3 Task state machine

Align with NeuralMatrix's existing orchestration vocabulary:

```text
PROPOSED
  -> READY
  -> LEASED
  -> STARTING
  -> IMPLEMENTING
  -> REVIEW_READY
  -> VERIFYING
  -> CHANGES_REQUESTED | VERIFIED
  -> INTEGRATION_QUEUED
  -> INTEGRATED
  -> CI_PROVEN
  -> LIVE_PROVEN
  -> CLOSED
```

Failure/suspension:

```text
WAITING_DEPENDENCY
WAITING_RESOURCE
WAITING_APPROVAL
BLOCKED
NEEDS_HELP
STALLED
INTERRUPTED
FAILED
SUPERSEDED
CANCELED
```

A task attempt never overwrites prior attempt evidence. Retry creates a new attempt and usually a new worktree/thread.

### 5.4 State invariants

- `IMPLEMENTING` requires task packet, exact base SHA, live worktree, write lease, and primary thread;
- only one mutable attempt per task is active;
- overlapping leases cannot both be active;
- worker output can reach `REVIEW_READY`, never `VERIFIED`;
- `VERIFIED` requires an independent verifier and controller validation;
- `INTEGRATED` requires the verified commit to exist in the integration history in dependency order;
- evidence binds to exact source SHA and is invalidated after relevant source/artifact changes;
- `PUBLICATION_READY` requires accepted final audit, no unresolved required findings/approvals, and current integration proof;
- `COMPLETED` is a Harness run state, not a NeuralMatrix checklist completion claim.

### 5.5 Concurrency and transactions

Use SQLite transactions plus process-local keyed locks:

- repository lock for fetch/worktree/ref operations;
- run lock for phase transitions;
- task lock for attempt/lease/agent commands;
- approval lock for one decision;
- artifact lock by digest only when necessary.

Every API mutation uses an `If-Match`/version precondition so two UI tabs cannot make contradictory approvals or retries.

---

## 6. Deterministic orchestration

### 6.1 Division of responsibility

| Concern | Controller code | Model |
|---|---|---|
| exact base/ref/worktree/branch | owns | receives |
| active authority selection | validates/imports | interprets and cites |
| task graph | schema/risk/path/DAG validation | Sol proposes |
| task objective/acceptance | freezes per attempt | follows; may report conflict |
| scheduling and concurrency | owns | none |
| path leases/serial paths | owns/enforces | obeys |
| implementation | observes/bounds | Luna/Terra performs |
| commands | records/limits; some controller-owned | requests/runs through Codex sandbox |
| validation classification | owns | may recommend |
| independent review | starts/freshens | Sol performs |
| integration order | owns | Terra resolves approved semantic work |
| completion/evidence | owns typed state | supplies structured handoff/findings |
| push/PR | owns after human approval | never improvises |
| merge | absent in v1 | none |

A model sentence such as “all tests pass” is not a state transition. The command and result must exist in the controller evidence ledger.

### 6.2 Architecture phase

Sol xhigh runs read-only in the inspection worktree with:

- user objective;
- exact base SHA and repository profile;
- instruction/authority router and digests;
- bounded repository map and code-navigation seeds;
- task schema and risk vocabulary;
- explicit instruction not to edit or claim completion.

It emits `nm.orchestration.task.v1[]` plus a run-level plan summary. The controller rejects:

- cycles;
- missing authority or checklist mapping where required;
- ambiguous base SHA;
- overlapping mutable paths without serial ownership;
- serial/forbidden paths assigned to ordinary workers;
- missing positive/negative proof;
- tasks too broad for configured diff/token budgets;
- implicit compatibility/fallback/normalization/repair;
- a proof claim beyond the requested validators/environments;
- unsupported model/effort/sandbox.

The user sees and may edit objective text, non-goals, budgets, and dispatch choices. Changes produce a new plan revision/digest; the original is retained.

### 6.3 Risk router

Do not blindly send every task to Luna and escalate only after damage. Route by explicit risk:

```text
Luna worker directly
  narrow isolated implementation
  tests/fixtures owned by the same component
  mechanical but semantically explicit client/UI change
  bounded docs or tooling outside serial authority

Terra directly
  canonical producer/consumer contract
  generated cross-language contract
  migration or persistence identity
  authentication/authorization/tenancy/privacy
  unsafe/native/codec/accelerator lifecycle
  required CI context/classifier
  OTA/release/signing/provenance
  cross-domain refactor or serial shared path

Sol
  architecture/decomposition
  independent task verification
  final integrated audit
```

Risk classification combines task-declared flags, path profile, authority documents, changed symbol metadata, and human override. High-risk routing is conservative; unknown paths broaden rather than downgrade.

### 6.4 Execution waves

The scheduler computes waves only among tasks that are:

- dependencies satisfied at the expected dependency SHAs;
- leased without overlap;
- within total/mutable/verifier limits;
- compatible with resource-class availability;
- not blocked on approval/user decision;
- based on a still-valid plan revision and base lineage.

Default NeuralMatrix capacity:

```text
max live Codex threads          6
max mutable task parents        3
max independent verifier        1
max read-only discovery slots   2, but total still <= 6
integration/live mutable        1
```

Native subagents count against the total thread budget. A worker cannot create unbounded children merely because the controller launched only three parents.

### 6.5 Scheduler algorithm

```rust
loop {
    reconcile_runtime_and_worktrees().await?;
    expire_or_warn_leases(now).await?;

    if scheduler_paused() {
        wait_for_event().await;
        continue;
    }

    let candidates = ready_tasks_ordered_by(
        priority,
        dependency_depth,
        age,
        risk,
        resource_wait_time,
    );

    for task in candidates {
        if !total_slot_available(task) { continue; }
        if !resource_available(task) { mark_waiting_resource(task); continue; }
        if !leases_available(task) { mark_waiting_dependency_or_lease(task); continue; }
        if !base_and_authority_still_valid(task) { block_for_replan(task); continue; }

        acquire_leases_transactionally(task)?;
        create_or_reconcile_worktree(task)?;
        start_agent_session(task).await?;
    }

    wait_for_domain_event_or_timer().await;
}
```

Fairness prevents a sequence of medium tasks from indefinitely starving a heavy task, but heavy/hardware jobs remain serial when their resource class is exclusive.

### 6.6 Heartbeat and watchdog

A session heartbeat advances from App Server events, active command updates, or explicit goal progress. The lease TTL is longer than the heartbeat interval.

First timeout:

- mark warning;
- steer the agent with a concrete request for patch, blocker, or `needs_help` handoff;
- do not release paths.

Second timeout:

- interrupt;
- reconcile commands/processes/diff;
- preserve worktree;
- close attempt as `STALLED`;
- only then release lease;
- start a new attempt with prior evidence or escalate.

### 6.7 Retry and remediation

A retry is not “same prompt again.” It includes:

- prior task packet/digest;
- failed command and artifacts;
- verifier findings;
- exact partial diff/head;
- explicit revised objective or correction set;
- unchanged protected semantics;
- a new token/tool budget and attempt ID;
- chosen model route.

Automatic remediation is capped, recommended at two rounds. After that, require human review or architecture rejection.

### 6.8 Stop conditions

Agents must return a blocker rather than improvise when:

- active authorities conflict;
- a required producer contract is not frozen;
- a required path is leased/serial/forbidden;
- implementation would require compatibility, normalization, repair, dual authority, fallback, or weakened proof;
- necessary target/hardware/credential is unavailable;
- task scope exceeds diff/token/tool budget materially;
- dependency head differs from the task packet;
- a migration or generated artifact requires an unassigned serial task;
- the observed repository contradicts the plan enough to invalidate decomposition.

The controller renders the stop condition and routes it to user, architect, or integrator.

---

## 7. Model and reasoning-effort policy

### 7.1 Default roles

| Role | Model | Effort | Sandbox | Purpose |
|---|---|---|---|---|
| architect | `gpt-5.6-sol` | `xhigh` | read-only | authority map, invariants, task graph |
| explorer | `gpt-5.6-luna` | `medium` | read-only | bounded source/test/contract discovery |
| normal worker | `gpt-5.6-luna` | `max` | workspace-write | narrow implementation and focused proof |
| CI triage | `gpt-5.6-luna` | `high` | read-only | classify logs/result semantics |
| high-risk worker | `gpt-5.6-terra` | `xhigh` | workspace-write | contracts, persistence, security, native, cross-domain |
| integrator | `gpt-5.6-terra` | `xhigh` | workspace-write | serial integration/conflict/evidence invalidation |
| verifier | `gpt-5.6-sol` | `xhigh` | read-only | adversarial task review |
| final auditor | `gpt-5.6-sol` | `max` | read-only | integrated system rejection attempt |

These are policy defaults, not hard-coded universal truth. Store them in the repository profile and show every override.

### 7.2 Why this split

- Sol is spent where global correctness and adversarial review have the highest leverage.
- Terra receives tasks whose semantics span components or whose failure could create subtle compatibility/security/persistence debt.
- Luna handles many bounded tasks economically, but uses a high effort for implementation because the scope has already been constrained.
- Read-only exploration uses lower effort and compact outputs to reduce repeated expensive repo rediscovery.

### 7.3 Model reroutes

Record both requested and effective model/effort from runtime settings/events. Cost and audit use effective values. The UI displays reroutes prominently; the controller must not pretend a Luna task remained Luna if runtime routed it to Terra or Sol.

### 7.4 Budget defaults

Start with task budgets, then tune from pilots:

```text
architect               120k tokens
read-only explorer       30k tokens
Luna bounded worker      80k tokens
Terra high-risk worker  140k tokens
verifier                 80k tokens
integrator              160k tokens
final auditor           120k tokens
```

The goal API carries the active budget where supported. The controller ledger remains authoritative across turns and subagents.

### 7.5 Budget behavior

- 70%: UI warning and agent receives remaining-budget reminder.
- 90%: ask agent to converge on required proof/handoff.
- 100%: no new turn without approved increase or a new attempt.
- running command may finish under command timeout.
- budget pressure never authorizes hidden scope reduction or weak tests.

---

## 8. Subagent architecture

### 8.1 Two kinds of “agent”

Harness Console must distinguish:

1. **Controller-created primary agents** — top-level Codex threads, each assigned a task attempt and explicit worktree/sandbox/lease.
2. **Codex-native child subagents** — spawned by a parent thread, inheriting its runtime/workspace boundary and represented by collaborative-agent events.

This distinction is central to correctness.

### 8.2 Mutable parallelism

Parallel write-owning work uses controller-created primary agents because only the controller can guarantee separate worktrees and non-overlapping leases. Do not treat native child agents as separate Git-isolated workers.

### 8.3 Safe child use cases

- locate producer/consumers/tests;
- compare authority docs to implementation;
- inspect command or CI logs;
- formulate negative cases;
- review a bounded diff read-only;
- research one independent code path;
- summarize a large test failure artifact.

The parent remains responsible for edits and final handoff.

### 8.4 Child policy

- counts against global thread maximum;
- inherits parent task/worktree and cannot broaden sandbox/network;
- default role `explorer`/`ci_triage`/read-only reviewer;
- no controller path lease of its own in v1;
- token/cost attributed both to child and parent task;
- visible nickname, role, requested/effective model/effort, current status;
- child failure does not automatically fail the task; parent/controller classifies impact;
- runaway child spawning is denied by configured depth/count.

Recommended:

```text
max child depth per primary   2
max live children per parent  2
max children created per task 6
```

### 8.5 Fresh-review contexts

Independent verification and final audit should be new controller-created read-only sessions, not child agents of the implementer. This avoids inherited anchoring and makes independence visible in state/evidence.

---

## 9. Git and worktree custody

### 9.1 Registered repository model

V1 registers an existing local NeuralMatrix clone as the **coordination repository**. It remains on `main`, clean, and synchronized with `origin/main` per repository policy. Harness uses its shared Git object database to create managed worktrees under the XDG data root.

This avoids duplicating a very large repository while preserving the existing rule that the primary checkout is for coordination/inspection only.

A managed bare mirror can be added later for users who do not want Harness attached to their normal clone. It is not required for v1.

### 9.2 Run preparation

Under repository lock:

```bash
git status --porcelain=v2
git branch --show-current
git fetch --prune origin
BASE_SHA="$(git rev-parse --verify origin/main^{commit})"
git worktree add --detach <inspection-path> "$BASE_SHA"
```

Persist the requested ref, exact SHA, fetch time, remote URL, primary HEAD/status, and authority digest. Never silently move a running run when `origin/main` advances.

### 9.3 Worktree kinds

| Kind | Branch | Mutable | Purpose |
|---|---|---:|---|
| inspection | detached base | no | architecture/context/search |
| task attempt | `agent/hc/<run>/<task>-a<n>` | yes | one worker task |
| verifier | detached task head or integration head | no | independent audit |
| integration | `agent/hc/<run>/integration` | yes, serial | accepted commits/generation/conflict resolution |

### 9.4 Branch management

The controller alone runs:

- branch/worktree creation;
- base resolution;
- commit creation;
- rebase/cherry-pick/merge conflict operations;
- push;
- PR creation.

Agents may inspect Git and request actions, but their role prompt explicitly prohibits branch/push/PR management.

### 9.5 Path leases

A lease contains:

```text
run_id, task_id, attempt, agent_session
base_sha
exact files or normalized directory prefixes/globs
lease kind: read/write/serial
acquired, heartbeat, expiry, released
```

Before dispatch, normalize globs to an overlap representation. On completion, compare actual changed paths to lease/serial/forbidden policy. Rename/delete destinations and symlinks are resolved carefully; a path escape is a critical failure.

### 9.6 Serial paths

Initial NeuralMatrix serial set includes:

- architecture spine and master completion checklist;
- generated shared contracts;
- migrations;
- Cargo lockfiles;
- GitHub workflows and CI claim/workflow registries;
- root agent instructions;
- any additional profile-declared public producer descriptor.

A normal worker may discover a needed serial change and return it as a dependency. Only a dedicated serial task or integrator receives the lease.

### 9.7 Diff and commit acceptance

Before a worker handoff can be reviewed:

```bash
git status --porcelain=v2
git diff --check <base-sha>...HEAD
git diff --name-status --find-renames <base-sha>...HEAD
git diff --binary --find-renames <base-sha>...HEAD
```

Controller checks:

- only leased paths changed;
- no forbidden runtime artifacts;
- no unexpected submodule/binary/vendor files;
- diff budget not exceeded without approved revision;
- generated paths changed only by owner;
- user Git identity is configured;
- no AI author/co-author/committer attribution;
- commit parent lineage is expected;
- command/evidence references match the committed SHA.

### 9.8 Integration

Terra integrator receives verified task commits and the integration worktree. The controller applies commits in DAG order. Mechanical clean cherry-picks can be controller-driven; semantic conflicts stop and go to Terra with authority context.

Conflict resolution invalidates any proof that depended on changed lines/artifacts. Regeneration can invalidate both producer and consumer proof. The UI shows this explicitly and queues reruns.

### 9.9 Cleanup

No worktree is removed when it has:

- a live process/session;
- active lease;
- unarchived uncommitted diff;
- unpushed commit the user has not declared disposable;
- preservation flag;
- unresolved evidence reference.

Cleanup is controller-owned, dry-run capable, and followed by `git worktree prune`. Global Docker/cache pruning is never implicit.

---

## 10. Context engineering and high-bandwidth operations

### 10.1 Objective

The context engine should prevent every agent from repeatedly rediscovering a 2.7 GB repository. It supplies the smallest authority-first packet that is likely to be sufficient and makes that packet inspectable.

### 10.2 Context layers

1. **Permanent repository policy**
   - `AGENTS.md`, `CODEX.md`, documentation authority rules, protected semantics.
2. **Repository map**
   - path domains, workspaces/packages, deployables, generated/serial paths, code-navigation seeds, CI claims/validators.
3. **Task packet**
   - objective, non-goals, exact base/dependencies, owned paths, success/negative tests, metrics, evidence, stop conditions.
4. **Selected source context**
   - active authorities, producer/consumer definitions, relevant tests, interfaces, recent failure evidence.
5. **Execution evidence**
   - commands, logs, findings, partial diff, prior attempt limits.

Do not continuously append all layers and all prior turns into one unbounded conversation. Use fresh verifier/integrator threads and compact task-specific handoffs.

### 10.3 Repository map v1

Build deterministic indexes from an exact SHA:

- `git ls-files` inventory, path/language/size/last-change metadata;
- active documentation links and status/classification from routers;
- excluded archive/vendor/binary/generated classes;
- `cargo metadata` for each detected workspace, packages, targets, features, dependencies;
- known component/profile/validator catalog;
- NeuralMatrix CI claim/workflow/fixture/quarantine registries;
- test file and named command catalog;
- symbol/search seeds extracted from profile and authority docs;
- FTS5 index for bounded text files and metadata, with `rg` as authoritative on-demand search.

Do not add embeddings/vector infrastructure in v1. Exact keywords, paths, symbols, Git metadata, active routers, and model-guided `rg` are adequate and easier to debug. Revisit only after measured misses.

### 10.4 Authority resolution

For a task:

1. map owned/read paths to profile domains;
2. load global instruction/authority chain;
3. add domain authority hints;
4. follow canonical index links when needed;
5. include relevant checklist rows and CI claim mappings;
6. exclude archived/supporting documents unless an active authority promotes or task explicitly needs historical evidence;
7. hash every included source and store context manifest.

The model may request an additional authority, but the controller records why and ensures it is not silently treated as higher authority.

### 10.5 Context packet

Example:

```json
{
  "schema": "harness.context.packet.v1",
  "run_id": "NM-20260805-014",
  "task_id": "MEDIA-002",
  "base_sha": "...",
  "profile_digest": "sha256:...",
  "instruction_sources": [
    {"path":"AGENTS.md","sha256":"...","class":"repository_policy"}
  ],
  "authorities": [
    {"path":"docs/architecture/PRODUCT_EVIDENCE_CONTRACT.md","sha256":"..."}
  ],
  "checklist_rows": ["MEDIA-..."],
  "owned_paths": ["central/rust-c2/..."],
  "code_seeds": [
    {"path":"...","symbols":["..."]}
  ],
  "test_seeds": ["cargo test -p ..."],
  "dependency_contracts": [],
  "prior_evidence": [],
  "excluded": [
    {"pattern":"docs/archive/**","reason":"historical, non-authoritative by default"}
  ],
  "estimated_tokens": 42000
}
```

The UI shows this manifest; prompts need not hide how context was selected.

### 10.6 High-bandwidth helper

Implement `harness-probe`, a safe CLI available inside the agent worktree:

```text
harness-probe search --query ... --paths ... --max-results 200
harness-probe read-many --manifest paths.json --max-total-bytes ...
harness-probe cargo-map --affected <path>...
harness-probe test-select --task-packet ...
harness-probe summarize-log --artifact <id> --focus ...
harness-probe context-show --task <id>
```

It performs multiple local operations in one command, returns compact structured output, and stores the full result as an artifact. This is the concrete higher-bandwidth harness improvement: the agent can batch a search/read/metadata investigation instead of spending a model turn on every file command.

Controls:

- read-only unless the subcommand explicitly represents a controller-owned action;
- path scoped to the worktree/repository;
- output byte/result count limit;
- no arbitrary network;
- command and artifact recorded;
- no secret-bearing environment dump;
- deterministic JSON output option.

A future stable dynamic-tool/MCP integration may replace selected shell calls, but v1 should not depend on experimental protocol extensions when a normal audited executable suffices.

### 10.7 Prompt/cache discipline

- keep stable policy and role instructions in a consistent prefix/order;
- put volatile task/evidence later;
- avoid injecting full logs; store and summarize;
- include exact excerpts/symbols, not whole giant files unless required;
- start fresh threads for independent review rather than dragging implementation history;
- track context compaction and correlate it with quality/corrections;
- display context size and long-context price multiplier risk before dispatch.

---

## 11. Validation and evidence architecture

### 11.1 Separate agent completion from proof

A worker handoff says what was changed and which commands it attempted. The controller verifies:

- worktree/diff/commit custody;
- command artifacts and exact SHA;
- task success criteria;
- required positive and negative tests;
- path/profile validators;
- result classification;
- independent review.

A task can be `REVIEW_READY` with passing focused tests but still fail verification for hidden fallback, wrong authority, missing negative proof, or claims beyond the tier.

### 11.2 Proof tiers

Use NeuralMatrix's T0–T6 definitions in the profile:

| Tier | Harness interpretation |
|---|---|
| T0 | deterministic algorithm/state/repository topology proof |
| T1 | exact canonical shape and negative rejection |
| T2 | component API with real local dependencies |
| T3 | property/fuzz/parser/state breadth |
| T4 | ordering/cancellation/backpressure/fault behavior |
| T5 | named OS/SDK/codec/accelerator/database/hardware target |
| T6 | exact-candidate live/capacity/recovery/rollout/fleet proof |

The UI never collapses these into one “tests passed” badge.

### 11.3 Result classes

```text
success
not_selected
source_failure
infrastructure_unavailable
inconclusive
cancelled_superseded
skipped_draft
quarantined_failure
```

Only `success`, and audited `not_selected` through the correct aggregate context, can satisfy a required check. Missing Docker, compiler, runner, model, fixture, camera, or hardware is `infrastructure_unavailable`, not success.

### 11.4 Validator catalog

Profiles define validators with:

```text
id
command template
path/domain/risk selector
proof tier
resource class
prerequisites
result parser
artifact rules
timeout
owner
```

The selector broadens on unknown paths. The controller records why a validator was selected or not selected.

### 11.5 Command evidence

Each command record includes:

```text
command and argv
cwd/worktree
source SHA before/after
start/end/duration
exit/signal/timeout
result class
resource class and host/runner/device
sanitized environment/profile/toolchain
stdout/stderr artifact digests
parsed test/benchmark report
claims proved and unproved
```

For commands run by Codex itself, App Server command events provide runtime visibility; Harness reconciles final evidence and may rerun required acceptance commands through its controller-owned runner to guarantee exact classification/artifact capture.

### 11.6 Task handoff

The supplied schema requires:

- task/attempt/base/head;
- touched paths;
- result summary;
- commands and artifacts;
- positive/negative tests;
- metrics changed;
- proof limits;
- checklist recommendation at a maximum justified state;
- next steps/errors.

The worker never directly updates the master checklist or labels the product complete.

### 11.7 Independent verifier

Sol verifier receives a fresh read-only context:

- task packet and authorities;
- exact task commit/diff;
- command evidence and proof limits;
- no implementer conversational transcript except structured handoff where useful.

Output schema:

```text
verdict: ACCEPT | REMEDIATE | REJECT_ARCHITECTURE
findings[]:
  severity
  invariant/authority violated
  exact file/line/symbol
  reasoning/reproduction
  required correction
  required test/evidence
```

The verifier's verdict is necessary but not sufficient: controller checks must also pass.

### 11.8 Integration evidence invalidation

Create explicit dependency links:

```text
Evidence -> source SHA / artifact digest / generated schema / model / migration
```

When integration changes one dependency, mark linked evidence stale and enqueue the minimum correct rerun. Examples:

- conflict resolution touches producer logic: producer and consumer T1/T2 stale;
- regenerated contract changes bytes: all consumer parity proof stale;
- model/engine manifest changes: relevant T5/T6 stale;
- rebase changes unrelated docs only: scoped code proof may remain valid, but exact-head merge proof still reruns.

### 11.9 Evidence bundle

Export a content-hashed bundle containing:

- run/task/attempt manifests;
- exact base/task/integration SHAs;
- profile/authority/context/schema/pricing digests;
- diffs/commits;
- commands/results/artifacts;
- findings/decisions;
- usage/cost ledger;
- claim/proof matrix;
- unproved claims;
- publication record.

Raw hidden reasoning and secrets are excluded by default. Bundle verification fails on any missing or modified artifact.

---

## 12. Usage, cost, and rate-limit telemetry

### 12.1 Token fields

The App Server protocol exposes input, cached input, cache-write input, output, reasoning-output, total, last-turn/total usage, and context window in the pinned source version. Store last-turn values or safe cumulative deltas.

Reasoning-output tokens are an output breakdown and must not be charged twice.

### 12.2 API-equivalent pricing

Configuration includes immutable effective-dated snapshots. The initial 2026-08-05 example is:

| Model | Input / 1M | Cached / 1M | Output / 1M |
|---|---:|---:|---:|
| Sol | $5.00 | $0.50 | $30.00 |
| Terra | $2.00 | $0.20 | $12.00 |
| Luna | $0.20 | $0.02 | $1.20 |

Cache writes are configured at 1.25× uncached input. Requests above the configured 272K input threshold receive the configured 2× input and 1.5× output multipliers. Price rules remain data, not code constants.

### 12.3 Formula

When cache-write input `W` is known:

```text
normal = max(input - cached - W, 0)
cost = normal*input_rate
     + cached*cached_rate
     + W*(input_rate*cache_write_multiplier)
     + output*output_rate
```

When `W` is unavailable, store and display a lower/upper range. Cost is calculated per turn/request before aggregation so long-context and model-reroute rules are applied correctly.

Use decimal or integer micro-dollars, never binary floating point for the authoritative ledger.

### 12.4 Subscription labeling

When Codex uses ChatGPT subscription authentication, the UI says **API-equivalent estimate**. It must not represent the estimate as the user's actual subscription bill. If rate-limit windows/rollout budget are exposed, show them separately from dollar estimates.

### 12.5 Dashboards

Aggregate by:

- model and reasoning effort;
- role;
- parent/child agent;
- task/attempt;
- run phase;
- accepted/failed/retried work;
- day/repository.

Useful metrics include cost per verified task and cost lost to failed/rejected attempts, but avoid optimizing for token minimization at the expense of correctness.

---

## 13. Security architecture

### 13.1 Threat model

Primary risks:

- an agent writes outside its intended task/worktree;
- prompt/repository content induces destructive/network/secret operations;
- App Server or daemon is exposed remotely without authentication;
- logs/events/artifacts retain credentials or customer data;
- a UI approval is replayed or applied to the wrong head/request;
- a process survives interruption and mutates after lease release;
- a stale or unavailable validation appears green;
- a compromised browser page issues local mutations;
- a Git push/PR uses a head different from the reviewed one.

### 13.2 Local-only boundary

- UI binds to `127.0.0.1` only;
- App Server uses stdio and has no network listener;
- remote use is SSH port forwarding;
- same-origin local session and CSRF token protect browser mutations;
- no CORS wildcard;
- no multi-user auth claims in v1.

### 13.3 Sandbox and network

Default by role:

```text
architect/explorer/verifier/final auditor: read-only, network disabled
worker/integrator: exact worktree writable, network disabled
```

Controller-owned `git fetch`, push, GitHub, package acquisition, or hardware operations are separate approved paths. An agent requesting network triggers approval/policy; it does not inherit daemon credentials wholesale.

### 13.4 Secrets

- rely on existing Codex authentication; do not store OpenAI API keys in Harness DB;
- do not expose full environment to agents or UI;
- allowlist variables per repository/toolchain;
- redact probable secrets in command previews and projected events;
- artifact files use 0600, directories 0700, service UMask 0077;
- never intentionally export credentials/customer data/camera URLs with userinfo;
- raw event access is local and retention-bounded.

### 13.5 Approval classes

| Risk | Examples | Default |
|---|---|---|
| low | read worktree, bounded search, controller-selected local test | trusted/automatic under policy |
| medium | network-enabled package fetch, new tool invocation, broader local command | explicit once |
| high | write outside task scope, push branch, create PR, destructive cleanup | individual explicit approval |
| critical | production deployment, live actuator/customer environment, credential change | unsupported or separate operator ceremony in v1 |

There is no global “approve all.” Decision binds exact request, thread/turn/item, worktree, and expected head where applicable.

### 13.6 Process custody

- launch subprocesses in process groups;
- keep PIDs and start identity;
- interrupt gracefully then kill within a bound;
- do not release write lease while a possibly mutating process lives;
- reconcile after daemon/App Server crash;
- uncertain process state blocks acceptance.

### 13.7 Browser safety

- escape/sanitize all agent/log/ANSI content;
- artifact downloads use safe MIME/content disposition;
- no arbitrary file URL rendering;
- no agent-triggered browser terminal;
- human terminal sessions are explicit and visually separate from agent activity;
- optimistic concurrency prevents stale approvals and publication.

---

## 14. Local API and UI architecture

### 14.1 API transport

- REST for resource queries and commands;
- Server-Sent Events for durable ordered state/activity updates;
- WebSocket only for a future/explicit human terminal byte stream;
- OpenAPI contract included in this blueprint;
- same API powers browser and `harnessctl` so CLI is not a bypass.

### 14.2 SSE semantics

Each event has a durable integer cursor. Client reconnects with `Last-Event-ID` or `cursor`. Server replays from raw/projected event store within retention; if too old, client reloads resource snapshots and resumes at current cursor.

Event classes:

```text
runtime.*
repository.*
run.*
task.*
agent.*
approval.*
worktree.*
command.*
validation.*
evidence.*
usage.*
publication.*
```

### 14.3 UI stack

- React + TypeScript + Vite;
- TanStack Router/Query;
- accessible Radix-style primitives;
- virtualized timeline/command lists;
- maintained diff renderer;
- xterm.js only for human terminal;
- embedded production assets in `harnessd`.

### 14.4 Core screens

1. Home/repository/run health.
2. New-run composer with exact base preparation.
3. Run list-and-inspector workspace.
4. Task graph optional view.
5. Diff/changes/integration view.
6. Approval center.
7. Evidence claim matrix/artifact browser.
8. Usage/cost dashboard.
9. Worktree manager.
10. Host/runtime/storage/runner health.
11. Settings/profile/pricing/retention.

The full wireframes and interaction contract are in `docs/UI_WIREFRAMES.md`.

### 14.5 Operational row design

Every task row shows, without opening it:

```text
status + task/title
role/model/effort/sandbox + child count
current goal/action
worktree/branch/head + diff summary
tokens/budget + API-equivalent cost + elapsed + heartbeat
approval/dependency/validation chips
```

This is more useful than a decorative agent graph. The graph is secondary.

### 14.6 “Thinking” UI contract

Label panels accurately:

- **Goal** — thread goal/objective/budget;
- **Plan** — explicit plan item states;
- **Reasoning summary** — concise runtime-provided summary;
- **Activity** — commands/files/tools/subagents;
- **Context** — sources supplied and compaction;
- **Review** — findings/verdict.

Never label an inferred animation or timer as “thoughts.”

---

## 15. NeuralMatrix repository profile

### 15.1 Profile purpose

The profile is a versioned policy adapter. It teaches the generic controller how NeuralMatrix routes authority, paths, proof, models, and resources. It does not copy the entire repository documentation into Harness or become an alternative contract.

The supplied `profiles/neuralmatrix/profile.toml` is the initial shape.

### 15.2 Required instruction/authority chain

Always load/hash:

```text
AGENTS.md
CODEX.md
docs/README.md
docs/INDEX_FOR_AGENTS.md
docs/architecture/CANONICAL_INDEX.md
docs/product/NEURALMATRIX_PRODUCT_CONTRACT.md
docs/architecture/PRODUCT_ARCHITECTURE_CONTRACT.md
docs/architecture/CANONICAL_AUTHORITY_DOCTRINE.md
docs/architecture/ENGINEERING_GOVERNANCE_CONTRACT.md
docs/architecture/CI_TEST_ARCHITECTURE.md
docs/architecture/MASTER_COMPLETION_CHECKLIST.md
```

Then add the surface-specific active contract(s). Archived plans/audits are supporting evidence only unless active authority explicitly promotes them.

### 15.3 Domain routing

Initial domains:

| Domain | Paths | Typical additional authority |
|---|---|---|
| edge | `edge/**` | edge streamer, resource/media/analytics/live contracts |
| C2 | `central/**` | C2, tenancy/RBAC, entitlement/security |
| contracts | `shared/**`, proto/schema | canonical authority, product evidence |
| dashboard | `dashboard/**`, `NewDesign/**` | product/tenancy/surface contracts |
| iOS | `ios-native/**` | iOS reference audit, evidence/sensor/playback |
| Apex | `apex-vms/**` | product/playback/evidence |
| Hyperwall | `hyperwall/**` | Hyperwall/media/runtime |
| CI/release | workflows/tools/infra/deploy | engineering governance, CI architecture, OTA/release |

Cross-domain task classification unions the authorities and raises risk.

### 15.4 Protected semantics injected into every mutable packet

```text
- one canonical producer/consumer semantic contract
- fail closed when canonical truth or required proof is missing
- no aliases, accept-both decoding, broad normalization, translation shims
- no stale/latest/raw-id/URL/binding/client repair
- no semantic/protocol fallback or dual authority
- operational resource adaptation only when active policy explicitly authorizes it
- exact-head evidence
- no AI commit attribution/co-author trailers
- no completion claim beyond executed proof
```

These statements are guardrails, not an instruction to refuse all operational adaptation. The task must read the active resource policy for authorized bounded degradation.

### 15.5 Model/risk examples

```text
Change isolated dashboard copy/layout
  -> Luna max worker + Sol verifier

Change C2 tenant-scoped data hydration
  -> Terra xhigh directly + Sol verifier

Change shared generated identity contract
  -> Terra xhigh producer task, serial integrator generation,
     consumer tasks after producer freeze, Sol verification

Fix focused Rust test in one crate
  -> Luna max; Terra escalation only with evidence

Change GitHub required context/classifier
  -> Terra xhigh + CI triage child + Sol verifier

Jetson NVDEC/ROI/native lifecycle
  -> Terra xhigh, explicit T5/T6 proof limits, hardware gate separate
```

### 15.6 Validator routing

Initial supplied validators:

- `git diff --check`;
- docs active-authority checks for docs changes;
- CI program self-tests for workflow/CI architecture changes;
- local AArch64 Jetson cross-build as heavy T5 with explicit prerequisites;
- local Debian 12 C2 build as heavy T5 with explicit prerequisites.

Extend from active component/CI claim registries. Do not create a feature-named workflow per Harness task; select existing credible commands/gates.

### 15.7 Completion checklist handling

- architect maps tasks to checklist rows when relevant;
- worker handoff may recommend a maximum justified state;
- verifier/controller validate evidence;
- Harness may propose a checklist patch as a serial task only when the user objective includes it;
- Harness never writes or changes a checklist state merely because its run reaches `COMPLETED`;
- T5/T6/live/rollout requirements remain explicit proof limits.

### 15.8 `.omx` and runtime state

Existing `.omx` generated prompts/inboxes are disposable runtime state and not authority. Harness runtime belongs outside the repo by default under XDG directories. Do not commit `.harness-runtime` or duplicate orchestrator state into NeuralMatrix.

### 15.9 Resource classes on the target workstation

Recommended defaults for the known high-core Linux host:

```text
control:  short Git/index/schema/static commands; concurrency 4–8
medium:   focused Rust/TypeScript tests/checks; concurrency 2–3
heavy:    link/container/workspace builds; concurrency 1
hardware: Jetson/fleet/device/live operation; exclusive/manual readiness
```

Agent slots and resource slots are separate. Three Luna workers may think concurrently while only one heavy build runs.

---

## 16. Linux process, packaging, and installation architecture

### 16.1 Daemon model

`harnessd` runs as an unprivileged user service:

```ini
[Service]
ExecStart=%h/.local/bin/harnessd serve
Restart=on-failure
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
```

The supplied unit is intentionally conservative. Add stronger `ProtectSystem`/`ReadWritePaths` only after installation knows the repository and XDG roots; do not break worktree/toolchain access with blanket hardening.

### 16.2 XDG storage

```text
config:    ~/.config/harness-console/
data:      ~/.local/share/harness-console/
cache:     ~/.cache/harness-console/
state/log: ~/.local/state/harness-console/
```

Worktrees/artifacts live in data unless the user selects a larger disk. Cache is disposable; DB/artifacts/config are durable.

### 16.3 One runtime package

Release tarball contains:

```text
harnessd
harnessctl
harness-probe
embedded UI
migrations/schemas/default profiles/default agent roles
systemd user unit
release manifest and checksums
operator docs
```

Node is build-time only. Rust binaries should target modern glibc builds for Fedora/Ubuntu packages or a suitable musl target when all dependencies permit. Test both rather than assuming full static compatibility.

### 16.4 Build system

Recommended commands:

```bash
cargo xtask ui-install --locked
cargo xtask ui-build
cargo test --workspace
cargo xtask openapi-check
cargo xtask schema-check
cargo xtask app-server-bindings-check
cargo xtask dist
```

`xtask` embeds UI assets, validates config/profile/schema examples, records the Codex compatibility tuple, and produces the release manifest.

### 16.5 Development environment

Use a `justfile` or `cargo xtask dev` to launch:

- Vite dev server;
- `harnessd` with UI proxy;
- fake App Server by default;
- optional live pinned App Server smoke;
- temporary SQLite/repo fixture.

Do not use a real NeuralMatrix worktree for most harness unit/UI development.

### 16.6 Browser/PWA vs Tauri

V1 uses a browser/PWA because it is easier to install, debug, secure locally, and operate remotely through SSH forwarding. Add Tauri later only for desktop affordances such as tray, native notifications, file pickers, and protocol registration. Keep the daemon/API architecture so Tauri remains a shell rather than a second controller.

### 16.7 Upgrade

- pause scheduling and active mutation;
- online backup DB/config/profile;
- install binary/assets;
- check migrations and Codex schema tuple;
- restart;
- replay adapter fixtures and reconcile sessions;
- resume.

Do not perform an unattended Codex CLI upgrade independently of Harness compatibility validation.

---

## 17. Database and artifact details

### 17.1 SQLite settings

Recommended connection configuration:

```sql
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA synchronous=NORMAL;       -- consider FULL for approval/publication txns
PRAGMA busy_timeout=5000;
PRAGMA temp_store=MEMORY;
```

Use a small write pool or serialized writer to preserve raw event order; read pool handles UI queries. Critical approval/publication transitions may explicitly checkpoint or use stronger synchronous semantics.

### 17.2 Missing schema additions before coding

Add to the supplied migration or a follow-up:

- `version` optimistic-concurrency columns on mutable resources;
- `projector_checkpoints`;
- `domain_events` or a stable outbound-event table separate from raw protocol events;
- `run_plan_revisions` and digests;
- `task_attempts` as a distinct table if tasks should remain stable across attempts;
- `findings` and finding disposition;
- `context_packets` and source manifests;
- `resource_leases` for CPU/heavy/hardware separate from path leases;
- `publications` for push/PR exact-head record;
- `operations` for long-running API commands/exports;
- artifact retention/pin references;
- repository/runtime health snapshots.

The first migration in this blueprint is intentionally a comprehensive starting point, not a reason to avoid refinement before v1 freeze.

### 17.3 Artifact store

Path shape:

```text
artifacts/sha256/ab/cd/<full-digest>
```

Metadata includes size, MIME, created time, retention class, sensitivity, source command/event, compression, and references. Write temp file, fsync as policy requires, hash, atomically rename. Never trust an extension for MIME or render behavior.

### 17.4 Retention

Default examples:

```text
raw projected protocol events     90 days
full command logs                 30 days unless evidence-pinned
run/task/evidence manifests       durable until explicit archive/delete
completed removable worktrees    7–30 days or explicit cleanup
failed/preserved worktrees        no automatic removal
pricing/profile/schema snapshots  permanent while referenced
```

Retention cleanup is reference-aware and dry-run visible. Storage pressure may stop execution but must not silently delete active evidence.

### 17.5 Search

Use SQLite FTS5 over sanitized summaries, task titles/objectives, file metadata, findings, and command summaries. Do not index secrets, full environment, raw private reasoning, or all command bytes by default.

---

## 18. Failure classification and recovery

### 18.1 Failure domains

```text
USER/POLICY          denied approval, canceled, changed objective
CODEX_RUNTIME        auth, protocol, service, stream, model reroute issue
CONTROLLER           illegal state, projection, DB, internal bug
GIT/WORKTREE         dirty primary, ref conflict, path violation, merge conflict
SOURCE               compile/test/assertion/lint/semantic failure
INFRASTRUCTURE       missing runner/tool/container/fixture/hardware
RESOURCE             disk/memory/CPU queue/timeout
EVIDENCE             stale SHA, missing/corrupt artifact, overclaim
PUBLICATION          remote auth/head drift/GitHub failure
```

This class is separate from agent verdict and command exit code.

### 18.2 Daemon restart

On startup:

1. migrate/open/check DB;
2. verify artifact roots;
3. start/verify App Server;
4. find nonterminal runs/sessions/commands/leases;
5. inspect actual worktrees/branches/heads/status;
6. list/resume known Codex threads;
7. reconcile active turns/approvals/subagents;
8. inspect process groups owned by daemon where possible;
9. mark uncertain mutable states `RECONCILIATION_REQUIRED`;
10. replay projections/SSE and resume scheduler only when safe.

Never assume an in-flight task failed or succeeded solely because the daemon restarted.

### 18.3 App Server crash

Stop new dispatch. Preserve task/worktree/controller state. Restart boundedly, verify schema, resume threads, reconcile active turn. If a mutable command could have continued without protocol observation, require review/revalidation.

### 18.4 Disk/DB failure

At disk thresholds:

- pause new worktrees/heavy commands;
- keep UI/read access where possible;
- show cleanup/backup options;
- classify command artifact failures explicitly;
- do not accept a validation whose required log/artifact could not be persisted.

### 18.5 Origin changes

Run remains pinned. UI shows that origin advanced. User can:

- continue exact pinned run;
- cancel/re-plan on new origin;
- after result, deliberately synchronize/integrate and invalidate proof.

No silent rebase.

### 18.6 Human edits

If the user edits a managed task worktree:

- record external change detection;
- suspend the agent or update custody based on policy;
- require diff reconciliation;
- invalidate relevant proof;
- attribute commit to configured user normally, not to an agent;
- never erase human changes during cleanup.

---

## 19. Observability

### 19.1 Internal logs

Structured JSON logs to journald/file with:

```text
request_id, run_id, task_id, agent_id, thread_id, turn_id,
worktree_id, command_id, approval_id, event_cursor, error_class
```

Do not put prompts, secrets, or full command output into normal service logs.

### 19.2 Metrics

Useful local metrics:

- App Server ready/restarts/request latency/protocol errors;
- raw append/projector/SSE lag;
- active/queued agents by role/model/state;
- path/resource lease utilization and waits;
- command duration/result/resource by validator;
- task attempts, retries, escalation, verifier rejection rate;
- tokens/cost by model/role/phase;
- context tokens and compaction count;
- worktree/disk/artifact/WAL size;
- approval wait time;
- exact-head evidence invalidations;
- daemon memory/file descriptors/event-loop lag.

A built-in local metrics page is sufficient for v1. Optional Prometheus/OpenTelemetry export can follow without becoming required to operate the product.

### 19.3 Audit trail

Human actions are events with local session identity:

- plan edit/approve;
- steer/interrupt;
- approval decision;
- budget increase;
- retry/escalate/reassign;
- preserve/delete worktree;
- approve integration/push/PR;
- evidence export;
- settings/profile/pricing changes.

---

## 20. Implementation approach

### 20.1 Build vertical, then parallel

The correct build order is:

1. daemon/UI/config/storage skeleton;
2. App Server integration and raw events;
3. one read-only thread visible live;
4. repository registration and one task worktree;
5. approval/diff/command/validation/evidence;
6. task graph and state machine;
7. usage/cost/budgets;
8. bounded parallel scheduler;
9. verifier/integrator/final audit;
10. NeuralMatrix context/profile;
11. GitHub publication/recovery/packaging/pilots.

Do not implement a swarm scheduler before a single task can be safely interrupted, recovered, diffed, verified, and costed.

### 20.2 PR train

`docs/IMPLEMENTATION_BACKLOG.md` defines HC-001 through HC-023 with dependencies, scope, tests, and exit gates. It is the recommended execution plan.

### 20.3 Recommended coding ownership

For building the harness itself:

- Terra xhigh orchestrates system-level PRs and integration;
- Luna max implements bounded crates/UI features under explicit interfaces;
- Sol xhigh reviews controller/Git/security/cost state changes;
- Sol max audits milestone boundaries;
- humans retain architecture/security/product decisions.

Use the same Harness principles manually before Harness exists: isolated worktrees, bounded tasks, independent review, no hidden fallback.

### 20.4 Key Rust types

```rust
pub struct RunId(Ulid);
pub struct TaskId(Ulid);
pub struct AttemptId(Ulid);
pub struct AgentSessionId(Ulid);
pub struct WorktreeId(Ulid);
pub struct ApprovalId(Ulid);
pub struct ArtifactDigest([u8; 32]);

pub enum ResultClass {
    Success,
    NotSelected,
    SourceFailure,
    InfrastructureUnavailable,
    Inconclusive,
    CancelledSuperseded,
    SkippedDraft,
    QuarantinedFailure,
}

pub enum SandboxMode { ReadOnly, WorkspaceWrite }
pub enum ResourceClass { Control, Medium, Heavy, Hardware(String) }
pub enum AgentRole { Architect, Explorer, Worker, Integrator, Verifier, FinalAuditor, CiTriage }
```

Use newtypes and closed enums for identifiers/results/states. Unknown protocol values belong in adapter types, not silently in domain enums.

### 20.5 Service interfaces

```rust
#[async_trait]
pub trait RepositoryManager {
    async fn inspect(&self, repo: RepositoryId) -> Result<RepositoryHealth>;
    async fn fetch_and_pin(&self, repo: RepositoryId, reference: &str) -> Result<GitSha>;
    async fn create_worktree(&self, spec: WorktreeSpec) -> Result<Worktree>;
    async fn verify_diff(&self, id: WorktreeId, policy: DiffPolicy) -> Result<VerifiedDiff>;
    async fn commit(&self, id: WorktreeId, spec: CommitSpec) -> Result<GitSha>;
    async fn integrate(&self, run: RunId, commits: Vec<GitSha>) -> Result<IntegrationResult>;
}

#[async_trait]
pub trait Orchestrator {
    async fn handle(&self, command: OrchestrationCommand) -> Result<Vec<DomainEvent>>;
    async fn reconcile(&self, run: RunId) -> Result<Vec<DomainEvent>>;
}

#[async_trait]
pub trait ValidatorEngine {
    async fn select(&self, task: &TaskPacket, diff: &VerifiedDiff) -> Result<Vec<ValidatorSpec>>;
    async fn execute(&self, validation: ValidationRequest) -> Result<ValidationResult>;
}

pub trait UsageLedger {
    fn observe(&self, sample: TokenSample) -> Result<Vec<CostEntry>>;
    fn summary(&self, scope: UsageScope) -> Result<UsageSummary>;
}
```

### 20.6 Output schemas

Use structured output schemas for:

- architect task graph;
- worker handoff;
- verifier findings/verdict;
- integrator reconciliation report;
- final audit;
- context probe responses where useful.

Schema validation failure does not trigger blind repair indefinitely. One bounded schema-correction turn is reasonable; repeated failure is a typed agent/runtime failure.

### 20.7 Frontend state

The browser is a projection consumer:

- initial REST snapshots;
- SSE domain events update TanStack Query caches;
- optimistic UI only for low-risk local display edits;
- approvals, retries, integration, and publication wait for server transaction response;
- route/query state controls filters/selection;
- raw event and huge log data load on demand.

Do not mirror the entire orchestrator state machine in frontend logic.

---

## 21. Testing and release assurance

The full matrix is in `docs/TEST_AND_ACCEPTANCE_PLAN.md`. Non-negotiable suites:

1. generated App Server schema + golden JSONL replays;
2. model/property tests for run/task/lease/controller state;
3. temporary Git repository worktree/path/symlink/conflict/cleanup tests;
4. process-group/output-flood/timeout/restart command tests;
5. exact decimal cost fixtures and property tests;
6. SQLite migration/backup/raw-append/projection recovery;
7. Playwright observe/approval/subagent/restart/integration/accessibility flows;
8. security tests for localhost, CSRF, secret redaction, artifact rendering, stale approvals;
9. fault injection at every state boundary;
10. Fedora/Nobara and Ubuntu/Debian package smoke;
11. live pinned App Server compatibility smoke;
12. NeuralMatrix pilot ladder.

Release must fail if any fault path can produce false `VERIFIED`, `PUBLICATION_READY`, or `COMPLETED` state.

---

## 22. NeuralMatrix pilot and rollout plan

### Pilot 0 — read-only architecture

Goal: prove App Server, context, UI, goal/plan/actions/tokens/cost, event durability, restart.

### Pilot 1 — docs-only bounded change

Goal: one Luna worktree, lease/diff/approval, verifier, integration, evidence, explicit draft PR.

Avoid architecture spine/serial docs for the first mutable task.

### Pilot 2 — isolated Rust component fix

Goal: positive/negative focused tests, controller rerun, exact commit, independent review, T0–T2 proof limits.

### Pilot 3 — two disjoint tasks

Goal: parallel worktrees, no overlap, fair resources, parent/child usage, dependency integration.

### Pilot 4 — contract-sensitive hard cut

Goal: direct Terra route, serial generated paths, consumer dependency order, no fallback/repair, fresh Sol rejection.

### Pilot 5 — heavy/hardware proof representation

Goal: C2 container/Jetson lane selection and result semantics. Unavailable target remains non-green; live production action remains operator-controlled.

### Pilot metrics

Track:

- accepted first-pass rate by role/model/risk;
- verifier finding rate/severity;
- remediation rounds;
- context misses and redundant searches;
- tokens/cost per verified task;
- wall/active time and resource waits;
- path-policy/approval incidents;
- stale evidence invalidations;
- operator interventions;
- false-green attempts prevented.

Use results to tune routing/context/budgets, not to weaken proof.

---

## 23. Deliberate simplifications

V1 intentionally omits:

- multi-user/server deployment;
- cloud-hosted controller;
- arbitrary provider/plugin marketplace;
- distributed workers;
- vector database/semantic embeddings;
- full IDE/editor;
- production deploy/actuator control;
- automatic branch-ready/merge;
- speculative agents editing without task leases;
- visual DAG as the default operational screen;
- unlimited subagents;
- hidden compatibility behavior when a model fails;
- raw hidden chain-of-thought capture.

These omissions keep the system durable and usable rather than over-engineered.

---

## 24. Principal risks and mitigations

| Risk | Mitigation |
|---|---|
| App Server protocol changes rapidly | pin version/schema, generated bindings, raw events, golden traces, fail closed |
| child agents appear isolated but share workspace | top-level worktree per mutable task; children read-only by policy |
| repository context is too large | authority router, exact repo map, bounded probe, fresh role-specific threads |
| agent claims outrun proof | controller validation, exact SHA, independent verifier, proof-tier UI |
| expensive Sol/Terra use | role routing, Luna bounded workers, cached stable context, per-task budgets |
| subscription usage differs from API price | label API-equivalent estimate, store source snapshot/rate limits separately |
| hidden process mutates after cancellation | process groups, reconciliation, lease held until safe |
| primary repo damaged | coordination lock, no edits, managed worktrees, exact preflight |
| parallel tasks conflict semantically | DAG + path/symbol/serial risk; bounded 3 workers; serial integration |
| logs leak secrets | allowlist/redaction, local permissions, retention, sanitized export |
| DB/event volume grows | compact projections, virtualized UI, artifact spill, reference-aware retention |
| UI becomes overwhelming | list+inspector default, progressive disclosure, action-oriented status |
| Harness becomes second NeuralMatrix authority | profile import only, explicit completion authority, no automatic checklist state |

---

## 25. Tape-out decisions

The following decisions should be treated as fixed for v1 unless a concrete implementation blocker produces a new ADR:

1. Rust daemon and CLI; React embedded web UI.
2. Localhost-only single-user operation.
3. One pinned Codex App Server child over stdio.
4. Raw App Server events persisted before projection.
5. SQLite/WAL plus content-addressed artifacts.
6. Deterministic controller owns state/Git/scheduling/evidence/publication.
7. One controller-created top-level thread/worktree per mutable task attempt.
8. Native subagents primarily read-only and counted against total capacity.
9. Default NeuralMatrix capacity: three mutable, one verifier, six total threads.
10. Sol architect/verifier/final audit; Luna bounded work; Terra risk/escalation/integration.
11. Authority-first context and no vector DB in v1.
12. REST + durable SSE; WebSocket only for explicit human terminal later.
13. Exact path leases and serial paths; primary checkout remains clean.
14. API-equivalent cost from immutable price snapshots; no fake billing precision.
15. No raw hidden reasoning retention by default.
16. No automatic push/PR; explicit user gate. Draft PR is maximum publication action in v1.
17. No automatic merge.
18. NeuralMatrix master checklist remains the only repository completion authority.
19. Missing infrastructure is never success.
20. Release only after restart/fault/Git/security/cost/UI tests and the pilot ladder.

### Decisions intentionally deferred to implementation start

These require observing the actual target host/pinned Codex release, not further conceptual debate:

- the exact Codex CLI version/schema digest to certify;
- the exact root Rust toolchain and minimum supported distro versions for the new harness repository;
- whether release binaries use glibc or musl for each dependency set;
- final UI component library/diff renderer after a small accessibility/performance spike;
- actual token budgets/concurrency resource numbers after Pilot 0–3 measurements;
- whether a separate managed bare repository is worth the disk cost after v1.

The architecture provides a closed procedure for selecting each.

---

## 26. Definition of done

Harness Console v1 is done when all of the following are true:

### Runtime

- pinned App Server initializes, streams, steers, interrupts, resumes, and exposes goals/usage/subagents;
- schema mismatch disables execution without losing historical UI;
- daemon restart reconciles nonterminal runs.

### Repository custody

- primary NeuralMatrix checkout remains clean/on-main;
- every mutable attempt has an isolated worktree/branch/base/head/lease;
- unexpected/serial/forbidden path changes block acceptance;
- controller owns commits/integration/push/PR; no AI attribution;
- failed work is preserved safely.

### Orchestration

- schema-valid task graph, bounded scheduler, watchdog, retries, escalation;
- default model policy works and requested/effective settings are visible;
- native subagents are mapped and budgeted;
- independent verifier and final audit cannot be bypassed by worker prose.

### Evidence

- commands and exact result classes are durable;
- T0–T6 and unproved claims are visible;
- exact-head invalidation works;
- evidence bundle verifies content hashes;
- missing infrastructure is non-green.

### UX

- user sees current goal/action/model/effort/worktree/SHA/tokens/cost for every agent;
- plan/diff/files/commands/evidence/usage/context tabs work;
- approval center, steer, interrupt, retry, escalate, preserve, integration review work;
- keyboard/accessibility/reconnect/large-log paths pass.

### Security/operations

- localhost/CSRF/path/process/secret/artifact tests pass;
- no raw hidden reasoning by default;
- systemd install/upgrade/backup/doctor/runbook work on Fedora/Nobara and Ubuntu/Debian;
- no automatic merge or production action path exists.

### NeuralMatrix pilots

- Pilots 0–4 complete with evidence and no unresolved critical/high controller, Git, evidence, or security defect;
- profile enforces active authority, no-fallback semantics, exact-head proof, and separate live/hardware state;
- Harness run completion does not alter checklist authority automatically.

---

## 27. Build package map

This blueprint contains:

```text
README.md
ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md
adrs/ADR-0001..0006
docs/UI_WIREFRAMES.md
docs/APP_SERVER_EVENT_MAPPING.md
docs/COST_ACCOUNTING.md
docs/IMPLEMENTATION_BACKLOG.md
docs/OPERATIONS_RUNBOOK.md
docs/TEST_AND_ACCEPTANCE_PLAN.md
config/harness.example.toml
profiles/neuralmatrix/profile.toml
schemas/nm.orchestration.task.v1.schema.json
schemas/nm.orchestration.handoff.v1.schema.json
schemas/harness.evidence.v1.schema.json
migrations/0001_initial.sql
openapi/harness-api.yaml
codex/agents/*.toml
packaging/systemd/harnessd.service
```

The fastest responsible start is HC-001 through HC-007, producing one durable read-only Codex thread in the GUI. The first source-writing milestone is HC-008 through HC-011. Parallel agents should not be enabled until HC-014 after worktree, approval, command, and state-machine custody are proven.

---

## 28. Reference basis

### Official Codex/OpenAI surfaces consulted

- Codex App Server protocol and generated schemas.
- App Server threads, turns, goals, approvals, reviews, usage, and collaborative-agent events.
- Codex subagent configuration and runtime inheritance guidance.
- Codex app product description for project/thread/worktree/diff UX benchmark.
- GPT-5.6 model catalog/guidance and current effective-dated model pricing.
- OpenAI Codex source protocol definitions for token and collaborative-agent fields.

### NeuralMatrix repository authority consulted

- `AGENTS.md`
- `CODEX.md`
- `README.md`
- `docs/INDEX_FOR_AGENTS.md`
- `docs/architecture/CANONICAL_INDEX.md`
- `docs/architecture/ENGINEERING_GOVERNANCE_CONTRACT.md`
- `docs/architecture/CI_TEST_ARCHITECTURE.md`
- `docs/tasks/PRODUCTION_READINESS_ORCHESTRATION_PLAN_2026-07-19.md`

The repository profile must re-read these from the exact run base rather than assuming the August 5, 2026 snapshot remains current.
