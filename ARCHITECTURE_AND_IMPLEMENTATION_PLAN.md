# BILDR

## Linux-First Codex Multi-Agent Control Plane for Local Git Repositories

**Status:** proposed architecture and implementation plan
**Strict repository profile:** `lazerusrm/BILDR`
**Default runtime profile:** repository-neutral `general`
**Target host:** Linux workstation/server
**Prepared:** 2026-08-05
**Primary binaries:** `harnessd`, `harnessctl`

---

## Executive decision

Build **BILDR** as a local supervisory control plane around a version-pinned Codex App Server. It should feel like a focused Codex desktop command center, but its differentiator is not another chat UI. Its job is to make multi-agent engineering **observable, bounded, reproducible, and safe**:

- every agent and native subagent is visible with requested/effective model, reasoning effort, current goal, plan, current action, parent, worktree, branch, SHA, token use, and API-equivalent cost;
- every mutable task runs in a controller-created Git worktree with explicit path leases;
- the controller, not a model, owns state transitions, scheduling, Git, validation, evidence, budgets, retries, and publication;
- a task whose owner profile is a controller/governor runs as a visible governing agent: it maintains the bounded plan, delegates read-only investigations to native child threads, reconciles or redirects those children, and receives automatic controller checkpoints before its token budget is exhausted;
- Sol plans, governs repository-wide goals, and independently audits; Luna performs bounded high-volume exploration/implementation; Terra handles complex/high-risk implementation and serial integration;
- repository-specific authority is imported through profiles rather than
  hard-coded; BILDR ships a strict self-profile while other Git repositories use
  the `general` profile or provide their own policy adapter;
- the human primarily steers one visible governor conversation; child threads remain inspectable, while approvals appear inline and internal evidence/worktree ledgers do not become separate mental workspaces;
- signed-in Codex homes are discovered without copying credentials; Harness can also create private app-managed Codex homes through Codex device authorization, usage is attributable by account/repository/agent, and optional capacity handoff happens only between attempts;
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

### 1.1 What BILDR is

BILDR is a durable local application that accepts an engineering objective scoped to a registered repository and exact base, turns it into a reviewed task graph, dispatches bounded Codex agents, records their runtime activity, verifies their changes, serially integrates accepted commits, and produces a reviewable exact-head result plus evidence.

It combines four product categories:

1. **Codex runtime console** — threads, turns, models, effort, plans, actions, approvals, diffs, reviews, goals, usage, and subagents.
2. **Engineering orchestrator** — task DAG, roles, leases, retries, escalation, integration, and stop conditions.
3. **Repository custody system** — exact base SHA, worktrees, branches, commits, path ownership, diff verification, and publication gates.
4. **Evidence ledger** — commands, proof tiers, result semantics, artifacts, findings, exact SHA, unproved claims, and estimated cost.

### 1.2 What it is not

BILDR is not:

- a replacement for Codex's own sandbox, approvals, or agent loop;
- a second architecture or completion authority for a registered repository;
- a hidden chain-of-thought viewer;
- a general IDE or terminal multiplexer;
- a continuous background coding service that pushes changes without review;
- a production deployment controller;
- a fleet/hardware lab automation platform in v1;
- a mechanism for turning unavailable tools or hardware into green proof;
- a compatibility or fallback layer for agent failures.

### 1.3 Primary user workflow

```text
1. Select a registered repository, governor model/effort, and state the objective.
2. Controller fetches and pins origin/main to an exact SHA.
3. Sol xhigh inspects active authority and emits a schema-valid task graph.
4. User reviews task boundaries, paths, models, budgets, tests, and proof limits.
5. Controller leases non-overlapping paths and creates task worktrees.
6. Luna/Terra workers implement bounded tasks; read-only native subagents assist.
   Repository-wide serial tasks use a Sol xhigh governor as the parent and show its
   delegated children explicitly; they do not masquerade as one ordinary worker.
7. Controller runs focused validations and captures evidence.
8. Fresh Sol verifier attempts to reject each task.
9. Terra integrates verified commits in dependency order.
10. Integration proof reruns; invalidated evidence is explicit.
11. Fresh Sol max final audit attempts to reject the complete result.
12. User receives required Sol final signoff and explicitly approves any push/draft PR action.
```

### 1.4 v1 success criteria

V1 is successful when it can run the repository pilot ladder and reliably
answer, at any time:

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

### 2.2 Repository policy basis

Each repository profile enforces the checkout's current rules:

- start from active authority instead of archived plans;
- keep the coordination checkout clean and create worktrees from a freshly
  resolved exact base;
- preserve canonical ownership and fail-closed behavior;
- bind evidence to an exact SHA and distinguish source failure, unavailable
  infrastructure, and inconclusive proof;
- use bounded mutable workers plus independent verification;
- prevent worker self-approval and automatic completion-authority updates;
- keep source, integration, environment, live, and rollout proof distinct.

Controller runtime state is disposable relative to repository authority. It may
export evidence and propose changes, but it never silently becomes the
repository's completion ledger.

### 2.3 Repository scale

Registered repositories can span multiple workspaces, languages, platforms, and
delivery paths. The context engine must route and cache; it cannot pass an
entire large repository to every role or assume one root test command
establishes product truth.

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
│          SQLite/WAL + artifacts       registered repository/object DB         │
│                                                                             │
│  Optional controlled external edges: origin/GitHub, container engine,       │
│  hosted CI and optional environment-specific validation targets             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component map

| Component              | Responsibility                                             | Must not own                       |
| ---------------------- | ---------------------------------------------------------- | ---------------------------------- |
| `harness-api`          | local REST/SSE, session/CSRF, DTOs                         | orchestration decisions            |
| `harness-domain`       | state machines, IDs, result classes, domain events         | I/O                                |
| `harness-codex`        | App Server process/protocol, raw events, requests          | task truth or Git                  |
| `harness-store`        | SQLite migrations/repositories, event journal, artifacts   | business policy                    |
| `harness-profile`      | repository policy, domain/risk/validator routing           | runtime mutable state              |
| `harness-git`          | locks, fetch, worktrees, leases, diff, commit, integration | semantic conflict decisions        |
| `harness-runner`       | controller-owned commands, resources, logs, cancellation   | model turns                        |
| `harness-context`      | authority routing, repo map, context packet, probe helper  | architecture authority             |
| `harness-orchestrator` | run/task state, scheduler, retries, escalation             | direct protocol/Git shell details  |
| `harness-evidence`     | validation, claims, artifact manifests, export             | release promotion decision         |
| `harness-usage`        | token deltas, price snapshots, cost/budgets                | billing claims beyond evidence     |
| `harnessd`             | composition root and lifecycle                             | domain logic embedded ad hoc       |
| `harnessctl`           | operator commands over the same API/domain services        | an independent hidden control path |
| `ui`                   | observability and explicit human controls                  | source-of-truth state              |

### 3.3 Proposed source repository layout

```text
bildr/
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
│   └── bildr/
├── runtime/
│   ├── roles/
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
- `thread/goal/set` is reserved for the persistent governor thread, whose goal
  may continue across turns and updates on material remediation or phase
  change;
- interview, architecture, review, verification, and audit are
  controller-bounded turns. They never install an App Server auto-continuing
  goal; the controller alone decides whether their completed result needs a
  distinct follow-up turn.

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
      ├─ IntentInterview (questions, human responses, draft/confirmed brief)
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
  -> INTERVIEWING (optional)
       -> READY_FOR_ARCHITECTURE (human confirms or skips)
  -> READY_FOR_ARCHITECTURE (when the optional interview is off)
  -> ARCHITECTING
  -> PLAN_ADVERSARIAL_REVIEW
       -> PLAN_REVISION_REQUIRED -> ARCHITECTING (review/revision loop)
       -> PLAN_REVIEW_REQUIRED (independently certified)
            -> PLAN_REVISION_REQUIRED (operator findings)
            -> PLAN_ADVERSARIAL_REVIEW (stale certificate bindings)
  -> READY_TO_EXECUTE
  -> EXECUTING
  -> TASK_VERIFICATION
  -> INTEGRATION_READY
  -> INTEGRATING
  -> INTEGRATION_VERIFICATION
  -> FINAL_AUDIT
  -> HUMAN_REVIEW
       -> EXECUTING (blocking findings reopen owned tasks and rebuild integration)
  -> PUBLICATION_READY
  -> DRAFT_PR_CREATED
       -> COMPLETED (directly when the profile has no CI gate; otherwise tasks pass through CI_PROVEN after required checks pass at the integration SHA)
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

`INTERVIEWING` is a real human gate, not an automatic planning loop. The
selected governor model asks at most one material intent question per completed
turn in a read-only inspection worktree. Each human response starts a new turn
on the same thread. Planning cannot start until the human confirms a concise
brief or explicitly skips the interview. A fresh architect thread receives the
original objective and confirmed brief, never the raw transcript. Automatic
plan approval cannot confirm or skip an interview.

The controller-owned plan response schema remains authoritative even when the
repository contains its own planning formats. If an architect completes useful
discovery but returns a schema-invalid final object, the controller starts a
focused serialization-repair turn from that response after the turn reaches a
safe boundary. The repair does not repeat repository inspection. It restates
the controller shape, preserves the useful critical path, and receives the same
independent adversarial review as any other plan. Two failed automatic shape
repairs stop with the concrete rejection visible for an operator decision;
explicit retry starts a new bounded repair cycle. The total run-token ceiling
remains authoritative.

Controller-owned packet facts do not justify another model turn. Before plan
validation, the controller binds the run identity, pinned base SHA, general-run
owner and execution route, reviewer route, active authorities, forbidden
runtime paths, lease and handoff metadata, and empty optional lists. It may
derive checklist rows directly from the architect's milestone titles. It does
not invent or repair the substantive objective, custody, milestones, success
criteria, evidence, proof limits, or budgets. A response with a useful semantic
plan therefore advances to adversarial review even if it omits mechanical
packet boilerplate; a response with a broken critical path still requires
revision.

`PLAN_REVIEW_REQUIRED` means the current plan digest and its base/profile/authority
bindings have already passed a fresh, read-only adversarial review with zero
blocking findings; it does not mean merely schema-valid. Automatic plan approval may perform the certified
`PLAN_REVIEW_REQUIRED -> READY_TO_EXECUTE` transition, but cannot bypass
`PLAN_ADVERSARIAL_REVIEW`. Blocking findings create a complete replacement plan
revision and repeat review automatically. Advisory findings remain on the
certificate and enter execution context without buying another planning round.
Review inspection is bounded to the executable critical path rather than a
second product inventory. If a review turn ends before emitting its structured
verdict, one verdict-only continuation may reuse that same native thread and
its inspected evidence; it cannot call tools or restart discovery. A fresh
reviewer is used only when that bounded recovery is unavailable or fails.

The run-token ceiling governs active work as well as future scheduling. Reaching
it pauses the scheduler and interrupts the active turn; the controller never
lets an already-running model continue spending merely because the next state
transition has not happened yet. Before a fresh review turn, the controller
also reserves enough remaining budget for the already-known context and plan
input; it pauses without sending the request when that input cannot fit.
Repeated/oscillating finding fingerprints or three non-shrinking review rounds
pause as `plan_review_deadlocked` with the findings history for an operator
decision; this is evidence-based convergence detection, not a raw review-count
cap. Temporary capacity/runtime failures remain queued, and the run token
ceiling remains authoritative.

### 5.3 Task state machine

Use a repository-neutral orchestration vocabulary:

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
- `FINAL_AUDIT` requires every profile-selected validator and automated acceptance command to have succeeded on the exact, clean integrated head; patch formatting is custody proof, not behavioral proof;
- a validator result is inadmissible when the checkout fingerprint or `HEAD` changes while it runs;
- evidence binds to exact source SHA and is invalidated after relevant source/artifact changes;
- `HUMAN_REVIEW` is a resting state. Approval and rejection both bind to the current signoff-packet digest and integration SHA; rejection names owned files and creates a fresh integration candidate rather than reviving the rejected SHA;
- `PUBLICATION_READY` requires accepted final audit, explicit human signoff, all path-selected platform acceptance entries, no unproved claims, and current integration proof;
- `CI_PROVEN` is profile-gated. When required, the controller must observe the
  draft PR still points at the integration SHA and every required check passes
  on that SHA. Profiles without this gate complete after draft creation without
  falsely claiming `CI_PROVEN`; neither path authorizes merge;
- `COMPLETED` is a BILDR run state, not a repository completion-authority claim.

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

| Concern                        | Controller code                       | Model                                 |
| ------------------------------ | ------------------------------------- | ------------------------------------- |
| exact base/ref/worktree/branch | owns                                  | receives                              |
| active authority selection     | validates/imports                     | interprets and cites                  |
| task graph                     | schema/risk/path/DAG validation       | Sol proposes                          |
| task objective/acceptance      | freezes per attempt                   | follows; may report conflict          |
| scheduling and concurrency     | owns                                  | none                                  |
| path leases/serial paths       | owns/enforces                         | obeys                                 |
| implementation                 | observes/bounds                       | Luna/Terra performs                   |
| commands                       | records/limits; some controller-owned | requests/runs through Codex sandbox   |
| validation selection/execution | owns gate/path matching, exact-SHA execution, mutation checks, and classification | may recommend; cannot waive           |
| independent review             | starts/freshens                       | Sol performs                          |
| integration order              | owns                                  | Terra resolves approved semantic work |
| completion/evidence            | owns typed state                      | supplies structured handoff/findings  |
| push/PR                        | owns after human approval             | never improvises                      |
| merge                          | absent in v1                          | none                                  |

A model sentence such as “all tests pass” is not a state transition. The command and result must exist in the controller evidence ledger.
Likewise, a review-model `accept` cannot substitute for a missing validator,
platform acceptance item, or human signoff. Review verdicts distinguish
blocking findings from advisories and include inspected-file/check/failure-mode
evidence; only blocking findings cause another repair cycle.

### 6.2 Architecture phase

Sol xhigh runs read-only in the inspection worktree with:

- user objective;
- exact base SHA and repository profile;
- instruction/authority router and digests;
- bounded repository map and code-navigation seeds;
- task schema and risk vocabulary;
- explicit instruction not to edit or claim completion.

It emits `harness.orchestration.task.v1[]` plus a run-level plan summary.
Deterministic controller validation rejects:

- cycles;
- missing authority or checklist mapping where required;
- ambiguous base SHA;
- overlapping mutable paths without serial ownership;
- serial/forbidden paths assigned to ordinary workers;
- tasks too broad for configured diff/token budgets;
- implicit compatibility/fallback/normalization/repair;
- a proof claim beyond the requested validators/environments;
- unsupported model/effort/sandbox.

After deterministic validation, a fresh read-only reviewer from the configured
integrator model family (Terra xhigh in the bundled profiles, deliberately
different from the Sol architect) inspects the repository and attempts to prove that the plan will stall, waste the budget,
ossify a provisional design, or finish metadata without delivering behavior.
It checks objective alignment, feasibility, critical-path liveness, milestone
and dependency sizing, available resources, behavior-first evidence, test
timing, recovery/replan authority, and immutable safety boundaries. It rejects
global gates built from mutable PR/branch/deployment inventories, read-only
inventory that blocks all code progress, SHA bookkeeping treated as the work,
and broad test construction around code that has not worked in its real
pipeline.

The required implementation/proof sequence is:

`vertical code slice -> real pipeline proof -> iterate -> certify behavior/code
shape -> targeted regressions -> broader hardening`.

Before pipeline proof, a plan asks only for the minimum smoke/probe/acceptance
path needed to learn. Durable regressions validate the authoritative path and
generic invalid-shape categories, not every historical alias, fallback,
rejection, or provisional internal. Existing failing tests are blockers only
when they protect a current certified contract through credible production
behavior; stale tests may be revised or removed.

Exact SHAs, worktree custody, manifests, and evidence digests remain mandatory
boundary receipts. They never substitute for direct behavioral proof from
running code. If a plan-created assumption or constraint prevents progress,
the governor may replan within the objective, immutable safety boundaries,
external-write approval policy, and remaining run budget.

Only a coherent `accept` verdict with zero **blocking** findings marks the digest
`CERTIFIED`; concrete advisory findings are retained for the governor. Every
review verdict also carries non-empty inspected-file evidence, a task-id
critical-path trace to behavioral proof, and one to three material failure modes
with mitigations. The controller—not reviewer prose—attaches budget arithmetic,
risk routing, planning spend, reviewer identity, and the plan/base/profile/
authority binding tuple to the certificate. When a human confirms an intent
brief, the certificate also binds its digest. Approval reopens review if that
binding changes.

A `changes_requested` verdict must contain concrete blocking findings; the
architect receives the prior plan and only those blockers and returns a full
replacement revision. `POST /runs/{runId}/plan/request_changes` gives the
operator the same path from a certified or convergence-blocked plan. Requested
objective, non-goal, budget, or dispatch corrections are expressed as findings
and materialized by the architect into a replacement digest, so the result is
always re-certified instead of mutating a certified object in place. Prior plan
JSON, certificates, and the complete review history are retained.

Approval rechecks the certificate's complete binding tuple and re-enters
adversarial review if any binding changed. It also recomputes whether remaining
run tokens cover planned task budgets, per-task verifier reserve, final audit,
and contingency. An infeasible plan cannot start without an explicit local-user
budget override. Automatic approval additionally requires no high-risk or
serial-path task, a controller execution reserve below
`automatic_plan_approval_max_execution_tokens`, and a reviewer from a different
model family; otherwise the certified plan waits for a human decision.

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

Default strict-profile capacity:

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

The controller compiles those inputs into a bounded
`harness.attempt-continuity.v1` packet. It prefers the task's declared durable
checkpoint, includes a bounded chain of recent valid attempt handoffs, and only
then falls back to a prior final agent message. Operator guidance is optional:
a blank Continue action means the controller selects the next action from the
durable milestone ledger without rewriting the goal. Raw reasoning and the full
conversational transcript are not copied. The prior worktree remains a read-only
recovery source; uncommitted edits are never implied to exist in the new isolated
attempt. An attested candidate tree may be materialized into a clean leased
worktree by the controller, after which normal path, diff-budget, and base-SHA
custody still applies. The run UI labels cold recovery as `bounded_handoff` and
links the source attempt.

Every governor-owned task contains 3-20 human-reviewable milestones. Every
governor turn finishes with `harness.governor-checkpoint.v1`: a monotonic
revision, the full milestone ledger, one active milestone while progressing, a
plain-language operator update, the controller-selected next action, workspace
state, and durable artifact locators. Completed milestones may not regress.
`blocked` is reserved for a genuine external, policy, credential, or approval
boundary; ordinary implementation choices and turn-budget rollover remain the
governor's responsibility.

Direct repetition of one verifier finding set is capped, recommended at two
rounds. Crossing that threshold produces a controller-authored strategy
correction and another bounded repair window; it does not require the human to
translate internal verifier feedback.

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

| Role             | Model           | Effort   | Sandbox         | Purpose                                                                    |
| ---------------- | --------------- | -------- | --------------- | -------------------------------------------------------------------------- |
| architect        | `gpt-5.6-sol`   | `xhigh`  | read-only       | authority map, invariants, task graph                                      |
| plan reviewer    | `gpt-5.6-terra` | `xhigh`  | read-only       | diverse adversarial plan liveness, feasibility, and evidence review        |
| governor         | `gpt-5.6-sol`   | `xhigh`  | workspace-write | preserve goal intent, bound/delegate work, reconcile evidence and failures |
| explorer         | `gpt-5.6-luna`  | `medium` | read-only       | bounded source/test/contract discovery                                     |
| normal worker    | `gpt-5.6-luna`  | `high`   | workspace-write | narrow implementation and focused proof                                    |
| CI triage        | `gpt-5.6-luna`  | `high`   | read-only       | classify logs/result semantics                                             |
| high-risk worker | `gpt-5.6-terra` | `xhigh`  | workspace-write | contracts, persistence, security, native, cross-domain                     |
| integrator       | `gpt-5.6-terra` | `xhigh`  | workspace-write | serial integration/conflict/evidence invalidation                          |
| verifier         | `gpt-5.6-sol`   | `xhigh`  | read-only       | adversarial task review                                                    |
| final auditor    | `gpt-5.6-sol`   | `max`    | read-only       | integrated system rejection attempt                                        |

These are policy defaults, not hard-coded universal truth. Store them in the repository profile and show every override.

### 7.2 Why this split

- Sol is spent where global correctness, goal governance, and adversarial review have the highest leverage.
- Terra receives tasks whose semantics span components or whose failure could create subtle compatibility/security/persistence debt.
- Luna handles many bounded tasks economically at high effort because the scope has already been constrained.
- Read-only exploration uses lower effort and compact outputs to reduce repeated expensive repo rediscovery.

Effort is a measured routing control, not a synonym for quality. Keep the
highest settings for roles where deeper global reasoning changes the outcome,
such as architecture, governance, and final audit. Compare the configured
setting with one lower level on representative completed runs before raising a
routine role. Do not compensate for an unclear task by increasing effort.

### 7.3 Model reroutes

Record both requested and effective model/effort from runtime settings/events. Cost and audit use effective values. The UI displays reroutes prominently; the controller must not pretend a Luna task remained Luna if runtime routed it to Terra or Sol.

### 7.4 Budget defaults

Start with task budgets, then tune from pilots:

```text
architect               120k tokens
plan reviewer            120k tokens
Sol goal governor       task-specific budget
read-only explorer       30k tokens
Luna bounded worker      80k tokens
Terra high-risk worker  140k tokens
verifier                 80k tokens
integrator              160k tokens
final auditor           120k tokens
```

The goal API carries the active budget where supported. The controller ledger remains authoritative across turns and subagents.

Governor budgets use two independent bounds. A goal envelope limits consecutive
work without durable milestone or artifact progress, while each attempt receives
a smaller adaptive slice. Durable progress automatically opens the next bounded
envelope, so productive goals do not require manual token replenishment. The
slice is the rounded 75th percentile of recent productive governor usage plus
50% headroom, bounded by the operator's attempt ceiling. Infrastructure,
authentication, and controller-policy failures are excluded from the learning
sample. With fewer than two usable samples, use the bounded cold-start default.
The no-progress envelope counts only the governor and its delegated descendant
threads for that task. Architecture, independent verifier, and unrelated task
usage still count against the total run ceiling, but cannot consume the
governor's autonomy window or prevent automatic verifier remediation.

### 7.5 Budget behavior

- 50%: send one outcome audit: compare activity with success criteria and tool
  evidence, and change strategy if it is not producing code, behavioral proof,
  or a concrete external blocker.
- 85%: ask for one concrete outcome, candidate materialization, and a
  tool-grounded checkpoint. Remind the governor that productive incomplete work
  continues automatically across turn boundaries.
- 100%: hard-stop the attempt; never silently widen its bound.
- exact percentages and token counts remain controller telemetry and are not
  repeated in model-facing checkpoint prose;
- run creation exposes the total goal envelope, defaulted from operator settings;
  a human continuation may explicitly add a bounded allowance to the next
  governor attempt, and the controller raises the run cap only enough to admit
  that attempt plus the configured read-only child ceilings before resuming
  scheduling, so ordinary bounded delegation cannot consume the governor's
  entire continuation window. Manual additions may be as large as 50m from the
  run surface; the internal hard attempt ceiling is 100m and the lifetime run
  ceiling is 1b so long-lived goals already above 100m remain resumable;
- native governor children have a controller-enforced 250k cumulative ceiling;
  crossing it interrupts the child and returns control to the governor instead
  of relying on prompt compliance or human babysitting.
- running command may finish under command timeout.
- budget pressure never authorizes hidden scope reduction or weak tests.
- a productive incomplete governor checkpoint first starts another bounded turn
  on the same native Codex thread and leased worktree so opaque reasoning state,
  prompt-cache locality, and useful child context remain available;
- a governor waiting only on delegated children uses one long native
  `wait_agent` call; child completion wakes it early, avoiding token-bearing
  short polling loops;
- if native thread reuse is unavailable or unsafe, the controller falls back to
  a fresh immutable attempt from the stored bounded handoff. Account, model,
  base, worktree-custody, or sandbox changes always take this cold path;
- daemon or App Server loss while a root governor is active preserves the
  interrupted attempt and automatically queues a fresh bounded attempt. A
  finalized checkpoint is useful continuity evidence but is not required for
  recovery; active root-governor custody or an infrastructure-stalled governor
  attempt is sufficient. Pending approvals remain fail-closed;
- process supervision may restart a crashed daemon, but a single missed HTTP
  health deadline is observational only and must never kill active agent work;
- an ordinary governor diff-custody rejection preserves the rejected worktree
  read-only and schedules a clean bounded attempt with the exact forbidden,
  serial, or `diff --check` findings; it never commits the rejected diff and it
  does not ask the human to translate an internal repair;
- three consecutive checkpoints with no milestone/artifact/workspace progress
  trigger a controller-authored strategy correction that forbids repeating the
  same probe or delegation; only a real approval/authority decision or an
  exhausted no-progress envelope stops automatic continuation and asks the user;
- verifier findings return to the governor in a fresh bounded repair window.
  The configured remediation count bounds repetition of one finding set: when
  crossed, the controller issues a strategy correction and resets that bounded
  cycle instead of asking the human to translate internal verifier feedback.
  A real approval/authority decision, an attempt that cannot materialize a new
  candidate within its no-progress window, or the selected total run ceiling
  may still stop the run;
- governor handoffs are controller records outside the repository. A generated
  `.omx`, `.harness-runtime`, or equivalent runtime file is captured and removed
  before source-diff custody without weakening the forbidden-path rule.

---

## 8. Subagent architecture

### 8.1 Two kinds of “agent”

BILDR must distinguish:

1. **Controller-created primary agents** — top-level Codex threads, each assigned a task attempt and explicit worktree/sandbox/lease.
2. **Codex-native child subagents** — spawned by a parent thread, inheriting its runtime/workspace boundary and represented by collaborative-agent events.

This distinction is central to correctness.

Harness does not implement a second mailbox. Codex owns native spawn, message,
follow-up, wait, interrupt, and list operations. Harness feature-detects that
capability before governor launch, journals each collaboration item, projects
human-readable sender/receiver lifecycle state, and routes operator steering to
the governor rather than making the operator schedule children directly.

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

The global live-thread and per-run discovery limits are controller-enforced in
v1. The depth, per-parent, and lifetime-created recommendations remain defense
in depth for a follow-up controller admission guard; exceeding a live limit is
detected from the native spawn event and pauses the run.

### 8.5 Fresh-review contexts

Independent verification and final audit should be new controller-created read-only sessions, not child agents of the implementer. This avoids inherited anchoring and makes independence visible in state/evidence.

---

## 9. Git and worktree custody

### 9.1 Registered repository model

V1 registers an existing local clone as the **coordination repository**. It
remains on its declared primary branch, clean, and synchronized with its base
reference per repository policy. BILDR uses its shared Git object database to
create managed worktrees under the XDG data root.

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

| Kind         | Branch                                 |     Mutable | Purpose                                         |
| ------------ | -------------------------------------- | ----------: | ----------------------------------------------- |
| inspection   | detached base                          |          no | architecture/context/search                     |
| task attempt | `agent/hc/<run>/<task>-a<n>`           |         yes | one worker task                                 |
| verifier     | detached task head or integration head |          no | independent audit                               |
| integration  | `agent/hc/<run>/integration`           | yes, serial | accepted commits/generation/conflict resolution |

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

An initial strict-profile serial set can include:

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

The coordination checkout is never an execution workspace and is never cleaned
by BILDR. Repository preflight requires it to be clean. Every mutable agent and
integration command runs in a controller-created worktree.

During retries, the controller keeps the current and immediately previous task
worktrees so the next attempt can inspect its direct predecessor. It removes
older superseded worktrees only when Git reports them clean and no live agent,
path lease, or explicit operator preservation applies. Branches, commits,
attempt records, findings, and content-addressed evidence remain durable.

After a successful run reaches `COMPLETED`, the controller removes every safe
managed worktree with non-forced `git worktree remove`, records each database
tombstone, and runs `git worktree prune`. Cleanup uses one background hygiene
lane, so large deletions neither block orchestration nor compete with each
other. A failed removal is not overridden; the cleanup report becomes
`attention_required` and identifies the retained worktree.

The controller binds this policy when it creates the run. Upgrading BILDR does
not retroactively delete worktrees from runs created under an older policy.

No worktree is removed when it has:

- a live process/session;
- active lease;
- unarchived uncommitted diff;
- a HEAD that differs from the controller's durable record;
- preservation flag;
- unresolved evidence reference.

Controller commands receive command-scoped temporary, home, cache, config,
data, and state directories unless the command explicitly needs an allowlisted
host location. After required stdout and stderr are copied into the artifact
store, the controller discards the command spool. This rule is
build-system-neutral: ignored build output lives inside the disposable
worktree, whether a repository uses Cargo, npm, Gradle, CMake, or another tool.

Global compiler, package-manager, container, and operating-system cache pruning
is never implicit. Those caches can be shared with unrelated work and need a
separate operator-reviewed storage policy.

---

## 10. Context engineering and high-bandwidth operations

### 10.1 Objective

The context engine should prevent every agent from repeatedly rediscovering a 2.7 GB repository. It supplies the smallest authority-first packet that is likely to be sufficient and makes that packet inspectable.

### 10.2 Context layers

1. **Permanent repository policy**
   - contributor guidance, product contracts, documentation authority rules,
     and protected semantics.
2. **Repository map**
   - path domains, workspaces/packages, deployables, generated/serial paths, code-navigation seeds, CI claims/validators.
3. **Task packet**
   - objective, non-goals, exact base/dependencies, owned paths, success/negative tests, metrics, evidence, stop conditions.
4. **Selected source context**
   - active authorities, producer/consumer definitions, relevant tests, interfaces, recent failure evidence.
5. **Execution evidence**
   - commands, logs, findings, partial diff, prior attempt limits.

Do not continuously append all layers and all prior turns into one unbounded conversation. Use fresh verifier/integrator threads and compact task-specific handoffs.
Codex memory may inform future heuristics after a run, but immediate retry and
supervision state is Harness-owned durable data and must not depend on
asynchronous memory generation.

#### Prompt assembly contract

- `thread/start` receives one stable developer policy: role, instruction trust,
  read/write autonomy, external-action boundaries, controller ownership, and
  evidence-grounded reporting. Repository text and task-specific context never
  enter this layer.
- `turn/start` receives the volatile repository evidence, task packet, current
  controller facts, and requested output exactly once. Put reusable source
  evidence before the volatile context receipt, and put the objective and action
  request after long evidence so the model sees the decision it must make last.
- State each rule once. Specify the outcome, why it matters, hard constraints,
  approval boundary, success evidence, and output contract. Prescribe a tool or
  step sequence only when the controller has evidence that the sequence is
  required; otherwise allow the selected model to choose the implementation.
- Supply structured-output schemas through the protocol instead of copying them
  into prompt prose. Expose only tools relevant to the role and sandbox, with
  concise tool descriptions.
- Ground long-run progress and completion claims in observed tool results.
  Do not request hidden reasoning transcripts. Do not repeat remaining-token or
  context-window countdowns in prose; the controller enforces budgets outside
  the task narrative.
- Keep the core contract model-neutral. Add a model-specific instruction only
  for a measured failure on representative runs, and remove obsolete tuning
  when stronger default behavior makes it unnecessary.

### 10.3 Repository map v1

Build deterministic indexes from an exact SHA:

- `git ls-files` inventory, path/language/size/last-change metadata;
- active documentation links and status/classification from routers;
- excluded archive/vendor/binary/generated classes;
- `cargo metadata` for each detected workspace, packages, targets, features, dependencies;
- known component/profile/validator catalog;
- repository CI claim, workflow, fixture, and quarantine registries;
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
  "run_id": "RUN-20260805-014",
  "task_id": "CORE-002",
  "base_sha": "...",
  "profile_digest": "sha256:...",
  "instruction_sources": [
    { "path": "CONTRIBUTING.md", "sha256": "...", "class": "repository_policy" }
  ],
  "authorities": [
    {
      "path": "ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md",
      "sha256": "..."
    }
  ],
  "checklist_rows": ["CORE-..."],
  "owned_paths": ["crates/harness-orchestrator/..."],
  "code_seeds": [{ "path": "...", "symbols": ["..."] }],
  "test_seeds": ["cargo test -p ..."],
  "dependency_contracts": [],
  "prior_evidence": [],
  "excluded": [
    {
      "pattern": "docs/archive/**",
      "reason": "historical, non-authoritative by default"
    }
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

Use the profile's T0–T6 definitions:

| Tier | Harness interpretation                                     |
| ---- | ---------------------------------------------------------- |
| T0   | deterministic algorithm/state/repository topology proof    |
| T1   | exact canonical shape and negative rejection               |
| T2   | component API with real local dependencies                 |
| T3   | property/fuzz/parser/state breadth                         |
| T4   | ordering/cancellation/backpressure/fault behavior          |
| T5   | named OS/SDK/codec/accelerator/database/hardware target    |
| T6   | exact-candidate live/capacity/recovery/rollout/fleet proof |

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
gate (`review_ready` or `integration`)
evidence class (`custody`, `contract`, or `behavioral`)
```

The selector broadens on unknown paths. The controller records why a validator was selected or not selected. Proof tiers remain claim-specific: a T5 hardware observation does not substitute for a configured T2 component check. A gate consumes explicit selected validator IDs at the exact SHA, not “anything at or above a tier.” The default sequence keeps expensive validation on the integrated head; `review_ready` validation is opt-in for cheap, stable checks so provisional code is not surrounded by a large brittle suite.

Profiles may also define path-selected acceptance entries. `automated` entries
run through the same controller-owned evidence path. `attested` entries remain
pending in the signoff packet until the local user records target identity and
observed behavior against the exact integration SHA. Missing tools, runners,
devices, or attestations are never converted to success.

`validation_policy.require_draft_pr_ci` is explicit and defaults false. A
profile that enables it treats an empty required-check set, an unreadable PR
head, or a PR head different from the integration SHA as non-proof. A profile
that leaves it disabled does not strand the run waiting for CI it never
declared and does not promote tasks through `CI_PROVEN`.

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

The App Server protocol exposes input, cached input, cache-write input, output, reasoning-output, total, last-call/total usage, and context window in the pinned source version. A Codex turn may contain many model calls between tools. Deduplicate notifications with the monotonic thread total, sum every distinct last-call sample into the durable turn total, and retain per-agent attribution.

Reasoning-output tokens are an output breakdown and must not be charged twice.

### 12.2 API-equivalent pricing

Configuration includes immutable effective-dated snapshots. The initial 2026-08-05 example is:

| Model | Input / 1M | Cached / 1M | Output / 1M |
| ----- | ---------: | ----------: | ----------: |
| Sol   |      $5.00 |       $0.50 |      $30.00 |
| Terra |      $2.00 |       $0.20 |      $12.00 |
| Luna  |      $0.20 |       $0.02 |       $1.20 |

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

| Risk     | Examples                                                                     | Default                                         |
| -------- | ---------------------------------------------------------------------------- | ----------------------------------------------- |
| low      | read worktree, bounded search, controller-selected local test                | trusted/automatic under policy                  |
| medium   | network-enabled package fetch, new tool invocation, broader local command    | explicit once                                   |
| high     | write outside task scope, push branch, create PR, destructive cleanup        | individual explicit approval                    |
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

## 15. Strict repository profile

### 15.1 Profile purpose

A profile is a versioned policy adapter. It tells the controller how a
repository routes authority, paths, proof, roles, and resources. It does not
copy repository documentation or become an alternative product contract.

The supplied `profiles/bildr/profile.toml` is a strict example for this
repository. The `general` profile remains the neutral default.

### 15.2 Authority chain

The BILDR profile loads and hashes:

```text
CONTRIBUTING.md
ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md
docs/STYLE_GUIDE.md
SECURITY.md
```

A different repository declares its own authorities. Archived plans and audits
remain supporting evidence unless an active authority explicitly promotes them.

### 15.3 Domain routing

The strict example defines four domains:

| Domain | Paths | Primary authority |
| --- | --- | --- |
| Controller | `crates/**`, `bins/**`, migrations | architecture and security contracts |
| Browser | `ui/**` | UI wireframes and acceptance plan |
| Contracts | schemas, OpenAPI, generated bindings, profiles | architecture and protocol mapping |
| Delivery | workflows, packaging, build tasks | contributor and operations guidance |

Cross-domain task classification unions the relevant authorities and raises
risk. Repository profiles can add or replace domains without changing the
controller.

### 15.4 Protected semantics

Every mutable packet preserves these rules:

- Keep the service bound to localhost.
- Bind evidence, review, and publication to the exact candidate SHA.
- Keep mutable work isolated by task and worktree lease.
- Require explicit approval for external writes.
- Never merge automatically.
- Treat missing, stale, or inconclusive proof as unsatisfied.
- Do not claim completion beyond executed proof.

These guardrails do not prohibit reasonable implementation changes. When a plan
or task constraint prevents the stated objective, review must identify the
constraint and revise the plan instead of preserving a deadlock.

### 15.5 Validation and test economics

The strict profile selects validators by changed path:

- Rust changes run the workspace test suite on the integrated head.
- Browser changes run type checks, unit tests, and a production build.
- Contract changes run schema, API, and generated-binding checks.
- Browser changes also run an end-to-end acceptance flow.
- Every change receives exact-head diff custody and required draft-PR CI.

Do not surround provisional code with a broad regression suite. First exercise
the implementation through the authoritative pipeline and confirm that it meets
the objective. Add focused durable tests after the code shape is accepted. Test
authoritative behavior and reject invalid classes generically; do not encode a
catalog of every discarded intermediate shape.

The `general` profile fails closed for code integration until the repository
provides a behavioral validator. Patch-format success alone is never completion
proof.

### 15.6 Completion authority

- The architect maps tasks to repository completion criteria when relevant.
- A worker handoff recommends only the state justified by its evidence.
- The controller and independent reviewers validate evidence.
- BILDR changes a repository checklist only when the objective explicitly owns
  that edit.
- A completed run does not automatically complete a repository milestone.
- Environment, live, and rollout requirements remain explicit proof limits.

### 15.7 Runtime state and resources

Runtime prompts, inboxes, and worktrees are disposable execution state, not
authority. Keep them outside registered repositories under XDG directories.
Never commit `.harness-runtime`.

Resource classes are independent of role slots:

```text
control:  short Git, schema, and static commands
medium:   focused language and contract checks
heavy:    workspace builds and integrated test suites
hardware: exclusive environment or device proof with explicit readiness
```

Multiple roles can reason concurrently while one heavy validator uses the build
slot.

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

Do not use a registered production checkout for routine controller unit or UI
development.

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
completed removable worktrees    immediately after durable signoff
failed/preserved worktrees        no automatic removal
pricing/profile/schema snapshots  permanent while referenced
```

Only the checkout is disposable. Git commits, branch refs, task records,
evidence, and explicit operator pins remain. Dirty, active, leased, or pinned
worktrees are retained and reported for attention.

Run archival is a non-destructive terminal-state transition. Only completed,
canceled, or failed runs may be archived; their manifests, usage, events, and
preserved worktrees remain addressable. The ordinary run selector excludes
archived runs unless the operator explicitly enables archived history.

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
If an interrupted governor has a valid progressing checkpoint, preserve the old
attempt and worktree, release its leases, and enqueue a bounded continuation
automatically. The human must not have to repair an internal task state or
restate the governor's next action after a daemon restart.

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
10. strict repository context/profile;
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
pub enum AgentRole {
    Architect,
    PlanReviewer,
    Explorer,
    Governor,
    Worker,
    HighRiskWorker,
    Integrator,
    Verifier,
    FinalAuditor,
    CiTriage,
}
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
12. repository pilot ladder.

Release must fail if any fault path can produce false `VERIFIED`, `PUBLICATION_READY`, or `COMPLETED` state.

---

## 22. Repository pilot and rollout plan

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

Goal: environment-specific lane selection and honest result semantics.
Unavailable targets remain non-green; live production actions remain
operator-controlled.

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

| Risk                                             | Mitigation                                                                       |
| ------------------------------------------------ | -------------------------------------------------------------------------------- |
| App Server protocol changes rapidly              | pin version/schema, generated bindings, raw events, golden traces, fail closed   |
| child agents appear isolated but share workspace | top-level worktree per mutable task; children read-only by policy                |
| repository context is too large                  | authority router, exact repo map, bounded probe, fresh role-specific threads     |
| agent claims outrun proof                        | controller validation, exact SHA, independent verifier, proof-tier UI            |
| expensive Sol/Terra use                          | role routing, Luna bounded workers, cached stable context, per-task budgets      |
| subscription usage differs from API price        | label API-equivalent estimate, store source snapshot/rate limits separately      |
| hidden process mutates after cancellation        | process groups, reconciliation, lease held until safe                            |
| primary repo damaged                             | coordination lock, no edits, managed worktrees, exact preflight                  |
| parallel tasks conflict semantically             | DAG + path/symbol/serial risk; bounded 3 workers; serial integration             |
| logs leak secrets                                | allowlist/redaction, local permissions, retention, sanitized export              |
| DB/event volume grows                            | compact projections, virtualized UI, artifact spill, reference-aware retention   |
| UI becomes overwhelming                          | list+inspector default, progressive disclosure, action-oriented status           |
| Harness becomes a second repository authority    | profile import only, explicit completion authority, no automatic checklist state |

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
9. Default strict-profile capacity: three mutable, one verifier, six total threads.
10. Sol architect/verifier/final audit; Luna bounded work; Terra risk/escalation/integration.
11. Authority-first context and no vector DB in v1.
12. REST + durable SSE; WebSocket only for explicit human terminal later.
13. Exact path leases and serial paths; primary checkout remains clean.
14. API-equivalent cost from immutable price snapshots; no fake billing precision.
15. No raw hidden reasoning retention by default.
16. No automatic push/PR; explicit user gate. Draft PR is maximum publication action in v1.
17. No automatic merge.
18. The registered repository retains its declared completion authority.
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

BILDR v1 is done when all of the following are true:

### Runtime

- pinned App Server initializes, streams, steers, interrupts, resumes, and exposes goals/usage/subagents;
- schema mismatch disables execution without losing historical UI;
- daemon restart reconciles nonterminal runs.

### Repository custody

- primary coordination checkout remains clean on its declared branch;
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

- user sees the governor's latest update, active/idle posture, model/effort, token use, and meaningful activity without navigating tabs;
- delegated child threads expose their status and latest message in the same pane, while steer/continue remain governor-routed;
- governor completion reconciles active children, renders the task as `Waiting on you`, and prevents late item events from reopening terminal child state;
- the run inspector exposes a live human-readable Git custody state, diff totals and changed paths, and only claims a single PR association when it is unambiguous;
- approvals render inline in Runs; usage groups by account, repository, and agent;
- repository discovery, governor selection, steer, interrupt, retry, and integration review work;
- keyboard/accessibility/reconnect/large-log paths pass.

### Security/operations

- localhost/CSRF/path/process/secret/artifact tests pass;
- no raw hidden reasoning by default;
- systemd install/upgrade/backup/doctor/runbook work on Fedora/Nobara and Ubuntu/Debian;
- no automatic merge or production action path exists.

### Repository pilots

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
profiles/bildr/profile.toml
schemas/harness.orchestration.task.v1.schema.json
schemas/harness.orchestration.handoff.v1.schema.json
schemas/harness.evidence.v1.schema.json
migrations/0001_initial.sql
openapi/harness-api.yaml
runtime/roles/*.toml
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

### Repository authority consulted

- `README.md`
- `CONTRIBUTING.md`
- `ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md`
- `SECURITY.md`
- `docs/TEST_AND_ACCEPTANCE_PLAN.md`
- `docs/UI_WIREFRAMES.md`

The repository profile must read its declared authorities from the exact run
base instead of assuming an earlier snapshot remains current.
