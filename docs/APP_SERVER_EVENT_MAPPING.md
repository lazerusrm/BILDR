# Codex App Server Integration and Event Mapping

**Status:** implementation contract for the pinned Codex release
**Transport:** one supervised `codex app-server` child over JSONL stdio
**Compatibility rule:** generated schema and exact Codex version are pinned; execution fails closed on an unapproved schema mismatch

## 1. Why App Server is the integration boundary

Harness Console needs more than a final response. It needs live thread, turn, plan, item, command, file-change, approval, diff, review, goal, token-usage, model-reroute, and subagent lifecycle events. The App Server protocol exposes those primitives directly and supports thread creation/resumption, turn start/steer/interrupt, review, approvals, and generated protocol schemas.

The high-level SDK may be used for small utilities or tests, but `harnessd` integrates with App Server directly so the GUI does not reconstruct state by scraping terminal output.

## 2. Process topology

```text
harnessd
  ├─ AppServerSupervisor
  │    └─ codex app-server  (stdin/stdout JSONL, stderr captured separately)
  ├─ JsonRpcClient
  ├─ RawEventWriter
  ├─ EventProjector
  └─ Command/API handlers
```

Rules:

- exactly one App Server process per `harnessd` instance in v1;
- use stdio, not an exposed network listener;
- assign every outbound request a controller-generated request ID;
- capture stdout protocol frames and stderr diagnostics independently;
- never parse human terminal formatting;
- start no agent execution until initialization, version check, and schema check pass;
- record the App Server PID, Codex version, build metadata when available, and protocol-schema SHA-256 on each run.

## 3. Schema lifecycle

At build/release time:

```bash
codex app-server generate-json-schema --out ./generated/codex-app-server-schema
codex app-server generate-ts --out ./generated/codex-app-server-ts
sha256sum ./generated/codex-app-server-schema/*
```

The Rust implementation should generate or hand-maintain only the small stable envelope types and use build-generated protocol bindings for the pinned release. Every inbound frame is also retained as raw JSON, which makes unknown additive fields recoverable even when the typed projector does not yet understand them.

Startup compatibility states:

| State | Behavior |
|---|---|
| exact version and schema | read/write execution enabled |
| configured compatible version with identical schema | enabled, warning recorded |
| additive unknown event but valid base envelope | raw capture enabled; projection marks unknown; execution continues only if compatibility policy allows |
| request/response or required event shape mismatch | execution disabled; historical UI remains read-only |
| App Server unavailable | daemon remains up; run start disabled; diagnostic shown |

Never silently switch to terminal scraping as a fallback.

## 4. Initialization

The supervisor starts App Server, sends the required initialize request, waits for successful initialization, then publishes `runtime.ready`. The service name should identify Harness Console. Client capabilities and experimental flags must be explicit and versioned.

Initialization failure classes:

- binary missing;
- version rejected;
- protocol framing failure;
- initialization response error;
- schema mismatch;
- authentication unavailable;
- process exits before ready.

These are runtime failures, not source failures in a NeuralMatrix task.

## 5. Thread mapping

Each controller-created agent session owns one primary Codex thread.

```text
agent_sessions.id        internal durable identity
codex_threads.thread_id  App Server identity
codex_threads.parent...  parent when emitted for native subagent/review threads
```

For mutable work, `thread/start` receives:

- `cwd`: exact task worktree;
- requested model and reasoning effort;
- sandbox mode and approval policy;
- service name;
- repository instruction sources as supported by the pinned version;
- task-specific developer instructions/context packet;
- metadata linking run/task/attempt/worktree.

For read-only agents, use the inspection/integration worktree as appropriate with read-only sandbox policy.

After start, immediately call the goal API so the UI and token/time budgets have a first-class current goal rather than inferring one from prose.

## 6. Goal mapping

Use the App Server goal methods as the live execution contract:

```text
thread/goal/set
thread/goal/get
thread/goal/clear
```

Internal projection:

| App Server field | Harness field |
|---|---|
| objective | `agent_sessions.current_goal` |
| status | goal status/event badge |
| tokenBudget | `agent_sessions.token_budget` |
| tokensUsed | goal usage progress |
| timeUsedSeconds | goal elapsed active time |

The controller updates the goal on task assignment, remediation, integration phase changes, and user steering that materially changes the objective. Small conversational steering is recorded without rewriting the authoritative task packet.

## 7. Turn control

Supported controller operations:

- start a turn with model/effort/cwd/sandbox/output schema;
- steer an active turn;
- interrupt an active turn;
- start a review turn/thread;
- resume a prior thread after daemon restart;
- list/filter threads for recovery and reconciliation.

The daemon enforces one active mutable turn per task. A native subagent may run concurrently only within the configured total thread limit and inherited sandbox boundary.

### Interrupt sequence

1. send App Server interrupt;
2. mark task `INTERRUPTING`;
3. wait a configurable grace period;
4. terminate the child command process group only when exposed/owned by the runtime and still active;
5. preserve partial diff, command logs, raw events, and worktree;
6. classify the attempt `INTERRUPTED`, not failed verification.

## 8. Event ingestion pipeline

```text
JSONL frame
  -> framing/size validation
  -> append raw_events transaction
  -> publish durable cursor
  -> typed projection
  -> domain events
  -> SSE fan-out
```

The raw append occurs before projection so projector crashes cannot lose protocol evidence. Projection is idempotent and replayable from a raw-event cursor.

Required envelope controls:

- maximum frame size;
- JSON depth/size limits;
- UTF-8 validation;
- request/notification distinction;
- duplicate sequence detection where a source sequence exists;
- timestamp on receipt using monotonic + wall-clock correlation;
- payload SHA-256;
- redaction pass only for UI/log derivatives, never destructive mutation of the original encrypted/private local artifact when policy permits retention.

Raw reasoning content is not retained by default even if the protocol can carry it. The ingest layer drops or replaces it with a metadata-only event according to configuration.

## 9. Notification-to-domain mapping

Exact method names and fields are generated from the pinned schema. The table below defines semantic categories, not an excuse to hard-code unverified names.

| App Server category | Harness domain event | Projection/use |
|---|---|---|
| thread started/updated/status | `agent.thread.*` | state, source kind, git info, waiting-on-approval |
| turn started/completed/error | `agent.turn.*` | active state, duration, terminal classification |
| turn plan updated | `agent.plan.updated` | checklist plan in inspector |
| turn diff updated | `agent.diff.updated` | fast diff badge; Git remains source of final diff truth |
| model reroute | `agent.model.rerouted` | requested/effective model and cost attribution |
| token usage updated | `usage.sampled` | per-turn delta, context use, cost estimate |
| item started/updated/completed | `agent.item.*` | activity timeline and item-specific projections |
| approval request/resolution | `approval.*` | approval center and blocked state |
| context compaction | `agent.context.compacted` | context inspector and quality diagnostics |
| goal set/update/clear | `agent.goal.*` | current goal and budget card |
| collab/subagent spawn/wait/resume/end | `agent.child.*` | parent/child tree, model/effort/effective status |
| review item/finding | `review.*` | finding list and verdict |
| rate-limit update | `usage.rate_limit.*` | host/run usage view |

## 10. Item-type mapping

The App Server may expose item variants such as agent messages, plan items, reasoning summaries, command execution, file changes, MCP/dynamic tool calls, collaborative-agent calls, web searches, review findings, and compaction. Map them into a stable internal union:

```rust
pub enum ActivityItemKind {
    AgentMessage,
    ReasoningSummary,
    Plan,
    Command,
    FileRead,
    FileChange,
    Search,
    ToolCall,
    SubagentCall,
    WebSearch,
    ReviewFinding,
    ContextCompaction,
    Unknown(String),
}
```

Every projected item keeps the raw payload event ID. Unknown variants render in the protocol/debug view and never disappear.

## 11. Subagent mapping

Native Codex subagent events include parent/sender identity, prompt/role/nickname, requested model and effort, effective model and effort, new thread identity, and terminal status where supported by the pinned release.

Projection rules:

- create a child `agent_session` with `runtime_kind = codex_native_subagent`;
- attach it to the controller-created parent session;
- inherit the parent task and worktree;
- mark sandbox as inherited, never independently broader;
- record requested and effective model/effort;
- aggregate usage into the parent task but retain a child breakdown;
- do not grant a separate path lease for a native child unless the controller has an explicit supported mechanism; native children are assumed to share the parent workspace.

This is why write-owning parallel tasks are top-level controller sessions in separate worktrees. Native children are best used for bounded read-only exploration, command/log analysis, and independent reasoning within that task.

## 12. Token usage mapping

Store both cumulative and last-turn values if supplied. The durable cost ledger uses per-turn `last_token_usage` or a monotonic delta, never repeatedly charges cumulative totals.

Expected token components:

```text
input_tokens
cached_input_tokens
cache_write_input_tokens  (when available)
output_tokens
reasoning_output_tokens   (an output breakdown, not an additional billable class)
total_tokens
```

Validation invariants:

```text
0 <= cached_input_tokens <= input_tokens
0 <= cache_write_input_tokens <= input_tokens
0 <= reasoning_output_tokens <= output_tokens
normal_uncached_input = max(input - cached - cache_write, 0)
```

When a field is missing, preserve `NULL`; do not invent zero when it changes cost confidence.

## 13. Approval mapping

Approval requests are projected with:

- exact thread/turn/item;
- operation class: command, file change, network, external write, or other;
- command/path/target details;
- sandbox and policy reason;
- risk level computed by Harness policy;
- decisions allowed by the App Server request shape;
- expiration/turn scope when present.

The UI decision is sent back through the exact App Server response method. The daemon records the human decision before forwarding it, then records the runtime acknowledgment. A failure to deliver the decision leaves the request unresolved and visible.

## 14. Diff truth

App Server diff events provide low-latency UI updates. Before verification or integration, the Worktree Manager independently computes:

```bash
git diff --binary --find-renames <task-base-sha>...HEAD
git diff --check <task-base-sha>...HEAD
git status --porcelain=v2
```

Git output, not the streamed diff item, is the acceptance source of truth. The two are reconciled; a mismatch is a runtime diagnostic.

## 15. Process failure and recovery

On App Server exit:

1. stop dispatching new actions;
2. mark live sessions `RUNTIME_DISCONNECTED` without destroying their task/worktree state;
3. persist stderr and exit status;
4. restart with bounded backoff;
5. initialize and verify version/schema;
6. list/resume known threads;
7. reconcile active turn status and outstanding approvals;
8. replay raw events/projections to connected UIs;
9. require human review if a mutable command may have outlived the lost protocol session.

Automatic restart is limited. Repeated incompatibility or authentication failure leaves execution disabled rather than looping indefinitely.

## 16. Testing the adapter

Build a `fake-app-server` test binary that replays golden JSONL traces and accepts scripted requests. Required scenarios:

- initialize and one successful thread/turn;
- plan, command, file change, diff, usage, and final message;
- approval accepted and denied;
- steer and interrupt;
- parent plus two subagents with different effective models;
- model reroute;
- context compaction;
- App Server crash mid-command and recovery;
- duplicated event/sequence;
- unknown additive item;
- malformed/oversized frame;
- schema mismatch;
- cumulative token update followed by last-turn update;
- daemon restart and thread reconciliation.

Golden traces are sanitized and versioned by Codex release. Adapter acceptance requires all traces plus a live smoke against the pinned local `codex app-server`.
