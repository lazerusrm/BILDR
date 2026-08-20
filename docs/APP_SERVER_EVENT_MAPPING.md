# Codex App Server Integration and Event Mapping

**Status:** implementation contract for the pinned Codex release
**Transport:** one supervised `codex app-server` child over JSONL stdio
**Compatibility rule:** generated schema and exact Codex version are pinned; execution fails closed on an unapproved schema mismatch

## 1. Why App Server is the integration boundary

BILDR needs more than a final response. It needs live thread, turn, plan, item,
command, file-change, approval, diff, review, goal, token-usage, model-reroute,
and subagent lifecycle events. The App Server protocol exposes those primitives
directly and supports thread creation/resumption, turn start/steer/interrupt,
review, approvals, and generated protocol schemas.

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

The supervisor starts App Server, sends the required initialize request, waits
for successful initialization, then publishes `runtime.ready`. The service
name identifies BILDR. Client capabilities and experimental flags must be
explicit and versioned.

Initialization failure classes:

- binary missing;
- version rejected;
- protocol framing failure;
- initialization response error;
- schema mismatch;
- authentication unavailable;
- process exits before ready.

These are runtime failures, not source failures in a repository task.

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
- one stable developer policy containing the role, trust boundary, autonomy,
  external-action boundary, and evidence-grounding rules;
- metadata linking run/task/attempt/worktree.

The first `turn/start` carries the volatile context packet, objective, task or
review data, and output request exactly once. Repository content never enters
`developerInstructions`, and a schema supplied through the protocol is not
reproduced in prompt prose. This separation keeps the instruction hierarchy
honest and preserves a reusable prompt prefix.

For a retry, Harness also injects a bounded controller-owned continuity packet
containing the prior terminal classification, model/effort outcome, operator
guidance, and the declared durable handoff or final agent message. Harness does
not replay raw reasoning or an unbounded transcript, and independent verifier
threads continue to start fresh.

When the approved task packet names a controller or governor owner, this
primary thread is projected as `governor` and uses the profile's dedicated
Sol xhigh governor route. Native child threads remain visible under it. Harness
sends two deterministic progress audits during a long turn. The first checks
whether activity is producing code or behavioral evidence; the second asks the
governor to materialize one concrete outcome and return a tool-grounded
checkpoint. Exact token counts remain controller telemetry rather than
model-facing countdown prose, and the controller still hard-stops at the
configured boundary.

At App Server initialization Harness reads `experimentalFeature/list` and
requires an enabled, non-deprecated `multi_agent` or `multi_agent_v2` capability
before it launches a governor. This is capability detection, not automatic
experimental-feature enablement. Codex remains the owner of its hosted mailbox
operations; Harness observes and governs them through protocol events.

When an incomplete productive governor returns without a source diff and the
repository, account, model, sandbox, and worktree custody are unchanged,
Harness starts another bounded `turn/start` on the same thread. A failed warm
continuation falls back to the controller-owned bounded handoff and a fresh
attempt. Verifier, integrator, and changed-custody work always use fresh
threads.

If the App Server continues a governor past 110 percent of its declared token
budget, Harness interrupts the turn after the 90-percent handoff checkpoint and
records `agent.governor.budget_hard_stop`. The preserved worktree and handoff
remain retryable; the scheduler does not silently fund an unbounded turn.

Codex 0.148.0 announces delegated children through parent-thread
`item/started` or `item/completed` notifications whose item type is
`subAgentActivity`. Harness uses `agentThreadId` as the child thread identity
and `agentPath` as its display name, then attributes subsequent child-thread
events and token usage to that visible child session. The enforced
`<family>_<effort>__<purpose>` child name supplies the requested route until a
runtime effective-model event is available; unstructured names fall back to
the parent route rather than guessing. The older
`thread/started.thread.parentThreadId` shape remains supported.

The governor remains an active App Server turn while a native `wait_agent`
tool call is pending, but no model tokens are generated during the blocked
wait. Harness prompts governors to use a five-minute wait rather than repeated
default-interval polls; native child completion resumes the parent early. No
external hook or synthetic keepalive turn is used.

For Codex 0.148.0, the `turn/start` response can race with a
`turn/started` notification and the two observed turn IDs are not assumed to be
interchangeable. The notification is authoritative for `active_turn_id`; a
late response may fill an empty projection but must never overwrite that
notification. Steering and interruption always use the projected notification
ID.

When the approved objective requires GitHub, the controller must first pass the
authenticated GitHub capability probe and then start the Codex turn with
`sandboxPolicy.networkAccess: true`. A task that does not require an external
network remains network-isolated. This prevents a Harness-created DNS denial
from being misreported as a bad credential.

The 0.148.0 App Server is launched with
`sandbox_workspace_write.network_access=true` so the initial thread context and
native children do not materialize the default network-off sandbox before the
first `turn/start` override arrives. Harness still sends
`networkAccess: false` for every non-GitHub turn. This changes only the network
boundary; filesystem writes remain restricted to the leased worktree.

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

A failed or governor-interrupted native child is terminal only for that child.
It must not transition the parent task to `NEEDS_HELP`, release the parent path
lease, fail the attempt, or preserve the governor's active worktree. The
governor checkpoint receives the child disposition and remains responsible for
reconciling or replacing the bounded assignment.

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
| collab/subagent spawn/message/follow-up/wait/interrupt/list/end | `agent.child.*` | readable governor collaboration timeline, parent/child tree, model/effort/effective status |
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

Store both cumulative and last-call values if supplied. In Codex 0.148.0,
`tokenUsage.last` describes one model call inside a turn; tool-heavy turns can
emit many distinct calls. The durable ledger deduplicates notifications by the
monotonic `tokenUsage.total` counter, then sums each distinct `last` call into
one turn total. It never charges the cumulative value directly.

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
