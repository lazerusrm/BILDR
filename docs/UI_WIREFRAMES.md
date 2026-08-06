# Harness Console UI and Interaction Specification

**Status:** implementation-ready UX specification
**Target:** local Linux browser/PWA, desktop-first
**Benchmark:** the clarity of the Codex app's project/thread/diff experience, extended with orchestration, worktree, evidence, usage, and approval controls

## 1. UX principles

1. **The current truth is always visible.** Every mutable agent row shows its task, current goal, model, reasoning effort, worktree, branch, base/head SHA, current action, heartbeat, token use, and estimated cost.
2. **Actions, not hidden thoughts.** Show plan steps, reasoning summaries, command/file/tool activity, review findings, and model reroutes. Do not imply access to private hidden chain-of-thought. Raw reasoning storage is disabled by default.
3. **One primary workspace.** The default run view is a dense but readable list-and-inspector layout. A DAG is optional, not the first screen.
4. **Progressive disclosure.** The overview shows state and risk; the inspector reveals raw commands, diffs, evidence, and event payloads.
5. **Failures remain inspectable.** Failed agents and preserved worktrees do not disappear. The UI explains whether the failure is source, infrastructure, inconclusive, interrupted, superseded, or policy-blocked.
6. **Human control is immediate.** Steer, interrupt, approve, escalate, reassign, preserve, compare, and open-editor actions are one or two clicks away.
7. **No false green.** A passed local test, completed agent response, verified task, integrated branch, exact-head CI, and live proof are visibly distinct states.
8. **Keyboard-first, mouse-friendly.** Common navigation and approval operations have shortcuts; every action remains accessible without memorizing them.

## 2. Information architecture

```text
Harness Console
├── Home
│   ├── repositories
│   ├── active runs
│   ├── recent runs
│   └── host/runtime health
├── Repository
│   ├── overview
│   ├── new run
│   ├── authority/context map
│   ├── worktrees
│   ├── validation catalog
│   └── profile settings
├── Run workspace
│   ├── overview/list
│   ├── task graph
│   ├── changes
│   ├── evidence
│   ├── approvals
│   ├── usage
│   └── run settings
├── Global approval center
├── Host/runtime
│   ├── Codex App Server
│   ├── process/resource usage
│   ├── local runners/hardware
│   └── logs
└── Settings
    ├── Codex and model defaults
    ├── pricing snapshots
    ├── retention
    ├── security
    └── appearance/accessibility
```

## 3. Desktop shell

Recommended dimensions assume a 1440×900 or larger desktop. The layout remains usable at 1180 pixels; below that, the inspector becomes a drawer.

```text
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Harness Console  NeuralMatrix ▾  Run NM-20260805-014  base 4d6662e  ● App Server  Slots 4/6  $18.42  ⚠2 │
├──────────────┬─────────────────────────────────────────────────────────────────────┬───────────────────────┤
│ NAVIGATION   │ RUN OVERVIEW                                                        │ INSPECTOR             │
│              │                                                                     │                       │
│ Home         │  Production evidence identity hard-cut                              │ Task MEDIA-002        │
│ Repositories │  EXECUTING  5/9 tasks verified  2 running  1 blocked                │ Activity | Plan | ... │
│  NeuralMatrix│                                                                     │                       │
│ Runs         │  ┌────────────────────────────────────────────────────────────────┐ │ Goal                  │
│  ● current   │  │ Architect  SOL · xhigh · read-only               COMPLETE      │ │ Enforce exact ...     │
│  ○ prior     │  │ task graph 9 tasks · 6 authorities · 82k tokens · $4.17       │ │                       │
│ Approvals 2  │  └────────────────────────────────────────────────────────────────┘ │ Luna · max            │
│ Worktrees    │                                                                     │ workspace-write       │
│ Evidence     │  ┌────────────────────────────────────────────────────────────────┐ │ 38.2k / 80k tokens    │
│ Usage        │  │ MEDIA-001  Exact event camera identity           VERIFIED      │ │ $0.11 API equiv.      │
│ Host         │  │ Terra · xhigh  agent/media-001  5e21c91                    ✓    │ │                       │
│ Settings     │  ├────────────────────────────────────────────────────────────────┤ │ Current action        │
│              │  │ MEDIA-002  C2 media projection                   RUNNING       │ │ cargo test -p ...     │
│              │  │ Luna · max  agent/media-002  command 7m12s      38.2k  $0.11   │ │ elapsed 07:12         │
│              │  ├────────────────────────────────────────────────────────────────┤ │                       │
│              │  │ IOS-001    Consume canonical identity             BLOCKED      │ │ Worktree             │
│              │  │ waiting for MEDIA-002                                              /data/.../MEDIA-002  │
│              │  └────────────────────────────────────────────────────────────────┘ │ branch · SHA          │
│              │                                                                     │ files +4/-1            │
├──────────────┴─────────────────────────────────────────────────────────────────────┴───────────────────────┤
│ Event stream connected · cursor 14921 · DB WAL healthy · primary checkout clean · origin/main unchanged   │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Persistent top bar

Left to right:

- product/repository selector;
- run selector and immutable base SHA;
- App Server health and version/schema badge;
- agent slots and local resource slots;
- run-level input/output/reasoning tokens and API-equivalent cost;
- pending approval count, highest risk first;
- global command palette;
- user/local-session menu.

Clicking the base SHA opens a compact provenance card: requested ref, resolved SHA, fetch timestamp, authority digest, Codex version, protocol-schema digest, profile version, and pricing snapshot IDs.

### Left rail

The left rail stays narrow. It is navigation, not a second status dashboard. Counts appear only when actionable: running agents, approvals, failed validations, retained worktrees.

## 4. Home screen

```text
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ Good afternoon                                    New run                            │
│                                                                                     │
│ ACTIVE                                                                          2   │
│ NeuralMatrix · Exact event evidence       EXECUTING     2 agents     $18.42      ›   │
│ NeuralMatrix · CI credibility remediation VERIFYING     1 verifier   $ 6.10      ›   │
│                                                                                     │
│ REPOSITORIES                                                                         │
│ NeuralMatrix  main @ 4d6662e  clean  14 managed worktrees  profile healthy      ›   │
│                                                                                     │
│ HOST                                                                                 │
│ Codex 0.x pinned · schema matched · 3/6 agent slots · heavy runner idle · DB 1.4 GB  │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

The primary action is **New run**. There is no empty generic chat box on the home screen; requests are always scoped to a registered repository and base.

## 5. New-run composer

```text
┌────────────────────────────────────────────────────────────────────────────┐
│ New NeuralMatrix run                                                       │
│                                                                            │
│ Objective                                                                  │
│ ┌────────────────────────────────────────────────────────────────────────┐ │
│ │ Audit and hard-cut event media camera identity across edge and C2...  │ │
│ └────────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│ Base          origin/main      resolved after Fetch & inspect              │
│ Run mode      ○ Plan only  ● Plan + implement  ○ Review existing branch    │
│ Risk posture  ● NeuralMatrix strict    mutable workers 3    verifier 1      │
│ Publication   ● Local only  ○ Draft PR after approval                      │
│                                                                            │
│ Advanced ▸  model overrides · budgets · allowed network · existing PR      │
│                                                                            │
│                                      Cancel      Fetch, pin, and inspect → │
└────────────────────────────────────────────────────────────────────────────┘
```

After fetch, show the exact SHA and any dirty-primary or remote-divergence problem before enabling **Start architecture**. The system never silently changes the requested base.

## 6. Run workspace

### 6.1 Run header

The run header contains:

- objective and optional user-edited display title;
- phase/state and progress counters;
- immutable base and current integration head;
- total duration, active time, tokens, estimated cost;
- buttons: **Pause scheduling**, **Stop run**, **Open run folder**, **Export evidence**;
- contextual primary action, such as **Review plan**, **Approve integration**, or **Create draft PR**.

### 6.2 Agent/task row

Every row must fit the information below without opening the inspector:

```text
[status] TASK-ID  title
         role badge · model · effort · sandbox · parent/subagent count
         current goal or current action
         worktree short path · branch · head SHA · files +N/-N
         token progress / budget · API-equivalent cost · elapsed · heartbeat
         dependency/approval/validation chips
```

Example:

```text
● MEDIA-002  Project canonical camera identity through C2
  worker · Luna · max · workspace-write · 2 read-only subagents
  Running focused c2-api contract tests
  …/MEDIA-002 · agent/hc/nm-014/MEDIA-002 · a1b9c02 · 4 files +183/-72
  38.2k / 80k · $0.11 · 12m 44s · heartbeat 8s                       [1 approval]
```

### 6.3 Status vocabulary

Use stable text and icons, not color alone:

- `QUEUED`, `WAITING_DEPENDENCY`, `LEASED`, `STARTING`, `RUNNING`, `WAITING_APPROVAL`, `STEERED`;
- `REVIEW_READY`, `VERIFYING`, `VERIFIED`, `CHANGES_REQUESTED`;
- `INTEGRATING`, `INTEGRATED`, `CI_PROVEN`, `LIVE_PROVEN`, `CLOSED`;
- `BLOCKED`, `NEEDS_HELP`, `STALLED`, `INTERRUPTED`, `FAILED`, `SUPERSEDED`, `CANCELED`.

A separate result badge uses NeuralMatrix semantics: `SUCCESS`, `NOT_SELECTED`, `SOURCE_FAILURE`, `INFRASTRUCTURE_UNAVAILABLE`, `INCONCLUSIVE`, `CANCELLED_SUPERSEDED`, `SKIPPED_DRAFT`, `QUARANTINED_FAILURE`.

## 7. Inspector

The right inspector keeps its selected agent while the center list updates. Tabs:

### Activity

A virtualized chronological stream combining:

- current goal and goal updates;
- plan-step transitions;
- concise reasoning summaries;
- commands with live elapsed time and bounded output preview;
- file reads/searches and file changes;
- subagent spawn/wait/resume/complete events;
- model reroutes and effective-effort changes;
- token samples and context compaction;
- approvals and decisions;
- validation/evidence events;
- final agent messages.

Default filters hide low-value repeated reads. A **Protocol** toggle exposes raw App Server notifications for debugging.

### Plan

Shows the agent plan as a checklist with state, not as prose buried in a transcript. The controller task packet appears above it, clearly distinguished as authoritative execution scope.

### Diff

- aggregate task diff against the task base SHA;
- per-file tree with added/deleted counts;
- side-by-side and unified views;
- comments/bookmarks local to Harness Console;
- badges for leased, serial, forbidden, generated, or unexpected paths;
- one-click **Compare with verified commit**, **Open in editor**, and **Copy file:line**;
- binary and large generated files display metadata, not inline blobs.

### Files

Shows reads, searches, writes, and ownership:

```text
Path                                               Access      Lease             Last activity
central/rust-c2/src/event_media.rs                  read/write  MEDIA-002         14:38:12
shared/contracts/generated/event_media.ts           attempted   SERIAL/denied     14:29:03
docs/architecture/PRODUCT_EVIDENCE_CONTRACT.md      read        authority         14:17:44
```

### Commands

A table with command, cwd, resource class, duration, exit, result classification, and stdout/stderr artifact links. Live output is streamed only for the selected command; full logs are retained as artifacts according to policy.

### Evidence

Shows claims, proof tier, exact SHA, validator, runner/device, result class, artifacts, and unproved claims. It must never collapse multiple proof tiers into one green check.

### Usage

```text
Model                  Turns  Input   Cached  Cache write  Output  Reasoning  API-equiv.
gpt-5.6-luna / max        14  31.0k    18.2k       2.1k     7.2k      3.8k       $0.11
Read-only subagents         2  12.4k     8.1k       0.0k     2.0k      0.7k       $0.03
Total                       16  43.4k    26.3k       2.1k     9.2k      4.5k       $0.14
```

Show requested and effective model separately when rerouted. For ChatGPT/Codex subscription authentication, label the dollar figure **API-equivalent estimate**, not invoice cost.

### Context

Shows exactly what was supplied:

- instruction-source digest;
- authority documents and digests;
- selected code/test files;
- repository-map snapshot;
- task packet version;
- context-token estimate;
- excluded files and why;
- compaction events.

This screen is essential for debugging agent quality without exposing hidden reasoning.

## 8. Subagent presentation

Subagents are grouped beneath their parent and collapsed by default:

```text
MEDIA-002  Luna · max                                 RUNNING
├─ explore-auth-paths    Luna · medium · read-only    COMPLETE
├─ inspect-contract-test Luna · medium · read-only    COMPLETE
└─ ci-triage             Luna · high · read-only      WAITING
```

Selecting a subagent uses the same inspector. The parent row aggregates subagent tokens and cost while preserving an expandable breakdown. A graph view is available from **Task graph**, but the list remains the operational default.

## 9. Approval center

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ Approvals  2                                                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│ HIGH  Push integration branch to origin                                     │
│ Run NM-014 · integration @ f219ac1 · 9 verified commits                     │
│ Scope: external write · Git remote                                           │
│ [View diff] [Deny] [Approve once]                                            │
├──────────────────────────────────────────────────────────────────────────────┤
│ MEDIUM  Run command with network enabled                                     │
│ MEDIA-004 · cargo fetch --locked · worktree only                             │
│ [View command/environment] [Deny] [Approve once]                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

Approval cards show:

- requesting agent/task and exact worktree;
- command or file change in full;
- target, network requirement, and paths affected;
- policy reason and risk;
- whether the approval is one-time, turn-scoped, task-scoped, or unavailable by policy.

There is no global **approve everything** action. High-risk external writes and production/hardware actions are always individual decisions.

## 10. Changes and integration view

```text
Task commits                              Integration
✓ MEDIA-001 5e21c91                       base 4d6662e
✓ MEDIA-002 a1b9c02                       applied MEDIA-001
✓ TEST-001  8a910af                       applying MEDIA-002 ...
! IOS-001   changes requested             pending TEST-001

Serial path queue                         Evidence invalidated
1. shared generated contracts             MEDIA-001 T1 after regeneration
2. MASTER_COMPLETION_CHECKLIST             none: update not yet authorized
```

The integrator view makes dependency order and evidence invalidation explicit. It shows conflict resolution as a first-class event; no semantic conflict can be dismissed as a mechanical merge without new proof.

## 11. Evidence view

Two modes:

- **Claim matrix:** checklist rows/claims × proof tiers with exact result and SHA.
- **Artifact browser:** logs, test reports, diffs, manifests, screenshots, model responses, review findings, and exported bundles.

Example matrix:

```text
Claim          T0     T1       T2       T5       T6      Highest valid state
MEDIA-ID-01    pass   pass     pass     n/a      absent  INTEGRATION_PROVEN
JETSON-ROI-04  pass   pass     pass     infra    absent  INTEGRATION_PROVEN
```

`infra` is not green. Hover/click explains the required device or runner and the exit condition.

## 12. Usage and cost dashboard

The run-level view contains:

- total tokens by model and role;
- input, cached input, cache-write input, output, and reasoning-output breakdown;
- estimated cost by task, parent/subagent, phase, model, and day;
- cost per verified task and per accepted line as optional diagnostics;
- long-context multiplier warnings;
- budget trajectory and projected upper bound;
- rate-limit windows when available;
- model-reroute history.

Avoid gamifying low token use. The objective is predictable cost per accepted result, not minimum tokens at the expense of proof.

## 13. Host/runtime view

Shows:

- `harnessd` version, uptime, event-loop lag, memory, open file count;
- Codex binary version, App Server PID, protocol schema digest, restart count;
- SQLite WAL size, projection lag, last backup/export;
- registered repository primary checkout state;
- active worktrees and disk use;
- control/medium/heavy/hardware resource queues;
- container engine, `git`, `gh`, Rust, Node build-tool versions;
- configured hardware targets and readiness, without treating absence as success.

## 14. Interaction controls

### Agent controls

- **Steer:** inject a correction into the active turn; saved as an event.
- **Interrupt:** request turn interruption, then terminate the process group only after a grace period.
- **Retry with evidence:** create a new attempt with the failure, command artifacts, and a revised bounded objective. Never resend the same prompt blindly.
- **Escalate model:** Luna → Terra, preserving task and evidence but starting a fresh thread when context anchoring is a concern.
- **Request independent review:** start the configured verifier in read-only mode.
- **Preserve worktree:** prevent cleanup.
- **Open editor/terminal:** human-owned local action; does not grant an agent extra permission.

### Run controls

- pause/resume scheduling;
- stop after current commands;
- cancel immediately with explicit warning;
- re-plan remaining tasks from current integration SHA;
- export run/evidence;
- approve integration;
- approve push/draft PR;
- close while preserving selected worktrees.

## 15. Keyboard shortcuts

```text
Ctrl/Cmd+K      command palette
G then H        Home
G then R        current Run
G then A        Approvals
J / K           next/previous task
Enter           open task inspector
S               steer selected agent
I               interrupt selected agent
D               open diff
E               open evidence
Shift+A         approve selected low/medium request after confirmation
?               shortcut help
```

Do not assign a single-key shortcut to destructive, push, or production actions.

## 16. Responsive behavior

At 1180–1439 pixels, collapse the left rail to icons. At 800–1179 pixels, the inspector becomes a right drawer. Below 800 pixels, the UI supports monitoring and approvals but disables complex side-by-side diff; it is not optimized for implementing from a phone.

## 17. Accessibility

- WCAG 2.2 AA contrast target;
- semantic headings, landmarks, tables, and live regions;
- complete keyboard navigation and visible focus;
- status text/icon in addition to color;
- reduced-motion preference;
- high-density and comfortable-density modes;
- command output and diff views work with screen readers in simplified mode;
- no continuously flashing token or elapsed-time indicators.

## 18. Visual language

- neutral dark and light themes;
- one accent for selection, not one color per model;
- model shown as compact text badges: `SOL`, `TERRA`, `LUNA`;
- effort shown beside model: `xhigh`, `max`;
- risk colors reserved for approvals/findings/results;
- monospace only for code, SHAs, paths, command output, and metrics;
- eight-pixel spacing grid, compact 32–36 pixel task rows in dense mode;
- animation only for state transitions and active execution, never decorative background motion.

## 19. UI implementation choices

Recommended frontend:

- React + TypeScript + Vite;
- TanStack Router and Query;
- Radix-style accessible primitives;
- `react-virtuoso` or equivalent for event and command streams;
- a maintained Monaco-compatible diff renderer only for diffs, not a full built-in IDE;
- xterm.js only for explicit human terminal sessions;
- Server-Sent Events for durable state/event updates;
- WebSocket only for interactive terminal byte streams.

The production build is embedded into the Rust daemon so runtime installation does not require Node.js.

## 20. UX acceptance gates

The UI is ready for the first NeuralMatrix pilot only when a user can:

1. register the repository and see a clean/pinned base;
2. start one architect thread and watch plan, actions, tokens, and goal live;
3. see every mutable task's worktree/branch/SHA before it writes;
4. distinguish parent agents from subagents and inspect both;
5. review a task diff and unexpected-path policy result;
6. approve or deny a command/file/external-write request;
7. interrupt, steer, retry with evidence, and escalate an agent;
8. see per-turn and aggregate API-equivalent cost with the price snapshot;
9. inspect exact-SHA validations and proof limits;
10. restart `harnessd` and resume the same run without losing event history;
11. export a self-contained run/evidence manifest;
12. reach draft-PR approval without any automatic merge or hidden repository mutation.
