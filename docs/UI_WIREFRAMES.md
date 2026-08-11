# BILDR UI and Interaction Specification

**Status:** implementation-ready UX specification
**Target:** local Linux browser/PWA, desktop-first
**Benchmark:** the clarity of the Codex desktop app's project/thread experience, extended with bounded orchestration and exact-head custody

> **Current interaction contract (2026-08-07).** The governor is the primary
> human conversation. The persistent navigation is Home, Repositories, Runs,
> Usage, Host, and Settings. Approvals are handled inline in Runs; validation
> evidence and worktree custody remain controller-owned backend concepts, not
> dedicated navigation destinations. This current contract supersedes older
> screen sketches below wherever they show separate Approvals, Worktrees, or
> Evidence pages or a multi-tab run inspector. The run workspace places the
> authoritative **Goal** and current **Plan** above agent rows, keeps prior
> failed/stalled architecture attempts in collapsed history, and uses a strong
> selected-row treatment so the inspector's subject is unambiguous.

## 1. UX principles

1. **The current truth is always visible.** Every mutable agent row shows its task, model, reasoning effort, current action, active/idle state, token use, and estimated cost; lower-level custody detail stays available without dominating the screen.
2. **Actions, not hidden thoughts.** Show plan steps, reasoning summaries, command/file/tool activity, review findings, and model reroutes. Do not imply access to private hidden chain-of-thought. Raw reasoning storage is disabled by default.
3. **One primary workspace.** The default run view is a dense but readable list-and-inspector layout. A DAG is optional, not the first screen.
4. **Progressive disclosure.** The overview shows state and risk; selecting a child thread changes the same activity pane and always provides an obvious route back to the governor.
5. **Failures remain inspectable.** Failed agents and preserved worktrees do not disappear. The UI explains whether the failure is source, infrastructure, inconclusive, interrupted, superseded, or policy-blocked.
6. **Human control is immediate.** Steer, interrupt, continue, and approve actions live beside the governor activity that requires them.
7. **No false green.** A passed local test, completed agent response, verified task, integrated branch, exact-head CI, and live proof are visibly distinct states.
8. **Keyboard-first, mouse-friendly.** Common navigation and approval operations have shortcuts; every action remains accessible without memorizing them.

## 2. Current information architecture

```text
BILDR
├── Home
│   ├── repositories
│   ├── active runs
│   ├── recent runs
│   └── host/runtime health
├── Repositories
│   ├── discovered local checkouts
│   ├── registration and health
│   └── new run
├── Runs
│   ├── governor and task rows
│   ├── latest governor update and meaningful activity
│   ├── inspectable delegated child threads
│   ├── governor steering/continuation
│   └── inline approvals
├── Usage
│   ├── by Codex account
│   ├── by repository
│   └── by agent
├── Host/runtime
│   ├── Codex App Server
│   ├── process/resource usage
│   ├── local runners/hardware
│   └── logs
└── Settings
    ├── plan approval, governor autonomy, and budgets
    ├── Codex account names and authentication
    ├── account handoff
    ├── retention
    ├── security
    └── appearance/accessibility
```

## 3. Desktop shell

Recommended dimensions assume a 1440×900 or larger desktop. The layout remains usable at 1180 pixels; below that, the inspector becomes a drawer.

```text
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BILDR           Example repo ▾  Run RUN-20260805-014 base 4d6662e  ● App Server  Slots 4/6  $18.42  ⚠2 │
├──────────────┬─────────────────────────────────────────────────────────────────────┬───────────────────────┤
│ NAVIGATION   │ RUN OVERVIEW                                                        │ INSPECTOR             │
│              │                                                                     │                       │
│ Home         │  Exact-head validation wiring                                       │ Task CORE-002         │
│ Repositories │  EXECUTING  5/9 tasks verified  2 running  1 blocked                │ Activity | Plan | ... │
│  Example repo│                                                                     │                       │
│ Runs         │  ┌────────────────────────────────────────────────────────────────┐ │ Goal                  │
│  ● current   │  │ Architect  SOL · xhigh · read-only               COMPLETE      │ │ Enforce exact ...     │
│  ○ prior     │  │ task graph 9 tasks · 6 authorities · 82k tokens · $4.17       │ │                       │
│ Approvals 2  │  └────────────────────────────────────────────────────────────────┘ │ Luna · high           │
│ Worktrees    │                                                                     │ workspace-write       │
│ Evidence     │  ┌────────────────────────────────────────────────────────────────┐ │ 38.2k / 80k tokens    │
│ Usage        │  │ CORE-001   Bind evidence to integration SHA      VERIFIED      │ │ $0.11 API equiv.      │
│ Host         │  │ Terra · xhigh  work/core-001  5e21c91                      ✓    │ │                       │
│ Settings     │  ├────────────────────────────────────────────────────────────────┤ │ Current action        │
│              │  │ CORE-002   API evidence projection               RUNNING       │ │ cargo test -p ...     │
│              │  │ Luna · high work/core-002  command 7m12s        38.2k  $0.11   │ │ elapsed 07:12         │
│              │  ├────────────────────────────────────────────────────────────────┤ │                       │
│              │  │ IOS-001    Consume canonical identity             BLOCKED      │ │ Worktree             │
│              │  │ waiting for CORE-002                                               /data/.../CORE-002   │
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

### Codex account and limits strip

Directly below the top bar, show the selected local Codex account, its plan,
and each backend-exposed rate-limit window as remaining percentage plus reset
time. The selector switches between detected and Harness-managed `CODEX_HOME`
profiles only when no agent session is active and includes **Add Codex
account**. Device authorization is initiated by the installed Codex binary;
Harness shows only the OpenAI verification URL and one-time code. Missing
windows say that no limit was exposed; the UI never invents a remaining value.
Live/stale state is based on the last successful App Server observation.
Hourly burn and exhaustion forecasts blend the local 24-hour and recent
four-hour trends with the current provider-window average; longer observations
carry more weight so brief activity bursts do not dominate the estimate.
Credentials stay inside private Codex homes and are never rendered by Harness.

### Left rail

The left rail stays narrow. It is navigation, not a second status dashboard. Counts appear only when actionable: running agents, approvals, failed validations, retained worktrees.

## 4. Home screen

```text
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ Good afternoon                                    New task                           │
│                                                                                     │
│ ACTIVE                                                                          2   │
│ Example repo · Exact event evidence       EXECUTING     2 agents     $18.42      ›   │
│ Example repo · CI credibility remediation VERIFYING     1 verifier   $ 6.10      ›   │
│                                                                                     │
│ REPOSITORIES                                                                         │
│ Example repo  main @ 4d6662e  clean  14 managed worktrees  profile healthy      ›   │
│                                                                                     │
│ HOST                                                                                 │
│ Codex 0.x pinned · schema matched · 3/6 agent slots · heavy runner idle · DB 1.4 GB  │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

The primary action is **New task**. There is no empty generic chat box on the home screen; requests are always scoped to a registered repository and base.

### Dirty-checkout onboarding

When repository inspection is blocked by a dirty primary checkout and no run
has used that registration, the repository row offers **Create clean
checkout**. The modal suggests a sibling directory, requires a destination that
does not exist, and states that the selected source checkout is not modified.
Harness clones the configured coordination branch, verifies the same origin,
Git identity, clean state, and authority contract, then replaces the existing
registration. The modal also explains that the source checkout supplies shared
Git objects and must remain in place.

## 5. New-task composer

```text
┌────────────────────────────────────────────────────────────────────────────┐
│ New repository task                                                        │
│                                                                            │
│ Objective                                                                  │
│ ┌────────────────────────────────────────────────────────────────────────┐ │
│ │ Audit and bind signoff evidence to the integrated exact head...       │ │
│ └────────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│ Interview     □ Deep interview before planning                             │
│ Base          origin/main      resolved after Fetch & inspect              │
│ Account       ● Automatic best available  ○ Work  ○ Personal               │
│ Plan approval ● Review before work  ○ Approve certified plan automatically │
│ Risk posture  ● Repository strict      mutable workers 3    verifier 1      │
│ Publication   ● Local only  ○ Draft PR after approval                      │
│                                                                            │
│ Advanced ▸  model overrides · budgets · allowed network · existing PR      │
│                                                                            │
│                                      Cancel      Fetch, pin, and inspect → │
└────────────────────────────────────────────────────────────────────────────┘
```

Every new task intends implementation. After fetch, show the exact SHA and any
dirty-primary or remote-divergence problem before enabling **Start
architecture**. Manual plan approval is the default; automatic approval is an
explicit global preference and per-task override. Both postures run the same
independent adversarial certification loop; automatic approval only removes the
human click after a plan reaches `CERTIFIED` with zero blocking findings and
passes the controller's budget, risk, serial-path, and model-diversity gates. It
never converts schema validity into approval. The system never silently changes
the requested base.

**Deep interview before planning** is optional and off by default. When
selected, the prepared run enters **Clarifying intent** instead of starting the
architect. The selected governor model uses a read-only repository view and
asks one material question at a time. The workspace shows the question,
rationale, current concise brief, **Continue interview**, **Use brief and
plan**, and **Skip interview**. While a turn is active, the panel shows its
current activity, elapsed time, and live input, cached-input, output,
reasoning-output, and turn-total token counters. Confirmation starts a fresh architect thread
with the original objective and confirmed brief. The transcript remains in run
history and is not planning input. Automatic plan approval does not answer or
confirm interview questions.

Creating a task and starting its architect are visibly distinct. A prepared
task shows a prominent **Waiting to start** architecture status and says that
planning has not begun. After **Start architecture**, the same surface becomes
an animated **Planning** status and shows the architect's latest action. The
right inspector mirrors that state instead of falling back to ambiguous copy
such as `Select a task` or `No active turn`.

If a completed architect response uses a repository-native or otherwise invalid
plan shape, the surface remains in **Planning** while a focused output repair
re-expresses the completed work in the controller schema. It shows the repair
turn's live activity and usage instead of asking the operator to buy the same
repository investigation again. Repeated invalid repairs stop with the exact
rejection and an explicit retry action.

Mechanical packet fields owned by the controller are canonicalized without a
new model turn. The UI must not present that canonicalization as renewed
architecture work or increase the displayed attempt count. A follow-up repair
turn is reserved for substantive or non-adaptable output defects.

After the architect proposes a plan, the same surface advances through
**Adversarial review** and, when blockers exist, **Revising plan** before
returning to review. The plan card explains whether the reviewer is checking
feasibility/liveness, whether the architect is addressing concrete findings, or
whether the digest is **Certified**. Approval controls are absent until
certification. Review/revision retries remain automatic while capacity and the
total run ceiling permit them. A verdict-only continuation reuses the same
reviewer row and thread; the UI labels it **Finalizing review** and does not
present it as a fresh repository review. Reaching the authoritative run ceiling
stops the active turn and shows the run as paused rather than leaving a working
spinner. A repeated/oscillating or non-shrinking finding history stops as
**Planning needs a decision** rather than consuming the rest of the ceiling.
The plan card shows the certificate, inspected-file/critical-path evidence,
advisory findings, revision history, planning spend, execution reserve, and the
reason automatic approval was deferred. The operator can **Request
changes** with a concrete blocking correction; Harness sends it through the same
replacement-plan and re-certification loop. An explicit **Approve with budget
override** action is shown only when the controller's remaining-budget check is
the blocker.

## 6. Run workspace

### 6.1 Run header

The run header contains:

- objective and optional user-edited display title;
- phase/state and progress counters;
- immutable base and current integration head;
- total duration, active time, tokens, estimated cost;
- buttons: **Pause scheduling**, **Stop run**, **Open run folder**, **Export evidence**;
- contextual primary action, such as **Review plan**, **Approve integration**, or **Create draft PR**.

A persistent **Governor sessions** switcher sits above the run header. It lists
every available run by human title and current state, shows active and total
counts, and keeps **New task** adjacent. Switching sessions swaps the center
workspace, inspector, usage, and event stream together; an explicit loading
state prevents the previous governor's detail from being shown under the new
selection. The switcher stays pinned to the top of the run workspace while the
operator scrolls and labels the selection as `Viewing run N of M`, so similar
task titles cannot make the current governor ambiguous. Run numbers follow
creation order—Run 1 is the first-created session—even when the backend returns
newest activity first.

Submitting **New task** creates the prepared run and immediately launches either
its optional read-only interview turn or its read-only architecture turn in the
same user action. Confirming or skipping the interview launches architecture;
the corresponding start controls remain as recovery actions when startup could
not be admitted. Completed,
canceled, or failed runs expose **Archive run**; archived runs keep their durable
history and preserved worktrees, disappear from the switcher by default, and
remain available behind **Show archived**.

### 6.2 Agent/task row

Every row must fit the information below without opening the inspector:

```text
[status] task title
         role badge · model · effort · sandbox · parent/subagent count
         current goal or current action
         thread usage / budget · API-equivalent cost
         dependency/approval/validation state
```

Controller-owned tasks display the role as **Governor**, the number and state
of delegated child threads, and the governor's current reconciliation action.
The task row, needs-help handoff, and default inspector selection always target
the root governor rather than the most recently created child. Child threads
show their own thread tokens and API-equivalent cost directly in the delegated
thread list. They are selectable for read-only activity inspection, but Continue, Steer, and
Interrupt remain governor controls so the operator does not become the child
scheduler.
Governor message previews follow new messages while the operator remains near
the bottom. Manual scrollback suspends that behavior for twelve seconds. The
full message-history surface opens at the latest message by default and uses
the same reading grace period.
When the governor returns control, the task reads **Waiting on you**. A child
that is still closing reads **Finishing** until the controller reconciles it;
late activity must not reopen a completed child as running. The same inspector
contains timestamped Governor Messages and one Work status card with live Git
custody state, diff totals, a bounded changed-file list, and single- versus
multi-PR scope. Raw run IDs, worktree paths, branches, and commit hashes are
not primary UI copy.
The Goal and Plan cards show the governor's bounded milestone ledger, including
completed, active, pending, and blocked outcomes. A single governor task is not
displayed as a single generic step. Continue accepts optional brief steering;
when left blank, Harness selects the next action from controller-owned progress
and recent valid handoffs. Human prose never silently replaces the goal.
`WAITING_RESOURCE` means the deterministic controller is retrying a required
capability such as authenticated GitHub API access; no model turn or token
budget is consumed in that state.

Example:

```text
● CORE-002  Project exact-head evidence through the API
  worker · Luna · high · workspace-write · 2 read-only subagents
  Running focused API contract tests
  …/CORE-002 · work/run-014/CORE-002 · a1b9c02 · 4 files +183/-72
  38.2k / 80k · $0.11 · 12m 44s · heartbeat 8s                       [1 approval]
```

### 6.3 Status vocabulary

Use stable text and icons, not color alone:

- `QUEUED`, `WAITING_DEPENDENCY`, `LEASED`, `STARTING`, `RUNNING`, `WAITING_APPROVAL`, `STEERED`;
- `REVIEW_READY`, `VERIFYING`, `VERIFIED`, `CHANGES_REQUESTED`;
- `INTEGRATING`, `INTEGRATED`, `CI_PROVEN`, `LIVE_PROVEN`, `CLOSED`;
- `BLOCKED`, `NEEDS_HELP`, `STALLED`, `INTERRUPTED`, `FAILED`, `SUPERSEDED`, `CANCELED`.

A separate result badge uses controller result semantics: `SUCCESS`,
`NOT_SELECTED`, `SOURCE_FAILURE`, `INFRASTRUCTURE_UNAVAILABLE`,
`INCONCLUSIVE`, `CANCELLED_SUPERSEDED`, `SKIPPED_DRAFT`, and
`QUARANTINED_FAILURE`.

## 7. Inspector

The right inspector keeps its selected agent while the center list updates. Its
default surface is a single plan-first scroll rather than a multi-tab diagnostic
console. The first card is **Plan progress**: a compact checklist of every
governor milestone with `Completed`, `In progress`, `Pending`, or `Blocked`
status, followed by the governor and delegated threads currently assigned to
the work and their present actions. This replaces the low-value Recent activity
timeline so goal progress is visible at a glance.

### Detailed activity

Detailed messages remain available from Governor Messages or the selected child
thread. Diagnostic activity may combine:

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

### Plan progress

Shows the agent plan as a checklist with state, not as prose buried in a
transcript. It is the inspector's primary glance surface, not a separate tab.
The controller task packet remains authoritative execution scope.

### Diff

- aggregate task diff against the task base SHA;
- per-file tree with added/deleted counts;
- side-by-side and unified views;
- comments/bookmarks local to BILDR;
- badges for leased, serial, forbidden, generated, or unexpected paths;
- one-click **Compare with verified commit**, **Open in editor**, and **Copy file:line**;
- binary and large generated files display metadata, not inline blobs.

### Files

Shows reads, searches, writes, and ownership:

```text
Path                                               Access      Lease             Last activity
crates/harness-api/src/lib.rs                       read/write  CORE-002          14:38:12
generated/protocol/example.json                     attempted   SERIAL/denied     14:29:03
ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md             read        authority         14:17:44
```

### Commands

A table with command, cwd, resource class, duration, exit, result classification, and stdout/stderr artifact links. Live output is streamed only for the selected command; full logs are retained as artifacts according to policy.

### Evidence

Shows claims, proof tier, exact SHA, validator, runner/device, result class, artifacts, and unproved claims. It must never collapse multiple proof tiers into one green check.

### Execution signoff

After integrated-head validation begins, show one controller-assembled packet:

- exact integration SHA and packet digest;
- every selected validator with evidence class (`custody`, `contract`, or
  `behavioral`), proof tier, result, and artifact links;
- platform acceptance items as `passed`, `pending attestation`, `attested`, or
  `not selected`, with selection tied to changed paths;
- final-auditor structured evidence and blocking/advisory findings;
- exact-head unproved claims and total run spend.

`HUMAN_REVIEW` must visibly rest. Its primary actions are **Approve signoff**
and **Request changes**. Approval binds to the displayed packet digest and SHA.
A rejection requires an affected repository file plus a behavioral correction
so the controller can reopen the owning task without discarding unrelated
verified work. Attested acceptance requires target/device identity and observed
behavior; a checkbox with no observation is not evidence. In
`DRAFT_PR_CREATED`, show required-CI status and a refresh action while the
controller polls; never imply that CI authorizes merge.

### Usage

```text
Model                  Turns  Input   Cached  Cache write  Output  Reasoning  API-equiv.
gpt-5.6-luna / high       14  31.0k    18.2k       2.1k     7.2k      3.8k       $0.11
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
CORE-002   Luna · high                                RUNNING
├─ explore-auth-paths    Luna · medium · read-only    COMPLETE
├─ inspect-contract-test Luna · medium · read-only    COMPLETE
└─ ci-triage             Luna · high · read-only      WAITING
```

Selecting a subagent uses the same inspector. The parent row aggregates subagent tokens and cost while preserving an expandable breakdown. A graph view is available from **Task graph**, but the list remains the operational default.

Run labels show effective operator posture rather than only the persistent
lifecycle state: for example, an unfinished `EXECUTING` run is presented as
**WORKING**, **PAUSED**, or **WAITING ON YOU** according to its live turns,
scheduler, approvals, and task handoffs. New task creation exposes the total run
budget. Governor continuation defaults to **Adaptive** and offers a bounded
additional-budget slider for unusually long attempts.

Plan progress starts with the complete goal rollup (completed phases and total
outcomes), then shows every phase state, followed by the selected phase's
granular outcomes. A selected phase count must never be presented as if it were
the count for the entire plan.

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
│ CORE-004 · cargo fetch --locked · worktree only                              │
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
✓ CORE-001  5e21c91                       base 4d6662e
✓ CORE-002  a1b9c02                       applied CORE-001
✓ TEST-001  8a910af                       applying CORE-002 ...
! IOS-001   changes requested             pending TEST-001

Serial path queue                         Evidence invalidated
1. shared generated contracts             CORE-001 T1 after regeneration
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

Settings exposes a compact **Planning and governor autonomy** section with
manual-by-default or automatic plan approval, automatic continuation, adaptive
budgeting, the no-progress token envelope, the per-attempt
ceiling, the next recommended attempt budget, sample count, and a plain-language
reason. The recommendation is advisory and always bounded by the operator's
ceiling. Productive same-thread continuation is visible in the activity
timeline but does not present a false `NEEDS HELP` interruption. A cold
rollover is labeled separately as a bounded-handoff recovery.
Verifier findings and controller strategy corrections use the same automatic
continuation posture and remain visible in the timeline without presenting a
routine Resume action.
The plan-approval control says **Approve certified plan automatically** and
links to the invariant that every plan first passes independent adversarial
review. The timeline shows proposed digest, reviewer findings, replacement
revision, certification, and approval as distinct events.

Settings also lists detected and Harness-managed Codex accounts. Friendly names
may be edited for either kind. Managed accounts can be re-authenticated or
removed; externally detected homes remain owned by their source. Automatic
capacity handoff selects another ready account only between attempts, never by
transplanting an active thread.

### Agent controls

- **Steer:** inject a correction into the active turn; saved as an event.
- **Interrupt:** request turn interruption, then terminate the process group only after a grace period.
- **Retry with evidence:** create a new attempt with the failure, command artifacts, and a revised bounded objective. Never resend the same prompt blindly.
- **Continue:** resume the unchanged goal from the structured milestone ledger;
  operator text is optional steering context, not a required recovery script.
- Runtime status labels context `fresh independent`, `native thread retained`,
  or `bounded handoff`, and explains the reuse decision.
- **Escalate model:** Luna → Terra, preserving task and evidence but starting a fresh thread when context anchoring is a concern.
- **Request independent review:** start the configured verifier in read-only mode.
- **Preserve worktree:** prevent cleanup.
- **Open editor/terminal:** human-owned local action; does not grant an agent extra permission.

### Run controls

- pause/resume scheduling;
- when a bounded governor window or run budget is exhausted, place the next
  window selector immediately beside **Resume work**; preselect the adaptive
  recommendation and allow a bounded operator override without requiring
  written steering. The selected value is the governor allowance; Harness
  reserves bounded child-thread headroom separately so ordinary delegation does
  not immediately exhaust the run cap. Manual continuation additions extend
  through 50m, and the control shows the projected lifetime run cap before the
  operator resumes;
- new-task budget selection is labeled **Total run ceiling** and explains that
  it covers planning, governor work, children, and review. Per-turn adaptive
  slices are internal checkpoints and automatically continue productive work;
  they are not separate user approvals or invitations to restate the goal;
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

The UI is ready for the first repository pilot only when a user can:

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
