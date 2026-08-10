# BILDR test and acceptance plan

**Status:** implementation and release contract

## 1. Test philosophy

BILDR controls mutable source work, local processes, approvals, evidence, and
Git publication. A green UI is not sufficient. Tests must establish
deterministic controller behavior, exact worktree custody, protocol
replayability, cost-accounting correctness, restart recovery, and fail-closed
security.

Use the lowest credible proof tier for each claim and preserve integration/system tests for cross-component behavior. Avoid arbitrary sleeps; use virtual time, controlled clocks, temporary repositories, scripted fake App Server processes, and process barriers.

## 2. Component test matrix

| Component | Primary tests | Critical negative cases |
|---|---|---|
| App Server adapter | golden JSONL replay, live pinned-version smoke | malformed frames, unknown events, schema mismatch, crash/restart |
| Event store/projector | append/replay/idempotence, cursor/SSE | duplicate events, projection crash, DB busy, WAL recovery |
| Controller state machine | property/model tests | illegal transition, duplicate terminal action, stale attempt |
| Scheduler | deterministic queue/resource tests | starvation, over-capacity, stale lease, heavy-lane collision |
| Worktree manager | temp Git repositories | dirty primary, stale base, path escape, concurrent mutation, failed cleanup |
| Lease/policy engine | glob/property tests | overlap, serial path, forbidden path, symlink escape |
| Context compiler | fixture repos and digests | archive selected as authority, oversized/vendor ingestion, stale index |
| Command runner | process-group and timeout tests | orphan child, output flood, signal race, cwd escape, network policy |
| Usage/cost | decimal/property tests | double-counting, cumulative reset, missing cache write, reroute |
| Approval broker | request/decision lifecycle | duplicate decision, delivery failure, expired request, broad approval |
| Validation/evidence | exact-SHA fixtures | missing infrastructure marked success, stale artifact, hash mismatch |
| Integration manager | multi-commit temp repo | semantic conflict, dependency order violation, evidence invalidation |
| UI | Playwright and accessibility | stale stream, reconnect, large logs/diffs, keyboard-only, mobile monitoring |
| Packaging | clean Fedora/Ubuntu VM | missing Node at runtime, systemd restart, XDG permissions, upgrade migration |

## 3. Controller state-machine properties

Use a model-based/property test harness. Invariants:

- a task cannot be `RUNNING` without a live attempt, worktree, lease, and thread;
- no two active mutable tasks hold overlapping write leases;
- a task cannot become `VERIFIED` from its own worker verdict;
- integration accepts only verified commits on the expected base/dependency chain;
- a source/head change invalidates affected exact-head evidence;
- `COMPLETED` requires no unresolved required approval/finding/validation;
- canceled/failed tasks preserve their final diff/worktree record;
- a retry increments attempt and never mutates the prior attempt record;
- `INFRASTRUCTURE_UNAVAILABLE` and `INCONCLUSIVE` cannot satisfy required proof;
- no controller transition updates repository completion authority automatically;
- push/PR/merge cannot occur without their explicit human gate; merge is absent in v1.

Generate random legal/illegal action sequences and verify the database plus emitted domain events.

## 4. Git/worktree scenarios

Create temporary repositories with branches, remotes, submodules, symlinks, lockfiles, generated files, and conflicting commits.

Required scenarios:

1. create inspection, task, verifier, and integration worktrees at exact SHAs;
2. enforce a clean primary coordination checkout;
3. reject branch from stale/local-only ref when profile requires `origin/main`;
4. reject writes outside leased paths;
5. detect a file moved into/out of leased scope;
6. detect symlink path escaping worktree;
7. deny serial/generated/migration/CI paths to a normal worker;
8. preserve uncommitted task work on interrupt/crash;
9. verify owner Git identity and reject AI attribution trailers;
10. cherry-pick verified commits in dependency order;
11. detect and stop on semantic/merge conflict;
12. invalidate previous validation after conflict resolution or regeneration;
13. never remove a worktree with a live PID, lease, or unarchived diff;
14. reconcile after an external human edit and require re-verification;
15. show remote advancement without silently rebasing the run.

## 5. App Server replay suite

Maintain sanitized traces per pinned Codex version:

- one-turn read-only architect;
- independent plan-review accept and changes-requested verdicts;
- automatic architect revision followed by a fresh plan review;
- multi-turn mutable worker;
- command output streaming;
- file change and diff updates;
- approval requests;
- goal set/update;
- token usage and context compaction;
- requested/effective model reroute;
- native subagent tree;
- review/finding/verdict;
- interrupt and resume;
- process exit during active turn;
- unknown additive protocol item.

Every adapter release must replay old supported traces and the new version trace. Unknown fields must survive raw storage.

## 6. Command-runner tests

Use a test helper that can fork children, write large stdout/stderr, ignore SIGTERM, open sockets, modify files, and exit with controlled codes.

Verify:

- process group ownership;
- graceful interrupt then bounded kill;
- timeout classification;
- stdout/stderr backpressure and artifact spill;
- no daemon memory growth proportional to full logs;
- exact cwd/worktree;
- sanitized environment allowlist;
- network policy/approval boundaries;
- result class separate from exit code;
- command hash and artifact hashes;
- restart reconciliation for commands whose parent state is uncertain.

## 7. Usage/cost tests

Use integer/decimal fixtures for all models and thresholds. Include:

- input/cached/cache-write/output split;
- reasoning as output breakdown;
- missing cache-write range;
- long-context multiplier at boundary;
- price snapshot effective dates;
- mixed tasks/subagents;
- effective model reroute;
- cumulative usage delta and counter reset;
- subscription labeling;
- totals at agent/task/run/day levels;
- stable historical result after config price update.

## 8. UI end-to-end flows

Playwright runs against fake App Server + temporary Git fixture.

### Flow A: observe one agent

- create run;
- see pinned base and authority digest;
- start architect;
- observe plan, actions, current goal, model/effort, token/cost updates;
- observe adversarial review reject a moving-target or premature-test plan;
- observe the replacement revision reach certification;
- confirm manual and automatic approval both reject an uncertified digest.

### Flow B: mutable task and approval

- dispatch worker into shown worktree;
- stream command/file events;
- receive network or write approval;
- deny once and observe agent continuation/blocker;
- approve a safe request;
- inspect final diff and evidence.

### Flow C: subagents

- parent spawns two read-only children;
- list collapses/expands;
- child usage aggregates correctly;
- effective model/effort appears;
- parent and child timelines remain distinct.

### Flow D: restart

- restart daemon during active run;
- reconnect SSE from cursor;
- reconcile App Server/thread/worktree;
- preserve UI state and no duplicate cost/events.

### Flow E: integration

- enable one cheap `review_ready` validator and prove its failure reopens the
  same task through bounded remediation before any semantic reviewer is spent;
- verify two non-overlapping task commits;
- integrate in dependency order;
- show attempt-head proof is historical rather than accepted for the new SHA;
- require path-selected custody, contract, and behavioral validators to pass on
  the clean integrated head; reject a green command that mutates the checkout;
- run automated platform acceptance and leave device acceptance pending for a
  SHA-bound human attestation;
- final reviewer returns one finding;
- remediation reopens only the task owning that file and creates a fresh
  integration candidate;
- final acceptance rests in `HUMAN_REVIEW`; exercise approve and reject paths
  against the signoff packet digest;
- explicit human approval creates a draft PR request;
- when CI proof is profile-required, reject an empty check set and a passing
  check set whose PR head differs from the integration SHA, then promote through
  `CI_PROVEN` only after every required check passes on the expected head.

### Flow F: accessibility

- complete key monitoring/approval/diff path using keyboard only;
- automated axe-style checks plus manual screen-reader smoke;
- reduced motion and high-density modes.

## 9. Performance targets

Measured on the intended 9950X Linux workstation with a realistic database:

- daemon cold start to UI health: < 2 seconds excluding Codex authentication failure;
- App Server initialization visible: < 5 seconds in normal local conditions;
- event receipt to UI render p95: < 250 ms for ordinary items;
- append durability before acknowledgement: < 25 ms p95 under normal local SSD load;
- UI remains responsive with 250,000 projected events in a run;
- event list renders only a bounded viewport;
- 100 MB command output does not exceed configured daemon memory by more than a bounded buffer;
- run-page initial query < 500 ms with indexes and summarized aggregates;
- cost aggregation for a large run < 200 ms or precomputed incrementally;
- worktree create/remove and Git operations report progress rather than blocking the API event loop.

Performance thresholds are release diagnostics until measured baselines and noise bounds are established.

## 10. Security tests

- listener refuses non-loopback bind under v1 policy;
- CSRF/session token required for state-changing browser requests;
- path canonicalization and symlink escape tests;
- no API/auth token persisted in database or browser storage;
- environment/command log redaction fixtures;
- secret-like values excluded from UI/search index;
- no raw reasoning retained under default config;
- approval replay/double-submit rejected;
- external push requires exact head and explicit approval;
- user-entered HTML/ANSI/log content cannot inject script;
- artifact downloads use content-disposition and safe MIME handling;
- local terminal sessions require explicit user creation and cannot be opened by an agent;
- database/artifact files created mode 0600/0700 under UMask 0077;
- systemd service runs without elevated privileges.

## 11. Fault-injection campaign

Before pilot:

- kill App Server at every major turn phase;
- kill daemon after raw append and before projection;
- kill daemon after approval decision and before runtime acknowledgment;
- fill disk during command artifact write;
- make SQLite temporarily busy/read-only;
- advance origin/main during run;
- manually edit task worktree outside the agent;
- expire lease while child process remains alive;
- remove Docker/runner/hardware prerequisite;
- return malformed handoff/task JSON;
- produce an output-schema violation;
- force integration conflict;
- corrupt one artifact byte and verify evidence bundle failure.

Each fault needs a documented terminal state and recovery route; none may produce a false verified/completed state.

## 12. Repository pilot ladder

### Pilot 0 — read-only architecture audit

No writes, no worktree beyond inspection. Success: authority-first context,
task graph, adversarial liveness/feasibility review, automatic correction of a
blocking plan, certified digest, tokens/cost, goal, restart/replay.

### Pilot 1 — docs-only bounded change

One Luna worker, one verifier, no serial architecture spine. Success: lease enforcement, diff, focused docs validators, commit, integration, draft PR approval.

### Pilot 2 — isolated Rust fix

One bounded crate/API change with positive and negative tests. Success: model routing, command/evidence capture, no claim beyond T0–T2.

### Pilot 3 — two disjoint tasks

Two worker worktrees plus verifier. Success: no lease overlap, fair scheduling, parallel usage attribution, dependency-aware integration.

### Pilot 4 — contract-sensitive work

Terra direct assignment, serial generated paths reserved for integrator, Sol independent review. Success: no compatibility repair, hard-cut negative tests, proof invalidation after generation.

### Pilot 5 — heavy and hardware representation

Select environment-specific lanes with explicit prerequisites. Success:
resource scheduling, `INFRASTRUCTURE_UNAVAILABLE` semantics, and
artifact/runner identity. Production hardware actions remain
operator-controlled.

## 13. Release acceptance checklist

A v1 release candidate is acceptable only when:

- all database migrations pass clean install and upgrade restore tests;
- App Server adapter passes golden replay and live pinned-version smoke;
- task/controller property tests pass;
- Git/worktree custody suite passes;
- cost accounting passes exact fixtures;
- UI E2E A–F passes;
- fault campaign has no false verified/completed state;
- listener/security/redaction tests pass;
- Fedora/Nobara and Ubuntu/Debian packaging smoke passes;
- daemon restart resumes an active run;
- first four repository pilots complete with reviewable evidence;
- no automatic merge, production access, or hidden fallback exists;
- operator runbook and protocol compatibility tuple are included in the release manifest.
