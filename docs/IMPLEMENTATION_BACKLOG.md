# Harness Console Dependency-Ordered Implementation Backlog

**Status:** implemented in v0.1.0; retained as the dependency-ordered delivery record
**Initial target:** Linux + `lazerusrm/NeuralMatrix`
**Rule:** every PR is independently reviewable and leaves the repository in a usable state; later PRs must not retroactively excuse missing tests in earlier foundations

## Delivery strategy

All HC-001 through HC-023 capabilities are now represented in the v0.1.0
implementation. The sequence below remains useful for auditing dependencies,
acceptance gates, and future changes; it is no longer a list of unimplemented
work.

Build one narrow vertical slice first: a pinned App Server process, one durable thread, and one live UI. Add Git mutation only after event durability and operator controls exist. Add parallel orchestration only after worktree and lease enforcement are proven. Add GitHub publication last.

Recommended branch/PR prefix: `harness/HC-###-<slug>`.

## Milestone map

| Milestone | PRs | Outcome |
|---|---|---|
| M0 Contracts | HC-001–003 | repository skeleton, ADRs, schemas, protocol pin |
| M1 Observe | HC-004–007 | one read-only Codex thread visible and durable |
| M2 Mutate safely | HC-008–011 | one task, one worktree, approvals, diff, validation |
| M3 Orchestrate | HC-012–015 | task DAG, bounded workers, verifier, integration |
| M4 NeuralMatrix | HC-016–018 | authority context, proof/evidence, repository profile |
| M5 Publish and harden | HC-019–023 | PR/CI, restart recovery, packaging, pilots, release |

---

## HC-001 — Workspace, ADRs, and build/release skeleton

**Depends on:** none
**Model recommendation:** Terra xhigh or human-led

### Scope

- Rust workspace with crates/binaries:
  - `harness-domain`
  - `harness-store`
  - `harness-codex`
  - `harness-git`
  - `harness-runner`
  - `harness-profile`
  - `harness-api`
  - `harnessd`
  - `harnessctl`
  - `xtask`
- React/TypeScript/Vite UI package.
- formatter/lint/test commands.
- embedded frontend release build.
- ADR-0001 through ADR-0006 from the architecture plan.
- license/security/contribution files for the harness repository.

### Tests

- workspace builds on supported Rust;
- empty UI embedded and served in a test;
- `cargo xtask dist --check` validates release contents;
- no runtime Node dependency.

### Exit gate

`harnessd serve` starts a localhost health endpoint and serves a version page from one release binary.

---

## HC-002 — Configuration, XDG paths, profiles, and secret-safe diagnostics

**Depends on:** HC-001

### Scope

- typed TOML config with layered defaults, file, environment, and CLI overrides;
- XDG path resolver and 0700/0600 creation;
- immutable pricing snapshot parser;
- repository profile loader and schema versioning;
- `harnessctl config validate` and `doctor` skeleton;
- diagnostics redact environment values and probable secrets.

### Tests

- precedence/property tests;
- invalid/unknown key rejection for production configuration;
- permissions and path resolution in temp homes;
- sample config/profile parse;
- redaction fixtures.

### Exit gate

A clean install prints resolved non-secret configuration and rejects unsafe non-loopback bind or `allow_agent_full_access` under v1 policy.

---

## HC-003 — SQLite migrations, raw event store, artifacts, and projections framework

**Depends on:** HC-001–002

### Scope

- SQLx SQLite store using WAL and busy timeout;
- initial migration supplied in this blueprint;
- content-addressed SHA-256 artifact store;
- append-only raw event API;
- projector cursor/checkpoint tables;
- online backup and migration-check commands;
- storage-retention framework without automatic destructive cleanup.

### Tests

- migration from empty DB;
- transaction/idempotence/concurrency tests;
- crash between raw append and projection;
- artifact dedup/hash corruption;
- online backup restore;
- restrictive permissions.

### Exit gate

A synthetic event is durably appended, projected, queried, replayed, backed up, and verified after process restart.

---

## HC-004 — Pinned Codex App Server supervisor and protocol adapter

**Depends on:** HC-002–003

### Scope

- spawn/supervise `codex app-server` over stdio;
- initialize handshake;
- exact version and generated schema digest check;
- typed request/response client with raw JSON retention;
- stderr artifact capture;
- bounded restart policy;
- `fake-app-server` scripted test binary;
- generated bindings update command.

### Tests

- initialize success/fail;
- malformed/oversized frame;
- request timeout/cancellation;
- process exit and restart;
- schema mismatch disables execution;
- unknown notification retained raw.

### Exit gate

`harnessctl runtime codex` shows ready/version/schema/PID, and fake/live smoke starts and completes a read-only thread without terminal scraping.

---

## HC-005 — Thread, turn, item, goal, and token projections

**Depends on:** HC-004

### Scope

- internal stable activity-item union;
- thread/start/list/resume mapping;
- turn/start/steer/interrupt mapping;
- `thread/goal/set/get/clear` mapping;
- plan, diff, command, file, message, reasoning-summary, compaction projections;
- native subagent parent/child projection;
- requested/effective model and effort;
- token sample storage using last-turn/delta logic.

### Tests

- golden traces covering all required item types;
- duplicate/reordered projection idempotence;
- native subagent tree;
- model reroute;
- cumulative token reset;
- raw reasoning excluded under default policy.

### Exit gate

One App Server run produces a complete durable timeline, current goal, parent/child structure, requested/effective model, and usage samples queryable after restart.

---

## HC-006 — Local REST/SSE API and browser session security

**Depends on:** HC-003, HC-005

### Scope

- versioned REST API from the supplied OpenAPI contract;
- SSE event stream with durable cursor/replay and heartbeat;
- same-origin local session + CSRF protection for mutations;
- request IDs, structured errors, optimistic concurrency tokens;
- no non-loopback listener in v1;
- terminal WebSocket endpoint stub remains disabled.

### Tests

- API contract tests;
- SSE reconnect/no duplicate cursor;
- CSRF and origin rejection;
- slow subscriber/backpressure;
- authorization of local state-changing calls;
- graceful daemon shutdown.

### Exit gate

A browser/client can list runtime health, create a read-only session, stream its events, reconnect from a cursor, and steer/interrupt it securely.

---

## HC-007 — Codex-style single-agent UI vertical slice

**Depends on:** HC-006

### Scope

- application shell, home, run workspace, inspector;
- live activity/plan/commands/files/usage/context tabs;
- model/effort/sandbox/current goal;
- token/cost placeholders from actual usage fields;
- steer and interrupt controls;
- virtualized event list;
- dark/light and keyboard basics;
- App Server health/schema diagnostics.

### Tests

- Playwright observe/steer/interrupt flow against fake App Server;
- SSE reconnect;
- 100k event virtualization fixture;
- accessibility smoke.

### Exit gate

Pilot 0 can be observed end to end from the browser, including restart/reconnect, without any Git mutation.

---

## HC-008 — Repository registration, Git coordination lock, and inspection worktree

**Depends on:** HC-002–003, HC-006

### Scope

- register existing clean repository clone;
- verify remote, default branch, Git identity, required files;
- advisory/OS lock plus DB repository lock;
- controller-owned `fetch --prune`;
- exact base-ref resolution;
- inspection worktree create/preserve/remove;
- primary-checkout clean/on-main invariant for NeuralMatrix;
- Git command audit artifacts.

### Tests

- temp repos/remotes;
- dirty primary, wrong branch, stale ref, missing identity;
- concurrent process lock;
- worktree create/reconcile/cleanup;
- remote advances after pin.

### Exit gate

A new run pins `origin/main`, records exact SHA, creates a separate inspection worktree, and never mutates the primary checkout.

---

## HC-009 — Task worktrees, path leases, policy engine, and diff custody

**Depends on:** HC-008

### Scope

- one worktree/branch per mutable task attempt;
- lease trie/glob overlap engine;
- serial/forbidden/generated path policies;
- symlink/canonical-path safety;
- Git status/diff verification against exact task base;
- unexpected-path block;
- preserve failed/interrupted worktrees;
- controller-owned commit with existing user Git identity and trailer checks.

### Tests

- full Git/worktree suite from the test plan;
- path glob/property tests;
- move/rename/symlink escape;
- concurrent lease race;
- AI attribution rejection;
- commit/diff/hash evidence.

### Exit gate

A bounded worker can write only its leased paths in its own worktree, and the controller can produce one verified task commit without branch management by the agent.

---

## HC-010 — Approval broker and command/file/network risk policy

**Depends on:** HC-005–006, HC-009

### Scope

- project App Server approval requests;
- risk classification and approval center;
- decision persistence before runtime forwarding;
- one-time/turn/task scope only where protocol supports it;
- deny broad/global approval;
- controller-owned external-write gates;
- blocked/waiting-on-approval state.

### Tests

- accept/deny/expire/delivery failure;
- duplicate/replayed decision;
- command/path details and redaction;
- high-risk push never batch-approved;
- daemon restart with unresolved approval.

### Exit gate

A mutable worker request can pause on approval, show exact risk/scope, be denied or approved once, and resume with an auditable decision.

---

## HC-011 — Command runner, resource classes, logs, and result classification

**Depends on:** HC-003, HC-009–010

### Scope

- controller-owned subprocess wrapper where Harness invokes validators/helpers;
- process groups, timeouts, interrupt/kill, bounded in-memory buffers;
- stdout/stderr spill to artifacts;
- resource classes `control`, `medium`, `heavy`, `hardware/exclusive`;
- CPU/memory/IO accounting when available;
- explicit result classification distinct from exit code;
- command UI and artifact download.

### Tests

- helper process fault suite;
- output flood/backpressure;
- orphan child/ignored SIGTERM;
- disk failure;
- environment allowlist/redaction;
- resource queue exclusivity.

### Exit gate

Focused validation commands run with reliable cancellation/logs/result semantics, and infrastructure absence cannot appear as success.

---

## HC-012 — Run/task state machine, task schemas, and plan review

**Depends on:** HC-005, HC-008–011

### Scope

- run/task/attempt domain state machines;
- JSON Schema validation for task/handoff/evidence;
- Sol architect output schema;
- DAG validation, dependency cycles, base SHA, authority refs, success criteria;
- plan review/edit/approve UI;
- task packet immutability per attempt;
- current-goal initialization.

### Tests

- model/property transition tests;
- invalid task graph/schema/cycle/path conflict;
- plan approval optimistic concurrency;
- task packet digest/history.

### Exit gate

A user objective becomes a schema-valid, human-reviewable, dependency-ordered NeuralMatrix task graph with no mutation yet dispatched.

---

## HC-013 — Usage ledger, price snapshots, cost UI, and budgets

**Depends on:** HC-005, HC-012

### Scope

- decimal/micro-dollar accounting from `COST_ACCOUNTING.md`;
- immutable effective-dated price snapshots;
- per-turn/task/agent/subagent/run totals;
- cache-write bounds and long-context rules;
- API-equivalent labeling;
- task/run budget warnings and enforcement;
- usage dashboard.

### Tests

- complete accounting fixture/property suite;
- historical snapshot immutability;
- effective-model reroute;
- missing-field confidence range;
- no reasoning double count.

### Exit gate

Every completed turn has an attributable token/cost entry or an explicit unknown reason; run totals survive restart and match the sum of entries.

---

## HC-014 — Bounded scheduler, workers, retries, escalation, and subagent policy

**Depends on:** HC-012–013

### Scope

- dependency/resource/lease-aware scheduler;
- default 3 mutable + 1 verifier, max 6 total;
- controller-created top-level mutable worker threads;
- native child subagents counted and shown;
- Luna worker, Terra direct-risk/escalation routing;
- heartbeat/watchdog;
- retry with prior evidence and new attempt;
- bounded automatic remediation; pause/stop controls;
- fairness and no starvation.

### Tests

- virtual-time scheduler model;
- capacity and resource conflicts;
- lease expiry/watchdog;
- child thread consumes total slot;
- failure escalation policy;
- pause/resume/restart.

### Exit gate

Two disjoint tasks can execute concurrently in separate worktrees while a third waits correctly on dependency/resource/lease, with accurate agent/subagent usage.

---

## HC-015 — Independent verification, findings, integration, and evidence invalidation

**Depends on:** HC-014

### Scope

- fresh Sol verifier thread read-only against task commit;
- structured findings and ACCEPT/REMEDIATE/REJECT_ARCHITECTURE;
- controller routing of findings to fresh remediation attempts;
- Terra integration worktree and dependency-ordered cherry-pick;
- serial path custody/generated outputs;
- conflict stop/re-plan;
- evidence invalidation graph;
- fresh Sol final audit.

### Tests

- worker cannot self-verify;
- finding lifecycle;
- remediation attempt history;
- integration conflict;
- regenerated file invalidates prior proof;
- final audit blocks completion.

### Exit gate

Verified task commits integrate on exact base, affected proof is rerun, and a fresh final auditor must accept before the run can reach human publication review.

---

## HC-016 — Context compiler, repository map, and high-bandwidth probe helper

**Depends on:** HC-008, HC-012

### Scope

- authority-first NeuralMatrix context compiler;
- bounded text inventory, FTS5 metadata, ripgrep search, Git file map;
- Rust workspace/package metadata per actual workspace;
- CI claim/workflow registry ingestion;
- task-specific context packets/digests;
- archive/vendor/generated exclusion policy;
- `harness-probe`: `search`, `read-many`, `cargo-map`, `test-select`, `summarize-log`;
- stable prompt prefix ordering for cache reuse;
- context inspector UI.

### Tests

- fixture repository authority graph;
- archived doc never selected as active authority absent promotion;
- large/binary/vendor exclusion;
- index invalidation on SHA change;
- deterministic context packet digest;
- bounded helper output and full artifact retention.

### Exit gate

The architect/worker receives a compact reproducible context packet rather than the full repository, and the UI can show every included/excluded source and digest.

---

## HC-017 — NeuralMatrix profile, model/risk router, and validator catalog

**Depends on:** HC-014, HC-016

### Scope

- load supplied profile;
- canonical authority and completion-authority import;
- domain/path/authority routing;
- risk flags for contracts, migrations, tenancy, auth, privacy, native/unsafe, hardware, OTA, required CI;
- direct Terra route for high-risk work;
- Sol architect/verifier/final-auditor roles;
- Luna explorer/worker/CI triage roles;
- docs, CI self-test, local C2, Jetson cross-build validators;
- proof tier/result semantics;
- no `.omx` runtime state as authority.

### Tests

- real-profile parsing against a pinned NeuralMatrix fixture/checkout;
- domain/risk/serial-path classification tables;
- no-fallback protected semantics appear in every mutable packet;
- heavy prerequisite unavailable result;
- master checklist never auto-updated as completion truth.

### Exit gate

Pilots 0–3 run under NeuralMatrix's actual repository governance with correct model routing, path custody, and proof semantics.

---

## HC-018 — Evidence bundles, claim matrix, and exact-SHA provenance

**Depends on:** HC-011, HC-015, HC-017

### Scope

- evidence schema and builder;
- claim/proof-tier matrix;
- exact source/base/head SHA, command, target/profile, runner/device, fixture/model/artifact digests, result class, unproved claims;
- content-hash manifest;
- sanitized export/import/verify;
- evidence browser;
- run summary suitable for PR body attachment.

### Tests

- artifact corruption and missing link;
- stale SHA rejection;
- infrastructure unavailable displayed non-green;
- deterministic manifest ordering;
- export excludes secrets/raw reasoning by default;
- bundle verification after transfer.

### Exit gate

A completed pilot exports a self-verifying bundle that accurately states the highest proof achieved and the claims still unproved.

---

## HC-019 — GitHub draft PR and exact-head CI integration

**Depends on:** HC-015, HC-018

### Scope

- explicit approval to push integration branch;
- exact expected head in push/PR record;
- draft PR creation through `gh` or GitHub integration;
- attach run summary/evidence links/artifacts as policy allows;
- poll/ingest PR checks and classify results;
- invoke NeuralMatrix required-check verifier when requested;
- no automatic ready/merge;
- remote head drift detection.

### Tests

- fake/local remote + mocked GitHub API/CLI;
- denied push;
- changed head after approval;
- failed/missing CI context;
- draft remains draft;
- no merge code path in v1.

### Exit gate

A human-approved accepted run creates one draft PR at the exact integration head and shows CI status without claiming merge readiness prematurely.

---

## HC-020 — Recovery, reconciliation, retention, and operational tooling

**Depends on:** HC-014–019

### Scope

- daemon/App Server restart reconciliation;
- thread resume/list and outstanding approval recovery;
- process/worktree/lease reconciliation;
- stale run/worktree detection;
- safe retention dry-runs;
- DB backup/projection rebuild;
- host/storage/runtime UI;
- `harnessctl` operations from the runbook.

### Tests

- kill/restart fault matrix;
- duplicate event/cost prevention;
- uncertain command state requires review;
- safe cleanup preconditions;
- backup/restore active and completed runs.

### Exit gate

The daemon can restart mid-run and either safely resume or clearly block/reconcile every active task; nothing silently disappears or becomes verified.

---

## HC-021 — Packaging, systemd hardening, upgrade, and Linux matrix

**Depends on:** HC-020

### Scope

- release tarball and checksum manifest;
- systemd user unit and install/uninstall scripts;
- Fedora/Nobara and Ubuntu/Debian smoke images/VMs;
- config migration/version check;
- Codex compatibility tuple in release manifest;
- optional browser auto-open/PWA install;
- runtime requires no Node.

### Tests

- clean install, first-run doctor, restart, upgrade backup/restore;
- restrictive UMask/XDG permissions;
- non-loopback refusal;
- missing optional prerequisites diagnostics;
- uninstall preserves user data unless explicitly requested.

### Exit gate

A release artifact installs on both Linux families, starts via `systemd --user`, and passes Pilot 0 without a development checkout.

---

## HC-022 — NeuralMatrix pilot campaign and quality remediation

**Depends on:** HC-021

### Scope

Run the pilot ladder from the test plan. Record:

- task quality and correction rate;
- accepted/rejected findings;
- tokens/cost by role;
- time in architecture/implementation/verification/integration;
- context-pack misses;
- lease/policy incidents;
- App Server/projection failures;
- operator interaction burden;
- false-green attempts prevented.

Convert recurring errors into controller tests, profile rules, context seeds, or agent instructions. Do not solve them with hidden compatibility behavior.

### Exit gate

Pilots 0–4 complete with no unresolved P0/P1 controller, Git custody, evidence, or security defect. Performance and cost baselines are documented.

---

## HC-023 — v1 release audit and freeze

**Depends on:** HC-022

### Scope

- fresh Sol max system audit plus human architecture/security review;
- validate every v1 acceptance criterion;
- freeze database/API/schema/profile versions;
- finalize operator/admin documentation;
- create release notes, known limitations, migration path;
- sign/checksum artifacts as appropriate for the harness project.

### Exit gate

Release verdict is explicit: `ACCEPT`, `REMEDIATE`, or `REJECT_ARCHITECTURE`. `ACCEPT` requires all v1 gates and no unresolved critical/high finding.

---

## Post-v1 backlog

These are deliberate later additions, not hidden v1 requirements:

1. profile packs for other user repositories;
2. optional Tauri shell around the local web UI;
3. richer visual DAG and historical performance analytics;
4. supported dynamic-tool/MCP integration replacing selected shell helpers once protocol stability is proven;
5. distributed remote workers with mutual authentication and signed task/evidence envelopes;
6. multi-user roles, TLS, and remote server deployment;
7. GitHub review-thread remediation workflows;
8. configurable additional model/provider adapters behind the same task/evidence contracts;
9. evaluation corpus and automatic harness-improvement suggestions;
10. signed release/update channel for Harness Console itself.

Do not begin distributed/multi-user/provider abstraction until the local single-user NeuralMatrix control plane is stable. Those features multiply security, state, and failure domains and are not necessary to realize the primary value.
