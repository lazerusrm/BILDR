# BILDR self-improvement readiness audit — 2026-08-11

**Status:** current named-base audit; findings become stale when the cited source changes
**Repository:** `lazerusrm/BILDR`
**Audited basis:** `release/initial-public-preview@5c7dc3b678b7811cf8d8676e39c9ffcd0ba02e55`
**Primary question:** what must change for BILDR to improve its own harness over time without weakening custody, evidence, security, or operator control?

## Verdict

**REVISE architecture before enabling cross-run self-improvement.**

BILDR has an unusually strong execution foundation: exact-SHA Git custody,
isolated worktrees, typed run and task state, raw event journaling, durable
projections, context receipts, evidence bundles, usage accounting, independent
review, and explicit publication gates. Those are the difficult prerequisites
for a trustworthy improvement loop.

The repository does not yet have a cross-run learning system. Current adaptation
is local to a run or attempt. There is no typed outcome corpus, taskset,
versioned grader bundle, held-out evaluation split, candidate registry,
champion/challenger experiment, promotion decision, rollback record, or
evidence-scoped knowledge lifecycle. Adding an optimizer directly to the current
orchestrator would create a self-confirming loop with no independent measure of
whether the harness actually improved.

The recommended shape is a separate, governed improvement subsystem whose first
release is observation and evaluation only. Candidate generation comes after
the evaluation substrate is credible. Automatic promotion comes last and is
limited to low-risk policy dimensions.

## Scope and method

The audit inspected:

- workspace and component topology;
- run, task, attempt, agent, evidence, and usage domain types;
- SQLite migrations and event projections;
- governor planning, review, retry, and continuation behavior;
- repository profile and security configuration;
- authority-first context compilation;
- schema and contract checks;
- CI and release branch state;
- UI and orchestration source shape;
- current research on eval-driven harness evolution and reward integrity.

This is an architecture and implementation-readiness audit. It is not a full
security penetration test or line-by-line defect audit of every function.

## What BILDR already does well

### 1. The controller, not a worker, owns mutable authority

`harness-orchestrator` delegates implementation but retains commit, integration,
publication, state-transition, and completion authority. `harness-git` owns
worktrees and exact-head checks. This is the correct foundation for improvement:
a proposed policy cannot be allowed to grade or publish itself.

### 2. Evidence is already exact-SHA and content-addressed

The database and evidence crate already model commands, validations, artifacts,
findings, handoffs, source SHAs, and proof tiers. An improvement experiment can
reuse these receipts rather than inventing a second evidence system.

### 3. Raw protocol events precede projection

`raw_events`, `domain_events`, and projector checkpoints provide a recoverable
observation plane. That permits trace reconstruction after the fact and allows
new projections to be introduced without losing the original event stream.

### 4. In-run adaptation is bounded and explicit

`harness-orchestrator/src/lib.rs` contains:

- a plan-quality contract;
- adversarial plan review;
- a governor replan contract;
- bounded continuation and token ceilings;
- retry continuity metadata;
- escalation routes;
- independent verification and final audit.

This is useful adaptation, but it should not be confused with cross-run
learning. The current loop can revise a strategy for one objective; it does not
measure whether a durable harness revision generalizes.

### 5. Context selection is deterministic and auditable

`harness-context` compiles an authority-first packet with a pinned base SHA,
profile digest, instruction digest, included and excluded sources, byte and
token bounds, and a final digest. This is an excellent seam for later
experimenting with context policy because champion and challenger packets can
be compared exactly.

### 6. The safety posture is conservative

`harness-profile` rejects non-loopback serving, full-access workers, automatic
external writes, automatic push, automatic pull-request creation, automatic
readiness, automatic merge, non-stdio protocol transport, missing redaction, and
non-SHA-256 custody under v1 policy. These rules should become part of the
frozen safety anchor rather than editable optimizer inputs.

## Ranked findings

### P0-SI-001 — There is no independent evaluation and promotion authority

**Evidence**

- Domain types cover runs, tasks, attempts, agents, worktrees, approvals,
  validations, evidence, findings, operations, and publications, but no eval
  suite, grader bundle, candidate, experiment, champion, promotion, or rollback.
- The database has rich execution and evidence tables but no cross-run
  evaluation or policy-version tables.
- Repository search finds no implementation of holdouts, champion/challenger,
  experiments, or promotion.

**Risk**

If candidate generation is added before a separate evaluation authority, the
same process can choose the change, choose the examples, choose the metric, and
declare success. That is not self-improvement; it is an unbounded
self-confirmation channel.

**Required correction**

Introduce typed tasksets, eval cases, immutable grader bundles, paired eval runs,
holdout partitions, candidate records, promotion decisions, and rollback
records before any candidate can affect production behavior.

### P0-SI-002 — No reward-integrity or holdout boundary exists

**Evidence**

The current validation system proves repository changes, but it does not define:

- a reward contract distinct from the objective;
- negative-control metrics;
- grader versioning;
- hidden holdout custody;
- leakage tracking;
- anti-tamper boundaries between a runtime and its grader;
- minimum sample size or uncertainty requirements.

**Risk**

A candidate can improve a proxy while degrading the real task, overfit known
cases, or learn to manipulate the evaluator. Tool-use systems are especially
vulnerable because a superficially successful final response can hide skipped
actions or incorrect state changes.

**Required correction**

Make reward integrity a first-class gate. Require at least one behavior-owned
deterministic signal, explicit side-effect checks where applicable, hidden
holdout evaluation, grader digests, leakage classification, and red-team
adjudication for every promotable experiment.

### P0-SI-003 — Core safety controls are not yet declared immutable to evolution

**Evidence**

The current config correctly hard-rejects dangerous v1 settings, but there is no
formal distinction between an editable harness component and a frozen outer
anchor.

**Risk**

A future optimizer could “improve” throughput by relaxing sandbox, approval,
evidence, redaction, Git custody, publication, or rollback rules.

**Required correction**

Create a frozen safety anchor with a digest and explicit protected dimensions.
Only a normal reviewed repository change may modify it. An improvement candidate
that touches a protected dimension is invalid, not merely high risk.

### P0-SI-004 — Production outcomes are not durably labeled across runs

**Evidence**

BILDR records task results, validations, findings, commands, costs, and
publication state. It does not record a stable cross-run outcome such as:

- accepted without correction;
- accepted after human correction;
- reopened or regressed;
- false-positive verifier acceptance;
- infrastructure-only failure;
- user-abandoned;
- downstream CI or production defect.

**Risk**

The system can optimize for completion, green local checks, or lower cost while
missing the human and downstream signals that determine whether work was useful.

**Required correction**

Add an immutable outcome record with provenance, confidence, delayed labels, and
correction history. Keep objective success, proof success, operator acceptance,
downstream regression, cost, and latency as separate signals.

### P1-SI-005 — Raw events are not a branch-aware, reusable trace artifact

**Evidence**

BILDR stores raw events, threads, turns, items, and parent agent sessions.
Compaction, retry, forked subagents, and resumed work are not yet normalized into
a content-addressed message graph with explicit branches and export receipts.

**Risk**

Linear replay can double-count shared prefixes, obscure which context a model
actually saw, and make subagent or compaction behavior impossible to compare
faithfully.

**Required correction**

Add a trace graph projection with nodes, typed edges, branch manifests,
redaction receipts, sampling settings, model calls, tool effects, and source
event references. Keep raw events authoritative; traces are derived,
versioned artifacts.

### P1-SI-006 — No durable lesson or knowledge lifecycle feeds the context compiler

**Evidence**

`harness-context` selects active authorities, task authorities, domain hints,
and owned files. It has no cross-run knowledge item with source evidence, scope,
confidence, contradiction state, expiry, or measured impact.

**Risk**

Copying summaries from prior runs directly into prompts would create stale,
self-reinforcing folklore. Conversely, ignoring prior outcomes forces repeated
rediscovery.

**Required correction**

Introduce evidence-backed knowledge items with scoped applicability, source
trace/evidence IDs, confidence, independent review, freshness, contradiction,
supersession, and usage-impact telemetry. Knowledge can supplement active
authority but never override it.

### P1-SI-007 — The optimizer action space is not explicit or risk-classified

**Evidence**

Profiles contain model routes, validators, context rules, security, and
orchestration settings, but there is no versioned policy bundle describing which
dimensions are editable and how each is promoted.

**Risk**

A candidate may combine unrelated prompt, routing, validator, and code changes,
making attribution impossible and rollback unsafe.

**Required correction**

Represent each editable component as a versioned policy dimension. Candidate
edits must be bounded add/delete/replace operations against one immutable
bundle, with predicted effect, risk class, expected metrics, and rollback
target.

### P1-SI-008 — The orchestrator and primary UI files are already concentration risks

**Evidence**

At the audited tree:

- `crates/harness-orchestrator/src/lib.rs` is approximately 544 KB.
- `ui/src/App.tsx` is approximately 200 KB.

Both centralize many concerns.

**Risk**

Adding trace mining, eval scheduling, candidate generation, experimentation, and
promotion into these files would increase coupling and make self-improvement
changes difficult to reason about or roll back.

**Required correction**

Create separate crates or modules for trace, eval, learning, and promotion.
Extract stable service interfaces from the orchestrator. Split the UI into
route-level feature modules before adding an Improvement Center.

### P1-SI-009 — Schema checking verifies parseability, not conformance

**Evidence**

`xtask::schema_check` parses JSON files into `serde_json::Value` and parses
selected TOML files. It does not validate JSON Schemas against the 2020-12
meta-schema, resolve local references, or validate example instances against
their declared schemas.

**Risk**

Malformed constraints, drift between Rust and JSON, and invalid fixtures can
pass the contract gate.

**Required correction**

Add schema meta-validation, schema-to-example validation, duplicate `$id`
rejection, known-schema discriminator checks, and generated Rust/TypeScript
round-trip fixtures.

### P1-SI-010 — Hosted proof for the current release head is unresolved

**Evidence**

PR #1 targets `main` from `release/initial-public-preview`. At the audited head,
the contribution metadata check passed, while `Build and test` ended as a
failure because the Rust validation step was canceled and the contract step was
skipped. The local validation report explicitly does not claim hosted proof.

**Risk**

A self-improvement program built on an unproven baseline cannot distinguish
candidate regression from baseline infrastructure state.

**Required correction**

Establish a green, immutable baseline before measuring candidate deltas. Record
infrastructure cancellation separately from source failure.

### P1-SI-011 — Default-branch and release-branch topology can mislead automation

**Evidence**

`main` contains only `LICENSE`; the implementation is on
`release/initial-public-preview` and is proposed into `main` by PR #1. The CI
workflow runs push validation only on `main`, while pull requests run
independently of base.

**Risk**

Repository scanners, external benchmarks, package automation, and future eval
workers may inspect the default branch and conclude that BILDR has no
implementation.

**Required correction**

Complete or deliberately revise the preview cutover, then bind improvement
baselines to an exact repository ref and SHA. Never infer the evaluation base
from the repository default branch alone.

### P2-SI-012 — No recurring quality-gardening program exists

**Evidence**

BILDR can execute user objectives, but it has no scheduled program that scans
for repository drift against explicit golden principles, updates quality
scores, and proposes small cleanup changes.

**Recommendation**

Add a low-risk, separately budgeted quality gardener after evals and rollback
are working. It should open narrowly scoped draft changes, never merge them, and
measure whether cleanup reduces future failures or context cost.

### P2-SI-013 — No external eval or training interchange exists

**Evidence**

BILDR has internal traces and evidence but no taskset/harness/runtime export.

**Recommendation**

Define a provider-neutral export first. Add Prime Verifiers and OpenAI Evals
adapters later. External training must consume redacted, licensed, explicitly
exported traces and return a versioned model or adapter candidate; it must not
become the source of truth for BILDR outcomes.

## Readiness matrix

| Capability | Current state | Required before |
|---|---|---|
| Exact-SHA execution evidence | Strong | Observation |
| Raw event journal | Strong | Trace projection |
| In-run replanning | Strong | No change |
| Outcome labels | Missing | Any optimization |
| Eval cases and tasksets | Missing | Any optimization |
| Independent grader bundles | Missing | Candidate comparison |
| Hidden holdouts | Missing | Promotion |
| Candidate/policy registry | Missing | Shadowing |
| Champion/challenger experiments | Missing | Shadowing |
| Promotion and rollback | Missing | Production use |
| Evidence-backed knowledge | Missing | Cross-run memory |
| External training export | Missing | Optional post-training |
| Frozen safety anchor | Missing formal contract | Any optimizer |

## Recommended first milestone

Do not start with training or code self-editing.

The first implementation milestone should:

1. normalize existing runs into immutable trace and outcome records;
2. let a human label delayed outcomes and corrections;
3. materialize a small, versioned BILDR eval suite from real failure classes;
4. run the current champion reproducibly;
5. display score, cost, latency, proof, and reward-integrity diagnostics;
6. make no change to future production behavior.

Only after the system can measure itself credibly should it be allowed to
propose a candidate.
