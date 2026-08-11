# BILDR governed self-improvement implementation plan

**Status:** dependency-ordered implementation plan
**Basis:** `release/initial-public-preview@5c7dc3b678b7811cf8d8676e39c9ffcd0ba02e55`
**Delivery rule:** every pull request is independently reviewable, keeps ordinary BILDR operation usable, and cannot claim a higher improvement mode than its evidence supports

## Program strategy

Build measurement before optimization, optimization before shadowing, shadowing
before canary, and canary before any automatic promotion.

```text
M0 Boundaries
  -> M1 Observe
  -> M2 Evaluate
  -> M3 Learn
  -> M4 Experiment
  -> M5 Promote
  -> M6 Extend
```

The first production release of this program is **observe only**. No milestone
may reach production by weakening the current controller, Git, approval,
publication, evidence, or security rules.

## Milestone map

| Milestone | Work packages | Outcome |
|---|---|---|
| M0 Boundaries | SI-001–SI-003 | frozen anchor, module seams, strict contracts |
| M1 Observe | SI-004–SI-007 | trace graph, outcomes, failures, Improvement Center read path |
| M2 Evaluate | SI-008–SI-012 | tasksets, graders, replay, statistics, holdout |
| M3 Learn | SI-013–SI-016 | knowledge, policy bundles, candidates, optimizer |
| M4 Experiment | SI-017–SI-019 | offline, shadow, canary |
| M5 Promote | SI-020–SI-022 | promotion, rollback, drift |
| M6 Extend | SI-023–SI-027 | gardening, external adapters, model training, code and meta evolution |

## M0 — Boundaries

### SI-001 — Adopt the self-improvement contract and frozen anchor

**Scope**

- land the architecture, audit, references, ADRs, schemas, and examples;
- define frozen and editable dimensions;
- add `observe_only` as the only initially valid mode;
- generate and pin a safety-anchor digest;
- reject unknown improvement modes and protected-dimension edits.

**Tests**

- anchor digest reproducibility;
- every Red dimension is rejected from candidate schemas;
- config cannot enable shadow/canary/promotion before compiled capability gates;
- ordinary runs work with improvement disabled.

**Exit gate**

The controller can report the anchor and mode, but cannot generate or execute a
candidate.

### SI-002 — Split orchestration and UI seams before adding the subsystem

**Scope**

- extract run execution, planning, retry, verification, integration, and
  publication services from the large orchestrator module;
- define stable observer hooks for run events and outcomes;
- split `App.tsx` into route-level feature modules;
- add architecture dependency checks preventing new improvement crates from
  importing internal orchestrator implementation.

**Tests**

- no behavior change on existing run fixtures;
- module dependency policy;
- API and UI regression;
- source-size budget or reviewed exception.

**Exit gate**

New improvement services can subscribe through interfaces without adding
evaluation logic to the production orchestrator.

### SI-003 — Strengthen schema and fixture validation

**Scope**

- validate all schemas against Draft 2020-12;
- resolve local references;
- require unique `$id`;
- validate every example by its `schema` discriminator;
- add Rust and TypeScript round-trip fixtures for active wire records;
- reject undocumented schema versions.

**Tests**

- malformed keyword and unresolved-reference fixtures;
- wrong-discriminator fixture;
- additional-property rejection;
- Rust/JSON/TypeScript compatibility.

**Exit gate**

`cargo xtask schema-check` proves conformance rather than parseability.

## M1 — Observe

### SI-004 — Add stable improvement domain types and persistence

**Depends on:** SI-001, SI-003

**Scope**

- IDs and closed enums for traces, outcomes, failures, tasksets, graders,
  candidates, experiments, policy bundles, promotions, rollbacks, and
  knowledge;
- append-only revision tables;
- migrations and online backup compatibility;
- event types and projections;
- retention and sensitivity classification.

**Tests**

- migration from every supported schema version;
- state-machine property tests;
- idempotent replay;
- backup/restore;
- unknown state fails closed.

**Exit gate**

The store can persist empty improvement programs without affecting ordinary
runs.

### SI-005 — Normalize raw events into a branch-aware trace graph

**Depends on:** SI-002, SI-004

**Scope**

- `harness-trace` crate;
- node and edge projection;
- shared-prefix deduplication;
- compaction, subagent, retry, remediation, and restart branches;
- trace manifest and digest;
- source raw-event receipts;
- redaction/export classification.

**Tests**

- golden traces for linear, compaction, subagent, retry, crash/replay;
- duplicate/reordered raw events;
- no orphan edge;
- branch reconstruction;
- redaction and digest stability.

**Exit gate**

A historical run can be projected into a deterministic graph and inspected
without changing that run.

### SI-006 — Capture immediate and delayed outcomes

**Depends on:** SI-004

**Scope**

- structured operator acceptance/correction;
- CI, review, reopen, rollback, and downstream regression labels;
- append-only outcome revisions;
- confidence and source;
- manual UI and CLI entry;
- automated labels only from typed authoritative sources.

**Tests**

- delayed label supersession;
- conflicting sources surfaced;
- no inferred success from completion;
- customer-data and free-text redaction.

**Exit gate**

Each pilot run has a visible outcome vector and provenance.

### SI-007 — Failure taxonomy and observation dashboard

**Depends on:** SI-005, SI-006

**Scope**

- deterministic initial failure taxonomy;
- occurrence and cluster records;
- human merge/split/reclassify;
- frequency, severity, cost, and recurrence;
- read-only Improvement Center overview and Trace Explorer.

**Tests**

- stable classification fixtures;
- unknown failure remains unknown;
- cluster edits preserve occurrence history;
- large-trace UI virtualization.

**Exit gate**

An operator can identify the highest-cost repeat failure and inspect its
supporting traces and outcomes.
