# Agent execution guide

## Purpose

Use this guide to execute the implementation plan with an orchestrator and
parallel implementation agents without duplicating contracts, crossing path
ownership, or weakening review.

The implementation plan remains authoritative for scope and acceptance. This
guide defines how agents coordinate.

## Roles

### Program orchestrator

Use one high-capability orchestrator for the stack.

Responsibilities:

- read ADR-0012 and all operator-control documents before dispatch;
- maintain the dependency graph and stack order;
- create task packets with exact paths, contracts, tests, and non-goals;
- prevent two agents from owning the same contract or mutable path;
- inspect every diff before integration;
- reconcile cross-lane assumptions;
- run or delegate independent specialist reviews;
- update the traceability matrix when implementation changes the design;
- refuse unplanned scope expansion;
- preserve a clean integration history.

The orchestrator does not implement every lane itself. It owns consistency.

### Contract and architecture reviewer

Use an independent strong reviewer for:

- domain state ownership;
- attention closure;
- mutable ownership proof;
- recovery/reconciliation;
- migration and replay;
- security/privacy;
- external-condition semantics;
- notification criticality;
- remote-execution boundary.

This reviewer must not be the author of the reviewed lane.

### Implementation agents

Use bounded agents for independent lanes. Each receives:

- exact objective;
- base SHA;
- task and dependency IDs;
- allowed paths;
- forbidden paths;
- authoritative documents;
- required tests;
- proof limits;
- stop conditions;
- handoff schema.

An implementation agent may not redefine a shared enum/schema without an
orchestrator-approved contract revision.

### Integration agent

The integration agent:

- applies lane commits in dependency order;
- resolves only mechanical conflicts;
- returns semantic conflicts to the orchestrator;
- runs focused and aggregate tests;
- audits module boundaries and duplicated logic;
- verifies generated contracts;
- never merges competing implementations by preserving both.

### Final auditor

Use an independent final audit against the exact integration head.

The auditor reviews:

- hard invariants;
- implementation-plan completion;
- traceability;
- fault and property evidence;
- security/privacy;
- performance;
- rollout defaults;
- known limitations.

## Required reading

Every agent reads:

1. `adrs/ADR-0012-operator-control-plane.md`;
2. `docs/operator-control-plane/README.md`;
3. the relevant sections of:
   - `ARCHITECTURE.md`;
   - `CONTRACTS.md`;
   - `IMPLEMENTATION_PLAN.md`;
   - `TRACEABILITY_MATRIX.md`;
   - `EVALUATION_AND_ROLLOUT.md`;
4. ADR-0011 and relevant supervision contracts;
5. existing authority files and repository profile;
6. current source modules being modified.

Agents working on remote-execution-adjacent fields also read
`REMOTE_EXECUTION_BOUNDARY.md`. They still implement no remote runtime.

## Dispatch sequence

### Phase 0 — Contract review

Before code:

1. confirm OCP-001 records and enum names;
2. confirm source owners;
3. confirm migration number;
4. confirm event vocabulary;
5. confirm module boundaries;
6. confirm schema versions;
7. confirm the one current stored `TaskPacket` contract; no alternate shapes,
   defaults, compatibility readers, or migration path are permitted;
8. record unresolved contract questions as explicit decisions.

Do not let implementation agents answer cross-cutting contract questions
independently.

### Phase 1 — Foundations

Dispatch:

- domain types;
- migration/store skeleton;
- trace context.

Keep write paths disabled.

### Phase 2 — Deterministic product core

Dispatch:

- canonical snapshot;
- attention source adapters;
- read-only API/CLI;
- attention/return UI.

This is the first useful vertical slice.

### Phase 3 — Artifact and wait support

Dispatch:

- investigation execution/artifact;
- external condition registry;
- knowledge proposal integration.

### Phase 4 — Liveness and recovery

Dispatch only after ownership contracts pass independent review:

- material progress;
- liveness observations/reducer;
- reconciliation;
- interventions.

Treat these as high-risk serial integration work even when file implementation
is parallel.

### Phase 5 — Presentation and adaptation

Dispatch:

- topology;
- presence/notification;
- supervisor integration;
- product/eval harness.

### Phase 6 — Activation

No implementation agent enables automatic behavior. Activation occurs only
through the reviewed rollout decision.

## Lane ownership

### Domain lane

Allowed:

```text
crates/harness-domain/src/operator_control.rs
minimal exports in crates/harness-domain/src/lib.rs
domain tests
schema examples when assigned
```

Forbidden:

```text
store SQL
orchestrator behavior
API routes
UI
```

Handoff:

- enum/state table;
- compatibility notes;
- validation APIs;
- tests;
- any deviation from contracts.

### Store lane

Allowed:

```text
migrations/0013_operator_control_plane.sql
crates/harness-store/src/operator_control/
minimal module registration
store tests
```

Forbidden:

```text
business classification
model invocation
UI projection semantics not specified by contract
generic state setters
```

Handoff:

- migration;
- repository interfaces;
- transaction/dedupe decisions;
- indexes;
- replay/fault evidence.

### Projection/attention lane

Allowed:

```text
operator_control/snapshot.rs
operator_control/attention.rs
projection schemas/examples
```

Forbidden:

```text
approval/publication authority
model-written current state
client-specific classification
```

Handoff:

- section rules;
- source adapters;
- limits/truncation;
- benchmark;
- lost-decision tests.

### Investigation lane

Allowed:

```text
investigation task validation
read-only context/tool setup
artifact schema/validator/evidence
fixtures
```

Forbidden:

```text
candidate creation
path lease for mutation
direct implementation task creation
automatic knowledge activation
```

Handoff:

- accepted/rejected examples;
- decision inventory;
- source/evidence limits;
- later-reuse example.

### External-condition lane

Allowed:

```text
typed adapters
runner/claims/results
API read/cancel
fault fixtures
```

Forbidden:

```text
arbitrary command string
generic condition-to-action
model polling
authority from result bytes
```

Handoff:

- identity rules;
- sequence/durability;
- ambiguity behavior;
- rate-limit/backoff;
- fault evidence.

### Progress/liveness lane

Allowed:

```text
material classifier
observations
episode reducer
observe/shadow policy
```

Forbidden:

```text
destructive action
fresh attempt
single opaque score
self-reported model status as truth
```

Handoff:

- labeled cases;
- classifier version;
- state reasons;
- false/missed classification report.

### Recovery lane

Allowed:

```text
ownership proof
startup/targeted reconciliation
safe idempotent actions
recovery report
```

Forbidden:

```text
worktree deletion/reset
fresh attempt without proof
ambiguous effect retry
direct approval state mutation
```

Handoff:

- inventory;
- action matrix;
- fault sequence results;
- preserved-work proof;
- unresolved conflicts.

### API/CLI lane

Allowed:

```text
versioned DTOs
routes
middleware integration
pagination
SSE
CLI renderers
```

Forbidden:

```text
direct DB access from CLI
client-side current-state inference
generic attention resolve
```

Handoff:

- OpenAPI diff;
- examples;
- security tests;
- error/exit behavior.

### UI lane

Allowed:

```text
ui/src/operator-control/
thin route/page composition
UI tests/e2e/accessibility
```

Forbidden:

```text
business classification
local closure of attention
raw process control
visual-only progress
feature code added to App.tsx
```

Handoff:

- workflows;
- accessibility;
- stale/truncated/error states;
- screenshots only as supplementary evidence;
- usability fixture.

### Trace/eval lane

Allowed:

```text
correlation propagation
redacted exports
property/fault harness
product study fixtures
```

Forbidden:

```text
promotion decision without evidence
raw secret/reasoning capture
candidate self-grading
```

Handoff:

- graph validation;
- redaction tests;
- dataset/splits;
- seeds;
- hard-gate report.

## Task packet template

Every dispatched task uses this minimum packet.

```markdown
# OCP task <ID>: <title>

## Objective

One observable outcome.

## Depends on

Exact task IDs and commit SHAs.

## Authoritative contracts

Paths and headings.

## Owned paths

Exact files/directories.

## Forbidden paths

Exact files/directories.

## Required behavior

Closed numbered list.

## Non-goals

Explicit exclusions.

## Current contract

The one stored/API/schema behavior that is valid. New code rejects omitted,
legacy, and alternate shapes rather than translating them.

## Security and authority

What the task cannot authorize.

## Required tests

Focused positive, negative, replay, concurrency, fault, or UI tests.

## Performance/limits

Bounds and benchmark fixture.

## Stop conditions

Conditions requiring orchestrator decision.

## Handoff

Required summary, commits, tests, evidence, limitations, and decisions.
```

## Stop conditions

An agent stops and reports a decision instead of guessing when:

- an existing authority owner conflicts with the design;
- a shared enum/schema requires a breaking change;
- migration numbering or released data differs;
- exact ownership cannot be proved;
- an external effect outcome is unknown;
- a source adapter lacks a typed terminal outcome;
- security/privacy requires new persisted data;
- a module boundary would force major monolith growth;
- a required test needs unavailable infrastructure;
- a generated contract cannot be reproduced;
- implementation contradicts a hard invariant;
- scope requires remote execution;
- activation thresholds are missing.

The report contains:

```text
decision key
question
options
recommended option
evidence
affected tasks/paths
whether work can continue independently
```

The orchestrator creates a durable decision in the implementation process.

## Handoff contract

Every agent returns:

```text
task ID
base and head SHA
files changed
contract implemented
behavior before/after
tests run and exact results
tests not run and why
schema/migration/API changes
security/authority review
performance result
known limitations
open decisions
follow-up tasks
```

For code lanes, include:

```text
candidate commit
diff summary
path ownership compliance
new dependencies
feature flags/defaults
rollback behavior
```

No handoff may claim:

- “all tests pass” when only focused tests ran;
- “recovered” when state was only preserved;
- “safe” without naming the invariant/test;
- “complete” with open blocking attention;
- “compatible” when the requested payload is not the current contract.

## Review checklists

### Domain

- one owner per fact;
- closed enums;
- explicit unknown;
- legal transitions;
- bounded values;
- no generic setters;
- no legacy acceptance or compatibility reader.

### Store

- foreign keys/indexes;
- no unsafe cascade;
- transactions;
- dedupe;
- replay;
- concurrent claims;
- digests;
- retention.

### Orchestrator

- deterministic facts before model judgment;
- exact preconditions;
- smallest action;
- stale target rejection;
- ambiguity preservation;
- receipts;
- idempotency;
- no authority expansion.

### API

- authentication/session;
- CSRF/same-origin;
- expected revision;
- source-owner mutation;
- pagination/limits;
- redaction;
- error shape;
- SSE replay.

### UI

- canonical DTO only;
- action source visible;
- no acknowledgement/resolve confusion;
- stale/unknown/truncated;
- accessible;
- no color-only state;
- no decorative progress;
- no raw sensitive content.

### Tests

- positive;
- negative;
- replay;
- concurrency;
- fault;
- property;
- performance;
- accessibility;
- rollout default.

## Conflict resolution

Classify conflicts:

### Mechanical

Examples:

- module export ordering;
- import changes;
- generated formatting.

Integration agent resolves and reruns tests.

### Contract

Examples:

- different enum/state;
- different source owner;
- duplicate table;
- conflicting closure rule;
- different authority.

Stop integration. Orchestrator chooses one design and updates dependent lanes.

### Behavioral

Examples:

- different retry policy;
- different liveness threshold;
- different notification priority.

Resolve through evidence and policy, not by combining both.

### Ownership/custody

Examples:

- two agents create mutable attempt logic;
- one lane adds direct worktree mutation;
- generic route bypasses source owner.

Treat as blocking high risk and require independent review.

## Test execution policy

Run the narrowest useful test during iteration, then the required aggregate
checks before lane handoff.

Suggested progression:

```text
module unit tests
crate tests
contract/schema/OpenAPI checks
cross-crate integration tests
UI typecheck/unit/e2e
property and fault suites
workspace fmt/clippy/tests
```

Do not add large redundant end-to-end tests for behavior already proven by
lower-level invariants. Fault suites should be explicit/nightly where they are
too expensive for every PR, while a bounded representative subset remains in
PR CI.

Record exact command, duration, and result.

## Documentation updates

Each implementation slice updates:

- its implementation-plan status;
- contract deviations;
- traceability rows;
- operational procedure;
- schema/API examples;
- rollout mode/default;
- known limitations.

Do not let code become the only current specification.

## Integration acceptance

The orchestrator accepts a lane only when:

- owned paths respected;
- no duplicate contract owner;
- diff is comprehensible and bounded;
- focused tests pass;
- negative/fault tests cover authority;
- handoff is complete;
- limitations are honest;
- dependent lanes can consume a stable interface;
- independent review is complete when required.

## Program completion

Before final audit:

1. generate the requirement-to-commit/test matrix;
2. run hard invariant suites;
3. run migration/replay;
4. run fault matrix;
5. run security/redaction;
6. run performance budgets;
7. run UI accessibility;
8. run product evaluation for activated features;
9. verify defaults remain disabled/observe/shadow where evidence is incomplete;
10. verify rollback;
11. update operations;
12. audit exact head independently.

The final audit should refuse completion for performative surfaces that lack
measured benefit, even when their implementation is technically correct.
