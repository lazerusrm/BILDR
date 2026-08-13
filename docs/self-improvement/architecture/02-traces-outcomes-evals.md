## 7. Trace architecture

### 7.1 Why a graph

Long-running work is not linear. A thread can:

- compact context;
- spawn several subagents;
- retry from a prior attempt;
- resume after restart;
- fork a remediation path;
- reuse a shared prefix.

A normalized trace is therefore a content-addressed directed acyclic graph.

### 7.2 Trace nodes

Node kinds include:

- system/developer/user/model message;
- reasoning summary;
- tool request and result;
- command;
- file read/change;
- approval request/decision;
- compaction request/summary;
- subagent spawn/join;
- validation/evidence/finding;
- operator feedback;
- outcome label.

Each node records its source raw-event IDs, redaction class, content digest,
model-call receipt where applicable, and runtime timestamp.

### 7.3 Edge types

- `next`
- `context_parent`
- `tool_result_of`
- `spawned_by`
- `joined_into`
- `compacted_from`
- `retry_of`
- `derived_from`
- `supersedes`

A branch manifest is one root-to-leaf execution path with shared-prefix
references. Branches can be evaluated or exported without duplicating common
nodes.

### 7.4 Redaction and export

Raw events remain local authority. Trace exports:

- omit credentials and secret-like environment values;
- omit raw private reasoning unless an explicit source permits retention;
- carry source-license and customer-data classification;
- include a redaction-policy digest;
- fail closed when a required redactor is unavailable;
- are content addressed and revocable from future export sets.

## 8. Outcome model

No single reward represents engineering quality. BILDR stores a vector of
signals and a separate promotion decision.

### 8.1 Immediate signals

- deterministic acceptance passed;
- required validator coverage;
- verifier finding count and severity;
- retries and remediation rounds;
- unexpected-path or policy violations;
- completion state;
- token, cost, wall time, tool calls, context size.

### 8.2 Human signals

- approved without correction;
- approved after correction;
- requested changes;
- abandoned because result was wrong or too costly;
- qualitative rating with a structured reason;
- correction patch and affected failure class.

### 8.3 Delayed signals

- CI failure after local acceptance;
- PR reopen or review regression;
- downstream defect;
- rollback;
- production incident;
- maintenance burden or recurring cleanup;
- later confirmation that a result remained correct.

Outcome revisions are append-only. A later label does not rewrite the original
measurement; it supersedes it with provenance.

## 9. Eval case lifecycle

```text
observed incident or curated task
  -> candidate case
  -> sanitized reproducible fixture
  -> deterministic baseline
  -> independent review
  -> development split
  -> hidden holdout or canary
  -> active
  -> quarantined / retired
```

Each case must prove:

- deterministic setup from an immutable source;
- candidate cannot modify the grader or expected answer;
- the current champion has a recorded baseline;
- flaky or infrastructure-sensitive behavior is classified;
- the case tests a real behavior, not an implementation detail;
- provenance and privacy permit reuse.

A failure miner may propose cases. It cannot activate or assign holdout splits.

## 10. Grader and reward integrity

### 10.1 Authoritative behavior owner

Every case names the lowest credible behavior owner. For code work this is
usually a test, build, state transition, or external observable effect—not a
model’s opinion of its own patch.

### 10.2 Multi-signal design

Use model graders only where deterministic evidence cannot express the property.
A qualitative grader is paired with:

- deterministic preconditions;
- a rubric and grader version;
- counterexamples;
- calibration samples;
- human adjudication sampling;
- disagreement and uncertainty reporting.

### 10.3 Negative controls

Every reward contract declares signals expected to move when the task is truly
solved. Examples:

- required API or tool effects occurred;
- a target file or state changed;
- a regression test fails before and passes after;
- no protected files changed;
- the final claim matches command evidence.

A score that improves while negative controls degrade is a reward-integrity
failure and blocks promotion.

### 10.4 Isolation and anti-tamper

The candidate runtime cannot write:

- grader code;
- fixture ground truth;
- holdout metadata;
- evaluation result tables;
- promotion state.

The grader runs in a separate read-only authority boundary and consumes
candidate artifacts by digest.
