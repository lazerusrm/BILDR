# BILDR governed self-improvement architecture

**Status:** target architecture
**Scope:** cross-run learning, evaluation, harness evolution, promotion, rollback, and optional external training
**Non-goal:** autonomous mutation of the running controller or weakening of operator custody

## 1. Objective

BILDR should become better at software-engineering work as it accumulates
evidence, while preserving four properties:

1. **Improvement is measurable.** A candidate must beat a pinned champion on
   relevant held-out tasks, not merely explain why it should be better.
2. **Improvement is attributable.** Every candidate declares the exact policy
   dimensions changed and predicts the expected effect before evaluation.
3. **Improvement is reversible.** A promoted bundle is immutable, content
   addressed, monitored, and linked to a known rollback target.
4. **Safety does not enter the optimization objective.** Custody, sandbox,
   approval, evidence, redaction, holdout, promotion, and rollback controls are
   hard constraints.

The architecture borrows the useful abstraction behind Prime Intellect Lab:
tasks define what is attempted, a harness defines how the system attempts it, a
runtime defines where it executes, and graders define the measured result.
BILDR adapts that abstraction to a local-first, exact-SHA engineering control
plane.

## 2. System model

```text
                     Human objectives and real repository work
                                      |
                                      v
+----------------------- Production execution plane ------------------------+
| Current BILDR controller, worktrees, agents, commands, validations,       |
| evidence, findings, costs, publication, and delayed operator feedback     |
+-------------------------------+-------------------------------------------+
                                |
                                v
+------------------------- Observation and trace plane ---------------------+
| Raw events -> normalized message graph -> branches -> effects -> outcome  |
| labels -> redacted export manifests -> quality and drift projections      |
+-------------------------------+-------------------------------------------+
                                |
                                v
+--------------------------- Evaluation plane ------------------------------+
| Tasksets -> eval cases -> runtime fixtures -> grader bundles -> paired     |
| champion/challenger samples -> holdout -> uncertainty -> reward integrity |
+-------------------------------+-------------------------------------------+
                                |
                                v
+--------------------- Learning and candidate plane ------------------------+
| Failure miner -> knowledge curator -> bounded optimizer -> candidate diff |
| + prediction + risk class + required evals + rollback target              |
+-------------------------------+-------------------------------------------+
                                |
                                v
+------------------------ Promotion control plane --------------------------+
| Offline -> replay -> shadow -> canary -> approval -> immutable champion    |
| -> drift monitor -> automatic stop / explicit rollback                    |
+---------------------------------------------------------------------------+
```

The planes are separate services and data authorities. A component that proposes
a change cannot be the sole component that grades, promotes, or publishes it.

## 3. Prime-compatible decomposition

### 3.1 Taskset: what must be solved

A BILDR taskset is a versioned collection of `eval_case` revisions. Each case
contains:

- objective and task-family classification;
- repository fixture or immutable setup procedure;
- allowed and forbidden mutable paths;
- expected side effects;
- deterministic acceptance checks;
- optional rubric graders;
- resource class and time/token budget;
- source provenance and license/privacy classification;
- split: train, development, holdout, canary, or quarantine.

The taskset is independent of the harness candidate. A taskset must be runnable
against the current champion, a challenger, a null/minimal harness when useful,
and external compatible harnesses.

### 3.2 Harness: how BILDR solves it

The harness is an immutable **policy bundle**. It is not only a system prompt.
A bundle may version:

- role/developer instructions;
- intent-interview behavior;
- planning and review contracts;
- context selection and ordering;
- retrieval and knowledge policy;
- model and reasoning routes;
- token, tool, retry, and continuation budgets;
- scheduler and delegation policy;
- skills and probe behavior;
- validator selection;
- compaction and handoff strategy;
- UI/operator defaults that affect execution;
- optional model or adapter identity.

Every component has a stable ID, content digest, schema version, owner, risk
class, and rollback compatibility.

### 3.3 Runtime: where the rollout happens

A runtime record binds:

- repository and exact source SHA;
- fixture/setup digest;
- worktree and sandbox class;
- BILDR and protocol versions;
- model and adapter versions;
- tool versions;
- host/runner fingerprint;
- network and secret posture;
- random seed and sampling settings;
- resource limits;
- grader isolation boundary.

The same task and policy bundle must be replayable in an equivalent runtime.
Infrastructure-unavailable is not a failed task and never counts as a passing
sample.

### 3.4 Grader bundle: what success means

A grader bundle contains independently versioned signals. It may combine:

- exact deterministic test or state transition;
- source or artifact validation;
- required side-effect count and identity;
- independent review verdict;
- human acceptance or correction;
- delayed regression/reopen signal;
- cost, latency, tool-call, and context-efficiency metrics;
- safety and policy violations;
- model rubric for qualitative properties.

Quality and safety gates are constraints. Cost is optimized only among
candidates that meet quality and safety floors.

## 4. The improvement unit

The unit of promotion is an immutable policy bundle, not a mutable prompt file
or live database row.

```json
{
  "bundle_id": "policy_01...",
  "parent_bundle_id": "policy_00...",
  "components": {
    "context_policy": "sha256:...",
    "worker_instructions": "sha256:...",
    "routing_policy": "sha256:...",
    "validator_policy": "sha256:..."
  },
  "safety_anchor_digest": "sha256:...",
  "created_from_candidate": "candidate_01..."
}
```

A candidate changes the smallest coherent set of components. Multi-component
changes require an interaction hypothesis and ablation plan.

## 5. Editable surfaces and risk classes

### Green: eligible for future guarded automatic promotion

Only after sufficient observation and explicit operator enablement:

- ordering or weighting among already approved context sources;
- bounded token budget within configured min/max;
- bounded read-only probe parameters;
- retry timing within fixed safety limits;
- cache/summary policy that cannot remove required authority.

Green changes still require paired evals, holdout success, no hard-constraint
regression, a minimum sample, and automatic rollback.

### Amber: eval-gated and human-approved

- role prompts and planning contracts;
- skills and procedural memory;
- model/effort routing;
- delegation strategy;
- validator selection or thresholds;
- knowledge retrieval rules;
- compaction and handoff strategy.

### Red: repository-change only

- controller state machines;
- Git/worktree/path custody;
- sandbox and network policy;
- approval semantics;
- secret handling and redaction;
- evidence and result-class semantics;
- taskset split and holdout access control;
- grader isolation and promotion code;
- external writes, push, publication, readiness, merge, or deployment;
- database integrity and migration rules;
- the frozen safety anchor itself.

A candidate touching Red is invalid for the promotion service. It may be
materialized only as a normal draft repository change.

## 6. Frozen safety anchor

The anchor is a versioned document plus generated digest embedded in the
controller. It includes:

- controller remains the sole mutable-state authority;
- primary checkout is never an agent worktree;
- task writes remain leased and exact-head bound;
- no external write without the existing explicit controller gate;
- no automatic merge;
- no hidden broad approval;
- localhost and origin protections remain;
- credentials are not copied into the database;
- raw private reasoning remains excluded by default;
- required proof cannot be converted to success when unavailable;
- worker and optimizer cannot approve their own output;
- optimizer cannot read hidden holdout answers or grader internals;
- grader runtime is isolated from the candidate runtime;
- promotion is digest-bound, reversible, and auditable;
- safety regressions cannot be traded for lower cost or higher aggregate score.

At startup and before every experiment, BILDR verifies the anchor digest. A
mismatch disables optimization and promotion but does not block ordinary
read-only inspection.
