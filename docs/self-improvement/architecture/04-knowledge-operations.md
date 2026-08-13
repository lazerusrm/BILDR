## 14. Knowledge lifecycle

A knowledge item is a testable, scoped statement derived from evidence.

Required fields:

- statement and type: fact, procedure, warning, heuristic, or anti-pattern;
- repository/task/model/runtime scope;
- source traces, outcomes, evidence, and findings;
- confidence and reviewer;
- created, revalidated, expires;
- contradiction and supersession links;
- context-use count;
- measured impact when included;
- authority relationship.

Rules:

- active repository authority always wins;
- unreviewed items are suggestions, not instructions;
- stale items stop being injected;
- contradictory items are surfaced, not blended;
- a candidate cannot cite its own result as independent evidence;
- knowledge promotion and policy promotion are separate decisions.

## 15. Quality gardening

A recurring quality gardener implements the “garbage collection” loop:

1. scan explicit golden principles and quality grades;
2. detect drift and repeated workaround shapes;
3. correlate drift with failures, costs, or context burden;
4. propose a narrow repository change;
5. run normal validation and independent review;
6. open a draft change only;
7. record whether the cleanup improved later runs.

Gardening is not allowed to edit the frozen anchor, change product semantics, or
merge automatically.

## 16. Optional external training

BILDR should first optimize non-weight harness components. Later it may export a
provider-neutral package:

```text
taskset manifest
harness/policy bundle
runtime contract
trace graph branches
grader bundle
redaction and license receipts
baseline metrics
```

Adapters may target Prime Verifiers/Lab or OpenAI Evals. An externally trained
model or adapter returns as another immutable candidate. It must pass the same
local holdout, shadow, canary, promotion, and rollback gates as a prompt or
scheduler change.

External training is never allowed to:

- read secrets or unapproved customer data;
- become the authority for local outcomes;
- bypass the frozen anchor;
- overwrite the active model route directly;
- train on hidden holdout cases.

## 17. Proposed component boundaries

Add new crates instead of expanding the orchestrator monolith:

### `harness-trace`

- raw-event to trace-DAG projection;
- branch manifests;
- redaction/export receipts;
- trace query and diff.

### `harness-eval`

- tasksets and eval cases;
- fixture setup;
- grader bundles;
- paired runner;
- statistics and holdout custody.

### `harness-learning`

- failure taxonomy and clustering;
- knowledge lifecycle;
- candidate generation contracts;
- prediction calibration.

### `harness-promotion`

- policy bundles;
- experiment state machine;
- shadow/canary assignment;
- promotion/rollback;
- drift monitoring.

Existing crates integrate through narrow interfaces:

- `harness-store`: persistence and append-only receipts;
- `harness-domain`: stable IDs/enums shared across services;
- `harness-context`: optional reviewed knowledge and policy variant;
- `harness-profile`: frozen anchor and editable-dimension declarations;
- `harness-orchestrator`: production-run adapter, not owner of eval logic;
- `harness-api`: routes and SSE projections;
- `harness-evidence`: experiment evidence binding;
- `harness-usage`: experiment cost attribution.

## 18. Data model

New tables, grouped by authority:

### Observation

- `trace_graphs`
- `trace_nodes`
- `trace_edges`
- `trace_branches`
- `trace_exports`
- `run_outcomes`
- `outcome_revisions`
- `failure_occurrences`
- `failure_clusters`

### Evaluation

- `tasksets`
- `taskset_revisions`
- `eval_cases`
- `eval_case_revisions`
- `eval_case_splits`
- `grader_bundles`
- `grader_definitions`
- `eval_runs`
- `eval_samples`
- `eval_signal_values`
- `reward_integrity_checks`
- `holdout_access_log`

### Learning

- `policy_components`
- `policy_bundles`
- `improvement_candidates`
- `candidate_edits`
- `candidate_predictions`
- `knowledge_items`
- `knowledge_evidence`
- `knowledge_usage`
- `optimizer_calibration`

### Promotion

- `experiments`
- `experiment_assignments`
- `promotion_decisions`
- `active_policy_bindings`
- `rollback_events`
- `drift_snapshots`

All mutable projections point to immutable revision records. Digests and source
versions are mandatory.

## 19. API outline

```text
GET  /api/v1/improvement/overview
GET  /api/v1/improvement/traces
GET  /api/v1/improvement/traces/{id}
POST /api/v1/improvement/outcomes
GET  /api/v1/improvement/failures
POST /api/v1/improvement/eval-cases
GET  /api/v1/improvement/tasksets
POST /api/v1/improvement/eval-runs
GET  /api/v1/improvement/eval-runs/{id}
POST /api/v1/improvement/candidates
GET  /api/v1/improvement/candidates/{id}
POST /api/v1/improvement/experiments
POST /api/v1/improvement/experiments/{id}/shadow
POST /api/v1/improvement/experiments/{id}/canary
POST /api/v1/improvement/promotions/{id}/approve
POST /api/v1/improvement/rollback
GET  /api/v1/improvement/knowledge
POST /api/v1/improvement/knowledge/{id}/review
```

Every mutation uses the existing local session, CSRF, optimistic concurrency,
approval, and event-journal conventions.

## 20. Improvement Center UX

### Overview

- active champion bundle and scope;
- frozen-anchor health;
- taskset coverage and stale cases;
- current quality/cost/latency trends;
- reward-integrity alarms;
- open candidates and experiments;
- canary and rollback state;
- drift and garbage-collection queue.

### Trace Explorer

- branch-aware graph;
- shared-prefix visualization;
- model/tool/command/effect timeline;
- redaction and export status;
- outcome revisions;
- compare successful and failed traces.

### Failure Lab

- failure clusters with frequency, severity, and cost;
- representative evidence;
- candidate eval cases;
- confirmed versus speculative causes;
- “materialize eval” workflow.

### Eval Suites

- taskset revisions and split custody;
- champion baseline;
- flake/infrastructure status;
- grader bundle and negative controls;
- leakage warnings;
- case retirement.

### Candidate Studio

- component-level diff;
- predicted deltas and causal hypothesis;
- risk classification;
- required evals/ablations;
- rejected-edit history;
- optimizer calibration.

### Experiments

- paired per-case comparison;
- uncertainty and variance;
- hard-gate matrix;
- reward-integrity signals;
- shadow/canary disagreement;
- cost and latency;
- promote, reject, or request another experiment.

### Knowledge

- reviewed facts/procedures/warnings;
- scope and freshness;
- sources and contradictions;
- context usage and measured impact;
- expire or supersede.

The UI displays summaries and evidence, not hidden private chain-of-thought.

## 21. Operational modes and kill switches

Global settings:

- `disabled`
- `observe_only`
- `suggest_only`
- `shadow_allowed`
- `guarded_promotion`

Independent kill switches:

- trace export;
- candidate generation;
- external evaluation;
- external training;
- shadow;
- canary;
- automatic low-risk promotion.

A single emergency action disables all improvement execution and preserves
ordinary BILDR operation plus read-only evidence access.

## 22. Threat model

Key threats and controls:

| Threat | Control |
|---|---|
| Candidate grades itself | Separate evaluator and promotion services |
| Holdout leakage | Split custody, access log, invalidation |
| Grader tampering | Isolated read-only grader runtime |
| Reward hacking | Negative controls, signal vector, red-team grader |
| Overfitting | hidden holdout, rotating cases, canary |
| Safety erosion | frozen anchor, Red dimensions invalid |
| Trace poisoning | provenance, confidence, independent review |
| Stale memory | expiry, contradiction, impact tracking |
| Evaluation flake | repetitions, result classes, quarantine |
| Cost-only optimization | hard quality/safety floors |
| Live self-edit | immutable bundles; repository changes only |
| Irreversible promotion | digest-bound rollback and retained champion |
| Customer-data export | classification, redaction, explicit export receipt |
| Monoculture drift | diversity controls and recurring external/human audits |

## 23. Success criteria

BILDR is meaningfully self-improving when it can demonstrate, on repeated
held-out and live-canary evidence, that a promoted policy bundle:

- reduces a known failure class;
- preserves or improves task success;
- does not increase human correction or downstream regression;
- preserves all custody and safety constraints;
- remains within cost and latency budgets;
- generalizes beyond the cases used to propose it;
- can be rolled back without data loss;
- leaves a complete causal and evidence trail.

A candidate count, generated prompt, training loss, or higher proxy score is not
success.
