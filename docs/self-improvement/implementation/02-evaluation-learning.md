## M2 — Evaluate

### SI-008 — Versioned eval cases and tasksets

**Depends on:** SI-004, SI-007

**Scope**

- `harness-eval` crate;
- case setup, objective, custody, resources, checks, privacy, and source;
- taskset revisions;
- train/development/holdout/canary/quarantine split authority;
- case materialization from a failure occurrence;
- immutable fixture digests.

**Tests**

- deterministic fixture creation;
- case revision history;
- split access controls;
- privacy/license blockers;
- champion baseline required before activation.

**Exit gate**

A real historical failure is reproducible as a development eval case.

### SI-009 — Grader bundles and reward contracts

**Depends on:** SI-008

**Scope**

- deterministic, model-rubric, human, and delayed grader types;
- negative-control signals;
- grader bundle digest;
- isolated read-only grader runtime;
- calibration and disagreement records;
- reward-integrity verdict.

**Tests**

- candidate cannot change grader files or ground truth;
- missing required side effect fails;
- model grader disagreement;
- grader version drift invalidates samples;
- known reward-hack fixture.

**Exit gate**

A case produces a signal vector and an independent reward-integrity result.

### SI-010 — Reproducible champion baseline runner

**Depends on:** SI-005, SI-008, SI-009

**Scope**

- run tasksets against an immutable policy bundle;
- exact runtime receipt;
- paired seed policy;
- result classification;
- experiment-specific cost attribution;
- local replay from retained artifacts.

**Tests**

- same manifest produces same deterministic result;
- infrastructure unavailable is not failure or success;
- candidate cannot access holdout;
- trace and evidence digests bind to sample.

**Exit gate**

The current BILDR bundle has a reproducible baseline on the first eval suite.

### SI-011 — Statistics, uncertainty, and regression budgets

**Depends on:** SI-010

**Scope**

- paired per-case deltas;
- bootstrap confidence intervals and an approved sequential option;
- minimum sample and successful-execution counts;
- variance and flake measures;
- critical-case regression gate;
- Pareto scorecard.

**Tests**

- synthetic known distributions;
- small-sample refusal;
- missing-pair handling;
- variance threshold;
- no aggregate win can hide a critical regression.

**Exit gate**

The evaluator can say `better`, `worse`, or `inconclusive` with an auditable
method.

### SI-012 — Hidden holdout custody and leakage invalidation

**Depends on:** SI-008–SI-011

**Scope**

- separate encryption/access boundary if secrets are needed;
- optimizer-denied holdout metadata and answers;
- access log;
- rotating holdout revisions;
- leakage declarations;
- automatic experiment invalidation.

**Tests**

- optimizer and candidate runtime access denied;
- leaked case invalidates results;
- holdout change requires new baseline;
- operator recovery path.

**Exit gate**

Promotion evidence can include a holdout receipt that the optimizer never saw.

## M3 — Learn

### SI-013 — Evidence-backed knowledge lifecycle

**Depends on:** SI-005–SI-007

**Scope**

- `harness-learning` crate;
- fact/procedure/warning/heuristic/anti-pattern records;
- scope, confidence, review, expiry, contradiction, supersession;
- optional context-compiler injection after active authority;
- usage and measured-impact telemetry.

**Tests**

- authority always wins;
- expired or contradicted item excluded;
- candidate cannot self-validate a knowledge item;
- deterministic packet receipt with and without item.

**Exit gate**

A reviewed lesson can reduce repeated exploration without becoming hidden
authority.

### SI-014 — Immutable policy component and bundle registry

**Depends on:** SI-001, SI-004

**Scope**

- component manifests and risk classes;
- bundle composition and digest;
- parent lineage;
- active bindings by repository/task family/model family;
- rollback compatibility;
- diff renderer.

**Tests**

- component digest stability;
- Red dimensions cannot be experiment edits;
- bundle lineage cycle rejection;
- exact rollback target.

**Exit gate**

The current production behavior is represented as a champion bundle.

### SI-015 — Improvement candidate contract and prediction calibration

**Depends on:** SI-007, SI-011, SI-014

**Scope**

- target failures;
- bounded component edits;
- causal hypothesis and predicted deltas;
- required ablations/evals;
- risk and budget;
- accepted/rejected candidate history;
- compare prediction to result.

**Tests**

- broad or unscoped edit rejected;
- missing prediction rejected;
- incompatible bundle rejected;
- calibration history remains immutable.

**Exit gate**

A human can author a candidate and see its exact predicted effect and required
evidence.

### SI-016 — Bounded optimizer

**Depends on:** SI-013–SI-015

**Scope**

- failure miner selects representative development evidence;
- optimizer sees only editable components and development cases;
- small candidate set and no-change control;
- rejected-edit buffer;
- one causal hypothesis per candidate;
- suggestions only.

**Tests**

- no holdout access;
- no protected component edit;
- bounded output/diff;
- repeated failed proposal suppressed;
- model failure cannot mutate policy.

**Exit gate**

BILDR can propose, but not activate, a policy candidate from recurring evidence.
