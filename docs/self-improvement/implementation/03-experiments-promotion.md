## M4 — Experiment

### SI-017 — Offline champion/challenger experiments

**Depends on:** SI-010–SI-012, SI-015

**Scope**

- `harness-promotion` crate begins with experiment state;
- paired champion/challenger assignments;
- development, ablation, and holdout stages;
- hard-gate matrix;
- experiment UI.

**Tests**

- same cases and runtime policy;
- state transition model;
- failed hard gate blocks progression;
- exact evidence bundle.

**Exit gate**

A candidate can be accepted or rejected offline with held-out evidence.

### SI-018 — Shadow execution

**Depends on:** SI-017

**Scope**

- replay or parallel read-only challenger;
- no effect on production result;
- disagreement, cost, latency, and distribution-drift measures;
- bounded sample and kill switch.

**Tests**

- challenger cannot write production worktree or state;
- production result independent of challenger failure;
- shadow budget enforcement;
- privacy classification.

**Exit gate**

A candidate survives real-distribution observation without affecting users.

### SI-019 — Guarded canary

**Depends on:** SI-018

**Scope**

- deterministic small-cohort assignment;
- concurrent champion fallback;
- allowlisted task families and policy dimensions;
- stop thresholds and maximum exposure;
- explicit operator start.

**Tests**

- assignment reproducibility;
- critical regression stops immediately;
- fallback custody;
- restart recovery;
- no external publication difference without existing approval.

**Exit gate**

A candidate can serve a bounded local canary and revert automatically on hard
failure.

## M5 — Promote

### SI-020 — Promotion decision and active bundle binding

**Depends on:** SI-019

**Scope**

- digest-bound decision schema;
- required offline/holdout/shadow/canary receipts;
- reviewer and operator approval;
- atomic active pointer change;
- scope-specific champion;
- no automatic re-promotion.

**Tests**

- stale or mismatched digest rejected;
- missing gate rejected;
- atomicity across crash;
- audit export.

**Exit gate**

A candidate becomes champion only through one complete promotion record.

### SI-021 — Rollback and emergency stop

**Depends on:** SI-020

**Scope**

- one-action rollback;
- automatic hard-constraint rollback;
- global improvement kill switch;
- preserve evidence and candidate lineage;
- operator notification and incident record.

**Tests**

- rollback during active runs affects only new assignments;
- crash recovery;
- repeated rollback idempotence;
- ordinary BILDR remains usable.

**Exit gate**

Every promoted bundle has a tested rollback path.

### SI-022 — Drift monitoring and promotion health

**Depends on:** SI-020, SI-021

**Scope**

- post-promotion quality, correction, regression, cost, and latency trends;
- distribution drift;
- grader drift;
- stale tasksets;
- alert and rollback recommendations.

**Tests**

- delayed outcomes update health;
- expected seasonality versus sudden drift;
- missing telemetry is visible, not healthy;
- threshold/version custody.

**Exit gate**

A promoted candidate remains under measurable post-promotion supervision.
