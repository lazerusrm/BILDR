## 11. Candidate generation

### 11.1 Inputs

The optimizer receives:

- failure clusters and representative redacted traces;
- current policy component manifests;
- development eval cases and scores;
- prior accepted and rejected edits;
- hard constraints and editable dimensions;
- budget and target task family.

It does not receive holdout answers or hidden grader implementation.

### 11.2 Output contract

Every candidate provides:

- parent bundle;
- exact component edits;
- target failure classes;
- causal hypothesis;
- predicted metric deltas;
- expected tradeoffs;
- risk class;
- required ablations;
- required eval suites;
- maximum rollout budget;
- rollback target;
- reasons the proposal could fail.

Decision observability matters: the prediction is recorded before results, so
the system can learn whether its optimizer is calibrated.

### 11.3 Candidate diversity

Generate a small bounded set:

- minimal targeted edit;
- alternative mechanism;
- no-change control when useful.

Avoid broad “improve everything” mutations. Prefer one causal hypothesis per
candidate.

## 12. Evaluation and statistics

### 12.1 Paired comparison

Champion and challenger run the same case revision, runtime class, seed policy,
budget, and grader bundle. Results are paired by case.

### 12.2 Repetition

Stochastic tasks use multiple seeds. Promotion requires:

- minimum case count;
- minimum successful execution count;
- confidence interval or approved sequential test;
- no critical per-case regression;
- bounded variance;
- stable result across retry;
- no unresolved infrastructure bias.

### 12.3 Scorecard

Suggested constrained objective:

```text
hard gates:
  safety violations == 0
  custody violations == 0
  critical regressions == 0
  reward integrity == PASS
  required proof coverage >= floor
  holdout quality delta >= floor

then optimize Pareto frontier:
  task success
  human correction rate
  verifier severity
  downstream regression rate
  token cost
  wall time
  tool calls
  context efficiency
  retry count
```

Do not collapse all signals into an opaque scalar for operator decisions.
A scalar may be used by an optimizer only with its component vector retained.

### 12.4 Splits

- **Training/mining:** visible to the optimizer.
- **Development:** visible scores, used for iteration.
- **Holdout:** cases and answers hidden from optimizer; used for promotion.
- **Canary:** sampled real tasks after shadow validation.
- **Quarantine:** flaky, leaked, privacy-blocked, or infrastructure-invalid.

Holdout access is logged. Any leak invalidates affected experiments.

## 13. Experiment stages

```text
PROPOSED
  -> VALIDATED
  -> BASELINING
  -> OFFLINE_RUNNING
  -> OFFLINE_PASSED
  -> HOLDOUT_RUNNING
  -> HOLDOUT_PASSED
  -> SHADOW_RUNNING
  -> SHADOW_PASSED
  -> CANARY_RUNNING
  -> PROMOTION_REVIEW
  -> PROMOTED
  -> MONITORING
  -> ROLLED_BACK | RETIRED
```

Failure, cancellation, inconclusive, infrastructure-unavailable, reward-integrity
failure, and leakage are distinct terminal or blocking states.

### 13.1 Shadow

The challenger observes or replays real work but cannot determine the production
result, change the repository, or affect the human-facing run. Shadowing measures
distribution fit, cost, latency, and disagreement.

### 13.2 Canary

Canary is permitted only for allowlisted task families and Green/Amber policy
dimensions. It has:

- small deterministic assignment;
- concurrent champion fallback;
- explicit stop thresholds;
- maximum task and cost count;
- no external publication difference without operator approval.

### 13.3 Promotion

A promotion decision binds:

- candidate and resulting bundle digests;
- champion digest;
- eval/taskset/grader versions;
- holdout receipt;
- statistical method and result;
- reward-integrity verdict;
- safety-anchor digest;
- reviewer and approval;
- activation scope;
- rollback target and triggers.

### 13.4 Rollback

Rollback is one controller action that switches the active bundle pointer to the
prior digest. It does not delete evidence. Automatic rollback may trigger on
hard safety/custody violations or explicit quality thresholds; automatic
re-promotion is forbidden.
