# ADR-0008: Require independent eval-gated promotion

- **Status:** accepted architecture
- **Date:** 2026-08-11

## Context

A self-improving harness can overfit visible cases, exploit reward seams, or
trade real task quality for a cheaper proxy. Repository validation proves a
change at one SHA; it does not by itself prove that a harness policy generalizes.

## Decision

A candidate may influence production only after:

1. paired champion/challenger evaluation;
2. immutable taskset, runtime, and grader versions;
3. minimum sample and uncertainty requirements;
4. hidden holdout evaluation;
5. reward-integrity and negative-control checks;
6. no critical per-case or safety regression;
7. shadow and, where allowed, bounded canary;
8. a digest-bound promotion decision and rollback target.

Quality and safety are hard gates. Cost and latency are optimized only after
those gates pass.

The optimizer cannot access holdout answers or mutate grader state. The
evaluator and promotion service are separate from candidate generation.

## Consequences

Promotion is slower but attributable. Some plausible candidates will remain
inconclusive. That is preferred to false improvement.

## Rejected alternatives

- promote on development score alone;
- use a single opaque scalar reward;
- treat model self-preference as sufficient;
- hide per-case regressions inside an aggregate improvement.
