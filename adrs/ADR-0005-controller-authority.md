# ADR-0005: Keep orchestration authority in deterministic controller code

**Status:** proposed

## Decision

Models may propose plans, implement bounded tasks, integrate, and review. `harnessd` exclusively owns task state, dependency scheduling, leases, Git operations, validation execution, evidence classification, retries, budgets, and publication gates.

## Rationale

A durable engineering tool cannot infer completion from agent prose. Deterministic state and machine-verifiable evidence prevent false completion, overlapping writes, stale-base proof, and uncontrolled publication.

## Consequences

- all model outputs use schemas and remain proposals until validated;
- a worker cannot verify itself or update repository completion authority;
- failures route by typed class rather than blind prompt retry;
- explicit human approval remains required for external writes.
