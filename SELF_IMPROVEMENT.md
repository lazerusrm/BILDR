# BILDR governed self-improvement

**Status:** architecture and implementation contract; no autonomous self-modification is enabled
**Repository basis:** `release/initial-public-preview@5c7dc3b678b7811cf8d8676e39c9ffcd0ba02e55`
**Decision:** BILDR should improve from its own execution evidence, but only through a versioned, eval-gated, reversible promotion system.

BILDR already adapts inside a run through plan revision, governor replanning,
bounded continuation, retry continuity, model escalation, independent
verification, and final audit. The missing capability is durable learning across
runs.

The target loop is:

```text
observe real work
  -> normalize traces and outcomes
  -> mine repeatable failure modes
  -> materialize versioned eval cases
  -> propose a bounded harness candidate
  -> compare champion and challenger on paired evals
  -> test hidden holdouts and reward-integrity controls
  -> shadow or canary the candidate
  -> approve and promote an immutable policy bundle
  -> monitor, regress, or roll back
  -> repeat
```

This is intentionally not an unrestricted process that edits the running
controller. Every editable component has a risk class. Core custody, approval,
security, evidence, and promotion rules form a frozen safety anchor and can
change only through a normal reviewed repository change.

## Design set

- [Current-state audit](docs/audits/SELF_IMPROVEMENT_AUDIT_2026-08-11.md)
- [Full architecture](docs/SELF_IMPROVEMENT_ARCHITECTURE.md)
- [Dependency-ordered implementation plan](docs/SELF_IMPROVEMENT_IMPLEMENTATION_PLAN.md)
- [Research and reference map](docs/SELF_IMPROVEMENT_REFERENCE.md)
- [Design validation](docs/audits/SELF_IMPROVEMENT_DESIGN_VALIDATION_2026-08-11.md)
- [ADR-0007: governed improvement loop](adrs/ADR-0007-governed-self-improvement-loop.md)
- [ADR-0008: eval-gated promotion](adrs/ADR-0008-eval-gated-promotion.md)
- [ADR-0009: frozen safety anchor](adrs/ADR-0009-frozen-safety-anchor.md)
- [ADR-0010: branch-aware trace graph](adrs/ADR-0010-branch-aware-trace-graph.md)

The `schemas/` additions define the first durable wire contracts for traces,
eval cases, grader bundles, improvement candidates, experiments, promotion
decisions, and knowledge items. The records under
`examples/self-improvement/` are non-authoritative conformance fixtures.

## Recommended operating modes

1. **Observe only** — score and inspect runs without changing future behavior.
2. **Suggest** — produce candidate diffs and eval evidence for human review.
3. **Shadow** — execute a challenger without allowing its result to mutate the
   production run.
4. **Guarded promotion** — automatically promote only explicitly allowlisted,
   low-risk policy dimensions after statistical and safety gates.
5. **Repository change** — prompts, skills, validators, controller code, and
   frozen-anchor changes land through a draft pull request and normal review.

BILDR should ship modes 1 and 2 first. Modes 3 and 4 are later milestones.
Controller-code self-improvement remains mode 5.
