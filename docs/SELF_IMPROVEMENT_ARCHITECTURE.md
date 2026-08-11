# BILDR governed self-improvement architecture

**Status:** target architecture
**Scope:** cross-run learning, evaluation, harness evolution, promotion, rollback, and optional external training
**Non-goal:** autonomous mutation of the running controller or weakening of operator custody

This architecture is split into reviewable sections. Together they define the
complete governed improvement system.

1. [Foundations and safety anchor](self-improvement/architecture/01-foundations.md)
2. [Traces, outcomes, evals, and reward integrity](self-improvement/architecture/02-traces-outcomes-evals.md)
3. [Candidates, experiments, promotion, and rollback](self-improvement/architecture/03-candidates-experiments.md)
4. [Knowledge, operations, components, data, API, UI, and threat model](self-improvement/architecture/04-knowledge-operations.md)

The central invariant is that proposal, evaluation, and promotion authority are
separate. BILDR may improve prompts, context policy, routing, skills, budgets,
validators, and eventually model adapters, but custody and safety controls remain
outside the optimization action space.
