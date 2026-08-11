# ADR-0007: Use a governed cross-run self-improvement loop

- **Status:** accepted architecture
- **Date:** 2026-08-11

## Context

BILDR adapts planning and execution inside a run but does not persist a
cross-run learning state. Directly allowing a worker or governor to rewrite
prompts, profiles, skills, or code from recent experience would mix proposal,
evaluation, and promotion authority.

## Decision

Add a separate governed loop:

```text
observe -> label -> materialize eval -> baseline -> propose -> evaluate
-> holdout -> shadow -> canary -> promote -> monitor -> rollback
```

The first shipped mode is observation only. Suggestion, shadow, canary, and
guarded promotion are separate capability gates.

The unit of promotion is an immutable policy bundle. The production controller
owns activation. The optimizer cannot approve or publish its own candidate.

## Consequences

Positive:

- improvement can accumulate across runs;
- each change is measurable and reversible;
- failed and rejected ideas remain useful evidence;
- external training can plug into the same promotion path.

Costs:

- additional storage and eval compute;
- delayed outcomes and task fixtures require curation;
- more explicit state machines and UI;
- weak feedback may yield no promotable candidate.

## Rejected alternatives

- mutate live prompts after every run;
- let one model propose and grade its own update;
- optimize only aggregate completion or cost;
- require external reinforcement learning before local evals exist.
