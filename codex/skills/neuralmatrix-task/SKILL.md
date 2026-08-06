---
name: neuralmatrix-task
description: Execute one Harness Console NeuralMatrix task packet in its leased worktree and return the required structured handoff.
---

# NeuralMatrix Harness Task

1. Read `AGENTS.md`, `CODEX.md`, the task packet's named active authorities, and
   its checklist rows before editing.
2. Confirm the exact base SHA, owned paths, serial/forbidden paths, dependencies,
   success criteria, required positive/negative tests, metrics, evidence, proof
   limits, diff/tool/token budgets, and stop conditions.
3. Use `harness-probe` for bounded discovery. Inspect outside owned paths only as
   needed; do not edit them.
4. Implement the canonical change. Do not add compatibility aliases, accept-both
   decoding, broad normalization, translation, stale/latest/raw-id/URL/binding or
   client repair, dual authority, semantic/protocol fallback, stubs, weakened
   tests, or hidden scope reduction.
5. Run the smallest credible tests named by the task. Record missing tools,
   fixtures, targets, runners, or hardware as unavailable, not success.
6. Return one `nm.orchestration.handoff.v1` object. State proof limits and the
   maximum checklist state the evidence could justify. Do not update completion
   state yourself.
7. Do not create/manage branches, rebase, push, open a pull request, merge, or add
   AI author/co-author attribution. Harness Console owns Git publication.
