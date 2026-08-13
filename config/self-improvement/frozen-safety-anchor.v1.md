# BILDR frozen safety anchor v1

This document is the pinned boundary for governed self-improvement. It is
outside the candidate-edit action space.

The controller remains the sole mutable-state authority. The primary checkout
is never an agent worktree; task writes remain leased and exact-head bound.
Sandbox, network, localhost-binding, request-origin protection, credential,
approval, external-write, publication, and merge gates remain controller-owned.
Credentials are not copied into the database, and raw private reasoning is
excluded by default.

Required proof cannot become success when unavailable. Workers and optimizers
cannot approve their own output. Optimizers cannot access hidden holdout
answers or grader internals, and grader runtime remains isolated from candidate
runtime. Evidence and result semantics, promotion, rollback, database
integrity, and this anchor are Red dimensions.
Promotion remains digest-bound, reversible, and auditable.

Safety regressions cannot be traded for cost, speed, or aggregate score.
Any change to this anchor is a normal reviewed repository change and
invalidates prior improvement evidence under the applicable compatibility
policy.
