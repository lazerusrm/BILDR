# Bounded AVO variation episodes

NVIDIA’s AVO work is a useful architecture reference: retain lineage and
retrieved knowledge, vary a candidate, evaluate it with a hard correctness
gate, and redirect after stagnation. BILDR implements that as a bounded,
typed contract rather than an unconstrained autonomous executor.

`harness.avo-episode.v1` is a canonical, SHA-256-bound episode snapshot. It
pins the champion bundle, knowledge snapshot, hard-gate policy, a score
baseline, variation budget, and stagnation limit. Every variation records its
parent incumbent, strategy, candidate receipt, hard-gate evidence, and score.
Only a passed hard gate can compete on score; only a strictly higher score can
become the next incumbent.

Episodes use BILDR's immutable `avo_episode` revision ledger. Admission binds
the aggregate ID to `episode_id`, verifies the canonical digest and lineage,
and resolves the champion and every candidate receipt against their immutable
stored records. Revisions may remain `running` while the bounded loop
continues, then terminate as `passed`, `failed`, or `inconclusive`; no revision
rewrites an earlier snapshot.

Each `hard_gate` receipt resolves to an immutable `OutcomeV1` *revision* (its
ID and stored payload hash, rather than an outcome aggregate ID). That outcome
must be the controller-projected authoritative validation for the exact
candidate task attempt. Admission resolves its validation record in the same
transaction and requires a completed, non-invalidated, run-owned attempt; the
outcome's source receipt hash, source revision, completion time, stable outcome
ID, and closed result (`passed`, `failed`, or `unavailable`) must all match the
canonical validation receipt. An outcome for another candidate, an operator
assertion, a missing validation, or a substituted digest is rejected.

## Operator workflow

The Improvement Center exposes a **Bounded AVO episodes** panel and the API
exposes the equivalent authenticated endpoints:

- `GET /api/v1/improvement/avo-episodes` lists the current immutable snapshot
  for each episode.
- `GET /api/v1/improvement/avo-episodes/{episodeId}` reads one current
  snapshot.
- `POST /api/v1/improvement/avo-episodes` records a canonical episode with one
  of the closed lifecycle states: `proposed`, `running`, `passed`, `failed`, or
  `inconclusive`.

The server derives the revision and event identities from the exact episode
digest and requested lifecycle state. Replaying the exact same import is
idempotent; changing any episode content or lifecycle state creates a distinct
immutable revision. Admission rejects missing, substituted, or digest-mismatched
champion, candidate, and hard-gate receipts.

These endpoints are intentionally recording and review surfaces, not a launch
button. They cannot start an agent, invoke a supervisor, alter a worktree,
contact an external environment, or promote a candidate. A future executor
needs its own explicit authority boundary and evaluation receipts before it can
consume a recorded trajectory.

The contract returns one of three non-authoritative directives:

- `continue` permits preparation of the next already-bounded variation.
- `request_advisory_redirect` tells supervision to review the retained
  trajectory after the configured stagnant streak. It grants no execution
  authority.
- `stop_budget` ends the episode.

An `improved` result is not a promotion. It must still be represented as a
normal BILDR candidate and pass the existing offline, holdout, shadow, canary,
independent-review, operator-approval, and rollback contracts. The AVO type
does not schedule agents, write repositories, call an environment, or enable
external notifications.

The initial BILDR use is repository improvement work. An ARC adapter is a
separate integration: it requires an approved ARC credential, a public-set
evaluation authority, isolated action/tool budgets, and a scored receipt
format before it may construct an episode. This prevents public benchmark
actions from being mistaken for an authorization to deploy changes.
