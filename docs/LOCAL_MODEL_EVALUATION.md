# Local-model evaluation lanes

These small, deterministic lanes compare a direct Qwodex session with BILDR
using the same immutable source revision, model, effort, task text, and budget.
They are intentionally local-only: no credentials, remote pushes, or production
repositories are needed. Record the route receipt, start/end time, turns,
retries, completion state, changed paths, validation receipts, and final diff.

For a Qwodex run, select one catalog model and effort once at admission. BILDR
must use that exact receipt for every normal role; it must not turn a local
comparison into an unreported multi-model ensemble.

| Lane | Objective | Acceptance | Measured outcome |
| --- | --- | --- | --- |
| Documentation scope | Add one named README section in a fixture without changing any other file. | `git diff --check --`; an exact changed-path assertion; a deterministic phrase/count assertion. | Exact-target completion, changed lines, unrelated paths, elapsed time. |
| Focused Rust repair | Fix one localized behavior defect and add its regression test. | `cargo test -p harness-orchestrator --lib`; `cargo fmt --all --check`; `git diff --check --`. | Focused test result, crate result, diff size, elapsed time. |
| Contract crossing | Change one behavior across its Rust owner and canonical schema/example. | Controller-owned exact-head tests plus `cargo xtask schema-check`. | Required-file coverage, gate pass rate, total elapsed time. |
| Recovery | Interrupt a bounded task during an owned turn, then use the controller's retry path. | Read-only run/evidence receipts show a new attempt and immutable prior attempt. | Retry latency, attempt increment, prior-attempt immutability, false-completion count. |
| AVO lineage | Submit a bounded AVO episode whose candidate and hard-gate receipts resolve exactly. | AVO contract/store tests and the episode digest/receipt query. A hard gate must be the controller-projected outcome for a completed, non-invalidated validation owned by that candidate task attempt. | Digest match, receipt resolution rate, stale-lineage rejection. |

## Comparison protocol

1. Create two clean worktrees at the same SHA beneath
   `/mnt/bulk-fast/agent-builds`.
2. Use the same selected model and reasoning effort. For direct Qwodex, use
   the credential-free local provider; record its version and provider line.
3. Give both lanes the same task objective and only the acceptance commands
   appropriate to the lane. Do not give the direct model BILDR-only internal
   state as an advantage or a handicap.
4. BILDR's validation receipts, route receipt, retries, and rejected output
   are part of the result. A direct run has no equivalent custody claim.
5. Compare correctness before speed. A run that changes the wrong files,
   lacks a passing gate, or cannot bind its evidence to the candidate head is
   a failed lane regardless of a shorter elapsed time.

The documentation lane is the first smoke test, not a capability claim. Move
to the focused repair and recovery lanes only after its diff and route receipt
are independently valid.
