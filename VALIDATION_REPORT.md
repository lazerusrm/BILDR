# BILDR PR2 validation report

**Validated:** August 12, 2026
**Platform:** Linux x86_64
**Candidate:** local PR2 remediation worktree

## Result

The PR2 remediation passes the repository's local build, contract, lint, unit,
and browser acceptance gates. The change closes the release-review findings for
legacy evaluation-custody migration safety, partial-persistence retry custody,
truthful improvement-mode presentation, and the Bubblewrap isolation boundary.

In particular, a legacy v8 database with no evaluation records is rebuilt to
the current custody shape. A populated legacy v8 database is rejected before
its schema-version marker is changed, because controller/evidence ownership
cannot be attributed safely after the fact. Retried observer-snapshot arms use
attempt-scoped worktree and artifact custody while immutable downstream wires
bind only the closed historical contract. A real evaluator recovery proof
injects a failure immediately after taskset-membership persistence, then
resumes through fresh historical and fixed arms, retains the two historical
audit chains, deduplicates their byte-identical materialization artifact by
content digest, completes the fixed sample, and cleans all transient sealed
inputs. Observe-only readiness now requires Bubblewrap exactly 0.11.0 with
namespace isolation. Hosted CI builds that exact Bubblewrap release after
verifying its published SHA-256.

## Verification

The following checks passed on the candidate worktree:

- `cargo fmt --all -- --check`
- `cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings`
- `cargo test --locked --offline --workspace --all-targets`
  - 221 tests passed; the repository's real two-arm pinned-worktree smoke remains intentionally ignored in the normal suite.
  - The legacy-v8 empty-repair and populated-fail-closed migrations passed.
  - The partial-command-stream retry and supported Bubblewrap isolation-boundary tests passed.
- `cargo test --offline -p harnessd evaluation::tests::isolated_controller_smoke_persists_and_replays -- --ignored --nocapture`
  - Passed after an injected post-membership-persistence failure, then a fresh historical retry and fixed-arm completion under real Bubblewrap.
  - Verified durable attempt audit chains, immutable fixed-sample replay, removed worktrees, and no transient sealed input or target custody.
- `cargo xtask check`
  - 20 JSON schemas and 17 examples conformed.
  - 85 local OpenAPI references resolved, the runtime-status fixture conformed, and 62 router paths matched.
- `npm --prefix ui run typecheck`
- `npm --prefix ui test`
  - 16 tests passed, including disabled, observe-only, and anchor-mismatch Improvement Center states.
- `npm --prefix ui run build`
- `npm --prefix ui run test:e2e`
  - 4 browser flows passed.
- `bash .github/scripts/check-repository-policy.sh`
- `git diff --check`

## Proof limits

This report is local source and host-boundary evidence only. It does not claim
hosted CI for the unpublished remediation, release publication, or a live
production deployment. Those require the exact committed candidate, final
independent review, and staged/local-harness evidence.
