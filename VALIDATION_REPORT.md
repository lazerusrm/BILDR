# BILDR validation report

**Validated:** August 10, 2026
**Platform:** Linux x86_64
**Release candidate:** 0.1.0

## Result

The local release candidate passes the repository's build, contract, lint, unit,
and browser acceptance gates.

Planning certification now fails closed around a structured certificate. The
certificate binds the plan, base, profile, authority set, reviewer, feasibility
assessment, and review evidence. Human findings use the same revision path as
reviewer findings. Blocking findings trigger revision; advisory findings remain
in execution context without forcing another full planning cycle. Repeated,
oscillating, or nonshrinking blockers stop for an explicit decision instead of
consuming the remaining run budget.

Execution completion now depends on controller-owned evidence from the exact
integrated head. Profiles bind validators to lifecycle gates and changed paths.
Code changes require behavioral proof. Automated acceptance and operator
attestations bind to the integrated SHA and deterministic signoff packet. Human
review is a resting state with approve and request-changes paths. Required
draft-PR checks prove only the expected remote head; incomplete, stale, or
malformed check results cannot advance the run.

The public repository uses neutral orchestration schemas, examples, role names,
and profile shapes. The strict BILDR profile validates Rust, browser, contract,
and delivery paths without importing policy from another repository. Public
change metadata rejects automation attribution, and the repository policy check
rejects tool-specific root instruction files.

## Verification

The following checks passed on the final local code shape:

- `cargo xtask check`
  - 8 JSON schema and example files parsed.
  - 59 OpenAPI references resolved.
  - 50 API routes matched their implementations.
  - The pinned protocol schema digest matched.
  - The production browser application built.
  - All 77 Rust tests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `npm --prefix ui run typecheck`
- `npm --prefix ui test -- --run`: 7 tests passed.
- `npm --prefix ui run test:e2e`: 2 browser flows passed.
- `bash .github/scripts/check-repository-policy.sh`
- Positive and negative contribution-metadata policy checks.
- `git diff --check`

## Proof limits

This local report does not claim hosted CI, release publication, or live
deployment proof. The draft pull request must run the public workflow on the
published head before the controller can record CI proof. BILDR never merges
automatically.
