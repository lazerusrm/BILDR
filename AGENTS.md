# Harness Console contributor guide

The product contract is `ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md`. UI work must
also satisfy `docs/UI_WIREFRAMES.md`; protocol work must use the pinned bundle
under `generated/codex-app-server-schema/` and
`docs/APP_SERVER_EVENT_MAPPING.md`.

## Build and verification

- `cargo xtask ui-build` builds the embedded browser application.
- `cargo fmt --all --check` checks Rust formatting.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` checks Rust code.
- `cargo test --workspace` runs the Rust suite.
- `cargo xtask check` runs contract, schema, UI, and release checks.
- `npm --prefix ui test` runs frontend unit tests.
- `npm --prefix ui run test:e2e` runs Playwright flows.

Generated build trees and caches must stay on the bulk build filesystem. Never
put runtime state, credentials, raw private reasoning, or NeuralMatrix checkout
data in this repository. Do not weaken localhost-only service boundaries,
worktree custody, exact-SHA evidence, explicit external-write approvals, or the
no-automatic-merge rule.
