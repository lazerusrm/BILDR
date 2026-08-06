# Contributing

The product contract is `ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md`. UI changes
must also satisfy `docs/UI_WIREFRAMES.md`; App Server changes must use the pinned
bundle in `generated/codex-app-server-schema/` and preserve the raw-first event
boundary.

Keep runtime state and build caches out of the checkout. On shared development
hosts, place Cargo targets, npm caches, and `ui/node_modules` on the designated
bulk build filesystem.

Before submitting a change, run:

```bash
npm --prefix ui run typecheck
npm --prefix ui test
npm --prefix ui run build
npm --prefix ui run test:e2e
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo xtask check
```

Changes must not weaken localhost-only service boundaries, exact-SHA evidence,
path/worktree custody, independent verification, explicit external-write
approval, or the no-automatic-merge rule. Add focused negative tests for each
security or state-machine correction.
