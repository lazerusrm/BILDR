# Harness Console v0.1.0 Validation Report

**Validated:** 2026-08-06
**Platform:** Linux x86_64
**Protocol target:** Codex CLI/App Server 0.146.0
**Protocol schema SHA-256:** `4e5515cf5ec697e85c16a627a49cf5759f5f102a5a770456d546670effee26fa`

## Result

Harness Console is implemented as a buildable local application rather than a
design-only blueprint. The Rust daemon, CLI, probe utility, embedded React UI,
durable SQLite store, Codex App Server adapter, Git custody layer, bounded
runner, orchestrator, evidence exporter, and release tooling all passed their
acceptance checks.

## Automated verification

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo test --workspace --all-targets --locked`: passed, 40 tests.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`:
  passed with no warnings.
- `npm --prefix ui run typecheck`: passed.
- `npm --prefix ui test`: passed, 3 component/contract tests.
- `npm --prefix ui run build`: passed; production assets were emitted and
  embedded by the daemon.
- `npm --prefix ui run test:e2e`: passed, 1 browser workflow in Chromium.
- `cargo xtask schema-check`: passed for all seven JSON/TOML contracts.
- `cargo xtask openapi-check`: passed; 38 local references resolved and all 32
  router paths are represented in the OpenAPI contract.
- `cargo xtask app-server-bindings-check`: passed against the pinned generated
  App Server v2 schema.
- `cargo xtask dist --check`: passed.
- `cargo xtask dist`: passed; emitted the optimized Linux x86_64 distribution
  with its SHA-256 sidecar. The checksum, gzip/tar structure, and packaged ELF
  binaries were verified, and the packaged `harnessd doctor --without-codex`
  preflight passed against clean XDG directories.
- SQLite migration smoke: passed with WAL mode, schema version 3, 42
  application tables, the approval worktree-binding column, and a clean
  `PRAGMA foreign_key_check`.
- Credential-pattern and archive-integrity audits: passed.

## End-to-end runtime verification

A clean temporary Git repository and bare `origin` were registered through the
real daemon/API. The deterministic fake App Server then exercised the same
JSONL protocol path used by Codex:

1. Runtime preflight accepted version 0.146.0 and the pinned schema digest.
2. Architecture produced a bounded plan, which was digest-bound and approved.
3. A worker changed its leased path in a dedicated worktree.
4. The controller custodied and committed the change.
5. An independent verifier accepted the exact task SHA.
6. The controller integrated the commit and required integration approval.
7. A final auditor accepted the exact integration SHA.
8. The run reached `COMPLETED`; usage/cost projections and exact-SHA evidence
   were queryable.
9. A deterministic `tar.zst` evidence bundle passed checksum, decompression,
   and archive-content checks.
10. After a clean daemon restart, the completed run, evidence, runtime status,
    WAL database, and zero projection lag were recovered successfully.

The served production UI returned HTTP 200 after restart. The Playwright flow
covered session bootstrap, repository/run navigation, responsive workspace
layout, inspector tabs, command controls, and compact/mobile behavior.

## UI acceptance

The production UI was reviewed at the supplied 1440 x 960 reference size
against `mockups/run-workspace.html`. It preserves the specified dense dark
three-pane console, status/header hierarchy, activity workspace, inspector,
navigation, typography, spacing, state colors, responsive collapse behavior,
keyboard focus, semantic landmarks, and reduced-motion support.

## Operational note

The end-to-end acceptance run uses `fake-app-server` so it is deterministic and
does not consume an external Codex turn. Normal operation requires an
authenticated Codex CLI 0.146.0; `harnessd doctor` refuses mutable execution if
the installed version or generated protocol digest differs from the pin.
