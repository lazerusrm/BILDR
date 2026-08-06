# ADR-0002: Use a local Rust daemon with an embedded browser/PWA UI

**Status:** proposed

## Decision

Ship `harnessd` and `harnessctl` as Rust binaries. `harnessd` serves an embedded React/TypeScript UI on localhost and runs under `systemd --user`. A Tauri shell is optional after v1.

## Rationale

This shape is simple to operate on Linux, keeps one durable controller process, integrates naturally with Git/processes/SQLite, requires no Node runtime, and preserves a Codex-app-like desktop experience without adding native-shell complexity early.

## Consequences

- REST/SSE is a real internal contract;
- state-changing browser requests require local-session/CSRF protection;
- remote access is an SSH tunnel in v1;
- multi-user/TLS deployment is out of scope.
