# ADR-0002: Use a local Rust daemon with an embedded browser/PWA UI

**Status:** proposed

## Decision

Ship `harnessd` and `harnessctl` as Rust binaries. `harnessd` serves an embedded React/TypeScript UI on localhost and runs under `systemd --user`. `harness-desktop` is a Tauri 2 / wry OS-webview shell that loads that same loopback origin; it is not a second controller.

## Rationale

This shape is simple to operate on Linux, keeps one durable controller process, integrates naturally with Git/processes/SQLite, requires no Node runtime, and preserves a Codex-app-like desktop experience without adding native-shell complexity early.

## Consequences

- REST/SSE is a real internal contract;
- state-changing browser requests require local-session/CSRF protection;
- remote access is an SSH tunnel in v1;
- multi-user/TLS deployment is out of scope.
