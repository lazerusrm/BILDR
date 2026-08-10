# Security policy

BILDR is a single-user, local-only control plane. It deliberately
does not provide network-facing authentication, multi-user authorization, or
TLS termination. Do not bind it to a LAN/WAN address or place it behind a
shared reverse proxy. Use an SSH tunnel to the loopback listener for remote
viewing.

## Security invariants

- The daemon accepts only loopback bind addresses.
- Mutations require a same-origin request, an HttpOnly SameSite session cookie,
  and a session-bound CSRF token.
- Codex execution is enabled only when the configured CLI version and generated
  App Server schema digest both match.
- Mutable agents receive isolated managed worktrees, exact path leases, no
  network access, and no authority to commit, push, publish, or merge.
- Git publication is an explicit exact-head operation. The harness creates only
  draft pull requests and never marks them ready or merges them.
- Raw private reasoning is discarded by default. Protocol frames, command
  output, and evidence use bounded ingestion and content-addressed storage.
- HTTP credential userinfo is removed from Git remotes and diagnostics before
  persistence or display.

Runtime databases, artifacts, command spools, exports, and worktrees belong in
the configured XDG state/data directories. Protect those directories as
sensitive local data and do not copy them into this repository.

## Reporting a vulnerability

Do not include credentials, private reasoning, proprietary source, or raw
runtime databases in a public report. Provide a minimal reproduction, affected
version, expected invariant, and sanitized logs to the repository owner through
a private security channel. Until a fix is available, stop the daemon and
preserve affected worktrees rather than bypassing a custody check.
