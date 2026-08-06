# Harness Console

Harness Console is a local-first control plane for running Codex against a
NeuralMatrix checkout. It combines a Rust daemon, an operator CLI, a dense
browser workspace, durable SQLite state, exact-SHA Git custody, bounded
parallel task execution, independent verification, evidence bundles, and
API-equivalent usage accounting.

The application is implemented from the blueprint retained in
`ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md`. Runtime data and managed worktrees
live under XDG directories; they are never written into this repository or the
registered NeuralMatrix checkout.

## What is included

- `harnessd`: localhost-only REST/SSE server, embedded React UI, Codex App
  Server supervisor, scheduler, approval broker, and recovery boundary.
- `harnessctl`: authenticated local API client for repository, run, task,
  approval, agent, worktree, evidence, and usage operations.
- `harness-probe`: bounded search, multi-file read, Cargo mapping, test
  selection, log summarization, and compiled-context inspection.
- `fake-app-server`: deterministic protocol simulator for smoke and failure
  testing without consuming a Codex turn.
- A pinned NeuralMatrix profile, Codex agent definitions, JSON Schemas,
  OpenAPI contract, SQLite migrations, systemd user unit, and release tooling.

## Requirements

- Linux, Git, Rust 1.97.1, Node.js 22+, npm, and `rg`.
- Codex CLI 0.146.0 authenticated for normal execution. The daemon verifies
  both the CLI version and generated App Server v2 schema digest before it
  enables mutable work.
- A clean NeuralMatrix coordination clone with `origin`, Git user name/email,
  and the authority files required by `profiles/neuralmatrix/profile.toml`.
- `gh` only when explicitly creating a draft pull request.

## Build

```bash
npm --prefix ui ci
npm --prefix ui run build
cargo build --workspace --all-targets
```

The compiled UI is embedded in `harnessd`, so build the UI before the daemon.
The repository also exposes the equivalent `cargo xtask ui-build` command.

## Run locally

Start the daemon and open the console:

```bash
cargo run -p harnessd -- serve
```

By default it listens only on `http://127.0.0.1:7310`. On first launch it uses
safe built-in defaults and the built-in NeuralMatrix profile. To install an
editable config:

```bash
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/harness-console"
cp config/harness.example.toml \
  "${XDG_CONFIG_HOME:-$HOME/.config}/harness-console/config.toml"
```

Run the preflight independently with:

```bash
cargo run -p harnessd -- doctor
```

For UI and API inspection without Codex:

```bash
cargo run -p harnessd -- serve --without-codex --no-browser
```

## First run

The browser guides the same flow as these CLI commands:

```bash
harnessctl repo add --path /path/to/NeuralMatrix
harnessctl repo list

harnessctl run create \
  --repo <repository-id> \
  --base origin/main \
  --mode plan_and_implement \
  --publication local_only \
  --objective-file examples/run-objective.md

harnessctl run start-architecture <run-id>
harnessctl run show <run-id>
harnessctl run approve-plan <run-id> --digest <plan-digest>
```

The controller fetches and pins the exact base SHA before architecture. Tasks
receive non-overlapping path leases and separate worktrees. Worker commits are
controller-custodied, independently verified, then cherry-picked in dependency
order into an integration worktree. Local runs complete only after explicit
integration approval and the final audit. Draft PR publication is a separate,
explicit action and the harness never merges automatically.

## Verification

```bash
npm --prefix ui run typecheck
npm --prefix ui test
npm --prefix ui run build
npm --prefix ui run test:e2e
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo xtask schema-check
cargo xtask openapi-check
cargo xtask app-server-bindings-check
```

`cargo xtask check` runs the contract checks, UI build, formatting check, and
workspace tests. `cargo xtask dist` produces a checksummed Linux tarball under
`dist/`.

The Playwright flow uses the system Chromium by default. Set
`HARNESS_CHROMIUM=/path/to/chromium` when it is installed elsewhere.

## Security and custody

- The HTTP listener rejects every non-loopback bind and every non-loopback
  `Host`, closing the usual DNS-rebinding path into localhost services.
- Browser/API mutations require a short-lived HttpOnly local session plus a
  per-session CSRF token and same-origin checks.
- App Server notifications are journaled before relational projection.
- Raw private reasoning is dropped by default; concise summaries are retained.
- Commands use process groups, timeouts, bounded concurrency, allowlisted
  environment inheritance, and complete content-addressed logs.
- Git commands are non-interactive and bounded; credential-bearing HTTP remote
  userinfo is removed before it can enter the database, API, or diagnostics.
- Command and file-change approvals are bound to both the exact task HEAD and a
  full mutable-worktree fingerprint, so a pending decision cannot authorize a
  changed staged, unstaged, or untracked state.
- Agents cannot push, publish, merge, or mutate the primary checkout. The
  controller performs exact-head operations after explicit approvals.
- Evidence and exports are tied to exact source SHAs and artifact digests.

The detailed operational and recovery procedures are in
`docs/OPERATIONS_RUNBOOK.md`; UI acceptance criteria are in
`docs/UI_WIREFRAMES.md`; App Server projection rules are in
`docs/APP_SERVER_EVENT_MAPPING.md`.

Security boundaries and contribution checks are summarized in `SECURITY.md`
and `CONTRIBUTING.md`.

## Development layout

```text
bins/       daemon, CLI, probe, and deterministic App Server simulator
crates/     domain, store, Codex, Git, runner, context, evidence, API, scheduler
ui/         React/Vite application matching mockups/run-workspace.html
generated/  pinned Codex App Server schemas and compatibility metadata
profiles/   NeuralMatrix authority, model, path, validator, and risk policy
```

License: MIT.
