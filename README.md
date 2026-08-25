# BILDR

[![CI](https://github.com/lazerusrm/BILDR/actions/workflows/ci.yml/badge.svg)](https://github.com/lazerusrm/BILDR/actions/workflows/ci.yml)

BILDR is a local-first control plane for running Codex against local
Git repositories. It combines a Rust daemon, an operator CLI, a focused
governor-first browser workspace, durable SQLite state, exact-SHA Git custody, bounded
parallel task execution, independent verification, evidence bundles, and
API-equivalent usage accounting.

## What is included

- `harnessd`: localhost-only REST/SSE server, embedded React UI, Codex App
  Server supervisor, scheduler, approval broker, and recovery boundary.
- `harnessctl`: authenticated local API client for repository, run, task,
  approval, agent, worktree, evidence, and usage operations.
- `harness-probe`: bounded search, multi-file read, Cargo mapping, test
  selection, log summarization, and compiled-context inspection.
- `harness-desktop`: Tauri 2 / wry OS-webview shell that loads the same
  localhost UI, starts or attaches to `harnessd`, and adds a tray, approval
  notifications, a native folder picker, and `bildr://` / `harness://` openers.
- `fake-app-server`: deterministic protocol simulator for smoke and failure
  testing without consuming a Codex turn.
- A repository-neutral default profile, an opt-in strict BILDR profile,
  runtime role definitions, JSON Schemas,
  OpenAPI contract, SQLite migrations, systemd user unit, and release tooling.

## Requirements

- Linux, Git, Rust 1.97.1, Node.js 22+, npm, and `rg`.
- Codex CLI 0.148.0 authenticated for normal execution. The daemon verifies
  both the CLI version and generated App Server v2 schema digest before it
  enables mutable work.
- A clean Git coordination clone with `origin` and Git user name/email. The
  strict BILDR profile additionally requires its authority files.
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
safe built-in defaults and the built-in `general` profile. Profiles are bound
to each repository registration, so one daemon can operate general repositories
and the BILDR checkout without weakening either. The UI registers new checkouts
as `general`; use `harnessctl repo add --profile bildr` for this repository's
stricter contract. A custom profile ID resolves from
`$XDG_CONFIG_HOME/harness-console/profiles/<id>.toml`; an explicit TOML path is
also accepted when registering from the CLI. The daemon `--profile` flag is
the default/doctor profile, not a global override for repositories already in
the database. To install an editable config:

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

The native desktop app uses the OS webview (WebKitGTK, WKWebView, or WebView2),
not a bundled Chromium. It probes loopback, starts `harnessd` when needed, and
opens one window on that origin:

```bash
cargo run -p harness-desktop -- --without-codex
```

Close the window to keep the app in the tray. Use **Register repository…** on
the tray, or **Browse…** in the registration dialog, to pick a local checkout
with the native folder picker. `bildr://open` and `harness://open` show the
window. Operator mutations still use the localhost REST and CSRF API; the
desktop process does not create runs, approve plans, or talk to Git or Codex
itself.

## First run

The registration dialog scans common development folders under the current
user's home directory, highlights Git and GitHub checkouts, and keeps a manual
path field as a fallback. Set `HARNESS_REPOSITORY_SEARCH_ROOTS` to a
colon-separated path list to replace the default search roots.

If a discovered checkout is blocked only because it is dirty, choose **Create
clean checkout** on the Repositories page. Pick a new sibling directory and the
Harness will clone the repository's active default branch, reuse the source checkout's Git objects, verify the
new checkout, and move the unused registration to it. The original checkout and
its untracked files are not changed. Keep that source checkout in place while
the coordination clone uses its shared object store.

The browser guides the same flow as these CLI commands:

```bash
harnessctl repo add --path /path/to/project
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

The controller fetches and pins the exact base SHA before architecture. Every
proposed plan then passes a fresh read-only adversarial review for feasibility,
critical-path liveness, behavior-first progress, test timing, and recovery
authority. Blocking findings trigger a complete replacement revision and
another review. Automatic plan approval can approve only a zero-finding
`CERTIFIED` digest; it cannot skip this loop. Tasks receive non-overlapping
path leases and separate worktrees. Worker commits are controller-custodied,
independently verified, then cherry-picked in dependency order into an
integration worktree. Local runs complete only after explicit integration
approval and the final audit. Draft PR publication is a separate, explicit
action and the harness never merges automatically.

At run creation, choose the governor family (Sol, Terra, or Luna) and thinking
level from low through max. Independent Sol verification and Sol final signoff
remain fixed. The Runs page keeps the governor as the primary conversation,
shows its latest update and meaningful activity, exposes delegated child
threads with their own token and API-equivalent cost for read-only inspection,
and renders pending approvals inline. Governor message history opens at the
latest update; manual scrollback gets a twelve-second reading grace before new
messages resume following the bottom.

The Settings page persists plan approval posture, reasoning-summary retention,
raw-reasoning retention, bounded governor continuation/budgets, account
handoff, and YOLO mode locally. YOLO applies `approval_policy = never` only to
new writable Codex threads inside controller-managed worktrees; push, draft-PR
publication, readiness changes, and merge retain their explicit controller
approval boundaries.

The account strip discovers the default `~/.codex` home, Codex accounts already
registered in Headroom, plus sibling
`.codex-*`, `.codex_*`, `codex-*`, and `codex_*` homes that contain Codex
credentials. It can also add a private Harness-managed Codex home using Codex
0.148.0 device authorization, then rename, re-authenticate, or remove those
managed profiles from Settings. Add explicit external homes with the colon-separated
`HARNESS_CODEX_ACCOUNT_HOMES` environment variable. Selecting an account starts
App Server with that `CODEX_HOME`, persists only the opaque local profile ID,
and refreshes the 0.148.0 `account/read` and `account/rateLimits/read` telemetry.
The limits strip keeps local observations and reports a smoothed hourly burn
forecast that weights the longer 24-hour trend most heavily, blends in the
recent four-hour pace, and anchors both to the provider window average.
Harness never copies authentication tokens into its database. Account switching
is blocked while agent sessions are active. When automatic handoff is enabled,
the scheduler may select a healthier signed-in account only between bounded
attempts when the active account has 10% capacity or less; an active thread is
never transplanted. Harness orchestrates the installed Codex login flow but
does not implement or persist OpenAI credentials itself.

The Usage page attributes token and API-equivalent cost samples by Codex
account, repository, and agent. Tool-heavy Codex turns are priced from every
distinct App Server model call rather than only the final call, with cumulative
thread counters used solely for deduplication. Older samples created before
account attribution are shown explicitly as unattributed history.

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
profiles/   repository-neutral defaults plus optional strict repository policies
```

License: Apache-2.0.
