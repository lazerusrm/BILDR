# Harness Console operations runbook

Harness Console is a single-user, Linux-local service. The daemon owns Codex
App Server, orchestration, SQLite, worktrees, command processes, evidence, and
the browser/API boundary. It never needs a public listener.

## Runtime prerequisites

Required:

- Codex CLI 0.146.0, authenticated through the normal Codex login flow;
- Git, `rg`, Bash, and the build tools required by the target repository;
- a clean NeuralMatrix coordination clone with `origin` and Git identity;
- `gh` only for the optional, explicit draft-PR operation.

Rust and Node are build-time requirements. Docker/Podman and hardware runners
are optional validator prerequisites; an unavailable prerequisite is recorded
as unavailable and never converted into a passing result.

## Runtime paths

Defaults follow the XDG base-directory convention:

```text
$XDG_CONFIG_HOME/harness-console/config.toml
$XDG_CONFIG_HOME/harness-console/profiles/neuralmatrix.toml
$XDG_DATA_HOME/harness-console/harness.sqlite3
$XDG_DATA_HOME/harness-console/artifacts/sha256/
$XDG_DATA_HOME/harness-console/worktrees/
$XDG_DATA_HOME/harness-console/exports/
$XDG_CACHE_HOME/harness-console/codex-schema-probes/
$XDG_STATE_HOME/harness-console/command-spool/
```

When an XDG variable is absent, normal `~/.config`, `~/.local/share`,
`~/.cache`, and `~/.local/state` fallbacks are used. Directories and the
database are created private to the user. Repository-local `.harness-runtime`
state is forbidden.

## Build and install

```bash
npm --prefix ui ci
npm --prefix ui run build
cargo build --release -p harnessd -p harnessctl -p harness-probe

install -Dm755 target/release/harnessd "$HOME/.local/bin/harnessd"
install -Dm755 target/release/harnessctl "$HOME/.local/bin/harnessctl"
install -Dm755 target/release/harness-probe "$HOME/.local/bin/harness-probe"
install -Dm644 packaging/systemd/harnessd.service \
  "$HOME/.config/systemd/user/harnessd.service"
mkdir -p "$HOME/.config/harness-console"
cp config/harness.example.toml "$HOME/.config/harness-console/config.toml"

systemctl --user daemon-reload
systemctl --user enable --now harnessd.service
```

Alternatively, `cargo xtask dist` creates a checksummed tarball with the three
release binaries, systemd unit, config/profile, Codex agent and skill files,
schemas, compatibility metadata, API contract, README, version, and license.

## Preflight and local startup

```bash
harnessd doctor
harnessd serve --no-browser
```

`doctor` validates the safe bind, XDG permissions, migrations/WAL/integrity,
profile, configured Codex version, and generated protocol digest. Use
`--without-codex` only to inspect storage/profile and run the UI in degraded
mode.

The default browser endpoint is `http://127.0.0.1:7310`. `harnessd` rejects
non-loopback IPs and hostnames that resolve to any non-loopback address.

Useful health commands:

```bash
harnessctl status
harnessctl doctor
harnessctl runtime
harnessctl runtime codex
```

## Register a NeuralMatrix checkout

```bash
harnessctl repo add --path /path/to/NeuralMatrix
harnessctl repo list
harnessctl repo inspect <repository-id>
```

Registration checks Git state, branch, identity, remote policy, filesystem
access, and all mandatory authority files. If a legitimate mirror uses a
different origin, pass an exact, intentional `--expected-origin`; this affects
only that registration and does not weaken the profile globally.

A repository may remain registered while temporarily blocked, but mutable run
creation stays disabled until inspection reports it ready.

## Create and execute a run

```bash
harnessctl run create \
  --repo <repository-id> \
  --base origin/main \
  --mode plan_and_implement \
  --publication local_only \
  --objective-file ./objective.md

harnessctl run start-architecture <run-id>
harnessctl run show <run-id>
harnessctl run approve-plan <run-id> --digest <plan-digest>
```

Run creation fetches `origin`, resolves an exact lowercase 40-character SHA,
creates a read-only inspection worktree, hashes authority, and freezes that
tuple. The architecture turn must return a schema-valid DAG. Execution cannot
begin until the operator approves the exact plan digest.

For a planning-only run, approval closes the run without creating mutable task
attempts. For implementation runs, the scheduler starts dependency-ready tasks
within configured agent/resource/path limits.

## Day-to-day controls

```bash
harnessctl run list
harnessctl run show <run-id>
harnessctl run pause <run-id>
harnessctl run resume <run-id>
harnessctl run stop <run-id> --interrupt

harnessctl approvals list --run <run-id>
harnessctl approvals decide <approval-id> accept --expected-version <version>
harnessctl approvals decide <approval-id> decline --note "reason"

harnessctl agent show <agent-id>
harnessctl agent steer <agent-id> "bounded correction"
harnessctl agent interrupt <agent-id>

harnessctl worktree list --run <run-id>
harnessctl worktree preserve <worktree-id> --reason "manual inspection"
```

Approvals are bound to the durable request, optimistic version, current task
HEAD, and a deterministic fingerprint of staged, unstaged, untracked, mode,
and symlink state where applicable. Any custody change makes the decision
stale. Unknown App Server request classes fail closed instead of being guessed.

## Verification, retry, and completion

Workers cannot commit. When a worker turn finishes, the controller validates
the complete tracked and untracked diff, path ownership, serial reservations,
forbidden paths, line/file budget, and unchanged base HEAD. Only then does the
controller create the task commit and start an independent read-only verifier.

If a task needs another immutable attempt:

```bash
harnessctl run retry-task <task-id> \
  --reason "verifier finding" \
  --revised-objective "corrected bounded objective" \
  --model-route same \
  --additional-token-budget 12000

harnessctl run request-review <task-id>
```

Use `--model-route escalate_terra` only for an intentional escalation. Prior
packet, diff, logs, findings, and worktree remain durable.

After every task is verified, dependency-ordered commits are composed into a
dedicated integration worktree. Review the displayed exact SHA, then:

```bash
harnessctl run approve-integration <run-id> --expected-head <40-char-sha>
```

Approval rechecks the worktree HEAD, runs controller validation/final audit,
records exact-SHA evidence, and starts a fresh read-only final-auditor thread.
The run remains in `FINAL_AUDIT` until that schema-valid independent verdict is
accepted. A rejected or lost audit blocks the run instead of producing a false
green. An accepted audit completes a `local_only` run. It never merges.

For a run created with `--publication draft_pr_after_approval`, publication is
a second explicit operation:

```bash
harnessctl run publish-draft-pr <run-id> \
  --expected-head <reviewed-sha> \
  --title "Bounded NeuralMatrix change"
```

The controller pushes only that exact reviewed head and invokes `gh pr create
--draft`. It does not mark the PR ready or merge it.

## Usage and evidence

```bash
harnessctl run usage <run-id>
harnessctl run evidence <run-id>
harnessctl run export <run-id>
```

Exports are written under the XDG data directory and returned with their
artifact digest. Bundles include exact source SHAs, task/agent state, command
and validation custody, evidence claims, proof limits, and content hashes. Raw
private reasoning is excluded by default.

Dollar figures are API-equivalent planning estimates, not subscription
invoices. Reasoning tokens are a breakdown of output and are never billed a
second time. Missing cache-write telemetry produces a bounded range.

## Shutdown and recovery

Pause runs or stop active runs before planned maintenance, then stop the user
service:

```bash
harnessctl run pause <run-id>
systemctl --user stop harnessd.service
```

SIGINT/SIGTERM stops HTTP intake, terminates the App Server process group, and
leaves SQLite/WAL, command spools, artifacts, and managed worktrees intact.
After an unplanned daemon loss, inspect active runs and preserve questionable
worktrees before retrying; a retry always creates a new attempt.

An App Server exit pauses unsafe progress, reconciles active sessions, and
preserves uncertain work. The daemon then makes at most three compatible
restart attempts with bounded backoff. Version or schema mismatch remains
fail-closed. A replacement process never causes stalled mutable work to be
silently marked successful; use the retained evidence to create an explicit
new attempt.

Never delete a managed worktree by hand while a run is active. Preservation is
the safe default for conflicts and failed attempts.

## Codex compatibility upgrade

The protocol tuple is deliberate. To evaluate a new Codex CLI:

```bash
cargo xtask codex-schema --codex /path/to/candidate/codex
cargo xtask app-server-bindings-check
```

The generation command copies the complete schema bundle and updates
`generated/CODEX_COMPATIBILITY.json`, but it does not silently rewrite the
intentional config pins. Review the schema/adapter diff, update both version
and digest, run the fake and live smoke suites, then release them together.

## Remote viewing

Use an SSH tunnel to the loopback listener:

```bash
ssh -L 7310:127.0.0.1:7310 user@linux-host
```

Do not expose the daemon or Codex App Server directly to a LAN/WAN. Multi-user
authentication, TLS termination, and remote authorization are outside v1.
