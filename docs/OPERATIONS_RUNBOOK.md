# BILDR operations runbook

BILDR is a single-user, Linux-local service. The daemon owns Codex
App Server, orchestration, SQLite, worktrees, command processes, evidence, and
the browser/API boundary. It never needs a public listener.

## Runtime prerequisites

Required:

- Codex CLI 0.149.1, authenticated through the normal Codex login flow;
- Bubblewrap exactly 0.11.0 with unprivileged user and network namespaces available when `self_improvement.mode = "observe_only"`; `harnessd doctor` probes this boundary and refuses observe-only startup readiness when it is unavailable;
- Git, `rg`, Bash, and the build tools required by the target repository;
- a clean Git coordination clone with `origin` and Git identity;
- `gh` only for the optional, explicit draft-PR operation.

Rust and Node are build-time requirements. Docker/Podman and hardware runners
are optional validator prerequisites; an unavailable prerequisite is recorded
as unavailable and never converted into a passing result.

## Runtime paths

Defaults follow the XDG base-directory convention:

```text
$XDG_CONFIG_HOME/harness-console/config.toml
$XDG_CONFIG_HOME/harness-console/profiles/bildr.toml
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

Registered custom profile IDs resolve from
`$XDG_CONFIG_HOME/harness-console/profiles/<id>.toml`. Keep repository-specific
policy there when it should not ship as part of BILDR. A missing custom profile
fails closed and reports the expected installed path.

## Build and install

```bash
npm --prefix ui ci
npm --prefix ui run build
cargo build --release -p harnessd -p harnessctl -p harness-probe -p harness-desktop

install -Dm755 target/release/harnessd "$HOME/.local/bin/harnessd"
install -Dm755 target/release/harnessctl "$HOME/.local/bin/harnessctl"
install -Dm755 target/release/harness-probe "$HOME/.local/bin/harness-probe"
install -Dm755 target/release/harness-desktop "$HOME/.local/bin/harness-desktop"
install -Dm644 packaging/desktop/bildr.desktop \
  "$HOME/.local/share/applications/bildr.desktop"
install -Dm644 packaging/systemd/harnessd.service \
  "$HOME/.config/systemd/user/harnessd.service"
mkdir -p "$HOME/.config/harness-console"
cp config/harness.example.toml "$HOME/.config/harness-console/config.toml"

systemctl --user daemon-reload
systemctl --user enable --now harnessd.service
```

Alternatively, `cargo xtask dist` creates a checksummed tarball with the
release binaries including `harness-desktop`, systemd unit, desktop launcher,
config/profile, Codex agent and skill files, schemas, compatibility metadata,
API contract, README, version, and license.

## Preflight and local startup

```bash
harnessd doctor
harnessd serve --no-browser
```

`doctor` validates the safe bind, XDG permissions, migrations/WAL/integrity,
profile, configured Codex version, generated protocol digest, and the exact
Bubblewrap namespace boundary whenever observe-only improvement is enabled. Use
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

## Explicit observer regression evaluation

The first development evaluation is an operator-invoked, controller-owned
regression for the historical trace-snapshot bound. It is available only when
self-improvement is effectively `observe_only` and the configured frozen
safety anchor matches:

```bash
harnessd evaluate-observer-snapshot --repository /path/to/registered/bildr
```

The command checks out the two pinned historical SHAs in managed worktrees,
uses a lockfile-filtered offline Cargo snapshot and isolated deterministic
grader, persists only receipt-bearing evaluation evidence, and prints the
evaluation/sample IDs. It does not accept candidate code, commands, fixtures,
grader inputs, or holdout data, and it cannot activate or promote a policy.

## Register a checkout

```bash
harnessctl repo add --path /path/to/project
harnessctl repo list
harnessctl repo inspect <repository-id>
```

The default `general` profile checks Git state, the active branch, identity,
remote policy, and filesystem access. The optional BILDR profile also requires
this repository's mandatory authority files. If a legitimate mirror uses a
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
tuple. The architecture turn must return a schema-valid DAG, then a fresh
read-only Sol reviewer adversarially checks the plan against the repository for
goal alignment, feasibility, critical-path liveness, behavior-first evidence,
appropriate test timing, and recovery authority. Blocking findings are returned
to a new architect revision automatically and reviewed again. Temporary
review/revision startup failures remain queued; the total run token ceiling is
the stopping authority.

When **Deep interview before planning** is selected, run creation stops in
`INTERVIEWING`. The selected governor model asks one material question per
completed read-only turn. Reply in the run workspace until the brief is ready,
then choose **Use brief and plan**. Choose **Skip interview** at any point to
plan from the original request. Confirmation and skip are explicit local-user
actions; automatic plan approval cannot perform them. A confirmed brief is
durable, is passed to a fresh architect and plan reviewer without the raw
transcript, and is bound by digest into the plan certificate.

If an interview turn fails, use **Retry interview**. The controller restores
the durable questions, responses, and current draft in a new read-only thread.
If architecture cannot start after confirmation or skip, the run remains
`READY_FOR_ARCHITECTURE`; use **Start architecture** after the runtime or
capacity issue clears.

The approve command accepts only the exact digest in `CERTIFIED` state. Manual
approval is the default. Automatic plan approval runs the identical
review/revision loop and performs only the final certified-to-approved
transition; it never approves a merely schema-valid proposal. Execution cannot
begin before certification and the configured approval posture.

Certification is not a blank thumbs-up. Run detail exposes the structured
certificate, advisory findings, revision history, and planning spend. Automatic
approval is deferred for high-risk or serial-path plans, same-family
architect/reviewer pairs, execution reserves above the configured threshold, or
insufficient remaining run budget. Use **Request changes** to feed a blocking
operator finding into the full-replacement revision loop. If review fingerprints
repeat or fail to shrink, the controller pauses with phase
`plan_review_deadlocked`; use the same action to give the next architect a
concrete correction. Budget-infeasible approval requires the explicit local-user
override control.

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

Daemon or App Server restart recovery follows the same contract automatically
when a root governor was actively pursuing an implementation task, when a valid
progressing checkpoint exists, or when the latest governor attempt was stalled
only by infrastructure loss. The interrupted attempt is preserved, its leases
are released, and the next bounded attempt is queued without an operator prompt.
A pending approval or genuinely blocked checkpoint remains stopped for a human
decision.
Delegated governor threads are also hard-stopped by the controller at their
250k token ceiling; the governor remains responsible for reconciling the bounded
result and selecting the next action.
If governor output fails Git custody, Harness preserves that attempt, reports the
exact policy findings to a clean bounded continuation, and keeps the rejected
diff uncommitted. Only a genuine authority/approval decision or exhausted
no-progress envelope is routed to the operator. That envelope counts the task's
governor and delegated descendants only; independent verifier work remains part
of the total run ceiling but cannot consume the remediation allowance.
An independent verifier rejection opens a fresh bounded repair window. Repeated
identical finding sets cross the configured remediation threshold into a
controller-authored strategy correction; they do not become a routine human
resume prompt. Every repair cycle remains charged to the selected total run
ceiling.

For a governor-owned task, a reason may be a short human priority rather than an
internal execution recipe. The controller compiles the next action from the
latest `harness.governor-checkpoint.v1` milestone ledger and up to five recent
valid handoffs. Candidate trees named by a structured checkpoint are
materialized only into a clean leased worktree and still pass the normal exact
base, owned-path, forbidden-path, and diff-budget gates before controller commit.

After every task is verified, dependency-ordered commits are composed into a
dedicated integration worktree. Review the displayed exact SHA, then:

```bash
harnessctl run approve-integration <run-id> --expected-head <40-char-sha>
```

Approval rechecks the worktree HEAD and runs every path-selected integration
validator plus automated platform acceptance against that exact, clean SHA.
The controller records command artifacts and the full before/after worktree
fingerprint; a validator that changes source is a failure. Missing behavioral
coverage for a configured code path is also a failure. Only then does Harness
assemble the signoff packet and start a fresh read-only final-auditor thread.

An accepted audit stops in `HUMAN_REVIEW`; it never auto-completes, including in
`local_only` mode. The Console shows the exact-head checks, proof classes,
unproved claims, platform acceptance, audit evidence, spend, packet digest, and
integration SHA. Complete any path-selected device attestations with the real
target identity and observed behavior, then approve the packet or reject it
with a blocking file finding. Approval is digest/SHA-bound. Rejection reopens
only mapped task owners, preserves the rejected integration worktree, and
creates a fresh integration candidate. Fileless or unmapped findings stop for a
decision rather than triggering a guessed broad repair.

For a run created with `--publication draft_pr_after_approval`, publication is
a second explicit operation:

```bash
harnessctl run publish-draft-pr <run-id> \
  --expected-head <reviewed-sha> \
  --title "Bounded repository change"
```

The controller pushes only that exact reviewed head and invokes `gh pr create
--draft`. When the repository profile sets `require_draft_pr_ci = true`, it then
polls the PR head plus `gh pr checks --required` read-only. Only an unchanged PR
head equal to the integration SHA and all-pass required checks promote tasks
through `CI_PROVEN` and complete the run. Pending, failed, skipped, unavailable,
empty required-check sets, or a force-pushed PR head never become proof. When
the profile does not declare this gate, draft creation completes the run without
claiming `CI_PROVEN`. Harness does not mark the PR ready or merge it.

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
After an unplanned daemon loss, governor work is preserved and resumed as a new
bounded attempt automatically when the run is executing, scheduling is enabled,
and no approval is pending. Inspect questionable worktrees before manually
retrying non-governor work.

An App Server exit pauses unsafe progress, reconciles active sessions, and
preserves uncertain work. The daemon then makes at most three compatible
restart attempts with bounded backoff. Version or schema mismatch remains
fail-closed. A replacement process never marks stalled mutable work successful;
for a governor it creates a new bounded attempt from retained continuity state.

Systemd process supervision uses `Restart=on-failure`. Local HTTP health probes
are observational and must not restart the daemon after a single timeout: a
busy but productive App Server can temporarily miss a probe deadline, and
killing it would manufacture an infrastructure stall.

Never delete a managed worktree by hand while a run is active. Preservation is
the safe default for conflicts and failed attempts.

## Run hygiene

BILDR keeps the registered coordination checkout read-only and requires it to
be clean at run preflight. Mutable work uses separate managed worktrees.

On a successful completion, BILDR removes clean managed worktrees without
`--force`, prunes stale Git worktree registrations, and marks the database rows
`REMOVED`. One background hygiene lane serializes deletions without blocking
normal orchestration. BILDR retains a worktree when an agent or lease is active,
the operator has pinned it, or Git finds tracked or untracked source changes. The
`run.hygiene.completed` event reports `clean` or `attention_required` and lists
anything retained. Failed and canceled runs remain preserved for diagnosis.
The policy is bound when a run is created. An upgrade does not automatically
delete worktrees that belong to older runs.

Retries keep the current and immediately previous task worktrees for direct
continuity. Older retry worktrees are compacted when they are clean and safe;
their Git commits and durable evidence remain available.

Controller-run commands use disposable command-local temporary, home, cache,
config, data, and state directories unless a command explicitly needs an
allowlisted host location. BILDR copies required logs into the
content-addressed artifact store before deleting the spool. It does not
automatically prune shared Cargo, npm, Gradle, Docker, compiler, or
operating-system caches.

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
