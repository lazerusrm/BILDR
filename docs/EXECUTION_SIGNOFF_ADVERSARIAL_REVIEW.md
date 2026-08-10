# Adversarial Review: Execution → Verification → Final Signoff

Scope: the implemented orchestrator endgame — worker completion → `REVIEW_READY` → verifier → `VERIFIED` → integration → `INTEGRATION_VERIFICATION` → `FINAL_AUDIT` → `HUMAN_REVIEW` → `PUBLICATION_READY` → publish/complete — as implemented in `harness-orchestrator` (`launch_verifier`, `apply_verifier_verdict`, integration validation, `launch_final_auditor`, `apply_final_audit_verdict`, `publish_draft_pr`, `run_validator`) against the posture the architecture doc promises (§5.4 invariants, §6.1 "a model sentence such as 'all tests pass' is not a state transition").

## What already holds up

- Diff custody at `REVIEW_READY` is real: forbidden-path scan, agent-created-commit rejection, controller-owned commit.
- Exact-SHA discipline is consistent: verifier binds to commit, final audit re-checks the integration head before and after, publication verifies head and branch, `push_exact` publishes only the reviewed SHA.
- Worker output structurally cannot reach `VERIFIED`; leases release only on independent accept; failed attempts preserve worktrees.
- The verifier remediation loop has repeated-finding-set detection with controller-authored strategy correction.
- The evidence ledger machinery (commands, validations, proof tiers, artifacts, export) is well-built where it is used.

## The core finding: nothing in the pipeline ever has to run the code

The doc's central promise — behavioral proof from running code, evidence ledger over model claims — is not enforced at any gate. Walk the chain as an adversary shipping a broken change:

1. **`REVIEW_READY` requires no test evidence.** The gate is diff custody only. A worker that wrote code and never ran anything passes identically to one that ran the full suite.
2. **The verifier cannot obtain proof.** `launch_verifier` gives it the packet and "inspect the complete diff" — it does *not* receive the evidence ledger or validation results, and its read-only sandbox cannot execute a real test suite (builds need writes). The prompt warns "a worker response is not proof," but the verifier is handed nothing that *is* proof. Its accept is therefore exactly the thing the doc forbids: a model sentence, promoted to a state transition.
3. **`INTEGRATION_VERIFICATION` is `git diff --check`.** The only controller-owned validation on the integrated head is a whitespace/patch-formatting check (recorded honestly as T1). The integrated head is never compiled, never tested, by anyone, at any point.
4. **The final auditor audits an empty ledger.** It receives the evidence snapshot and is told to reject claims that outrun their proof tier — but since no gate forced any validator to run, the snapshot contains custody checks and verdicts, not behavior. A strict auditor should block every run; a sycophantic one (the LLM default) reads the diff, finds it plausible, and accepts. Either way the audit cannot do its stated job.
5. **`HUMAN_REVIEW` is a zero-duration passthrough.** `apply_final_audit_verdict` transitions `HUMAN_REVIEW → PUBLICATION_READY` in the same function call. In `draft_pr_after_approval` mode the publish action is the de-facto human gate; in **`local_only` mode the run auto-transitions to `COMPLETED` and closes every task with no human action at all**.

Supporting evidence that this is a gap and not a choice:

- `ValidatorRule` (command, proof tier, resource class, `path_globs`, `manual_prerequisites`) exists in the profile, and `run_validator` correctly records commands, validations, evidence, and artifacts — but its **only caller is the manual API endpoint**. No lifecycle transition invokes it. `path_globs` is never used for selection; `selector_reason` is hardcoded boilerplate.
- Task packets carry `required_positive_tests` / `required_negative_tests` / `required_evidence`, but they are prose delivered to agents. Nothing binds them to validator IDs and nothing deterministically checks they were satisfied before `VERIFIED`.
- `CI_PROVEN` and `LIVE_PROVEN` exist in the task state machine with legal transitions and are never set by any code path — dead vocabulary marking the missing capability.
- Proof tiers T0–T6 exist, but no policy anywhere maps a lifecycle gate to a minimum tier.

## Ranked findings

### 1. No validation gates — the evidence posture is declared, not enforced
As above. The fix is not more prompting; it is deterministic: gates that refuse to transition without recorded validation evidence at the right SHA and tier.

### 2. Task-level evidence (even when it exists) dies at integration, and nothing replaces it
§5.4 says evidence binds to exact source SHA and is invalidated by relevant changes. Integration is exactly such a change — merged attempts can conflict semantically while merging cleanly. Even in the best case where workers ran tests at their attempt heads, the integrated head has zero direct behavioral proof, and no re-validation occurs. The integrated artifact — the only thing that ships — is the least-proven object in the system.

### 3. The verifier is structurally blind to the evidence ledger
Give the verifier the attempt's validation results and command history in its prompt, and make the controller — not the verifier — enforce "required tests ran and passed" (see gate design below). The verifier's comparative advantage is semantic review of the diff against authorities; asking it to also be the test-execution gate assigns it a job it physically cannot do in a read-only sandbox.

### 4. `HUMAN_REVIEW` is not a state, and the human cannot reject
The state exists in the machine but no run ever rests there. In PR mode the human's only verbs at `PUBLICATION_READY` are publish, stop, or archive — there is no reject-with-findings that routes back into remediation (the same gap as the planning review's finding #1, one stage later). In `local_only` mode there is no human involvement at all between objective submission and `COMPLETED`. If local-only-no-signoff is intended, it should be an explicit profile policy, not an accident of the transition code.

### 5. Final-audit rejection dead-ends in `BLOCKED`
Verifier rejections feed a structured remediation loop with strategy correction. Final-audit rejections transition the run to `BLOCKED` and stop. There is no path that converts audit findings into remediation tasks or a governor repair window — the richest findings in the pipeline (whole-system, cross-task) are the only ones with no automated consumer.

### 6. Accept-verdicts again require zero findings and no evidence of work
Same bistability as plan review: the verifier and final auditor must suppress advisory observations to accept, and a one-line summary satisfies validation. The structured-certificate fix (required fields: files inspected, checks performed, findings considered-and-dismissed) and blocking/advisory severity split apply here identically.

### 7. No representation of platform/environment acceptance
Nothing models "this repository ships a browser UI, mobile app, desktop app, or
device firmware, and done means proven there." `ResourceClass::Hardware` and
`manual_prerequisites` are the right hooks, but no structure exists for a
per-platform acceptance matrix, and no gate consumes one. For products with
multiple delivery targets, this is the difference between "the unit tests pass"
and "the product works."

## Proposed: deterministic release-gate + signoff plan

The theme of every fix: the controller, not a model and not a human's memory, decides whether "done" has been earned. Four pieces, buildable incrementally:

### A. Validation policy in the repository profile

Extend the profile so validators are *bound to gates*, not just defined:

```toml
[[validators]]
id = "cargo-test-workspace"
command = ["cargo", "test", "--workspace"]
proof_tier = "T2"
resource_class = "medium"
path_globs = ["crates/**", "bins/**"]

[validation_policy]
review_ready  = { min_tier = "T2", validators = "path-matched" }   # attempt head
integration   = { min_tier = "T2", validators = "all-matched" }    # integrated head
publication   = { min_tier = "T3", validators = "acceptance" }     # integrated head
```

Gate semantics, enforced in the orchestrator transitions:

- **`REVIEW_READY` → verifier launch**: controller runs path-matched validators against the attempt head in the attempt worktree (it already owns the commit step — run validators immediately after). Failures send the attempt back to the worker with the validator output, exactly like a verifier rejection. The verifier then receives the validation results in its prompt and reviews semantics, not test execution.
- **`INTEGRATION_VERIFICATION`**: replace "diff --check only" with diff-check **plus** all matched validators against the integrated head. This directly fixes finding #2 — the integrated head becomes the *most*-proven object instead of the least.
- **`PUBLICATION_READY`**: requires the acceptance suite (below) recorded at the exact integration SHA, no stale evidence.

Deterministic rule throughout: a gate consumes only `ValidationRecord`s whose `source_sha` equals the gate's SHA and whose `result_class` is `Success`. Everything else is invisible to the gate — staleness handled by construction.

### B. Platform acceptance matrix

Add a `[[acceptance]]` section to the profile declaring what "the product works" means per target:

```toml
[[acceptance]]
id = "web-console-smoke"
kind = "automated"            # controller runs it
command = ["npm", "run", "e2e:smoke"]
proof_tier = "T3"
resource_class = "medium"

[[acceptance]]
id = "device-ota-smoke"
kind = "attested"             # human performs it, controller records it
instructions = "Flash integration build to bench device; verify boot + telemetry."
proof_tier = "T4"
resource_class = "bench-device"
```

- `automated` entries run like validators at the publication gate (browser e2e via headless runner, desktop app smoke, simulator/emulator suites where CI-able).
- `attested` entries generate a signoff item the human completes in the UI; the controller records a signed `EvidenceClaim` (actor, timestamp, integration SHA, free-text observations, optional artifact upload — screenshots, logs). This is the honest shape for on-device/iOS/Android testing that can't run headlessly: the system doesn't pretend to automate it, but it also refuses to call the run publishable while the attestation is missing. `manual_prerequisites` already anticipated this.
- Repos without an acceptance section publish on validator evidence alone — the matrix is opt-in per profile, so a pure-library repo isn't blocked on ceremony.

### C. A real signoff packet, assembled deterministically

At final audit, the controller compiles a **signoff packet** (stored, exportable, rendered in the UI):

- objective + approved plan digest + revision history;
- per-task: verdicts, validation results with tiers, diff stats;
- integrated-head validation results (the gate-A evidence);
- acceptance matrix status: each item green/red/pending-attestation;
- unproved-claims rollup from the evidence ledger;
- budget actuals vs. plan.

The final auditor receives this packet instead of a raw evidence dump — its job narrows to "does the diff match the claims and authorities," which is what a read-only LLM can actually do. The human sees the same packet at `HUMAN_REVIEW`, which becomes a real resting state in PR mode: run pauses there until the human approves (→ `PUBLICATION_READY`) or **rejects with findings** (→ remediation, closing finding #4). Route final-audit and human rejections through the existing verifier-remediation machinery — new bounded governor repair window seeded with the findings — closing finding #5.

### D. Populate the dead states

Once A–C exist, `CI_PROVEN` becomes reachable: an optional profile hook that watches the draft PR's CI checks (`gh pr checks`) and promotes tasks when the external pipeline confirms the integrated head. `LIVE_PROVEN` stays out of v1 (matches the no-auto-merge posture) but the vocabulary stops lying about capabilities.

**Build order:** A at the integration gate first (one function, biggest win — the shipped artifact gets proven), then A at review-ready, then C (signoff packet + resting `HUMAN_REVIEW` + reject path), then B, then D. Each step is independently valuable.

## Adversarial amendments adopted during implementation

The proposed direction was correct, but five details would have recreated the
same semantic deadlocks this design is meant to remove:

1. **A numeric `min_tier` is not the gate.** Proof tiers describe what a claim
   may establish; they are not mutually substitutable quality levels. A T5
   device observation cannot stand in for a path-selected component test, and a
   T2 test cannot stand in for custody. The implemented gate selects explicit
   validator IDs by lifecycle gate and changed path, requires every selection to
   succeed at the exact SHA, and separately requires behavioral evidence when a
   configured code path changed.
2. **Validator source mutation is a failed validation.** Checking `HEAD` alone
   misses validators that rewrite tracked files or create non-ignored source.
   Harness now compares the full worktree fingerprint before and after every
   controller-run validator or acceptance command. A green command that changes
   the checkout is recorded as `source_failure` and cannot enter the signoff
   packet as proof.
3. **Review-ready validation is opt-in; integration validation is mandatory.**
   Broad stable suites belong after the code shape has survived implementation
   and semantic review. Profiles may attach cheap, focused checks to
   `review_ready`, but the default expensive gate runs once on the integrated
   candidate. This avoids manufacturing a large brittle test surface around
   provisional internals while still making the artifact that ships the
   best-proven object.
4. **Whole-run remediation cannot merely call task retry.** At final audit all
   tasks are already `INTEGRATED`, the integration worktree exists, and old
   exact-head evidence must remain historical. The implemented repair path maps
   blocking file findings to task ownership, reopens only those tasks, resets
   unaffected tasks to their verified commits, preserves the rejected
   integration worktree, clears current-head signoff bindings, and creates a new
   integration worktree/branch. An unmapped or fileless blocker stops for an
   operator decision instead of guessing a repair target.
5. **CI proof necessarily follows draft publication and must be explicit.** `gh pr checks` cannot be
   a pre-publication gate because no PR exists yet. The correct order is exact
   integrated-head validators and acceptance → final audit → human signoff →
   explicit draft publication → profile-required CI observation. The observer
   verifies the PR still points at the integration SHA before accepting its
   checks. Passing required checks promote tasks through `CI_PROVEN`; profiles
   without a declared CI gate complete after draft creation without fabricating
   CI proof or deadlocking on an empty check set. Merge remains absent.

The resulting controller path is:

```text
controller commit
  -> optional focused review-ready validators
  -> semantic verifier (structured evidence; advisory/blocking split)
  -> fresh integration SHA
  -> mandatory path-selected validators + automated acceptance
  -> deterministic signoff packet
  -> final semantic audit
  -> resting HUMAN_REVIEW
       -> approve exact packet/SHA
       -> attest pending platform items
       -> reject mapped files and rebuild a new SHA
  -> explicit draft PR
  -> profile-required CI observed at that SHA -> CI_PROVEN
```

The general profile intentionally cannot call a code-changing run proven from
`git diff --check`; a repository must supply a behavioral validator. The
strict profile uses path-selected authoritative validators rather than inventing
an indiscriminate test expansion.

## LLM failure-mode coverage map

| Common failure mode | Covered today? | Gap / fix |
| --- | --- | --- |
| "All tests pass" without running tests | ❌ nothing requires any test to run | gates A |
| Verifier affirms from code-reading alone | ❌ structurally blind, read-only | evidence in prompt + controller-owned gates (A, #3) |
| Semantic merge conflicts after clean merges | ❌ diff --check only | integrated-head re-validation (A, #2) |
| Looks-done-isn't-done (product never exercised) | ❌ | acceptance matrix (B) |
| Evidence staleness across SHAs | ⚠️ invariant stated, not enforced at gates | SHA-exact gate consumption (A) |
| Zero-findings bistability in accepts | ❌ | advisory/blocking split (#6) |
| Human rubber-stamp / human bypassed | ⚠️ PR mode implicit; local mode absent | resting HUMAN_REVIEW + signoff packet (C) |
| Findings with no consumer | ⚠️ verifier loop yes; final audit no | remediation routing (#5, C) |
| Fabricated manual-test claims | ❌ unrepresentable today | signed attestations bound to SHA (B) |
