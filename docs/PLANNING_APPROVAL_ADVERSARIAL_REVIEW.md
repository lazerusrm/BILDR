# Adversarial Review: Planning → Approval Pipeline

Scope: `ARCHITECTING → PLAN_ADVERSARIAL_REVIEW → (PLAN_REVISION_REQUIRED ↔) → PLAN_REVIEW_REQUIRED → READY_TO_EXECUTE`, as specified in `ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md` §5.2/§6.2 and implemented in `harness-domain` (run state machine) and `harness-orchestrator` (`launch_plan_reviewer`, `apply_plan_review_verdict`, `start_plan_revision`, `approve_plan`).

## What already holds up

- Explicit, enforced state edges; no state inferred from UI counts.
- Fresh reviewer per revision; digest re-check before review; digest + `If-Match` precondition on approval; approval races handled.
- Full-replacement revisions (no prose patches → no plan drift).
- Automatic approval can only take the *certified* transition; it cannot skip adversarial review. Recovered plans are forced back through certification.
- The loop is budget-bounded, and pending approvals are fail-closed.

## Findings (ranked)

### 1. The human cannot reject a certified plan — only approve or abandon
`PLAN_REVIEW_REQUIRED` has exactly two outgoing edges: `READY_TO_EXECUTE` and `COMPLETED` (plan-only). There is no edge back to `ARCHITECTING`/`PLAN_ADVERSARIAL_REVIEW`, and the API surface has only `POST /runs/{id}/plan/approve`. The doc promises "the user sees and may edit objective text, non-goals, budgets, and dispatch choices" with each edit producing a new digest — but there is no state edge, no endpoint, and no re-certification path for a human-modified or human-rejected plan. Today the human's disagreement channel is "cancel the run and start over," which discards the entire planning spend.

**Fix:** add `PLAN_REVIEW_REQUIRED → PLAN_REVISION_REQUIRED` driven by a `POST /plan/request_changes` carrying human findings in the same finding schema the reviewer uses, so human feedback enters the existing revision machinery instead of being out-of-band. Human edits to objective/budgets should invalidate the certificate and re-enter review.

### 2. Advisory observations are unrepresentable — `accept` requires zero findings
`validate_plan_review_verdict` rejects `accept` with any findings, and the `severity` field on findings is never consulted. The reviewer's real choice for a minor concern is: inflate it to blocking (costing a full replacement architect run + fresh review, ~240k tokens per nit) or silently drop it (information loss — the governor never hears it). This bistability pushes an LLM reviewer toward either nitpick-churn or rubber-stamping, the two failure modes you most want to avoid.

**Fix:** split findings into `blocking` and `advisory`. Only blocking findings force revision; advisory findings attach to the certificate and are injected into the governor's context at execution. This is a small schema/validator change with outsized effect on loop economics.

### 3. No convergence detection in the review loop — the only stop is token exhaustion
The doc explicitly (and reasonably) rejects an arbitrary review-count cap, but the result is that a non-converging loop's terminal signal is "run token budget exhausted" — the whole budget burned on planning with nothing to show and no diagnosis. Two concrete shapes an LLM pair will produce: (a) oscillation, where revision N fixes finding X in a way revision N+1's reviewer flags, reintroducing X's fix as a new finding; (b) moving-target review, where each round surfaces fresh nitpicks because zero-findings is the bar (see #2). The governor loop already has the right pattern — repeated-finding-set detection triggering a controller-authored strategy correction (§7.5) — but the plan loop has no analog.

**Fix:** fingerprint findings across revisions (normalized description + file). On a repeated fingerprint, an A→B→A oscillation, or K revisions without a shrinking finding count, pause and escalate to the human with the findings-diff across revisions. That is an off-ramp, not a count cap: a converging loop never hits it.

### 4. Architect and reviewer share one brain — independence is procedural, not epistemic
Both roles are Sol at xhigh (the reviewer uses the verifier route, same model family). A fresh thread removes conversational contamination but not correlated priors: if Sol holds a wrong belief about how this repository behaves, both the plan and its "independent" refutation attempt share it. This is the known weakness of self-review: same-model reviewers agree with themselves at well above chance. The doc's word "independently certified" overstates what the mechanism provides.

**Fix (pick by cost tolerance):** route plan review of runs containing high-risk/serial tasks to a different model family; or add a second cheap diverse-lens pass (e.g., Terra checking only contracts/persistence claims); or at minimum record in the certificate that reviewer and architect shared a model, and make that fact gate #5's auto-approval policy.

### 5. An `accept` requires no evidence the review actually happened
The only deterministic requirement on an accept is a non-empty summary. A fluent one-paragraph "this plan is feasible and well-sequenced" passes — and confident affirmation without performed verification is *the* canonical LLM reviewer failure. The contract prose asks for liveness/feasibility/test-timing analysis, but nothing checks it occurred.

**Fix:** make the certificate structured and deterministically validated: required fields for files actually inspected (checkable against the read-only session's recorded commands), the critical-path trace, budget arithmetic (task-budget sum vs. remaining ceiling), and the top ~3 failure modes considered with why each is mitigated. Reject accepts with empty sections. This converts "did you really look?" from vibes into a schema.

### 6. The certificate binds the plan digest, but not the world it was certified against
`approve_plan` verifies only the plan digest. The repository profile (model routes, efforts, budgets) and authority documents can change between certification and approval without invalidating the certificate — the reviewer certified dispatch to Terra-xhigh at given budgets, and execution may run something else. The scheduler's `base_and_authority_still_valid` re-check covers base lineage per task, but not profile/dispatch drift.

**Fix:** the certificate records the tuple (plan digest, base SHA, profile digest, authority digests); `approve_plan` and the automatic-approval path verify the full tuple and drop to re-review on mismatch.

### 7. Certify-then-starve: no remaining-budget feasibility check at approval
The revision loop itself consumes the run ceiling (`enforce_run_budget` only checks exhaustion). A plan certified after the loop has eaten 70% of the budget sails into `EXECUTING` guaranteed to stall mid-graph — the worst place to run out, since partial execution leaves worktrees and leases to unwind.

**Fix:** at approval time, compare the plan's summed task budgets plus verifier/integrator overhead against the remaining ceiling; block (or require explicit human override) when the plan cannot plausibly complete. This also gives #3's escalation a natural trigger.

### 8. Repository content is untrusted input to the only gate before autonomous spend
Architect and reviewer both read the repo; with `automatic_plan_approval` on, a reviewer steered by adversarial repo content (README/comments/fixtures addressed to "the reviewer") auto-launches execution with no human viewing the plan. Both agents being the same model (#4) makes injection *correlated* — one payload steers both.

**Fix:** gate automatic approval by plan risk, not just certification: auto-approve only when the plan contains no high-risk/serial tasks and total budget is under a configured threshold; otherwise fall back to human approval. Cheap, and it aligns the autonomy level with blast radius. Framing repo text as data in the planning prompts helps but is not a boundary.

## Smaller notes

- The reviewer doesn't receive the deterministic-validation results or risk-router output, so it re-derives (or contradicts) them; passing them as context is nearly free and reduces inconsistent verdicts.
- The human approval surface should render the certificate, findings history, revision count, and planning spend so far — otherwise the human approval is the same rubber stamp #5 worries about, one level up.
- The post-certification auto-approve path re-reads state and digest after dropping the lock — that race is handled correctly; no action needed.

## LLM failure-mode coverage map

| Common failure mode | Covered today? | Gap |
| --- | --- | --- |
| Claiming completion without doing work | ✅ evidence ledger, no self-approval | accept-verdicts exempt (#5) |
| Plan drift across revisions | ✅ full-replacement revisions, digests | — |
| Reviewer/author collusion via context | ✅ fresh reviewer threads | same-model priors (#4) |
| Sycophantic/confident affirmation | ❌ | structured certificate (#5) |
| Endless refinement loops | ⚠️ budget ceiling only | convergence off-ramp (#3) |
| Nitpick-vs-rubber-stamp bistability | ❌ | advisory findings channel (#2) |
| Stale-context decisions (TOCTOU) | ⚠️ base lineage only | profile/authority binding (#6) |
| Prompt injection from workspace | ❌ in planning phase | risk-gated auto-approval (#8) |
| Human kept meaningfully in the loop | ⚠️ approve-only | reject/edit path (#1) |
