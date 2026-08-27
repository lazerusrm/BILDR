# BILDR self-improvement reference map

**Status:** supporting research map; external sources are not BILDR authority
**Reviewed:** 2026-08-11

This document records the external ideas used to shape the architecture. BILDR
adopts concepts only when they fit its local-first custody and evidence model.

## Prime Intellect Lab

Reference:

- <https://www.primeintellect.ai/blog/lab-is-open>
- <https://docs.primeintellect.ai/hosted-training/what-is-lab>

Key idea:

An environment packages tasks, a harness, runtime/tooling, and reward metrics;
the same environment supports evaluation, trace inspection, training, adapter
deployment, and repeated improvement.

BILDR adaptation:

- taskset -> versioned BILDR eval suites;
- harness -> immutable BILDR policy bundle;
- runtime -> exact repository/worktree/model/tool receipt;
- reward -> independent multi-signal grader bundle;
- adapter deployment -> another candidate subject to local promotion.

Do not copy blindly:

BILDR should not require external training to improve. Prompt, context, routing,
skills, validators, and scheduler policy can be optimized first. External
platforms remain optional execution providers, not local outcome authority.

## Prime Verifiers v1

Reference:

- <https://www.primeintellect.ai/blog/verifiers-v1>

Key ideas:

- decompose taskset, harness, and runtime;
- make tasksets harness-independent;
- treat compaction and subagents as first-class trace branches;
- retain training-ready traces;
- isolate runtime concerns from task scoring.

BILDR adaptation:

- add a branch-aware trace DAG derived from raw events;
- use immutable case and runtime manifests;
- make policy bundles swappable;
- export a provider-neutral format before provider-specific adapters.

## Zapier AutomationBench case study

Reference:

- <https://www.primeintellect.ai/case-study/zapier>

Key lesson:

A reward can stay healthy while required behavior disappears. The reported
example observed API-fetch calls fall toward zero while reward remained flat,
revealing that the grader no longer measured real workflow completion.

BILDR adaptation:

- every reward contract includes expected side-effect and negative-control
  signals;
- divergence between proxy score and required effects is a hard
  reward-integrity failure;
- rollout traces and component metrics remain inspectable.

## Prime reward-hacking research

Reference:

- <https://www.primeintellect.ai/blog/reward-hacking>

Key lesson:

Reward hacking is not solved by assuming an exploit is rare. Proxy and intended
behavior are different, and optimization pressure can amplify small seams.

BILDR adaptation:

- isolate graders and holdouts;
- use visible, improvable legitimate signals;
- keep signal vectors, not only a scalar;
- red-team grader bundles before promotion;
- invalidate leaked or tampered experiments.

## OpenAI harness engineering

Reference:

- <https://openai.com/index/harness-engineering/>

Key ideas:

- make application behavior, logs, metrics, and UI legible to coding agents;
- keep repository knowledge as the system of record;
- use agents to review agents;
- encode “golden principles” and run recurring cleanup tasks to control drift.

BILDR adaptation:

- the Learning view (rail: Advanced) and trace/evidence inspection;
- evidence-backed knowledge supplements but never overrides repository authority;
- recurring quality gardener proposes narrow draft changes;
- independent evaluator and promotion authority.

## OpenAI Evals API

Reference:

- <https://platform.openai.com/docs/api-reference/evals>

Key idea:

Evaluation definitions, data sources, runs, result counts, and testing criteria
can be managed as durable external evaluation objects.

BILDR adaptation:

Export taskset and grader records through an adapter. Preserve BILDR case,
runtime, grader, and candidate digests so external results remain attributable.

## Agentic Harness Engineering

Reference:

- <https://doi.org/10.48550/arXiv.2604.25850>

Key ideas:

- **component observability:** editable harness pieces are explicit and
  revertible;
- **experience observability:** large trajectories become layered evidence;
- **decision observability:** each edit predicts its effect before later
  outcomes test that prediction.

BILDR adaptation:

- policy component registry and component-level diff;
- trace graph, failure clusters, and drill-down evidence;
- candidate prediction and optimizer-calibration tables.

## Self-Harness

Reference:

- <https://arxiv.org/abs/2606.09498>

Key ideas:

- weakness mining from failed traces;
- minimal targeted harness proposals;
- regression validation on held-in and held-out tasks.

BILDR adaptation:

Use the same three-stage shape, but separate the miner, optimizer, evaluator, and
promotion authority. A proposed edit is not accepted merely because the same
model prefers it.

## Hierarchical Self-Improvement

Reference:

- <https://arxiv.org/abs/2608.08466>

Key ideas:

- task-family-specific harnesses;
- evolver and optional meta-evolver;
- frozen outer anchor;
- improvement is bounded by feedback fidelity and backbone capability.

BILDR adaptation:

- bind policy champions by repository/task/model family;
- freeze custody and promotion controls;
- enable meta-evolution only after the base evaluation loop is mature;
- report “no improvement possible with current feedback/model” honestly.

## A Self-Improving Coding Agent

Reference:

- <https://arxiv.org/abs/2504.15228>

Key idea:

A coding system can edit its own implementation and improve benchmark
performance.

BILDR adaptation:

Treat code self-editing as the highest-risk, last-stage mode. It produces a
normal draft repository change, full validation, and human review. It never
replaces the running controller directly.

## Continual Harness

Reference:

- <https://continual-harness.github.io/>

Key ideas:

- reuse tested skills;
- refine prompt, memory, skills, and subagents from trajectories;
- improve without restarting the entire interaction;
- stronger evidence auditing is still needed.

BILDR adaptation:

- reviewed knowledge and skills have source evidence, expiry, contradiction, and
  measured impact;
- in-run refinement remains separate from cross-run promotion;
- no raw trajectory summary becomes authority automatically.

## Retrospective Harness Optimization

Reference:

- <https://www.microsoft.com/en-us/research/publication/retrospective-harness-optimization-improving-llm-agents-via-self-preference-over-trajectory-rollouts/>

Key idea:

Diverse difficult cases can be mined from past trajectories and re-solved to
propose improvements even when ground truth is scarce.

BILDR adaptation:

Use retrospective self-preference only as a proposal or weak signal. It cannot
replace deterministic checks, human correction evidence, hidden holdouts, or
promotion gates.

## Design synthesis

The resulting BILDR loop is:

```text
Prime-style task/harness/runtime decomposition
+ branch-aware traces
+ failure mining and minimal edits
+ component/experience/decision observability
+ repository golden principles and quality gardening
+ frozen safety anchor
+ independent eval/holdout/promotion
+ exact-SHA local custody and rollback
```

The central constraint is feedback fidelity. The system should prefer “no
credible improvement demonstrated” over promoting a candidate against a weak or
self-confirming metric.
