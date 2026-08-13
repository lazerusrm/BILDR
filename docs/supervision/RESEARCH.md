# Research Basis for BILDR Supervisory Orchestration

**Prepared:** 2026-08-13

This document records the external findings that materially shaped ADR-0011 and
distinguishes them from BILDR-specific hypotheses that require replay evidence.

## Model selection

OpenAI positions GPT-5.6 Terra as the tier balancing intelligence and cost and
Sol as the frontier tier for the most complex professional work. Current model
guidance exposes `none`, `low`, `medium`, `high`, `xhigh`, and `max` effort and
recommends measuring representative workloads rather than assuming the largest
setting is always optimal.

Sources:

- https://openai.com/index/gpt-5-6/
- https://developers.openai.com/api/docs/guides/latest-model
- https://developers.openai.com/api/docs/models/gpt-5.6-terra
- https://developers.openai.com/api/docs/models/gpt-5.6-sol

BILDR translation:

- Terra `high` is the routine supervisor because the decisions are structured
  but high leverage;
- one Terra `xhigh` retry handles policy-detected ambiguity or impact;
- Sol `xhigh` receives only a crisp evidence-bounded expert question;
- `max` is not automatic and remains explicit/exceptional;
- real BILDR traces decide whether the initial route survives activation.

This is an initial product default, not proof that Terra high wins every BILDR
case.

## Cascades and selective escalation

RouteLLM studies dynamic routing between stronger and weaker models to move the
cost/quality frontier. FrugalGPT studies model cascades and budget-aware
selection.

- RouteLLM: https://arxiv.org/abs/2406.18665
- FrugalGPT: https://arxiv.org/abs/2305.05176

BILDR uses a cheaper capable routine tier, escalates only when controller facts
predict additional value, records route/decision/outcome/cost, and keeps routing
policy outside unchecked model discretion. It does not initially train a
router; deterministic risk, disagreement, repeated-failure, and uncertainty
gates are easier to audit. The selected shallow cascade is:

```text
Terra high -> one Terra xhigh retry -> Sol xhigh consultation
```

## Measure progress, not only final success

AgentBoard argues that final success alone reveals too little about multi-turn
agent behavior and adds fine-grained progress views.

- https://arxiv.org/abs/2401.13178

BILDR stores material-progress events and milestone, criterion, candidate,
validation, and finding deltas; separates progress from activity; evaluates a
decision by its later outcome window; and avoids one opaque completion
percentage. The exact vector fields and thresholds remain product choices.

## Ground judgment in the environment

AJ-Bench reports that judges that acquire environment/tool evidence outperform
text-only judge baselines on its benchmark, while substantial verification
limits remain.

- https://arxiv.org/abs/2604.18240

BILDR therefore supplies controller-observed state, tool results, exact SHAs,
validations, artifacts, and evidence references. Agent prose is input, not state
truth. Completion still needs independent environment-grounded verification,
and expert recommendations remain advisory. The supervisor receives a bounded
snapshot with targeted read-only lookup rather than unrestricted environment
control.

## Monitor trajectories and intervene selectively

E-valuator frames online trajectory monitoring as sequential hypothesis testing
and evaluates controlled early termination/token savings.

- https://arxiv.org/abs/2512.03109

BILDR evaluates trajectories before final failure, distinguishes `watch` from a
cancel authorization, requires multiple deterministic signs before `stalled`,
measures false intervention and missed stalls, and uses material events/liveness
timers rather than frequent model polling. The first release does not claim the
paper's statistical guarantees; it uses transparent versioned thresholds and
replay evaluation.

## Interleave evidence and small actions

ReAct demonstrates the value of interleaving reasoning with grounded actions.

- https://arxiv.org/abs/2210.03629

BILDR asks each supervisor turn for the smallest legal next action, returns the
action outcome as fresh evidence, avoids predicting an entire dynamic run, and
stores concise summaries rather than hidden reasoning. The controller—not the
model—executes a closed action vocabulary.

## Carry useful feedback across attempts

Reflexion shows that structured feedback from prior attempts can improve later
behavior without weight updates.

- https://arxiv.org/abs/2303.11366

BILDR gives fresh attempts bounded prior evidence and a typed strategy
correction; carries verifier findings and failure signatures explicitly; avoids
restarting broad discovery when a candidate exists; and caps repeated
remediation. Controller-authored continuity never becomes self-certification.

## Agent interfaces are part of performance

SWE-agent emphasizes that the agent-computer interface materially affects
software-engineering performance.

- https://arxiv.org/abs/2405.15793

BILDR uses a compact purpose-built snapshot instead of raw database/event access,
closed orchestration actions, rejectable stale/illegal targets, and targeted
read-only probes. This is why the design adds four durable contracts rather than
only a larger prompt.

## Repeated-run policy reliability

tau-bench evaluates tool agents under domain policies and highlights reliable
behavior across repeated runs, including pass-to-the-k style consistency.

- https://arxiv.org/abs/2406.12045

BILDR runs each supervisory eval case repeatedly, measures policy compliance and
illegal-action rate, and binds results to exact model, prompt, policy, and schema
versions. A route with a good average but rare authority violations is not safe
for automatic orchestration.

## Judge limitations

LLM judges can exhibit ordering, verbosity, self-preference, and calibration
problems. BILDR therefore does not treat self-reported confidence as a
probability, let a candidate be sole grader of its traces, accept free-form
completion verdicts, collapse efficiency into an unexplained scalar, or assume
Sol branding makes an answer correct. High-impact labels require independent
review and environment evidence.

## Research-to-design matrix

| Finding | BILDR response |
|---|---|
| routing can improve cost/quality | Terra routine tier and bounded Sol escalation |
| final success hides trajectory quality | progress and efficiency vectors |
| environment-aware judging is stronger | controller-compiled evidence snapshot |
| online monitoring can catch poor trajectories | material events and liveness review |
| grounded action improves reasoning | one closed action then fresh evidence |
| prior feedback helps retries | typed continuity and strategy correction |
| interfaces shape performance | strict snapshots, schemas, and handlers |
| repeated consistency matters | replay, holdout, and repeated-run metrics |
| judges remain imperfect | deterministic policy, verifier, and human authority |

## Open empirical questions

Research does not determine whether Terra high or xhigh is best on BILDR
traces, the correct no-progress thresholds, optimal expert-call limit,
direct-Sol task classes, coalescing/liveness intervals, production acceptance
gates, or whether a cheaper/open model can replace Terra later. Model, effort,
thresholds, prompts, and policy versions are therefore observable and
eval-gated while controller authority remains fixed.
