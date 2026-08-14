# Product research and feature decisions

## Research method

This review used four tests before admitting a feature:

1. **Concrete user failure:** name the failure the operator experiences.
2. **Mechanism:** explain why the proposed capability changes that failure.
3. **Measurement:** define a primary outcome and a countermetric.
4. **Authority fit:** preserve BILDR's controller, exact-SHA custody, approval,
   evidence, and publication boundaries.

A feature is excluded when it mainly increases visible complexity, agent count,
notifications, or animation without improving a user outcome.

The comparison repository was treated as an operational incident catalog, not a
product template. Its useful lessons concern lost decisions, stale status,
process failure, ownership continuity, external waits, restart recovery, and
bounded summaries. Its terminal/session implementation, broad runtime support,
public-message integration, and metaphor vocabulary are not adopted.

## User jobs

### Leave a run without monitoring it continuously

The operator needs routine work to continue within existing authority while
critical decisions, credentials, custody failures, and destructive operations
remain impossible to miss.

### Return and recover context quickly

The operator needs a current evidence-backed view of what changed, what requires
action, what was recovered, and what the next legal actions are. Reconstructing
state from conversation or logs is not acceptable.

### Distinguish progress from activity

The operator needs to know whether work produced a candidate, evidence,
validation advance, resolved blocker, investigation result, or integration
advance—not merely that a model used tokens or invoked tools.

### Preserve work through failure

A daemon, process, session, transport, or account failure must not discard
useful work or create a second writer. Unknown completion must remain explicit.

### Investigate without contaminating implementation

Read-only analysis should produce reusable evidence without acquiring mutable
path authority, creating a candidate, or mixing hypotheses into implementation.

### Know exactly what requires action

Approvals, decisions, credentials, recovery conflicts, policy exceptions, and
external dependencies must stay visible until their owning subsystem records a
typed outcome.

### Understand why the system acted

Every intervention, retry, pause, routing choice, and recovery outcome should be
traceable to controller observations, policy, exact identities, and receipts.

## External evidence

### Multi-agent work is valuable only for the right shape

Anthropic reports that a multi-agent research system improved an internal
research evaluation by 90.2 percent, but used about 15 times the tokens of
ordinary chats. It also reports that coding has fewer naturally parallel tasks,
and describes early failures from spawning too many agents, duplicated work,
endless search, excessive updates, weak delegation, and inadequate stateful
error recovery.

Source:
https://www.anthropic.com/engineering/multi-agent-research-system

Implication for BILDR:

- preserve bounded parallel work for genuinely independent tasks;
- do not use worker count as a product metric;
- invest first in state, ownership, artifacts, evaluation, and recovery;
- require plan/path separation and measurable value before fan-out.

### Simplicity and evaluation should precede orchestration complexity

OpenAI's practical guide to building agents recommends starting with a strong
single-agent baseline, introducing orchestration only when required, defining
evaluations, and retaining human intervention for high-risk or failure-threshold
situations.

Source:
https://cdn.openai.com/business-guides-and-resources/a-practical-guide-to-building-agents.pdf

Implication:

- deterministic controller capabilities come before additional autonomous roles;
- adaptive liveness, notification suppression, and routing start in observe or
  shadow mode;
- user and safety evidence—not implementation completion—controls activation.

### Stateful work needs durable resume and external artifacts

Anthropic's engineering report describes stateful errors that compound across
long-running work and the cost of restarting from the beginning. Temporal's
durable-execution model similarly reconstructs workflow state from persisted
event history after crashes and infrastructure failures.

Sources:
https://www.anthropic.com/engineering/multi-agent-research-system
https://docs.temporal.io/workflow-execution

Implication:

- process state is an observation, not task identity;
- BILDR needs durable reconciliation episodes, cursors, receipts, and preserved
  artifacts;
- a restart must rebuild legal current state rather than replaying prose;
- unknown external effects must block automatic retry.

### Operators need several forms of oversight

A 2026 qualitative study of 17 experienced developers working with coding agents
identified a priori control, co-planning, real-time monitoring, and post-hoc
review as distinct oversight modes. Participants relied heavily on test results
and described review difficulty as a central challenge.

Source:
https://arxiv.org/abs/2601.06124

Implication:

- BILDR should not reduce oversight to a transcript;
- plans, live evidence, attention, exact candidate/validation identity, and final
  review each need explicit surfaces;
- topology and return views must improve review decisions, not merely show
  activity.

### Automated resumption cues can outperform manual notes

A controlled Microsoft study with 371 programmers found automated task-resumption
cues doubled successful task resumption compared with note-taking. Participants
preferred chronological code snippets to diff-only or method-list cues.

Source:
https://www.microsoft.com/en-us/research/publication/assisting-programmers-resuming-interrupted-programming-tasks/

Implication:

- build a deterministic chronological return-to-work view from material events,
  attention changes, recovery outcomes, and source links;
- measure time and correctness of resumption against current UI, not only user
  preference;
- never require the operator to maintain a parallel note system.

### Notification batching can reduce interruption but is not universally calming

Microsoft research on notification batching found productivity benefits in
some conditions while effects on stress and perceived control were mixed.

Source:
https://www.microsoft.com/en-us/research/publication/intelligent-notification-scheduling-and-batching/

Implication:

- treat fewer interruptions and correct response time as primary outcomes;
- keep batching opt-in or gated until measured;
- never defer critical custody/security/decision events;
- measure delayed important action and notification reopening as countermetrics.

### Reconciliation is a proven control-plane pattern

Kubernetes controllers repeatedly compare desired and actual state and make
small idempotent changes, while representing uncertainty and retry rather than
assuming a process completed because it was invoked.

Source:
https://kubernetes.io/docs/concepts/architecture/controller/

Implication:

- BILDR recovery should inventory, compare, and apply the smallest proven-safe
  action;
- recovery should be repeatable and idempotent;
- ambiguous state should remain paused/unknown instead of becoming failed or
  healthy by convenience.

### Distributed execution needs content identity and provenance

Bazel's remote execution APIs use content-addressed inputs/results and explicit
execution actions. SLSA provenance provides a standard vocabulary for binding
artifacts to build inputs and process identity.

Sources:
https://bazel.build/remote/rbe
https://slsa.dev/spec/v1.1/provenance

Implication:

- future remote execution requires content-addressed bundles, immutable leases,
  result manifests, provenance, and central re-verification;
- SSH reachability alone is not a custody model;
- remote nodes must have no publication, merge, approval, or evidence-acceptance
  authority.

### Correlated traces are a standard observability primitive

OpenTelemetry and W3C Trace Context define interoperable trace/span identity and
causal propagation across distributed operations.

Sources:
https://opentelemetry.io/docs/concepts/signals/traces/
https://www.w3.org/TR/trace-context/

Implication:

- propagate trace context across model turns, commands, artifacts, validations,
  decisions, interventions, and recovery;
- retain BILDR domain IDs separately;
- use links for fan-out/fan-in and redact exports.

## Feature-benefit analysis

### Durable attention ledger — adopt

**Failure:** a required decision or approval is buried by later activity or task
completion.

**Mechanism:** source-owned durable items remain open until a typed source
outcome.

**Primary metrics:** lost-decision rate, decision discovery time, overdue open
items.

**Countermetrics:** duplicate items, false criticality, operator reopening,
alert fatigue.

**Hard gate:** zero unresolved authoritative sources absent from the ledger in
replay and fault suites.

### Investigation artifacts — adopt

**Failure:** analysis is lost in conversation, repeated, or mixed with code.

**Mechanism:** read-only task kind produces validated immutable findings and a
decision inventory.

**Primary metrics:** reuse rate, reduction in repeated reads/tokens, later
implementation correctness.

**Countermetrics:** artifact overhead, stale findings, unsupported confidence,
sensitive-data exposure.

**Hard gate:** investigation cannot acquire mutable lease, create candidate, or
enter integration.

### Stateful liveness episodes — observe, then shadow

**Failure:** timeouts interrupt healthy work or fail to detect repeated
non-progress.

**Mechanism:** combine material progress, process/session/command state,
validator trend, external waits, repeated semantic actions, and recovery state
across an episode.

**Primary metrics:** precision/recall for intervention-worthy stalls, time to
useful progress after intervention, no-progress tokens.

**Countermetrics:** healthy-work interruption, destructive error, repeated
intervention, additional model cost.

**Hard gate:** zero kill/reset/replacement from uncertain ownership.

### Ownership-safe reconciliation — adopt

**Failure:** restart loses work, duplicates a writer, or retries an ambiguous
external effect.

**Mechanism:** exact inventory, exclusive ownership proof, preserved worktree,
idempotent action receipts.

**Primary metrics:** preserved-work rate, recovery success, time to restored
legal state.

**Countermetrics:** false pause, recovery latency, manual intervention rate.

**Hard gates:** zero duplicate mutable owners, zero discarded unknown work, zero
automatic retry of unknown external effects.

### Canonical control-plane snapshot — adopt

**Failure:** browser, CLI, recovery, and supervision present conflicting state.

**Mechanism:** one bounded revisioned server projection with explicit unknown,
stale, and truncated sections.

**Primary metrics:** cross-surface consistency, compile latency, support/debug
time.

**Countermetrics:** stale projection duration, payload size, source-query cost.

**Hard gate:** authority handlers never authorize from the snapshot.

### Return-to-work view — adopt and run controlled study

**Failure:** operator reconstructs context from multiple screens and transcripts.

**Mechanism:** chronological material changes plus current attention, work,
recovery, capacity, and next legal actions.

**Primary metrics:** time to correct first action, task-state comprehension,
source-navigation count.

**Countermetrics:** incorrect action, important omission, view size, perceived
clutter.

**Promotion:** must beat current UI on objective resumption outcomes.

### Presence-aware notification policy — adopt classification; gate suppression

**Failure:** routine progress interrupts the operator, while broad batching risks
hiding critical work.

**Mechanism:** deterministic priority, durable delivery, maximum defer boundary,
critical bypass, and return digest.

**Primary metrics:** interruption count, critical response time, delivery
success.

**Countermetrics:** delayed important action, missed/reopened notification,
operator mode confusion.

**Activation:** mirror classification first; suppression only after replay and
operator evidence.

### Topology view — table first; graph optional

**Failure:** complex run ownership, dependencies, evidence, and blockers are
hard to understand.

**Mechanism:** canonical node/edge projection with source references.

**Primary metrics:** answer accuracy and time for ownership/dependency/evidence
questions.

**Countermetrics:** navigation actions, accessibility failures, render latency,
visual clutter.

**Decision:** accessible table/list is required. Graph remains off unless it
improves measured comprehension.

### External condition registry — adopt wake-only

**Failure:** long CI/review/resource waits waste model turns or depend on
conversation continuity.

**Mechanism:** typed durable adapters observe exact conditions and emit material
events.

**Primary metrics:** model turns/tokens avoided, wake latency, continuity.

**Countermetrics:** stale poll, rate-limit pressure, duplicate wake, false
satisfaction.

**Hard gate:** result bytes never authorize an action and generic arbitrary
condition-to-command is absent.

### Resource-aware routing — integrate later with evals

**Failure:** fixed routes can overspend or use unavailable capacity.

**Mechanism:** select within an approved capability class using task/risk class,
held-out eval performance, latency, cost, and account headroom.

**Primary metrics:** outcome quality per cost/time and avoided capacity failure.

**Countermetrics:** quality regression, route instability, biased selection.

**Decision:** quota alone never triggers downgrade; implement only after route
replay data exists.

### Governed knowledge reuse — integrate existing system

**Failure:** repeated investigations rediscover validated facts/procedures.

**Mechanism:** propose evidence-bound scoped knowledge candidates through the
existing review/freshness pipeline.

**Primary metrics:** repeated failure/discovery reduction and verification
outcome.

**Countermetrics:** stale/incorrect influence, operator correction, retrieval
cost.

**Decision:** no free-form conversation memory and no automatic activation from
a single incident.

### Remote execution nodes — defer

**Potential value:** macOS, GPU, hardware, large-build, and isolated validation
capacity.

**Risks:** remote ownership, credentials, transport ambiguity, node compromise,
result provenance, duplicated dispatch, local replacement after unknown
completion.

**Decision:** reserve contract fields and document the boundary, but implement
only under a separate RFC with content-addressed inputs, signed leases,
attestation/capabilities, provenance, central verification, quarantine, and
fault evidence.

## Rejected performative metrics and surfaces

Do not optimize or claim product value from:

- number of workers or concurrent turns;
- tokens consumed;
- command/tool volume;
- animated activity indicators;
- number of notifications sent;
- one opaque health/progress/efficiency score;
- graph size or visual density;
- number of generated findings without downstream validation;
- autonomous action count;
- self-reported model confidence;
- raw completion speed without correctness, evidence, and cost.

## Open empirical questions

The implementation and rollout program must answer:

1. Which material-event taxonomy best predicts useful progress by task class?
2. Which liveness evidence combinations achieve adequate precision without
   excessive delay?
3. Does the return view improve correct first action across real BILDR runs?
4. Which routine notification categories can be deferred safely?
5. Does a graph improve comprehension beyond the accessible table?
6. How often are investigation artifacts reused and how much repeated work do
   they prevent?
7. Which reconciliation actions are common enough to automate after shadow
   evidence?
8. Can resource-aware routing improve cost/availability without quality loss?
9. Which knowledge candidates remain valid across repository and runtime
   changes?
10. Which remote workloads justify the added trust and operational surface?

Until those questions have evidence, the corresponding adaptive behavior stays
disabled, observe-only, shadow, or advisory.
