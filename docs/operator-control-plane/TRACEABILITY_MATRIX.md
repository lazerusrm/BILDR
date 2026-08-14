# Feature traceability matrix

## Purpose

This matrix ensures every proposed capability has a named user failure, a source
owner, an implementation slice, executable evidence, a primary metric,
countermetrics, and an activation gate. A feature without a complete row does
not enter active rollout.

## User outcome to implementation

| Capability | User failure addressed | Authoritative owner | Implementation | Primary evidence | Activation |
|---|---|---|---|---|---|
| durable attention | decisions/approvals disappear behind progress | source subsystem plus attention reducer | OCP-001/002/004/011/012 | source lifecycle replay, fault, operator discovery study | active after zero-loss/closure gates |
| canonical snapshot | browser/CLI/recovery/supervision disagree | snapshot compiler over source tables/events | OCP-002/003/011 | classification fixtures, replay, performance | active read-only after integrity gates |
| return view | operator reconstructs interrupted context manually | snapshot/return compiler and presentation cursor | OCP-003/011/012 | controlled resumption study | canary after objective usability win |
| investigation artifact | findings lost/repeated or mixed with code | investigation task + evidence store | OCP-001/002/005/011/012 | custody/schema/reuse evaluation | active after no-mutation and evidence gates |
| material progress | activity mistaken for useful work | deterministic classifier | OCP-001/002/007 | labeled held-out event corpus | active facts after precision review |
| liveness episode | false/missed stall handling | deterministic observation/episode reducer | OCP-002/007/008 | held-out traces, shadow outcomes | observe -> shadow -> advisory |
| reconciliation | restart loses work or duplicates writer | reconciliation controller + Git/worktree/runtime owners | OCP-002/009/011/012 | full fault matrix and ownership properties | reports first; safe actions later |
| typed intervention | unstructured/destructive intervention | policy/action executor with receipts | OCP-008/009/010/015 | replay, stale/dedupe/custody tests | shadow -> advisory -> active-low-risk |
| external conditions | model polling and lost long waits | adapter registry + scheduler wake | OCP-002/006/011/012 | continuity/restart/rate-limit tests | wake-only after hard gates |
| presence/notifications | routine interruption or hidden critical action | deterministic classifier/delivery store | OCP-002/014/011/012 | mirror/shadow/operator study | critical delivery first; batching opt-in |
| topology | ownership/dependency/evidence difficult to inspect | topology projection | OCP-003/013 | factual comprehension/accessibility study | table active; graph evidence-gated |
| correlation | slow root cause and poor action explanation | trace/correlation store | OCP-017/011/013 | causal validity/redaction/support study | active read-only after security gates |
| governed knowledge reuse | validated discovery repeated | existing knowledge governance | OCP-005/016 | held-out reuse/staleness evaluation | candidate-only under existing promotion |
| remote execution | hardware/platform capacity absent | future separate controller protocol | separate RFC only | protocol/fault/provenance evidence | deferred |

## Capability ownership matrix

| Fact | Authoritative owner | Projection may do | Projection/model/client must not do |
|---|---|---|---|
| approval | approval broker | show source state/attention | approve, invalidate, or reinterpret |
| operator decision | decision owner | show options/outcome | synthesize option or close |
| credential availability | runtime/account capability probe | show redacted requirement/result | store credential value |
| publication | publication controller | show required action/outcome | push/publish/merge |
| evidence acceptance | evidence owner | show refs/status | accept evidence |
| Git/worktree | Git/controller custody | show exact identity/fingerprint | mutate or infer ownership |
| task/run state | scheduler/orchestrator | summarize/current view | set state from prose/UI |
| material progress | deterministic classifier over source events | show event | infer from tokens/output |
| liveness | episode reducer | show state/reasons | rewrite measurements |
| recovery | reconciliation controller | show report/safe actions | force/skip proof |
| external condition | adapter/registry | show current wait/result | execute result as command |
| notification delivery | delivery store | show receipt/health | resolve source attention |
| knowledge | existing governance pipeline | show active scoped item | auto-activate from incident |

## Contract-to-file matrix

| Contract | Domain | Store | Orchestrator | API/CLI | UI | Tests |
|---|---|---|---|---|---|---|
| attention | `harness-domain/operator_control.rs` | `harness-store/operator_control/attention.rs` | source adapters/classification | attention routes/commands | AttentionCenter/Detail | source lifecycle, replay, concurrency, UX |
| investigation | same | `investigations.rs` | task validation/artifact flow | investigation routes/CLI | InvestigationPanel | sandbox, schema, sensitivity, reuse |
| progress | same | `material_progress.rs` | `progress.rs` | read only | timeline/status | labeled corpus/determinism |
| liveness | same | `liveness.rs` | `liveness.rs` | run liveness reads | LivenessPanel | state model, shadow, false/missed cases |
| reconciliation/ownership | same | `reconciliation.rs` | `reconcile.rs` | recovery routes/CLI | RecoveryPanel | full fault matrix/property/concurrency |
| external condition | same | `external_conditions.rs` | `external_conditions.rs` | condition routes/CLI | ConditionsPanel | adapter identity/sequence/fault |
| snapshot/return | same DTOs | `snapshots.rs` | `snapshot.rs` | snapshot/return | Status/Return | classification/perf/usability |
| notification/presence | same | `notifications.rs` | `notifications.rs` | presence/delivery health | Presence/Health | mirror/shadow/delivery fault |
| topology | same | optional persisted snapshot | topology compiler | topology route/CLI | table/optional graph | causal/accessibility/perf |
| correlation | trace crate types | trace store | propagation | trace route/CLI | evidence navigation | parent/link/redaction/restart |

## Attention source-adapter matrix

| Source | Opens/updates on | Valid closure | Blocking logic | Critical examples |
|---|---|---|---|---|
| approval | pending approval/revision change | exact approval outcome | source risk/current operation | destructive/security/publication |
| decision | versioned option set | typed answer/decline | source says required for frontier | public contract, data loss, policy |
| credential | capability absent/expired | successful bounded probe or cancel | task/run requires capability | production credential/identity failure |
| publication | explicit publish action pending | publication outcome/cancel | publication phase only | protected branch/remote mismatch |
| policy | exception/decision required | exact policy outcome | policy-defined | authority/tenancy/privacy |
| evidence | required proof absent/rejected | evidence owner acceptance/cancel | success criteria/final audit | false completion risk |
| external condition | wait registered/failed/expired | exact terminal result/cancel | dependency/critical path | deadline/continuity break |
| reconciliation | ownership/effect/version conflict | recovery resolution receipt | mutable dispatch/critical path | duplicate writer/unknown effect |
| infrastructure | controller/runtime/projection/delivery degradation | verified restore/permanent typed failure/cancel | affected capability | integrity/security/delivery down |

## Liveness evidence matrix

| Evidence | Healthy/quiet | External wait | Degraded/stall | Ownership/recovery |
|---|---:|---:|---:|---:|
| new material progress | strong healthy | may close wait | contradicts unchanged stall | preserve identity |
| bounded command active with matching identity | quiet active | neutral | delay stall boundary | supports live owner |
| exact external condition active | neutral | strong | prevents ordinary stall | preserve/wait |
| repeated unchanged semantic action | weak | neutral | strong degraded/stall | inspect identity |
| repeated typed failure | weak | neutral | strong degraded/stall | may require fresh attempt after proof |
| validation trend improving | healthy | neutral | contradicts stall | preserve candidate |
| validation unchanged/regressing | neutral | neutral | supporting evidence | verify exact candidate |
| process/session missing | not failure alone | neutral | supporting only | triggers reconciliation |
| worktree changed/useful candidate | material/preserved | neutral | may show progress | blocks discard/requires custody |
| command/external effect unknown | unknown | unknown | no intervention | recovery required; no retry |
| identity/fingerprint mismatch | unknown | neutral | no stall action | ownership unknown, high attention |

No single row, including timeout or process death, proves a confirmed stall or
authorizes replacement.

## Recovery action matrix

| Finding combination | Allowed automatic/advisory action | Forbidden action |
|---|---|---|
| live exact owner and compatible session | attach/continue or no action | fresh attempt |
| owner absent, command terminal, work preserved, compatible context | resume compatible/durable context | reset/delete work |
| candidate present, verifier missing | requeue exact-candidate verification | rebuild candidate blindly |
| approval bound to stale HEAD/fingerprint | invalidate through approval owner | reuse approval |
| lease expired but owner live/unknown | preserve, inspect, open attention | steal lease |
| owner/process/session proven absent, no unknown effects, proof valid | authorize one fresh attempt | concurrent replacement |
| command/external effect unknown | preserve/pause, attention, targeted reconciliation | automatic retry |
| version incompatible | preserve/pause, exact mismatch attention | silent migration of active agent |
| worktree fingerprint mismatch | preserve, inspect, attention | checkout/reset/clean |
| corrupt artifact/snapshot digest | quarantine affected record, keep source state | treat as valid/empty |

## Notification matrix

| Class | Interactive | Focus | Unattended | Maximum rule |
|---|---|---|---|---|
| critical attention/security/custody | immediate | immediate | immediate | no defer |
| high attention | immediate | immediate or short bounded defer | bounded defer with deadline | never beyond configured hard maximum |
| normal attention | immediate UI; optional desktop | batch | digest | resurfaced until source outcome |
| material progress | live UI | batch | digest | chronological and source-linked |
| terminal outcome | live UI/optional desktop | batch or immediate by risk | digest unless high | never closes unrelated attention |
| recovery update | immediate when conflict/high | batch routine | digest routine | conflicts bypass |
| system degradation | by impact | by impact | by impact | critical delivery failure bypasses |
| routine capacity | UI | batch | digest | may be omitted only with explicit truncation |

## Test traceability

| Invariant/user claim | Required tests |
|---|---|
| decisions never disappear | source adapter table tests, interleaved replay, restart/rebuild, task completion |
| one mutable owner | state/property model, concurrent claim, process/session identity, fault matrix |
| useful work preserved | worktree/candidate/untracked fixtures, restart at boundaries, digest comparison |
| ambiguous effects not retried | command/external adapter failpoints and repeated reconciliation |
| investigation read-only | sandbox/path/Git/candidate/integration negative tests |
| snapshot canonical | classification fixtures, deterministic ordering/truncation/digest, performance |
| return view improves resumption | randomized controlled operator study |
| liveness improves intervention | held-out trace labels, shadow outcomes, hard destructive gates |
| notification reduces interruption safely | mirror replay, opt-in study, critical delivery fault tests |
| topology improves comprehension | table vs table+graph task study and accessibility |
| actions explainable | trace parent/link/source/policy/receipt validation |
| knowledge safe | scope/freshness/contradiction/review/rollback tests |

## Rollout traceability

| Capability | Default when first merged | Next mode requires |
|---|---|---|
| attention/snapshot read models | disabled until migration/replay, then active | all source adapters hard-gated |
| return view | disabled/canary | objective usability improvement |
| investigation | disabled | custody/evidence/security suites |
| external conditions | disabled then wake-only | adapter continuity/restart gates |
| progress classifier | observe | held-out precision review |
| liveness | observe | shadow safety/quality, then advisory |
| reconciliation | report/attention only | fault evidence for each safe action |
| interventions | shadow | advisory evaluation, then reversible canary |
| notification batching | mirror only | opt-in study and critical bypass proof |
| topology graph | disabled | table+graph usability/accessibility win |
| knowledge proposals | candidate-only | existing promotion governance |
| remote execution | absent | separate accepted RFC and protocol evidence |

## Excluded feature traceability

| Excluded feature | Reason | Reconsideration condition |
|---|---|---|
| public-message/social execution | privacy, prompt injection, weak core value | separate product/security case |
| terminal multiplexer injection | brittle presentation state as control path | never as canonical controller primitive |
| broad multi-harness support now | weakens pinned protocol/custody/accounting | provider abstraction with equal conformance evidence |
| continuous model polling | unnecessary cost/races | no expected condition; deterministic events preferred |
| worker count KPI | encourages costly duplication | never as success metric |
| one health/progress score | hides evidence and uncertainty | never as authority; optional display only with vectors |
| quota-only downgrade | quality/capability risk | held-out route evaluation within approved class |
| free-form global memory | stale/sensitive/unreviewed influence | use existing governed knowledge only |
| automatic publication/merge | outside authority boundary | not in this architecture |
| replacement under unknown ownership | duplicate writer/work loss | only after exclusive ownership proof |
| remote execution implementation | trust/provenance/transport not yet built | separate RFC passes boundary gates |

## Completion rule

The program is not complete until every active capability row has:

```text
implemented source owner and contract
positive and negative tests
fault/replay coverage where stateful
primary metric and countermetric result
security/privacy/performance evidence
explicit rollout mode and rollback
independent review on exact head
```

Rows with inconclusive user benefit remain disabled or use their simpler
deterministic form.
