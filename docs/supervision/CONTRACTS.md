# Supervisory Orchestrator Contract Boundaries

## Two-layer contract

Each model call has two distinct contracts.

### Controller envelope

This layer is never generated or modified by Terra or Sol. It lives in typed
Rust state and durable store columns and binds:

- run, task, attempt, session, snapshot, goal revision, plan digest, and base SHA;
- requested and effective model and reasoning effort;
- read-only execution mode and tool/network policy;
- role and authority (`Supervisor` or advisory `Expert`);
- maximum child count;
- token, time, request-count, and expiry limits;
- account attribution and usage receipt;
- action/expert dedupe signature;
- allowed actions and policy version.

A model payload is rejected when it conflicts with this envelope. The envelope
is also rechecked immediately before any controller command.

### Model-visible payload

The JSON Schemas under `schemas/` define the bounded information the model may
consume or produce:

- `harness.supervisor-snapshot.v1`: immutable controller facts presented to Terra;
- `harness.supervisor-decision.v1`: Terra assessments and closed action proposals;
- `harness.expert-request.v1`: the bounded question/context/evidence presented to Sol;
- `harness.expert-response.v1`: Sol's advisory answer.

The expert payload deliberately does not grant or negotiate route, effort,
write access, child creation, budget, or authority. Those are controller
envelope properties and must not be represented as model choices.

## Identity and freshness

Every decision/response must match its controller envelope. The runtime checks
at least run, snapshot, snapshot revision, goal revision, plan digest, target,
expiry, and current state. Stale content remains auditable but is never applied.

## Schema evolution

- keep `additionalProperties: false` at model boundaries;
- add a new discriminator for breaking changes;
- never weaken a controller-envelope invariant through a schema revision;
- persist schema, prompt, model, effort, and policy versions with every trace;
- require `cargo xtask schema-check` and conforming examples before activation;
- reject unknown enum values rather than repairing them into a legal action.

## Expert category mapping

The supervisor decision may use a detailed internal reason code. The
controller normalizes it into the bounded expert request category vocabulary
before materializing the request. The mapping is versioned, deterministic, and
included in the policy receipt. A category mapping never satisfies an
escalation gate by itself.

## No direct effect

Schema conformance does not authorize execution. The path remains:

```text
schema-valid model payload
 -> envelope/freshness check
 -> deterministic policy
 -> transactional precondition check
 -> existing controller command
 -> durable outcome event
```

An expert response stops before the action-policy step. It becomes evidence in
a new supervisor snapshot; Terra must propose a separate legal action.
