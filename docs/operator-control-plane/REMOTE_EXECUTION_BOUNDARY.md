# Remote execution boundary

## Status

Deferred architecture boundary. This document authorizes no remote runtime,
network listener, node registration, credential transfer, or scheduling change.

Remote execution may be valuable for macOS/iOS builds, GPU and hardware tests,
large compilation, isolated validation, and machines with specialized tools.
It also introduces a new trust domain and failure modes that cannot be solved by
starting an agent over SSH.

## Decision boundary

A future RFC may propose remote execution only after the local operator control
plane proves:

- durable task/attempt identity;
- one mutable owner;
- ownership-safe reconciliation;
- immutable artifacts and evidence;
- explicit unknown completion;
- causal correlation;
- exact-SHA and worktree custody;
- bounded external-condition handling;
- controller-only publication and evidence acceptance.

The first operator-control implementation may reserve optional `execution_node_id`
and node correlation fields. They remain null locally.

## Non-negotiable invariants

1. `harnessd` remains the global controller authority.
2. A node receives an immutable, signed, expiring task lease; it cannot choose
   or expand its work.
3. All input identities are content-addressed and bound to exact repository,
   base/dependency SHAs, policy, toolchain, and task packet.
4. A task/worktree has at most one recognized mutable lease across local and
   remote execution.
5. Transport loss after dispatch creates `unknown_completion`, not failure or
   permission to start local replacement.
6. Nodes cannot push, publish, merge, accept evidence, approve, grant
   credentials, or mutate the primary checkout.
7. Results are manifests and artifacts with digests/provenance, not trusted
   status prose.
8. The controller re-verifies candidate ancestry, manifest integrity, required
   tests/evidence, and policy before candidate admission.
9. Node credentials are least-privilege, scoped, time-bounded, revocable, and
   never shared with model context.
10. Compromised or inconsistent nodes are quarantined; their results never
    advance current state.
11. Remote execution has no silent local fallback.
12. Every state transition, dispatch, result, rejection, and recovery is
    correlated and auditable.

## Threat model

Design for:

- node impersonation and stolen keys;
- controller impersonation;
- replayed or duplicated lease/result messages;
- tampered source/input/toolchain bundles;
- compromised node/runtime/model;
- malicious repository content;
- path traversal and symlink escape;
- secret exfiltration;
- result/candidate substitution;
- lease expiry during execution;
- network partition after task receipt or external effect;
- node reboot/disk loss;
- stale or partially written worktree;
- fabricated capability or test evidence;
- denial of service/resource exhaustion;
- clock skew;
- downgrade to an incompatible runtime/policy;
- controller restart with in-flight remote work;
- local replacement while remote completion is unknown.

## Proposed components

### Node registry

Controller-owned records:

```text
node ID
public key / certificate identity
registration and approval state
capability descriptor digest
platform/architecture/resources
allowed repository/task/risk classes
runtime/toolchain versions
sandbox/network policy
last attestation/health
current leases
quarantine/revocation state
```

Node self-report is untrusted until validated. Registration is explicit and local
operator-approved.

### Capability descriptor

A signed bounded descriptor names:

```text
OS/architecture
CPU/memory/storage
GPU/hardware identities and drivers
installed toolchain/container/runtime digests
sandbox and network capabilities
supported task execution kinds
maximum concurrency/resource classes
attestation method and freshness
```

Scheduler selects only nodes whose validated descriptor satisfies the task.
Capability labels do not grant authority.

### Immutable task lease

The controller-issued lease binds:

```text
lease ID and generation
node ID
repository/run/task/attempt IDs
execution kind and role
base/dependency SHAs
source/input bundle digests
policy/profile/toolchain/runtime digests
owned/forbidden paths
sandbox/network/secret scopes
budgets/deadlines
required artifact/evidence/result schemas
correlation context
expiry and controller signature
```

A node rejects unknown fields, invalid signature, wrong identity, expired lease,
unsupported capability, or mismatched input digest.

### Content-addressed input

Prefer a minimal Merkle/CAS bundle containing exact source tree, dependencies,
configuration, generated contracts, task packet, and read-only context.

Do not grant ordinary remote repository credentials when a content-addressed
bundle suffices. Cache hits are verified by digest. Inputs are immutable for the
attempt.

### Execution receipt

Node persists an accepted-lease receipt before execution and reports:

```text
lease and node identity
input/toolchain/policy digests
accepted time
sandbox identity
process/execution generation
correlation ID
```

Controller state becomes dispatched/accepted only from authenticated receipts,
not a network send success.

### Result manifest

Terminal result contains:

```text
lease/node/execution identity
start/end times and result class
exact candidate commit/tree/parent identities if any
worktree-state/fingerprint summary
artifact/log/command/validation/evidence digests
resource/usage accounting
external effects and ambiguity
policy/toolchain/runtime identities
known limitations
node signature
manifest digest
```

Large content is referenced from content-addressed storage. Manifest result class
never self-authorizes admission.

### Provenance

Use SLSA-compatible provenance vocabulary where practical, binding subject
artifacts to source/input, builder/node identity, invocation/task lease,
materials, parameters, environment, timestamps, and completeness/reproducibility
claims.

Provenance is evidence, not proof of a trustworthy node by itself. Controller
policy determines required attestation and re-verification.

## Transport semantics

Use authenticated encrypted bidirectional communication with explicit protocol
version, message IDs, sequence numbers, acknowledgements, replay protection,
bounded payloads, flow control, heartbeat/lease renewal, and key rotation.

The protocol should support disconnected results through an authenticated
content store only if equivalent identity, sequence, and replay guarantees are
preserved.

### Dispatch lifecycle

```text
planned
lease_issued
sent
accepted
running
result_available
result_received
verifying
admitted | rejected | quarantined | unknown_completion
```

A connection close does not imply a terminal result.

### Idempotency

Dispatch and result submission are idempotent by lease/generation/message/result
digest. Duplicate identical results receive the same receipt. Conflicting
results quarantine the node/lease and open critical attention.

## Scheduling

The central scheduler considers validated capability, risk policy, task kind,
resource availability, locality/cache, account/model route, current lease,
expected transfer cost, reliability history, and canary policy.

Do not use remote capacity merely to maximize concurrency. Remote dispatch must
beat local execution on a defined user outcome or satisfy unavailable hardware/
platform requirements.

High-risk tasks may require local-only execution or remote execution plus local
independent verification. Publication and final audit remain controller-owned.

## Node lifecycle

### Registration

1. Operator initiates registration locally.
2. Controller generates one-time enrollment challenge.
3. Node presents key and capability/attestation evidence.
4. Operator reviews identity, capability, scopes, and policy.
5. Controller records approved identity/scopes.
6. Node runs nonmutable conformance checks.
7. Node enters disabled/observe, then canary after evidence.

### Draining

Draining stops new leases and permits current leases only within policy.
Unfinished work is reconciled; it is not automatically moved.

### Quarantine

Quarantine immediately stops new dispatch, rejects unverified pending results,
marks active leases for reconciliation, rotates/revokes credentials where
needed, preserves evidence, and opens critical attention.

### Key rotation and revocation

Support overlapping bounded rotation, signed transition, explicit revocation,
and replay denial. An expired/revoked key cannot submit a late result without a
reviewed recovery path.

## Worktree and Git custody

A node may construct an isolated execution directory from the immutable input
bundle. For mutable implementation it may create a local candidate commit only
within the lease.

The controller must verify:

```text
parent/ancestry exactly matches lease/dependencies
commit/tree/artifact digests
changed paths within lease
no forbidden/reserved path mutation
no unexpected submodule/LFS/symlink escape
candidate author/metadata policy
worktree fingerprint and untracked/staged state
required evidence/validation identities
```

The node cannot update controller refs, origin, primary checkout, integration
worktree, or publication state.

## Credentials and secrets

Prefer zero repository/provider credentials on nodes. When a task requires a
secret:

- use dedicated least-privilege scoped credential;
- bind to node/lease/task and short expiry;
- deliver outside model context through a controller-approved secret channel;
- prevent persistence in logs/artifacts/environment snapshots;
- record use metadata but not value;
- revoke on completion/quarantine;
- treat any unknown external effect as requiring reconciliation.

Remote tasks with public-side effects should initially be forbidden.

## Observability and correlation

Propagate W3C trace context plus lease/node/domain identity. Record dispatch,
acceptance, process/command, artifact/result, controller verification, admission,
rejection, retry, transport failure, and reconciliation.

Logs are bounded, content-addressed, sensitivity-labeled, and redacted. Node
metrics are not trusted as sole evidence; controller observations and receipts
remain distinct.

## Failure scenarios

### Network loss after send

If no authenticated acceptance receipt exists, controller may retry the same
lease/message idempotently. It does not issue a new generation until the
original is resolved/expired under protocol proof.

### Network loss after acceptance or during execution

State becomes disconnected/unknown, lease remains owned remotely, and no local
replacement starts. Reconnect/result query/reconciliation determines outcome.

### Node reboot

Node reconstructs accepted leases from durable state and either resumes under
compatible policy or reports preserved/failed/unknown. Controller does not infer
failure from heartbeat loss.

### Controller restart

Rebuild dispatch/result state from durable event history and query nodes using
lease identities. Preserve unknown completion and prevent duplicate dispatch.

### Duplicate result

Accept identical manifest digest idempotently. Conflicting digest quarantines
and opens attention.

### Invalid candidate ancestry or manifest

Reject/quarantine result, preserve artifacts for audit, open attention by risk,
and do not advance task/candidate.

### Node offline with uncommitted work

Do not start local replacement until node storage is recovered or policy proves
work irrecoverable and exclusive ownership. Report potential work loss
explicitly.

### Result after lease expiry

Expiry stops authority to continue, not necessarily result submission. Treat as
late evidence; verify identity/policy and require explicit admission rules. Never
accept automatically because the node says success.

### Local fallback requested

Fallback is a new attempt only after original lease/owner/effects are reconciled
and exclusive ownership proof exists.

### Compromise suspected

Quarantine node, revoke keys/secrets, freeze active leases, reject pending
unverified results, preserve traces/artifacts, require local re-verification or
re-execution, and audit affected prior outputs according to policy.

## Evaluation gates before implementation

A future RFC must provide:

- concrete workloads/user value and local baseline;
- protocol state machines and formal invariants;
- node/controller threat model and security review;
- content-addressed input/result design;
- lease signature/key/attestation design;
- unknown-completion and reconciliation design;
- credential isolation;
- provenance and central verification;
- capacity/cost/latency model;
- conformance simulator/fake node;
- fault-injection matrix;
- rollout/quarantine/revocation/rollback runbooks;
- no-publication-authority proof.

## Required fault tests before canary

```text
duplicate/out-of-order/replayed messages
transport loss before/after acceptance and result
controller/node restart at every state
lease expiry/renewal/clock skew
node identity/key rotation/revocation
input/cache/toolchain tamper
malformed/oversize manifest/artifact
conflicting duplicate result
candidate ancestry/path violation
unknown external effect
secret/log redaction failure
node compromise/quarantine
content store partial/corrupt data
concurrent local/remote claim attempt
late result after replacement proof
```

Canary is forbidden until zero duplicate mutable ownership, zero authority
bypass, zero untracked secret leak, zero ambiguous automatic retry, and correct
unknown-completion behavior are proven.

## Rejected shortcuts

- raw SSH command execution;
- shared writable network filesystem;
- shared repository credentials;
- node self-selecting tasks/scopes;
- process heartbeat as task completion;
- local retry after network timeout;
- result prose without manifest/digest/provenance;
- trusting remote tests without central policy/identity verification;
- direct remote push/publication/merge;
- remote evidence acceptance;
- permanent broad secrets;
- silent local replacement;
- optimizing for node count/utilization as product value.

## Reconsideration criteria

Reopen remote execution only when local operator-control foundations are active
and measured, at least one important workload cannot be served acceptably
locally, expected user benefit exceeds transport/operational cost, and the
separate RFC passes security, custody, protocol, fault, product, and operations
review.
