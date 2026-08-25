# Run-scoped agent disciplines

BILDR exposes two default-on choices when a user creates a task. Both choices
are persisted in immutable run metadata and apply only to new threads in that
run; neither can silently change a thread that is already executing.

## Ponytail minimal implementation

The **Ponytail minimal implementation** option applies its decision ladder to
architect, governor, worker, high-risk worker, and integrator prompts:

1. establish that a change is necessary;
2. reuse existing capability where it meets the requirement;
3. prefer platform and installed dependencies before adding code or packages;
4. make the smallest clear change that satisfies the acceptance evidence.

It never authorizes skipping validation, security, accessibility, custody, or
the user's stated requirements. Review, interview, supervision, and evidence
roles remain independent of implementation pressure.

## Caveman compact handoffs

The **Caveman compact handoffs** option asks every run role to make narration,
status, and handoffs concise. It is intentionally a native prompt discipline,
not a transcript proxy or a lossy context compressor.

Exact material stays exact: required JSON, source patches, command output,
errors, digests, receipts, security evidence, accessibility evidence, and user
requirements must be returned unchanged. The option therefore cannot rewrite
provider traffic, access credentials, or substitute a summary for a durable
record.

The names acknowledge useful open-source ideas, but BILDR has no runtime
dependency on the upstream Ponytail or Caveman projects. This keeps the
controller's custody boundary and local model routes explicit.
