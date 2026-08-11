# ADR-0010: Project branch-aware trace graphs from raw events

- **Status:** accepted architecture
- **Date:** 2026-08-11

## Context

BILDR stores raw protocol events and relational thread/turn projections.
Long-running execution includes shared prefixes, compaction, retries,
remediation, resumed work, and subagents. A linear transcript loses those
relationships and duplicates context.

## Decision

Keep raw events authoritative and add a derived, content-addressed trace DAG.

Nodes represent messages, model calls, tools, commands, changes, approvals,
compaction, subagent operations, validation, findings, feedback, and outcomes.
Typed edges represent ordering, context, tool results, spawning, joining,
compaction, retry, derivation, and supersession. A branch manifest is a
root-to-leaf path with shared-node references.

Trace export includes redaction, privacy/license, runtime, and source-event
receipts.

## Consequences

Evaluation and optional training can preserve the context actually observed by
each branch. Projection and storage are more complex, but raw-event replay
remains the recovery authority.

## Rejected alternatives

- export one flattened transcript;
- store only summaries;
- make exported traces the primary event authority.
