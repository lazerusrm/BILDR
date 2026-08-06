# ADR-0003: Persist raw runtime events before typed projection

**Status:** proposed

## Decision

Append every accepted App Server notification/request outcome to SQLite before projecting it into run/task/agent/activity state. Store large logs and artifacts by content hash.

## Rationale

The Codex protocol evolves, the UI needs restart/replay, and projector bugs must not destroy runtime evidence. Full event sourcing is unnecessary; immutable raw events plus normal relational state provides the required durability with lower complexity.

## Consequences

- projections are idempotent and rebuildable;
- unknown additive events remain inspectable;
- raw reasoning is explicitly dropped/metadata-only under default policy;
- retention and artifact verification are first-class operations.
