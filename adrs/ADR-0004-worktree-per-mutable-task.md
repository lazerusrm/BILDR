# ADR-0004: Give every mutable task attempt its own Git worktree

**Status:** proposed

## Decision

The controller creates a top-level Codex thread and isolated Git worktree for each mutable task attempt. Native Codex child subagents inherit the parent's workspace and are used primarily for read-only exploration, analysis, and review.

## Rationale

Parallel source mutation is safe only with explicit Git/path custody. Worktrees share the repository object database while providing separate working directories and branches. Native child agents do not establish independent filesystem isolation.

## Consequences

- path leases and serial paths are controller-enforced;
- agents do not manage branches, rebases, pushes, or PRs;
- failed worktrees are preserved until reconciled;
- integration is a separate serial worktree.
