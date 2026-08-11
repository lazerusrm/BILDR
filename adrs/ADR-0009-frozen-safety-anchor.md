# ADR-0009: Keep a frozen safety anchor outside optimization

- **Status:** accepted architecture
- **Date:** 2026-08-11

## Context

An optimizer rewarded for success, speed, or cost can discover that safety,
custody, evidence, or approval checks are obstacles. Treating those checks as
weighted objectives allows a candidate to trade them away.

## Decision

Define a digest-bound frozen safety anchor. It protects:

- controller authority and exact-head custody;
- worktree/path leases;
- sandbox, network, credential, and redaction policy;
- approval and external-write gates;
- result and evidence semantics;
- evaluator, holdout, promotion, and rollback isolation;
- prohibition on automatic merge and live controller replacement.

Candidate schemas cannot express protected-dimension edits. Any change to the
anchor is a normal reviewed repository change and invalidates prior promotion
evidence as defined by compatibility policy.

## Consequences

The improvement action space is smaller and safer. The system cannot claim a
performance improvement by weakening its own referee.

## Rejected alternatives

- mark safety settings “high risk” but still editable;
- allow emergency optimizer overrides;
- let a meta-evolver rewrite the outer anchor.
