# ADR-0001: Integrate Codex through App Server

**Status:** proposed

## Decision

`harnessd` supervises one version-pinned `codex app-server` process over JSONL stdio and consumes the generated protocol schema. It does not scrape Codex terminal output and does not make the high-level SDK the primary GUI backend.

## Rationale

The supervisory product needs live thread, turn, item, plan, diff, command, approval, goal, usage, model-reroute, review, and subagent lifecycle events. App Server exposes that control surface and supports generated schemas.

## Consequences

- exact Codex version/schema become a release compatibility tuple;
- raw events are retained for replay and forward-compatible debugging;
- execution disables on incompatible schema instead of falling back to scraping;
- the adapter requires a golden trace suite per supported version.
