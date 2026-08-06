# ADR-0006: Display execution reasoning summaries, not private hidden chain-of-thought

**Status:** proposed

## Decision

The UI displays requested/effective model, reasoning effort, current goal, plan steps, concise reasoning summaries, commands, file/tool activity, subagent lifecycle, findings, and evidence. Raw reasoning is not retained by default and the product never promises private chain-of-thought visibility.

## Rationale

These surfaces are sufficient to understand and steer engineering work while avoiding misleading claims, sensitive internal reasoning retention, and unnecessary storage risk.

## Consequences

- the activity timeline focuses on actions and concise summaries;
- context packets and tool traces provide quality diagnostics;
- a debug protocol view may show event metadata while honoring the raw-reasoning policy.
