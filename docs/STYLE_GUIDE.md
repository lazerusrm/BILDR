# Documentation style

Write documentation for the person who must use or maintain the software.
Follow the [Google developer documentation style guide](https://developers.google.com/style)
unless a product contract requires a stricter form.

## Core rules

- Start with the outcome, requirement, or user task.
- Use active voice, present tense, and direct language.
- Address the reader as "you" when giving instructions.
- Use sentence case for headings.
- Keep sentences and paragraphs focused on one idea.
- Define unfamiliar terms before using abbreviations.
- Use the same term for the same concept throughout the repository.
- Use parallel grammar in lists and tables.
- Separate requirements from background and rationale.
- Use inclusive, accessible language. Do not use idioms when literal language is
  clearer.

## Procedures

Use a numbered list when order matters. Begin each step with an imperative verb.
Put prerequisites before the procedure and verification after it. State expected
results so readers can tell whether a step succeeded.

## Code and contracts

Name the authoritative path, command, field, or state directly. Describe
observable behavior before implementation details. Use examples that represent
the current supported shape; do not preserve obsolete shapes as normative
examples.

Document proof honestly. Distinguish a command that ran from a behavior that the
command demonstrated. Never describe missing, skipped, stale, or inconclusive
evidence as success.
