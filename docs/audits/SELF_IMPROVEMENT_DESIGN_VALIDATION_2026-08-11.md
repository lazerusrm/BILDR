# BILDR self-improvement design validation — 2026-08-11

**Scope:** documentation, ADRs, JSON Schemas, and example records introduced by the governed self-improvement design

## Checks performed

- all seven JSON Schema documents parse;
- each schema passes Draft 2020-12 meta-schema validation;
- schema discriminator values are unique;
- all seven example records validate against the schema named by their
  `schema` discriminator, including format checks;
- every local Markdown link in the added files resolves;
- all added text files end with a newline;
- no added line has trailing whitespace.

## Repository checks expected on the published head

The pull-request workflow should run:

- contribution metadata policy;
- repository file policy;
- browser typecheck, tests, build, and end-to-end tests;
- Rust format, lint, and workspace tests;
- `cargo xtask schema-check`;
- OpenAPI and protocol-binding checks.

## Proof limits

This change does not implement the runtime subsystem, migrations, APIs, UI, or
promotion service. It enables no self-modification, shadowing, canary,
promotion, external training, or code evolution.

The existing `cargo xtask schema-check` parses JSON but does not yet validate
schemas or examples against one another. The stronger validation described
above was performed independently for this design package; SI-003 makes that
behavior a repository-owned gate.
