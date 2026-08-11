## M6 — Extend

### SI-023 — Recurring quality gardener

**Depends on:** SI-007, SI-013, SI-017

- encode reviewed golden principles;
- scan drift on a schedule;
- propose small draft repository changes;
- route through normal validation and review;
- measure downstream impact;
- never merge automatically.

### SI-024 — Provider-neutral eval and trace export

**Depends on:** SI-005, SI-008–SI-012

- taskset/harness/runtime/grader manifest;
- redaction, license, and privacy receipts;
- branch-aware trace export;
- revocation and retention;
- import result as external experiment evidence.

### SI-025 — Prime Verifiers and OpenAI Evals adapters

**Depends on:** SI-024

- Prime-compatible taskset/harness/runtime adapter;
- OpenAI Evals dataset and grader adapter;
- exact external config/digest;
- no external provider becomes local outcome authority.

### SI-026 — Optional model or adapter training

**Depends on:** SI-017, SI-024, SI-025

- training dataset approval;
- exclude holdout;
- model/adapter manifest and provenance;
- return trained artifact as an ordinary candidate;
- full local evaluation and promotion gates.

### SI-027 — Code and meta-evolution research mode

**Depends on:** all prior safety and evaluation milestones

- controller-code candidate only as a draft repository change;
- separate branch and full CI;
- no live binary replacement;
- meta-evolver may propose optimizer changes only under the frozen anchor;
- stronger human and independent review;
- default disabled and experimental.

## Pilot ladder

1. **Historical scoring:** project completed runs and label outcomes; no candidate.
2. **Context-order candidate:** compare context selection on a small development
   suite; suggestion only.
3. **Budget-route candidate:** test bounded token/effort routing; suggestion only.
4. **Prompt or skill candidate:** offline plus holdout; human promotion only.
5. **Validator-selection candidate:** must preserve proof floors.
6. **Shadow candidate:** no production effect.
7. **Local canary:** allowlisted task family with fallback.
8. **External adapter:** trained artifact re-enters as a candidate.
9. **Repository-code candidate:** draft pull request only.

## Program-level acceptance

The program is ready for guarded promotion only when:

- the baseline branch and hosted CI are green;
- trace replay and outcome revision are tested across crashes;
- schema and example conformance is enforced;
- the first eval suite contains real failures and negative controls;
- holdout access is independently tested;
- reward-hacking fixtures are caught;
- paired statistics refuse noisy or undersized results;
- shadow cannot affect production;
- canary fallback and rollback are exercised;
- the frozen anchor is mechanically enforced;
- all evidence is exportable by exact digest;
- an operator can disable the subsystem without stopping ordinary BILDR.
