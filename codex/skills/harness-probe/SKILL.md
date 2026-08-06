---
name: harness-probe
description: Use Harness Console's bounded local repository probe to batch search, read-many, workspace mapping, test selection, and log summarization without flooding model context.
---

# Harness Probe

Use `harness-probe` before making many repetitive `rg`, `cat`, or metadata calls.
It is read-only and stores full outputs as Harness artifacts while returning a
bounded JSON or text summary.

Examples:

```bash
harness-probe search --query 'EventMediaIdentity|camera_uid' \
  --paths central shared docs --max-results 200 --format json

harness-probe read-many --manifest /tmp/paths.json \
  --max-total-bytes 300000 --format json

harness-probe cargo-map --affected central/rust-c2/src/event_media.rs --format json

harness-probe test-select --task-packet "$HARNESS_TASK_PACKET" --format json

harness-probe summarize-log --artifact "$HARNESS_ARTIFACT_ID" \
  --focus 'first root source failure, not cascade errors' --format json
```

Rules:

- stay inside the current repository/worktree;
- honor output and path limits;
- use the returned artifact ID when a later agent/reviewer needs full evidence;
- do not dump secrets, environment, vendor trees, binaries, or archives into context;
- if the helper reports a policy or path denial, stop rather than bypass it.
