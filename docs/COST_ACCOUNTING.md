# Token and API-Equivalent Cost Accounting

**Status:** implementation contract
**Snapshot basis:** model prices are immutable, effective-dated configuration; historical runs never recalculate against a newer price table

## 1. Terminology

- **Observed usage:** token fields reported by the pinned Codex App Server.
- **API-equivalent estimate:** what the observed token mix would cost under the selected OpenAI API price snapshot.
- **Actual billed cost:** only available when an external billing source provides it. ChatGPT/Codex subscription execution must not be labeled as an API invoice.
- **Confidence:** `exact`, `bounded`, or `unknown` based on available token components and price rules.

## 2. Required storage

Every token sample stores:

```text
thread_id
turn_id
observed_at
requested_model
effective_model
input_tokens
cached_input_tokens
cache_write_input_tokens nullable
output_tokens
reasoning_output_tokens
reported_total_tokens
model_context_window nullable
sample_kind turn_total | derived_delta
source_event_id
```

The cost entry stores the exact pricing snapshot ID and formula explanation used at the time.

## 3. Delta selection

Preferred order:

1. each distinct App Server `tokenUsage.last` model call, deduplicated by the
   monotonic `tokenUsage.total` counter;
2. the sum of those calls as the durable per-turn `turn_total` sample;
3. a monotonic delta of cumulative thread usage when call-level detail is not
   available;
4. a bounded/unknown estimate when cumulative counters reset or decrease.

A Codex turn may contain many model calls between tools, commands, and native
subagent waits. The ledger must retain all of them; a cumulative sample is
never directly billed more than once.

## 4. Base formula

Let:

```text
I = input_tokens
C = cached_input_tokens
W = cache_write_input_tokens
O = output_tokens
R = reasoning_output_tokens
P_i = input price per token
P_c = cached-input price per token
P_o = output price per token
M_w = cache-write input multiplier
```

Reasoning output is part of output and is not added a second time.

When `W` is known:

```text
N = max(I - C - W, 0)
base_cost = N*P_i + C*P_c + W*(P_i*M_w) + O*P_o
```

When `W` is missing:

```text
lower_bound: assume W = 0
upper_bound: assume all non-cached input may be cache-write input
```

The UI shows a range and `bounded` confidence. Configuration may choose a different documented bound only through a new versioned accounting policy.

## 5. Long-context adjustment

Apply the model snapshot's long-context rule per request/turn, not to the entire run aggregate. For a threshold `T`:

```text
if request_input_tokens > T:
    non-cached and cache-write input price *= long_context_input_multiplier
    output price *= long_context_output_multiplier
```

Cached-input treatment follows the model's published rule in the active snapshot. The GPT-5.6 long-context input multiplier applies to cached input as well as uncached and cache-write input. If the source does not specify a component unambiguously, mark the estimate bounded and retain the assumption in `explanation`.

## 6. Model reroutes

Cost uses the effective model for the relevant turn. The UI displays:

```text
requested: gpt-5.6-luna / max
actual:    gpt-5.6-terra / xhigh
reason:    model reroute event
```

If a turn contains usage across more than one effective model and the runtime does not split it, mark cost `unknown` rather than assigning all usage to an arbitrary model.

## 7. Current configured snapshots

The example configuration contains effective-dated 2026-08-05 snapshots:

| Model | Input / 1M | Cached input / 1M | Output / 1M |
|---|---:|---:|---:|
| `gpt-5.6-sol` | $5.00 | $0.50 | $30.00 |
| `gpt-5.6-terra` | $2.00 | $0.20 | $12.00 |
| `gpt-5.6-luna` | $0.20 | $0.02 | $1.20 |

The supplied example also records a 1.25× cache-write input multiplier and the model-specific long-context threshold/multipliers. These are configuration, not hard-coded constants.

## 8. Budget enforcement

Budgets exist at four levels:

- turn goal;
- task attempt;
- run phase;
- whole run.

Budget states:

```text
normal < 70%
warning 70–89%
critical 90–99%
exhausted >= 100%
```

At `critical`, the controller instructs the agent to converge, run required proof, or return a blocker/handoff. At `exhausted`, new turns stop unless the user approves an increase or the controller creates a documented escalation attempt. An already-running command may finish within its command timeout.

Budget exhaustion must not authorize weakened tests, narrower unapproved scope, compatibility behavior, or false completion.

## 9. Cost presentation

Always show:

- total and per-model input/cached/cache-write/output/reasoning tokens;
- lower and upper dollar estimate where needed;
- pricing snapshot date/ID;
- requested/effective model and effort;
- task, parent/subagent, and phase attribution;
- context-window warning;
- subscription/API-equivalent label.

Do not show an apparently exact two-decimal value when the estimate is bounded. Example: `$1.82–$2.09 API equivalent`.

## 10. Validation tests

Property/unit tests must cover:

- reasoning output not double-counted;
- cached input never charged at full input rate;
- cache-write lower/upper bounds;
- long-context threshold boundary at `T` and `T+1`;
- cumulative delta reset;
- model reroute;
- null/missing components;
- negative or impossible runtime counters rejected;
- historical snapshot immutability;
- aggregate cost equals sum of accepted per-turn entries within decimal tolerance.

Use integer micro-dollars or a decimal type internally; do not use binary floating point for the authoritative ledger.
