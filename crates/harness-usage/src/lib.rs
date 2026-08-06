//! Integer-only API-equivalent cost accounting.

use harness_domain::{
    CostConfidence, CostEstimate, DomainError, PricingSnapshot, TokenUsage, UsageSummary,
};
use thiserror::Error;

const TOKEN_DENOMINATOR: u128 = 1_000_000;

pub fn estimate(usage: &TokenUsage, pricing: &PricingSnapshot) -> Result<CostEstimate, UsageError> {
    usage.validate()?;

    let input_multiplier = if pricing
        .long_context_threshold_tokens
        .is_some_and(|threshold| usage.input_tokens > threshold)
    {
        ratio(
            pricing.long_context_input_multiplier_numerator,
            pricing.long_context_input_multiplier_denominator,
        )
    } else {
        (1, 1)
    };
    let output_multiplier = if pricing
        .long_context_threshold_tokens
        .is_some_and(|threshold| usage.input_tokens > threshold)
    {
        ratio(
            pricing.long_context_output_multiplier_numerator,
            pricing.long_context_output_multiplier_denominator,
        )
    } else {
        (1, 1)
    };

    let input_rate = apply_ratio(
        u128::from(pricing.input_microusd_per_million),
        input_multiplier,
    );
    let output_rate = apply_ratio(
        u128::from(pricing.output_microusd_per_million),
        output_multiplier,
    );
    let cache_write_rate = apply_ratio(
        input_rate,
        (
            u128::from(pricing.cache_write_multiplier_numerator),
            u128::from(pricing.cache_write_multiplier_denominator.max(1)),
        ),
    );
    let cached_rate = u128::from(pricing.cached_input_microusd_per_million);

    let cached = u128::from(usage.cached_input_tokens);
    let non_cached = u128::from(usage.input_tokens - usage.cached_input_tokens);
    let output = u128::from(usage.output_tokens);

    let fixed = token_cost(cached, cached_rate) + token_cost(output, output_rate);
    let (lower, upper, confidence, explanation) = match usage.cache_write_input_tokens {
        Some(cache_write) => {
            let cache_write = u128::from(cache_write);
            if cache_write > non_cached {
                return Err(UsageError::ImpossibleCounters(
                    "cached plus cache-write input exceeds total input".to_owned(),
                ));
            }
            let normal = non_cached - cache_write;
            let total =
                fixed + token_cost(normal, input_rate) + token_cost(cache_write, cache_write_rate);
            (
                total,
                total,
                CostConfidence::Exact,
                "Per-turn observed usage; reasoning output is included in output and not charged twice."
                    .to_owned(),
            )
        }
        None => {
            let all_normal = fixed + token_cost(non_cached, input_rate);
            let all_cache_write = fixed + token_cost(non_cached, cache_write_rate);
            (
                all_normal.min(all_cache_write),
                all_normal.max(all_cache_write),
                CostConfidence::Bounded,
                "Cache-write input was not reported; the range spans zero through all non-cached input. Reasoning output is not charged twice."
                    .to_owned(),
            )
        }
    };

    Ok(CostEstimate {
        lower_microusd: to_u64(lower)?,
        upper_microusd: to_u64(upper)?,
        confidence,
        pricing_snapshot_ids: vec![pricing.id.clone()],
        explanation,
    })
}

#[must_use]
pub fn format_usd(microusd: u64) -> String {
    let dollars = microusd / 1_000_000;
    let cents = (microusd % 1_000_000) / 10_000;
    format!("${dollars}.{cents:02}")
}

pub fn add_sample(
    summary: &mut UsageSummary,
    usage: &TokenUsage,
    cost: &CostEstimate,
) -> Result<(), UsageError> {
    usage.validate()?;
    let was_empty = summary.total_tokens == 0
        && summary.cost.lower_microusd == 0
        && summary.cost.upper_microusd == 0
        && summary.cost.pricing_snapshot_ids.is_empty();
    summary.input_tokens = summary.input_tokens.saturating_add(usage.input_tokens);
    summary.cached_input_tokens = summary
        .cached_input_tokens
        .saturating_add(usage.cached_input_tokens);
    summary.cache_write_input_tokens = match (
        summary.cache_write_input_tokens,
        usage.cache_write_input_tokens,
    ) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (None, Some(right)) if summary.total_tokens == 0 => Some(right),
        _ => None,
    };
    summary.output_tokens = summary.output_tokens.saturating_add(usage.output_tokens);
    summary.reasoning_output_tokens = summary
        .reasoning_output_tokens
        .saturating_add(usage.reasoning_output_tokens);
    summary.total_tokens = summary.total_tokens.saturating_add(usage.total_tokens);
    summary.cost.lower_microusd = summary
        .cost
        .lower_microusd
        .saturating_add(cost.lower_microusd);
    summary.cost.upper_microusd = summary
        .cost
        .upper_microusd
        .saturating_add(cost.upper_microusd);
    summary.cost.confidence = if was_empty {
        cost.confidence
    } else {
        aggregate_confidence(summary.cost.confidence, cost.confidence)
    };
    for id in &cost.pricing_snapshot_ids {
        if !summary.cost.pricing_snapshot_ids.contains(id) {
            summary.cost.pricing_snapshot_ids.push(id.clone());
        }
    }
    Ok(())
}

fn aggregate_confidence(left: CostConfidence, right: CostConfidence) -> CostConfidence {
    match (left, right) {
        (CostConfidence::Unknown, _) | (_, CostConfidence::Unknown) => CostConfidence::Unknown,
        (CostConfidence::Bounded, _) | (_, CostConfidence::Bounded) => CostConfidence::Bounded,
        _ => CostConfidence::Exact,
    }
}

fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> (u128, u128) {
    (
        u128::from(numerator.unwrap_or(1)),
        u128::from(denominator.unwrap_or(1).max(1)),
    )
}

fn apply_ratio(value: u128, (numerator, denominator): (u128, u128)) -> u128 {
    value.saturating_mul(numerator) / denominator.max(1)
}

fn token_cost(tokens: u128, rate_microusd_per_million: u128) -> u128 {
    // Round to the nearest micro-dollar for each independently priced component.
    tokens
        .saturating_mul(rate_microusd_per_million)
        .saturating_add(TOKEN_DENOMINATOR / 2)
        / TOKEN_DENOMINATOR
}

fn to_u64(value: u128) -> Result<u64, UsageError> {
    u64::try_from(value).map_err(|_| UsageError::Overflow)
}

#[derive(Debug, Error)]
pub enum UsageError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("impossible usage counters: {0}")]
    ImpossibleCounters(String),
    #[error("cost arithmetic overflow")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pricing() -> PricingSnapshot {
        PricingSnapshot {
            id: "test".to_owned(),
            model: "model".to_owned(),
            effective_at: "2026-01-01T00:00:00Z".to_owned(),
            input_microusd_per_million: 5_000_000,
            cached_input_microusd_per_million: 500_000,
            output_microusd_per_million: 30_000_000,
            cache_write_multiplier_numerator: 5,
            cache_write_multiplier_denominator: 4,
            long_context_threshold_tokens: Some(100),
            long_context_input_multiplier_numerator: Some(2),
            long_context_input_multiplier_denominator: Some(1),
            long_context_output_multiplier_numerator: Some(3),
            long_context_output_multiplier_denominator: Some(2),
        }
    }

    #[test]
    fn reasoning_is_not_double_counted() {
        let with_reasoning = TokenUsage {
            input_tokens: 10,
            cached_input_tokens: 0,
            cache_write_input_tokens: Some(0),
            output_tokens: 10,
            reasoning_output_tokens: 8,
            total_tokens: 20,
            model_context_window: None,
        };
        let without_reasoning = TokenUsage {
            reasoning_output_tokens: 0,
            ..with_reasoning.clone()
        };
        assert_eq!(
            estimate(&with_reasoning, &pricing())
                .unwrap()
                .lower_microusd,
            estimate(&without_reasoning, &pricing())
                .unwrap()
                .lower_microusd
        );
    }

    #[test]
    fn missing_cache_write_produces_range() {
        let usage = TokenUsage {
            input_tokens: 1_000_000,
            cached_input_tokens: 100_000,
            cache_write_input_tokens: None,
            output_tokens: 0,
            reasoning_output_tokens: 0,
            total_tokens: 1_000_000,
            model_context_window: None,
        };
        let cost = estimate(&usage, &pricing()).unwrap();
        assert_eq!(cost.confidence, CostConfidence::Bounded);
        assert!(cost.upper_microusd > cost.lower_microusd);
    }

    #[test]
    fn long_context_boundary_is_strictly_greater() {
        let usage_at = TokenUsage {
            input_tokens: 100,
            cache_write_input_tokens: Some(0),
            ..TokenUsage::default()
        };
        let usage_over = TokenUsage {
            input_tokens: 101,
            cache_write_input_tokens: Some(0),
            ..TokenUsage::default()
        };
        let at = estimate(&usage_at, &pricing()).unwrap().lower_microusd;
        let over = estimate(&usage_over, &pricing()).unwrap().lower_microusd;
        assert!(over >= at.saturating_mul(2));
    }
}
