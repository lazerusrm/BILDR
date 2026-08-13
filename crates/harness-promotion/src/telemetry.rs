use crate::{ContractError, Receipt, ReceiptKind, digest};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Quality,
    Correction,
    Regression,
    Cost,
    Latency,
    Distribution,
    Grader,
    Taskset,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Degraded,
    Unknown,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricTelemetry {
    pub metric: Metric,
    pub present: bool,
    pub within_threshold: bool,
    pub source_receipt: Receipt,
    pub observation_start_ms: i64,
    pub observation_end_ms: i64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthBatch {
    pub threshold_policy_digest: String,
    pub observation_start_ms: i64,
    pub observation_end_ms: i64,
    pub metrics: Vec<MetricTelemetry>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionHealth {
    pub state: HealthState,
    pub missing: Vec<Metric>,
    pub breached: Vec<Metric>,
}
const REQUIRED: [Metric; 8] = [
    Metric::Quality,
    Metric::Correction,
    Metric::Regression,
    Metric::Cost,
    Metric::Latency,
    Metric::Distribution,
    Metric::Grader,
    Metric::Taskset,
];
pub fn validate_health_telemetry(batch: &HealthBatch) -> Result<(), ContractError> {
    let metrics = batch
        .metrics
        .iter()
        .map(|value| value.metric)
        .collect::<BTreeSet<_>>();
    (digest(&batch.threshold_policy_digest)
        && batch.observation_start_ms <= batch.observation_end_ms
        && batch.metrics.len() == REQUIRED.len()
        && metrics == REQUIRED.into_iter().collect()
        && batch.metrics.iter().all(|value| {
            value.observation_start_ms == batch.observation_start_ms
                && value.observation_end_ms == batch.observation_end_ms
                && value.source_receipt.valid_as(ReceiptKind::Telemetry)
        }))
    .then_some(())
    .ok_or(ContractError::Missing)
}
pub fn promotion_health(batch: &HealthBatch) -> PromotionHealth {
    let supplied = batch
        .metrics
        .iter()
        .map(|value| value.metric)
        .collect::<BTreeSet<_>>();
    let mut missing = REQUIRED
        .into_iter()
        .filter(|metric| !supplied.contains(metric))
        .collect::<Vec<_>>();
    missing.extend(
        batch
            .metrics
            .iter()
            .filter(|value| !value.present)
            .map(|value| value.metric),
    );
    missing.sort();
    missing.dedup();
    let breached: Vec<Metric> = batch
        .metrics
        .iter()
        .filter(|value| value.present && !value.within_threshold)
        .map(|value| value.metric)
        .collect();
    let state = if validate_health_telemetry(batch).is_err() || !missing.is_empty() {
        HealthState::Unknown
    } else if breached.is_empty() {
        HealthState::Healthy
    } else {
        HealthState::Degraded
    };
    PromotionHealth {
        state,
        missing,
        breached,
    }
}
