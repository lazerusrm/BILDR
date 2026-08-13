use crate::{ContractError, Receipt, ReceiptKind, digest, digest_without_self, id};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Offline,
    Holdout,
    Shadow,
    Canary,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageState {
    Pending,
    Running,
    Passed,
    Failed,
    Inconclusive,
    InfrastructureUnavailable,
    Invalidated,
    Stopped,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentState {
    Proposed,
    OfflineRunning,
    HoldoutRunning,
    ShadowRunning,
    CanaryRunning,
    PromotionReview,
    Failed,
    Inconclusive,
    Invalidated,
    Stopped,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageReceipt {
    pub stage: Stage,
    pub state: StageState,
    pub evidence: Option<StageEvidence>,
    pub sample_count: u64,
    pub successful_pairs: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageEvidence {
    pub stage: Stage,
    pub id: String,
    pub digest: String,
}
impl StageReceipt {
    pub fn pending(stage: Stage) -> Self {
        Self {
            stage,
            state: StageState::Pending,
            evidence: None,
            sample_count: 0,
            successful_pairs: 0,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateReceipt {
    pub gate_id: String,
    pub passed: bool,
    pub evidence: Receipt,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentV1 {
    pub schema: String,
    pub experiment_id: String,
    pub candidate_id: String,
    pub candidate_receipt: Receipt,
    pub champion_bundle_id: String,
    pub champion_bundle_receipt: Receipt,
    pub challenger_bundle_id: String,
    pub challenger_bundle_receipt: Receipt,
    pub runtime_policy_digest: String,
    pub stages: Vec<StageReceipt>,
    pub hard_gates: Vec<GateReceipt>,
    pub state: ExperimentState,
    pub sha256: String,
}
pub fn verify_experiment(value: &ExperimentV1) -> Result<(), ContractError> {
    let stages = [Stage::Offline, Stage::Holdout, Stage::Shadow, Stage::Canary];
    (value.schema == "harness.experiment.v1"
        && [
            value.experiment_id.as_str(),
            value.candidate_id.as_str(),
            value.champion_bundle_id.as_str(),
            value.challenger_bundle_id.as_str(),
        ]
        .iter()
        .all(|v| id(v))
        && value.champion_bundle_id != value.challenger_bundle_id
        && value.candidate_receipt.valid_as(ReceiptKind::Candidate)
        && value.candidate_receipt.id == value.candidate_id
        && value
            .champion_bundle_receipt
            .valid_as(ReceiptKind::ChampionBundle)
        && value.champion_bundle_receipt.id == value.champion_bundle_id
        && value
            .challenger_bundle_receipt
            .valid_as(ReceiptKind::ChallengerBundle)
        && value.challenger_bundle_receipt.id == value.challenger_bundle_id
        && digest(&value.runtime_policy_digest)
        && value.stages.iter().map(|v| v.stage).eq(stages)
        && value.stages.iter().all(|v| {
            v.successful_pairs <= v.sample_count
                && v.evidence.as_ref().is_none_or(|evidence| {
                    evidence.stage == v.stage && id(&evidence.id) && digest(&evidence.digest)
                })
                && (!matches!(v.state, StageState::Passed) || v.evidence.is_some())
        })
        && coherent_state(value)
        && value
            .hard_gates
            .iter()
            .all(|v| id(&v.gate_id) && v.evidence.valid_as(ReceiptKind::HardGate))
        && !value.hard_gates.is_empty()
        && digest_without_self(value)? == value.sha256)
        .then_some(())
        .ok_or(ContractError::Digest)
}
fn coherent_state(value: &ExperimentV1) -> bool {
    use ExperimentState as E;
    use StageState as S;
    let states = value
        .stages
        .iter()
        .map(|stage| stage.state)
        .collect::<Vec<_>>();
    match value.state {
        E::Proposed => states == [S::Pending; 4],
        E::OfflineRunning => states == [S::Running, S::Pending, S::Pending, S::Pending],
        E::HoldoutRunning => states == [S::Passed, S::Running, S::Pending, S::Pending],
        E::ShadowRunning => states == [S::Passed, S::Passed, S::Running, S::Pending],
        E::CanaryRunning => states == [S::Passed, S::Passed, S::Passed, S::Running],
        E::PromotionReview => states == [S::Passed; 4],
        E::Failed => states.contains(&S::Failed),
        E::Inconclusive => {
            states.contains(&S::Inconclusive) || states.contains(&S::InfrastructureUnavailable)
        }
        E::Invalidated => states.contains(&S::Invalidated),
        E::Stopped => states.contains(&S::Stopped),
    }
}
pub fn advance_stage(value: &ExperimentV1, stage: Stage) -> Result<ExperimentV1, ContractError> {
    verify_experiment(value)?;
    if !value.hard_gates.iter().all(|g| g.passed) {
        return Err(ContractError::HardGate);
    }
    let expected = match stage {
        Stage::Offline => ExperimentState::Proposed,
        Stage::Holdout => ExperimentState::OfflineRunning,
        Stage::Shadow => ExperimentState::HoldoutRunning,
        Stage::Canary => ExperimentState::ShadowRunning,
    };
    if value.state != expected {
        return Err(ContractError::StageOrder);
    }
    let index = match stage {
        Stage::Offline => 0,
        Stage::Holdout => 1,
        Stage::Shadow => 2,
        Stage::Canary => 3,
    };
    if value.stages[index].state != StageState::Pending
        || (index > 0 && value.stages[index - 1].state != StageState::Passed)
    {
        return Err(ContractError::StageOrder);
    }
    let mut next = value.clone();
    next.stages[index].state = StageState::Running;
    next.state = match stage {
        Stage::Offline => ExperimentState::OfflineRunning,
        Stage::Holdout => ExperimentState::HoldoutRunning,
        Stage::Shadow => ExperimentState::ShadowRunning,
        Stage::Canary => ExperimentState::CanaryRunning,
    };
    next.sha256 = digest_without_self(&next)?;
    Ok(next)
}
pub fn assign_cohort(experiment_id: &str, subject_id: &str, basis_points: u16) -> bool {
    basis_points <= 10_000
        && u16::from_be_bytes(
            Sha256::digest(format!("{experiment_id}\0{subject_id}").as_bytes())[..2]
                .try_into()
                .expect("two bytes"),
        ) % 10_000
            < basis_points
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionResult {
    Passed,
    Failed,
    Unavailable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChallengerResult {
    Passed,
    Failed,
    Unavailable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    CriticalRegression,
    SampleBudget,
    CostBudget,
    Emergency,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExposureBudget {
    pub max_samples: u64,
    pub max_cost_microusd: u64,
    pub critical_failures: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowObservation {
    pub production: ProductionResult,
    pub challenger: ChallengerResult,
    pub cost_microusd: u64,
    pub critical: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFamily {
    DevelopmentEval,
    ShadowReplay,
    CanaryTraffic,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditDimension {
    Prompt,
    ToolPolicy,
    RetrievalPolicy,
    ModelRoute,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowAuthority {
    pub isolation_custody_receipt: Receipt,
    pub task_family: TaskFamily,
    pub edit_dimensions: Vec<EditDimension>,
    pub assignment_receipt: Receipt,
    pub fallback_receipt: Receipt,
}
impl ShadowAuthority {
    fn valid(&self) -> bool {
        !self.edit_dimensions.is_empty()
            && self
                .edit_dimensions
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.edit_dimensions.len()
            && self
                .isolation_custody_receipt
                .valid_as(ReceiptKind::IsolationCustody)
            && self.assignment_receipt.valid_as(ReceiptKind::Assignment)
            && self.fallback_receipt.valid_as(ReceiptKind::Fallback)
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryAuthority {
    pub shadow: ShadowAuthority,
    pub operator_start_receipt: Receipt,
}
impl CanaryAuthority {
    fn valid(&self) -> bool {
        self.shadow.valid()
            && self
                .operator_start_receipt
                .valid_as(ReceiptKind::OperatorStart)
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowState {
    pub budget: ExposureBudget,
    pub observed: u64,
    pub spent_microusd: u64,
    pub critical_failures: u64,
    pub production: ProductionResult,
    pub fallback_used: bool,
    pub stop: Option<StopReason>,
}
impl ShadowState {
    pub fn new(budget: ExposureBudget) -> Self {
        Self {
            budget,
            observed: 0,
            spent_microusd: 0,
            critical_failures: 0,
            production: ProductionResult::Unavailable,
            fallback_used: false,
            stop: None,
        }
    }
}
/// Canary uses the same bounded, fallback-preserving state contract as shadow.
pub type CanaryState = ShadowState;
pub fn observe_shadow(
    authority: &ShadowAuthority,
    mut state: ShadowState,
    observation: ShadowObservation,
) -> Result<ShadowState, ContractError> {
    if !authority.valid() {
        return Err(ContractError::Missing);
    }
    if state.stop.is_some() {
        return Err(ContractError::Stopped);
    }
    if state.observed >= state.budget.max_samples
        || state
            .spent_microusd
            .saturating_add(observation.cost_microusd)
            > state.budget.max_cost_microusd
    {
        state.stop = Some(if state.observed >= state.budget.max_samples {
            StopReason::SampleBudget
        } else {
            StopReason::CostBudget
        });
        return Ok(state);
    }
    state.observed += 1;
    state.spent_microusd += observation.cost_microusd;
    state.production = observation.production;
    state.fallback_used = observation.challenger != ChallengerResult::Passed;
    if observation.critical && observation.challenger == ChallengerResult::Failed {
        state.critical_failures += 1;
        if state.critical_failures >= state.budget.critical_failures {
            state.stop = Some(StopReason::CriticalRegression);
        }
    }
    Ok(state)
}

pub fn observe_canary(
    authority: &CanaryAuthority,
    state: CanaryState,
    observation: ShadowObservation,
) -> Result<CanaryState, ContractError> {
    if !authority.valid() {
        return Err(ContractError::Missing);
    }
    observe_shadow(&authority.shadow, state, observation)
}
