use crate::{ContractError, digest, digest_without_self, id, verify_experiment};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRole {
    IndependentReviewer,
    Operator,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptKind {
    Offline,
    Holdout,
    Shadow,
    Canary,
    RewardIntegrity,
    Statistics,
    IndependentReview,
    OperatorApproval,
    Promotion,
    Rollback,
    HardGate,
    IsolationCustody,
    Assignment,
    Fallback,
    OperatorStart,
    Telemetry,
    Candidate,
    ChampionBundle,
    ChallengerBundle,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub kind: ReceiptKind,
    pub id: String,
    pub digest: String,
}
impl Receipt {
    pub fn valid_as(&self, kind: ReceiptKind) -> bool {
        self.kind == kind && id(&self.id) && digest(&self.digest)
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalReceipt {
    pub role: ApprovalRole,
    pub receipt: Receipt,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredReceipts {
    pub offline: Receipt,
    pub holdout: Receipt,
    pub shadow: Receipt,
    pub canary: Receipt,
    pub reward_integrity: Receipt,
    pub statistics: Receipt,
}
impl RequiredReceipts {
    pub fn all(prefix: &str) -> Self {
        let receipt = |kind, suffix| Receipt {
            kind,
            id: format!("{prefix}-{suffix}"),
            digest: "a".repeat(64),
        };
        Self {
            offline: receipt(ReceiptKind::Offline, "offline"),
            holdout: receipt(ReceiptKind::Holdout, "holdout"),
            shadow: receipt(ReceiptKind::Shadow, "shadow"),
            canary: receipt(ReceiptKind::Canary, "canary"),
            reward_integrity: receipt(ReceiptKind::RewardIntegrity, "reward"),
            statistics: receipt(ReceiptKind::Statistics, "statistics"),
        }
    }
    fn valid(&self) -> bool {
        [
            (&self.offline, ReceiptKind::Offline),
            (&self.holdout, ReceiptKind::Holdout),
            (&self.shadow, ReceiptKind::Shadow),
            (&self.canary, ReceiptKind::Canary),
            (&self.reward_integrity, ReceiptKind::RewardIntegrity),
            (&self.statistics, ReceiptKind::Statistics),
        ]
        .iter()
        .all(|(receipt, kind)| receipt.valid_as(*kind))
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionDecisionV1 {
    pub schema: String,
    pub promotion_id: String,
    pub experiment_id: String,
    pub candidate_id: String,
    pub experiment_digest: String,
    pub runtime_policy_digest: String,
    pub from_bundle_id: String,
    pub to_bundle_id: String,
    pub safety_anchor_digest: String,
    pub required_receipts: RequiredReceipts,
    pub approvals: Vec<ApprovalReceipt>,
    pub rollback_evidence: Receipt,
    pub sha256: String,
}
impl PromotionDecisionV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn approved(
        promotion_id: &str,
        experiment_id: &str,
        candidate_id: &str,
        from: &str,
        to: &str,
        anchor: &str,
        required_receipts: RequiredReceipts,
        rollback: Receipt,
    ) -> Self {
        let approval = |role, kind, suffix| ApprovalReceipt {
            role,
            receipt: Receipt {
                kind,
                id: format!("{promotion_id}-{suffix}"),
                digest: "a".repeat(64),
            },
        };
        let mut value = Self {
            schema: "harness.promotion-decision.v1".into(),
            promotion_id: promotion_id.into(),
            experiment_id: experiment_id.into(),
            candidate_id: candidate_id.into(),
            experiment_digest: "a".repeat(64),
            runtime_policy_digest: "a".repeat(64),
            from_bundle_id: from.into(),
            to_bundle_id: to.into(),
            safety_anchor_digest: anchor.into(),
            required_receipts,
            approvals: vec![
                approval(
                    ApprovalRole::IndependentReviewer,
                    ReceiptKind::IndependentReview,
                    "review",
                ),
                approval(
                    ApprovalRole::Operator,
                    ReceiptKind::OperatorApproval,
                    "operator",
                ),
            ],
            rollback_evidence: rollback,
            sha256: String::new(),
        };
        value.sha256 = digest_without_self(&value).expect("serializable promotion contract");
        value
    }
}
pub fn verify_promotion(value: &PromotionDecisionV1) -> Result<(), ContractError> {
    let roles = value
        .approvals
        .iter()
        .map(|v| v.role)
        .collect::<std::collections::BTreeSet<_>>();
    (value.schema == "harness.promotion-decision.v1"
        && [
            value.promotion_id.as_str(),
            value.experiment_id.as_str(),
            value.candidate_id.as_str(),
            value.from_bundle_id.as_str(),
            value.to_bundle_id.as_str(),
        ]
        .iter()
        .all(|v| id(v))
        && value.from_bundle_id != value.to_bundle_id
        && digest(&value.safety_anchor_digest)
        && digest(&value.experiment_digest)
        && digest(&value.runtime_policy_digest)
        && value.required_receipts.valid()
        && value.approvals.len() == 2
        && roles.len() == 2
        && value.approvals[0].role == ApprovalRole::IndependentReviewer
        && value.approvals[0]
            .receipt
            .valid_as(ReceiptKind::IndependentReview)
        && value.approvals[1].role == ApprovalRole::Operator
        && value.approvals[1]
            .receipt
            .valid_as(ReceiptKind::OperatorApproval)
        && value.rollback_evidence.valid_as(ReceiptKind::Rollback)
        && digest_without_self(value)? == value.sha256)
        .then_some(())
        .ok_or(ContractError::Missing)
}
pub fn verify_promotion_against_experiment(
    value: &PromotionDecisionV1,
    experiment: &crate::ExperimentV1,
) -> Result<(), ContractError> {
    verify_promotion(value)?;
    verify_experiment(experiment)?;
    let stages = [
        &value.required_receipts.offline,
        &value.required_receipts.holdout,
        &value.required_receipts.shadow,
        &value.required_receipts.canary,
    ];
    (value.experiment_id == experiment.experiment_id
        && value.candidate_id == experiment.candidate_id
        && value.from_bundle_id == experiment.champion_bundle_id
        && value.to_bundle_id == experiment.challenger_bundle_id
        && value.experiment_digest == experiment.sha256
        && value.runtime_policy_digest == experiment.runtime_policy_digest
        && experiment.state == crate::ExperimentState::PromotionReview
        && experiment
            .stages
            .iter()
            .all(|stage| stage.state == crate::StageState::Passed)
        && experiment.hard_gates.iter().all(|gate| gate.passed)
        && stages
            .iter()
            .zip(&experiment.stages)
            .all(|(receipt, stage)| {
                stage.evidence.as_ref().is_some_and(|evidence| {
                    receipt.id == evidence.id && receipt.digest == evidence.digest
                })
            }))
    .then_some(())
    .ok_or(ContractError::Stale)
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtomicBindingCommand {
    pub expected_active_bundle_id: String,
    pub to_bundle_id: String,
    pub safety_anchor_digest: String,
    pub promotion_id: String,
    pub promotion_receipt: Receipt,
}
pub fn atomic_binding_command(
    value: &PromotionDecisionV1,
    active: &str,
    anchor: &str,
) -> Result<AtomicBindingCommand, ContractError> {
    verify_promotion(value)?;
    if active != value.from_bundle_id || anchor != value.safety_anchor_digest {
        return Err(ContractError::Stale);
    }
    Ok(AtomicBindingCommand {
        expected_active_bundle_id: active.into(),
        to_bundle_id: value.to_bundle_id.clone(),
        safety_anchor_digest: anchor.into(),
        promotion_id: value.promotion_id.clone(),
        promotion_receipt: Receipt {
            kind: ReceiptKind::Promotion,
            id: value.promotion_id.clone(),
            digest: value.sha256.clone(),
        },
    })
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackContract {
    pub schema: String,
    pub promotion_id: String,
    pub expected_active_bundle_id: String,
    pub rollback_target_bundle_id: String,
    pub safety_anchor_digest: String,
    pub promotion_receipt: Receipt,
    pub emergency_stop: bool,
    pub reason: RollbackReason,
    pub evidence: Receipt,
    pub sha256: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackBindingCommand {
    pub expected_active_bundle_id: String,
    pub rollback_target_bundle_id: String,
    pub safety_anchor_digest: String,
    pub rollback_receipt: Receipt,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RollbackReplay {
    Apply,
    ExactReplay,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackReason {
    OperatorRequested,
    HardConstraint,
    GlobalKillSwitch,
}
impl RollbackContract {
    pub fn from_promotion(
        decision: &PromotionDecisionV1,
        current: &str,
        evidence: Receipt,
    ) -> Result<Self, ContractError> {
        verify_promotion(decision)?;
        if current != decision.to_bundle_id {
            return Err(ContractError::Stale);
        }
        let mut value = Self {
            schema: "harness.rollback.v1".into(),
            promotion_id: decision.promotion_id.clone(),
            expected_active_bundle_id: current.into(),
            rollback_target_bundle_id: decision.from_bundle_id.clone(),
            safety_anchor_digest: decision.safety_anchor_digest.clone(),
            promotion_receipt: Receipt {
                kind: ReceiptKind::Promotion,
                id: decision.promotion_id.clone(),
                digest: decision.sha256.clone(),
            },
            emergency_stop: false,
            reason: RollbackReason::OperatorRequested,
            evidence,
            sha256: String::new(),
        };
        value.sha256 = digest_without_self(&value)?;
        Ok(value)
    }
}
pub fn validate_rollback(
    value: &RollbackContract,
    active: &str,
    anchor: &str,
) -> Result<(), ContractError> {
    (value.schema == "harness.rollback.v1"
        && value.expected_active_bundle_id == active
        && value.safety_anchor_digest == anchor
        && [
            value.promotion_id.as_str(),
            value.expected_active_bundle_id.as_str(),
            value.rollback_target_bundle_id.as_str(),
        ]
        .iter()
        .all(|v| id(v))
        && value.expected_active_bundle_id != value.rollback_target_bundle_id
        && digest(&value.safety_anchor_digest)
        && value.promotion_receipt.valid_as(ReceiptKind::Promotion)
        && value.promotion_receipt.id == value.promotion_id
        && value.evidence.valid_as(ReceiptKind::Rollback)
        && (!value.emergency_stop
            || matches!(
                value.reason,
                RollbackReason::HardConstraint | RollbackReason::GlobalKillSwitch
            ))
        && digest_without_self(value)? == value.sha256)
        .then_some(())
        .ok_or(ContractError::Stale)
}
pub fn rollback_binding_command(
    value: &RollbackContract,
    active: &str,
    anchor: &str,
    persisted_rollback_digest: Option<&str>,
) -> Result<(RollbackBindingCommand, RollbackReplay), ContractError> {
    if active == value.rollback_target_bundle_id {
        validate_rollback(value, &value.expected_active_bundle_id, anchor)?;
        if persisted_rollback_digest == Some(value.sha256.as_str()) {
            return Ok((
                RollbackBindingCommand {
                    expected_active_bundle_id: value.expected_active_bundle_id.clone(),
                    rollback_target_bundle_id: value.rollback_target_bundle_id.clone(),
                    safety_anchor_digest: value.safety_anchor_digest.clone(),
                    rollback_receipt: Receipt {
                        kind: ReceiptKind::Rollback,
                        id: value.promotion_id.clone(),
                        digest: value.sha256.clone(),
                    },
                },
                RollbackReplay::ExactReplay,
            ));
        }
        return Err(ContractError::Stale);
    }
    validate_rollback(value, active, anchor)?;
    Ok((
        RollbackBindingCommand {
            expected_active_bundle_id: active.into(),
            rollback_target_bundle_id: value.rollback_target_bundle_id.clone(),
            safety_anchor_digest: anchor.into(),
            rollback_receipt: Receipt {
                kind: ReceiptKind::Rollback,
                id: value.promotion_id.clone(),
                digest: value.sha256.clone(),
            },
        },
        RollbackReplay::Apply,
    ))
}
