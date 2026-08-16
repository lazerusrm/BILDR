//! Closed, deterministic OCP-018 fault-matrix contract.
//!
//! This crate does not execute controller actions.  It validates the immutable
//! result record emitted by the independently executed invariant/fault tests.
//! A missing, unavailable, failed, or reordered result is never promotion
//! evidence.

use crate::{DigestError, canonical_digest_without_self, hash, sha40, token};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorControlInvariant {
    OneMutableOwner,
    UnknownCannotAuthorizeReplacement,
    SourceOnlyAttentionClosure,
    CompletionCannotHideBlockingAttention,
    PresentationCannotResolve,
    InvestigationCannotMutateOrCreateCandidate,
    UnknownExternalEffectNeverAutoRetried,
    ProjectionNeverAuthorizes,
    ReplayDeterministic,
    StaleVersionOrDigestRejected,
    CriticalNotificationNotOmitted,
    RemoteRuntimeAbsent,
}

impl OperatorControlInvariant {
    pub const fn case_id(self) -> &'static str {
        match self {
            Self::OneMutableOwner => "one_mutable_owner",
            Self::UnknownCannotAuthorizeReplacement => "unknown_cannot_authorize_replacement",
            Self::SourceOnlyAttentionClosure => "source_only_attention_closure",
            Self::CompletionCannotHideBlockingAttention => {
                "completion_cannot_hide_blocking_attention"
            }
            Self::PresentationCannotResolve => "presentation_cannot_resolve",
            Self::InvestigationCannotMutateOrCreateCandidate => {
                "investigation_cannot_mutate_or_create_candidate"
            }
            Self::UnknownExternalEffectNeverAutoRetried => {
                "unknown_external_effect_never_auto_retried"
            }
            Self::ProjectionNeverAuthorizes => "projection_never_authorizes",
            Self::ReplayDeterministic => "replay_deterministic",
            Self::StaleVersionOrDigestRejected => "stale_version_or_digest_rejected",
            Self::CriticalNotificationNotOmitted => "critical_notification_not_omitted",
            Self::RemoteRuntimeAbsent => "remote_runtime_absent",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultInjection {
    ConcurrentClaim,
    OwnershipUnknown,
    ForgedClosure,
    TerminalStateWithOpenAttention,
    ForgedPresentationReceipt,
    InvestigationWriteOrCandidateRequest,
    UnknownExternalEffect,
    ProjectionMutationRequest,
    ReorderedReplay,
    StaleVersionOrDigest,
    CriticalDeliveryPolicy,
    RemoteDispatchRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorControlFaultCase {
    pub invariant: OperatorControlInvariant,
    pub injection: FaultInjection,
    pub test_selector: &'static str,
}

pub const OPERATOR_CONTROL_FAULT_CASES: [OperatorControlFaultCase; 12] = [
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::OneMutableOwner,
        injection: FaultInjection::ConcurrentClaim,
        test_selector: "cargo test -p harness-store --lib proof_consumption_authorizes_exactly_one_replacement_and_scheduler_lease -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::UnknownCannotAuthorizeReplacement,
        injection: FaultInjection::OwnershipUnknown,
        test_selector: "cargo test -p harness-store --lib proof_consumption_refuses_any_unreconciled_command_history -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::SourceOnlyAttentionClosure,
        injection: FaultInjection::ForgedClosure,
        test_selector: "cargo test -p harness-domain --lib attention_transitions_are_source_owned_and_terminal_receipts_idempotent -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::CompletionCannotHideBlockingAttention,
        injection: FaultInjection::TerminalStateWithOpenAttention,
        test_selector: "cargo test -p harness-store --lib attention_lifecycle_records_deterministic_causal_receipts -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::PresentationCannotResolve,
        injection: FaultInjection::ForgedPresentationReceipt,
        test_selector: "cargo test -p harness-store --lib notification_presentation_is_exact_session_scoped_idempotent_and_authority_neutral -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::InvestigationCannotMutateOrCreateCandidate,
        injection: FaultInjection::InvestigationWriteOrCandidateRequest,
        test_selector: "cargo test -p harness-orchestrator --lib investigation_launch_and_artifact_completion_are_read_only_and_bound -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::UnknownExternalEffectNeverAutoRetried,
        injection: FaultInjection::UnknownExternalEffect,
        test_selector: "cargo test -p harness-orchestrator --lib automatic_fresh_attempt_routes_remain_unavailable -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::ProjectionNeverAuthorizes,
        injection: FaultInjection::ProjectionMutationRequest,
        test_selector: "cargo test -p harness-store --lib return_view_preserves_current_observe_only_sections_and_cursor_cannot_regress -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::ReplayDeterministic,
        injection: FaultInjection::ReorderedReplay,
        test_selector: "cargo test -p harness-store --lib snapshot_is_reused_only_at_the_exact_source_cursors -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::StaleVersionOrDigestRejected,
        injection: FaultInjection::StaleVersionOrDigest,
        test_selector: "cargo test -p harness-store --lib wait_intervention_is_idempotent_and_cannot_apply_to_a_stale_episode -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::CriticalNotificationNotOmitted,
        injection: FaultInjection::CriticalDeliveryPolicy,
        test_selector: "cargo test -p harness-store --lib shadow_batch_is_exact_idempotent_and_keeps_critical_attention_immediate -- --exact --test-threads=1",
    },
    OperatorControlFaultCase {
        invariant: OperatorControlInvariant::RemoteRuntimeAbsent,
        injection: FaultInjection::RemoteDispatchRequest,
        test_selector: "cargo test -p xtask remote_dispatch_capability_is_absent -- --exact --test-threads=1",
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultOutcome {
    Held,
    Violated,
    InfrastructureUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorControlFaultResultV1 {
    pub case_id: String,
    pub invariant: OperatorControlInvariant,
    pub injection: FaultInjection,
    pub test_selector: String,
    pub outcome: FaultOutcome,
    pub evidence_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorControlFaultMatrixRunV1 {
    pub schema: String,
    pub implementation_sha: String,
    pub results: Vec<OperatorControlFaultResultV1>,
    pub sha256: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum FaultMatrixError {
    #[error("invalid OCP-018 fault matrix: {0}")]
    Invalid(&'static str),
    #[error("fault matrix result is missing or duplicated: {0}")]
    ResultSet(String),
    #[error("fault matrix promotion gate failed: {0}")]
    Gate(String),
    #[error(transparent)]
    Digest(#[from] DigestError),
}

impl OperatorControlFaultMatrixRunV1 {
    pub fn digest(&self) -> Result<String, FaultMatrixError> {
        Ok(canonical_digest_without_self(self)?)
    }

    pub fn validate(&self) -> Result<(), FaultMatrixError> {
        if self.schema != "harness.operator-control-fault-matrix.v1"
            || !sha40(&self.implementation_sha)
            || !hash(&self.sha256)
            || self.digest()? != self.sha256
        {
            return Err(FaultMatrixError::Invalid(
                "schema, implementation SHA, or digest",
            ));
        }
        if self.results.len() != OPERATOR_CONTROL_FAULT_CASES.len() {
            return Err(FaultMatrixError::Invalid("result count"));
        }
        let mut prior = None;
        let mut ids = BTreeSet::new();
        for result in &self.results {
            if !token(&result.case_id)
                || result.test_selector.is_empty()
                || result.test_selector.len() > 512
                || !hash(&result.evidence_digest)
            {
                return Err(FaultMatrixError::Invalid("result shape"));
            }
            if prior.is_some_and(|value| value >= result.case_id.as_str()) {
                return Err(FaultMatrixError::Invalid("result order"));
            }
            prior = Some(result.case_id.as_str());
            if !ids.insert(result.case_id.as_str()) {
                return Err(FaultMatrixError::ResultSet(result.case_id.clone()));
            }
            let Some(expected) = OPERATOR_CONTROL_FAULT_CASES
                .iter()
                .find(|case| case.invariant == result.invariant)
            else {
                return Err(FaultMatrixError::ResultSet(result.case_id.clone()));
            };
            if result.case_id != expected.invariant.case_id()
                || result.injection != expected.injection
                || result.test_selector != expected.test_selector
            {
                return Err(FaultMatrixError::ResultSet(result.case_id.clone()));
            }
        }
        for expected in OPERATOR_CONTROL_FAULT_CASES {
            if !ids.contains(expected.invariant.case_id()) {
                return Err(FaultMatrixError::ResultSet(
                    expected.invariant.case_id().to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// A release gate is intentionally stricter than shape validation: an
    /// unavailable test is preserved in the record but is never a passing
    /// invariant result.
    pub fn promotion_gate(&self) -> Result<(), FaultMatrixError> {
        self.validate()?;
        for result in &self.results {
            match result.outcome {
                FaultOutcome::Held => {}
                FaultOutcome::Violated => {
                    return Err(FaultMatrixError::Gate(format!(
                        "{} was violated",
                        result.case_id
                    )));
                }
                FaultOutcome::InfrastructureUnavailable => {
                    return Err(FaultMatrixError::Gate(format!(
                        "{} was unavailable",
                        result.case_id
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(outcome: FaultOutcome) -> OperatorControlFaultMatrixRunV1 {
        let mut results = OPERATOR_CONTROL_FAULT_CASES
            .iter()
            .map(|case| OperatorControlFaultResultV1 {
                case_id: case.invariant.case_id().to_owned(),
                invariant: case.invariant,
                injection: case.injection,
                test_selector: case.test_selector.to_owned(),
                outcome,
                evidence_digest: "a".repeat(64),
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| left.case_id.cmp(&right.case_id));
        let mut value = OperatorControlFaultMatrixRunV1 {
            schema: "harness.operator-control-fault-matrix.v1".to_owned(),
            implementation_sha: "b".repeat(40),
            results,
            sha256: String::new(),
        };
        value.sha256 = value.digest().expect("digest");
        value
    }

    #[test]
    fn complete_held_matrix_is_closed_canonical_and_promotion_eligible() {
        let value = run(FaultOutcome::Held);
        assert!(value.validate().is_ok());
        assert!(value.promotion_gate().is_ok());
    }

    #[test]
    fn missing_reordered_or_noncanonical_results_fail_closed() {
        let mut missing = run(FaultOutcome::Held);
        missing.results.pop();
        missing.sha256 = missing.digest().expect("digest");
        assert!(missing.validate().is_err());

        let mut reordered = run(FaultOutcome::Held);
        reordered.results.swap(0, 1);
        reordered.sha256 = reordered.digest().expect("digest");
        assert!(reordered.validate().is_err());

        let mut renamed = run(FaultOutcome::Held);
        renamed.results[0].test_selector = "alternate test".to_owned();
        renamed.sha256 = renamed.digest().expect("digest");
        assert!(renamed.validate().is_err());
    }

    #[test]
    fn violation_or_unavailable_result_never_passes_promotion() {
        let mut violated = run(FaultOutcome::Held);
        violated.results[0].outcome = FaultOutcome::Violated;
        violated.sha256 = violated.digest().expect("digest");
        assert!(violated.validate().is_ok());
        assert!(violated.promotion_gate().is_err());

        let unavailable = run(FaultOutcome::InfrastructureUnavailable);
        assert!(unavailable.validate().is_ok());
        assert!(unavailable.promotion_gate().is_err());
    }
}
