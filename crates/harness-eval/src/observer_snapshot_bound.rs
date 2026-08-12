//! Controller-owned contract for the sanitized observer snapshot bound case.
//!
//! This is an overlay test, never a production trace. The independent grader
//! receives only immutable controller receipts and process exit facts; command
//! stdout is deliberately not an authority.

use serde::{Deserialize, Serialize};

use crate::{
    DigestError, IsolationCapability, SampleClassification, canonical_digest_without_self, hash,
    sha40,
};

pub const OBSERVER_SNAPSHOT_HISTORICAL_BASE: &str = "6bc83a51d83a82fb5ba4e5722db683de830533ca";
pub const OBSERVER_SNAPSHOT_BOUNDED_FIX: &str = "5f70f85a45ce358df135543617c2925dcbaf127f";
pub const OBSERVER_SNAPSHOT_TARGET_PATH: &str = "crates/harness-store/src/queries.rs";
pub const OBSERVER_SNAPSHOT_TEST_NAME: &str = "m2_trace_snapshot_bound_regression";

pub const OBSERVER_SNAPSHOT_COMMAND: [&str; 10] = [
    "/cargo-toolchain/bin/cargo",
    "test",
    "--locked",
    "--offline",
    "--jobs",
    "1",
    "-p",
    "harness-store",
    "--lib",
    OBSERVER_SNAPSHOT_TEST_NAME,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverSnapshotArm {
    Historical,
    Fixed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverSnapshotSignal {
    HistoricalBugReproduced,
    FixedBoundEnforced,
}

#[must_use]
pub fn observer_snapshot_fixture_digest(arm: ObserverSnapshotArm) -> String {
    let source = match arm {
        ObserverSnapshotArm::Historical => {
            include_str!("../evaluation-fixtures/observer_snapshot_bound_historical_overlay.rs")
        }
        ObserverSnapshotArm::Fixed => {
            include_str!("../evaluation-fixtures/observer_snapshot_bound_fixed_overlay.rs")
        }
    };
    sha256(source.as_bytes())
}

#[must_use]
pub fn observer_snapshot_command_digest() -> String {
    sha256(
        ["harness.eval.command.v1"]
            .into_iter()
            .chain(OBSERVER_SNAPSHOT_COMMAND)
            .collect::<Vec<_>>()
            .join("\0")
            .as_bytes(),
    )
}

#[must_use]
pub fn observer_snapshot_setup_digest(arm: ObserverSnapshotArm, base_checkout_sha: &str) -> String {
    sha256(
        [
            "harness.eval.fixture-setup.v3",
            "append-overlay-to-existing-test-module",
            base_checkout_sha,
            OBSERVER_SNAPSHOT_TARGET_PATH,
            &observer_snapshot_fixture_digest(arm),
            "generated-minimal-domain-receipts",
            "negative-control-exactly-10000",
            "bound-check-10001",
            &observer_snapshot_command_digest(),
        ]
        .join("\0")
        .as_bytes(),
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSnapshotMaterializationReceiptV1 {
    pub schema: String,
    pub base_checkout_sha: String,
    pub target_path: String,
    pub arm: ObserverSnapshotArm,
    pub overlay_digest: String,
    pub resulting_target_digest: String,
    pub command_digest: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSnapshotControllerReceiptV1 {
    pub schema: String,
    pub arm: ObserverSnapshotArm,
    pub historical_base_sha: String,
    pub fixed_source_sha: String,
    pub signal: ObserverSnapshotSignal,
    pub materialization: ObserverSnapshotMaterializationReceiptV1,
    pub fixture_digest: String,
    pub setup_digest: String,
    pub command_digest: String,
    pub candidate_isolation: ObserverSnapshotIsolationPinV1,
    pub grader_isolation: ObserverSnapshotIsolationPinV1,
    pub registry_manifest_digest: String,
    pub git_manifest_digest: String,
    pub toolchain_manifest_digest: String,
    pub target_scope_digest: String,
    pub isolation: IsolationCapability,
    pub classification: SampleClassification,
    pub controller_exit_success: bool,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSnapshotIsolationPinV1 {
    pub backend: String,
    pub backend_version: String,
    pub namespaces: Vec<String>,
    pub candidate_access: String,
    pub grader_access: String,
    pub artifact_access: String,
    pub available: bool,
    pub policy_digest: String,
    pub receipt_digest: String,
}

const BUBBLEWRAP_VERSION: &str = "bubblewrap 0.11.0";
const BUBBLEWRAP_NAMESPACES: [&str; 6] = ["network", "user", "pid", "ipc", "uts", "cgroup"];

fn isolation_pin_digest(pin: &ObserverSnapshotIsolationPinV1) -> String {
    sha256(
        format!(
            "harness.eval.isolation.receipt.v2\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
            pin.backend,
            pin.backend_version,
            pin.namespaces.join(","),
            pin.candidate_access,
            pin.grader_access,
            pin.artifact_access,
            pin.available,
            pin.policy_digest,
        )
        .as_bytes(),
    )
}

fn valid_isolation_pin(pin: &ObserverSnapshotIsolationPinV1) -> bool {
    pin.backend == "bubblewrap"
        && pin.backend_version == BUBBLEWRAP_VERSION
        && pin
            .namespaces
            .iter()
            .map(String::as_str)
            .eq(BUBBLEWRAP_NAMESPACES)
        && pin.available
        && hash(&pin.policy_digest)
        && hash(&pin.receipt_digest)
        && isolation_pin_digest(pin) == pin.receipt_digest
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverSnapshotGrade {
    Pass,
    Fail,
    InfrastructureUnavailable,
    InvalidReceipt,
}

pub fn verify_observer_snapshot_materialization_receipt(
    receipt: &ObserverSnapshotMaterializationReceiptV1,
) -> Result<(), DigestError> {
    (receipt.schema == "harness.eval.observer-snapshot-materialization.v1"
        && sha40(&receipt.base_checkout_sha)
        && receipt.target_path == OBSERVER_SNAPSHOT_TARGET_PATH
        && receipt.overlay_digest == observer_snapshot_fixture_digest(receipt.arm)
        && hash(&receipt.resulting_target_digest)
        && receipt.command_digest == observer_snapshot_command_digest()
        && canonical_digest_without_self(receipt)? == receipt.sha256)
        .then_some(())
        .ok_or(DigestError::DigestMismatch)
}

pub fn verify_observer_snapshot_controller_receipt(
    receipt: &ObserverSnapshotControllerReceiptV1,
) -> Result<(), DigestError> {
    let (expected_base, expected_signal) = match receipt.arm {
        ObserverSnapshotArm::Historical => (
            OBSERVER_SNAPSHOT_HISTORICAL_BASE,
            ObserverSnapshotSignal::HistoricalBugReproduced,
        ),
        ObserverSnapshotArm::Fixed => (
            OBSERVER_SNAPSHOT_BOUNDED_FIX,
            ObserverSnapshotSignal::FixedBoundEnforced,
        ),
    };
    (receipt.schema == "harness.eval.observer-snapshot-receipt.v2"
        && receipt.historical_base_sha == OBSERVER_SNAPSHOT_HISTORICAL_BASE
        && receipt.fixed_source_sha == OBSERVER_SNAPSHOT_BOUNDED_FIX
        && receipt.signal == expected_signal
        && receipt.materialization.arm == receipt.arm
        && receipt.materialization.base_checkout_sha == expected_base
        && receipt.fixture_digest == observer_snapshot_fixture_digest(receipt.arm)
        && receipt.setup_digest == observer_snapshot_setup_digest(receipt.arm, expected_base)
        && receipt.command_digest == observer_snapshot_command_digest()
        && valid_isolation_pin(&receipt.candidate_isolation)
        && receipt.candidate_isolation.candidate_access == "read_write"
        && receipt.candidate_isolation.grader_access == "not_exposed"
        && receipt.candidate_isolation.artifact_access == "not_exposed"
        && valid_isolation_pin(&receipt.grader_isolation)
        && receipt.grader_isolation.candidate_access == "not_exposed"
        && receipt.grader_isolation.grader_access == "read_only"
        && receipt.grader_isolation.artifact_access == "read_only"
        && hash(&receipt.registry_manifest_digest)
        && hash(&receipt.git_manifest_digest)
        && hash(&receipt.toolchain_manifest_digest)
        && hash(&receipt.target_scope_digest)
        && verify_observer_snapshot_materialization_receipt(&receipt.materialization).is_ok()
        && canonical_digest_without_self(receipt)? == receipt.sha256)
        .then_some(())
        .ok_or(DigestError::DigestMismatch)
}

#[must_use]
pub fn grade_observer_snapshot_controller_receipt(
    receipt: &ObserverSnapshotControllerReceiptV1,
) -> ObserverSnapshotGrade {
    if verify_observer_snapshot_controller_receipt(receipt).is_err() {
        return ObserverSnapshotGrade::InvalidReceipt;
    }
    if receipt.isolation == IsolationCapability::InfrastructureUnavailable
        || receipt.classification == SampleClassification::InfrastructureUnavailable
    {
        return ObserverSnapshotGrade::InfrastructureUnavailable;
    }
    let expected_classification = match receipt.arm {
        ObserverSnapshotArm::Historical => SampleClassification::Fail,
        ObserverSnapshotArm::Fixed => SampleClassification::Pass,
    };
    if receipt.isolation == IsolationCapability::Available
        && receipt.classification == expected_classification
        && receipt.controller_exit_success
    {
        ObserverSnapshotGrade::Pass
    } else {
        ObserverSnapshotGrade::Fail
    }
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn materialization(base: &str) -> ObserverSnapshotMaterializationReceiptV1 {
        let mut receipt = ObserverSnapshotMaterializationReceiptV1 {
            schema: "harness.eval.observer-snapshot-materialization.v1".to_owned(),
            base_checkout_sha: base.to_owned(),
            target_path: OBSERVER_SNAPSHOT_TARGET_PATH.to_owned(),
            arm: if base == OBSERVER_SNAPSHOT_HISTORICAL_BASE {
                ObserverSnapshotArm::Historical
            } else {
                ObserverSnapshotArm::Fixed
            },
            overlay_digest: observer_snapshot_fixture_digest(
                if base == OBSERVER_SNAPSHOT_HISTORICAL_BASE {
                    ObserverSnapshotArm::Historical
                } else {
                    ObserverSnapshotArm::Fixed
                },
            ),
            resulting_target_digest: sha256(b"controller-materialized-target"),
            command_digest: observer_snapshot_command_digest(),
            sha256: String::new(),
        };
        receipt.sha256 = canonical_digest_without_self(&receipt).unwrap();
        receipt
    }

    fn receipt(arm: ObserverSnapshotArm) -> ObserverSnapshotControllerReceiptV1 {
        let base = match arm {
            ObserverSnapshotArm::Historical => OBSERVER_SNAPSHOT_HISTORICAL_BASE,
            ObserverSnapshotArm::Fixed => OBSERVER_SNAPSHOT_BOUNDED_FIX,
        };
        let signal = match arm {
            ObserverSnapshotArm::Historical => ObserverSnapshotSignal::HistoricalBugReproduced,
            ObserverSnapshotArm::Fixed => ObserverSnapshotSignal::FixedBoundEnforced,
        };
        let mut receipt = ObserverSnapshotControllerReceiptV1 {
            schema: "harness.eval.observer-snapshot-receipt.v2".to_owned(),
            arm,
            historical_base_sha: OBSERVER_SNAPSHOT_HISTORICAL_BASE.to_owned(),
            fixed_source_sha: OBSERVER_SNAPSHOT_BOUNDED_FIX.to_owned(),
            signal,
            materialization: materialization(base),
            fixture_digest: observer_snapshot_fixture_digest(arm),
            setup_digest: observer_snapshot_setup_digest(arm, base),
            command_digest: observer_snapshot_command_digest(),
            candidate_isolation: isolation("read_write", "not_exposed", "not_exposed"),
            grader_isolation: isolation("not_exposed", "read_only", "read_only"),
            registry_manifest_digest: sha256(b"registry"),
            git_manifest_digest: sha256(b"git"),
            toolchain_manifest_digest: sha256(b"toolchain"),
            target_scope_digest: sha256(b"target"),
            isolation: IsolationCapability::Available,
            classification: match arm {
                ObserverSnapshotArm::Historical => SampleClassification::Fail,
                ObserverSnapshotArm::Fixed => SampleClassification::Pass,
            },
            controller_exit_success: true,
            sha256: String::new(),
        };
        receipt.sha256 = canonical_digest_without_self(&receipt).unwrap();
        receipt
    }

    fn isolation(
        candidate_access: &str,
        grader_access: &str,
        artifact_access: &str,
    ) -> ObserverSnapshotIsolationPinV1 {
        let mut pin = ObserverSnapshotIsolationPinV1 {
            backend: "bubblewrap".to_owned(),
            backend_version: BUBBLEWRAP_VERSION.to_owned(),
            namespaces: BUBBLEWRAP_NAMESPACES.map(str::to_owned).to_vec(),
            candidate_access: candidate_access.to_owned(),
            grader_access: grader_access.to_owned(),
            artifact_access: artifact_access.to_owned(),
            available: true,
            policy_digest: sha256(b"policy"),
            receipt_digest: String::new(),
        };
        pin.receipt_digest = isolation_pin_digest(&pin);
        pin
    }

    #[test]
    fn fixed_command_uses_actual_nul_framing_and_both_arms_grade() {
        assert_eq!(
            observer_snapshot_command_digest(),
            sha256(b"harness.eval.command.v1\0/cargo-toolchain/bin/cargo\0test\0--locked\0--offline\0--jobs\x001\0-p\0harness-store\0--lib\0m2_trace_snapshot_bound_regression"),
        );
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&receipt(ObserverSnapshotArm::Historical)),
            ObserverSnapshotGrade::Pass
        );
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&receipt(ObserverSnapshotArm::Fixed)),
            ObserverSnapshotGrade::Pass
        );
    }

    #[test]
    fn grader_rejects_source_or_materialization_mismatch_and_infra_is_not_success() {
        let mut mismatched = receipt(ObserverSnapshotArm::Fixed);
        mismatched.fixed_source_sha = OBSERVER_SNAPSHOT_HISTORICAL_BASE.to_owned();
        mismatched.sha256 = canonical_digest_without_self(&mismatched).unwrap();
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&mismatched),
            ObserverSnapshotGrade::InvalidReceipt
        );

        let mut swapped = receipt(ObserverSnapshotArm::Fixed);
        swapped.materialization.arm = ObserverSnapshotArm::Historical;
        swapped.materialization.overlay_digest =
            observer_snapshot_fixture_digest(ObserverSnapshotArm::Historical);
        swapped.materialization.sha256 =
            canonical_digest_without_self(&swapped.materialization).unwrap();
        swapped.sha256 = canonical_digest_without_self(&swapped).unwrap();
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&swapped),
            ObserverSnapshotGrade::InvalidReceipt
        );

        let mut unavailable = receipt(ObserverSnapshotArm::Fixed);
        unavailable.isolation = IsolationCapability::InfrastructureUnavailable;
        unavailable.classification = SampleClassification::InfrastructureUnavailable;
        unavailable.sha256 = canonical_digest_without_self(&unavailable).unwrap();
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&unavailable),
            ObserverSnapshotGrade::InfrastructureUnavailable
        );

        let mut forged = receipt(ObserverSnapshotArm::Fixed);
        forged.candidate_isolation.receipt_digest = sha256(b"forged");
        forged.sha256 = canonical_digest_without_self(&forged).unwrap();
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&forged),
            ObserverSnapshotGrade::InvalidReceipt
        );
    }

    #[test]
    fn historical_reproduction_is_a_failed_sample_despite_operational_success() {
        let historical = receipt(ObserverSnapshotArm::Historical);
        assert!(historical.controller_exit_success);
        assert_eq!(historical.classification, SampleClassification::Fail);
        assert_eq!(
            grade_observer_snapshot_controller_receipt(&historical),
            ObserverSnapshotGrade::Pass
        );
    }
}
