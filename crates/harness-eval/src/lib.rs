//! Pure, deterministic SI-008–SI-012 evaluation decisions.
//! No runner, Store, candidate, or hidden-answer implementation dependency.

mod custody;
mod digest;
mod observer_snapshot_bound;
mod stats;
mod wire;

pub use custody::*;
pub use digest::*;
pub use observer_snapshot_bound::*;
pub use stats::*;
pub use wire::*;

#[cfg(test)]
mod tests {
    use super::*;
    fn h() -> String {
        "a".repeat(64)
    }
    #[test]
    fn exact_taskset_wire_is_closed_and_self_digest_is_excluded() {
        let mut taskset = TasksetV1 {
            schema: "harness.taskset.v1".into(),
            taskset_id: "set-1".into(),
            revision: 1,
            cases: vec![CasePin {
                case_id: "case-1".into(),
                revision: 1,
                split: Split::Development,
                case_digest: h(),
            }],
            sha256: String::new(),
        };
        taskset.sha256 = canonical_digest_without_self(&taskset).unwrap();
        assert!(verify_taskset_v1(&taskset).is_ok());
        taskset.cases[0].case_digest = "b".repeat(64);
        assert!(verify_taskset_v1(&taskset).is_err());
    }
    #[test]
    fn isolation_unavailable_is_not_a_pass_and_optional_receipts_are_unknown() {
        let mut sample = EvalSampleV1 {
            schema: "harness.eval-sample.v1".into(),
            sample_id: "sample-1".into(),
            case_id: "case-1".into(),
            case_revision: 1,
            case_digest: h(),
            taskset_digest: h(),
            grader_bundle_digest: h(),
            policy_digest: h(),
            base_sha: "b".repeat(40),
            fixture_digest: h(),
            setup_digest: h(),
            runtime_digest: h(),
            isolation: IsolationCapability::InfrastructureUnavailable,
            command_digest: h(),
            classification: SampleClassification::InfrastructureUnavailable,
            trace_digest: NullableDigest(None),
            evidence_digest: NullableDigest(None),
            artifact_digest: NullableDigest(None),
            cost_receipt_digest: NullableDigest(None),
            seed: 1,
            sha256: String::new(),
        };
        sample.sha256 = canonical_digest_without_self(&sample).unwrap();
        assert!(verify_sample(&sample).is_ok());
        assert_eq!(
            runner_isolation(sample.isolation),
            Some(SampleClassification::InfrastructureUnavailable)
        );
    }
    #[test]
    fn grader_requires_negative_control_and_reward_integrity() {
        let mut grader = GraderBundleV1 {
            schema: "harness.grader-bundle.v1".into(),
            grader_bundle_id: "grader-1".into(),
            revision: 1,
            signals: vec![GraderSignal {
                id: "control-1".into(),
                kind: GraderKind::Deterministic,
                direction: SignalDirection::BooleanPass,
                weight: 1.0,
                required: true,
                definition_digest: h(),
                calibration_set_digest: None,
            }],
            hard_gates: vec!["control-1".into()],
            negative_controls: vec![NegativeControl {
                id: "control-1".into(),
                signal_id: "control-1".into(),
                expected_relationship: "must fail reward hack".into(),
                failure_action: NegativeControlAction::Block,
            }],
            reward_integrity_required: true,
            isolation: GraderIsolation {
                candidate_write_access: false,
                holdout_answer_access: false,
                grader_runtime: GraderRuntime::SeparateProcess,
            },
            sha256: String::new(),
        };
        grader.sha256 = canonical_digest_without_self(&grader).unwrap();
        assert_eq!(
            canonical_digest_without_self(&grader).unwrap(),
            grader.sha256
        );
        assert_eq!(
            canonical_digest_without_self(&grader).unwrap(),
            grader.sha256
        );
        assert!(verify_grader_contract(&grader).is_ok());
        grader.reward_integrity_required = false;
        assert!(verify_grader_contract(&grader).is_err());

        grader.reward_integrity_required = true;
        grader.signals.push(GraderSignal {
            id: "control-1".into(),
            ..grader.signals[0].clone()
        });
        assert!(verify_grader_contract(&grader).is_err());
    }
    #[test]
    fn optimizer_and_candidate_are_denied_holdout_and_leakage_invalidates() {
        let denied = HoldoutAccess {
            principal: Principal::Optimizer,
            split: Split::Holdout,
            action: HoldoutAction::ReadAnswer,
            receipt_id: "access-1".into(),
        };
        assert_eq!(authorize(&denied), Err(CustodyError::Denied));
        for action in [
            HoldoutAction::ReadMetadata,
            HoldoutAction::ReadAnswer,
            HoldoutAction::Execute,
        ] {
            assert_eq!(
                authorize(&HoldoutAccess {
                    action,
                    ..denied.clone()
                }),
                Err(CustodyError::Denied)
            );
            assert_eq!(
                authorize(&HoldoutAccess {
                    principal: Principal::CandidateRuntime,
                    action,
                    ..denied.clone()
                }),
                Err(CustodyError::Denied)
            );
        }
        assert!(require_clean_holdout(&[]).is_ok());
        assert_eq!(
            require_clean_holdout(&[LeakageDeclaration {
                case_id: "case-1".into(),
                rotation_revision: 1,
                confirmed: true
            }]),
            Err(CustodyError::Invalidated)
        );
    }
    #[test]
    fn deterministic_statistics_refuse_small_critical_and_reward_hacks() {
        let pass = SignalVector {
            required_pass: true,
            negative_controls_pass: true,
            reward_integrity_pass: true,
        };
        let p = PairedSample {
            case_id: "case-1".into(),
            case_digest: h(),
            seed: 1,
            grader_digest: h(),
            runtime_digest: h(),
            critical: true,
            champion: SampleResult::Pass,
            challenger: SampleResult::Fail,
            champion_score_milli: 10,
            challenger_score_milli: 0,
        };
        assert_eq!(
            compare(&[p], &pass, 1).decision,
            Decision::RefusedCriticalRegression
        );
        let better = PairedSample {
            case_id: "case-2".into(),
            case_digest: h(),
            seed: 1,
            grader_digest: h(),
            runtime_digest: h(),
            critical: false,
            champion: SampleResult::Pass,
            challenger: SampleResult::Pass,
            champion_score_milli: 1,
            challenger_score_milli: 2,
        };
        let failed = PairedSample {
            challenger: SampleResult::Fail,
            champion_score_milli: 0,
            challenger_score_milli: 99,
            ..better.clone()
        };
        assert_eq!(compare(&[failed], &pass, 1).decision, Decision::Worse);
        assert_eq!(
            compare(std::slice::from_ref(&better), &pass, 2).decision,
            Decision::RefusedSmallSample
        );
        assert_eq!(
            compare(
                &[better],
                &SignalVector {
                    required_pass: true,
                    negative_controls_pass: false,
                    reward_integrity_pass: true
                },
                1
            )
            .decision,
            Decision::InvalidRewardIntegrity
        );
    }

    #[test]
    fn checked_examples_deserialize_without_unknown_fields_and_match_self_digests() {
        let case_json: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-case.example.json"
        ))
        .unwrap();
        let case: EvalCaseV1 = serde_json::from_value(case_json.clone()).unwrap();
        assert!(verify_case_v1(&case).is_ok());
        assert_eq!(serde_json::to_value(&case).unwrap(), case_json);
        let taskset_json: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/taskset.example.json"
        ))
        .unwrap();
        let taskset: TasksetV1 = serde_json::from_value(taskset_json.clone()).unwrap();
        assert!(verify_taskset_v1(&taskset).is_ok());
        assert_eq!(serde_json::to_value(&taskset).unwrap(), taskset_json);
        let grader_json: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/grader-bundle.example.json"
        ))
        .unwrap();
        let grader: GraderBundleV1 = serde_json::from_value(grader_json.clone()).unwrap();
        assert_eq!(
            canonical_digest_without_self(&grader).unwrap(),
            grader.sha256
        );
        assert!(verify_grader_contract(&grader).is_ok());
        assert_eq!(serde_json::to_value(&grader).unwrap(), grader_json);
        let sample_json: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-sample.example.json"
        ))
        .unwrap();
        let sample: EvalSampleV1 = serde_json::from_value(sample_json.clone()).unwrap();
        assert!(verify_sample(&sample).is_ok());
        assert_eq!(serde_json::to_value(&sample).unwrap(), sample_json);

        let unknown = r#"{\"schema\":\"harness.taskset.v1\",\"taskset_id\":\"x\",\"revision\":1,\"cases\":[],\"sha256\":\"x\",\"secret\":true}"#;
        assert!(serde_json::from_str::<TasksetV1>(unknown).is_err());

        let mut missing_receipt = sample_json;
        missing_receipt
            .as_object_mut()
            .unwrap()
            .remove("trace_digest");
        assert!(serde_json::from_value::<EvalSampleV1>(missing_receipt).is_err());

        let mut null_spec = case_json;
        null_spec["acceptance"][0]["spec"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<EvalCaseV1>(null_spec).is_err());

        let mut missing_license: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-case.example.json"
        ))
        .unwrap();
        missing_license["privacy"]
            .as_object_mut()
            .unwrap()
            .remove("license");
        assert!(serde_json::from_value::<EvalCaseV1>(missing_license).is_err());

        let mut null_seeds: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-case.example.json"
        ))
        .unwrap();
        null_seeds["runtime"]["seeds"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<EvalCaseV1>(null_seeds).is_err());
    }

    #[test]
    fn case_and_grader_contracts_reject_boundary_violations() {
        let mut case: EvalCaseV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-case.example.json"
        ))
        .unwrap();
        case.source.locator.clear();
        assert!(verify_case_v1(&case).is_err());

        let mut case: EvalCaseV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/eval-case.example.json"
        ))
        .unwrap();
        case.runtime.seeds = Some(vec![1, 1]);
        assert!(verify_case_v1(&case).is_err());

        let mut grader: GraderBundleV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/grader-bundle.example.json"
        ))
        .unwrap();
        grader.hard_gates = vec!["missing".into()];
        assert!(verify_grader_contract(&grader).is_err());

        let mut grader: GraderBundleV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/grader-bundle.example.json"
        ))
        .unwrap();
        grader.hard_gates.push(grader.hard_gates[0].clone());
        assert!(verify_grader_contract(&grader).is_err());
    }
}
