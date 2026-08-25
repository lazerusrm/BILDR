//! Pure M4/M5 promotion contracts; validation and command construction only.
mod avo;
mod digest;
mod experiment;
mod promotion;
mod telemetry;
pub use avo::*;
pub use digest::*;
pub use experiment::*;
pub use promotion::*;
pub use telemetry::*;

#[cfg(test)]
mod tests {
    use super::*;
    const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    fn receipt(kind: ReceiptKind, id: &str) -> Receipt {
        Receipt {
            kind,
            id: id.into(),
            digest: H.into(),
        }
    }
    fn authority() -> ShadowAuthority {
        ShadowAuthority {
            isolation_custody_receipt: receipt(ReceiptKind::IsolationCustody, "custody"),
            task_family: TaskFamily::ShadowReplay,
            edit_dimensions: vec![EditDimension::Prompt],
            assignment_receipt: receipt(ReceiptKind::Assignment, "assignment"),
            fallback_receipt: receipt(ReceiptKind::Fallback, "fallback"),
        }
    }
    fn experiment() -> ExperimentV1 {
        let mut value = ExperimentV1 {
            schema: "harness.experiment.v1".into(),
            experiment_id: "experiment-1".into(),
            candidate_id: "candidate-1".into(),
            candidate_receipt: receipt(ReceiptKind::Candidate, "candidate-1"),
            champion_bundle_id: "champion-1".into(),
            champion_bundle_receipt: receipt(ReceiptKind::ChampionBundle, "champion-1"),
            challenger_bundle_id: "challenger-1".into(),
            challenger_bundle_receipt: receipt(ReceiptKind::ChallengerBundle, "challenger-1"),
            runtime_policy_digest: H.into(),
            stages: vec![
                StageReceipt::pending(Stage::Offline),
                StageReceipt::pending(Stage::Holdout),
                StageReceipt::pending(Stage::Shadow),
                StageReceipt::pending(Stage::Canary),
            ],
            hard_gates: vec![GateReceipt {
                gate_id: "gate-1".into(),
                passed: true,
                evidence: receipt(ReceiptKind::HardGate, "gate-receipt"),
            }],
            state: ExperimentState::Proposed,
            sha256: String::new(),
        };
        value.sha256 = digest_without_self(&value).unwrap();
        value
    }
    #[test]
    fn states_have_exact_stage_vectors_and_advancement_requires_prior_evidence() {
        let initial = experiment();
        assert!(verify_experiment(&initial).is_ok());
        let running = advance_stage(&initial, Stage::Offline).unwrap();
        assert_eq!(running.stages[0].state, StageState::Running);
        assert_eq!(
            advance_stage(&running, Stage::Holdout),
            Err(ContractError::StageOrder)
        );
        let mut canary = initial.clone();
        for stage in canary.stages.iter_mut().take(3) {
            stage.state = StageState::Passed;
            stage.evidence = Some(StageEvidence {
                stage: stage.stage,
                id: format!("evidence-{:?}", stage.stage).to_lowercase(),
                digest: H.into(),
            });
        }
        canary.stages[3].state = StageState::Running;
        canary.state = ExperimentState::CanaryRunning;
        canary.sha256 = digest_without_self(&canary).unwrap();
        assert!(verify_experiment(&canary).is_ok());
        canary.stages[3].state = StageState::Pending;
        canary.sha256 = digest_without_self(&canary).unwrap();
        assert!(verify_experiment(&canary).is_err());
        let mut swapped = initial;
        swapped.candidate_receipt.id = "other-candidate".into();
        swapped.sha256 = digest_without_self(&swapped).unwrap();
        assert!(verify_experiment(&swapped).is_err());
    }
    #[test]
    fn bounded_exposure_requires_authority_and_preserves_production() {
        let budget = ExposureBudget {
            max_samples: 2,
            max_cost_microusd: 10,
            critical_failures: 1,
        };
        let observation = ShadowObservation {
            production: ProductionResult::Passed,
            challenger: ChallengerResult::Failed,
            cost_microusd: 1,
            critical: true,
        };
        assert_eq!(
            observe_shadow(
                &ShadowAuthority {
                    edit_dimensions: vec![],
                    ..authority()
                },
                ShadowState::new(budget),
                observation
            ),
            Err(ContractError::Missing)
        );
        let state = observe_shadow(&authority(), ShadowState::new(budget), observation).unwrap();
        assert_eq!(state.production, ProductionResult::Passed);
        assert_eq!(state.stop, Some(StopReason::CriticalRegression));
        let canary = CanaryAuthority {
            shadow: authority(),
            operator_start_receipt: receipt(ReceiptKind::OperatorStart, "operator-start"),
        };
        assert_eq!(
            observe_canary(&canary, CanaryState::new(budget), observation)
                .unwrap()
                .stop,
            Some(StopReason::CriticalRegression)
        );
    }
    #[test]
    fn promotion_binds_an_exact_experiment_and_rollback_replays_only_exactly() {
        let mut exp = experiment();
        for stage in &mut exp.stages {
            stage.state = StageState::Passed;
            stage.evidence = Some(StageEvidence {
                stage: stage.stage,
                id: format!("receipt-{:?}", stage.stage).to_lowercase(),
                digest: H.into(),
            });
        }
        exp.state = ExperimentState::PromotionReview;
        exp.sha256 = digest_without_self(&exp).unwrap();
        let mut required = RequiredReceipts::all("receipt");
        for (target, source) in [
            (&mut required.offline, &exp.stages[0]),
            (&mut required.holdout, &exp.stages[1]),
            (&mut required.shadow, &exp.stages[2]),
            (&mut required.canary, &exp.stages[3]),
        ] {
            target.id = source.evidence.as_ref().unwrap().id.clone();
        }
        let mut decision = PromotionDecisionV1::approved(
            "promotion-1",
            "experiment-1",
            "candidate-1",
            "champion-1",
            "challenger-1",
            H,
            required,
            receipt(ReceiptKind::Rollback, "rollback-evidence"),
        );
        decision.experiment_digest = exp.sha256.clone();
        decision.runtime_policy_digest = exp.runtime_policy_digest.clone();
        decision.sha256 = digest_without_self(&decision).unwrap();
        assert!(verify_promotion_against_experiment(&decision, &exp).is_ok());
        exp.stages[3].state = StageState::Running;
        exp.sha256 = digest_without_self(&exp).unwrap();
        assert!(verify_promotion_against_experiment(&decision, &exp).is_err());
        exp.stages[3].state = StageState::Passed;
        exp.hard_gates[0].passed = false;
        exp.sha256 = digest_without_self(&exp).unwrap();
        assert!(verify_promotion_against_experiment(&decision, &exp).is_err());
        let rollback = RollbackContract::from_promotion(
            &decision,
            "challenger-1",
            receipt(ReceiptKind::Rollback, "rollback-command"),
        )
        .unwrap();
        assert_eq!(
            rollback_binding_command(&rollback, "other", H, None),
            Err(ContractError::Stale)
        );
        assert_eq!(
            rollback_binding_command(&rollback, "champion-1", H, Some(&rollback.sha256))
                .unwrap()
                .1,
            RollbackReplay::ExactReplay
        );
    }
    #[test]
    fn health_requires_complete_bound_windowed_telemetry() {
        let metrics = [
            Metric::Quality,
            Metric::Correction,
            Metric::Regression,
            Metric::Cost,
            Metric::Latency,
            Metric::Distribution,
            Metric::Grader,
            Metric::Taskset,
        ]
        .map(|metric| MetricTelemetry {
            metric,
            present: true,
            within_threshold: true,
            source_receipt: receipt(ReceiptKind::Telemetry, "telemetry"),
            observation_start_ms: 1,
            observation_end_ms: 2,
        });
        let batch = HealthBatch {
            threshold_policy_digest: H.into(),
            observation_start_ms: 1,
            observation_end_ms: 2,
            metrics: metrics.to_vec(),
        };
        assert_eq!(promotion_health(&batch).state, HealthState::Healthy);
        let partial = HealthBatch {
            metrics: batch.metrics[..2].to_vec(),
            ..batch
        };
        assert_eq!(promotion_health(&partial).state, HealthState::Unknown);
    }
    #[test]
    fn contract_examples_are_typed_canonical_and_valid() {
        let experiment: ExperimentV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/experiment.example.json"
        ))
        .unwrap();
        assert_eq!(digest_without_self(&experiment).unwrap(), experiment.sha256);
        assert!(verify_experiment(&experiment).is_ok());

        let promotion: PromotionDecisionV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/promotion-decision.example.json"
        ))
        .unwrap();
        assert_eq!(digest_without_self(&promotion).unwrap(), promotion.sha256);
        assert!(verify_promotion(&promotion).is_ok());

        let rollback: RollbackContract = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/rollback.example.json"
        ))
        .unwrap();
        assert_eq!(digest_without_self(&rollback).unwrap(), rollback.sha256);
        assert!(validate_rollback(&rollback, "challenger-1", H).is_ok());
    }
}
