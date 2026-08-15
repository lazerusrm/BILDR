//! Controller-owned external-condition registration.
//!
//! Provider adapters remain disabled until their policy/configuration and
//! independent rollout evidence exist. The controller clock and a
//! controller-owned repository filesystem are different: neither accepts a
//! provider endpoint, command, credential, or untrusted result parser.

use std::path::Path;

use fs2::available_space;
use harness_domain::{
    ConditionObservation, ConditionObservationId, ExternalCondition, ExternalConditionAdapter,
    ExternalConditionId, ExternalConditionOwnerType, ExternalConditionPollPolicy,
    ExternalConditionState, LocalCapacitySpec, RunId, RunSummary, now_ms,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::{Orchestrator, OrchestratorError};

impl Orchestrator {
    /// Registers one local absolute-time gate owned by an existing run.
    ///
    /// This boundary deliberately accepts no URL, command, credential, or
    /// provider result. Later reconciliation may record one immutable
    /// observation and material event, but it cannot resume or mutate work.
    /// An identical request returns the original condition id.
    pub fn register_run_time_gate(
        &self,
        run_id: &RunId,
        not_before_ms: i64,
        deadline_ms: Option<i64>,
    ) -> Result<ExternalCondition, OrchestratorError> {
        if not_before_ms < 0 {
            return Err(OrchestratorError::Validation(
                "time-gate not_before_ms must be a UTC epoch millisecond timestamp".to_owned(),
            ));
        }
        if deadline_ms.is_some_and(|deadline| deadline < not_before_ms) {
            return Err(OrchestratorError::Validation(
                "time-gate deadline_ms must not precede not_before_ms".to_owned(),
            ));
        }
        let run = self.store.run(run_id)?;
        let source_identity_digest =
            time_gate_source_identity(&run.id, &run.base_sha, not_before_ms, deadline_ms);
        let source_id = format!("operator-time-gate-{}", &source_identity_digest[..32]);
        let registered_at_ms = now_ms();
        let mut condition = ExternalCondition {
            schema: "harness.external-condition.v1".to_owned(),
            condition_id: ExternalConditionId::new(),
            owner_type: ExternalConditionOwnerType::Run,
            owner_id: run.id.to_string(),
            adapter: ExternalConditionAdapter::TimeGate,
            source_id,
            spec: json!({"not_before_ms": not_before_ms}),
            state: ExternalConditionState::Open,
            sequence: 0,
            poll_policy: ExternalConditionPollPolicy {
                initial_ms: 1_000,
                maximum_ms: 60_000,
                deadline_ms,
            },
            source_identity_digest,
            last_observation: None,
            version: 1,
            opened_at_ms: registered_at_ms,
            updated_at_ms: registered_at_ms,
            sha256: String::new(),
        };
        condition.sha256 = condition
            .digest()
            .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
        Ok(self
            .store
            .register_or_read_external_condition_by_source_identity(&condition)?)
    }

    /// Registers one repository-root filesystem capacity gate owned by an
    /// existing run. The selected filesystem is derived from durable run and
    /// repository custody; callers cannot select a path, provider, command,
    /// or result-to-action mapping.
    ///
    /// A satisfied or expired gate later emits one material controller event
    /// solely to wake observation/supervision. It cannot resume, retry,
    /// release, or otherwise mutate work.
    pub fn register_run_local_capacity_gate(
        &self,
        run_id: &RunId,
        minimum_available_bytes: u64,
        deadline_ms: Option<i64>,
    ) -> Result<ExternalCondition, OrchestratorError> {
        let spec = LocalCapacitySpec {
            schema: LocalCapacitySpec::SCHEMA.to_owned(),
            resource: LocalCapacitySpec::RESOURCE.to_owned(),
            minimum_available_bytes,
        };
        spec.validate()
            .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
        if deadline_ms.is_some_and(|deadline| deadline < 0) {
            return Err(OrchestratorError::Validation(
                "local-capacity deadline_ms must be a UTC epoch millisecond timestamp".to_owned(),
            ));
        }
        let run = self.store.run(run_id)?;
        let repository = self.store.repository(&run.repository_id)?;
        let source_identity_digest = local_capacity_source_identity(
            &run.id,
            &run.base_sha,
            &repository.id,
            &repository.root_path,
            &spec,
            deadline_ms,
        );
        let source_id = format!("operator-local-capacity-{}", &source_identity_digest[..32]);
        let registered_at_ms = now_ms();
        let mut condition = ExternalCondition {
            schema: "harness.external-condition.v1".to_owned(),
            condition_id: ExternalConditionId::new(),
            owner_type: ExternalConditionOwnerType::Run,
            owner_id: run.id.to_string(),
            adapter: ExternalConditionAdapter::HardwareCapacity,
            source_id,
            spec: serde_json::to_value(spec)
                .map_err(|error| OrchestratorError::Validation(error.to_string()))?,
            state: ExternalConditionState::Open,
            sequence: 0,
            poll_policy: ExternalConditionPollPolicy {
                initial_ms: 1_000,
                maximum_ms: 60_000,
                deadline_ms,
            },
            source_identity_digest,
            last_observation: None,
            version: 1,
            opened_at_ms: registered_at_ms,
            updated_at_ms: registered_at_ms,
            sha256: String::new(),
        };
        condition.sha256 = condition
            .digest()
            .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
        Ok(self
            .store
            .register_or_read_external_condition_by_source_identity(&condition)?)
    }

    /// Reconciles the bounded local-capacity adapter for one run. This runs
    /// before runtime readiness because a repository filesystem observation
    /// has no App Server dependency. Its only controller-visible effect is a
    /// durable condition event; the scheduler still receives no result-to-
    /// action mapping from this adapter.
    pub(crate) fn reconcile_local_capacity_conditions(
        &self,
        run: &RunSummary,
    ) -> Result<u32, OrchestratorError> {
        self.reconcile_local_capacity_conditions_with_observer(run, |path| {
            available_space(path).map_err(|_| ())
        })
    }

    pub(crate) fn reconcile_local_capacity_conditions_with_observer<F>(
        &self,
        run: &RunSummary,
        mut observe: F,
    ) -> Result<u32, OrchestratorError>
    where
        F: FnMut(&Path) -> Result<u64, ()>,
    {
        let repository = match self.store.repository(&run.repository_id) {
            Ok(repository) => repository,
            Err(error) => {
                warn!(
                    run_id = %run.id,
                    %error,
                    "local-capacity condition cannot resolve controller-owned repository custody"
                );
                return Ok(0);
            }
        };
        let now = now_ms();
        let mut advanced = 0_u32;
        let mut cursor: Option<(i64, ExternalConditionId)> = None;
        loop {
            let conditions = self
                .store
                .list_open_external_conditions_for_owner_adapter_before(
                    ExternalConditionOwnerType::Run,
                    run.id.as_str(),
                    ExternalConditionAdapter::HardwareCapacity,
                    cursor
                        .as_ref()
                        .map(|(updated_at_ms, condition_id)| (*updated_at_ms, condition_id)),
                    crate::EXTERNAL_CONDITION_SCAN_LIMIT,
                )?;
            let Some(last) = conditions.last() else {
                break;
            };
            let next_cursor = (last.updated_at_ms, last.condition_id.clone());
            for condition in conditions {
                if !local_capacity_poll_due(&condition, now) {
                    continue;
                }
                let expected_identity = match local_capacity_expected_identity(
                    run,
                    &repository.id,
                    &repository.root_path,
                    &condition,
                ) {
                    Ok(identity) => identity,
                    Err(error) => {
                        warn!(
                            run_id = %run.id,
                            condition_id = %condition.condition_id,
                            %error,
                            "local-capacity condition has an invalid stored specification"
                        );
                        continue;
                    }
                };
                let availability = observe(Path::new(&repository.root_path));
                let Some(outcome) =
                    local_capacity_outcome(&condition, &expected_identity, now, availability)
                else {
                    continue;
                };
                let (state, event_type, payload) = match outcome {
                    LocalCapacityOutcome::Waiting {
                        available_bytes,
                        minimum_available_bytes,
                    } => (
                        ExternalConditionState::Open,
                        None,
                        json!({
                            "adapter": "hardware_capacity",
                            "resource": LocalCapacitySpec::RESOURCE,
                            "available_bytes": available_bytes,
                            "minimum_available_bytes": minimum_available_bytes,
                            "reason": "below_threshold",
                            "consequential_action": "none",
                        }),
                    ),
                    LocalCapacityOutcome::Satisfied {
                        available_bytes,
                        minimum_available_bytes,
                    } => (
                        ExternalConditionState::Satisfied,
                        Some("external_condition.local_capacity_satisfied"),
                        json!({
                            "adapter": "hardware_capacity",
                            "resource": LocalCapacitySpec::RESOURCE,
                            "available_bytes": available_bytes,
                            "minimum_available_bytes": minimum_available_bytes,
                            "reason": "threshold_reached",
                            "consequential_action": "none",
                        }),
                    ),
                    LocalCapacityOutcome::DeadlineElapsed {
                        minimum_available_bytes,
                    } => (
                        ExternalConditionState::Unsatisfied,
                        Some("external_condition.local_capacity_deadline_elapsed"),
                        json!({
                            "adapter": "hardware_capacity",
                            "resource": LocalCapacitySpec::RESOURCE,
                            "minimum_available_bytes": minimum_available_bytes,
                            "reason": "deadline_elapsed",
                            "consequential_action": "none",
                        }),
                    ),
                    LocalCapacityOutcome::SourceUnavailable {
                        minimum_available_bytes,
                    } => (
                        ExternalConditionState::Unknown,
                        Some("external_condition.local_capacity_source_unavailable"),
                        json!({
                            "adapter": "hardware_capacity",
                            "resource": LocalCapacitySpec::RESOURCE,
                            "minimum_available_bytes": minimum_available_bytes,
                            "reason": "source_unavailable",
                            "consequential_action": "none",
                        }),
                    ),
                    LocalCapacityOutcome::ContinuityBreak {
                        minimum_available_bytes,
                    } => (
                        ExternalConditionState::Unknown,
                        Some("external_condition.local_capacity_continuity_break"),
                        json!({
                            "adapter": "hardware_capacity",
                            "resource": LocalCapacitySpec::RESOURCE,
                            "minimum_available_bytes": minimum_available_bytes,
                            "reason": "controller_source_identity_changed",
                            "consequential_action": "none",
                        }),
                    ),
                };
                let source_event_id = format!(
                    "local-capacity-{}-{}",
                    &condition.sha256[..24],
                    condition.sequence.saturating_add(1),
                );
                let mut observation = ConditionObservation {
                    schema: "harness.condition-observation.v1".to_owned(),
                    observation_id: ConditionObservationId::new(),
                    condition_id: condition.condition_id.clone(),
                    source_event_id,
                    sequence: condition.sequence.saturating_add(1),
                    observed_at_ms: now,
                    state,
                    payload,
                    sha256: String::new(),
                };
                observation.sha256 = observation
                    .digest()
                    .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
                let recorded = match event_type {
                    Some(event_type) => self.store.record_external_condition_observation_and_emit(
                        &condition.condition_id,
                        condition.version,
                        &observation,
                        &run.id,
                        event_type,
                        &json!({
                            "adapter": "hardware_capacity",
                            "state": match state {
                                ExternalConditionState::Satisfied => "satisfied",
                                ExternalConditionState::Unsatisfied => "unsatisfied",
                                ExternalConditionState::Unknown => "unknown",
                                ExternalConditionState::Open | ExternalConditionState::Cancelled => {
                                    return Err(OrchestratorError::Protocol(
                                        "local-capacity terminal event has a nonterminal state".to_owned(),
                                    ));
                                }
                            },
                            "consequential_action": "none",
                        }),
                    ),
                    None => self.store.record_external_condition_observation(
                        &condition.condition_id,
                        condition.version,
                        &observation,
                    ),
                };
                match recorded {
                    Ok(_) => advanced = advanced.saturating_add(1),
                    Err(error) => warn!(
                        run_id = %run.id,
                        condition_id = %condition.condition_id,
                        %error,
                        "local-capacity condition could not be recorded and remains unresolved"
                    ),
                }
            }
            cursor = Some(next_cursor);
        }
        Ok(advanced)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalCapacityOutcome {
    Waiting {
        available_bytes: u64,
        minimum_available_bytes: u64,
    },
    Satisfied {
        available_bytes: u64,
        minimum_available_bytes: u64,
    },
    DeadlineElapsed {
        minimum_available_bytes: u64,
    },
    SourceUnavailable {
        minimum_available_bytes: u64,
    },
    ContinuityBreak {
        minimum_available_bytes: u64,
    },
}

fn local_capacity_expected_identity(
    run: &RunSummary,
    repository_id: &harness_domain::RepositoryId,
    repository_root: &str,
    condition: &ExternalCondition,
) -> Result<String, OrchestratorError> {
    let spec = condition
        .local_capacity_spec()
        .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
    spec.validate()
        .map_err(|error| OrchestratorError::Validation(error.to_string()))?;
    Ok(local_capacity_source_identity(
        &run.id,
        &run.base_sha,
        repository_id,
        repository_root,
        &spec,
        condition.poll_policy.deadline_ms,
    ))
}

fn local_capacity_outcome(
    condition: &ExternalCondition,
    expected_identity: &str,
    now: i64,
    available_bytes: Result<u64, ()>,
) -> Option<LocalCapacityOutcome> {
    if condition.adapter != ExternalConditionAdapter::HardwareCapacity
        || condition.state != ExternalConditionState::Open
    {
        return None;
    }
    let spec = condition.local_capacity_spec().ok()?;
    if spec.validate().is_err() {
        return None;
    }
    if condition.source_identity_digest != expected_identity {
        return Some(LocalCapacityOutcome::ContinuityBreak {
            minimum_available_bytes: spec.minimum_available_bytes,
        });
    }
    if condition
        .poll_policy
        .deadline_ms
        .is_some_and(|deadline| now >= deadline)
    {
        return Some(LocalCapacityOutcome::DeadlineElapsed {
            minimum_available_bytes: spec.minimum_available_bytes,
        });
    }
    match available_bytes {
        Ok(available_bytes) if available_bytes >= spec.minimum_available_bytes => {
            Some(LocalCapacityOutcome::Satisfied {
                available_bytes,
                minimum_available_bytes: spec.minimum_available_bytes,
            })
        }
        Ok(available_bytes) => Some(LocalCapacityOutcome::Waiting {
            available_bytes,
            minimum_available_bytes: spec.minimum_available_bytes,
        }),
        Err(()) => Some(LocalCapacityOutcome::SourceUnavailable {
            minimum_available_bytes: spec.minimum_available_bytes,
        }),
    }
}

fn local_capacity_poll_due(condition: &ExternalCondition, now: i64) -> bool {
    let Some(last_observation) = &condition.last_observation else {
        return true;
    };
    let interval = condition_poll_interval_ms(condition);
    now.saturating_sub(last_observation.observed_at_ms) >= interval
}

fn condition_poll_interval_ms(condition: &ExternalCondition) -> i64 {
    let exponent = condition.sequence.saturating_sub(1).min(62);
    let multiplier = 1_u64 << exponent;
    let interval = condition
        .poll_policy
        .initial_ms
        .saturating_mul(multiplier)
        .min(condition.poll_policy.maximum_ms);
    i64::try_from(interval).unwrap_or(i64::MAX)
}

fn time_gate_source_identity(
    run_id: &RunId,
    base_sha: &str,
    not_before_ms: i64,
    deadline_ms: Option<i64>,
) -> String {
    let deadline = deadline_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned());
    hex::encode(Sha256::digest(
        format!(
            "harness.operator-time-gate-source.v1\\0{run_id}\\0{base_sha}\\0{not_before_ms}\\0{deadline}"
        )
        .as_bytes(),
    ))
}

fn local_capacity_source_identity(
    run_id: &RunId,
    base_sha: &str,
    repository_id: &harness_domain::RepositoryId,
    repository_root: &str,
    spec: &LocalCapacitySpec,
    deadline_ms: Option<i64>,
) -> String {
    let deadline = deadline_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned());
    hex::encode(Sha256::digest(
        format!(
            "harness.operator-local-capacity-source.v1\\0{run_id}\\0{base_sha}\\0{repository_id}\\0{repository_root}\\0{}\\0{}\\0{deadline}",
            spec.resource, spec.minimum_available_bytes,
        )
        .as_bytes(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_identity_binds_the_exact_run_revision_and_window() {
        let run = RunId::from("run-1");
        let base = "a".repeat(40);
        assert_eq!(
            time_gate_source_identity(&run, &base, 10, Some(20)),
            time_gate_source_identity(&run, &base, 10, Some(20)),
        );
        assert_ne!(
            time_gate_source_identity(&run, &base, 10, Some(20)),
            time_gate_source_identity(&run, &base, 11, Some(20)),
        );
    }

    #[test]
    fn local_capacity_identity_binds_controller_owned_repository_and_threshold() {
        let run = RunId::from("run-1");
        let repository = harness_domain::RepositoryId::from("repo-1");
        let spec = LocalCapacitySpec {
            schema: LocalCapacitySpec::SCHEMA.to_owned(),
            resource: LocalCapacitySpec::RESOURCE.to_owned(),
            minimum_available_bytes: 1_024,
        };
        let first = local_capacity_source_identity(
            &run,
            &"a".repeat(40),
            &repository,
            "/controller/repository-root",
            &spec,
            Some(20),
        );
        assert_eq!(
            first,
            local_capacity_source_identity(
                &run,
                &"a".repeat(40),
                &repository,
                "/controller/repository-root",
                &spec,
                Some(20),
            )
        );
        assert_ne!(
            first,
            local_capacity_source_identity(
                &run,
                &"a".repeat(40),
                &repository,
                "/other-controller-root",
                &spec,
                Some(20),
            )
        );
    }

    fn local_capacity_condition(deadline_ms: Option<i64>) -> ExternalCondition {
        let spec = LocalCapacitySpec {
            schema: LocalCapacitySpec::SCHEMA.to_owned(),
            resource: LocalCapacitySpec::RESOURCE.to_owned(),
            minimum_available_bytes: 100,
        };
        let mut condition = ExternalCondition {
            schema: "harness.external-condition.v1".to_owned(),
            condition_id: ExternalConditionId::new(),
            owner_type: ExternalConditionOwnerType::Run,
            owner_id: "run-1".to_owned(),
            adapter: ExternalConditionAdapter::HardwareCapacity,
            source_id: "operator-local-capacity-test".to_owned(),
            spec: serde_json::to_value(spec).expect("capacity spec serializes"),
            state: ExternalConditionState::Open,
            sequence: 0,
            poll_policy: ExternalConditionPollPolicy {
                initial_ms: 1_000,
                maximum_ms: 60_000,
                deadline_ms,
            },
            source_identity_digest: "a".repeat(64),
            last_observation: None,
            version: 1,
            opened_at_ms: 1,
            updated_at_ms: 1,
            sha256: String::new(),
        };
        condition.sha256 = condition.digest().expect("capacity condition digest");
        condition
    }

    #[test]
    fn local_capacity_outcome_preserves_deadline_and_continuity_boundaries() {
        let expired = local_capacity_condition(Some(10));
        assert_eq!(
            local_capacity_outcome(&expired, &"a".repeat(64), 10, Ok(1_000)),
            Some(LocalCapacityOutcome::DeadlineElapsed {
                minimum_available_bytes: 100
            }),
            "a late capacity reading cannot satisfy an expired gate"
        );
        let open = local_capacity_condition(Some(20));
        assert_eq!(
            local_capacity_outcome(&open, &"b".repeat(64), 10, Ok(1_000)),
            Some(LocalCapacityOutcome::ContinuityBreak {
                minimum_available_bytes: 100
            }),
            "a repository source change must not be reinterpreted as capacity"
        );
        assert_eq!(
            local_capacity_outcome(&open, &"a".repeat(64), 10, Ok(99)),
            Some(LocalCapacityOutcome::Waiting {
                available_bytes: 99,
                minimum_available_bytes: 100
            })
        );
    }
}
