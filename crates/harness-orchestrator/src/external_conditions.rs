//! Controller-owned external-condition registration.
//!
//! Provider adapters remain disabled until their policy/configuration and
//! independent rollout evidence exist. The controller clock is different: it
//! has no provider credential or result parser, so a local absolute-time gate
//! can be registered safely as a wake-only fact today.

use harness_domain::{
    ExternalCondition, ExternalConditionAdapter, ExternalConditionId, ExternalConditionOwnerType,
    ExternalConditionPollPolicy, ExternalConditionState, RunId, now_ms,
};
use serde_json::json;
use sha2::{Digest, Sha256};

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
}
