//! Controller-owned liveness interventions.
//!
//! Each exposed operation is a closed, independently auditable controller
//! action. The store owns the atomic state/receipt/audit transition; this
//! layer serializes it with other controller mutations.

use harness_domain::{LivenessEpisode, LivenessEpisodeId};

use crate::{Orchestrator, OrchestratorError};

impl Orchestrator {
    /// Pauses the scheduler only for the exact run already bound to a
    /// confirmed-stall or recovery-required liveness episode. It neither
    /// resumes nor retries work, and it cannot release any custody.
    pub async fn pause_scheduler_for_liveness_episode(
        &self,
        episode_id: &LivenessEpisodeId,
        expected_version: u64,
        actor: &str,
    ) -> Result<LivenessEpisode, OrchestratorError> {
        let _guard = self.operation_lock.lock().await;
        Ok(self.store.execute_pause_for_operator_intervention(
            episode_id,
            expected_version,
            actor,
        )?)
    }
}
