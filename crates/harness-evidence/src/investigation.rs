//! Validation boundary for immutable, read-only investigation artifacts.

use harness_domain::InvestigationArtifact;
use harness_store::Store;

use crate::EvidenceError;

/// The only artifact intake boundary exposed to controller code. It records
/// evidence and does not create implementation work, publish, or grant a
/// mutable lease.
#[derive(Clone)]
pub struct InvestigationEvidenceService {
    store: Store,
}

impl InvestigationEvidenceService {
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn record(
        &self,
        artifact: &InvestigationArtifact,
    ) -> Result<InvestigationArtifact, EvidenceError> {
        artifact
            .validate()
            .map_err(|error| EvidenceError::Invalid(error.to_string()))?;
        Ok(self.store.record_investigation_artifact(artifact)?)
    }
}
