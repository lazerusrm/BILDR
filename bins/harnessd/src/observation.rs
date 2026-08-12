use harness_domain::{
    ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState, RetentionClass,
    SensitivityClass,
};
use harness_store::{NewImprovementRevision, Store, TraceProjectionSnapshot};
use harness_trace::{DomainEventReceipt, RawEventReceipt, StructuralReceipt, TraceInput, project};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing::warn;

const OBSERVER_KEY_PREFIX: &str = "trace-observer:v2:";
const OBSERVER_ERROR_KEY_PREFIX: &str = "trace-observer-error:v2:";
const REDACTION_POLICY_DIGEST: &str =
    "a9e49a7ef0d3d733c9f00c3ab0df99452aa3ccff99e83a47d8786c1e24c9981d";

#[derive(Clone)]
pub struct ObservationService {
    store: Store,
}

impl ObservationService {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn observe_once(&self) {
        let runs = match self.store.trace_projection_candidate_runs() {
            Ok(runs) => runs,
            Err(error) => {
                warn!(%error, "trace observer could not list candidate runs");
                return;
            }
        };
        for run_id in runs {
            if let Err(error) = self.observe_run(&run_id) {
                let key = format!("{OBSERVER_ERROR_KEY_PREFIX}{run_id}");
                let marker = projection_error_marker(&error);
                if self.store.runtime_metadata(&key).ok().flatten().as_ref() != Some(&marker) {
                    warn!(run_id = %run_id, %error, "trace observer isolated projection failure");
                    let _ = self.store.put_runtime_metadata(&key, &marker);
                }
            }
        }
    }

    fn observe_run(&self, run_id: &harness_domain::RunId) -> Result<(), harness_store::StoreError> {
        // Outcome observations share the observer's read-only cadence but use
        // only typed Store authorities; they must not depend on trace cursor
        // movement to catch a newly recorded validation or lifecycle row.
        self.store.project_authoritative_outcomes(run_id)?;
        self.store.project_failures_for_run(run_id)?;
        let error_key = format!("{OBSERVER_ERROR_KEY_PREFIX}{run_id}");
        if self
            .store
            .runtime_metadata(&error_key)?
            .as_ref()
            .is_some_and(snapshot_limit_is_deferred)
        {
            return Ok(());
        }
        let snapshot = self.store.trace_projection_snapshot(run_id)?;
        if self.store.runtime_metadata(&error_key)?.is_some() {
            self.store.delete_runtime_metadata(&error_key)?;
        }
        let runtime_digest = digest(&[
            "harness.trace.runtime.v1",
            &snapshot.base_sha,
            &snapshot.authority_digest,
            &snapshot.profile_digest,
        ]);
        let cursor = json!({"raw":snapshot.max_raw_event_id,"domain":snapshot.max_domain_event_id,"structural":snapshot.structural_digest,"runtime":runtime_digest});
        let cursor_digest = digest(&[
            "harness.trace.cursor.v2",
            &serde_json::to_string(&cursor).map_err(harness_store::StoreError::from)?,
        ]);
        let key = format!("{OBSERVER_KEY_PREFIX}{run_id}");
        if self.store.runtime_metadata(&key)?.as_ref() == Some(&json!(cursor_digest)) {
            return Ok(());
        }
        let manifest = project(&trace_input(snapshot, runtime_digest)?).map_err(|error| {
            harness_store::StoreError::Validation(format!("trace projection failed: {error}"))
        })?;
        let payload = serde_json::to_value(&manifest)?;
        let payload_sha256 = payload_digest(&payload)?;
        self.store
            .append_improvement_revision(&NewImprovementRevision {
                id: format!("trace-{}-{}", run_id, &cursor_digest[..16]),
                aggregate_kind: ImprovementRecordKind::Trace,
                aggregate_id: format!("trace-{run_id}"),
                schema: ImprovementSchema::TraceV2,
                state: ImprovementState::Captured,
                payload,
                payload_sha256,
                sensitivity: SensitivityClass::Internal,
                retention_class: RetentionClass::Governance,
                export_allowed: false,
                idempotency_key: format!("trace:v2:{run_id}:{cursor_digest}"),
                event_id: ImprovementEventId::from(format!("trace-event-{}", &cursor_digest[..20])),
                source_raw_event_id: None,
                source_domain_event_id: None,
            })?;
        self.store.put_runtime_metadata(&key, &json!(cursor_digest))
    }
}

fn snapshot_limit_is_deferred(marker: &Value) -> bool {
    marker.get("status") == Some(&json!("deferred"))
        && marker.get("reason") == Some(&json!("snapshot_limit"))
}

fn projection_error_marker(error: &harness_store::StoreError) -> Value {
    match error {
        harness_store::StoreError::TraceProjectionBound {
            raw_receipts,
            domain_receipts,
            payload_bytes,
        } => json!({
            "status":"deferred",
            "reason":"snapshot_limit",
            "raw_receipts":raw_receipts,
            "domain_receipts":domain_receipts,
            "payload_bytes":payload_bytes,
        }),
        _ => json!({
            "status":"deferred",
            "reason":"projection_failed",
        }),
    }
}

fn trace_input(
    snapshot: TraceProjectionSnapshot,
    runtime_digest: String,
) -> Result<TraceInput, harness_store::StoreError> {
    let relations = snapshot
        .relations
        .clone()
        .into_iter()
        .map(|row| match row.kind.as_str() {
            "context_parent" => Ok(harness_trace::RelationInput {
                from: row.from,
                to: row.to,
                kind: harness_trace::TraceRelationKind::ContextParent,
            }),
            "derived_from" => Ok(harness_trace::RelationInput {
                from: row.from,
                to: row.to,
                kind: harness_trace::TraceRelationKind::DerivedFrom,
            }),
            "spawned_by" => Ok(harness_trace::RelationInput {
                from: row.from,
                to: row.to,
                kind: harness_trace::TraceRelationKind::SpawnedBy,
            }),
            _ => Err(harness_store::StoreError::Validation(
                "unknown durable trace relation kind".to_owned(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TraceInput {
        trace_id: format!("trace-{}", snapshot.run_id),
        run_id: snapshot.run_id.to_string(),
        task_attempt_id: None,
        runtime_digest,
        redaction_policy_digest: REDACTION_POLICY_DIGEST.to_owned(),
        sensitivity: "internal".to_owned(),
        raw_events: snapshot
            .raw_events
            .into_iter()
            .map(|row| RawEventReceipt {
                id: row.id,
                execution_scope_id: row.agent_session_id,
                lifecycle_group_id: None,
                thread_id: row.thread_id,
                turn_id: row.turn_id,
                direction: row.direction,
                method: row.method,
                request_id: row.request_id,
                received_at: row.received_at,
                payload: row.payload,
                payload_sha256: row.payload_sha256,
                source_sequence: row.source_sequence,
                redaction_class: row.redaction_class,
            })
            .collect(),
        domain_events: snapshot
            .domain_events
            .into_iter()
            .map(|row| DomainEventReceipt {
                id: row.id,
                event_type: row.event_type,
                occurred_at: row.occurred_at,
                payload: row.payload,
                payload_sha256: row.payload_sha256,
                redaction_class: row.redaction_class,
            })
            .collect(),
        structural_receipts: snapshot
            .structural_receipts
            .into_iter()
            .map(|row| StructuralReceipt {
                id: row.id,
                kind: row.kind,
                occurred_at: row.occurred_at,
                metadata: serde_json::from_value(row.metadata).unwrap_or_default(),
            })
            .collect(),
        relations,
    })
}

fn digest(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn payload_digest(value: &Value) -> Result<String, harness_store::StoreError> {
    let serialized = serde_json::to_string(value)?;
    let mut hasher = Sha256::new();
    hasher.update(serialized.as_bytes());
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use harness_domain::{ImprovementRecordKind, RepositoryId, RunId, RunState};
    use harness_store::{NewRepository, NewRun};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn oversized_run_is_deferred_without_blocking_a_bounded_run() {
        let temp = TempDir::new().unwrap();
        let store = Store::in_memory(&temp.path().join("artifacts")).unwrap();
        let repository_id = RepositoryId::from("repository-observer-bounds");
        store
            .create_repository(&NewRepository {
                id: repository_id.clone(),
                profile_id: "fixture".to_owned(),
                profile_version: 1,
                display_name: "Observer bounds".to_owned(),
                root_path: PathBuf::from("/tmp/observer-bounds"),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "READY".to_owned(),
            })
            .unwrap();
        let large_run = RunId::from("run-observer-large");
        let small_run = RunId::from("run-observer-small");
        for run_id in [&large_run, &small_run] {
            store
                .create_run(&NewRun {
                    id: run_id.clone(),
                    repository_id: repository_id.clone(),
                    title: "Observer fixture".to_owned(),
                    objective: "Project a bounded trace".to_owned(),
                    mode: "standard".to_owned(),
                    publication_mode: "none".to_owned(),
                    state: RunState::Created.to_string(),
                    phase: "created".to_owned(),
                    base_ref: "main".to_owned(),
                    base_sha: "fixture".to_owned(),
                    authority_digest: "fixture".to_owned(),
                    profile_digest: "fixture".to_owned(),
                    codex_version: None,
                    protocol_schema_sha256: None,
                    requested_by: "test".to_owned(),
                    token_budget: None,
                })
                .unwrap();
        }
        store
            .transition_run(
                &large_run,
                RunState::Blocked,
                "observer_fixture",
                Some(1),
                Some(("budget_exhausted", "fixture")),
            )
            .unwrap();
        for _ in 0..10_000_i64 {
            store
                .emit_domain_event(
                    Some(&large_run),
                    "run",
                    large_run.as_str(),
                    "test.receipt",
                    &json!({}),
                    None,
                )
                .unwrap();
        }

        ObservationService::new(store.clone()).observe_once();

        assert_eq!(
            store
                .runtime_metadata(&format!("{OBSERVER_ERROR_KEY_PREFIX}{large_run}"))
                .unwrap(),
            Some(json!({
                "status":"deferred",
                "reason":"snapshot_limit",
                "raw_receipts":0,
                "domain_receipts":10_001,
                "payload_bytes":null,
            }))
        );
        assert!(
            store
                .runtime_metadata(&format!("{OBSERVER_KEY_PREFIX}{large_run}"))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .improvement_current_revision(
                    ImprovementRecordKind::Trace,
                    &format!("trace-{large_run}"),
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .improvement_current_revision(
                    ImprovementRecordKind::Trace,
                    &format!("trace-{small_run}"),
                )
                .unwrap()
                .is_some()
        );
        assert!(!store.outcome_vector(&large_run).unwrap().items.is_empty());
        let failures = store.failure_cluster_overview(&repository_id).unwrap();
        assert!(
            failures
                .iter()
                .any(|cluster| cluster.representative_run_id.as_ref() == Some(&large_run))
        );

        let marker = store
            .runtime_metadata(&format!("{OBSERVER_ERROR_KEY_PREFIX}{large_run}"))
            .unwrap();
        ObservationService::new(store.clone()).observe_once();
        assert_eq!(
            store
                .runtime_metadata(&format!("{OBSERVER_ERROR_KEY_PREFIX}{large_run}"))
                .unwrap(),
            marker
        );
    }
}
