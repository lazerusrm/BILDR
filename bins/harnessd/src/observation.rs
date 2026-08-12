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
                warn!(run_id = %run_id, %error, "trace observer isolated projection failure");
            }
        }
    }

    fn observe_run(&self, run_id: &harness_domain::RunId) -> Result<(), harness_store::StoreError> {
        // Outcome observations share the observer's read-only cadence but use
        // only typed Store authorities; they must not depend on trace cursor
        // movement to catch a newly recorded validation or lifecycle row.
        self.store.project_authoritative_outcomes(run_id)?;
        self.store.project_failures_for_run(run_id)?;
        let snapshot = self.store.trace_projection_snapshot(run_id)?;
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
