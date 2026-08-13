use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawEventReceipt {
    pub id: i64,
    /// Controller-owned execution scope (for example, a durable agent session
    /// identity). Raw protocol thread/turn fields are never topology inputs.
    #[serde(default)]
    pub execution_scope_id: Option<String>,
    /// Controller-owned lifecycle grouping identity. It is intentionally
    /// distinct from a raw protocol item id.
    #[serde(default)]
    pub lifecycle_group_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub direction: String,
    pub method: String,
    pub request_id: Option<String>,
    pub received_at: i64,
    pub payload: Value,
    pub payload_sha256: String,
    pub source_sequence: Option<String>,
    pub redaction_class: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructuralReceipt {
    pub id: String,
    pub kind: String,
    pub occurred_at: Option<i64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainEventReceipt {
    pub id: i64,
    pub event_type: String,
    pub occurred_at: i64,
    pub payload: Value,
    pub payload_sha256: String,
    pub redaction_class: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceRelationKind {
    Next,
    ContextParent,
    ToolResultOf,
    SpawnedBy,
    JoinedInto,
    CompactedFrom,
    RetryOf,
    DerivedFrom,
    Supersedes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationInput {
    pub from: String,
    pub to: String,
    pub kind: TraceRelationKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceInput {
    pub trace_id: String,
    pub run_id: String,
    pub task_attempt_id: Option<String>,
    pub runtime_digest: String,
    pub redaction_policy_digest: String,
    pub sensitivity: String,
    pub raw_events: Vec<RawEventReceipt>,
    #[serde(default)]
    pub domain_events: Vec<DomainEventReceipt>,
    #[serde(default)]
    pub structural_receipts: Vec<StructuralReceipt>,
    /// Relations are based on explicit durable identities: `raw:<id>` or
    /// `structural:<id>`. The projector never guesses cross-branch causality.
    #[serde(default)]
    pub relations: Vec<RelationInput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum SourceReceipt {
    RawEvent { raw_event_id: i64 },
    DomainEvent { domain_event_id: i64 },
    Structural { receipt_id: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceNode {
    pub id: String,
    pub kind: String,
    pub content_sha256: String,
    pub source_receipts: Vec<SourceReceipt>,
    pub redaction_class: String,
    pub timestamp_ms: Option<i64>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceEdge {
    pub from: String,
    pub to: String,
    pub kind: TraceRelationKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceBranch {
    pub id: String,
    pub root_node_id: String,
    pub leaf_node_id: String,
    pub node_ids: Vec<String>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectionDiagnostic {
    pub code: String,
    pub detail: String,
    pub source_receipts: Vec<SourceReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceManifest {
    pub schema: String,
    pub trace_id: String,
    pub run_id: String,
    pub task_attempt_id: Option<String>,
    pub source_event_range: Option<SourceEventRange>,
    pub runtime_digest: String,
    pub redaction_policy_digest: String,
    pub sensitivity: String,
    pub nodes: Vec<TraceNode>,
    pub edges: Vec<TraceEdge>,
    pub branches: Vec<TraceBranch>,
    pub diagnostics: Vec<ProjectionDiagnostic>,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceEventRange {
    pub first_id: i64,
    pub last_id: i64,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProjectionError {
    #[error("trace input has no source receipts")]
    EmptyInput,
    #[error("duplicate raw receipt id {0}")]
    DuplicateRawReceipt(i64),
    #[error("duplicate structural receipt id {0}")]
    DuplicateStructuralReceipt(String),
    #[error("invalid {field}: {value}")]
    InvalidInput { field: String, value: String },
    #[error("payload digest mismatch for {receipt}")]
    PayloadDigestMismatch { receipt: String },
    #[error("relation references unknown receipt {0}")]
    UnknownRelationReceipt(String),
    #[error("relation would introduce a cycle: {from} -> {to}")]
    Cycle { from: String, to: String },
}
