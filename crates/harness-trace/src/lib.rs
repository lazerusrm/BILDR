//! Deterministic, redacted projection of durable execution receipts into a trace DAG.
//!
//! This crate deliberately has no database or runtime dependency. Callers provide a
//! bounded snapshot from the raw-event authority and persist the resulting immutable
//! manifest through their own envelope store.

mod canonical;
mod model;
mod project;
mod redaction;
mod validate;

pub use model::{
    DomainEventReceipt, ProjectionDiagnostic, ProjectionError, RawEventReceipt, RelationInput,
    SourceReceipt, StructuralReceipt, TraceInput, TraceManifest, TraceNode, TraceRelationKind,
};
pub use project::project;
pub use validate::validate_manifest;
