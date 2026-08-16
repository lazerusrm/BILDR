//! Durable, source-owned operator-control repositories.
//!
//! Each submodule owns one bounded projection/repository.  This keeps control
//! plane behavior out of the existing query monolith and makes its event and
//! snapshot custody independently testable.

mod approval;
mod attention;
pub(crate) mod correlation;
mod external_conditions;
mod investigations;
mod knowledge;
mod liveness;
mod notifications;
mod progress;
mod reconciliation;
mod snapshots;
mod topology;

pub use attention::*;
pub(crate) use liveness::checked_observation_row;
pub(crate) use reconciliation::{
    checked_action_receipt_row, checked_finding_row, checked_reconciliation_row,
};
pub use snapshots::*;
