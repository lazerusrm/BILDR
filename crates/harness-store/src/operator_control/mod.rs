//! Durable, source-owned operator-control repositories.
//!
//! Each submodule owns one bounded projection/repository.  This keeps control
//! plane behavior out of the legacy query monolith and makes its event and
//! snapshot custody independently testable.

mod approval;
mod attention;
mod correlation;
mod external_conditions;
mod investigations;
mod liveness;
mod progress;
mod reconciliation;
mod snapshots;

pub use attention::*;
pub use snapshots::*;
