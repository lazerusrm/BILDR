//! Pure, deterministic failure classification and human-curated cluster lineage.
//!
//! This module consumes only typed terminal codes and pre-scoped cost receipts.
//! Free-text reasons are deliberately absent from classification and fingerprints.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod learning_contracts;
pub use learning_contracts::*;

pub const FAILURE_TAXONOMY_VERSION: &str = "harness.failure-taxonomy.v1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Unknown,
    PolicyBlocked,
    BudgetExhausted,
    InfrastructureUnavailable,
    ProtocolError,
    IntegrationConflict,
    SourceFailure,
    Inconclusive,
    CancelledSuperseded,
}

impl FailureClass {
    #[must_use]
    pub fn classify_terminal_code(code: Option<&str>) -> Self {
        match code {
            Some("policy_blocked") => Self::PolicyBlocked,
            Some("budget_exhausted") => Self::BudgetExhausted,
            Some("infrastructure_unavailable") => Self::InfrastructureUnavailable,
            Some("protocol_error") => Self::ProtocolError,
            Some("integration_conflict") => Self::IntegrationConflict,
            Some("source_failure") => Self::SourceFailure,
            Some("inconclusive") => Self::Inconclusive,
            Some("cancelled_superseded") => Self::CancelledSuperseded,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalCode {
    PolicyBlocked,
    BudgetExhausted,
    InfrastructureUnavailable,
    ProtocolError,
    IntegrationConflict,
    SourceFailure,
    Inconclusive,
    CancelledSuperseded,
}

impl TerminalCode {
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "policy_blocked" => Self::PolicyBlocked,
            "budget_exhausted" => Self::BudgetExhausted,
            "infrastructure_unavailable" => Self::InfrastructureUnavailable,
            "protocol_error" => Self::ProtocolError,
            "integration_conflict" => Self::IntegrationConflict,
            "source_failure" => Self::SourceFailure,
            "inconclusive" => Self::Inconclusive,
            "cancelled_superseded" => Self::CancelledSuperseded,
            _ => return None,
        })
    }

    #[must_use]
    pub const fn class(self) -> FailureClass {
        match self {
            Self::PolicyBlocked => FailureClass::PolicyBlocked,
            Self::BudgetExhausted => FailureClass::BudgetExhausted,
            Self::InfrastructureUnavailable => FailureClass::InfrastructureUnavailable,
            Self::ProtocolError => FailureClass::ProtocolError,
            Self::IntegrationConflict => FailureClass::IntegrationConflict,
            Self::SourceFailure => FailureClass::SourceFailure,
            Self::Inconclusive => FailureClass::Inconclusive,
            Self::CancelledSuperseded => FailureClass::CancelledSuperseded,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureScope {
    AttemptTerminal,
    RunTerminal,
    TypedOutcome,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Unknown,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CostAttribution {
    /// A disjoint durable scope, such as one attempt's agent-session set.
    pub scope_id: Option<String>,
    pub lower_microusd: Option<u64>,
    pub upper_microusd: Option<u64>,
}

impl CostAttribution {
    #[must_use]
    pub fn known(scope_id: impl Into<String>, lower_microusd: u64, upper_microusd: u64) -> Self {
        Self {
            scope_id: Some(scope_id.into()),
            lower_microusd: Some(lower_microusd),
            upper_microusd: Some(upper_microusd),
        }
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            scope_id: None,
            lower_microusd: None,
            upper_microusd: None,
        }
    }

    fn valid(&self) -> bool {
        match (&self.scope_id, self.lower_microusd, self.upper_microusd) {
            (Some(scope), Some(lower), Some(upper)) => safe_token(scope) && lower <= upper,
            (None, None, None) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailureInput {
    pub occurrence_id: String,
    pub repository_id: String,
    pub source_id: String,
    pub scope: FailureScope,
    pub terminal_code: Option<TerminalCode>,
    pub severity: Severity,
    pub cost: CostAttribution,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailureOccurrence {
    pub occurrence_id: String,
    pub repository_id: String,
    pub source_id: String,
    pub scope: FailureScope,
    pub terminal_code: Option<TerminalCode>,
    pub automatic_class: FailureClass,
    pub severity: Severity,
    pub fingerprint: String,
    pub cost: CostAttribution,
    pub classification_revisions: Vec<ClassificationRevision>,
}

/// The deliberately small, stable wire projection for `harness.failure.v1`.
///
/// It omits mutable human-curation history. Those records have their own
/// append-only API and must not be confused with the observed occurrence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureWireOccurrence {
    pub schema: FailureWireSchema,
    pub id: String,
    pub repository_id: String,
    pub source: FailureWireSource,
    pub terminal_code: Option<TerminalCode>,
    pub automatic_class: FailureClass,
    pub severity: Severity,
    pub taxonomy_version: FailureTaxonomyVersion,
    pub fingerprint_sha256: String,
    pub cost: FailureWireCost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum FailureWireSchema {
    #[serde(rename = "harness.failure.v1")]
    V1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureWireSource {
    pub kind: FailureScope,
    pub id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum FailureTaxonomyVersion {
    #[serde(rename = "harness.failure-taxonomy.v1")]
    V1,
}

/// Schema-expressible wire cost. `additional_microusd` avoids a non-portable
/// JSON-Schema sibling comparison while preserving internal lower/upper bounds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FailureWireCost {
    Unknown,
    Known {
        scope_id: String,
        lower_microusd: u64,
        additional_microusd: u64,
    },
}

impl TryFrom<&CostAttribution> for FailureWireCost {
    type Error = LearningError;

    fn try_from(value: &CostAttribution) -> Result<Self, Self::Error> {
        match (&value.scope_id, value.lower_microusd, value.upper_microusd) {
            (None, None, None) => Ok(Self::Unknown),
            (Some(scope_id), Some(lower_microusd), Some(upper_microusd))
                if !scope_id.is_empty() =>
            {
                Ok(Self::Known {
                    scope_id: scope_id.clone(),
                    lower_microusd,
                    additional_microusd: upper_microusd
                        .checked_sub(lower_microusd)
                        .ok_or(LearningError::InvalidCost)?,
                })
            }
            _ => Err(LearningError::InvalidCost),
        }
    }
}

impl TryFrom<FailureWireCost> for CostAttribution {
    type Error = LearningError;

    fn try_from(value: FailureWireCost) -> Result<Self, Self::Error> {
        match value {
            FailureWireCost::Unknown => Ok(Self::unknown()),
            FailureWireCost::Known {
                scope_id,
                lower_microusd,
                additional_microusd,
            } => Ok(Self::known(
                scope_id,
                lower_microusd,
                lower_microusd
                    .checked_add(additional_microusd)
                    .ok_or(LearningError::InvalidCost)?,
            )),
        }
    }
}

impl FailureOccurrence {
    #[must_use]
    pub fn fingerprint_for(
        repository_id: &str,
        scope: FailureScope,
        class: FailureClass,
    ) -> String {
        fingerprint(repository_id, scope, class)
    }

    pub fn from_typed(input: FailureInput) -> Result<Self, LearningError> {
        if !safe_token(&input.occurrence_id)
            || !safe_token(&input.repository_id)
            || !safe_token(&input.source_id)
        {
            return Err(LearningError::InvalidOccurrence(input.occurrence_id));
        }
        if !input.cost.valid() {
            return Err(LearningError::InvalidCost);
        }
        let automatic_class = input
            .terminal_code
            .map_or(FailureClass::Unknown, TerminalCode::class);
        let fingerprint = fingerprint(&input.repository_id, input.scope, automatic_class);
        Ok(Self {
            occurrence_id: input.occurrence_id,
            repository_id: input.repository_id,
            source_id: input.source_id,
            scope: input.scope,
            terminal_code: input.terminal_code,
            automatic_class,
            severity: input.severity,
            fingerprint,
            cost: input.cost,
            classification_revisions: Vec::new(),
        })
    }

    #[must_use]
    pub fn effective_class(&self) -> FailureClass {
        self.classification_revisions
            .last()
            .map_or(self.automatic_class, |revision| revision.class)
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.classification_revisions.len() as u64
    }
}

impl From<&FailureOccurrence> for FailureWireOccurrence {
    fn from(value: &FailureOccurrence) -> Self {
        Self {
            schema: FailureWireSchema::V1,
            id: value.occurrence_id.clone(),
            repository_id: value.repository_id.clone(),
            source: FailureWireSource {
                kind: value.scope,
                id: value.source_id.clone(),
            },
            terminal_code: value.terminal_code,
            automatic_class: value.automatic_class,
            severity: value.severity,
            taxonomy_version: FailureTaxonomyVersion::V1,
            fingerprint_sha256: value.fingerprint.clone(),
            cost: FailureWireCost::try_from(&value.cost)
                .expect("FailureOccurrence only contains validated cost"),
        }
    }
}

impl TryFrom<FailureWireOccurrence> for FailureOccurrence {
    type Error = LearningError;

    fn try_from(value: FailureWireOccurrence) -> Result<Self, Self::Error> {
        let expected_class = value
            .terminal_code
            .map_or(FailureClass::Unknown, TerminalCode::class);
        if value.automatic_class != expected_class {
            return Err(LearningError::InvalidWire("terminal_code/class pairing"));
        }
        let occurrence = Self::from_typed(FailureInput {
            occurrence_id: value.id,
            repository_id: value.repository_id,
            source_id: value.source.id,
            scope: value.source.kind,
            terminal_code: value.terminal_code,
            severity: value.severity,
            cost: CostAttribution::try_from(value.cost)?,
        })?;
        if occurrence.fingerprint != value.fingerprint_sha256 {
            return Err(LearningError::InvalidWire("fingerprint"));
        }
        Ok(occurrence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClassificationRevision {
    pub revision: u64,
    pub class: FailureClass,
    pub actor: String,
    pub reason_code: EditReason,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailureCluster {
    pub cluster_id: String,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipAction {
    Assigned,
    Merged,
    Split,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditReason {
    OperatorCorrection,
    DuplicateCluster,
    DistinctFailureMode,
    SourceCorrection,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipRevision {
    pub occurrence_id: String,
    pub cluster_id: String,
    pub revision: u64,
    pub action: MembershipAction,
    pub actor: String,
    pub reason_code: EditReason,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClusterEdit {
    Merged {
        source_cluster: String,
        target_cluster: String,
        actor: String,
        reason_code: EditReason,
    },
    Split {
        source_cluster: String,
        target_clusters: Vec<String>,
        actor: String,
        reason_code: EditReason,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailureLedger {
    occurrences: BTreeMap<String, FailureOccurrence>,
    clusters: BTreeMap<String, FailureCluster>,
    memberships: Vec<MembershipRevision>,
    cluster_edits: Vec<ClusterEdit>,
}

impl FailureLedger {
    pub fn replay(
        occurrences: impl IntoIterator<Item = FailureOccurrence>,
    ) -> Result<Self, LearningError> {
        let mut occurrences = occurrences.into_iter().collect::<Vec<_>>();
        occurrences.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));
        let mut ledger = Self::default();
        for occurrence in occurrences {
            ledger.record(occurrence)?;
        }
        Ok(ledger)
    }

    pub fn record(&mut self, occurrence: FailureOccurrence) -> Result<(), LearningError> {
        if self.occurrences.contains_key(&occurrence.occurrence_id) {
            return Err(LearningError::DuplicateOccurrence(occurrence.occurrence_id));
        }
        self.occurrences
            .insert(occurrence.occurrence_id.clone(), occurrence);
        Ok(())
    }

    pub fn create_cluster(&mut self, cluster_id: impl Into<String>) -> Result<(), LearningError> {
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() || self.clusters.contains_key(&cluster_id) {
            return Err(LearningError::DuplicateCluster(cluster_id));
        }
        self.clusters.insert(
            cluster_id.clone(),
            FailureCluster {
                cluster_id,
                revision: 0,
            },
        );
        Ok(())
    }

    pub fn reclassify(
        &mut self,
        occurrence_id: &str,
        expected_revision: u64,
        class: FailureClass,
        actor: impl Into<String>,
        reason_code: EditReason,
    ) -> Result<(), LearningError> {
        let occurrence = self
            .occurrences
            .get_mut(occurrence_id)
            .ok_or_else(|| LearningError::UnknownOccurrence(occurrence_id.to_owned()))?;
        if occurrence.revision() != expected_revision {
            return Err(LearningError::StaleOccurrence {
                id: occurrence_id.to_owned(),
                expected: expected_revision,
                actual: occurrence.revision(),
            });
        }
        let revision = occurrence.revision() + 1;
        occurrence
            .classification_revisions
            .push(ClassificationRevision {
                revision,
                class,
                actor: actor.into(),
                reason_code,
            });
        Ok(())
    }

    pub fn assign(
        &mut self,
        occurrence_id: &str,
        cluster_id: &str,
        expected_cluster_revision: u64,
        actor: impl Into<String>,
        reason_code: EditReason,
    ) -> Result<(), LearningError> {
        self.require_occurrence(occurrence_id)?;
        if self
            .memberships
            .iter()
            .any(|revision| revision.occurrence_id == occurrence_id)
        {
            return Err(LearningError::AlreadyClustered(occurrence_id.to_owned()));
        }
        self.require_cluster_revision(cluster_id, expected_cluster_revision)?;
        self.append_membership(
            occurrence_id,
            cluster_id,
            MembershipAction::Assigned,
            actor.into(),
            reason_code,
        );
        self.bump_cluster(cluster_id);
        Ok(())
    }

    pub fn merge(
        &mut self,
        source_cluster: &str,
        expected_source_revision: u64,
        target_cluster: &str,
        expected_target_revision: u64,
        actor: impl Into<String>,
        reason_code: EditReason,
    ) -> Result<(), LearningError> {
        if source_cluster == target_cluster {
            return Err(LearningError::InvalidMerge);
        }
        self.require_cluster_revision(source_cluster, expected_source_revision)?;
        self.require_cluster_revision(target_cluster, expected_target_revision)?;
        let occurrences = self.current_members(source_cluster);
        let actor = actor.into();
        for occurrence in occurrences {
            self.append_membership(
                &occurrence,
                target_cluster,
                MembershipAction::Merged,
                actor.clone(),
                reason_code,
            );
        }
        self.cluster_edits.push(ClusterEdit::Merged {
            source_cluster: source_cluster.to_owned(),
            target_cluster: target_cluster.to_owned(),
            actor,
            reason_code,
        });
        self.bump_cluster(source_cluster);
        self.bump_cluster(target_cluster);
        Ok(())
    }

    pub fn split(
        &mut self,
        source_cluster: &str,
        expected_source_revision: u64,
        moves: &[(String, String, u64)],
        actor: impl Into<String>,
        reason_code: EditReason,
    ) -> Result<(), LearningError> {
        self.require_cluster_revision(source_cluster, expected_source_revision)?;
        if moves.is_empty() {
            return Err(LearningError::InvalidSplit);
        }
        let source_members = self.current_members(source_cluster);
        let mut targets = BTreeSet::new();
        let mut moved = BTreeSet::new();
        for (occurrence, target, expected) in moves {
            if target == source_cluster || !moved.insert(occurrence) {
                return Err(LearningError::InvalidSplit);
            }
            if !source_members.contains(occurrence) {
                return Err(LearningError::NotClusterMember {
                    occurrence: occurrence.clone(),
                    cluster: source_cluster.to_owned(),
                });
            }
            self.require_cluster_revision(target, *expected)?;
            targets.insert(target.clone());
        }
        let actor = actor.into();
        for (occurrence, target, _) in moves {
            self.append_membership(
                occurrence,
                target,
                MembershipAction::Split,
                actor.clone(),
                reason_code,
            );
        }
        self.bump_cluster(source_cluster);
        let target_clusters = targets.iter().cloned().collect();
        for target in targets {
            self.bump_cluster(&target);
        }
        self.cluster_edits.push(ClusterEdit::Split {
            source_cluster: source_cluster.to_owned(),
            target_clusters,
            actor,
            reason_code,
        });
        Ok(())
    }

    #[must_use]
    pub fn occurrence(&self, id: &str) -> Option<&FailureOccurrence> {
        self.occurrences.get(id)
    }
    #[must_use]
    pub fn cluster(&self, id: &str) -> Option<&FailureCluster> {
        self.clusters.get(id)
    }
    #[must_use]
    pub fn membership_history(&self) -> &[MembershipRevision] {
        &self.memberships
    }
    #[must_use]
    pub fn cluster_edit_history(&self) -> &[ClusterEdit] {
        &self.cluster_edits
    }
    #[must_use]
    pub fn current_members(&self, cluster_id: &str) -> BTreeSet<String> {
        let mut current = BTreeMap::<&str, &str>::new();
        for revision in &self.memberships {
            current.insert(&revision.occurrence_id, &revision.cluster_id);
        }
        current
            .into_iter()
            .filter(|(_, cluster)| *cluster == cluster_id)
            .map(|(occurrence, _)| occurrence.to_owned())
            .collect()
    }

    fn require_occurrence(&self, id: &str) -> Result<(), LearningError> {
        self.occurrences
            .get(id)
            .map(|_| ())
            .ok_or_else(|| LearningError::UnknownOccurrence(id.to_owned()))
    }
    fn require_cluster_revision(&self, id: &str, expected: u64) -> Result<(), LearningError> {
        let cluster = self
            .clusters
            .get(id)
            .ok_or_else(|| LearningError::UnknownCluster(id.to_owned()))?;
        if cluster.revision == expected {
            Ok(())
        } else {
            Err(LearningError::StaleCluster {
                id: id.to_owned(),
                expected,
                actual: cluster.revision,
            })
        }
    }
    fn append_membership(
        &mut self,
        occurrence: &str,
        cluster: &str,
        action: MembershipAction,
        actor: String,
        reason_code: EditReason,
    ) {
        self.memberships.push(MembershipRevision {
            occurrence_id: occurrence.to_owned(),
            cluster_id: cluster.to_owned(),
            revision: self.memberships.len() as u64 + 1,
            action,
            actor,
            reason_code,
        });
    }
    fn bump_cluster(&mut self, id: &str) {
        self.clusters
            .get_mut(id)
            .expect("validated cluster")
            .revision += 1;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CostTotal {
    pub lower_microusd: u64,
    pub upper_microusd: u64,
    pub unknown_occurrences: u64,
}

pub fn aggregate_disjoint_cost<'a>(
    occurrences: impl IntoIterator<Item = &'a FailureOccurrence>,
) -> CostTotal {
    let mut total = CostTotal::default();
    let mut seen = BTreeSet::new();
    for occurrence in occurrences {
        match (
            &occurrence.cost.scope_id,
            occurrence.cost.lower_microusd,
            occurrence.cost.upper_microusd,
        ) {
            (Some(scope), Some(lower), Some(upper)) if seen.insert(scope) => {
                total.lower_microusd = total.lower_microusd.saturating_add(lower);
                total.upper_microusd = total.upper_microusd.saturating_add(upper);
            }
            (Some(_), Some(_), Some(_)) => {}
            _ => total.unknown_occurrences += 1,
        }
    }
    total
}

fn fingerprint(repository: &str, scope: FailureScope, class: FailureClass) -> String {
    let mut hasher = Sha256::new();
    for part in [
        FAILURE_TAXONOMY_VERSION,
        repository,
        scope_code(scope),
        class_code(class),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn safe_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

const fn scope_code(scope: FailureScope) -> &'static str {
    match scope {
        FailureScope::AttemptTerminal => "attempt_terminal",
        FailureScope::RunTerminal => "run_terminal",
        FailureScope::TypedOutcome => "typed_outcome",
    }
}

const fn class_code(class: FailureClass) -> &'static str {
    match class {
        FailureClass::Unknown => "unknown",
        FailureClass::PolicyBlocked => "policy_blocked",
        FailureClass::BudgetExhausted => "budget_exhausted",
        FailureClass::InfrastructureUnavailable => "infrastructure_unavailable",
        FailureClass::ProtocolError => "protocol_error",
        FailureClass::IntegrationConflict => "integration_conflict",
        FailureClass::SourceFailure => "source_failure",
        FailureClass::Inconclusive => "inconclusive",
        FailureClass::CancelledSuperseded => "cancelled_superseded",
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum LearningError {
    #[error("invalid occurrence {0}")]
    InvalidOccurrence(String),
    #[error("invalid cost attribution")]
    InvalidCost,
    #[error("duplicate occurrence {0}")]
    DuplicateOccurrence(String),
    #[error("duplicate or empty cluster {0}")]
    DuplicateCluster(String),
    #[error("unknown occurrence {0}")]
    UnknownOccurrence(String),
    #[error("unknown cluster {0}")]
    UnknownCluster(String),
    #[error("stale occurrence {id}: expected {expected}, actual {actual}")]
    StaleOccurrence {
        id: String,
        expected: u64,
        actual: u64,
    },
    #[error("stale cluster {id}: expected {expected}, actual {actual}")]
    StaleCluster {
        id: String,
        expected: u64,
        actual: u64,
    },
    #[error("occurrence {occurrence} is not in cluster {cluster}")]
    NotClusterMember { occurrence: String, cluster: String },
    #[error("cannot merge a cluster into itself")]
    InvalidMerge,
    #[error("occurrence {0} already has cluster lineage; use merge or split")]
    AlreadyClustered(String),
    #[error("split must move each occurrence once to a different cluster")]
    InvalidSplit,
    #[error("invalid failure wire value: {0}")]
    InvalidWire(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(id: &str, code: Option<TerminalCode>, cost: CostAttribution) -> FailureInput {
        FailureInput {
            occurrence_id: id.into(),
            repository_id: "repo".into(),
            source_id: format!("source-{id}"),
            scope: FailureScope::AttemptTerminal,
            terminal_code: code,
            severity: Severity::Unknown,
            cost,
        }
    }

    #[test]
    fn exact_terminal_mapping_is_closed_and_unknown_is_preserved() {
        assert_eq!(
            FailureClass::classify_terminal_code(Some("policy_blocked")),
            FailureClass::PolicyBlocked
        );
        assert_eq!(
            FailureClass::classify_terminal_code(Some("made_up")),
            FailureClass::Unknown
        );
        assert_eq!(
            FailureClass::classify_terminal_code(None),
            FailureClass::Unknown
        );
        assert_eq!(
            TerminalCode::parse("policy_blocked").map(TerminalCode::class),
            Some(FailureClass::PolicyBlocked)
        );
        assert_eq!(TerminalCode::parse("future_terminal_code"), None);
    }

    #[test]
    fn terminal_classification_and_fingerprint_ignore_unmodeled_prose() {
        let left = FailureOccurrence::from_typed(input(
            "left",
            TerminalCode::parse("protocol_error"),
            CostAttribution::unknown(),
        ))
        .unwrap();
        let right = FailureOccurrence::from_typed(input(
            "right",
            TerminalCode::parse("protocol_error"),
            CostAttribution::unknown(),
        ))
        .unwrap();
        assert_eq!(left.automatic_class, FailureClass::ProtocolError);
        assert_eq!(left.fingerprint, right.fingerprint);
        let unknown_left = FailureOccurrence::from_typed(input(
            "unknown-left",
            TerminalCode::parse("new_code"),
            CostAttribution::unknown(),
        ))
        .unwrap();
        let unknown_right = FailureOccurrence::from_typed(input(
            "unknown-right",
            TerminalCode::parse("another_code"),
            CostAttribution::unknown(),
        ))
        .unwrap();
        assert_eq!(unknown_left.automatic_class, FailureClass::Unknown);
        assert_eq!(unknown_left.fingerprint, unknown_right.fingerprint);
    }

    #[test]
    fn disjoint_cost_does_not_double_count_and_unknown_remains_unknown() {
        let first = FailureOccurrence::from_typed(input(
            "one",
            TerminalCode::parse("source_failure"),
            CostAttribution::known("attempt-1", 10, 20),
        ))
        .unwrap();
        let duplicate = FailureOccurrence::from_typed(input(
            "two",
            TerminalCode::parse("source_failure"),
            CostAttribution::known("attempt-1", 10, 20),
        ))
        .unwrap();
        let unknown =
            FailureOccurrence::from_typed(input("three", None, CostAttribution::unknown()))
                .unwrap();
        assert_eq!(
            aggregate_disjoint_cost([&first, &duplicate, &unknown]),
            CostTotal {
                lower_microusd: 10,
                upper_microusd: 20,
                unknown_occurrences: 1
            }
        );
    }

    #[test]
    fn severity_is_typed_input_not_inferred_from_scope() {
        let mut typed = input(
            "run",
            TerminalCode::parse("infrastructure_unavailable"),
            CostAttribution::unknown(),
        );
        typed.scope = FailureScope::RunTerminal;
        typed.severity = Severity::Low;
        let occurrence = FailureOccurrence::from_typed(typed).unwrap();
        assert_eq!(occurrence.severity, Severity::Low);
    }

    #[test]
    fn lineage_is_append_only_and_stale_edits_conflict() {
        let mut ledger = FailureLedger::default();
        ledger
            .record(
                FailureOccurrence::from_typed(input(
                    "one",
                    TerminalCode::parse("source_failure"),
                    CostAttribution::unknown(),
                ))
                .unwrap(),
            )
            .unwrap();
        ledger.create_cluster("a").unwrap();
        ledger.create_cluster("b").unwrap();
        ledger.create_cluster("c").unwrap();
        ledger
            .assign("one", "a", 0, "operator", EditReason::OperatorCorrection)
            .unwrap();
        ledger
            .reclassify(
                "one",
                0,
                FailureClass::Unknown,
                "operator",
                EditReason::OperatorCorrection,
            )
            .unwrap();
        assert_eq!(
            ledger.reclassify(
                "one",
                0,
                FailureClass::SourceFailure,
                "operator",
                EditReason::OperatorCorrection
            ),
            Err(LearningError::StaleOccurrence {
                id: "one".into(),
                expected: 0,
                actual: 1
            })
        );
        ledger
            .merge("a", 1, "b", 0, "operator", EditReason::DuplicateCluster)
            .unwrap();
        ledger
            .split(
                "b",
                1,
                &[("one".into(), "c".into(), 0)],
                "operator",
                EditReason::DistinctFailureMode,
            )
            .unwrap();
        assert_eq!(ledger.current_members("c"), BTreeSet::from(["one".into()]));
        assert_eq!(ledger.membership_history().len(), 3);
        assert_eq!(
            ledger.occurrence("one").unwrap().automatic_class,
            FailureClass::SourceFailure
        );
        assert_eq!(
            ledger.occurrence("one").unwrap().effective_class(),
            FailureClass::Unknown
        );
    }

    #[test]
    fn replay_is_order_independent() {
        let first = FailureOccurrence::from_typed(input(
            "a",
            TerminalCode::parse("budget_exhausted"),
            CostAttribution::unknown(),
        ))
        .unwrap();
        let second = FailureOccurrence::from_typed(input(
            "b",
            TerminalCode::parse("policy_blocked"),
            CostAttribution::unknown(),
        ))
        .unwrap();
        assert_eq!(
            FailureLedger::replay([first.clone(), second.clone()]).unwrap(),
            FailureLedger::replay([second, first]).unwrap()
        );
    }

    #[test]
    fn failure_wire_is_an_exact_schema_fixture_and_rejects_mutated_pairings() {
        let occurrence = FailureOccurrence::from_typed(FailureInput {
            occurrence_id: "failure_example".into(),
            repository_id: "repository_example".into(),
            source_id: "attempt_example".into(),
            scope: FailureScope::AttemptTerminal,
            terminal_code: TerminalCode::parse("budget_exhausted"),
            severity: Severity::Unknown,
            cost: CostAttribution::unknown(),
        })
        .unwrap();
        let wire = FailureWireOccurrence::from(&occurrence);
        let fixture: FailureWireOccurrence = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/failure.example.json"
        ))
        .unwrap();
        assert_eq!(fixture.id, wire.id);
        assert_eq!(fixture.source.kind, wire.source.kind);
        assert_eq!(fixture.terminal_code, wire.terminal_code);
        assert_eq!(fixture.automatic_class, wire.automatic_class);
        assert_eq!(fixture.cost, wire.cost);
        assert_eq!(FailureOccurrence::try_from(fixture).unwrap(), occurrence);
        assert_eq!(
            serde_json::to_value(&wire).unwrap()["schema"],
            "harness.failure.v1"
        );

        let mut mutated = wire;
        mutated.automatic_class = FailureClass::Unknown;
        assert_eq!(
            FailureOccurrence::try_from(mutated),
            Err(LearningError::InvalidWire("terminal_code/class pairing"))
        );
    }

    #[test]
    fn failure_schema_rejects_mismatched_terminal_pairings_and_partial_costs() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../schemas/harness.failure.v1.schema.json"
        ))
        .unwrap();
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&schema)
            .unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/failure.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&fixture));

        let mut mismatched = fixture.clone();
        mismatched["automatic_class"] = serde_json::Value::String("unknown".into());
        assert!(!validator.is_valid(&mismatched));

        let mut partial_cost = fixture;
        partial_cost["cost"]["scope_id"] = serde_json::Value::String("attempt-1".into());
        assert!(!validator.is_valid(&partial_cost));
    }

    #[test]
    fn identifiers_and_cost_scopes_are_opaque_bounded_tokens() {
        for invalid in [
            "operator@example.com",
            "path/segment",
            "two words",
            &"x".repeat(129),
        ] {
            let mut value = input("valid", None, CostAttribution::unknown());
            value.source_id = invalid.to_owned();
            assert!(FailureOccurrence::from_typed(value).is_err());
        }
        assert!(
            FailureOccurrence::from_typed(input(
                "valid",
                None,
                CostAttribution::known("scope@example.com", 1, 1),
            ))
            .is_err()
        );
    }
}
