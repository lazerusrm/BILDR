//! Closed, controller-owned contracts for the operator control plane.
//!
//! These records describe authoritative controller facts and bounded read
//! models. They never grant execution, Git, approval, or publication authority.

use std::{collections::BTreeMap, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;

const MAX_IDENTIFIER_LEN: usize = 160;
const MAX_TITLE_LEN: usize = 240;
const MAX_SUMMARY_LEN: usize = 4_000;
const MAX_SECTION_ROWS: usize = 1_000;
const SHA256_HEX_LEN: usize = 64;
const TRACE_ID_HEX_LEN: usize = 32;
const SPAN_ID_HEX_LEN: usize = 16;
const MAX_INVESTIGATION_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
const MAX_INVESTIGATION_FINDINGS: usize = 200;
const MAX_INVESTIGATION_RECOMMENDATIONS: usize = 100;
const MAX_INVESTIGATION_DECISIONS: usize = 100;
const MAX_INVESTIGATION_REFS: usize = 1_000;
const MAX_INVESTIGATION_LIST_ITEM_LEN: usize = 4_000;
const MAX_CONDITION_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_CONDITION_SPEC_BYTES: usize = 64 * 1024;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OperatorControlError {
    #[error("invalid {field}: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },
    #[error("illegal attention transition from {from:?} to {to:?}")]
    IllegalAttentionTransition {
        from: AttentionState,
        to: AttentionState,
    },
}

fn validate_identifier(value: &str, field: &'static str) -> Result<(), OperatorControlError> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_LEN {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "must be non-empty and bounded",
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "contains a path-unsafe character",
        });
    }
    Ok(())
}

fn validate_text(
    value: &str,
    field: &'static str,
    maximum: usize,
) -> Result<(), OperatorControlError> {
    if value.trim().is_empty() || value.chars().count() > maximum {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "must be non-empty and bounded",
        });
    }
    Ok(())
}

fn validate_hex(
    value: &str,
    field: &'static str,
    length: usize,
) -> Result<(), OperatorControlError> {
    if value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "must be fixed-length hexadecimal",
        });
    }
    Ok(())
}

fn validate_lower_hex(
    value: &str,
    field: &'static str,
    length: usize,
) -> Result<(), OperatorControlError> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "must be fixed-length lowercase hexadecimal",
        });
    }
    Ok(())
}

fn validate_relative_repo_path(
    value: &str,
    field: &'static str,
) -> Result<(), OperatorControlError> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_LEN
        || value.starts_with('/')
        || value.starts_with('\\')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "must be a bounded relative repository path",
        });
    }
    Ok(())
}

fn validate_bounded_texts(
    values: &[String],
    field: &'static str,
    maximum_items: usize,
    maximum_len: usize,
) -> Result<(), OperatorControlError> {
    if values.len() > maximum_items {
        return Err(OperatorControlError::InvalidField {
            field,
            reason: "exceeds the bounded item limit",
        });
    }
    for value in values {
        validate_text(value, field, maximum_len)?;
    }
    Ok(())
}

fn digest_json<T: Serialize>(value: &T) -> Result<String, OperatorControlError> {
    let raw = serde_json::to_vec(value).map_err(|_| OperatorControlError::InvalidField {
        field: "operator control payload",
        reason: "must serialize as JSON",
    })?;
    Ok(hex::encode(Sha256::digest(raw)))
}

macro_rules! operator_control_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Ulid::generate().to_string())
            }

            pub fn parse(value: impl AsRef<str>) -> Result<Self, OperatorControlError> {
                let value = value.as_ref();
                validate_identifier(value, stringify!($name))?;
                Ok(Self(value.to_owned()))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = OperatorControlError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = OperatorControlError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }
    };
}

operator_control_id!(AttentionItemId);
operator_control_id!(InvestigationArtifactId);
operator_control_id!(MaterialProgressEventId);
operator_control_id!(LivenessEpisodeId);
operator_control_id!(LivenessObservationId);
operator_control_id!(InterventionId);
operator_control_id!(ReconciliationEpisodeId);
operator_control_id!(OwnershipProofId);
operator_control_id!(ExternalConditionId);
operator_control_id!(ConditionObservationId);
operator_control_id!(ControlPlaneSnapshotId);
operator_control_id!(ReturnViewId);
operator_control_id!(NotificationDeliveryId);
operator_control_id!(TopologySnapshotId);
operator_control_id!(CorrelationLinkId);

/// Closed execution authority for a task. Legacy packets deserialize to the
/// least surprising existing behavior: an implementation task.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionKind {
    #[default]
    Implementation,
    Investigation,
    Verification,
    Review,
    Integration,
}

impl crate::TaskPacket {
    /// Enforces the authority boundary for the newly explicit task kind.
    /// Scheduling still owns sandbox and lease creation; this packet-level
    /// check rejects an investigation before it can request mutable custody.
    pub fn validate_execution_contract(&self) -> Result<(), OperatorControlError> {
        if self.execution_kind == TaskExecutionKind::Investigation
            && (!self.owned_paths.is_empty() || !self.reserved_serial_paths.is_empty())
        {
            return Err(OperatorControlError::InvalidField {
                field: "investigation task custody",
                reason: "investigations cannot request mutable path ownership or serial leases",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionCategory {
    Decision,
    Approval,
    Credential,
    PolicyException,
    DestructiveAction,
    Publication,
    MissingEvidence,
    ExternalDependency,
    RecoveryConflict,
    Infrastructure,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionSeverity {
    Info,
    Normal,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionState {
    Open,
    Acknowledged,
    WaitingExternal,
    Resolved,
    Declined,
    Superseded,
    Invalidated,
}

impl AttentionState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Resolved | Self::Declined | Self::Superseded | Self::Invalidated
        )
    }

    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Open => matches!(
                next,
                Self::Acknowledged
                    | Self::WaitingExternal
                    | Self::Resolved
                    | Self::Declined
                    | Self::Superseded
                    | Self::Invalidated
            ),
            Self::Acknowledged => matches!(
                next,
                Self::WaitingExternal
                    | Self::Resolved
                    | Self::Declined
                    | Self::Superseded
                    | Self::Invalidated
            ),
            Self::WaitingExternal => matches!(
                next,
                Self::Open | Self::Resolved | Self::Declined | Self::Superseded | Self::Invalidated
            ),
            Self::Resolved | Self::Declined | Self::Superseded | Self::Invalidated => next == self,
        }
    }

    pub fn validate_transition(
        self,
        next: Self,
        same_terminal_receipt: bool,
    ) -> Result<(), OperatorControlError> {
        if !self.can_transition_to(next) || (self.is_terminal() && !same_terminal_receipt) {
            return Err(OperatorControlError::IllegalAttentionTransition {
                from: self,
                to: next,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionSourceType {
    Approval,
    Decision,
    CredentialRequirement,
    Publication,
    PolicyDecision,
    EvidenceGap,
    ExternalCondition,
    Reconciliation,
    Infrastructure,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttentionSourceRef {
    pub source_type: AttentionSourceType,
    pub source_id: String,
    pub source_revision: u64,
}

impl AttentionSourceRef {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        validate_identifier(&self.source_id, "attention source id")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttentionResurfacingPolicy {
    pub policy: String,
    pub maximum_defer_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttentionResolution {
    pub outcome: String,
    pub actor_type: String,
    pub actor_id: String,
    pub resolved_at_ms: i64,
    pub authority_event_id: String,
    pub bound_head_sha: Option<String>,
    pub worktree_fingerprint: Option<String>,
    pub receipt_sha256: String,
}

impl AttentionResolution {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        validate_identifier(&self.authority_event_id, "attention authority event id")?;
        validate_hex(
            &self.receipt_sha256,
            "attention receipt sha256",
            SHA256_HEX_LEN,
        )?;
        if let Some(head) = &self.bound_head_sha {
            validate_hex(head, "attention bound head sha", 40)?;
        }
        if let Some(fingerprint) = &self.worktree_fingerprint {
            validate_hex(
                fingerprint,
                "attention worktree fingerprint",
                SHA256_HEX_LEN,
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttentionItem {
    pub schema: String,
    pub attention_id: AttentionItemId,
    pub repository_id: Option<String>,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub source: AttentionSourceRef,
    pub category: AttentionCategory,
    pub severity: AttentionSeverity,
    pub state: AttentionState,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub option_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub blocked_refs: Vec<String>,
    pub dedupe_key: String,
    pub opened_event_id: String,
    pub opened_at_ms: i64,
    pub acknowledged_at_ms: Option<i64>,
    pub due_at_ms: Option<i64>,
    pub resurfacing: AttentionResurfacingPolicy,
    pub resolution: Option<AttentionResolution>,
    pub version: u64,
}

impl AttentionItem {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.schema != "harness.attention-item.v1" {
            return Err(OperatorControlError::InvalidField {
                field: "attention schema",
                reason: "must be harness.attention-item.v1",
            });
        }
        self.source.validate()?;
        validate_text(&self.title, "attention title", MAX_TITLE_LEN)?;
        validate_text(&self.summary, "attention summary", MAX_SUMMARY_LEN)?;
        validate_identifier(&self.dedupe_key, "attention dedupe key")?;
        validate_identifier(&self.opened_event_id, "attention opened event id")?;
        if self.state.is_terminal() != self.resolution.is_some() {
            return Err(OperatorControlError::InvalidField {
                field: "attention resolution",
                reason: "must exist exactly for a terminal attention state",
            });
        }
        if let Some(resolution) = &self.resolution {
            resolution.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationScope {
    #[serde(default)]
    pub owned_read_paths: Vec<String>,
    #[serde(default)]
    pub forbidden_paths: Vec<String>,
    pub time_budget_ms: u64,
    pub token_budget: u64,
}

impl InvestigationScope {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.owned_read_paths.is_empty()
            || self.owned_read_paths.len() > MAX_INVESTIGATION_REFS
            || self.forbidden_paths.len() > MAX_INVESTIGATION_REFS
            || self.time_budget_ms == 0
            || self.token_budget == 0
        {
            return Err(OperatorControlError::InvalidField {
                field: "investigation scope",
                reason: "must have bounded read paths and positive time and token budgets",
            });
        }
        for path in self.owned_read_paths.iter().chain(&self.forbidden_paths) {
            validate_relative_repo_path(path, "investigation scope path")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationFindingClassification {
    Confirmed,
    Supported,
    Hypothesis,
    Disproven,
    Inconclusive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationSensitivity {
    Public,
    Internal,
    Restricted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationFinding {
    pub finding_id: String,
    pub classification: InvestigationFindingClassification,
    pub summary: String,
    pub confidence_milli: u16,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub affected_refs: Vec<String>,
    pub risk: AttentionSeverity,
    #[serde(default)]
    pub limitations: Vec<String>,
}

impl InvestigationFinding {
    fn validate(&self) -> Result<(), OperatorControlError> {
        validate_identifier(&self.finding_id, "investigation finding id")?;
        validate_text(
            &self.summary,
            "investigation finding summary",
            MAX_SUMMARY_LEN,
        )?;
        if self.confidence_milli > 1_000 {
            return Err(OperatorControlError::InvalidField {
                field: "investigation finding confidence",
                reason: "must be in 0..=1000",
            });
        }
        validate_bounded_texts(
            &self.evidence_refs,
            "investigation finding evidence refs",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )?;
        validate_bounded_texts(
            &self.affected_refs,
            "investigation finding affected refs",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )?;
        validate_bounded_texts(
            &self.limitations,
            "investigation finding limitations",
            MAX_INVESTIGATION_REFS,
            MAX_INVESTIGATION_LIST_ITEM_LEN,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRecommendation {
    pub recommendation_id: String,
    pub summary: String,
    pub required_authority: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub alternatives: Vec<String>,
    pub risk: AttentionSeverity,
    pub next_verification: String,
}

impl InvestigationRecommendation {
    fn validate(&self) -> Result<(), OperatorControlError> {
        validate_identifier(&self.recommendation_id, "investigation recommendation id")?;
        validate_text(
            &self.summary,
            "investigation recommendation summary",
            MAX_SUMMARY_LEN,
        )?;
        validate_identifier(
            &self.required_authority,
            "investigation recommendation required authority",
        )?;
        validate_bounded_texts(
            &self.evidence_refs,
            "investigation recommendation evidence refs",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )?;
        validate_bounded_texts(
            &self.alternatives,
            "investigation recommendation alternatives",
            MAX_INVESTIGATION_REFS,
            MAX_INVESTIGATION_LIST_ITEM_LEN,
        )?;
        validate_text(
            &self.next_verification,
            "investigation recommendation next verification",
            MAX_SUMMARY_LEN,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionInventoryItem {
    pub decision_id: String,
    pub question: String,
    pub state: String,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub impact: String,
    pub recommended_option: Option<String>,
    pub required_actor: String,
    #[serde(default)]
    pub blocking_refs: Vec<String>,
    pub independent_work_can_continue: bool,
}

impl DecisionInventoryItem {
    fn validate(&self) -> Result<(), OperatorControlError> {
        validate_identifier(&self.decision_id, "investigation decision id")?;
        validate_text(
            &self.question,
            "investigation decision question",
            MAX_SUMMARY_LEN,
        )?;
        validate_identifier(&self.state, "investigation decision state")?;
        validate_bounded_texts(
            &self.options,
            "investigation decision options",
            MAX_INVESTIGATION_REFS,
            MAX_SUMMARY_LEN,
        )?;
        validate_bounded_texts(
            &self.evidence_refs,
            "investigation decision evidence refs",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )?;
        validate_text(
            &self.impact,
            "investigation decision impact",
            MAX_SUMMARY_LEN,
        )?;
        if let Some(option) = &self.recommended_option {
            validate_text(
                option,
                "investigation decision recommended option",
                MAX_SUMMARY_LEN,
            )?;
        }
        validate_identifier(
            &self.required_actor,
            "investigation decision required actor",
        )?;
        validate_bounded_texts(
            &self.blocking_refs,
            "investigation decision blocking refs",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationArtifact {
    pub schema: String,
    pub artifact_id: InvestigationArtifactId,
    pub run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub question: String,
    pub scope: InvestigationScope,
    pub base_sha: String,
    pub repository_state_digest: String,
    #[serde(default)]
    pub methods: Vec<String>,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub findings: Vec<InvestigationFinding>,
    #[serde(default)]
    pub recommendations: Vec<InvestigationRecommendation>,
    #[serde(default)]
    pub decision_inventory: Vec<DecisionInventoryItem>,
    #[serde(default)]
    pub limitations: Vec<String>,
    #[serde(default)]
    pub rejected_hypotheses: Vec<String>,
    pub sensitivity: InvestigationSensitivity,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    pub created_at_ms: i64,
    pub sha256: String,
}

impl InvestigationArtifact {
    pub fn digest(&self) -> Result<String, OperatorControlError> {
        let mut unsigned = self.clone();
        unsigned.sha256.clear();
        digest_json(&unsigned)
    }

    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.schema != "harness.investigation-artifact.v1" {
            return Err(OperatorControlError::InvalidField {
                field: "investigation artifact schema",
                reason: "must be harness.investigation-artifact.v1",
            });
        }
        validate_identifier(self.artifact_id.as_str(), "investigation artifact id")?;
        for (field, value) in [
            ("investigation run id", &self.run_id),
            ("investigation task id", &self.task_id),
            ("investigation attempt id", &self.attempt_id),
        ] {
            validate_identifier(value, field)?;
        }
        validate_text(&self.question, "investigation question", MAX_SUMMARY_LEN)?;
        self.scope.validate()?;
        validate_lower_hex(&self.base_sha, "investigation base SHA", 40)?;
        validate_lower_hex(
            &self.repository_state_digest,
            "investigation repository state digest",
            SHA256_HEX_LEN,
        )?;
        if self.created_at_ms < 0 {
            return Err(OperatorControlError::InvalidField {
                field: "investigation creation time",
                reason: "must be a UTC epoch millisecond timestamp",
            });
        }
        validate_bounded_texts(
            &self.methods,
            "investigation methods",
            MAX_INVESTIGATION_REFS,
            MAX_INVESTIGATION_LIST_ITEM_LEN,
        )?;
        validate_bounded_texts(
            &self.sources,
            "investigation sources",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )?;
        if self.findings.len() > MAX_INVESTIGATION_FINDINGS
            || self.recommendations.len() > MAX_INVESTIGATION_RECOMMENDATIONS
            || self.decision_inventory.len() > MAX_INVESTIGATION_DECISIONS
        {
            return Err(OperatorControlError::InvalidField {
                field: "investigation artifact collections",
                reason: "exceeds the contract bounds",
            });
        }
        for finding in &self.findings {
            finding.validate()?;
        }
        for recommendation in &self.recommendations {
            recommendation.validate()?;
        }
        for decision in &self.decision_inventory {
            decision.validate()?;
        }
        validate_bounded_texts(
            &self.limitations,
            "investigation limitations",
            MAX_INVESTIGATION_REFS,
            MAX_INVESTIGATION_LIST_ITEM_LEN,
        )?;
        validate_bounded_texts(
            &self.rejected_hypotheses,
            "investigation rejected hypotheses",
            MAX_INVESTIGATION_REFS,
            MAX_INVESTIGATION_LIST_ITEM_LEN,
        )?;
        validate_bounded_texts(
            &self.artifact_refs,
            "investigation artifact refs",
            MAX_INVESTIGATION_REFS,
            MAX_IDENTIFIER_LEN,
        )?;
        let raw = serde_json::to_vec(self).map_err(|_| OperatorControlError::InvalidField {
            field: "investigation artifact",
            reason: "must serialize as JSON",
        })?;
        if raw.len() > MAX_INVESTIGATION_PAYLOAD_BYTES {
            return Err(OperatorControlError::InvalidField {
                field: "investigation artifact payload",
                reason: "exceeds the 2 MiB contract bound",
            });
        }
        validate_lower_hex(
            &self.sha256,
            "investigation artifact sha256",
            SHA256_HEX_LEN,
        )?;
        if self.digest()? != self.sha256 {
            return Err(OperatorControlError::InvalidField {
                field: "investigation artifact sha256",
                reason: "does not match the canonical artifact payload",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterialProgressKind {
    CandidateChanged,
    ValidationAdvanced,
    EvidenceRecorded,
    ExternalConditionChanged,
    ReconciliationAdvanced,
    AttentionChanged,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterialProgressEvent {
    pub schema: String,
    pub event_id: MaterialProgressEventId,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub kind: MaterialProgressKind,
    pub source_event_id: String,
    pub occurred_at_ms: i64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivenessState {
    Healthy,
    QuietActive,
    WaitingExternal,
    Degraded,
    SuspectedStall,
    ConfirmedStall,
    OwnershipUncertain,
    RecoveryRequired,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivenessObservationKind {
    MaterialProgress,
    RuntimeHeartbeat,
    CommandActivity,
    ExternalWait,
    OwnershipEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionKind {
    Wait,
    RequestOperatorDecision,
    RequestReconciliation,
    QueueReadOnlyReview,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationTrigger {
    DaemonRestart,
    AppServerLoss,
    ProcessLoss,
    VersionTransition,
    AccountHandoff,
    WorktreeMismatch,
    UncertainCommandCompletion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationState {
    Open,
    Claimed,
    AwaitingEvidence,
    Resolved,
    Refused,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationFindingKind {
    LiveOwner,
    UnknownOwner,
    PreservedCandidate,
    StaleApproval,
    AmbiguousExternalEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationActionKind {
    Preserve,
    ResumeProvenOwner,
    InvalidateStaleApproval,
    ReleaseProvenDeadLease,
    AuthorizeFreshAttempt,
    OpenAttention,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalConditionAdapter {
    CiCheck,
    ReviewState,
    CredentialAvailability,
    TimeGate,
    HardwareCapacity,
    ServiceAvailability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalConditionOwnerType {
    Run,
    Task,
    Attempt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalConditionState {
    Open,
    Satisfied,
    Unsatisfied,
    Unknown,
    Cancelled,
}

impl ExternalConditionState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Satisfied | Self::Unsatisfied | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalConditionPollPolicy {
    pub initial_ms: u64,
    pub maximum_ms: u64,
    pub deadline_ms: Option<i64>,
}

impl ExternalConditionPollPolicy {
    fn validate(&self) -> Result<(), OperatorControlError> {
        if self.initial_ms == 0 || self.maximum_ms < self.initial_ms {
            return Err(OperatorControlError::InvalidField {
                field: "external condition poll policy",
                reason: "must have a positive initial interval no larger than its maximum",
            });
        }
        if self.deadline_ms.is_some_and(|deadline| deadline < 0) {
            return Err(OperatorControlError::InvalidField {
                field: "external condition deadline",
                reason: "must be a UTC epoch millisecond timestamp",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConditionObservation {
    pub schema: String,
    pub observation_id: ConditionObservationId,
    pub condition_id: ExternalConditionId,
    pub source_event_id: String,
    pub sequence: u64,
    pub observed_at_ms: i64,
    pub state: ExternalConditionState,
    pub payload: Value,
    pub sha256: String,
}

impl ConditionObservation {
    pub fn digest(&self) -> Result<String, OperatorControlError> {
        let mut unsigned = self.clone();
        unsigned.sha256.clear();
        digest_json(&unsigned)
    }

    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.schema != "harness.condition-observation.v1" {
            return Err(OperatorControlError::InvalidField {
                field: "condition observation schema",
                reason: "must be harness.condition-observation.v1",
            });
        }
        validate_identifier(self.observation_id.as_str(), "condition observation id")?;
        validate_identifier(
            self.condition_id.as_str(),
            "condition observation condition id",
        )?;
        validate_identifier(
            &self.source_event_id,
            "condition observation source event id",
        )?;
        if self.observed_at_ms < 0 {
            return Err(OperatorControlError::InvalidField {
                field: "condition observation time",
                reason: "must be a UTC epoch millisecond timestamp",
            });
        }
        let raw = serde_json::to_vec(self).map_err(|_| OperatorControlError::InvalidField {
            field: "condition observation",
            reason: "must serialize as JSON",
        })?;
        if raw.len() > MAX_CONDITION_PAYLOAD_BYTES {
            return Err(OperatorControlError::InvalidField {
                field: "condition observation payload",
                reason: "exceeds the bounded payload limit",
            });
        }
        validate_lower_hex(&self.sha256, "condition observation sha256", SHA256_HEX_LEN)?;
        if self.digest()? != self.sha256 {
            return Err(OperatorControlError::InvalidField {
                field: "condition observation sha256",
                reason: "does not match the canonical observation payload",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalCondition {
    pub schema: String,
    pub condition_id: ExternalConditionId,
    pub owner_type: ExternalConditionOwnerType,
    pub owner_id: String,
    pub adapter: ExternalConditionAdapter,
    pub source_id: String,
    pub spec: Value,
    pub state: ExternalConditionState,
    pub sequence: u64,
    pub poll_policy: ExternalConditionPollPolicy,
    pub source_identity_digest: String,
    pub last_observation: Option<ConditionObservation>,
    pub version: u64,
    pub opened_at_ms: i64,
    pub updated_at_ms: i64,
    pub sha256: String,
}

impl ExternalCondition {
    pub fn digest(&self) -> Result<String, OperatorControlError> {
        let mut unsigned = self.clone();
        unsigned.sha256.clear();
        digest_json(&unsigned)
    }

    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.schema != "harness.external-condition.v1" {
            return Err(OperatorControlError::InvalidField {
                field: "external condition schema",
                reason: "must be harness.external-condition.v1",
            });
        }
        validate_identifier(self.condition_id.as_str(), "external condition id")?;
        validate_identifier(&self.owner_id, "external condition owner id")?;
        validate_identifier(&self.source_id, "external condition source id")?;
        validate_lower_hex(
            &self.source_identity_digest,
            "external condition source identity digest",
            SHA256_HEX_LEN,
        )?;
        if self.version == 0 || self.opened_at_ms < 0 || self.updated_at_ms < self.opened_at_ms {
            return Err(OperatorControlError::InvalidField {
                field: "external condition version or timestamps",
                reason: "must be monotonic and use UTC epoch milliseconds",
            });
        }
        self.poll_policy.validate()?;
        let spec =
            serde_json::to_vec(&self.spec).map_err(|_| OperatorControlError::InvalidField {
                field: "external condition spec",
                reason: "must serialize as JSON",
            })?;
        if spec.len() > MAX_CONDITION_SPEC_BYTES {
            return Err(OperatorControlError::InvalidField {
                field: "external condition spec",
                reason: "exceeds the bounded specification limit",
            });
        }
        if let Some(observation) = &self.last_observation {
            observation.validate()?;
            if observation.condition_id != self.condition_id
                || observation.sequence != self.sequence
                || observation.state != self.state
            {
                return Err(OperatorControlError::InvalidField {
                    field: "external condition last observation",
                    reason: "must bind the exact condition, current sequence, and current state",
                });
            }
        } else if self.sequence != 0 {
            return Err(OperatorControlError::InvalidField {
                field: "external condition sequence",
                reason: "cannot advance without an observation",
            });
        }
        let raw = serde_json::to_vec(self).map_err(|_| OperatorControlError::InvalidField {
            field: "external condition",
            reason: "must serialize as JSON",
        })?;
        if raw.len() > MAX_CONDITION_PAYLOAD_BYTES {
            return Err(OperatorControlError::InvalidField {
                field: "external condition payload",
                reason: "exceeds the bounded payload limit",
            });
        }
        validate_lower_hex(&self.sha256, "external condition sha256", SHA256_HEX_LEN)?;
        if self.digest()? != self.sha256 {
            return Err(OperatorControlError::InvalidField {
                field: "external condition sha256",
                reason: "does not match the canonical condition payload",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorPresenceMode {
    #[default]
    Interactive,
    Focus,
    Unattended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationClass {
    Critical,
    ActionRequired,
    Routine,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationState {
    Pending,
    Deferred,
    Delivered,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSectionState {
    Current,
    Stale,
    Unknown,
    Error,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSection {
    pub state: SnapshotSectionState,
    #[serde(default)]
    pub rows: Vec<Value>,
    pub source_cursor: u64,
    pub truncated: bool,
    pub detail: Option<String>,
}

impl SnapshotSection {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.rows.len() > MAX_SECTION_ROWS {
            return Err(OperatorControlError::InvalidField {
                field: "snapshot section rows",
                reason: "exceeds the bounded section limit",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotTruncation {
    pub section: String,
    pub omitted_rows: u64,
    pub limit: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneSnapshot {
    pub schema: String,
    pub snapshot_id: ControlPlaneSnapshotId,
    pub revision: u64,
    pub compiled_at_ms: i64,
    pub event_cursor: u64,
    pub consistency: String,
    pub system: SnapshotSection,
    pub accounts: SnapshotSection,
    pub scheduler: SnapshotSection,
    pub runs: SnapshotSection,
    pub attention: SnapshotSection,
    pub attempts: SnapshotSection,
    pub investigations: SnapshotSection,
    pub progress: SnapshotSection,
    pub liveness: SnapshotSection,
    pub reconciliation: SnapshotSection,
    pub external_conditions: SnapshotSection,
    pub cost: SnapshotSection,
    pub notifications: SnapshotSection,
    pub limits: SnapshotSection,
    #[serde(default)]
    pub truncation: Vec<SnapshotTruncation>,
    #[serde(default)]
    pub source_cursors: BTreeMap<String, u64>,
    pub sha256: String,
}

impl ControlPlaneSnapshot {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.schema != "harness.control-plane-snapshot.v1" {
            return Err(OperatorControlError::InvalidField {
                field: "snapshot schema",
                reason: "must be harness.control-plane-snapshot.v1",
            });
        }
        validate_hex(&self.sha256, "snapshot sha256", SHA256_HEX_LEN)?;
        for section in [
            &self.system,
            &self.accounts,
            &self.scheduler,
            &self.runs,
            &self.attention,
            &self.attempts,
            &self.investigations,
            &self.progress,
            &self.liveness,
            &self.reconciliation,
            &self.external_conditions,
            &self.cost,
            &self.notifications,
            &self.limits,
        ] {
            section.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReturnView {
    pub schema: String,
    pub return_view_id: ReturnViewId,
    pub snapshot_id: ControlPlaneSnapshotId,
    pub snapshot_revision: u64,
    pub event_cursor: u64,
    pub acknowledged_cursor: u64,
    #[serde(default)]
    pub sections: BTreeMap<String, SnapshotSection>,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyNode {
    pub id: String,
    pub kind: String,
    pub source_ref: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
    pub source_ref: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySnapshot {
    pub schema: String,
    pub snapshot_id: TopologySnapshotId,
    pub run_id: String,
    #[serde(default)]
    pub nodes: Vec<TopologyNode>,
    #[serde(default)]
    pub edges: Vec<TopologyEdge>,
    pub source_cursor: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
}

impl TraceContext {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        validate_lower_hex(&self.trace_id, "trace id", TRACE_ID_HEX_LEN)?;
        validate_lower_hex(&self.span_id, "span id", SPAN_ID_HEX_LEN)?;
        if self.trace_id.bytes().all(|byte| byte == b'0')
            || self.span_id.bytes().all(|byte| byte == b'0')
        {
            return Err(OperatorControlError::InvalidField {
                field: "trace context",
                reason: "trace and span identifiers must not be all zeroes",
            });
        }
        if let Some(parent) = &self.parent_span_id {
            validate_lower_hex(parent, "parent span id", SPAN_ID_HEX_LEN)?;
            if parent.bytes().all(|byte| byte == b'0') || parent == &self.span_id {
                return Err(OperatorControlError::InvalidField {
                    field: "parent span id",
                    reason: "must not be all zeroes or equal the current span id",
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrelationLink {
    pub schema: String,
    pub link_id: CorrelationLinkId,
    pub trace: TraceContext,
    pub from_kind: String,
    pub from_id: String,
    pub to_kind: String,
    pub to_id: String,
    pub relation: String,
    pub created_at_ms: i64,
}

impl CorrelationLink {
    pub fn validate(&self) -> Result<(), OperatorControlError> {
        if self.schema != "harness.correlation-link.v1" {
            return Err(OperatorControlError::InvalidField {
                field: "correlation schema",
                reason: "must be harness.correlation-link.v1",
            });
        }
        self.trace.validate()?;
        for (field, value) in [
            ("correlation from kind", &self.from_kind),
            ("correlation from id", &self.from_id),
            ("correlation to kind", &self.to_kind),
            ("correlation to id", &self.to_id),
            ("correlation relation", &self.relation),
        ] {
            validate_identifier(value, field)?;
        }
        if self.from_kind == self.to_kind && self.from_id == self.to_id {
            return Err(OperatorControlError::InvalidField {
                field: "correlation link",
                reason: "must connect distinct source records",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_operator_control_enums_reject_unknown_values() {
        assert!(serde_json::from_str::<AttentionState>("\"open\"").is_ok());
        assert!(serde_json::from_str::<AttentionState>("\"later\"").is_err());
        assert!(serde_json::from_str::<TaskExecutionKind>("\"investigation\"").is_ok());
        assert!(serde_json::from_str::<TaskExecutionKind>("\"shell\"").is_err());
    }

    #[test]
    fn identifiers_and_trace_context_are_bounded_and_path_safe() {
        assert!(AttentionItemId::parse("attn_01").is_ok());
        assert!(AttentionItemId::parse("../attn").is_err());
        assert!(AttentionItemId::parse("a".repeat(MAX_IDENTIFIER_LEN + 1)).is_err());
        assert!(
            TraceContext {
                trace_id: "a".repeat(TRACE_ID_HEX_LEN),
                span_id: "b".repeat(SPAN_ID_HEX_LEN),
                parent_span_id: Some("c".repeat(SPAN_ID_HEX_LEN)),
            }
            .validate()
            .is_ok()
        );
        assert!(
            TraceContext {
                trace_id: "not-a-trace".to_owned(),
                span_id: "b".repeat(SPAN_ID_HEX_LEN),
                parent_span_id: None,
            }
            .validate()
            .is_err()
        );
        assert!(
            TraceContext {
                trace_id: "0".repeat(TRACE_ID_HEX_LEN),
                span_id: "b".repeat(SPAN_ID_HEX_LEN),
                parent_span_id: None,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn attention_transitions_are_source_owned_and_terminal_receipts_idempotent() {
        assert!(
            AttentionState::Open
                .validate_transition(AttentionState::Acknowledged, false)
                .is_ok()
        );
        assert!(
            AttentionState::Acknowledged
                .validate_transition(AttentionState::Open, false)
                .is_err()
        );
        assert!(
            AttentionState::Resolved
                .validate_transition(AttentionState::Resolved, false)
                .is_err()
        );
        assert!(
            AttentionState::Resolved
                .validate_transition(AttentionState::Resolved, true)
                .is_ok()
        );
    }

    #[test]
    fn investigation_packets_cannot_request_mutable_custody() {
        let packet: crate::TaskPacket = serde_json::from_value(serde_json::json!({
            "schema": "harness.orchestration.task.v1",
            "program_id": "program",
            "task_id": "task",
            "title": "Investigation",
            "state": "ready",
            "priority": "P1",
            "execution_mode": "controller",
            "execution_kind": "investigation",
            "owner_profile": "general",
            "reviewer_profile": "general",
            "checklist_rows": [],
            "authority_refs": [],
            "base_sha": "",
            "depends_on": [],
            "owned_paths": ["src"],
            "forbidden_paths": [],
            "reserved_serial_paths": [],
            "objective": "Gather facts",
            "milestones": [],
            "non_goals": [],
            "success_criteria": [],
            "required_positive_tests": [],
            "required_negative_tests": [],
            "required_metrics": [],
            "required_evidence": [],
            "proof_limits": [],
            "diff_budget": {"files": 1, "lines": 1},
            "token_budget": 1000,
            "lease_expires_at": "controller-managed",
            "stop_conditions": [],
            "handoff_path": "controller://investigation"
        }))
        .expect("packet deserializes");
        assert!(packet.validate_execution_contract().is_err());
    }

    #[test]
    fn legacy_task_packets_default_to_implementation() {
        let packet: crate::TaskPacket = serde_json::from_value(serde_json::json!({
            "schema": "harness.orchestration.task.v1",
            "program_id": "program",
            "task_id": "task",
            "title": "Legacy task",
            "state": "ready",
            "priority": "P1",
            "execution_mode": "controller",
            "owner_profile": "general",
            "reviewer_profile": "general",
            "checklist_rows": [],
            "authority_refs": [],
            "base_sha": "",
            "depends_on": [],
            "owned_paths": [],
            "forbidden_paths": [],
            "reserved_serial_paths": [],
            "objective": "Preserve legacy packets",
            "milestones": [],
            "non_goals": [],
            "success_criteria": [],
            "required_positive_tests": [],
            "required_negative_tests": [],
            "required_metrics": [],
            "required_evidence": [],
            "proof_limits": [],
            "diff_budget": {"files": 1, "lines": 1},
            "token_budget": 1000,
            "lease_expires_at": "controller-managed",
            "stop_conditions": [],
            "handoff_path": "controller://legacy"
        }))
        .expect("legacy packet deserializes");
        assert_eq!(packet.execution_kind, TaskExecutionKind::Implementation);
    }
}
