//! Stable domain types shared by the daemon, CLI, store, and UI API.
//!
//! Protocol-specific unknown values stay in `harness-codex`; controller state
//! uses closed enums so an additive wire value cannot silently advance a run.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use ulid::Ulid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Ulid::generate().to_string())
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

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl FromStr for $name {
            type Err = std::convert::Infallible;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Ok(Self::from(value))
            }
        }
    };
}

id_type!(RepositoryId);
id_type!(RunId);
id_type!(PlanRevisionId);
id_type!(TaskId);
id_type!(AttemptId);
id_type!(AgentSessionId);
id_type!(WorktreeId);
id_type!(ApprovalId);
id_type!(ArtifactId);
id_type!(CommandRunId);
id_type!(ValidationId);
id_type!(EvidenceId);
id_type!(FindingId);
id_type!(OperationId);
id_type!(PublicationId);

#[must_use]
pub fn now_ms() -> i64 {
    let millis = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[must_use]
pub fn format_timestamp(milliseconds: i64) -> String {
    let Ok(value) = OffsetDateTime::from_unix_timestamp_nanos(i128::from(milliseconds) * 1_000_000)
    else {
        return "1970-01-01T00:00:00Z".to_owned();
    };
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    Created,
    Preparing,
    Interviewing,
    ReadyForArchitecture,
    Architecting,
    PlanAdversarialReview,
    PlanRevisionRequired,
    PlanReviewRequired,
    ReadyToExecute,
    Executing,
    TaskVerification,
    IntegrationReady,
    Integrating,
    IntegrationVerification,
    FinalAudit,
    HumanReview,
    PublicationReady,
    DraftPrCreated,
    Completed,
    Paused,
    Blocked,
    Stopping,
    Canceled,
    Failed,
    Archived,
}

impl RunState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Canceled | Self::Failed | Self::Archived
        )
    }

    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        use RunState as S;
        if self == next {
            return true;
        }
        if matches!(
            next,
            S::Paused | S::Blocked | S::Stopping | S::Canceled | S::Failed
        ) {
            return !self.is_terminal();
        }
        matches!(
            (self, next),
            (S::Created, S::Preparing)
                | (S::Preparing, S::Interviewing | S::ReadyForArchitecture)
                | (S::Interviewing, S::ReadyForArchitecture)
                | (S::ReadyForArchitecture, S::Architecting)
                | (S::Architecting, S::ReadyForArchitecture)
                | (S::Architecting, S::PlanRevisionRequired)
                | (S::Architecting, S::PlanAdversarialReview)
                | (S::PlanAdversarialReview, S::PlanRevisionRequired)
                | (S::PlanAdversarialReview, S::PlanReviewRequired)
                | (S::PlanRevisionRequired, S::Architecting)
                | (S::PlanReviewRequired, S::PlanAdversarialReview)
                | (S::PlanReviewRequired, S::PlanRevisionRequired)
                | (S::PlanReviewRequired, S::Completed)
                | (S::PlanReviewRequired, S::ReadyToExecute)
                | (S::ReadyToExecute, S::Executing)
                | (S::Executing, S::TaskVerification)
                | (S::TaskVerification, S::IntegrationReady)
                | (S::IntegrationReady, S::Integrating)
                | (S::Integrating, S::IntegrationVerification)
                | (S::IntegrationVerification, S::FinalAudit)
                | (S::FinalAudit, S::Executing)
                | (S::FinalAudit, S::HumanReview)
                | (S::HumanReview, S::Executing)
                | (S::HumanReview, S::PublicationReady)
                | (S::PublicationReady, S::DraftPrCreated)
                | (S::PublicationReady, S::Completed)
                | (S::DraftPrCreated, S::Completed)
                | (
                    S::Paused,
                    S::ReadyToExecute | S::Executing | S::TaskVerification
                )
                | (
                    S::Blocked,
                    S::Interviewing
                        | S::ReadyForArchitecture
                        | S::PlanAdversarialReview
                        | S::PlanRevisionRequired
                        | S::PlanReviewRequired
                        | S::ReadyToExecute
                        | S::Executing
                )
                | (S::Stopping, S::Canceled)
                | (S::Completed | S::Canceled | S::Failed, S::Archived)
        )
    }
}

impl fmt::Display for RunState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            serde_json::to_value(self)
                .unwrap_or_default()
                .as_str()
                .unwrap_or("UNKNOWN"),
        )
    }
}

impl FromStr for RunState {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        serde_json::from_value(Value::String(value.to_owned()))
            .map_err(|_| DomainError::UnknownState(value.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    Proposed,
    Ready,
    WaitingDependency,
    WaitingResource,
    WaitingApproval,
    Leased,
    Starting,
    Implementing,
    ReviewReady,
    Verifying,
    ChangesRequested,
    Verified,
    IntegrationQueued,
    Integrating,
    Integrated,
    CiProven,
    LiveProven,
    Closed,
    Blocked,
    NeedsHelp,
    Stalled,
    Interrupted,
    Failed,
    Superseded,
    Canceled,
}

impl TaskState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Closed | Self::Failed | Self::Superseded | Self::Canceled
        )
    }

    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        use TaskState as S;
        if self == next {
            return true;
        }
        if matches!(
            next,
            S::WaitingDependency
                | S::WaitingResource
                | S::WaitingApproval
                | S::Blocked
                | S::NeedsHelp
                | S::Stalled
                | S::Interrupted
                | S::Failed
                | S::Canceled
        ) {
            return !self.is_terminal();
        }
        matches!(
            (self, next),
            (S::Proposed, S::Ready)
                | (
                    S::Ready | S::WaitingDependency | S::WaitingResource,
                    S::Leased
                )
                | (S::Leased, S::Starting)
                | (S::Starting, S::Implementing)
                | (S::Implementing, S::ReviewReady)
                | (S::ReviewReady, S::Verifying)
                | (S::Verifying, S::ChangesRequested | S::Verified)
                | (S::ChangesRequested, S::Ready | S::Superseded)
                | (S::Verified, S::IntegrationQueued)
                | (S::IntegrationQueued, S::Integrating)
                | (S::Integrating, S::Integrated)
                | (
                    S::Integrated,
                    S::ChangesRequested | S::Verified | S::CiProven | S::Closed
                )
                | (S::CiProven, S::LiveProven | S::Closed)
                | (S::LiveProven, S::Closed)
                | (
                    S::Blocked | S::NeedsHelp | S::Interrupted | S::Stalled | S::Failed,
                    S::Ready | S::Superseded
                )
        )
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            serde_json::to_value(self)
                .unwrap_or_default()
                .as_str()
                .unwrap_or("UNKNOWN"),
        )
    }
}

impl FromStr for TaskState {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        serde_json::from_value(Value::String(value.to_owned()))
            .map_err(|_| DomainError::UnknownState(value.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultClass {
    Success,
    NotSelected,
    SourceFailure,
    InfrastructureUnavailable,
    Inconclusive,
    CancelledSuperseded,
    SkippedDraft,
    QuarantinedFailure,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Interviewer,
    Architect,
    PlanReviewer,
    Explorer,
    Governor,
    Worker,
    HighRiskWorker,
    Integrator,
    Verifier,
    FinalAuditor,
    CiTriage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClass {
    Control,
    Medium,
    Heavy,
    Hardware(String),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ProofTier {
    T0,
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetState {
    Normal,
    Warning,
    Critical,
    Exhausted,
}

#[must_use]
pub fn budget_state(used: u64, budget: u64) -> BudgetState {
    if budget == 0 || used >= budget {
        BudgetState::Exhausted
    } else if used.saturating_mul(100) >= budget.saturating_mul(90) {
        BudgetState::Critical
    } else if used.saturating_mul(100) >= budget.saturating_mul(70) {
        BudgetState::Warning
    } else {
        BudgetState::Normal
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiffBudget {
    pub files: u32,
    pub lines: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskMilestone {
    pub id: String,
    pub title: String,
    pub objective: String,
    pub success_criteria: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskPacket {
    pub schema: String,
    pub program_id: String,
    pub task_id: String,
    pub title: String,
    pub state: String,
    pub priority: String,
    pub execution_mode: String,
    pub owner_profile: String,
    pub reviewer_profile: String,
    pub checklist_rows: Vec<String>,
    pub authority_refs: Vec<String>,
    pub base_sha: String,
    #[serde(default)]
    pub dependency_shas: std::collections::BTreeMap<String, String>,
    pub depends_on: Vec<String>,
    pub owned_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub reserved_serial_paths: Vec<String>,
    pub objective: String,
    /// Human-reviewable, bounded outcomes inside a governor-owned objective.
    /// Kept on the task packet so the plan remains useful even when the
    /// governor thread is not running. Older stored packets deserialize with
    /// an empty list and are bootstrapped by the first structured checkpoint.
    #[serde(default)]
    pub milestones: Vec<TaskMilestone>,
    pub non_goals: Vec<String>,
    pub success_criteria: Vec<String>,
    pub required_positive_tests: Vec<String>,
    pub required_negative_tests: Vec<String>,
    pub required_metrics: Vec<String>,
    pub required_evidence: Vec<String>,
    pub proof_limits: Vec<String>,
    pub diff_budget: DiffBudget,
    pub token_budget: u64,
    #[serde(default)]
    pub tool_budget: Option<u64>,
    pub lease_expires_at: String,
    pub stop_conditions: Vec<String>,
    pub handoff_path: String,
    #[serde(default)]
    pub risk_flags: Vec<String>,
}

impl TaskPacket {
    #[must_use]
    pub fn is_high_risk(&self) -> bool {
        self.risk_flags.iter().any(|flag| {
            matches!(
                flag.as_str(),
                "canonical_contract"
                    | "generated_contract"
                    | "migration"
                    | "tenancy"
                    | "authentication"
                    | "authorization"
                    | "privacy"
                    | "unsafe_native"
                    | "hardware"
                    | "ota_release"
                    | "ci_required_context"
                    | "serial_path"
            )
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunPlan {
    pub schema: String,
    pub summary: String,
    pub tasks: Vec<TaskPacket>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: Option<u64>,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
    pub model_context_window: Option<u64>,
}

impl TokenUsage {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.cached_input_tokens > self.input_tokens {
            return Err(DomainError::InvalidUsage(
                "cached input exceeds input".to_owned(),
            ));
        }
        if self.cache_write_input_tokens.unwrap_or_default() > self.input_tokens {
            return Err(DomainError::InvalidUsage(
                "cache-write input exceeds input".to_owned(),
            ));
        }
        if self.reasoning_output_tokens > self.output_tokens {
            return Err(DomainError::InvalidUsage(
                "reasoning output exceeds output".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PricingSnapshot {
    pub id: String,
    pub model: String,
    pub effective_at: String,
    pub input_microusd_per_million: u64,
    pub cached_input_microusd_per_million: u64,
    pub output_microusd_per_million: u64,
    pub cache_write_multiplier_numerator: u64,
    pub cache_write_multiplier_denominator: u64,
    pub long_context_threshold_tokens: Option<u64>,
    pub long_context_input_multiplier_numerator: Option<u64>,
    pub long_context_input_multiplier_denominator: Option<u64>,
    pub long_context_output_multiplier_numerator: Option<u64>,
    pub long_context_output_multiplier_denominator: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CostEstimate {
    pub lower_microusd: u64,
    pub upper_microusd: u64,
    pub confidence: CostConfidence,
    pub pricing_snapshot_ids: Vec<String>,
    pub explanation: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostConfidence {
    Exact,
    Bounded,
    #[default]
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComponentStatus {
    pub state: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CodexRuntimeStatus {
    pub state: String,
    pub detail: Option<String>,
    pub version: Option<String>,
    pub required_version: Option<String>,
    pub protocol_schema_sha256: Option<String>,
    pub schema_match: bool,
    #[serde(default)]
    pub native_multi_agent: bool,
    #[serde(default)]
    pub native_multi_agent_feature: Option<String>,
    pub pid: Option<u32>,
    pub restart_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerStatus {
    pub paused: bool,
    pub active_total: u32,
    pub max_total: u32,
    pub active_mutable: u32,
    pub max_mutable: u32,
    pub active_verifiers: u32,
    pub max_verifiers: u32,
    pub queued_tasks: u32,
}

/// Deliberately closed: later modes cannot become active until a reviewed build
/// adds both the enum member and its capability implementation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImprovementMode {
    #[default]
    Disabled,
    ObserveOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImprovementRuntimeStatus {
    pub configured_mode: ImprovementMode,
    pub effective_mode: ImprovementMode,
    pub anchor_sha256: String,
    pub configured_anchor_sha256: String,
    pub anchor_match: bool,
    pub observation_enabled: bool,
    pub candidate_generation_enabled: bool,
    pub candidate_execution_enabled: bool,
    pub detail: Option<String>,
}

impl ImprovementRuntimeStatus {
    #[must_use]
    pub fn from_config(
        configured_mode: ImprovementMode,
        anchor_sha256: &str,
        configured_anchor_sha256: &str,
        anchor_match: bool,
    ) -> Self {
        let observation_enabled = anchor_match && configured_mode == ImprovementMode::ObserveOnly;
        Self {
            configured_mode,
            effective_mode: if observation_enabled {
                ImprovementMode::ObserveOnly
            } else {
                ImprovementMode::Disabled
            },
            anchor_sha256: anchor_sha256.to_owned(),
            configured_anchor_sha256: configured_anchor_sha256.to_owned(),
            anchor_match,
            observation_enabled,
            // SI-001 exposes no candidate-producing or candidate-running path.
            candidate_generation_enabled: false,
            candidate_execution_enabled: false,
            detail: (!anchor_match).then(|| {
                "frozen safety-anchor digest mismatch; improvement capabilities are disabled"
                    .to_owned()
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImprovementDimension {
    ContextSourceOrdering,
    TokenBudget,
    ReadOnlyProbeParameters,
    RetryTiming,
    CacheSummaryPolicy,
    RolePrompts,
    PlanningContracts,
    Skills,
    ProceduralMemory,
    ModelEffortRouting,
    DelegationStrategy,
    ValidatorSelection,
    ValidatorThresholds,
    KnowledgeRetrieval,
    CompactionHandoff,
    ControllerStateMachines,
    GitWorktreePathCustody,
    SandboxNetworkPolicy,
    ApprovalSemantics,
    SecretHandlingRedaction,
    EvidenceResultSemantics,
    TasksetHoldoutAccess,
    GraderPromotionIsolation,
    ExternalWritesPublicationMerge,
    DatabaseIntegrityMigration,
    FrozenSafetyAnchor,
}

impl ImprovementDimension {
    #[must_use]
    pub const fn risk_class(self) -> ImprovementRiskClass {
        match self {
            Self::ContextSourceOrdering
            | Self::TokenBudget
            | Self::ReadOnlyProbeParameters
            | Self::RetryTiming
            | Self::CacheSummaryPolicy => ImprovementRiskClass::Green,
            Self::RolePrompts
            | Self::PlanningContracts
            | Self::Skills
            | Self::ProceduralMemory
            | Self::ModelEffortRouting
            | Self::DelegationStrategy
            | Self::ValidatorSelection
            | Self::ValidatorThresholds
            | Self::KnowledgeRetrieval
            | Self::CompactionHandoff => ImprovementRiskClass::Amber,
            _ => ImprovementRiskClass::Red,
        }
    }
}

impl FromStr for ImprovementDimension {
    type Err = CandidateEditValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let dimension = match value {
            "context_source_ordering" => Self::ContextSourceOrdering,
            "token_budget" => Self::TokenBudget,
            "read_only_probe_parameters" => Self::ReadOnlyProbeParameters,
            "retry_timing" => Self::RetryTiming,
            "cache_summary_policy" => Self::CacheSummaryPolicy,
            "role_prompts" => Self::RolePrompts,
            "planning_contracts" => Self::PlanningContracts,
            "skills" => Self::Skills,
            "procedural_memory" => Self::ProceduralMemory,
            "model_effort_routing" => Self::ModelEffortRouting,
            "delegation_strategy" => Self::DelegationStrategy,
            "validator_selection" => Self::ValidatorSelection,
            "validator_thresholds" => Self::ValidatorThresholds,
            "knowledge_retrieval" => Self::KnowledgeRetrieval,
            "compaction_handoff" => Self::CompactionHandoff,
            "controller_state_machines" => Self::ControllerStateMachines,
            "git_worktree_path_custody" => Self::GitWorktreePathCustody,
            "sandbox_network_policy" => Self::SandboxNetworkPolicy,
            "approval_semantics" => Self::ApprovalSemantics,
            "secret_handling_redaction" => Self::SecretHandlingRedaction,
            "evidence_result_semantics" => Self::EvidenceResultSemantics,
            "taskset_holdout_access" => Self::TasksetHoldoutAccess,
            "grader_promotion_isolation" => Self::GraderPromotionIsolation,
            "external_writes_publication_merge" => Self::ExternalWritesPublicationMerge,
            "database_integrity_migration" => Self::DatabaseIntegrityMigration,
            "frozen_safety_anchor" => Self::FrozenSafetyAnchor,
            _ => {
                return Err(CandidateEditValidationError::UnknownDimension(
                    value.to_owned(),
                ));
            }
        };
        Ok(dimension)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImprovementRiskClass {
    Green,
    Amber,
    Red,
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum CandidateEditValidationError {
    #[error("unknown improvement dimension: {0}")]
    UnknownDimension(String),
    #[error("protected Red improvement dimension: {0}")]
    ProtectedDimension(String),
    #[error("improvement dimension {dimension} requires risk class {expected:?}, not {actual:?}")]
    ContradictoryRiskClass {
        dimension: String,
        expected: ImprovementRiskClass,
        actual: ImprovementRiskClass,
    },
}

/// Candidate edits are limited to the explicit Green and Amber action space.
pub fn validate_candidate_edit_dimension(
    value: &str,
) -> Result<ImprovementDimension, CandidateEditValidationError> {
    let dimension = ImprovementDimension::from_str(value)?;
    if dimension.risk_class() == ImprovementRiskClass::Red {
        return Err(CandidateEditValidationError::ProtectedDimension(
            value.to_owned(),
        ));
    }
    Ok(dimension)
}

/// A wire candidate must label each editable dimension with its fixed taxonomy.
pub fn validate_candidate_edit(
    value: &str,
    risk_class: ImprovementRiskClass,
) -> Result<ImprovementDimension, CandidateEditValidationError> {
    let dimension = validate_candidate_edit_dimension(value)?;
    let expected = dimension.risk_class();
    if risk_class != expected {
        return Err(CandidateEditValidationError::ContradictoryRiskClass {
            dimension: value.to_owned(),
            expected,
            actual: risk_class,
        });
    }
    Ok(dimension)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeStatus {
    pub daemon: ComponentStatus,
    pub codex: CodexRuntimeStatus,
    pub database: ComponentStatus,
    pub scheduler: SchedulerStatus,
    pub self_improvement: ImprovementRuntimeStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositorySummary {
    pub id: RepositoryId,
    pub profile_id: String,
    pub display_name: String,
    pub root_path: String,
    pub origin_url: Option<String>,
    pub default_branch: String,
    pub primary_branch: Option<String>,
    pub primary_head: Option<String>,
    pub primary_clean: bool,
    pub health: String,
    pub blockers: Vec<String>,
    pub managed_worktree_count: u32,
    pub authority_digest: Option<String>,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSummary {
    pub id: RunId,
    pub repository_id: RepositoryId,
    pub title: String,
    pub objective: String,
    pub mode: String,
    pub publication_mode: String,
    pub state: RunState,
    pub phase: String,
    pub base_ref: String,
    pub base_sha: String,
    pub integration_branch: Option<String>,
    pub integration_sha: Option<String>,
    pub authority_digest: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub scheduler_paused: bool,
    pub run_token_budget: Option<u64>,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSummary {
    pub id: TaskId,
    pub run_id: RunId,
    pub external_task_id: String,
    pub title: String,
    pub objective: String,
    pub state: TaskState,
    pub priority: String,
    pub owner_profile: String,
    pub reviewer_profile: String,
    pub attempt: u32,
    pub base_sha: String,
    pub head_sha: Option<String>,
    pub token_budget: Option<u64>,
    pub dependencies: Vec<String>,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSummary {
    pub id: AgentSessionId,
    pub parent_agent_id: Option<AgentSessionId>,
    pub task_id: Option<TaskId>,
    pub role: AgentRole,
    pub codex_account_id: Option<String>,
    pub nickname: Option<String>,
    pub state: String,
    pub requested_model: String,
    pub effective_model: Option<String>,
    pub requested_reasoning_effort: String,
    pub effective_reasoning_effort: Option<String>,
    pub sandbox_mode: SandboxMode,
    pub cwd: String,
    pub current_goal: Option<String>,
    pub current_action: Option<String>,
    pub token_budget: Option<u64>,
    pub tokens_used: u64,
    /// Usage charged against the currently displayed bounded allowance. This
    /// equals `tokens_used` for fresh agents and is rebased for warm governor
    /// turns without changing the cumulative accounting ledger.
    #[serde(default)]
    pub budget_tokens_used: u64,
    pub estimated_cost_lower: String,
    pub estimated_cost_upper: String,
    pub heartbeat_at: Option<String>,
    pub thread_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub active_turn_started_at: Option<String>,
    pub active_turn_usage: Option<TokenUsage>,
    pub context_strategy: String,
    pub context_source_attempt_id: Option<AttemptId>,
    pub context_reuse_reason: Option<String>,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorktreeSummary {
    pub id: WorktreeId,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub kind: String,
    pub path: String,
    pub branch: Option<String>,
    pub base_sha: String,
    pub head_sha: Option<String>,
    pub state: String,
    pub preserved_reason: Option<String>,
    pub dirty: bool,
    pub files_changed: u32,
    pub additions: u64,
    pub deletions: u64,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalSummary {
    pub id: ApprovalId,
    pub run_id: RunId,
    pub agent_id: Option<AgentSessionId>,
    pub task_id: Option<TaskId>,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub approval_type: String,
    pub risk_level: RiskLevel,
    pub request: Value,
    pub state: String,
    pub decision: Option<String>,
    pub created_at: String,
    pub resolved_at: Option<String>,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityItem {
    pub id: String,
    pub sequence: i64,
    pub kind: String,
    pub state: String,
    pub summary: Option<String>,
    pub payload: Value,
    pub occurred_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatestAgentMessage {
    pub id: String,
    pub text: String,
    pub phase: Option<String>,
    pub occurred_at: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageSummary {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: Option<u64>,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
    pub cost: CostEstimate,
    pub by_model: Vec<ModelUsageSummary>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ModelUsageSummary {
    pub model: String,
    pub turns: u64,
    pub usage: TokenUsage,
    pub cost: CostEstimate,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageGroup {
    pub id: String,
    pub label: String,
    pub detail: String,
    pub turns: u64,
    pub usage: TokenUsage,
    pub cost: CostEstimate,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageBreakdown {
    pub total: UsageSummary,
    pub by_account: Vec<UsageGroup>,
    pub by_repository: Vec<UsageGroup>,
    pub by_agent: Vec<UsageGroup>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: i64,
    pub run_id: Option<RunId>,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub occurred_at: i64,
    pub payload: Value,
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("unknown state: {0}")]
    UnknownState(String),
    #[error("illegal state transition from {from} to {to}")]
    IllegalTransition { from: String, to: String },
    #[error("invalid usage counters: {0}")]
    InvalidUsage(String),
    #[error("validation failed: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_state_blocks_false_completion_jump() {
        assert!(!RunState::Executing.can_transition_to(RunState::Completed));
        assert!(RunState::HumanReview.can_transition_to(RunState::PublicationReady));
        assert!(RunState::HumanReview.can_transition_to(RunState::Executing));
        assert!(RunState::FinalAudit.can_transition_to(RunState::Executing));
    }

    #[test]
    fn plan_requires_adversarial_certification_before_approval() {
        assert!(RunState::Architecting.can_transition_to(RunState::PlanAdversarialReview));
        assert!(!RunState::Architecting.can_transition_to(RunState::PlanReviewRequired));
        assert!(RunState::PlanAdversarialReview.can_transition_to(RunState::PlanRevisionRequired));
        assert!(RunState::PlanRevisionRequired.can_transition_to(RunState::Architecting));
        assert!(RunState::PlanAdversarialReview.can_transition_to(RunState::PlanReviewRequired));
        assert!(RunState::PlanReviewRequired.can_transition_to(RunState::PlanRevisionRequired));
        assert!(RunState::PlanReviewRequired.can_transition_to(RunState::PlanAdversarialReview));
    }

    #[test]
    fn optional_interview_has_one_human_gate_before_architecture() {
        assert!(RunState::Preparing.can_transition_to(RunState::Interviewing));
        assert!(RunState::Preparing.can_transition_to(RunState::ReadyForArchitecture));
        assert!(RunState::Interviewing.can_transition_to(RunState::ReadyForArchitecture));
        assert!(!RunState::Interviewing.can_transition_to(RunState::Architecting));
    }

    #[test]
    fn workers_cannot_self_verify_by_transition() {
        assert!(!TaskState::Implementing.can_transition_to(TaskState::Verified));
        assert!(TaskState::Verifying.can_transition_to(TaskState::Verified));
        assert!(TaskState::Integrated.can_transition_to(TaskState::ChangesRequested));
        assert!(TaskState::Integrated.can_transition_to(TaskState::Verified));
        assert!(TaskState::Integrated.can_transition_to(TaskState::CiProven));
    }

    #[test]
    fn budget_boundaries_match_contract() {
        assert_eq!(budget_state(69, 100), BudgetState::Normal);
        assert_eq!(budget_state(70, 100), BudgetState::Warning);
        assert_eq!(budget_state(90, 100), BudgetState::Critical);
        assert_eq!(budget_state(100, 100), BudgetState::Exhausted);
    }

    #[test]
    fn reasoning_must_be_output_subset() {
        let usage = TokenUsage {
            output_tokens: 2,
            reasoning_output_tokens: 3,
            ..TokenUsage::default()
        };
        assert!(usage.validate().is_err());
    }

    #[test]
    fn candidate_edits_reject_all_red_and_unknown_dimensions() {
        for dimension in [
            "controller_state_machines",
            "git_worktree_path_custody",
            "sandbox_network_policy",
            "approval_semantics",
            "secret_handling_redaction",
            "evidence_result_semantics",
            "taskset_holdout_access",
            "grader_promotion_isolation",
            "external_writes_publication_merge",
            "database_integrity_migration",
            "frozen_safety_anchor",
        ] {
            assert!(matches!(
                validate_candidate_edit_dimension(dimension),
                Err(CandidateEditValidationError::ProtectedDimension(_))
            ));
        }
        assert!(matches!(
            validate_candidate_edit_dimension("a_later_dimension"),
            Err(CandidateEditValidationError::UnknownDimension(_))
        ));
        assert!(validate_candidate_edit_dimension("context_source_ordering").is_ok());
        assert!(validate_candidate_edit_dimension("role_prompts").is_ok());
    }

    #[test]
    fn candidate_edit_risk_must_match_the_fixed_dimension_taxonomy() {
        assert!(
            validate_candidate_edit("context_source_ordering", ImprovementRiskClass::Green).is_ok()
        );
        assert!(validate_candidate_edit("role_prompts", ImprovementRiskClass::Amber).is_ok());
        assert!(matches!(
            validate_candidate_edit("role_prompts", ImprovementRiskClass::Green),
            Err(CandidateEditValidationError::ContradictoryRiskClass { .. })
        ));
        assert!(matches!(
            validate_candidate_edit("frozen_safety_anchor", ImprovementRiskClass::Red),
            Err(CandidateEditValidationError::ProtectedDimension(_))
        ));
        assert!(matches!(
            validate_candidate_edit("later_dimension", ImprovementRiskClass::Green),
            Err(CandidateEditValidationError::UnknownDimension(_))
        ));
    }
}
