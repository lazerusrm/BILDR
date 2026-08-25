use std::path::PathBuf;

use harness_domain::{
    AgentRole, AgentSessionId, ApprovalId, ArtifactId, AttemptId, CommandRunId, EvidenceId,
    ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState,
    InvestigationArtifactId, LivenessEpisodeId, ProofTier, ReconciliationEpisodeId, RepositoryId,
    ResultClass, RetentionClass, RiskLevel, RunId, SandboxMode, SensitivityClass, TaskId,
    TaskPacket, ValidationId, WorktreeId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize)]
pub struct DatabaseHealth {
    pub ready: bool,
    pub integrity: String,
    pub journal_mode: String,
    pub schema_version: String,
    pub raw_event_count: i64,
    pub projection_lag: i64,
}

#[derive(Clone, Debug)]
pub struct NewRepository {
    pub id: RepositoryId,
    pub profile_id: String,
    pub profile_version: u32,
    pub display_name: String,
    pub root_path: PathBuf,
    pub origin_url: Option<String>,
    pub default_branch: String,
    pub expected_coordination_branch: Option<String>,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct RepositoryHealthInput {
    pub repository_id: RepositoryId,
    pub primary_branch: Option<String>,
    pub primary_head_sha: Option<String>,
    pub primary_clean: bool,
    pub origin_head_sha: Option<String>,
    pub git_identity_name_present: bool,
    pub git_identity_email_present: bool,
    pub authority_digest: Option<String>,
    pub blockers: Vec<String>,
    pub details: Value,
}

#[derive(Clone, Debug)]
pub struct NewRun {
    pub id: RunId,
    pub repository_id: RepositoryId,
    pub title: String,
    pub objective: String,
    pub mode: String,
    pub publication_mode: String,
    pub state: String,
    pub phase: String,
    pub base_ref: String,
    pub base_sha: String,
    pub authority_digest: String,
    pub profile_digest: String,
    pub codex_version: Option<String>,
    pub protocol_schema_sha256: Option<String>,
    pub requested_by: String,
    pub token_budget: Option<u64>,
}

/// Controller-selected route receipt owned by exactly one run. This is a
/// write-once custody record, deliberately separate from operational metadata.
#[derive(Clone, Debug)]
pub struct NewImmutableRunModelRoute {
    pub run_id: RunId,
    pub schema: String,
    pub provider: String,
    pub model: String,
    pub reasoning_effort: String,
    pub model_profile_sha256: Option<String>,
    pub route_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableRunModelRoute {
    pub run_id: RunId,
    pub schema: String,
    pub provider: String,
    pub model: String,
    pub reasoning_effort: String,
    pub model_profile_sha256: Option<String>,
    pub route_sha256: String,
}

/// Immutable launch-time binding between a controller session and the route
/// receipt for its run. It makes provider identity durable even where model
/// slugs overlap between providers.
#[derive(Clone, Debug)]
pub struct NewAgentModelRouteBinding {
    pub agent_session_id: AgentSessionId,
    pub run_id: RunId,
    pub provider: String,
    pub model: String,
    pub reasoning_effort: String,
    pub model_profile_sha256: Option<String>,
    pub route_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentModelRouteBinding {
    pub agent_session_id: AgentSessionId,
    pub run_id: RunId,
    pub provider: String,
    pub model: String,
    pub reasoning_effort: String,
    pub model_profile_sha256: Option<String>,
    pub route_sha256: String,
}

#[derive(Clone, Debug)]
pub struct NewWorktree {
    pub id: WorktreeId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub kind: String,
    pub path: PathBuf,
    pub branch: Option<String>,
    pub base_sha: String,
    pub head_sha: Option<String>,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct NewAgentSession {
    pub id: AgentSessionId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub parent_agent_session_id: Option<AgentSessionId>,
    pub runtime_kind: String,
    pub codex_account_id: Option<String>,
    pub role: AgentRole,
    pub nickname: Option<String>,
    pub requested_model: String,
    pub requested_reasoning_effort: String,
    pub sandbox_mode: SandboxMode,
    pub approval_policy: String,
    pub cwd: PathBuf,
    pub state: String,
    pub current_goal: Option<String>,
    pub token_budget: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct RawEventInput {
    pub run_id: Option<RunId>,
    pub agent_session_id: Option<AgentSessionId>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub direction: String,
    pub method: String,
    pub request_id: Option<String>,
    pub payload: Value,
    pub source_sequence: Option<String>,
    pub redaction_class: String,
}

#[derive(Clone, Debug)]
pub struct NewApproval {
    pub id: ApprovalId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub agent_session_id: Option<AgentSessionId>,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub approval_type: String,
    pub risk_level: RiskLevel,
    pub request: Value,
    pub expected_head_sha: Option<String>,
    pub expected_worktree_fingerprint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NewTaskAttempt {
    pub id: AttemptId,
    pub task_id: TaskId,
    pub attempt_number: u32,
    pub state: String,
    pub packet: TaskPacket,
    pub packet_sha256: String,
    pub base_sha: String,
    pub requested_model_route: String,
}

#[derive(Clone, Debug)]
pub struct PriorAttemptContext {
    pub attempt_id: AttemptId,
    pub attempt_number: u32,
    pub state: String,
    pub terminal_class: Option<String>,
    pub failure_reason: Option<String>,
    pub worktree_path: Option<PathBuf>,
    pub agent_id: Option<AgentSessionId>,
    pub role: Option<String>,
    pub requested_model: Option<String>,
    pub effective_model: Option<String>,
    pub requested_reasoning_effort: Option<String>,
    pub effective_reasoning_effort: Option<String>,
    pub tokens_used: u64,
    pub verifier_verdict: Option<String>,
    pub last_agent_message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NewArtifact {
    pub id: ArtifactId,
    pub run_id: Option<RunId>,
    pub task_attempt_id: Option<AttemptId>,
    pub kind: String,
    pub logical_name: String,
    pub storage_path: PathBuf,
    pub sha256: String,
    pub media_type: String,
    pub compression: Option<String>,
    pub sensitivity: String,
    pub byte_length: u64,
    pub retention_class: String,
    pub pinned: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub kind: String,
    pub logical_name: String,
    pub storage_path: PathBuf,
    pub sha256: String,
    pub media_type: String,
    pub byte_length: u64,
    pub verified_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct NewCommandRecord {
    pub id: CommandRunId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub agent_session_id: Option<AgentSessionId>,
    pub worktree_id: Option<WorktreeId>,
    pub command: Value,
    pub cwd: PathBuf,
    pub source_sha_before: Option<String>,
    pub source_sha_after: Option<String>,
    pub resource_class: String,
    pub host_identity: Option<String>,
    pub target_profile: Option<String>,
    pub started_at: i64,
    pub completed_at: i64,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub result_class: ResultClass,
    pub stdout_artifact_id: Option<ArtifactId>,
    pub stderr_artifact_id: Option<ArtifactId>,
    pub error: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct NewValidationRecord {
    pub id: ValidationId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub worktree_id: WorktreeId,
    pub validator_id: String,
    pub proof_tier: ProofTier,
    pub source_sha: String,
    pub selector_reason: String,
    pub result_class: ResultClass,
    pub command_run_id: Option<CommandRunId>,
    pub started_at: i64,
    pub completed_at: i64,
}

#[derive(Clone, Debug)]
pub struct NewEvidenceRecord {
    pub id: EvidenceId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub validation_id: Option<ValidationId>,
    pub claim_id: String,
    pub checklist_rows: Vec<String>,
    pub source_sha: String,
    pub proof_tier: ProofTier,
    pub result_class: ResultClass,
    pub evidence: Value,
    pub unproved_claims: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct NewContextPacket {
    pub id: String,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub role: String,
    pub base_sha: String,
    pub profile_digest: String,
    pub packet: Value,
    pub packet_sha256: String,
    pub estimated_tokens: u64,
    pub sources: Vec<ContextSourceRecord>,
}

#[derive(Clone, Debug)]
pub struct ContextSourceRecord {
    pub path: String,
    pub source_class: String,
    pub content_sha256: String,
    pub included: bool,
    pub reason: String,
    pub estimated_tokens: u64,
}

#[derive(Clone, Debug)]
pub struct NativeSubagentActivityRecord {
    pub parent_agent_session_id: AgentSessionId,
    pub parent_thread_id: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredSession {
    pub id: String,
    pub expires_at: i64,
    pub csrf_secret_hash: String,
    pub revoked: bool,
}

#[derive(Clone, Debug)]
pub struct NewImprovementRevision {
    pub id: String,
    pub aggregate_kind: ImprovementRecordKind,
    pub aggregate_id: String,
    pub schema: ImprovementSchema,
    pub state: ImprovementState,
    pub payload: Value,
    pub payload_sha256: String,
    pub sensitivity: SensitivityClass,
    pub retention_class: RetentionClass,
    pub export_allowed: bool,
    pub idempotency_key: String,
    pub event_id: ImprovementEventId,
    pub source_raw_event_id: Option<i64>,
    pub source_domain_event_id: Option<i64>,
}

/// A narrowly derived knowledge candidate from one immutable investigation
/// finding.  The controller, rather than the caller, derives the statement,
/// evidence receipt, sensitivity, retention, and deterministic identity.
#[derive(Clone, Debug)]
pub struct NewInvestigationKnowledgeCandidate {
    pub artifact_id: InvestigationArtifactId,
    pub expected_artifact_sha256: String,
    pub finding_id: String,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
}

/// An exact human review over the current immutable knowledge candidate. The
/// caller supplies no replacement statement, evidence, scope, or lifecycle
/// fields: the Store carries all candidate facts into the next revision.
#[derive(Clone, Debug)]
pub struct ReviewKnowledgeCandidate {
    pub knowledge_id: String,
    pub expected_knowledge_sha256: String,
    pub decision: harness_domain::KnowledgeReviewDecision,
    pub reviewer_id: String,
}

/// A narrowly derived, display-only knowledge candidate from repeated,
/// independently recovered liveness episodes. The controller derives all
/// statements and evidence from immutable observations; callers cannot submit
/// free-form learning content or activate the candidate.
#[derive(Clone, Debug)]
pub struct NewLivenessKnowledgeCandidate {
    pub episode_id: LivenessEpisodeId,
    pub expected_episode_sha256: String,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
}

/// A narrowly derived, display-only knowledge candidate from repeated
/// reconciliation episodes that each preserved custody without authorizing a
/// replacement. The controller derives every factual claim and source receipt;
/// callers cannot treat a preservation record as a successful recovery or
/// activate the candidate.
#[derive(Clone, Debug)]
pub struct NewReconciliationKnowledgeCandidate {
    pub episode_id: ReconciliationEpisodeId,
    pub expected_episode_sha256: String,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ImprovementRevisionRecord {
    pub id: String,
    pub aggregate_kind: ImprovementRecordKind,
    pub aggregate_id: String,
    pub revision: u64,
    pub schema: ImprovementSchema,
    pub state: ImprovementState,
    pub payload: Value,
    pub payload_sha256: String,
    pub sensitivity: SensitivityClass,
    pub retention_class: RetentionClass,
    pub export_allowed: bool,
    pub source_domain_event_id: Option<i64>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ImprovementEventRecord {
    pub id: ImprovementEventId,
    pub aggregate_kind: ImprovementRecordKind,
    pub aggregate_id: String,
    pub revision_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload_sha256: String,
    pub idempotency_key: String,
    pub source_raw_event_id: Option<i64>,
    pub occurred_at: i64,
}

#[derive(Clone, Debug)]
pub struct TraceProjectionSnapshot {
    pub run_id: RunId,
    pub base_sha: String,
    pub authority_digest: String,
    pub profile_digest: String,
    pub raw_events: Vec<TraceProjectionRawReceipt>,
    pub domain_events: Vec<TraceProjectionDomainReceipt>,
    pub structural_receipts: Vec<TraceProjectionStructuralReceipt>,
    pub relations: Vec<TraceProjectionRelation>,
    pub max_raw_event_id: i64,
    pub max_domain_event_id: i64,
    pub structural_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceProjectionWatermark {
    pub base_sha: String,
    pub authority_digest: String,
    pub profile_digest: String,
    pub max_raw_event_id: i64,
    pub max_domain_event_id: i64,
    pub structural_digest: String,
}

#[derive(Clone, Debug)]
pub struct TraceProjectionRawReceipt {
    pub agent_session_id: Option<String>,
    pub id: i64,
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

#[derive(Clone, Debug)]
pub struct TraceProjectionDomainReceipt {
    pub source_raw_event_id: Option<i64>,
    pub id: i64,
    pub event_type: String,
    pub occurred_at: i64,
    pub payload: Value,
    pub payload_sha256: String,
    pub redaction_class: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct TraceProjectionStructuralReceipt {
    pub id: String,
    pub kind: String,
    pub occurred_at: Option<i64>,
    pub metadata: Value,
}

#[derive(Clone, Debug)]
pub struct TraceProjectionRelation {
    pub from: String,
    pub to: String,
    pub kind: String,
}

#[derive(Clone, Debug)]
pub struct NewOperatorOutcome {
    pub run_id: RunId,
    pub subject: harness_domain::OutcomeSubject,
    pub dimension: harness_domain::OutcomeDimension,
    pub classification: harness_domain::OutcomeClassification,
    pub code: String,
    pub reason_code: Option<String>,
    pub note: Option<String>,
    pub correction_artifact_id: Option<ArtifactId>,
    pub supersedes: Vec<String>,
    pub actor: String,
    pub idempotency_key: String,
}

// SI-007 failure observation Store contracts. These are read-model records and
// closed edit inputs; they deliberately carry no free-text rationale/payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureProjectionReceipt {
    pub inserted: u64,
    pub already_projected: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureOccurrenceRecord {
    pub id: String,
    pub repository_id: RepositoryId,
    pub source_kind: String,
    pub source_id: String,
    pub terminal_code: Option<String>,
    pub automatic_class: String,
    pub severity: String,
    pub fingerprint_sha256: String,
    pub cost_scope_id: Option<String>,
    pub cost_lower_microusd: Option<u64>,
    pub cost_upper_microusd: Option<u64>,
    pub source_domain_event_id: Option<i64>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureClusterOverview {
    pub cluster_id: String,
    pub repository_id: RepositoryId,
    pub version: u64,
    pub occurrences: u64,
    pub unknown_cost_occurrences: u64,
    pub cost_lower_microusd: u64,
    pub cost_upper_microusd: u64,
    pub representative_occurrence_id: Option<String>,
    pub representative_run_id: Option<RunId>,
    pub representative_trace_id: Option<String>,
    pub effective_class: Option<String>,
    pub severity: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureTraceSummary {
    pub occurrence_id: String,
    pub source_kind: String,
    /// Digest of the durable source identity; never exposes raw IDs or text.
    pub source_receipt_sha256: String,
    pub source_domain_event_id: Option<i64>,
    pub automatic_class: String,
    pub severity: String,
}

/// Store-neutral read composition: an already-redacted persisted TraceV2
/// revision plus the closed outcome vector for its run.
#[derive(Clone, Debug)]
pub struct FailureTraceComposition {
    pub trace_id: String,
    pub run_id: RunId,
    pub trace_manifest: Value,
    pub outcomes: harness_domain::OutcomeVector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureSplitMove {
    pub occurrence_id: String,
    pub target_cluster_id: String,
    pub expected_target_version: u64,
}

/// A closed observation assembled only from a durable Store authority.
/// This is deliberately separate from `NewOperatorOutcome`: no actor, note,
/// or caller-provided provenance may cross this boundary.
#[derive(Clone, Debug)]
pub struct AuthoritativeOutcomeInput {
    pub run_id: RunId,
    pub subject: harness_domain::OutcomeSubject,
    pub dimension: harness_domain::OutcomeDimension,
    pub classification: harness_domain::OutcomeClassification,
    pub code: String,
    pub source_kind: harness_domain::OutcomeSourceKind,
    pub source_record_id: String,
    pub source_record_sha256: String,
    pub source_sha: Option<String>,
    pub source_domain_event_id: Option<i64>,
    pub observed_at: i64,
}

// SI-008–SI-012 custody inputs. Content is always an immutable harness-eval
// wire value or a digest; no fixture/answer/output text crosses this boundary.
#[derive(Clone, Debug)]
pub struct NewTasksetMembership {
    pub taskset_revision_id: String,
    pub eval_case_revision_id: String,
    pub ordinal: u64,
}

#[derive(Clone, Debug)]
pub struct NewEvaluationRun {
    pub id: String,
    pub controller_run_id: RunId,
    pub taskset_revision_id: String,
    pub grader_bundle_revision_id: String,
    pub base_sha: String,
    pub fixture_digest: String,
    pub runtime_digest: String,
    pub seed_policy_digest: String,
    pub champion_policy_digest: String,
    pub challenger_policy_digest: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvaluationRunReceipt {
    pub id: String,
    pub controller_run_id: RunId,
    pub taskset_revision_id: String,
    pub grader_bundle_revision_id: String,
    pub split: harness_eval::Split,
    pub status: EvaluationRunStatus,
    pub invalidated: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ImmutableRevision<T> {
    pub id: String,
    pub aggregate_id: String,
    pub revision: u64,
    pub payload_sha256: String,
    pub wire: T,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyChampionBinding {
    pub id: String,
    pub repository_id: RepositoryId,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
    pub policy_bundle_revision_id: String,
    pub bundle_sha256: String,
    pub previous_binding_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NewPolicyChampionBinding {
    pub id: String,
    pub repository_id: RepositoryId,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
    pub policy_bundle_revision_id: String,
    /// Controller-verified frozen anchor expected by this binding.
    pub expected_safety_anchor_digest: String,
    pub expected_previous_binding_id: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EvaluationLaunchPins {
    pub taskset_revision_id: String,
    pub grader_bundle_revision_id: String,
    pub split: harness_eval::Split,
    pub taskset: ImmutableRevision<harness_eval::TasksetV1>,
    pub grader_bundle: ImmutableRevision<harness_eval::GraderBundleV1>,
    pub eval_cases: Vec<ImmutableRevision<harness_eval::EvalCaseV1>>,
}

/// Read-only, server-derived materialization authority.  It contains only
/// durable IDs/digests and deliberately has no failure text or fixture body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureDevelopmentCaseSource {
    pub occurrence_id: String,
    pub repository_id: RepositoryId,
    pub run_id: RunId,
    pub base_sha: String,
    pub source_receipt_sha256: String,
    pub source_kind: String,
    pub source_domain_event_id: Option<i64>,
    pub trace_revision_id: Option<String>,
    pub trace_digest: Option<String>,
    pub outcome_revision_id: Option<String>,
    pub outcome_digest: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationRunStatus {
    Recording,
    Completed,
    InfrastructureUnavailable,
    Invalidated,
}

#[derive(Clone, Debug)]
pub struct NewEvaluationRunStatus {
    pub id: String,
    pub evaluation_run_id: String,
    pub status: EvaluationRunStatus,
    pub receipt_digest: String,
    pub idempotency_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationArm {
    Champion,
    Challenger,
}

#[derive(Clone, Debug)]
pub struct NewEvaluationSample {
    pub id: String,
    pub evaluation_run_id: String,
    pub controller_evidence_id: EvidenceId,
    pub grader_evidence_id: EvidenceId,
    pub eval_case_revision_id: String,
    pub arm: EvaluationArm,
    pub sample: harness_eval::EvalSampleV1,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvaluationSampleReceipt {
    pub id: String,
    pub evaluation_run_id: String,
    pub controller_evidence_id: EvidenceId,
    pub grader_evidence_id: EvidenceId,
    pub eval_case_revision_id: String,
    pub arm: EvaluationArm,
    pub seed: u64,
    pub classification: harness_eval::SampleClassification,
    pub sample_digest: String,
    pub invalidated: bool,
}

#[derive(Clone, Debug)]
pub struct NewPairedStatVerdict {
    pub id: String,
    pub champion_evaluation_run_id: String,
    pub challenger_evaluation_run_id: String,
    pub statistics: harness_eval::Statistics,
    pub critical_regression: bool,
    pub reward_integrity_pass: bool,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairedStatVerdictReceipt {
    pub id: String,
    pub invalidated: bool,
}

#[derive(Clone, Debug)]
pub struct NewHoldoutAccess {
    pub id: String,
    pub taskset_revision_id: Option<String>,
    pub eval_case_revision_id: Option<String>,
    pub principal: harness_eval::Principal,
    pub action: harness_eval::HoldoutAction,
    pub custody_digest: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HoldoutAccessReceipt {
    pub id: String,
    pub granted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationInvalidationTarget {
    EvaluationRun,
    EvaluationSample,
    StatVerdict,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationInvalidationReason {
    HoldoutLeakage,
    GraderDrift,
    FixtureDrift,
    CustodyViolation,
}
#[derive(Clone, Debug)]
pub struct NewEvaluationInvalidation {
    pub id: String,
    pub target: EvaluationInvalidationTarget,
    pub target_id: String,
    pub reason: EvaluationInvalidationReason,
    pub holdout_access_log_id: Option<String>,
    pub idempotency_key: String,
}
