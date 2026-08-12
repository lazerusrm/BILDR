use std::path::PathBuf;

use harness_domain::{
    AgentRole, AgentSessionId, ApprovalId, ArtifactId, AttemptId, CommandRunId, EvidenceId,
    ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState, ProofTier,
    RepositoryId, ResultClass, RetentionClass, RiskLevel, RunId, SandboxMode, SensitivityClass,
    TaskId, TaskPacket, ValidationId, WorktreeId,
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
