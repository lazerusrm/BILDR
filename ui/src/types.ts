export type RunState =
  | "CREATED"
  | "PREPARING"
  | "INTERVIEWING"
  | "READY_FOR_ARCHITECTURE"
  | "ARCHITECTING"
  | "PLAN_ADVERSARIAL_REVIEW"
  | "PLAN_REVISION_REQUIRED"
  | "PLAN_REVIEW_REQUIRED"
  | "READY_TO_EXECUTE"
  | "EXECUTING"
  | "TASK_VERIFICATION"
  | "INTEGRATION_READY"
  | "INTEGRATING"
  | "INTEGRATION_VERIFICATION"
  | "FINAL_AUDIT"
  | "HUMAN_REVIEW"
  | "PUBLICATION_READY"
  | "DRAFT_PR_CREATED"
  | "COMPLETED"
  | "PAUSED"
  | "BLOCKED"
  | "STOPPING"
  | "CANCELED"
  | "FAILED"
  | "ARCHIVED";

export interface ComponentStatus {
  state: string;
  detail: string | null;
}

export interface RuntimeStatus {
  daemon: ComponentStatus;
  codex: {
    state: string;
    detail: string | null;
    version: string | null;
    required_version: string | null;
    protocol_schema_sha256: string | null;
    schema_match: boolean;
    native_multi_agent: boolean;
    native_multi_agent_feature: string | null;
    pid: number | null;
    restart_count: number;
  };
  database: ComponentStatus;
  scheduler: {
    paused: boolean;
    active_total: number;
    max_total: number;
    active_mutable: number;
    max_mutable: number;
    active_verifiers: number;
    max_verifiers: number;
    queued_tasks: number;
  };
  self_improvement: {
    configured_mode: "disabled" | "observe_only";
    effective_mode: "disabled" | "observe_only";
    anchor_sha256: string;
    configured_anchor_sha256: string;
    anchor_match: boolean;
    observation_enabled: boolean;
    candidate_generation_enabled: boolean;
    candidate_execution_enabled: boolean;
    detail: string | null;
  };
}

export type OutcomeDimension =
  | "operator_acceptance"
  | "operator_correction"
  | "validation"
  | "evidence"
  | "verifier_findings"
  | "completion_state"
  | "resource_use"
  | "ci_required_checks"
  | "review_regression"
  | "pr_reopened"
  | "rollback"
  | "downstream_regression";

export interface OutcomeVector {
  run_id: string;
  items: OutcomeVectorItem[];
}

export interface OutcomeVectorItem {
  outcome_id: string;
  subject: { kind: "run" | "task_attempt" | "publication"; id: string };
  dimension: OutcomeDimension;
  revisions: OutcomeRevision[];
  conflicted: boolean;
}

export interface FailureOverview {
  taxonomy_version: "harness.failure-taxonomy.v1";
  classified_occurrences: number;
  unknown_occurrences: number;
  clusters: FailureClusterSummary[];
}

export type FailureClass =
  | "unknown"
  | "policy_blocked"
  | "budget_exhausted"
  | "infrastructure_unavailable"
  | "protocol_error"
  | "integration_conflict"
  | "source_failure"
  | "inconclusive"
  | "cancelled_superseded";

export type FailureSeverity = "unknown" | "low" | "medium" | "high" | "critical";

export interface FailureClusterSummary {
  id: string;
  failure_class: FailureClass;
  frequency: number;
  severity: FailureSeverity;
  cost_upper_microusd: number | null;
  unknown_cost_occurrences: number;
  representative_occurrence_id: string | null;
  representative_run_id: string | null;
  representative_trace_id: string | null;
}

export type FailureTraceKind =
  | "system_message"
  | "developer_message"
  | "user_message"
  | "model_message"
  | "reasoning_summary"
  | "tool_request"
  | "tool_result"
  | "command"
  | "file_read"
  | "file_change"
  | "approval_request"
  | "approval_decision"
  | "compaction"
  | "subagent_spawn"
  | "subagent_join"
  | "validation"
  | "finding"
  | "operator_feedback"
  | "outcome"
  | "unknown_protocol"
  | "run_lifecycle"
  | "attempt_boundary"
  | "runtime_restart";

export type FailureTraceRedaction =
  | "none"
  | "secret_removed"
  | "private_reasoning_removed"
  | "customer_data_removed"
  | "content_withheld";

export interface FailureTraceRow {
  id: string;
  kind: FailureTraceKind;
  timestamp_ms: number | null;
  redaction_class: FailureTraceRedaction;
  source_receipt_count: number;
}

export interface FailureTrace {
  trace_id: string;
  run_id: string;
  rows: FailureTraceRow[];
  outcomes: OutcomeVector;
}

export type EvaluationSplit = "training" | "development" | "holdout" | "canary" | "quarantine";
export type EvaluationArm = "champion" | "challenger";
export type EvaluationRunStatus = "recording" | "completed" | "infrastructure_unavailable" | "invalidated";
export type EvaluationSampleClassification = "pass" | "fail" | "infrastructure_unavailable" | "invalidated";
export type FailureSourceKind = "attempt_terminal" | "run_terminal" | "typed_outcome";

/** Receipt-only M2 records; no fixture, command, evidence, or artifact payloads. */
export interface EvaluationRunSummary {
  id: string;
  controller_run_id: string;
  taskset_revision_id: string;
  grader_bundle_revision_id: string;
  split: EvaluationSplit;
  status: EvaluationRunStatus;
  invalidated: boolean;
}

export interface EvaluationSampleSummary {
  id: string;
  evaluation_run_id: string;
  eval_case_revision_id: string;
  arm: EvaluationArm;
  seed: number;
  classification: EvaluationSampleClassification;
  sample_digest: string;
  invalidated: boolean;
}

export interface EvaluationCaseSummary {
  revision_id: string;
  case_id: string;
  revision: number;
  payload_sha256: string;
  case_sha256: string;
  split: EvaluationSplit;
  task_family: string;
  base_sha: string;
  setup_digest: string;
  grader_bundle_id: string;
  grader_bundle_revision: number;
  grader_bundle_digest: string;
}

export interface EvaluationOccurrenceSource {
  occurrence_id: string;
  repository_id: string;
  run_id: string;
  base_sha: string;
  source_receipt_sha256: string;
  source_kind: FailureSourceKind;
  trace_revision_id: string | null;
  trace_digest: string | null;
  outcome_revision_id: string | null;
  outcome_digest: string | null;
}

export interface OutcomeRevision {
  revision_id: string;
  revision: number;
  outcome: {
    classification: "positive" | "negative" | "neutral" | "unknown";
    code: string;
    supersedes: string[];
  };
  is_head: boolean;
}

export interface Repository {
  id: string;
  profile_id: string;
  display_name: string;
  root_path: string;
  origin_url?: string;
  default_branch: string;
  primary_branch?: string;
  primary_head?: string;
  primary_clean: boolean;
  health: string;
  blockers: string[];
  managed_worktree_count: number;
  authority_digest?: string;
  version: number;
}

export interface RepositoryDiscovery {
  root_path: string;
  display_name: string;
  origin_url?: string;
  is_github: boolean;
  compatible: boolean;
  registered: boolean;
}

export interface OperatorSettings {
  store_reasoning_summaries: boolean;
  store_raw_reasoning: boolean;
  yolo_mode: boolean;
  allow_automatic_external_writes: boolean;
  automatic_external_writes_locked: boolean;
  automatic_account_handoff: boolean;
  adaptive_governor_budgets: boolean;
  automatic_governor_continuation: boolean;
  automatic_plan_approval: boolean;
  governor_goal_token_budget: number;
  governor_attempt_token_ceiling: number;
  recommended_governor_attempt_tokens: number;
  governor_budget_sample_count: number;
  governor_budget_reason: string;
}

export interface CodexRateLimitWindow {
  kind: "primary" | "secondary";
  used_percent: number;
  remaining_percent: number;
  window_duration_mins?: number;
  resets_at?: number;
}

export interface CodexRateLimit {
  limit_id: string;
  limit_name?: string;
  plan_type?: string;
  windows: CodexRateLimitWindow[];
}

export interface CodexAccountProfile {
  id: string;
  label: string;
  codex_home: string;
  selected: boolean;
  state: "detected" | "ready" | "signed_out" | "unavailable";
  account_type?: string;
  email?: string;
  plan_type?: string;
  rate_limits: CodexRateLimit[];
  observed_at?: number;
  detail?: string;
  managed?: boolean;
}

export interface CodexAccountsSnapshot {
  selected_account_id?: string;
  accounts: CodexAccountProfile[];
}

export interface CodexAccountLoginStatus {
  id: string;
  label: string;
  state: "waiting_for_user" | "completed" | "failed" | "canceling" | "canceled";
  verification_url?: string;
  user_code?: string;
  detail?: string;
}

export interface Run {
  id: string;
  repository_id: string;
  title: string;
  objective: string;
  mode: string;
  publication_mode: string;
  state: RunState;
  phase: string;
  base_ref: string;
  base_sha: string;
  integration_branch?: string;
  integration_sha?: string;
  authority_digest: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  failure_reason?: string | null;
  scheduler_paused: boolean;
  run_token_budget?: number;
  version: number;
}

export interface IntentBrief {
  refined_objective: string;
  intended_final_shape: string[];
  hard_constraints: string[];
  preferences: string[];
  non_goals: string[];
  acceptance_examples: string[];
  planner_may_decide: string[];
  assumptions_to_validate: string[];
}

export interface IntentInterviewSnapshot {
  schema: string;
  status:
    | "not_started"
    | "running"
    | "waiting_for_human"
    | "ready_for_confirmation"
    | "confirmed"
    | "skipped"
    | "failed";
  agent_id?: string | null;
  turn_count: number;
  messages: Array<{
    role: "human" | "interviewer" | string;
    kind: "question" | "answer" | "direction" | "brief_ready" | string;
    text: string;
    why_it_matters?: string | null;
    suggested_answer?: string | null;
    recorded_at: string;
  }>;
  draft_brief?: IntentBrief | null;
  draft_digest?: string | null;
  confirmed_brief?: IntentBrief | null;
  confirmed_digest?: string | null;
  started_at?: string | null;
  updated_at: string;
  confirmed_at?: string | null;
  skipped_at?: string | null;
  last_error?: string | null;
}

export interface Task {
  id: string;
  run_id: string;
  external_task_id: string;
  title: string;
  objective: string;
  state: string;
  priority: string;
  owner_profile: string;
  reviewer_profile: string;
  attempt: number;
  base_sha: string;
  head_sha?: string;
  token_budget?: number;
  dependencies: string[];
  failure_reason?: string | null;
  version: number;
}

export interface Agent {
  id: string;
  parent_agent_id?: string;
  task_id?: string;
  role: string;
  codex_account_id?: string;
  nickname?: string;
  state: string;
  requested_model: string;
  effective_model?: string;
  requested_reasoning_effort: string;
  effective_reasoning_effort?: string;
  sandbox_mode: string;
  cwd: string;
  current_goal?: string;
  current_action?: string;
  failure_reason?: string | null;
  started_at?: string;
  completed_at?: string | null;
  token_budget?: number;
  tokens_used: number;
  budget_tokens_used?: number;
  estimated_cost_lower: string;
  estimated_cost_upper: string;
  heartbeat_at?: string;
  thread_id?: string;
  active_turn_id?: string;
  active_turn_started_at?: string | null;
  active_turn_usage?: TokenUsageSnapshot | null;
  context_strategy?: string;
  context_source_attempt_id?: string;
  context_reuse_reason?: string;
  version: number;
}

export interface TokenUsageSnapshot {
  input_tokens: number;
  cached_input_tokens: number;
  cache_write_input_tokens?: number | null;
  output_tokens: number;
  reasoning_output_tokens: number;
  total_tokens: number;
  model_context_window?: number | null;
}

export interface Worktree {
  id: string;
  run_id: string;
  task_id?: string;
  kind: string;
  path: string;
  branch?: string;
  base_sha: string;
  head_sha?: string;
  state: string;
  preserved_reason?: string;
  dirty: boolean;
  files_changed: number;
  additions: number;
  deletions: number;
  version: number;
}

export interface WorktreeDiffSummary {
  worktree_id: string;
  state: "clean" | "uncommitted" | "committed" | "committed_and_uncommitted";
  dirty: boolean;
  head_changed: boolean;
  files_changed: number;
  additions: number;
  deletions: number;
  changed_paths: string[];
  changed_paths_truncated: boolean;
}

export interface Approval {
  id: string;
  run_id: string;
  agent_id?: string;
  task_id?: string;
  thread_id: string;
  turn_id?: string;
  approval_type: string;
  risk_level: "low" | "medium" | "high" | "critical";
  request: unknown;
  state: string;
  decision?: string;
  created_at: string;
  resolved_at?: string;
  version: number;
}

export interface ActivityItem {
  id: string;
  sequence: number;
  kind: string;
  state: string;
  summary?: string;
  payload: unknown;
  occurred_at: string;
}

export interface LatestAgentMessage {
  id: string;
  text: string;
  phase?: string;
  occurred_at: string;
}

export interface ActivityPage {
  items: ActivityItem[];
  latest_message?: LatestAgentMessage | null;
  messages?: LatestAgentMessage[];
  next_cursor?: number;
}

export interface CostEstimate {
  lower_microusd: number;
  upper_microusd: number;
  confidence: "exact" | "bounded" | "unknown";
  pricing_snapshot_ids: string[];
  explanation: string;
}

export interface Usage {
  input_tokens: number;
  cached_input_tokens: number;
  cache_write_input_tokens?: number;
  output_tokens: number;
  reasoning_output_tokens: number;
  total_tokens: number;
  cost: CostEstimate;
  by_model: Array<{
    model: string;
    turns: number;
    usage: Omit<Usage, "cost" | "by_model">;
    cost: CostEstimate;
  }>;
}

export interface UsageGroup {
  id: string;
  label: string;
  detail: string;
  turns: number;
  usage: Omit<Usage, "cost" | "by_model">;
  cost: CostEstimate;
}

export interface UsageBreakdown {
  total: Usage;
  by_account: UsageGroup[];
  by_repository: UsageGroup[];
  by_agent: UsageGroup[];
}

export interface RunPlan {
  schema: string;
  summary: string;
  tasks: Array<{
    task_id: string;
    title: string;
    objective: string;
    depends_on: string[];
    success_criteria: string[];
    milestones?: TaskMilestone[];
  }>;
}

export interface TaskMilestone {
  id: string;
  title: string;
  objective: string;
  success_criteria: string[];
}

export interface PlanReviewFinding {
  severity: "blocking" | "advisory";
  file?: string | null;
  line?: number | null;
  description: string;
  required_correction: string;
}

export interface PlanReviewEvidence {
  inspected_files: string[];
  critical_path: Array<{
    task_id: string;
    why_critical: string;
    behavioral_proof: string;
  }>;
  failure_modes: Array<{
    failure_mode: string;
    mitigation: string;
  }>;
}

export interface PlanCertificate {
  schema: string;
  run_id: string;
  revision: number;
  plan_digest: string;
  base_sha: string;
  profile_digest: string;
  authority_digest: string;
  intent_brief_digest?: string;
  reviewer_agent_id: string;
  reviewer: {
    architect_model: string;
    reviewer_model: string;
    reviewer_reasoning_effort: string;
    same_model_family: boolean;
  };
  summary: string;
  evidence: PlanReviewEvidence;
  advisory_findings: PlanReviewFinding[];
  budget: {
    planning_tokens_used: number;
    run_token_ceiling: number;
    remaining_run_tokens: number;
    planned_task_tokens: number;
    verifier_reserve_tokens: number;
    final_audit_reserve_tokens: number;
    contingency_tokens: number;
    required_execution_tokens: number;
    feasible: boolean;
  };
  risk: {
    high_risk_tasks: string[];
    serial_tasks: string[];
    automatic_approval_token_threshold: number;
  };
  automatic_approval_eligible: boolean;
  automatic_approval_blockers: string[];
  certified_at: string;
}

export interface PlanReviewRecord {
  revision: number;
  plan_digest: string;
  source: "agent" | "human";
  reviewer_agent_id?: string | null;
  verdict: "accept" | "changes_requested";
  summary: string;
  findings: PlanReviewFinding[];
  evidence?: PlanReviewEvidence | null;
  blocking_fingerprint?: string | null;
  blocking_count: number;
  recorded_at: string;
}

export interface SignoffPacket {
  schema: string;
  packet_digest: string;
  run_id: string;
  objective: string;
  intent_brief?: IntentBrief | null;
  intent_brief_digest?: string | null;
  plan_digest: string;
  plan_revision: number;
  plan_review_history: PlanReviewRecord[];
  integration_sha: string;
  profile_digest: string;
  authority_digest: string;
  task_reviews: Array<{
    task: Task;
    verifier_verdict?: Record<string, unknown> | null;
  }>;
  integration_validation: {
    source_sha?: string;
    behavioral_required?: boolean;
    changed_paths?: string[];
    results?: Array<{
      validator_id: string;
      validation_id: string;
      proof_tier: string;
      evidence_class: "custody" | "contract" | "behavioral";
      result_class: string;
      exit_code?: number | null;
      timed_out: boolean;
    }>;
  };
  acceptance: Array<{
    id: string;
    kind: "automated" | "attested";
    required: boolean;
    status: "not_selected" | "passed" | "failed" | "pending_attestation" | "attested";
    instructions: string;
    proof_tier: string;
    result?: Record<string, unknown> | null;
    attestation?: Record<string, unknown> | null;
  }>;
  exact_head_evidence: Array<Record<string, unknown>>;
  unproved_claims: string[];
  total_tokens_used: number;
  final_audit?: Record<string, unknown> | null;
  human_decision?: Record<string, unknown> | null;
}

export interface GovernorMilestone {
  id: string;
  title: string;
  status: "pending" | "in_progress" | "completed" | "blocked";
  outcome: string;
  acceptance: string[];
}

export interface GovernorCheckpoint {
  schema: "harness.governor-checkpoint.v1";
  revision: number;
  status: "progressing" | "blocked" | "complete";
  operator_update: string;
  milestones: GovernorMilestone[];
  current_milestone_id?: string | null;
  next_action?: string | null;
  blocked_on?: string | null;
  durable_artifacts: Array<{
    kind: string;
    locator: string;
    summary: string;
    base_sha?: string | null;
    digest?: string | null;
  }>;
  workspace_state: string;
}

export interface RunDetail {
  run: Run;
  intent_interview?: IntentInterviewSnapshot | null;
  tasks: Task[];
  agents: Agent[];
  worktrees: Worktree[];
  approvals: Approval[];
  plan?: RunPlan;
  plan_digest?: string;
  plan_certificate?: PlanCertificate;
  plan_review_history?: PlanReviewRecord[];
  planning_tokens_used?: number;
  signoff_packet?: SignoffPacket;
  draft_pr_ci?: {
    status?: string;
    checked_at?: number;
    head_sha?: string;
    checks?: Array<Record<string, unknown>>;
    error?: string;
  };
  automatic_plan_approval: boolean;
  preferred_codex_account_id?: string;
  governor_progress?: Record<string, GovernorCheckpoint>;
}

export interface EvidenceSnapshot {
  schema: string;
  run: Run;
  tasks: Task[];
  agents: Agent[];
  evidence: Array<Record<string, unknown>>;
  artifacts: Array<Record<string, unknown>>;
}
