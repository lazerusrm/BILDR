export type RunState =
  | "CREATED"
  | "PREPARING"
  | "READY_FOR_ARCHITECTURE"
  | "ARCHITECTING"
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
  detail?: string;
}

export interface RuntimeStatus {
  daemon: ComponentStatus;
  codex: {
    state: string;
    detail?: string;
    version?: string;
    required_version?: string;
    protocol_schema_sha256?: string;
    schema_match: boolean;
    pid?: number;
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
  scheduler_paused: boolean;
  run_token_budget?: number;
  version: number;
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
  version: number;
}

export interface Agent {
  id: string;
  parent_agent_id?: string;
  task_id?: string;
  role: string;
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
  token_budget?: number;
  tokens_used: number;
  estimated_cost_lower: string;
  estimated_cost_upper: string;
  heartbeat_at?: string;
  thread_id?: string;
  active_turn_id?: string;
  version: number;
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

export interface RunPlan {
  schema: string;
  summary: string;
  tasks: unknown[];
}

export interface RunDetail {
  run: Run;
  tasks: Task[];
  agents: Agent[];
  worktrees: Worktree[];
  approvals: Approval[];
  plan?: RunPlan;
  plan_digest?: string;
}

export interface EvidenceSnapshot {
  schema: string;
  run: Run;
  tasks: Task[];
  agents: Agent[];
  evidence: Array<Record<string, unknown>>;
  artifacts: Array<Record<string, unknown>>;
}
