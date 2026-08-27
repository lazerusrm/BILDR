import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize } from "node:path";

const root = normalize(
  process.env.HARNESS_UI_DIST || join(import.meta.dirname, "..", "dist"),
);
const sha = "0123456789abcdef0123456789abcdef01234567";
const now = new Date().toISOString();
const repository = {
  id: "repo-1",
  profile_id: "bildr",
  display_name: "BILDR",
  root_path: "/srv/BILDR",
  origin_url: "git@github.com:example/BILDR.git",
  default_branch: "main",
  primary_branch: "main",
  primary_head: sha,
  primary_clean: true,
  health: "READY",
  blockers: [],
  managed_worktree_count: 2,
  authority_digest: "a".repeat(64),
  version: 1,
};
const discovery = {
  root_path: repository.root_path,
  display_name: repository.display_name,
  origin_url: repository.origin_url,
  is_github: true,
  compatible: true,
  registered: true,
};
const settings = {
  store_reasoning_summaries: true,
  store_raw_reasoning: false,
  yolo_mode: false,
  allow_automatic_external_writes: false,
  automatic_external_writes_locked: true,
  automatic_account_handoff: true,
  adaptive_governor_budgets: true,
  automatic_governor_continuation: true,
  automatic_plan_approval: false,
  governor_goal_token_budget: 5000000,
  governor_attempt_token_ceiling: 1000000,
  recommended_governor_attempt_tokens: 650000,
  governor_budget_sample_count: 3,
  governor_budget_reason: "Mock recommendation",
};
const codexAccounts = {
  selected_account_id: "codex-main",
  accounts: [
    {
      id: "codex-main",
      label: "codex-main",
      codex_home: "/home/example/.codex",
      selected: true,
      state: "ready",
      account_type: "chatgpt",
      email: "operator@example.com",
      plan_type: "pro",
      observed_at: Date.now(),
      detail: null,
      managed: false,
      rate_limits: [
        {
          limit_id: "codex",
          limit_name: null,
          plan_type: "pro",
          windows: [
            {
              kind: "primary",
              used_percent: 4,
              remaining_percent: 96,
              window_duration_mins: 10080,
              resets_at: Math.floor(Date.now() / 1000) + 86400,
            },
          ],
        },
        {
          limit_id: "codex_bengalfox",
          limit_name: "GPT-5.3-Codex-Spark",
          plan_type: "pro",
          windows: [
            {
              kind: "primary",
              used_percent: 0,
              remaining_percent: 100,
              window_duration_mins: 10080,
              resets_at: Math.floor(Date.now() / 1000) + 172800,
            },
          ],
        },
      ],
    },
  ],
};
const run = {
  id: "run-01JHARNESS",
  repository_id: repository.id,
  title: "CI credibility remediation",
  objective:
    "Restore exact validator semantics without weakening negative proof.",
  mode: "plan_and_implement",
  publication_mode: "local_only",
  state: "EXECUTING",
  phase: "executing",
  base_ref: "origin/main",
  base_sha: sha,
  authority_digest: "b".repeat(64),
  created_at: now,
  started_at: now,
  scheduler_paused: false,
  run_token_budget: 240000,
  pinned: true,
  version: 5,
};
const archivedRun = {
  id: "run-04JHARNESS",
  repository_id: "repo-1",
  title: "Retired probe latency sweep",
  objective: "Superseded by the ledger benchmark.",
  mode: "plan_and_implement",
  publication_mode: "local_only",
  state: "ARCHIVED",
  phase: "archived",
  base_ref: "origin/main",
  base_sha: sha,
  authority_digest: "c".repeat(64),
  created_at: now,
  started_at: now,
  completed_at: now,
  scheduler_paused: true,
  run_token_budget: 120000,
  pinned: false,
  version: 9,
};
const preparedRun = {
  ...run,
  id: "run-02JHARNESS",
  title: "Device shape testing and PR audit",
  objective:
    "Audit open pull requests and run device proof where the code shape requires it.",
  state: "READY_FOR_ARCHITECTURE",
  phase: "ready_for_architecture",
  started_at: null,
  created_at: new Date(Date.parse(now) + 60_000).toISOString(),
  version: 1,
};
let interviewRun;
let interviewSnapshot;
const interviewBrief = {
  refined_objective:
    "Prove the requested behavior on the authoritative user path.",
  intended_final_shape: [
    "The requested behavior is observable in the primary workflow.",
  ],
  hard_constraints: ["Preserve existing unrelated behavior."],
  preferences: [],
  non_goals: ["Unrelated repository redesign."],
  acceptance_examples: [
    "A headless user flow exercises the behavior from task creation through its result.",
  ],
  planner_may_decide: ["Implementation details not fixed by the human."],
  assumptions_to_validate: [
    "The authoritative workflow is available in the repository.",
  ],
};
const interviewDigest = "e".repeat(64);
const task = {
  id: "task-1",
  run_id: run.id,
  external_task_id: "CORE-001",
  title: "Restore validator credibility",
  objective:
    "Implement the bounded CI validator correction and prove the negative path.",
  state: "IMPLEMENTING",
  priority: "P0",
  owner_profile: "worker",
  reviewer_profile: "verifier",
  attempt: 1,
  base_sha: sha,
  token_budget: 80000,
  dependencies: [],
  version: 3,
};
const architect = {
  id: "agent-architect",
  role: "architect",
  nickname: "architect",
  state: "COMPLETED",
  requested_model: "gpt-5.6-sol",
  effective_model: "gpt-5.6-sol",
  requested_reasoning_effort: "xhigh",
  effective_reasoning_effort: "xhigh",
  sandbox_mode: "read-only",
  cwd: "/state/worktrees/run/inspection",
  current_goal: "Decompose the exact-SHA remediation run",
  current_action: "Plan approved",
  token_budget: 120000,
  tokens_used: 18400,
  estimated_cost_lower: "$1.12",
  estimated_cost_upper: "$1.12",
  heartbeat_at: now,
  thread_id: "thread-architect",
  version: 4,
};
const worker = {
  id: "agent-worker",
  task_id: task.id,
  role: "governor",
  nickname: "CORE-001",
  state: "RUNNING",
  requested_model: "gpt-5.6-luna",
  effective_model: "gpt-5.6-luna",
  requested_reasoning_effort: "high",
  effective_reasoning_effort: "high",
  sandbox_mode: "workspace-write",
  cwd: "/state/worktrees/run/tasks/core-001-1",
  current_goal: task.objective,
  current_action: "Running focused negative tests",
  token_budget: 80000,
  tokens_used: 27620,
  estimated_cost_lower: "$2.31",
  estimated_cost_upper: "$2.48",
  heartbeat_at: now,
  thread_id: "thread-worker",
  active_turn_id: "turn-worker",
  active_turn_started_at: new Date(Date.now() - 65_000).toISOString(),
  active_turn_usage: {
    input_tokens: 25000,
    cached_input_tokens: 12800,
    cache_write_input_tokens: 0,
    output_tokens: 2620,
    reasoning_output_tokens: 1700,
    total_tokens: 27620,
    model_context_window: 258400,
  },
  version: 7,
};
const worktrees = [
  {
    id: "wt-inspection",
    run_id: run.id,
    kind: "inspection",
    path: architect.cwd,
    base_sha: sha,
    head_sha: sha,
    state: "READY",
    dirty: false,
    files_changed: 0,
    additions: 0,
    deletions: 0,
    version: 1,
  },
  {
    id: "wt-task",
    run_id: run.id,
    task_id: task.id,
    kind: "task",
    path: worker.cwd,
    branch: "harness/run/core-001/1",
    base_sha: sha,
    head_sha: sha,
    state: "ACTIVE",
    dirty: true,
    files_changed: 3,
    additions: 84,
    deletions: 19,
    version: 3,
  },
];
const approval = {
  id: "approval-1",
  run_id: run.id,
  agent_id: worker.id,
  task_id: task.id,
  thread_id: worker.thread_id,
  turn_id: worker.active_turn_id,
  approval_type: "item/commandExecution/requestApproval",
  risk_level: "medium",
  request: { command: "cargo test -p validator -- negative_case" },
  state: "pending",
  created_at: now,
  version: 1,
};
const usage = {
  input_tokens: 40120,
  cached_input_tokens: 12800,
  output_tokens: 5900,
  reasoning_output_tokens: 3700,
  total_tokens: 46020,
  cost: {
    lower_microusd: 3430000,
    upper_microusd: 3600000,
    confidence: "bounded",
    pricing_snapshot_ids: ["mock-2026"],
    explanation: "Mock API-equivalent estimate",
  },
  by_model: [],
};
const usageBreakdown = {
  total: usage,
  by_account: [
    {
      id: "codex-main",
      label: "codex-main",
      detail: "Codex account",
      turns: 1,
      usage,
      cost: usage.cost,
    },
  ],
  by_repository: [
    {
      id: repository.id,
      label: repository.display_name,
      detail: "Repository",
      turns: 1,
      usage,
      cost: usage.cost,
    },
  ],
  by_agent: [
    {
      id: worker.id,
      label: worker.nickname,
      detail: "governor · gpt-5.6-sol",
      turns: 1,
      usage,
      cost: usage.cost,
    },
  ],
};
const detail = {
  run,
  tasks: [task],
  agents: [architect, worker],
  worktrees,
  approvals: [approval],
  plan: {
    schema: "harness.orchestration.plan.v1",
    summary: "One bounded validator task with independent verification.",
    tasks: [
      {
        ...task,
        task_id: task.external_task_id,
        success_criteria: ["Focused proof passes"],
      },
    ],
  },
  plan_digest: "c".repeat(64),
  automatic_plan_approval: false,
  preferred_codex_account_id: null,
};

const interviewRunDetail = () => ({
  ...detail,
  run: interviewRun,
  intent_interview: interviewSnapshot,
  tasks: [],
  agents: [],
  worktrees: [],
  approvals: [],
  plan: null,
  plan_digest: null,
});

const json = (response, value, status = 200) => {
  response.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  response.end(JSON.stringify(value));
};

const providerSwitchStatus = {
  active_provider: "openai",
  available_providers: ["openai", "qwen-local-switcher"],
  switchable: true,
  restart_required: true,
  detail: "Switch only when every run is terminal.",
};

const runModelCatalog = {
  provider: "openai-codex",
  models: [
    {
      id: "gpt-5.6-sol",
      display_name: "SOL · strongest",
      reasoning_efforts: ["low", "medium", "high", "xhigh", "max"],
      profile_sha256: "e".repeat(64),
    },
    {
      id: "gpt-5.6-terra",
      display_name: "Terra · balanced",
      reasoning_efforts: ["low", "medium", "high", "xhigh"],
      profile_sha256: "f".repeat(64),
    },
    {
      id: "gpt-5.6-luna",
      display_name: "Luna · economical",
      reasoning_efforts: ["low", "medium", "high"],
      profile_sha256: "a".repeat(64),
    },
  ],
};

const apiResponse = (pathname) => {
  if (pathname === "/api/v1/runtime") {
    return {
      daemon: { state: "ready", detail: "Harness Console 0.1.0" },
      codex: {
        state: "ready",
        detail: "Version and schema matched",
        version: "codex-cli 0.149.1",
        required_version: "codex-cli 0.149.1",
        protocol_schema_sha256: "d".repeat(64),
        schema_match: true,
        pid: 7310,
        restart_count: 0,
      },
      database: { state: "ready", detail: "SQLite WAL · projection lag 0" },
      scheduler: {
        paused: false,
        active_total: 1,
        max_total: 6,
        active_mutable: 1,
        max_mutable: 3,
        active_verifiers: 0,
        max_verifiers: 1,
        queued_tasks: 0,
      },
    };
  }
  if (
    pathname === "/api/v1/codex/accounts" ||
    pathname === "/api/v1/codex/accounts/codex-main/select"
  )
    return codexAccounts;
  if (pathname === "/api/v1/repositories") return [repository];
  if (pathname === "/api/v1/repositories/discover") return [discovery];
  if (pathname === "/api/v1/repositories/repo-1/prepare-clean-checkout") {
    return {
      ...repository,
      root_path: "/srv/BILDR-clean",
      primary_clean: true,
      health: "READY",
      blockers: [],
    };
  }
  if (pathname === "/api/v1/settings") return settings;
  if (pathname === "/api/v1/runs")
    return {
      items: [
        preparedRun,
        run,
        archivedRun,
        ...(interviewRun ? [interviewRun] : []),
      ],
      next_cursor: null,
    };
  if (pathname === "/api/v1/approvals") return [approval];
  if (pathname === "/api/v1/worktrees") return worktrees;
  if (pathname === `/api/v1/runs/${run.id}`) return detail;
  if (pathname === `/api/v1/runs/${preparedRun.id}`) {
    return {
      ...detail,
      run: preparedRun,
      tasks: [],
      agents: [],
      approvals: [],
      plan: null,
      plan_digest: null,
    };
  }
  if (interviewRun && pathname === `/api/v1/runs/${interviewRun.id}`) {
    return interviewRunDetail();
  }
  if (pathname === `/api/v1/runs/${run.id}/usage`) return usage;
  if (pathname === `/api/v1/runs/${preparedRun.id}/usage`) return usage;
  if (interviewRun && pathname === `/api/v1/runs/${interviewRun.id}/usage`)
    return usage;
  if (pathname === "/api/v1/usage") return usageBreakdown;
  if (pathname === "/api/v1/improvement/avo-episodes") return [];
  if (pathname === "/api/v1/models") return runModelCatalog;
  if (pathname === "/api/v1/provider") return providerSwitchStatus;
  if (pathname === `/api/v1/runs/${run.id}/evidence`) {
    return {
      schema: "harness.evidence.snapshot.v1",
      run,
      tasks: [task],
      agents: [architect, worker],
      evidence: [],
      artifacts: [],
    };
  }
  if (pathname === `/api/v1/agents/${worker.id}/activity`) {
    return {
      items: [
        {
          id: "activity-1",
          sequence: 1,
          kind: "commandExecution",
          state: "running",
          summary: "Focused negative test",
          payload: {},
          occurred_at: now,
        },
      ],
      latest_message: {
        id: "message-1",
        text: "I am running the focused negative proof now and keeping the delegated work bounded.",
        occurred_at: now,
        phase: "commentary",
      },
      messages: [
        {
          id: "message-1",
          text: "I am running the focused negative proof now and keeping the delegated work bounded.",
          occurred_at: now,
          phase: "commentary",
        },
      ],
    };
  }
  return undefined;
};

const mutationResponse = (pathname) => {
  const pinned = [run, preparedRun, archivedRun].find(
    (item) => pathname === `/api/v1/runs/${item.id}/pin`,
  );
  if (pinned) return { ...pinned, pinned: !pinned.pinned };
  const archiving = [run, preparedRun, archivedRun].find(
    (item) => pathname === `/api/v1/runs/${item.id}/archive`,
  );
  if (archiving) return { ...archiving, state: "ARCHIVED", phase: "archived" };
  if (pathname === "/api/v1/runs") {
    interviewRun = {
      ...preparedRun,
      id: "run-03JHARNESS",
      title: "Authoritative workflow proof",
      objective:
        "Prove the requested behavior through the authoritative workflow.",
      state: "INTERVIEWING",
      phase: "interviewing",
      created_at: new Date(Date.parse(now) + 120_000).toISOString(),
      version: 1,
    };
    interviewSnapshot = {
      schema: "harness.intent-interview.v1",
      status: "not_started",
      agent_id: null,
      turn_count: 0,
      messages: [],
      draft_brief: null,
      draft_digest: null,
      confirmed_brief: null,
      confirmed_digest: null,
      started_at: null,
      updated_at: now,
      confirmed_at: null,
      skipped_at: null,
      last_error: null,
    };
    return interviewRun;
  }
  if (
    interviewRun &&
    pathname === `/api/v1/runs/${interviewRun.id}/interview/start`
  ) {
    interviewSnapshot = {
      ...interviewSnapshot,
      status: "waiting_for_human",
      agent_id: "agent-interviewer",
      turn_count: 1,
      started_at: now,
      updated_at: now,
      messages: [
        {
          role: "interviewer",
          kind: "question",
          text: "Which observable result must the authoritative workflow prove?",
          why_it_matters:
            "The answer determines acceptance without fixing an implementation.",
          suggested_answer:
            "Exercise the primary workflow from user action through the visible result.",
          recorded_at: now,
        },
      ],
    };
    return { operation: "start_intent_interview", accepted: true };
  }
  if (
    interviewRun &&
    pathname === `/api/v1/runs/${interviewRun.id}/interview/respond`
  ) {
    interviewSnapshot = {
      ...interviewSnapshot,
      status: "ready_for_confirmation",
      turn_count: 2,
      updated_at: now,
      draft_brief: interviewBrief,
      draft_digest: interviewDigest,
      messages: [
        ...interviewSnapshot.messages,
        {
          role: "human",
          kind: "answer",
          text: "Exercise the primary workflow and verify the visible result.",
          why_it_matters: null,
          suggested_answer: null,
          recorded_at: now,
        },
        {
          role: "interviewer",
          kind: "brief_ready",
          text: "The intent brief is ready for confirmation.",
          why_it_matters: null,
          suggested_answer: null,
          recorded_at: now,
        },
      ],
    };
    return { operation: "respond_to_intent_interview", accepted: true };
  }
  if (
    interviewRun &&
    pathname === `/api/v1/runs/${interviewRun.id}/interview/confirm`
  ) {
    interviewRun = {
      ...interviewRun,
      state: "ARCHITECTING",
      phase: "architecting",
      started_at: now,
      version: interviewRun.version + 1,
    };
    interviewSnapshot = {
      ...interviewSnapshot,
      status: "confirmed",
      confirmed_brief: interviewBrief,
      confirmed_digest: interviewDigest,
      confirmed_at: now,
      updated_at: now,
    };
    return { operation: "confirm_intent_interview", accepted: true };
  }
  return undefined;
};

const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".map": "application/json",
  ".svg": "image/svg+xml",
};


// Operator control plane fixtures. Shapes follow ui/src/types.ts and the routes
// registered in crates/harness-api/src/operator_control.rs.
const ms = (minutesAgo) => Date.now() - minutesAgo * 60_000;
const digest = (seed) => seed.padEnd(64, "0").slice(0, 64);

const attentionItems = [
  {
    schema: "harness.attention-item.v1",
    attention_id: "att-001",
    repository_id: repository.id,
    run_id: run.id,
    task_id: "task-core-001",
    source: {
      source_type: "approval",
      source_id: approval.id,
      source_revision: 1,
    },
    category: "command_approval",
    severity: "high",
    state: "open",
    title: "Command approval needed",
    summary:
      "The governor wants to run cargo test -p validator -- negative_case in the managed worktree.",
    option_refs: ["approve", "deny"],
    evidence_refs: [],
    blocked_refs: ["task-core-001"],
    dedupe_key: "approval:command:core-001",
    opened_event_id: "evt-4411",
    opened_at_ms: ms(6),
    acknowledged_at_ms: null,
    due_at_ms: null,
    resurfacing: { policy: "until_resolved", maximum_defer_ms: 900_000 },
    resolution: null,
    version: 1,
  },
  {
    schema: "harness.attention-item.v1",
    attention_id: "att-002",
    repository_id: repository.id,
    run_id: preparedRun.id,
    task_id: null,
    source: {
      source_type: "external_condition",
      source_id: "cond-ci-001",
      source_revision: 3,
    },
    category: "waiting_external",
    severity: "normal",
    state: "waiting_external",
    title: "Waiting on required CI checks",
    summary:
      "The draft pull request head has two required checks still queued at the exact integration SHA.",
    option_refs: [],
    evidence_refs: ["cond-ci-001"],
    blocked_refs: [preparedRun.id],
    dedupe_key: "condition:ci_check:pr-118",
    opened_event_id: "evt-4380",
    opened_at_ms: ms(41),
    acknowledged_at_ms: ms(30),
    due_at_ms: null,
    resurfacing: { policy: "on_state_change", maximum_defer_ms: 1_800_000 },
    resolution: null,
    version: 3,
  },
];

const materialProgress = [
  {
    schema: "harness.material-progress.v1",
    event_id: "mp-003",
    run_id: run.id,
    task_id: "task-core-001",
    attempt_id: "attempt-1",
    kind: "candidate_changed",
    source_event_id: "evt-4402",
    occurred_at_ms: ms(8),
    classifier_version: "2026.08.1",
    summary: "Candidate advanced to 3 changed files (+84 / -19).",
    evidence_refs: [],
    candidate_sha: sha,
    milestone_refs: ["CORE-001"],
    sha256: digest("mp003"),
  },
  {
    schema: "harness.material-progress.v1",
    event_id: "mp-002",
    run_id: run.id,
    task_id: "task-core-001",
    attempt_id: "attempt-1",
    kind: "validation_advanced",
    source_event_id: "evt-4390",
    occurred_at_ms: ms(19),
    classifier_version: "2026.08.1",
    summary: "Focused negative proof reached the validator stage.",
    evidence_refs: ["ev-221"],
    candidate_sha: sha,
    milestone_refs: ["CORE-001"],
    sha256: digest("mp002"),
  },
  {
    schema: "harness.material-progress.v1",
    event_id: "mp-001",
    run_id: run.id,
    task_id: null,
    attempt_id: null,
    kind: "attention_changed",
    source_event_id: "evt-4370",
    occurred_at_ms: ms(52),
    classifier_version: "2026.08.1",
    summary: "Plan certified with zero blocking findings.",
    evidence_refs: ["ev-210"],
    candidate_sha: null,
    milestone_refs: [],
    sha256: digest("mp001"),
  },
];

const livenessEpisodes = [
  {
    schema: "harness.liveness-episode.v1",
    episode_id: "live-002",
    run_id: run.id,
    task_id: "task-core-001",
    attempt_id: "attempt-1",
    state: "healthy",
    version: 4,
    opened_at_ms: ms(58),
    updated_at_ms: ms(1),
    state_reason_codes: ["turn_active", "recent_material_progress"],
    last_material_progress_at_ms: ms(8),
    next_review_at_ms: ms(-4),
    intervention_count: 0,
    outcome: null,
    sha256: digest("live002"),
  },
  {
    schema: "harness.liveness-episode.v1",
    episode_id: "live-001",
    run_id: preparedRun.id,
    task_id: null,
    attempt_id: null,
    state: "waiting_external",
    version: 2,
    opened_at_ms: ms(41),
    updated_at_ms: ms(11),
    state_reason_codes: ["external_condition_open"],
    last_material_progress_at_ms: ms(41),
    next_review_at_ms: ms(-9),
    intervention_count: 1,
    outcome: null,
    sha256: digest("live001"),
  },
];

const interventionReceipts = {
  "live-001": [
    {
      schema: "harness.intervention-receipt.v1",
      intervention_id: "int-001",
      episode_id: "live-001",
      kind: "wait",
      source_event_id: "evt-4381",
      target_version: 2,
      policy_version: "2026.08.1",
      requested_by: "controller",
      created_at_ms: ms(11),
      sha256: digest("int001"),
    },
  ],
  "live-002": [],
};

const externalConditions = [
  {
    schema: "harness.external-condition-summary.v1",
    condition_id: "cond-ci-001",
    owner_type: "run",
    owner_id: preparedRun.id,
    adapter: "ci_check",
    source_id: "pr-118",
    state: "open",
    sequence: 3,
    poll_policy: { initial_ms: 30_000, maximum_ms: 300_000, deadline_ms: null },
    last_observation_state: "open",
    last_observed_at_ms: ms(2),
    version: 3,
    opened_at_ms: ms(41),
    updated_at_ms: ms(2),
  },
];

const conditionObservations = {
  "cond-ci-001": [
    {
      observation_id: "obs-003",
      condition_id: "cond-ci-001",
      state: "open",
      source_event_id: "check:event:3",
      observed_at_ms: ms(2),
      detail: "2 required checks queued",
      sha256: digest("obs003"),
    },
    {
      observation_id: "obs-002",
      condition_id: "cond-ci-001",
      state: "unknown",
      source_event_id: "check:event:2",
      observed_at_ms: ms(22),
      detail: "Checks not yet reported for the exact head",
      sha256: digest("obs002"),
    },
  ],
};

const investigations = [
  {
    schema: "harness.investigation-artifact-summary.v1",
    artifact_id: "inv-001",
    run_id: run.id,
    task_id: "task-core-001",
    attempt_id: "attempt-1",
    question: "Why did the validator accept a weakened negative case?",
    sensitivity: "internal",
    base_sha: sha,
    finding_count: 2,
    recommendation_count: 1,
    decision_count: 1,
    created_at_ms: ms(27),
    artifact_sha256: digest("inv001"),
  },
];

const operatorPresence = {
  schema: "harness.operator-presence.v1",
  operator_id: "local",
  mode: "interactive",
  version: 2,
  updated_at_ms: ms(120),
  sha256: digest("presence"),
};

const notificationDeliveries = [
  {
    schema: "harness.notification-delivery.v1",
    delivery_id: "del-001",
    attention_id: "att-001",
    class: "action_required",
    state: "pending",
    channel: "in_product_mirror",
    source_event_id: "evt-4411",
    created_at_ms: ms(6),
    payload_sha256: digest("del001p"),
    sha256: digest("del001"),
  },
];

const notificationDeliveryHealth = {
  schema: "harness.notification-delivery-health.v1",
  channel: "in_product_mirror",
  current_attention_revisions: 2,
  examined_current_revisions: 2,
  presented_examined_revisions: 1,
  unpresented_examined_revisions: 1,
  unpresented_critical_examined_revisions: 0,
  unpresented_action_required_examined_revisions: 1,
  unverified_claim_examined_revisions: 0,
  oldest_unpresented_opened_at_ms: ms(6),
  latest_presentation_receipt_at_ms: ms(30),
  truncated: false,
  desktop_delivery_enabled: false,
  batching_enabled: false,
  suppression_enabled: false,
};

const section = (rows, state = "current", cursor = 4411) => ({
  state,
  rows,
  source_cursor: cursor,
  truncated: false,
  detail: null,
});

const controlPlaneSnapshot = {
  schema: "harness.control-plane-snapshot.v1",
  snapshot_id: "snap-0007",
  revision: 7,
  compiled_at_ms: ms(1),
  event_cursor: 4411,
  consistency: "current",
  system: section([{ component: "app_server", state: "ready" }]),
  accounts: section([{ account: "codex-main", remaining_percent: 96 }]),
  scheduler: section([{ active_total: 1, max_total: 6, queued_tasks: 0 }]),
  runs: section([
    { run_id: run.id, state: run.state, title: run.title },
    { run_id: preparedRun.id, state: preparedRun.state, title: preparedRun.title },
  ]),
  attention: section(attentionItems),
  attempts: section([{ task_id: "task-core-001", attempt: 1, owner: "governor" }]),
  investigations: section(investigations),
  progress: section(materialProgress),
  liveness: section(livenessEpisodes),
  reconciliation: section([], "current"),
  external_conditions: section(externalConditions),
  cost: section([{ total_tokens: 46_000, upper_microusd: 3_600_000 }]),
  notifications: section(notificationDeliveries),
  limits: section([{ limit_id: "codex", remaining_percent: 96 }]),
  truncation: [],
  source_cursors: { attention: 4411, progress: 4402, liveness: 4400 },
  sha256: digest("snap0007"),
};

const controllerEvents = [
  {
    event_id: "evt-4411",
    occurred_at_ms: ms(6),
    event_type: "attention.opened",
    aggregate_type: "attention",
    aggregate_id: "att-001",
  },
  {
    event_id: "evt-4402",
    occurred_at_ms: ms(8),
    event_type: "progress.candidate_changed",
    aggregate_type: "task",
    aggregate_id: "task-core-001",
  },
  {
    event_id: "evt-4390",
    occurred_at_ms: ms(19),
    event_type: "progress.validation_advanced",
    aggregate_type: "task",
    aggregate_id: "task-core-001",
  },
  {
    event_id: "evt-4381",
    occurred_at_ms: ms(11),
    event_type: "liveness.intervention_recorded",
    aggregate_type: "liveness",
    aggregate_id: "live-001",
  },
];

const returnView = {
  schema: "harness.return-view.v1",
  return_view_id: "rv-0007",
  snapshot_id: controlPlaneSnapshot.snapshot_id,
  snapshot_revision: controlPlaneSnapshot.revision,
  event_cursor: 4411,
  acknowledged_cursor: 4362,
  sections: {
    material_changes: section(controllerEvents),
    attention: section(attentionItems),
    runs: controlPlaneSnapshot.runs,
    attempts: controlPlaneSnapshot.attempts,
    investigations: section(investigations),
    reconciliation: section([]),
    liveness: section(livenessEpisodes),
    external_conditions: section(externalConditions),
    accounts: controlPlaneSnapshot.accounts,
    cost: controlPlaneSnapshot.cost,
    limits: controlPlaneSnapshot.limits,
  },
  sha256: digest("rv0007"),
};

const runTopology = {
  schema: "harness.run-topology.v1",
  snapshot_id: controlPlaneSnapshot.snapshot_id,
  run_id: run.id,
  nodes: [
    { id: run.id, kind: "run", source_ref: `run:${run.id}` },
    { id: "task-core-001", kind: "task", source_ref: "task:CORE-001" },
    { id: worker.id, kind: "agent", source_ref: `agent:${worker.id}` },
  ],
  edges: [
    { from: run.id, to: "task-core-001", kind: "plans", source_ref: "plan:1" },
    { from: "task-core-001", to: worker.id, kind: "assigned", source_ref: "lease:1" },
  ],
  source_cursor: 4411,
  sha256: digest("topo"),
};

const operatorControlResponse = (pathname, search) => {
  if (pathname === "/api/v1/control-plane/snapshot") return controlPlaneSnapshot;
  if (pathname === "/api/v1/control-plane/return-view") return returnView;
  if (pathname === "/api/v1/attention")
    return {
      items: attentionItems,
      includes_terminal: search.get("include_terminal") === "true",
      next_cursor: null,
    };
  if (pathname === "/api/v1/material-progress") return materialProgress;
  if (pathname === "/api/v1/liveness") return livenessEpisodes;
  const runLiveness = pathname.match(/^\/api\/v1\/runs\/([^/]+)\/liveness$/);
  if (runLiveness)
    return livenessEpisodes.filter(
      (episode) => episode.run_id === runLiveness[1],
    );
  if (pathname === "/api/v1/investigations") return investigations;
  if (pathname === "/api/v1/external-conditions") return externalConditions;
  if (pathname === "/api/v1/operator-presence") return operatorPresence;
  if (pathname === "/api/v1/notification-deliveries") return notificationDeliveries;
  if (pathname === "/api/v1/notification-delivery-health")
    return notificationDeliveryHealth;
  if (pathname === "/api/v1/notification-shadow-batches") return [];
  if (pathname === "/api/v1/reconciliations") return [];
  const presented = notificationDeliveries.find(
    (item) =>
      pathname === `/api/v1/notification-deliveries/${item.delivery_id}/presentations`,
  );
  if (presented)
    return {
      schema: "harness.notification-presentation-receipt.v1",
      receipt_id: "rcpt-001",
      delivery_id: presented.delivery_id,
      operator_id: "local",
      delivery_sha256: presented.sha256,
      presented_at_ms: Date.now(),
      sha256: digest("rcpt001"),
    };
  if (pathname === "/api/v1/improvement/knowledge") return [];
  if (pathname === `/api/v1/runs/${run.id}/topology`) return runTopology;
  if (pathname === `/api/v1/runs/${preparedRun.id}/topology`)
    return { ...runTopology, run_id: preparedRun.id, nodes: [], edges: [] };
  const condition = externalConditions.find(
    (item) => pathname === `/api/v1/external-conditions/${item.condition_id}`,
  );
  if (condition) return condition;
  const observed = Object.keys(conditionObservations).find(
    (id) => pathname === `/api/v1/external-conditions/${id}/observations`,
  );
  if (observed) return conditionObservations[observed];
  const receipts = Object.keys(interventionReceipts).find(
    (id) => pathname === `/api/v1/liveness/${id}/interventions`,
  );
  if (receipts) return interventionReceipts[receipts];
  const artifact = investigations.find(
    (item) => pathname === `/api/v1/investigations/${item.artifact_id}`,
  );
  if (artifact)
    return {
      schema: "harness.investigation-artifact.v1",
      ...artifact,
      findings: [
        {
          finding_id: "f-1",
          severity: "blocking",
          summary: "The negative case was satisfied by a broadened matcher.",
          evidence_ref: "ev-221",
        },
      ],
      recommendations: [
        { recommendation_id: "r-1", summary: "Pin the matcher to the exact failure string." },
      ],
      decisions: [
        { decision_id: "d-1", summary: "Blocks independent work", accepted: true },
      ],
    };
  return undefined;
};

const server = createServer((request, response) => {
  const url = new URL(request.url || "/", "http://127.0.0.1:4173");
  if (url.pathname === "/api/v1/session") {
    return json(
      response,
      {
        csrf_token: "mock-csrf-token-value",
        expires_at_ms: Date.now() + 43_200_000,
      },
      201,
    );
  }
  if (url.pathname === "/api/v1/events") {
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "close",
    });
    response.end("event: heartbeat\ndata: 1\n\n");
    return;
  }
  if (request.method === "DELETE" && /^\/api\/v1\/runs\/[^/]+$/.test(url.pathname)) {
    response.writeHead(204).end();
    return;
  }
  if (url.pathname.startsWith("/api/v1/")) {
    const mutation =
      request.method === "POST" ? mutationResponse(url.pathname) : undefined;
    const value =
      mutation === undefined
        ? (apiResponse(url.pathname) ??
          operatorControlResponse(url.pathname, url.searchParams))
        : mutation;
    if (value === undefined) {
      console.error(`mock: no fixture for ${request.method} ${url.pathname}`);
      return json(response, { error: { message: "mock route not found" } }, 404);
    }
    return json(response, value);
  }

  const relative = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  const requested = normalize(join(root, relative));
  const file =
    requested.startsWith(root) &&
    existsSync(requested) &&
    statSync(requested).isFile()
      ? requested
      : join(root, "index.html");
  response.writeHead(200, {
    "content-type": contentTypes[extname(file)] || "application/octet-stream",
    "cache-control": "no-store",
  });
  createReadStream(file).pipe(response);
});

server.listen(4173, "127.0.0.1");

const stop = () => server.close(() => process.exit(0));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
