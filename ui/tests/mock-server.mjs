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
  version: 5,
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

const apiResponse = (pathname) => {
  if (pathname === "/api/v1/runtime") {
    return {
      daemon: { state: "ready", detail: "Harness Console 0.1.0" },
      codex: {
        state: "ready",
        detail: "Version and schema matched",
        version: "codex-cli 0.147.0",
        required_version: "codex-cli 0.147.0",
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
      items: [preparedRun, run, ...(interviewRun ? [interviewRun] : [])],
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
  if (url.pathname.startsWith("/api/v1/")) {
    const mutation =
      request.method === "POST" ? mutationResponse(url.pathname) : undefined;
    const value = mutation === undefined ? apiResponse(url.pathname) : mutation;
    return value === undefined
      ? json(response, { error: { message: "mock route not found" } }, 404)
      : json(response, value);
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
