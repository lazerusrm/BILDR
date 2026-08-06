import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize } from "node:path";

const root = normalize(join(import.meta.dirname, "..", "dist"));
const sha = "0123456789abcdef0123456789abcdef01234567";
const now = new Date().toISOString();
const repository = {
  id: "repo-1",
  profile_id: "neuralmatrix",
  display_name: "NeuralMatrix",
  root_path: "/srv/NeuralMatrix",
  origin_url: "git@github.com:example/NeuralMatrix.git",
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
const run = {
  id: "run-01JHARNESS",
  repository_id: repository.id,
  title: "CI credibility remediation",
  objective: "Restore exact validator semantics without weakening negative proof.",
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
const task = {
  id: "task-1",
  run_id: run.id,
  external_task_id: "MEDIA-001",
  title: "Restore validator credibility",
  objective: "Implement the bounded CI validator correction and prove the negative path.",
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
  role: "worker",
  nickname: "MEDIA-001",
  state: "RUNNING",
  requested_model: "gpt-5.6-luna",
  effective_model: "gpt-5.6-luna",
  requested_reasoning_effort: "high",
  effective_reasoning_effort: "high",
  sandbox_mode: "workspace-write",
  cwd: "/state/worktrees/run/tasks/media-001-1",
  current_goal: task.objective,
  current_action: "Running focused negative tests",
  token_budget: 80000,
  tokens_used: 27620,
  estimated_cost_lower: "$2.31",
  estimated_cost_upper: "$2.48",
  heartbeat_at: now,
  thread_id: "thread-worker",
  active_turn_id: "turn-worker",
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
    branch: "harness/run/media-001/1",
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
const detail = {
  run,
  tasks: [task],
  agents: [architect, worker],
  worktrees,
  approvals: [approval],
  plan: {
    schema: "nm.orchestration.plan.v1",
    summary: "One bounded validator task with independent verification.",
    tasks: [task],
  },
  plan_digest: "c".repeat(64),
};

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
        version: "codex-cli 0.146.0",
        required_version: "codex-cli 0.146.0",
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
  if (pathname === "/api/v1/repositories") return [repository];
  if (pathname === "/api/v1/runs") return { items: [run], next_cursor: null };
  if (pathname === "/api/v1/approvals") return [approval];
  if (pathname === "/api/v1/worktrees") return worktrees;
  if (pathname === `/api/v1/runs/${run.id}`) return detail;
  if (pathname === `/api/v1/runs/${run.id}/usage`) return usage;
  if (pathname === `/api/v1/runs/${run.id}/evidence`) {
    return { schema: "harness.evidence.snapshot.v1", run, tasks: [task], agents: [architect, worker], evidence: [], artifacts: [] };
  }
  if (pathname === `/api/v1/agents/${worker.id}/activity`) {
    return {
      items: [
        { id: "activity-1", sequence: 1, kind: "commandExecution", state: "running", summary: "Focused negative test", payload: {}, occurred_at: now },
      ],
    };
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
    return json(response, { csrf_token: "mock-csrf-token-value", expires_at_ms: Date.now() + 43_200_000 }, 201);
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
    const value = apiResponse(url.pathname);
    return value === undefined
      ? json(response, { error: { message: "mock route not found" } }, 404)
      : json(response, value);
  }

  const relative = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  const requested = normalize(join(root, relative));
  const file = requested.startsWith(root) && existsSync(requested) && statSync(requested).isFile()
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
