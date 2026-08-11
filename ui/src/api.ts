import type {
  ActivityPage,
  Agent,
  Approval,
  CodexAccountLoginStatus,
  CodexAccountsSnapshot,
  EvidenceSnapshot,
  OperatorSettings,
  Repository,
  RepositoryDiscovery,
  Run,
  RunDetail,
  RuntimeStatus,
  Task,
  Usage,
  UsageBreakdown,
  Worktree,
  WorktreeDiffSummary,
} from "./types";

type JsonBody = Record<string, unknown>;

class HarnessApi {
  private csrf = "";
  private session?: Promise<void>;

  ensureSession(): Promise<void> {
    if (!this.session) {
      this.session = fetch("/api/v1/session", {
        method: "POST",
        credentials: "same-origin",
        headers: { Accept: "application/json" },
      })
        .then(async (response) => {
          if (!response.ok) throw await this.error(response);
          const result = (await response.json()) as { csrf_token: string };
          this.csrf = result.csrf_token;
        })
        .catch((error) => {
          this.session = undefined;
          throw error;
        });
    }
    return this.session;
  }

  private async error(response: Response): Promise<Error> {
    const fallback = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: { message?: string } };
      return new Error(body.error?.message || fallback);
    } catch {
      return new Error(fallback);
    }
  }

  async request<T>(
    path: string,
    init: RequestInit = {},
    mutation = false,
  ): Promise<T> {
    await this.ensureSession();
    const headers = new Headers(init.headers);
    headers.set("Accept", "application/json");
    if (init.body) headers.set("Content-Type", "application/json");
    if (mutation) headers.set("X-Harness-CSRF", this.csrf);
    const response = await fetch(`/api/v1${path}`, {
      ...init,
      headers,
      credentials: "same-origin",
    });
    if (!response.ok) throw await this.error(response);
    return (await response.json()) as T;
  }

  get<T>(path: string): Promise<T> {
    return this.request<T>(path);
  }

  post<T>(path: string, body: JsonBody = {}): Promise<T> {
    return this.request<T>(
      path,
      { method: "POST", body: JSON.stringify(body) },
      true,
    );
  }

  runtime = () => this.get<RuntimeStatus>("/runtime");
  codexAccounts = () => this.get<CodexAccountsSnapshot>("/codex/accounts");
  repositories = () => this.get<Repository[]>("/repositories");
  discoverRepositories = () =>
    this.get<RepositoryDiscovery[]>("/repositories/discover");
  settings = () => this.get<OperatorSettings>("/settings");
  runs = () =>
    this.get<{ items: Run[] }>("/runs?limit=200").then((value) => value.items);
  approvals = () => this.get<Approval[]>("/approvals?state=pending");
  worktrees = () => this.get<Worktree[]>("/worktrees");
  run = (id: string) => this.get<RunDetail>(`/runs/${id}`);
  tasks = (runId: string) => this.get<Task[]>(`/runs/${runId}/tasks`);
  agent = (id: string) => this.get<Agent>(`/agents/${id}`);
  activity = (id: string) =>
    this.get<ActivityPage>(`/agents/${id}/activity?limit=500`);
  usage = (runId: string) => this.get<Usage>(`/runs/${runId}/usage`);
  usageBreakdown = () => this.get<UsageBreakdown>("/usage");
  evidence = (runId: string) =>
    this.get<EvidenceSnapshot>(`/runs/${runId}/evidence`);
  worktreeDiff = (worktreeId: string) =>
    this.get<WorktreeDiffSummary>(`/worktrees/${worktreeId}/diff`);

  registerRepository(rootPath: string) {
    return this.post<Repository>("/repositories", {
      profile_id: "general",
      root_path: rootPath,
    });
  }

  prepareCoordinationCheckout(repositoryId: string, destinationPath: string) {
    return this.post<Repository>(
      `/repositories/${repositoryId}/prepare-clean-checkout`,
      {
        destination_path: destinationPath,
      },
    );
  }

  updateSettings(
    settings: Partial<
      Pick<
        OperatorSettings,
        | "store_reasoning_summaries"
        | "store_raw_reasoning"
        | "yolo_mode"
        | "automatic_account_handoff"
        | "adaptive_governor_budgets"
        | "automatic_governor_continuation"
        | "automatic_plan_approval"
        | "governor_goal_token_budget"
        | "governor_attempt_token_ceiling"
      >
    >,
  ) {
    return this.post<OperatorSettings>("/settings", { ...settings });
  }

  selectCodexAccount(accountId: string) {
    return this.post<CodexAccountsSnapshot>(
      `/codex/accounts/${encodeURIComponent(accountId)}/select`,
    );
  }

  startCodexAccountLogin(label: string, accountId?: string) {
    return this.post<CodexAccountLoginStatus>("/codex/accounts/login", {
      label,
      account_id: accountId || null,
    });
  }

  codexAccountLoginStatus(loginId: string) {
    return this.get<CodexAccountLoginStatus>(
      `/codex/accounts/login/${encodeURIComponent(loginId)}`,
    );
  }

  cancelCodexAccountLogin(loginId: string) {
    return this.post<CodexAccountLoginStatus>(
      `/codex/accounts/login/${encodeURIComponent(loginId)}/cancel`,
    );
  }

  renameCodexAccount(accountId: string, label: string) {
    return this.post<CodexAccountsSnapshot>(
      `/codex/accounts/${encodeURIComponent(accountId)}/rename`,
      { label },
    );
  }

  removeCodexAccount(accountId: string) {
    return this.post<CodexAccountsSnapshot>(
      `/codex/accounts/${encodeURIComponent(accountId)}/remove`,
    );
  }

  createRun(
    repositoryId: string,
    objective: string,
    publication: string,
    governorModel: string,
    governorReasoningEffort: string,
    automaticPlanApproval: boolean,
    runTokenBudget: number,
    deepInterview: boolean,
    codexAccountId?: string,
  ) {
    return this.post<Run>("/runs", {
      repository_id: repositoryId,
      objective,
      mode: "plan_and_implement",
      publication,
      governor_model: governorModel,
      governor_reasoning_effort: governorReasoningEffort,
      automatic_plan_approval: automaticPlanApproval,
      run_token_budget: runTokenBudget,
      deep_interview: deepInterview,
      codex_account_id: codexAccountId || null,
    });
  }

  startArchitecture(runId: string) {
    return this.post(`/runs/${runId}/start-architecture`);
  }

  startIntentInterview(runId: string) {
    return this.post(`/runs/${runId}/interview/start`);
  }

  respondToIntentInterview(runId: string, message: string) {
    return this.post(`/runs/${runId}/interview/respond`, { message });
  }

  confirmIntentInterview(runId: string, briefDigest: string) {
    return this.post(`/runs/${runId}/interview/confirm`, {
      brief_digest: briefDigest,
    });
  }

  skipIntentInterview(runId: string) {
    return this.post(`/runs/${runId}/interview/skip`);
  }

  archiveRun(runId: string) {
    return this.post<Run>(`/runs/${runId}/archive`);
  }
}

export const api = new HarnessApi();
