import type {
  ActivityItem,
  Agent,
  Approval,
  EvidenceSnapshot,
  Repository,
  Run,
  RunDetail,
  RuntimeStatus,
  Task,
  Usage,
  Worktree,
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

  async request<T>(path: string, init: RequestInit = {}, mutation = false): Promise<T> {
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
    return this.request<T>(path, { method: "POST", body: JSON.stringify(body) }, true);
  }

  runtime = () => this.get<RuntimeStatus>("/runtime");
  repositories = () => this.get<Repository[]>("/repositories");
  runs = () => this.get<{ items: Run[] }>("/runs?limit=200").then((value) => value.items);
  approvals = () => this.get<Approval[]>("/approvals?state=pending");
  worktrees = () => this.get<Worktree[]>("/worktrees");
  run = (id: string) => this.get<RunDetail>(`/runs/${id}`);
  tasks = (runId: string) => this.get<Task[]>(`/runs/${runId}/tasks`);
  agent = (id: string) => this.get<Agent>(`/agents/${id}`);
  activity = (id: string) =>
    this.get<{ items: ActivityItem[] }>(`/agents/${id}/activity?limit=500`).then(
      (value) => value.items,
    );
  usage = (runId: string) => this.get<Usage>(`/runs/${runId}/usage`);
  evidence = (runId: string) => this.get<EvidenceSnapshot>(`/runs/${runId}/evidence`);

  registerRepository(rootPath: string) {
    return this.post<Repository>("/repositories", {
      profile_id: "neuralmatrix",
      root_path: rootPath,
    });
  }

  createRun(repositoryId: string, objective: string, mode: string, publication: string) {
    return this.post<Run>("/runs", {
      repository_id: repositoryId,
      objective,
      base_ref: "origin/main",
      mode,
      publication,
    });
  }
}

export const api = new HarnessApi();
