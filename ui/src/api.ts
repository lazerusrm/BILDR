import type {
  ActivityPage,
  Agent,
  AvoEpisode,
  AvoEpisodePayload,
  AvoEpisodeState,
  AttentionItem,
  AttentionPage,
  Approval,
  CodexAccountLoginStatus,
  CodexAccountsSnapshot,
  ConditionObservation,
  ControlPlaneSnapshot,
  EvidenceSnapshot,
  EvaluationCaseSummary,
  EvaluationOccurrenceSource,
  EvaluationRunSummary,
  EvaluationSampleSummary,
  ExternalCondition,
  ExternalConditionSummary,
  FailureOverview,
  FailureTrace,
  InvestigationArtifact,
  InvestigationArtifactSummary,
  InterventionReceipt,
  KnowledgeItem,
  KnowledgeReviewDecision,
  LivenessEpisode,
  MaterialProgressEvent,
  NotificationDelivery,
  NotificationDeliveryHealth,
  NotificationPresentationReceipt,
  NotificationShadowBatch,
  OutcomeVector,
  OperatorPresence,
  OperatorPresenceMode,
  OperatorSettings,
  ProviderSwitchStatus,
  Repository,
  RepositoryDiscovery,
  RunModelCatalog,
  Run,
  RunDetail,
  RuntimeStatus,
  SupervisorAction,
  TopologySnapshot,
  ReturnView,
  Task,
  Usage,
  UsageBreakdown,
  Worktree,
  WorktreeDiffSummary,
} from "./types";

type JsonBody = Record<string, unknown>;

export class ApiRequestError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
    this.name = "ApiRequestError";
  }
}

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

  private async error(response: Response): Promise<ApiRequestError> {
    const fallback = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: { message?: string } };
      return new ApiRequestError(response.status, body.error?.message || fallback);
    } catch {
      return new ApiRequestError(response.status, fallback);
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
    if (response.status === 204) return undefined as T;
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

  delete<T>(path: string): Promise<T> {
    return this.request<T>(path, { method: "DELETE" }, true);
  }

  runtime = () => this.get<RuntimeStatus>("/runtime");
  runModelCatalog = () => this.get<RunModelCatalog>("/models");
  providerSwitchStatus = () => this.get<ProviderSwitchStatus>("/provider");
  switchProvider = (provider: string) =>
    this.post<ProviderSwitchStatus>("/provider", { provider });
  codexAccounts = (force = false) =>
    this.get<CodexAccountsSnapshot>(
      `/codex/accounts${force ? "?force=true" : ""}`,
    );
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
  outcomes = (runId: string) =>
    this.get<OutcomeVector>(`/improvement/outcomes?run_id=${encodeURIComponent(runId)}`);
  avoEpisodes = () => this.get<AvoEpisode[]>("/improvement/avo-episodes?limit=50");
  recordAvoEpisode = (episode: AvoEpisodePayload, state: AvoEpisodeState) =>
    this.post<AvoEpisode>("/improvement/avo-episodes", { episode, state });
  improvementFailures = (repositoryId: string) =>
    this.get<FailureOverview>(
      `/improvement/failures?repository_id=${encodeURIComponent(repositoryId)}`,
    );
  knowledgeItems = (repositoryId: string) =>
    this.get<KnowledgeItem[]>(
      `/improvement/knowledge?repository_id=${encodeURIComponent(repositoryId)}&limit=50`,
    );
  reviewKnowledgeCandidate = (
    knowledgeId: string,
    expectedKnowledgeSha256: string,
    decision: KnowledgeReviewDecision,
  ) => this.post<KnowledgeItem>(
    `/improvement/knowledge/${encodeURIComponent(knowledgeId)}/review`,
    {
      expected_knowledge_sha256: expectedKnowledgeSha256,
      decision,
    },
  );
  improvementTrace = (traceId: string) =>
    this.get<FailureTrace>(
      `/improvement/traces/${encodeURIComponent(traceId)}`,
    );
  evaluationRun = (id: string) =>
    this.get<EvaluationRunSummary>(`/improvement/evaluations/runs/${encodeURIComponent(id)}`);
  evaluationSample = (id: string) =>
    this.get<EvaluationSampleSummary>(`/improvement/evaluations/samples/${encodeURIComponent(id)}`);
  evaluationCase = (id: string) =>
    this.get<EvaluationCaseSummary>(`/improvement/evaluations/cases/${encodeURIComponent(id)}`);
  evaluationOccurrenceSource = (id: string) =>
    this.get<EvaluationOccurrenceSource>(`/improvement/evaluations/occurrences/${encodeURIComponent(id)}`);
  worktreeDiff = (worktreeId: string) =>
    this.get<WorktreeDiffSummary>(`/worktrees/${worktreeId}/diff`);

  registerRepository(rootPath: string) {
    return this.post<Repository>("/repositories", {
      profile_id: "general",
      root_path: rootPath,
    });
  }

  createLocalProject(parentPath: string, projectName: string) {
    return this.post<Repository>("/repositories/new-local", {
      parent_path: parentPath,
      project_name: projectName,
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
        | "supervision_enabled"
        | "governor_goal_token_budget"
        | "governor_attempt_token_ceiling"
      >
    >,
  ) {
    return this.post<OperatorSettings>("/settings", { ...settings });
  }

  requestSupervisorReview(runId: string) {
    return this.post(`/runs/${encodeURIComponent(runId)}/supervision/review`);
  }

  requestPonytailAudit(runId: string) {
    return this.post(`/runs/${encodeURIComponent(runId)}/ponytail/audit`);
  }

  applySupervisorAction(actionId: string) {
    return this.post<SupervisorAction>(
      `/supervisor-actions/${encodeURIComponent(actionId)}/apply`,
    );
  }
  controlPlaneSnapshot = () =>
    this.get<ControlPlaneSnapshot>("/control-plane/snapshot");
  controlPlaneReturnView = () =>
    this.get<ReturnView>("/control-plane/return-view");
  materialProgress = (runId?: string) => {
    const params = new URLSearchParams({ limit: "50" });
    if (runId) params.set("run_id", runId);
    return this.get<MaterialProgressEvent[]>(`/material-progress?${params}`);
  };
  liveness = (runId?: string) => {
    const params = new URLSearchParams({ limit: "50" });
    if (runId) params.set("run_id", runId);
    return this.get<LivenessEpisode[]>(`/liveness?${params}`);
  };
  runLiveness = (runId: string) =>
    this.get<LivenessEpisode[]>(
      `/runs/${encodeURIComponent(runId)}/liveness?limit=50`,
    );
  interventionReceipts = (episodeId: string) =>
    this.get<InterventionReceipt[]>(
      `/liveness/${encodeURIComponent(episodeId)}/interventions?limit=50`,
    );
  pauseSchedulerForLivenessEpisode = (episodeId: string, expectedVersion: number) =>
    this.post<LivenessEpisode>(
      `/liveness/${encodeURIComponent(episodeId)}/interventions/pause-scheduler`,
      { expected_version: expectedVersion },
    );
  operatorPresence = () =>
    this.get<OperatorPresence>("/operator-presence");
  setOperatorPresence = (mode: OperatorPresenceMode, expectedVersion: number) =>
    this.post<OperatorPresence>("/operator-presence", {
      mode,
      expected_version: expectedVersion,
    });
  notificationDeliveries = () =>
    this.get<NotificationDelivery[]>("/notification-deliveries?limit=50");
  recordNotificationPresentation = (deliveryId: string, expectedDeliverySha256: string) =>
    this.post<NotificationPresentationReceipt>(
      `/notification-deliveries/${encodeURIComponent(deliveryId)}/presentations`,
      { expected_delivery_sha256: expectedDeliverySha256 },
    );
  notificationShadowBatches = () =>
    this.get<NotificationShadowBatch[]>("/notification-shadow-batches?limit=20");
  createNotificationShadowBatch = (expectedPresenceVersion: number) =>
    this.post<NotificationShadowBatch>("/notification-shadow-batches", {
      expected_presence_version: expectedPresenceVersion,
    });
  notificationDeliveryHealth = () =>
    this.get<NotificationDeliveryHealth>("/notification-delivery-health");
  topology = (runId: string) =>
    this.get<TopologySnapshot>(
      `/runs/${encodeURIComponent(runId)}/topology`,
    );
  attention = (cursor?: string, includeTerminal = false) => {
    const params = new URLSearchParams({ limit: "50" });
    if (cursor) params.set("cursor", cursor);
    if (includeTerminal) params.set("include_terminal", "true");
    return this.get<AttentionPage>(`/attention?${params}`);
  };
  acknowledgeAttention = (attentionId: string, expectedVersion: number) =>
    this.post<AttentionItem>(
      `/attention/${encodeURIComponent(attentionId)}/acknowledge`,
      { expected_version: expectedVersion },
    );
  acknowledgeReturnView = (
    expectedSnapshotRevision: number,
    acknowledgedCursor: number,
  ) =>
    this.post("/control-plane/return-view/cursor", {
      expected_snapshot_revision: expectedSnapshotRevision,
      acknowledged_cursor: acknowledgedCursor,
    });
  investigations = (runId?: string, taskId?: string) => {
    const params = new URLSearchParams({ limit: "50" });
    if (runId) params.set("run_id", runId);
    if (taskId) params.set("task_id", taskId);
    return this.get<InvestigationArtifactSummary[]>(`/investigations?${params}`);
  };
  investigation = (artifactId: string) =>
    this.get<InvestigationArtifact>(`/investigations/${encodeURIComponent(artifactId)}`);
  externalConditions = (includeTerminal = false) => {
    const params = new URLSearchParams({ limit: "50" });
    if (includeTerminal) params.set("include_terminal", "true");
    return this.get<ExternalConditionSummary[]>(`/external-conditions?${params}`);
  };
  externalCondition = (conditionId: string) =>
    this.get<ExternalCondition>(`/external-conditions/${encodeURIComponent(conditionId)}`);
  conditionObservations = (conditionId: string) =>
    this.get<ConditionObservation[]>(
      `/external-conditions/${encodeURIComponent(conditionId)}/observations?limit=50`,
    );

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
    runModel: string,
    runReasoningEffort: string,
    automaticPlanApproval: boolean,
    runTokenBudget: number,
    deepInterview: boolean,
    ponytailMode: "off" | "lite" | "full" | "ultra",
    compactHandoffs: boolean,
    codexAccountId?: string,
  ) {
    return this.post<Run>("/runs", {
      repository_id: repositoryId,
      objective,
      mode: "plan_and_implement",
      publication,
      run_model: {
        model: runModel,
        reasoning_effort: runReasoningEffort,
      },
      automatic_plan_approval: automaticPlanApproval,
      run_token_budget: runTokenBudget,
      deep_interview: deepInterview,
      ponytail_mode: ponytailMode,
      compact_handoffs: compactHandoffs,
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

  setRunPinned(runId: string, pinned: boolean) {
    return this.post<Run>(`/runs/${runId}/pin`, { pinned });
  }

  deleteRun(runId: string) {
    return this.delete<void>(`/runs/${runId}`);
  }

  recordOperatorOutcome(body: {
    run_id: string;
    subject: { kind: "run" | "task_attempt" | "publication"; id: string };
    dimension: "operator_acceptance" | "operator_correction" | "review_regression" | "pr_reopened" | "rollback" | "downstream_regression";
    classification: "positive" | "negative" | "neutral" | "unknown";
    code: string;
    reason_code?: string | null;
    note?: string | null;
    correction_artifact_id?: string | null;
    supersedes?: string[];
    idempotency_key: string;
  }) {
    return this.post("/improvement/outcomes", body);
  }
}

export const api = new HarnessApi();
