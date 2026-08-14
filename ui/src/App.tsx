import {
  Activity,
  AlertTriangle,
  Archive,
  Bot,
  Check,
  ChevronDown,
  CircleDollarSign,
  ClipboardCheck,
  Clock3,
  Database,
  FolderGit2,
  Gauge,
  GitBranch,
  GitCompareArrows,
  Home,
  Moon,
  Network,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Search,
  ServerCog,
  Settings,
  ShieldCheck,
  Square,
  Sun,
  X,
  Zap,
} from "lucide-react";
import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import { api } from "./api";
import { ImprovementCenter } from "./improvement/ImprovementCenter";
import type {
  Agent,
  Approval,
  CodexAccountProfile,
  CodexAccountLoginStatus,
  CodexAccountsSnapshot,
  CodexRateLimitWindow,
  GovernorCheckpoint,
  IntentBrief,
  IntentInterviewSnapshot,
  LatestAgentMessage,
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

type View = "home" | "repositories" | "runs" | "improvement" | "usage" | "host" | "settings";
type Modal =
  | "register"
  | "prepare-checkout"
  | "new-run"
  | "account-login"
  | "palette"
  | "messages"
  | null;

const nav: Array<{ view: View; label: string; icon: typeof Home }> = [
  { view: "home", label: "Home", icon: Home },
  { view: "repositories", label: "Repositories", icon: FolderGit2 },
  { view: "runs", label: "Runs", icon: Activity },
  { view: "improvement", label: "Improvement", icon: Network },
  { view: "usage", label: "Usage", icon: CircleDollarSign },
];
const systemNav: Array<{ view: View; label: string; icon: typeof Home }> = [
  { view: "host", label: "Host", icon: ServerCog },
  { view: "settings", label: "Settings", icon: Settings },
];

const RATE_HISTORY_KEY = "harness-rate-limit-history-v1";
const MESSAGE_SCROLL_READING_GRACE_MS = 12_000;
const RUN_BUDGET_OPTIONS = [
  500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000,
  50_000_000, 100_000_000, 250_000_000, 500_000_000, 1_000_000_000,
];
const ADDITIONAL_BUDGET_OPTIONS = [
  0, 250_000, 500_000, 1_000_000, 2_000_000, 4_000_000, 5_000_000,
  10_000_000, 20_000_000, 50_000_000,
];

export type RateLimitSample = {
  observedAt: number;
  remaining: number;
  resetsAt?: number;
};

type RateLimitHistory = Record<string, RateLimitSample[]>;

export default function App() {
  const [view, setView] = useState<View>("home");
  const [runtime, setRuntime] = useState<RuntimeStatus>();
  const [codexAccounts, setCodexAccounts] = useState<CodexAccountsSnapshot>({
    accounts: [],
  });
  const [rateHistory, setRateHistory] =
    useState<RateLimitHistory>(readRateLimitHistory);
  const [repositories, setRepositories] = useState<Repository[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [runPostures, setRunPostures] = useState<Record<string, string>>({});
  const [approvals, setApprovals] = useState<Approval[]>([]);
  const [runDetail, setRunDetail] = useState<RunDetail>();
  const [usage, setUsage] = useState<Usage>();
  const [usageBreakdown, setUsageBreakdown] = useState<UsageBreakdown>();
  const [operatorSettings, setOperatorSettings] = useState<OperatorSettings>();
  const [latestMessage, setLatestMessage] = useState<LatestAgentMessage>();
  const [messageHistory, setMessageHistory] = useState<LatestAgentMessage[]>(
    [],
  );
  const [governorLatestMessage, setGovernorLatestMessage] =
    useState<LatestAgentMessage>();
  const [worktreeDiff, setWorktreeDiff] = useState<WorktreeDiffSummary>();
  const [selectedRunId, setSelectedRunId] = useState<string>();
  const [showArchivedRuns, setShowArchivedRuns] = useState(false);
  const [selectedTaskId, setSelectedTaskId] = useState<string>();
  const [selectedAgentId, setSelectedAgentId] = useState<string>();
  const [modal, setModal] = useState<Modal>(null);
  const [prepareRepositoryId, setPrepareRepositoryId] = useState<string>();
  const [accountLoginTargetId, setAccountLoginTargetId] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");
  const [stream, setStream] = useState<
    "connecting" | "connected" | "disconnected"
  >("connecting");
  const [light, setLight] = useState(
    () => localStorage.getItem("harness-theme") === "light",
  );
  const reloadTimer = useRef<number | undefined>(undefined);
  const refreshInFlight = useRef(false);
  const requestedRunRef = useRef<string | undefined>(undefined);
  const activityAgentRef = useRef<string | undefined>(undefined);
  const governorAgentRef = useRef<string | undefined>(undefined);
  const currentDetail =
    runDetail?.run.id === selectedRunId ? runDetail : undefined;
  const selectableRuns = showArchivedRuns
    ? runs
    : runs.filter((run) => run.state !== "ARCHIVED");
  const visibleRunPostures = currentDetail
    ? {
        ...runPostures,
        [currentDetail.run.id]: effectiveRunPosture(
          currentDetail.run,
          currentDetail,
        ),
      }
    : runPostures;
  const activeRunPostureKey = runs
    .filter((run) => !terminal(run.state))
    .map((run) => run.id)
    .sort()
    .join(":");

  const loadGlobal = useCallback(async () => {
    const [
      nextRuntime,
      nextAccounts,
      nextRepositories,
      nextRuns,
      nextApprovals,
      nextUsageBreakdown,
      nextSettings,
    ] = await Promise.all([
      api.runtime(),
      api.codexAccounts().catch(() => ({ accounts: [] })),
      api.repositories(),
      api.runs(),
      api.approvals(),
      api.usageBreakdown(),
      api.settings().catch(() => undefined),
    ]);
    setRuntime(nextRuntime);
    setCodexAccounts(nextAccounts);
    setRepositories(nextRepositories);
    setRuns(nextRuns);
    setApprovals(nextApprovals);
    setUsageBreakdown(nextUsageBreakdown);
    setOperatorSettings(nextSettings);
    setSelectedRunId(
      (current) => {
        const currentRun = nextRuns.find((run) => run.id === current);
        if (currentRun && currentRun.state !== "ARCHIVED") return current;
        return (
          nextRuns.find((run) => !terminal(run.state))?.id ||
          nextRuns.find((run) => run.state !== "ARCHIVED")?.id
        );
      },
    );
  }, []);

  const loadRun = useCallback(async (runId: string) => {
    requestedRunRef.current = runId;
    const [detail, nextUsage] = await Promise.all([
      api.run(runId),
      api.usage(runId),
    ]);
    if (requestedRunRef.current !== runId) return;
    setRunDetail(detail);
    setRunPostures((current) => ({
      ...current,
      [runId]: effectiveRunPosture(detail.run, detail),
    }));
    setUsage(nextUsage);
    setSelectedTaskId((current) =>
      current && detail.tasks.some((task) => task.id === current)
        ? current
        : detail.tasks.find((task) =>
            [
              "NEEDS_HELP",
              "CHANGES_REQUESTED",
              "BLOCKED",
              "STALLED",
              "WAITING_APPROVAL",
            ].includes(task.state),
          )?.id ||
          detail.tasks.find((task) =>
            ["IMPLEMENTING", "VERIFYING"].includes(task.state),
          )?.id ||
          detail.tasks[0]?.id,
    );
    setSelectedAgentId((current) => {
      if (!current) return undefined;
      const selected = detail.agents.find((agent) => agent.id === current);
      return selected ? current : undefined;
    });
  }, []);

  const refresh = useCallback(async () => {
    if (refreshInFlight.current) return;
    refreshInFlight.current = true;
    try {
      await loadGlobal();
      if (selectedRunId) await loadRun(selectedRunId);
    } catch (caught) {
      setError(message(caught));
    } finally {
      refreshInFlight.current = false;
    }
  }, [loadGlobal, loadRun, selectedRunId]);

  useEffect(() => {
    document.documentElement.dataset.theme = light ? "light" : "dark";
    localStorage.setItem("harness-theme", light ? "light" : "dark");
  }, [light]);

  useEffect(() => {
    let alive = true;
    api
      .ensureSession()
      .then(loadGlobal)
      .catch((caught) => alive && setError(message(caught)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [loadGlobal]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      api
        .codexAccounts()
        .then(setCodexAccounts)
        .catch(() => undefined);
    }, 30_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    setRateHistory((current) => recordRateLimitHistory(current, codexAccounts));
  }, [codexAccounts]);

  useEffect(() => {
    if (!activeRunPostureKey) return;
    let alive = true;
    const loadPostures = async () => {
      const details = await Promise.all(
        activeRunPostureKey
          .split(":")
          .map((runId) => api.run(runId).catch(() => undefined)),
      );
      if (!alive) return;
      setRunPostures((current) => {
        const next = { ...current };
        for (const detail of details) {
          if (detail) {
            next[detail.run.id] = effectiveRunPosture(detail.run, detail);
          }
        }
        return next;
      });
    };
    void loadPostures();
    const timer = window.setInterval(loadPostures, 15_000);
    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, [activeRunPostureKey]);

  useEffect(() => {
    try {
      localStorage.setItem(RATE_HISTORY_KEY, JSON.stringify(rateHistory));
    } catch {
      // Usage forecasts remain available for this page session when storage is unavailable.
    }
  }, [rateHistory]);

  useEffect(() => {
    if (!selectedRunId) {
      setRunDetail(undefined);
      return;
    }
    loadRun(selectedRunId).catch((caught) => setError(message(caught)));
  }, [loadRun, selectedRunId]);

  useEffect(() => {
    const agentId =
      selectedAgentId ||
      primaryTaskAgent(currentDetail?.agents || [], selectedTaskId)?.id;
    if (!agentId) {
      activityAgentRef.current = undefined;
      setLatestMessage(undefined);
      setMessageHistory([]);
      return;
    }
    if (activityAgentRef.current !== agentId) {
      activityAgentRef.current = agentId;
      setLatestMessage(undefined);
      setMessageHistory([]);
    }
    let alive = true;
    api
      .activity(agentId)
      .then((page) => {
        if (!alive || activityAgentRef.current !== agentId) return;
        setLatestMessage(page.latest_message || undefined);
        setMessageHistory(
          page.messages || (page.latest_message ? [page.latest_message] : []),
        );
      })
      .catch((caught) => alive && setError(message(caught)));
    return () => {
      alive = false;
    };
  }, [currentDetail, selectedAgentId, selectedTaskId]);

  useEffect(() => {
    const governor = primaryTaskAgent(
      currentDetail?.agents || [],
      selectedTaskId,
    );
    if (!governor) {
      governorAgentRef.current = undefined;
      setGovernorLatestMessage(undefined);
      return;
    }
    if (governorAgentRef.current !== governor.id) {
      governorAgentRef.current = governor.id;
      setGovernorLatestMessage(undefined);
    }
    let alive = true;
    api
      .activity(governor.id)
      .then((page) => {
        if (alive && governorAgentRef.current === governor.id)
          setGovernorLatestMessage(page.latest_message || undefined);
      })
      .catch((caught) => alive && setError(message(caught)));
    return () => {
      alive = false;
    };
  }, [currentDetail, selectedTaskId]);

  useEffect(() => {
    let source: EventSource | undefined;
    api.ensureSession().then(() => {
      source = new EventSource(
        `/api/v1/events${selectedRunId ? `?run_id=${selectedRunId}` : ""}`,
      );
      source.onopen = () => setStream("connected");
      source.onerror = () => setStream("disconnected");
      source.addEventListener("domain", () => {
        window.clearTimeout(reloadTimer.current);
        reloadTimer.current = window.setTimeout(() => refresh(), 500);
      });
      source.addEventListener("heartbeat", () => {
        setStream("connected");
      });
    });
    return () => {
      source?.close();
      window.clearTimeout(reloadTimer.current);
    };
  }, [refresh, selectedRunId]);

  useEffect(() => {
    let chord = "";
    const listener = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setModal("palette");
        return;
      }
      if (event.key === "Escape") {
        setModal(null);
        return;
      }
      if (isTyping(event.target)) return;
      chord = `${chord}${event.key.toLowerCase()}`.slice(-2);
      if (chord === "gh") setView("home");
      if (chord === "gr") setView("runs");
      window.setTimeout(() => (chord = ""), 700);
    };
    window.addEventListener("keydown", listener);
    return () => window.removeEventListener("keydown", listener);
  }, []);

  const selectedTask = currentDetail?.tasks.find(
    (task) => task.id === selectedTaskId,
  );
  const selectedGovernor = primaryTaskAgent(
    currentDetail?.agents || [],
    selectedTaskId,
  );
  const selectedAgent =
    currentDetail?.agents.find((agent) => agent.id === selectedAgentId) ||
    selectedGovernor;
  const selectedControlAgent = selectedTask ? selectedGovernor : selectedAgent;
  const selectedWorktree = selectedTaskId
    ? currentDetail?.worktrees.find((tree) => tree.task_id === selectedTaskId)
    : currentDetail?.worktrees.find(
        (tree) =>
          tree.kind ===
          (selectedAgent?.role === "final_auditor"
            ? "integration"
            : "inspection"),
      );
  const currentRun =
    currentDetail?.run || runs.find((run) => run.id === selectedRunId);

  useEffect(() => {
    if (!selectedWorktree) {
      setWorktreeDiff(undefined);
      return;
    }
    let alive = true;
    const load = () =>
      api
        .worktreeDiff(selectedWorktree.id)
        .then((summary) => alive && setWorktreeDiff(summary))
        .catch(() => alive && setWorktreeDiff(undefined));
    load();
    const timer = window.setInterval(load, 10_000);
    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, [selectedWorktree?.id]);

  const runAction = async (
    label: string,
    action: () => Promise<unknown>,
    success: string,
  ) => {
    setBusy(label);
    setError("");
    try {
      await action();
      setToast(success);
      window.setTimeout(() => setToast(""), 3200);
      await refresh();
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy("");
    }
  };

  const chooseRun = (id: string) => {
    if (id === selectedRunId) {
      setView("runs");
      return;
    }
    requestedRunRef.current = id;
    setSelectedRunId(id);
    setSelectedTaskId(undefined);
    setSelectedAgentId(undefined);
    setView("runs");
  };

  if (loading) return <LoadingScreen />;

  return (
    <div className="app-frame">
      <TopBar
        repository={repositories.find(
          (repository) => repository.id === currentRun?.repository_id,
        )}
        runtime={runtime}
        usage={usage}
        approvals={approvals}
        light={light}
        onTheme={() => setLight((value) => !value)}
        onPalette={() => setModal("palette")}
      />
      <AccountBar
        snapshot={codexAccounts}
        history={rateHistory}
        busy={busy === "codex-account"}
        onSelect={(accountId) =>
          runAction(
            "codex-account",
            async () =>
              setCodexAccounts(await api.selectCodexAccount(accountId)),
            "Codex account selected",
          )
        }
        onRefresh={() =>
          runAction(
            "codex-account",
            async () => setCodexAccounts(await api.codexAccounts(true)),
            "All detected Codex account limits refreshed",
          )
        }
        onAdd={() => {
          setAccountLoginTargetId(undefined);
          setModal("account-login");
        }}
        onReauthenticate={(accountId) => {
          setAccountLoginTargetId(accountId);
          setModal("account-login");
        }}
      />
      <div
        className={`shell ${view === "runs" && currentDetail ? "with-inspector" : ""}`}
      >
        <Rail
          view={view}
          approvals={approvals.length}
          activeRuns={runs.filter((run) => !terminal(run.state)).length}
          onChange={setView}
        />
        <main className="main" id="main-content">
          {error && (
            <div className="notice error" role="alert">
              <AlertTriangle size={15} />
              <span>{error}</span>
              <button onClick={() => setError("")} aria-label="Dismiss error">
                <X size={14} />
              </button>
            </div>
          )}
          {toast && (
            <div className="toast">
              <Check size={14} />
              {toast}
            </div>
          )}
          {view === "runs" && (selectableRuns.length > 0 || runs.length > 0) && (
            <RunSwitcher
              runs={selectableRuns}
              selectedRunId={selectedRunId}
              postures={visibleRunPostures}
              archivedCount={runs.filter((run) => run.state === "ARCHIVED").length}
              showArchived={showArchivedRuns}
              onToggleArchived={() => {
                const next = !showArchivedRuns;
                setShowArchivedRuns(next);
                if (!next && currentRun?.state === "ARCHIVED") {
                  setSelectedRunId(
                    runs.find((run) => run.state !== "ARCHIVED")?.id,
                  );
                }
              }}
              onSelect={chooseRun}
              onNew={() => setModal("new-run")}
            />
          )}
          {view === "home" && (
            <HomeView
              repositories={repositories}
              runs={runs}
              postures={visibleRunPostures}
              runtime={runtime}
              onNewRun={() =>
                setModal(repositories.length ? "new-run" : "register")
              }
              onRun={chooseRun}
              onRegister={() => setModal("register")}
            />
          )}
          {view === "repositories" && (
            <RepositoriesView
              repositories={repositories}
              onRegister={() => setModal("register")}
              onInspect={(id) =>
                runAction(
                  "inspect",
                  () => api.post(`/repositories/${id}/inspect`),
                  "Repository inspected",
                )
              }
              onPrepare={(id) => {
                setPrepareRepositoryId(id);
                setModal("prepare-checkout");
              }}
            />
          )}
          {view === "runs" && !currentRun && (
            <EmptyRuns onNew={() => setModal("new-run")} />
          )}
          {view === "runs" && currentRun && !currentDetail && (
            <RunLoading run={currentRun} />
          )}
          {view === "runs" && currentRun && currentDetail && (
            <RunWorkspace
              detail={currentDetail}
              usage={usage}
              settings={operatorSettings}
              busy={busy}
              governorLatestMessage={
                governorLatestMessage ||
                (selectedAgent?.role === "governor" ? latestMessage : undefined)
              }
              selectedTaskId={selectedTaskId}
              selectedAgentId={selectedAgentId}
              onSelect={(taskId, agentId) => {
                setSelectedTaskId(taskId);
                setSelectedAgentId(agentId);
              }}
              onStart={() =>
                runAction(
                  "start",
                  () => api.post(`/runs/${currentRun.id}/start-architecture`),
                  "Architect started",
                )
              }
              onStartInterview={() =>
                runAction(
                  "interview-start",
                  () => api.startIntentInterview(currentRun.id),
                  "Intent interview started",
                )
              }
              onInterviewRespond={(response) =>
                runAction(
                  "interview-respond",
                  () => api.respondToIntentInterview(currentRun.id, response),
                  "Response sent",
                )
              }
              onInterviewConfirm={(digest) =>
                runAction(
                  "interview-confirm",
                  () => api.confirmIntentInterview(currentRun.id, digest),
                  "Intent brief confirmed; architect started",
                )
              }
              onInterviewSkip={() =>
                runAction(
                  "interview-skip",
                  () => api.skipIntentInterview(currentRun.id),
                  "Interview skipped; architect started",
                )
              }
              onPause={(additionalTokenBudget = 0) =>
                runAction(
                  "pause",
                  () =>
                    api.post(
                      `/runs/${currentRun.id}/scheduler/${currentRun.scheduler_paused ? "resume" : "pause"}`,
                      currentRun.scheduler_paused
                        ? { additional_token_budget: additionalTokenBudget }
                        : {},
                    ),
                  currentRun.scheduler_paused
                    ? "Work resumed with a fresh bounded budget"
                    : "Scheduling paused",
                )
              }
              onApprove={(allowBudgetOverride = false) =>
                runAction(
                  "approve",
                  () =>
                    api.post(`/runs/${currentRun.id}/plan/approve`, {
                      task_graph_digest: currentDetail.plan_digest || "",
                      allow_budget_override: allowBudgetOverride,
                      note: allowBudgetOverride
                        ? "Explicit operator override of controller budget feasibility"
                        : undefined,
                    }),
                  allowBudgetOverride
                    ? "Task graph approved with explicit budget override"
                    : "Task graph approved",
                )
              }
              onRequestPlanChanges={(finding) =>
                runAction(
                  "plan-changes",
                  () =>
                    api.post(
                      `/runs/${currentRun.id}/plan/request_changes`,
                      {
                        task_graph_digest: currentDetail.plan_digest || "",
                        summary: "Operator requested a corrected plan",
                        findings: [
                          {
                            severity: "blocking",
                            file: null,
                            line: null,
                            description: finding,
                            required_correction: finding,
                          },
                        ],
                      },
                    ),
                  "Plan feedback accepted; revision started",
                )
              }
              onResumePlanReview={() =>
                runAction(
                  "resume-plan-review",
                  () =>
                    api.post(
                      `/runs/${currentRun.id}/plan/resume-review`,
                    ),
                  "Independent final plan review started",
                )
              }
              onRequestSupervisorReview={() =>
                runAction(
                  "supervisor-review",
                  () => api.requestSupervisorReview(currentRun.id),
                  "Terra started a read-only blocker analysis; recovery still requires your approval",
                )
              }
              onApplySupervisorAction={(actionId) =>
                runAction(
                  `apply-supervisor-action-${actionId}`,
                  () => api.applySupervisorAction(actionId),
                  "Supervisor proposal revalidated through the controller",
                )
              }
              onStop={() =>
                runAction(
                  "stop",
                  () =>
                    api.post(`/runs/${currentRun.id}/stop`, {
                      mode: "interrupt_turns",
                      preserve_all_worktrees: true,
                    }),
                  "Run stopped; worktrees preserved",
                )
              }
              onArchive={() =>
                runAction(
                  "archive",
                  () => api.archiveRun(currentRun.id),
                  "Run archived; durable history and worktrees preserved",
                )
              }
              onApproveIntegration={() =>
                runAction(
                  "integration",
                  () =>
                    api.post(`/runs/${currentRun.id}/approve-integration`, {
                      expected_head_sha: currentRun.integration_sha || "",
                      note: "Reviewed and approved in BILDR",
                    }),
                  "Integration approved and validated",
                )
              }
              onApproveSignoff={() =>
                runAction(
                  "signoff-approve",
                  () =>
                    api.post(`/runs/${currentRun.id}/signoff/approve`, {
                      expected_head_sha: currentRun.integration_sha || "",
                      expected_packet_digest:
                        currentDetail.signoff_packet?.packet_digest || "",
                      note: "Reviewed controller signoff evidence in BILDR",
                    }),
                  currentRun.publication_mode === "local_only"
                    ? "Signoff approved; local run completed"
                    : "Signoff approved; publication is ready",
                )
              }
              onRequestSignoffChanges={(file, finding) =>
                runAction(
                  "signoff-changes",
                  () =>
                    api.post(
                      `/runs/${currentRun.id}/signoff/request_changes`,
                      {
                        expected_head_sha: currentRun.integration_sha || "",
                        expected_packet_digest:
                          currentDetail.signoff_packet?.packet_digest || "",
                        summary: "Operator rejected the integrated candidate",
                        findings: [
                          {
                            severity: "blocking",
                            file,
                            line: null,
                            description: finding,
                            required_correction: finding,
                          },
                        ],
                      },
                    ),
                  "Signoff feedback mapped to a fresh repair attempt",
                )
              }
              onAttestAcceptance={(acceptanceId, targetIdentity, observations) =>
                runAction(
                  `acceptance-${acceptanceId}`,
                  () =>
                    api.post(
                      `/runs/${currentRun.id}/signoff/acceptance/${encodeURIComponent(acceptanceId)}/attest`,
                      {
                        expected_head_sha: currentRun.integration_sha || "",
                        expected_packet_digest:
                          currentDetail.signoff_packet?.packet_digest || "",
                        target_identity: targetIdentity,
                        observations,
                      },
                    ),
                  `Acceptance ${acceptanceId} attested against the integrated head`,
                )
              }
              onPublish={() =>
                runAction(
                  "publish",
                  () =>
                    api.post(`/runs/${currentRun.id}/publish-draft-pr`, {
                      expected_head_sha: currentRun.integration_sha || "",
                      title: currentRun.title,
                      body_appendix:
                        "Created only after explicit approval in BILDR.",
                    }),
                  "Draft pull request created",
                )
              }
              onRefreshCi={() =>
                runAction(
                  "refresh-ci",
                  () => api.post(`/runs/${currentRun.id}/draft-pr/refresh-ci`, {}),
                  "Required draft-PR checks refreshed",
                )
              }
              onRetry={(taskId, guidance, additionalTokenBudget) =>
                runAction(
                  "retry",
                  () =>
                    api.post(`/tasks/${taskId}/retry`, {
                      reason:
                        guidance,
                      revised_objective: undefined,
                      model_route: "same",
                      additional_token_budget: additionalTokenBudget,
                    }),
                  "Task continued in a new attempt",
                )
              }
            />
          )}
          {view === "usage" && (
            <UsageView breakdown={usageBreakdown} accounts={codexAccounts} />
          )}
          {view === "improvement" && (
            <ImprovementCenter
              repositoryId={currentRun?.repository_id || repositories[0]?.id}
              runtime={runtime}
            />
          )}
          {view === "host" && (
            <HostView runtime={runtime} repositories={repositories} />
          )}
          {view === "settings" && (
            <SettingsView
              light={light}
              accounts={codexAccounts}
              onAccounts={setCodexAccounts}
              onSettings={setOperatorSettings}
              onRefresh={refresh}
              onAddAccount={() => {
                setAccountLoginTargetId(undefined);
                setModal("account-login");
              }}
              onReauthenticate={(accountId) => {
                setAccountLoginTargetId(accountId);
                setModal("account-login");
              }}
              onTheme={() => setLight((value) => !value)}
            />
          )}
        </main>
        {view === "runs" && currentRun && currentDetail && (
          <Inspector
            task={selectedTask}
            agent={selectedAgent}
            governor={selectedGovernor}
            worktree={selectedWorktree}
            worktreeDiff={worktreeDiff}
            detail={currentDetail}
            latestMessage={latestMessage}
            messages={messageHistory}
            governorLatestMessage={governorLatestMessage}
            busy={busy}
            onOpenMessages={() => setModal("messages")}
            onSelectGovernor={() =>
              selectedTask &&
              selectedGovernor &&
              setSelectedAgentId(selectedGovernor.id)
            }
            onSteer={(text) =>
              selectedControlAgent &&
              runAction(
                "steer",
                () =>
                  api.post(`/agents/${selectedControlAgent.id}/steer`, {
                    message: text,
                    update_goal: false,
                  }),
                selectedTask
                  ? "Governor steering delivered"
                  : "Steering delivered",
              )
            }
            onInterrupt={() =>
              selectedControlAgent &&
              runAction(
                "interrupt",
                () => api.post(`/agents/${selectedControlAgent.id}/interrupt`),
                selectedTask
                  ? "Governor interrupt requested"
                  : "Interrupt requested",
              )
            }
            onRequestReview={() =>
              selectedTask &&
              runAction(
                "request-review",
                () => api.post(`/tasks/${selectedTask.id}/request-review`),
                "Independent verifier queued",
              )
            }
            onApprovalDecision={(approval, decision) =>
              runAction(
                `approval-${approval.id}`,
                () =>
                  api.post(`/approvals/${approval.id}/decision`, {
                    decision,
                    expected_version: approval.version,
                  }),
                decision === "accept" ? "Approval delivered" : "Request denied",
              )
            }
          />
        )}
      </div>
      <StatusBar
        stream={stream}
        runtime={runtime}
        repository={repositories.find(
          (item) => item.id === currentRun?.repository_id,
        )}
      />
      {modal === "register" && (
        <RegisterModal
          onClose={() => setModal(null)}
          onDone={async () => {
            setModal(null);
            await loadGlobal();
          }}
        />
      )}
      {modal === "prepare-checkout" && prepareRepositoryId && (
        <PrepareCheckoutModal
          repository={repositories.find(
            (item) => item.id === prepareRepositoryId,
          )}
          onClose={() => setModal(null)}
          onDone={async () => {
            setModal(null);
            setToast("Clean coordination checkout is ready");
            window.setTimeout(() => setToast(""), 3200);
            await loadGlobal();
          }}
        />
      )}
      {modal === "new-run" && (
        <NewRunModal
          repositories={repositories}
          accounts={codexAccounts}
          settings={operatorSettings}
          onClose={() => setModal(null)}
          onDone={async (run, startError) => {
            setModal(null);
            await loadGlobal();
            chooseRun(run.id);
            if (startError) {
              setError(`Task was created, but planning could not start: ${startError}`);
            }
          }}
        />
      )}
      {modal === "account-login" && (
        <AccountLoginModal
          account={codexAccounts.accounts.find(
            (item) => item.id === accountLoginTargetId,
          )}
          onClose={() => setModal(null)}
          onDone={async () => {
            setModal(null);
            await loadGlobal();
          }}
        />
      )}
      {modal === "palette" && (
        <CommandPalette
          onClose={() => setModal(null)}
          onNavigate={(next) => {
            setView(next);
            setModal(null);
          }}
          onNewRun={() => setModal("new-run")}
          onRegister={() => setModal("register")}
        />
      )}
      {modal === "messages" && (
        <MessageHistoryModal
          title={
            selectedAgent?.role === "governor"
              ? "Governor messages"
              : "Thread messages"
          }
          messages={messageHistory}
          onClose={() => setModal(null)}
        />
      )}
    </div>
  );
}

function TopBar({
  repository,
  runtime,
  usage,
  approvals,
  light,
  onTheme,
  onPalette,
}: {
  repository?: Repository;
  runtime?: RuntimeStatus;
  usage?: Usage;
  approvals: Approval[];
  light: boolean;
  onTheme: () => void;
  onPalette: () => void;
}) {
  return (
    <header className="topbar">
      <a href="#main-content" className="skip-link">
        Skip to content
      </a>
      <div className="brand-mark">
        <Zap size={15} fill="currentColor" />
        <span>BILDR</span>
      </div>
      <button className="crumb-button" title="Current repository">
        {repository?.display_name || "No repository"}
        <ChevronDown size={13} />
      </button>
      <div className="top-spacer" />
      <div
        className={`top-pill ${runtime?.codex.state === "ready" ? "healthy" : "unhealthy"}`}
        title={runtime?.codex.detail ?? undefined}
      >
        <i className="status-dot" />
        <span>App Server {runtime?.codex.version || "offline"}</span>
        <span className="wide-only">
          ·{" "}
          {runtime?.codex.schema_match
            ? "schema matched"
            : "execution disabled"}
        </span>
      </div>
      <div className="top-pill wide-only">
        Slots {runtime?.scheduler.active_total || 0} /{" "}
        {runtime?.scheduler.max_total || 0}
      </div>
      <div className="top-pill wide-only">
        {formatTokens(usage?.total_tokens || 0)} ·{" "}
        {formatCost(usage?.cost.upper_microusd || 0)}
      </div>
      <button
        className={`top-pill interactive ${approvals.length ? "attention" : ""}`}
        onClick={onPalette}
        title="Open command palette"
      >
        <Search size={13} />{" "}
        <span>
          {approvals.length ? `${approvals.length} approvals` : "⌘ K"}
        </span>
      </button>
      <button
        className="icon-button"
        onClick={onTheme}
        title={light ? "Use dark theme" : "Use light theme"}
        aria-label={light ? "Use dark theme" : "Use light theme"}
      >
        {light ? <Moon size={15} /> : <Sun size={15} />}
      </button>
    </header>
  );
}

type AccountMeter = {
  key: string;
  label: string;
  window?: CodexRateLimitWindow;
  historyKey?: string;
};

function AccountBar({
  snapshot,
  history,
  busy,
  onSelect,
  onRefresh,
  onAdd,
  onReauthenticate,
}: {
  snapshot: CodexAccountsSnapshot;
  history: RateLimitHistory;
  busy: boolean;
  onSelect: (accountId: string) => void;
  onRefresh: () => void;
  onAdd: () => void;
  onReauthenticate: (accountId: string) => void;
}) {
  const account =
    snapshot.accounts.find(
      (item) => item.id === snapshot.selected_account_id,
    ) || snapshot.accounts.find((item) => item.selected);
  const meters = accountMeters(account);
  const live = Boolean(
    account?.state === "ready" &&
    account.observed_at &&
    Date.now() - account.observed_at < 90_000,
  );
  return (
    <section
      className="account-bar"
      aria-label="Codex account and usage limits"
    >
      <div className="account-identity">
        <label htmlFor="codex-account-select">Codex account</label>
        <div>
          <select
            id="codex-account-select"
            value={snapshot.selected_account_id || ""}
            disabled={busy}
            onChange={(event) =>
              event.target.value === "__add__"
                ? onAdd()
                : onSelect(event.target.value)
            }
            title={account?.codex_home}
          >
            {!snapshot.accounts.length && (
              <option value="">No account detected</option>
            )}
            {snapshot.accounts.map((item) => (
              <option value={item.id} key={item.id}>
                {accountOptionLabel(item)}
              </option>
            ))}
            <option value="__add__">＋ Add Codex account…</option>
          </select>
          <ChevronDown size={12} />
        </div>
        <span>
          {account
            ? `${account.label} · ${account.plan_type || account.account_type || account.state}`
            : "Codex App Server unavailable"}
        </span>
      </div>
      <div className="account-meters">
        {meters.map((meter) => (
          <LimitMeter
            meter={meter}
            samples={meter.historyKey ? history[meter.historyKey] || [] : []}
            key={meter.key}
          />
        ))}
      </div>
      <div className={`account-live ${live ? "live" : "stale"}`}>
        {account?.managed &&
          ["signed_out", "unavailable"].includes(account.state) && (
            <button
              className="button subtle account-reauth"
              onClick={() => onReauthenticate(account.id)}
            >
              Re-authenticate
            </button>
          )}
        <button
          className="account-refresh"
          onClick={onRefresh}
          disabled={busy}
          title="Refresh limits for every detected Codex account"
          aria-label="Refresh limits for every detected Codex account"
        >
          <RefreshCw size={12} className={busy ? "spin" : ""} />
        </button>
        <strong>
          <i />
          {live ? "Live" : account?.observed_at ? "Stale" : "Waiting"}
        </strong>
        <span>
          {account?.observed_at
            ? `observed ${relativeObserved(account.observed_at)}`
            : account?.detail || "no telemetry"}
        </span>
      </div>
    </section>
  );
}

function LimitMeter({
  meter,
  samples,
}: {
  meter: AccountMeter;
  samples: RateLimitSample[];
}) {
  const remaining = meter.window?.remaining_percent;
  const filled =
    remaining === undefined ? 0 : Math.round((remaining / 100) * 12);
  const tone =
    remaining === undefined
      ? "none"
      : remaining <= 10
        ? "danger"
        : remaining <= 30
          ? "warning"
          : "healthy";
  const forecast = meter.window
    ? rateLimitForecast(samples, meter.window)
    : undefined;
  return (
    <div className={`limit-meter ${tone}`}>
      <div>
        <span>{meter.label}</span>
        <strong>{remaining === undefined ? "—" : `${remaining}% left`}</strong>
      </div>
      <div
        className="limit-segments"
        aria-label={
          remaining === undefined
            ? `${meter.label}: not limited`
            : `${meter.label}: ${remaining}% remaining`
        }
      >
        {Array.from({ length: 12 }, (_, index) => (
          <i className={index < filled ? "filled" : ""} key={index} />
        ))}
      </div>
      <small>
        {meter.window ? resetLabel(meter.window.resets_at) : "no limit exposed"}
      </small>
      {forecast && (
        <small className="limit-forecast" title={forecast.detail}>
          {forecast.label}
        </small>
      )}
    </div>
  );
}

function accountMeters(account?: CodexAccountProfile): AccountMeter[] {
  if (!account || account.state !== "ready" || !account.observed_at)
    return [
      { key: "session", label: "Session" },
      { key: "weekly", label: "Weekly all" },
      { key: "spark", label: "Weekly Spark" },
    ];
  const windows = account.rate_limits.flatMap((limit) =>
    limit.windows.map((window) => ({
      limit,
      window,
      spark: /spark|bengalfox/i.test(
        `${limit.limit_id} ${limit.limit_name || ""}`,
      ),
    })),
  );
  const session = windows.find(
    (item) =>
      (item.window.window_duration_mins || 0) > 0 &&
      (item.window.window_duration_mins || 0) < 1_440,
  );
  const weeklyAll = windows.find(
    (item) => !item.spark && (item.window.window_duration_mins || 0) >= 10_000,
  );
  const weeklySpark = windows.find(
    (item) => item.spark && (item.window.window_duration_mins || 0) >= 10_000,
  );
  const selected = new Set([session, weeklyAll, weeklySpark].filter(Boolean));
  const meters: AccountMeter[] = [
    {
      key: "session",
      label: session
        ? durationLabel(session.window.window_duration_mins)
        : "Session 5h",
      window: session?.window,
      historyKey:
        session &&
        rateLimitHistoryKey(account.id, session.limit.limit_id, session.window),
    },
    {
      key: "weekly-all",
      label: "Weekly all",
      window: weeklyAll?.window,
      historyKey:
        weeklyAll &&
        rateLimitHistoryKey(
          account.id,
          weeklyAll.limit.limit_id,
          weeklyAll.window,
        ),
    },
    {
      key: "weekly-spark",
      label: "Weekly Spark",
      window: weeklySpark?.window,
      historyKey:
        weeklySpark &&
        rateLimitHistoryKey(
          account.id,
          weeklySpark.limit.limit_id,
          weeklySpark.window,
        ),
    },
  ];
  for (const item of windows) {
    if (!selected.has(item)) {
      meters.push({
        key: `${item.limit.limit_id}-${item.window.kind}`,
        label:
          item.limit.limit_name ||
          durationLabel(item.window.window_duration_mins),
        window: item.window,
        historyKey: rateLimitHistoryKey(
          account.id,
          item.limit.limit_id,
          item.window,
        ),
      });
    }
  }
  return meters.slice(0, 4);
}

function accountCapacity(account: CodexAccountProfile) {
  if (account.state !== "ready" || !account.observed_at) return undefined;
  const general = account.rate_limits.filter(
    (limit) =>
      !/spark|bengalfox/i.test(`${limit.limit_id} ${limit.limit_name || ""}`),
  );
  const values = general.flatMap((limit) =>
    limit.windows.map((window) => window.remaining_percent),
  );
  return values.length ? Math.min(...values) : undefined;
}

function durationLabel(minutes?: number) {
  if (!minutes) return "Limit";
  if (minutes < 1_440) return `Session ${Math.round(minutes / 60)}h`;
  return `${Math.round(minutes / 1_440)} day`;
}

function resetLabel(timestamp?: number) {
  if (!timestamp) return "reset unavailable";
  const date = new Date(timestamp * 1_000);
  return `resets ${date.toLocaleString([], { weekday: "short", hour: "numeric", minute: "2-digit" })}`;
}

function rateLimitHistoryKey(
  accountId: string,
  limitId: string,
  window: CodexRateLimitWindow,
) {
  return `${accountId}:${limitId}:${window.kind}:${window.window_duration_mins || 0}`;
}

function readRateLimitHistory(): RateLimitHistory {
  if (typeof localStorage === "undefined") return {};
  try {
    const parsed = JSON.parse(localStorage.getItem(RATE_HISTORY_KEY) || "{}");
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as RateLimitHistory)
      : {};
  } catch {
    return {};
  }
}

export function recordRateLimitHistory(
  history: RateLimitHistory,
  snapshot: CodexAccountsSnapshot,
  now = Date.now(),
): RateLimitHistory {
  let next = history;
  const cutoff = now - 48 * 60 * 60 * 1_000;
  for (const account of snapshot.accounts) {
    if (account.state !== "ready" || !account.observed_at) continue;
    const observedAt = account.observed_at;
    for (const limit of account.rate_limits) {
      for (const window of limit.windows) {
        const key = rateLimitHistoryKey(account.id, limit.limit_id, window);
        const retained = (history[key] || []).filter(
          (sample) => sample.observedAt >= cutoff,
        );
        const last = retained.at(-1);
        if (last && observedAt <= last.observedAt) continue;
        const newCycle = Boolean(
          last &&
          (last.resetsAt !== window.resets_at ||
            window.remaining_percent > last.remaining),
        );
        const samples = newCycle ? [] : retained;
        const previous = samples.at(-1);
        if (previous && observedAt - previous.observedAt < 5 * 60 * 1_000)
          continue;
        if (next === history) next = { ...history };
        next[key] = [
          ...samples,
          {
            observedAt,
            remaining: window.remaining_percent,
            resetsAt: window.resets_at,
          },
        ].slice(-640);
      }
    }
  }
  return next;
}

export function rateLimitForecast(
  samples: RateLimitSample[],
  window: CodexRateLimitWindow,
  now = Date.now(),
) {
  const currentRemaining = window.remaining_percent;
  if (currentRemaining <= 0)
    return {
      label: "usage depleted",
      detail: "No capacity remains in this window.",
    };
  const resetAt = window.resets_at ? window.resets_at * 1_000 : undefined;
  const matching = samples
    .filter(
      (sample) =>
        sample.observedAt <= now &&
        sample.observedAt >= now - 24 * 60 * 60 * 1_000 &&
        sample.resetsAt === window.resets_at,
    )
    .sort((left, right) => left.observedAt - right.observedAt);
  const trend = (values: RateLimitSample[], minimumHours: number) => {
    const first = values[0];
    const last = values.at(-1);
    const hours =
      first && last ? (last.observedAt - first.observedAt) / 3_600_000 : 0;
    const drain = first && last ? first.remaining - last.remaining : 0;
    return hours >= minimumHours && drain >= 1
      ? { rate: drain / hours, hours }
      : undefined;
  };
  const longTrend = trend(matching, 2);
  const recentTrend = trend(
    matching.filter((sample) => sample.observedAt >= now - 4 * 3_600_000),
    1,
  );
  let windowAverage: { rate: number; hours: number } | undefined;
  if (resetAt && window.window_duration_mins && window.used_percent > 0) {
    const windowStart = resetAt - window.window_duration_mins * 60_000;
    const elapsedHours = (now - windowStart) / 3_600_000;
    if (elapsedHours >= 0.25 && now < resetAt) {
      windowAverage = {
        rate: window.used_percent / elapsedHours,
        hours: elapsedHours,
      };
    }
  }

  const components: Array<{ rate: number; weight: number; label: string }> = [];
  if (longTrend)
    components.push({
      rate: longTrend.rate,
      weight: 4,
      label: `${formatHours(longTrend.hours)} local trend`,
    });
  if (
    recentTrend &&
    (!longTrend || recentTrend.hours + 0.25 < longTrend.hours)
  )
    components.push({
      rate: recentTrend.rate,
      weight: 2,
      label: `${formatHours(recentTrend.hours)} recent trend`,
    });
  if (windowAverage)
    components.push({
      rate: windowAverage.rate,
      weight:
        longTrend || recentTrend
          ? longTrend?.hours && longTrend.hours >= 6
            ? 2
            : 4
          : 1,
      label: `${formatHours(windowAverage.hours)} current-window average`,
    });
  const totalWeight = components.reduce(
    (sum, component) => sum + component.weight,
    0,
  );
  const rate = totalWeight
    ? components.reduce(
        (sum, component) => sum + component.rate * component.weight,
        0,
      ) / totalWeight
    : undefined;

  if (!rate || !Number.isFinite(rate) || rate <= 0) {
    return {
      label:
        window.used_percent > 0 ? "learning burn rate" : "no drain observed",
      detail:
        "A forecast appears after enough elapsed window time or at least one hour and 1% of observed drawdown.",
    };
  }

  const exhaustionAt = now + (currentRemaining / rate) * 3_600_000;
  const outcome =
    resetAt && exhaustionAt >= resetAt
      ? "lasts past reset"
      : `empty ${new Date(exhaustionAt).toLocaleString([], { weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}`;
  return {
    label: `avg burn ${formatBurnRate(rate)}%/h · ${outcome}`,
    detail: `Smoothed from ${components.map((component) => component.label).join(" plus ")}. Longer observations carry more weight so brief bursts do not dominate. This is a pace estimate, not a provider guarantee.`,
  };
}

function formatBurnRate(rate: number) {
  if (rate < 0.01) return "<0.01";
  if (rate < 0.1) return rate.toFixed(2);
  return rate.toFixed(1);
}

function formatHours(hours: number) {
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  return `${hours.toFixed(hours < 10 ? 1 : 0)}h`;
}

function relativeObserved(timestamp: number, now = Date.now()) {
  const seconds = Math.max(0, Math.round((now - timestamp) / 1_000));
  if (seconds < 5) return "now";
  if (seconds < 60) return `${seconds}s ago`;
  return `${Math.round(seconds / 60)}m ago`;
}

export function accountOptionLabel(
  account: CodexAccountProfile,
  now = Date.now(),
) {
  const identity = `${account.email || account.label}${account.plan_type ? ` · ${account.plan_type}` : ""}`;
  const capacity = accountCapacity(account);
  const telemetry = account.observed_at
    ? `checked ${relativeObserved(account.observed_at, now)}${now - account.observed_at > 90_000 ? " (stale)" : ""}`
    : "telemetry pending";
  if (capacity !== undefined) return `${identity} · ${capacity}% left · ${telemetry}`;
  if (account.state === "ready") return `${identity} · limits unavailable · ${telemetry}`;
  return `${identity} · ${roleLabel(account.state)} · ${telemetry}`;
}

function Rail({
  view,
  approvals,
  activeRuns,
  onChange,
}: {
  view: View;
  approvals: number;
  activeRuns: number;
  onChange: (view: View) => void;
}) {
  return (
    <nav className="rail" aria-label="Primary navigation">
      <div className="rail-section">Workspace</div>
      {nav.map((item) => {
        const Icon = item.icon;
        const count = item.view === "runs" ? activeRuns + approvals : 0;
        return (
          <button
            key={item.view}
            className={`nav-item ${view === item.view ? "active" : ""}`}
            onClick={() => onChange(item.view)}
            title={item.label}
          >
            <Icon size={16} />
            <span>{item.label}</span>
            {count > 0 && <i className="nav-count">{count}</i>}
          </button>
        );
      })}
      <div className="rail-section">System</div>
      {systemNav.map((item) => {
        const Icon = item.icon;
        return (
          <button
            key={item.view}
            className={`nav-item ${view === item.view ? "active" : ""}`}
            onClick={() => onChange(item.view)}
            title={item.label}
          >
            <Icon size={16} />
            <span>{item.label}</span>
          </button>
        );
      })}
      <div className="rail-shortcuts">
        <kbd>G</kbd>
        <kbd>H</kbd>
        <span>Home</span>
        <kbd>G</kbd>
        <kbd>R</kbd>
        <span>Runs</span>
      </div>
    </nav>
  );
}

function HomeView({
  repositories,
  runs,
  postures,
  runtime,
  onNewRun,
  onRun,
  onRegister,
}: {
  repositories: Repository[];
  runs: Run[];
  postures: Record<string, string>;
  runtime?: RuntimeStatus;
  onNewRun: () => void;
  onRun: (id: string) => void;
  onRegister: () => void;
}) {
  const active = runs.filter((run) => !terminal(run.state));
  return (
    <div className="page home-page">
      <PageTitle
        eyebrow="Local orchestration"
        title="Good afternoon"
        description="Exact repository truth, active work, and runtime health in one place."
        action={
          <button className="button primary" onClick={onNewRun}>
            <Plus size={14} />
            New task
          </button>
        }
      />
      <SectionHeader title="Active" count={active.length} />
      <div className="stack">
        {active.length ? (
          active.map((run) => {
            const posture = postures[run.id] || effectiveRunPosture(run);
            return (
              <button
                className="home-row"
                key={run.id}
                onClick={() => onRun(run.id)}
              >
                <StateIcon
                  state={posture}
                  working={["WORKING", "PLANNING"].includes(posture)}
                />
                <div>
                  <strong>{run.title}</strong>
                  <span>{posture}</span>
                </div>
                <div className="home-row-meta">
                  {runLifecycleSummary(run)}
                  <span>Open ›</span>
                </div>
              </button>
            );
          })
        ) : (
          <EmptyCard
            icon={<Activity />}
            title="No active tasks"
            text="Start with an objective; Harness prepares a safe workspace before asking Codex to plan."
            action={
              <button className="button" onClick={onNewRun}>
                Create a task
              </button>
            }
          />
        )}
      </div>
      <SectionHeader title="Repositories" count={repositories.length} />
      <div className="stack">
        {repositories.length ? (
          repositories.map((repository) => (
            <div className="home-row static" key={repository.id}>
              <FolderGit2 size={17} />
              <div>
                <strong>{repository.display_name}</strong>
                <span>
                  {repository.blockers.length
                    ? repository.blockers.join(" · ")
                    : `${repository.primary_branch || repository.default_branch} · clean · ${repository.managed_worktree_count} managed workspaces`}
                </span>
              </div>
              <StatusBadge value={repository.health} />
            </div>
          ))
        ) : (
          <EmptyCard
            icon={<FolderGit2 />}
            title="Register a repository"
            text="Choose any clean local Git checkout. Harness keeps its managed work separate from your primary checkout."
            action={
              <button className="button" onClick={onRegister}>
                Register repository
              </button>
            }
          />
        )}
      </div>
      <SectionHeader title="Host" />
      <div className="health-grid">
        <HealthCard
          icon={<Bot />}
          label="Codex App Server"
          state={runtime?.codex.state || "unknown"}
          detail={runtime?.codex.detail || "Connecting"}
        />
        <HealthCard
          icon={<Database />}
          label="Local history"
          state={runtime?.database.state || "unknown"}
          detail={
            runtime?.database.state === "ready" ? "Available" : "Checking"
          }
        />
        <HealthCard
          icon={<Gauge />}
          label="Scheduler"
          state={runtime?.scheduler.paused ? "paused" : "ready"}
          detail={`${runtime?.scheduler.active_total || 0}/${runtime?.scheduler.max_total || 0} agent slots · ${runtime?.scheduler.queued_tasks || 0} queued`}
        />
      </div>
    </div>
  );
}

function RepositoriesView({
  repositories,
  onRegister,
  onInspect,
  onPrepare,
}: {
  repositories: Repository[];
  onRegister: () => void;
  onInspect: (id: string) => void;
  onPrepare: (id: string) => void;
}) {
  return (
    <div className="page">
      <PageTitle
        eyebrow="Local checkouts"
        title="Repositories"
        description="Registered coordination checkouts and anything that needs attention."
        action={
          <button className="button primary" onClick={onRegister}>
            <Plus size={14} />
            Register
          </button>
        }
      />
      <div className="table-card">
        <div className="table-head repo-grid">
          <span>Repository</span>
          <span>Branch</span>
          <span>Origin / attention</span>
          <span>Workspaces</span>
          <span>Health</span>
          <span />
        </div>
        {repositories.map((repository) => (
          <div className="table-row repo-grid" key={repository.id}>
            <div className="cell-main">
              <FolderGit2 size={16} />
              <span>
                <strong>{repository.display_name}</strong>
                <small className="mono">{repository.root_path}</small>
              </span>
            </div>
            <span>{repository.primary_branch || "—"}</span>
            <span
              className={`truncate ${repository.blockers.length ? "danger" : ""}`}
              title={repository.blockers.join("; ")}
            >
              {repository.blockers[0] || repository.origin_url || "missing"}
            </span>
            <span>{repository.managed_worktree_count}</span>
            <StatusBadge value={repository.health} />
            <div className="repo-actions">
              {repository.blockers.includes("primary checkout is dirty") && (
                <button
                  className="button primary"
                  onClick={() => onPrepare(repository.id)}
                >
                  <FolderGit2 size={13} />
                  Create clean checkout
                </button>
              )}
              <button
                className="button subtle"
                onClick={() => onInspect(repository.id)}
              >
                <RefreshCw size={13} />
                Inspect
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function RunSwitcher({
  runs,
  selectedRunId,
  postures,
  archivedCount,
  showArchived,
  onToggleArchived,
  onSelect,
  onNew,
}: {
  runs: Run[];
  selectedRunId?: string;
  postures: Record<string, string>;
  archivedCount: number;
  showArchived: boolean;
  onToggleArchived: () => void;
  onSelect: (id: string) => void;
  onNew: () => void;
}) {
  const orderedRuns = [...runs].sort(
    (left, right) =>
      Date.parse(left.created_at) - Date.parse(right.created_at),
  );
  const selected =
    orderedRuns.find((run) => run.id === selectedRunId) || orderedRuns[0];
  const selectedPosition = Math.max(
    1,
    orderedRuns.findIndex((run) => run.id === selected?.id) + 1,
  );
  const open = runs.filter((run) => !terminal(run.state)).length;
  const selectedPosture = selected
    ? postures[selected.id] || effectiveRunPosture(selected)
    : undefined;
  return (
    <section className="run-switcher" aria-label="Governor sessions">
      <div className="run-switcher-label">
        <Activity size={15} />
        <span>
          <strong>Switch run</strong>
          <small>
            {orderedRuns.length
              ? `Viewing run ${selectedPosition} of ${runs.length} · ${open} open`
              : "No unarchived runs"}
          </small>
        </span>
      </div>
      <label className="run-switch-select">
        <select
          aria-label="Governor session"
          value={selected?.id || ""}
          onChange={(event) => onSelect(event.target.value)}
          disabled={!orderedRuns.length}
        >
          {orderedRuns.map((run, index) => (
            <option value={run.id} key={run.id}>
              {index + 1}. {run.title} —{" "}
              {postures[run.id] || effectiveRunPosture(run)}
            </option>
          ))}
        </select>
        <ChevronDown size={14} aria-hidden="true" />
      </label>
      {selected && (
        <div className="run-switcher-state">
          <StateIcon
            state={selectedPosture || selected.state}
            working={["WORKING", "PLANNING"].includes(selectedPosture || "")}
          />
          <StatusBadge value={selectedPosture || selected.state} />
        </div>
      )}
      {archivedCount > 0 && (
        <button
          className="button"
          onClick={onToggleArchived}
          aria-pressed={showArchived}
        >
          <Archive size={13} />
          {showArchived ? "Hide archived" : `Show archived (${archivedCount})`}
        </button>
      )}
      <button className="button" onClick={onNew}>
        <Plus size={13} />
        New task
      </button>
    </section>
  );
}

function RunLoading({ run }: { run: Run }) {
  return (
    <div className="run-loading" role="status">
      <div className="runtime-spinner" aria-hidden="true" />
      <div>
        <strong>Opening governor session</strong>
        <span title="Run lifecycle times use this browser's local time zone">
          {run.title} · {runLifecycleSummary(run)}
        </span>
      </div>
    </div>
  );
}

function EmptyRuns({ onNew }: { onNew: () => void }) {
  return (
    <div className="page">
      <PageTitle
        eyebrow="Orchestration"
        title="Runs"
        description="No task is selected."
      />
      <EmptyCard
        icon={<Activity />}
        title="Create the first task"
        text="Harness pins origin/main, compiles active authority, and starts with read-only architecture."
        action={
          <button className="button primary" onClick={onNew}>
            <Plus size={14} />
            New task
          </button>
        }
      />
    </div>
  );
}

function RunWorkspace({
  detail,
  usage,
  settings,
  governorLatestMessage,
  busy,
  selectedTaskId,
  selectedAgentId,
  onSelect,
  onStart,
  onStartInterview,
  onInterviewRespond,
  onInterviewConfirm,
  onInterviewSkip,
  onPause,
  onApprove,
  onRequestPlanChanges,
  onResumePlanReview,
  onRequestSupervisorReview,
  onApplySupervisorAction,
  onApproveIntegration,
  onApproveSignoff,
  onRequestSignoffChanges,
  onAttestAcceptance,
  onPublish,
  onRefreshCi,
  onStop,
  onArchive,
  onRetry,
}: {
  detail: RunDetail;
  usage?: Usage;
  settings?: OperatorSettings;
  governorLatestMessage?: LatestAgentMessage;
  busy: string;
  selectedTaskId?: string;
  selectedAgentId?: string;
  onSelect: (task?: string, agent?: string) => void;
  onStart: () => void;
  onStartInterview: () => void;
  onInterviewRespond: (response: string) => void;
  onInterviewConfirm: (digest: string) => void;
  onInterviewSkip: () => void;
  onPause: (additionalTokenBudget?: number) => void;
  onApprove: (allowBudgetOverride?: boolean) => void;
  onRequestPlanChanges: (finding: string) => void;
  onResumePlanReview: () => void;
  onRequestSupervisorReview: () => void;
  onApplySupervisorAction: (actionId: string) => void;
  onApproveIntegration: () => void;
  onApproveSignoff: () => void;
  onRequestSignoffChanges: (file: string, finding: string) => void;
  onAttestAcceptance: (
    acceptanceId: string,
    targetIdentity: string,
    observations: string,
  ) => void;
  onPublish: () => void;
  onRefreshCi: () => void;
  onStop: () => void;
  onArchive: () => void;
  onRetry: (
    taskId: string,
    guidance: string,
    additionalTokenBudget: number,
  ) => void;
}) {
  const { run, tasks, agents } = detail;
  const [resumeTokenBudget, setResumeTokenBudget] = useState(0);
  const posture = effectiveRunPosture(run, detail);
  const verified = tasks.filter((task) =>
    ["VERIFIED", "INTEGRATED", "CI_PROVEN", "LIVE_PROVEN", "CLOSED"].includes(
      task.state,
    ),
  ).length;
  const activeTurns = agents.filter(
    (agent) =>
      agent.active_turn_id &&
      ["STARTING", "RUNNING", "STEERED", "WAITING_APPROVAL"].includes(
        agent.state,
      ),
  ).length;
  const starting = tasks.filter((task) =>
    ["LEASED", "STARTING"].includes(task.state),
  ).length;
  const progress = tasks.length
    ? Math.round((verified / tasks.length) * 100)
    : planningRunState(run.state)
      ? 8
      : 0;
  const architectAgents = agents.filter((agent) => !agent.task_id);
  const previousArchitectureAttempts = architectAgents.filter(
    (agent) =>
      agent.role === "architect" &&
      ["FAILED", "STALLED", "INTERRUPTED"].includes(agent.state),
  );
  const visibleArchitectureAgents = architectAgents.filter(
    (agent) => !previousArchitectureAttempts.includes(agent),
  );
  const currentArchitect =
    visibleArchitectureAgents.find(
      (agent) => agent.active_turn_id && activeAgentState(agent.state),
    ) || visibleArchitectureAgents.at(-1);
  const interviewAgent = detail.intent_interview?.agent_id
    ? agents.find((agent) => agent.id === detail.intent_interview?.agent_id)
    : undefined;
  const focusTask = tasks.find(
    (task) => task.id === selectedTaskId && retryableState(task.state),
  );
  const selectedTask = tasks.find((task) => task.id === selectedTaskId);
  const selectedTaskAgent = selectedTask
    ? primaryTaskAgent(agents, selectedTask.id)
    : undefined;
  const selectedTaskChildren = agents.filter(
    (item) => item.parent_agent_id === selectedTaskAgent?.id,
  );
  const governorResumeTask = [selectedTask, ...tasks].find((task, index, all) => {
    if (!task || all.findIndex((candidate) => candidate?.id === task.id) !== index) {
      return false;
    }
    return (
      retryableState(task.state) &&
      primaryTaskAgent(agents, task.id)?.role === "governor"
    );
  });
  const budgetExhausted = Boolean(
    run.scheduler_paused &&
      run.run_token_budget &&
      usage &&
      usage.total_tokens >= run.run_token_budget,
  );
  const resumeNeedsBudget = Boolean(
    run.scheduler_paused && (budgetExhausted || governorResumeTask),
  );
  const recommendedResumeBudget =
    settings?.recommended_governor_attempt_tokens || 650_000;
  const existingGovernorBudget = governorResumeTask?.token_budget || 0;
  const nextGovernorAllowance = resumeTokenBudget
    ? Math.min(existingGovernorBudget + resumeTokenBudget, 100_000_000)
    : Math.max(existingGovernorBudget, recommendedResumeBudget);
  const projectedRunBudget = usage
    ? Math.max(
        run.run_token_budget || 0,
        usage.total_tokens + nextGovernorAllowance + 500_000,
      )
    : run.run_token_budget;
  return (
    <div className="run-page">
      <div className="run-heading">
        <div>
          <h1>{run.title}</h1>
          <p>
            {posture} · {verified} of {tasks.length}{" "}
            tasks verified ·{" "}
            {run.integration_sha
              ? "integration ready"
              : "integration not created"}
          </p>
        </div>
        <div className="actions">
          {!terminal(run.state) && resumeNeedsBudget && (
            <label className="resume-budget-picker">
              <span>
                Next work window
                {projectedRunBudget
                  ? ` · total cap ${formatTokens(projectedRunBudget)}`
                  : ""}
              </span>
              <select
                value={resumeTokenBudget}
                onChange={(event) =>
                  setResumeTokenBudget(Number(event.target.value))
                }
                aria-label="Next work window token budget"
                title="Governor allowance; bounded child-thread headroom is reserved automatically"
              >
                {ADDITIONAL_BUDGET_OPTIONS.map((budget) => (
                  <option key={budget} value={budget}>
                    {budget === 0
                      ? `Adaptive · ${formatTokens(recommendedResumeBudget)}`
                      : `+ ${formatTokens(budget)}`}
                  </option>
                ))}
              </select>
            </label>
          )}
          {!terminal(run.state) ? (
            <>
              <button
                className="button"
                onClick={() => {
                  if (run.scheduler_paused && governorResumeTask) {
                    onRetry(governorResumeTask.id, "", resumeTokenBudget);
                  } else {
                    onPause(resumeTokenBudget);
                  }
                }}
                disabled={!!busy}
              >
                {run.scheduler_paused ? (
                  <Play size={13} />
                ) : (
                  <Pause size={13} />
                )}
                {run.scheduler_paused ? "Resume work" : "Pause scheduling"}
              </button>
              <RunPrimaryAction
                run={run}
                detail={detail}
                posture={posture}
                busy={busy}
                onStart={onStart}
                onApprove={onApprove}
                onApproveIntegration={onApproveIntegration}
                onApproveSignoff={onApproveSignoff}
                onPublish={onPublish}
                onRefreshCi={onRefreshCi}
              />
              <button
                className="icon-button danger-hover"
                onClick={onStop}
                disabled={!!busy}
                title="Stop run"
              >
                <Square size={13} />
              </button>
            </>
          ) : run.state !== "ARCHIVED" ? (
            <button className="button" onClick={onArchive} disabled={!!busy}>
              <Archive size={13} />
              Archive run
            </button>
          ) : (
            <StatusBadge value="ARCHIVED" />
          )}
        </div>
      </div>
      <div className="progress" aria-label={`${progress}% of tasks verified`}>
        <i style={{ width: `${progress}%` }} />
      </div>
      <div className="metrics">
        <Metric
          label="Active turns"
          value={String(activeTurns)}
          note={
            starting
              ? `${starting} attempt starting`
              : `${agents.length} sessions total`
          }
        />
        <Metric
          label="Verified tasks"
          value={`${verified} / ${tasks.length}`}
          note={`${tasks.filter((task) => task.state === "WAITING_DEPENDENCY").length} waiting`}
        />
        <Metric
          label="API-equivalent"
          value={formatCost(usage?.cost.upper_microusd || 0)}
          note={usage?.cost.confidence || "no usage yet"}
        />
        <Metric
          label="Elapsed"
          value={elapsed(run.started_at || run.created_at)}
          note={run.scheduler_paused ? "scheduler paused" : "active wall time"}
        />
      </div>
      <div
        className="run-lifecycle"
        title="Run lifecycle times use this browser's local time zone"
      >
        <strong>Run lifecycle</strong>
        <span>{runLifecycleSummary(run)}</span>
      </div>
      <SupervisorObservationPanel
        detail={detail}
        busy={busy}
        onRequestReview={onRequestSupervisorReview}
        onApplyAction={onApplySupervisorAction}
      />
      <BlockedRunRecoveryPanel
        detail={detail}
        busy={busy}
        onResumeReview={onResumePlanReview}
        onRequestChanges={onRequestPlanChanges}
        onResumeWork={() => onPause(0)}
        onRetry={(taskId) => onRetry(taskId, "", 0)}
      />
      {detail.intent_interview && (
        <IntentInterviewPanel
          interview={detail.intent_interview}
          interviewer={interviewAgent}
          runState={run.state}
          busy={busy}
          onStart={onStartInterview}
          onRespond={onInterviewRespond}
          onConfirm={onInterviewConfirm}
          onSkip={onInterviewSkip}
        />
      )}
      {(run.state === "READY_FOR_ARCHITECTURE" ||
        planningRunState(run.state) ||
        busy === "start") && (
        <ArchitectureStatusPanel
          run={run}
          architect={currentArchitect}
          starting={busy === "start"}
        />
      )}
      <GoalPlanPanel
        detail={detail}
        busy={busy}
        onApprove={onApprove}
        onRequestChanges={onRequestPlanChanges}
      />
      <ExecutionSignoffPanel
        detail={detail}
        busy={busy}
        onApprove={onApproveSignoff}
        onRequestChanges={onRequestSignoffChanges}
        onAttest={onAttestAcceptance}
      />
      {focusTask && (
        <NeedsHelpPanel
          key={focusTask.id}
          task={focusTask}
          governor={selectedTaskAgent}
          children={selectedTaskChildren}
          latestMessage={governorLatestMessage}
          settings={settings}
          busy={busy}
          onRetry={(guidance, additionalTokenBudget) =>
            onRetry(focusTask.id, guidance, additionalTokenBudget)
          }
        />
      )}
      {selectedTask &&
        !focusTask &&
        [
          "LEASED",
          "STARTING",
          "IMPLEMENTING",
          "VERIFYING",
          "WAITING_APPROVAL",
        ].includes(selectedTask.state) && (
          <TaskRuntimePanel
            task={selectedTask}
            agent={selectedTaskAgent}
            children={selectedTaskChildren}
          />
        )}
      {architectAgents.length > 0 && (
        <>
          <SectionHeader title="Architecture and review" />
          {visibleArchitectureAgents.map((agent) => (
            <AgentRow
              key={agent.id}
              run={run}
              agent={agent}
              selected={selectedAgentId === agent.id}
              onClick={() => onSelect(undefined, agent.id)}
            />
          ))}
          {previousArchitectureAttempts.length > 0 && (
            <details className="attempt-history">
              <summary>
                Previous architecture attempts{" "}
                <span>{previousArchitectureAttempts.length}</span>
              </summary>
              <div>
                {previousArchitectureAttempts.map((agent) => (
                  <AgentRow
                    key={agent.id}
                    run={run}
                    agent={agent}
                    selected={selectedAgentId === agent.id}
                    onClick={() => onSelect(undefined, agent.id)}
                  />
                ))}
              </div>
            </details>
          )}
        </>
      )}
      <SectionHeader
        title="Implementation tasks"
        count={tasks.length}
        aside={
          starting
            ? `${starting} starting · ${activeTurns} working`
            : `${activeTurns} working`
        }
      />
      <div className="task-stack">
        {tasks.map((task) => {
          const taskAgents = agents.filter(
            (agent) => agent.task_id === task.id,
          );
          const agent = primaryTaskAgent(taskAgents, task.id);
          return (
            <AgentRow
              key={task.id}
              run={run}
              task={task}
              agent={agent}
              selected={selectedTaskId === task.id}
              selectedAgentId={selectedAgentId}
              onClick={() => onSelect(task.id, agent?.id)}
              onSelectChild={(childId) => onSelect(task.id, childId)}
              children={agents.filter(
                (item) => item.parent_agent_id === agent?.id,
              )}
            />
          );
        })}
        {!tasks.length && (
          <div className="pending-plan">
            <Network size={20} />
            <div>
              <strong>
                {run.state === "INTERVIEWING"
                  ? "Clarifying the intended result"
                  : planningRunState(run.state)
                  ? planningStateTitle(run.state)
                  : "Waiting to start planning"}
              </strong>
              <span>
                {run.state === "INTERVIEWING"
                  ? "Answer the current question, confirm the resulting brief, or skip the interview to continue from the original request."
                  : planningRunState(run.state)
                  ? "Implementation begins only after independent plan certification and the configured approval policy."
                  : "Select Start architecture to begin repository research."}
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function IntentInterviewPanel({
  interview,
  interviewer,
  runState,
  busy,
  onStart,
  onRespond,
  onConfirm,
  onSkip,
}: {
  interview: IntentInterviewSnapshot;
  interviewer?: Agent;
  runState: string;
  busy: string;
  onStart: () => void;
  onRespond: (response: string) => void;
  onConfirm: (digest: string) => void;
  onSkip: () => void;
}) {
  const [response, setResponse] = useState("");
  useEffect(() => setResponse(""), [interview.turn_count]);
  const working =
    interview.status === "running" ||
    busy === "interview-start" ||
    busy === "interview-respond";
  const latestQuestion = [...interview.messages]
    .reverse()
    .find((item) => item.kind === "question");
  const submitResponse = (event: FormEvent) => {
    event.preventDefault();
    const value = response.trim();
    if (!value) return;
    onRespond(value);
  };

  if (interview.status === "skipped" && runState !== "INTERVIEWING") {
    return null;
  }
  if (interview.status === "confirmed" && interview.confirmed_brief) {
    return (
      <section className="intent-interview-panel confirmed">
        <header>
          <div>
            <span className="eyebrow">Confirmed intent</span>
            <h2>Planning brief</h2>
          </div>
          <StatusBadge value="CONFIRMED" />
        </header>
        <p>
          The architect and independent plan reviewer receive this brief as the
          human-approved description of the intended final shape.
        </p>
        <IntentBriefView brief={interview.confirmed_brief} />
      </section>
    );
  }

  return (
    <section className={`intent-interview-panel ${working ? "working" : ""}`}>
      <header>
        <div>
          <span className="eyebrow">Before planning</span>
          <h2>Deep interview</h2>
        </div>
        <StatusBadge value={working ? "WORKING" : interview.status} />
      </header>
      {working ? (
        <div className="intent-working">
          <div className="runtime-spinner" aria-hidden="true" />
          <div className="intent-working-copy">
            <strong>
              {interview.turn_count > 1
                ? "Updating the intent brief"
                : "Finding the highest-leverage question"}
            </strong>
            <span>
              The selected governor model is using a read-only repository view.
              It cannot start planning until you confirm or skip this interview.
            </span>
          </div>
          <LiveTurnTelemetry
            agent={interviewer?.active_turn_id ? interviewer : undefined}
            fallbackAction={
              interview.turn_count > 1
                ? "Updating the intent brief from your answer"
                : "Finding the highest-leverage question"
            }
          />
        </div>
      ) : interview.status === "waiting_for_human" && latestQuestion ? (
        <>
          <div className="intent-question">
            <strong>{latestQuestion.text}</strong>
            {latestQuestion.why_it_matters && (
              <span>{latestQuestion.why_it_matters}</span>
            )}
            {latestQuestion.suggested_answer && (
              <span>
                Suggested starting point: {latestQuestion.suggested_answer}
              </span>
            )}
          </div>
          <form className="intent-response" onSubmit={submitResponse}>
            <label>
              <span>Your answer</span>
              <textarea
                rows={4}
                value={response}
                onChange={(event) => setResponse(event.target.value)}
                placeholder="Describe the result or tradeoff you want. You can leave implementation choices to the planner."
              />
            </label>
            <button
              className="button primary"
              disabled={!!busy || !response.trim()}
            >
              Send answer
            </button>
          </form>
        </>
      ) : interview.status === "ready_for_confirmation" &&
        interview.draft_brief ? (
        <>
          <p>
            Review the durable handoff below. Confirming starts a fresh architect
            thread; the conversation transcript is not used as planning input.
          </p>
          <IntentBriefView brief={interview.draft_brief} />
          <div className="intent-confirm-actions">
            <button
              className="button primary"
              disabled={!!busy || !interview.draft_digest}
              onClick={() =>
                interview.draft_digest && onConfirm(interview.draft_digest)
              }
            >
              <Check size={14} />
              Use brief and plan
            </button>
          </div>
          <form className="intent-response revise" onSubmit={submitResponse}>
            <label>
              <span>Request one change before planning</span>
              <textarea
                rows={3}
                value={response}
                onChange={(event) => setResponse(event.target.value)}
                placeholder="For example: make the mobile behavior a requirement, not a preference."
              />
            </label>
            <button className="button" disabled={!!busy || !response.trim()}>
              Continue interview
            </button>
          </form>
        </>
      ) : (
        <div className="intent-recovery">
          <div>
            <strong>
              {interview.status === "failed"
                ? "The interview turn did not complete"
                : "The interview is ready to start"}
            </strong>
            <span>
              {interview.last_error ||
                "The interviewer asks only questions that can materially change the intended result."}
            </span>
          </div>
          <button className="button primary" onClick={onStart} disabled={!!busy}>
            <Bot size={14} />
            {interview.status === "failed" ? "Retry interview" : "Start interview"}
          </button>
        </div>
      )}
      {runState === "INTERVIEWING" && (
        <button className="intent-skip" onClick={onSkip} disabled={!!busy}>
          Skip interview and plan from the original request
        </button>
      )}
    </section>
  );
}

function IntentBriefView({ brief }: { brief: IntentBrief }) {
  const sections: Array<[string, string[]]> = [
    ["Intended final shape", brief.intended_final_shape],
    ["Hard constraints", brief.hard_constraints],
    ["Preferences", brief.preferences],
    ["Acceptance examples", brief.acceptance_examples],
    ["Non-goals", brief.non_goals],
    ["Planner may decide", brief.planner_may_decide],
    ["Assumptions to validate", brief.assumptions_to_validate],
  ];
  return (
    <div className="intent-brief">
      <div className="intent-objective">
        <span>Refined objective</span>
        <strong>{brief.refined_objective}</strong>
      </div>
      <div className="intent-brief-grid">
        {sections
          .filter(([, values]) => values.length > 0)
          .map(([label, values]) => (
            <div key={label}>
              <span>{label}</span>
              <ul>
                {values.map((value) => (
                  <li key={value}>{value}</li>
                ))}
              </ul>
            </div>
          ))}
      </div>
    </div>
  );
}

function ArchitectureStatusPanel({
  run,
  architect,
  starting,
}: {
  run: Run;
  architect?: Agent;
  starting: boolean;
}) {
  const planning = starting || planningRunState(run.state);
  const title = starting
    ? "Starting the architect"
    : planning
      ? planningStateTitle(run.state)
      : "Planning has not started yet";
  const detail = starting
    ? "Harness is opening the read-only Sol planning thread now."
    : planning
      ? architect?.current_action ||
        planningStateDetail(run.state)
      : "This task is prepared and waiting. Select Start architecture to begin repository research and planning.";
  return (
    <section
      className={`architecture-status-panel ${planning ? "working" : "ready"}`}
      aria-label="Architecture status"
      aria-live="polite"
    >
      <div className="architecture-status-icon" aria-hidden="true">
        {planning ? <div className="runtime-spinner" /> : <Bot size={17} />}
      </div>
      <div>
        <div className="eyebrow">
          {planning ? "Planning in progress" : "Ready to plan"}
        </div>
        <h2>{title}</h2>
        <p>{detail}</p>
      </div>
      <div className="architecture-status-facts">
        <StatusBadge value={planning ? "PLANNING" : "WAITING TO START"} />
        <span>
          {architect
            ? `${shortModel(agentModel(architect))} · ${agentEffort(architect)} thinking`
            : "SOL · xhigh thinking"}
        </span>
      </div>
      {planning && (
        <LiveTurnTelemetry
          agent={architect?.active_turn_id ? architect : undefined}
          fallbackAction={
            starting ? "Opening the planning turn" : planningStateTitle(run.state)
          }
        />
      )}
    </section>
  );
}

function GoalPlanPanel({
  detail,
  busy,
  onApprove,
  onRequestChanges,
}: {
  detail: RunDetail;
  busy: string;
  onApprove: (allowBudgetOverride?: boolean) => void;
  onRequestChanges: (finding: string) => void;
}) {
  const goal = detail.run.objective;
  const [showChangeRequest, setShowChangeRequest] = useState(false);
  const [changeRequest, setChangeRequest] = useState("");
  const certificate = detail.plan_certificate;
  const reviewHistory = detail.plan_review_history || [];
  const reviewDeadlocked =
    detail.run.state === "BLOCKED" &&
    detail.run.phase === "plan_review_deadlocked";
  const canRequestChanges =
    detail.run.state === "PLAN_REVIEW_REQUIRED" || reviewDeadlocked;
  const budgetOverrideRequired = certificate?.budget.feasible === false;
  return (
    <section className="goal-plan-grid" aria-label="Goal and plan">
      <article className="goal-plan-card">
        <header>
          <div>
            <span>Goal</span>
            <strong>What the governor is pursuing</strong>
          </div>
          <StatusBadge
            value={
              detail.run.state === "PLAN_REVIEW_REQUIRED"
                ? "PLAN CERTIFIED"
                : detail.run.state === "PLAN_ADVERSARIAL_REVIEW"
                  ? "REVIEWING PLAN"
                  : detail.run.state === "PLAN_REVISION_REQUIRED"
                    ? "REVISING PLAN"
                : effectiveRunPosture(detail.run, detail)
            }
          />
        </header>
        <div className="goal-plan-scroll">
          <p>{goal}</p>
        </div>
      </article>
      <article className="goal-plan-card">
        <header>
          <div>
            <span>Plan</span>
            <strong>
              {detail.plan
                ? detail.run.state === "PLAN_ADVERSARIAL_REVIEW"
                  ? "Plan under adversarial review"
                  : detail.run.state === "PLAN_REVISION_REQUIRED"
                    ? "Plan rejected; revision is underway"
                    : "Current implementation plan"
                : busy === "start"
                  ? "Starting the architect"
                  : planningRunState(detail.run.state)
                    ? planningStateTitle(detail.run.state)
                    : "Planning has not started"}
            </strong>
          </div>
          {detail.run.state === "PLAN_REVIEW_REQUIRED" ? (
            <StatusBadge value="YOUR APPROVAL" />
          ) : detail.run.state === "PLAN_ADVERSARIAL_REVIEW" ? (
            <StatusBadge value="CERTIFYING" />
          ) : detail.run.state === "PLAN_REVISION_REQUIRED" ? (
            <StatusBadge value="REVISING" />
          ) : (
            <span className="plan-policy">
              {detail.automatic_plan_approval
                ? "Auto approve after certification"
                : "Manual approval"}
            </span>
          )}
        </header>
        <div className="goal-plan-scroll">
          {detail.plan ? (
            <>
              <p className="plan-summary">{detail.plan.summary}</p>
              <ol className="plan-task-list">
                {detail.plan.tasks.map((task) => {
                  const runtimeTask = detail.tasks.find(
                    (item) => item.external_task_id === task.task_id,
                  );
                  const progress = runtimeTask
                    ? detail.governor_progress?.[runtimeTask.id]
                    : undefined;
                  const milestones = progress?.milestones?.length
                    ? progress.milestones
                    : (task.milestones || []).map((milestone) => ({
                        ...milestone,
                        status: "pending" as const,
                        outcome: milestone.objective,
                        acceptance: milestone.success_criteria,
                      }));
                  return (
                    <li key={task.task_id}>
                      <b>{task.title}</b>
                      <span>{task.objective}</span>
                      {milestones.length > 0 ? (
                        <ol className="milestone-list">
                          {milestones.map((milestone) => (
                            <li
                              className={`milestone ${milestone.status}`}
                              key={milestone.id}
                            >
                              <StatusBadge value={milestone.status} />
                              <div>
                                <b>{milestone.title}</b>
                                <span>{milestone.outcome}</span>
                              </div>
                            </li>
                          ))}
                        </ol>
                      ) : (
                        <small>
                          This legacy task predates milestone checkpoints. The
                          governor will publish its bounded steps on continuation.
                        </small>
                      )}
                    </li>
                  );
                })}
              </ol>
              {certificate && (
                <section
                  className="plan-certificate"
                  aria-label="Plan certificate"
                >
                  <div className="plan-certificate-heading">
                    <b>Certification evidence · revision {certificate.revision}</b>
                    <StatusBadge
                      value={
                        certificate.automatic_approval_eligible
                          ? "AUTO ELIGIBLE"
                          : "HUMAN DECISION"
                      }
                    />
                  </div>
                  <p>{certificate.summary}</p>
                  <div className="plan-certificate-facts">
                    <span>
                      Planning spend {formatTokens(detail.planning_tokens_used || certificate.budget.planning_tokens_used)}
                    </span>
                    <span>
                      Execution reserve {formatTokens(certificate.budget.required_execution_tokens)} / {formatTokens(certificate.budget.remaining_run_tokens)} remaining
                    </span>
                    <span>
                      {shortModel(certificate.reviewer.reviewer_model)} · {certificate.reviewer.reviewer_reasoning_effort} review
                    </span>
                    <span>
                      {certificate.evidence.inspected_files.length} files inspected · {certificate.evidence.failure_modes.length} failure modes
                    </span>
                  </div>
                  <ol className="certificate-critical-path">
                    {certificate.evidence.critical_path.map((step) => (
                      <li key={step.task_id}>
                        <b>{step.task_id}</b>
                        <span>{step.behavioral_proof}</span>
                      </li>
                    ))}
                  </ol>
                  {certificate.advisory_findings.length > 0 && (
                    <div className="certificate-advisories">
                      <b>Execution advisories</b>
                      {certificate.advisory_findings.map((finding, index) => (
                        <span key={`${finding.description}-${index}`}>
                          {finding.description}
                        </span>
                      ))}
                    </div>
                  )}
                  {certificate.automatic_approval_blockers.length > 0 && (
                    <div className="certificate-blockers">
                      {certificate.automatic_approval_blockers.map((reason) => (
                        <span key={reason}>{reason}</span>
                      ))}
                    </div>
                  )}
                </section>
              )}
              {reviewHistory.length > 0 && (
                <details className="plan-review-history">
                  <summary>
                    Review history · {reviewHistory.length} round
                    {reviewHistory.length === 1 ? "" : "s"}
                  </summary>
                  {reviewHistory.map((record, index) => (
                    <div key={`${record.revision}-${record.source}-${index}`}>
                      <b>
                        Revision {record.revision} · {record.source} · {record.verdict.replaceAll("_", " ")}
                      </b>
                      <span>
                        {record.blocking_count} blocking · {record.findings.filter((finding) => finding.severity === "advisory").length} advisory
                      </span>
                      <p>{record.summary}</p>
                    </div>
                  ))}
                </details>
              )}
            </>
          ) : (
            <p>
              {busy === "start"
                ? "Harness is opening the read-only planning thread now."
                : planningRunState(detail.run.state)
                  ? planningStateDetail(detail.run.state)
                  : "Select Start architecture to begin repository research and build the implementation plan."}
            </p>
          )}
        </div>
        {canRequestChanges && (
          <footer className="plan-decision-footer">
            {showChangeRequest ? (
              <div className="plan-change-request">
                <label htmlFor="plan-change-request">
                  Blocking change required
                </label>
                <textarea
                  id="plan-change-request"
                  value={changeRequest}
                  onChange={(event) => setChangeRequest(event.target.value)}
                  placeholder="Describe the concrete plan defect and the correction the next revision must make."
                  rows={3}
                />
                <div>
                  <button
                    className="button subtle"
                    onClick={() => setShowChangeRequest(false)}
                    disabled={!!busy}
                  >
                    Cancel
                  </button>
                  <button
                    className="button primary"
                    onClick={() => {
                      onRequestChanges(changeRequest.trim());
                      setChangeRequest("");
                      setShowChangeRequest(false);
                    }}
                    disabled={!!busy || changeRequest.trim().length < 8}
                  >
                    Request revised plan
                  </button>
                </div>
              </div>
            ) : (
              <>
                <span>
                  {reviewDeadlocked
                    ? "Automated review stopped after repeated or non-shrinking blockers. Give the architect one concrete direction."
                    : "The plan is certified. Approve it or send a blocking finding through the same revision loop."}
                </span>
                <button
                  className="button subtle"
                  onClick={() => setShowChangeRequest(true)}
                  disabled={!!busy || !detail.plan_digest}
                >
                  Request changes
                </button>
                {!reviewDeadlocked && (
                  <button
                    className="button primary"
                    onClick={() => onApprove(budgetOverrideRequired)}
                    disabled={!!busy || !detail.plan_digest}
                  >
                    <ClipboardCheck size={14} />
                    {budgetOverrideRequired
                      ? "Approve with budget override"
                      : "Approve plan"}
                  </button>
                )}
              </>
            )}
          </footer>
        )}
      </article>
    </section>
  );
}

function ExecutionSignoffPanel({
  detail,
  busy,
  onApprove,
  onRequestChanges,
  onAttest,
}: {
  detail: RunDetail;
  busy: string;
  onApprove: () => void;
  onRequestChanges: (file: string, finding: string) => void;
  onAttest: (
    acceptanceId: string,
    targetIdentity: string,
    observations: string,
  ) => void;
}) {
  const [showRejection, setShowRejection] = useState(false);
  const [findingFile, setFindingFile] = useState("");
  const [findingText, setFindingText] = useState("");
  const [attesting, setAttesting] = useState("");
  const [targetIdentity, setTargetIdentity] = useState("");
  const [observations, setObservations] = useState("");
  const packet = detail.signoff_packet;
  if (
    !packet &&
    !["INTEGRATION_VERIFICATION", "FINAL_AUDIT", "HUMAN_REVIEW"].includes(
      detail.run.state,
    )
  ) {
    return null;
  }
  const results = packet?.integration_validation.results || [];
  const behavioral = results.filter(
    (result) => result.evidence_class === "behavioral",
  );
  const finalAuditSummary =
    packet?.final_audit &&
    typeof packet.final_audit.summary === "string"
      ? packet.final_audit.summary
      : undefined;
  const canDecide = detail.run.state === "HUMAN_REVIEW" && Boolean(packet);
  return (
    <section className="signoff-panel" aria-label="Execution signoff">
      <header>
        <div>
          <span>Execution signoff</span>
          <strong>
            {detail.run.state === "HUMAN_REVIEW"
              ? "Controller proof is ready for your decision"
              : detail.run.state === "FINAL_AUDIT"
                ? "Independent audit of the proven integration head"
                : "Running authoritative integrated-head validation"}
          </strong>
        </div>
        <StatusBadge
          value={detail.run.state === "HUMAN_REVIEW" ? "YOUR DECISION" : detail.run.state}
        />
      </header>
      {packet ? (
        <div className="signoff-body">
          <div className="signoff-facts">
            <span>Head <code>{packet.integration_sha.slice(0, 12)}</code></span>
            <span>{results.length} authoritative checks passed</span>
            <span>{behavioral.length} behavioral checks</span>
            <span>{packet.exact_head_evidence.length} exact-head evidence records</span>
            <span>Packet <code>{packet.packet_digest.slice(0, 12)}</code></span>
            <span>{formatTokens(packet.total_tokens_used)} total run tokens</span>
          </div>
          <div className="signoff-validator-list">
            {results.map((result) => (
              <div key={result.validation_id}>
                <StatusBadge value={result.result_class} />
                <b>{result.validator_id}</b>
                <span>{result.evidence_class} · {result.proof_tier}</span>
              </div>
            ))}
          </div>
          {packet.acceptance.filter((item) => item.required).length > 0 && (
            <div className="signoff-acceptance-list">
              <b>Platform acceptance</b>
              {packet.acceptance
                .filter((item) => item.required)
                .map((item) => (
                  <div key={item.id}>
                    <StatusBadge value={item.status} />
                    <div>
                      <b>{item.id}</b>
                      <span>{item.instructions}</span>
                    </div>
                    {canDecide && item.status === "pending_attestation" && (
                      <button
                        className="button subtle"
                        onClick={() => setAttesting(item.id)}
                        disabled={!!busy}
                      >
                        Record result
                      </button>
                    )}
                  </div>
                ))}
            </div>
          )}
          {canDecide && attesting && (
            <div className="signoff-attestation">
              <b>Attest {attesting}</b>
              <label htmlFor="acceptance-target">Target or device identity</label>
              <input
                id="acceptance-target"
                value={targetIdentity}
                onChange={(event) => setTargetIdentity(event.target.value)}
                placeholder="Device, OS, simulator, or bench identifier"
              />
              <label htmlFor="acceptance-observations">Observed behavior</label>
              <textarea
                id="acceptance-observations"
                value={observations}
                onChange={(event) => setObservations(event.target.value)}
                placeholder="State what you ran and what actually happened."
                rows={3}
              />
              <div>
                <button
                  className="button subtle"
                  onClick={() => setAttesting("")}
                  disabled={!!busy}
                >
                  Cancel
                </button>
                <button
                  className="button primary"
                  onClick={() => {
                    onAttest(
                      attesting,
                      targetIdentity.trim(),
                      observations.trim(),
                    );
                    setAttesting("");
                    setTargetIdentity("");
                    setObservations("");
                  }}
                  disabled={
                    !!busy ||
                    targetIdentity.trim().length < 2 ||
                    observations.trim().length < 8
                  }
                >
                  Sign attestation
                </button>
              </div>
            </div>
          )}
          {finalAuditSummary && <p>{finalAuditSummary}</p>}
          {packet.unproved_claims.length > 0 && (
            <div className="signoff-unproved">
              <b>Publication blockers</b>
              {packet.unproved_claims.map((claim) => (
                <span key={claim}>{claim}</span>
              ))}
            </div>
          )}
        </div>
      ) : (
        <p className="signoff-waiting">
          The controller is running the profile-selected validators against the
          exact integrated SHA. Final audit cannot start until they pass.
        </p>
      )}
      {canDecide && (
        <footer>
          {showRejection ? (
            <div className="signoff-rejection">
              <label htmlFor="signoff-finding-file">Affected repository file</label>
              <input
                id="signoff-finding-file"
                value={findingFile}
                onChange={(event) => setFindingFile(event.target.value)}
                placeholder="crates/example/src/lib.rs"
              />
              <label htmlFor="signoff-finding-text">Blocking correction</label>
              <textarea
                id="signoff-finding-text"
                value={findingText}
                onChange={(event) => setFindingText(event.target.value)}
                placeholder="Describe the observed failure and the concrete behavior the repair must produce."
                rows={3}
              />
              <div>
                <button
                  className="button subtle"
                  onClick={() => setShowRejection(false)}
                  disabled={!!busy}
                >
                  Cancel
                </button>
                <button
                  className="button primary"
                  onClick={() => {
                    onRequestChanges(findingFile.trim(), findingText.trim());
                    setShowRejection(false);
                    setFindingFile("");
                    setFindingText("");
                  }}
                  disabled={
                    !!busy ||
                    findingFile.trim().length < 3 ||
                    findingText.trim().length < 8
                  }
                >
                  Reject and repair
                </button>
              </div>
            </div>
          ) : (
            <>
              <span>
                Approval is bound to this packet and integration SHA. A rejection
                must name a file so Harness can reopen the owning task without
                discarding unrelated proven work.
              </span>
              <button
                className="button subtle"
                onClick={() => setShowRejection(true)}
                disabled={!!busy}
              >
                Request changes
              </button>
              <button
                className="button primary"
                onClick={onApprove}
                disabled={!!busy || (packet?.unproved_claims.length || 0) > 0}
              >
                <ClipboardCheck size={14} />
                Approve signoff
              </button>
            </>
          )}
        </footer>
      )}
    </section>
  );
}

function BudgetControl({
  label,
  value,
  valueLabel,
  options,
  hint,
  compact = false,
  onChange,
}: {
  label: string;
  value: number;
  valueLabel: string;
  options: number[];
  hint: string;
  compact?: boolean;
  onChange: (value: number) => void;
}) {
  const values = [...new Set([...options, value])].sort(
    (left, right) => left - right,
  );
  const index = Math.max(0, values.indexOf(value));
  return (
    <label className={`budget-control ${compact ? "compact" : ""}`}>
      <span>
        <b>{label}</b>
        <strong>{valueLabel}</strong>
      </span>
      <input
        type="range"
        min={0}
        max={values.length - 1}
        step={1}
        value={index}
        onChange={(event) => onChange(values[Number(event.target.value)])}
        aria-label={`${label} token budget`}
      />
      <small>{hint}</small>
    </label>
  );
}

export function blockedPlanRecovery(run: Run, planDigest?: string) {
  if (
    run.state === "BLOCKED" &&
    run.phase === "plan_review_budget_exhausted"
  ) {
    return {
      kind: "resume_review" as const,
      reason:
        run.failure_reason ||
        "The independent reviewer reached its bounded session budget before returning a verdict.",
      hasPlan: Boolean(planDigest),
    };
  }
  if (
    run.state === "BLOCKED" &&
    run.phase === "plan_review_deadlocked"
  ) {
    return {
      kind: "revise_plan" as const,
      reason:
        run.failure_reason ||
        "Repeated review findings did not converge without an operator decision.",
      hasPlan: Boolean(planDigest),
    };
  }
  return undefined;
}

export function SupervisorObservationPanel({
  detail,
  busy = "",
  onRequestReview = () => undefined,
  onApplyAction = () => undefined,
}: {
  detail: RunDetail;
  busy?: string;
  onRequestReview?: () => void;
  onApplyAction?: (actionId: string) => void;
}) {
  const mode = detail.supervision_mode || "disabled";
  const snapshot = detail.supervisor_snapshot;
  const review = detail.supervisor_review;
  const decision = detail.supervisor_decision;
  const actionReceipts = detail.supervisor_actions || [];
  const observing = mode === "observe_only";
  const advisory = mode === "advisory";
  const reviewRunning = ["STARTING", "RUNNING"].includes(review?.state || "");
  const canRequest = advisory && !reviewRunning && !terminal(detail.run.state);
  return (
    <section className="supervisor-observation" aria-label="Supervisory observation">
      <header>
        <div className="supervisor-observation-icon" aria-hidden="true">
          {advisory ? <Bot size={17} /> : <Database size={17} />}
        </div>
        <div>
          <div className="eyebrow">Thread supervision</div>
          <h2>
            {advisory
              ? "Human-approved recovery advisor"
              : observing
                ? "Observe-only custody"
                : "Supervision is disabled"}
          </h2>
          <p>
            {advisory
              ? "Terra can analyze a fresh immutable blocker snapshot, read-only. It cannot resume, retry, replan, edit, approve proof, or take any action; those remain your choices below."
              : observing
              ? "Immutable controller snapshots are being recorded. Terra, Sol, and automatic actions remain off."
              : "No supervisory model is running, no snapshot is being recorded, and no automatic action is available."}
          </p>
        </div>
        <StatusBadge value={advisory ? "ADVISORY" : observing ? "OBSERVE ONLY" : "DISABLED"} />
      </header>
      {snapshot && (
        <div className="supervisor-observation-receipt">
          <span>
            Latest snapshot r{snapshot.revision} · {formatLocalTimestamp(snapshot.created_at)}
          </span>
          <span>Trigger: {humanizeSupervisorTrigger(snapshot.trigger_kind)}</span>
          <span title={snapshot.payload_sha256}>
            Event {snapshot.event_cursor} · SHA-256 {snapshot.payload_sha256.slice(0, 12)}…
          </span>
        </div>
      )}
      {review && (
        <div className="supervisor-observation-receipt">
          <span>
            Terra review · {humanAgentState(review.state)} · started {formatLocalTimestamp(review.created_at)}
          </span>
          <span>
            {review.completed_at
              ? `Completed ${formatLocalTimestamp(review.completed_at)}`
              : review.failure_reason || "Read-only analysis in progress"}
          </span>
        </div>
      )}
      {decision && (
        <div className="supervisor-decision">
          <strong>{decision.policy_state === "STALE" ? "Superseded analysis" : "Terra’s read-only assessment"}</strong>
          <p>{decision.payload.summary || "A bounded assessment was recorded without a displayable summary."}</p>
          {decision.policy_state !== "STALE" && decision.payload.actions?.length ? (
            <div className="supervisor-proposals" aria-label="Advisory recovery proposals">
              {decision.payload.actions.slice(0, 3).map((action, index) => (
                <span key={action.action_id || index}>
                  {action.kind?.replaceAll("_", " ") || "proposal"}: {action.summary || action.expected_observable_outcome || "Review the recorded evidence before acting."}
                </span>
              ))}
            </div>
          ) : null}
          {decision.payload.uncertainties?.length ? (
            <p className="muted">Uncertainty: {decision.payload.uncertainties[0]}</p>
          ) : null}
        </div>
      )}
      {actionReceipts.length > 0 && (
        <div className="supervisor-action-receipts" aria-label="Supervisor action receipts">
          <strong>Controller action policy</strong>
          {actionReceipts.slice(0, 6).map((action) => (
            <div key={action.id} className="supervisor-action-receipt" title={action.proposal_sha256}>
              <span>
                {action.kind.replaceAll("_", " ")} · {humanAgentState(action.state)}
                {action.policy_reason ? ` — ${action.policy_reason}` : " — awaiting controller policy"}
              </span>
              {action.state === "PROPOSED" && (
                <button
                  className="button secondary small"
                  onClick={() => onApplyAction(action.id)}
                  disabled={!!busy}
                >
                  Revalidate and apply
                </button>
              )}
            </div>
          ))}
          <p className="muted">These receipts explain proposed actions. They do not apply, resume, retry, or alter work.</p>
        </div>
      )}
      {canRequest && (
        <div className="supervisor-observation-actions">
          <button className="button" onClick={onRequestReview} disabled={!!busy}>
            <Search size={14} />
            {snapshot ? "Analyze current blocker" : "Analyze this run"}
          </button>
          <span>Starts one bounded, read-only Terra turn. It will not act on its recommendation.</span>
        </div>
      )}
    </section>
  );
}

function humanizeSupervisorTrigger(trigger: string) {
  return trigger.replaceAll("_", " ");
}

function BlockedRunRecoveryPanel({
  detail,
  busy,
  onResumeReview,
  onRequestChanges,
  onResumeWork,
  onRetry,
}: {
  detail: RunDetail;
  busy: string;
  onResumeReview: () => void;
  onRequestChanges: (finding: string) => void;
  onResumeWork: () => void;
  onRetry: (taskId: string) => void;
}) {
  const recovery = blockedPlanRecovery(detail.run, detail.plan_digest);
  const retryTask = detail.tasks.find((task) => retryableState(task.state));
  const blocked = blockerStatus(
    detail.run,
    retryTask,
    detail.agents.find((agent) =>
      ["BLOCKED", "FAILED", "STALLED", "INTERRUPTED"].includes(agent.state),
    ),
  );
  const [showRevision, setShowRevision] = useState(false);
  const [revisionRequest, setRevisionRequest] = useState("");
  if (!recovery && !blocked) return null;

  const canSubmitRevision = Boolean(detail.plan_digest) && revisionRequest.trim().length >= 8;
  return (
    <section className="blocked-run-recovery" aria-label="Blocked run recovery">
      <header>
        <div className="blocked-run-recovery-icon" aria-hidden="true">
          <AlertTriangle size={18} />
        </div>
        <div>
          <div className="eyebrow">Blocked, action available</div>
          <h2>
            {recovery?.kind === "resume_review"
              ? "The final plan review can resume"
              : "Choose a safe recovery"}
          </h2>
          <p>
            <strong>Why:</strong> {recovery?.reason || blocked?.reason || "Harness recorded a blocked condition."}
          </p>
        </div>
        <StatusBadge value="RECOVERY AVAILABLE" />
      </header>
      {recovery?.kind === "resume_review" && (
        <p className="blocked-run-recovery-explainer">
          Resume starts a bounded, read-only verdict review. If the App Server still has the prior reviewer thread, its evidence is reused; otherwise Harness starts a fresh independent review of the same immutable plan. It does not approve the plan or begin implementation.
        </p>
      )}
      {!detail.plan_digest ? (
        <p className="blocked-run-recovery-unavailable">
          Harness cannot find the retained plan needed for a safe recovery. The recorded evidence is preserved; create a scoped follow-up rather than guessing at a continuation.
        </p>
      ) : showRevision ? (
        <div className="blocked-run-revision">
          <label htmlFor="blocked-plan-revision">
            Concrete correction for the architect
          </label>
          <textarea
            id="blocked-plan-revision"
            value={revisionRequest}
            onChange={(event) => setRevisionRequest(event.target.value)}
            placeholder="Describe the plan defect and the specific correction the next revision must make."
            rows={3}
            maxLength={8000}
          />
          <div>
            <button
              className="button subtle"
              onClick={() => setShowRevision(false)}
              disabled={!!busy}
            >
              Cancel
            </button>
            <button
              className="button primary"
              onClick={() => {
                onRequestChanges(revisionRequest.trim());
                setRevisionRequest("");
                setShowRevision(false);
              }}
              disabled={!!busy || !canSubmitRevision}
            >
              Request plan revision
            </button>
          </div>
        </div>
      ) : (
        <div className="blocked-run-recovery-actions">
          {recovery?.kind === "resume_review" && (
            <button
              className="button primary"
              onClick={onResumeReview}
              disabled={!!busy}
            >
              <Play size={13} />
              Resume final review
            </button>
          )}
          {recovery?.kind !== "resume_review" && detail.run.scheduler_paused && (
            <button className="button primary" onClick={onResumeWork} disabled={!!busy}>
              <Play size={13} />
              Resume work
            </button>
          )}
          {recovery?.kind !== "resume_review" && !detail.run.scheduler_paused && retryTask && (
            <button className="button primary" onClick={() => onRetry(retryTask.id)} disabled={!!busy}>
              <Play size={13} />
              Retry affected task
            </button>
          )}
          <button className="button subtle" onClick={() => setShowRevision(true)} disabled={!!busy}>
            Request plan revision
          </button>
        </div>
      )}
    </section>
  );
}

function NeedsHelpPanel({
  task,
  governor,
  children,
  latestMessage,
  settings,
  busy,
  onRetry,
}: {
  task: Task;
  governor?: Agent;
  children: Agent[];
  latestMessage?: LatestAgentMessage;
  settings?: OperatorSettings;
  busy: string;
  onRetry: (guidance: string, additionalTokenBudget: number) => void;
}) {
  const [guidance, setGuidance] = useState("");
  const [additionalTokenBudget, setAdditionalTokenBudget] = useState(0);
  const governing = governor?.role === "governor";
  const completedChildren = children.filter((child) =>
    ["TURN_COMPLETE", "COMPLETED", "FAILED", "INTERRUPTED"].includes(
      child.state,
    ),
  ).length;
  const budgetPaused =
    governor?.state === "PAUSED" &&
    /budget|turn slice|reconcil/i.test(governor.current_action || "");
  const noProgressPaused = /no-progress/i.test(governor?.current_action || "");
  return (
    <section className="needs-help-panel">
      <header>
        <div className="needs-help-icon">
          <AlertTriangle size={17} />
        </div>
        <div>
          <div className="eyebrow">
            Governor control · {humanTaskState(task.state)}
          </div>
          <h2>
            {budgetPaused
              ? "The controller is reconciling a bounded governor turn"
              : noProgressPaused
                ? "The governor exhausted its no-progress safety window"
                : governing
                  ? "The governor paused after this bounded attempt"
                  : "The task owner finished this attempt and needs your direction"}
          </h2>
          <p>
            {latestMessage
              ? `Governor update ${timeAgo(latestMessage.occurred_at)}`
              : "The governor's final message is loading."}
          </p>
        </div>
      </header>
      <div className="needs-help-facts">
        <span>
          <b>{shortModel(agentModel(governor))}</b> ·{" "}
          {roleLabel(governor?.role)} · {agentEffort(governor)}
        </span>
        <span>
          {governor ? humanAgentState(governor.state) : "Not launched"} ·{" "}
          {governor?.current_action || "handoff available"}
        </span>
        <span>
          {children.length} delegated · {completedChildren} finished
        </span>
        <span>
          {formatTokens(agentBudgetUsage(governor))}
          {governor?.token_budget
            ? ` / ${formatTokens(governor.token_budget)}`
            : ""}{" "}
          tokens
        </span>
      </div>
      <div>
        <div className="inspector-label">Governor update</div>
        <div className="handoff-message">
          {latestMessage
            ? humanAgentMessage(latestMessage.text)
            : "No completed governor update was projected for this attempt. Harness can still continue from controller-owned history."}
        </div>
      </div>
      <div className="help-compose">
        <textarea
          value={guidance}
          onChange={(event) => setGuidance(event.target.value)}
          maxLength={1000}
          rows={3}
          placeholder="Optional: add a priority, decision, or new fact. Leave blank and Harness will choose the next action."
        />
        <BudgetControl
          label="Next attempt"
          value={additionalTokenBudget}
          options={ADDITIONAL_BUDGET_OPTIONS}
          onChange={setAdditionalTokenBudget}
          valueLabel={
            additionalTokenBudget
              ? `Adaptive + ${formatTokens(additionalTokenBudget)} tokens`
              : "Adaptive"
          }
          hint={`Harness currently recommends ${formatTokens(settings?.recommended_governor_attempt_tokens || 650_000)} governor tokens. Bounded child-thread headroom is reserved automatically; manual additions are available through 50m with a 100m hard attempt ceiling.`}
          compact
        />
        <div>
          <span>
            {guidance.length}/1000 · durable progress and recent attempts are
            included automatically
          </span>
          <button
            className="button primary"
            onClick={() => onRetry(guidance.trim(), additionalTokenBudget)}
            disabled={!!busy}
          >
            <Play size={13} />
            Continue governor
          </button>
        </div>
      </div>
    </section>
  );
}

function TaskRuntimePanel({
  task,
  agent,
  children,
}: {
  task: Task;
  agent?: Agent;
  children: Agent[];
}) {
  const activeTurn = Boolean(
    agent?.active_turn_id &&
    ["STARTING", "RUNNING", "STEERED", "WAITING_APPROVAL"].includes(
      agent.state,
    ),
  );
  const preparing = task.state === "LEASED";
  const connecting = task.state === "STARTING";
  const governing = agent?.role === "governor";
  const title = preparing
    ? `Preparing attempt ${task.attempt}`
    : connecting
      ? `Connecting ${shortModel(agentModel(agent))} agent`
      : activeTurn
        ? governing
          ? `${shortModel(agentModel(agent))} governor is overseeing ${children.length} delegated thread${children.length === 1 ? "" : "s"}`
          : `${shortModel(agentModel(agent))} is actively working`
        : task.state === "WAITING_APPROVAL"
          ? "Agent is waiting for your approval"
          : "Processing the agent handoff";
  const detail = preparing
    ? "Creating and validating a fresh isolated workspace. No Codex turn is active yet."
    : connecting
      ? "The agent session exists and Harness is waiting for the App Server turn to begin."
      : activeTurn
        ? `${agent?.current_action || "The turn is active; commands and messages will appear in Recent activity."}${agent?.context_strategy === "bounded_handoff" ? " Prior attempt handoff is loaded." : ""}`
        : "No turn is active right now. Harness is projecting the completed response and deciding the next task state.";
  return (
    <section
      className={`task-runtime-panel ${activeTurn || preparing || connecting ? "working" : ""}`}
    >
      <div className="runtime-spinner" aria-hidden="true" />
      <div>
        <div className="eyebrow">
          Selected task · {humanTaskState(task.state)}
        </div>
        <h2>{title}</h2>
        <p>{detail}</p>
      </div>
      <div className="runtime-facts">
        <strong>{activeTurn ? "Turn active" : "No active turn"}</strong>
        <span>
          {agent?.heartbeat_at
            ? `Last activity ${timeAgo(agent.heartbeat_at)}`
            : "Waiting for first agent heartbeat"}
        </span>
      </div>
      {activeTurn && (
        <LiveTurnTelemetry
          agent={agent}
          fallbackAction="Waiting for the first activity event"
        />
      )}
    </section>
  );
}

export function LiveTurnTelemetry({
  agent,
  fallbackAction,
}: {
  agent?: Agent;
  fallbackAction: string;
}) {
  const [clock, setClock] = useState(() => Date.now());
  const startedAt = agent?.active_turn_started_at;
  useEffect(() => {
    if (!startedAt) return;
    setClock(Date.now());
    const timer = window.setInterval(() => setClock(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, [startedAt]);
  const usage = agent?.active_turn_usage;
  const value = (tokens?: number) =>
    usage && tokens !== undefined ? formatTokens(tokens) : "—";
  return (
    <div
      className={`live-turn-telemetry ${usage ? "has-usage" : "awaiting-usage"}`}
      role="group"
      aria-label="Live turn telemetry"
    >
      <div className="live-turn-heading">
        <i aria-hidden="true" />
        <strong aria-live="polite">
          {agent?.current_action || fallbackAction}
        </strong>
        <span>
          {startedAt ? formatTurnElapsed(startedAt, clock) : "Starting turn"}
          {agent?.heartbeat_at ? ` · activity ${timeAgo(agent.heartbeat_at)}` : ""}
        </span>
      </div>
      <dl className="live-turn-metrics" aria-label="Current turn token usage">
        <div>
          <dt>Input</dt>
          <dd>{value(usage?.input_tokens)}</dd>
        </div>
        <div>
          <dt>Cached input</dt>
          <dd>{value(usage?.cached_input_tokens)}</dd>
        </div>
        <div>
          <dt>Output</dt>
          <dd>{value(usage?.output_tokens)}</dd>
        </div>
        <div title="Reasoning tokens are included in output">
          <dt>Reasoning in output</dt>
          <dd>{value(usage?.reasoning_output_tokens)}</dd>
        </div>
        <div>
          <dt>Turn total</dt>
          <dd>{value(usage?.total_tokens)}</dd>
        </div>
      </dl>
      {!usage && (
        <span className="live-turn-awaiting">
          Waiting for the first model-usage update
        </span>
      )}
    </div>
  );
}

function RunPrimaryAction({
  run,
  detail,
  posture,
  busy,
  onStart,
  onApprove,
  onApproveIntegration,
  onApproveSignoff,
  onPublish,
  onRefreshCi,
}: {
  run: Run;
  detail: RunDetail;
  posture: string;
  busy: string;
  onStart: () => void;
  onApprove: (allowBudgetOverride?: boolean) => void;
  onApproveIntegration: () => void;
  onApproveSignoff: () => void;
  onPublish: () => void;
  onRefreshCi: () => void;
}) {
  if (run.state === "READY_FOR_ARCHITECTURE")
    return (
      <button className="button primary" onClick={onStart} disabled={!!busy}>
        <Bot size={14} />
        Start architecture
      </button>
    );
  if (run.state === "PLAN_REVIEW_REQUIRED")
    return (
      <button
        className="button primary"
        onClick={() => onApprove()}
        disabled={!!busy || !detail.plan_digest}
      >
        <ClipboardCheck size={14} />
        Approve task graph
      </button>
    );
  if (run.state === "INTEGRATION_READY")
    return (
      <button
        className="button primary"
        onClick={onApproveIntegration}
        disabled={!!busy || !run.integration_sha}
      >
        <GitCompareArrows size={14} />
        Approve integration
      </button>
    );
  if (run.state === "HUMAN_REVIEW")
    return (
      <button
        className="button primary"
        onClick={onApproveSignoff}
        disabled={!!busy || !detail.signoff_packet}
      >
        <ClipboardCheck size={14} />
        Approve signoff
      </button>
    );
  if (
    run.state === "PUBLICATION_READY" &&
    run.publication_mode === "draft_pr_after_approval"
  )
    return (
      <button
        className="button primary"
        onClick={onPublish}
        disabled={!!busy || !run.integration_sha}
      >
        <GitBranch size={14} />
        Create draft PR
      </button>
    );
  if (run.state === "DRAFT_PR_CREATED")
    return (
      <button className="button primary" onClick={onRefreshCi} disabled={!!busy}>
        <Activity size={14} />
        {detail.draft_pr_ci?.status === "pending"
          ? "Refresh pending CI"
          : "Refresh required CI"}
      </button>
    );
  return (
    <button className="button primary muted" disabled>
      <Activity size={14} />
      {posture}
    </button>
  );
}

function AgentRow({
  run,
  task,
  agent,
  selected,
  selectedAgentId,
  onClick,
  onSelectChild,
  children = [],
}: {
  run?: Run;
  task?: Task;
  agent?: Agent;
  selected: boolean;
  selectedAgentId?: string;
  onClick: () => void;
  onSelectChild?: (id: string) => void;
  children?: Agent[];
}) {
  const state = task?.state || agent?.state || "QUEUED";
  const preparing = Boolean(
    task && ["LEASED", "STARTING"].includes(task.state),
  );
  const activeTurn = Boolean(
    agent?.active_turn_id &&
    ["STARTING", "RUNNING", "STEERED", "WAITING_APPROVAL"].includes(
      agent.state,
    ),
  );
  const displayAgent = preparing && !agent?.active_turn_id ? undefined : agent;
  const displayState = preparing
    ? "STARTING"
    : activeTurn
      ? "WORKING"
      : task
        ? humanTaskState(state).toUpperCase()
        : state;
  const blocker = blockerStatus(run, task, displayAgent);
  const currentActivity =
    task?.state === "LEASED"
      ? `Preparing attempt ${task.attempt} · creating isolated workspace`
      : task?.state === "STARTING"
        ? "Agent session is connecting · waiting for turn start"
        : activeTurn
          ? `${displayAgent?.current_action || "Agent turn is active"}${displayAgent?.role === "governor" ? ` · ${children.length} delegated thread${children.length === 1 ? "" : "s"}` : ""}${displayAgent?.context_strategy === "bounded_handoff" ? " · prior handoff loaded" : displayAgent?.context_strategy === "native_thread_reuse" ? " · governor context retained" : ""}`
          : blocker?.reason ||
            displayAgent?.current_action ||
            displayAgent?.current_goal ||
            (task?.dependencies.length
              ? `Waiting on ${task.dependencies.join(", ")}`
              : task?.objective) ||
            "Waiting for runtime activity";
  const standalonePurpose =
    agent?.role === "final_auditor"
      ? "final quality review"
      : "planning and task design";
  const usage = displayAgent
    ? `${formatTokens(agentBudgetUsage(displayAgent))}${displayAgent.token_budget ? ` / ${formatTokens(displayAgent.token_budget)}` : ""}`
    : "—";
  const cost =
    displayAgent?.estimated_cost_upper || (preparing ? "pending" : "$0.00");
  return (
    <div
      className={`agent-card ${selected ? "selected" : ""} ${activeTurn ? "working" : ""}`}
    >
      <button
        className="agent-row agent-row-button"
        onClick={onClick}
        aria-pressed={selected}
      >
        <StateIcon state={displayState} working={activeTurn || preparing} />
        <div className="agent-copy">
          <strong>
            {task
              ? `${task.external_task_id} · ${task.title}`
              : `${roleLabel(agent?.role)} · ${standalonePurpose}`}
          </strong>
          <span>{currentActivity}</span>
        </div>
        <div className="agent-model">
          <b>{shortModel(agentModel(displayAgent))}</b>
          <span>
            {displayAgent ? `${roleLabel(displayAgent.role)} · ` : ""}
            {agentEffort(displayAgent)} ·{" "}
            {displayAgent
              ? permissionLabel(displayAgent.sandbox_mode)
              : preparing
                ? "not launched"
                : "—"}
          </span>
        </div>
        <div className="agent-usage">
          <strong>Thread usage / budget · {usage}</strong>
          <span>API cost · {cost}</span>
          {displayAgent && (
            <span
              className="agent-lifecycle"
              title="Thread lifecycle times use this browser's local time zone"
            >
              {threadLifecycleRowSummary(displayAgent)}
            </span>
          )}
        </div>
        <div className="agent-state-cell">
          {selected && <span className="selected-marker">Selected</span>}
          <StatusBadge value={displayState} />
        </div>
      </button>
      {children.length > 0 && (
        <div className="children">
          <div className="children-label">Delegated threads</div>
          {children.map((child) => {
            const childSelected = selectedAgentId === child.id;
            const childState = delegatedThreadDisplayState(child, activeTurn);
            return (
              <button
                type="button"
                className={`child ${childSelected ? "selected" : ""}`}
                aria-pressed={childSelected}
                key={child.id}
                onClick={() => onSelectChild?.(child.id)}
              >
                <span>
                  ↳ <b>{childDisplayName(child)}</b>
                  <small>
                    {childState === "FINISHING"
                      ? "Finishing background work while the governor waits for you"
                      : blockerStatus(run, task, child)?.reason ||
                        child.current_action ||
                        child.current_goal ||
                        "Waiting for activity"}
                  </small>
                  <small title="Thread lifecycle times use this browser's local time zone">
                    {threadLifecycleRowSummary(child)}
                  </small>
                </span>
                <span>
                  {childSelected && (
                    <b className="selected-marker">Selected · </b>
                  )}
                  {shortModel(agentModel(child))} · {agentEffort(child)}
                </span>
                <span className="child-usage">
                  {formatTokens(child.tokens_used || 0)} tokens · API cost ·{" "}
                  {child.estimated_cost_upper || "$0.00"}
                </span>
                <StatusBadge value={childState} />
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function Inspector({
  task,
  agent,
  governor,
  worktree,
  worktreeDiff,
  detail,
  latestMessage,
  messages,
  governorLatestMessage,
  busy,
  onOpenMessages,
  onSelectGovernor,
  onSteer,
  onInterrupt,
  onRequestReview,
  onApprovalDecision,
}: {
  task?: Task;
  agent?: Agent;
  governor?: Agent;
  worktree?: Worktree;
  worktreeDiff?: WorktreeDiffSummary;
  detail: RunDetail;
  latestMessage?: LatestAgentMessage;
  messages: LatestAgentMessage[];
  governorLatestMessage?: LatestAgentMessage;
  busy: string;
  onOpenMessages: () => void;
  onSelectGovernor: () => void;
  onSteer: (text: string) => void;
  onInterrupt: () => void;
  onRequestReview: () => void;
  onApprovalDecision: (approval: Approval, decision: string) => void;
}) {
  const [steer, setSteer] = useState("");
  const preparing = Boolean(
    task &&
    ["LEASED", "STARTING"].includes(task.state) &&
    !agent?.active_turn_id,
  );
  const currentAgent = preparing ? undefined : agent;
  const controlAgent = task ? governor : currentAgent;
  const controlName = task
    ? "governor"
    : roleLabel(controlAgent?.role).toLowerCase();
  const viewingChild = Boolean(currentAgent?.parent_agent_id);
  const currentWorktree =
    preparing && worktree?.state === "PRESERVED" ? undefined : worktree;
  const taskState =
    task?.state === "LEASED"
      ? "STARTING"
      : task
        ? humanTaskState(task.state).toUpperCase()
        : undefined;
  const pendingApprovals = detail.approvals.filter(
    (approval) => approval.state === "pending",
  );
  return (
    <aside className="inspector">
      <div className="inspector-head">
        <div className="inspector-head-line">
          <div>
            <div className="eyebrow">
              {task
                ? `${viewingChild ? "Child thread" : roleLabel(currentAgent?.role)} · task ${task.external_task_id} · attempt ${task.attempt} · ${taskState?.replaceAll("_", " ")}`
                : roleLabel(agent?.role)}
            </div>
            <h2>
              {viewingChild && currentAgent
                ? childDisplayName(currentAgent)
                : task?.title ||
                  agent?.current_goal ||
                  (planningRunState(detail.run.state)
                    ? planningStateTitle(detail.run.state)
                    : detail.run.state === "READY_FOR_ARCHITECTURE"
                      ? "Planning not started"
                      : "Select a task")}
            </h2>
          </div>
          {viewingChild && (
            <button className="button subtle" onClick={onSelectGovernor}>
              <Bot size={13} />
              Governor
            </button>
          )}
        </div>
        <p>
          {currentAgent?.current_action ||
            (currentAgent?.active_turn_id
              ? "Working now"
              : detail.run.state === "READY_FOR_ARCHITECTURE"
                ? "Select Start architecture to begin"
                : planningRunState(detail.run.state)
                  ? planningStateDetail(detail.run.state)
                  : "No active turn")}
        </p>
      </div>
      {pendingApprovals.length > 0 && (
        <div className="inline-approvals">
          <div className="inspector-label">Needs your approval</div>
          {pendingApprovals.map((approval) => (
            <div className="inline-approval" key={approval.id}>
              <div>
                <strong>{approvalLabel(approval)}</strong>
                <span>{approvalSummary(approval.request)}</span>
              </div>
              <button
                className="button subtle"
                onClick={() => onApprovalDecision(approval, "decline")}
                disabled={!!busy}
              >
                Deny
              </button>
              <button
                className="button primary"
                onClick={() => onApprovalDecision(approval, "accept")}
                disabled={!!busy}
              >
                Approve
              </button>
            </div>
          ))}
        </div>
      )}
      <div className="inspector-body">
        <InspectorContent
          task={task}
          agent={currentAgent}
          governor={governor}
          worktree={currentWorktree}
          worktreeDiff={worktreeDiff}
          detail={detail}
          latestMessage={latestMessage}
          messages={messages}
          governorLatestMessage={governorLatestMessage}
          onOpenMessages={onOpenMessages}
        />
      </div>
      {task?.state === "REVIEW_READY" && (
        <div className="task-action-box">
          <button
            className="button primary"
            onClick={onRequestReview}
            disabled={!!busy}
          >
            <ShieldCheck size={13} />
            Request independent review
          </button>
        </div>
      )}
      {controlAgent?.active_turn_id && (
        <div className="steer-box">
          <label>Message the {controlName}</label>
          <textarea
            value={steer}
            onChange={(event) => setSteer(event.target.value)}
            placeholder={
              task
                ? "Give the governor direction; it will manage the child threads…"
                : `Give the ${controlName} direction…`
            }
            rows={3}
          />
          <div>
            <span>Send without leaving this run</span>
            <button
              className="icon-button danger-hover"
              onClick={onInterrupt}
              disabled={!!busy}
              title={`Interrupt ${controlName} turn`}
            >
              <Square size={13} />
            </button>
            <button
              className="button primary"
              onClick={() => {
                if (steer.trim()) {
                  onSteer(steer.trim());
                  setSteer("");
                }
              }}
              disabled={!steer.trim() || !!busy}
            >
              Send
            </button>
          </div>
        </div>
      )}
    </aside>
  );
}

function InspectorContent({
  task,
  agent,
  governor,
  worktree,
  worktreeDiff,
  detail,
  latestMessage,
  messages,
  governorLatestMessage,
  onOpenMessages,
}: {
  task?: Task;
  agent?: Agent;
  governor?: Agent;
  worktree?: Worktree;
  worktreeDiff?: WorktreeDiffSummary;
  detail: RunDetail;
  latestMessage?: LatestAgentMessage;
  messages: LatestAgentMessage[];
  governorLatestMessage?: LatestAgentMessage;
  onOpenMessages: () => void;
}) {
  const viewingChild = Boolean(agent?.parent_agent_id);
  const visibleMessages = messages.length
    ? messages
    : latestMessage
      ? [latestMessage]
      : [];
  const blocker = blockerStatus(detail.run, task, agent);
  return (
    <>
      {viewingChild && (
        <InspectorCard label="Read-only child inspection">
          <p>
            You are viewing this delegated thread's work. Continue and Steer
            remain routed to the governor.
          </p>
          <small>
            Governor: {governor?.state.replaceAll("_", " ")} ·{" "}
            {governor?.current_action ||
              governorLatestMessage?.text.slice(0, 180) ||
              "no recent action"}
          </small>
        </InspectorCard>
      )}
      {blocker && (
        <InspectorCard label="Blocker and next available step">
          <p>
            <strong>Why</strong> · {blocker.reason}
          </p>
          <small>
            <strong>Next</strong> · {blocker.nextStep}
          </small>
        </InspectorCard>
      )}
      <PlanProgressPanel task={task} governor={governor} detail={detail} />
      <MessageHistoryPreview
        label={
          agent?.role === "governor" ? "Governor messages" : "Thread messages"
        }
        messages={visibleMessages}
        onOpen={onOpenMessages}
      />
      <WorkStatusCard
        task={task}
        worktree={worktree}
        diff={worktreeDiff}
        run={detail.run}
      />
      <InspectorCard
        label={agent?.role === "governor" ? "Governor status" : "Thread status"}
      >
        <p>
          <strong>
            {agent
              ? humanAgentState(agent.state)
              : task
                ? humanTaskState(task.state)
                : "Unknown"}
          </strong>{" "}
          · {agent?.current_action || "No current action"}
        </p>
        <div className="mini-progress">
          <i
            style={{
              width: `${agent?.token_budget ? Math.min(100, (agentBudgetUsage(agent) / agent.token_budget) * 100) : 0}%`,
            }}
          />
        </div>
        <small>
          {shortModel(agentModel(agent))} · {agentEffort(agent)} thinking
        </small>
        <small>
          Current turn usage / budget · {formatTokens(agentBudgetUsage(agent))} /{" "}
          {formatTokens(agent?.token_budget || 0)} · API cost ·{" "}
          {agent?.estimated_cost_upper || "$0.00"} ·{" "}
          {agent?.active_turn_id ? "turn active" : "no active turn"}
        </small>
        <details className="thread-details">
          <summary>Goal and runtime</summary>
          <p>{agent?.current_goal || task?.objective || "No active goal"}</p>
          <small>
            {contextStrategyLabel(agent?.context_strategy)} ·{" "}
            {agent?.heartbeat_at
              ? `heartbeat ${timeAgo(agent.heartbeat_at)}`
              : "no heartbeat"}
          </small>
          {agent && (
            <small title="Thread lifecycle times use this browser's local time zone">
              {threadLifecycleSummary(agent)}
            </small>
          )}
        </details>
      </InspectorCard>
    </>
  );
}

function PlanProgressPanel({
  task,
  governor,
  detail,
}: {
  task?: Task;
  governor?: Agent;
  detail: RunDetail;
}) {
  if (!task) return null;

  const planTask = detail.plan?.tasks.find(
    (candidate) => candidate.task_id === task.external_task_id,
  );
  const checkpoint = detail.governor_progress?.[task.id];
  const milestones = checkpoint?.milestones?.length
    ? checkpoint.milestones
    : (planTask?.milestones || []).map((milestone) => ({
        id: milestone.id,
        title: milestone.title,
        status: "pending" as const,
        outcome: milestone.objective,
        acceptance: milestone.success_criteria,
      }));
  const assignedAgents = governor
    ? [
        governor,
        ...detail.agents.filter(
          (candidate) => candidate.parent_agent_id === governor.id,
        ),
      ]
    : [];
  const completed = milestones.filter(
    (milestone) => milestone.status === "completed",
  ).length;
  const active = milestones.filter(
    (milestone) =>
      milestone.status === "in_progress" || milestone.status === "blocked",
  ).length;
  const planTasks = detail.plan?.tasks || [];
  const completedTaskStates = [
    "VERIFIED",
    "INTEGRATED",
    "CI_PROVEN",
    "LIVE_PROVEN",
    "CLOSED",
  ];
  const phases = planTasks.map((candidate) => {
    const runtimeTask = detail.tasks.find(
      (item) => item.external_task_id === candidate.task_id,
    );
    const phaseCheckpoint = runtimeTask
      ? detail.governor_progress?.[runtimeTask.id]
      : undefined;
    const phaseMilestones = phaseCheckpoint?.milestones?.length
      ? phaseCheckpoint.milestones
      : (candidate.milestones || []).map((milestone) => ({
          ...milestone,
          status: "pending" as const,
          outcome: milestone.objective,
          acceptance: milestone.success_criteria,
        }));
    const phaseComplete = Boolean(
      (runtimeTask && completedTaskStates.includes(runtimeTask.state)) ||
        phaseCheckpoint?.status === "complete",
    );
    const completedOutcomes = phaseComplete
      ? phaseMilestones.length
      : phaseMilestones.filter((milestone) => milestone.status === "completed")
          .length;
    const activeOutcomes = phaseMilestones.filter((milestone) =>
      ["in_progress", "blocked"].includes(milestone.status),
    ).length;
    return {
      plan: candidate,
      task: runtimeTask,
      milestones: phaseMilestones,
      completed: phaseComplete,
      completedOutcomes,
      activeOutcomes,
    };
  });
  const completedPhases = phases.filter((phase) => phase.completed).length;
  const totalOutcomes = phases.reduce(
    (total, phase) => total + phase.milestones.length,
    0,
  );
  const completedOutcomes = phases.reduce(
    (total, phase) => total + phase.completedOutcomes,
    0,
  );
  const selectedPhase = phases.find(
    (phase) => phase.plan.task_id === task.external_task_id,
  );
  const selectedPhaseNumber = Math.max(
    1,
    phases.findIndex((phase) => phase === selectedPhase) + 1,
  );
  return (
    <InspectorCard label="Plan progress">
      <div className="plan-progress-summary">
        <strong>
          {completedPhases} of {phases.length || "—"} phases completed
        </strong>
        <span>
          {completedOutcomes} of {totalOutcomes || "—"} outcomes completed
        </span>
      </div>
      {phases.length > 1 && (
        <div className="plan-phase-list">
          {phases.map((phase, index) => {
            const selected = phase.plan.task_id === task.external_task_id;
            const status = phase.completed
              ? "COMPLETED"
              : phase.activeOutcomes
                ? "IN PROGRESS"
                : phase.task
                  ? humanTaskState(phase.task.state).toUpperCase()
                  : "PENDING";
            return (
              <div
                className={`plan-phase ${selected ? "selected" : ""}`}
                key={phase.plan.task_id}
              >
                <span>{index + 1}</span>
                <strong>{phase.plan.title}</strong>
                <StatusBadge value={status} />
              </div>
            );
          })}
        </div>
      )}
      <div className="selected-phase-summary">
        <span>
          Selected phase {selectedPhaseNumber} of {phases.length || 1}
        </span>
        <strong>{planTask?.title || task.title}</strong>
        <small>
          {completed} of {milestones.length || "—"} outcomes completed ·{" "}
          {active
            ? `${active} active outcome${active === 1 ? "" : "s"}`
            : "no active outcome"}
        </small>
      </div>
      <div className="inspector-plan-scroll">
        {milestones.length ? (
          <ol className="inspector-milestones">
            {milestones.map((milestone) => (
              <li
                className={`inspector-milestone ${milestone.status}`}
                key={milestone.id}
              >
                <StatusBadge value={milestone.status} />
                <div>
                  <strong>{milestone.title}</strong>
                  {milestone.outcome !== milestone.title && (
                    <span>{milestone.outcome}</span>
                  )}
                </div>
              </li>
            ))}
          </ol>
        ) : (
          <div className="plan-progress-empty">
            The governor will publish its bounded milestones when work begins.
          </div>
        )}
      </div>
      {assignedAgents.length > 0 && (
        <div className="plan-assignments">
          <div className="inspector-label">Agents on this work</div>
          {assignedAgents.map((assigned) => {
            const working = Boolean(
              assigned.active_turn_id && activeAgentState(assigned.state),
            );
            return (
              <div className="plan-assignment" key={assigned.id}>
                <StateIcon state={assigned.state} working={working} />
                <div>
                  <strong>
                    {assigned.id === governor?.id
                      ? "Governor"
                      : childDisplayName(assigned)}
                  </strong>
                  <span>
                    {assigned.current_action ||
                      assigned.current_goal ||
                      "Waiting for the next assignment"}
                  </span>
                </div>
                <StatusBadge
                  value={working ? "WORKING" : humanAgentState(assigned.state)}
                />
              </div>
            );
          })}
        </div>
      )}
    </InspectorCard>
  );
}

function WorkStatusCard({
  task,
  worktree,
  diff,
  run,
}: {
  task?: Task;
  worktree?: Worktree;
  diff?: WorktreeDiffSummary;
  run: Run;
}) {
  const status = workStatusSummary(task, worktree, diff);
  const prScope = pullRequestScope(run, task);
  const files = diff?.files_changed ?? worktree?.files_changed ?? 0;
  const additions = diff?.additions ?? worktree?.additions ?? 0;
  const deletions = diff?.deletions ?? worktree?.deletions ?? 0;
  const remote =
    run.state === "DRAFT_PR_CREATED"
      ? "Draft pull request created"
      : run.state === "PUBLICATION_READY" &&
          run.publication_mode === "draft_pr_after_approval"
        ? "Ready to create a draft pull request"
        : prScope === "No pull request linked"
          ? "No remote pull request action recorded"
          : "Remote merge state is not yet confirmed by Harness";
  return (
    <InspectorCard label="Work status">
      <div className="work-status-head">
        <StatusBadge value={status.label} />
        <span>{prScope}</span>
      </div>
      <div className="work-diff">
        <div>
          <strong>{files}</strong>
          <span>files changed</span>
        </div>
        <div className="success">
          <strong>+{additions}</strong>
          <span>added</span>
        </div>
        <div className="danger">
          <strong>−{deletions}</strong>
          <span>removed</span>
        </div>
      </div>
      <p>{status.detail}</p>
      <small>{remote}</small>
      {files > 0 && (
        <details className="work-files">
          <summary>Changed files ({files})</summary>
          <div>
            {(diff?.changed_paths || []).map((path) => (
              <code key={path}>{path}</code>
            ))}
          </div>
          {diff?.changed_paths_truncated && (
            <small>Showing the first 200 changed files.</small>
          )}
        </details>
      )}
      {worktree?.preserved_reason && (
        <small>{humanizeWorktreeReason(worktree.preserved_reason)}</small>
      )}
    </InspectorCard>
  );
}

function MessageHistoryPreview({
  label,
  messages,
  onOpen,
}: {
  label: string;
  messages: LatestAgentMessage[];
  onOpen: () => void;
}) {
  const { scrollRef, onScroll } = useLatestMessageScroll(messages.length);
  return (
    <section className="inspector-card message-history-card">
      <div
        role="button"
        tabIndex={0}
        className="message-history-open"
        onClick={onOpen}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onOpen();
          }
        }}
        aria-label={`Open ${label.toLowerCase()}`}
      >
        <span className="inspector-label">{label}</span>
        <span className="message-open-hint">Open full history ↗</span>
        <div
          className="message-history-preview"
          ref={scrollRef}
          onScroll={onScroll}
        >
          {messages.length ? (
            messages.map((item) => (
              <MessageEntry message={item} compact key={item.id} />
            ))
          ) : (
            <div className="message-history-empty">
              No completed messages yet.
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

function MessageHistoryModal({
  title,
  messages,
  onClose,
}: {
  title: string;
  messages: LatestAgentMessage[];
  onClose: () => void;
}) {
  const { scrollRef, onScroll } = useLatestMessageScroll(messages.length);
  return (
    <ModalFrame
      title={title}
      eyebrow="Timestamped local scrollback"
      onClose={onClose}
      wide
    >
      <div className="message-review" ref={scrollRef} onScroll={onScroll}>
        {messages.length ? (
          messages.map((item) => <MessageEntry message={item} key={item.id} />)
        ) : (
          <EmptyInspector
            icon={<Bot />}
            title="No messages yet"
            text="Governor commentary and final updates will appear here as they land."
          />
        )}
      </div>
    </ModalFrame>
  );
}

function useLatestMessageScroll(messageCount: number) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const readingUntilRef = useRef(0);
  const initializedRef = useRef(false);

  useEffect(() => {
    const element = scrollRef.current;
    if (!element || messageCount === 0) return;
    if (
      initializedRef.current &&
      Date.now() < readingUntilRef.current
    ) {
      return;
    }
    const frame = window.requestAnimationFrame(() => {
      element.scrollTop = element.scrollHeight;
      initializedRef.current = true;
      readingUntilRef.current = 0;
    });
    return () => window.cancelAnimationFrame(frame);
  }, [messageCount]);

  const onScroll = useCallback(() => {
    const element = scrollRef.current;
    if (!element || !initializedRef.current) return;
    const distanceFromBottom =
      element.scrollHeight - element.scrollTop - element.clientHeight;
    readingUntilRef.current =
      distanceFromBottom > 24
        ? Date.now() + MESSAGE_SCROLL_READING_GRACE_MS
        : 0;
  }, []);

  return { scrollRef, onScroll };
}

function MessageEntry({
  message,
  compact = false,
}: {
  message: LatestAgentMessage;
  compact?: boolean;
}) {
  const checkpoint = parseGovernorCheckpoint(message.text);
  return (
    <article className={`message-entry ${compact ? "compact" : ""}`}>
      <header>
        <span>{message.phase === "final_answer" ? "Final" : "Update"}</span>
        <time
          dateTime={message.occurred_at}
          title={formatLocalTimestamp(message.occurred_at)}
        >
          {formatLocalTimestamp(message.occurred_at)}
        </time>
      </header>
      <div>{checkpoint?.operator_update || message.text}</div>
      {checkpoint?.next_action && (
        <small className="message-next-action">
          Next: {checkpoint.next_action}
        </small>
      )}
    </article>
  );
}

function UsageView({
  breakdown,
  accounts,
}: {
  breakdown?: UsageBreakdown;
  accounts: CodexAccountsSnapshot;
}) {
  const usage = breakdown?.total;
  const accountLabels = new Map(
    accounts.accounts.map((account) => [
      account.id,
      account.email || account.label,
    ]),
  );
  const accountGroups =
    breakdown?.by_account.map((group) => ({
      ...group,
      label: accountLabels.get(group.id) || group.label,
    })) || [];
  return (
    <div className="page usage-page">
      <PageTitle
        eyebrow="Where the work goes"
        title="Usage"
        description="Token and time spend grouped by account, repository, and agent. API cost is an equivalent estimate, not an invoice."
      />
      <div className="metrics usage-metrics">
        <Metric
          label="Total tokens"
          value={formatTokens(usage?.total_tokens || 0)}
          note="all model traffic"
        />
        <Metric
          label="Input"
          value={formatTokens(usage?.input_tokens || 0)}
          note={`${formatTokens(usage?.cached_input_tokens || 0)} cached`}
        />
        <Metric
          label="Output"
          value={formatTokens(usage?.output_tokens || 0)}
          note={`${formatTokens(usage?.reasoning_output_tokens || 0)} reasoning included`}
        />
        <Metric
          label="Cache writes"
          value={
            usage?.cache_write_input_tokens === undefined
              ? "Not reported"
              : formatTokens(usage.cache_write_input_tokens)
          }
          note={
            usage?.cache_write_input_tokens === undefined
              ? "cost shown as a range"
              : "priced separately"
          }
        />
        <Metric
          label="Turns"
          value={String(
            (breakdown?.by_agent || []).reduce(
              (sum, row) => sum + row.turns,
              0,
            ),
          )}
          note={`${breakdown?.by_agent.length || 0} agents`}
        />
        <Metric
          label="API-equivalent cost"
          value={formatCost(usage?.cost.upper_microusd || 0)}
          note={usage?.cost.confidence || "unknown confidence"}
        />
      </div>
      <div className="usage-breakdowns">
        <UsageGroupList title="By account" groups={accountGroups} />
        <UsageGroupList
          title="By repository"
          groups={breakdown?.by_repository || []}
        />
        <UsageGroupList title="By agent" groups={breakdown?.by_agent || []} />
      </div>
    </div>
  );
}

function UsageGroupList({
  title,
  groups,
}: {
  title: string;
  groups: UsageBreakdown["by_account"];
}) {
  return (
    <section className="usage-group">
      <div className="section-header">
        <span>{title}</span>
        <i>{groups.length}</i>
      </div>
      {groups.length ? (
        groups.map((group) => (
          <div className="usage-group-row" key={group.id}>
            <div>
              <strong>{group.label}</strong>
              <span>
                {group.detail} · {group.turns} turns
              </span>
            </div>
            <div>
              <strong>{formatTokens(group.usage.total_tokens)}</strong>
              <span>{formatCost(group.cost.upper_microusd)}</span>
            </div>
          </div>
        ))
      ) : (
        <div className="usage-empty">Usage appears after completed turns.</div>
      )}
    </section>
  );
}

function HostView({
  runtime,
  repositories,
}: {
  runtime?: RuntimeStatus;
  repositories: Repository[];
}) {
  return (
    <div className="page">
      <PageTitle
        eyebrow="Runtime health"
        title="Host and App Server"
        description="Runs stay disabled unless the installed Codex version and app protocol are compatible."
      />
      <div className="health-grid large">
        <HealthCard
          icon={<Bot />}
          label="Codex App Server"
          state={runtime?.codex.state || "unknown"}
          detail={runtime?.codex.detail || "Not connected"}
        />
        <HealthCard
          icon={<Database />}
          label="Local history"
          state={runtime?.database.state || "unknown"}
          detail={runtime?.database.detail || "Not checked"}
        />
        <HealthCard
          icon={<Gauge />}
          label="Scheduler"
          state={runtime?.scheduler.paused ? "paused" : "ready"}
          detail={`${runtime?.scheduler.active_mutable || 0}/${runtime?.scheduler.max_mutable || 0} mutable · ${runtime?.scheduler.active_verifiers || 0}/${runtime?.scheduler.max_verifiers || 0} verifier`}
        />
      </div>
      <div className="detail-grid">
        <InspectorCard label="Compatibility">
          <dl className="key-values">
            <dt>Installed</dt>
            <dd>{runtime?.codex.version || "unavailable"}</dd>
            <dt>Required</dt>
            <dd>{runtime?.codex.required_version || "not pinned"}</dd>
            <dt>Schema</dt>
            <dd>
              {runtime?.codex.schema_match
                ? "exact match"
                : "mismatch / unavailable"}
            </dd>
            <dt>Native collaboration</dt>
            <dd>
              {runtime?.codex.native_multi_agent
                ? `ready · ${runtime.codex.native_multi_agent_feature || "multi-agent"}`
                : "disabled / unavailable"}
            </dd>
          </dl>
        </InspectorCard>
        <InspectorCard label="Repository safety">
          <dl className="key-values">
            <dt>Registered</dt>
            <dd>{repositories.length}</dd>
            <dt>Clean primaries</dt>
            <dd>{repositories.filter((item) => item.primary_clean).length}</dd>
            <dt>Blocked</dt>
            <dd>
              {repositories.filter((item) => item.blockers.length).length}
            </dd>
            <dt>External writes</dt>
            <dd>disabled</dd>
          </dl>
        </InspectorCard>
      </div>
    </div>
  );
}

export function SettingsView({
  light,
  accounts,
  onAccounts,
  onSettings,
  onRefresh,
  onAddAccount,
  onReauthenticate,
  onTheme,
}: {
  light: boolean;
  accounts: CodexAccountsSnapshot;
  onAccounts: (snapshot: CodexAccountsSnapshot) => void;
  onSettings: (settings: OperatorSettings) => void;
  onRefresh: () => Promise<void>;
  onAddAccount: () => void;
  onReauthenticate: (accountId: string) => void;
  onTheme: () => void;
}) {
  const [settings, setSettings] = useState<OperatorSettings>();
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const settingsUpdateLocked = !settings || busy !== "";
  useEffect(() => {
    api
      .settings()
      .then(setSettings)
      .catch((caught) => setError(message(caught)));
  }, []);
  const update = async (
    key:
      | "store_reasoning_summaries"
      | "store_raw_reasoning"
      | "yolo_mode"
      | "automatic_account_handoff"
      | "adaptive_governor_budgets"
      | "automatic_governor_continuation"
      | "automatic_plan_approval"
      | "supervision_enabled"
      | "governor_goal_token_budget"
      | "governor_attempt_token_ceiling",
    value: boolean | number,
  ) => {
    setBusy(key);
    setError("");
    try {
      const next = await api.updateSettings({ [key]: value });
      setSettings(next);
      onSettings(next);
      await onRefresh();
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy("");
    }
  };
  return (
    <div className="page narrow-page">
      <PageTitle
        eyebrow="Local preferences"
        title="Settings"
        description="Preferences are stored on this computer. Publishing and other external changes still require approval."
      />
      {error && (
        <div className="notice error" role="alert">
          <AlertTriangle size={15} />
          <span>{error}</span>
        </div>
      )}
      <div className="settings-card">
        <div>
          <strong>Appearance</strong>
          <span>Choose a high-contrast dark or light workspace.</span>
        </div>
        <button className="button" onClick={onTheme}>
          {light ? <Moon size={14} /> : <Sun size={14} />}
          {light ? "Dark theme" : "Light theme"}
        </button>
      </div>
      <div className="settings-section-title">Supervision</div>
      <SettingToggle
        title="Human-approved thread supervision"
        text="On lets Terra analyze durable material blockers in a read-only thread. Harness shows its diagnosis and recovery choices, but never resumes, retries, replans, edits, approves proof, or takes any recommendation automatically."
        enabled={settings?.supervision_enabled ?? false}
        disabled={settingsUpdateLocked}
        onChange={(value) => update("supervision_enabled", value)}
      />
      <div className="settings-section-title">
        Planning and governor autonomy
      </div>
      <SettingToggle
        title="Automatically approve certified plans"
        text="Every plan first passes independent adversarial review. When off, Harness pauses after certification; when on, a zero-finding certified digest begins implementation automatically."
        enabled={settings?.automatic_plan_approval ?? false}
        disabled={settingsUpdateLocked}
        onChange={(value) => update("automatic_plan_approval", value)}
      />
      <SettingToggle
        title="Automatic governor continuation"
        text="Roll productive, incomplete governor checkpoints into a fresh bounded attempt without asking you to babysit each turn."
        enabled={settings?.automatic_governor_continuation ?? true}
        disabled={settingsUpdateLocked}
        onChange={(value) => update("automatic_governor_continuation", value)}
      />
      <SettingToggle
        title="Automatic account handoff"
        text="Between attempts, move new work to a ready Codex account when the selected account has 10% capacity or less. Active threads are never transplanted."
        enabled={settings?.automatic_account_handoff ?? true}
        disabled={settingsUpdateLocked}
        onChange={(value) => update("automatic_account_handoff", value)}
      />
      <SettingToggle
        title="Adaptive attempt budgets"
        text="Use successful governor history to choose the next attempt budget while respecting your hard ceiling."
        enabled={settings?.adaptive_governor_budgets ?? true}
        disabled={settingsUpdateLocked}
        onChange={(value) => update("adaptive_governor_budgets", value)}
      />
      <div className="settings-card autonomy-settings">
        <div>
          <strong>Governor token budgets</strong>
          <span>
            {settings?.governor_budget_reason ||
              "Loading the local governor history…"}
          </span>
          <small>
            {settings
              ? `${settings.governor_budget_sample_count} usable samples · next attempt ${formatTokens(settings.recommended_governor_attempt_tokens)}`
              : "No recommendation loaded"}
          </small>
        </div>
        <div className="setting-selects">
          <label>
            <span>Goal budget</span>
            <select
              value={settings?.governor_goal_token_budget || 5_000_000}
              disabled={settingsUpdateLocked}
              onChange={(event) =>
                update("governor_goal_token_budget", Number(event.target.value))
              }
            >
              <option value={2_000_000}>2M tokens</option>
              <option value={5_000_000}>5M tokens</option>
              <option value={10_000_000}>10M tokens</option>
              <option value={25_000_000}>25M tokens</option>
              <option value={50_000_000}>50M tokens</option>
              <option value={100_000_000}>100M tokens</option>
              <option value={250_000_000}>250M tokens</option>
              <option value={500_000_000}>500M tokens</option>
              <option value={1_000_000_000}>1B tokens</option>
            </select>
          </label>
          <label>
            <span>Per-attempt limit</span>
            <select
              value={settings?.governor_attempt_token_ceiling || 1_000_000}
              disabled={settingsUpdateLocked}
              onChange={(event) =>
                update(
                  "governor_attempt_token_ceiling",
                  Number(event.target.value),
                )
              }
            >
              <option value={400_000}>400k tokens</option>
              <option value={650_000}>650k tokens</option>
              <option value={1_000_000}>1M tokens</option>
              <option value={1_500_000}>1.5M tokens</option>
              <option value={5_000_000}>5M tokens</option>
              <option value={10_000_000}>10M tokens</option>
              <option value={25_000_000}>25M tokens</option>
              <option value={50_000_000}>50M tokens</option>
              <option value={100_000_000}>100M tokens</option>
            </select>
          </label>
        </div>
      </div>
      <div className="settings-section-title">Codex accounts</div>
      <AccountSettings
        accounts={accounts}
        onAccounts={onAccounts}
        onAdd={onAddAccount}
        onReauthenticate={onReauthenticate}
      />
      <div className="settings-section-title">Privacy and permissions</div>
      <SettingToggle
        title="Reasoning summaries"
        text="Retain concise reasoning summaries in the local event journal."
        enabled={settings?.store_reasoning_summaries ?? true}
        disabled={settingsUpdateLocked}
        onChange={(value) => update("store_reasoning_summaries", value)}
      />
      <SettingToggle
        title="Raw reasoning events"
        text="Retain raw reasoning payloads locally. Leave this off unless you need protocol-level diagnostics."
        enabled={settings?.store_raw_reasoning ?? false}
        disabled={settingsUpdateLocked}
        warning
        onChange={(value) => update("store_raw_reasoning", value)}
      />
      <SettingToggle
        title="YOLO mode for managed worktrees"
        text="New writable Codex threads run without per-command or per-file prompts. Path custody and the managed-worktree boundary still apply."
        enabled={settings?.yolo_mode ?? false}
        disabled={settingsUpdateLocked}
        warning
        onChange={(value) => update("yolo_mode", value)}
      />
      <SettingToggle
        title="Automatic external writes"
        text="Push, PR publication, readiness changes, and merge remain separate explicit product actions."
        enabled={settings?.allow_automatic_external_writes ?? false}
        disabled
        locked
      />
      <div className="settings-card">
        <div>
          <strong>Keyboard navigation</strong>
          <span>
            <kbd>G</kbd> <kbd>H</kbd> home · <kbd>G</kbd> <kbd>R</kbd> runs ·{" "}
            <kbd>⌘</kbd> <kbd>K</kbd> commands
          </span>
        </div>
      </div>
    </div>
  );
}

function SettingToggle({
  title,
  text,
  enabled,
  disabled,
  locked = false,
  warning = false,
  onChange,
}: {
  title: string;
  text: string;
  enabled: boolean;
  disabled?: boolean;
  locked?: boolean;
  warning?: boolean;
  onChange?: (value: boolean) => void;
}) {
  const description =
    title === "Automatic governor continuation"
      ? "Continue productive checkpoints as bounded turns on the same governor thread and worktree; fall back to a durable handoff only when warm reuse is unsafe or unavailable."
      : text;
  return (
    <div
      className={`settings-card ${warning && enabled ? "warning-setting" : ""}`}
    >
      <div>
        <strong>{title}</strong>
        <span>{description}</span>
        {locked && <small>Required safety setting.</small>}
      </div>
      <button
        type="button"
        role="switch"
        aria-checked={enabled}
        className={`toggle-switch ${enabled ? "enabled" : ""}`}
        disabled={disabled || locked}
        onClick={() => onChange?.(!enabled)}
      >
        <i />
        <span>{locked ? "Locked" : enabled ? "On" : "Off"}</span>
      </button>
    </div>
  );
}

function AccountSettings({
  accounts,
  onAccounts,
  onAdd,
  onReauthenticate,
}: {
  accounts: CodexAccountsSnapshot;
  onAccounts: (snapshot: CodexAccountsSnapshot) => void;
  onAdd: () => void;
  onReauthenticate: (accountId: string) => void;
}) {
  return (
    <div className="account-settings">
      <div className="account-settings-intro">
        <div>
          <strong>Signed-in Codex accounts</strong>
          <span>
            Harness can use managed or detected Codex homes. Account changes
            happen only between active turns.
          </span>
        </div>
        <button className="button primary" onClick={onAdd}>
          <Plus size={13} /> Add account
        </button>
      </div>
      {accounts.accounts.map((account) => (
        <AccountSettingsRow
          key={account.id}
          account={account}
          selected={accounts.selected_account_id === account.id}
          onAccounts={onAccounts}
          onReauthenticate={() => onReauthenticate(account.id)}
        />
      ))}
      {!accounts.accounts.length && (
        <div className="account-settings-empty">
          No Codex account is available. Add one to sign in with ChatGPT.
        </div>
      )}
    </div>
  );
}

function AccountSettingsRow({
  account,
  selected,
  onAccounts,
  onReauthenticate,
}: {
  account: CodexAccountProfile;
  selected: boolean;
  onAccounts: (snapshot: CodexAccountsSnapshot) => void;
  onReauthenticate: () => void;
}) {
  const [label, setLabel] = useState(account.label);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  useEffect(() => setLabel(account.label), [account.label]);
  const rename = async () => {
    setBusy("rename");
    setError("");
    try {
      onAccounts(await api.renameCodexAccount(account.id, label.trim()));
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy("");
    }
  };
  const remove = async () => {
    if (
      !window.confirm(
        `Remove ${account.label} from BILDR? Its managed local Codex credentials will be deleted.`,
      )
    )
      return;
    setBusy("remove");
    setError("");
    try {
      onAccounts(await api.removeCodexAccount(account.id));
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy("");
    }
  };
  return (
    <div className="account-settings-row">
      <div className="account-settings-main">
        <input
          aria-label={`Name for ${account.email || account.label}`}
          value={label}
          onChange={(event) => setLabel(event.target.value)}
          maxLength={60}
        />
        <span>
          {account.email || "Email not exposed"} ·{" "}
          {account.plan_type || account.state}
          {selected ? " · currently selected" : ""}
        </span>
        <small>
          {account.managed
            ? "Managed by BILDR"
            : "Detected Codex home"}
        </small>
        {error && <small className="danger">{error}</small>}
      </div>
      <div className="account-settings-actions">
        <button
          className="button subtle"
          onClick={rename}
          disabled={!label.trim() || label === account.label || !!busy}
        >
          Save name
        </button>
        {account.managed && (
          <button
            className="button subtle"
            onClick={onReauthenticate}
            disabled={!!busy}
          >
            Re-authenticate
          </button>
        )}
        {account.managed && (
          <button
            className="button subtle danger-hover"
            onClick={remove}
            disabled={selected || !!busy}
            title={
              selected
                ? "Select another account before removing this one"
                : "Remove managed account"
            }
          >
            Remove
          </button>
        )}
      </div>
    </div>
  );
}

function AccountLoginModal({
  account,
  onClose,
  onDone,
}: {
  account?: CodexAccountProfile;
  onClose: () => void;
  onDone: () => Promise<void>;
}) {
  const [label, setLabel] = useState(account?.label || "");
  const [status, setStatus] = useState<CodexAccountLoginStatus>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!status || status.state !== "waiting_for_user") return;
    const timer = window.setInterval(() => {
      api
        .codexAccountLoginStatus(status.id)
        .then(setStatus)
        .catch((caught) => setError(message(caught)));
    }, 1_000);
    return () => window.clearInterval(timer);
  }, [status?.id, status?.state]);
  useEffect(() => {
    if (status?.state === "completed") void onDone();
  }, [status?.state]);
  const start = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      setStatus(await api.startCodexAccountLogin(label.trim(), account?.id));
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy(false);
    }
  };
  const close = () => {
    if (status && ["waiting_for_user", "canceling"].includes(status.state)) {
      void api.cancelCodexAccountLogin(status.id).catch(() => undefined);
    }
    onClose();
  };
  return (
    <ModalFrame
      title={account ? "Re-authenticate account" : "Add Codex account"}
      eyebrow="Sign in with ChatGPT"
      onClose={close}
      wide
    >
      <form onSubmit={start}>
        {!status && (
          <>
            <label className="field">
              <span>Account name</span>
              <input
                autoFocus
                value={label}
                onChange={(event) => setLabel(event.target.value)}
                placeholder="Work, Personal, Backup…"
                maxLength={60}
                required
              />
              <small>
                This friendly name is stored only in BILDR.
              </small>
            </label>
            <div className="pin-note">
              <ShieldCheck size={15} />
              <span>
                Codex 0.147 opens OpenAI's device authorization. Credentials
                stay in a private Harness-managed Codex home.
              </span>
            </div>
          </>
        )}
        {status && (
          <div className={`device-login-state tone-${tone(status.state)}`}>
            <strong>
              {status.state === "waiting_for_user"
                ? "Finish signing in"
                : roleLabel(status.state)}
            </strong>
            <p>{status.detail}</p>
            {status.verification_url && status.user_code && (
              <div className="device-login-instructions">
                <a
                  className="button primary"
                  href={status.verification_url}
                  target="_blank"
                  rel="noreferrer"
                >
                  Open OpenAI sign-in
                </a>
                <button
                  type="button"
                  className="device-code"
                  onClick={() =>
                    navigator.clipboard?.writeText(status.user_code || "")
                  }
                  title="Copy device code"
                >
                  <span>One-time code</span>
                  <strong>{status.user_code}</strong>
                </button>
              </div>
            )}
          </div>
        )}
        {error && <div className="form-error">{error}</div>}
        <div className="modal-actions">
          <button type="button" className="button" onClick={close}>
            {status?.state === "waiting_for_user" ? "Cancel sign-in" : "Close"}
          </button>
          {!status && (
            <button className="button primary" disabled={!label.trim() || busy}>
              {busy ? "Starting sign-in…" : "Continue to OpenAI"}
            </button>
          )}
        </div>
      </form>
    </ModalFrame>
  );
}

function RegisterModal({
  onClose,
  onDone,
}: {
  onClose: () => void;
  onDone: () => Promise<void>;
}) {
  const [path, setPath] = useState("");
  const [query, setQuery] = useState("");
  const [discoveries, setDiscoveries] = useState<RepositoryDiscovery[]>([]);
  const [scanning, setScanning] = useState(true);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const scan = useCallback(async () => {
    setScanning(true);
    setError("");
    try {
      const found = await api.discoverRepositories();
      setDiscoveries(found);
      const suggested =
        found.find((item) => item.compatible && !item.registered) ||
        found.find((item) => !item.registered);
      setPath((current) => current || suggested?.root_path || "");
    } catch (caught) {
      setError(message(caught));
    } finally {
      setScanning(false);
    }
  }, []);
  useEffect(() => {
    scan();
  }, [scan]);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      await api.registerRepository(path);
      await onDone();
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy(false);
    }
  };
  const normalizedQuery = query.trim().toLowerCase();
  const visibleDiscoveries = normalizedQuery
    ? discoveries.filter((item) =>
        `${item.display_name} ${item.root_path} ${item.origin_url || ""}`
          .toLowerCase()
          .includes(normalizedQuery),
      )
    : discoveries;
  return (
    <ModalFrame
      title="Register a repository"
      eyebrow="Local Git checkout"
      onClose={onClose}
      wide
    >
      <form onSubmit={submit}>
        <div className="discovery-header">
          <div>
            <strong>Discovered checkouts</strong>
            <span>
              {discoveries.length
                ? `${discoveries.length} local Git checkouts found`
                : "Choose a GitHub checkout or enter a local path."}
            </span>
          </div>
          <button
            type="button"
            className="button subtle"
            onClick={scan}
            disabled={scanning}
          >
            <RefreshCw size={13} className={scanning ? "spin" : ""} />
            {scanning ? "Scanning…" : "Scan again"}
          </button>
        </div>
        {discoveries.length > 8 && (
          <label className="discovery-search">
            <Search size={14} />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Filter by repository, path, or origin…"
              autoFocus
            />
          </label>
        )}
        <div className="discovery-list">
          {scanning && !discoveries.length ? (
            <div className="discovery-empty">
              Looking for local Git checkouts…
            </div>
          ) : visibleDiscoveries.length ? (
            visibleDiscoveries.map((item) => (
              <button
                type="button"
                className={`discovery-row ${path === item.root_path ? "selected" : ""}`}
                key={item.root_path}
                onClick={() => setPath(item.root_path)}
                disabled={item.registered}
              >
                <FolderGit2 size={16} />
                <span>
                  <strong>{item.display_name}</strong>
                  <small className="mono">{item.root_path}</small>
                  <small>{item.origin_url || "No origin remote"}</small>
                </span>
                <span className="discovery-badges">
                  {item.compatible && <em>Ready</em>}
                  {item.is_github && <em>GitHub</em>}
                  {item.registered && <em>Registered</em>}
                </span>
              </button>
            ))
          ) : (
            <div className="discovery-empty">
              {normalizedQuery
                ? "No checkout matches this filter."
                : "No checkouts found in common folders. Enter a path below."}
            </div>
          )}
        </div>
        <label className="field">
          <span>Repository root</span>
          <input
            value={path}
            onChange={(event) => setPath(event.target.value)}
            placeholder="/home/you/Documents/project"
            required
          />
          <small>
            Harness checks the active branch, origin, Git identity, cleanliness,
            and repository instructions.
          </small>
        </label>
        {error && <div className="form-error">{error}</div>}
        <div className="modal-actions">
          <button type="button" className="button" onClick={onClose}>
            Cancel
          </button>
          <button className="button primary" disabled={!path.trim() || busy}>
            {busy ? "Inspecting…" : "Register repository"}
          </button>
        </div>
      </form>
    </ModalFrame>
  );
}

function PrepareCheckoutModal({
  repository,
  onClose,
  onDone,
}: {
  repository?: Repository;
  onClose: () => void;
  onDone: () => Promise<void>;
}) {
  const [destination, setDestination] = useState(() =>
    suggestedCoordinationPath(repository?.root_path),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!repository) return;
    setBusy(true);
    setError("");
    try {
      await api.prepareCoordinationCheckout(repository.id, destination);
      await onDone();
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy(false);
    }
  };
  return (
    <ModalFrame
      title="Create clean coordination checkout"
      eyebrow="Non-destructive onboarding"
      onClose={onClose}
      wide
    >
      <form onSubmit={submit}>
        <div className="checkout-source">
          <FolderGit2 size={17} />
          <div>
            <strong>Source checkout stays untouched</strong>
            <span className="mono">
              {repository?.root_path || "Repository unavailable"}
            </span>
            <small>
              {repository?.blockers.join(" · ") || "No blocker recorded"}
            </small>
          </div>
        </div>
        <label className="field">
          <span>New checkout directory</span>
          <input
            autoFocus
            value={destination}
            onChange={(event) => setDestination(event.target.value)}
            placeholder="/home/you/Documents/project-Harness"
            required
          />
          <small>
            The directory must not already exist. Harness clones{" "}
            <b>{repository?.default_branch || "the active branch"}</b>,
            preserves the origin, verifies cleanliness, then uses the new
            checkout for coordination.
          </small>
        </label>
        <div className="pin-note">
          <ShieldCheck size={15} />
          <span>
            The source repository supplies existing Git objects to avoid a
            second full download. Keep the source checkout in place; its
            untracked files are never copied or changed.
          </span>
        </div>
        {error && <div className="form-error">{error}</div>}
        <div className="modal-actions">
          <button type="button" className="button" onClick={onClose}>
            Cancel
          </button>
          <button
            className="button primary"
            disabled={!repository || !destination.trim() || busy}
          >
            {busy ? "Cloning and verifying…" : "Clone, verify, and use"}
          </button>
        </div>
      </form>
    </ModalFrame>
  );
}

function suggestedCoordinationPath(root?: string) {
  if (!root) return "";
  const normalized = root.replace(/\/+$/, "");
  const separator = normalized.lastIndexOf("/");
  const parent = separator > 0 ? normalized.slice(0, separator) : normalized;
  const name = separator >= 0 ? normalized.slice(separator + 1) : "repository";
  return `${parent}/${name}-Harness`;
}

function NewRunModal({
  repositories,
  accounts,
  settings,
  onClose,
  onDone,
}: {
  repositories: Repository[];
  accounts: CodexAccountsSnapshot;
  settings?: OperatorSettings;
  onClose: () => void;
  onDone: (run: Run, startError?: string) => Promise<void>;
}) {
  const [repository, setRepository] = useState(repositories[0]?.id || "");
  const [objective, setObjective] = useState("");
  const [automaticPlanApproval, setAutomaticPlanApproval] = useState(false);
  const [deepInterview, setDeepInterview] = useState(false);
  const [codexAccountId, setCodexAccountId] = useState("");
  const [publication, setPublication] = useState("local_only");
  const [governorModel, setGovernorModel] = useState("gpt-5.6-sol");
  const [governorEffort, setGovernorEffort] = useState("xhigh");
  const [runTokenBudget, setRunTokenBudget] = useState(
    settings?.governor_goal_token_budget || 5_000_000,
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(
    () =>
      setAutomaticPlanApproval(settings?.automatic_plan_approval || false),
    [settings?.automatic_plan_approval],
  );
  const selectedRepository = repositories.find(
    (item) => item.id === repository,
  );
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const run = await api.createRun(
        repository,
        objective,
        publication,
        governorModel,
        governorEffort,
        automaticPlanApproval,
        runTokenBudget,
        deepInterview,
        codexAccountId || undefined,
      );
      try {
        if (run.state === "INTERVIEWING") {
          await api.startIntentInterview(run.id);
        } else {
          await api.startArchitecture(run.id);
        }
        await onDone(run);
      } catch (caught) {
        await onDone(run, message(caught));
      }
    } catch (caught) {
      setError(message(caught));
    } finally {
      setBusy(false);
    }
  };
  return (
    <ModalFrame
      title="New task"
      eyebrow="Goal and oversight"
      onClose={onClose}
      wide
    >
      <form onSubmit={submit}>
        <label className="field">
          <span>What should the governor accomplish?</span>
          <textarea
            autoFocus
            rows={6}
            value={objective}
            onChange={(event) => setObjective(event.target.value)}
            placeholder="Describe the outcome, important constraints, and what must remain unchanged."
            required
          />
        </label>
        <label className={`interview-option ${deepInterview ? "selected" : ""}`}>
          <input
            type="checkbox"
            checked={deepInterview}
            onChange={(event) => setDeepInterview(event.target.checked)}
          />
          <span>
            <strong>Deep interview before planning</strong>
            <small>
              Optional · clarify the intended final shape one material decision
              at a time, then confirm a concise brief before the architect starts.
            </small>
          </span>
        </label>
        <div className="form-grid">
          <label className="field">
            <span>Repository</span>
            <select
              value={repository}
              onChange={(event) => setRepository(event.target.value)}
            >
              {repositories.map((item) => (
                <option value={item.id} key={item.id}>
                  {item.display_name} · {item.health}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Base</span>
            <input
              value={`origin/${selectedRepository?.default_branch || "main"}`}
              readOnly
            />
          </label>
        </div>
        <BudgetControl
          label="Total run ceiling"
          value={runTokenBudget}
          valueLabel={`${formatTokens(runTokenBudget)} tokens`}
          options={RUN_BUDGET_OPTIONS}
          onChange={setRunTokenBudget}
          hint={`This is the aggregate ceiling for planning, governor work, children, and review. Harness automatically opens bounded governor turns and retries productive work within it; the current turn recommendation is ${formatTokens(settings?.recommended_governor_attempt_tokens || 650_000)} tokens.`}
        />
        <div className="form-grid">
          <label className="field">
            <span>Governor model</span>
            <select
              value={governorModel}
              onChange={(event) => setGovernorModel(event.target.value)}
            >
              <option value="gpt-5.6-sol">SOL · strongest governor</option>
              <option value="gpt-5.6-terra">Terra · balanced</option>
              <option value="gpt-5.6-luna">Luna · economical</option>
            </select>
          </label>
          <label className="field">
            <span>Thinking level</span>
            <select
              value={governorEffort}
              onChange={(event) => setGovernorEffort(event.target.value)}
            >
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
              <option value="xhigh">XHigh</option>
              <option value="max">Max</option>
            </select>
          </label>
        </div>
        <div className="form-grid">
          <label className="field">
            <span>Codex account</span>
            <select
              value={codexAccountId}
              onChange={(event) => setCodexAccountId(event.target.value)}
            >
              <option value="">Automatic · best available</option>
              {accounts.accounts.map((account) => (
                <option value={account.id} key={account.id}>
                  {accountOptionLabel(account)}
                </option>
              ))}
            </select>
            <small>
              A specific account is selected at the next safe attempt boundary.
              Automatic mode can hand off when capacity is low.
            </small>
          </label>
          <div className="field">
            <span>Plan approval</span>
            <div className="compact-choice">
              <label>
                <input
                  type="radio"
                  checked={!automaticPlanApproval}
                  onChange={() => setAutomaticPlanApproval(false)}
                />
                <span>
                  <b>Review before work</b>
                  <small>Recommended · pause after planning</small>
                </span>
              </label>
              <label>
                <input
                  type="radio"
                  checked={automaticPlanApproval}
                  onChange={() => setAutomaticPlanApproval(true)}
                />
                <span>
                  <b>Approve certified plan</b>
                  <small>Begin automatically after adversarial review</small>
                </span>
              </label>
            </div>
          </div>
        </div>
        <div className="choice-group">
          <span>Publication</span>
          <label>
            <input
              type="radio"
              checked={publication === "local_only"}
              onChange={() => setPublication("local_only")}
            />
            Keep local
          </label>
          <label>
            <input
              type="radio"
              checked={publication === "draft_pr_after_approval"}
              onChange={() => setPublication("draft_pr_after_approval")}
            />
            Draft PR after approval
          </label>
        </div>
        <div className="pin-note">
          <ShieldCheck size={15} />
          <span>
            Every task intends implementation. The selected governor manages the
            work; independent SOL verification and final signoff remain fixed.
          </span>
        </div>
        {error && <div className="form-error">{error}</div>}
        <div className="modal-actions">
          <button type="button" className="button" onClick={onClose}>
            Cancel
          </button>
          <button
            className="button primary"
            disabled={!objective.trim() || !repository || busy}
          >
            {busy ? "Creating and starting…" : "Create and start task"}
          </button>
        </div>
      </form>
    </ModalFrame>
  );
}

function CommandPalette({
  onClose,
  onNavigate,
  onNewRun,
  onRegister,
}: {
  onClose: () => void;
  onNavigate: (view: View) => void;
  onNewRun: () => void;
  onRegister: () => void;
}) {
  const [query, setQuery] = useState("");
  const commands = [
    { label: "New task", icon: Plus, action: onNewRun },
    { label: "Register repository", icon: FolderGit2, action: onRegister },
    ...nav.map((item) => ({
      label: `Go to ${item.label}`,
      icon: item.icon,
      action: () => onNavigate(item.view),
    })),
    ...systemNav.map((item) => ({
      label: `Go to ${item.label}`,
      icon: item.icon,
      action: () => onNavigate(item.view),
    })),
  ].filter((item) => item.label.toLowerCase().includes(query.toLowerCase()));
  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <div className="palette" onMouseDown={(event) => event.stopPropagation()}>
        <div className="palette-input">
          <Search size={17} />
          <input
            autoFocus
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search commands…"
          />
          <kbd>esc</kbd>
        </div>
        <div className="palette-results">
          {commands.map((item) => {
            const Icon = item.icon;
            return (
              <button key={item.label} onClick={item.action}>
                <Icon size={15} />
                <span>{item.label}</span>
                <small>↵</small>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function ModalFrame({
  title,
  eyebrow,
  onClose,
  wide,
  children,
}: {
  title: string;
  eyebrow: string;
  onClose: () => void;
  wide?: boolean;
  children: ReactNode;
}) {
  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <section
        className={`modal ${wide ? "wide" : ""}`}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <span className="eyebrow">{eyebrow}</span>
            <h2>{title}</h2>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="Close">
            <X size={16} />
          </button>
        </header>
        {children}
      </section>
    </div>
  );
}

function PageTitle({
  eyebrow,
  title,
  description,
  action,
}: {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return (
    <div className="page-title">
      <div>
        <span className="eyebrow">{eyebrow}</span>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {action}
    </div>
  );
}
function SectionHeader({
  title,
  count,
  aside,
}: {
  title: string;
  count?: number;
  aside?: string;
}) {
  return (
    <div className="section-header">
      <span>{title}</span>
      {count !== undefined && <i>{count}</i>}
      {aside && <small>{aside}</small>}
    </div>
  );
}
function Metric({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{note}</small>
    </div>
  );
}
function HealthCard({
  icon,
  label,
  state,
  detail,
}: {
  icon: ReactNode;
  label: string;
  state: string;
  detail: string;
}) {
  return (
    <div className="health-card">
      <div className={`health-icon tone-${tone(state)}`}>{icon}</div>
      <div>
        <span>{label}</span>
        <strong>{state}</strong>
        <small>{detail}</small>
      </div>
    </div>
  );
}
function InspectorCard({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="inspector-card">
      <div className="inspector-label">{label}</div>
      {children}
    </div>
  );
}
function EmptyCard({
  icon,
  title,
  text,
  action,
}: {
  icon: ReactNode;
  title: string;
  text: string;
  action?: ReactNode;
}) {
  return (
    <div className="empty-card">
      <div>{icon}</div>
      <strong>{title}</strong>
      <p>{text}</p>
      {action}
    </div>
  );
}
function EmptyInspector({
  icon,
  title,
  text,
}: {
  icon: ReactNode;
  title: string;
  text: string;
}) {
  return (
    <div className="empty-inspector">
      <div>{icon}</div>
      <strong>{title}</strong>
      <p>{text}</p>
    </div>
  );
}
function StatusBadge({ value }: { value: string }) {
  const animated = [
    "WORKING",
    "RUNNING",
    "STARTING",
    "FINISHING",
    "EXECUTING",
    "ARCHITECTING",
    "PLAN_ADVERSARIAL_REVIEW",
    "PLAN_REVISION_REQUIRED",
    "INTEGRATING",
    "VERIFYING",
  ].includes(value);
  return (
    <span
      className={`status-badge tone-${tone(value)} ${animated ? "animated" : ""}`}
    >
      {value.replaceAll("_", " ")}
    </span>
  );
}
function StateIcon({
  state,
  working = false,
}: {
  state: string;
  working?: boolean;
}) {
  const value = tone(state);
  return (
    <i className={`state-icon tone-${value} ${working ? "working" : ""}`}>
      {value === "success" ? (
        <Check size={10} />
      ) : value === "warning" ? (
        <Clock3 size={9} />
      ) : value === "danger" ? (
        <X size={9} />
      ) : (
        <span />
      )}
    </i>
  );
}

function StatusBar({
  stream,
  runtime,
  repository,
}: {
  stream: string;
  runtime?: RuntimeStatus;
  repository?: Repository;
}) {
  const streamLabel =
    stream === "connected"
      ? "Live updates on"
      : stream === "connecting"
        ? "Connecting live updates"
        : "Live updates disconnected";
  return (
    <footer className="statusbar">
      <span className={`stream-${stream}`}>
        <i />
        {streamLabel}
      </span>
      <span>
        {runtime?.codex.state === "ready"
          ? "App Server ready"
          : "App Server unavailable"}
      </span>
      <span>
        {runtime?.database.state === "ready"
          ? "Local history ready"
          : "Local history needs attention"}
      </span>
      {repository && (
        <span>
          {repository.primary_clean
            ? "Checkout clean"
            : "Checkout needs attention"}
        </span>
      )}
    </footer>
  );
}

function LoadingScreen() {
  return (
    <div className="loading-screen">
      <div className="brand-mark">
        <Zap size={17} fill="currentColor" />
        BILDR
      </div>
      <div className="loading-line">
        <i />
      </div>
      <span>Opening secure local session…</span>
    </div>
  );
}

export function effectiveRunPosture(run: Run, detail?: RunDetail) {
  if (terminal(run.state)) return run.state.replaceAll("_", " ");
  if (detail) {
    if (
      detail.approvals.some((approval) => approval.state === "pending") ||
      detail.tasks.some((task) => task.state === "WAITING_APPROVAL")
    )
      return "WAITING FOR APPROVAL";
    if (detail.tasks.some((task) => retryableState(task.state)))
      return "WAITING ON YOU";
    if (run.scheduler_paused) return "PAUSED";
    const active = detail.agents.some(
      (agent) => agent.active_turn_id && activeAgentState(agent.state),
    );
    if (active)
      return run.state === "PLAN_ADVERSARIAL_REVIEW"
        ? "REVIEWING PLAN"
        : planningRunState(run.state)
          ? "PLANNING"
          : "WORKING";
    if (
      detail.tasks.some((task) =>
        ["READY", "LEASED", "STARTING", "IMPLEMENTING", "VERIFYING"].includes(
          task.state,
        ),
      )
    )
      return "QUEUED";
  }
  if (run.scheduler_paused) return "PAUSED";
  if (run.state === "INTERVIEWING") return "CLARIFYING INTENT";
  if (run.state === "READY_FOR_ARCHITECTURE") return "READY TO PLAN";
  if (run.state === "PLAN_ADVERSARIAL_REVIEW") return "REVIEWING PLAN";
  if (run.state === "PLAN_REVISION_REQUIRED") return "REVISING PLAN";
  if (run.state === "ARCHITECTING") return "PLANNING";
  return run.state.replaceAll("_", " ");
}

function planningRunState(state: string) {
  return [
    "ARCHITECTING",
    "PLAN_ADVERSARIAL_REVIEW",
    "PLAN_REVISION_REQUIRED",
  ].includes(state);
}

function planningStateTitle(state: string) {
  if (state === "PLAN_ADVERSARIAL_REVIEW")
    return "Independent reviewer is stress-testing the plan";
  if (state === "PLAN_REVISION_REQUIRED")
    return "Architect is correcting blocking plan findings";
  return "Architect is researching and building the plan";
}

function planningStateDetail(state: string) {
  if (state === "PLAN_ADVERSARIAL_REVIEW")
    return "Checking feasibility, critical-path liveness, behavior-first milestones, test timing, and recovery authority.";
  if (state === "PLAN_REVISION_REQUIRED")
    return "Turning the review findings into a complete replacement plan; approval remains unavailable until it is certified.";
  return "Reading repository authority, decomposing the goal, and producing reviewable milestones.";
}

function terminal(state: string) {
  return ["COMPLETED", "CANCELED", "FAILED", "ARCHIVED"].includes(state);
}
function parseGovernorCheckpoint(text: string) {
  try {
    const value = JSON.parse(text) as GovernorCheckpoint;
    return value?.schema === "harness.governor-checkpoint.v1"
      ? value
      : undefined;
  } catch {
    return undefined;
  }
}
function humanAgentMessage(text: string) {
  return parseGovernorCheckpoint(text)?.operator_update || text;
}
function agentBudgetUsage(agent?: Agent) {
  return agent?.budget_tokens_used ?? agent?.tokens_used ?? 0;
}
function retryableState(state: string) {
  return [
    "NEEDS_HELP",
    "CHANGES_REQUESTED",
    "INTERRUPTED",
    "STALLED",
    "BLOCKED",
    "FAILED",
  ].includes(state);
}
function primaryTaskAgent(agents: Agent[], taskId?: string) {
  if (!taskId) return undefined;
  const roots = agents.filter(
    (agent) => agent.task_id === taskId && !agent.parent_agent_id,
  );
  const governors = roots.filter((agent) => agent.role === "governor");
  const owners = governors.length
    ? governors
    : roots.filter(
        (agent) =>
          !["verifier", "integrator", "final_auditor"].includes(agent.role),
      );
  const candidates = owners.length ? owners : roots;
  const active = candidates.filter((agent) =>
    ["STARTING", "RUNNING", "STEERED", "WAITING_APPROVAL"].includes(
      agent.state,
    ),
  );
  return active.at(-1) || candidates.at(-1);
}
function namedSubagentRoute(agent?: Agent) {
  if (!agent?.parent_agent_id || !agent.nickname) return undefined;
  const match = /^(sol|terra|luna)_(low|medium|high|xhigh|max)__/.exec(
    agent.nickname,
  );
  return match ? { model: `gpt-5.6-${match[1]}`, effort: match[2] } : undefined;
}
function agentModel(agent?: Agent) {
  return (
    agent?.effective_model ||
    namedSubagentRoute(agent)?.model ||
    agent?.requested_model
  );
}
function agentEffort(agent?: Agent) {
  return (
    agent?.effective_reasoning_effort ||
    namedSubagentRoute(agent)?.effort ||
    agent?.requested_reasoning_effort ||
    "—"
  );
}
function permissionLabel(mode?: string) {
  if (mode === "read-only") return "read only";
  if (mode === "workspace-write") return "can edit files";
  if (mode === "danger-full-access") return "full access";
  return mode?.replaceAll("-", " ") || "permissions pending";
}
function childDisplayName(agent: Agent) {
  const purpose = agent.nickname
    ?.split("__")
    .slice(1)
    .join(" ")
    .replaceAll("_", " ")
    .trim();
  return purpose
    ? purpose.replace(/\b\w/g, (letter) => letter.toUpperCase())
    : roleLabel(agent.role);
}
function activeAgentState(state: string) {
  return [
    "STARTING",
    "RUNNING",
    "STEERED",
    "WAITING_APPROVAL",
    "FINISHING",
  ].includes(state);
}
function contextStrategyLabel(strategy?: string) {
  if (strategy === "native_thread_reuse")
    return "Existing governor context retained";
  if (strategy === "bounded_handoff") return "Bounded handoff context";
  return "Fresh independent context";
}
function delegatedThreadDisplayState(child: Agent, governorActive: boolean) {
  return !governorActive && activeAgentState(child.state)
    ? "FINISHING"
    : child.state;
}
function humanTaskState(state: string) {
  if (state === "NEEDS_HELP") return "Waiting on you";
  if (state === "WAITING_APPROVAL") return "Waiting for approval";
  return roleLabel(state);
}
function humanAgentState(state: string) {
  if (state === "TURN_COMPLETE") return "Turn complete";
  if (state === "FINISHING") return "Finishing";
  if (state === "PAUSED") return "Paused";
  return roleLabel(state);
}
export function runLifecycleSummary(run: Run) {
  if (!run.started_at) {
    return `Local time · created ${formatLocalTimestamp(run.created_at)} · not started`;
  }
  return `Local time · started ${formatLocalTimestamp(run.started_at)} · ${run.completed_at ? `completed ${formatLocalTimestamp(run.completed_at)}` : "completion pending"}`;
}
export function threadLifecycleSummary(agent: Agent) {
  if (!agent.started_at) return "Local time · start not recorded";
  return `Local time · started ${formatLocalTimestamp(agent.started_at)} · ${agent.completed_at ? `completed ${formatLocalTimestamp(agent.completed_at)}` : "completion pending"}`;
}
function threadLifecycleRowSummary(agent: Agent) {
  if (!agent.started_at) return "Local · start not recorded";
  return `Local · start ${formatLocalClock(agent.started_at)} → ${agent.completed_at ? `done ${formatLocalClock(agent.completed_at)}` : "pending"}`;
}
export function blockerStatus(
  run: Run | undefined,
  task?: Task,
  agent?: Agent,
) {
  const blockedThread = Boolean(
    agent &&
      (["BLOCKED", "FAILED", "STALLED", "INTERRUPTED"].includes(agent.state) ||
        agent.failure_reason),
  );
  const blockedTask = Boolean(
    task &&
      (["BLOCKED", "FAILED", "STALLED", "INTERRUPTED", "NEEDS_HELP"].includes(
        task.state,
      ) || task.failure_reason),
  );
  const blockedRun = Boolean(run && (run.state === "BLOCKED" || run.failure_reason));
  if (!blockedThread && !blockedTask && !blockedRun) return undefined;

  const reason =
    agent?.failure_reason ||
    task?.failure_reason ||
    run?.failure_reason ||
    agent?.current_action ||
    "Harness recorded a blocked state without a more specific runtime reason.";
  const nextStep = agent?.parent_agent_id
    ? "This delegated thread is read-only. Return to the governor; only the governor can continue or retry its owning task."
    : task && retryableState(task.state)
      ? "Use Continue governor below. You can add a decision or new fact and choose the next attempt budget before continuing."
      : run?.state === "BLOCKED" && run.phase === "plan_review_deadlocked"
        ? "Describe one concrete plan defect in Request changes below; Harness will send that bounded correction through the revision loop."
        : run?.state === "BLOCKED" && run.phase === "plan_review_budget_exhausted"
          ? "Use the recovery panel above to resume the bounded final plan review or give the architect one concrete plan correction."
        : run?.scheduler_paused
          ? "Select Resume work after confirming the recorded condition is resolved."
          : "No safe automatic continuation is available at this run phase. Resolve the recorded condition, preserve the current evidence, then start a scoped follow-up if needed.";
  return { reason, nextStep };
}
function workStatusSummary(
  task?: Task,
  worktree?: Worktree,
  diff?: WorktreeDiffSummary,
) {
  if (!worktree)
    return {
      label: "NO WORKSPACE YET",
      detail: "Harness has not created a mutable workspace for this work yet.",
    };
  if (
    task &&
    ["INTEGRATED", "CI_PROVEN", "LIVE_PROVEN", "CLOSED"].includes(task.state)
  )
    return {
      label: "INTEGRATED",
      detail:
        "The controller-committed change is integrated into this run. This does not by itself claim that a remote pull request was merged.",
    };
  if (task?.state === "VERIFIED")
    return {
      label: "VERIFIED COMMIT",
      detail:
        "An independent reviewer accepted the controller-owned commit; it is waiting for integration.",
    };
  if (diff?.state === "committed_and_uncommitted")
    return {
      label: "COMMITTED + UNCOMMITTED",
      detail:
        "The workspace has controller-visible commits plus additional uncommitted changes.",
    };
  if (diff?.state === "uncommitted")
    return {
      label: "UNCOMMITTED",
      detail:
        "The workspace contains local changes that have not yet passed controller commit and review.",
    };
  if (diff?.state === "committed")
    return {
      label: "COMMITTED",
      detail:
        "The workspace head contains commits relative to the run base and has no additional uncommitted changes.",
    };
  if (
    worktree.state === "REVIEW_READY" ||
    task?.state === "REVIEW_READY" ||
    task?.state === "VERIFYING"
  )
    return {
      label: "COMMITTED",
      detail:
        "Harness committed the custody-checked change and retained it for review.",
    };
  if (worktree.state === "CONFLICTED")
    return {
      label: "CONFLICT",
      detail: "The workspace has an integration conflict that needs attention.",
    };
  if (worktree.dirty || worktree.files_changed > 0)
    return {
      label: "UNCOMMITTED",
      detail:
        "The workspace contains local changes that have not yet passed controller commit and review.",
    };
  if (worktree.state === "PRESERVED")
    return {
      label: "NO LOCAL CHANGES",
      detail: "This attempt was preserved without a projected local diff.",
    };
  return {
    label: "CLEAN WORKSPACE",
    detail:
      "The managed workspace is clean and no local diff is currently projected.",
  };
}
function pullRequestScope(run: Run, task?: Task) {
  const text = [run.title, run.objective, task?.title, task?.objective]
    .filter(Boolean)
    .join("\n");
  const numbers = Array.from(
    text.matchAll(/(?:#|\bPR\s*#?)\s*(\d{2,7})\b/gi),
    (match) => match[1],
  );
  const unique = [...new Set(numbers)];
  if (unique.length === 1) return `PR #${unique[0]}`;
  if (unique.length > 1 && unique.length <= 3)
    return `PRs ${unique.map((number) => `#${number}`).join(", ")}`;
  if (unique.length > 3) return `${unique.length} referenced PRs`;
  if (
    /\b(?:all|every|multiple)\s+(?:open\s+)?(?:pull requests?|PRs?)\b/i.test(
      text,
    )
  )
    return "Multiple pull requests";
  return "No pull request linked";
}
function humanizeWorktreeReason(reason: string) {
  return reason
    .replaceAll("_", " ")
    .replace(/^./, (letter) => letter.toUpperCase());
}
function approvalLabel(approval: Approval) {
  const kind = approval.approval_type.toLowerCase();
  if (kind.includes("command")) return "Command approval";
  if (kind.includes("file")) return "File change approval";
  if (kind.includes("tool")) return "Tool approval";
  return "Approval request";
}
function approvalSummary(request: unknown) {
  if (request && typeof request === "object") {
    const value = request as Record<string, unknown>;
    if (typeof value.command === "string") return value.command;
    if (typeof value.reason === "string") return value.reason;
  }
  return previewJson(request);
}
function tone(value: string) {
  const state = value.toLowerCase();
  if (
    [
      "complete",
      "completed",
      "verified",
      "integrated",
      "ci_proven",
      "live_proven",
      "closed",
      "success",
      "ready",
      "healthy",
      "accept",
      "resolved",
    ].some((item) => state.includes(item))
  )
    return "success";
  if (
    [
      "failed",
      "error",
      "canceled",
      "denied",
      "decline",
      "critical",
      "source_failure",
      "unavailable",
    ].some((item) => state.includes(item))
  )
    return "danger";
  if (
    [
      "waiting",
      "blocked",
      "paused",
      "approval",
      "warning",
      "high",
      "inconclusive",
      "needs_help",
      "stalled",
    ].some((item) => state.includes(item))
  )
    return "warning";
  return "active";
}
function formatTokens(value: number) {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}m`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toLocaleString();
}
function formatCost(microusd: number) {
  return `$${(microusd / 1_000_000).toFixed(2)}`;
}
function shortSha(value?: string) {
  return value ? value.slice(0, 7) : "—";
}
function shortModel(value?: string) {
  return value ? value.replace("gpt-5.6-", "").toUpperCase() : "—";
}
function roleLabel(value?: string) {
  return value
    ? value
        .replaceAll("_", " ")
        .replace(/\b\w/g, (letter) => letter.toUpperCase())
    : "Agent";
}
function formatLocalTimestamp(value?: string) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
      });
}
function formatLocalClock(value?: string) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleTimeString([], {
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
      });
}
function elapsed(value: string) {
  const ms = Math.max(0, Date.now() - new Date(value).getTime());
  const minutes = Math.floor(ms / 60_000);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}
export function formatTurnElapsed(value: string, now = Date.now()) {
  const started = new Date(value).getTime();
  if (Number.isNaN(started)) return "—";
  const seconds = Math.max(0, Math.floor((now - started) / 1_000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}
function timeAgo(value: string) {
  const seconds = Math.max(
    0,
    Math.floor((Date.now() - new Date(value).getTime()) / 1000),
  );
  if (seconds < 60) return `${seconds}s ago`;
  return `${Math.floor(seconds / 60)}m ago`;
}
function previewJson(value: unknown) {
  const raw = JSON.stringify(value);
  return raw.length > 220 ? `${raw.slice(0, 219)}…` : raw;
}
function message(value: unknown) {
  return value instanceof Error ? value.message : String(value);
}
function isTyping(target: EventTarget | null) {
  return (
    target instanceof HTMLInputElement ||
    target instanceof HTMLTextAreaElement ||
    target instanceof HTMLSelectElement
  );
}

export {
  agentEffort,
  agentModel,
  delegatedThreadDisplayState,
  formatCost,
  formatTokens,
  humanTaskState,
  primaryTaskAgent,
  pullRequestScope,
  roleLabel,
  shortModel,
  shortSha,
  terminal,
  tone,
  workStatusSummary,
};
