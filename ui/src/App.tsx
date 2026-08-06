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
  FileCode2,
  FileSearch,
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
  TerminalSquare,
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

type View =
  | "home"
  | "repositories"
  | "runs"
  | "approvals"
  | "worktrees"
  | "evidence"
  | "usage"
  | "host"
  | "settings";
type InspectorTab = "Activity" | "Plan" | "Diff" | "Files" | "Commands" | "Evidence" | "Usage" | "Context";
type Modal = "register" | "new-run" | "palette" | null;

const nav: Array<{ view: View; label: string; icon: typeof Home }> = [
  { view: "home", label: "Home", icon: Home },
  { view: "repositories", label: "Repositories", icon: FolderGit2 },
  { view: "runs", label: "Runs", icon: Activity },
  { view: "approvals", label: "Approvals", icon: ClipboardCheck },
  { view: "worktrees", label: "Worktrees", icon: GitBranch },
  { view: "evidence", label: "Evidence", icon: ShieldCheck },
  { view: "usage", label: "Usage", icon: CircleDollarSign },
];
const systemNav: Array<{ view: View; label: string; icon: typeof Home }> = [
  { view: "host", label: "Host", icon: ServerCog },
  { view: "settings", label: "Settings", icon: Settings },
];
const inspectorTabs: InspectorTab[] = [
  "Activity",
  "Plan",
  "Diff",
  "Files",
  "Commands",
  "Evidence",
  "Usage",
  "Context",
];

export default function App() {
  const [view, setView] = useState<View>("home");
  const [runtime, setRuntime] = useState<RuntimeStatus>();
  const [repositories, setRepositories] = useState<Repository[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [approvals, setApprovals] = useState<Approval[]>([]);
  const [worktrees, setWorktrees] = useState<Worktree[]>([]);
  const [runDetail, setRunDetail] = useState<RunDetail>();
  const [usage, setUsage] = useState<Usage>();
  const [evidence, setEvidence] = useState<EvidenceSnapshot>();
  const [activity, setActivity] = useState<ActivityItem[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string>();
  const [selectedTaskId, setSelectedTaskId] = useState<string>();
  const [selectedAgentId, setSelectedAgentId] = useState<string>();
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>("Activity");
  const [modal, setModal] = useState<Modal>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");
  const [stream, setStream] = useState<"connecting" | "connected" | "disconnected">("connecting");
  const [cursor, setCursor] = useState(0);
  const [light, setLight] = useState(() => localStorage.getItem("harness-theme") === "light");
  const reloadTimer = useRef<number | undefined>(undefined);

  const loadGlobal = useCallback(async () => {
    const [nextRuntime, nextRepositories, nextRuns, nextApprovals, nextWorktrees] = await Promise.all([
      api.runtime(),
      api.repositories(),
      api.runs(),
      api.approvals(),
      api.worktrees(),
    ]);
    setRuntime(nextRuntime);
    setRepositories(nextRepositories);
    setRuns(nextRuns);
    setApprovals(nextApprovals);
    setWorktrees(nextWorktrees);
    setSelectedRunId((current) => current || nextRuns.find((run) => !terminal(run.state))?.id || nextRuns[0]?.id);
  }, []);

  const loadRun = useCallback(async (runId: string) => {
    const [detail, nextUsage, nextEvidence] = await Promise.all([
      api.run(runId),
      api.usage(runId),
      api.evidence(runId),
    ]);
    setRunDetail(detail);
    setUsage(nextUsage);
    setEvidence(nextEvidence);
    setSelectedTaskId((current) =>
      current && detail.tasks.some((task) => task.id === current)
        ? current
        : detail.tasks.find((task) => ["IMPLEMENTING", "VERIFYING", "WAITING_APPROVAL"].includes(task.state))?.id ||
          detail.tasks[0]?.id,
    );
  }, []);

  const refresh = useCallback(async () => {
    try {
      await loadGlobal();
      if (selectedRunId) await loadRun(selectedRunId);
    } catch (caught) {
      setError(message(caught));
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
    if (!selectedRunId) {
      setRunDetail(undefined);
      return;
    }
    loadRun(selectedRunId).catch((caught) => setError(message(caught)));
  }, [loadRun, selectedRunId]);

  useEffect(() => {
    const agentId = selectedAgentId || runDetail?.agents.find((agent) => agent.task_id === selectedTaskId)?.id;
    if (!agentId) {
      setActivity([]);
      return;
    }
    api.activity(agentId).then(setActivity).catch((caught) => setError(message(caught)));
  }, [runDetail, selectedAgentId, selectedTaskId]);

  useEffect(() => {
    let source: EventSource | undefined;
    api.ensureSession().then(() => {
      source = new EventSource(`/api/v1/events${selectedRunId ? `?run_id=${selectedRunId}` : ""}`);
      source.onopen = () => setStream("connected");
      source.onerror = () => setStream("disconnected");
      source.addEventListener("domain", (raw) => {
        const event = raw as MessageEvent;
        const id = Number(event.lastEventId);
        if (Number.isFinite(id)) setCursor(id);
        window.clearTimeout(reloadTimer.current);
        reloadTimer.current = window.setTimeout(() => refresh(), 180);
      });
      source.addEventListener("heartbeat", (raw) => {
        const value = Number((raw as MessageEvent).data);
        if (Number.isFinite(value)) setCursor(value);
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

  const selectedTask = runDetail?.tasks.find((task) => task.id === selectedTaskId);
  const selectedAgent =
    runDetail?.agents.find((agent) => agent.id === selectedAgentId) ||
    runDetail?.agents.find((agent) => agent.task_id === selectedTaskId);
  const selectedWorktree = selectedTaskId
    ? runDetail?.worktrees.find((tree) => tree.task_id === selectedTaskId)
    : runDetail?.worktrees.find((tree) =>
        tree.kind === (selectedAgent?.role === "final_auditor" ? "integration" : "inspection"),
      );
  const currentRun = runDetail?.run || runs.find((run) => run.id === selectedRunId);

  const runAction = async (label: string, action: () => Promise<unknown>, success: string) => {
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
    setSelectedRunId(id);
    setSelectedAgentId(undefined);
    setView("runs");
  };

  if (loading) return <LoadingScreen />;

  return (
    <div className="app-frame">
      <TopBar
        repository={repositories.find((repository) => repository.id === currentRun?.repository_id)}
        run={currentRun}
        runtime={runtime}
        usage={usage}
        approvals={approvals}
        light={light}
        onTheme={() => setLight((value) => !value)}
        onPalette={() => setModal("palette")}
      />
      <div className={`shell ${view === "runs" && currentRun ? "with-inspector" : ""}`}>
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
              <button onClick={() => setError("")} aria-label="Dismiss error"><X size={14} /></button>
            </div>
          )}
          {toast && <div className="toast"><Check size={14} />{toast}</div>}
          {view === "home" && (
            <HomeView
              repositories={repositories}
              runs={runs}
              runtime={runtime}
              onNewRun={() => setModal(repositories.length ? "new-run" : "register")}
              onRun={chooseRun}
              onRegister={() => setModal("register")}
            />
          )}
          {view === "repositories" && (
            <RepositoriesView
              repositories={repositories}
              onRegister={() => setModal("register")}
              onInspect={(id) => runAction("inspect", () => api.post(`/repositories/${id}/inspect`), "Repository inspected")}
            />
          )}
          {view === "runs" && !currentRun && <EmptyRuns onNew={() => setModal("new-run")} />}
          {view === "runs" && currentRun && runDetail && (
            <RunWorkspace
              detail={runDetail}
              usage={usage}
              busy={busy}
              selectedTaskId={selectedTaskId}
              selectedAgentId={selectedAgentId}
              onSelect={(taskId, agentId) => {
                setSelectedTaskId(taskId);
                setSelectedAgentId(agentId);
              }}
              onStart={() => runAction("start", () => api.post(`/runs/${currentRun.id}/start-architecture`), "Architect started")}
              onPause={() =>
                runAction(
                  "pause",
                  () => api.post(`/runs/${currentRun.id}/scheduler/${currentRun.scheduler_paused ? "resume" : "pause"}`),
                  currentRun.scheduler_paused ? "Scheduling resumed" : "Scheduling paused",
                )
              }
              onApprove={() =>
                runAction(
                  "approve",
                  () =>
                    api.post(`/runs/${currentRun.id}/plan/approve`, {
                      task_graph_digest: runDetail.plan_digest || "",
                    }),
                  "Task graph approved",
                )
              }
              onStop={() =>
                runAction(
                  "stop",
                  () => api.post(`/runs/${currentRun.id}/stop`, { mode: "interrupt_turns", preserve_all_worktrees: true }),
                  "Run stopped; worktrees preserved",
                )
              }
              onApproveIntegration={() =>
                runAction(
                  "integration",
                  () =>
                    api.post(`/runs/${currentRun.id}/approve-integration`, {
                      expected_head_sha: currentRun.integration_sha || "",
                      note: "Reviewed and approved in Harness Console",
                    }),
                  "Integration approved and validated",
                )
              }
              onPublish={() =>
                runAction(
                  "publish",
                  () =>
                    api.post(`/runs/${currentRun.id}/publish-draft-pr`, {
                      expected_head_sha: currentRun.integration_sha || "",
                      title: currentRun.title,
                      body_appendix: "Created only after explicit approval in Harness Console.",
                    }),
                  "Draft pull request created",
                )
              }
              onExport={() => runAction("export", () => api.post(`/runs/${currentRun.id}/evidence/export`), "Evidence bundle exported")}
            />
          )}
          {view === "approvals" && (
            <ApprovalsView
              approvals={approvals}
              agents={runDetail?.agents || []}
              onDecision={(approval, decision) =>
                runAction(
                  `approval-${approval.id}`,
                  () => api.post(`/approvals/${approval.id}/decision`, { decision, expected_version: approval.version }),
                  decision === "accept" ? "Approval delivered" : "Request denied",
                )
              }
            />
          )}
          {view === "worktrees" && (
            <WorktreesView
              worktrees={worktrees}
              runs={runs}
              onPreserve={(id) => runAction("preserve", () => api.post(`/worktrees/${id}/preserve`, { reason: "Preserved from UI" }), "Worktree preserved")}
            />
          )}
          {view === "evidence" && <EvidenceView evidence={evidence} runs={runs} onRun={chooseRun} />}
          {view === "usage" && <UsageView usage={usage} run={currentRun} />}
          {view === "host" && <HostView runtime={runtime} repositories={repositories} />}
          {view === "settings" && <SettingsView light={light} onTheme={() => setLight((value) => !value)} />}
        </main>
        {view === "runs" && currentRun && runDetail && (
          <Inspector
            task={selectedTask}
            agent={selectedAgent}
            worktree={selectedWorktree}
            detail={runDetail}
            usage={usage}
            evidence={evidence}
            activity={activity}
            tab={inspectorTab}
            busy={busy}
            onTab={setInspectorTab}
            onSteer={(text) =>
              selectedAgent &&
              runAction("steer", () => api.post(`/agents/${selectedAgent.id}/steer`, { message: text, update_goal: false }), "Steering delivered")
            }
            onInterrupt={() =>
              selectedAgent &&
              runAction("interrupt", () => api.post(`/agents/${selectedAgent.id}/interrupt`), "Interrupt requested")
            }
            onRetry={() =>
              selectedTask &&
              runAction(
                "retry",
                () =>
                  api.post(`/tasks/${selectedTask.id}/retry`, {
                    reason: "Retry requested from the inspected failure evidence",
                    model_route: selectedTask.state === "CHANGES_REQUESTED" ? "escalate_terra" : "same",
                    additional_token_budget: 0,
                  }),
                "A new immutable task attempt was queued",
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
          />
        )}
      </div>
      <StatusBar stream={stream} cursor={cursor} runtime={runtime} repository={repositories.find((item) => item.id === currentRun?.repository_id)} run={currentRun} />
      {modal === "register" && <RegisterModal onClose={() => setModal(null)} onDone={async () => { setModal(null); await loadGlobal(); }} />}
      {modal === "new-run" && (
        <NewRunModal
          repositories={repositories}
          onClose={() => setModal(null)}
          onDone={async (run) => {
            setModal(null);
            await loadGlobal();
            chooseRun(run.id);
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
    </div>
  );
}

function TopBar({
  repository,
  run,
  runtime,
  usage,
  approvals,
  light,
  onTheme,
  onPalette,
}: {
  repository?: Repository;
  run?: Run;
  runtime?: RuntimeStatus;
  usage?: Usage;
  approvals: Approval[];
  light: boolean;
  onTheme: () => void;
  onPalette: () => void;
}) {
  return (
    <header className="topbar">
      <a href="#main-content" className="skip-link">Skip to content</a>
      <div className="brand-mark"><Zap size={15} fill="currentColor" /><span>Harness Console</span></div>
      <button className="crumb-button" title="Current repository">
        {repository?.display_name || "No repository"}<ChevronDown size={13} />
      </button>
      {run && <div className="run-crumb mono">{shortId(run.id)} · {shortSha(run.base_sha)}</div>}
      <div className="top-spacer" />
      <div className={`top-pill ${runtime?.codex.state === "ready" ? "healthy" : "unhealthy"}`} title={runtime?.codex.detail}>
        <i className="status-dot" />
        <span>App Server {runtime?.codex.version || "offline"}</span>
        <span className="wide-only">· {runtime?.codex.schema_match ? "schema matched" : "execution disabled"}</span>
      </div>
      <div className="top-pill wide-only">Slots {runtime?.scheduler.active_total || 0} / {runtime?.scheduler.max_total || 0}</div>
      <div className="top-pill wide-only">{formatTokens(usage?.total_tokens || 0)} · {formatCost(usage?.cost.upper_microusd || 0)}</div>
      <button className={`top-pill interactive ${approvals.length ? "attention" : ""}`} onClick={onPalette} title="Open command palette">
        <Search size={13} /> <span>{approvals.length ? `${approvals.length} approvals` : "⌘ K"}</span>
      </button>
      <button className="icon-button" onClick={onTheme} title={light ? "Use dark theme" : "Use light theme"} aria-label={light ? "Use dark theme" : "Use light theme"}>
        {light ? <Moon size={15} /> : <Sun size={15} />}
      </button>
    </header>
  );
}

function Rail({ view, approvals, activeRuns, onChange }: { view: View; approvals: number; activeRuns: number; onChange: (view: View) => void }) {
  return (
    <nav className="rail" aria-label="Primary navigation">
      <div className="rail-section">Workspace</div>
      {nav.map((item) => {
        const Icon = item.icon;
        const count = item.view === "runs" ? activeRuns : item.view === "approvals" ? approvals : 0;
        return (
          <button key={item.view} className={`nav-item ${view === item.view ? "active" : ""}`} onClick={() => onChange(item.view)} title={item.label}>
            <Icon size={16} /><span>{item.label}</span>{count > 0 && <i className="nav-count">{count}</i>}
          </button>
        );
      })}
      <div className="rail-section">System</div>
      {systemNav.map((item) => {
        const Icon = item.icon;
        return <button key={item.view} className={`nav-item ${view === item.view ? "active" : ""}`} onClick={() => onChange(item.view)} title={item.label}><Icon size={16} /><span>{item.label}</span></button>;
      })}
      <div className="rail-shortcuts"><kbd>G</kbd><kbd>H</kbd><span>Home</span><kbd>G</kbd><kbd>R</kbd><span>Runs</span></div>
    </nav>
  );
}

function HomeView({ repositories, runs, runtime, onNewRun, onRun, onRegister }: { repositories: Repository[]; runs: Run[]; runtime?: RuntimeStatus; onNewRun: () => void; onRun: (id: string) => void; onRegister: () => void }) {
  const active = runs.filter((run) => !terminal(run.state));
  return (
    <div className="page home-page">
      <PageTitle eyebrow="Local orchestration" title="Good afternoon" description="Exact repository truth, active work, and runtime health in one place." action={<button className="button primary" onClick={onNewRun}><Plus size={14} />New run</button>} />
      <SectionHeader title="Active" count={active.length} />
      <div className="stack">
        {active.length ? active.map((run) => <button className="home-row" key={run.id} onClick={() => onRun(run.id)}><StateIcon state={run.state} /><div><strong>{run.title}</strong><span>{run.state.replaceAll("_", " ")} · base {shortSha(run.base_sha)}</span></div><div className="home-row-meta">{elapsed(run.created_at)}<span>Open ›</span></div></button>) : <EmptyCard icon={<Activity />} title="No active runs" text="Start with a repository-scoped objective; Harness will pin an exact base before asking Codex to plan." action={<button className="button" onClick={onNewRun}>Create a run</button>} />}
      </div>
      <SectionHeader title="Repositories" count={repositories.length} />
      <div className="stack">
        {repositories.length ? repositories.map((repository) => <div className="home-row static" key={repository.id}><FolderGit2 size={17} /><div><strong>{repository.display_name}</strong><span>{repository.primary_branch || repository.default_branch} @ {shortSha(repository.primary_head)} · {repository.primary_clean ? "clean" : "dirty"} · {repository.managed_worktree_count} managed worktrees</span></div><StatusBadge value={repository.health} /></div>) : <EmptyCard icon={<FolderGit2 />} title="Register NeuralMatrix" text="Point Harness Console at your existing clean clone. The primary checkout remains coordination-only." action={<button className="button" onClick={onRegister}>Register repository</button>} />}
      </div>
      <SectionHeader title="Host" />
      <div className="health-grid">
        <HealthCard icon={<Bot />} label="Codex App Server" state={runtime?.codex.state || "unknown"} detail={runtime?.codex.detail || "Connecting"} />
        <HealthCard icon={<Database />} label="SQLite journal" state={runtime?.database.state || "unknown"} detail={runtime?.database.detail || "Checking"} />
        <HealthCard icon={<Gauge />} label="Scheduler" state={runtime?.scheduler.paused ? "paused" : "ready"} detail={`${runtime?.scheduler.active_total || 0}/${runtime?.scheduler.max_total || 0} agent slots · ${runtime?.scheduler.queued_tasks || 0} queued`} />
      </div>
    </div>
  );
}

function RepositoriesView({ repositories, onRegister, onInspect }: { repositories: Repository[]; onRegister: () => void; onInspect: (id: string) => void }) {
  return <div className="page"><PageTitle eyebrow="Custody roots" title="Repositories" description="Registered coordination clones and their current blockers." action={<button className="button primary" onClick={onRegister}><Plus size={14} />Register</button>} /><div className="table-card"><div className="table-head repo-grid"><span>Repository</span><span>Primary</span><span>Origin</span><span>Worktrees</span><span>Health</span><span /></div>{repositories.map((repository) => <div className="table-row repo-grid" key={repository.id}><div className="cell-main"><FolderGit2 size={16} /><span><strong>{repository.display_name}</strong><small className="mono">{repository.root_path}</small></span></div><span className="mono">{repository.primary_branch || "—"}<small>{shortSha(repository.primary_head)}</small></span><span className="truncate">{repository.origin_url || "missing"}</span><span>{repository.managed_worktree_count}</span><StatusBadge value={repository.health} /><button className="button subtle" onClick={() => onInspect(repository.id)}><RefreshCw size={13} />Inspect</button></div>)}</div></div>;
}

function EmptyRuns({ onNew }: { onNew: () => void }) {
  return <div className="page"><PageTitle eyebrow="Orchestration" title="Runs" description="No run is selected." /><EmptyCard icon={<Activity />} title="Create the first run" text="Harness pins origin/main, compiles active authority, and starts with read-only architecture." action={<button className="button primary" onClick={onNew}><Plus size={14} />New run</button>} /></div>;
}

function RunWorkspace({ detail, usage, busy, selectedTaskId, selectedAgentId, onSelect, onStart, onPause, onApprove, onApproveIntegration, onPublish, onStop, onExport }: { detail: RunDetail; usage?: Usage; busy: string; selectedTaskId?: string; selectedAgentId?: string; onSelect: (task?: string, agent?: string) => void; onStart: () => void; onPause: () => void; onApprove: () => void; onApproveIntegration: () => void; onPublish: () => void; onStop: () => void; onExport: () => void }) {
  const { run, tasks, agents, worktrees } = detail;
  const verified = tasks.filter((task) => ["VERIFIED", "INTEGRATED", "CI_PROVEN", "LIVE_PROVEN", "CLOSED"].includes(task.state)).length;
  const running = agents.filter((agent) => ["STARTING", "RUNNING", "STEERED", "WAITING_APPROVAL"].includes(agent.state)).length;
  const progress = tasks.length ? Math.round((verified / tasks.length) * 100) : run.state === "ARCHITECTING" ? 8 : 0;
  const architectAgents = agents.filter((agent) => !agent.task_id);
  return (
    <div className="run-page">
      <div className="run-heading"><div><div className="eyebrow">Run {shortId(run.id)} · base <span className="mono">{shortSha(run.base_sha)}</span></div><h1>{run.title}</h1><p>{run.state.replaceAll("_", " ")} · {verified} of {tasks.length} tasks verified · {run.integration_sha ? `integration ${shortSha(run.integration_sha)}` : "integration head not created"}</p></div><div className="actions"><button className="button" onClick={onPause} disabled={!!busy}>{run.scheduler_paused ? <Play size={13} /> : <Pause size={13} />}{run.scheduler_paused ? "Resume" : "Pause scheduling"}</button><button className="button" onClick={onExport} disabled={!!busy}><Archive size={13} />Export evidence</button><RunPrimaryAction run={run} detail={detail} busy={busy} onStart={onStart} onApprove={onApprove} onApproveIntegration={onApproveIntegration} onPublish={onPublish} /><button className="icon-button danger-hover" onClick={onStop} disabled={!!busy} title="Stop run"><Square size={13} /></button></div></div>
      <div className="progress" aria-label={`${progress}% of tasks verified`}><i style={{ width: `${progress}%` }} /></div>
      <div className="metrics"><Metric label="Running agents" value={String(running)} note={`${agents.length} sessions total`} /><Metric label="Verified tasks" value={`${verified} / ${tasks.length}`} note={`${tasks.filter((task) => task.state === "WAITING_DEPENDENCY").length} waiting`} /><Metric label="API-equivalent" value={formatCost(usage?.cost.upper_microusd || 0)} note={usage?.cost.confidence || "no usage yet"} /><Metric label="Elapsed" value={elapsed(run.created_at)} note={run.scheduler_paused ? "scheduler paused" : "active wall time"} /></div>
      {architectAgents.length > 0 && <><SectionHeader title="Architecture and review" />{architectAgents.map((agent) => <AgentRow key={agent.id} agent={agent} selected={selectedAgentId === agent.id} onClick={() => onSelect(undefined, agent.id)} worktree={worktrees.find((tree) => tree.kind === (agent.role === "final_auditor" ? "integration" : "inspection"))} />)}</>}
      <SectionHeader title="Implementation tasks" count={tasks.length} aside={`${running} running`} />
      <div className="task-stack">
        {tasks.map((task) => {
          const taskAgents = agents.filter((agent) => agent.task_id === task.id);
          const agent = taskAgents.find((item) => ["STARTING", "RUNNING", "STEERED", "WAITING_APPROVAL"].includes(item.state)) || taskAgents.at(-1);
          return <AgentRow key={task.id} task={task} agent={agent} worktree={worktrees.find((tree) => tree.task_id === task.id)} selected={selectedTaskId === task.id && (!selectedAgentId || selectedAgentId === agent?.id)} onClick={() => onSelect(task.id, agent?.id)} children={agents.filter((item) => item.parent_agent_id === agent?.id)} />;
        })}
        {!tasks.length && <div className="pending-plan"><Network size={20} /><div><strong>{run.state === "ARCHITECTING" ? "Architect is building the task graph" : "No task graph yet"}</strong><span>Mutation cannot start until a schema-valid plan is reviewed and explicitly approved.</span></div></div>}
      </div>
    </div>
  );
}

function RunPrimaryAction({ run, detail, busy, onStart, onApprove, onApproveIntegration, onPublish }: { run: Run; detail: RunDetail; busy: string; onStart: () => void; onApprove: () => void; onApproveIntegration: () => void; onPublish: () => void }) {
  if (run.state === "READY_FOR_ARCHITECTURE") return <button className="button primary" onClick={onStart} disabled={!!busy}><Bot size={14} />Start architecture</button>;
  if (run.state === "PLAN_REVIEW_REQUIRED") return <button className="button primary" onClick={onApprove} disabled={!!busy || !detail.plan_digest}><ClipboardCheck size={14} />Approve task graph</button>;
  if (run.state === "INTEGRATION_READY") return <button className="button primary" onClick={onApproveIntegration} disabled={!!busy || !run.integration_sha}><GitCompareArrows size={14} />Approve integration</button>;
  if (run.state === "PUBLICATION_READY" && run.publication_mode === "draft_pr_after_approval") return <button className="button primary" onClick={onPublish} disabled={!!busy || !run.integration_sha}><GitBranch size={14} />Create draft PR</button>;
  return <button className="button primary muted" disabled><Activity size={14} />{run.state.replaceAll("_", " ")}</button>;
}

function AgentRow({ task, agent, worktree, selected, onClick, children = [] }: { task?: Task; agent?: Agent; worktree?: Worktree; selected: boolean; onClick: () => void; children?: Agent[] }) {
  const state = task?.state || agent?.state || "QUEUED";
  const standalonePurpose = agent?.role === "final_auditor" ? "integrated exact-SHA audit" : "authority map and task graph";
  return <button className={`agent-card ${selected ? "selected" : ""}`} onClick={onClick}><div className="agent-row"><StateIcon state={state} /><div className="agent-copy"><strong>{task ? `${task.external_task_id} · ${task.title}` : `${roleLabel(agent?.role)} · ${standalonePurpose}`}</strong><span>{agent?.current_action || agent?.current_goal || (task?.dependencies.length ? `Waiting on ${task.dependencies.join(", ")}` : task?.objective) || "Waiting for runtime activity"}</span></div><div className="agent-model"><b>{shortModel(agent?.effective_model || agent?.requested_model)}</b><span>{agent?.effective_reasoning_effort || agent?.requested_reasoning_effort || "—"} · {agent?.sandbox_mode || "—"}</span></div><div className="agent-worktree mono"><span>{worktree?.branch || (worktree?.kind === "inspection" ? "inspection" : "not created")}</span><small>{shortSha(worktree?.head_sha || task?.head_sha || task?.base_sha)}</small></div><div className="agent-usage"><strong>{formatTokens(agent?.tokens_used || 0)}{agent?.token_budget ? ` / ${formatTokens(agent.token_budget)}` : ""}</strong><span>{agent?.estimated_cost_upper || "$0.00"}</span></div><StatusBadge value={state} /></div>{children.length > 0 && <div className="children">{children.map((child) => <div className="child" key={child.id}><span>↳ <b>{child.nickname || shortId(child.id)}</b></span><span>{shortModel(child.effective_model || child.requested_model)} · {child.effective_reasoning_effort || child.requested_reasoning_effort}</span><StatusBadge value={child.state} /></div>)}</div>}</button>;
}

function Inspector({ task, agent, worktree, detail, usage, evidence, activity, tab, busy, onTab, onSteer, onInterrupt, onRetry, onRequestReview }: { task?: Task; agent?: Agent; worktree?: Worktree; detail: RunDetail; usage?: Usage; evidence?: EvidenceSnapshot; activity: ActivityItem[]; tab: InspectorTab; busy: string; onTab: (tab: InspectorTab) => void; onSteer: (text: string) => void; onInterrupt: () => void; onRetry: () => void; onRequestReview: () => void }) {
  const [steer, setSteer] = useState("");
  const retryable = task && ["NEEDS_HELP", "CHANGES_REQUESTED", "INTERRUPTED", "STALLED", "BLOCKED", "FAILED"].includes(task.state);
  return <aside className="inspector"><div className="inspector-head"><div className="eyebrow">{task ? `Task ${task.external_task_id}` : roleLabel(agent?.role)}</div><h2>{task?.title || agent?.current_goal || "Select a task"}</h2><p className="mono">{worktree?.branch || "inspection"} · {shortSha(worktree?.head_sha || task?.head_sha || detail.run.base_sha)}</p></div><div className="tabs" role="tablist">{inspectorTabs.map((item) => <button role="tab" aria-selected={tab === item} className={`tab ${tab === item ? "active" : ""}`} onClick={() => onTab(item)} key={item}>{item}</button>)}</div><div className="inspector-body"><InspectorContent tab={tab} task={task} agent={agent} worktree={worktree} detail={detail} usage={usage} evidence={evidence} activity={activity} /></div>{(retryable || task?.state === "REVIEW_READY") && <div className="task-action-box">{retryable && <button className="button primary" onClick={onRetry} disabled={!!busy}><RefreshCw size={13} />Retry with evidence</button>}{task?.state === "REVIEW_READY" && <button className="button primary" onClick={onRequestReview} disabled={!!busy}><ShieldCheck size={13} />Request independent review</button>}</div>}{agent?.active_turn_id && <div className="steer-box"><textarea value={steer} onChange={(event) => setSteer(event.target.value)} placeholder="Steer the active turn…" rows={2} /><div><span>⌘ ↵ to send</span><button className="icon-button danger-hover" onClick={onInterrupt} disabled={!!busy} title="Interrupt turn"><Square size={13} /></button><button className="button primary" onClick={() => { if (steer.trim()) { onSteer(steer.trim()); setSteer(""); } }} disabled={!steer.trim() || !!busy}>Steer</button></div></div>}</aside>;
}

function InspectorContent({ tab, task, agent, worktree, detail, usage, evidence, activity }: { tab: InspectorTab; task?: Task; agent?: Agent; worktree?: Worktree; detail: RunDetail; usage?: Usage; evidence?: EvidenceSnapshot; activity: ActivityItem[] }) {
  if (tab === "Activity") return <><InspectorCard label="Current goal"><p>{agent?.current_goal || task?.objective || "No active goal"}</p><div className="mini-progress"><i style={{ width: `${agent?.token_budget ? Math.min(100, ((agent.tokens_used || 0) / agent.token_budget) * 100) : 0}%` }} /></div><small>{formatTokens(agent?.tokens_used || 0)} / {formatTokens(agent?.token_budget || 0)} tokens · {agent?.active_turn_id ? "turn active" : "no active turn"}</small></InspectorCard><InspectorCard label="Runtime"><p><strong className="violet">{shortModel(agent?.effective_model || agent?.requested_model)}</strong> · {agent?.effective_reasoning_effort || agent?.requested_reasoning_effort || "—"} · {agent?.sandbox_mode || "—"}</p><p className="mono muted-text">{agent?.cwd || worktree?.path || "—"}</p><small>{worktree ? `${worktree.files_changed} files · +${worktree.additions} / −${worktree.deletions}` : "No mutable worktree"} · {agent?.heartbeat_at ? `heartbeat ${timeAgo(agent.heartbeat_at)}` : "no heartbeat"}</small></InspectorCard><div className="inspector-section-title">Live activity</div><Timeline items={activity} /></>;
  if (tab === "Plan") return <><InspectorCard label="Controller task packet"><p>{task?.objective || detail.plan?.summary || "Architecture plan is pending."}</p>{task && <dl className="key-values"><dt>Priority</dt><dd>{task.priority}</dd><dt>Owner / reviewer</dt><dd>{task.owner_profile} / {task.reviewer_profile}</dd><dt>Dependencies</dt><dd>{task.dependencies.join(", ") || "none"}</dd><dt>Base</dt><dd className="mono">{shortSha(task.base_sha)}</dd></dl>}</InspectorCard>{detail.plan?.tasks.map((item, index) => <div className="plan-step" key={index}><i>{index + 1}</i><span><strong>{String((item as Record<string, unknown>).task_id || `Task ${index + 1}`)}</strong><small>{String((item as Record<string, unknown>).objective || "")}</small></span></div>)}</>;
  if (tab === "Diff") return <><InspectorCard label="Task diff custody"><dl className="key-values"><dt>Base</dt><dd className="mono">{shortSha(task?.base_sha)}</dd><dt>Head</dt><dd className="mono">{shortSha(worktree?.head_sha || task?.head_sha)}</dd><dt>Files</dt><dd>{worktree?.files_changed || 0}</dd><dt>Lines</dt><dd><span className="success">+{worktree?.additions || 0}</span> / <span className="danger">−{worktree?.deletions || 0}</span></dd><dt>Custody</dt><dd>{worktree?.state || "not created"}</dd></dl></InspectorCard><EmptyInspector icon={<GitCompareArrows />} title="Diff payload stays controller-owned" text="The aggregate patch is retained as evidence after custody checks. Large and binary files render as metadata." /></>;
  if (tab === "Files") return <><div className="inspector-section-title">Repository access</div><div className="file-row"><FileCode2 size={14} /><span><strong>{worktree?.path || "No worktree"}</strong><small>task root · {agent?.sandbox_mode || "read-only"}</small></span></div><EmptyInspector icon={<FileSearch />} title="No projected file events" text="Reads, searches, writes, leases, and denials appear here as App Server items arrive." /></>;
  if (tab === "Commands") return <><div className="inspector-section-title">Controller-visible commands</div>{activity.filter((item) => item.kind.toLowerCase().includes("command")).map((item) => <div className="command-row" key={item.id}><TerminalSquare size={14} /><span><code>{item.summary || item.kind}</code><small>{item.state} · {formatDate(item.occurred_at)}</small></span></div>)}{!activity.some((item) => item.kind.toLowerCase().includes("command")) && <EmptyInspector icon={<TerminalSquare />} title="No command events yet" text="Live previews are bounded; complete stdout and stderr are retained as hashed artifacts." />}</>;
  if (tab === "Evidence") return <><div className="inspector-section-title">Exact-SHA claims</div>{evidence?.evidence.length ? evidence.evidence.map((record, index) => <div className="evidence-card" key={String(record.id || index)}><ShieldCheck size={15} /><span><strong>{String(record.claim_id || "Evidence claim")}</strong><small>{String(record.proof_tier || "—")} · {String(record.result_class || "unknown")} · {shortSha(String(record.source_sha || ""))}</small></span></div>) : <EmptyInspector icon={<ShieldCheck />} title="No evidence claims yet" text="A worker response is never shown as proof. Validator evidence is attached to an exact source SHA and proof tier." />}</>;
  if (tab === "Usage") return <UsageTable usage={usage} compact />;
  return <><InspectorCard label="Provenance"><dl className="key-values"><dt>Base SHA</dt><dd className="mono">{shortSha(detail.run.base_sha)}</dd><dt>Authority</dt><dd className="mono">{shortSha(detail.run.authority_digest)}</dd><dt>Task packet</dt><dd>{task ? `attempt ${task.attempt}` : "architecture"}</dd><dt>Codex thread</dt><dd className="mono">{shortId(agent?.thread_id)}</dd></dl></InspectorCard><div className="inspector-section-title">Context policy</div><div className="context-rule"><Check size={13} />Active instruction and authority sources first</div><div className="context-rule"><Check size={13} />Archive, vendor, generated, binary, and secret-like paths excluded</div><div className="context-rule"><Check size={13} />Every included source has a stable SHA-256 digest</div></>;
}

function ApprovalsView({ approvals, agents, onDecision }: { approvals: Approval[]; agents: Agent[]; onDecision: (approval: Approval, decision: string) => void }) {
  return <div className="page"><PageTitle eyebrow="Human control" title="Approval center" description="Highest risk first. Every decision is durable and delivered back to the originating App Server request." /><div className="approval-layout"><div className="stack">{approvals.length ? approvals.map((approval) => <div className="approval-card" key={approval.id}><div className="approval-risk"><AlertTriangle size={15} /><StatusBadge value={approval.risk_level} /></div><div><strong>{approval.approval_type.replaceAll("/", " · ")}</strong><p>{agents.find((agent) => agent.id === approval.agent_id)?.current_goal || "Runtime requested an explicit operator decision."}</p><code>{previewJson(approval.request)}</code><small>{formatDate(approval.created_at)} · thread {shortId(approval.thread_id)}</small></div><div className="approval-actions"><button className="button" onClick={() => onDecision(approval, "decline")}>Deny</button><button className="button primary" onClick={() => onDecision(approval, "accept")}>Approve once</button></div></div>) : <EmptyCard icon={<ShieldCheck />} title="No pending approvals" text="Sandbox escapes, network access, file mutations, and other guarded actions appear here." />}</div><aside className="policy-card"><ShieldCheck size={20} /><h3>V1 approval policy</h3><p>Approvals never authorize auto-push, PR creation, merge, broad filesystem access, or writes outside a leased worktree.</p><ul><li>Request payload is hashed before display.</li><li>Exact HEAD and mutable-worktree fingerprint are rechecked.</li><li>Decisions are recorded as human actions.</li></ul></aside></div></div>;
}

function WorktreesView({ worktrees, runs, onPreserve }: { worktrees: Worktree[]; runs: Run[]; onPreserve: (id: string) => void }) {
  return <div className="page"><PageTitle eyebrow="Git custody" title="Managed worktrees" description="Inspection, task, integration, and retained failure worktrees. The primary clone stays coordination-only." /><div className="table-card"><div className="table-head worktree-grid"><span>Worktree</span><span>Run</span><span>Branch / head</span><span>Diff</span><span>State</span><span /></div>{worktrees.map((tree) => <div className="table-row worktree-grid" key={tree.id}><div className="cell-main"><GitBranch size={15} /><span><strong>{tree.kind}</strong><small className="mono">{tree.path}</small></span></div><span>{runs.find((run) => run.id === tree.run_id)?.title || shortId(tree.run_id)}</span><span className="mono truncate">{tree.branch || "detached"}<small>{shortSha(tree.head_sha || tree.base_sha)}</small></span><span>+{tree.additions} / −{tree.deletions}<small>{tree.files_changed} files</small></span><StatusBadge value={tree.state} /><button className="button subtle" onClick={() => onPreserve(tree.id)} disabled={tree.state === "PRESERVED"}>Preserve</button></div>)}</div></div>;
}

function EvidenceView({ evidence, runs, onRun }: { evidence?: EvidenceSnapshot; runs: Run[]; onRun: (id: string) => void }) {
  return <div className="page"><PageTitle eyebrow="No false green" title="Evidence" description="Claims remain separated by source SHA, proof tier, validator, and result class." /><div className="evidence-summary"><Metric label="Evidence records" value={String(evidence?.evidence.length || 0)} note="exact-SHA claims" /><Metric label="Artifacts" value={String(evidence?.artifacts.length || 0)} note="content addressed" /><Metric label="Run" value={evidence ? shortId(evidence.run.id) : "—"} note={evidence?.run.title || "select a run"} /></div><div className="table-card"><div className="table-head evidence-grid"><span>Claim</span><span>SHA</span><span>Tier</span><span>Result</span><span>Unproved</span></div>{evidence?.evidence.map((record, index) => <div className="table-row evidence-grid" key={String(record.id || index)}><strong>{String(record.claim_id || "claim")}</strong><span className="mono">{shortSha(String(record.source_sha || ""))}</span><span>{String(record.proof_tier || "—")}</span><StatusBadge value={String(record.result_class || "unknown")} /><span>{Array.isArray(record.unproved_claims) ? record.unproved_claims.length : 0}</span></div>)}</div>{!evidence && <div className="run-chips">{runs.map((run) => <button className="button" key={run.id} onClick={() => onRun(run.id)}>{run.title}</button>)}</div>}</div>;
}

function UsageView({ usage, run }: { usage?: Usage; run?: Run }) {
  return <div className="page"><PageTitle eyebrow="API-equivalent estimate" title="Usage" description="Reasoning is an output breakdown, never an extra billed class. Missing cache-write counters produce a range." /><div className="metrics"><Metric label="Input" value={formatTokens(usage?.input_tokens || 0)} note={`${formatTokens(usage?.cached_input_tokens || 0)} cached`} /><Metric label="Output" value={formatTokens(usage?.output_tokens || 0)} note={`${formatTokens(usage?.reasoning_output_tokens || 0)} reasoning`} /><Metric label="Total" value={formatTokens(usage?.total_tokens || 0)} note={`${usage?.by_model.length || 0} effective models`} /><Metric label="API-equivalent" value={formatCost(usage?.cost.upper_microusd || 0)} note={usage?.cost.confidence || "unknown confidence"} /></div><UsageTable usage={usage} /><div className="callout"><CircleDollarSign size={17} /><div><strong>Not an invoice</strong><p>For subscription-authenticated Codex sessions, dollar values are current price-snapshot API-equivalent estimates for capacity planning. Run: {run?.title || "none selected"}.</p></div></div></div>;
}

function UsageTable({ usage, compact = false }: { usage?: Usage; compact?: boolean }) {
  return <div className={`usage-table ${compact ? "compact" : ""}`}><div className="usage-head"><span>Effective model</span><span>Turns</span><span>Input</span><span>Cached</span><span>Output</span><span>Reasoning</span><span>Estimate</span></div>{usage?.by_model.map((row) => <div className="usage-line" key={row.model}><strong>{row.model}</strong><span>{row.turns}</span><span>{formatTokens(row.usage.input_tokens)}</span><span>{formatTokens(row.usage.cached_input_tokens)}</span><span>{formatTokens(row.usage.output_tokens)}</span><span>{formatTokens(row.usage.reasoning_output_tokens)}</span><span>{formatCost(row.cost.upper_microusd)}</span></div>)}{!usage?.by_model.length && <div className="usage-empty">Usage samples appear after completed turns.</div>}</div>;
}

function HostView({ runtime, repositories }: { runtime?: RuntimeStatus; repositories: Repository[] }) {
  return <div className="page"><PageTitle eyebrow="Local runtime" title="Host and App Server" description="Execution remains disabled unless the exact Codex version and generated protocol schema match." /><div className="health-grid large"><HealthCard icon={<Bot />} label="Codex App Server" state={runtime?.codex.state || "unknown"} detail={runtime?.codex.detail || "Not connected"} /><HealthCard icon={<Database />} label="SQLite" state={runtime?.database.state || "unknown"} detail={runtime?.database.detail || "Not checked"} /><HealthCard icon={<Gauge />} label="Scheduler" state={runtime?.scheduler.paused ? "paused" : "ready"} detail={`${runtime?.scheduler.active_mutable || 0}/${runtime?.scheduler.max_mutable || 0} mutable · ${runtime?.scheduler.active_verifiers || 0}/${runtime?.scheduler.max_verifiers || 0} verifier`} /></div><div className="detail-grid"><InspectorCard label="Compatibility"><dl className="key-values"><dt>Installed</dt><dd>{runtime?.codex.version || "unavailable"}</dd><dt>Required</dt><dd>{runtime?.codex.required_version || "not pinned"}</dd><dt>Schema</dt><dd>{runtime?.codex.schema_match ? "exact match" : "mismatch / unavailable"}</dd><dt>PID</dt><dd>{runtime?.codex.pid || "—"}</dd></dl></InspectorCard><InspectorCard label="Repository invariants"><dl className="key-values"><dt>Registered</dt><dd>{repositories.length}</dd><dt>Clean primaries</dt><dd>{repositories.filter((item) => item.primary_clean).length}</dd><dt>Blocked</dt><dd>{repositories.filter((item) => item.blockers.length).length}</dd><dt>External writes</dt><dd>disabled</dd></dl></InspectorCard></div></div>;
}

function SettingsView({ light, onTheme }: { light: boolean; onTheme: () => void }) {
  return <div className="page narrow-page"><PageTitle eyebrow="Local preferences" title="Settings" description="Security-critical policy comes from typed configuration and cannot be weakened in this UI." /><div className="settings-card"><div><strong>Appearance</strong><span>Choose a high-contrast dark or light workspace.</span></div><button className="button" onClick={onTheme}>{light ? <Moon size={14} /> : <Sun size={14} />}{light ? "Dark theme" : "Light theme"}</button></div><div className="settings-card"><div><strong>Raw reasoning retention</strong><span>Hidden reasoning text is dropped; concise reasoning summaries remain visible.</span></div><StatusBadge value="disabled" /></div><div className="settings-card"><div><strong>Automatic external writes</strong><span>Push, PR creation, readiness changes, and merge always require explicit product flows.</span></div><StatusBadge value="disabled" /></div><div className="settings-card"><div><strong>Keyboard navigation</strong><span><kbd>G</kbd> <kbd>H</kbd> home · <kbd>G</kbd> <kbd>R</kbd> runs · <kbd>⌘</kbd> <kbd>K</kbd> commands</span></div></div></div>;
}

function RegisterModal({ onClose, onDone }: { onClose: () => void; onDone: () => Promise<void> }) {
  const [path, setPath] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const submit = async (event: FormEvent) => { event.preventDefault(); setBusy(true); setError(""); try { await api.registerRepository(path); await onDone(); } catch (caught) { setError(message(caught)); } finally { setBusy(false); } };
  return <ModalFrame title="Register NeuralMatrix" eyebrow="Coordination clone" onClose={onClose}><form onSubmit={submit}><label className="field"><span>Repository root</span><input autoFocus value={path} onChange={(event) => setPath(event.target.value)} placeholder="/home/you/Documents/NeuralMatrix" required /><small>Must be a clean clone on main with origin and Git identity configured.</small></label>{error && <div className="form-error">{error}</div>}<div className="modal-actions"><button type="button" className="button" onClick={onClose}>Cancel</button><button className="button primary" disabled={!path.trim() || busy}>{busy ? "Inspecting…" : "Register and inspect"}</button></div></form></ModalFrame>;
}

function NewRunModal({ repositories, onClose, onDone }: { repositories: Repository[]; onClose: () => void; onDone: (run: Run) => Promise<void> }) {
  const [repository, setRepository] = useState(repositories[0]?.id || "");
  const [objective, setObjective] = useState("");
  const [mode, setMode] = useState("plan_and_implement");
  const [publication, setPublication] = useState("local_only");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: FormEvent) => { event.preventDefault(); setBusy(true); setError(""); try { const run = await api.createRun(repository, objective, mode, publication); await onDone(run); } catch (caught) { setError(message(caught)); } finally { setBusy(false); } };
  return <ModalFrame title="New NeuralMatrix run" eyebrow="Pin before execution" onClose={onClose} wide><form onSubmit={submit}><label className="field"><span>Objective</span><textarea autoFocus rows={7} value={objective} onChange={(event) => setObjective(event.target.value)} placeholder="Audit and hard-cut… Include the intended behavior, forbidden fallbacks, positive and negative proof, and what must remain unchanged." required /></label><div className="form-grid"><label className="field"><span>Repository</span><select value={repository} onChange={(event) => setRepository(event.target.value)}>{repositories.map((item) => <option value={item.id} key={item.id}>{item.display_name} · {item.health}</option>)}</select></label><label className="field"><span>Base</span><input value="origin/main" readOnly /></label></div><div className="choice-group"><span>Run mode</span><label><input type="radio" checked={mode === "plan_only"} onChange={() => setMode("plan_only")} />Plan only</label><label><input type="radio" checked={mode === "plan_and_implement"} onChange={() => setMode("plan_and_implement")} />Plan + implement</label></div><div className="choice-group"><span>Publication</span><label><input type="radio" checked={publication === "local_only"} onChange={() => setPublication("local_only")} />Local only</label><label><input type="radio" checked={publication === "draft_pr_after_approval"} onChange={() => setPublication("draft_pr_after_approval")} />Draft PR after approval</label></div><div className="pin-note"><GitBranch size={15} /><span>Fetch resolves the exact base SHA before architecture starts. A later remote advance never changes this run.</span></div>{error && <div className="form-error">{error}</div>}<div className="modal-actions"><button type="button" className="button" onClick={onClose}>Cancel</button><button className="button primary" disabled={!objective.trim() || !repository || busy}>{busy ? "Fetching and pinning…" : "Fetch, pin, and inspect →"}</button></div></form></ModalFrame>;
}

function CommandPalette({ onClose, onNavigate, onNewRun, onRegister }: { onClose: () => void; onNavigate: (view: View) => void; onNewRun: () => void; onRegister: () => void }) {
  const [query, setQuery] = useState("");
  const commands = [{ label: "New run", icon: Plus, action: onNewRun }, { label: "Register repository", icon: FolderGit2, action: onRegister }, ...nav.map((item) => ({ label: `Go to ${item.label}`, icon: item.icon, action: () => onNavigate(item.view) })), ...systemNav.map((item) => ({ label: `Go to ${item.label}`, icon: item.icon, action: () => onNavigate(item.view) }))].filter((item) => item.label.toLowerCase().includes(query.toLowerCase()));
  return <div className="modal-backdrop" onMouseDown={onClose}><div className="palette" onMouseDown={(event) => event.stopPropagation()}><div className="palette-input"><Search size={17} /><input autoFocus value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search commands…" /><kbd>esc</kbd></div><div className="palette-results">{commands.map((item) => { const Icon = item.icon; return <button key={item.label} onClick={item.action}><Icon size={15} /><span>{item.label}</span><small>↵</small></button>; })}</div></div></div>;
}

function ModalFrame({ title, eyebrow, onClose, wide, children }: { title: string; eyebrow: string; onClose: () => void; wide?: boolean; children: ReactNode }) {
  return <div className="modal-backdrop" onMouseDown={onClose}><section className={`modal ${wide ? "wide" : ""}`} role="dialog" aria-modal="true" aria-label={title} onMouseDown={(event) => event.stopPropagation()}><header><div><span className="eyebrow">{eyebrow}</span><h2>{title}</h2></div><button className="icon-button" onClick={onClose} aria-label="Close"><X size={16} /></button></header>{children}</section></div>;
}

function PageTitle({ eyebrow, title, description, action }: { eyebrow: string; title: string; description: string; action?: ReactNode }) {
  return <div className="page-title"><div><span className="eyebrow">{eyebrow}</span><h1>{title}</h1><p>{description}</p></div>{action}</div>;
}
function SectionHeader({ title, count, aside }: { title: string; count?: number; aside?: string }) { return <div className="section-header"><span>{title}</span>{count !== undefined && <i>{count}</i>}{aside && <small>{aside}</small>}</div>; }
function Metric({ label, value, note }: { label: string; value: string; note: string }) { return <div className="metric"><span>{label}</span><strong>{value}</strong><small>{note}</small></div>; }
function HealthCard({ icon, label, state, detail }: { icon: ReactNode; label: string; state: string; detail: string }) { return <div className="health-card"><div className={`health-icon tone-${tone(state)}`}>{icon}</div><div><span>{label}</span><strong>{state}</strong><small>{detail}</small></div></div>; }
function InspectorCard({ label, children }: { label: string; children: ReactNode }) { return <div className="inspector-card"><div className="inspector-label">{label}</div>{children}</div>; }
function EmptyCard({ icon, title, text, action }: { icon: ReactNode; title: string; text: string; action?: ReactNode }) { return <div className="empty-card"><div>{icon}</div><strong>{title}</strong><p>{text}</p>{action}</div>; }
function EmptyInspector({ icon, title, text }: { icon: ReactNode; title: string; text: string }) { return <div className="empty-inspector"><div>{icon}</div><strong>{title}</strong><p>{text}</p></div>; }
function StatusBadge({ value }: { value: string }) { return <span className={`status-badge tone-${tone(value)}`}>{value.replaceAll("_", " ")}</span>; }
function StateIcon({ state }: { state: string }) { const value = tone(state); return <i className={`state-icon tone-${value}`}>{value === "success" ? <Check size={10} /> : value === "warning" ? <Clock3 size={9} /> : value === "danger" ? <X size={9} /> : <span />}</i>; }

function Timeline({ items }: { items: ActivityItem[] }) {
  return <div className="timeline">{items.length ? [...items].reverse().map((item, index) => <div className={`timeline-item ${index === 0 && item.state !== "completed" ? "active" : ""}`} key={item.id}><div className="timeline-time">{formatDate(item.occurred_at)} · {item.kind.replaceAll("/", " · ")}</div><div className="timeline-copy">{item.summary || item.kind}</div></div>) : <EmptyInspector icon={<Activity />} title="Waiting for activity" text="Plan steps, concise reasoning summaries, commands, files, reviews, and usage samples stream here." />}</div>;
}

function StatusBar({ stream, cursor, runtime, repository, run }: { stream: string; cursor: number; runtime?: RuntimeStatus; repository?: Repository; run?: Run }) {
  return <footer className="statusbar"><span className={`stream-${stream}`}><i />Event stream {stream}</span><span>cursor {cursor.toLocaleString()}</span><span>{runtime?.database.state === "ready" ? "SQLite WAL healthy" : "database degraded"}</span><span>{repository?.primary_clean ? "primary checkout clean" : "primary needs attention"}</span>{run && <span className="mono">{run.base_ref} {shortSha(run.base_sha)}</span>}</footer>;
}

function LoadingScreen() { return <div className="loading-screen"><div className="brand-mark"><Zap size={17} fill="currentColor" />Harness Console</div><div className="loading-line"><i /></div><span>Opening secure local session…</span></div>; }

function terminal(state: string) { return ["COMPLETED", "CANCELED", "FAILED", "ARCHIVED"].includes(state); }
function tone(value: string) { const state = value.toLowerCase(); if (["complete", "completed", "verified", "integrated", "ci_proven", "live_proven", "closed", "success", "ready", "healthy", "accept", "resolved"].some((item) => state.includes(item))) return "success"; if (["failed", "error", "canceled", "denied", "decline", "critical", "source_failure", "unavailable"].some((item) => state.includes(item))) return "danger"; if (["waiting", "blocked", "paused", "approval", "warning", "high", "inconclusive", "needs_help", "stalled"].some((item) => state.includes(item))) return "warning"; return "active"; }
function formatTokens(value: number) { if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}m`; if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`; return value.toLocaleString(); }
function formatCost(microusd: number) { return `$${(microusd / 1_000_000).toFixed(2)}`; }
function shortSha(value?: string) { return value ? value.slice(0, 7) : "—"; }
function shortId(value?: string) { return value ? value.slice(0, 10) : "—"; }
function shortModel(value?: string) { return value ? value.replace("gpt-5.6-", "").toUpperCase() : "—"; }
function roleLabel(value?: string) { return value ? value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()) : "Agent"; }
function formatDate(value?: string) { if (!value) return "—"; const date = new Date(value); return Number.isNaN(date.getTime()) ? value : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }); }
function elapsed(value: string) { const ms = Math.max(0, Date.now() - new Date(value).getTime()); const minutes = Math.floor(ms / 60_000); if (minutes < 60) return `${minutes}m`; const hours = Math.floor(minutes / 60); return `${hours}h ${minutes % 60}m`; }
function timeAgo(value: string) { const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000)); if (seconds < 60) return `${seconds}s ago`; return `${Math.floor(seconds / 60)}m ago`; }
function previewJson(value: unknown) { const raw = JSON.stringify(value); return raw.length > 220 ? `${raw.slice(0, 219)}…` : raw; }
function message(value: unknown) { return value instanceof Error ? value.message : String(value); }
function isTyping(target: EventTarget | null) { return target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement; }

export { formatCost, formatTokens, roleLabel, shortModel, shortSha, terminal, tone };
