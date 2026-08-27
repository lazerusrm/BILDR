import { useEffect, useState } from "react";
import {
  Archive,
  CircleCheck,
  CircleSlash,
  Pause,
  Pin,
  PinOff,
  Play,
  Trash2,
  TriangleAlert,
} from "lucide-react";
import type { Run } from "../types";

export type ThreadPosture = "working" | "waiting" | "stopped" | "done" | "failed";

const WAITING = new Set([
  "PLAN_REVIEW_REQUIRED",
  "HUMAN_REVIEW",
  "PUBLICATION_READY",
  "DRAFT_PR_CREATED",
  "INTEGRATION_READY",
  "PAUSED",
  "BLOCKED",
]);
const STOPPED = new Set(["CANCELED", "STOPPING", "ARCHIVED"]);
const ARCHIVABLE = new Set(["COMPLETED", "CANCELED", "FAILED"]);

/** Collapse the run lifecycle into the four states a thread list can show. */
export function threadPosture(run: Run): ThreadPosture {
  const state = String(run.state).toUpperCase();
  if (state === "FAILED") return "failed";
  if (state === "COMPLETED") return "done";
  if (STOPPED.has(state)) return "stopped";
  if (run.scheduler_paused || WAITING.has(state)) return "waiting";
  return "working";
}

/** Pinned threads first, then most recently touched. */
export function orderThreads(runs: Run[]): Run[] {
  const key = (run: Run) => run.started_at || run.created_at || "";
  return [...runs].sort((left, right) => {
    if (Boolean(left.pinned) !== Boolean(right.pinned)) return left.pinned ? -1 : 1;
    return key(right).localeCompare(key(left));
  });
}

/** True when a run is archived and belongs in the Archived section. */
export function isArchived(run: Run) {
  return String(run.state).toUpperCase() === "ARCHIVED";
}

/**
 * What each action will do to this run, so a live thread warns before it is
 * stopped rather than silently cancelling work.
 */
export function threadActionEffect(run: Run) {
  const state = String(run.state).toUpperCase();
  const live = !ARCHIVABLE.has(state) && state !== "ARCHIVED";
  return {
    archive: isArchived(run)
      ? "Already archived"
      : live
        ? "Stops the thread first"
        : undefined,
    deleteWarnsLive: live,
  };
}

const ICONS = {
  working: Play,
  waiting: Pause,
  stopped: CircleSlash,
  done: CircleCheck,
  failed: TriangleAlert,
} as const;

const LABELS = {
  working: "Working",
  waiting: "Waiting",
  stopped: "Stopped",
  done: "Completed",
  failed: "Failed",
} as const;

type Menu = { run: Run; x: number; y: number };

export function ThreadList({
  runs,
  selectedRunId,
  busy,
  onSelect,
  onPin,
  onArchive,
  onDelete,
}: {
  runs: Run[];
  selectedRunId?: string;
  busy: string;
  onSelect: (runId: string) => void;
  onPin: (run: Run) => void;
  onArchive: (run: Run) => void;
  onDelete: (run: Run) => void;
}) {
  const [confirming, setConfirming] = useState("");
  const [menu, setMenu] = useState<Menu>();
  const ordered = orderThreads(runs);

  useEffect(() => {
    if (!menu) return;
    const close = () => setMenu(undefined);
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenu(undefined);
    };
    document.addEventListener("pointerdown", close);
    document.addEventListener("keydown", escape);
    window.addEventListener("blur", close);
    return () => {
      document.removeEventListener("pointerdown", close);
      document.removeEventListener("keydown", escape);
      window.removeEventListener("blur", close);
    };
  }, [menu]);

  if (!ordered.length) {
    return <p className="thread-empty">No threads yet.</p>;
  }

  const openMenu = (event: React.MouseEvent, run: Run) => {
    event.preventDefault();
    setMenu({
      run,
      x: Math.min(event.clientX, window.innerWidth - 200),
      y: Math.min(event.clientY, window.innerHeight - 140),
    });
  };

  return (
    <div className="thread-list" role="list" aria-label="Threads">
      {ordered.map((run) => {
        const posture = threadPosture(run);
        const Icon = ICONS[posture];
        if (confirming === run.id) {
          return (
            <div className="thread-confirm" key={run.id}>
              <span>
                Delete “{run.title}” and its records?
                {threadActionEffect(run).deleteWarnsLive
                  ? " This stops the thread and removes its worktrees."
                  : ""}
              </span>
              <div>
                <button
                  className="button subtle"
                  onClick={() => setConfirming("")}
                >
                  Cancel
                </button>
                <button
                  className="button danger"
                  disabled={busy === run.id}
                  onClick={() => {
                    onDelete(run);
                    setConfirming("");
                  }}
                >
                  Delete
                </button>
              </div>
            </div>
          );
        }
        return (
          <div
            className={`thread-row ${selectedRunId === run.id ? "active" : ""}`}
            role="listitem"
            key={run.id}
            onContextMenu={(event) => openMenu(event, run)}
          >
            <button
              className="thread-open"
              onClick={() => onSelect(run.id)}
              title={`${run.title} · ${LABELS[posture]}`}
            >
              <Icon size={13} className={`thread-icon thread-${posture}`} />
              <span>{run.title}</span>
              {run.pinned && <Pin size={11} className="thread-pinned" />}
            </button>
            <div className="thread-actions">
              <button
                onClick={() => onPin(run)}
                disabled={busy === run.id}
                title={run.pinned ? "Unpin thread" : "Pin thread"}
                aria-label={run.pinned ? "Unpin thread" : "Pin thread"}
              >
                {run.pinned ? <PinOff size={12} /> : <Pin size={12} />}
              </button>
              <button
                onClick={(event) => openMenu(event, run)}
                disabled={busy === run.id}
                title="Thread actions"
                aria-label="Thread actions"
              >
                <span aria-hidden="true">⋯</span>
              </button>
            </div>
          </div>
        );
      })}
      {menu && (
        <div
          className="thread-menu"
          role="menu"
          style={{ left: menu.x, top: menu.y }}
          onPointerDown={(event) => event.stopPropagation()}
        >
          <div className="thread-menu-title">{menu.run.title}</div>
          <button
            role="menuitem"
            onClick={() => {
              onPin(menu.run);
              setMenu(undefined);
            }}
          >
            {menu.run.pinned ? <PinOff size={13} /> : <Pin size={13} />}
            {menu.run.pinned ? "Unpin" : "Pin"}
          </button>
          <button
            role="menuitem"
            disabled={isArchived(menu.run)}
            title={threadActionEffect(menu.run).archive}
            onClick={() => {
              onArchive(menu.run);
              setMenu(undefined);
            }}
          >
            <Archive size={13} />
            Archive
            {threadActionEffect(menu.run).archive && (
              <small>{threadActionEffect(menu.run).archive}</small>
            )}
          </button>
          <button
            role="menuitem"
            className="thread-menu-danger"
            onClick={() => {
              setConfirming(menu.run.id);
              setMenu(undefined);
            }}
          >
            <Trash2 size={13} />
            Delete
            {threadActionEffect(menu.run).deleteWarnsLive && (
              <small>Stops the thread</small>
            )}
          </button>
        </div>
      )}
    </div>
  );
}

