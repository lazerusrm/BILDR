import { useCallback, useEffect, useState } from "react";
import { api } from "../api";
import type { AttentionItem } from "../types";
import { humanizeRefs, refLabel, type RefLabels } from "./refs";

const ACTIONABLE = new Set(["open", "acknowledged", "waiting_external"]);

/**
 * Attention an operator can still act on.
 *
 * Terminal states are dropped, and so is anything whose run has been archived
 * or no longer exists: an item that outlives its run cannot be acted on, and
 * asking for action on work that is gone is worse than showing nothing.
 * `liveRunIds` being undefined means the run list has not loaded yet, so items
 * are kept rather than briefly hidden.
 */
export function actionableAttention(
  items: AttentionItem[],
  liveRunIds?: ReadonlySet<string>,
) {
  return items.filter((item) => {
    if (!ACTIONABLE.has(item.state)) return false;
    if (!liveRunIds || !item.run_id) return true;
    return liveRunIds.has(item.run_id);
  });
}

/**
 * Turn a controller category into the action it asks of the operator. The
 * backend title is a source path, which reads as noise on a landing screen.
 */
export function attentionHeadline(item: AttentionItem) {
  const known: Record<string, string> = {
    command_approval: "Approve a command",
    file_approval: "Approve a file write",
    external_write_approval: "Approve an external write",
    plan_review: "Review the plan",
    signoff: "Sign off the result",
    publication: "Approve publication",
    waiting_external: "Waiting on an external check",
    credential_requirement: "Credentials needed",
    evidence_gap: "Evidence is missing",
    reconciliation: "Ownership needs reconciling",
    infrastructure: "Infrastructure needs attention",
  };
  return (
    known[item.category] ||
    item.category.replaceAll("_", " ").replace(/^./, (c) => c.toUpperCase())
  );
}

/**
 * Source-owned attention, rendered where the operator lands. Acknowledging
 * records that the item was seen; it never resolves, approves, or resumes.
 */
export function NeedsYou({
  labels,
  liveRunIds,
  onOpenRun,
}: {
  labels: RefLabels;
  liveRunIds?: ReadonlySet<string>;
  onOpenRun: (runId: string) => void;
}) {
  const [items, setItems] = useState<AttentionItem[]>();
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    try {
      const page = await api.attention();
      setItems(page.items);
      setError("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Attention unavailable");
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 15_000);
    return () => window.clearInterval(timer);
  }, [load]);

  const acknowledge = async (item: AttentionItem) => {
    setBusy(item.attention_id);
    try {
      const next = await api.acknowledgeAttention(item.attention_id, item.version);
      setItems((current) =>
        current?.map((candidate) =>
          candidate.attention_id === next.attention_id ? next : candidate,
        ),
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not record acknowledgement");
    } finally {
      setBusy("");
    }
  };

  if (error) {
    return (
      <div className="needs-you-error" role="status">
        Attention records unavailable · {error}
      </div>
    );
  }
  const visible = items ? actionableAttention(items, liveRunIds) : undefined;
  if (!visible?.length) return null;

  return (
    <div className="stack" role="list" aria-label="Needs you">
      {visible.map((item) => {
        const where = item.run_id ? refLabel(item.run_id, labels) : undefined;
        const blocks = item.blocked_refs
          .map((ref) => refLabel(ref, labels))
          .join(", ");
        return (
          <div className="attention-row" role="listitem" key={item.attention_id}>
            <span className={`severity severity-${item.severity}`}>
              {item.severity}
            </span>
            <div>
              <strong>{attentionHeadline(item)}</strong>
              {where && <span className="attention-where">{where}</span>}
              <span>{humanizeRefs(item.summary, labels)}</span>
              {item.state === "waiting_external" ? (
                <small>Waiting on an external condition</small>
              ) : (
                blocks && <small>Blocks {blocks}</small>
              )}
            </div>
            <div className="attention-row-actions">
              {item.state === "open" && (
                <button
                  className="button subtle"
                  onClick={() => void acknowledge(item)}
                  disabled={busy === item.attention_id}
                  title="Records that you saw this. It does not resolve or approve anything."
                >
                  {busy === item.attention_id ? "Recording…" : "Mark seen"}
                </button>
              )}
              {item.run_id && (
                <button
                  className="button"
                  onClick={() => onOpenRun(item.run_id as string)}
                >
                  Open run
                </button>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
