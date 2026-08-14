import { RefreshCw } from "lucide-react";
import { useCallback, useEffect, useState } from "react";

import { api } from "../api";
import type {
  AttentionItem,
  AttentionPage,
  ControlPlaneSnapshot,
  ExternalCondition,
  InvestigationArtifact,
  ReturnView,
  SnapshotSection,
} from "../types";

export function AttentionCenter() {
  const [snapshot, setSnapshot] = useState<ControlPlaneSnapshot>();
  const [returnView, setReturnView] = useState<ReturnView>();
  const [page, setPage] = useState<AttentionPage>();
  const [investigations, setInvestigations] = useState<InvestigationArtifact[]>([]);
  const [conditions, setConditions] = useState<ExternalCondition[]>([]);
  const [selected, setSelected] = useState<AttentionItem>();
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setBusy("refresh");
    try {
      const [nextSnapshot, nextReturnView, nextPage, nextInvestigations, nextConditions] = await Promise.all([
        api.controlPlaneSnapshot(),
        api.controlPlaneReturnView(),
        api.attention(),
        api.investigations(),
        api.externalConditions(),
      ]);
      setSnapshot(nextSnapshot);
      setReturnView(nextReturnView);
      setPage(nextPage);
      setInvestigations(nextInvestigations);
      setConditions(nextConditions);
      setSelected((current) =>
        current
          ? nextPage.items.find((item) => item.attention_id === current.attention_id)
          : nextPage.items[0],
      );
      setError("");
    } catch (cause) {
      setError(displayError(cause, "Could not load operator-control state."));
    } finally {
      setBusy("");
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const acknowledge = async () => {
    if (!selected || selected.state !== "open") return;
    setBusy(`ack:${selected.attention_id}`);
    try {
      const updated = await api.acknowledgeAttention(
        selected.attention_id,
        selected.version,
      );
      setPage((current) =>
        current && {
          ...current,
          items: current.items.map((item) =>
            item.attention_id === updated.attention_id ? updated : item,
          ),
        },
      );
      setSelected(updated);
      setError("");
    } catch (cause) {
      setError(
        displayError(
          cause,
          "Acknowledgement was not recorded. Refresh before trying again.",
        ),
      );
      await load();
    } finally {
      setBusy("");
    }
  };

  const acknowledgeReturnView = async () => {
    if (!snapshot || !returnView) return;
    setBusy("return");
    try {
      await api.acknowledgeReturnView(
        returnView.snapshot_revision,
        returnView.event_cursor,
      );
      await load();
    } catch (cause) {
      setError(
        displayError(
          cause,
          "Return view changed before acknowledgement. Refresh and try again.",
        ),
      );
    } finally {
      setBusy("");
    }
  };

  return (
    <div className="page control-plane-page">
      <header className="page-title">
        <div>
          <span className="eyebrow">Operator control plane</span>
          <h1>Attention & return view</h1>
        </div>
        <div className="control-plane-actions">
          <SnapshotDisclosure snapshot={snapshot} />
          <button
            className="button secondary"
            type="button"
            onClick={() => void load()}
            disabled={busy === "refresh"}
          >
            <RefreshCw size={14} aria-hidden="true" />
            {busy === "refresh" ? "Refreshing" : "Refresh"}
          </button>
        </div>
      </header>
      {error && <p className="form-error" role="alert">{error}</p>}
      <section className="control-plane-return" aria-labelledby="return-view-heading">
        <div>
          <span className="eyebrow">Return view</span>
          <h2 id="return-view-heading">What changed and what still needs you</h2>
        </div>
        <p>
          Snapshot revision {returnView?.snapshot_revision ?? "unknown"} · acknowledged cursor {returnView?.acknowledged_cursor ?? "unknown"}
        </p>
        <SectionStatus
          name="Controller events since last view"
          section={returnView?.sections.material_changes}
        />
        <EventTimeline section={returnView?.sections.material_changes} />
        <SectionStatus name="Needs action" section={returnView?.sections.attention} />
        <SectionStatus name="Current work" section={returnView?.sections.runs} />
        <SectionStatus name="Active attempts" section={returnView?.sections.attempts} />
        <SectionStatus name="Investigations" section={returnView?.sections.investigations} />
        <SectionStatus name="Recovery" section={returnView?.sections.reconciliation} />
        <SectionStatus name="Waiting & blocked" section={returnView?.sections.liveness} />
        <SectionStatus name="External conditions" section={returnView?.sections.external_conditions} />
        <SectionStatus name="Account capacity" section={returnView?.sections.accounts} />
        <SectionStatus name="Cost" section={returnView?.sections.cost} />
        <SectionStatus name="Limits" section={returnView?.sections.limits} />
        <button
          className="button secondary"
          type="button"
          onClick={() => void acknowledgeReturnView()}
          disabled={!snapshot || !returnView || busy === "return"}
        >
          {busy === "return" ? "Recording…" : "Mark return view seen"}
        </button>
      </section>
      <section className="control-plane-support" aria-label="Investigation artifacts and external conditions">
        <InvestigationArtifacts artifacts={investigations} />
        <ExternalConditions conditions={conditions} />
      </section>
      <div className="control-plane-layout">
        <section className="control-plane-list" aria-labelledby="attention-heading">
          <div className="section-heading">
            <div>
              <span className="eyebrow">Source-owned queue</span>
              <h2 id="attention-heading">Attention</h2>
            </div>
            <span className="count-pill">{page?.items.length ?? 0}</span>
          </div>
          {!page && !error && <p className="empty-state">Loading attention…</p>}
          {page?.items.length === 0 && (
            <p className="empty-state">No active source-owned attention items.</p>
          )}
          <div className="control-plane-items" role="list">
            {page?.items.map((item) => (
              <button
                key={item.attention_id}
                className={`control-plane-item ${selected?.attention_id === item.attention_id ? "selected" : ""}`}
                type="button"
                role="listitem"
                aria-pressed={selected?.attention_id === item.attention_id}
                onClick={() => setSelected(item)}
              >
                <span className={`severity severity-${item.severity}`}>{item.severity}</span>
                <strong>{item.title}</strong>
                <small>{item.category.replaceAll("_", " ")} · {item.state.replaceAll("_", " ")}</small>
              </button>
            ))}
          </div>
          {page?.next_cursor && (
            <p className="control-plane-note">More source-owned items are available; this first slice keeps the current page bounded.</p>
          )}
        </section>
        <AttentionDetail item={selected} busy={busy} onAcknowledge={() => void acknowledge()} />
      </div>
      {snapshot?.truncation.length ? (
        <p className="control-plane-note" role="status">
          Snapshot is bounded: {snapshot.truncation.map((item) => `${item.section} omitted ${item.omitted_rows}`).join(" · ")}
        </p>
      ) : null}
    </div>
  );
}

function InvestigationArtifacts({ artifacts }: { artifacts: InvestigationArtifact[] }) {
  return (
    <section className="control-plane-support-card" aria-labelledby="investigations-heading">
      <span className="eyebrow">Immutable evidence</span>
      <h2 id="investigations-heading">Investigations</h2>
      {artifacts.length === 0 ? (
        <p className="empty-state">No recorded investigation artifacts.</p>
      ) : (
        <ul className="control-plane-support-list">
          {artifacts.map((artifact) => (
            <li key={artifact.artifact_id}>
              <strong>{artifact.question}</strong>
              <span>{artifact.findings.length} findings · {artifact.recommendations.length} recommendations</span>
              <small>
                {localTime(artifact.created_at_ms)} · {artifact.sensitivity} · {artifact.base_sha.slice(0, 12)}
              </small>
            </li>
          ))}
        </ul>
      )}
      <p className="control-plane-note">Artifacts are evidence records. They cannot create implementation work or grant mutable custody.</p>
    </section>
  );
}

function ExternalConditions({ conditions }: { conditions: ExternalCondition[] }) {
  return (
    <section className="control-plane-support-card" aria-labelledby="conditions-heading">
      <span className="eyebrow">Passive waits</span>
      <h2 id="conditions-heading">External conditions</h2>
      {conditions.length === 0 ? (
        <p className="empty-state">No active external conditions.</p>
      ) : (
        <ul className="control-plane-support-list">
          {conditions.map((condition) => (
            <li key={condition.condition_id}>
              <strong>{condition.adapter.replaceAll("_", " ")} · {condition.state}</strong>
              <span>{condition.owner_type}:{condition.owner_id} · sequence {condition.sequence}</span>
              <small>{condition.last_observation ? localTime(condition.last_observation.observed_at_ms) : "No observation recorded"}</small>
            </li>
          ))}
        </ul>
      )}
      <p className="control-plane-note">These are stored observations only. This view does not poll a provider, wake work, or execute a result.</p>
    </section>
  );
}

function AttentionDetail({
  item,
  busy,
  onAcknowledge,
}: {
  item?: AttentionItem;
  busy: string;
  onAcknowledge: () => void;
}) {
  if (!item) {
    return <aside className="control-plane-detail"><p className="empty-state">Select an attention item to inspect its source and evidence.</p></aside>;
  }
  return (
    <aside className="control-plane-detail" aria-live="polite">
      <span className={`severity severity-${item.severity}`}>{item.severity}</span>
      <h2>{item.title}</h2>
      <p>{item.summary}</p>
      <dl className="control-plane-facts">
        <div><dt>State</dt><dd>{item.state.replaceAll("_", " ")}</dd></div>
        <div><dt>Source</dt><dd className="mono">{item.source.source_type}:{item.source.source_id} r{item.source.source_revision}</dd></div>
        <div><dt>Blocked refs</dt><dd>{item.blocked_refs.length ? item.blocked_refs.join(", ") : "None recorded"}</dd></div>
        <div><dt>Evidence refs</dt><dd>{item.evidence_refs.length ? item.evidence_refs.join(", ") : "None recorded"}</dd></div>
      </dl>
      {item.state === "open" && (
        <button
          className="button secondary"
          type="button"
          onClick={onAcknowledge}
          disabled={busy === `ack:${item.attention_id}`}
        >
          {busy === `ack:${item.attention_id}` ? "Recording…" : "Mark seen"}
        </button>
      )}
      <p className="control-plane-note">
        Mark seen only records acknowledgement. It does not resolve this item, approve work, or resume execution.
      </p>
    </aside>
  );
}

function SnapshotDisclosure({ snapshot }: { snapshot?: ControlPlaneSnapshot }) {
  if (!snapshot) return <span className="control-plane-note">Snapshot unavailable</span>;
  const unknown = Object.values(snapshot).filter(
    (value): value is SnapshotSection => typeof value === "object" && value !== null && "state" in value && (value as SnapshotSection).state === "unknown",
  ).length;
  return (
    <span className="control-plane-note" title={snapshot.sha256}>
      rev {snapshot.revision} · cursor {snapshot.event_cursor} · {unknown} unknown sections
    </span>
  );
}

function SectionStatus({ name, section }: { name: string; section?: SnapshotSection }) {
  return (
    <div className={`return-section return-${section?.state ?? "unknown"}`}>
      <strong>{name}</strong>
      <span>{section ? `${section.state} · ${section.rows.length} rows${section.truncated ? " · bounded" : ""}` : "unknown"}</span>
      {section?.detail && <small>{section.detail}</small>}
    </div>
  );
}

function EventTimeline({ section }: { section?: SnapshotSection }) {
  if (!section?.rows.length) return null;
  return (
    <ol className="control-plane-timeline" aria-label="Controller events since the last acknowledged view">
      {section.rows.map((row, index) => (
        <li key={`${text(row.event_id)}-${index}`}>
          <time dateTime={timestamp(row.occurred_at_ms)}>{localTime(row.occurred_at_ms)}</time>
          <span>{text(row.event_type)}</span>
          <small className="mono">{text(row.aggregate_type)}:{text(row.aggregate_id)}</small>
        </li>
      ))}
    </ol>
  );
}

function text(value: unknown) {
  return typeof value === "string" || typeof value === "number" ? String(value) : "unknown";
}

function timestamp(value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? new Date(value).toISOString()
    : undefined;
}

function localTime(value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? new Date(value).toLocaleString()
    : "Time unavailable";
}

function displayError(cause: unknown, fallback: string) {
  return cause instanceof Error ? cause.message : fallback;
}
