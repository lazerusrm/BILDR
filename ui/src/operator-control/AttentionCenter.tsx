import { RefreshCw } from "lucide-react";
import { type ReactNode, useCallback, useEffect, useRef, useState } from "react";

import { ApiRequestError, api } from "../api";
import type {
  AttentionItem,
  AttentionPage,
  ConditionObservation,
  ControlPlaneSnapshot,
  ExternalCondition,
  ExternalConditionSummary,
  InvestigationArtifact,
  InvestigationArtifactSummary,
  InterventionReceipt,
  LivenessEpisode,
  MaterialProgressEvent,
  NotificationDelivery,
  NotificationDeliveryHealth,
  NotificationShadowBatch,
  OperatorPresence,
  OperatorPresenceMode,
  ReturnView,
  SnapshotSection,
  TopologySnapshot,
} from "../types";

export function AttentionCenter() {
  const [snapshot, setSnapshot] = useState<ControlPlaneSnapshot>();
  const [returnView, setReturnView] = useState<ReturnView>();
  const [page, setPage] = useState<AttentionPage>();
  const [investigations, setInvestigations] = useState<InvestigationArtifactSummary[]>([]);
  const [conditions, setConditions] = useState<ExternalConditionSummary[]>([]);
  const [progress, setProgress] = useState<MaterialProgressEvent[]>([]);
  const [liveness, setLiveness] = useState<LivenessEpisode[]>([]);
  const [presence, setPresence] = useState<OperatorPresence>();
  const [notificationDeliveries, setNotificationDeliveries] = useState<NotificationDelivery[]>([]);
  const [notificationHealth, setNotificationHealth] = useState<NotificationDeliveryHealth>();
  const [notificationShadowBatches, setNotificationShadowBatches] = useState<NotificationShadowBatch[]>([]);
  const [selectedLivenessEpisodeId, setSelectedLivenessEpisodeId] = useState<string>();
  const [interventionHistory, setInterventionHistory] = useState<InterventionHistory>({
    episodeId: undefined,
    state: "idle",
    receipts: [],
  });
  const [topology, setTopology] = useState<TopologySnapshot>();
  const [topologyRunId, setTopologyRunId] = useState<string>();
  const [selectedInvestigationId, setSelectedInvestigationId] = useState<string>();
  const [selectedConditionId, setSelectedConditionId] = useState<string>();
  const [conditionHistory, setConditionHistory] = useState<ConditionHistory>({
    conditionId: undefined,
    state: "idle",
    observations: [],
  });
  const [investigationDetail, setInvestigationDetail] = useState<InvestigationDetailState>({
    artifactId: undefined,
    state: "idle",
  });
  const [conditionDetail, setConditionDetail] = useState<ExternalConditionDetailState>({
    conditionId: undefined,
    state: "idle",
  });
  const [selected, setSelected] = useState<AttentionItem>();
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const conditionHistoryRequest = useRef(0);
  const interventionHistoryRequest = useRef(0);
  const investigationDetailRequest = useRef(0);
  const conditionDetailRequest = useRef(0);

  const load = useCallback(async () => {
    setBusy("refresh");
    try {
      const [nextSnapshot, nextReturnView, nextPage, nextInvestigations, nextConditions, nextProgress, nextLiveness, nextPresence, nextNotificationDeliveries, nextNotificationHealth, nextNotificationShadowBatches] = await Promise.all([
        api.controlPlaneSnapshot(),
        api.controlPlaneReturnView(),
        api.attention(),
        api.investigations(),
        api.externalConditions(),
        api.materialProgress(),
        api.liveness(),
        api.operatorPresence().catch((cause: unknown) => {
          if (cause instanceof ApiRequestError && cause.status === 404) return undefined;
          throw cause;
        }),
        api.notificationDeliveries(),
        api.notificationDeliveryHealth(),
        api.notificationShadowBatches(),
      ]);
      setSnapshot(nextSnapshot);
      setReturnView(nextReturnView);
      setPage(nextPage);
      setInvestigations(nextInvestigations);
      setConditions(nextConditions);
      setProgress(nextProgress);
      setLiveness(nextLiveness);
      setPresence(nextPresence);
      setNotificationDeliveries(nextNotificationDeliveries);
      setNotificationHealth(nextNotificationHealth);
      setNotificationShadowBatches(nextNotificationShadowBatches);
      const runIds = nextSnapshot.runs.rows
        .map((row) => typeof row.run_id === "string" ? row.run_id : undefined)
        .filter((value): value is string => Boolean(value));
      setTopologyRunId((current) => current && runIds.includes(current) ? current : runIds[0]);
      setTopology(undefined);
      conditionHistoryRequest.current += 1;
      setConditionHistory({ conditionId: undefined, state: "idle", observations: [] });
      interventionHistoryRequest.current += 1;
      setInterventionHistory({ episodeId: undefined, state: "idle", receipts: [] });
      conditionDetailRequest.current += 1;
      setConditionDetail({ conditionId: undefined, state: "idle" });
      investigationDetailRequest.current += 1;
      setInvestigationDetail({ artifactId: undefined, state: "idle" });
      setSelectedInvestigationId((current) =>
        current && nextInvestigations.some((artifact) => artifact.artifact_id === current)
          ? current
          : nextInvestigations[0]?.artifact_id,
      );
      setSelectedConditionId((current) =>
        current && nextConditions.some((condition) => condition.condition_id === current)
          ? current
          : nextConditions[0]?.condition_id,
      );
      setSelectedLivenessEpisodeId((current) =>
        current && nextLiveness.some((episode) => episode.episode_id === current)
          ? current
          : nextLiveness[0]?.episode_id,
      );
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

  const selectInvestigation = useCallback(async (artifactId: string) => {
    const request = investigationDetailRequest.current + 1;
    investigationDetailRequest.current = request;
    setSelectedInvestigationId(artifactId);
    setInvestigationDetail({ artifactId, state: "loading" });
    try {
      const artifact = await api.investigation(artifactId);
      if (investigationDetailRequest.current === request) {
        setInvestigationDetail({ artifactId, state: "loaded", artifact });
      }
    } catch (cause) {
      if (investigationDetailRequest.current === request) {
        setInvestigationDetail({ artifactId, state: "error" });
        setError(displayError(cause, "Could not load immutable investigation evidence."));
      }
    }
  }, []);

  const selectCondition = useCallback(async (conditionId: string) => {
    const request = conditionDetailRequest.current + 1;
    conditionDetailRequest.current = request;
    setSelectedConditionId(conditionId);
    conditionHistoryRequest.current += 1;
    setConditionHistory({ conditionId: undefined, state: "idle", observations: [] });
    setConditionDetail({ conditionId, state: "loading" });
    try {
      const condition = await api.externalCondition(conditionId);
      if (conditionDetailRequest.current === request) {
        setConditionDetail({ conditionId, state: "loaded", condition });
      }
    } catch (cause) {
      if (conditionDetailRequest.current === request) {
        setConditionDetail({ conditionId, state: "error" });
        setError(displayError(cause, "Could not load condition detail."));
      }
    }
  }, []);

  const loadConditionHistory = useCallback(async (conditionId: string) => {
    const request = conditionHistoryRequest.current + 1;
    conditionHistoryRequest.current = request;
    setConditionHistory({ conditionId, state: "loading", observations: [] });
    try {
      const observations = await api.conditionObservations(conditionId);
      if (conditionHistoryRequest.current === request) {
        setConditionHistory({ conditionId, state: "loaded", observations });
      }
    } catch (cause) {
      if (conditionHistoryRequest.current === request) {
        setConditionHistory({ conditionId, state: "error", observations: [] });
        setError(displayError(cause, "Could not load the recorded condition history."));
      }
    }
  }, []);

  const selectLivenessEpisode = useCallback(async (episodeId: string) => {
    const request = interventionHistoryRequest.current + 1;
    interventionHistoryRequest.current = request;
    setSelectedLivenessEpisodeId(episodeId);
    setInterventionHistory({ episodeId, state: "loading", receipts: [] });
    try {
      const receipts = await api.interventionReceipts(episodeId);
      if (interventionHistoryRequest.current === request) {
        setInterventionHistory({ episodeId, state: "loaded", receipts });
      }
    } catch (cause) {
      if (interventionHistoryRequest.current === request) {
        setInterventionHistory({ episodeId, state: "error", receipts: [] });
        setError(displayError(cause, "Could not load immutable intervention receipts."));
      }
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

  const loadTopology = useCallback(async (runId: string) => {
    setTopologyRunId(runId);
    setTopology(undefined);
    try {
      setTopology(await api.topology(runId));
    } catch (cause) {
      setError(displayError(cause, "Could not load the bounded run topology."));
    }
  }, []);

  const updatePresence = async (mode: OperatorPresenceMode) => {
    if (presence?.mode === mode) return;
    setBusy("presence");
    try {
      const updated = await api.setOperatorPresence(mode, presence?.version ?? 0);
      setPresence(updated);
      setError("");
    } catch (cause) {
      setError(displayError(cause, "Presence changed elsewhere. Refresh before trying again."));
      await load();
    } finally {
      setBusy("");
    }
  };

  const recordNotificationShadowBatch = async () => {
    if (!presence) return;
    setBusy("notification-shadow");
    try {
      await api.createNotificationShadowBatch(presence.version);
      await load();
    } catch (cause) {
      setError(displayError(cause, "The shadow plan could not be recorded. Refresh before trying again."));
    } finally {
      setBusy("");
    }
  };

  const pauseSchedulerForLivenessEpisode = async (episode: LivenessEpisode) => {
    if (!canPauseSchedulerForLivenessEpisode(episode)) return;
    setBusy(`liveness-pause:${episode.episode_id}`);
    try {
      const updated = await api.pauseSchedulerForLivenessEpisode(
        episode.episode_id,
        episode.version,
      );
      setLiveness((current) =>
        current.map((item) => item.episode_id === updated.episode_id ? updated : item),
      );
      setError("");
      await selectLivenessEpisode(updated.episode_id);
    } catch (cause) {
      setError(displayError(cause, "The liveness episode changed before the scheduler pause was recorded. Refresh before trying again."));
      await load();
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
      <section className="control-plane-support" aria-label="Operator control records">
        <PresenceAndNotifications
          presence={presence}
          deliveries={notificationDeliveries}
          health={notificationHealth}
          shadowBatches={notificationShadowBatches}
          busy={busy === "presence" || busy === "notification-shadow"}
          onPresence={(mode) => void updatePresence(mode)}
          onRecordShadowBatch={() => void recordNotificationShadowBatch()}
        />
        <MaterialProgressTimeline events={progress} />
        <LivenessEpisodes
          episodes={liveness}
          selectedEpisodeId={selectedLivenessEpisodeId}
          history={interventionHistory}
          busy={busy}
          onSelect={(episodeId) => void selectLivenessEpisode(episodeId)}
          onPauseScheduler={(episode) => void pauseSchedulerForLivenessEpisode(episode)}
        />
        <RunTopology
          runIds={snapshot?.runs.rows.map((row) => text(row.run_id)).filter((id) => id !== "unknown") ?? []}
          selectedRunId={topologyRunId}
          topology={topology}
          onSelect={(runId) => void loadTopology(runId)}
        />
        <InvestigationArtifacts
          artifacts={investigations}
          selectedId={selectedInvestigationId}
          detail={investigationDetail}
          onSelect={(artifactId) => void selectInvestigation(artifactId)}
        />
        <ExternalConditions
          conditions={conditions}
          selectedId={selectedConditionId}
          detail={conditionDetail}
          history={conditionHistory}
          onSelect={(conditionId) => void selectCondition(conditionId)}
          onLoadHistory={(conditionId) => void loadConditionHistory(conditionId)}
        />
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

function PresenceAndNotifications({
  presence,
  deliveries,
  health,
  shadowBatches,
  busy,
  onPresence,
  onRecordShadowBatch,
}: {
  presence?: OperatorPresence;
  deliveries: NotificationDelivery[];
  health?: NotificationDeliveryHealth;
  shadowBatches: NotificationShadowBatch[];
  busy: boolean;
  onPresence: (mode: OperatorPresenceMode) => void;
  onRecordShadowBatch: () => void;
}) {
  return (
    <section className="control-plane-support-card" aria-labelledby="notification-heading">
      <span className="eyebrow">Presentation only</span>
      <h2 id="notification-heading">Delivery mirror</h2>
      {!presence ? (
        <>
          <p className="empty-state">No presence preference has been configured for this session.</p>
          <div className="control-plane-run-picker" aria-label="Configure local presence preference">
            {(["interactive", "focus", "unattended"] as const).map((mode) => (
              <button key={mode} className="button secondary" type="button" disabled={busy} onClick={() => onPresence(mode)}>Use {mode}</button>
            ))}
          </div>
        </>
      ) : (
        <>
          <p className="control-plane-note">Local presence is currently <strong>{presence.mode}</strong>. It is version {presence.version} and does not change controller authority.</p>
          <div className="control-plane-run-picker" aria-label="Set local presence preference">
            {(["interactive", "focus", "unattended"] as const).map((mode) => (
              <button key={mode} className={`button secondary ${presence.mode === mode ? "selected" : ""}`} type="button" aria-pressed={presence.mode === mode} disabled={busy} onClick={() => onPresence(mode)}>{mode}</button>
            ))}
          </div>
          <button className="button secondary" type="button" disabled={busy} onClick={onRecordShadowBatch}>Record shadow plan</button>
        </>
      )}
      {deliveries.length === 0 ? <p className="empty-state">No visible attention has been mirrored yet.</p> : (
        <ul className="control-plane-support-list">
          {deliveries.map((delivery) => <li key={delivery.delivery_id}><div className="control-plane-static-record"><strong>{delivery.class.replaceAll("_", " ")}</strong><span>{delivery.state} · {delivery.channel.replaceAll("_", " ")}</span><small>{localTime(delivery.created_at_ms)} · {delivery.source_event_id}</small></div></li>)}
        </ul>
      )}
      {!health ? <p className="empty-state">Loading current delivery health…</p> : (
        <p className="control-plane-note">
          Mirror health examined <strong>{health.examined_current_revisions}</strong> of <strong>{health.current_attention_revisions}</strong> current attention revisions: <strong>{health.delivered_examined_revisions}</strong> verified receipt{health.delivered_examined_revisions === 1 ? "" : "s"} and <strong>{health.undelivered_examined_revisions}</strong> not currently verified.
          {health.undelivered_critical_examined_revisions > 0 ? ` ${health.undelivered_critical_examined_revisions} critical revision${health.undelivered_critical_examined_revisions === 1 ? " is" : "s are"} not verified.` : ""}
          {health.failed_examined_revisions > 0 ? ` ${health.failed_examined_revisions} examined receipt${health.failed_examined_revisions === 1 ? " is" : "s are"} recorded failed.` : ""}
          {health.truncated ? " Results are bounded; unexamined current revisions are unknown." : ""}
        </p>
      )}
      {shadowBatches[0] ? (
        <p className="control-plane-note">
          Latest shadow plan <strong>{shadowBatches[0].batch_id}</strong> covers <strong>{shadowBatches[0].entries.length}</strong> current revisions at presence version {shadowBatches[0].presence.version}. Critical entries remain immediate; this is comparison evidence only.
        </p>
      ) : <p className="control-plane-note">No shadow plan has been recorded for this local presence preference.</p>}
      <p className="control-plane-note">The immediate in-product mirror remains active. Shadow plans do not batch, suppress, send a desktop alert, or close the source attention item.</p>
    </section>
  );
}

function MaterialProgressTimeline({ events }: { events: MaterialProgressEvent[] }) {
  return (
    <section className="control-plane-support-card" aria-labelledby="progress-heading">
      <span className="eyebrow">Classified facts</span>
      <h2 id="progress-heading">Material progress</h2>
      {events.length === 0 ? (
        <p className="empty-state">No material progress has been classified.</p>
      ) : (
        <ul className="control-plane-support-list">
          {events.map((event) => (
            <li key={event.event_id}>
              <div className="control-plane-static-record">
                <strong>{event.kind.replaceAll("_", " ")}</strong>
                <span>{event.summary}</span>
                <small>{localTime(event.occurred_at_ms)} · {event.task_id ?? event.run_id ?? "system"}</small>
              </div>
            </li>
          ))}
        </ul>
      )}
      <p className="control-plane-note">Only a closed controller-event allow-list appears here. Output, token use, and repeated commands are not progress.</p>
    </section>
  );
}

type InterventionHistory = {
  episodeId?: string;
  state: "idle" | "loading" | "loaded" | "error";
  receipts: InterventionReceipt[];
};

function LivenessEpisodes({
  episodes,
  selectedEpisodeId,
  history,
  busy,
  onSelect,
  onPauseScheduler,
}: {
  episodes: LivenessEpisode[];
  selectedEpisodeId?: string;
  history: InterventionHistory;
  busy: string;
  onSelect: (episodeId: string) => void;
  onPauseScheduler: (episode: LivenessEpisode) => void;
}) {
  const selectedEpisode = episodes.find((episode) => episode.episode_id === selectedEpisodeId);
  return (
    <section className="control-plane-support-card" aria-labelledby="liveness-heading">
      <span className="eyebrow">Observe only</span>
      <h2 id="liveness-heading">Liveness episodes</h2>
      {episodes.length === 0 ? (
        <p className="empty-state">No liveness episodes are recorded.</p>
      ) : (
        <ul className="control-plane-support-list">
          {episodes.map((episode) => (
            <li key={episode.episode_id}>
              <button
                className={`control-plane-support-item ${selectedEpisodeId === episode.episode_id ? "selected" : ""}`}
                type="button"
                aria-pressed={selectedEpisodeId === episode.episode_id}
                onClick={() => onSelect(episode.episode_id)}
              >
                <strong>{episode.state.replaceAll("_", " ")}</strong>
                <span>{episode.task_id ?? episode.attempt_id ?? episode.run_id ?? "Unscoped episode"}</span>
                <small>{localTime(episode.updated_at_ms)} · {episode.intervention_count} recorded interventions · {episode.state_reason_codes.join(", ") || "No reason code"}</small>
              </button>
            </li>
          ))}
        </ul>
      )}
      <InterventionHistoryPanel episodeId={selectedEpisodeId} history={history} onLoad={onSelect} />
      {selectedEpisode && canPauseSchedulerForLivenessEpisode(selectedEpisode) && (
        <button
          className="button danger"
          type="button"
          onClick={() => onPauseScheduler(selectedEpisode)}
          disabled={busy === `liveness-pause:${selectedEpisode.episode_id}`}
        >
          {busy === `liveness-pause:${selectedEpisode.episode_id}` ? "Recording scheduler pause…" : "Pause this run's scheduler"}
        </button>
      )}
      <p className="control-plane-note">Observation remains read-only except for an explicit exact-revision pause on a selected confirmed-stall or recovery-required episode. That pause cannot retry, resume, release, or change an attempt.</p>
    </section>
  );
}

function canPauseSchedulerForLivenessEpisode(episode: LivenessEpisode) {
  return episode.state === "confirmed_stall" || episode.state === "recovery_required";
}

function InterventionHistoryPanel({
  episodeId,
  history,
  onLoad,
}: {
  episodeId?: string;
  history: InterventionHistory;
  onLoad: (episodeId: string) => void;
}) {
  if (!episodeId) return null;
  const current = history.episodeId === episodeId ? history : { state: "idle", receipts: [] };
  return (
    <div className="control-plane-detail" aria-label="Intervention receipt history">
      <div className="section-heading"><h3>Intervention receipts</h3>{current.state === "idle" && <button className="button secondary" type="button" onClick={() => onLoad(episodeId)}>Load receipts</button>}</div>
      {current.state === "loading" && <p className="empty-state">Loading immutable receipts…</p>}
      {current.state === "error" && <p className="form-error">Could not load intervention receipts.</p>}
      {current.state === "loaded" && current.receipts.length === 0 && <p className="empty-state">No intervention receipt is recorded for this episode.</p>}
      {current.state === "loaded" && current.receipts.length > 0 && (
        <ul className="control-plane-support-list">
          {current.receipts.map((receipt) => <li key={receipt.intervention_id}><div className="control-plane-static-record"><strong>{receipt.kind.replaceAll("_", " ")}</strong><span>{receipt.requested_by} · policy {receipt.policy_version}</span><small>{localTime(receipt.created_at_ms)} · revision {receipt.target_version} · {receipt.source_event_id}</small></div></li>)}
        </ul>
      )}
      <p className="control-plane-note">Receipts prove a completed controller-path action against an exact episode revision; this page cannot request or replay one.</p>
    </div>
  );
}

function RunTopology({
  runIds,
  selectedRunId,
  topology,
  onSelect,
}: {
  runIds: string[];
  selectedRunId?: string;
  topology?: TopologySnapshot;
  onSelect: (runId: string) => void;
}) {
  return (
    <section className="control-plane-support-card" aria-labelledby="topology-heading">
      <span className="eyebrow">Bounded table</span>
      <h2 id="topology-heading">Run topology</h2>
      {runIds.length === 0 ? (
        <p className="empty-state">No current run is available for topology inspection.</p>
      ) : (
        <>
          <div className="control-plane-run-picker" aria-label="Select run topology">
            {runIds.map((runId) => (
              <button key={runId} className={`button secondary ${selectedRunId === runId ? "selected" : ""}`} type="button" onClick={() => onSelect(runId)}>
                {runId}
              </button>
            ))}
          </div>
          {!topology ? <p className="control-plane-note">Select a run to load its factual ownership and dependency table.</p> : (
            <>
              <p className="control-plane-note">Cursor {topology.source_cursor} · {topology.nodes.length} nodes · {topology.edges.length} edges</p>
              <ul className="control-plane-support-list">
                {topology.nodes.map((node) => <li key={node.id}><div className="control-plane-static-record"><strong>{node.kind}</strong><span className="mono">{node.source_ref}</span></div></li>)}
              </ul>
            </>
          )}
        </>
      )}
      <p className="control-plane-note">This table is a read-only projection. Layout, inferred links, and controller actions are intentionally absent.</p>
    </section>
  );
}

type ConditionHistory = {
  conditionId?: string;
  state: "idle" | "loading" | "loaded" | "error";
  observations: ConditionObservation[];
};

type InvestigationDetailState = {
  artifactId?: string;
  state: "idle" | "loading" | "loaded" | "error";
  artifact?: InvestigationArtifact;
};

type ExternalConditionDetailState = {
  conditionId?: string;
  state: "idle" | "loading" | "loaded" | "error";
  condition?: ExternalCondition;
};

const DETAIL_ITEM_LIMIT = 20;

function InvestigationArtifacts({
  artifacts,
  selectedId,
  detail,
  onSelect,
}: {
  artifacts: InvestigationArtifactSummary[];
  selectedId?: string;
  detail: InvestigationDetailState;
  onSelect: (artifactId: string) => void;
}) {
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
              <button
                className={`control-plane-support-item ${selectedId === artifact.artifact_id ? "selected" : ""}`}
                type="button"
                aria-pressed={selectedId === artifact.artifact_id}
                onClick={() => onSelect(artifact.artifact_id)}
              >
                <strong>{artifact.question}</strong>
                <span>{artifact.finding_count} findings · {artifact.recommendation_count} recommendations</span>
                <small>
                  {localTime(artifact.created_at_ms)} · {artifact.sensitivity} · {artifact.base_sha.slice(0, 12)}
                </small>
              </button>
            </li>
          ))}
        </ul>
      )}
      <InvestigationDetail artifactId={selectedId} detail={detail} onLoad={onSelect} />
      <p className="control-plane-note">Artifacts are evidence records. They cannot create implementation work or grant mutable custody.</p>
    </section>
  );
}

function ExternalConditions({
  conditions,
  selectedId,
  detail,
  history,
  onSelect,
  onLoadHistory,
}: {
  conditions: ExternalConditionSummary[];
  selectedId?: string;
  detail: ExternalConditionDetailState;
  history: ConditionHistory;
  onSelect: (conditionId: string) => void;
  onLoadHistory: (conditionId: string) => void;
}) {
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
              <button
                className={`control-plane-support-item ${selectedId === condition.condition_id ? "selected" : ""}`}
                type="button"
                aria-pressed={selectedId === condition.condition_id}
                onClick={() => onSelect(condition.condition_id)}
              >
                <strong>{condition.adapter.replaceAll("_", " ")} · {condition.state}</strong>
                <span>{condition.owner_type}:{condition.owner_id} · sequence {condition.sequence}</span>
                <small>{condition.last_observed_at_ms !== null ? localTime(condition.last_observed_at_ms) : "No observation recorded"}</small>
              </button>
            </li>
          ))}
        </ul>
      )}
      <ExternalConditionDetail
        conditionId={selectedId}
        detail={detail}
        history={history}
        onLoadDetail={onSelect}
        onLoadHistory={onLoadHistory}
      />
      <p className="control-plane-note">These are stored observations only. This view does not poll a provider, wake work, or execute a result.</p>
    </section>
  );
}

export function InvestigationDetail({
  artifactId,
  detail,
  onLoad,
}: {
  artifactId?: string;
  detail: InvestigationDetailState;
  onLoad: (artifactId: string) => void;
}) {
  if (!artifactId) {
    return <p className="control-plane-empty-detail">Select an investigation to inspect its bounded evidence and decisions.</p>;
  }
  if (detail.artifactId !== artifactId || detail.state === "idle") {
    return <button className="button secondary" type="button" onClick={() => onLoad(artifactId)}>Load immutable evidence detail</button>;
  }
  if (detail.state === "loading") {
    return <p className="control-plane-empty-detail">Loading immutable evidence detail…</p>;
  }
  if (detail.state === "error" || !detail.artifact) {
    return <button className="button secondary" type="button" onClick={() => onLoad(artifactId)}>Retry immutable evidence detail</button>;
  }
  const artifact = detail.artifact;
  return (
    <section className="control-plane-record-detail" aria-live="polite" aria-label="Selected investigation detail">
      <h3>Selected investigation</h3>
      <dl className="control-plane-facts compact">
        <div><dt>Artifact</dt><dd className="mono">{artifact.artifact_id}</dd></div>
        <div><dt>Run / task / attempt</dt><dd className="mono">{artifact.run_id} / {artifact.task_id} / {artifact.attempt_id}</dd></div>
        <div><dt>Scope</dt><dd>{artifact.scope.owned_read_paths.join(", ")}</dd></div>
        <div><dt>Forbidden paths</dt><dd>{artifact.scope.forbidden_paths.length ? artifact.scope.forbidden_paths.join(", ") : "None recorded"}</dd></div>
        <div><dt>Budget</dt><dd>{artifact.scope.time_budget_ms.toLocaleString()} ms · {artifact.scope.token_budget.toLocaleString()} tokens</dd></div>
      </dl>
      <DetailList
        heading="Findings"
        empty="No findings recorded."
        values={artifact.findings.map((finding) => (
          <><strong>{finding.classification} · {finding.confidence_milli / 10}% · {finding.risk}</strong><span>{finding.summary}</span><small>Evidence: {joinOrNone(finding.evidence_refs)} · Affected: {joinOrNone(finding.affected_refs)}</small></>
        ))}
      />
      <DetailList
        heading="Recommendations"
        empty="No recommendations recorded."
        values={artifact.recommendations.map((recommendation) => (
          <><strong>{recommendation.required_authority} · {recommendation.risk}</strong><span>{recommendation.summary}</span><small>Next verification: {recommendation.next_verification}</small></>
        ))}
      />
      <DetailList
        heading="Decision inventory"
        empty="No unresolved decisions recorded."
        values={artifact.decision_inventory.map((decision) => (
          <><strong>{decision.required_actor} · {decision.state}</strong><span>{decision.question}</span><small>{decision.independent_work_can_continue ? "Independent work may continue" : "Blocks independent work"} · {decision.recommended_option ? `Recommended: ${decision.recommended_option}` : "No recommendation"}</small></>
        ))}
      />
      <DetailList heading="Limitations" empty="No limitations recorded." values={artifact.limitations.map((value) => <span>{value}</span>)} />
      <DetailList heading="Rejected hypotheses" empty="No rejected hypotheses recorded." values={artifact.rejected_hypotheses.map((value) => <span>{value}</span>)} />
    </section>
  );
}

export function ExternalConditionDetail({
  conditionId,
  detail,
  history,
  onLoadDetail,
  onLoadHistory,
}: {
  conditionId?: string;
  detail: ExternalConditionDetailState;
  history: ConditionHistory;
  onLoadDetail: (conditionId: string) => void;
  onLoadHistory: (conditionId: string) => void;
}) {
  if (!conditionId) {
    return <p className="control-plane-empty-detail">Select an external condition to inspect its source-owned history.</p>;
  }
  if (detail.conditionId !== conditionId || detail.state === "idle") {
    return <button className="button secondary" type="button" onClick={() => onLoadDetail(conditionId)}>Load passive condition detail</button>;
  }
  if (detail.state === "loading") {
    return <p className="control-plane-empty-detail">Loading passive condition detail…</p>;
  }
  if (detail.state === "error" || !detail.condition) {
    return <button className="button secondary" type="button" onClick={() => onLoadDetail(conditionId)}>Retry passive condition detail</button>;
  }
  const condition = detail.condition;
  const hasCurrentHistory = history.conditionId === condition.condition_id;
  return (
    <section className="control-plane-record-detail" aria-live="polite" aria-label="Selected external condition detail">
      <h3>Selected external condition</h3>
      <dl className="control-plane-facts compact">
        <div><dt>Owner</dt><dd className="mono">{condition.owner_type}:{condition.owner_id}</dd></div>
        <div><dt>Source</dt><dd className="mono">{condition.adapter}:{condition.source_id}</dd></div>
        <div><dt>State</dt><dd>{condition.state} · sequence {condition.sequence} · version {condition.version}</dd></div>
        <div><dt>Recorded timing</dt><dd>{condition.poll_policy.initial_ms.toLocaleString()}–{condition.poll_policy.maximum_ms.toLocaleString()} ms · {condition.poll_policy.deadline_ms !== null ? localTime(condition.poll_policy.deadline_ms) : "No deadline"}</dd></div>
        <div><dt>Last observation</dt><dd>{condition.last_observation ? `${condition.last_observation.state} at ${localTime(condition.last_observation.observed_at_ms)}` : "None recorded"}</dd></div>
      </dl>
      {!hasCurrentHistory || history.state === "idle" ? (
        <button className="button secondary" type="button" onClick={() => onLoadHistory(condition.condition_id)}>
          Load recorded observation history
        </button>
      ) : history.state === "loading" ? (
        <p className="control-plane-note">Loading recorded observation history…</p>
      ) : history.state === "error" ? (
        <button className="button secondary" type="button" onClick={() => onLoadHistory(condition.condition_id)}>
          Retry recorded observation history
        </button>
      ) : (
        <DetailList
          heading="Observation history"
          empty="No observations recorded."
          values={history.observations.map((observation) => (
            <><strong>{observation.state} · sequence {observation.sequence}</strong><span>{localTime(observation.observed_at_ms)}</span><small className="mono">{observation.source_event_id}</small></>
          ))}
        />
      )}
    </section>
  );
}

function DetailList({
  heading,
  empty,
  values,
}: {
  heading: string;
  empty: string;
  values: ReactNode[];
}) {
  const visible = values.slice(0, DETAIL_ITEM_LIMIT);
  return (
    <section className="control-plane-detail-list">
      <h4>{heading}</h4>
      {visible.length ? (
        <ul>
          {visible.map((value, index) => <li key={index}>{value}</li>)}
          {values.length > visible.length && <li className="control-plane-note">{values.length - visible.length} additional bounded records are not expanded in this view.</li>}
        </ul>
      ) : <p className="control-plane-note">{empty}</p>}
    </section>
  );
}

function joinOrNone(values: string[]) {
  return values.length ? values.slice(0, DETAIL_ITEM_LIMIT).join(", ") : "None recorded";
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
