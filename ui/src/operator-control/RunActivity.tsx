import { useCallback, useEffect, useState } from "react";
import { api } from "../api";
import type {
  ExternalConditionSummary,
  InterventionReceipt,
  InvestigationArtifactSummary,
  LivenessEpisode,
  MaterialProgressEvent,
} from "../types";

const localTime = (value: number) => new Date(value).toLocaleString();

/**
 * Controller-classified activity for one run: what materially changed, whether
 * the run is live, what it is waiting on, and what was investigated. Every
 * record here is read-only; nothing on this panel resumes or retries work.
 */
export function RunActivity({ runId }: { runId: string }) {
  const [progress, setProgress] = useState<MaterialProgressEvent[]>([]);
  const [liveness, setLiveness] = useState<LivenessEpisode[]>([]);
  const [conditions, setConditions] = useState<ExternalConditionSummary[]>([]);
  const [investigations, setInvestigations] = useState<
    InvestigationArtifactSummary[]
  >([]);
  const [receipts, setReceipts] = useState<Record<string, InterventionReceipt[]>>(
    {},
  );
  const [unavailable, setUnavailable] = useState(false);

  const load = useCallback(async () => {
    try {
      const [nextProgress, nextLiveness, nextConditions, nextInvestigations] =
        await Promise.all([
          api.materialProgress(runId),
          api.runLiveness(runId),
          api.externalConditions(),
          api.investigations(runId),
        ]);
      setProgress(nextProgress);
      setLiveness(nextLiveness);
      setConditions(
        nextConditions.filter(
          (condition) =>
            condition.owner_id === runId || condition.owner_type !== "run",
        ),
      );
      setInvestigations(nextInvestigations);
      setUnavailable(false);
    } catch {
      setUnavailable(true);
    }
  }, [runId]);

  useEffect(() => {
    void load();
  }, [load]);

  const loadReceipts = async (episodeId: string) => {
    if (receipts[episodeId]) return;
    try {
      const next = await api.interventionReceipts(episodeId);
      setReceipts((current) => ({ ...current, [episodeId]: next }));
    } catch {
      setReceipts((current) => ({ ...current, [episodeId]: [] }));
    }
  };

  if (unavailable) return null;
  const empty =
    !progress.length &&
    !liveness.length &&
    !conditions.length &&
    !investigations.length;
  if (empty) return null;

  const waiting = conditions.filter((condition) => condition.state === "open");

  return (
    <details className="run-activity">
      <summary>
        Activity
        <small>
          {[
            progress.length ? `${progress.length} changes` : "",
            waiting.length ? `${waiting.length} waiting` : "",
            investigations.length
              ? `${investigations.length} investigated`
              : "",
          ]
            .filter(Boolean)
            .join(" · ") || "no controller records"}
        </small>
      </summary>
      <div className="run-activity-body">
        {progress.length > 0 && (
          <section aria-label="Material changes">
            <div className="inspector-label">Material changes</div>
            <ol className="activity-list">
              {progress.map((event) => (
                <li key={event.event_id}>
                  <strong>{event.kind.replaceAll("_", " ")}</strong>
                  <span>{event.summary}</span>
                  <small>{localTime(event.occurred_at_ms)}</small>
                </li>
              ))}
            </ol>
            <small className="activity-note">
              Only a closed controller-event allow-list appears here. Output,
              token use, and repeated commands are not progress.
            </small>
          </section>
        )}
        {liveness.length > 0 && (
          <section aria-label="Connection">
            <div className="inspector-label">Connection</div>
            <ol className="activity-list">
              {liveness.map((episode) => (
                <li key={episode.episode_id}>
                  <strong>{episode.state.replaceAll("_", " ")}</strong>
                  <span>{episode.state_reason_codes.join(", ") || "no reason recorded"}</span>
                  <small>
                    {localTime(episode.updated_at_ms)} ·{" "}
                    {episode.intervention_count} interventions
                  </small>
                  {episode.intervention_count > 0 && (
                    <details
                      onToggle={() => void loadReceipts(episode.episode_id)}
                    >
                      <summary>Receipts</summary>
                      {(receipts[episode.episode_id] || []).map((receipt) => (
                        <p key={receipt.intervention_id}>
                          {receipt.kind.replaceAll("_", " ")} ·{" "}
                          {localTime(receipt.created_at_ms)} · by{" "}
                          {receipt.requested_by}
                        </p>
                      ))}
                    </details>
                  )}
                </li>
              ))}
            </ol>
          </section>
        )}
        {waiting.length > 0 && (
          <section aria-label="Waiting on">
            <div className="inspector-label">Waiting on</div>
            <ol className="activity-list">
              {waiting.map((condition) => (
                <li key={condition.condition_id}>
                  <strong>{condition.adapter.replaceAll("_", " ")}</strong>
                  <span className="mono">{condition.source_id}</span>
                  <small>
                    {condition.last_observed_at_ms
                      ? `last observed ${localTime(condition.last_observed_at_ms)}`
                      : "not yet observed"}
                  </small>
                </li>
              ))}
            </ol>
            <small className="activity-note">
              Stored observations only. Viewing this does not poll a provider or
              wake work.
            </small>
          </section>
        )}
        {investigations.length > 0 && (
          <section aria-label="Investigations">
            <div className="inspector-label">Investigations</div>
            <ol className="activity-list">
              {investigations.map((artifact) => (
                <li key={artifact.artifact_id}>
                  <strong>{artifact.question}</strong>
                  <span>
                    {artifact.finding_count} findings ·{" "}
                    {artifact.recommendation_count} recommendations
                  </span>
                  <small>{localTime(artifact.created_at_ms)}</small>
                </li>
              ))}
            </ol>
          </section>
        )}
      </div>
    </details>
  );
}
