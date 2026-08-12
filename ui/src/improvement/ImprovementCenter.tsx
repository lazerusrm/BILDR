import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import type { EvaluationOccurrenceSource, FailureClusterSummary, FailureOverview, FailureTrace } from "../types";
import { EvaluationSourceCard } from "./EvaluationSourceCard";
import { OutcomePanel } from "./OutcomePanel";
import { TraceExplorer } from "./TraceExplorer";
import { VirtualRows } from "./VirtualRows";

export function ImprovementCenter({ repositoryId }: { repositoryId?: string }) {
  const [overview, setOverview] = useState<FailureOverview>();
  const [selected, setSelected] = useState<FailureClusterSummary>();
  const [trace, setTrace] = useState<FailureTrace>();
  const [error, setError] = useState("");
  const [traceError, setTraceError] = useState("");
  const [evaluationSource, setEvaluationSource] = useState<EvaluationOccurrenceSource>();
  const [evaluationSourceError, setEvaluationSourceError] = useState("");

  useEffect(() => {
    let active = true;
    setOverview(undefined);
    setSelected(undefined);
    setTrace(undefined);
    setError("");
    if (!repositoryId) return () => { active = false; };
    const load = () => api.improvementFailures(repositoryId).then(
      (value) => {
        if (!active) return;
        setOverview(value);
        setError("");
        setSelected((current) => current
          ? value.clusters.find((cluster) => cluster.id === current.id)
          : undefined);
      },
      (cause: unknown) => active && setError(displayError(cause, "Could not load failure observations")),
    );
    void load();
    const timer = window.setInterval(load, 15_000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [repositoryId]);

  const traceId = selected?.representative_trace_id;
  useEffect(() => {
    let active = true;
    setTrace(undefined);
    setTraceError("");
    if (!traceId) return () => { active = false; };
    api.improvementTrace(traceId).then(
      (value) => active && setTrace(value),
      (cause: unknown) => active && setTraceError(displayError(cause, "Could not load supporting trace")),
    );
    return () => { active = false; };
  }, [traceId]);

  const occurrenceId = selected?.representative_occurrence_id;
  useEffect(() => {
    let active = true;
    setEvaluationSource(undefined);
    setEvaluationSourceError("");
    if (!occurrenceId) return () => { active = false; };
    api.evaluationOccurrenceSource(occurrenceId).then(
      (value) => active && setEvaluationSource(value),
      (cause: unknown) => active && setEvaluationSourceError(displayError(cause, "Could not resolve evaluation source")),
    );
    return () => { active = false; };
  }, [occurrenceId]);

  const clusters = useMemo(
    () => [...(overview?.clusters || [])].sort(compareClusters),
    [overview],
  );
  return (
    <div className="page improvement-page">
      <header className="page-title">
        <div><span className="eyebrow">Observe only</span><h1>Improvement Center</h1></div>
        <p>Failure observations are receipt-backed. Unknown remains unknown.</p>
      </header>
      {!repositoryId && <p className="improvement-empty">Register a repository to inspect durable failure observations.</p>}
      {error && <p role="alert" className="form-error">{error}</p>}
      {overview && <>
        <div className="metrics">
          <Metric label="Taxonomy" value={overview.taxonomy_version} />
          <Metric label="Classified" value={String(overview.classified_occurrences)} />
          <Metric label="Unknown" value={String(overview.unknown_occurrences)} />
        </div>
        <section aria-label="Repeat failures" className="improvement-section">
          <h2>Highest-cost repeat failures</h2>
          {!clusters.length ? <p className="improvement-empty">No failure clusters have been observed.</p> : <VirtualRows
            items={clusters}
            renderRow={(cluster) => (
              <button className="improvement-cluster" type="button" onClick={() => setSelected(cluster)}>
                <strong>{cluster.failure_class}</strong><span>{cluster.frequency} occurrences · {cluster.severity}</span>
                <small>{costDisclosure(cluster)}</small>
              </button>
            )}
          />}
        </section>
      </>}
      {traceError && <p role="alert" className="form-error">{traceError}</p>}
      <TraceExplorer traceId={traceId || undefined} rows={trace?.rows || []} loading={Boolean(traceId && !trace && !traceError)} />
      <EvaluationSourceCard source={evaluationSource} loading={Boolean(occurrenceId && !evaluationSource && !evaluationSourceError)} error={evaluationSourceError} />
      {selected?.representative_run_id && <OutcomePanel runId={selected.representative_run_id} />}
    </div>
  );
}

export function compareClusters(left: FailureClusterSummary, right: FailureClusterSummary) {
  return (right.cost_upper_microusd ?? -1) - (left.cost_upper_microusd ?? -1)
    || right.frequency - left.frequency
    || left.id.localeCompare(right.id);
}

export function costDisclosure(cluster: FailureClusterSummary) {
  const unknown = cluster.unknown_cost_occurrences > 0
    ? `${cluster.unknown_cost_occurrences} costs unknown`
    : "";
  if (cluster.cost_upper_microusd === null) return unknown || "cost unavailable";
  const known = `$${(cluster.cost_upper_microusd / 1_000_000).toFixed(2)} upper estimate`;
  return unknown ? `${known} · ${unknown}` : known;
}

function displayError(cause: unknown, fallback: string) {
  return cause instanceof Error ? cause.message : fallback;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="metric"><small>{label}</small><strong>{value}</strong></div>;
}
