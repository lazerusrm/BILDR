import type { EvaluationOccurrenceSource } from "../types";

export function EvaluationSourceCard({ source, loading, error }: {
  source?: EvaluationOccurrenceSource;
  loading: boolean;
  error?: string;
}) {
  if (!source && !loading && !error) return null;
  return <section aria-label="Evaluation source" className="improvement-section">
    <h2>Evaluation source</h2>
    {loading && <p className="improvement-empty">Resolving immutable source receipt…</p>}
    {error && <p role="alert" className="form-error">{error}</p>}
    {source && <dl className="metrics">
      <Metric label="Source" value={source.source_kind} />
      <Metric label="Run" value={source.run_id} />
      <Metric label="Base" value={source.base_sha} />
      <Metric label="Trace" value={source.trace_revision_id || "unavailable"} />
    </dl>}
  </section>;
}

export function evaluationSourceLabel(source: EvaluationOccurrenceSource) {
  return `${source.source_kind} · ${source.run_id} · ${source.base_sha}`;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div className="metric"><small>{label}</small><strong>{value}</strong></div>;
}
