import { VirtualRows } from "./VirtualRows";
import type { FailureTraceRow } from "../types";

export function TraceExplorer({ traceId, rows, loading = false }: { traceId?: string; rows: readonly FailureTraceRow[]; loading?: boolean }) {
  if (!traceId) return <section className="improvement-empty">Select a failure occurrence to inspect its supporting trace.</section>;
  return (
    <section aria-label="Trace Explorer" className="trace-explorer">
      <header><span className="eyebrow">Receipt-backed trace</span><h2>Trace Explorer</h2><code>{traceId}</code></header>
      {loading && <p>Loading redacted trace…</p>}
      <VirtualRows
        items={rows}
        renderRow={(row) => (
          <article className="trace-row">
            <strong>{row.kind}</strong>
            <span>{row.timestamp_ms === null ? "time unavailable" : new Date(row.timestamp_ms).toLocaleString()}</span>
            <small>{row.redaction_class} · {row.source_receipt_count} receipts</small>
          </article>
        )}
      />
    </section>
  );
}
