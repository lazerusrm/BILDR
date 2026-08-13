import { type FormEvent, useEffect, useMemo, useState } from "react";
import { api } from "../api";
import type { OutcomeDimension, OutcomeVector } from "../types";

type ManualLabel = {
  value: string;
  label: string;
  dimension: Extract<OutcomeDimension, "operator_acceptance" | "operator_correction" | "review_regression" | "pr_reopened" | "rollback" | "downstream_regression">;
  classification: "positive" | "negative" | "neutral" | "unknown";
  code: string;
};

export const manualOutcomeLabels: ManualLabel[] = [
  { value: "accepted_without_correction", label: "Accepted without correction", dimension: "operator_acceptance", classification: "positive", code: "accepted_without_correction" },
  { value: "accepted_after_correction", label: "Accepted after correction", dimension: "operator_acceptance", classification: "positive", code: "accepted_after_correction" },
  { value: "changes_requested", label: "Changes requested", dimension: "operator_acceptance", classification: "negative", code: "changes_requested" },
  { value: "abandoned_wrong", label: "Abandoned: incorrect", dimension: "operator_acceptance", classification: "negative", code: "abandoned_wrong" },
  { value: "abandoned_cost", label: "Abandoned: too costly", dimension: "operator_acceptance", classification: "negative", code: "abandoned_cost" },
  { value: "correction_recorded", label: "Correction recorded", dimension: "operator_correction", classification: "neutral", code: "correction_recorded" },
  { value: "correction_not_available", label: "Correction unavailable", dimension: "operator_correction", classification: "unknown", code: "correction_not_available" },
  { value: "review_regression", label: "Review regression", dimension: "review_regression", classification: "negative", code: "review_regression" },
  { value: "review_no_regression", label: "No review regression", dimension: "review_regression", classification: "positive", code: "review_no_regression" },
  { value: "reopened", label: "PR reopened", dimension: "pr_reopened", classification: "negative", code: "reopened" },
  { value: "not_reopened", label: "PR not reopened", dimension: "pr_reopened", classification: "positive", code: "not_reopened" },
  { value: "rollback_recorded", label: "Rollback recorded", dimension: "rollback", classification: "negative", code: "rollback_recorded" },
  { value: "no_rollback", label: "No rollback", dimension: "rollback", classification: "neutral", code: "no_rollback" },
  { value: "downstream_regression", label: "Downstream regression", dimension: "downstream_regression", classification: "negative", code: "downstream_regression" },
  { value: "no_downstream_regression", label: "No downstream regression", dimension: "downstream_regression", classification: "positive", code: "no_downstream_regression" },
];

export const outcomeStatus = (conflicted: boolean) =>
  conflicted ? "conflicting observations" : "observed";

export function operatorOutcomeRequest(
  runId: string,
  label: ManualLabel,
  values: { reasonCode: string; note: string; correctionArtifactId: string; supersedes: string[] },
) {
  return {
    run_id: runId,
    subject: { kind: "run" as const, id: runId },
    dimension: label.dimension,
    classification: label.classification,
    code: label.code,
    reason_code: values.reasonCode || null,
    note: values.note || null,
    correction_artifact_id: values.correctionArtifactId || null,
    supersedes: values.supersedes,
    idempotency_key: crypto.randomUUID(),
  };
}

/** Staged SI-006 panel; SI-007 can mount it in the Improvement Center. */
export function OutcomePanel({ runId }: { runId: string }) {
  const [vector, setVector] = useState<OutcomeVector>();
  const [error, setError] = useState("");
  const [selection, setSelection] = useState(manualOutcomeLabels[0].value);
  const [reasonCode, setReasonCode] = useState("");
  const [note, setNote] = useState("");
  const [correctionArtifactId, setCorrectionArtifactId] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const refresh = () => api.outcomes(runId).then(setVector);
  useEffect(() => {
    let active = true;
    refresh().catch((cause: unknown) => {
      if (active) setError(cause instanceof Error ? cause.message : "Could not load outcomes");
    });
    return () => { active = false; };
  }, [runId]);

  const label = manualOutcomeLabels.find((item) => item.value === selection) ?? manualOutcomeLabels[0];
  const supersedes = useMemo(
    () => vector?.items
      .filter((item) => item.conflicted && item.subject.kind === "run" && item.subject.id === runId && item.dimension === label.dimension)
      .flatMap((item) => item.revisions.filter((revision) => revision.is_head).map((revision) => revision.revision_id)) ?? [],
    [label.dimension, runId, vector],
  );
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      await api.recordOperatorOutcome(operatorOutcomeRequest(runId, label, { reasonCode, note, correctionArtifactId, supersedes }));
      setNote("");
      await refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not record outcome");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section aria-label="Outcome observations">
      <h3>Outcome observations</h3>
      {error && <p role="alert">Outcome observations unavailable: {error}</p>}
      {!vector ? <p>Loading outcome observations…</p> : !vector.items.length ? <p>No outcome observations recorded.</p> : <ul>{vector.items.map((item) => <li key={item.outcome_id}>{item.dimension}: {outcomeStatus(item.conflicted)}</li>)}</ul>}
      <form onSubmit={submit}>
        <label>Observation <select value={selection} onChange={(event) => setSelection(event.target.value)}>{manualOutcomeLabels.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></label>
        <label>Reason code <input value={reasonCode} maxLength={80} pattern="[a-z0-9_]{1,80}" onChange={(event) => setReasonCode(event.target.value)} /></label>
        <label>Note <textarea value={note} maxLength={1000} onChange={(event) => setNote(event.target.value)} /></label>
        <label>Correction artifact ID <input value={correctionArtifactId} onChange={(event) => setCorrectionArtifactId(event.target.value)} /></label>
        {supersedes.length > 0 && <p>Resolves {supersedes.length} conflicting observation{ supersedes.length === 1 ? "" : "s" }.</p>}
        <button type="submit" disabled={submitting}>{submitting ? "Recording…" : "Record observation"}</button>
      </form>
    </section>
  );
}
