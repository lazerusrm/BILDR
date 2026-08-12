import { describe, expect, it } from "vitest";
import { evaluationSourceLabel } from "./EvaluationSourceCard";
import type { EvaluationOccurrenceSource, EvaluationRunSummary, EvaluationSampleSummary } from "../types";

describe("EvaluationSourceCard", () => {
  it("accepts the receipt-only occurrence shape without payload fields", () => {
    const source: EvaluationOccurrenceSource = {
      occurrence_id: "failure-1", repository_id: "repo-1", run_id: "run-1",
      base_sha: "a".repeat(40), source_receipt_sha256: "b".repeat(64),
      source_kind: "run_terminal",
      trace_revision_id: "trace-1", trace_digest: "c".repeat(64),
      outcome_revision_id: null, outcome_digest: null,
    };
    expect(evaluationSourceLabel(source)).toBe(`run_terminal · run-1 · ${"a".repeat(40)}`);
    expect("fixture" in source).toBe(false);
    expect("evidence" in source).toBe(false);
  });

  it("keeps run and sample state as closed receipt fields", () => {
    const run: EvaluationRunSummary = {
      id: "evaluation-run-1", controller_run_id: "run-1", taskset_revision_id: "taskset-r1",
      grader_bundle_revision_id: "grader-r1", split: "development", status: "completed", invalidated: false,
    };
    const sample: EvaluationSampleSummary = {
      id: "sample-1", evaluation_run_id: run.id, eval_case_revision_id: "case-r1",
      arm: "challenger", seed: 7, classification: "pass", sample_digest: "d".repeat(64), invalidated: false,
    };
    expect(run.status).toBe("completed");
    expect(sample.classification).toBe("pass");
    expect("evidence" in sample).toBe(false);
    expect("artifact" in sample).toBe(false);
  });
});
