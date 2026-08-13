import { describe, expect, it } from "vitest";
import { manualOutcomeLabels, operatorOutcomeRequest, outcomeStatus } from "./OutcomePanel";

describe("OutcomePanel manual submission", () => {
  it("submits only a closed operator label and never source or actor", () => {
    const request = operatorOutcomeRequest("run-1", manualOutcomeLabels[1], {
      reasonCode: "verification_gap_corrected",
      note: "safe summary",
      correctionArtifactId: "artifact-1",
      supersedes: ["revision-a", "revision-b"],
    });
    expect(request).toMatchObject({
      subject: { kind: "run", id: "run-1" },
      dimension: "operator_acceptance",
      classification: "positive",
      code: "accepted_after_correction",
      supersedes: ["revision-a", "revision-b"],
    });
    expect(request).not.toHaveProperty("source");
    expect(request).not.toHaveProperty("actor");
  });

  it("renders conflict status instead of selecting a winner", () => {
    expect(outcomeStatus(true)).toBe("conflicting observations");
  });
});
