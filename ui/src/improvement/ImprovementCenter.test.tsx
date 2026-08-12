import { describe, expect, it } from "vitest";
import type { FailureClusterSummary } from "../types";
import { compareClusters, costDisclosure } from "./ImprovementCenter";

describe("Improvement Center", () => {
  it("ranks a selected failure deterministically without hiding unknown cost", () => {
    const clusters: FailureClusterSummary[] = [
      { id: "unknown", failure_class: "unknown", frequency: 9, severity: "unknown", cost_upper_microusd: null, unknown_cost_occurrences: 9, representative_occurrence_id: null, representative_run_id: null, representative_trace_id: null },
      { id: "known", failure_class: "protocol_error", frequency: 1, severity: "high", cost_upper_microusd: 3_000_000, unknown_cost_occurrences: 0, representative_occurrence_id: "occ:1", representative_run_id: "run:1", representative_trace_id: "trace:1" },
    ];
    expect([...clusters].sort(compareClusters).map((cluster) => cluster.id)).toEqual(["known", "unknown"]);
    expect(costDisclosure({ ...clusters[1], unknown_cost_occurrences: 2 })).toBe(
      "$3.00 upper estimate · 2 costs unknown",
    );
  });
});
