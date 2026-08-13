import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { FailureClusterSummary, RuntimeStatus } from "../types";
import {
  compareClusters,
  costDisclosure,
  ImprovementCenter,
  improvementModePresentation,
} from "./ImprovementCenter";

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

  it("renders disabled, observe-only, and anchor-mismatch states without claiming observation", () => {
    const runtime = (selfImprovement: RuntimeStatus["self_improvement"]): RuntimeStatus => ({
      daemon: { state: "ready", detail: null },
      codex: { state: "ready", detail: null, version: "0.147.0", required_version: "0.147.0", protocol_schema_sha256: "a".repeat(64), schema_match: true, native_multi_agent: true, native_multi_agent_feature: "multi_agent", pid: 1, restart_count: 0 },
      database: { state: "ready", detail: null },
      scheduler: { paused: false, active_total: 0, max_total: 6, active_mutable: 0, max_mutable: 3, active_verifiers: 0, max_verifiers: 1, queued_tasks: 0 },
      self_improvement: selfImprovement,
    });
    const disabled = runtime({ configured_mode: "disabled", effective_mode: "disabled", anchor_sha256: "a".repeat(64), configured_anchor_sha256: "a".repeat(64), anchor_match: true, observation_enabled: false, candidate_generation_enabled: false, candidate_execution_enabled: false, detail: null });
    const observeOnly = runtime({ ...disabled.self_improvement, configured_mode: "observe_only", effective_mode: "observe_only", observation_enabled: true });
    const mismatch = runtime({ ...observeOnly.self_improvement, effective_mode: "disabled", anchor_match: false, detail: "Configured anchor does not match the frozen anchor." });

    expect(improvementModePresentation(disabled).label).toBe("Improvement disabled");
    expect(renderToStaticMarkup(createElement(ImprovementCenter, { runtime: disabled }))).toContain("Improvement disabled");
    expect(improvementModePresentation(observeOnly).label).toBe("Observe only");
    expect(renderToStaticMarkup(createElement(ImprovementCenter, { runtime: observeOnly }))).toContain("Observe only");
    const markup = renderToStaticMarkup(createElement(ImprovementCenter, { runtime: mismatch }));
    expect(improvementModePresentation(mismatch).label).toBe("Safety anchor mismatch");
    expect(markup).toContain("Safety anchor mismatch");
    expect(markup).toContain("Configured anchor does not match the frozen anchor.");
  });
});
