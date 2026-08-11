import { describe, expect, it } from "vitest";
import runtimeStatusFixture from "../../examples/openapi/runtime-status.example.json";
import type { RuntimeStatus } from "./types";

const runtimeStatus: RuntimeStatus = {
  ...runtimeStatusFixture,
  self_improvement: {
    ...runtimeStatusFixture.self_improvement,
    configured_mode:
      runtimeStatusFixture.self_improvement.configured_mode as RuntimeStatus["self_improvement"]["configured_mode"],
    effective_mode:
      runtimeStatusFixture.self_improvement.effective_mode as RuntimeStatus["self_improvement"]["effective_mode"],
  },
};

describe("RuntimeStatus contract", () => {
  it("accepts the checked Rust wire record and keeps improvement modes closed", () => {
    expect(["disabled", "observe_only"]).toContain(runtimeStatus.self_improvement.configured_mode);
    expect(["disabled", "observe_only"]).toContain(runtimeStatus.self_improvement.effective_mode);
    expect(runtimeStatus.codex.native_multi_agent).toBe(true);
    expect(runtimeStatus.scheduler.queued_tasks).toBe(3);
  });
});
