import { describe, expect, it } from "vitest";
import {
  formatCost,
  formatTokens,
  roleLabel,
  shortModel,
  shortSha,
  terminal,
  tone,
} from "./App";

describe("workspace presentation helpers", () => {
  it("keeps terminal and active run states distinct", () => {
    expect(terminal("COMPLETED")).toBe(true);
    expect(terminal("ARCHIVED")).toBe(true);
    expect(terminal("FINAL_AUDIT")).toBe(false);
  });

  it("maps operational states to stable visual tones", () => {
    expect(tone("VERIFIED")).toBe("success");
    expect(tone("WAITING_APPROVAL")).toBe("warning");
    expect(tone("INFRASTRUCTURE_UNAVAILABLE")).toBe("danger");
    expect(tone("IMPLEMENTING")).toBe("active");
  });

  it("formats custody and usage values compactly", () => {
    expect(shortSha("0123456789abcdef")).toBe("0123456");
    expect(shortModel("gpt-5.6-sol")).toBe("SOL");
    expect(roleLabel("final_auditor")).toBe("Final Auditor");
    expect(formatTokens(12_400)).toBe("12.4k");
    expect(formatCost(6_125_000)).toBe("$6.13");
  });
});
