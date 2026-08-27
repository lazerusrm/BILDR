import { describe, expect, it } from "vitest";
import { actionableAttention } from "./NeedsYou";
import type { AttentionItem } from "../types";

const item = (
  attention_id: string,
  state: AttentionItem["state"],
): AttentionItem =>
  ({
    attention_id,
    state,
    severity: "high",
    title: attention_id,
    summary: "",
    blocked_refs: [],
    category: "command_approval",
    run_id: "run-1",
    version: 1,
  }) as unknown as AttentionItem;

describe("actionableAttention", () => {
  it("keeps states an operator can still act on and drops terminal ones", () => {
    const kept = actionableAttention([
      item("open-1", "open"),
      item("ack-1", "acknowledged"),
      item("wait-1", "waiting_external"),
      item("resolved-1", "resolved"),
      item("declined-1", "declined"),
      item("superseded-1", "superseded"),
      item("invalidated-1", "invalidated"),
    ]);
    expect(kept.map((candidate) => candidate.attention_id)).toEqual([
      "open-1",
      "ack-1",
      "wait-1",
    ]);
  });

  it("returns nothing when every item is terminal", () => {
    expect(actionableAttention([item("resolved-1", "resolved")])).toEqual([]);
  });
});

describe("actionableAttention run scoping", () => {
  const withRun = (id: string, runId: string | null): AttentionItem =>
    ({ ...item(id, "open"), run_id: runId }) as AttentionItem;

  it("drops attention whose run is archived or deleted", () => {
    const live = new Set(["run-live"]);
    const kept = actionableAttention(
      [
        withRun("a", "run-live"),
        withRun("b", "run-archived"),
        withRun("c", "run-deleted"),
      ],
      live,
    );
    expect(kept.map((i) => i.attention_id)).toEqual(["a"]);
  });

  it("keeps run-less attention, which is not scoped to a run", () => {
    expect(
      actionableAttention([withRun("global", null)], new Set(["run-live"]))
        .map((i) => i.attention_id),
    ).toEqual(["global"]);
  });

  it("keeps everything until the run list has loaded", () => {
    expect(
      actionableAttention([withRun("a", "run-x")], undefined).map(
        (i) => i.attention_id,
      ),
    ).toEqual(["a"]);
  });
});
