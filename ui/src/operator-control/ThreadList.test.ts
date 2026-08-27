import { describe, expect, it } from "vitest";
import {
  orderThreads,
  isArchived,
  threadActionEffect,
  threadPosture,
} from "./ThreadList";
import type { Run } from "../types";

const run = (over: Partial<Run>): Run =>
  ({
    id: "r",
    title: "t",
    state: "EXECUTING",
    scheduler_paused: false,
    created_at: "2026-08-25T00:00:00Z",
    ...over,
  }) as unknown as Run;

describe("threadPosture", () => {
  it("reports work in flight", () => {
    expect(threadPosture(run({ state: "EXECUTING" }))).toBe("working");
    expect(threadPosture(run({ state: "ARCHITECTING" }))).toBe("working");
  });

  it("treats a paused scheduler as waiting even while executing", () => {
    expect(
      threadPosture(run({ state: "EXECUTING", scheduler_paused: true })),
    ).toBe("waiting");
  });

  it("separates waiting, stopped, done, and failed", () => {
    expect(threadPosture(run({ state: "PLAN_REVIEW_REQUIRED" }))).toBe("waiting");
    expect(threadPosture(run({ state: "BLOCKED" }))).toBe("waiting");
    expect(threadPosture(run({ state: "CANCELED" }))).toBe("stopped");
    expect(threadPosture(run({ state: "ARCHIVED" }))).toBe("stopped");
    expect(threadPosture(run({ state: "COMPLETED" }))).toBe("done");
    expect(threadPosture(run({ state: "FAILED" }))).toBe("failed");
  });

  it("does not call a failed run stopped", () => {
    expect(threadPosture(run({ state: "FAILED", scheduler_paused: true }))).toBe(
      "failed",
    );
  });
});

describe("orderThreads", () => {
  it("puts pinned threads first, then most recently started", () => {
    const ordered = orderThreads([
      run({ id: "old", created_at: "2026-08-01T00:00:00Z" }),
      run({ id: "new", created_at: "2026-08-24T00:00:00Z" }),
      run({ id: "pin", created_at: "2026-07-01T00:00:00Z", pinned: true }),
    ]);
    expect(ordered.map((item) => item.id)).toEqual(["pin", "new", "old"]);
  });

  it("prefers started_at over created_at when a run has begun", () => {
    const ordered = orderThreads([
      run({ id: "a", created_at: "2026-08-01T00:00:00Z" }),
      run({
        id: "b",
        created_at: "2026-07-01T00:00:00Z",
        started_at: "2026-08-20T00:00:00Z",
      }),
    ]);
    expect(ordered.map((item) => item.id)).toEqual(["b", "a"]);
  });

  it("does not mutate the input", () => {
    const input = [run({ id: "a" }), run({ id: "b", pinned: true })];
    orderThreads(input);
    expect(input.map((item) => item.id)).toEqual(["a", "b"]);
  });
});

describe("threadActionEffect", () => {
  it("warns that archiving a live thread stops it first", () => {
    expect(threadActionEffect(run({ state: "EXECUTING" })).archive).toBe(
      "Stops the thread first",
    );
    expect(threadActionEffect(run({ state: "PLAN_REVIEW_REQUIRED" })).archive).toBe(
      "Stops the thread first",
    );
  });

  it("needs no warning once a thread has stopped or finished", () => {
    for (const state of ["COMPLETED", "FAILED", "CANCELED"] as Run["state"][]) {
      expect(threadActionEffect(run({ state })).archive).toBeUndefined();
      expect(threadActionEffect(run({ state })).deleteWarnsLive).toBe(false);
    }
  });

  it("reports an archived thread as already archived", () => {
    expect(threadActionEffect(run({ state: "ARCHIVED" })).archive).toBe(
      "Already archived",
    );
    expect(isArchived(run({ state: "ARCHIVED" }))).toBe(true);
    expect(isArchived(run({ state: "EXECUTING" }))).toBe(false);
  });

  it("warns that deleting a live thread stops it", () => {
    expect(threadActionEffect(run({ state: "EXECUTING" })).deleteWarnsLive).toBe(
      true,
    );
  });
});
