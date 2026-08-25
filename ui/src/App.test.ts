import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  LiveTurnTelemetry,
  RegisterModal,
  agentEffort,
  agentModel,
  delegatedThreadDisplayState,
  effectiveRunPosture,
  formatCost,
  formatTurnElapsed,
  formatTokens,
  humanTaskState,
  isDesktopShell,
  primaryTaskAgent,
  pullRequestScope,
  rateLimitForecast,
  recordRateLimitHistory,
  registerPathFromSearch,
  roleLabel,
  shortModel,
  shortSha,
  terminal,
  tone,
  workStatusSummary,
} from "./App";
import type { Agent, Run, Task, Worktree, WorktreeDiffSummary } from "./types";

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
    expect(roleLabel("governor")).toBe("Governor");
    expect(formatTokens(12_400)).toBe("12.4k");
    expect(formatCost(6_125_000)).toBe("$6.13");
    expect(
      formatTurnElapsed("2026-08-11T12:00:00Z", Date.parse("2026-08-11T12:01:05Z")),
    ).toBe("1m 5s");
  });

  it("renders authoritative active-turn token categories without double counting", () => {
    const agent = {
      id: "agent-live",
      role: "interviewer",
      state: "RUNNING",
      requested_model: "gpt-5.6-sol",
      requested_reasoning_effort: "xhigh",
      active_turn_id: "turn-live",
      active_turn_started_at: "2026-08-11T12:00:00Z",
      active_turn_usage: {
        input_tokens: 12_400,
        cached_input_tokens: 10_000,
        cache_write_input_tokens: 0,
        output_tokens: 640,
        reasoning_output_tokens: 220,
        total_tokens: 13_040,
      },
      current_action: "Finding the highest-leverage question",
    } as Agent;
    const markup = renderToStaticMarkup(
      createElement(LiveTurnTelemetry, {
        agent,
        fallbackAction: "Starting the interview",
      }),
    );
    expect(markup).toContain("Live turn telemetry");
    expect(markup).toContain("Cached input");
    expect(markup).toContain("Reasoning in output");
    expect(markup).toContain("12.4k");
    expect(markup).toContain("10.0k");
    expect(markup).toContain("13.0k");
  });

  it("keeps the governor as the task control surface", () => {
    const governor = {
      id: "governor",
      task_id: "task-1",
      role: "governor",
      state: "FAILED",
      requested_model: "gpt-5.6-sol",
      requested_reasoning_effort: "xhigh",
    } as Agent;
    const child = {
      id: "child",
      parent_agent_id: "governor",
      task_id: "task-1",
      nickname: "terra_medium__inventory",
      role: "explorer",
      state: "TURN_COMPLETE",
      requested_model: "gpt-5.6-sol",
      requested_reasoning_effort: "xhigh",
    } as Agent;

    const verifier: Agent = {
      ...governor,
      id: "verifier",
      role: "verifier",
      state: "TURN_COMPLETE",
    };
    expect(primaryTaskAgent([governor, child, verifier], "task-1")?.id).toBe("governor");
    expect(agentModel(child)).toBe("gpt-5.6-terra");
    expect(agentEffort(child)).toBe("medium");
  });

  it("separates operator waiting, child finishing, and repository custody state", () => {
    const child = { state: "RUNNING" } as Agent;
    const task = { state: "NEEDS_HELP", title: "Repair PR #3108", objective: "Repair PR #3108" } as Task;
    const run = { title: "Repair PR #3108", objective: "Repair PR #3108" } as Run;
    const worktree = { state: "ACTIVE", dirty: true, files_changed: 3, additions: 12, deletions: 4 } as Worktree;
    expect(humanTaskState(task.state)).toBe("Waiting on you");
    expect(delegatedThreadDisplayState(child, false)).toBe("FINISHING");
    expect(workStatusSummary(task, worktree).label).toBe("UNCOMMITTED");
    expect(workStatusSummary(task, worktree).detail.split(" ").length).toBeLessThan(8);
    expect(pullRequestScope(run, task)).toBe("PR #3108");
  });

  it("keeps work-status copy to a short sentence", () => {
    const committed = workStatusSummary(
      { state: "IMPLEMENTING" } as Task,
      { state: "ACTIVE", dirty: false, files_changed: 0 } as Worktree,
      { state: "committed" } as WorktreeDiffSummary,
    );
    expect(committed.label).toBe("COMMITTED");
    expect(committed.detail.length).toBeLessThan(40);
    const integrated = workStatusSummary(
      { state: "INTEGRATED" } as Task,
      { state: "ACTIVE" } as Worktree,
    );
    expect(integrated.detail.toLowerCase()).toContain("not a merged pr");
  });

  it("shows the effective run posture instead of only its lifecycle state", () => {
    const run = {
      id: "run-1",
      state: "EXECUTING",
      scheduler_paused: false,
    } as Run;
    expect(
      effectiveRunPosture(run, {
        run,
        tasks: [{ state: "NEEDS_HELP" } as Task],
        agents: [],
        worktrees: [],
        approvals: [],
        automatic_plan_approval: false,
      }),
    ).toBe("WAITING ON YOU");
    expect(effectiveRunPosture({ ...run, scheduler_paused: true })).toBe(
      "PAUSED",
    );
    expect(
      effectiveRunPosture({
        ...run,
        state: "PLAN_ADVERSARIAL_REVIEW",
      }),
    ).toBe("REVIEWING PLAN");
    expect(
      effectiveRunPosture({
        ...run,
        state: "PLAN_REVISION_REQUIRED",
      }),
    ).toBe("REVISING PLAN");
    expect(
      effectiveRunPosture({
        ...run,
        state: "INTERVIEWING",
      }),
    ).toBe("CLARIFYING INTENT");
  });

  it("smooths recent burn against the longer window and resets history on replenishment", () => {
    const now = Date.UTC(2026, 7, 7, 18);
    const window = {
      kind: "primary" as const,
      used_percent: 50,
      remaining_percent: 50,
      window_duration_mins: 10_080,
      resets_at: Math.floor((now + 6 * 3_600_000) / 1_000),
    };
    const forecast = rateLimitForecast(
      [
        {
          observedAt: now - 2 * 3_600_000,
          remaining: 70,
          resetsAt: window.resets_at,
        },
        { observedAt: now, remaining: 50, resetsAt: window.resets_at },
      ],
      window,
      now,
    );
    expect(forecast.label).toContain("avg burn 5.2%/h");
    expect(forecast.label).toContain("lasts past reset");
    expect(forecast.detail).toContain("Longer observations carry more weight");

    const key = "account:codex:primary:10080";
    const accountSnapshot = (remaining: number, used: number) => ({
      selected_account_id: "account",
      accounts: [
        {
          id: "account",
          label: "account",
          codex_home: "/tmp/codex",
          selected: true,
          state: "ready" as const,
          rate_limits: [
            {
              limit_id: "codex",
              windows: [
                {
                  ...window,
                  remaining_percent: remaining,
                  used_percent: used,
                },
              ],
            },
          ],
          observed_at: now,
        },
      ],
    });
    const replenished = recordRateLimitHistory(
      {
        [key]: [
          {
            observedAt: now - 60_000,
            remaining: 40,
            resetsAt: window.resets_at,
          },
        ],
      },
      accountSnapshot(95, 5),
      now,
    );
    expect(replenished[key]).toHaveLength(1);
    expect(replenished[key][0].remaining).toBe(95);

    const paced = recordRateLimitHistory(
      {
        [key]: [
          {
            observedAt: now - 60_000,
            remaining: 50,
            resetsAt: window.resets_at,
          },
        ],
      },
      accountSnapshot(49, 51),
      now,
    );
    expect(paced[key]).toHaveLength(1);
  });
});

describe("desktop register helpers", () => {
  it("reads the folder-picker query used by the native shell", () => {
    expect(registerPathFromSearch("?register=%2Fhome%2Fsrc%2Fapp")).toBe(
      "/home/src/app",
    );
    expect(registerPathFromSearch("register=/tmp/repo&shell=desktop")).toBe(
      "/tmp/repo",
    );
    expect(registerPathFromSearch("")).toBe("");
    expect(isDesktopShell("?shell=desktop")).toBe(true);
    expect(isDesktopShell("")).toBe(false);
  });

  it("offers a native browse control when the desktop shell is active", () => {
    const markup = renderToStaticMarkup(
      createElement(RegisterModal, {
        initialPath: "/home/src/app",
        allowNativeBrowse: true,
        onClose: () => undefined,
        onDone: async () => undefined,
      }),
    );
    expect(markup).toContain("Browse");
    expect(markup).toContain("/home/src/app");
    expect(markup).toContain("bildr://pick-folder");
  });
});
