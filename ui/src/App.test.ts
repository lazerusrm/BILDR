import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  LiveTurnTelemetry,
  SettingsView,
  SupervisorObservationPanel,
  accountOptionLabel,
  agentEffort,
  agentModel,
  blockerStatus,
  blockedPlanRecovery,
  delegatedThreadDisplayState,
  effectiveRunPosture,
  formatCost,
  formatTurnElapsed,
  formatTokens,
  humanTaskState,
  primaryTaskAgent,
  pullRequestScope,
  rateLimitForecast,
  recordRateLimitHistory,
  roleLabel,
  runLifecycleSummary,
  shortModel,
  shortSha,
  terminal,
  threadLifecycleSummary,
  tone,
  workStatusSummary,
} from "./App";
import type { Agent, Run, RunDetail, Task, Worktree } from "./types";

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

  it("makes blockers actionable and shows durable run and thread times in local time", () => {
    const run = {
      id: "run-blocked",
      state: "BLOCKED",
      phase: "plan_review_deadlocked",
      created_at: "2026-08-12T18:00:00Z",
      started_at: "2026-08-12T18:05:00Z",
      completed_at: "2026-08-12T18:10:00Z",
      failure_reason: "plan review exhausted its bounded budget",
    } as Run;
    const task = {
      id: "task-blocked",
      state: "BLOCKED",
      failure_reason: "the next repository decision is required",
    } as Task;
    const agent = {
      id: "agent-blocked",
      state: "BLOCKED",
      started_at: "2026-08-12T18:05:01Z",
      completed_at: "2026-08-12T18:09:59Z",
      failure_reason: "controller token budget exhausted",
    } as Agent;

    expect(runLifecycleSummary(run)).toContain("Local time · started");
    expect(runLifecycleSummary(run)).toContain("completed");
    expect(threadLifecycleSummary(agent)).toContain("Local time · started");
    expect(threadLifecycleSummary(agent)).toContain("completed");
    expect(blockerStatus(run, task, agent)).toEqual({
      reason: "controller token budget exhausted",
      nextStep:
        "Use Continue governor below. You can add a decision or new fact and choose the next attempt budget before continuing.",
    });
  });

  it("states the supervisory safety boundary and displays only a durable snapshot receipt", () => {
    const disabled = renderToStaticMarkup(
      createElement(SupervisorObservationPanel, {
        detail: { supervision_mode: "disabled" } as RunDetail,
      }),
    );
    expect(disabled).toContain("Supervision is disabled");
    expect(disabled).toContain("no automatic action is available");

    const observing = renderToStaticMarkup(
      createElement(SupervisorObservationPanel, {
        detail: {
          supervision_mode: "observe_only",
          supervisor_snapshot: {
            id: "snapshot-1",
            run_id: "run-1",
            revision: 3,
            event_cursor: 42,
            trigger_kind: "attempt_failed",
            payload_sha256: "a".repeat(64),
            byte_length: 512,
            created_at: "2026-08-13T18:00:00Z",
          },
        } as RunDetail,
      }),
    );
    expect(observing).toContain("Observe-only custody");
    expect(observing).toContain("Terra, Sol, and automatic actions remain off");
    expect(observing).toContain("Latest snapshot r3");
    expect(observing).toContain("Event 42");
  });

  it("exposes the observe-only supervisory control in settings", () => {
    const settings = renderToStaticMarkup(
      createElement(SettingsView, {
        light: false,
        accounts: { accounts: [] },
        onAccounts: () => undefined,
        onSettings: () => undefined,
        onRefresh: async () => undefined,
        onAddAccount: () => undefined,
        onReauthenticate: () => undefined,
        onTheme: () => undefined,
      }),
    );
    expect(settings).toContain("Observe-only supervision");
    expect(settings).toContain("never starts Terra or Sol");
  });

  it("offers a concrete recovery for an interrupted plan review before tasks exist", () => {
    const run = {
      id: "run-blocked-review",
      state: "BLOCKED",
      phase: "plan_review_budget_exhausted",
      failure_reason: "session token budget exhausted",
    } as Run;
    expect(blockedPlanRecovery(run, "digest-1")).toEqual({
      kind: "resume_review",
      reason: "session token budget exhausted",
      hasPlan: true,
    });
    expect(blockerStatus(run)).toEqual({
      reason: "session token budget exhausted",
      nextStep:
        "Use the recovery panel above to resume the bounded final plan review or give the architect one concrete plan correction.",
    });
  });

  it("labels non-active account capacity with its observation time", () => {
    const now = Date.UTC(2026, 7, 12, 18, 0, 0);
    expect(
      accountOptionLabel(
        {
          id: "other-account",
          label: "Other account",
          codex_home: "/tmp/codex-other",
          selected: false,
          state: "ready",
          rate_limits: [
            {
              limit_id: "codex",
              windows: [
                {
                  kind: "primary",
                  used_percent: 24,
                  remaining_percent: 76,
                },
              ],
            },
          ],
          observed_at: now - 20_000,
        },
        now,
      ),
    ).toContain("76% left · checked 20s ago");
  });

  it("does not ingest unavailable-account capacity as a fresh forecast sample", () => {
    const history = {
      "other-account:codex:primary:60": [
        { observedAt: 100, remaining: 76, resetsAt: 1_786_630_416 },
      ],
    };
    expect(
      recordRateLimitHistory(history, {
        accounts: [
          {
            id: "other-account",
            label: "Other account",
            codex_home: "/tmp/codex-other",
            selected: false,
            state: "unavailable",
            rate_limits: [
              {
                limit_id: "codex",
                windows: [
                  {
                    kind: "primary",
                    used_percent: 20,
                    remaining_percent: 80,
                    window_duration_mins: 60,
                    resets_at: 1_786_630_416,
                  },
                ],
              },
            ],
            observed_at: 200,
          },
        ],
      }),
    ).toEqual(history);
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
    expect(pullRequestScope(run, task)).toBe("PR #3108");
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
