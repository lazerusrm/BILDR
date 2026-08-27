import { expect, test } from "@playwright/test";

test("renders the governor-first run workspace and usage breakdown", async ({
  page,
}) => {
  await page.goto("/");
  await expect(page.getByText("BILDR").first()).toBeVisible();
  await expect(
    page.getByRole("navigation", { name: "Primary navigation" }),
  ).toBeVisible();
  await expect(page.getByText("BILDR").first()).toBeVisible();

  await page.getByRole("button", { name: "Runs" }).click();
  await page
    .getByRole("combobox", { name: "Governor session" })
    .selectOption("run-01JHARNESS");
  await expect(
    page.getByRole("region", { name: "Governor sessions" }),
  ).toContainText("Viewing run 1 of 2 · 2 open");
  await expect(
    page.getByRole("region", { name: "Governor sessions" }),
  ).toContainText("WAITING FOR APPROVAL");
  await expect(
    page.getByRole("heading", { name: "CI credibility remediation" }),
  ).toBeVisible();
  await expect(page.locator(".attempt-history summary")).toContainText(
    "Architecture",
  );
  await expect(
    page.getByRole("region", { name: "Goal and plan" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: /CORE-001/ }).first(),
  ).toBeVisible();
  await expect(
    page.getByText("Messages", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("Plan progress", { exact: true })).toBeVisible();
  await expect(page.getByText(/completed$/).first()).toBeVisible();
  await expect(page.getByText("Agents", { exact: true })).toBeVisible();
  await expect(page.getByText("Recent activity", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Needs your approval")).toBeVisible();
  await expect(page.getByText("Message", { exact: true })).toBeVisible();
  const liveTurn = page.getByRole("group", {
    name: "Live turn telemetry",
  });
  await expect(liveTurn).toContainText("27.6k this turn");
  await liveTurn.locator(".live-turn-fold > summary").click();
  await expect(liveTurn).toContainText("Input");
  await expect(liveTurn).toContainText("25.0k");
  await expect(liveTurn).toContainText("Reasoning in output");
  await expect(page.locator(".statusbar")).toContainText("Live updates");
  const main = page.locator("main");
  await main.evaluate((element) => {
    element.scrollTop = 700;
  });
  const mainBox = await main.boundingBox();
  const switcherBox = await page
    .getByRole("region", { name: "Governor sessions" })
    .boundingBox();
  expect(mainBox).not.toBeNull();
  expect(switcherBox).not.toBeNull();
  expect(switcherBox!.y).toBeLessThanOrEqual(mainBox!.y + 10);
  await main.evaluate((element) => {
    element.scrollTop = 0;
  });
  await page.getByRole("button", { name: "Open messages" }).click();
  await expect(page.getByRole("heading", { name: "Messages" })).toBeVisible();
  await page.getByRole("button", { name: "Close" }).click();

  await page
    .getByRole("navigation", { name: "Primary navigation" })
    .getByRole("button", { name: "Usage" })
    .click();
  await expect(page.getByRole("heading", { name: "Usage" })).toBeVisible();
  await expect(page.getByText("By account", { exact: true })).toBeVisible();
  await expect(page.getByText("By repository", { exact: true })).toBeVisible();
  await expect(page.getByText("By agent", { exact: true })).toBeVisible();
  await expect(page.getByText("Cache writes", { exact: true })).toBeVisible();
  await expect(
    page.getByText("API-equivalent cost", { exact: true }),
  ).toBeVisible();
});

test("makes a prepared task's idle architecture state explicit", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Runs" }).click();
  await expect(
    page.getByRole("region", { name: "Governor sessions" }),
  ).toContainText("Viewing run 2 of 2 · 2 open");
  await expect(
    page.getByRole("region", { name: "Governor sessions" }),
  ).toContainText("READY TO PLAN");

  const status = page.getByRole("region", { name: "Architecture status" });
  await expect(status).toContainText("Planning has not started yet");
  await expect(status).toContainText("WAITING TO START");
  await expect(
    page.getByRole("button", { name: "Start architecture" }),
  ).toBeVisible();
  await expect(page.getByText("Plan progress", { exact: true })).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: "Planning not started" }),
  ).toBeVisible();

  await page
    .getByRole("combobox", { name: "Governor session" })
    .selectOption("run-01JHARNESS");
  await expect(
    page.getByRole("heading", { name: "CI credibility remediation" }),
  ).toBeVisible();

  await page.getByRole("button", { name: "New task" }).click();
  await expect(page.locator(".advanced-fields > summary")).toContainText(
    "5.0m ceiling",
  );
  await page.locator(".advanced-fields > summary").click();
  await expect(
    page.getByRole("slider", { name: "Total run ceiling token budget" }),
  ).toBeVisible();
  await expect(page.getByText("5.0m tokens", { exact: true })).toBeVisible();
  const deepInterview = page.getByRole("checkbox", {
    name: /Deep interview before planning/,
  });
  await expect(deepInterview).not.toBeChecked();
  await deepInterview.check();
  await expect(deepInterview).toBeChecked();
});

test("shows a blocked thread's durable reason, recovery step, and local lifecycle times", async ({
  page,
}) => {
  await page.route("**/api/v1/runs/run-01JHARNESS", async (route) => {
    const response = await route.fetch();
    const detail = await response.json();
    detail.run = {
      ...detail.run,
      state: "BLOCKED",
      phase: "task_recovery",
      failure_reason: "the controller needs an operator decision",
    };
    detail.tasks = detail.tasks.map((task: Record<string, unknown>) => ({
      ...task,
      state: "BLOCKED",
      failure_reason: "the owner thread exhausted its bounded budget",
    }));
    detail.agents = detail.agents.map((agent: Record<string, unknown>) =>
      agent.id === "agent-worker"
        ? {
            ...agent,
            state: "BLOCKED",
            failure_reason: "session token budget exhausted",
            started_at: "2026-08-12T18:05:01Z",
            completed_at: "2026-08-12T18:09:59Z",
          }
        : agent,
    );
    await route.fulfill({ response, json: detail });
  });

  await page.goto("/");
  await page.getByRole("button", { name: "Runs" }).click();
  await page
    .getByRole("combobox", { name: "Governor session" })
    .selectOption("run-01JHARNESS");
  await expect(page.locator(".needs-help-panel")).toBeVisible();
  await expect(page.locator(".run-lifecycle")).toContainText("done");
  await expect(
    page.getByRole("button", { name: "Continue" }),
  ).toBeVisible();
});

test("offers run-level recovery when final plan review stopped before task creation", async ({
  page,
}) => {
  let resumeRequested = false;
  await page.route("**/api/v1/runs/run-01JHARNESS", async (route) => {
    const response = await route.fetch();
    const detail = await response.json();
    detail.run = {
      ...detail.run,
      state: "BLOCKED",
      phase: "plan_review_budget_exhausted",
      failure_reason: "session token budget exhausted",
    };
    detail.tasks = [];
    await route.fulfill({ response, json: detail });
  });
  await page.route(
    "**/api/v1/runs/run-01JHARNESS/plan/resume-review",
    async (route) => {
      resumeRequested = true;
      await route.fulfill({
        status: 202,
        contentType: "application/json",
        body: JSON.stringify({ state: "accepted" }),
      });
    },
  );

  await page.goto("/");
  await page.getByRole("button", { name: "Runs" }).click();
  await page
    .getByRole("combobox", { name: "Governor session" })
    .selectOption("run-01JHARNESS");

  const recovery = page.getByRole("region", { name: "Blocked run recovery" });
  await expect(recovery).toContainText("Resume review");
  await expect(recovery).toContainText("session token budget exhausted");
  await page.getByRole("button", { name: "Resume review" }).click();
  await expect.poll(() => resumeRequested).toBe(true);

  await page.getByRole("button", { name: "Revise plan" }).click();
  const correction = page.getByRole("textbox", {
    name: "What should change?",
  });
  await correction.fill("Keep the existing evidence but shorten the final review.");
  await expect(
    page.getByRole("button", { name: "Revise plan" }),
  ).toBeEnabled();
});

test("completes a deep interview and hands the confirmed brief to planning", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "New task" }).click();
  await page
    .getByRole("textbox", { name: "What should the governor accomplish?" })
    .fill("Prove the requested behavior through the authoritative workflow.");
  await page.locator(".advanced-fields > summary").click();
  await page
    .getByRole("checkbox", { name: /Deep interview before planning/ })
    .check();
  await page.getByRole("button", { name: "Create and start task" }).click();

  await expect(page.getByRole("heading", { name: "Deep interview" })).toBeVisible();
  await expect(
    page.getByText(
      "Which observable result must the authoritative workflow prove?",
    ),
  ).toBeVisible();
  await expect(page.getByText(/Suggested starting point:/)).toContainText(
    "Exercise the primary workflow",
  );

  await page
    .getByRole("textbox", { name: "Your answer" })
    .fill("Exercise the primary workflow and verify the visible result.");
  await page.getByRole("button", { name: "Send answer" }).click();

  await expect(page.getByText("Intended final shape", { exact: true })).toBeVisible();
  await expect(
    page.getByText(
      "A headless user flow exercises the behavior from task creation through its result.",
    ),
  ).toBeVisible();
  await page.getByRole("button", { name: "Use brief and plan" }).click();

  await expect(page.getByText("Confirmed intent", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Governor sessions" }),
  ).toContainText("PLANNING");
});

test("shows live token progress while the deep interviewer is working", async ({
  page,
}) => {
  let interviewerStarted = false;
  await page.route(
    "**/api/v1/runs/run-03JHARNESS/interview/start",
    async (route) => {
      interviewerStarted = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          operation: "start_intent_interview",
          accepted: true,
        }),
      });
    },
  );
  await page.route("**/api/v1/runs/run-03JHARNESS", async (route) => {
    const response = await route.fetch();
    if (!interviewerStarted) {
      await route.fulfill({ response });
      return;
    }
    const detail = await response.json();
    detail.intent_interview = {
      ...detail.intent_interview,
      status: "running",
      agent_id: "agent-interviewer-live",
      turn_count: 1,
      started_at: new Date(Date.now() - 35_000).toISOString(),
    };
    detail.agents = [
      {
        id: "agent-interviewer-live",
        role: "interviewer",
        state: "RUNNING",
        requested_model: "gpt-5.6-sol",
        effective_model: "gpt-5.6-sol",
        requested_reasoning_effort: "xhigh",
        effective_reasoning_effort: "xhigh",
        sandbox_mode: "read-only",
        cwd: "/state/worktrees/run/inspection",
        current_action: "Inspecting the authoritative workflow",
        token_budget: 120000,
        tokens_used: 18640,
        estimated_cost_lower: "$0.94",
        estimated_cost_upper: "$0.94",
        heartbeat_at: new Date().toISOString(),
        thread_id: "thread-interviewer-live",
        active_turn_id: "turn-interviewer-live",
        active_turn_started_at: new Date(Date.now() - 35_000).toISOString(),
        active_turn_usage: {
          input_tokens: 18000,
          cached_input_tokens: 14400,
          cache_write_input_tokens: 0,
          output_tokens: 640,
          reasoning_output_tokens: 220,
          total_tokens: 18640,
          model_context_window: 258400,
        },
        version: 2,
      },
    ];
    await route.fulfill({ response, json: detail });
  });

  await page.goto("/");
  await page.getByRole("button", { name: "New task" }).click();
  await page
    .getByRole("textbox", { name: "What should the governor accomplish?" })
    .fill("Clarify the authoritative workflow before planning.");
  await page.locator(".advanced-fields > summary").click();
  await page
    .getByRole("checkbox", { name: /Deep interview before planning/ })
    .check();
  await page.getByRole("button", { name: "Create and start task" }).click();

  const telemetry = page.getByRole("group", {
    name: "Live turn telemetry",
  });
  await expect(page.getByRole("heading", { name: "Deep interview" })).toBeVisible();
  await expect(telemetry).toContainText("Inspecting the authoritative workflow");
  await expect(telemetry).toContainText("18.0k");
  await expect(telemetry).toContainText("14.4k");
  await expect(telemetry).toContainText("640");
  await expect(telemetry).toContainText("Reasoning in output");
});
