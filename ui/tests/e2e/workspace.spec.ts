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
  await expect(page.getByText("Architecture and review")).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Goal and plan" }),
  ).toBeVisible();
  await expect(page.getByText("What the governor is pursuing")).toBeVisible();
  await expect(
    page.getByRole("button", { name: /CORE-001/ }).first(),
  ).toBeVisible();
  await expect(
    page.getByText("Governor messages", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("Plan progress", { exact: true })).toBeVisible();
  await expect(page.getByText(/completed$/).first()).toBeVisible();
  await expect(page.getByText("Agents on this work", { exact: true })).toBeVisible();
  await expect(page.getByText("Recent activity", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Needs your approval")).toBeVisible();
  await expect(page.getByText("Message the governor")).toBeVisible();
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
  await page.getByRole("button", { name: "Open governor messages" }).click();
  await expect(page.getByText("Timestamped local scrollback")).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Governor messages" }),
  ).toBeVisible();
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

test("completes a deep interview and hands the confirmed brief to planning", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "New task" }).click();
  await page
    .getByRole("textbox", { name: "What should the governor accomplish?" })
    .fill("Prove the requested behavior through the authoritative workflow.");
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
