import { expect, test } from "@playwright/test";

test("renders the dense run workspace and primary operator surfaces", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Harness Console").first()).toBeVisible();
  await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();
  await expect(page.getByText("NeuralMatrix").first()).toBeVisible();

  await page.getByRole("button", { name: "Runs" }).click();
  await expect(page.getByRole("heading", { name: "CI credibility remediation" })).toBeVisible();
  await expect(page.getByText("Architecture and review")).toBeVisible();
  await expect(page.getByRole("button", { name: /MEDIA-001/ }).first()).toBeVisible();
  await expect(page.getByText("Activity", { exact: true })).toBeVisible();
  await expect(page.locator(".statusbar")).toContainText("Event stream");

  await page
    .getByRole("navigation", { name: "Primary navigation" })
    .getByRole("button", { name: /Approvals/ })
    .click();
  await expect(page.getByRole("heading", { name: "Approval center" })).toBeVisible();
  await expect(page.getByText(/commandExecution/)).toBeVisible();
});
