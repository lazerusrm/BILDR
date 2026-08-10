import { existsSync } from "node:fs";
import { defineConfig } from "@playwright/test";

const configuredBrowser = process.env.HARNESS_CHROMIUM;
const systemBrowser = ["/usr/bin/chromium-browser", "/usr/bin/chromium"].find(
  existsSync,
);
const executablePath = configuredBrowser || systemBrowser;

export default defineConfig({
  testDir: "./tests/e2e",
  timeout: 30_000,
  expect: { timeout: 8_000 },
  fullyParallel: false,
  reporter: "line",
  outputDir: "../target/playwright-results",
  use: {
    baseURL: "http://127.0.0.1:4173",
    browserName: "chromium",
    headless: true,
    viewport: { width: 1440, height: 960 },
    launchOptions: {
      ...(executablePath ? { executablePath } : {}),
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    },
  },
  webServer: {
    command: "node tests/mock-server.mjs",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: false,
    timeout: 15_000,
  },
});
