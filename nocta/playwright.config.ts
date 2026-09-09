import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  timeout: 30_000,
  expect: { timeout: 10_000 },
  /* The environment provisions Chromium at a pinned build. Point at it
     directly instead of downloading a matching one for the installed
     @playwright/test version. */
  use: {
    baseURL: "http://localhost:5173",
    trace: "off",
    screenshot: "off",
    launchOptions: {
      executablePath:
        process.env.NOCTA_CHROME ?? "/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    },
  },
  projects: [
    { name: "phone", use: { ...devices["Pixel 7"] } },
    { name: "desktop", use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } } },
  ],
});
