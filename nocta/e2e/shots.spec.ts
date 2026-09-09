import { test, expect } from "@playwright/test";

/**
 * Screenshots are verification, not decoration: a passing assertion proves
 * the DOM, only the image proves the design.
 */

const DIR = "docs/shots";

test("capture the customer landing", async ({ page }, info) => {
  await page.goto("/");
  await page.getByPlaceholder("Nina").waitFor();
  // Let the shader settle so the frame is representative.
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  await page.screenshot({ path: `${DIR}/01-landing-${info.project.name}.png`, fullPage: false });
});

test("capture a live Runde with four sharers", async ({ page, browser }) => {
  await page.goto("/");
  await page.getByPlaceholder("Nina").fill("Rajinder");
  await page.getByRole("combobox").selectOption("z-k4");
  await page.getByPlaceholder("Langstrasse 84, 8004 Zürich").fill("Langstrasse 84, 8004 Zürich");
  await page.getByTestId("start-runde").click();
  const code = (await page.getByTestId("runde-code").textContent())!.trim();

  await page.getByTestId("add-RB-250").click();
  await page.getByTestId("add-ZW-PAP-175").click();

  for (const name of ["Nina", "Jonas", "Alia"]) {
    const ctx = await browser.newContext();
    const guest = await ctx.newPage();
    await guest.goto(`/r/${code}`);
    await guest.getByPlaceholder("Dein Name").fill(name);
    await guest.getByTestId("join-existing").click();
    await guest.getByTestId("add-CC-500").click();
    if (name === "Alia") await guest.getByTestId("add-BJ-465").click();
    await ctx.close();
  }

  await expect(page.getByText(/Zu 4t spart ihr/)).toBeVisible({ timeout: 10_000 });
  await page.screenshot({ path: `${DIR}/02-runde-live.png`, fullPage: true });
});

test("capture the operator console", async ({ page }) => {
  await page.goto("/ops");
  await expect(page.getByText("Was das gemeinsame Bestellen einbringt")).toBeVisible();
  await page.screenshot({ path: `${DIR}/03-ops-tonight.png`, fullPage: true });

  await page.getByRole("button", { name: "Nachtarbeit" }).click();
  await expect(page.getByText("ArG Art. 17b Abs. 1")).toBeVisible();
  await page.screenshot({ path: `${DIR}/04-ops-nightwork.png`, fullPage: true });

  await page.getByRole("button", { name: "Prognose" }).click();
  await expect(page.getByText("Nachtkurve")).toBeVisible();
  // Wait for the staggered bars to finish rather than capturing mid-animation.
  await page.waitForFunction(() => {
    const bars = document.querySelectorAll("[data-testid='night-curve'] > div > div");
    if (bars.length === 0) return false;
    return [...bars].every((b) => (b as HTMLElement).getBoundingClientRect().height > 2);
  });
  await page.screenshot({ path: `${DIR}/05-ops-forecast.png`, fullPage: true });

  await page.getByRole("button", { name: "Disposition" }).click();
  await expect(page.getByText(/Touren ·/)).toBeVisible();
  await page.screenshot({ path: `${DIR}/06-ops-dispatch.png`, fullPage: true });
});

test("capture the courier app", async ({ page }) => {
  await page.goto("/courier/c-luca");
  await expect(page.getByText("Nachtstunden")).toBeVisible();
  await page.screenshot({ path: `${DIR}/07-courier.png`, fullPage: true });
});
