import { test, expect, type Page } from "@playwright/test";

/**
 * The Runde is the product, so the test that matters most is two independent
 * browser contexts sharing one cart and watching it stay reconciled.
 */

async function startRunde(page: Page, hostName: string) {
  await page.goto("/");
  await page.getByPlaceholder("Nina").fill(hostName);
  // Pin the zone so fees and minimums are deterministic: Kreis 4 charges
  // CHF 5.90 delivery against a CHF 15.00 minimum.
  await page.getByRole("combobox").selectOption("z-k4");
  await page.getByPlaceholder("Langstrasse 84, 8004 Zürich").fill("Militärstrasse 12, 8004 Zürich");
  await page.getByTestId("start-runde").click();
  await expect(page.getByTestId("runde-code")).toBeVisible();
  return (await page.getByTestId("runde-code").textContent())!.trim();
}

test("a host can open a Runde and receive a shareable code", async ({ page }) => {
  const code = await startRunde(page, "Rajinder");
  expect(code).toMatch(/^[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{6}$/);
  await expect(page.getByTestId("countdown")).toBeVisible();
});

test("the catalogue shows the PBV unit price and allergens before purchase", async ({ page }) => {
  await startRunde(page, "Rajinder");
  // Preisbekanntgabeverordnung Art. 11: CHF per litre / per kilogram.
  await expect(page.getByText("CHF 33.71/kg").first()).toBeVisible();
  // LIV: allergen declaration must be available before a distance purchase.
  await expect(page.getByText(/Enthält:/).first()).toBeVisible();
});

test("the order cannot be placed below the zone minimum", async ({ page }) => {
  await startRunde(page, "Rajinder");
  await page.getByTestId("add-VAL-500").click();
  const place = page.getByTestId("place-order");
  await expect(place).toBeDisabled();
  await expect(place).toContainText("bis Mindestbestellung");
});

test("two people share one cart live and the shares reconcile", async ({ page, browser }) => {
  const code = await startRunde(page, "Rajinder");

  // Host adds one Red Bull: CHF 3.50 goods + CHF 5.90 delivery.
  await page.getByTestId("add-RB-250").click();
  await expect(page.getByTestId("payable")).toContainText("CHF 9.40");

  // A second person joins from an entirely separate browser context.
  const guestContext = await browser.newContext();
  const guest = await guestContext.newPage();
  await guest.goto(`/r/${code}`);
  await guest.getByPlaceholder("Dein Name").fill("Nina");
  await guest.getByTestId("join-existing").click();
  await expect(guest.getByTestId("runde-code")).toHaveText(code);

  await guest.getByTestId("add-ZW-PAP-175").click();

  // The host sees the guest's item arrive without reloading.
  await expect(page.getByText("Nina")).toBeVisible({ timeout: 10_000 });
  await expect(page.getByText(/Zu 2t spart ihr/)).toBeVisible();

  // Goods are now CHF 3.50 + CHF 5.90; the single CHF 5.90 fee is split in two.
  await expect(page.getByTestId("payable")).toContainText("CHF 15.30");

  // The invariant that makes a shared cart trustworthy: the individual
  // shares must sum exactly to what the group is charged.
  const shareTexts = await page.getByTestId("share-total").allTextContents();
  const rappen = (t: string) => Math.round(Number(t.replace(/[^0-9.]/g, "")) * 100);
  const sum = shareTexts.reduce((a, t) => a + rappen(t), 0);
  const payable = rappen((await page.getByTestId("payable").textContent())!);
  expect(shareTexts.length).toBe(2);
  expect(sum).toBe(payable);

  await guestContext.close();
});

test("a full Runde reaches the minimum and can be placed", async ({ page }) => {
  await startRunde(page, "Rajinder");
  for (const sku of ["RB-250", "ZW-PAP-175", "BJ-465"]) {
    await page.getByTestId(`add-${sku}`).click();
  }
  const place = page.getByTestId("place-order");
  await expect(place).toBeEnabled();
  await place.click();
  await expect(page.getByText("Bestellt — unterwegs zu euch 🛵")).toBeVisible();
});

test("an unknown Runde code reports a clear error rather than a blank screen", async ({ page }) => {
  await page.goto("/r/ZZZZZZ");
  await expect(page.getByText("Das hat nicht geklappt")).toBeVisible();
  await expect(page.getByText(/Runde ZZZZZZ not found/)).toBeVisible();
});
