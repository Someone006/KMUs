import { test, expect } from "@playwright/test";

test.describe("operator console", () => {
  test("shows tonight's contribution margin and the group-ordering uplift", async ({ page }) => {
    await page.goto("/ops");
    await expect(page.getByText("NOCTA · LEITSTAND")).toBeVisible();
    await expect(page.getByText("Deckungsbeitrag").first()).toBeVisible();
    await expect(page.getByText("Was das gemeinsame Bestellen einbringt")).toBeVisible();
    await expect(page.getByText("Hätten alle einzeln bestellt")).toBeVisible();
  });

  test("computes Swiss night-work supplements with their legal basis", async ({ page }) => {
    await page.goto("/ops");
    await page.getByRole("button", { name: "Nachtarbeit" }).click();
    await expect(page.getByText("Nachtstunden (23–06)")).toBeVisible();
    await expect(page.getByText("ArG Art. 17b Abs. 1")).toBeVisible();
    await expect(page.getByText("ArG Art. 17b Abs. 2")).toBeVisible();
    // Both regimes must actually appear in the roster.
    await expect(page.getByText("+25% Lohnzuschlag (ArG 17b Abs. 1)").first()).toBeVisible();
    await expect(page.getByText("10% Zeitzuschlag (ArG 17b Abs. 2)").first()).toBeVisible();
  });

  test("forecasts the night curve and the staffing it requires", async ({ page }) => {
    await page.goto("/ops");
    await page.getByRole("button", { name: "Prognose" }).click();
    await expect(page.getByText("Nachtkurve")).toBeVisible();
    await expect(page.getByText("Kurier:innen zur Spitze")).toBeVisible();
    await expect(page.getByText("Break-even-Warenkorb")).toBeVisible();
    await expect(page.getByText("Nachbestellen")).toBeVisible();
  });

  test("verifies the hash-chained audit ledger", async ({ page }) => {
    await page.goto("/ops");
    await page.getByRole("button", { name: "Protokoll" }).click();
    await expect(page.getByText(/Protokoll unversehrt/)).toBeVisible();
  });

  test("batches placed orders into routed runs", async ({ page }) => {
    await page.goto("/ops");
    await page.getByRole("button", { name: "Disposition" }).click();
    await expect(page.getByText(/Touren ·/)).toBeVisible();
  });
});

test("courier app shows the shift, its night pay and the assigned stops", async ({ page }) => {
  await page.goto("/courier");
  await expect(page.getByText("Wer fährt heute Nacht?")).toBeVisible();
  await page.getByText("Luca Bernasconi").click();
  await expect(page.getByText("Nachtstunden")).toBeVisible();
  // Luca has 41 nights this year, so he is on the 10% time-compensation regime.
  await expect(page.getByText("10% Zeitgutschrift")).toBeVisible();
});
