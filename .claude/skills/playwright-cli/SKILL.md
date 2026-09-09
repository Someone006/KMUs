---
name: playwright-cli
description: Drive Playwright from the command line to test, screenshot, and verify a running web app. Use to prove UI changes actually work in a real browser rather than assuming they do.
---

# Playwright CLI

## Setup in a sandboxed environment

Chromium is often pre-installed. Check `PLAYWRIGHT_BROWSERS_PATH` before downloading anything; if browsers are provisioned, `playwright install` is a waste of bandwidth and may fail. Launch with `executablePath` pointing at the provisioned binary when versions mismatch.

Headless with `--no-sandbox` is required as root in most containers.

## Commands

```bash
npx playwright test                    # run the suite
npx playwright test --reporter=list    # readable CI output
npx playwright test -g "shared cart"   # filter by title
npx playwright test --headed --debug   # step through
npx playwright screenshot --viewport-size=390,844 URL out.png
```

## Selector discipline

Priority order: `getByRole` → `getByLabel` → `getByText` → `getByTestId`. **Never** CSS class selectors — they encode styling, so every redesign breaks every test. If an element is hard to select by role, that is usually an accessibility bug worth fixing rather than routing around.

## Waiting

Never `waitForTimeout`. Playwright auto-waits on assertions and actions; if you need more, assert on the condition you actually care about:

```ts
await expect(page.getByTestId("basket-total")).toHaveText("CHF 38.40");
```

`waitForTimeout` is how a suite becomes flaky, and a flaky suite is worse than no suite because it trains people to ignore red.

## Screenshots as verification

Screenshot after each meaningful state and look at the images. A passing assertion proves the DOM; the screenshot proves the design. Full page: `await page.screenshot({ path, fullPage: true })`.

For multi-surface apps, capture each surface at both phone (390×844) and desktop (1440×900) widths.

## Determinism

Freeze the clock and seed data before asserting on anything time-dependent:

```ts
await page.clock.setFixedTime(new Date("2026-09-12T22:40:00Z"));
```

Tests that pass only in the evening are broken tests that have not failed yet.
