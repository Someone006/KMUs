import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { computeRunde, generateCode, suggestToThreshold, FREE_DELIVERY_THRESHOLD } from "../engine/runde.js";
import { computeShiftCost, nightMinutesOf, regimeComparison, checkDailyRest } from "../engine/nightshift.js";
import { computeDropEconomics, breakEvenBasket, summariseNight } from "../engine/economics.js";
import { haversineM, planRoute, batchIntoRuns, travelMinutes } from "../engine/dispatch.js";
import { forecastNight, parLevels } from "../engine/forecast.js";
import type { Participant, Product, Runde, RundeItem, Zone, Drop } from "../domain/types.js";

/* ------------------------------- fixtures -------------------------------- */

const ZONE: Zone = {
  id: "z-k4", name: "Kreis 4", canton: "ZH", lat: 47.3782, lng: 8.5245,
  radius: 900, baseFee: 590, minBasket: 1500, driveMinutes: 4,
};

function product(id: string, price: number, cost: number, chilled = false): Product {
  return {
    id, sku: id.toUpperCase(), name: `Product ${id}`, brand: "Test",
    category: "softdrink", price, cost, vatClass: "food",
    unitSize: 500, unitMeasure: "ml", unitCount: 1, allergens: [],
    chilled, stock: 100, emoji: "🥤", tags: [],
  };
}

const PRODUCTS = new Map<string, Product>([
  ["p1", product("p1", 320, 175)],
  ["p2", product("p2", 590, 380)],
  ["p3", product("p3", 990, 650, true)],
]);

const NOW = Date.UTC(2026, 8, 12, 20, 40, 0);

function runde(): Runde {
  return {
    id: "r1", code: "ABC234", zoneId: ZONE.id, address: "Langstrasse 84",
    note: "", status: "open", createdAt: NOW - 600_000,
    closesAt: NOW + 900_000, version: 1,
  };
}

function participants(n: number): Participant[] {
  return Array.from({ length: n }, (_, i) => ({
    id: `pt${i}`, rundeId: "r1", name: `Person ${i}`, hue: i * 40,
    joinedAt: NOW - 500_000 + i * 1000, isHost: i === 0,
  }));
}

function item(id: string, participantId: string, productId: string, qty: number, price?: number): RundeItem {
  return {
    id, rundeId: "r1", participantId, productId, qty,
    unitPrice: price ?? PRODUCTS.get(productId)?.price ?? 0, addedAt: NOW,
  };
}

/* --------------------------------- Runde --------------------------------- */

describe("Runde engine", () => {
  test("everyone's share sums exactly to the order total", () => {
    const view = computeRunde({
      runde: runde(), zone: ZONE, participants: participants(4),
      items: [
        item("i1", "pt0", "p1", 2),
        item("i2", "pt1", "p2", 1),
        item("i3", "pt2", "p1", 3),
        item("i4", "pt3", "p3", 1),
      ],
      products: PRODUCTS, now: NOW,
    });
    const summed = view.shares.reduce((a, s) => a + s.total, 0);
    assert.equal(summed, view.totals.total, "shares must reconcile to the total");
    assert.equal(
      view.shares.reduce((a, s) => a + s.feeShare, 0),
      view.totals.deliveryFee,
      "fee shares must reconcile to the fee",
    );
  });

  test("only people who ordered carry the delivery fee", () => {
    const view = computeRunde({
      runde: runde(), zone: ZONE, participants: participants(3),
      items: [item("i1", "pt0", "p1", 1), item("i2", "pt1", "p1", 1)],
      products: PRODUCTS, now: NOW,
    });
    const idle = view.shares.find((s) => s.participantId === "pt2")!;
    assert.equal(idle.feeShare, 0, "a lurker pays nothing");
    assert.equal(idle.total, 0);
    assert.equal(view.sharerCount, 2);
  });

  test("a large basket earns free delivery", () => {
    const view = computeRunde({
      runde: runde(), zone: ZONE, participants: participants(2),
      items: [item("i1", "pt0", "p3", 7)], // 7 x 9.90 = CHF 69.30
      products: PRODUCTS, now: NOW,
    });
    assert.ok(view.totals.goods >= FREE_DELIVERY_THRESHOLD);
    assert.equal(view.totals.deliveryFee, 0);
    assert.equal(view.totals.toFreeDelivery, 0);
  });

  test("reports what the group saves versus ordering alone", () => {
    const view = computeRunde({
      runde: runde(), zone: ZONE, participants: participants(4),
      items: [
        item("i1", "pt0", "p1", 1), item("i2", "pt1", "p1", 1),
        item("i3", "pt2", "p1", 1), item("i4", "pt3", "p1", 1),
      ],
      products: PRODUCTS, now: NOW,
    });
    // Four solo orders would pay 4 x 5.90; together they pay one 5.90.
    assert.equal(view.saved, 590 * 4 - 590);
    assert.equal(view.feePerPerson, Math.round(590 / 4));
  });

  test("a delisted product does not become free goods", () => {
    const view = computeRunde({
      runde: runde(), zone: ZONE, participants: participants(1),
      items: [item("i1", "pt0", "ghost", 3, 500)],
      products: PRODUCTS, now: NOW,
    });
    assert.equal(view.totals.goods, 0);
    assert.equal(view.shares[0].lines.length, 0);
  });

  test("reports the gap to the zone minimum", () => {
    const view = computeRunde({
      runde: runde(), zone: ZONE, participants: participants(1),
      items: [item("i1", "pt0", "p1", 1)], // CHF 3.20 against a CHF 15.00 minimum
      products: PRODUCTS, now: NOW,
    });
    assert.equal(view.totals.meetsMinimum, false);
    assert.equal(view.totals.toMinimum, 1500 - 320);
  });

  test("suggests the cheapest item that closes the gap", () => {
    const catalogue = [...PRODUCTS.values()];
    const pick = suggestToThreshold(400, catalogue, new Set());
    assert.equal(pick?.id, "p2", "CHF 5.90 is the cheapest item clearing a CHF 4.00 gap");
    assert.equal(suggestToThreshold(0, catalogue, new Set()), null);
  });

  test("share codes avoid characters that are ambiguous when read aloud", () => {
    for (let i = 0; i < 200; i++) {
      assert.match(generateCode(), /^[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{6}$/);
    }
  });
});

/* ------------------------------- night work ------------------------------- */

describe("ArG night-work computation", () => {
  test("counts only the minutes inside 23:00-06:00", () => {
    // 20:00 -> 02:00 Zurich (CEST, UTC+2) = 18:00 -> 00:00 UTC
    const start = Date.UTC(2026, 8, 12, 18, 0);
    const end = Date.UTC(2026, 8, 13, 0, 0);
    const { night, evening, day } = nightMinutesOf(start, end);
    assert.equal(night, 180, "23:00-02:00 is three night hours");
    assert.equal(evening, 180, "20:00-23:00 is evening work, not night work");
    assert.equal(day, 0);
  });

  test("remains correct across the autumn DST transition", () => {
    // 2026-10-25: clocks go back, so 22:00 -> 06:00 is nine real hours.
    const start = Date.UTC(2026, 9, 24, 20, 0); // 22:00 CEST
    const end = Date.UTC(2026, 9, 25, 5, 0);    // 06:00 CET
    const { night } = nightMinutesOf(start, end);
    assert.equal(night, 480, "the repeated hour is genuinely worked and genuinely paid");
  });

  test("remains correct across the spring DST transition", () => {
    // 2026-03-29: clocks jump forward, so 22:00 -> 06:00 is seven real hours.
    const start = Date.UTC(2026, 2, 28, 21, 0); // 22:00 CET
    const end = Date.UTC(2026, 2, 29, 4, 0);    // 06:00 CEST
    const { night } = nightMinutesOf(start, end);
    assert.equal(night, 360, "the skipped hour is not worked and not paid");
  });

  test("occasional night work earns the 25% cash supplement", () => {
    const shift = computeShiftCost({
      courierId: "c1", courierName: "Sara", nightsThisYear: 12,
      hourlyWage: 2650,
      start: Date.UTC(2026, 8, 12, 20, 0), // 22:00 local
      end: Date.UTC(2026, 8, 13, 0, 0),    // 02:00 local
    });
    assert.equal(shift.compensation, "wage_supplement_25");
    assert.equal(shift.nightMinutes, 180);
    // 3 night hours x CHF 26.50 x 25% = CHF 19.875 -> 1988 Rappen
    assert.equal(shift.supplementCost, 1988);
    assert.equal(shift.timeCompensationMinutes, 0);
    assert.ok(shift.requiresPermit);
  });

  test("regular night work switches to 10% time compensation", () => {
    const shift = computeShiftCost({
      courierId: "c2", courierName: "Luca", nightsThisYear: 41,
      hourlyWage: 2800,
      start: Date.UTC(2026, 8, 12, 20, 0),
      end: Date.UTC(2026, 8, 13, 0, 0),
    });
    assert.equal(shift.compensation, "time_compensation_10");
    assert.equal(shift.supplementCost, 0, "the cash supplement may not be paid under Art. 17b(2)");
    assert.equal(shift.timeCompensationMinutes, 18, "10% of 180 night minutes");
  });

  test("a shift with no night hours carries no supplement", () => {
    const shift = computeShiftCost({
      courierId: "c3", courierName: "Timo", nightsThisYear: 4, hourlyWage: 2600,
      start: Date.UTC(2026, 8, 12, 14, 0), // 16:00 local
      end: Date.UTC(2026, 8, 12, 20, 0),   // 22:00 local
    });
    assert.equal(shift.compensation, "none");
    assert.equal(shift.supplementCost, 0);
    assert.equal(shift.requiresPermit, false);
  });

  test("flags a night shift longer than nine hours", () => {
    const shift = computeShiftCost({
      courierId: "c4", courierName: "Nadia", nightsThisYear: 5, hourlyWage: 2700,
      start: Date.UTC(2026, 8, 12, 16, 0), // 18:00 local
      end: Date.UTC(2026, 8, 13, 2, 0),    // 04:00 local -> 10h
    });
    assert.ok(shift.violations.some((v) => v.code === "EXCEEDS_MAX_NIGHT_WORK"));
    assert.equal(shift.violations[0].legalRef, "ArG Art. 17a");
  });

  test("flags night work by a minor", () => {
    const shift = computeShiftCost({
      courierId: "c5", courierName: "Jonas", nightsThisYear: 1, hourlyWage: 2400,
      age: 17,
      start: Date.UTC(2026, 8, 12, 21, 0),
      end: Date.UTC(2026, 8, 13, 0, 0),
    });
    assert.ok(shift.violations.some((v) => v.code === "MINOR_NIGHT_WORK"));
  });

  test("flags less than eleven hours of daily rest", () => {
    const previousEnd = Date.UTC(2026, 8, 13, 1, 0);
    const nextStart = Date.UTC(2026, 8, 13, 9, 0); // only 8h later
    assert.ok(checkDailyRest(previousEnd, nextStart));
    assert.equal(checkDailyRest(previousEnd, previousEnd + 12 * 3_600_000), null);
  });

  test("quantifies what crossing the 25-night threshold saves", () => {
    const c = regimeComparison(2700, 300);
    assert.ok(c.savingPerShiftWhenRegular > 0);
    assert.equal(c.thresholdNights, 25);
  });
});

/* -------------------------------- economics ------------------------------- */

describe("Drop economics", () => {
  const COURIER_PER_MIN = 2800 / 60 * 1.25 / 100 * 100 / 100; // ~CHF 0.58/min

  test("a small solo basket loses money, which is the whole thesis", () => {
    const e = computeDropEconomics({
      basketGross: 1400, basketCost: 900, deliveryFee: 590,
      // Solo means unbatched: the courier makes the whole round trip for
      // this one basket. That is exactly why night quick commerce dies.
      itemCount: 4, chilled: false, minutesAttributed: 20,
      courierCostPerMinute: 58, sharers: 1,
    });
    assert.ok(e.contribution < 0, `expected a loss, got ${e.contribution}`);
    assert.equal(e.profitable, false);
  });

  test("the same drop shared by six people is profitable", () => {
    const e = computeDropEconomics({
      basketGross: 8400, basketCost: 5400, deliveryFee: 0, // over the free threshold
      itemCount: 24, chilled: false, minutesAttributed: 14,
      courierCostPerMinute: 58, sharers: 6,
    });
    assert.ok(e.contribution > 0, `expected a profit, got ${e.contribution}`);
    assert.ok(e.contribution > e.contributionIfSolo, "sharing must beat the solo counterfactual");
  });

  test("chilled items cost more to pack", () => {
    const base = { basketGross: 4000, basketCost: 2600, deliveryFee: 590, itemCount: 8, minutesAttributed: 12, courierCostPerMinute: 58, sharers: 3 };
    const cold = computeDropEconomics({ ...base, chilled: true });
    const warm = computeDropEconomics({ ...base, chilled: false });
    assert.ok(cold.packagingCost > warm.packagingCost);
    assert.ok(cold.contribution < warm.contribution);
  });

  test("break-even basket rises as the drop gets more expensive to serve", () => {
    const near = breakEvenBasket(0.38, 590, 8, 58);
    const far = breakEvenBasket(0.38, 590, 22, 58);
    assert.ok(far > near, "a longer drop demands a bigger basket");
  });

  test("a margin rate below the payment rate can never break even", () => {
    assert.equal(breakEvenBasket(0.001, 0, 10, 58), Number.POSITIVE_INFINITY);
  });

  test("summarising an empty night does not divide by zero", () => {
    const s = summariseNight([], 0);
    assert.equal(s.drops, 0);
    assert.equal(s.contributionPct, 0);
    assert.equal(s.dropsPerCourierHour, 0);
  });
});

/* --------------------------------- dispatch ------------------------------- */

describe("Dispatch", () => {
  test("haversine matches a known Zurich distance", () => {
    // Langstrasse to Zurich HB is roughly 1.1 km as the crow flies.
    const d = haversineM({ lat: 47.3782, lng: 8.5245 }, { lat: 47.3779, lng: 8.5403 });
    assert.ok(d > 1000 && d < 1300, `got ${d}m`);
  });

  test("travel time applies a street-detour factor", () => {
    assert.ok(travelMinutes(1000) > (1 / 22) * 60, "streets are longer than straight lines");
  });

  test("2-opt finds the sensible order for a deliberately bad input", () => {
    const depot = { lat: 47.3765, lng: 8.5280 };
    // Four stops in a line, supplied in a deliberately crossing order.
    const drops: Drop[] = [
      { rundeId: "a", code: "A", lat: 47.3800, lng: 8.5280, address: "A", items: 2, chilled: false, basket: 2000, placedAt: 1 },
      { rundeId: "c", code: "C", lat: 47.3860, lng: 8.5280, address: "C", items: 2, chilled: false, basket: 2000, placedAt: 2 },
      { rundeId: "b", code: "B", lat: 47.3830, lng: 8.5280, address: "B", items: 2, chilled: false, basket: 2000, placedAt: 3 },
    ];
    const plan = planRoute(depot, drops);
    assert.deepEqual(plan.order.map((i) => drops[i].code), ["A", "B", "C"]);
    assert.equal(plan.etaMinutes.length, 3);
    assert.ok(plan.etaMinutes[0] < plan.etaMinutes[2], "ETAs increase along the route");
  });

  test("attributed minutes cover the whole run", () => {
    const depot = { lat: 47.3765, lng: 8.5280 };
    const drops: Drop[] = [
      { rundeId: "a", code: "A", lat: 47.3800, lng: 8.5280, address: "A", items: 2, chilled: false, basket: 2000, placedAt: 1 },
      { rundeId: "b", code: "B", lat: 47.3830, lng: 8.5300, address: "B", items: 2, chilled: false, basket: 2000, placedAt: 2 },
    ];
    const plan = planRoute(depot, drops);
    const attributed = plan.attributedMinutes.reduce((a, b) => a + b, 0);
    assert.ok(
      Math.abs(attributed - plan.totalMinutes) < 0.5,
      `attributed ${attributed} vs total ${plan.totalMinutes}`,
    );
  });

  test("an empty run plans to nothing rather than throwing", () => {
    const plan = planRoute({ lat: 47.3765, lng: 8.5280 }, []);
    assert.equal(plan.totalMinutes, 0);
    assert.deepEqual(plan.order, []);
  });

  test("batching respects the maximum stops per run", () => {
    const depot = { lat: 47.3765, lng: 8.5280 };
    const drops: Drop[] = Array.from({ length: 9 }, (_, i) => ({
      rundeId: `r${i}`, code: `C${i}`,
      lat: 47.3765 + i * 0.0004, lng: 8.5280 + i * 0.0004,
      address: `Street ${i}`, items: 3, chilled: false, basket: 2500, placedAt: i,
    }));
    const { runs } = batchIntoRuns(depot, drops, NOW, 3);
    assert.ok(runs.every((r) => r.drops.length <= 3));
    assert.equal(runs.reduce((a, r) => a + r.drops.length, 0), 9, "no drop is lost");
  });

  test("every pending drop is assigned to exactly one run", () => {
    const depot = { lat: 47.3765, lng: 8.5280 };
    const drops: Drop[] = Array.from({ length: 7 }, (_, i) => ({
      rundeId: `r${i}`, code: `C${i}`,
      lat: 47.37 + Math.random() * 0.04, lng: 8.51 + Math.random() * 0.04,
      address: `Street ${i}`, items: 3, chilled: i % 3 === 0, basket: 2500, placedAt: i,
    }));
    const { runs } = batchIntoRuns(depot, drops, NOW);
    const codes = runs.flatMap((r) => r.drops.map((d) => d.code)).sort();
    assert.deepEqual(codes, drops.map((d) => d.code).sort());
  });
});

/* --------------------------------- forecast ------------------------------- */

describe("Nightfall forecast", () => {
  function history(nights: number, perHour: number) {
    const out = [];
    for (let n = 1; n <= nights; n++) {
      const day = new Date(Date.UTC(2026, 8, 12) - n * 7 * 86_400_000);
      for (const hour of [20, 21, 22, 23]) {
        for (let k = 0; k < perHour; k++) {
          const t = new Date(day);
          t.setUTCHours(hour, k, 0, 0);
          out.push({ placedAt: t.getTime(), zoneId: "z-k4", basket: 2500, sharers: 2 });
        }
      }
    }
    return out;
  }

  test("learns from repeated observations of the same weekday and hour", () => {
    const f = forecastNight(history(6, 5), "z-k4", Date.UTC(2026, 8, 12, 20, 0));
    // Fixture hours are UTC; 20:00 UTC is 22:00 in Zurich, and the engine
    // buckets by local hour because that is when the customer is awake.
    const h22 = f.hours.find((h) => h.hour === 22)!;
    assert.equal(h22.confidence, "high");
    assert.ok(Math.abs(h22.expectedOrders - 5) < 1.5, `got ${h22.expectedOrders}`);
  });

  test("falls back to a baseline shape where there is no history", () => {
    const f = forecastNight([], "z-k4", Date.UTC(2026, 8, 12, 20, 0));
    assert.ok(f.totalExpected > 0);
    assert.ok(f.hours.every((h) => h.confidence === "low"));
    assert.equal(f.hours.length, 10);
  });

  test("expects more on a weekend than a weekday", () => {
    const friday = forecastNight([], "z-k4", Date.UTC(2026, 8, 11, 20, 0)); // Friday
    const tuesday = forecastNight([], "z-k4", Date.UTC(2026, 8, 8, 20, 0)); // Tuesday
    assert.ok(friday.isWeekend);
    assert.equal(tuesday.isWeekend, false);
    assert.ok(friday.totalExpected > tuesday.totalExpected);
  });

  test("staffs to the peak, not to the average", () => {
    const f = forecastNight([], "z-k4", Date.UTC(2026, 8, 11, 20, 0));
    const avg = f.totalExpected / f.hours.length;
    const peak = f.hours.find((h) => h.hour === f.peakHour)!;
    assert.ok(peak.expectedOrders > avg);
    assert.ok(f.peakCouriers >= 1);
  });

  test("par levels reorder the riskiest lines first", () => {
    const f = forecastNight([], "z-k4", Date.UTC(2026, 8, 11, 20, 0));
    const levels = parLevels(f.totalExpected, [
      { productId: "p1", name: "Red Bull", emoji: "🐂", share: 0.3, stock: 2 },
      { productId: "p2", name: "Valser", emoji: "💧", share: 0.3, stock: 999 },
    ]);
    assert.equal(levels[0].productId, "p1", "the short line sorts first");
    assert.equal(levels[0].stockoutRisk, "high");
    assert.equal(levels[1].stockoutRisk, "none");
    assert.equal(levels[1].reorder, 0);
  });
});
