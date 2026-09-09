/**
 * The operator console contract. This is what the business is actually run
 * from, and what other night-delivery operators would pay for.
 */

import { Router } from "express";
import { wrap, NotFoundError } from "./errors.js";
import { appendEvent, newId, verifyLedger, getDb } from "../db/index.js";
import * as repo from "../db/repo.js";
import { DEPOT } from "../db/seed.js";
import { computeRunde } from "../engine/runde.js";
import { batchIntoRuns } from "../engine/dispatch.js";
import { computeDropEconomics, summariseNight, breakEvenBasket } from "../engine/economics.js";
import { computeShiftCost, regimeComparison, NIGHT_RULES, checkDailyRest } from "../engine/nightshift.js";
import { forecastNight, parLevels } from "../engine/forecast.js";
import { formatCHF } from "../domain/money.js";
import { nightKey, localParts } from "../domain/time.js";
import type { Drop } from "../domain/types.js";

export const opsRouter = Router();

/** Blended employer cost per courier minute across tonight's roster. */
function courierCostPerMinute(now: number): { perMinute: number; shifts: ReturnType<typeof computeShiftCost>[] } {
  const couriers = new Map(repo.allCouriers().map((c) => [c.id, c]));
  const shifts = repo.allShifts().map((s) => {
    const c = couriers.get(s.courier_id);
    return computeShiftCost({
      courierId: s.courier_id,
      courierName: c?.name ?? s.courier_id,
      start: s.start,
      end: s.end,
      hourlyWage: c?.hourlyWage ?? 2700,
      nightsThisYear: c?.nightsThisYear ?? 0,
    });
  });
  const totalCost = shifts.reduce((a, s) => a + s.totalCost, 0);
  const totalMinutes = shifts.reduce((a, s) => a + s.totalMinutes, 0);
  return {
    // Fall back to a sane rate rather than dividing by zero on an empty roster.
    perMinute: totalMinutes > 0 ? totalCost / totalMinutes : 2700 / 60,
    shifts,
  };
}

/**
 * Rundes in the given states, resolved into drops with their basket and cost.
 *
 * The night summary spans everything ordered tonight, however far along it is;
 * dispatch only looks at what has not been assigned yet. Conflating the two
 * makes the night's revenue vanish the moment a courier is given the run.
 */
const NIGHT_STATES = ["placed", "dispatched", "delivered"];
const UNASSIGNED_STATES = ["placed"];

function dropsIn(states: string[]): Array<{ drop: Drop; basketCost: number; sharers: number; items: number }> {
  const products = repo.productMap();
  return states.flatMap((state) => repo.rundesByStatus(state)).map((runde) => {
    const zone = repo.getZone(runde.zoneId)!;
    const items = repo.itemsOf(runde.id);
    const view = computeRunde({
      runde, zone,
      participants: repo.participantsOf(runde.id),
      items, products, now: Date.now(),
    });
    const basketCost = items.reduce((a, i) => {
      const p = products.get(i.productId);
      return a + (p ? p.cost * i.qty : 0);
    }, 0);
    const chilled = items.some((i) => products.get(i.productId)?.chilled);
    return {
      drop: {
        rundeId: runde.id,
        code: runde.code,
        lat: runde.lat,
        lng: runde.lng,
        address: runde.address,
        items: view.itemCount,
        chilled,
        basket: view.totals.goods,
        placedAt: runde.placedAt ?? runde.createdAt,
      },
      basketCost,
      sharers: view.sharerCount,
      items: view.itemCount,
    };
  });
}

/* --------------------------------- overview -------------------------------- */

opsRouter.get("/overview", wrap((_req, res) => {
  const now = Date.now();
  const { perMinute, shifts } = courierCostPerMinute(now);
  const drops = dropsIn(NIGHT_STATES);

  const { runs } = batchIntoRuns(DEPOT, drops.map((d) => d.drop), now);

  // Attribute run minutes back to each drop, then price each drop honestly.
  const minutesByRunde = new Map<string, number>();
  for (const run of runs) {
    run.drops.forEach((d, idx) => {
      minutesByRunde.set(d.rundeId, run.plan.attributedMinutes[idx]);
    });
  }

  const economics = drops.map((d) => {
    const view = computeRunde({
      runde: repo.getRundeById(d.drop.rundeId)!,
      zone: repo.getZone(repo.getRundeById(d.drop.rundeId)!.zoneId)!,
      participants: repo.participantsOf(d.drop.rundeId),
      items: repo.itemsOf(d.drop.rundeId),
      products: repo.productMap(),
      now,
    });
    return {
      code: d.drop.code,
      address: d.drop.address,
      ...computeDropEconomics({
        basketGross: d.drop.basket,
        basketCost: d.basketCost,
        deliveryFee: view.totals.deliveryFee,
        itemCount: d.items,
        chilled: d.drop.chilled,
        minutesAttributed: minutesByRunde.get(d.drop.rundeId) ?? 12,
        courierCostPerMinute: perMinute,
        sharers: d.sharers,
      }),
    };
  });

  const courierMinutes = shifts.reduce((a, s) => a + s.totalMinutes, 0);
  const summary = summariseNight(economics, courierMinutes);

  const openRundes = repo.rundesByStatus("open");

  res.json({
    now,
    nightKey: nightKey(now),
    localTime: `${String(localParts(now).hour).padStart(2, "0")}:${String(localParts(now).minute).padStart(2, "0")}`,
    summary: {
      ...summary,
      revenueLabel: formatCHF(summary.revenue),
      contributionLabel: formatCHF(summary.contribution),
      avgBasketLabel: formatCHF(summary.avgBasket),
      courierCostLabel: formatCHF(summary.courierCost),
    },
    openRundes: openRundes.length,
    placedDrops: drops.length,
    activeCouriers: shifts.filter((s) => s.start <= now && s.end >= now).length,
    drops: economics.map((e) => ({
      ...e,
      revenueLabel: formatCHF(e.revenue),
      contributionLabel: formatCHF(e.contribution),
      courierCostLabel: formatCHF(e.courierCost),
      contributionIfSoloLabel: formatCHF(e.contributionIfSolo),
    })),
    /* The single most valuable number on the screen: what group ordering earns. */
    groupUplift: (() => {
      const withGroups = economics.reduce((a, e) => a + e.contribution, 0);
      const withoutGroups = economics.reduce((a, e) => a + e.contributionIfSolo, 0);
      return {
        actual: withGroups,
        actualLabel: formatCHF(withGroups),
        counterfactual: withoutGroups,
        counterfactualLabel: formatCHF(withoutGroups),
        delta: withGroups - withoutGroups,
        deltaLabel: formatCHF(withGroups - withoutGroups),
      };
    })(),
  });
}));

/* --------------------------------- dispatch -------------------------------- */

opsRouter.get("/dispatch", wrap((_req, res) => {
  const now = Date.now();
  const drops = dropsIn(UNASSIGNED_STATES);
  const { runs } = batchIntoRuns(DEPOT, drops.map((d) => d.drop), now);
  const couriers = repo.allCouriers().filter((c) => c.active);

  res.json({
    depot: DEPOT,
    runs: runs.map((run, i) => ({
      id: `run-${i + 1}`,
      // Round-robin assignment; a real deployment would honour who is free.
      courier: couriers[i % Math.max(1, couriers.length)] ?? null,
      stops: run.plan.order.map((stopIdx, position) => ({
        position: position + 1,
        code: run.drops[stopIdx].code,
        address: run.drops[stopIdx].address,
        items: run.drops[stopIdx].items,
        chilled: run.drops[stopIdx].chilled,
        basketLabel: formatCHF(run.drops[stopIdx].basket),
        etaMinutes: run.plan.etaMinutes[position],
        lat: run.drops[stopIdx].lat,
        lng: run.drops[stopIdx].lng,
      })),
      distanceM: run.plan.distanceM,
      distanceLabel: `${(run.plan.distanceM / 1000).toFixed(1)} km`,
      minutes: run.plan.totalMinutes,
      chilledRisk: run.plan.chilledRisk,
      dropsPerHour: run.plan.totalMinutes > 0
        ? Math.round((run.drops.length / (run.plan.totalMinutes / 60)) * 10) / 10
        : 0,
    })),
    unbatched: 0,
  });
}));

opsRouter.post("/dispatch/assign", wrap((req, res) => {
  const now = Date.now();
  const drops = dropsIn(UNASSIGNED_STATES);
  const { runs } = batchIntoRuns(DEPOT, drops.map((d) => d.drop), now);
  const couriers = repo.allCouriers().filter((c) => c.active);
  const d = getDb();

  let assigned = 0;
  runs.forEach((run, i) => {
    const runId = newId();
    const courier = couriers[i % Math.max(1, couriers.length)];
    d.prepare(
      "INSERT INTO runs (id, courier_id, status, distance_m, minutes, created_at, stop_order) VALUES (?, ?, 'dispatched', ?, ?, ?, ?)",
    ).run(runId, courier?.id ?? null, run.plan.distanceM, run.plan.totalMinutes, now,
      JSON.stringify(run.plan.order.map((idx) => run.drops[idx].code)));

    for (const drop of run.drops) {
      repo.setRundeStatus(drop.rundeId, "dispatched");
      repo.setRundeRun(drop.rundeId, runId);
      assigned++;
    }
    appendEvent("run.dispatched", courier?.id ?? "unassigned", runId, {
      stops: run.drops.map((s) => s.code),
      distanceM: run.plan.distanceM,
      minutes: run.plan.totalMinutes,
    }, now);
  });

  res.json({ runs: runs.length, dropsAssigned: assigned });
}));

/* ---------------------------------- labour --------------------------------- */

opsRouter.get("/labour", wrap((_req, res) => {
  const now = Date.now();
  const { shifts } = courierCostPerMinute(now);

  // Daily-rest check across each courier's consecutive shifts.
  const byCourier = new Map<string, typeof shifts>();
  for (const s of shifts) {
    if (!byCourier.has(s.courierId)) byCourier.set(s.courierId, []);
    byCourier.get(s.courierId)!.push(s);
  }
  const restViolations: Array<{ courier: string; message: string; legalRef: string }> = [];
  for (const [, list] of byCourier) {
    const sorted = [...list].sort((a, b) => a.start - b.start);
    for (let i = 1; i < sorted.length; i++) {
      const v = checkDailyRest(sorted[i - 1].end, sorted[i].start);
      if (v) restViolations.push({ courier: sorted[i].courierName, message: v.message, legalRef: v.legalRef });
    }
  }

  const totalNightMinutes = shifts.reduce((a, s) => a + s.nightMinutes, 0);
  const totalSupplement = shifts.reduce((a, s) => a + s.supplementCost, 0);
  const totalTimeComp = shifts.reduce((a, s) => a + s.timeCompensationCost, 0);
  const totalCost = shifts.reduce((a, s) => a + s.totalCost, 0);

  res.json({
    rules: NIGHT_RULES,
    shifts: shifts.map((s) => ({
      courierId: s.courierId,
      courierName: s.courierName,
      start: s.start,
      end: s.end,
      totalMinutes: s.totalMinutes,
      nightMinutes: s.nightMinutes,
      eveningMinutes: s.eveningMinutes,
      compensation: s.compensation,
      compensationLabel:
        s.compensation === "wage_supplement_25"
          ? "+25% Lohnzuschlag (ArG 17b Abs. 1)"
          : s.compensation === "time_compensation_10"
            ? "10% Zeitzuschlag (ArG 17b Abs. 2)"
            : "kein Zuschlag",
      baseCostLabel: formatCHF(s.baseCost),
      supplementCostLabel: formatCHF(s.supplementCost),
      timeCompensationMinutes: s.timeCompensationMinutes,
      timeCompensationCostLabel: formatCHF(s.timeCompensationCost),
      totalCostLabel: formatCHF(s.totalCost),
      requiresPermit: s.requiresPermit,
      violations: s.violations,
    })),
    totals: {
      nightMinutes: totalNightMinutes,
      nightHours: Math.round((totalNightMinutes / 60) * 10) / 10,
      supplementCost: totalSupplement,
      supplementCostLabel: formatCHF(totalSupplement),
      timeCompensationCost: totalTimeComp,
      timeCompensationCostLabel: formatCHF(totalTimeComp),
      totalCost,
      totalCostLabel: formatCHF(totalCost),
    },
    restViolations,
    permitRequired: shifts.some((s) => s.requiresPermit),
    regimeExample: regimeComparison(2700, 300),
  });
}));

/* --------------------------------- forecast -------------------------------- */

opsRouter.get("/forecast", wrap((req, res) => {
  const now = Date.now();
  const zoneId = typeof req.query.zoneId === "string" ? req.query.zoneId : "z-k4";
  const zone = repo.getZone(zoneId);
  if (!zone) throw new NotFoundError(`Zone ${zoneId}`);

  const history = repo.historyOrders();
  const forecast = forecastNight(history, zoneId, now);

  // The depot stocks for every zone it serves, not just the one on screen.
  const depotExpected = repo
    .allZones()
    .reduce((total, z) => total + forecastNight(history, z.id, now).totalExpected, 0);

  // Product mix from what actually sells, so par levels reflect reality.
  const products = repo.allProducts();
  const weights = products.map((p) => ({
    productId: p.id,
    name: p.name,
    emoji: p.emoji,
    // Raw weights; parLevels normalises them. Bestsellers move roughly
    // three times a long-tail line.
    share: p.tags.includes("bestseller") ? 0.9 : 0.3,
    stock: p.stock,
  }));

  res.json({
    zone: { id: zone.id, name: zone.name },
    forecast,
    depotExpected,
    parLevels: parLevels(depotExpected, weights).slice(0, 12),
    breakEven: (() => {
      const perMin = courierCostPerMinute(now).perMinute;
      const withFee = breakEvenBasket(0.38, zone.baseFee, 12, perMin);
      // Above the free-delivery threshold the fee disappears but the drop
      // still costs the same - this is where margin quietly leaks.
      const freeDelivery = breakEvenBasket(0.38, 0, 12, perMin);
      return {
        basket: withFee,
        basketLabel: formatCHF(withFee),
        freeDeliveryBasket: freeDelivery,
        freeDeliveryBasketLabel: formatCHF(freeDelivery),
        currentMinimum: zone.minBasket,
        currentMinimumLabel: formatCHF(zone.minBasket),
      };
    })(),
  });
}));

/* ---------------------------------- ledger --------------------------------- */

opsRouter.get("/ledger", wrap((_req, res) => {
  const rows = getDb()
    .prepare("SELECT seq, at, kind, actor, subject, payload, hash FROM events ORDER BY seq DESC LIMIT 40")
    .all() as Array<{ seq: number; at: number; kind: string; actor: string; subject: string; payload: string; hash: string }>;

  res.json({
    integrity: verifyLedger(),
    events: rows.map((r) => ({
      seq: r.seq,
      at: r.at,
      kind: r.kind,
      actor: r.actor,
      subject: r.subject,
      payload: JSON.parse(r.payload),
      hash: r.hash.slice(0, 12),
    })),
  });
}));
