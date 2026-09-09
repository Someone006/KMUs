/**
 * The courier surface. Used one-handed, outdoors, at night, often while
 * walking. Everything here is optimised for "glance and tap", never for
 * completeness.
 */

import { Router } from "express";
import { wrap, NotFoundError, DomainError } from "./errors.js";
import { appendEvent, getDb } from "../db/index.js";
import * as repo from "../db/repo.js";
import { DEPOT } from "../db/seed.js";
import { planRoute } from "../engine/dispatch.js";
import { computeShiftCost, describeLocalWindow } from "../engine/nightshift.js";
import { computeRunde } from "../engine/runde.js";
import { formatCHF } from "../domain/money.js";
import type { Drop } from "../domain/types.js";

export const courierRouter = Router();

courierRouter.get("/couriers", wrap((_req, res) => {
  res.json({ couriers: repo.allCouriers() });
}));

courierRouter.get("/courier/:id", wrap((req, res) => {
  const now = Date.now();
  const courier = repo.allCouriers().find((c) => c.id === req.params.id);
  if (!courier) throw new NotFoundError(`Courier ${req.params.id}`);

  const shiftRow = repo.allShifts().find((s) => s.courier_id === courier.id);
  const shift = shiftRow
    ? computeShiftCost({
        courierId: courier.id,
        courierName: courier.name,
        start: shiftRow.start,
        end: shiftRow.end,
        hourlyWage: courier.hourlyWage,
        nightsThisYear: courier.nightsThisYear,
      })
    : null;

  // Drops assigned to this courier's dispatched runs.
  const runRows = getDb()
    .prepare("SELECT id FROM runs WHERE courier_id = ? AND status = 'dispatched'")
    .all(courier.id) as Array<{ id: string }>;
  const runIds = new Set(runRows.map((r) => r.id));

  const products = repo.productMap();
  const assigned = repo
    .rundesByStatus("dispatched")
    .filter((r) => r.runId && runIds.has(r.runId));

  const drops: Drop[] = assigned.map((runde) => {
    const items = repo.itemsOf(runde.id);
    const view = computeRunde({
      runde,
      zone: repo.getZone(runde.zoneId)!,
      participants: repo.participantsOf(runde.id),
      items, products, now,
    });
    return {
      rundeId: runde.id,
      code: runde.code,
      lat: runde.lat,
      lng: runde.lng,
      address: runde.address,
      items: view.itemCount,
      chilled: items.some((i) => products.get(i.productId)?.chilled),
      basket: view.totals.goods,
      placedAt: runde.placedAt ?? runde.createdAt,
    };
  });

  const plan = planRoute(DEPOT, drops);

  res.json({
    courier,
    shift: shift
      ? {
          window: `${describeLocalWindow(shift.start)} – ${describeLocalWindow(shift.end)}`,
          nightMinutes: shift.nightMinutes,
          nightHours: Math.round((shift.nightMinutes / 60) * 10) / 10,
          compensation: shift.compensation,
          compensationLabel:
            shift.compensation === "wage_supplement_25"
              ? "+25% Nachtzuschlag"
              : shift.compensation === "time_compensation_10"
                ? "10% Zeitgutschrift"
                : "kein Zuschlag",
          // The courier sees what the night is worth to them. Transparency
          // about night pay is the cheapest retention tool a fleet has.
          earnings: formatCHF(shift.baseCost + shift.supplementCost),
          supplementLabel: formatCHF(shift.supplementCost),
          timeCreditMinutes: shift.timeCompensationMinutes,
          violations: shift.violations,
        }
      : null,
    stops: plan.order.map((idx, position) => ({
      position: position + 1,
      rundeId: drops[idx].rundeId,
      code: drops[idx].code,
      address: drops[idx].address,
      items: drops[idx].items,
      chilled: drops[idx].chilled,
      etaMinutes: plan.etaMinutes[position],
      lat: drops[idx].lat,
      lng: drops[idx].lng,
    })),
    totals: {
      stops: drops.length,
      distanceLabel: `${(plan.distanceM / 1000).toFixed(1)} km`,
      minutes: Math.round(plan.totalMinutes),
      chilledRisk: plan.chilledRisk,
    },
  });
}));

courierRouter.post("/courier/:id/deliver/:code", wrap((req, res) => {
  const now = Date.now();
  const runde = repo.getRundeByCode(req.params.code);
  if (!runde) throw new NotFoundError(`Runde ${req.params.code}`);
  if (runde.status !== "dispatched") {
    throw new DomainError(
      "NOT_DISPATCHED",
      `Runde ${runde.code} is ${runde.status}, not out for delivery.`,
    );
  }

  repo.setRundeStatus(runde.id, "delivered");
  appendEvent("runde.delivered", req.params.id, runde.id, {
    code: runde.code,
    deliveredAt: now,
    minutesFromPlacement: runde.placedAt
      ? Math.round((now - runde.placedAt) / 60_000)
      : null,
  }, now);

  res.json({ code: runde.code, status: "delivered", at: now });
}));
