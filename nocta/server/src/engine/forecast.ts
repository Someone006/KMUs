/**
 * Nightfall - demand forecasting for the night curve.
 *
 * Night demand is not a flat rate. It is a curve with a hard peak, and its
 * shape differs sharply between a Tuesday and a Saturday. Staffing to the
 * average means paying couriers to stand still until 22:00 and losing orders
 * at 00:30. Since night hours carry a 25% ArG supplement, over-staffing at
 * night is the most expensive mistake this business can make.
 *
 * Model: EWMA over the same weekday-and-hour bucket, with a graceful fallback
 * to a zone baseline shape when history is thin. Deliberately simple - it is
 * auditable by the operator, and a forecast an operator does not trust is a
 * forecast they override.
 *
 * PURE.
 */

import { localParts, nightKey } from "../domain/time.js";

export const FORECAST_MODEL = {
  version: "2026-09-01",
  /** Trading window, local hours. 18:00 through 03:00. */
  hours: [18, 19, 20, 21, 22, 23, 0, 1, 2, 3],
  /** EWMA weight on the most recent comparable night. */
  alpha: 0.35,
  /** Below this many observations the bucket is low-confidence. */
  minObservations: 3,
  /**
   * Baseline shape of a night, indexed by the hours above, as a share of the
   * night's total. Derived from the seeded trading history; an operator can
   * retune it per city.
   */
  shapeWeekend: [0.04, 0.06, 0.09, 0.12, 0.16, 0.17, 0.14, 0.11, 0.07, 0.04],
  shapeWeekday: [0.07, 0.11, 0.15, 0.18, 0.17, 0.13, 0.09, 0.05, 0.03, 0.02],
} as const;

export interface HistoricalOrder {
  placedAt: number;
  zoneId: string;
  basket: number;
  sharers: number;
}

export interface HourForecast {
  hour: number;
  expectedOrders: number;
  /** Poisson-ish band around the expectation, for staffing decisions. */
  low: number;
  high: number;
  confidence: "high" | "medium" | "low";
  observations: number;
  /** Couriers needed to serve this hour at the model's drop rate. */
  couriersNeeded: number;
}

export interface NightForecast {
  zoneId: string;
  targetNight: string;
  isWeekend: boolean;
  hours: HourForecast[];
  totalExpected: number;
  peakHour: number;
  /** Maximum concurrent couriers required across the night. */
  peakCouriers: number;
  /** Courier-hours to roster, the number that becomes a shift plan. */
  courierHours: number;
}

/** Drops one courier completes in an hour, given batching. */
export const DROPS_PER_COURIER_HOUR = 3.6;

function bucketKey(weekday: number, hour: number): string {
  return `${weekday}:${hour}`;
}

/**
 * Weekday of the TRADING NIGHT, not of the wall clock: 01:00 on Saturday
 * belongs to Friday night. Getting this wrong shifts the whole peak by a day.
 */
function tradingWeekday(epochMs: number): number {
  const p = localParts(epochMs);
  return p.hour < 6 ? (p.weekday + 6) % 7 : p.weekday;
}

export function forecastNight(
  history: HistoricalOrder[],
  zoneId: string,
  targetEpochMs: number,
  baselineOrdersPerNight = 40,
): NightForecast {
  const zoneHistory = history.filter((h) => h.zoneId === zoneId);
  const targetWeekday = tradingWeekday(targetEpochMs);
  const isWeekend = targetWeekday === 5 || targetWeekday === 6;

  // Bucket history by (trading weekday, hour), newest last.
  const buckets = new Map<string, Array<{ night: string; count: number }>>();
  const perNightHour = new Map<string, number>();
  for (const order of zoneHistory) {
    const p = localParts(order.placedAt);
    const key = `${nightKey(order.placedAt)}|${p.hour}`;
    perNightHour.set(key, (perNightHour.get(key) ?? 0) + 1);
  }
  for (const [key, count] of perNightHour) {
    const [night, hourStr] = key.split("|");
    const hour = Number(hourStr);
    // Reconstruct the weekday from the night key (its date is the evening date).
    const wd = tradingWeekday(new Date(`${night}T20:00:00`).getTime());
    const bk = bucketKey(wd, hour);
    if (!buckets.has(bk)) buckets.set(bk, []);
    buckets.get(bk)!.push({ night, count });
  }
  for (const list of buckets.values()) list.sort((a, b) => a.night.localeCompare(b.night));

  const shape = isWeekend ? FORECAST_MODEL.shapeWeekend : FORECAST_MODEL.shapeWeekday;
  const weekendLift = isWeekend ? 1.9 : 1.0;

  const hours: HourForecast[] = FORECAST_MODEL.hours.map((hour, idx) => {
    const observations = buckets.get(bucketKey(targetWeekday, hour)) ?? [];

    let expected: number;
    if (observations.length >= FORECAST_MODEL.minObservations) {
      // EWMA, oldest to newest, so recent nights dominate.
      expected = observations[0].count;
      for (let i = 1; i < observations.length; i++) {
        expected =
          FORECAST_MODEL.alpha * observations[i].count +
          (1 - FORECAST_MODEL.alpha) * expected;
      }
    } else if (observations.length > 0) {
      // Blend the thin history with the baseline shape rather than trusting either.
      const mean = observations.reduce((a, o) => a + o.count, 0) / observations.length;
      const baseline = baselineOrdersPerNight * weekendLift * shape[idx];
      expected = 0.5 * mean + 0.5 * baseline;
    } else {
      expected = baselineOrdersPerNight * weekendLift * shape[idx];
    }

    const confidence: HourForecast["confidence"] =
      observations.length >= FORECAST_MODEL.minObservations
        ? "high"
        : observations.length > 0
          ? "medium"
          : "low";

    // Order arrivals are approximately Poisson; sigma = sqrt(lambda).
    const sigma = Math.sqrt(Math.max(expected, 1));
    return {
      hour,
      expectedOrders: Math.round(expected * 10) / 10,
      low: Math.max(0, Math.round(expected - 1.28 * sigma)),
      high: Math.round(expected + 1.28 * sigma),
      confidence,
      observations: observations.length,
      couriersNeeded: Math.max(
        expected > 0.5 ? 1 : 0,
        Math.ceil(expected / DROPS_PER_COURIER_HOUR),
      ),
    };
  });

  const totalExpected = Math.round(hours.reduce((a, h) => a + h.expectedOrders, 0));
  const peak = hours.reduce((a, h) => (h.expectedOrders > a.expectedOrders ? h : a), hours[0]);

  return {
    zoneId,
    targetNight: nightKey(targetEpochMs),
    isWeekend,
    hours,
    totalExpected,
    peakHour: peak.hour,
    peakCouriers: Math.max(...hours.map((h) => h.couriersNeeded)),
    courierHours: hours.reduce((a, h) => a + h.couriersNeeded, 0),
  };
}

export interface ParLevel {
  productId: string;
  name: string;
  emoji: string;
  expectedUnits: number;
  /** Expectation plus a safety buffer sized to demand volatility. */
  parLevel: number;
  currentStock: number;
  reorder: number;
  stockoutRisk: "none" | "low" | "high";
}

/** Units in a typical night basket. Group Rundes pull this well above one. */
export const AVG_UNITS_PER_ORDER = 6.5;

/**
 * Per-SKU stock targets for the coming night. A stockout at 01:00 is not a
 * lost line - the whole Runde collapses, because one missing item makes the
 * host reopen a decision six people already made.
 *
 * `expectedOrders` is DEPOT-WIDE: one depot serves every zone, so sizing its
 * shelves from a single zone's forecast guarantees a stockout.
 * `share` values are normalised here, so callers may pass raw weights.
 */
export function parLevels(
  expectedOrders: number,
  productMix: Array<{ productId: string; name: string; emoji: string; share: number; stock: number }>,
  serviceLevelZ = 1.65, // ~95%
): ParLevel[] {
  const totalShare = productMix.reduce((a, p) => a + p.share, 0);
  const expectedUnitsTotal = expectedOrders * AVG_UNITS_PER_ORDER;

  return productMix
    .map((p) => {
      const expectedUnits =
        totalShare > 0 ? (expectedUnitsTotal * p.share) / totalShare : 0;
      const sigma = Math.sqrt(Math.max(expectedUnits, 1));
      const par = Math.ceil(expectedUnits + serviceLevelZ * sigma);
      const reorder = Math.max(0, par - p.stock);
      return {
        productId: p.productId,
        name: p.name,
        emoji: p.emoji,
        expectedUnits: Math.round(expectedUnits * 10) / 10,
        parLevel: par,
        currentStock: p.stock,
        reorder,
        stockoutRisk:
          p.stock >= par ? "none" : p.stock >= expectedUnits ? "low" : "high",
      } satisfies ParLevel;
    })
    .sort((a, b) => b.reorder - a.reorder);
}
