/**
 * Drop economics.
 *
 * The only equation that decides whether this business lives:
 *
 *   contribution = basket margin
 *                - courier cost (incl. ArG night supplement)
 *                - packaging
 *                - payment processing
 *                - variable platform cost
 *
 * Gorillas, Getir and Smood all died with this number negative and hidden by
 * funding. NOCTA computes it per drop, live, and refuses to hide it.
 *
 * PURE.
 */

import type { Rappen } from "../domain/money.js";

export const COST_MODEL = {
  version: "2026-09-01",
  /** Cold bag liner + insulation for any chilled line. */
  packagingChilled: 85 as Rappen,
  packagingBase: 45 as Rappen,
  packagingPerItem: 6 as Rappen,

  /** TWINT / card blended rate in basis points. 130 bp = 1.30%. */
  paymentPct: 130,
  paymentFixed: 25 as Rappen,
  /** Hosting, SMS, support amortised per order. */
  platformVariable: 18 as Rappen,
  /** Minutes spent at the door: park, walk up, hand over. */
  serviceMinutesPerDrop: 4,
  /** Extra minutes per chilled drop (cold bag handling). */
  serviceMinutesChilled: 1,
} as const;

export interface DropEconomicsInput {
  /** Gross goods value in the basket. */
  basketGross: Rappen;
  /** What those goods cost us. */
  basketCost: Rappen;
  /** Delivery fee actually charged (0 when the free threshold is met). */
  deliveryFee: Rappen;
  itemCount: number;
  chilled: boolean;
  /** Driving + service minutes attributed to this drop within its run. */
  minutesAttributed: number;
  /** Employer cost per courier minute, from computeShiftCost. */
  courierCostPerMinute: number;
  /** People sharing this drop. Reporting only; economics are per drop. */
  sharers: number;
}

export interface DropEconomics {
  revenue: Rappen;
  goodsMargin: Rappen;
  courierCost: Rappen;
  packagingCost: Rappen;
  paymentCost: Rappen;
  platformCost: Rappen;
  totalVariableCost: Rappen;
  contribution: Rappen;
  /** Contribution as a percentage of revenue, one decimal. */
  contributionPct: number;
  profitable: boolean;
  sharers: number;
  /** Contribution this drop would have had with a single customer's basket. */
  contributionIfSolo: Rappen;
}

export function computeDropEconomics(input: DropEconomicsInput): DropEconomics {
  const revenue = input.basketGross + input.deliveryFee;
  const goodsMargin = input.basketGross - input.basketCost;

  const courierCost = Math.round(input.minutesAttributed * input.courierCostPerMinute);

  const packagingCost =
    COST_MODEL.packagingBase +
    COST_MODEL.packagingPerItem * input.itemCount +
    (input.chilled ? COST_MODEL.packagingChilled : 0);

  const paymentCost =
    Math.round((revenue * COST_MODEL.paymentPct) / 10_000) + COST_MODEL.paymentFixed;

  const platformCost = COST_MODEL.platformVariable;

  const totalVariableCost = courierCost + packagingCost + paymentCost + platformCost;
  // The delivery fee is revenue, not margin on goods - add it back explicitly.
  const contribution = goodsMargin + input.deliveryFee - totalVariableCost;

  // Counterfactual: the same drop carrying one person's share of the basket.
  const soloShare = input.sharers > 1 ? 1 / input.sharers : 1;
  const soloGross = Math.round(input.basketGross * soloShare);
  const soloCost = Math.round(input.basketCost * soloShare);
  const soloItems = Math.max(1, Math.round(input.itemCount * soloShare));
  const soloRevenue = soloGross + input.deliveryFee;
  const soloPackaging =
    COST_MODEL.packagingBase +
    COST_MODEL.packagingPerItem * soloItems +
    (input.chilled ? COST_MODEL.packagingChilled : 0);
  const soloPayment =
    Math.round((soloRevenue * COST_MODEL.paymentPct) / 10_000) + COST_MODEL.paymentFixed;
  const contributionIfSolo =
    soloGross - soloCost + input.deliveryFee -
    (courierCost + soloPackaging + soloPayment + platformCost);

  return {
    revenue,
    goodsMargin,
    courierCost,
    packagingCost,
    paymentCost,
    platformCost,
    totalVariableCost,
    contribution,
    contributionPct: revenue > 0 ? Math.round((contribution / revenue) * 1000) / 10 : 0,
    profitable: contribution > 0,
    sharers: input.sharers,
    contributionIfSolo,
  };
}

/**
 * Minimum basket that makes a drop break even, given the cost of serving it.
 * Operators set their zone minimum from this instead of guessing.
 */
export function breakEvenBasket(
  marginRate: number, // 0..1, gross margin on goods
  deliveryFee: Rappen,
  minutesAttributed: number,
  courierCostPerMinute: number,
  itemCount = 6,
  chilled = false,
): Rappen {
  const courier = minutesAttributed * courierCostPerMinute;
  const packaging =
    COST_MODEL.packagingBase +
    COST_MODEL.packagingPerItem * itemCount +
    (chilled ? COST_MODEL.packagingChilled : 0);
  const fixed = courier + packaging + COST_MODEL.platformVariable + COST_MODEL.paymentFixed;
  // basket*margin + fee - fixed - basket*paymentRate = 0
  const paymentRate = COST_MODEL.paymentPct / 10_000;
  const denominator = marginRate - paymentRate;
  if (denominator <= 0) return Number.POSITIVE_INFINITY;
  return Math.max(0, Math.ceil((fixed - deliveryFee) / denominator));
}

export interface NightSummary {
  drops: number;
  revenue: Rappen;
  contribution: Rappen;
  contributionPct: number;
  avgBasket: Rappen;
  avgSharers: number;
  dropsPerCourierHour: number;
  unprofitableDrops: number;
  courierCost: Rappen;
}

export function summariseNight(
  drops: DropEconomics[],
  courierMinutes: number,
): NightSummary {
  const n = drops.length;
  if (n === 0) {
    return {
      drops: 0, revenue: 0, contribution: 0, contributionPct: 0, avgBasket: 0,
      avgSharers: 0, dropsPerCourierHour: 0, unprofitableDrops: 0, courierCost: 0,
    };
  }
  const revenue = drops.reduce((a, d) => a + d.revenue, 0);
  const contribution = drops.reduce((a, d) => a + d.contribution, 0);
  return {
    drops: n,
    revenue,
    contribution,
    contributionPct: revenue > 0 ? Math.round((contribution / revenue) * 1000) / 10 : 0,
    avgBasket: Math.round(revenue / n),
    avgSharers: Math.round((drops.reduce((a, d) => a + d.sharers, 0) / n) * 10) / 10,
    dropsPerCourierHour:
      courierMinutes > 0 ? Math.round((n / (courierMinutes / 60)) * 10) / 10 : 0,
    unprofitableDrops: drops.filter((d) => !d.profitable).length,
    courierCost: drops.reduce((a, d) => a + d.courierCost, 0),
  };
}
