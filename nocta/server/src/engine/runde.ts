/**
 * The Runde engine.
 *
 * A Runde is one cart at one address shared by several people. Each person
 * owns their lines and pays their own share; the delivery fee is split between
 * them. This is the entire economic thesis of the product: the cost of a drop
 * is fixed, so the only cheap way to make a night order profitable is to put
 * more people behind it.
 *
 * PURE. No I/O, no clock of its own, no framework. Everything this module
 * decides is a function of the state handed to it.
 */

import { type Rappen, splitExact, roundToFiveRappen, vatIncludedIn } from "../domain/money.js";
import type { Participant, Product, Runde, RundeItem, Zone } from "../domain/types.js";

/** How the delivery fee is divided between sharers. */
export type SplitPolicy = "equal" | "proportional";

export interface LineView {
  itemId: string;
  productId: string;
  participantId: string;
  name: string;
  emoji: string;
  qty: number;
  unitPrice: Rappen;
  lineTotal: Rappen;
}

export interface ShareView {
  participantId: string;
  name: string;
  hue: number;
  isHost: boolean;
  itemCount: number;
  /** Goods only. */
  goods: Rappen;
  /** This person's slice of the delivery fee. */
  feeShare: Rappen;
  /** goods + feeShare, what they actually owe. */
  total: Rappen;
  lines: LineView[];
}

export interface RundeTotals {
  goods: Rappen;
  deliveryFee: Rappen;
  /** goods + deliveryFee, before cash rounding. */
  total: Rappen;
  /** What is actually charged, rounded to 5 Rappen. */
  payable: Rappen;
  vatFood: Rappen;
  vatService: Rappen;
  /** Rappen still needed to reach the zone minimum. 0 once cleared. */
  toMinimum: Rappen;
  /** Rappen still needed for free delivery. 0 once cleared. */
  toFreeDelivery: Rappen;
  meetsMinimum: boolean;
}

export interface RundeView {
  runde: Runde;
  zone: Zone;
  totals: RundeTotals;
  shares: ShareView[];
  sharerCount: number;
  itemCount: number;
  /** Delivery cost per person. The number that sells the feature. */
  feePerPerson: Rappen;
  /** What each person would have paid ordering alone. */
  soloFeeEach: Rappen;
  /** Total francs the group keeps by sharing one drop. */
  saved: Rappen;
  msRemaining: number;
}

/** Baskets at or above this get free delivery; the threshold drives basket-building. */
export const FREE_DELIVERY_THRESHOLD: Rappen = 6000; // CHF 60.00

export interface ComputeInput {
  runde: Runde;
  zone: Zone;
  participants: Participant[];
  items: RundeItem[];
  products: Map<string, Product>;
  now: number;
  splitPolicy?: SplitPolicy;
}

export function computeRunde(input: ComputeInput): RundeView {
  const { runde, zone, participants, items, products, now } = input;
  const policy: SplitPolicy = input.splitPolicy ?? "equal";

  const linesByParticipant = new Map<string, LineView[]>();
  for (const p of participants) linesByParticipant.set(p.id, []);

  let goods = 0;
  let vatFood = 0;

  for (const item of items) {
    const product = products.get(item.productId);
    if (!product) continue; // delisted mid-Runde; treated as removed, never as free
    const lineTotal = item.unitPrice * item.qty;
    goods += lineTotal;
    vatFood += vatIncludedIn(lineTotal, product.vatClass);

    const bucket = linesByParticipant.get(item.participantId);
    if (!bucket) continue; // participant left; their lines leave with them
    bucket.push({
      itemId: item.id,
      productId: item.productId,
      participantId: item.participantId,
      name: product.name,
      emoji: product.emoji,
      qty: item.qty,
      unitPrice: item.unitPrice,
      lineTotal,
    });
  }

  const deliveryFee = goods >= FREE_DELIVERY_THRESHOLD ? 0 : zone.baseFee;
  const vatService = vatIncludedIn(deliveryFee, "service");
  const total = goods + deliveryFee;

  // Only people who actually ordered something carry the fee.
  const contributing = participants.filter(
    (p) => (linesByParticipant.get(p.id)?.length ?? 0) > 0,
  );
  const feeBearers = contributing.length > 0 ? contributing : participants.slice(0, 1);

  const weights =
    policy === "proportional"
      ? feeBearers.map((p) =>
          (linesByParticipant.get(p.id) ?? []).reduce((a, l) => a + l.lineTotal, 0),
        )
      : undefined;

  // splitExact guarantees the shares sum to deliveryFee. Invariant asserted in tests.
  const feeShares = splitExact(deliveryFee, feeBearers.length, weights);
  const feeByParticipant = new Map<string, Rappen>();
  feeBearers.forEach((p, i) => feeByParticipant.set(p.id, feeShares[i]));

  const shares: ShareView[] = participants.map((p) => {
    const lines = linesByParticipant.get(p.id) ?? [];
    const goodsForP = lines.reduce((a, l) => a + l.lineTotal, 0);
    const feeShare = feeByParticipant.get(p.id) ?? 0;
    return {
      participantId: p.id,
      name: p.name,
      hue: p.hue,
      isHost: p.isHost,
      itemCount: lines.reduce((a, l) => a + l.qty, 0),
      goods: goodsForP,
      feeShare,
      total: goodsForP + feeShare,
      lines,
    };
  });

  const sharerCount = Math.max(1, feeBearers.length);
  const soloFeeEach = zone.baseFee;

  const totals: RundeTotals = {
    goods,
    deliveryFee,
    total,
    payable: roundToFiveRappen(total),
    vatFood,
    vatService,
    toMinimum: Math.max(0, zone.minBasket - goods),
    toFreeDelivery: Math.max(0, FREE_DELIVERY_THRESHOLD - goods),
    meetsMinimum: goods >= zone.minBasket,
  };

  return {
    runde,
    zone,
    totals,
    shares,
    sharerCount,
    itemCount: items.reduce((a, i) => a + i.qty, 0),
    feePerPerson: Math.round(deliveryFee / sharerCount),
    soloFeeEach,
    // Ordering alone, each of them pays a full fee. Together, one fee.
    saved: Math.max(0, soloFeeEach * sharerCount - deliveryFee),
    msRemaining: Math.max(0, runde.closesAt - now),
  };
}

/**
 * Suggest the single cheapest item that closes the gap to the next threshold.
 * Commerce skill: a threshold works only when the user is shown the exact
 * item that clears it.
 */
export function suggestToThreshold(
  gap: Rappen,
  catalogue: Product[],
  excludeIds: Set<string>,
): Product | null {
  if (gap <= 0) return null;
  const candidates = catalogue
    .filter((p) => p.stock > 0 && !excludeIds.has(p.id) && p.price >= gap)
    .sort((a, b) => a.price - b.price);
  if (candidates.length > 0) return candidates[0];

  // Nothing single-handedly closes it; offer the best value item instead.
  return (
    catalogue
      .filter((p) => p.stock > 0 && !excludeIds.has(p.id))
      .sort((a, b) => b.price - a.price)[0] ?? null
  );
}

/** A short, unambiguous share code. No 0/O/1/I/L - people read these aloud. */
const CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ";

export function generateCode(random: () => number = Math.random): string {
  let out = "";
  for (let i = 0; i < 6; i++) {
    out += CODE_ALPHABET[Math.floor(random() * CODE_ALPHABET.length)];
  }
  return out;
}

export const HUE_PALETTE = [265, 190, 330, 45, 150, 15, 220, 95];
