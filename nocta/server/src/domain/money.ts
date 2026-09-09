/**
 * Money. All internal amounts are integer Rappen (1/100 CHF).
 * Floating point never touches a currency value.
 *
 * Swiss VAT (MWSTG Art. 25), rates effective 2024-01-01:
 *   - 2.6%  reduced rate: food and non-alcoholic beverages
 *   - 8.1%  standard rate: services, including a separately invoiced delivery service
 *
 * Consumer prices in Switzerland are quoted VAT-inclusive (PBV Art. 4), so we
 * store gross prices and extract the contained VAT rather than adding it on.
 */

export type Rappen = number;

/** VAT classes we actually sell. No alcohol, no tobacco. */
export type VatClass = "food" | "service";

/** Versioned reference data. Changing a rate is a data edit plus a test. */
export const VAT_RATES = {
  version: "2024-01-01",
  source: "MWSTG Art. 25",
  rates: {
    food: 260, // 2.60% in basis points x100 -> 260 = 2.60%
    service: 810, // 8.10%
  },
} as const;

/** Basis-points-times-100 representation keeps rate maths in integers. */
const RATE_SCALE = 10_000;

export function vatRate(cls: VatClass): number {
  return VAT_RATES.rates[cls];
}

/**
 * VAT contained within a gross amount.
 *   vat = gross * rate / (1 + rate)
 * Rounded half-up to the nearest Rappen.
 */
export function vatIncludedIn(gross: Rappen, cls: VatClass): Rappen {
  const rate = vatRate(cls);
  return Math.round((gross * rate) / (RATE_SCALE + rate));
}

export function netOf(gross: Rappen, cls: VatClass): Rappen {
  return gross - vatIncludedIn(gross, cls);
}

/**
 * Swiss cash rounding: payable totals settle to the nearest 5 Rappen.
 * Applied only to the final amount tendered, never to line items, or the
 * lines stop summing to the total.
 */
export function roundToFiveRappen(amount: Rappen): Rappen {
  return Math.round(amount / 5) * 5;
}

export function formatCHF(amount: Rappen): string {
  const sign = amount < 0 ? "-" : "";
  const abs = Math.abs(amount);
  const francs = Math.floor(abs / 100);
  const rappen = abs % 100;
  return `${sign}CHF ${francs}.${String(rappen).padStart(2, "0")}`;
}

export function parseCHF(input: string): Rappen {
  const cleaned = input.replace(/[^0-9.,-]/g, "").replace(",", ".");
  // Number("") is 0, so an input with no digits at all would silently become
  // CHF 0.00. Require an actual numeral before trusting the conversion.
  if (!/^-?\d+(\.\d+)?$/.test(cleaned)) {
    throw new RangeError(`Not a monetary amount: ${input}`);
  }
  const value = Number(cleaned);
  if (!Number.isFinite(value)) {
    throw new RangeError(`Not a monetary amount: ${input}`);
  }
  return Math.round(value * 100);
}

/**
 * Split an amount into `n` shares that sum EXACTLY to the original.
 * Largest-remainder method: everyone gets the floor, then the leftover
 * Rappen go one each to the largest remainders.
 *
 * `weights` lets a split be proportional; omit for an equal split.
 * This function is the reason a Runde's shares always reconcile.
 */
export function splitExact(amount: Rappen, n: number, weights?: number[]): Rappen[] {
  if (!Number.isInteger(n) || n <= 0) {
    throw new RangeError(`Cannot split into ${n} shares`);
  }
  const w = weights ?? new Array(n).fill(1);
  if (w.length !== n) {
    throw new RangeError(`Expected ${n} weights, received ${w.length}`);
  }
  const totalWeight = w.reduce((a, b) => a + b, 0);

  // Degenerate case: nobody has weight (e.g. every basket empty) -> split equally.
  if (totalWeight <= 0) return splitExact(amount, n);

  const exact = w.map((wi) => (amount * wi) / totalWeight);
  const shares = exact.map((e) => Math.floor(e));
  let remainder = amount - shares.reduce((a, b) => a + b, 0);

  const order = exact
    .map((e, i) => ({ i, frac: e - Math.floor(e) }))
    .sort((a, b) => b.frac - a.frac || a.i - b.i);

  for (let k = 0; remainder > 0; k++, remainder--) {
    shares[order[k % n].i] += 1;
  }
  return shares;
}
