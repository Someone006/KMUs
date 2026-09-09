/**
 * Preisbekanntgabeverordnung (PBV) Art. 11 - Grundpreis.
 *
 * Retail sale of goods offered by volume or weight must display the unit price
 * alongside the selling price: CHF per litre for liquids, CHF per kilogram for
 * solids. Delivery apps routinely omit this; it is a cheap compliance win and
 * genuinely helps customers compare a 500ml can against a 1.5l bottle.
 */

import { type Rappen, formatCHF } from "./money.js";

export type MeasureUnit = "ml" | "g" | "piece";

export interface Measurable {
  /** Size of ONE item in its base unit. 500 for a 500ml can. */
  unitSize: number;
  unitMeasure: MeasureUnit;
  /** Items in the pack. 6 for a six-pack. */
  unitCount: number;
}

export interface UnitPrice {
  /** Price per litre or per kilogram, in Rappen. */
  amount: Rappen;
  /** "l" or "kg". */
  per: string;
  label: string;
}

/**
 * Unit price, or null where PBV imposes no Grundpreis duty
 * (goods sold per piece, e.g. a chocolate bar counted as one item).
 */
export function unitPrice(price: Rappen, m: Measurable): UnitPrice | null {
  const totalBase = m.unitSize * m.unitCount;
  if (totalBase <= 0) return null;

  switch (m.unitMeasure) {
    case "ml": {
      const perLitre = Math.round((price * 1000) / totalBase);
      return { amount: perLitre, per: "l", label: `${formatCHF(perLitre)}/l` };
    }
    case "g": {
      const perKg = Math.round((price * 1000) / totalBase);
      return { amount: perKg, per: "kg", label: `${formatCHF(perKg)}/kg` };
    }
    case "piece":
      return null;
  }
}

/** Human-readable pack description: "6 x 500ml", "330ml", "180g". */
export function packLabel(m: Measurable): string {
  if (m.unitMeasure === "piece") {
    return m.unitCount > 1 ? `${m.unitCount} Stück` : "1 Stück";
  }
  const one = `${m.unitSize}${m.unitMeasure}`;
  return m.unitCount > 1 ? `${m.unitCount} × ${one}` : one;
}
