import type { Rappen, VatClass } from "./money.js";
import type { MeasureUnit } from "./pbv.js";

/** LIV / LGV allergen classes requiring declaration for distance selling. */
export type Allergen =
  | "gluten" | "milk" | "egg" | "soy" | "nuts" | "peanut"
  | "sesame" | "sulphite" | "celery" | "mustard" | "fish" | "crustacean";

export type Category = "energy" | "softdrink" | "water" | "juice" | "coffee"
  | "chips" | "sweets" | "chocolate" | "icecream" | "savoury" | "hangover";

export interface Product {
  id: string;
  sku: string;
  name: string;
  brand: string;
  category: Category;
  /** Gross, VAT-inclusive shelf price in Rappen. */
  price: Rappen;
  /** What we pay our wholesaler, in Rappen. Drives margin, never shown. */
  cost: Rappen;
  vatClass: VatClass;
  unitSize: number;
  unitMeasure: MeasureUnit;
  unitCount: number;
  allergens: Allergen[];
  /** Requires the cold bag; adds packaging cost and constrains run length. */
  chilled: boolean;
  stock: number;
  emoji: string;
  tags: string[];
}

export interface Zone {
  id: string;
  name: string;
  canton: string;
  lat: number;
  lng: number;
  /** Serviced radius in metres. */
  radius: number;
  baseFee: Rappen;
  minBasket: Rappen;
  /** Minutes from depot to the zone centroid at night speed. */
  driveMinutes: number;
}

export type RundeStatus = "open" | "locked" | "placed" | "dispatched" | "delivered" | "cancelled";

export interface Participant {
  id: string;
  rundeId: string;
  name: string;
  /** Palette index, so each sharer reads as a distinct colour in the UI. */
  hue: number;
  joinedAt: number;
  isHost: boolean;
}

export interface RundeItem {
  id: string;
  rundeId: string;
  participantId: string;
  productId: string;
  qty: number;
  /** Price captured at add-time. Catalogue changes never mutate a live cart. */
  unitPrice: Rappen;
  addedAt: number;
}

export interface Runde {
  id: string;
  code: string;
  zoneId: string;
  address: string;
  note: string;
  status: RundeStatus;
  createdAt: number;
  /** Auto-lock deadline. Creates urgency and bounds dispatch planning. */
  closesAt: number;
  /** Monotonic, bumped on every mutation. Clients reconcile against it. */
  version: number;
}

export interface Courier {
  id: string;
  name: string;
  /** Gross hourly wage in Rappen. */
  hourlyWage: Rappen;
  /** Nights worked in the rolling year - decides ArG 25% vs 10% treatment. */
  nightsThisYear: number;
  active: boolean;
}

export interface Drop {
  rundeId: string;
  code: string;
  lat: number;
  lng: number;
  address: string;
  items: number;
  chilled: boolean;
  basket: Rappen;
  placedAt: number;
}

export interface Run {
  id: string;
  courierId: string | null;
  stops: Drop[];
  /** Index into stops, in visit order. */
  order: number[];
  distanceM: number;
  minutes: number;
  createdAt: number;
}
