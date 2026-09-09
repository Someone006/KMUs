/**
 * Seed a realistic Zurich night operation: catalogue, zones, couriers, and
 * eight weeks of trading history so the forecast has something to learn from.
 *
 * Deterministic: a fixed-seed PRNG means the demo, the tests and the
 * screenshots all show the same numbers.
 */

import { getDb, appendEvent, newId, DB_PATH } from "./index.js";
import { nightKey, localParts, localDateKey } from "../domain/time.js";
import type { Allergen } from "../domain/types.js";

/** mulberry32 - small, fast, and reproducible across runs. */
function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

interface SeedProduct {
  sku: string; name: string; brand: string; category: string;
  price: number; cost: number; size: number; measure: "ml" | "g" | "piece";
  count: number; allergens: Allergen[]; chilled: boolean; stock: number;
  emoji: string; tags: string[];
}

const CATALOGUE: SeedProduct[] = [
  // --- Energy -------------------------------------------------------------
  { sku: "RB-250", name: "Red Bull", brand: "Red Bull", category: "energy", price: 350, cost: 195, size: 250, measure: "ml", count: 1, allergens: [], chilled: true, stock: 40, emoji: "🐂", tags: ["bestseller", "wachbleiben"] },
  { sku: "RB-250-SF", name: "Red Bull Sugarfree", brand: "Red Bull", category: "energy", price: 350, cost: 195, size: 250, measure: "ml", count: 1, allergens: [], chilled: true, stock: 20, emoji: "🐂", tags: ["zuckerfrei"] },
  { sku: "MON-500", name: "Monster Energy", brand: "Monster", category: "energy", price: 420, cost: 250, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 27, emoji: "⚡", tags: ["bestseller"] },
  // --- Softdrinks ---------------------------------------------------------
  { sku: "CC-500", name: "Coca-Cola", brand: "Coca-Cola", category: "softdrink", price: 320, cost: 175, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 50, emoji: "🥤", tags: ["bestseller"] },
  { sku: "CCZ-500", name: "Coca-Cola Zero", brand: "Coca-Cola", category: "softdrink", price: 320, cost: 175, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 33, emoji: "🥤", tags: ["zuckerfrei"] },
  { sku: "RIV-500", name: "Rivella Rot", brand: "Rivella", category: "softdrink", price: 320, cost: 180, size: 500, measure: "ml", count: 1, allergens: ["milk"], chilled: true, stock: 30, emoji: "🇨🇭", tags: ["schweizer-klassiker"] },
  { sku: "FAN-500", name: "Fanta Orange", brand: "Fanta", category: "softdrink", price: 320, cost: 175, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 23, emoji: "🍊", tags: [] },
  { sku: "SPR-500", name: "Sprite", brand: "Sprite", category: "softdrink", price: 320, cost: 175, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 22, emoji: "🍋", tags: [] },
  { sku: "ICT-500", name: "Ice Tea Lemon", brand: "Migros", category: "softdrink", price: 300, cost: 155, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 28, emoji: "🧊", tags: [] },
  // --- Water --------------------------------------------------------------
  { sku: "VAL-500", name: "Valser Prickelnd", brand: "Valser", category: "water", price: 250, cost: 110, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 43, emoji: "💧", tags: ["schweizer-klassiker"] },
  { sku: "VAL-150", name: "Valser Still 1.5l", brand: "Valser", category: "water", price: 350, cost: 150, size: 1500, measure: "ml", count: 1, allergens: [], chilled: false, stock: 20, emoji: "💧", tags: ["grosspackung"] },
  { sku: "EVI-500", name: "Evian", brand: "Evian", category: "water", price: 280, cost: 130, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 18, emoji: "💧", tags: [] },
  // --- Juice / Coffee -----------------------------------------------------
  { sku: "MIC-500", name: "Michel Orange", brand: "Michel", category: "juice", price: 380, cost: 220, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 15, emoji: "🍊", tags: [] },
  { sku: "EMM-230", name: "Emmi Caffè Latte Macchiato", brand: "Emmi", category: "coffee", price: 350, cost: 200, size: 230, measure: "ml", count: 1, allergens: ["milk"], chilled: true, stock: 23, emoji: "☕", tags: ["wachbleiben", "schweizer-klassiker"] },
  // --- Chips --------------------------------------------------------------
  { sku: "ZW-PAP-175", name: "Zweifel Paprika Chips", brand: "Zweifel", category: "chips", price: 590, cost: 380, size: 175, measure: "g", count: 1, allergens: ["milk"], chilled: false, stock: 33, emoji: "🥔", tags: ["bestseller", "schweizer-klassiker"] },
  { sku: "ZW-NAT-175", name: "Zweifel Nature Chips", brand: "Zweifel", category: "chips", price: 590, cost: 380, size: 175, measure: "g", count: 1, allergens: [], chilled: false, stock: 25, emoji: "🥔", tags: ["schweizer-klassiker"] },
  { sku: "PRI-165", name: "Pringles Original", brand: "Pringles", category: "chips", price: 560, cost: 350, size: 165, measure: "g", count: 1, allergens: ["gluten", "milk"], chilled: false, stock: 20, emoji: "🥫", tags: ["bestseller"] },
  { sku: "DOR-170", name: "Doritos Nacho Cheese", brand: "Doritos", category: "chips", price: 550, cost: 340, size: 170, measure: "g", count: 1, allergens: ["milk", "gluten"], chilled: false, stock: 18, emoji: "🔺", tags: [] },
  // --- Sweets / Chocolate -------------------------------------------------
  { sku: "HAR-200", name: "Haribo Goldbären", brand: "Haribo", category: "sweets", price: 420, cost: 250, size: 200, measure: "g", count: 1, allergens: [], chilled: false, stock: 27, emoji: "🐻", tags: ["bestseller"] },
  { sku: "MM-200", name: "M&M's Peanut", brand: "M&M's", category: "sweets", price: 480, cost: 300, size: 200, measure: "g", count: 1, allergens: ["peanut", "milk", "soy"], chilled: false, stock: 17, emoji: "🍬", tags: [] },
  { sku: "TOB-100", name: "Toblerone", brand: "Toblerone", category: "chocolate", price: 390, cost: 230, size: 100, measure: "g", count: 1, allergens: ["milk", "nuts", "egg"], chilled: false, stock: 22, emoji: "🍫", tags: ["schweizer-klassiker"] },
  { sku: "LIN-100", name: "Lindt Excellence 70%", brand: "Lindt", category: "chocolate", price: 450, cost: 270, size: 100, measure: "g", count: 1, allergens: ["milk", "soy"], chilled: false, stock: 15, emoji: "🍫", tags: ["schweizer-klassiker"] },
  { sku: "RAG-50", name: "Ragusa Classique", brand: "Camille Bloch", category: "chocolate", price: 250, cost: 140, size: 50, measure: "g", count: 1, allergens: ["milk", "nuts", "soy"], chilled: false, stock: 23, emoji: "🍫", tags: ["schweizer-klassiker"] },
  { sku: "CAI-81", name: "Cailler Branches", brand: "Cailler", category: "chocolate", price: 300, cost: 175, size: 27, measure: "g", count: 3, allergens: ["milk", "nuts", "soy"], chilled: false, stock: 20, emoji: "🍫", tags: ["schweizer-klassiker"] },
  // --- Ice cream (chilled, constrains run length) --------------------------
  { sku: "BJ-465", name: "Ben & Jerry's Cookie Dough", brand: "Ben & Jerry's", category: "icecream", price: 990, cost: 650, size: 465, measure: "ml", count: 1, allergens: ["milk", "egg", "gluten", "soy"], chilled: true, stock: 10, emoji: "🍦", tags: ["bestseller"] },
  { sku: "MAG-110", name: "Magnum Classic", brand: "Magnum", category: "icecream", price: 420, cost: 250, size: 110, measure: "ml", count: 1, allergens: ["milk", "soy", "nuts"], chilled: true, stock: 13, emoji: "🍫", tags: [] },
  // --- Savoury / Recovery -------------------------------------------------
  { sku: "ZW-NUS-200", name: "Zweifel Nüssli Mix", brand: "Zweifel", category: "savoury", price: 620, cost: 400, size: 200, measure: "g", count: 1, allergens: ["peanut", "nuts", "gluten", "soy"], chilled: false, stock: 15, emoji: "🥜", tags: [] },
  { sku: "BRE-6", name: "Laugenbrezel 6er", brand: "Hausbäckerei", category: "savoury", price: 650, cost: 380, size: 60, measure: "g", count: 6, allergens: ["gluten"], chilled: false, stock: 8, emoji: "🥨", tags: ["teilen"] },
  { sku: "ISO-500", name: "Isostar Elektrolyt", brand: "Isostar", category: "hangover", price: 390, cost: 210, size: 500, measure: "ml", count: 1, allergens: [], chilled: true, stock: 12, emoji: "🧪", tags: ["morgen-danach"] },
  { sku: "BAN-4", name: "Bananen 4er", brand: "Frisch", category: "hangover", price: 320, cost: 165, size: 120, measure: "g", count: 4, allergens: [], chilled: false, stock: 9, emoji: "🍌", tags: ["morgen-danach"] },
];

const ZONES = [
  { id: "z-k4", name: "Kreis 4 · Langstrasse", canton: "ZH", lat: 47.3782, lng: 8.5245, radius: 900, baseFee: 590, minBasket: 1500, drive: 4 },
  { id: "z-k5", name: "Kreis 5 · Zürich West", canton: "ZH", lat: 47.3885, lng: 8.5170, radius: 1100, baseFee: 590, minBasket: 1500, drive: 6 },
  { id: "z-k1", name: "Kreis 1 · Niederdorf", canton: "ZH", lat: 47.3725, lng: 8.5440, radius: 800, baseFee: 690, minBasket: 1500, drive: 7 },
  { id: "z-k3", name: "Kreis 3 · Wiedikon", canton: "ZH", lat: 47.3690, lng: 8.5100, radius: 1200, baseFee: 690, minBasket: 2000, drive: 8 },
  { id: "z-k6", name: "Kreis 6 · Unterstrass", canton: "ZH", lat: 47.3900, lng: 8.5420, radius: 1200, baseFee: 690, minBasket: 2000, drive: 9 },
  { id: "z-k11", name: "Kreis 11 · Oerlikon", canton: "ZH", lat: 47.4100, lng: 8.5450, radius: 1400, baseFee: 790, minBasket: 2500, drive: 14 },
];

const COURIERS = [
  { id: "c-luca", name: "Luca Bernasconi", wage: 2800, nights: 41 },
  { id: "c-sara", name: "Sara Wyss", wage: 2650, nights: 12 },
  { id: "c-nadia", name: "Nadia Hoxha", wage: 2700, nights: 33 },
  { id: "c-timo", name: "Timo Frei", wage: 2600, nights: 8 },
];

/** Depot: Militärstrasse, Kreis 4. Central to the serviced zones. */
export const DEPOT = { lat: 47.3765, lng: 8.5280 };

const NIGHT_SHAPE_WEEKEND = [0.04, 0.06, 0.09, 0.12, 0.16, 0.17, 0.14, 0.11, 0.07, 0.04];
const NIGHT_SHAPE_WEEKDAY = [0.07, 0.11, 0.15, 0.18, 0.17, 0.13, 0.09, 0.05, 0.03, 0.02];
const TRADING_HOURS = [18, 19, 20, 21, 22, 23, 0, 1, 2, 3];

/**
 * Epoch whose Europe/Zurich wall clock reads `hour`:00 on the given local
 * date. Going through Intl rather than a fixed offset keeps the roster correct
 * across the summer-time boundary.
 */
function zurichEpochFor(dateKey: string, hour: number): number {
  const guess = Date.parse(`${dateKey}T${String(hour).padStart(2, "0")}:00:00Z`);
  const drift = localParts(guess).hour - hour;
  return guess - drift * 3_600_000;
}

/** Defaults to the real clock so a fresh demo always has a live roster. */
export function seed(referenceNow = Date.now()): void {
  const d = getDb();
  const rng = makeRng(20260912);

  d.exec(`
    DELETE FROM runde_items; DELETE FROM participants; DELETE FROM rundes;
    DELETE FROM runs; DELETE FROM shifts; DELETE FROM history_orders;
    DELETE FROM couriers; DELETE FROM products; DELETE FROM zones; DELETE FROM events;
  `);

  const insertProduct = d.prepare(
    `INSERT INTO products (id, sku, name, brand, category, price, cost, vat_class,
      unit_size, unit_measure, unit_count, allergens, chilled, stock, emoji, tags)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'food', ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  for (const p of CATALOGUE) {
    insertProduct.run(
      `p-${p.sku.toLowerCase()}`, p.sku, p.name, p.brand, p.category, p.price, p.cost,
      p.size, p.measure, p.count, JSON.stringify(p.allergens),
      p.chilled ? 1 : 0, p.stock, p.emoji, JSON.stringify(p.tags),
    );
  }

  const insertZone = d.prepare(
    "INSERT INTO zones (id, name, canton, lat, lng, radius, base_fee, min_basket, drive_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
  );
  for (const z of ZONES) {
    insertZone.run(z.id, z.name, z.canton, z.lat, z.lng, z.radius, z.baseFee, z.minBasket, z.drive);
  }

  const insertCourier = d.prepare(
    "INSERT INTO couriers (id, name, hourly_wage, nights_this_year, active) VALUES (?, ?, ?, ?, 1)",
  );
  for (const c of COURIERS) insertCourier.run(c.id, c.name, c.wage, c.nights);

  // --- Eight weeks of trading history, shaped like a real night ------------
  const insertHistory = d.prepare(
    "INSERT INTO history_orders (id, placed_at, zone_id, basket, sharers) VALUES (?, ?, ?, ?, ?)",
  );
  const DAY = 86_400_000;
  let historyCount = 0;

  for (let daysAgo = 56; daysAgo >= 1; daysAgo--) {
    const dayStart = referenceNow - daysAgo * DAY;
    const weekday = new Date(dayStart).getUTCDay();
    const isWeekend = weekday === 5 || weekday === 6;
    const shape = isWeekend ? NIGHT_SHAPE_WEEKEND : NIGHT_SHAPE_WEEKDAY;
    // Volume grows week over week - a young business, not a steady state.
    const growth = 1 + (56 - daysAgo) / 56 * 0.45;
    const nightVolume = (isWeekend ? 62 : 30) * growth * (0.85 + rng() * 0.3);

    TRADING_HOURS.forEach((hour, idx) => {
      const expected = nightVolume * shape[idx];
      const count = Math.round(expected + (rng() - 0.5) * Math.sqrt(expected) * 2);
      for (let k = 0; k < Math.max(0, count); k++) {
        // Hours past midnight belong to the following calendar day.
        const dayOffset = hour < 6 ? DAY : 0;
        const base = new Date(dayStart);
        base.setUTCHours(hour, Math.floor(rng() * 60), 0, 0);
        const placedAt = base.getTime() + dayOffset;

        // Zone popularity: the nightlife districts dominate.
        const r = rng();
        const zoneId =
          r < 0.32 ? "z-k4" : r < 0.55 ? "z-k5" : r < 0.72 ? "z-k1"
          : r < 0.84 ? "z-k3" : r < 0.94 ? "z-k6" : "z-k11";

        // Group orders are commoner late and at weekends.
        const groupBias = (isWeekend ? 0.35 : 0.18) + (hour >= 22 || hour < 4 ? 0.2 : 0);
        const sharers = rng() < groupBias ? 2 + Math.floor(rng() * 5) : 1;
        const basket = Math.round((1200 + rng() * 1400) * (0.75 + sharers * 0.55));

        insertHistory.run(newId(), placedAt, zoneId, basket, sharers);
        historyCount++;
      }
    });
  }

  // --- Tonight's roster ----------------------------------------------------
  // Shifts begin at 18:00 local on the night currently being traded.
  const shiftStart = zurichEpochFor(nightKey(referenceNow), 18);
  const insertShift = d.prepare(
    "INSERT INTO shifts (id, courier_id, start, end, night_key) VALUES (?, ?, ?, ?, ?)",
  );
  const roster: Array<[string, number, number]> = [
    ["c-luca", 0, 8],      // 18:00 -> 02:00
    ["c-sara", 2, 9],      // 20:00 -> 03:00
    ["c-nadia", 3, 9],     // 21:00 -> 03:00
    ["c-timo", 4, 8.5],    // 22:00 -> 02:30
  ];
  for (const [courierId, offsetH, durationH] of roster) {
    const start = shiftStart + offsetH * 3_600_000;
    const end = start + durationH * 3_600_000;
    insertShift.run(newId(), courierId, start, end, nightKey(start));
  }

  appendEvent("system.seeded", "system", "nocta", {
    products: CATALOGUE.length,
    zones: ZONES.length,
    couriers: COURIERS.length,
    historyOrders: historyCount,
    referenceNow,
  }, referenceNow);

  console.log(
    `Seeded ${CATALOGUE.length} products, ${ZONES.length} zones, ${COURIERS.length} couriers, ` +
    `${historyCount} historical orders, ${roster.length} shifts.\n  -> ${DB_PATH}`,
  );
}

// Run directly: `npm run seed`
if (process.argv[1] && import.meta.url.endsWith(process.argv[1].split("/").pop() ?? "")) {
  seed();
}
