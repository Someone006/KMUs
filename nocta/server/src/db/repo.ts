/**
 * Row <-> domain mapping. The only place that knows SQLite column names.
 * Everything above this file speaks domain types.
 */

import { getDb } from "./index.js";
import type {
  Courier, Participant, Product, Runde, RundeItem, Zone,
} from "../domain/types.js";
import type { Allergen, Category } from "../domain/types.js";

type ProductRow = {
  id: string; sku: string; name: string; brand: string; category: string;
  price: number; cost: number; vat_class: string; unit_size: number;
  unit_measure: string; unit_count: number; allergens: string;
  chilled: number; stock: number; emoji: string; tags: string;
};

function toProduct(r: ProductRow): Product {
  return {
    id: r.id, sku: r.sku, name: r.name, brand: r.brand,
    category: r.category as Category,
    price: r.price, cost: r.cost,
    vatClass: r.vat_class as Product["vatClass"],
    unitSize: r.unit_size,
    unitMeasure: r.unit_measure as Product["unitMeasure"],
    unitCount: r.unit_count,
    allergens: JSON.parse(r.allergens) as Allergen[],
    chilled: r.chilled === 1,
    stock: r.stock, emoji: r.emoji,
    tags: JSON.parse(r.tags) as string[],
  };
}

export function allProducts(): Product[] {
  return (getDb().prepare("SELECT * FROM products ORDER BY category, name").all() as ProductRow[])
    .map(toProduct);
}

export function productMap(): Map<string, Product> {
  return new Map(allProducts().map((p) => [p.id, p]));
}

export function getProduct(id: string): Product | null {
  const row = getDb().prepare("SELECT * FROM products WHERE id = ?").get(id) as ProductRow | undefined;
  return row ? toProduct(row) : null;
}

export function decrementStock(productId: string, qty: number): void {
  getDb()
    .prepare("UPDATE products SET stock = MAX(0, stock - ?) WHERE id = ?")
    .run(qty, productId);
}

type ZoneRow = {
  id: string; name: string; canton: string; lat: number; lng: number;
  radius: number; base_fee: number; min_basket: number; drive_minutes: number;
};

function toZone(r: ZoneRow): Zone {
  return {
    id: r.id, name: r.name, canton: r.canton, lat: r.lat, lng: r.lng,
    radius: r.radius, baseFee: r.base_fee, minBasket: r.min_basket,
    driveMinutes: r.drive_minutes,
  };
}

export function allZones(): Zone[] {
  return (getDb().prepare("SELECT * FROM zones ORDER BY name").all() as ZoneRow[]).map(toZone);
}

export function getZone(id: string): Zone | null {
  const row = getDb().prepare("SELECT * FROM zones WHERE id = ?").get(id) as ZoneRow | undefined;
  return row ? toZone(row) : null;
}

type RundeRow = {
  id: string; code: string; zone_id: string; address: string; note: string;
  status: string; created_at: number; closes_at: number; version: number;
  lat: number; lng: number; placed_at: number | null; run_id: string | null;
};

export interface RundeRecord extends Runde {
  lat: number;
  lng: number;
  placedAt: number | null;
  runId: string | null;
}

function toRunde(r: RundeRow): RundeRecord {
  return {
    id: r.id, code: r.code, zoneId: r.zone_id, address: r.address, note: r.note,
    status: r.status as Runde["status"], createdAt: r.created_at,
    closesAt: r.closes_at, version: r.version,
    lat: r.lat, lng: r.lng, placedAt: r.placed_at, runId: r.run_id,
  };
}

export function getRundeByCode(code: string): RundeRecord | null {
  const row = getDb()
    .prepare("SELECT * FROM rundes WHERE code = ?")
    .get(code.toUpperCase()) as RundeRow | undefined;
  return row ? toRunde(row) : null;
}

export function getRundeById(id: string): RundeRecord | null {
  const row = getDb().prepare("SELECT * FROM rundes WHERE id = ?").get(id) as RundeRow | undefined;
  return row ? toRunde(row) : null;
}

export function rundesByStatus(status: string): RundeRecord[] {
  return (getDb().prepare("SELECT * FROM rundes WHERE status = ? ORDER BY created_at").all(status) as RundeRow[])
    .map(toRunde);
}

export function insertRunde(r: RundeRecord): void {
  getDb()
    .prepare(
      `INSERT INTO rundes (id, code, zone_id, address, note, status, created_at, closes_at, version, lat, lng)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(r.id, r.code, r.zoneId, r.address, r.note, r.status, r.createdAt, r.closesAt, r.version, r.lat, r.lng);
}

/** Every mutation bumps the version so clients can reconcile a stale view. */
export function bumpVersion(rundeId: string): number {
  const d = getDb();
  d.prepare("UPDATE rundes SET version = version + 1 WHERE id = ?").run(rundeId);
  const row = d.prepare("SELECT version FROM rundes WHERE id = ?").get(rundeId) as { version: number };
  return row.version;
}

export function setRundeStatus(rundeId: string, status: string, placedAt?: number): void {
  if (placedAt !== undefined) {
    getDb().prepare("UPDATE rundes SET status = ?, placed_at = ? WHERE id = ?").run(status, placedAt, rundeId);
  } else {
    getDb().prepare("UPDATE rundes SET status = ? WHERE id = ?").run(status, rundeId);
  }
}

export function setRundeRun(rundeId: string, runId: string): void {
  getDb().prepare("UPDATE rundes SET run_id = ? WHERE id = ?").run(runId, rundeId);
}

type ParticipantRow = {
  id: string; runde_id: string; name: string; hue: number;
  joined_at: number; is_host: number;
};

export function participantsOf(rundeId: string): Participant[] {
  return (getDb().prepare("SELECT * FROM participants WHERE runde_id = ? ORDER BY joined_at").all(rundeId) as ParticipantRow[])
    .map((r) => ({
      id: r.id, rundeId: r.runde_id, name: r.name, hue: r.hue,
      joinedAt: r.joined_at, isHost: r.is_host === 1,
    }));
}

export function insertParticipant(p: Participant): void {
  getDb()
    .prepare("INSERT INTO participants (id, runde_id, name, hue, joined_at, is_host) VALUES (?, ?, ?, ?, ?, ?)")
    .run(p.id, p.rundeId, p.name, p.hue, p.joinedAt, p.isHost ? 1 : 0);
}

export function getParticipant(id: string): Participant | null {
  const r = getDb().prepare("SELECT * FROM participants WHERE id = ?").get(id) as ParticipantRow | undefined;
  return r
    ? { id: r.id, rundeId: r.runde_id, name: r.name, hue: r.hue, joinedAt: r.joined_at, isHost: r.is_host === 1 }
    : null;
}

type ItemRow = {
  id: string; runde_id: string; participant_id: string; product_id: string;
  qty: number; unit_price: number; added_at: number;
};

export function itemsOf(rundeId: string): RundeItem[] {
  return (getDb().prepare("SELECT * FROM runde_items WHERE runde_id = ? ORDER BY added_at").all(rundeId) as ItemRow[])
    .map((r) => ({
      id: r.id, rundeId: r.runde_id, participantId: r.participant_id,
      productId: r.product_id, qty: r.qty, unitPrice: r.unit_price, addedAt: r.added_at,
    }));
}

export function findItem(rundeId: string, participantId: string, productId: string): RundeItem | null {
  const r = getDb()
    .prepare("SELECT * FROM runde_items WHERE runde_id = ? AND participant_id = ? AND product_id = ?")
    .get(rundeId, participantId, productId) as ItemRow | undefined;
  return r
    ? { id: r.id, rundeId: r.runde_id, participantId: r.participant_id, productId: r.product_id, qty: r.qty, unitPrice: r.unit_price, addedAt: r.added_at }
    : null;
}

export function insertItem(i: RundeItem): void {
  getDb()
    .prepare("INSERT INTO runde_items (id, runde_id, participant_id, product_id, qty, unit_price, added_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
    .run(i.id, i.rundeId, i.participantId, i.productId, i.qty, i.unitPrice, i.addedAt);
}

export function updateItemQty(itemId: string, qty: number): void {
  if (qty <= 0) {
    getDb().prepare("DELETE FROM runde_items WHERE id = ?").run(itemId);
  } else {
    getDb().prepare("UPDATE runde_items SET qty = ? WHERE id = ?").run(qty, itemId);
  }
}

export function deleteItem(itemId: string): void {
  getDb().prepare("DELETE FROM runde_items WHERE id = ?").run(itemId);
}

type CourierRow = {
  id: string; name: string; hourly_wage: number;
  nights_this_year: number; active: number;
};

export function allCouriers(): Courier[] {
  return (getDb().prepare("SELECT * FROM couriers ORDER BY name").all() as CourierRow[]).map((r) => ({
    id: r.id, name: r.name, hourlyWage: r.hourly_wage,
    nightsThisYear: r.nights_this_year, active: r.active === 1,
  }));
}

export interface ShiftRow {
  id: string;
  courier_id: string;
  start: number;
  end: number;
  night_key: string;
}

export function allShifts(): ShiftRow[] {
  return getDb().prepare("SELECT * FROM shifts ORDER BY start").all() as unknown as ShiftRow[];
}

export function insertShift(s: ShiftRow): void {
  getDb()
    .prepare("INSERT INTO shifts (id, courier_id, start, end, night_key) VALUES (?, ?, ?, ?, ?)")
    .run(s.id, s.courier_id, s.start, s.end, s.night_key);
}

export function historyOrders(): Array<{ placedAt: number; zoneId: string; basket: number; sharers: number }> {
  return (getDb().prepare("SELECT placed_at, zone_id, basket, sharers FROM history_orders").all() as Array<{
    placed_at: number; zone_id: string; basket: number; sharers: number;
  }>).map((r) => ({ placedAt: r.placed_at, zoneId: r.zone_id, basket: r.basket, sharers: r.sharers }));
}
