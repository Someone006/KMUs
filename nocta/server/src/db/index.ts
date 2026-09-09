/**
 * Persistence. node:sqlite - zero native dependencies, single file, and fast
 * enough that a night's traffic never touches the limits.
 *
 * Two kinds of memory live here (see the `memory` skill):
 *   - working tables, freely mutable projections the app reads
 *   - `events`, an append-only hash-chained ledger that is the authority
 */

import { DatabaseSync } from "node:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

export const DB_PATH = process.env.NOCTA_DB ?? new URL("../../data/nocta.db", import.meta.url).pathname;

let db: DatabaseSync | null = null;

export function getDb(): DatabaseSync {
  if (db) return db;
  mkdirSync(dirname(DB_PATH), { recursive: true });
  db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA foreign_keys = ON");
  migrate(db);
  return db;
}

export function resetDb(): void {
  if (db) {
    db.close();
    db = null;
  }
}

function migrate(d: DatabaseSync): void {
  d.exec(`
    CREATE TABLE IF NOT EXISTS products (
      id TEXT PRIMARY KEY,
      sku TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      brand TEXT NOT NULL,
      category TEXT NOT NULL,
      price INTEGER NOT NULL,
      cost INTEGER NOT NULL,
      vat_class TEXT NOT NULL,
      unit_size INTEGER NOT NULL,
      unit_measure TEXT NOT NULL,
      unit_count INTEGER NOT NULL,
      allergens TEXT NOT NULL,
      chilled INTEGER NOT NULL,
      stock INTEGER NOT NULL,
      emoji TEXT NOT NULL,
      tags TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS zones (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      canton TEXT NOT NULL,
      lat REAL NOT NULL,
      lng REAL NOT NULL,
      radius INTEGER NOT NULL,
      base_fee INTEGER NOT NULL,
      min_basket INTEGER NOT NULL,
      drive_minutes INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS rundes (
      id TEXT PRIMARY KEY,
      code TEXT NOT NULL UNIQUE,
      zone_id TEXT NOT NULL REFERENCES zones(id),
      address TEXT NOT NULL,
      note TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      closes_at INTEGER NOT NULL,
      version INTEGER NOT NULL DEFAULT 1,
      lat REAL NOT NULL,
      lng REAL NOT NULL,
      placed_at INTEGER,
      run_id TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_rundes_status ON rundes(status);

    CREATE TABLE IF NOT EXISTS participants (
      id TEXT PRIMARY KEY,
      runde_id TEXT NOT NULL REFERENCES rundes(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      hue INTEGER NOT NULL,
      joined_at INTEGER NOT NULL,
      is_host INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_participants_runde ON participants(runde_id);

    CREATE TABLE IF NOT EXISTS runde_items (
      id TEXT PRIMARY KEY,
      runde_id TEXT NOT NULL REFERENCES rundes(id) ON DELETE CASCADE,
      participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
      product_id TEXT NOT NULL REFERENCES products(id),
      qty INTEGER NOT NULL,
      unit_price INTEGER NOT NULL,
      added_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_items_runde ON runde_items(runde_id);

    CREATE TABLE IF NOT EXISTS couriers (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      hourly_wage INTEGER NOT NULL,
      nights_this_year INTEGER NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS shifts (
      id TEXT PRIMARY KEY,
      courier_id TEXT NOT NULL REFERENCES couriers(id),
      start INTEGER NOT NULL,
      end INTEGER NOT NULL,
      night_key TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS runs (
      id TEXT PRIMARY KEY,
      courier_id TEXT REFERENCES couriers(id),
      status TEXT NOT NULL,
      distance_m INTEGER NOT NULL,
      minutes REAL NOT NULL,
      created_at INTEGER NOT NULL,
      stop_order TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS history_orders (
      id TEXT PRIMARY KEY,
      placed_at INTEGER NOT NULL,
      zone_id TEXT NOT NULL,
      basket INTEGER NOT NULL,
      sharers INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_history_zone ON history_orders(zone_id, placed_at);

    CREATE TABLE IF NOT EXISTS events (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      at INTEGER NOT NULL,
      kind TEXT NOT NULL,
      actor TEXT NOT NULL,
      subject TEXT NOT NULL,
      payload TEXT NOT NULL,
      prev_hash TEXT NOT NULL,
      hash TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events_subject ON events(subject);
  `);
}

export function newId(): string {
  return randomUUID();
}

/* -------------------------------------------------------------------------- */
/*  Append-only ledger                                                        */
/* -------------------------------------------------------------------------- */

export interface LedgerEvent {
  seq: number;
  at: number;
  kind: string;
  actor: string;
  subject: string;
  payload: unknown;
  prevHash: string;
  hash: string;
}

const GENESIS = "0".repeat(64);

/**
 * Append an event, chaining its hash to the previous one. Any later edit to a
 * stored row breaks the chain, which is what makes the log evidence rather
 * than a convenience.
 */
export function appendEvent(
  kind: string,
  actor: string,
  subject: string,
  payload: unknown,
  at: number,
): LedgerEvent {
  const d = getDb();
  const prev = d
    .prepare("SELECT hash FROM events ORDER BY seq DESC LIMIT 1")
    .get() as { hash: string } | undefined;
  const prevHash = prev?.hash ?? GENESIS;
  const body = JSON.stringify({ at, kind, actor, subject, payload });
  const hash = createHash("sha256").update(prevHash + body).digest("hex");

  const info = d
    .prepare(
      `INSERT INTO events (at, kind, actor, subject, payload, prev_hash, hash)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(at, kind, actor, subject, JSON.stringify(payload), prevHash, hash);

  return {
    seq: Number(info.lastInsertRowid),
    at, kind, actor, subject, payload, prevHash, hash,
  };
}

/** Recompute the chain. Returns the first sequence number that fails, if any. */
export function verifyLedger(): { ok: boolean; checked: number; brokenAt: number | null } {
  const d = getDb();
  const rows = d
    .prepare("SELECT seq, at, kind, actor, subject, payload, prev_hash, hash FROM events ORDER BY seq ASC")
    .all() as Array<{
      seq: number; at: number; kind: string; actor: string;
      subject: string; payload: string; prev_hash: string; hash: string;
    }>;

  let prevHash = GENESIS;
  for (const row of rows) {
    const body = JSON.stringify({
      at: row.at, kind: row.kind, actor: row.actor,
      subject: row.subject, payload: JSON.parse(row.payload),
    });
    const expected = createHash("sha256").update(prevHash + body).digest("hex");
    if (row.prev_hash !== prevHash || row.hash !== expected) {
      return { ok: false, checked: rows.length, brokenAt: row.seq };
    }
    prevHash = row.hash;
  }
  return { ok: true, checked: rows.length, brokenAt: null };
}
