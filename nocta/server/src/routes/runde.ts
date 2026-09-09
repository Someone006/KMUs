/**
 * The customer-facing contract: create a Runde, share the code, everyone adds
 * to one cart, one drop, everyone pays their own share.
 */

import { Router } from "express";
import { randomUUID } from "node:crypto";
import {
  DomainError, NotFoundError, ValidationError, int, optionalStr, str, wrap,
} from "./errors.js";
import { publish, roomSize, subscribe } from "./stream.js";
import { appendEvent, newId } from "../db/index.js";
import * as repo from "../db/repo.js";
import {
  FREE_DELIVERY_THRESHOLD, HUE_PALETTE, computeRunde, generateCode, suggestToThreshold,
} from "../engine/runde.js";
import { unitPrice, packLabel } from "../domain/pbv.js";
import { formatCHF } from "../domain/money.js";
import type { RundeRecord } from "../db/repo.js";

export const rundeRouter = Router();

/** Default window before a Runde auto-locks. Long enough to gather a flat. */
const DEFAULT_WINDOW_MIN = 25;
const MAX_PARTICIPANTS = 12;

function loadOrThrow(code: string): RundeRecord {
  const runde = repo.getRundeByCode(code);
  if (!runde) throw new NotFoundError(`Runde ${code.toUpperCase()}`);
  return runde;
}

function requireOpen(runde: RundeRecord): void {
  if (runde.status !== "open") {
    throw new DomainError(
      "RUNDE_CLOSED",
      `This Runde is ${runde.status} and can no longer be changed.`,
    );
  }
}

/** The full view every surface renders from. One shape, one source of truth. */
function viewOf(runde: RundeRecord, now: number) {
  const zone = repo.getZone(runde.zoneId);
  if (!zone) throw new NotFoundError(`Zone ${runde.zoneId}`);
  const products = repo.productMap();
  const view = computeRunde({
    runde,
    zone,
    participants: repo.participantsOf(runde.id),
    items: repo.itemsOf(runde.id),
    products,
    now,
  });

  const chosen = new Set(repo.itemsOf(runde.id).map((i) => i.productId));
  const gap = view.totals.toFreeDelivery > 0
    ? view.totals.toFreeDelivery
    : view.totals.toMinimum;
  const suggestion = suggestToThreshold(gap, [...products.values()], chosen);

  return {
    ...view,
    live: roomSize(runde.id),
    freeDeliveryThreshold: FREE_DELIVERY_THRESHOLD,
    suggestion: suggestion
      ? {
          id: suggestion.id,
          name: suggestion.name,
          emoji: suggestion.emoji,
          price: suggestion.price,
          priceLabel: formatCHF(suggestion.price),
          reason:
            view.totals.toFreeDelivery > 0
              ? `Für ${formatCHF(suggestion.price)} dazu — dann liefern wir gratis`
              : `Für ${formatCHF(suggestion.price)} dazu — dann ist die Mindestbestellung erreicht`,
        }
      : null,
  };
}

/** Broadcast the new state to everyone watching, then return it to the caller. */
function commitAndBroadcast(runde: RundeRecord, now: number, event: string) {
  const version = repo.bumpVersion(runde.id);
  const fresh = repo.getRundeById(runde.id)!;
  const view = viewOf(fresh, now);
  publish(runde.id, event, { ...view, version });
  return view;
}

/* --------------------------------- catalogue -------------------------------- */

rundeRouter.get("/catalog", wrap((_req, res) => {
  const products = repo.allProducts().map((p) => ({
    id: p.id,
    sku: p.sku,
    name: p.name,
    brand: p.brand,
    category: p.category,
    price: p.price,
    priceLabel: formatCHF(p.price),
    // PBV Art. 11: the unit price is a legal duty, not a nicety.
    unitPrice: unitPrice(p.price, p)?.label ?? null,
    pack: packLabel(p),
    allergens: p.allergens,
    chilled: p.chilled,
    inStock: p.stock > 0,
    stock: p.stock,
    emoji: p.emoji,
    tags: p.tags,
  }));
  res.json({ products, vatNote: "Preise inkl. 2.6% MWST (Lebensmittel)." });
}));

rundeRouter.get("/zones", wrap((_req, res) => {
  res.json({
    zones: repo.allZones().map((z) => ({
      id: z.id,
      name: z.name,
      canton: z.canton,
      baseFee: z.baseFee,
      baseFeeLabel: formatCHF(z.baseFee),
      minBasket: z.minBasket,
      minBasketLabel: formatCHF(z.minBasket),
      driveMinutes: z.driveMinutes,
    })),
  });
}));

/* ---------------------------------- Runde ---------------------------------- */

rundeRouter.post("/runde", wrap((req, res) => {
  const now = Date.now();
  const zoneId = str(req.body, "zoneId");
  const zone = repo.getZone(zoneId);
  if (!zone) throw new ValidationError(`Unknown zone "${zoneId}"`, "zoneId");

  const address = str(req.body, "address", { min: 4, max: 160 });
  const hostName = str(req.body, "hostName", { min: 1, max: 40 });
  const note = optionalStr(req.body, "note");
  const windowMinutes = req.body?.windowMinutes === undefined
    ? DEFAULT_WINDOW_MIN
    : int(req.body, "windowMinutes", { min: 5, max: 120 });

  // Scatter the drop inside the zone so the map and routing look real.
  const jitter = () => (Math.random() - 0.5) * 0.008;

  const runde: RundeRecord = {
    id: newId(),
    code: generateCode(),
    zoneId,
    address,
    note,
    status: "open",
    createdAt: now,
    closesAt: now + windowMinutes * 60_000,
    version: 1,
    lat: zone.lat + jitter(),
    lng: zone.lng + jitter(),
    placedAt: null,
    runId: null,
  };
  repo.insertRunde(runde);

  const host = {
    id: newId(),
    rundeId: runde.id,
    name: hostName,
    hue: HUE_PALETTE[0],
    joinedAt: now,
    isHost: true,
  };
  repo.insertParticipant(host);

  appendEvent("runde.created", host.id, runde.id, { code: runde.code, zoneId, address }, now);

  res.status(201).json({ ...viewOf(runde, now), you: host });
}));

rundeRouter.get("/runde/:code", wrap((req, res) => {
  const runde = loadOrThrow(req.params.code);
  res.json(viewOf(runde, Date.now()));
}));

rundeRouter.post("/runde/:code/join", wrap((req, res) => {
  const now = Date.now();
  const runde = loadOrThrow(req.params.code);
  requireOpen(runde);

  const name = str(req.body, "name", { min: 1, max: 40 });
  const existing = repo.participantsOf(runde.id);

  // Rejoining under the same name returns the same identity rather than
  // creating a duplicate - people reload, and a duplicate splits their cart.
  const already = existing.find((p) => p.name.toLowerCase() === name.toLowerCase());
  if (already) {
    res.json({ ...viewOf(runde, now), you: already, rejoined: true });
    return;
  }

  if (existing.length >= MAX_PARTICIPANTS) {
    throw new DomainError(
      "RUNDE_FULL",
      `A Runde holds at most ${MAX_PARTICIPANTS} people.`,
    );
  }

  const participant = {
    id: newId(),
    rundeId: runde.id,
    name,
    hue: HUE_PALETTE[existing.length % HUE_PALETTE.length],
    joinedAt: now,
    isHost: false,
  };
  repo.insertParticipant(participant);
  appendEvent("runde.joined", participant.id, runde.id, { name }, now);

  const view = commitAndBroadcast(runde, now, "joined");
  res.status(201).json({ ...view, you: participant });
}));

/* ---------------------------------- items ---------------------------------- */

rundeRouter.post("/runde/:code/items", wrap((req, res) => {
  const now = Date.now();
  const runde = loadOrThrow(req.params.code);
  requireOpen(runde);

  const participantId = str(req.body, "participantId");
  const participant = repo.getParticipant(participantId);
  if (!participant || participant.rundeId !== runde.id) {
    throw new ValidationError("You are not in this Runde", "participantId");
  }

  const productId = str(req.body, "productId");
  const product = repo.getProduct(productId);
  if (!product) throw new ValidationError(`Unknown product "${productId}"`, "productId");

  const qty = req.body?.qty === undefined ? 1 : int(req.body, "qty", { min: 1, max: 24 });

  if (product.stock < qty) {
    throw new DomainError(
      "OUT_OF_STOCK",
      `Only ${product.stock} × ${product.name} left tonight.`,
    );
  }

  // Adding the same product again increases the line rather than duplicating it.
  const existing = repo.findItem(runde.id, participantId, productId);
  if (existing) {
    repo.updateItemQty(existing.id, existing.qty + qty);
  } else {
    repo.insertItem({
      id: newId(),
      rundeId: runde.id,
      participantId,
      productId,
      qty,
      // Price captured now: a catalogue change never rewrites a live cart.
      unitPrice: product.price,
      addedAt: now,
    });
  }

  appendEvent("runde.item_added", participantId, runde.id, {
    productId, qty, unitPrice: product.price,
  }, now);

  res.status(201).json(commitAndBroadcast(runde, now, "items"));
}));

rundeRouter.patch("/runde/:code/items/:itemId", wrap((req, res) => {
  const now = Date.now();
  const runde = loadOrThrow(req.params.code);
  requireOpen(runde);

  const qty = int(req.body, "qty", { min: 0, max: 24 });
  const item = repo.itemsOf(runde.id).find((i) => i.id === req.params.itemId);
  if (!item) throw new NotFoundError("Item");

  repo.updateItemQty(item.id, qty);
  appendEvent("runde.item_changed", item.participantId, runde.id, {
    itemId: item.id, from: item.qty, to: qty,
  }, now);

  res.json(commitAndBroadcast(runde, now, "items"));
}));

rundeRouter.delete("/runde/:code/items/:itemId", wrap((req, res) => {
  const now = Date.now();
  const runde = loadOrThrow(req.params.code);
  requireOpen(runde);

  const item = repo.itemsOf(runde.id).find((i) => i.id === req.params.itemId);
  if (!item) throw new NotFoundError("Item");

  repo.deleteItem(item.id);
  appendEvent("runde.item_removed", item.participantId, runde.id, { itemId: item.id }, now);

  res.json(commitAndBroadcast(runde, now, "items"));
}));

/* --------------------------------- checkout -------------------------------- */

rundeRouter.post("/runde/:code/place", wrap((req, res) => {
  const now = Date.now();
  const runde = loadOrThrow(req.params.code);
  requireOpen(runde);

  const view = viewOf(runde, now);
  if (view.itemCount === 0) {
    throw new DomainError("EMPTY_RUNDE", "Nobody has added anything yet.");
  }
  if (!view.totals.meetsMinimum) {
    throw new DomainError(
      "BELOW_MINIMUM",
      `${formatCHF(view.totals.toMinimum)} short of the ${formatCHF(view.zone.minBasket)} minimum for ${view.zone.name}.`,
    );
  }

  // Reserve stock at placement, not at add-time: holding stock for an open
  // Runde that may never close would starve the rest of the night.
  for (const item of repo.itemsOf(runde.id)) {
    repo.decrementStock(item.productId, item.qty);
  }

  repo.setRundeStatus(runde.id, "placed", now);
  appendEvent("runde.placed", "system", runde.id, {
    payable: view.totals.payable,
    goods: view.totals.goods,
    deliveryFee: view.totals.deliveryFee,
    sharers: view.sharerCount,
    vatFood: view.totals.vatFood,
    vatService: view.totals.vatService,
  }, now);

  const placed = repo.getRundeById(runde.id)!;
  const fresh = viewOf(placed, now);
  publish(runde.id, "placed", fresh);
  res.json(fresh);
}));

/* ----------------------------------- live ---------------------------------- */

rundeRouter.get("/runde/:code/stream", (req, res) => {
  const runde = repo.getRundeByCode(req.params.code);
  if (!runde) {
    res.status(404).json({ error: { code: "NOT_FOUND", message: "Runde not found" } });
    return;
  }
  const unsubscribe = subscribe(runde.id, randomUUID(), res);
  req.on("close", unsubscribe);
});
