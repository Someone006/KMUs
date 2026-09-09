# NOCTA — Anatomy

One real transaction, traced through every layer.

> **Nina is in a flat on Langstrasse at 22:47 on a Friday. She wants chips.
> Three flatmates want things too. Nobody wants to pay CHF 5.90 four times.**

---

## 1. Nina opens a Runde

**Surface** — `web/src/surfaces/Customer.tsx`
She picks a Quartier, types the address and her name. The form holds three
fields because a fourth would cost conversion at 22:47.

**Contract** — `server/src/routes/runde.ts` → `POST /api/runde`
Every field is parsed at the boundary by `str()` / `int()` from
`routes/errors.ts`. An unknown `zoneId` is a `ValidationError` (400), never a
crash. Below this line the code assumes valid data.

**Engine** — `engine/runde.ts:generateCode()`
Six characters from an alphabet with no `0/O/1/I/L`, because these codes are
read aloud across a kitchen.

**Ledger** — `db/index.ts:appendEvent("runde.created")`
Hashed against the previous event. The chain is the audit record; the
`rundes` row is a projection of it.

*Data out:* a `RundeView` plus Nina's `participant.id`, stashed in
`sessionStorage` so a reload does not orphan her cart.

---

## 2. Three flatmates join

**Surface** — `RundeRoom.tsx` opens an `EventSource` on
`GET /api/runde/:code/stream`.

**Transport** — `routes/stream.ts`
SSE, not WebSockets: the traffic is one-directional, it survives proxies, and
the browser reconnects unaided. A 10-second comment frame keeps intermediaries
from closing an idle connection. A dead socket is dropped from the room rather
than being allowed to throw into the publisher's loop.

**Cut point** — SSE can still die silently. `RundeRoom` therefore also polls
every 5 seconds and reconciles on `runde.version`, discarding any frame older
than what it already holds. Out-of-order delivery shows stale data for at most
one tick; it never flickers backwards.

Rejoining under a name already present returns the *same* participant instead
of creating a second one — people reload, and a duplicate would split their
cart in half.

---

## 3. Everyone adds items

`POST /api/runde/:code/items` → `repo.insertItem` or `updateItemQty`.

Two decisions worth naming:

- **Price is captured at add-time** (`unitPrice` on the item row). A catalogue
  change at 23:30 never rewrites a cart someone is already looking at.
- **Stock is *not* reserved here.** An open Runde may never close; holding
  stock for it would starve the rest of the night. Reservation happens at
  placement, and the race is handled there.

`commitAndBroadcast()` bumps the version, recomputes the view once, and pushes
the identical payload to every subscriber. One computation, one shape, no
surface deriving its own totals.

---

## 4. The split — where the product lives

**Engine** — `engine/runde.ts:computeRunde()`. Pure: state in, decision out.

1. Lines are grouped per participant; a delisted product contributes **zero**,
   never a free item.
2. `deliveryFee` is the zone fee, or 0 once goods clear CHF 60.
3. Only participants **with items** carry the fee. A lurker pays nothing.
4. `splitExact(fee, n, weights)` divides it.

`splitExact` (`domain/money.ts`) is the load-bearing function of the whole
product. Largest-remainder: everyone gets the floor, leftover Rappen go one
each to the largest fractional parts. Its invariant —

```
shares.reduce((a, b) => a + b) === amount     for every amount, every n
```

— is asserted exhaustively across 0–2000 Rappen × 1–12 people in
`tests/money.test.ts`, and again end-to-end in `e2e/runde.spec.ts`, where the
rendered share totals are summed from the DOM and compared to the rendered
payable. A shared cart whose shares do not reconcile is worthless, so this is
tested at both ends.

*Observed:* CHF 5.90 across four sharers → `[1.48, 1.48, 1.47, 1.47]`.

---

## 5. Nina places the order

`POST /api/runde/:code/place` refuses on `EMPTY_RUNDE` or `BELOW_MINIMUM` —
`DomainError`, 409, with a message safe to show and a code safe to switch on.
Stock decrements now. Status → `placed`. `runde.placed` enters the ledger
carrying the decision *inputs* (goods, fee, sharers, both VAT components), not
just the outcome — "fee was CHF 5.90" is useless in six months.

---

## 6. Dispatch batches the drop

**Engine** — `engine/dispatch.ts`

`batchIntoRuns` seeds on the **oldest waiting drop** so nobody is starved, then
absorbs nearest neighbours while the run stays within 45 minutes and no chilled
item would sit past 25 minutes. `planRoute` builds a tour by nearest-neighbour
and improves it with 2-opt; for the 3–5 stops a night run really contains this
reaches the optimal tour and runs in microseconds.

Distances are haversine × **1.35**, a European street-grid detour factor, so
ETAs are honest rather than flattering.

Each drop's `attributedMinutes` is its own leg plus its own doorstep time plus
an even share of the return leg. Those minutes are what
`engine/economics.ts` charges the drop for — which is why a batched drop and a
solo drop show different courier costs on the same screen.

---

## 7. The operator sees what it earned

`GET /api/ops/overview` composes the pure engines:

```
repo (I/O)  →  computeRunde      (what was ordered)
            →  batchIntoRuns     (how it will be driven)
            →  computeShiftCost  (what the courier legally costs)
            →  computeDropEconomics
```

`computeShiftCost` is the piece nobody else automates. It walks the shift
minute by minute in **Europe/Zurich** via `Intl` — so the repeated hour on
2026-10-25 is genuinely worked and genuinely paid, and the skipped hour on
2026-03-29 is neither. It then branches on `nightsThisYear`: under 25 nights,
a **25% cash supplement** (ArG 17b Abs. 1); at 25 or more, a **10% time
credit** that may not be paid out (ArG 17b Abs. 2). Both DST transitions and
both regimes are covered in `tests/engines.test.ts`.

The console's headline is the counterfactual: contribution as it happened,
beside contribution had every sharer ordered alone.

---

## 8. The courier delivers

`GET /api/courier/:id` returns the shift, its night pay, and the stops in visit
order. `POST /api/courier/:id/deliver/:code` refuses anything not
`dispatched` (`NOT_DISPATCHED`, 409) and appends `runde.delivered` with minutes
from placement.

---

## Layer table

| Layer | Owns | Must not know |
|---|---|---|
| `surfaces/` | pixels, input state, optimistic UI | business rules |
| `routes/` | validation, status codes, SSE | storage details |
| `engine/` | rules and arithmetic — **pure** | I/O, Express, `Date.now()` |
| `domain/` | money, time, unit pricing — **pure** | anything above |
| `db/` | persistence, hash-chained ledger | why |

Dependencies point downward only. `engine/` importing `db/` would end the
ability to test a rule without a database — and to license the engine on its
own, which is the second business in here.

---

## Where it breaks

| Trigger | Detection | Behaviour |
|---|---|---|
| SSE drops | no frame, heartbeat gap | 5s poll continues; badge shows `verbinde…` |
| Out-of-order frame | `version` older than held | frame discarded |
| Two people edit one line | version counter | last write wins **per line**, never per cart |
| Product delisted mid-Runde | missing from `products` map | line contributes 0; never free |
| Stock hits 0 | check at add and at place | `OUT_OF_STOCK` 409 naming the remaining count |
| Below zone minimum | check at place | `BELOW_MINIMUM` 409 stating the exact gap |
| Empty roster | `totalMinutes === 0` | falls back to CHF 27/h rather than dividing by zero |
| No forecast history | `observations < 3` | zone baseline shape, marked low-confidence |
| Ledger tampered | `verifyLedger()` | console shows the broken sequence number |
