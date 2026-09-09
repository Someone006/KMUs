# NOCTA — Night Commerce OS

**Getränke und Snacks in der Nacht. Eine Runde, eine Lieferung, jeder zahlt seins.**

An operations platform for Swiss night delivery of non-alcoholic drinks and
snacks, built around a live shared cart — a **Runde** — that makes small night
orders economically viable.

No alcohol. No tobacco.

---

## The idea in one paragraph

Quick commerce dies of contribution margin per drop. A CHF 14 basket delivered
alone at 01:00 earns **7 Rappen** once a Swiss courier's legally mandated 25%
night supplement is paid. The same drop, shared by six flatmates as one CHF 47.60
Runde, earns **CHF 12.89** — because the cost of a drop is fixed and basket size
is the only cheap lever. Every Runde also drags 3–8 new people into the app to add
their own items, so the growth mechanic and the margin mechanic are the same
mechanic.

---

## Three surfaces

| Route | Who | What |
|---|---|---|
| `/` | customer | start or join a Runde |
| `/r/:code` | the group | live shared cart, per-person shares, countdown |
| `/ops` | operator | margin per drop, dispatch, ArG night work, forecast, audit ledger |
| `/courier` | courier | shift, night pay, routed stops |

## Run it

```bash
npm install --prefix server
npm install --prefix web
npm install                     # Playwright

npm run seed  --prefix server   # catalogue, zones, couriers, 8 weeks of history
npm run dev                     # API :8787 + web :5173
npm run demo  --prefix server   # optional: populate a plausible night
```

Open <http://localhost:5173>.

## Verify it

```bash
npm test --prefix server        # 54 unit tests across every engine
npm run typecheck               # server + web, strict
npx playwright test             # 24 E2E, phone + desktop
```

All green as committed. The E2E suite drives **two independent browser
contexts** through one shared cart and asserts that the rendered per-person
shares sum exactly to the rendered total.

## Architecture

```
web/src/surfaces/   customer PWA · ops console · courier app
server/src/routes/  HTTP contract, validation, SSE
server/src/engine/  PURE — runde · economics · nightshift · dispatch · forecast
server/src/domain/  PURE — money (Rappen) · pbv · time (Europe/Zurich)
server/src/db/      SQLite + append-only hash-chained ledger
```

`engine/` and `domain/` never import `db/`, Express, or `Date.now()`. Every rule
is `(state, input) => decision`, which is what makes them testable without
infrastructure — and licensable on their own.

## What is genuinely built here

- **`splitExact`** — largest-remainder division whose shares provably sum to the
  original amount, asserted exhaustively over 0–2000 Rappen × 1–12 people. A
  shared cart that does not reconcile is worthless.
- **ArG night-work engine** — walks a shift minute-by-minute in Europe/Zurich,
  so both DST transitions are correct, then applies the **25% wage supplement**
  (occasional) or the **10% time credit** (25+ nights/year, not payable in cash).
- **Drop economics** — contribution per drop against real courier cost, plus the
  counterfactual: what the same night would have earned had everyone ordered alone.
- **Dispatch** — nearest-neighbour + 2-opt with a 1.35 street-detour factor,
  batching bounded by run time and cold-chain limits.
- **Nightfall forecast** — EWMA over weekday×hour buckets of the *trading* night
  (01:00 Saturday belongs to Friday), falling back to a zone baseline when history
  is thin, driving staffing and depot par levels.
- **PBV unit pricing** and **allergen declaration**, shown before purchase.
- **Hash-chained ledger** — every event chained to the previous; the console
  reports the exact sequence number if the chain ever breaks.

## Documentation

- [`docs/BLUEPRINT.md`](docs/BLUEPRINT.md) — thesis, the wedge with real
  arithmetic, data model, module boundaries, failure modes
- [`docs/ANATOMY.md`](docs/ANATOMY.md) — one real order traced through every layer
- [`docs/BUSINESS.md`](docs/BUSINESS.md) — market, pricing, go-to-market, moat,
  and what would kill it
- [`docs/shots/`](docs/shots) — screenshots of every surface

## Legal

Implemented against ArG Art. 16/17/17a/17b, MWSTG Art. 25, PBV Art. 3–4 and 11,
LIV/LGV allergen duties and revDSG. Reference values are versioned data with
effective dates and citations, not inline conditionals.

Have a Swiss employment lawyer confirm the roster and obtain the cantonal
night-work authorisation before the first shift. The software computes the duty;
it does not discharge it.
