# NOCTA — Night Commerce OS
## System Blueprint v1.0

### 1. Thesis

An operations platform for Swiss night delivery of **non-alcoholic drinks and snacks**, built around a live shared cart ("Runde") that makes small night orders economically viable.

### 2. The wedge

Quick commerce does not die of bad apps. It dies of **contribution margin per
drop**. Gorillas, Getir and - as of 30 April 2026 - Smood all collapsed on the
same arithmetic. Here is that arithmetic for a Swiss night, computed by
`engine/economics.ts` rather than asserted:

A courier on an occasional night shift costs **CHF 28.46/hour** as an employer
cost, once the 25% ArG night supplement is applied. One solo customer, one
unbatched round trip:

```
CHF 14.00 basket, 4 items, 20 minutes door to door

  Warenmarge (36%)        + CHF  5.04
  Liefergebühr            + CHF  5.90
  Kurier (20 min)         - CHF  9.49
  Verpackung              - CHF  0.69
  Zahlung (1.3% + 0.25)   - CHF  0.51
  Plattform               - CHF  0.18
  ------------------------------------
  Deckungsbeitrag         = CHF  0.07
```

**Seven Rappen.** Not a catastrophe - which is precisely why operators miss it.
A night of these leaves nothing to pay rent, the depot, or a single hour of
office time. Raise the fee and conversion dies; cut courier pay and you are in
breach of ArG Art. 17b.

**Our answer: change the numerator.** At night, demand is inherently
*collective* - a flat, a WG, a student house, friends after a shift. Six people
want snacks at the same address at the same time, and every existing platform
models one lonely customer with one cart, charging each of them a separate
delivery.

A **Runde** is one cart, many people, one drop:

```
CHF 47.60 basket, 18 items, 6 sharers, batched into a 14-minute leg

  Warenmarge (36%)        + CHF 17.14
  Liefergebühr            + CHF  5.90
  Kurier (14 min)         - CHF  6.64
  Verpackung (gekühlt)    - CHF  2.38
  Zahlung                 - CHF  0.95
  Plattform               - CHF  0.18
  ------------------------------------
  Deckungsbeitrag         = CHF 12.89
```

The cost of a drop barely moved. The contribution moved by **CHF 12.82**.

Break-even basket, same 14-minute leg: **CHF 5.71** while the delivery fee is
charged, but **CHF 22.71** once a basket crosses the free-delivery threshold.
That second number is where quick commerce quietly bleeds, and it is on the
operator's screen in NOCTA rather than in a quarterly post-mortem.

Second-order effect, and the reason this compounds: **every Runde pulls 3-8
people into the app** to add their own items. The growth mechanic and the
margin mechanic are the same mechanic, so customer acquisition cost trends
toward zero exactly as basket size rises.

Measured on the seeded demo night: CHF 57.93 contribution across 5 drops
against CHF 12.84 had every participant ordered alone - **+CHF 45.09 on five
deliveries**.

### 3. Why this is defensible

| Layer | Durability |
|---|---|
| Shared-cart UX | copyable in a quarter |
| Batching + forecast | copyable in a year |
| **Embedded Swiss regulatory logic** (ArG night work, PBV unit pricing, split VAT, LIV allergens) | requires jurisdictional expertise, kept current |
| **Cross-operator demand data** | every operator's nights improve every other operator's forecast |

### 4. Swiss regulatory surface (encoded as versioned data, never as `if` statements)

| Rule | Source | Encoded in |
|---|---|---|
| Night work 23:00-06:00 requires permit; **25% wage supplement** for occasional night work (<25 nights/yr), or **10% time compensation** for regular night work (>=25 nights/yr) | ArG Art. 16, 17, 17b | `engine/nightshift.ts` |
| Max 9h night shift within a 10h window; 11h daily rest | ArG Art. 17a, ArGV 1 | `engine/nightshift.ts` |
| Reduced VAT **2.6%** on food & non-alcoholic drinks; standard **8.1%** on the delivery service | MWSTG Art. 25 | `domain/money.ts` |
| Unit price (Grundpreis) per litre / per kg must be displayed | PBV Art. 11 | `domain/pbv.ts` |
| Total price incl. all charges shown before purchase | PBV Art. 3-4 | checkout contract |
| Allergen declaration available before distance purchase | LIV Art. 11, LGV | catalogue schema |
| Cash amounts rounded to 5 Rappen | Swiss convention | `domain/money.ts` |
| Data minimisation; Swiss-resident storage | revDSG Art. 6 | ledger design |

**We sell no alcohol and no tobacco.** This removes cantonal curfews and age verification from scope entirely, and is itself an operating advantage: we can trade through hours when alcohol-carrying competitors face restrictions.

### 5. Domain glossary

- **Runde** — a shared cart at one address, open for a limited window, with N participants. Swiss German for "a round".
- **Participant** — a person inside a Runde. Owns their own lines and pays their own share.
- **Drop** — one physical delivery to one address. The unit of cost.
- **Run** — an ordered sequence of drops assigned to one courier. The unit of dispatch.
- **Contribution margin** — basket margin minus all variable costs of the drop. The unit of truth.
- **Night hours** — minutes worked between 23:00 and 06:00 Europe/Zurich. The unit of legal exposure.

### 6. Module boundaries (dependencies point downward only)

```
surfaces/  customer PWA | ops console | courier app
   |
routes/    HTTP contract + SSE - validation, serialisation, status codes
   |
engine/    PURE. runde | economics | nightshift | dispatch | forecast
   |
domain/    PURE. money | pbv | catalog types | time
   |
db/        SQLite persistence + append-only hash-chained ledger
```

`engine/` may not import `db/`, `express`, or `Date.now()`. Every engine function is `(state, input, clock) => decision`. This is what makes the rules testable without infrastructure and licensable as a standalone product.

### 7. Failure modes

| Trigger | Blast radius | Detection | Degradation |
|---|---|---|---|
| SSE connection drops mid-Runde | one participant sees stale cart | client heartbeat gap > 15s | poll fallback every 5s, banner on reconnect |
| Two participants edit the same line | lost update | version counter on Runde | last-write-wins per line, never per cart |
| Courier goes offline mid-run | drops stall | no ping > 10 min | run returns to dispatch pool, flagged |
| Forecast has no history for a slot | bad staffing | row count < 3 | fall back to the zone's weekday baseline, mark low-confidence |
| Rounding drift in fee split | shares do not sum to total | invariant assertion | largest-remainder distribution, asserted in tests |
| Stock reaches zero after add | order cannot be fulfilled | reservation on add | line marked unavailable, sharer prompted to swap |

### 8. Build order

1. `domain/money` + `domain/pbv` — the arithmetic everything depends on
2. `engine/runde` — split logic with exact-sum invariant
3. `engine/economics` — margin per drop
4. `engine/nightshift` — ArG computation
5. `engine/dispatch` + `engine/forecast`
6. `db` schema, seed, ledger
7. `routes` + SSE
8. Customer PWA -> Ops console -> Courier app
9. Unit tests, Playwright E2E, screenshots

### 9. Cut line

If time runs short, drop in this order: three.js ambience, courier app polish, forecast confidence intervals. Never drop: the split-sum invariant, the ArG computation, or the margin calculation. Those are the product.
