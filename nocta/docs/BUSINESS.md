# NOCTA — The Business Case

## 1. What happened to this market

| Player | Status | Note |
|---|---|---|
| **Smood** | **Ceased all operations 30 April 2026** | Geneva-founded, 25 cities, couriers employed under a CLA |
| Just Eat × Migros | Launched 4 May 2026 | Geneva, Valais, Ticino only — **no Zurich on-demand** |
| Uber Eats Grocery | Restaurants and kiosks only | not a grocery proposition |
| midnyt | 18:00–04:00, nationwide | thin operation, no ops tooling |
| Night Snacky | 19:00–03:00, Lucerne | single-city |
| Stash | 10-minute delivery, Zurich | own warehouse, daytime focus |

**Zurich currently has no on-demand grocery option.** The largest operator in
the country exited the market four months ago, and the replacement covers three
regions that do not include Zurich. This is a gap in the market right now, and
the gap will not stay open.

## 2. Why the incumbents died

Not demand. Demand at 01:00 on a Saturday in Kreis 4 is not the problem.

They died of contribution margin per drop, hidden behind funding until the
funding stopped. A CHF 14 basket cannot pay for a CHF 9.49 courier leg, and in
Switzerland that leg is expensive by **law**: night work between 23:00 and
06:00 carries a mandatory 25% supplement (ArG Art. 17b Abs. 1). You cannot cut
your way out of it.

There are exactly three levers:

| Lever | Ceiling | Cost to pull |
|---|---|---|
| Raise the delivery fee | low — conversion collapses | free, and self-defeating |
| More drops per courier-hour | ~2× via batching | engineering |
| **Bigger basket** | **3×+** | **a shared cart** |

NOCTA pulls the third and the second.

## 3. Two businesses in one codebase

### Business A — the night delivery service (your operation)

Run it in Zurich tonight. The seeded catalogue, six Kreis zones, four couriers
and a working roster are already in the repository.

Illustrative Zurich unit economics, from `engine/economics.ts`:

| | Solo, unbatched | Runde, 6 sharers, batched |
|---|---|---|
| Basket | CHF 14.00 | CHF 47.60 |
| Contribution | **CHF 0.07** | **CHF 12.89** |

At 60 drops a night with a 3.0 average Runde size, contribution runs roughly
CHF 450–700 per night before fixed costs — the difference between a business
and a hobby being, almost entirely, the average number of people per Runde.
**That is the single metric to manage.**

### Business B — NOCTA as vertical SaaS

Every other night-delivery operator in Switzerland and the DACH region has the
same two problems: they cannot prove a drop is profitable, and they are
guessing at night-work law. midnyt, Night Snacky, and the dozens of
single-city operators are the buyer.

| Tier | Price | For | Includes |
|---|---|---|---|
| **Kiosk** | CHF 149/mo | one zone, ≤2 couriers | Runden, catalogue, PBV pricing, **full ArG night-work engine** |
| **Nacht** | CHF 389/mo | 2–6 zones, ≤10 couriers | + dispatch batching, forecast, par levels, margin per drop |
| **Flotte** | CHF 990/mo + 0.6% GMV | multi-city | + multi-depot, roster planning, API, cross-operator benchmarks |

Compliance sits in the **entry** tier. Charging extra to obey the law is both
unsellable and indefensible.

At 40 Nacht-tier operators the SaaS line alone is ~CHF 187k ARR, on software
that is already built and that you are running yourself every night — which is
also the strongest sales asset there is.

## 4. The moat, in ascending durability

1. **Shared cart UX** — copyable in a quarter.
2. **Batching and forecasting** — copyable in a year.
3. **Embedded Swiss regulatory logic** — ArG night-work regimes, DST-correct
   local-time computation, the 2.6%/8.1% VAT split, PBV unit pricing, LIV
   allergen declaration. A competitor must acquire and *maintain* the same
   jurisdictional expertise. Narrow and durable.
4. **Cross-operator demand data** — every operator's nights sharpen every other
   operator's forecast. This compounds and cannot be bought.

Build 3 and 4 deliberately; 1 and 2 are table stakes that buy the time to.

## 5. Go to market

**Phase 1 — operate (weeks 1–8).** Run one depot in Kreis 4/5. Prove the Runde
mechanic on real Friday nights. The metric that matters is average sharers per
Runde, not order count.

**Phase 2 — seed the loop (weeks 4–16).** Every Runde already invites 3–8
people. Put the code where groups already are: WG noticeboards, student
residences, sports clubs after training, shift-worker break rooms. Never
discount into negative contribution — a cohort bought with discounts churns the
moment it pays full price.

**Phase 3 — license (month 6+).** Approach single-city operators with the ArG
module, which is the painful one, then expand into dispatch and forecasting.
Land on the pain, expand into the platform. Import their catalogue on day one:
migration *in*, never out.

## 6. What would kill this

Stated plainly, because a plan without these is marketing:

- **Average Runde size stays near 1.** The whole thesis fails. Watch it weekly
  from night one; if groups do not form, the mechanic is wrong, not the copy.
- **A large player re-enters Zurich.** Likely within 18 months. The answer is
  not to out-spend them — it is depth: the night, the group, and Swiss law.
- **Courier supply.** Night work is bewilligungspflichtig and unpopular.
  Transparent night pay in the courier app is a retention tool, not decoration.
- **Cold chain.** Ice cream after 25 minutes in a bag is a refund and a lost
  customer. The dispatch engine constrains for it; the packaging has to too.

## 7. Legal footing

We sell **no alcohol and no tobacco**. That removes cantonal night-sales
curfews and age verification from scope entirely — and it is an operating
advantage, since we can trade through hours that constrain alcohol-carrying
competitors.

What still binds, and is implemented:

| Duty | Source | Where |
|---|---|---|
| Night work permit, 25% / 10% compensation, 9h cap, 11h rest | ArG Art. 16, 17, 17a, 17b | `engine/nightshift.ts` |
| Reduced VAT 2.6% on food, 8.1% on the delivery service | MWSTG Art. 25 | `domain/money.ts` |
| Unit price per litre / per kilogram | PBV Art. 11 | `domain/pbv.ts` |
| Total price shown before purchase | PBV Art. 3–4 | checkout contract |
| Allergen declaration before distance purchase | LIV / LGV | catalogue schema |
| Data minimisation, retention by design | revDSG Art. 6 | ledger design |

Reference values live in versioned data (`NIGHT_RULES`, `VAT_RATES`,
`COST_MODEL`) with an effective date and a citation. When the law changes you
edit data and add a test — you do not hunt through conditionals.

> Implemented to the best reading of the cited provisions; have a Swiss
> employment lawyer confirm the roster before your first night, and obtain the
> cantonal night-work authorisation. The software computes the duty — it does
> not discharge it.
