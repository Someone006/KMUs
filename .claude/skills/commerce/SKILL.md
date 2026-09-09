---
name: commerce
description: Unit economics and conversion mechanics for selling physical goods, especially delivery and quick commerce. Use when designing pricing, fees, carts, checkout, or any margin-bearing flow.
---

# Commerce

## The only equation that matters

```
Contribution margin per drop =
    basket gross margin
  − courier cost (incl. legally mandated supplements)
  − packaging
  − payment processing
  − variable platform cost
```

If this is negative, growth accelerates bankruptcy. Every quick-commerce failure — Gorillas, Getir, Smood — is this number being negative and hidden by funding.

**Corollary:** the three levers are basket size, drops per courier-hour, and cost per drop. A feature that does not move one of them is decoration.

## Basket size is the cheapest lever

Raising average basket from CHF 14 to CHF 38 fixes the equation without touching operations. Mechanics that work:
- **Group ordering** — several people, one drop. Multiplies basket while cost per drop is flat. Strongest available lever.
- Threshold framing — "CHF 6 to free delivery" with a live progress indicator and a suggested item that closes the gap exactly.
- Complements at the cart, not the catalogue. Offer what completes the occasion.

Mechanics that do not work: blanket discounts (buys revenue, sells margin), and surge fees (trains customers to wait).

## Pricing rules

- Show the true total early. Fees revealed at checkout are the single largest cart-abandonment cause.
- Price in minor units internally; format once at the edge.
- A delivery fee is a price, not an apology. State what it buys ("courier, paid night rate").
- Never discount into negative contribution margin to win a cohort. That cohort churns at full price.

## Checkout

Fewest possible fields. Guest first, account after. Persist the cart server-side keyed to a token so a dropped connection does not destroy the order. Confirm with a concrete promise ("at your door by 23:41"), never a vague one.
