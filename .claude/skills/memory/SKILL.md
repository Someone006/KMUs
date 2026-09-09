---
name: memory
description: Design what a system remembers and how it recalls it — audit trails, event logs, user-visible history, and personalisation state. Use when modelling anything that must be reconstructible or that should learn from the past.
---

# Memory

## Two kinds

**Ledger memory** is append-only and authoritative. Events, not states. Never updated, never deleted; corrections are compensating entries. Everything else is a projection you can rebuild by replaying it. If you cannot reconstruct current state from the ledger alone, the ledger is incomplete.

**Working memory** is the fast, mutable projection the application reads. It is disposable by definition. Treat any bug in it as a rebuild, not a patch.

## Rules

- Every event carries: what happened, when (UTC epoch ms), who caused it, and what it referenced. Missing "who" makes an audit trail worthless.
- Chain records with a hash of the previous record when tamper-evidence matters. Cheap to add, impossible to retrofit.
- Store the *decision inputs* alongside the decision. "Fee was CHF 5.90" is useless in six months; "fee was CHF 5.90 under ruleset v3, distance 2.4 km, 4 sharers" is defensible.
- Data minimisation is a memory design constraint, not a legal afterthought. Store the derived conclusion, not the raw sensitive input.

## Product memory

Remembering the customer is the cheapest retention feature that exists:
- Reorder in one tap from any past order.
- Remember the group, not just the person — the flat that ordered together on Friday is the unit worth recalling.
- Recall must be visible and correctable. Invisible personalisation reads as surveillance; correctable recall reads as service.

## Forgetting

Define retention per data class at design time, and enforce deletion with a scheduled job that you test. A retention policy nobody executes is a liability with extra steps.
