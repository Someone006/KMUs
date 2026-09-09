---
name: blueprint
description: Produce a system blueprint before implementation — the contract set, data model, module boundaries, failure modes, and build order. Use at the start of any multi-module build.
---

# Blueprint

A blueprint is the smallest document that lets someone else build the same system.

## Required sections, in order

1. **One-line thesis.** What this system does and for whom. If it needs two sentences, the scope is wrong.
2. **The wedge.** The one capability competitors lack that makes this defensible. Name the competitor and the gap explicitly.
3. **Domain glossary.** Every noun the code will use, defined once. Ambiguous nouns are the top source of architectural drift.
4. **Data model.** Tables, keys, and the invariants that must hold across them. Mark which fields are append-only and which are derived.
5. **Module boundaries.** For each module: its single responsibility, its inputs, its outputs, and what it is *forbidden* to know about. Draw the dependency direction — it must be acyclic.
6. **Contract set.** Every API route with method, path, request shape, response shape, and error codes. This is the integration surface; freeze it before building either side.
7. **Failure modes.** For each: trigger, blast radius, detection, and degradation behaviour. A system without an enumerated failure list has not been designed.
8. **Build order.** Dependency-sorted. Each step must end at a runnable, verifiable state.

## Rules

- Derived data is never stored unless it is also proven recomputable; write the recompute function first.
- Any rule that regulators or law can change lives in **data**, not in `if` statements. Version it, date it, cite the source.
- The core engine must be a pure function of (state, input) → decision. Side effects live at the edges only. This is what makes the engine testable and sellable.
- Name the module boundary you would break first under deadline pressure, and defend it explicitly.

## Anti-patterns

Do not blueprint the UI first — the UI is the last thing to stabilise. Do not include effort estimates. Do not enumerate technologies you will not use.
