---
name: programmer
description: Practical implementation craft — structuring modules, choosing data shapes, and writing code that survives change. Use while writing any non-trivial application code.
---

# Programmer

## Structure

- **Pure core, imperative shell.** Business rules are pure functions of their inputs. Database calls, HTTP, clocks, and randomness live in a thin outer layer. This is the single highest-leverage structural choice: it makes rules testable without infrastructure and sellable as a standalone engine.
- **Data shapes before functions.** Get the types right and most functions write themselves. Get them wrong and no amount of clever code recovers.
- **Make illegal states unrepresentable.** A discriminated union beats a bag of optional fields plus a comment.
- **One module, one reason to change.** If two unrelated requirements both edit the same file, split it.

## Concrete rules

- Derive, don't duplicate. A total stored alongside its line items will drift; compute it.
- Append-only for anything audited. Corrections are new rows, never mutations.
- Every list endpoint is paginated from day one. Retrofitting pagination is a migration.
- Name booleans for their true state: `isOpen`, not `closed`.
- Prefer a boring loop you can read to a clever chain you cannot.
- Comment *why*, never *what*. The code says what.

## Reference data

Rules that come from outside your company — tax rates, labour law supplements, opening hours, fee tables — belong in versioned data files with an effective date and a citation, never inlined in conditionals. When the law changes you edit data and add a test, not logic.

## Verification

Run the code. Read the output. A change you have not executed is a hypothesis, and describing a hypothesis as a result is the worst thing an engineer can do.
