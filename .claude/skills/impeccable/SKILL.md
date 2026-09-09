---
name: impeccable
description: Engineering rigor standard. Use when writing or reviewing production code to enforce correctness, error handling, determinism, and verification before claiming a task is done.
---

# Impeccable

The standard: a stranger reads this at 03:00 during an incident and is not confused.

## Non-negotiables

- **No silent failure.** Every `catch` either recovers, or logs with context and rethrows. Never `catch {}`. Never swallow a rejected promise.
- **Validate at the boundary.** Every HTTP body, query param, env var, and file read is untrusted. Parse into a typed shape at the edge; the interior assumes validity.
- **Money is integers.** Store minor units (Rappen). Never float arithmetic on currency. Format only at render.
- **Time is explicit.** Store UTC epoch ms. Any local-time logic names its timezone (`Europe/Zurich`) and handles DST. Never rely on the server's local clock for business rules.
- **Determinism in tests.** Inject the clock and the RNG. A test that fails at midnight is a broken test.
- **Idempotency.** Any endpoint that creates or charges takes an idempotency key and returns the prior result on replay.

## Error taxonomy

Distinguish three classes and never conflate them:
- `ValidationError` (4xx, caller's fault, safe to show the message)
- `DomainError` (4xx/409, business rule refused it, safe to show, must carry a machine-readable `code`)
- `InternalError` (5xx, our fault, log the detail, return a correlation id, never leak internals)

## Definition of done

A task is done only when all hold:
1. The happy path is exercised by an automated test.
2. At least one failure path is exercised by an automated test.
3. The code runs — you executed it, not imagined it.
4. Types check and the linter is clean.
5. You re-read your own diff adversarially and fixed what you found.

If any of these is false, the task is in progress. Say so; do not round up to "complete".

## Reporting

Report what you actually ran and what it actually printed. If a test fails, show the output. If you skipped something, name it and say why. Never describe intended behavior in the past tense.
