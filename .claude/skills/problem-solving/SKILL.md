---
name: problem-solving
description: Diagnose the actual problem behind a request or a bug before solving it. Use when a symptom is reported, when a fix does not stick, or when the obvious solution feels unsatisfying.
---

# Problem Solving

## For business problems

Ask what breaks if this is *not* solved, and who pays. A problem nobody pays for is a preference. Then find the **binding constraint** — the one limit that, if lifted, changes the outcome. Optimising a non-binding constraint produces beautiful, worthless work.

Quick-commerce example: the binding constraint is never "the app is ugly". It is contribution margin per drop. Every feature is judged by whether it moves basket size, drop density, or cost per drop.

## For bugs

1. **Reproduce it deterministically.** Without a reproduction you are guessing, and a guessed fix cannot be verified.
2. **Read the actual error.** Not the summary of the error. The whole stack, the whole log line.
3. **Bisect.** Halve the search space per step — by commit, by input, by layer. Five steps beat fifty guesses.
4. **Find the cause, not the site.** The line that throws is usually downstream of the line that is wrong.
5. **Prove the fix** by reproducing the failure first, applying the fix, and watching the same reproduction pass.

"It works now" without a known cause means it will break again with an unknown cause.

## Root-cause discipline

"Flake", "race condition", and "environment issue" are hypotheses, not conclusions. Each needs evidence: a re-run that passes on identical input, an interleaving you can name, or a diff in the environment you can show.

Never resolve a failing check by weakening the check.
