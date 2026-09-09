---
name: planning
description: Turn a vague ambition into a dependency-ordered build plan with verifiable checkpoints. Use before executing any multi-step build, and again whenever a core assumption changes.
---

# Planning

## Sequence

1. **Restate the ask in one sentence** using the requester's own nouns. If your sentence introduces a noun they never said, you have already drifted.
2. **List the assumptions** your plan depends on. Rank by "damage if wrong". Verify the top two before writing code.
3. **Find the wedge** — the one thing that must be true for this to be worth building. Everything else is scaffolding.
4. **Order by dependency, not by enthusiasm.** The boring data model comes before the beautiful screen.
5. **Define checkpoints** where the system is runnable and verifiable. A plan with no runnable state before step 9 is a plan that fails at step 9.
6. **Name the cut line.** Decide in advance what gets dropped if time runs out, so the drop is a decision and not an accident.

## Replanning

A changed premise invalidates the plan, not just a task. When a core assumption dies:
- Say plainly which assumption died and what it took with it.
- Re-derive the wedge from scratch. Do not patch the old wedge.
- Keep only the work that survives the new premise on its own merits.

Sunk work is not a reason to keep a direction. Announce the pivot, then execute the new plan fully.

## Anti-patterns

- Planning past the first checkpoint in detail. Detail decays; plan step 1 precisely and step 5 loosely.
- Parallelising work that shares an unfrozen contract.
- Treating "I asked a clarifying question" as progress.
