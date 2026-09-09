---
name: motionsites
description: Motion design for web interfaces — choreography, scroll behaviour, and performance budgets that make a site feel alive without feeling gimmicky. Use when adding animation to any page or app.
---

# Motion Sites

Motion exists to explain change. If an animation does not clarify what happened, where something came from, or what is now possible, delete it.

## Timing

| Intent | Duration | Easing |
|---|---|---|
| Micro feedback (hover, tap) | 80–120ms | `ease-out` |
| Element enter | 180–260ms | `cubic-bezier(.22,1,.36,1)` |
| Element exit | 120–180ms | `ease-in` |
| Layout / shared element | 260–420ms | spring, damping ~26 |
| Ambient / background | 4–20s | linear, looping |

Exits are always faster than entrances — users have already decided; do not make them wait to leave.

## Choreography

- **Stagger** lists at 30–50ms per item, capped at ~8 items. Beyond that it reads as lag, so animate the container instead.
- **Origin matters.** Things enter from where they came from. A cart item flies from the product, not from nowhere.
- **One focal motion at a time.** Two simultaneous animations compete and both lose.
- **Preserve continuity.** If an element persists across states, animate it between them rather than cross-fading two copies.

## Scroll

Trigger on intersection, animate once, never re-trigger on scroll-up. Never hijack scroll speed or direction. Parallax only on decorative layers, never on text the user is reading. Anything scroll-driven must be `transform`/`opacity` only.

## Performance budget

- Animate `transform` and `opacity` exclusively. Animating `width`, `top`, `height`, or `box-shadow` triggers layout and drops frames.
- `will-change` applied immediately before the animation and removed after — never left on permanently.
- Ambient loops must idle under ~2% CPU; pause them when the tab is hidden via `visibilitychange`.
- Budget: 60fps on a mid-range Android. Test there, not on a laptop.

## Accessibility

Honour `prefers-reduced-motion: reduce` by replacing movement with a cross-fade — never by disabling feedback entirely, which leaves users with no confirmation their action registered.
