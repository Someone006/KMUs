---
name: awesome-design
description: Visual design system construction — colour, type, depth, and composition that reads as intentional. Use when defining the look of a product or any high-craft visual surface.
---

# Awesome Design

## Colour

Build in **OKLCH**, not hex — perceptually uniform lightness makes ramps predictable and dark-mode inversion sane.

- One neutral ramp (9–11 steps), one accent, at most one semantic-warning hue. That is the whole palette.
- Dark UI: surfaces are near-neutral with a slight hue cast toward the accent (~2–4% chroma). Absolutely neutral greys look dead; saturated greys look cheap.
- Elevation in dark mode is expressed by *lighter surfaces*, not by heavier shadows. Shadows barely read on dark grounds.
- Accent is for action and current state only. An accent used for decoration stops meaning "act here".

## Type

- Two families maximum: one for text, optionally one monospace for numerals and codes.
- Use tabular figures for any number that updates in place, or the layout will shiver.
- Scale by ratio (1.2 for dense UI, 1.25–1.333 for marketing), not by arbitrary sizes.
- Tighten tracking as size grows: display text at −1% to −3%, body at 0, small caps and 11px labels at +2% to +6%.
- Weight carries hierarchy more cheaply than size. Prefer 600 at the same size over 400 at a larger one.

## Depth

Pick one lighting model and never contradict it. Layer order: ground → surface → raised → overlay. Each step adds lightness *and* a hairline border at ~8–12% opacity; that hairline is what separates crisp from muddy.

## Composition

- Asymmetry with a strong anchor beats centred symmetry for anything that is not a landing hero.
- Whitespace is proportional to importance. Crowding an element says it does not matter.
- Repeat a shape language — if corners are 12px, everything is 12px or a deliberate multiple.
- Contrast comes from one dominant dimension: size, weight, colour, or space. Pushing all four is noise.

## The tell

Great interfaces feel *quiet*. If a screenshot is exciting but the screen is tiring after five minutes, the design failed.
