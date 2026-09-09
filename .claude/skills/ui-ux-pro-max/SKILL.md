---
name: ui-ux-pro-max
description: Interface engineering standards — layout systems, states, accessibility, forms, and touch ergonomics. Use when building or reviewing any user interface.
---

# UI/UX Pro Max

## Layout

- One spacing scale, powers of a 4px base: 4/8/12/16/24/32/48/64. Never an off-scale value.
- Establish rhythm with a grid, break it deliberately once per page for the hero element.
- Line length 45–75 characters. Wider is unreadable regardless of how good it looks in a mock.
- Optical alignment beats mathematical alignment for icons and glyphs; trust the eye.

## The five states

Every data-bearing component must define all five, or it is unfinished:
**empty** (what is missing + the action that fills it) · **loading** (skeleton matching final geometry, so nothing jumps) · **partial** (some data, some pending) · **error** (cause + recovery action) · **ideal**.

Most bugs users notice are missing states, not wrong logic.

## Touch and night ergonomics

- Minimum 44×44px hit target; 48px for anything used one-handed while walking.
- Primary actions in the bottom third — the thumb arc. Top-bar primary actions are a desktop habit that fails on phones.
- Never place a destructive action adjacent to a frequent one.
- Night use means dark surfaces and reduced peak luminance. Pure white text on pure black causes halation: use ~92% white on a ~8% lightness ground.

## Forms

Single column. Labels above fields, always visible — placeholders are not labels. Validate on blur, not per keystroke; show success only where it reduces doubt. Input `type`, `inputmode`, and `autocomplete` set correctly on every field. Never block paste.

## Accessibility (non-negotiable)

- 4.5:1 contrast for body text, 3:1 for large text and meaningful UI boundaries.
- Never encode meaning in colour alone — pair with text, icon, or position.
- Visible focus ring on every interactive element. Never `outline: none` without a replacement.
- Full keyboard operability; logical tab order; focus trapped inside modals and restored on close.
- Respect `prefers-reduced-motion`.
- Semantic elements first. A `div` with a click handler is a bug.

## Feedback timing

<100ms feels instant, no indicator. 100ms–1s: inline spinner. >1s: skeleton or progress with a real estimate. >10s: make it interruptible and backgroundable.
