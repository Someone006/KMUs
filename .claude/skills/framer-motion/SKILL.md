---
name: framer-motion
description: Implementation patterns for the Framer Motion / motion React library — variants, layout animation, exit transitions, gestures, and the correctness traps. Use when animating a React interface.
---

# Framer Motion

Package is now `motion` (`import { motion, AnimatePresence } from "motion/react"`). The older `framer-motion` import path still works.

## Variants over inline props

Define named states once and let them cascade to children. This is the difference between maintainable and unmanageable animation code.

```tsx
const list = {
  hidden: { opacity: 0 },
  show: { opacity: 1, transition: { staggerChildren: 0.04, delayChildren: 0.08 } },
};
const item = {
  hidden: { opacity: 0, y: 12 },
  show: { opacity: 1, y: 0, transition: { duration: 0.24, ease: [0.22, 1, 0.36, 1] } },
};

<motion.ul variants={list} initial="hidden" animate="show">
  {rows.map(r => <motion.li key={r.id} variants={item}>{r.label}</motion.li>)}
</motion.ul>
```

Children inherit the parent's variant name automatically — do not re-specify `initial`/`animate` on them.

## Exit animations

`AnimatePresence` only animates the removal of its *direct* children, and every child needs a stable `key`. For lists where items reorder, add `mode="popLayout"`. For one-in-one-out transitions use `mode="wait"`.

## Layout animation

`layout` on an element animates position and size changes automatically — this is the highest-value feature in the library. Use `layoutId` to morph one element into another across components (a product card into a cart row). Wrap groups in `LayoutGroup` when independent components must animate in sync.

Trap: `layout` on an element whose *children* change text length causes distortion. Fix with `layout="position"`, which animates position only.

## Springs

Prefer `{ type: "spring", stiffness: 380, damping: 30, mass: 0.8 }` for anything the user directly manipulates; use duration-based easing for anything system-initiated. A spring with `damping` under ~20 oscillates and reads as unserious.

## Gestures and scroll

`whileHover` / `whileTap` (`{ scale: 0.97 }`) give the cheapest possible quality signal — put `whileTap` on every button. Use `useScroll` + `useTransform` for scroll-linked effects, and always wrap the raw progress in `useSpring` to remove jitter.

## Performance and correctness

- Values that animate frequently belong in `useMotionValue`, which bypasses React re-render entirely.
- Never animate a `motion` component whose parent re-renders every frame; hoist state.
- Respect reduced motion with `useReducedMotion()` and branch to opacity-only variants.
- `initial={false}` prevents an unwanted animation on first mount when hydrating persisted state.
