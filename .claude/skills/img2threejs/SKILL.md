---
name: img2threejs
description: Turn a flat image or a design concept into a performant three.js / WebGL scene — depth from imagery, shader-driven ambience, and mobile-safe budgets. Use when adding 3D or generative visuals to a web page.
---

# img2three.js

## Approach ladder — pick the lowest rung that achieves the effect

1. **CSS/SVG.** Most "3D" hero effects are a gradient and a transform. Reach for WebGL only when 1–3 genuinely cannot do it.
2. **Shader plane.** A single full-screen quad with a fragment shader — noise fields, gradient flow, particle simulation. Cheapest real GPU effect, no geometry, no textures.
3. **Depth displacement.** Image + generated depth map on a subdivided plane; parallax on pointer movement. Convincing 2.5D from one photograph.
4. **Instanced geometry.** Thousands of objects in one draw call via `InstancedMesh`. This is how you get a city, a starfield, or a crowd without dying.
5. **Loaded models.** Last resort — heavy, slow to load, and rarely what a web page needs.

## Image to depth, practically

Luminance is a usable depth proxy for stylised scenes: sample the image into a canvas, read pixel luminance, displace plane vertices along Z. Blur the depth source first or you get spiky artefacts at edges. For real photographs, a pre-baked depth map from a depth estimation model beats any runtime heuristic.

## Scene defaults that matter

```js
renderer.setPixelRatio(Math.min(devicePixelRatio, 2)); // >2 is invisible and halves fps
```
- Use `ACESFilmicToneMapping` and `SRGBColorSpace` — the single biggest quality difference for beginners.
- Fog matched exactly to the background colour is what makes a scene feel like a place rather than objects floating in a void.
- One key light plus environment/ambient. Multiple hard lights read as amateur.

## Performance budget

- Under 100 draw calls on mobile. Merge or instance anything repeated.
- Pause `requestAnimationFrame` when the canvas is off-screen (`IntersectionObserver`) and on `visibilitychange`. An always-running WebGL loop is the fastest way to drain a phone at 02:00.
- Dispose geometries, materials, and textures on unmount — three.js does not garbage-collect GPU resources for you.
- Always provide a static poster image fallback when WebGL context creation fails.

## Restraint

3D on a commerce site must never sit between the user and the buy button. Keep it above the fold as atmosphere, or behind content as texture — never as an obstacle.
