/**
 * Ambient background: a single full-screen shader quad.
 *
 * Lowest rung of the img2three.js ladder that achieves the effect - no
 * geometry, no textures, one draw call. It is atmosphere behind the content,
 * never an obstacle in front of the buy button.
 */

import { useEffect, useRef } from "react";
import * as THREE from "three";

const VERTEX = `
  varying vec2 vUv;
  void main() {
    vUv = uv;
    gl_Position = vec4(position, 1.0);
  }
`;

/**
 * Layered value noise drifting upward, plus a warm horizon glow standing in
 * for city light. Cheap enough to idle at well under the mobile budget.
 */
const FRAGMENT = `
  precision mediump float;
  varying vec2 vUv;
  uniform float uTime;
  uniform vec2  uResolution;

  float hash(vec2 p) {
    return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453123);
  }

  float noise(vec2 p) {
    vec2 i = floor(p);
    vec2 f = fract(p);
    vec2 u = f * f * (3.0 - 2.0 * f);
    return mix(
      mix(hash(i + vec2(0.0, 0.0)), hash(i + vec2(1.0, 0.0)), u.x),
      mix(hash(i + vec2(0.0, 1.0)), hash(i + vec2(1.0, 1.0)), u.x),
      u.y);
  }

  float fbm(vec2 p) {
    float total = 0.0;
    float amplitude = 0.5;
    for (int i = 0; i < 4; i++) {
      total += noise(p) * amplitude;
      p *= 2.02;
      amplitude *= 0.5;
    }
    return total;
  }

  void main() {
    vec2 uv = vUv;
    float aspect = uResolution.x / max(uResolution.y, 1.0);
    vec2 p = vec2(uv.x * aspect, uv.y);

    // Slow upward drift - the movement should be felt, not watched.
    float clouds = fbm(p * 2.6 + vec2(uTime * 0.012, -uTime * 0.028));

    vec3 deep   = vec3(0.055, 0.043, 0.082);
    vec3 violet = vec3(0.130, 0.093, 0.220);
    vec3 glow   = vec3(0.320, 0.180, 0.330);

    vec3 color = mix(deep, violet, clouds * 0.85);

    // City light pooling along the bottom edge.
    float horizon = pow(1.0 - uv.y, 3.4);
    color += glow * horizon * 0.42;

    // A few stars, only in the upper half where the horizon glow is weakest.
    float star = step(0.9985, hash(floor(p * 460.0)));
    float twinkle = 0.55 + 0.45 * sin(uTime * 1.6 + hash(floor(p * 460.0)) * 42.0);
    color += vec3(star * twinkle * 0.5 * smoothstep(0.25, 0.95, uv.y));

    // Vignette keeps attention on the content sitting above this.
    float d = distance(uv, vec2(0.5));
    color *= 1.0 - d * 0.55;

    gl_FragColor = vec4(color, 1.0);
  }
`;

export function NightSky({ className = "" }: { className?: string }) {
  const hostRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;

    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    let renderer: THREE.WebGLRenderer;
    try {
      renderer = new THREE.WebGLRenderer({ antialias: false, alpha: false, powerPreference: "low-power" });
    } catch {
      // No WebGL: the CSS gradient underneath is the poster fallback.
      return;
    }

    // Beyond 2x the extra pixels are invisible and halve the frame rate.
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(host.clientWidth, host.clientHeight);
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    host.appendChild(renderer.domElement);

    const scene = new THREE.Scene();
    const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0, 1);
    const uniforms = {
      uTime: { value: 0 },
      uResolution: { value: new THREE.Vector2(host.clientWidth, host.clientHeight) },
    };
    const material = new THREE.ShaderMaterial({
      vertexShader: VERTEX, fragmentShader: FRAGMENT, uniforms,
    });
    const geometry = new THREE.PlaneGeometry(2, 2);
    scene.add(new THREE.Mesh(geometry, material));

    let frame = 0;
    let visible = true;
    const start = performance.now();

    const render = () => {
      uniforms.uTime.value = (performance.now() - start) / 1000;
      renderer.render(scene, camera);
    };

    const loop = () => {
      if (!visible) return;
      render();
      frame = requestAnimationFrame(loop);
    };

    // Never leave a WebGL loop running behind a hidden tab at 02:00.
    const onVisibility = () => {
      visible = !document.hidden;
      if (visible) loop();
      else cancelAnimationFrame(frame);
    };
    document.addEventListener("visibilitychange", onVisibility);

    const observer = new IntersectionObserver(([entry]) => {
      visible = entry.isIntersecting && !document.hidden;
      if (visible) loop();
      else cancelAnimationFrame(frame);
    });
    observer.observe(host);

    const onResize = () => {
      renderer.setSize(host.clientWidth, host.clientHeight);
      uniforms.uResolution.value.set(host.clientWidth, host.clientHeight);
      if (reduced) render();
    };
    window.addEventListener("resize", onResize);

    if (reduced) render(); // one static frame instead of motion
    else loop();

    return () => {
      cancelAnimationFrame(frame);
      observer.disconnect();
      document.removeEventListener("visibilitychange", onVisibility);
      window.removeEventListener("resize", onResize);
      // three.js does not garbage-collect GPU resources for us.
      geometry.dispose();
      material.dispose();
      renderer.dispose();
      host.removeChild(renderer.domElement);
    };
  }, []);

  return (
    <div
      ref={hostRef}
      aria-hidden="true"
      className={`pointer-events-none absolute inset-0 overflow-hidden ${className}`}
      style={{ background: "linear-gradient(180deg, #0b0913 0%, #151024 60%, #241634 100%)" }}
    />
  );
}
