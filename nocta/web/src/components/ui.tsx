/** Shared primitives. One spacing scale, one shape language, five states. */

import { motion } from "motion/react";
import type { ReactNode } from "react";

export function Stat({
  label, value, sub, tone = "default", large = false,
}: {
  label: string; value: ReactNode; sub?: ReactNode;
  tone?: "default" | "good" | "bad" | "warn" | "accent"; large?: boolean;
}) {
  const toneColor = {
    default: "var(--ink)", good: "var(--good)", bad: "var(--bad)",
    warn: "var(--warn)", accent: "var(--accent)",
  }[tone];
  return (
    <div className="card p-4">
      <div className="label">{label}</div>
      <div
        className={`tnum mt-1.5 font-semibold ${large ? "text-3xl" : "text-xl"}`}
        style={{ color: toneColor, letterSpacing: large ? "-0.02em" : undefined }}
      >
        {value}
      </div>
      {sub ? <div className="mt-1 text-[13px] text-muted">{sub}</div> : null}
    </div>
  );
}

export function Pill({
  children, tone = "default",
}: { children: ReactNode; tone?: "default" | "good" | "bad" | "warn" | "accent" }) {
  const styles = {
    default: { bg: "var(--raised)", fg: "var(--muted)" },
    good: { bg: "oklch(0.78 0.152 158 / 0.15)", fg: "var(--good)" },
    bad: { bg: "oklch(0.688 0.194 22 / 0.16)", fg: "var(--bad)" },
    warn: { bg: "oklch(0.812 0.154 78 / 0.16)", fg: "var(--warn)" },
    accent: { bg: "var(--accent-soft)", fg: "var(--accent)" },
  }[tone];
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-semibold"
      style={{ background: styles.bg, color: styles.fg }}
    >
      {children}
    </span>
  );
}

export function Button({
  children, onClick, variant = "primary", disabled, full, type = "button", ...rest
}: {
  children: ReactNode; onClick?: () => void;
  variant?: "primary" | "ghost" | "quiet"; disabled?: boolean; full?: boolean;
  type?: "button" | "submit";
} & Record<string, unknown>) {
  const base =
    "inline-flex items-center justify-center gap-2 rounded-xl px-4 font-semibold transition-colors duration-100 disabled:opacity-40 disabled:cursor-not-allowed";
  // 48px tall: this is used one-handed, outdoors, while walking.
  const size = "min-h-[48px] text-[15px]";
  const styles = {
    primary: "text-[#140f1f]",
    ghost: "text-ink border border-hairline hover:bg-raised",
    quiet: "text-muted hover:text-ink",
  }[variant];
  return (
    <motion.button
      type={type}
      whileTap={disabled ? undefined : { scale: 0.975 }}
      transition={{ duration: 0.08 }}
      onClick={onClick}
      disabled={disabled}
      className={`${base} ${size} ${styles} ${full ? "w-full" : ""}`}
      style={variant === "primary" ? { background: "var(--accent)" } : undefined}
      {...rest}
    >
      {children}
    </motion.button>
  );
}

/** Empty state: names what is absent AND the action that fills it. */
export function Empty({ icon, title, hint }: { icon: string; title: string; hint?: string }) {
  return (
    <div className="flex flex-col items-center justify-center px-6 py-12 text-center">
      <div className="text-3xl opacity-60">{icon}</div>
      <div className="mt-3 font-semibold text-ink">{title}</div>
      {hint ? <div className="mt-1 max-w-[34ch] text-[13px] leading-relaxed text-faint">{hint}</div> : null}
    </div>
  );
}

/** Loading skeleton matching the final geometry, so nothing jumps on resolve. */
export function Skeleton({ h = 64, className = "" }: { h?: number; className?: string }) {
  return (
    <div
      className={`animate-pulse rounded-2xl ${className}`}
      style={{ height: h, background: "var(--surface)" }}
    />
  );
}

export function ErrorNote({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div
      className="rounded-2xl border p-4 text-[14px]"
      style={{ borderColor: "oklch(0.688 0.194 22 / 0.35)", background: "oklch(0.688 0.194 22 / 0.10)" }}
    >
      <div className="font-semibold" style={{ color: "var(--bad)" }}>Das hat nicht geklappt</div>
      <div className="mt-1 text-muted">{message}</div>
      {onRetry ? (
        <button onClick={onRetry} className="mt-3 text-[13px] font-semibold" style={{ color: "var(--accent)" }}>
          Nochmal versuchen
        </button>
      ) : null}
    </div>
  );
}

export function SectionTitle({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <div className="mb-3 flex items-baseline justify-between gap-4">
      <h2 className="text-[15px] font-semibold tracking-tight text-ink">{children}</h2>
      {right}
    </div>
  );
}

/** Horizontal bar for a share-of-total. Meaning never rides on colour alone. */
export function Bar({ value, max, tone = "accent" }: { value: number; max: number; tone?: string }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  const color =
    tone === "good" ? "var(--good)" : tone === "bad" ? "var(--bad)"
    : tone === "warn" ? "var(--warn)" : "var(--accent)";
  return (
    <div className="h-1.5 w-full overflow-hidden rounded-full" style={{ background: "var(--raised)" }}>
      <motion.div
        className="h-full rounded-full"
        style={{ background: color }}
        initial={{ width: 0 }}
        animate={{ width: `${pct}%` }}
        transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
      />
    </div>
  );
}
