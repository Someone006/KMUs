/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ground: "var(--ground)",
        surface: "var(--surface)",
        raised: "var(--raised)",
        overlay: "var(--overlay)",
        hairline: "var(--hairline)",
        ink: "var(--ink)",
        muted: "var(--muted)",
        faint: "var(--faint)",
        accent: "var(--accent)",
        "accent-soft": "var(--accent-soft)",
        warn: "var(--warn)",
        good: "var(--good)",
        bad: "var(--bad)",
      },
      fontFamily: {
        sans: ['"Inter var"', "Inter", "system-ui", "-apple-system", "Segoe UI", "sans-serif"],
        mono: ['"JetBrains Mono"', "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      borderRadius: { xl: "12px", "2xl": "16px", "3xl": "22px" },
      transitionTimingFunction: { out: "cubic-bezier(.22,1,.36,1)" },
    },
  },
  plugins: [],
};
