/** Presentation helpers. Formatting happens once, at the edge. */

export function chf(rappen: number): string {
  const sign = rappen < 0 ? "−" : "";
  const abs = Math.abs(rappen);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

export function chfFull(rappen: number): string {
  return `CHF ${chf(rappen)}`;
}

/** mm:ss for a countdown. Never negative — a closed Runde shows 0:00. */
export function countdown(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

export function hourLabel(hour: number): string {
  return `${String(hour).padStart(2, "0")}`;
}

export function clockOf(epochMs: number): string {
  return new Intl.DateTimeFormat("de-CH", {
    timeZone: "Europe/Zurich", hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(epochMs));
}

export function hueColor(hue: number, lightness = 0.72, chroma = 0.16): string {
  return `oklch(${lightness} ${chroma} ${hue})`;
}

export const ALLERGEN_DE: Record<string, string> = {
  gluten: "Gluten", milk: "Milch", egg: "Ei", soy: "Soja", nuts: "Schalenfrüchte",
  peanut: "Erdnuss", sesame: "Sesam", sulphite: "Sulfite", celery: "Sellerie",
  mustard: "Senf", fish: "Fisch", crustacean: "Krebstiere",
};

export const CATEGORY_DE: Record<string, string> = {
  energy: "Energy", softdrink: "Softdrinks", water: "Wasser", juice: "Saft",
  coffee: "Kaffee", chips: "Chips", sweets: "Süsses", chocolate: "Schoggi",
  icecream: "Glace", savoury: "Salziges", hangover: "Morgen danach",
};
