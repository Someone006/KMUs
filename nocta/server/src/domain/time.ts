/**
 * Time. Everything is stored as UTC epoch milliseconds; every business rule
 * that depends on a wall clock names its timezone explicitly.
 *
 * Swiss labour law is defined in local time, so DST is a correctness concern,
 * not a detail. Intl handles the transitions; hand-rolled offsets do not.
 */

export const ZONE = "Europe/Zurich";

export interface LocalParts {
  year: number;
  month: number; // 1-12
  day: number;
  hour: number; // 0-23
  minute: number;
  weekday: number; // 0=Sunday .. 6=Saturday
}

const partsFormatter = new Intl.DateTimeFormat("en-CH", {
  timeZone: ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  weekday: "short",
});

const WEEKDAYS: Record<string, number> = {
  Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6,
};

/** Decompose an epoch into Europe/Zurich wall-clock parts. */
export function localParts(epochMs: number): LocalParts {
  const parts = partsFormatter.formatToParts(new Date(epochMs));
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  const hour = Number(get("hour"));
  return {
    year: Number(get("year")),
    month: Number(get("month")),
    day: Number(get("day")),
    // Intl emits "24" for midnight under hourCycle h23/h24 in some ICU builds.
    hour: hour === 24 ? 0 : hour,
    minute: Number(get("minute")),
    weekday: WEEKDAYS[get("weekday")] ?? 0,
  };
}

/** Minutes since local midnight, 0..1439. */
export function minutesOfDay(epochMs: number): number {
  const p = localParts(epochMs);
  return p.hour * 60 + p.minute;
}

/** "2026-09-12" in Europe/Zurich. Used to bucket a trading night. */
export function localDateKey(epochMs: number): string {
  const p = localParts(epochMs);
  return `${p.year}-${String(p.month).padStart(2, "0")}-${String(p.day).padStart(2, "0")}`;
}

/**
 * A trading night runs past midnight, so the night of Friday includes
 * Saturday 02:00. Anything before 06:00 belongs to the previous day's night.
 */
export function nightKey(epochMs: number): string {
  const p = localParts(epochMs);
  if (p.hour < 6) return localDateKey(epochMs - 24 * 3600_000);
  return localDateKey(epochMs);
}

export function isWeekendNight(epochMs: number): boolean {
  const p = localParts(epochMs);
  const day = p.hour < 6 ? (p.weekday + 6) % 7 : p.weekday;
  return day === 5 || day === 6; // Friday or Saturday night
}

export const MINUTE = 60_000;
export const HOUR = 3_600_000;

/** Injectable clock. Engines never call Date.now() themselves. */
export interface Clock {
  now(): number;
}

export const systemClock: Clock = { now: () => Date.now() };

export function fixedClock(epochMs: number): Clock {
  return { now: () => epochMs };
}
