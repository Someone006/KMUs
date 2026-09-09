/**
 * Swiss night-work computation - Arbeitsgesetz (ArG).
 *
 * For a business that operates 18:00-04:00 this is the largest controllable
 * cost line and the largest legal exposure. It is also the rule most small
 * operators get wrong, because the supplement depends on how OFTEN someone
 * works nights, not on the shift itself.
 *
 *   ArG Art. 16     Night work (23:00-06:00) is prohibited without authorisation.
 *   ArG Art. 17     Permit regime for regular and occasional night work.
 *   ArG Art. 17b(1) Occasional night work: +25% WAGE supplement on night hours.
 *   ArG Art. 17b(2) Regular night work (>= 25 nights per calendar year):
 *                   10% TIME compensation instead - it may not be paid out.
 *   ArG Art. 17a    Night shift max 9h work within a 10h window.
 *   ArG Art. 15a/6  11 hours daily rest between shifts.
 *   ArG Art. 29-32  Night work prohibited for employees under 18.
 *
 * PURE. Reference values live in NIGHT_RULES so a legal change is a data edit.
 */

import { localParts, minutesOfDay, nightKey, ZONE } from "../domain/time.js";
import type { Rappen } from "../domain/money.js";

export const NIGHT_RULES = {
  version: "2026-01-01",
  source: "ArG Art. 16, 17, 17a, 17b; ArGV 1",
  zone: ZONE,
  /** Night window in minutes from local midnight: 23:00 -> 06:00. */
  nightStartMin: 23 * 60,
  nightEndMin: 6 * 60,
  /** Evening work 20:00-23:00 needs no permit and carries no supplement. */
  eveningStartMin: 20 * 60,
  /** Threshold separating occasional from regular night work. */
  regularThresholdNightsPerYear: 25,
  /** Occasional night work: cash supplement, in percent. */
  occasionalWageSupplementPct: 25,
  /** Regular night work: time compensation, in percent. Not payable in cash. */
  regularTimeCompensationPct: 10,
  maxNightWorkMinutes: 9 * 60,
  maxNightWindowMinutes: 10 * 60,
  minDailyRestMinutes: 11 * 60,
  minAgeForNightWork: 18,
} as const;

export interface ShiftInput {
  courierId: string;
  courierName: string;
  /** UTC epoch ms. */
  start: number;
  end: number;
  hourlyWage: Rappen;
  /** Nights already worked in the current calendar year, excluding this one. */
  nightsThisYear: number;
  /** Optional; when under 18 the shift is illegal outright. */
  age?: number;
}

export type Compensation = "wage_supplement_25" | "time_compensation_10" | "none";

export interface ShiftViolation {
  code:
    | "EXCEEDS_MAX_NIGHT_WORK"
    | "EXCEEDS_NIGHT_WINDOW"
    | "MINOR_NIGHT_WORK"
    | "NEGATIVE_DURATION";
  message: string;
  legalRef: string;
}

export interface ShiftCost {
  courierId: string;
  courierName: string;
  start: number;
  end: number;
  totalMinutes: number;
  /** Minutes falling inside 23:00-06:00 Europe/Zurich, DST-correct. */
  nightMinutes: number;
  eveningMinutes: number;
  dayMinutes: number;
  compensation: Compensation;
  /** Base wage for all hours worked. */
  baseCost: Rappen;
  /** Cash supplement under Art. 17b(1). Zero under the regular-night regime. */
  supplementCost: Rappen;
  /** Paid time off accrued under Art. 17b(2), expressed in minutes. */
  timeCompensationMinutes: number;
  /** Employer cost of that time off, so the two regimes are comparable. */
  timeCompensationCost: Rappen;
  /** What this shift truly costs the employer. */
  totalCost: Rappen;
  /** Employer cost per minute - the input to per-drop economics. */
  costPerMinute: number;
  violations: ShiftViolation[];
  /** True when Art. 16/17 authorisation is required for this shift. */
  requiresPermit: boolean;
  nightKey: string;
}

/**
 * Minutes of [start, end) that fall inside the night window, evaluated in
 * local wall-clock time so DST transitions are handled by Intl rather than by
 * arithmetic. Minute-by-minute is O(shift length) and exact; a shift is at
 * most a few hundred iterations, so cleverness here would buy nothing.
 */
export function nightMinutesOf(start: number, end: number): {
  night: number;
  evening: number;
  day: number;
} {
  let night = 0;
  let evening = 0;
  let day = 0;
  const MIN = 60_000;
  for (let t = start; t < end; t += MIN) {
    const m = minutesOfDay(t);
    if (m >= NIGHT_RULES.nightStartMin || m < NIGHT_RULES.nightEndMin) night++;
    else if (m >= NIGHT_RULES.eveningStartMin) evening++;
    else day++;
  }
  return { night, evening, day };
}

export function computeShiftCost(shift: ShiftInput): ShiftCost {
  const violations: ShiftViolation[] = [];
  const totalMinutes = Math.max(0, Math.round((shift.end - shift.start) / 60_000));

  if (shift.end <= shift.start) {
    violations.push({
      code: "NEGATIVE_DURATION",
      message: "Shift ends before it starts.",
      legalRef: "-",
    });
  }

  const { night, evening, day } = nightMinutesOf(shift.start, shift.end);

  if (shift.age !== undefined && shift.age < NIGHT_RULES.minAgeForNightWork && night > 0) {
    violations.push({
      code: "MINOR_NIGHT_WORK",
      message: `${shift.courierName} is under ${NIGHT_RULES.minAgeForNightWork}; night work is prohibited.`,
      legalRef: "ArG Art. 31",
    });
  }
  if (night > 0 && totalMinutes > NIGHT_RULES.maxNightWorkMinutes) {
    violations.push({
      code: "EXCEEDS_MAX_NIGHT_WORK",
      message: `Night shift of ${(totalMinutes / 60).toFixed(1)}h exceeds the 9h maximum.`,
      legalRef: "ArG Art. 17a",
    });
  }
  if (night > 0 && totalMinutes > NIGHT_RULES.maxNightWindowMinutes) {
    violations.push({
      code: "EXCEEDS_NIGHT_WINDOW",
      message: "Night work must fall within a 10h window including breaks.",
      legalRef: "ArG Art. 17a",
    });
  }

  const isRegular = shift.nightsThisYear >= NIGHT_RULES.regularThresholdNightsPerYear;
  const compensation: Compensation =
    night === 0 ? "none" : isRegular ? "time_compensation_10" : "wage_supplement_25";

  const perMinute = shift.hourlyWage / 60;
  const baseCost = Math.round(perMinute * totalMinutes);

  let supplementCost = 0;
  let timeCompensationMinutes = 0;
  let timeCompensationCost = 0;

  if (compensation === "wage_supplement_25") {
    supplementCost = Math.round(
      perMinute * night * (NIGHT_RULES.occasionalWageSupplementPct / 100),
    );
  } else if (compensation === "time_compensation_10") {
    // Compensated in time, not cash - but the employer still funds those hours.
    timeCompensationMinutes = Math.round(
      night * (NIGHT_RULES.regularTimeCompensationPct / 100),
    );
    timeCompensationCost = Math.round(perMinute * timeCompensationMinutes);
  }

  const totalCost = baseCost + supplementCost + timeCompensationCost;

  return {
    courierId: shift.courierId,
    courierName: shift.courierName,
    start: shift.start,
    end: shift.end,
    totalMinutes,
    nightMinutes: night,
    eveningMinutes: evening,
    dayMinutes: day,
    compensation,
    baseCost,
    supplementCost,
    timeCompensationMinutes,
    timeCompensationCost,
    totalCost,
    costPerMinute: totalMinutes > 0 ? totalCost / totalMinutes : 0,
    violations,
    requiresPermit: night > 0,
    nightKey: nightKey(shift.start),
  };
}

/** Daily rest between consecutive shifts, ArG Art. 15a. */
export function checkDailyRest(
  previousEnd: number,
  nextStart: number,
): ShiftViolation | null {
  const restMinutes = Math.round((nextStart - previousEnd) / 60_000);
  if (restMinutes >= NIGHT_RULES.minDailyRestMinutes) return null;
  return {
    code: "EXCEEDS_NIGHT_WINDOW",
    message: `Only ${(restMinutes / 60).toFixed(1)}h rest between shifts; 11h required.`,
    legalRef: "ArG Art. 15a",
  };
}

/**
 * Which regime is cheaper for a given courier, and what crossing the
 * 25-night threshold does to the cost base. Operators plan rosters around
 * this and currently do it in their heads.
 */
export function regimeComparison(hourlyWage: Rappen, nightMinutesPerShift: number) {
  const perMinute = hourlyWage / 60;
  const occasional = Math.round(perMinute * nightMinutesPerShift * 0.25);
  const regular = Math.round(perMinute * nightMinutesPerShift * 0.10);
  return {
    occasionalSupplementPerShift: occasional,
    regularCompensationPerShift: regular,
    savingPerShiftWhenRegular: occasional - regular,
    thresholdNights: NIGHT_RULES.regularThresholdNightsPerYear,
    note:
      "Past 25 night shifts a year the 25% cash supplement is replaced by 10% " +
      "compensatory rest, which may not be paid out in cash.",
  };
}

export function describeLocalWindow(epochMs: number): string {
  const p = localParts(epochMs);
  return `${String(p.hour).padStart(2, "0")}:${String(p.minute).padStart(2, "0")}`;
}
