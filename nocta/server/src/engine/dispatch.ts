/**
 * Dispatch: batching drops into runs, and ordering the stops within a run.
 *
 * Drop density is the second lever on contribution margin. One courier
 * carrying four drops on one loop costs barely more than one carrying a single
 * drop, so batching converts directly into margin.
 *
 * Nearest-neighbour construction followed by 2-opt improvement. For the 3-8
 * stops a night run actually contains this reaches the optimal tour in
 * practice, and it runs in microseconds.
 *
 * PURE.
 */

import type { Drop, Run } from "../domain/types.js";

export const DISPATCH_MODEL = {
  version: "2026-09-01",
  /** Average night speed in a Swiss city: little traffic, many 30 km/h zones. */
  kmhNight: 22,
  serviceMinutesPerDrop: 4,
  serviceMinutesChilled: 1,
  /** Beyond this a cold bag stops holding temperature. */
  maxChilledMinutes: 25,
  maxStopsPerRun: 5,
  /** A run longer than this hurts promised ETAs on the last drop. */
  maxRunMinutes: 45,
} as const;

export interface LatLng {
  lat: number;
  lng: number;
}

/** Great-circle distance in metres. */
export function haversineM(a: LatLng, b: LatLng): number {
  const R = 6_371_000;
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
}

/**
 * Street distance is longer than the crow flies. 1.35 is the usual factor for
 * a European city grid; it keeps ETAs honest rather than optimistic.
 */
const DETOUR_FACTOR = 1.35;

export function travelMinutes(distanceM: number): number {
  const streetM = distanceM * DETOUR_FACTOR;
  return (streetM / 1000 / DISPATCH_MODEL.kmhNight) * 60;
}

export interface RoutePlan {
  order: number[];
  distanceM: number;
  driveMinutes: number;
  serviceMinutes: number;
  totalMinutes: number;
  /** Cumulative minutes from depot departure to each stop, in visit order. */
  etaMinutes: number[];
  /** Minutes attributed to each stop for margin accounting, in stop index order. */
  attributedMinutes: number[];
  chilledRisk: boolean;
}

function tourDistance(depot: LatLng, stops: LatLng[], order: number[]): number {
  let d = 0;
  let cur = depot;
  for (const i of order) {
    d += haversineM(cur, stops[i]);
    cur = stops[i];
  }
  d += haversineM(cur, depot); // the courier has to come back
  return d;
}

/** Nearest-neighbour construction from the depot. */
function nearestNeighbour(depot: LatLng, stops: LatLng[]): number[] {
  const unvisited = new Set(stops.map((_, i) => i));
  const order: number[] = [];
  let cur = depot;
  while (unvisited.size > 0) {
    let best = -1;
    let bestD = Infinity;
    for (const i of unvisited) {
      const d = haversineM(cur, stops[i]);
      if (d < bestD) {
        bestD = d;
        best = i;
      }
    }
    order.push(best);
    unvisited.delete(best);
    cur = stops[best];
  }
  return order;
}

/** 2-opt: repeatedly reverse a segment while that shortens the tour. */
function twoOpt(depot: LatLng, stops: LatLng[], initial: number[]): number[] {
  let order = [...initial];
  let best = tourDistance(depot, stops, order);
  let improved = true;
  let guard = 0;
  while (improved && guard++ < 100) {
    improved = false;
    for (let i = 0; i < order.length - 1; i++) {
      for (let j = i + 1; j < order.length; j++) {
        const candidate = [
          ...order.slice(0, i),
          ...order.slice(i, j + 1).reverse(),
          ...order.slice(j + 1),
        ];
        const d = tourDistance(depot, stops, candidate);
        if (d < best - 0.5) {
          order = candidate;
          best = d;
          improved = true;
        }
      }
    }
  }
  return order;
}

export function planRoute(depot: LatLng, drops: Drop[]): RoutePlan {
  if (drops.length === 0) {
    return {
      order: [], distanceM: 0, driveMinutes: 0, serviceMinutes: 0, totalMinutes: 0,
      etaMinutes: [], attributedMinutes: [], chilledRisk: false,
    };
  }
  const points: LatLng[] = drops.map((d) => ({ lat: d.lat, lng: d.lng }));
  const order = twoOpt(depot, points, nearestNeighbour(depot, points));

  const distanceM = tourDistance(depot, points, order);
  const serviceEach = order.map(
    (i) =>
      DISPATCH_MODEL.serviceMinutesPerDrop +
      (drops[i].chilled ? DISPATCH_MODEL.serviceMinutesChilled : 0),
  );
  const serviceMinutes = serviceEach.reduce((a, b) => a + b, 0);

  // Walk the tour to get a real ETA per stop, not an average.
  const etaMinutes: number[] = [];
  const attributedMinutes = new Array(drops.length).fill(0);
  let cur = depot;
  let elapsed = 0;
  order.forEach((stopIdx, position) => {
    const leg = haversineM(cur, points[stopIdx]);
    const legMin = travelMinutes(leg);
    elapsed += legMin;
    etaMinutes.push(Math.round(elapsed));
    // Each drop carries its own leg plus its own doorstep time. The return
    // leg is shared, so it is spread evenly across the run.
    attributedMinutes[stopIdx] = legMin + serviceEach[position];
    elapsed += serviceEach[position];
    cur = points[stopIdx];
  });
  const returnMin = travelMinutes(haversineM(cur, depot));
  const share = returnMin / drops.length;
  for (let i = 0; i < attributedMinutes.length; i++) attributedMinutes[i] += share;

  const driveMinutes = travelMinutes(distanceM);
  const totalMinutes = driveMinutes + serviceMinutes;

  // Would any chilled item sit in the bag too long?
  const chilledRisk = order.some(
    (stopIdx, position) =>
      drops[stopIdx].chilled && etaMinutes[position] > DISPATCH_MODEL.maxChilledMinutes,
  );

  return {
    order,
    distanceM: Math.round(distanceM),
    driveMinutes: Math.round(driveMinutes * 10) / 10,
    serviceMinutes,
    totalMinutes: Math.round(totalMinutes * 10) / 10,
    etaMinutes,
    attributedMinutes: attributedMinutes.map((m) => Math.round(m * 10) / 10),
    chilledRisk,
  };
}

/**
 * Group pending drops into runs. Greedy spatial clustering: seed on the
 * oldest waiting drop (so nobody is starved), then absorb the nearest drops
 * that keep the run inside its time and chilled constraints.
 */
export function batchIntoRuns(
  depot: LatLng,
  pending: Drop[],
  now: number,
  maxStops: number = DISPATCH_MODEL.maxStopsPerRun,
): { runs: Array<{ drops: Drop[]; plan: RoutePlan }>; unassigned: Drop[] } {
  const remaining = [...pending].sort((a, b) => a.placedAt - b.placedAt);
  const runs: Array<{ drops: Drop[]; plan: RoutePlan }> = [];

  while (remaining.length > 0) {
    const seed = remaining.shift()!;
    const group = [seed];

    // Absorb nearest neighbours while the constraints still hold.
    while (group.length < maxStops && remaining.length > 0) {
      let bestIdx = -1;
      let bestD = Infinity;
      for (let i = 0; i < remaining.length; i++) {
        const d = Math.min(
          ...group.map((g) =>
            haversineM({ lat: g.lat, lng: g.lng }, { lat: remaining[i].lat, lng: remaining[i].lng }),
          ),
        );
        if (d < bestD) {
          bestD = d;
          bestIdx = i;
        }
      }
      if (bestIdx < 0) break;

      const candidate = [...group, remaining[bestIdx]];
      const plan = planRoute(depot, candidate);
      const hasChilled = candidate.some((d) => d.chilled);
      const withinTime = plan.totalMinutes <= DISPATCH_MODEL.maxRunMinutes;
      const chilledOk = !hasChilled || !plan.chilledRisk;
      if (!withinTime || !chilledOk) break;

      group.push(remaining[bestIdx]);
      remaining.splice(bestIdx, 1);
    }

    runs.push({ drops: group, plan: planRoute(depot, group) });
  }

  return { runs, unassigned: [] };
}

export function runFromPlan(
  id: string,
  courierId: string | null,
  drops: Drop[],
  plan: RoutePlan,
  createdAt: number,
): Run {
  return {
    id,
    courierId,
    stops: drops,
    order: plan.order,
    distanceM: plan.distanceM,
    minutes: plan.totalMinutes,
    createdAt,
  };
}
