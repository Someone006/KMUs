/** Typed client for the NOCTA API. Every call funnels through one place so
 *  error handling and the error taxonomy stay consistent across surfaces. */

export interface ApiError {
  code: string;
  message: string;
  correlationId?: string;
}

export class ApiCallError extends Error {
  constructor(readonly api: ApiError, readonly status: number) {
    super(api.message);
    this.name = "ApiCallError";
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    ...init,
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!res.ok) {
    let body: { error?: ApiError } = {};
    try {
      body = await res.json();
    } catch {
      // A non-JSON error body still has to surface as something readable.
    }
    throw new ApiCallError(
      body.error ?? { code: "UNKNOWN", message: `Request failed (${res.status})` },
      res.status,
    );
  }
  return res.json() as Promise<T>;
}

export const api = {
  health: () => request<{ ok: boolean; products: number }>("/health"),
  catalog: () => request<{ products: CatalogItem[]; vatNote: string }>("/catalog"),
  zones: () => request<{ zones: ZoneView[] }>("/zones"),

  createRunde: (body: { zoneId: string; address: string; hostName: string; note?: string }) =>
    request<RundeView & { you: Participant }>("/runde", { method: "POST", body: JSON.stringify(body) }),
  getRunde: (code: string) => request<RundeView>(`/runde/${code}`),
  join: (code: string, name: string) =>
    request<RundeView & { you: Participant }>(`/runde/${code}/join`, {
      method: "POST", body: JSON.stringify({ name }),
    }),
  addItem: (code: string, participantId: string, productId: string, qty = 1) =>
    request<RundeView>(`/runde/${code}/items`, {
      method: "POST", body: JSON.stringify({ participantId, productId, qty }),
    }),
  setQty: (code: string, itemId: string, qty: number) =>
    request<RundeView>(`/runde/${code}/items/${itemId}`, {
      method: "PATCH", body: JSON.stringify({ qty }),
    }),
  place: (code: string) => request<RundeView>(`/runde/${code}/place`, { method: "POST" }),

  overview: () => request<Overview>("/ops/overview"),
  dispatch: () => request<DispatchView>("/ops/dispatch"),
  assign: () => request<{ runs: number; dropsAssigned: number }>("/ops/dispatch/assign", { method: "POST" }),
  labour: () => request<LabourView>("/ops/labour"),
  forecast: (zoneId: string) => request<ForecastView>(`/ops/forecast?zoneId=${zoneId}`),
  ledger: () => request<LedgerView>("/ops/ledger"),

  couriers: () => request<{ couriers: Courier[] }>("/couriers"),
  courier: (id: string) => request<CourierView>(`/courier/${id}`),
  deliver: (courierId: string, code: string) =>
    request<{ code: string; status: string }>(`/courier/${courierId}/deliver/${code}`, { method: "POST" }),
};

/* ---------------------------------- types --------------------------------- */

export interface CatalogItem {
  id: string; sku: string; name: string; brand: string; category: string;
  price: number; priceLabel: string; unitPrice: string | null; pack: string;
  allergens: string[]; chilled: boolean; inStock: boolean; stock: number;
  emoji: string; tags: string[];
}

export interface ZoneView {
  id: string; name: string; canton: string; baseFee: number;
  baseFeeLabel: string; minBasket: number; minBasketLabel: string; driveMinutes: number;
}

export interface Participant {
  id: string; rundeId: string; name: string; hue: number; joinedAt: number; isHost: boolean;
}

export interface Line {
  itemId: string; productId: string; participantId: string;
  name: string; emoji: string; qty: number; unitPrice: number; lineTotal: number;
}

export interface Share {
  participantId: string; name: string; hue: number; isHost: boolean;
  itemCount: number; goods: number; feeShare: number; total: number; lines: Line[];
}

export interface RundeView {
  runde: { id: string; code: string; zoneId: string; address: string; note: string; status: string; createdAt: number; closesAt: number; version: number };
  zone: { id: string; name: string; baseFee: number; minBasket: number };
  totals: {
    goods: number; deliveryFee: number; total: number; payable: number;
    vatFood: number; vatService: number; toMinimum: number;
    toFreeDelivery: number; meetsMinimum: boolean;
  };
  shares: Share[];
  sharerCount: number; itemCount: number; feePerPerson: number;
  soloFeeEach: number; saved: number; msRemaining: number;
  live: number; freeDeliveryThreshold: number;
  suggestion: { id: string; name: string; emoji: string; price: number; priceLabel: string; reason: string } | null;
}

export interface DropEconomics {
  code: string; address: string; sharers: number;
  revenue: number; revenueLabel: string;
  contribution: number; contributionLabel: string; contributionPct: number;
  profitable: boolean; courierCost: number; courierCostLabel: string;
  packagingCost: number; paymentCost: number; platformCost: number;
  goodsMargin: number; contributionIfSolo: number; contributionIfSoloLabel: string;
}

export interface Overview {
  now: number; nightKey: string; localTime: string;
  summary: {
    drops: number; revenue: number; revenueLabel: string;
    contribution: number; contributionLabel: string; contributionPct: number;
    avgBasket: number; avgBasketLabel: string; avgSharers: number;
    dropsPerCourierHour: number; unprofitableDrops: number;
    courierCost: number; courierCostLabel: string;
  };
  openRundes: number; placedDrops: number; activeCouriers: number;
  drops: DropEconomics[];
  groupUplift: {
    actual: number; actualLabel: string;
    counterfactual: number; counterfactualLabel: string;
    delta: number; deltaLabel: string;
  };
}

export interface DispatchView {
  depot: { lat: number; lng: number };
  runs: Array<{
    id: string;
    courier: Courier | null;
    stops: Array<{ position: number; code: string; address: string; items: number; chilled: boolean; basketLabel: string; etaMinutes: number; lat: number; lng: number }>;
    distanceM: number; distanceLabel: string; minutes: number;
    chilledRisk: boolean; dropsPerHour: number;
  }>;
}

export interface Courier {
  id: string; name: string; hourlyWage: number; nightsThisYear: number; active: boolean;
}

export interface LabourView {
  rules: { nightStartMin: number; nightEndMin: number; regularThresholdNightsPerYear: number; source: string; version: string };
  shifts: Array<{
    courierId: string; courierName: string; start: number; end: number;
    totalMinutes: number; nightMinutes: number; eveningMinutes: number;
    compensation: string; compensationLabel: string;
    baseCostLabel: string; supplementCostLabel: string;
    timeCompensationMinutes: number; timeCompensationCostLabel: string;
    totalCostLabel: string; requiresPermit: boolean;
    violations: Array<{ code: string; message: string; legalRef: string }>;
  }>;
  totals: {
    nightMinutes: number; nightHours: number;
    supplementCostLabel: string; timeCompensationCostLabel: string; totalCostLabel: string;
  };
  restViolations: Array<{ courier: string; message: string; legalRef: string }>;
  permitRequired: boolean;
  regimeExample: { occasionalSupplementPerShift: number; regularCompensationPerShift: number; savingPerShiftWhenRegular: number; thresholdNights: number; note: string };
}

export interface ForecastView {
  zone: { id: string; name: string };
  forecast: {
    targetNight: string; isWeekend: boolean; totalExpected: number;
    peakHour: number; peakCouriers: number; courierHours: number;
    hours: Array<{ hour: number; expectedOrders: number; low: number; high: number; confidence: string; observations: number; couriersNeeded: number }>;
  };
  parLevels: Array<{ productId: string; name: string; emoji: string; expectedUnits: number; parLevel: number; currentStock: number; reorder: number; stockoutRisk: string }>;
  depotExpected: number;
  breakEven: { basket: number; basketLabel: string; freeDeliveryBasket: number; freeDeliveryBasketLabel: string; currentMinimum: number; currentMinimumLabel: string };
}

export interface LedgerView {
  integrity: { ok: boolean; checked: number; brokenAt: number | null };
  events: Array<{ seq: number; at: number; kind: string; actor: string; subject: string; payload: unknown; hash: string }>;
}

export interface CourierView {
  courier: Courier;
  shift: {
    window: string; nightMinutes: number; nightHours: number;
    compensation: string; compensationLabel: string; earnings: string;
    supplementLabel: string; timeCreditMinutes: number;
    violations: Array<{ code: string; message: string; legalRef: string }>;
  } | null;
  stops: Array<{ position: number; rundeId: string; code: string; address: string; items: number; chilled: boolean; etaMinutes: number; lat: number; lng: number }>;
  totals: { stops: number; distanceLabel: string; minutes: number; chilledRisk: boolean };
}
