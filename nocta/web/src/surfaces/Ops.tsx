/**
 * The operator console. Four questions, answered on one screen:
 * is tonight profitable, where are the couriers, is the roster legal,
 * and what will the next hours need.
 */

import { useEffect, useState } from "react";
import { motion } from "motion/react";
import { Bar, Button, Empty, ErrorNote, Pill, SectionTitle, Skeleton, Stat } from "../components/ui";
import { api, type DispatchView, type ForecastView, type LabourView, type LedgerView, type Overview } from "../lib/api";
import { chf, chfFull, clockOf, hourLabel } from "../lib/format";

type Tab = "tonight" | "dispatch" | "labour" | "forecast" | "ledger";

export function Ops() {
  const [tab, setTab] = useState<Tab>("tonight");
  const [overview, setOverview] = useState<Overview | null>(null);
  const [dispatch, setDispatch] = useState<DispatchView | null>(null);
  const [labour, setLabour] = useState<LabourView | null>(null);
  const [forecast, setForecast] = useState<ForecastView | null>(null);
  const [ledger, setLedger] = useState<LedgerView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = () => {
    Promise.all([
      api.overview(), api.dispatch(), api.labour(),
      api.forecast("z-k4"), api.ledger(),
    ])
      .then(([o, d, l, f, le]) => {
        setOverview(o); setDispatch(d); setLabour(l); setForecast(f); setLedger(le);
        setError(null);
      })
      .catch((e: Error) => setError(e.message));
  };

  useEffect(() => {
    load();
    const t = setInterval(load, 15_000);
    return () => clearInterval(t);
  }, []);

  if (error) return <div className="mx-auto max-w-[1100px] p-6"><ErrorNote message={error} onRetry={load} /></div>;

  if (!overview || !dispatch || !labour || !forecast || !ledger) {
    return (
      <div className="mx-auto max-w-[1100px] space-y-4 p-6">
        <Skeleton h={40} />
        <div className="grid grid-cols-4 gap-4">
          {[0, 1, 2, 3].map((i) => <Skeleton key={i} h={104} />)}
        </div>
        <Skeleton h={320} />
      </div>
    );
  }

  const s = overview.summary;

  return (
    <div className="mx-auto max-w-[1100px] px-5 pb-16 pt-6">
      <header className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2.5">
            <span className="text-lg">🌙</span>
            <span className="text-[13px] font-semibold tracking-[0.16em]">NOCTA · LEITSTAND</span>
          </div>
          <div className="mt-1 text-[13px] text-faint">
            Nacht {overview.nightKey} · Zürich {overview.localTime}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Pill tone={overview.activeCouriers > 0 ? "good" : "warn"}>
            {overview.activeCouriers} {overview.activeCouriers === 1 ? "Kurier:in" : "Kurier:innen"} im Dienst
          </Pill>
          <Pill>{overview.openRundes} offene {overview.openRundes === 1 ? "Runde" : "Runden"}</Pill>
        </div>
      </header>

      <nav className="mt-6 flex gap-1 overflow-x-auto border-b border-hairline">
        {([
          ["tonight", "Heute Nacht"], ["dispatch", "Disposition"],
          ["labour", "Nachtarbeit"], ["forecast", "Prognose"], ["ledger", "Protokoll"],
        ] as Array<[Tab, string]>).map(([id, label]) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            aria-pressed={tab === id}
            className="relative shrink-0 px-4 py-3 text-[14px] font-semibold transition-colors"
            style={{ color: tab === id ? "var(--ink)" : "var(--faint)" }}
          >
            {label}
            {tab === id ? (
              <motion.div layoutId="ops-tab" className="absolute inset-x-2 bottom-0 h-[2px]" style={{ background: "var(--accent)" }} />
            ) : null}
          </button>
        ))}
      </nav>

      <div className="mt-6">
        {/* ------------------------------ tonight ------------------------ */}
        {tab === "tonight" ? (
          <div className="space-y-6">
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              <Stat label="Umsatz" value={s.revenueLabel} sub={`${s.drops} Lieferungen`} large />
              <Stat
                label="Deckungsbeitrag"
                value={s.contributionLabel}
                sub={`${s.contributionPct}% vom Umsatz`}
                tone={s.contribution >= 0 ? "good" : "bad"}
                large
              />
              <Stat label="Ø Warenkorb" value={s.avgBasketLabel} sub={`Ø ${s.avgSharers} Personen pro Runde`} large />
              <Stat
                label="Stops pro Kurierstunde"
                value={s.dropsPerCourierHour}
                sub={s.unprofitableDrops > 0 ? `${s.unprofitableDrops} Lieferung(en) im Minus` : "alle Lieferungen im Plus"}
                tone={s.unprofitableDrops > 0 ? "warn" : "good"}
                large
              />
            </div>

            {/* The single most valuable number in the product. */}
            <div className="card p-5">
              <SectionTitle right={<Pill tone="accent">Runden-Effekt</Pill>}>
                Was das gemeinsame Bestellen einbringt
              </SectionTitle>
              <div className="grid gap-5 sm:grid-cols-3">
                <div>
                  <div className="label">Deckungsbeitrag heute</div>
                  <div className="tnum mt-1 text-2xl font-semibold" style={{ color: "var(--good)" }}>
                    {overview.groupUplift.actualLabel}
                  </div>
                </div>
                <div>
                  <div className="label">Hätten alle einzeln bestellt</div>
                  <div className="tnum mt-1 text-2xl font-semibold" style={{ color: "var(--bad)" }}>
                    {overview.groupUplift.counterfactualLabel}
                  </div>
                </div>
                <div>
                  <div className="label">Differenz</div>
                  <div className="tnum mt-1 text-2xl font-semibold" style={{ color: "var(--accent)" }}>
                    {overview.groupUplift.deltaLabel}
                  </div>
                </div>
              </div>
              <p className="mt-4 max-w-[70ch] text-[13px] leading-relaxed text-muted">
                Die Kosten einer Lieferung sind fix. Nur der Warenkorb lässt sich billig
                vergrössern — und geteilte Runden sind der stärkste Hebel dafür.
              </p>
            </div>

            <div>
              <SectionTitle right={<span className="text-[12px] text-faint">Deckungsbeitrag je Lieferung</span>}>
                Lieferungen heute Nacht
              </SectionTitle>
              {overview.drops.length === 0 ? (
                <Empty icon="📦" title="Noch keine Bestellung" hint="Sobald eine Runde abgeschlossen wird, erscheint sie hier mit ihrer Marge." />
              ) : (
                <div className="card divide-y" style={{ borderColor: "var(--hairline)" }}>
                  {overview.drops.map((d) => (
                    <div key={d.code} className="flex items-center gap-4 p-3.5">
                      <span className="tnum w-[74px] shrink-0 text-[13px] font-semibold tracking-wider">{d.code}</span>
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-[13.5px]">{d.address}</span>
                        <span className="mt-1 block text-[11.5px] text-faint">
                          {d.sharers} {d.sharers === 1 ? "Person" : "Personen"} · Kurier {d.courierCostLabel} · Umsatz {d.revenueLabel}
                        </span>
                      </span>
                      <span className="w-[92px] shrink-0 text-right">
                        <span
                          className="tnum block text-[15px] font-semibold"
                          style={{ color: d.profitable ? "var(--good)" : "var(--bad)" }}
                        >
                          {d.contributionLabel}
                        </span>
                        <span className="tnum block text-[11px] text-faint">{d.contributionPct}%</span>
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        ) : null}

        {/* ----------------------------- dispatch ------------------------ */}
        {tab === "dispatch" ? (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <SectionTitle>Touren · {dispatch.runs.length} geplant</SectionTitle>
              <Button
                variant="ghost"
                disabled={busy || dispatch.runs.length === 0}
                onClick={async () => {
                  setBusy(true);
                  await api.assign().catch(() => undefined);
                  load();
                  setBusy(false);
                }}
              >
                {busy ? "Wird zugeteilt…" : "Touren zuteilen"}
              </Button>
            </div>

            {dispatch.runs.length === 0 ? (
              <Empty icon="🛵" title="Keine offenen Lieferungen" hint="Abgeschlossene Runden werden automatisch zu Touren gebündelt." />
            ) : (
              dispatch.runs.map((run) => (
                <div key={run.id} className="card p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="flex items-center gap-3">
                      <span className="text-[14px] font-semibold">{run.courier?.name ?? "Nicht zugeteilt"}</span>
                      <Pill>{run.stops.length} Stops</Pill>
                      <Pill>{run.distanceLabel}</Pill>
                      <Pill>{Math.round(run.minutes)} min</Pill>
                      <Pill tone={run.dropsPerHour >= 3 ? "good" : "warn"}>
                        {run.dropsPerHour} Stops/h
                      </Pill>
                    </div>
                    {run.chilledRisk ? <Pill tone="bad">Kühlkette gefährdet</Pill> : null}
                  </div>
                  <ol className="mt-3.5 space-y-1.5">
                    {run.stops.map((stop) => (
                      <li key={stop.code} className="flex items-center gap-3 text-[13px]">
                        <span
                          className="tnum grid h-6 w-6 shrink-0 place-items-center rounded-full text-[11px] font-bold text-[#140f1f]"
                          style={{ background: "var(--accent)" }}
                        >
                          {stop.position}
                        </span>
                        <span className="min-w-0 flex-1 truncate">{stop.address}</span>
                        {stop.chilled ? <span className="shrink-0 text-[11px] text-faint">❄</span> : null}
                        <span className="tnum shrink-0 text-faint">{stop.items} Art.</span>
                        <span className="tnum w-[52px] shrink-0 text-right font-semibold">+{stop.etaMinutes}′</span>
                      </li>
                    ))}
                  </ol>
                </div>
              ))
            )}
          </div>
        ) : null}

        {/* ------------------------------ labour ------------------------- */}
        {tab === "labour" ? (
          <div className="space-y-5">
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              <Stat label="Nachtstunden (23–06)" value={`${labour.totals.nightHours} h`} sub="ArG Art. 16" />
              <Stat label="Lohnzuschläge 25%" value={labour.totals.supplementCostLabel} sub="ArG Art. 17b Abs. 1" tone="warn" />
              <Stat label="Zeitzuschläge 10%" value={labour.totals.timeCompensationCostLabel} sub="ArG Art. 17b Abs. 2" />
              <Stat label="Personalkosten Nacht" value={labour.totals.totalCostLabel} sub="Arbeitgeberkosten" large />
            </div>

            {labour.permitRequired ? (
              <div className="rounded-2xl p-4" style={{ background: "oklch(0.812 0.154 78 / 0.12)", border: "1px solid oklch(0.812 0.154 78 / 0.3)" }}>
                <div className="text-[14px] font-semibold" style={{ color: "var(--warn)" }}>
                  Bewilligungspflichtige Nachtarbeit im Einsatzplan
                </div>
                <p className="mt-1 max-w-[80ch] text-[13px] leading-relaxed text-muted">
                  Arbeit zwischen 23:00 und 06:00 ist nach ArG Art. 16 bewilligungspflichtig. Bis
                  zu {labour.rules.regularThresholdNightsPerYear} Nächten pro Kalenderjahr gilt der
                  Lohnzuschlag von 25%; ab dann tritt der Zeitzuschlag von 10% an dessen Stelle
                  und darf nicht mehr ausbezahlt werden.
                </p>
              </div>
            ) : null}

            {labour.restViolations.length > 0 ? (
              <div className="rounded-2xl p-4" style={{ background: "oklch(0.688 0.194 22 / 0.12)", border: "1px solid oklch(0.688 0.194 22 / 0.3)" }}>
                <div className="text-[14px] font-semibold" style={{ color: "var(--bad)" }}>Ruhezeit unterschritten</div>
                <ul className="mt-1.5 space-y-1">
                  {labour.restViolations.map((v, i) => (
                    <li key={i} className="text-[13px] text-muted">{v.courier}: {v.message} <span className="text-faint">({v.legalRef})</span></li>
                  ))}
                </ul>
              </div>
            ) : null}

            <div className="card overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-[13px]">
                  <thead>
                    <tr className="border-b border-hairline text-left" style={{ color: "var(--faint)" }}>
                      <th className="p-3 font-semibold">Kurier:in</th>
                      <th className="p-3 font-semibold">Schicht</th>
                      <th className="p-3 text-right font-semibold">Nacht</th>
                      <th className="p-3 font-semibold">Regime</th>
                      <th className="p-3 text-right font-semibold">Zuschlag</th>
                      <th className="p-3 text-right font-semibold">Total</th>
                    </tr>
                  </thead>
                  <tbody>
                    {labour.shifts.map((sh) => (
                      <tr key={sh.courierId} className="border-b last:border-0" style={{ borderColor: "var(--hairline)" }}>
                        <td className="p-3 font-semibold">{sh.courierName}</td>
                        <td className="tnum p-3 text-muted">{clockOf(sh.start)}–{clockOf(sh.end)}</td>
                        <td className="tnum p-3 text-right">{(sh.nightMinutes / 60).toFixed(1)} h</td>
                        <td className="p-3">
                          <Pill tone={sh.compensation === "wage_supplement_25" ? "warn" : sh.compensation === "none" ? "default" : "accent"}>
                            {sh.compensationLabel}
                          </Pill>
                        </td>
                        <td className="tnum p-3 text-right">
                          {sh.compensation === "time_compensation_10"
                            ? `${sh.timeCompensationMinutes} min`
                            : sh.supplementCostLabel}
                        </td>
                        <td className="tnum p-3 text-right font-semibold">{sh.totalCostLabel}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <p className="text-[12px] leading-relaxed text-faint">
              Grundlage: {labour.rules.source} · Regelwerk-Version {labour.rules.version}. Die Berechnung
              erfolgt in Europe/Zurich und berücksichtigt Sommer-/Winterzeitwechsel.
            </p>
          </div>
        ) : null}

        {/* ----------------------------- forecast ------------------------ */}
        {tab === "forecast" ? (
          <div className="space-y-6">
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              <Stat label="Erwartete Bestellungen" value={forecast.forecast.totalExpected} sub={`${forecast.zone.name} · Depot gesamt ${forecast.depotExpected}`} large />
              <Stat label="Spitzenstunde" value={`${hourLabel(forecast.forecast.peakHour)}:00`} sub={forecast.forecast.isWeekend ? "Wochenende" : "Wochentag"} large />
              <Stat label="Kurier:innen zur Spitze" value={forecast.forecast.peakCouriers} sub={`${forecast.forecast.courierHours} Kurierstunden total`} large />
              <Stat
                label="Break-even-Warenkorb"
                value={forecast.breakEven.basketLabel}
                sub={`bei Gratislieferung ${forecast.breakEven.freeDeliveryBasketLabel} · Minimum ${forecast.breakEven.currentMinimumLabel}`}
                tone={forecast.breakEven.basket > forecast.breakEven.currentMinimum ? "warn" : "good"}
                large
              />
            </div>

            <div className="card p-5">
              <SectionTitle>Nachtkurve</SectionTitle>
              <div className="flex items-end gap-1.5" data-testid="night-curve" style={{ height: 190 }}>
                {forecast.forecast.hours.map((h, i) => {
                  // Scale to the largest expectation so the curve uses the
                  // full height; scaling to the upper band flattens it.
                  const max = Math.max(...forecast.forecast.hours.map((x) => x.expectedOrders), 1);
                  return (
                    <div key={h.hour} className="flex flex-1 flex-col items-center gap-1.5">
                      <span className="tnum text-[10px] text-faint">{h.expectedOrders}</span>
                      <motion.div
                        className="w-full rounded-t-md"
                        style={{
                          background: h.hour === forecast.forecast.peakHour ? "var(--accent)" : "var(--overlay)",
                          // Confidence is shown by opacity AND stated in the legend,
                          // never by colour alone.
                          opacity: h.confidence === "high" ? 1 : h.confidence === "medium" ? 0.72 : 0.45,
                        }}
                        initial={{ height: 0 }}
                        animate={{ height: `${Math.max(3, (h.expectedOrders / max) * 132)}px` }}
                        // Stagger by position so the curve fills left to right; keying
                        // on the clock hour would animate 00-03 before 18-23.
                        transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1], delay: 0.03 * i }}
                      />
                      <span className="tnum text-[10.5px] text-faint">{hourLabel(h.hour)}</span>
                      <span className="tnum text-[10px]" style={{ color: "var(--accent)" }}>{h.couriersNeeded}</span>
                    </div>
                  );
                })}
              </div>
              <div className="mt-3 flex flex-wrap gap-x-5 gap-y-1 text-[11.5px] text-faint">
                <span>Oben: erwartete Bestellungen</span>
                <span>Unten violett: benötigte Kurier:innen</span>
                <span>Blasse Balken: wenig Verlaufsdaten</span>
              </div>
            </div>

            <div>
              <SectionTitle right={<span className="text-[12px] text-faint">Sollbestand für die kommende Nacht</span>}>
                Nachbestellen
              </SectionTitle>
              <div className="card divide-y" style={{ borderColor: "var(--hairline)" }}>
                {forecast.parLevels.map((p) => (
                  <div key={p.productId} className="flex items-center gap-4 p-3">
                    <span className="text-lg">{p.emoji}</span>
                    <span className="min-w-0 flex-1 truncate text-[13.5px]">{p.name}</span>
                    <span className="w-24 shrink-0"><Bar value={p.currentStock} max={Math.max(p.parLevel, 1)} tone={p.stockoutRisk === "high" ? "bad" : p.stockoutRisk === "low" ? "warn" : "good"} /></span>
                    <span className="tnum w-[104px] shrink-0 text-right text-[12px] text-faint">
                      {p.currentStock} / {p.parLevel} Soll
                    </span>
                    <span className="w-[92px] shrink-0 text-right">
                      {p.reorder > 0
                        ? <Pill tone={p.stockoutRisk === "high" ? "bad" : "warn"}>+{p.reorder} bestellen</Pill>
                        : <Pill tone="good">gedeckt</Pill>}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : null}

        {/* ------------------------------ ledger ------------------------- */}
        {tab === "ledger" ? (
          <div className="space-y-4">
            <div
              className="rounded-2xl p-4"
              style={{
                background: ledger.integrity.ok ? "oklch(0.78 0.152 158 / 0.10)" : "oklch(0.688 0.194 22 / 0.12)",
                border: `1px solid ${ledger.integrity.ok ? "oklch(0.78 0.152 158 / 0.3)" : "oklch(0.688 0.194 22 / 0.3)"}`,
              }}
            >
              <div className="text-[14px] font-semibold" style={{ color: ledger.integrity.ok ? "var(--good)" : "var(--bad)" }}>
                {ledger.integrity.ok
                  ? `Protokoll unversehrt · ${ledger.integrity.checked} Einträge geprüft`
                  : `Hash-Kette gebrochen bei Eintrag ${ledger.integrity.brokenAt}`}
              </div>
              <p className="mt-1 text-[13px] text-muted">
                Jeder Eintrag ist mit dem Hash des vorherigen verkettet. Eine nachträgliche
                Änderung bricht die Kette und wird hier sichtbar.
              </p>
            </div>
            <div className="card divide-y" style={{ borderColor: "var(--hairline)" }}>
              {ledger.events.map((e) => (
                <div key={e.seq} className="flex items-center gap-3 p-3 text-[12.5px]">
                  <span className="tnum w-9 shrink-0 text-faint">{e.seq}</span>
                  <span className="tnum w-12 shrink-0 text-faint">{clockOf(e.at)}</span>
                  <span className="w-[150px] shrink-0 font-semibold">{e.kind}</span>
                  <span className="min-w-0 flex-1 truncate font-mono text-[11px] text-faint">{JSON.stringify(e.payload)}</span>
                  <span className="shrink-0 font-mono text-[11px] text-faint">{e.hash}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
