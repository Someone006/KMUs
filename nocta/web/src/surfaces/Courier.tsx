/**
 * The courier surface. Held one-handed, outdoors, at night, often walking.
 * Big targets, few words, and the pay for the night stated plainly — night
 * transparency is the cheapest retention tool a small fleet has.
 */

import { useEffect, useState } from "react";
import { motion } from "motion/react";
import { useParams, useNavigate } from "react-router-dom";
import { Button, Empty, ErrorNote, Pill, Skeleton } from "../components/ui";
import { api, type Courier as CourierType, type CourierView } from "../lib/api";

export function CourierPicker() {
  const [couriers, setCouriers] = useState<CourierType[]>([]);
  const navigate = useNavigate();

  useEffect(() => { api.couriers().then((r) => setCouriers(r.couriers)).catch(() => undefined); }, []);

  return (
    <div className="mx-auto max-w-[480px] px-5 py-10">
      <div className="text-[13px] font-semibold tracking-[0.16em]">NOCTA · KURIER</div>
      <h1 className="mt-4 text-2xl font-semibold tracking-tight">Wer fährt heute Nacht?</h1>
      <div className="mt-6 space-y-2.5">
        {couriers.map((c) => (
          <button
            key={c.id}
            onClick={() => navigate(`/courier/${c.id}`)}
            className="card flex w-full items-center justify-between p-4 text-left"
          >
            <span>
              <span className="block text-[15px] font-semibold">{c.name}</span>
              <span className="mt-0.5 block text-[12px] text-faint">
                {c.nightsThisYear} Nächte dieses Jahr
              </span>
            </span>
            <Pill tone={c.nightsThisYear >= 25 ? "accent" : "warn"}>
              {c.nightsThisYear >= 25 ? "10% Zeitzuschlag" : "25% Lohnzuschlag"}
            </Pill>
          </button>
        ))}
      </div>
    </div>
  );
}

export function Courier() {
  const { id = "" } = useParams();
  const [view, setView] = useState<CourierView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<Set<string>>(new Set());

  const load = () => api.courier(id).then(setView).catch((e: Error) => setError(e.message));
  useEffect(() => { load(); const t = setInterval(load, 15_000); return () => clearInterval(t); }, [id]);

  if (error) return <div className="mx-auto max-w-[480px] p-5"><ErrorNote message={error} onRetry={load} /></div>;
  if (!view) return <div className="mx-auto max-w-[480px] space-y-3 p-5"><Skeleton h={150} /><Skeleton h={220} /></div>;

  return (
    <div className="mx-auto max-w-[480px] px-4 pb-12 pt-5">
      <header className="card p-4">
        <div className="flex items-start justify-between">
          <div>
            <div className="label">Schicht</div>
            <div className="mt-0.5 text-[19px] font-semibold">{view.courier.name}</div>
            <div className="tnum mt-1 text-[13px] text-muted">{view.shift?.window ?? "keine Schicht geplant"}</div>
          </div>
          {view.shift ? (
            <Pill tone={view.shift.compensation === "wage_supplement_25" ? "warn" : "accent"}>
              {view.shift.compensationLabel}
            </Pill>
          ) : null}
        </div>

        {view.shift ? (
          <div className="mt-4 grid grid-cols-3 gap-3">
            <div>
              <div className="label">Nachtstunden</div>
              <div className="tnum mt-0.5 text-[17px] font-semibold">{view.shift.nightHours} h</div>
            </div>
            <div>
              <div className="label">Verdienst</div>
              <div className="tnum mt-0.5 text-[17px] font-semibold">{view.shift.earnings}</div>
            </div>
            <div>
              <div className="label">
                {view.shift.compensation === "time_compensation_10" ? "Zeitgutschrift" : "davon Zuschlag"}
              </div>
              <div className="tnum mt-0.5 text-[17px] font-semibold" style={{ color: "var(--accent)" }}>
                {view.shift.compensation === "time_compensation_10"
                  ? `${view.shift.timeCreditMinutes} min`
                  : view.shift.supplementLabel}
              </div>
            </div>
          </div>
        ) : null}

        {view.shift && view.shift.violations.length > 0 ? (
          <div className="mt-3 rounded-xl p-3" style={{ background: "oklch(0.688 0.194 22 / 0.12)" }}>
            {view.shift.violations.map((v, i) => (
              <div key={i} className="text-[12.5px]" style={{ color: "var(--bad)" }}>
                {v.message} <span className="text-faint">({v.legalRef})</span>
              </div>
            ))}
          </div>
        ) : null}
      </header>

      <div className="mt-3 flex items-center gap-2">
        <Pill>{view.totals.stops} Stops</Pill>
        <Pill>{view.totals.distanceLabel}</Pill>
        <Pill>{view.totals.minutes} min</Pill>
        {view.totals.chilledRisk ? <Pill tone="bad">Kühlware zuerst</Pill> : null}
      </div>

      <div className="mt-5 space-y-2.5">
        {view.stops.length === 0 ? (
          <Empty icon="🛵" title="Keine Tour zugeteilt" hint="Sobald der Leitstand Touren zuteilt, erscheinen deine Stops hier." />
        ) : (
          view.stops.map((stop) => {
            const isDone = done.has(stop.code);
            return (
              <motion.div key={stop.code} layout className="card-raised p-4" style={{ opacity: isDone ? 0.5 : 1 }}>
                <div className="flex items-start gap-3">
                  <span
                    className="tnum grid h-9 w-9 shrink-0 place-items-center rounded-full text-[14px] font-bold text-[#140f1f]"
                    style={{ background: isDone ? "var(--good)" : "var(--accent)" }}
                  >
                    {isDone ? "✓" : stop.position}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="text-[15px] font-semibold leading-snug">{stop.address}</div>
                    <div className="tnum mt-1 text-[12.5px] text-faint">
                      {stop.code} · {stop.items} Artikel · in ~{stop.etaMinutes} min
                      {stop.chilled ? " · ❄ gekühlt" : ""}
                    </div>
                  </div>
                </div>
                {!isDone ? (
                  <Button
                    full
                    variant="ghost"
                    data-testid={`deliver-${stop.code}`}
                    onClick={async () => {
                      try {
                        await api.deliver(id, stop.code);
                        setDone((prev) => new Set(prev).add(stop.code));
                      } catch (e) {
                        setError((e as Error).message);
                      }
                    }}
                  >
                    Übergeben
                  </Button>
                ) : null}
              </motion.div>
            );
          })
        )}
      </div>
    </div>
  );
}
