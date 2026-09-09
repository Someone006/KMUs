/** Landing: start a Runde or join one. One decision per screen. */

import { Suspense, lazy, useEffect, useState } from "react";
import { motion } from "motion/react";
import { useNavigate } from "react-router-dom";
import { Button, ErrorNote } from "../components/ui";
import { api, ApiCallError, type ZoneView } from "../lib/api";
import { chfFull } from "../lib/format";

/* three.js is ~600kB and purely decorative, so it must never block the
   first paint of an ordering screen on mobile data. */
const NightSky = lazy(() =>
  import("../components/NightSky").then((m) => ({ default: m.NightSky })),
);

const fadeUp = {
  hidden: { opacity: 0, y: 14 },
  show: { opacity: 1, y: 0, transition: { duration: 0.32, ease: [0.22, 1, 0.36, 1] as const } },
};

const stagger = {
  hidden: {},
  show: { transition: { staggerChildren: 0.055, delayChildren: 0.08 } },
};

export function Customer() {
  const navigate = useNavigate();
  const [zones, setZones] = useState<ZoneView[]>([]);
  const [mode, setMode] = useState<"start" | "join">("start");
  const [zoneId, setZoneId] = useState("");
  const [address, setAddress] = useState("");
  const [name, setName] = useState("");
  const [code, setCode] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.zones()
      .then((r) => {
        setZones(r.zones);
        setZoneId(r.zones[0]?.id ?? "");
      })
      .catch((e: Error) => setError(e.message));
  }, []);

  const zone = zones.find((z) => z.id === zoneId);

  async function start() {
    setBusy(true);
    setError(null);
    try {
      const created = await api.createRunde({ zoneId, address, hostName: name });
      sessionStorage.setItem(`nocta:${created.runde.code}`, created.you.id);
      navigate(`/r/${created.runde.code}`);
    } catch (e) {
      setError(e instanceof ApiCallError ? e.api.message : (e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function join() {
    setBusy(true);
    setError(null);
    try {
      const joined = await api.join(code.trim().toUpperCase(), name);
      sessionStorage.setItem(`nocta:${joined.runde.code}`, joined.you.id);
      navigate(`/r/${joined.runde.code}`);
    } catch (e) {
      setError(e instanceof ApiCallError ? e.api.message : (e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  const canStart = zoneId && address.trim().length >= 4 && name.trim().length >= 1;
  const canJoin = code.trim().length === 6 && name.trim().length >= 1;

  return (
    <div className="relative min-h-screen">
      <div className="absolute inset-x-0 top-0 h-[62vh]">
        <Suspense fallback={null}>
          <NightSky />
        </Suspense>
        <div
          className="absolute inset-x-0 bottom-0 h-40"
          style={{ background: "linear-gradient(180deg, transparent, var(--ground))" }}
        />
      </div>

      <motion.main
        variants={stagger}
        initial="hidden"
        animate="show"
        className="relative mx-auto w-full max-w-[560px] px-5 pb-16 pt-[12vh]"
      >
        <motion.div variants={fadeUp} className="flex items-center gap-2.5">
          <span className="text-2xl">🌙</span>
          <span className="text-[15px] font-semibold tracking-[0.16em]">NOCTA</span>
        </motion.div>

        <motion.h1
          variants={fadeUp}
          className="mt-7 text-[40px] font-semibold leading-[1.05] tracking-[-0.03em] sm:text-[52px]"
        >
          Eine Runde.<br />
          Eine Lieferung.<br />
          <span style={{ color: "var(--accent)" }}>Jeder zahlt seins.</span>
        </motion.h1>

        <motion.p variants={fadeUp} className="mt-5 max-w-[46ch] text-[16px] leading-relaxed text-muted">
          Getränke und Snacks bis 04:00 in Zürich. Startet zusammen eine Runde —
          alle legen rein, was sie wollen, und die Liefergebühr teilt sich durch euch alle.
        </motion.p>

        {zone ? (
          <motion.div variants={fadeUp} className="mt-4 flex flex-wrap gap-x-5 gap-y-1 text-[13px] text-faint">
            {/* Separators are drawn between items, so a wrap never leaves
                a dangling middot at the end of a line. */}
            {[
              `Lieferung ${zone.baseFeeLabel}`,
              `Mindestbestellung ${zone.minBasketLabel}`,
              "Gratis ab CHF 60.00",
            ].map((t, i) => (
              <span key={t} className="flex items-center gap-x-5">
                {i > 0 ? <span aria-hidden="true">·</span> : null}
                {t}
              </span>
            ))}
          </motion.div>
        ) : null}

        <motion.div variants={fadeUp} className="card mt-9 overflow-hidden">
          <div className="flex border-b border-hairline">
            {(["start", "join"] as const).map((m) => (
              <button
                key={m}
                onClick={() => { setMode(m); setError(null); }}
                aria-pressed={mode === m}
                className="relative flex-1 py-4 text-[14px] font-semibold transition-colors"
                style={{ color: mode === m ? "var(--ink)" : "var(--faint)" }}
              >
                {m === "start" ? "Runde starten" : "Runde beitreten"}
                {mode === m ? (
                  <motion.div
                    layoutId="tab-underline"
                    className="absolute inset-x-0 bottom-0 h-[2px]"
                    style={{ background: "var(--accent)" }}
                  />
                ) : null}
              </button>
            ))}
          </div>

          <div className="space-y-4 p-5">
            <Field label="Dein Name">
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Nina"
                autoComplete="given-name"
                maxLength={40}
                className="input"
              />
            </Field>

            {mode === "start" ? (
              <>
                <Field label="Quartier">
                  <select value={zoneId} onChange={(e) => setZoneId(e.target.value)} className="input">
                    {zones.map((z) => (
                      <option key={z.id} value={z.id}>
                        {z.name} · ab {z.minBasketLabel}
                      </option>
                    ))}
                  </select>
                </Field>
                <Field label="Adresse">
                  <input
                    value={address}
                    onChange={(e) => setAddress(e.target.value)}
                    placeholder="Langstrasse 84, 8004 Zürich"
                    autoComplete="street-address"
                    maxLength={160}
                    className="input"
                  />
                </Field>
              </>
            ) : (
              <Field label="Runden-Code">
                <input
                  value={code}
                  onChange={(e) => setCode(e.target.value.toUpperCase().slice(0, 6))}
                  placeholder="K7M2QP"
                  inputMode="text"
                  autoCapitalize="characters"
                  className="input tnum text-center text-[26px] font-semibold tracking-[0.36em]"
                />
              </Field>
            )}

            {error ? <ErrorNote message={error} /> : null}

            <Button
              full
              disabled={busy || (mode === "start" ? !canStart : !canJoin)}
              onClick={mode === "start" ? start : join}
              data-testid={mode === "start" ? "start-runde" : "join-runde"}
            >
              {busy ? "Einen Moment…" : mode === "start" ? "Runde eröffnen" : "Beitreten"}
            </Button>
          </div>
        </motion.div>

        <motion.div variants={fadeUp} className="mt-7 grid grid-cols-3 gap-3">
          {[
            { n: "1", t: "Runde eröffnen", d: "Adresse rein, Code raus." },
            { n: "2", t: "Code teilen", d: "In den WG-Chat, in die Gruppe." },
            { n: "3", t: "Alle legen rein", d: "Live, gleichzeitig, jeder seins." },
          ].map((s) => (
            <div key={s.n} className="card p-4">
              <div className="tnum text-[13px] font-semibold" style={{ color: "var(--accent)" }}>{s.n}</div>
              <div className="mt-1.5 text-[13px] font-semibold leading-snug">{s.t}</div>
              <div className="mt-1 text-[12px] leading-snug text-faint">{s.d}</div>
            </div>
          ))}
        </motion.div>

        <motion.p variants={fadeUp} className="mt-8 text-[12px] leading-relaxed text-faint">
          Alle Preise inkl. 2.6% MWST. Grundpreise pro Liter bzw. Kilogramm sind bei
          jedem Artikel angegeben (PBV Art. 11). Allergene siehst du vor dem Bestellen.
          Kein Alkohol, kein Tabak.
        </motion.p>
      </motion.main>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      {/* Labels stay visible. A placeholder is not a label. */}
      <span className="label mb-1.5 block">{label}</span>
      {children}
    </label>
  );
}
