/**
 * The Runde room. Several people, one cart, one drop, live.
 *
 * Everything on this screen serves one of two jobs: show people what the
 * others are adding (so the group keeps ordering), or show how close the
 * basket is to the next threshold (so the basket grows).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { useParams } from "react-router-dom";
import { Button, Empty, ErrorNote, Pill, Skeleton } from "../components/ui";
import { api, ApiCallError, type CatalogItem, type RundeView } from "../lib/api";
import { ALLERGEN_DE, CATEGORY_DE, chf, chfFull, countdown, hueColor } from "../lib/format";

export function RundeRoom() {
  const { code = "" } = useParams();
  const [view, setView] = useState<RundeView | null>(null);
  const [catalog, setCatalog] = useState<CatalogItem[]>([]);
  const [me, setMe] = useState<string | null>(() => sessionStorage.getItem(`nocta:${code}`));
  const [joinName, setJoinName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(Date.now());
  const [category, setCategory] = useState<string>("all");
  const [placing, setPlacing] = useState(false);
  const [connected, setConnected] = useState(false);
  const lastVersion = useRef(0);

  /* ---- initial load ---- */
  useEffect(() => {
    Promise.all([api.getRunde(code), api.catalog()])
      .then(([r, c]) => {
        setView(r);
        lastVersion.current = r.runde.version;
        setCatalog(c.products);
      })
      .catch((e: Error) => setError(e.message));
  }, [code]);

  /* ---- live updates over SSE, with a polling fallback ---- */
  useEffect(() => {
    if (!view) return;
    const source = new EventSource(`/api/runde/${code}/stream`);
    const onUpdate = (e: MessageEvent) => {
      try {
        const next = JSON.parse(e.data) as RundeView;
        // Ignore an out-of-order frame rather than flickering backwards.
        if (next.runde.version >= lastVersion.current) {
          lastVersion.current = next.runde.version;
          setView(next);
        }
      } catch {
        /* a malformed frame must not kill the stream */
      }
    };
    source.addEventListener("connected", () => setConnected(true));
    source.addEventListener("joined", onUpdate);
    source.addEventListener("items", onUpdate);
    source.addEventListener("placed", onUpdate);
    source.onerror = () => setConnected(false);

    // Belt and braces: SSE can die silently behind a proxy.
    const poll = setInterval(() => {
      api.getRunde(code).then((r) => {
        if (r.runde.version >= lastVersion.current) {
          lastVersion.current = r.runde.version;
          setView(r);
        }
      }).catch(() => { /* transient; the next tick retries */ });
    }, 5000);

    return () => { source.close(); clearInterval(poll); };
  }, [code, view !== null]);

  useEffect(() => {
    const t = setInterval(() => setTick(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  const act = useCallback(async (fn: () => Promise<RundeView>) => {
    setError(null);
    try {
      const next = await fn();
      lastVersion.current = next.runde.version;
      setView(next);
    } catch (e) {
      setError(e instanceof ApiCallError ? e.api.message : (e as Error).message);
    }
  }, []);

  const categories = useMemo(() => {
    const seen = new Set(catalog.map((p) => p.category));
    return ["all", ...[...seen]];
  }, [catalog]);

  const shown = category === "all" ? catalog : catalog.filter((p) => p.category === category);

  if (error && !view) return <div className="mx-auto max-w-[560px] p-5"><ErrorNote message={error} /></div>;

  if (!view) {
    return (
      <div className="mx-auto max-w-[560px] space-y-3 p-5">
        <Skeleton h={92} /><Skeleton h={140} /><Skeleton h={280} />
      </div>
    );
  }

  const closed = view.runde.status !== "open";
  const myShare = view.shares.find((s) => s.participantId === me);
  const remaining = Math.max(0, view.runde.closesAt - tick);
  const urgent = remaining < 5 * 60_000 && !closed;

  /* ---- not yet a participant: join first ---- */
  if (!me) {
    return (
      <div className="mx-auto max-w-[560px] px-5 py-10">
        <div className="card p-6">
          <div className="label">Runde</div>
          <div className="tnum mt-1 text-[34px] font-semibold tracking-[0.22em]">{view.runde.code}</div>
          <p className="mt-3 text-[15px] text-muted">
            {view.shares.length} {view.shares.length === 1 ? "Person ist" : "Leute sind"} schon dabei,
            Lieferung an {view.runde.address}.
          </p>
          <div className="mt-5 space-y-3">
            <input
              className="input"
              value={joinName}
              placeholder="Dein Name"
              maxLength={40}
              onChange={(e) => setJoinName(e.target.value)}
            />
            {error ? <ErrorNote message={error} /> : null}
            <Button
              full
              disabled={joinName.trim().length < 1}
              data-testid="join-existing"
              onClick={async () => {
                try {
                  const joined = await api.join(code, joinName);
                  sessionStorage.setItem(`nocta:${code}`, joined.you.id);
                  setMe(joined.you.id);
                  setView(joined);
                } catch (e) {
                  setError(e instanceof ApiCallError ? e.api.message : (e as Error).message);
                }
              }}
            >
              Bei dieser Runde mitmachen
            </Button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-[860px] px-4 pb-[210px] pt-5 sm:px-5">
      {/* ---------------------------- header ---------------------------- */}
      <header className="card p-4">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="label">Runden-Code · teilen</div>
            <div className="tnum mt-0.5 text-[30px] font-semibold leading-none tracking-[0.2em]" data-testid="runde-code">
              {view.runde.code}
            </div>
            <div className="mt-2 truncate text-[13px] text-muted">{view.runde.address}</div>
          </div>
          <div className="shrink-0 text-right">
            <div className="label">{closed ? "Status" : "Schliesst in"}</div>
            <motion.div
              key={urgent ? "urgent" : "calm"}
              className="tnum mt-0.5 text-[26px] font-semibold leading-none"
              style={{ color: closed ? "var(--good)" : urgent ? "var(--warn)" : "var(--ink)" }}
              animate={urgent ? { opacity: [1, 0.55, 1] } : { opacity: 1 }}
              transition={urgent ? { duration: 1.6, repeat: Infinity } : { duration: 0.2 }}
              data-testid="countdown"
            >
              {closed ? "bestellt" : countdown(remaining)}
            </motion.div>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-2">
          {view.shares.map((s) => (
            <motion.div
              key={s.participantId}
              layout
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              className="flex items-center gap-1.5 rounded-full py-1 pl-1 pr-2.5"
              style={{ background: "var(--raised)" }}
            >
              <span
                className="grid h-6 w-6 place-items-center rounded-full text-[11px] font-bold text-[#140f1f]"
                style={{ background: hueColor(s.hue) }}
              >
                {s.name.slice(0, 1).toUpperCase()}
              </span>
              <span className="text-[12px] font-medium">{s.name}</span>
              {s.itemCount > 0 ? (
                <span className="tnum text-[11px] text-faint">{s.itemCount}</span>
              ) : null}
            </motion.div>
          ))}
          <span className="ml-auto flex items-center gap-1.5 text-[11px] text-faint">
            <span
              className="h-1.5 w-1.5 rounded-full"
              style={{ background: connected ? "var(--good)" : "var(--faint)" }}
            />
            {connected ? "live" : "verbinde…"}
          </span>
        </div>
      </header>

      {/* --------------------- the reason to share ---------------------- */}
      {view.sharerCount > 1 ? (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          className="mt-3 rounded-2xl p-4"
          style={{ background: "var(--accent-soft)", border: "1px solid var(--hairline)" }}
        >
          <div className="text-[14px] font-semibold" style={{ color: "var(--accent)" }}>
            Zu {view.sharerCount}t spart ihr {chfFull(view.saved)} Liefergebühr
          </div>
          <div className="mt-1 text-[12.5px] leading-relaxed text-muted">
            Einzeln bestellt zahlt jede Person {chfFull(view.soloFeeEach)}. Zusammen sind
            es {chfFull(view.feePerPerson)} pro Kopf — eine Fahrt, ein Halt.
          </div>
        </motion.div>
      ) : null}

      {error ? <div className="mt-3"><ErrorNote message={error} /></div> : null}

      {/* ---------------------------- catalogue -------------------------- */}
      <section className="mt-6">
        <div className="-mx-4 mb-3 flex gap-2 overflow-x-auto px-4 pb-1 sm:mx-0 sm:px-0">
          {categories.map((c) => (
            <button
              key={c}
              onClick={() => setCategory(c)}
              aria-pressed={category === c}
              className="shrink-0 rounded-full px-3.5 py-2 text-[13px] font-semibold transition-colors"
              style={{
                background: category === c ? "var(--accent)" : "var(--surface)",
                color: category === c ? "#140f1f" : "var(--muted)",
                border: "1px solid var(--hairline)",
              }}
            >
              {c === "all" ? "Alles" : CATEGORY_DE[c] ?? c}
            </button>
          ))}
        </div>

        <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-3">
          {shown.map((p) => {
            const mine = myShare?.lines.find((l) => l.productId === p.id);
            return (
              <motion.div key={p.id} layout className="card flex flex-col p-3">
                <div className="flex items-start justify-between gap-2">
                  <span className="text-[26px] leading-none">{p.emoji}</span>
                  {p.chilled ? <Pill>gekühlt</Pill> : null}
                </div>
                <div className="mt-2 text-[13.5px] font-semibold leading-tight">{p.name}</div>
                <div className="mt-0.5 text-[11.5px] text-faint">{p.pack}</div>
                <div className="mt-auto pt-2.5">
                  <div className="tnum text-[15px] font-semibold">{p.priceLabel}</div>
                  {/* PBV Art. 11 - the unit price is a legal duty, not a nicety. */}
                  {p.unitPrice ? (
                    <div className="tnum text-[11px] text-faint">{p.unitPrice}</div>
                  ) : null}
                  {p.allergens.length > 0 ? (
                    <div className="mt-1 text-[10.5px] leading-tight text-faint">
                      Enthält: {p.allergens.map((a) => ALLERGEN_DE[a] ?? a).join(", ")}
                    </div>
                  ) : null}
                </div>

                {closed ? null : mine ? (
                  <div className="mt-2.5 flex items-center gap-2">
                    <StepButton label={`${p.name} weniger`} onClick={() => act(() => api.setQty(code, mine.itemId, mine.qty - 1))}>−</StepButton>
                    <span className="tnum flex-1 text-center text-[15px] font-semibold">{mine.qty}</span>
                    <StepButton label={`${p.name} mehr`} onClick={() => act(() => api.setQty(code, mine.itemId, mine.qty + 1))}>+</StepButton>
                  </div>
                ) : (
                  <button
                    disabled={!p.inStock}
                    onClick={() => act(() => api.addItem(code, me, p.id))}
                    data-testid={`add-${p.sku}`}
                    className="mt-2.5 min-h-[40px] w-full rounded-xl text-[13px] font-semibold transition-colors disabled:opacity-40"
                    style={{ background: "var(--raised)", border: "1px solid var(--hairline)" }}
                  >
                    {p.inStock ? "Dazu" : "Aus"}
                  </button>
                )}
              </motion.div>
            );
          })}
        </div>
      </section>

      {/* --------------------------- who ordered what -------------------- */}
      <section className="mt-7">
        <h2 className="mb-3 text-[15px] font-semibold tracking-tight">Wer was drin hat</h2>
        {view.itemCount === 0 ? (
          <Empty icon="🛒" title="Noch nichts drin" hint="Leg als Erste:r was rein — die anderen sehen es sofort." />
        ) : (
          <div className="space-y-2.5">
            <AnimatePresence mode="popLayout">
              {view.shares.filter((s) => s.lines.length > 0).map((s) => (
                <motion.div
                  key={s.participantId}
                  layout
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -6 }}
                  transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
                  className="card p-3.5"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <span className="h-2.5 w-2.5 rounded-full" style={{ background: hueColor(s.hue) }} />
                      <span className="text-[14px] font-semibold">
                        {s.name}{s.participantId === me ? " (du)" : ""}
                      </span>
                      {s.isHost ? <Pill tone="accent">Gastgeber:in</Pill> : null}
                    </div>
                    <span className="tnum text-[14px] font-semibold" data-testid="share-total">{chfFull(s.total)}</span>
                  </div>
                  <div className="mt-2 space-y-1">
                    {s.lines.map((l) => (
                      <div key={l.itemId} className="flex items-center justify-between text-[13px] text-muted">
                        <span className="truncate">{l.emoji} {l.qty} × {l.name}</span>
                        <span className="tnum shrink-0 pl-3">{chf(l.lineTotal)}</span>
                      </div>
                    ))}
                    <div className="flex items-center justify-between pt-1 text-[12px] text-faint">
                      <span>Anteil Lieferung</span>
                      <span className="tnum">{chf(s.feeShare)}</span>
                    </div>
                  </div>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        )}
      </section>

      {/* ------------------------------ checkout ------------------------- */}
      <div
        className="fixed inset-x-0 bottom-0 z-20 border-t border-hairline px-4 pb-[max(16px,env(safe-area-inset-bottom))] pt-3.5"
        style={{ background: "oklch(0.196 0.021 292 / 0.94)", backdropFilter: "blur(16px)" }}
      >
        <div className="mx-auto max-w-[860px]">
          {view.suggestion && !closed ? (
            <motion.button
              layout
              onClick={() => act(() => api.addItem(code, me, view.suggestion!.id))}
              className="mb-3 flex w-full items-center gap-3 rounded-xl p-2.5 text-left"
              style={{ background: "var(--accent-soft)" }}
            >
              <span className="text-xl">{view.suggestion.emoji}</span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[13px] font-semibold">{view.suggestion.name}</span>
                <span className="block text-[11.5px] text-muted">{view.suggestion.reason}</span>
              </span>
              <span className="shrink-0 text-[13px] font-bold" style={{ color: "var(--accent)" }}>+</span>
            </motion.button>
          ) : null}

          <div className="mb-2 flex items-baseline justify-between text-[13px]">
            <span className="text-muted">
              Waren {chfFull(view.totals.goods)} · Lieferung{" "}
              {view.totals.deliveryFee === 0 ? "gratis" : chfFull(view.totals.deliveryFee)}
            </span>
            <span className="tnum text-[20px] font-semibold" data-testid="payable">
              {chfFull(view.totals.payable)}
            </span>
          </div>

          {myShare ? (
            <div className="mb-2.5 text-[12px] text-faint">
              Dein Anteil: <span className="tnum font-semibold text-muted">{chfFull(myShare.total)}</span>
              {view.totals.toFreeDelivery > 0
                ? ` · noch ${chfFull(view.totals.toFreeDelivery)} bis Gratislieferung`
                : ""}
            </div>
          ) : null}

          {closed ? (
            <div className="rounded-xl p-3 text-center text-[14px] font-semibold" style={{ background: "var(--raised)", color: "var(--good)" }}>
              Bestellt — unterwegs zu euch 🛵
            </div>
          ) : (
            <Button
              full
              data-testid="place-order"
              disabled={placing || view.itemCount === 0 || !view.totals.meetsMinimum}
              onClick={async () => {
                setPlacing(true);
                await act(() => api.place(code));
                setPlacing(false);
              }}
            >
              {view.itemCount === 0
                ? "Noch nichts drin"
                : !view.totals.meetsMinimum
                  ? `Noch ${chfFull(view.totals.toMinimum)} bis Mindestbestellung`
                  : placing
                    ? "Wird bestellt…"
                    : `Runde abschliessen · ${chfFull(view.totals.payable)}`}
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}

function StepButton({
  children, onClick, label,
}: { children: React.ReactNode; onClick: () => void; label: string }) {
  return (
    <motion.button
      whileTap={{ scale: 0.92 }}
      onClick={onClick}
      aria-label={label}
      className="grid h-10 w-10 place-items-center rounded-xl text-[17px] font-semibold"
      style={{ background: "var(--raised)", border: "1px solid var(--hairline)" }}
    >
      {children}
    </motion.button>
  );
}
