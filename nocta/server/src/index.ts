/**
 * NOCTA API server.
 *
 * Thin imperative shell: validate at the boundary, call a pure engine,
 * persist, broadcast. No business rule lives in this file.
 */

import express from "express";
import cors from "cors";
import { errorHandler } from "./routes/errors.js";
import { rundeRouter } from "./routes/runde.js";
import { opsRouter } from "./routes/ops.js";
import { courierRouter } from "./routes/courier.js";
import { getDb, verifyLedger } from "./db/index.js";

const PORT = Number(process.env.PORT ?? 8787);

const app = express();
app.use(cors());
app.use(express.json({ limit: "128kb" }));

app.get("/api/health", (_req, res) => {
  const db = getDb();
  const products = db.prepare("SELECT COUNT(*) AS n FROM products").get() as { n: number };
  res.json({
    ok: true,
    products: products.n,
    ledger: verifyLedger(),
    now: Date.now(),
  });
});

app.use("/api", rundeRouter);
app.use("/api/ops", opsRouter);
app.use("/api", courierRouter);

app.use(errorHandler);

// Fail loudly on startup rather than serving a half-initialised database.
try {
  getDb();
} catch (err) {
  console.error("Database initialisation failed:", err);
  process.exit(1);
}

app.listen(PORT, () => {
  console.log(`NOCTA API listening on http://localhost:${PORT}`);
});
