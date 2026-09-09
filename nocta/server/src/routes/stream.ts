/**
 * Server-sent events for live Rundes.
 *
 * A Runde is only compelling if everyone sees the same cart at the same time.
 * SSE over WebSockets because the traffic is one-directional, it survives
 * proxies, and the browser reconnects on its own.
 */

import type { Response } from "express";

interface Subscriber {
  id: string;
  res: Response;
}

const rooms = new Map<string, Set<Subscriber>>();

/** Heartbeat interval. Clients treat a gap of >15s as a dropped connection. */
const HEARTBEAT_MS = 10_000;

export function subscribe(rundeId: string, subscriberId: string, res: Response): () => void {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders?.();

  const sub: Subscriber = { id: subscriberId, res };
  if (!rooms.has(rundeId)) rooms.set(rundeId, new Set());
  rooms.get(rundeId)!.add(sub);

  res.write(`event: connected\ndata: ${JSON.stringify({ rundeId })}\n\n`);

  const heartbeat = setInterval(() => {
    // A comment frame keeps intermediaries from closing an idle connection.
    res.write(`: ping\n\n`);
  }, HEARTBEAT_MS);

  return () => {
    clearInterval(heartbeat);
    rooms.get(rundeId)?.delete(sub);
    if (rooms.get(rundeId)?.size === 0) rooms.delete(rundeId);
  };
}

export function publish(rundeId: string, event: string, data: unknown): void {
  const room = rooms.get(rundeId);
  if (!room) return;
  const frame = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const sub of room) {
    try {
      sub.res.write(frame);
    } catch (err) {
      // A dead socket must not take down the publisher for everyone else.
      console.warn("SSE write failed, dropping subscriber", sub.id, err);
      room.delete(sub);
    }
  }
}

export function roomSize(rundeId: string): number {
  return rooms.get(rundeId)?.size ?? 0;
}
