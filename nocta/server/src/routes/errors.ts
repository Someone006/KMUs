/**
 * Error taxonomy. Three classes, never conflated (see the `impeccable` skill):
 *   ValidationError - the caller's fault, message is safe to show
 *   DomainError     - a business rule refused it, carries a machine-readable code
 *   InternalError    - our fault, detail is logged, caller gets a correlation id
 */

import type { NextFunction, Request, Response } from "express";
import { randomUUID } from "node:crypto";

export class ValidationError extends Error {
  readonly status = 400;
  readonly code = "VALIDATION";
  constructor(message: string, readonly field?: string) {
    super(message);
    this.name = "ValidationError";
  }
}

export class DomainError extends Error {
  readonly status: number;
  constructor(readonly code: string, message: string, status = 409) {
    super(message);
    this.name = "DomainError";
    this.status = status;
  }
}

export class NotFoundError extends Error {
  readonly status = 404;
  readonly code = "NOT_FOUND";
  constructor(what: string) {
    super(`${what} not found`);
    this.name = "NotFoundError";
  }
}

/* ---------------------------- boundary parsing ---------------------------- */

export function str(body: unknown, field: string, opts: { max?: number; min?: number } = {}): string {
  const raw = (body as Record<string, unknown>)?.[field];
  if (typeof raw !== "string") throw new ValidationError(`"${field}" must be a string`, field);
  const value = raw.trim();
  const min = opts.min ?? 1;
  const max = opts.max ?? 200;
  if (value.length < min) throw new ValidationError(`"${field}" must be at least ${min} characters`, field);
  if (value.length > max) throw new ValidationError(`"${field}" must be at most ${max} characters`, field);
  return value;
}

export function optionalStr(body: unknown, field: string, fallback = "", max = 500): string {
  const raw = (body as Record<string, unknown>)?.[field];
  if (raw === undefined || raw === null || raw === "") return fallback;
  if (typeof raw !== "string") throw new ValidationError(`"${field}" must be a string`, field);
  const value = raw.trim();
  if (value.length > max) throw new ValidationError(`"${field}" must be at most ${max} characters`, field);
  return value;
}

export function int(body: unknown, field: string, opts: { min?: number; max?: number } = {}): number {
  const raw = (body as Record<string, unknown>)?.[field];
  const value = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isInteger(value)) throw new ValidationError(`"${field}" must be a whole number`, field);
  if (opts.min !== undefined && value < opts.min) {
    throw new ValidationError(`"${field}" must be at least ${opts.min}`, field);
  }
  if (opts.max !== undefined && value > opts.max) {
    throw new ValidationError(`"${field}" must be at most ${opts.max}`, field);
  }
  return value;
}

/* ------------------------------- middleware ------------------------------- */

/** Wrap an async handler so a rejected promise reaches the error middleware. */
export function wrap(
  fn: (req: Request, res: Response, next: NextFunction) => unknown | Promise<unknown>,
) {
  return (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

export function errorHandler(
  err: unknown,
  _req: Request,
  res: Response,
  _next: NextFunction,
): void {
  if (err instanceof ValidationError || err instanceof NotFoundError) {
    res.status(err.status).json({ error: { code: err.code, message: err.message } });
    return;
  }
  if (err instanceof DomainError) {
    res.status(err.status).json({ error: { code: err.code, message: err.message } });
    return;
  }
  const correlationId = randomUUID();
  console.error(`[${correlationId}]`, err);
  res.status(500).json({
    error: {
      code: "INTERNAL",
      message: "Something failed on our side.",
      correlationId,
    },
  });
}
