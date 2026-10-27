// Shared pieces of the harvester's HTTP API (server.ts and picks.ts).
import type http from 'node:http';
import type { z } from 'zod';

/** A request failure with the status and message the client sees. */
export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export type Json = Record<string, unknown>;

export function send(res: http.ServerResponse, status: number, body: Json): void {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(text),
    'Cache-Control': 'no-store',
  });
  res.end(text);
}

export function parse<T extends z.ZodType>(schema: T, value: unknown): z.output<T> {
  const r = schema.safeParse(value);
  if (!r.success) {
    throw new HttpError(
      400,
      r.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`.replace(/^: /, '')).join('; '),
    );
  }
  return r.data;
}
