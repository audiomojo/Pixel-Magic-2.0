import { afterEach, expect, test, vi } from 'vitest';
import type pg from 'pg';
import { createPool, withTx } from '../../src/db.js';
import { logger } from '../../src/logger.js';

afterEach(() => {
  vi.restoreAllMocks();
});

// Chaos (Task 13): pg attaches the whole client to an idle-client error, and pino serialised it
// (≈ 5 KB per idle connection on every Postgres restart).
test('an idle client error logs its code and message, not the pg client', async () => {
  const warn = vi.spyOn(logger, 'warn').mockImplementation(() => undefined);
  const pool = createPool('postgres://postgres@127.0.0.1:1/none');
  const err = Object.assign(new Error('terminating connection due to administrator command'), {
    code: '57P01',
    client: { connectionParameters: { host: '127.0.0.1' }, _queryQueue: [] },
  });
  pool.emit('error', err, {});
  expect(warn).toHaveBeenCalledWith(
    { err: { code: '57P01', message: 'terminating connection due to administrator command' } },
    'idle postgres client error',
  );
  await pool.end();
});

test('a client whose ROLLBACK failed is discarded, never returned to the pool mid-transaction', async () => {
  const released: unknown[] = [];
  const client = {
    query: (sql: string) =>
      sql === 'ROLLBACK' ? Promise.reject(new Error('connection lost')) : Promise.resolve({}),
    release: (err?: unknown) => released.push(err),
  };
  const pool = { connect: () => Promise.resolve(client) } as unknown as pg.Pool;
  await expect(
    withTx(pool, () => {
      throw new Error('boom');
    }),
  ).rejects.toThrow('boom');
  expect(released).toEqual([expect.objectContaining({ message: 'connection lost' })]);
  // A non-Error rejection still discards the client.
  released.length = 0;
  const odd = {
    ...client,
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- the point: a non-Error rejection
    query: (sql: string) => (sql === 'ROLLBACK' ? Promise.reject('gone') : Promise.resolve({})),
  };
  const pool3 = { connect: () => Promise.resolve(odd) } as unknown as pg.Pool;
  await expect(withTx(pool3, () => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
  expect(released).toEqual([expect.objectContaining({ message: 'gone' })]);
  // A clean rollback hands the client back for reuse.
  const ok = { ...client, query: () => Promise.resolve({}) };
  released.length = 0;
  const pool2 = { connect: () => Promise.resolve(ok) } as unknown as pg.Pool;
  await expect(withTx(pool2, () => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
  expect(released).toEqual([undefined]);
});
