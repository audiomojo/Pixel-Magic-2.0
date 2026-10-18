import { afterAll, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import type pg from 'pg';
import { withTx } from '../../src/db.js';
import { logger } from '../../src/logger.js';
import { startPg } from '../helpers/pg.js';

let pool: pg.Pool;
let truncate: () => Promise<void>;
let stop: () => Promise<void>;

beforeAll(async () => {
  ({ pool, truncate, stop } = await startPg());
}, 120_000);
afterAll(async () => {
  await stop();
});
beforeEach(async () => {
  await truncate();
});

async function rows<T extends pg.QueryResultRow>(
  sql: string,
  params: unknown[] = [],
): Promise<T[]> {
  return (await pool.query<T>(sql, params)).rows;
}
async function one<T extends pg.QueryResultRow>(sql: string, params: unknown[] = []): Promise<T> {
  const r = (await rows<T>(sql, params))[0];
  if (!r) throw new Error('no row');
  return r;
}

const ins = `INSERT INTO files(rel_path, original_name, size_bytes, mtime_ns, state) VALUES ('a.jpg','a.jpg',1,1,$1)`;

test('partial unique allows re-drop after final', async () => {
  await pool.query(ins, ['INGESTED']);
  await pool.query(ins, ['DISCOVERED']); // earlier row is final
  await expect(pool.query(ins, ['CLAIMED'])).rejects.toMatchObject({ code: '23505' });
});
test('state CHECK rejects unknown', async () => {
  await expect(pool.query(ins, ['NOPE'])).rejects.toMatchObject({ code: '23514' });
});
test('bigint holds signed phash and parses as BigInt', async () => {
  await pool.query(
    `INSERT INTO contents(sha256, ext, kind, mime, size_bytes, state, phash) VALUES ($1,'jpg','still','image/jpeg',1,'HASHED',$2)`,
    ['0'.repeat(64), (-(2n ** 63n)).toString()],
  );
  expect((await one<{ phash: bigint }>('SELECT phash FROM contents')).phash).toBe(-(2n ** 63n));
});
test('singletons seeded', async () => {
  expect(
    (await one<{ version: bigint }>('SELECT version FROM cluster_state WHERE id=1')).version,
  ).toBe(0n);
});
test('updated_at trigger bumps', async () => {
  const r = await one<{ id: bigint; updated_at: Date }>(`${ins} RETURNING id, updated_at`, [
    'DISCOVERED',
  ]);
  await pool.query('SELECT pg_sleep(0.01)');
  const u = await one<{ updated_at: Date }>(
    `UPDATE files SET state = 'CLAIMED' WHERE id = $1 RETURNING updated_at`,
    [r.id],
  );
  expect(u.updated_at.getTime()).toBeGreaterThan(r.updated_at.getTime());
});
test('updated_at uses clock time, not transaction start', async () => {
  const [start, stamped] = await withTx(pool, async (c) => {
    const t0 = (await c.query<{ t: Date }>('SELECT now() AS t')).rows[0]?.t ?? new Date(0);
    await c.query('SELECT pg_sleep(0.05)');
    const r = await c.query<{ updated_at: Date }>(`${ins} RETURNING updated_at`, ['DISCOVERED']);
    return [t0, r.rows[0]?.updated_at ?? new Date(0)] as const;
  });
  expect(stamped.getTime() - start.getTime()).toBeGreaterThanOrEqual(40);
});
test('withTx rolls back on error', async () => {
  await expect(
    withTx(pool, async (c) => {
      await c.query(ins, ['DISCOVERED']);
      throw new Error('boom');
    }),
  ).rejects.toThrow('boom');
  expect((await one<{ n: number }>('SELECT count(*)::int AS n FROM files')).n).toBe(0);
});
test('staging_builds has cluster_version', async () => {
  await pool.query(
    `INSERT INTO staging_builds(gen, watermark, built_at, entries) VALUES (1, now(), now(), 0)`,
  );
  expect(
    (await one<{ cluster_version: bigint }>('SELECT cluster_version FROM staging_builds'))
      .cluster_version,
  ).toBe(0n);
});
test('truncate keeps singletons', async () => {
  await pool.query('UPDATE cluster_state SET version = 7');
  await truncate();
  expect(
    (await one<{ version: bigint }>('SELECT version FROM cluster_state WHERE id=1')).version,
  ).toBe(0n);
  expect((await one<{ n: number }>('SELECT count(*)::int AS n FROM scan_stats WHERE id=1')).n).toBe(
    1,
  );
});

test('pool sessions bound hung statements and idle transactions', async () => {
  const s = await one<{ st: string; idle: string }>(
    `SELECT current_setting('statement_timeout') AS st,
            current_setting('idle_in_transaction_session_timeout') AS idle`,
  );
  expect(s).toEqual({ st: '1min', idle: '2min' });
});

test('an idle pooled client losing its backend is logged, not fatal', async () => {
  const warn = vi.spyOn(logger, 'warn').mockImplementation(() => undefined);
  try {
    const victim = await pool.connect();
    const killer = await pool.connect();
    const { pid } = (await victim.query<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
      .rows[0]!;
    victim.release(); // now idle in the pool, as during a Postgres restart
    await killer.query('SELECT pg_terminate_backend($1)', [pid]);
    killer.release();
    // Without a pool 'error' listener the idle client's socket error is an uncaught exception.
    await vi.waitFor(() => expect(warn).toHaveBeenCalled(), { timeout: 5000 });
    expect((await pool.query<{ one: number }>('SELECT 1 AS one')).rows[0]!.one).toBe(1);
  } finally {
    warn.mockRestore();
  }
});

test('similar-set re-pointing on a merge is indexed (migration 0003)', async () => {
  const r = await rows<{ indexdef: string }>(
    `SELECT indexdef FROM pg_indexes WHERE tablename = 'groups' AND indexname = 'groups_similar_set_id_idx'`,
  );
  expect(r.map((x) => x.indexdef)).toEqual([
    'CREATE INDEX groups_similar_set_id_idx ON public.groups USING btree (similar_set_id)',
  ]);
});

test('files records a removed duplicate: when, bytes, or a mismatch flag (migration 0004)', async () => {
  const r = await one<{
    copy_removed_at: Date | null;
    copy_reclaimed_bytes: bigint | null;
    copy_mismatch: boolean;
  }>(`${ins} RETURNING copy_removed_at, copy_reclaimed_bytes, copy_mismatch`, ['COPY']);
  expect(r).toEqual({ copy_removed_at: null, copy_reclaimed_bytes: null, copy_mismatch: false });
});

test('only a COPY row can be marked removed, and never while flagged a mismatch', async () => {
  const id = (await one<{ id: bigint }>(`${ins} RETURNING id`, ['INGESTED'])).id;
  await expect(
    pool.query('UPDATE files SET copy_removed_at = now() WHERE id = $1', [id]),
  ).rejects.toMatchObject({ code: '23514' });
  const c = (await one<{ id: bigint }>(`${ins} RETURNING id`, ['COPY'])).id;
  await expect(
    pool.query('UPDATE files SET copy_removed_at = now(), copy_mismatch = true WHERE id = $1', [c]),
  ).rejects.toMatchObject({ code: '23514' });
});

test('the duplicate sweep cursor is a singleton that truncate resets', async () => {
  await pool.query('UPDATE copy_sweep_state SET after_id = 42 WHERE id = 1');
  await truncate();
  expect(
    (await one<{ after_id: bigint }>('SELECT after_id FROM copy_sweep_state WHERE id = 1'))
      .after_id,
  ).toBe(0n);
  await expect(pool.query('INSERT INTO copy_sweep_state (id) VALUES (2)')).rejects.toMatchObject({
    code: '23514',
  });
});
