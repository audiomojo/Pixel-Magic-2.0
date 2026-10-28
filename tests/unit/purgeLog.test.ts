// The durable purge record `purged.csv` (spec §7): format, parse and the both-way reconcile.
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import {
  formatEvent,
  parseLog,
  purgeLogPath,
  reconcilePurgeLog,
  type PurgeEvent,
} from '../../src/purgeLog.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

const A = 'a'.repeat(64);
const B = 'b'.repeat(64);
const HEADER = 'action,sha256,size_bytes,at,names';
const log = { info: () => {}, warn: () => {} };
interface R {
  names: string[];
  unblocked_at: Date | null;
}
const ev = (
  action: 'purge' | 'unblock',
  sha: string,
  at: string,
  names = ['x.jpg'],
): PurgeEvent => ({
  action,
  sha256: sha,
  sizeBytes: 1234n,
  at: new Date(at),
  names,
});

let pgh: PgHandle;
let pool: pg.Pool;
let dir: string;
beforeAll(async () => {
  pgh = await startPg();
  pool = pgh.pool;
}, 120_000);
afterAll(() => pgh.stop());
beforeEach(async () => {
  await pgh.truncate();
  dir = await mkdtemp(join(tmpdir(), 'purgelog-'));
});

test('format and parse round-trip awkward names', () => {
  const e = ev('purge', A, '2026-10-28T10:00:00.123Z', [
    'a,b.jpg',
    'q"uote.jpg',
    'p|ipe',
    'new\nline',
    'été-日本.heic',
  ]);
  const line = formatEvent(e);
  const { events, bad } = parseLog(
    `${HEADER}\n${line}\n${formatEvent(ev('unblock', B, '2026-10-29T00:00:00.000Z'))}\n`,
  );
  expect(bad).toEqual([]);
  expect(events).toHaveLength(2);
  expect(events[0]).toEqual(e);
  expect(events[1]!.action).toBe('unblock');
});

test('a garbage line is reported by number, the rest kept', () => {
  const good = formatEvent(ev('purge', A, '2026-10-28T10:00:00.000Z', ['n\nm']));
  const { events, bad } = parseLog(
    `${HEADER}\nnot,a,line\n${good}\npurge,${A},xx,2026-10-28T10:00:00.000Z,[]\n`,
  );
  expect(events).toHaveLength(1);
  expect(bad).toEqual([2, 4]);
});

test('the reader keeps a raw newline inside a quoted field and counts lines', () => {
  const raw = `${HEADER}\npurge,${A},1,2026-10-28T10:00:00.000Z,"[\n""a""]"\r\nbroken\n`;
  const { events, bad } = parseLog(raw);
  expect(events[0]!.names).toEqual(['a']);
  expect(bad).toEqual([4]);
});

test('imports a file into an empty database: purge then unblock, unblock then purge', async () => {
  await writeFile(
    purgeLogPath(dir),
    [
      HEADER,
      formatEvent(ev('purge', A, '2026-10-28T10:00:00.000Z', ['one'])),
      formatEvent(ev('unblock', A, '2026-10-28T11:00:00.000Z')),
      formatEvent(ev('unblock', B, '2026-10-28T10:00:00.000Z')),
      formatEvent(ev('purge', B, '2026-10-28T11:00:00.000Z', ['two'])),
      '',
    ].join('\n'),
  );
  const r = await reconcilePurgeLog(pool, dir, log);
  expect(r).toEqual({ imported: 2, appended: 0, bad: 0 });
  const { rows } = await pool.query<R>(
    'SELECT sha256, names, unblocked_at FROM purged ORDER BY sha256',
  );
  expect(rows[0]!.unblocked_at?.toISOString()).toBe('2026-10-28T11:00:00.000Z');
  expect(rows[0]!.names).toEqual(['one']);
  expect(rows[1]!.unblocked_at).toBeNull();
  expect(rows[1]!.names).toEqual(['two']);
});

test('database rows missing from the file are appended once', async () => {
  await pool.query(
    `INSERT INTO purged (sha256, size_bytes, names, purged_at, unblocked_at) VALUES
     ($1, 5, '{a,"b,c"}', '2026-10-28T10:00:00.123456Z', NULL),
     ($2, 6, '{z}', '2026-10-28T10:00:00Z', '2026-10-28T12:00:00.987654Z')`,
    [A, B],
  );
  expect(await reconcilePurgeLog(pool, dir, log)).toEqual({ imported: 0, appended: 2, bad: 0 });
  const text = await readFile(purgeLogPath(dir), 'utf8');
  expect(text.startsWith(`${HEADER}\n`)).toBe(true);
  expect(text.trimEnd().split('\n')).toHaveLength(3);
  expect(text).toContain('unblock');
  expect(await reconcilePurgeLog(pool, dir, log)).toEqual({ imported: 0, appended: 0, bad: 0 });
  expect(await readFile(purgeLogPath(dir), 'utf8')).toBe(text);
});

test('nothing to write leaves no file', async () => {
  expect(await reconcilePurgeLog(pool, dir, log)).toEqual({ imported: 0, appended: 0, bad: 0 });
  await expect(readFile(purgeLogPath(dir))).rejects.toThrow();
});

test('a wiped database rebuilds from the file, and a newer database state is appended', async () => {
  await pool.query(
    `INSERT INTO purged (sha256, size_bytes, names, purged_at) VALUES ($1, 5, '{a}', '2026-10-28T10:00:00Z')`,
    [A],
  );
  await reconcilePurgeLog(pool, dir, log);
  await pool.query('UPDATE purged SET unblocked_at = $2 WHERE sha256 = $1', [
    A,
    '2026-10-28T12:00:00Z',
  ]);
  expect(await reconcilePurgeLog(pool, dir, log)).toMatchObject({ appended: 1 });
  await pool.query('TRUNCATE purged');
  expect(await reconcilePurgeLog(pool, dir, log)).toEqual({ imported: 1, appended: 0, bad: 0 });
  const { rows } = await pool.query<R>('SELECT unblocked_at FROM purged');
  expect(rows[0]!.unblocked_at?.toISOString()).toBe('2026-10-28T12:00:00.000Z');
});

test('two concurrent reconciles write no duplicate lines', async () => {
  await pool.query(
    `INSERT INTO purged (sha256, size_bytes, names, purged_at) VALUES ($1, 5, '{a}', now()), ($2, 5, '{b}', now())`,
    [A, B],
  );
  await Promise.all([reconcilePurgeLog(pool, dir, log), reconcilePurgeLog(pool, dir, log)]);
  const lines = (await readFile(purgeLogPath(dir), 'utf8')).trimEnd().split('\n');
  expect(lines).toHaveLength(3);
  await rm(dir, { recursive: true, force: true });
});
