import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { loadHarvestSettings } from '../../src/harvest/config.js';
import { createRun } from '../../src/harvest/runs.js';
import { registerSources } from '../../src/harvest/sources.js';
import { STALLED_REASON } from '../../src/harvest/constants.js';
import { checkGate } from '../../src/harvest/throttle.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(() => pgh.truncate());

const NOW = new Date('2026-10-19T12:00:00Z');
const minutesAgo = (m: number) => new Date(NOW.getTime() - m * 60_000);

/** 1000 blocks of 1 byte-block unit each: `used` blocks in use. */
const probe = (used: bigint, blocks = 1000n, bsize = 1n) => ({
  mountOutput: () => Promise.resolve(''),
  diskutilInfo: () => Promise.resolve(null),
  statfs: () => Promise.resolve({ blocks, bavail: blocks - used, bsize }),
});

function deps(used: bigint, env: Record<string, string> = {}, bsize = 1n) {
  return {
    pool: pgh.pool,
    settings: loadHarvestSettings({ DATA_DIR: '/tmp/x', DATABASE_URL: pgh.url, ...env }, '/tmp'),
    sys: probe(used, 1000n, bsize),
  };
}

async function newRun(): Promise<string> {
  const [s] = await registerSources(pgh.pool, [
    { identity: 'local:U-1', labelBase: 'Drive', kind: 'local', mountPath: '/Volumes/D' },
  ]);
  return createRun(pgh.pool, [{ sourceId: s!.id, relPath: '' }], false);
}

const setPending = (files: number, bytes = 0) =>
  pgh.pool.query('UPDATE scan_stats SET pending_files = $1, pending_bytes = $2 WHERE id = 1', [
    files,
    bytes,
  ]);

async function pause(runId: string, reason: string, since: Date) {
  await pgh.pool.query(
    'UPDATE harvest_runs SET paused_reason = $2, paused_since = $3 WHERE id = $1',
    [runId, reason, since],
  );
}

describe('disk', () => {
  test('open under the limit', async () => {
    expect(await checkGate(deps(790n), await newRun(), 10n, NOW)).toEqual({ open: true });
  });

  test('a file that tips 79 % over 80 % shuts the gate', async () => {
    const g = await checkGate(deps(790n), await newRun(), 14n, NOW);
    expect(g).toEqual({ open: false, reason: 'disk 80.4 %' });
  });

  test('exactly at the limit is open; the file size rounds up to blocks', async () => {
    expect(await checkGate(deps(790n), await newRun(), 10n, NOW)).toEqual({ open: true });
    // 4096-byte blocks: 5000 bytes = 2 blocks
    const g = await checkGate(deps(799n, {}, 4096n), await newRun(), 5000n, NOW);
    expect(g).toMatchObject({ open: false, reason: 'disk 80.1 %' });
  });

  test('zero blocks never shuts the gate', async () => {
    const d = { ...deps(0n), sys: probe(0n, 0n) };
    expect(await checkGate(d, await newRun(), 1n, NOW)).toEqual({ open: true });
  });
});

describe('backlog', () => {
  test('pending_files at the cap shuts with a thousands separator', async () => {
    await setPending(20000);
    expect(await checkGate(deps(0n), await newRun(), 1n, NOW)).toEqual({
      open: false,
      reason: 'backlog 20,000',
    });
  });

  test('below the cap is open', async () => {
    await setPending(19999);
    expect(await checkGate(deps(0n), await newRun(), 1n, NOW)).toEqual({ open: true });
  });

  test('unlinked SENT items in the last hour count when scan_stats lags', async () => {
    const runId = await newRun();
    const { rows } = await pgh.pool.query<{ id: string }>('SELECT id FROM harvest_sources');
    const src = rows[0]!.id;
    const add = async (i: number, sentAt: Date, fileId: number | null) =>
      pgh.pool.query(
        `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state, sent_at, file_id)
         VALUES ($1, $2, $2, 10, 1, 'photo', 'SENT', $3, $4)`,
        [src, `f${i}`, sentAt, fileId],
      );
    for (let i = 0; i < 20; i++) await add(i, minutesAgo(5), null);
    await add(100, minutesAgo(5), 7); // linked: not counted
    await add(101, minutesAgo(90), null); // too old: not counted
    const d = deps(0n, { HARVEST_MAX_PENDING_FILES: '20' });
    expect(await checkGate(d, runId, 1n, NOW)).toMatchObject({ open: false, reason: 'backlog 20' });
    const d21 = deps(0n, { HARVEST_MAX_PENDING_FILES: '21' });
    expect(await checkGate(d21, runId, 1n, NOW)).toEqual({ open: true });
  });

  test('the byte cap applies only when set', async () => {
    await setPending(1, 1_200_000_000);
    expect(await checkGate(deps(0n), await newRun(), 1n, NOW)).toEqual({ open: true });
    const d = deps(0n, { HARVEST_MAX_PENDING_BYTES: '1000000000' });
    expect(await checkGate(d, await newRun(), 1n, NOW)).toEqual({
      open: false,
      reason: 'backlog 1.2 GB',
    });
  });

  test('byte cap formats smaller sizes', async () => {
    const run = await newRun();
    const d = deps(0n, { HARVEST_MAX_PENDING_BYTES: '1' });
    await setPending(1, 512);
    expect(await checkGate(d, run, 1n, NOW)).toMatchObject({ reason: 'backlog 512 B' });
    await setPending(1, 2_500);
    expect(await checkGate(d, run, 1n, NOW)).toMatchObject({ reason: 'backlog 2.5 KB' });
    await setPending(1, 3_400_000);
    expect(await checkGate(d, run, 1n, NOW)).toMatchObject({ reason: 'backlog 3.4 MB' });
  });

  test('disk wins the reason when both are shut', async () => {
    await setPending(20000);
    const g = await checkGate(deps(900n), await newRun(), 1n, NOW);
    expect(g).toMatchObject({ open: false, reason: 'disk 90.1 %' });
  });
});

describe('stall', () => {
  const shut = () => deps(900n);
  const STALLED = { open: false, reason: STALLED_REASON };

  test('not stalled when the run has not been paused for the disk', async () => {
    const run = await newRun();
    expect(await checkGate(shut(), run, 1n, NOW)).not.toEqual(STALLED);
    await pause(run, 'paused by you', minutesAgo(60));
    expect(await checkGate(shut(), run, 1n, NOW)).not.toEqual(STALLED);
  });

  test('not stalled before 10 minutes', async () => {
    const run = await newRun();
    await pause(run, 'disk 90.1 %', minutesAgo(9));
    expect(await checkGate(shut(), run, 1n, NOW)).not.toEqual(STALLED);
  });

  test('stalled after 10 minutes with nothing in flight', async () => {
    const run = await newRun();
    await pause(run, 'disk 90.1 %', minutesAgo(10));
    expect(await checkGate(shut(), run, 1n, NOW)).toEqual({
      open: false,
      reason: STALLED_REASON,
    });
    await pause(run, 'stalled: the vault is filling the disk', minutesAgo(30));
    expect(await checkGate(shut(), run, 1n, NOW)).toEqual(STALLED);
  });

  test('a paused_reason with no paused_since is not stalled', async () => {
    const run = await newRun();
    await pgh.pool.query("UPDATE harvest_runs SET paused_reason = 'disk 90.1 %' WHERE id = $1", [
      run,
    ]);
    expect(await checkGate(shut(), run, 1n, NOW)).not.toEqual(STALLED);
  });

  test('a backlog reason is never stalled', async () => {
    const run = await newRun();
    await pause(run, 'backlog 20,000', minutesAgo(60));
    await setPending(20000);
    expect(await checkGate(deps(0n), run, 1n, NOW)).not.toEqual(STALLED);
  });

  const addFile = (state: string, extra = '') =>
    pgh.pool.query(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state ${extra ? ', ' + extra.split('=')[0] : ''})
       VALUES ('a', 'a', 1, 1, $1 ${extra ? ', ' + extra.split('=')[1] : ''})`,
      [state],
    );

  test.each([
    ['pending files', async () => void (await setPending(3))],
    ['a DISCOVERED file', async () => void (await addFile('DISCOVERED'))],
    ['a CLAIMED file', async () => void (await addFile('CLAIMED'))],
    ['an unreclaimed COPY', async () => void (await addFile('COPY'))],
  ])('%s prevents the stall', async (_n, arrange) => {
    const run = await newRun();
    await pause(run, 'disk 90.1 %', minutesAgo(60));
    await arrange();
    expect(await checkGate(shut(), run, 1n, NOW)).not.toEqual(STALLED);
  });

  test('reclaimed or mismatched COPY rows do not prevent the stall', async () => {
    const run = await newRun();
    await pause(run, 'disk 90.1 %', minutesAgo(60));
    await pgh.pool.query(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state, copy_removed_at)
       VALUES ('r', 'r', 1, 1, 'COPY', now()), ('m', 'm', 2, 2, 'COPY', NULL)`,
    );
    await pgh.pool.query("UPDATE files SET copy_mismatch = true WHERE rel_path = 'm'");
    await addFile('INGESTED');
    expect(await checkGate(shut(), run, 1n, NOW)).toEqual(STALLED);
  });

  test('harvester files still on their way prevent the stall', async () => {
    const run = await newRun();
    await pause(run, 'disk 90.1 %', minutesAgo(60));
    const { rows } = await pgh.pool.query<{ id: string }>('SELECT id FROM harvest_sources');
    await pgh.pool.query(
      `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state, sent_at)
       VALUES ($1, 'f', 'f', 10, 1, 'photo', 'SENT', $2)`,
      [rows[0]!.id, minutesAgo(1)],
    );
    expect(await checkGate(shut(), run, 1n, NOW)).not.toEqual(STALLED);
  });
});

test('defaults now to the clock', async () => {
  expect(await checkGate(deps(0n), await newRun(), 1n)).toEqual({ open: true });
});

test('the backlog queries can use the sent-unlinked index', async () => {
  const client = await pgh.pool.connect();
  try {
    await client.query('SET enable_seqscan = off');
    for (const sql of [
      `SELECT count(*) FROM harvest_items WHERE state = 'SENT' AND file_id IS NULL AND sent_at > $1`,
      `SELECT 1 FROM harvest_items WHERE state = 'SENT' AND file_id IS NULL AND sent_at > $1 LIMIT 1`,
    ]) {
      const { rows } = await client.query<{ 'QUERY PLAN': string }>(`EXPLAIN ${sql}`, [NOW]);
      expect(rows.map((r) => r['QUERY PLAN']).join('\n')).toContain(
        'harvest_items_sent_unlinked_idx',
      );
    }
  } finally {
    client.release();
  }
});
