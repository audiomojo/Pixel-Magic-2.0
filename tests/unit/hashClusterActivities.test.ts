import { createHash, randomBytes } from 'node:crypto';
import { copyFile, mkdir } from 'node:fs/promises';
import { dirname, extname, join } from 'node:path';
import pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { captureKey } from '../../src/activities/cluster.js';
import { createDecodePool, type WorkerPool } from '../../src/activities/hashing.js';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { classify, type Facts } from '../../src/classify.js';
import { DataLayout, sha256File, vaultRel } from '../../src/fsops.js';
import { Grouper } from '../../src/grouping.js';
import { HashIndex } from '../../src/hashIndex.js';
import { toSigned } from '../../src/hashing.js';
import type { DecodeResult } from '../../src/types.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { F, NOEXIF, T } from '../helpers/facts.js';
import { photo, truncatedJpeg } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

const mix = (i: number) => BigInt.asUintN(64, BigInt(i + 1) * 0x9e3779b97f4a7c15n);
const OK: DecodeResult = {
  facts: { width: 640, height: 480, phash: '12345', dhash: '18446744073709551615', mime: 'x' },
  quarantineReason: null,
};
const tsx = { execArgv: ['--import', 'tsx'] };

let pgh: PgHandle;
let pool: pg.Pool;
let deps: Deps;
let layout: DataLayout;
let close: () => Promise<void>;
let acts: ReturnType<typeof createActivities>;

beforeAll(async () => {
  pgh = await startPg();
  pool = pgh.pool;
  ({ deps, layout, close } = await testDeps(pgh));
  acts = createActivities(deps);
}, 120_000);
afterAll(async () => {
  await close();
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
  acts = createActivities(deps); // a fresh cluster cache per test
});

/** Writes `make(dir)`'s file into the vault; returns the decodeContent args. */
async function vaultFile(make: (dir: string) => Promise<string>): Promise<[string, string]> {
  const src = await make(await tmpDir());
  const [sha, ext] = [await sha256File(src), extname(src).slice(1)];
  const dest = join(layout.root, vaultRel(sha, ext));
  await mkdir(dirname(dest), { recursive: true });
  await copyFile(src, dest);
  return [sha, ext];
}

async function seedContent(o: { state: string; mime?: string }): Promise<string> {
  const sha = randomBytes(32).toString('hex');
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, mime, size_bytes, state) VALUES ($1, 'jpg', 'still', $2, 1, $3)`,
    [sha, o.mime ?? null, o.state],
  );
  return sha;
}

const hex = (name: string) => createHash('sha256').update(name).digest('hex');
/** The facts as stored: names become real SHA-256s. */
const stored = (f: Facts): Facts => ({ ...f, sha256: hex(f.sha256) });

/** Inserts HASHED rows from Facts (names mapped through `hex`); returns their SHA-256s. */
async function seedHashed(...facts: Facts[]): Promise<string[]> {
  for (const f of facts.map(stored))
    await pool.query(
      `INSERT INTO contents (sha256, ext, kind, mime, size_bytes, width, height, phash, dhash,
                             captured_at, subsec, camera, burst_uuid, state, hashed_at)
       VALUES ($1, 'jpg', 'still', $2, 1, $3, $4, $5, $6, $7, $8, $9, $10, 'HASHED', now())`,
      [
        f.sha256,
        f.mime,
        f.width,
        f.height,
        toSigned(f.phash),
        toSigned(f.dhash),
        f.captured && new Date(f.captured.s * 1000),
        f.captured?.sub ?? null,
        f.camera,
        f.burstUuid,
      ],
    );
  return facts.map((f) => hex(f.sha256));
}

/** clusterContent, asserting it clustered (null means the content was a retired DUPLICATE). */
async function cluster(sha: string): Promise<number> {
  const g = await runActivity(acts.clusterContent, [sha]);
  if (g === null) throw new Error(`clusterContent ${sha}: unexpectedly DUPLICATE`);
  return g;
}
async function similarSetOf(groupId: number): Promise<bigint | null> {
  const r = await pool.query<{ s: bigint | null }>(
    'SELECT similar_set_id AS s FROM groups WHERE id = $1',
    [groupId],
  );
  return r.rows[0]!.s;
}
async function count(table: string, where = 'true'): Promise<number> {
  const r = await pool.query<{ n: bigint }>(`SELECT count(*) AS n FROM ${table} WHERE ${where}`);
  return Number(r.rows[0]!.n);
}
async function stateOf(sha: string) {
  const r = await pool.query<{ state: string; last_error: string | null }>(
    'SELECT state, last_error FROM contents WHERE sha256 = $1',
    [sha],
  );
  return r.rows[0]!;
}

// ---------------------------------------------------------------- decodeContent

test('decodeContent: timeout is retryable Poison, and the hung child is killed', async () => {
  const hang = createDecodePool('tests/helpers/hangWorker.ts', 1, tsx);
  try {
    const a = createActivities({
      ...deps,
      decodePool: hang,
      settings: { ...deps.settings, decodeTimeoutS: 1 },
    });
    const args = await vaultFile((d) => photo(d, 'jpg'));
    await expect(runActivity(a.decodeContent, args)).rejects.toMatchObject({
      type: 'Poison',
      nonRetryable: false,
    });
    await expect.poll(() => hang.stats().totalWorkers, { timeout: 5000 }).toBe(0);
    // A later attempt gets a fresh child (and, this one hanging too, times out again).
    await expect(runActivity(a.decodeContent, args)).rejects.toMatchObject({ type: 'Poison' });
  } finally {
    await hang.terminate(true);
  }
}, 30_000);

test('decodeContent: a child killed by a signal (OOM) is transient, not Poison, and is replaced', async () => {
  const crashy = createDecodePool('tests/helpers/crashWorker.ts', 1, tsx);
  try {
    const a = createActivities({ ...deps, decodePool: crashy });
    await expect(runActivity(a.decodeContent, ['0'.repeat(64), 'crash'])).rejects.toMatchObject({
      type: 'DecodeKilled',
      nonRetryable: false,
    });
    const r = await runActivity(a.decodeContent, await vaultFile((d) => photo(d, 'jpg')));
    expect(r.quarantineReason).toBeNull();
    expect(r.facts?.width).toBe(640);
  } finally {
    await crashy.terminate(true);
  }
}, 30_000);

test('decodeContent needs no database', async () => {
  const offline = createActivities({
    ...deps,
    pool: new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/x' }),
  });
  const r = await runActivity(offline.decodeContent, await vaultFile((d) => photo(d, 'jpg')));
  expect(r.quarantineReason).toBeNull();
  expect(typeof r.facts?.phash).toBe('string');
  expect(BigInt(r.facts!.phash)).toBeGreaterThanOrEqual(0n);
  expect(r.facts).toMatchObject({ width: 640, height: 480, mime: 'image/jpeg' });
}, 30_000);

test(
  'decodeContent: corrupt file returns quarantine reason',
  async () =>
    expect(await runActivity(acts.decodeContent, await vaultFile(truncatedJpeg))).toEqual({
      facts: null,
      quarantineReason: 'truncated',
    }),
  30_000,
);

test('decodeContent: a missing vault file is an ordinary (transient) error, not Poison', async () => {
  const err: unknown = await runActivity(acts.decodeContent, ['f'.repeat(64), 'jpg']).catch(
    (e: unknown) => e,
  );
  expect(err).toBeInstanceOf(Error);
  expect((err as Error).message).toMatch(/ENOENT/);
  expect(err).not.toMatchObject({ type: 'Poison' });
}, 30_000);

test('decodeContent: a pool failure is Poison whatever the case of its message', async () => {
  const pool = {
    exec: () => ({ timeout: () => Promise.reject(new Error('Worker Terminating')) }),
  } as unknown as WorkerPool;
  const a = createActivities({ ...deps, decodePool: pool });
  await expect(runActivity(a.decodeContent, ['0'.repeat(64), 'jpg'])).rejects.toMatchObject({
    type: 'Poison',
  });
});

test('reopenFailed puts a FAILED content back to INGESTED and touches nothing else', async () => {
  const failed = await seedContent({ state: 'FAILED' });
  await pool.query(`UPDATE contents SET last_error = 'x' WHERE sha256 = $1`, [failed]);
  const hashed = await seedContent({ state: 'HASHED' });
  await runActivity(acts.reopenFailed, [failed]);
  await runActivity(acts.reopenFailed, [hashed]);
  await runActivity(acts.reopenFailed, [failed]); // a rerun is a no-op
  expect(await stateOf(failed)).toEqual({ state: 'INGESTED', last_error: null });
  expect(await stateOf(hashed)).toEqual({ state: 'HASHED', last_error: null });
});

// ---------------------------------------------------------------- recordHash / markFailed

test('recordHash is idempotent', async () => {
  const sha = await seedContent({ state: 'INGESTED' });
  expect(await runActivity(acts.recordHash, [sha, OK])).toBe('HASHED');
  expect(await runActivity(acts.recordHash, [sha, OK])).toBe('HASHED');
  const r = (
    await pool.query<{ phash: bigint; dhash: bigint; width: number; height: number }>(
      'SELECT phash, dhash, width, height FROM contents WHERE sha256 = $1',
      [sha],
    )
  ).rows[0]!;
  expect(r).toEqual({ phash: 12345n, dhash: -1n, width: 640, height: 480 });
});

test('recordHash quarantines', async () => {
  const sha = await seedContent({ state: 'INGESTED' });
  const q: DecodeResult = { facts: null, quarantineReason: 'truncated' };
  expect(await runActivity(acts.recordHash, [sha, q])).toBe('QUARANTINED');
  expect(await runActivity(acts.recordHash, [sha, OK])).toBe('QUARANTINED');
  const r = await pool.query(
    'SELECT state, quarantine_reason, phash FROM contents WHERE sha256 = $1',
    [sha],
  );
  expect(r.rows[0]).toEqual({ state: 'QUARANTINED', quarantine_reason: 'truncated', phash: null });
});

test('recordHash sets hashed_at and never writes mime', async () => {
  const sha = await seedContent({ state: 'INGESTED', mime: 'image/heic' });
  await runActivity(acts.recordHash, [sha, { ...OK, facts: { ...OK.facts!, mime: 'image/jpeg' } }]);
  const r = (
    await pool.query<{ hashed_at: Date | null; mime: string | null }>(
      'SELECT hashed_at, mime FROM contents WHERE sha256 = $1',
      [sha],
    )
  ).rows[0]!;
  expect(r.hashed_at).not.toBeNull();
  expect(r.mime).toBe('image/heic');
});

test('recordHash after clustering still returns HASHED', async () =>
  expect(await runActivity(acts.recordHash, [await seedContent({ state: 'CLUSTERED' }), OK])).toBe(
    'HASHED',
  ));

test('recordHash refuses a FAILED or missing row, and an empty result', async () => {
  const failed = await seedContent({ state: 'FAILED' });
  await expect(runActivity(acts.recordHash, [failed, OK])).rejects.toMatchObject({
    nonRetryable: true,
  });
  expect((await stateOf(failed)).state).toBe('FAILED');
  await expect(runActivity(acts.recordHash, ['e'.repeat(64), OK])).rejects.toThrow(/missing/);
  const sha = await seedContent({ state: 'INGESTED' });
  await expect(
    runActivity(acts.recordHash, [sha, { facts: null, quarantineReason: null }]),
  ).rejects.toThrow(/neither facts nor a quarantine reason/);
  expect((await stateOf(sha)).state).toBe('INGESTED');
});

test('poison path: decode timeouts, then markFailed sets FAILED', async () => {
  const [sha, ext] = await vaultFile((d) => photo(d, 'jpg'));
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state) VALUES ($1, $2, 'still', 1, 'INGESTED')`,
    [sha, ext],
  );
  const hang = createDecodePool('tests/helpers/hangWorker.ts', 1, tsx);
  let error: string;
  try {
    const a = createActivities({
      ...deps,
      decodePool: hang,
      settings: { ...deps.settings, decodeTimeoutS: 1 },
    });
    error = await runActivity(a.decodeContent, [sha, ext]).then(
      () => 'resolved',
      (e: unknown) => (e as Error).message,
    );
  } finally {
    await hang.terminate(true);
  }
  expect(error).toMatch(/timed out/i);
  await runActivity(acts.markFailed, [sha, error]);
  expect(await stateOf(sha)).toEqual({ state: 'FAILED', last_error: error });
  await runActivity(acts.markFailed, [sha, 'again']); // already handled: a no-op
  expect(await stateOf(sha)).toEqual({ state: 'FAILED', last_error: error });
}, 30_000);

test('markFailed never regresses a hashed row', async () => {
  const sha = await seedContent({ state: 'HASHED' });
  await runActivity(acts.markFailed, [sha, 'late']);
  expect(await stateOf(sha)).toEqual({ state: 'HASHED', last_error: null });
});

// ---------------------------------------------------------------- clusterContent

test('captureKey floors to the UTC second', () => {
  expect(captureKey(new Date('2021-10-03T21:02:11.900Z'), '482')).toEqual({
    s: 1_633_294_931,
    sub: '482',
  });
  expect(captureKey(new Date(-1500), null)).toEqual({ s: -2, sub: null });
  expect(captureKey(null, '482')).toBeNull();
});

test('cluster: copy joins, burst frame gets own group in same similar set', async () => {
  const [a, c, b] = await seedHashed(
    F('a'),
    F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
    F('b', { phash: 1n, captured: { s: 1_600_000_000, sub: '999' } }),
  );
  const [ga, gc, gb] = [await cluster(a!), await cluster(c!), await cluster(b!)];
  expect(typeof ga).toBe('number');
  expect(ga).toBe(gc);
  expect(gb).not.toBe(ga);
  expect(await similarSetOf(ga)).not.toBeNull();
  expect(await similarSetOf(ga)).toBe(await similarSetOf(gb));
  const r = await pool.query<{ state: string; clustered_at: Date | null; copy_group_id: bigint }>(
    'SELECT state, clustered_at, copy_group_id FROM contents ORDER BY sha256',
  );
  expect(r.rows.every((x) => x.state === 'CLUSTERED' && x.clustered_at !== null)).toBe(true);
  expect(await count('cluster_state', 'version = 3')).toBe(1);
});

test('cluster reloads when another worker wrote', async () => {
  const [z, a, c] = await seedHashed(
    F('z', { phash: mix(1), dhash: mix(2) }),
    F('a'),
    F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
  );
  await runActivity(acts.clusterContent, [z!]); // warm acts' cache
  const ga = await runActivity(createActivities(deps).clusterContent, [a!]); // another worker writes
  expect(await runActivity(acts.clusterContent, [c!])).toBe(ga); // acts' cache is stale and must reload
});

test('cluster is idempotent', async () => {
  const [a] = await seedHashed(F('a'));
  expect(await cluster(a!)).toBe(await cluster(a!));
  expect(await count('groups')).toBe(1);
  expect(await count('cluster_state', 'version = 1')).toBe(1);
});

test('concurrent clusterContent calls serialise', async () => {
  const [a, c] = await seedHashed(
    F('a'),
    F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
  );
  const [ga, gc] = await Promise.all([
    runActivity(createActivities(deps).clusterContent, [a!]),
    runActivity(createActivities(deps).clusterContent, [c!]),
  ]);
  expect(ga).toBe(gc);
  expect(await count('groups', `kind = 'copy' AND merged_into IS NULL`)).toBe(1);
});

test('the cluster transaction bounds idle-in-transaction just above the 15-minute activity timeout', async () => {
  const seen: string[] = [];
  // Reads the session setting on the cluster transaction's own connection, under the lock.
  const spy = new Proxy(pool, {
    get(target, prop, recv) {
      if (prop !== 'connect') return Reflect.get(target, prop, recv) as unknown;
      return async () => {
        const c = await target.connect();
        const query = c.query.bind(c) as (sql: string, v?: unknown[]) => Promise<pg.QueryResult>;
        return new Proxy(c, {
          get(ct, p, r) {
            if (p !== 'query') return Reflect.get(ct, p, r) as unknown;
            return async (sql: string, v?: unknown[]) => {
              const out = await query(sql, v);
              if (typeof sql === 'string' && sql.includes('pg_advisory_xact_lock')) {
                const s = await query(
                  `SELECT current_setting('idle_in_transaction_session_timeout') AS t`,
                );
                seen.push(String((s.rows[0] as { t: string }).t));
              }
              return out;
            };
          },
        });
      };
    },
  });
  const [a] = await seedHashed(F('a'));
  await runActivity(createActivities({ ...deps, pool: spy }).clusterContent, [a!]);
  expect(seen).toEqual(['16min']);
});

test('concurrent calls on one worker share its cache safely', async () => {
  const [a, c, d] = await seedHashed(
    F('a'),
    F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
    F('d', { phash: 1n, width: 1000, height: 750, mime: 'image/png' }),
  );
  const gs = await Promise.all([cluster(a!), cluster(c!), cluster(d!)]);
  expect(new Set(gs).size).toBe(1);
});

test('cluster uses thresholds from settings', async () => {
  const strict = createActivities({
    ...deps,
    settings: { ...deps.settings, lookAlikePhash: 0, lookAlikeDhash: 0 },
  });
  const [a, c] = await seedHashed(
    F('a'),
    F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
  );
  expect(await runActivity(strict.clusterContent, [a!])).not.toBe(
    await runActivity(strict.clusterContent, [c!]),
  );
});

test('cluster flags a refused copy union as copy_ambiguous', async () => {
  const [f1, f2, x] = await seedHashed(
    F('f1', { captured: { s: 1_600_000_000, sub: '1' } }),
    F('f2', { phash: 0b111n, captured: { s: 1_600_000_000, sub: '2' } }),
    F('x', { phash: 1n, ...NOEXIF }),
  );
  const [g1, g2, gx] = [await cluster(f1!), await cluster(f2!), await cluster(x!)];
  expect(g1).not.toBe(g2);
  expect([g1, g2]).toContain(gx);
  const r = await pool.query<{ sha256: string; copy_ambiguous: boolean }>(
    'SELECT sha256, copy_ambiguous FROM contents WHERE copy_ambiguous',
  );
  expect(r.rows.map((row) => row.sha256)).toEqual([x]);
});

test('a bridge that arrived first is flagged when the conflicting frame arrives', async () => {
  const [x, f1, f2] = await seedHashed(
    F('x', { phash: 1n, ...NOEXIF }),
    F('f1', { captured: { s: 1_600_000_000, sub: '1' } }),
    F('f2', { phash: 0b111n, captured: { s: 1_600_000_000, sub: '2' } }),
  );
  const [gx, g1, g2] = [await cluster(x!), await cluster(f1!), await cluster(f2!)];
  expect(g1).toBe(gx);
  expect(g2).not.toBe(g1);
  const r = await pool.query<{ sha256: string }>(
    'SELECT sha256 FROM contents WHERE copy_ambiguous ORDER BY sha256',
  );
  expect(r.rows.map((row) => row.sha256)).toEqual([x]);
});

test('a reloaded cluster cache still flags a copy of a bridge when the bridge is flagged', async () => {
  const [x, r, f1, f2] = await seedHashed(
    F('x', { phash: 1n, ...NOEXIF }),
    F('r', { phash: 0b1111001n, width: 2000, ...NOEXIF }), // tight to x only
    F('f1', { captured: { s: 1_600_000_000, sub: '1' } }),
    F('f2', { phash: 0b111n, captured: { s: 1_600_000_000, sub: '2' } }),
  );
  for (const sha of [x!, r!, f1!]) await cluster(sha);
  // Another worker (empty cache) clusters the conflicting frame: copy edges come from the index.
  await runActivity(createActivities(deps).clusterContent, [f2!]);
  const flagged = await pool.query<{ sha256: string }>(
    'SELECT sha256 FROM contents WHERE copy_ambiguous ORDER BY sha256',
  );
  expect(flagged.rows.map((row) => row.sha256)).toEqual([x!, r!].sort());
});

test('cluster refuses content that is not HASHED, without bumping the version', async () => {
  const sha = await seedContent({ state: 'INGESTED' });
  await expect(cluster(sha)).rejects.toMatchObject({ nonRetryable: true });
  await expect(cluster('d'.repeat(64))).rejects.toThrow(/missing/);
  expect(await count('cluster_state', 'version = 0')).toBe(1);
});

test('a failed cluster transaction leaves no trace and the next call recovers', async () => {
  const [a, c] = await seedHashed(
    F('a'),
    F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
  );
  await cluster(a!); // warm cache
  let boom = true;
  const flaky = createActivities({
    ...deps,
    checkpoint: (name) => {
      if (name === 'cluster:persisted' && boom) {
        boom = false;
        throw new Error('simulated crash before commit');
      }
    },
  });
  await runActivity(flaky.clusterContent, [a!]); // CLUSTERED: returns before the checkpoint
  await expect(runActivity(flaky.clusterContent, [c!])).rejects.toThrow(/simulated/);
  expect((await stateOf(c!)).state).toBe('HASHED');
  expect(await count('groups')).toBe(1);
  expect(await runActivity(flaky.clusterContent, [c!])).toBe(await cluster(a!));
});

// ---------------------------------------------------------------- order independence across a restart

/** Same pipeline as clusterContent, all in memory: index → classify → Grouper. */
function inMemory(facts: readonly Facts[]) {
  let id = 0;
  const g = new Grouper(() => ++id);
  const idx = new HashIndex();
  for (const f of facts) {
    g.add(
      f,
      idx.matches(f, T).map((m) => [m.sha256, classify(f, m, T)] as const),
    );
    idx.append([f]);
  }
  return { copy: g.copyPartition(), similar: g.similarPartition() };
}

/** Partitions straight from the stored rows, checking every stored id is a root. */
async function dbPartitions() {
  const rows = (
    await pool.query<{
      sha256: string;
      copy_group_id: bigint;
      copy_merged: bigint | null;
      similar_set_id: bigint | null;
      similar_merged: bigint | null;
    }>(
      `SELECT c.sha256, c.copy_group_id, g.merged_into AS copy_merged, g.similar_set_id,
              s.merged_into AS similar_merged
         FROM contents c JOIN groups g ON g.id = c.copy_group_id
         LEFT JOIN groups s ON s.id = g.similar_set_id
        WHERE c.state = 'CLUSTERED'`,
    )
  ).rows;
  for (const r of rows) {
    expect(r.copy_merged).toBeNull();
    expect(r.similar_merged).toBeNull();
  }
  const part = (key: (r: (typeof rows)[number]) => string) => {
    const m = new Map<string, string[]>();
    for (const r of rows) m.set(key(r), [...(m.get(key(r)) ?? []), r.sha256]);
    return new Set([...m.values()].map((l) => l.sort().join(',')));
  };
  return {
    copy: part((r) => String(r.copy_group_id)),
    similar: part((r) =>
      r.similar_set_id === null ? `c${r.copy_group_id}` : `s${r.similar_set_id}`,
    ),
  };
}

test('restart with merged sets reproduces the in-memory partition, in any order', async () => {
  const K = 1_600_000_000;
  const far = 0xffff_ffff_0000_0000n; // the second family's dHash, far from the first's
  const at = (s: number) => ({ captured: { s, sub: '123' } });
  const facts = [
    // Copy family: c3 bridges c1 and c2 (same key, both subsecs) into one copy group, merging
    // the two similar sets that c1s and c2s (other seconds) gave them.
    F('c1'),
    F('c1s', { phash: 0x1n, ...at(K + 1) }),
    F('c2', { phash: 0xffffn }),
    F('c2s', { phash: 0x7fffn, ...at(K + 2) }),
    // Similar family: sb bridges two similar sets.
    F('s1', { dhash: far, ...at(K + 10) }),
    F('s1b', { dhash: far, phash: 0x1n, ...at(K + 11) }),
    F('s2', { dhash: far, phash: 0xffffn, ...at(K + 12) }),
    F('s2b', { dhash: far, phash: 0x7fffn, ...at(K + 13) }),
    F('c3', { phash: 0xffn }),
    F('sb', { dhash: far, phash: 0xffn, ...at(K + 14) }),
    // After the merges, from a cold cache: d joins the merged copy group, e the merged set.
    F('d', { phash: 0x0fn }),
    F('e', { phash: 0x0en, ...at(K + 20) }),
  ];
  const sha = await seedHashed(...facts);
  const byName = new Map(facts.map((f, i) => [f.sha256, sha[i]!]));
  const run = async (names: string[]) => {
    const worker = createActivities(deps); // a fresh worker process: empty cache
    const out: (number | null)[] = [];
    for (const n of names) out.push(await runActivity(worker.clusterContent, [byName.get(n)!]));
    return out;
  };
  await run(['c1', 'c1s', 'c2', 'c2s', 's1', 's1b', 's2', 's2b']);
  const [g3] = await run(['c3', 'sb']);
  expect(await count('groups', `kind = 'copy' AND merged_into IS NOT NULL`)).toBe(2);
  expect(await count('groups', `kind = 'similar' AND merged_into IS NOT NULL`)).toBe(2);
  const [gd, ge] = await run(['d', 'e']);
  expect(gd).toBe(g3);
  expect(await similarSetOf(ge!)).toBe(await similarSetOf(g3!));

  const n = (...names: string[]) =>
    names
      .map((x) => hex(x))
      .sort()
      .join(',');
  const want = {
    copy: new Set([
      n('c1', 'c2', 'c3', 'd'),
      n('c1s'),
      n('c2s'),
      n('e'),
      ...['s1', 's1b', 's2', 's2b', 'sb'].map((x) => n(x)),
    ]),
    similar: new Set([
      n('c1', 'c2', 'c3', 'd', 'c1s', 'c2s', 'e'),
      n('s1', 's1b', 's2', 's2b', 'sb'),
    ]),
  };
  const db = await dbPartitions();
  expect(db).toEqual(want);
  const all = facts.map(stored);
  expect(inMemory(all)).toEqual(want);
  expect(inMemory([...all].reverse())).toEqual(want);
  const shuffled = [...all].sort((x, y) => (x.sha256 < y.sha256 ? -1 : 1));
  expect(inMemory(shuffled)).toEqual(want);
});

test('corrupt stored clustering fails loudly instead of mis-grouping', async () => {
  const [a, b] = await seedHashed(F('a'), F('b', { phash: mix(9), dhash: mix(10) }));
  const unhashed = await seedContent({ state: 'HASHED' });
  await expect(cluster(unhashed)).rejects.toThrow(/no hashes or dimensions/);

  await pool.query(`UPDATE contents SET state = 'CLUSTERED' WHERE sha256 = $1`, [a]);
  await expect(cluster(b!)).rejects.toThrow(/CLUSTERED without a group/);

  await pool.query(
    `INSERT INTO groups (id, kind, merged_into) VALUES (101, 'copy', 102), (102, 'copy', 101)`,
  );
  await pool.query(`UPDATE contents SET copy_group_id = 101 WHERE sha256 = $1`, [a]);
  await expect(cluster(b!)).rejects.toThrow(/merged_into cycle at 101/);
  expect((await stateOf(b!)).state).toBe('HASHED');
});
