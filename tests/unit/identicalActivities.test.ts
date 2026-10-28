// dedupeIdentical against a real Postgres, a temp vault and the real decode pool (pixelDigest
// crosses workerpool's JSON IPC in every test here).
import { appendFile, copyFile, link, mkdir, stat, unlink, writeFile } from 'node:fs/promises';
import { dirname, extname, join } from 'node:path';
import { ExifTool } from 'exiftool-vendored';
import type pg from 'pg';
import sharp from 'sharp';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import type { WorkerPool } from '../../src/activities/hashing.js';
import {
  dedupeIdenticalAll,
  dedupeIdenticalWith,
  findFamily,
  removePurged,
  removeRetired,
  planFamily,
  reportCsv,
  retireFamily,
  type IdenticalCtx,
} from '../../src/activities/identical.js';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { withTx } from '../../src/db.js';
import { DataLayout, sha256File, vaultRel } from '../../src/fsops.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { photo, truncatedJpeg } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let pgh: PgHandle;
let pool: pg.Pool;
let deps: Deps;
let layout: DataLayout;
let close: () => Promise<void>;
let acts: ReturnType<typeof createActivities>;
let et: ExifTool;
/** pixelDigest calls made through the decode pool since the last reset. */
let digests: string[] = [];

/** The real decode pool, counting the pixelDigest calls that pass through it. */
function counting(p: WorkerPool): WorkerPool {
  return new Proxy(p, {
    get(target, prop, recv) {
      if (prop === 'exec')
        return (method: string, args: unknown[]) => {
          if (method === 'pixelDigest') digests.push(String(args[0]));
          return target.exec(method, args);
        };
      return Reflect.get(target, prop, recv) as unknown;
    },
  });
}

beforeAll(async () => {
  pgh = await startPg();
  pool = pgh.pool;
  const t = await testDeps(pgh);
  layout = t.layout;
  close = t.close;
  deps = { ...t.deps, decodePool: counting(t.deps.decodePool) };
  acts = createActivities(deps);
  et = new ExifTool({ maxProcs: 1 });
}, 120_000);
afterAll(async () => {
  await et.end();
  await close();
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
  acts = createActivities(deps); // a fresh cluster cache per test
  digests = [];
});

let base: string | undefined;
/** A camera JPEG of scene 0, written once; every identical variant is a byte copy of it. */
async function baseJpeg(): Promise<string> {
  base ??= await photo(await tmpDir(), 'jpg');
  return base;
}

/** A byte copy of the base JPEG with `tags` written (pixels unchanged, bytes different). */
async function variant(tags: Record<string, string>): Promise<string> {
  const p = join(await tmpDir(), 'v.jpg');
  await copyFile(await baseJpeg(), p);
  if (Object.keys(tags).length > 0) await et.write(p, tags, ['-overwrite_original']);
  return p;
}

interface Opts {
  name?: string;
  live?: string;
  ingestedAt?: Date;
}

/** Vaults `src` as a new content and runs it through decode + recordHash (real hashes). */
async function hashed(src: string, o: Opts = {}): Promise<string> {
  const sha = await sha256File(src);
  const ext = extname(src).slice(1).toLowerCase();
  const dest = join(layout.root, vaultRel(sha, ext));
  await mkdir(dirname(dest), { recursive: true });
  await copyFile(src, dest);
  const size = (await stat(src)).size;
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, mime, size_bytes, live_content_id, state, ingested_at)
     VALUES ($1, $2, 'still', 'image/jpeg', $3, $4, 'INGESTED', $5)`,
    [sha, ext, size, o.live ?? null, o.ingestedAt ?? new Date()],
  );
  await pool.query(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
     VALUES ($1, $2, $3, 1, $4, $5, 'INGESTED')`,
    [`drop/${o.name ?? 'x.jpg'}`, o.name ?? 'x.jpg', size, ext, sha],
  );
  const d = await runActivity(acts.decodeContent, [sha, ext]);
  expect(await runActivity(acts.recordHash, [sha, d])).toBe('HASHED');
  return sha;
}

async function clustered(src: string, o: Opts = {}): Promise<string> {
  const sha = await hashed(src, o);
  expect(await runActivity(acts.clusterContent, [sha])).not.toBeNull();
  return sha;
}

/** Gives `sha` the suspect key of `like` (its pixels may differ: the key alone is a hint). */
async function sameKey(sha: string, like: string): Promise<void> {
  await pool.query(
    `UPDATE contents c SET phash = l.phash, dhash = l.dhash, width = l.width, height = l.height
       FROM contents l WHERE c.sha256 = $1 AND l.sha256 = $2`,
    [sha, like],
  );
}

interface Row {
  state: string;
  ext: string;
  duplicate_of: string | null;
  duplicate_rule: string | null;
  duplicate_at: Date | null;
  duplicate_removed_at: Date | null;
  duplicate_reclaimed_bytes: bigint | null;
  duplicate_mismatch: boolean;
  live_content_id: string | null;
  size_bytes: bigint;
  copy_group_id: bigint | null;
}
async function row(sha: string): Promise<Row> {
  const r = await pool.query<Row>('SELECT * FROM contents WHERE sha256 = $1', [sha]);
  return r.rows[0]!;
}
const vaultPath = async (sha: string) => layout.vaultFile(sha, (await row(sha)).ext);
const exists = (p: string) =>
  stat(p).then(
    () => true,
    () => false,
  );
async function version(): Promise<bigint> {
  const r = await pool.query<{ version: bigint }>('SELECT version FROM cluster_state WHERE id = 1');
  return r.rows[0]!.version;
}
async function compares() {
  const r = await pool.query<{ sha_a: string; sha_b: string; verdict: string; detail: string }>(
    'SELECT sha_a, sha_b, verdict, detail FROM pixel_compares ORDER BY sha_a, sha_b',
  );
  return r.rows;
}
const dedupe = (sha: string, a = acts) => runActivity(a.dedupeIdentical, [sha]);
const pair = (x: string, y: string) => (x < y ? [x, y] : [y, x]);

function ctx(over: Partial<IdenticalCtx> = {}): IdenticalCtx & { logs: string[] } {
  const logs: string[] = [];
  return {
    ...deps,
    beat: () => undefined,
    log: {
      info: (m, meta) => logs.push(`INFO ${m} ${JSON.stringify(meta)}`),
      warn: (m, meta) => logs.push(`WARN ${m} ${JSON.stringify(meta)}`),
    },
    logs,
    ...over,
  };
}

// ---------------------------------------------------------------- the decode pool

test('pixelDigest crosses the decode pool intact (workerpool JSON round trip)', async () => {
  const p = await baseJpeg();
  const d = (await deps.decodePool.exec('pixelDigest', [p, 'jpg'])) as Record<string, unknown>;
  expect(Object.keys(d).sort()).toEqual(['channels', 'height', 'sha256', 'width']);
  expect(d).toMatchObject({ width: 640, height: 480, channels: 3 });
  expect(d['sha256']).toMatch(/^[0-9a-f]{64}$/);
  await expect(
    deps.decodePool.exec('pixelDigest', [await truncatedJpeg(await tmpDir()), 'jpg']),
  ).rejects.toMatchObject({ name: 'QuarantineError' });
});

// ---------------------------------------------------------------- outcomes

test('no suspects: UNIQUE, nothing decoded or recorded', async () => {
  const a = await hashed(await photo(await tmpDir(), 'jpg', { scene: 4 }));
  expect(await dedupe(a)).toBe('UNIQUE');
  expect(digests).toEqual([]);
  expect(await compares()).toEqual([]);
});

test('a newcomer that loses is retired: never clustered, vault file gone, record kept', async () => {
  const cam = await clustered(await variant({}), { name: 'DSCF2223.JPG' });
  const v0 = await version();
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'DSCF2223_7f.JPG' });
  const nPath = await vaultPath(n);
  expect(await dedupe(n)).toBe('DUPLICATE');

  const r = await row(n);
  expect(r).toMatchObject({
    state: 'DUPLICATE',
    duplicate_of: cam,
    duplicate_rule: 'camera-original',
    duplicate_mismatch: false,
    duplicate_reclaimed_bytes: r.size_bytes,
    copy_group_id: null,
  });
  expect(r.duplicate_at).toBeInstanceOf(Date);
  expect(r.duplicate_removed_at).toBeInstanceOf(Date);
  expect(await exists(nPath)).toBe(false);
  expect(await exists(await vaultPath(cam))).toBe(true);
  expect(await row(cam)).toMatchObject({ state: 'CLUSTERED' });
  const [lo, hi] = pair(cam, n);
  expect(await compares()).toEqual([
    expect.objectContaining({ sha_a: lo, sha_b: hi, verdict: 'identical' }),
  ]);
  expect(await version()).toBe(v0); // no CLUSTERED content was retired
  // The workflow would stop here; a clusterContent that runs anyway is a clean no-op (R7).
  expect(await runActivity(acts.clusterContent, [n])).toBeNull();
  expect(await row(n)).toMatchObject({ state: 'DUPLICATE', copy_group_id: null });
  // Idempotent: a retry reports the same outcome and decodes nothing.
  digests = [];
  expect(await dedupe(n)).toBe('DUPLICATE');
  expect(digests).toEqual([]);
});

test('a newcomer that wins retires the CLUSTERED suspect and clusters where it was', async () => {
  const d = await tmpDir();
  const editor = await clustered(await variant({ Software: 'Picasa 3.0' }), { name: 'e.jpg' });
  // A partner the editor copy clustered with: the same scene at half size.
  const partner = await clustered(await photo(d, 'jpg', { scale: 0.5 }), { name: 'small.jpg' });
  const before = await row(editor);
  const pBefore = await row(partner);
  expect(before.copy_group_id).not.toBeNull();
  const v0 = await version();

  const n = await hashed(await variant({}), { name: 'cam.jpg' });
  expect(await dedupe(n)).toBe('KEPT');
  expect(await row(editor)).toMatchObject({
    state: 'DUPLICATE',
    duplicate_of: n,
    duplicate_rule: 'camera-original',
  });
  expect(await exists(await vaultPath(editor))).toBe(false);
  expect(await version()).toBe(v0 + 1n); // every cluster cache reloads without the retired one

  // The newcomer then clusters normally, into the groups the retired one was in.
  const g = await runActivity(acts.clusterContent, [n]);
  expect(g).not.toBeNull();
  const after = await row(n);
  expect(after.state).toBe('CLUSTERED');
  const setOf = async (id: bigint | null) =>
    (
      await pool.query<{ s: bigint | null }>(
        `SELECT coalesce(similar_set_id, id) AS s FROM groups WHERE id = $1`,
        [id],
      )
    ).rows[0]!.s;
  if (before.copy_group_id === pBefore.copy_group_id)
    expect(after.copy_group_id).toBe(pBefore.copy_group_id);
  else expect(await setOf(after.copy_group_id)).toBe(await setOf(pBefore.copy_group_id));
});

test('three identical: one keeper, two retired; a retired HASHED suspect then clusters as a no-op', async () => {
  const picasa = await hashed(await variant({ Software: 'Picasa 3.0' }));
  const photos = await clustered(await variant({ Software: 'Photos 9.0' }));
  const cam = await hashed(await variant({ Make: 'FUJIFILM' }));
  expect(await dedupe(cam)).toBe('KEPT');
  for (const s of [picasa, photos]) {
    expect(await row(s)).toMatchObject({
      state: 'DUPLICATE',
      duplicate_of: cam,
      duplicate_rule: 'camera-original',
    });
    expect(await exists(await vaultPath(s))).toBe(false);
  }
  expect((await compares()).map((c) => c.verdict)).toEqual(['identical', 'identical']);
  expect(await runActivity(acts.clusterContent, [picasa])).toBeNull();
  expect(await runActivity(acts.clusterContent, [cam])).not.toBeNull();
});

test('an older duplicate of a loser is re-pointed to the final keeper', async () => {
  const rich = await hashed(await variant({ Software: 'Picasa 3.0', Make: 'FUJIFILM' }));
  const poor = await hashed(await variant({ Software: 'Picasa 3.0' }));
  expect(await dedupe(poor)).toBe('DUPLICATE');
  expect(await row(poor)).toMatchObject({ duplicate_of: rich, duplicate_rule: 'more-metadata' });
  const cam = await hashed(await variant({}));
  expect(await dedupe(cam)).toBe('KEPT');
  expect(await row(rich)).toMatchObject({ state: 'DUPLICATE', duplicate_of: cam });
  expect(await row(poor)).toMatchObject({ state: 'DUPLICATE', duplicate_of: cam });
});

test('one pixel different: recorded different, nothing retired, never decoded again', async () => {
  const d = await tmpDir();
  const raw = await sharp(await baseJpeg())
    .raw()
    .toBuffer({ resolveWithObject: true });
  const px = Buffer.from(raw.data);
  px[1000] = (px[1000] ?? 0) ^ 1;
  const off = join(d, 'off.png');
  await sharp(px, { raw: raw.info }).png().toFile(off);
  const a = await clustered(await baseJpeg());
  const b = await hashed(off);
  await sameKey(b, a);
  expect(await dedupe(b)).toBe('UNIQUE');
  expect(await compares()).toEqual([
    expect.objectContaining({ verdict: 'different', sha_a: pair(a, b)[0], sha_b: pair(a, b)[1] }),
  ]);
  expect(await row(a)).toMatchObject({ state: 'CLUSTERED' });
  expect(await row(b)).toMatchObject({ state: 'HASHED' });
  expect(digests).toHaveLength(2);
  digests = [];
  expect(await dedupe(b)).toBe('UNIQUE');
  expect(digests).toEqual([]); // the 'different' verdict is final
  expect(await runActivity(acts.clusterContent, [b])).not.toBeNull(); // still a similar look-alike
});

test('RAW suspects and RAW newcomers are never candidates', async () => {
  const n = await hashed(await variant({}));
  const fake = 'd'.repeat(64);
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state, phash, dhash, width, height)
     SELECT $1, 'dng', 'still', 10, 'HASHED', phash, dhash, width, height
       FROM contents WHERE sha256 = $2`,
    [fake, n],
  );
  const dest = layout.vaultFile(fake, 'dng');
  await mkdir(dirname(dest), { recursive: true });
  await copyFile(await baseJpeg(), dest);
  expect(await dedupe(n)).toBe('UNIQUE');
  expect(await dedupe(fake)).toBe('UNIQUE');
  expect(digests).toEqual([]);
  expect(await compares()).toEqual([]);
});

test('a decode error or a missing file is an error verdict: nothing retired, retried next run', async () => {
  const n = await hashed(await variant({}));
  // A suspect whose file is a truncated JPEG (its sha is right; it just does not decode).
  const bad = await truncatedJpeg(await tmpDir());
  const badSha = await sha256File(bad);
  const dest = layout.vaultFile(badSha, 'jpg');
  await mkdir(dirname(dest), { recursive: true });
  await copyFile(bad, dest);
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state, phash, dhash, width, height)
     SELECT $1, 'jpg', 'still', 10, 'HASHED', phash, dhash, width, height
       FROM contents WHERE sha256 = $2`,
    [badSha, n],
  );
  // And an identical suspect whose vault file has gone missing.
  const gone = await hashed(await variant({ Software: 'Picasa 3.0' }));
  await unlink(await vaultPath(gone));

  expect(await dedupe(n)).toBe('UNIQUE');
  const c = await compares();
  expect(c.map((x) => x.verdict)).toEqual(['error', 'error']);
  for (const x of c) expect(x.detail).toBeTruthy();
  for (const s of [n, badSha, gone]) expect((await row(s)).state).toBe('HASHED');
  expect(await exists(await vaultPath(n))).toBe(true);
  digests = [];
  expect(await dedupe(n)).toBe('UNIQUE');
  expect(digests.length).toBeGreaterThan(0); // error verdicts are retried
});

test('a vault file that no longer matches its sha256 is an error verdict, never a keeper', async () => {
  const cam = await hashed(await variant({}));
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }));
  await appendFile(await vaultPath(cam), Buffer.from([0])); // pixels intact, bytes changed
  expect(await dedupe(n)).toBe('UNIQUE');
  const [c, ...rest] = await compares();
  expect(rest).toEqual([]);
  expect(c).toMatchObject({ verdict: 'error' });
  expect(c?.detail).toMatch(/sha256/);
  expect(await row(n)).toMatchObject({ state: 'HASHED' });
  expect(await exists(await vaultPath(n))).toBe(true);
});

test('a loser whose bytes change before removal is flagged duplicate_mismatch and kept', async () => {
  const cam = await clustered(await variant({}));
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }));
  const nPath = await vaultPath(n);
  const tampered = createActivities({
    ...deps,
    checkpoint: async (name) => {
      if (name === 'identical_retired') await appendFile(nPath, Buffer.from('x'));
    },
  });
  expect(await dedupe(n, tampered)).toBe('DUPLICATE');
  expect(await row(n)).toMatchObject({
    state: 'DUPLICATE',
    duplicate_of: cam,
    duplicate_mismatch: true,
    duplicate_removed_at: null,
  });
  expect(await exists(nPath)).toBe(true);
  expect(await removeRetired(ctx(), n)).toBe('MISMATCH');
  expect(await exists(nPath)).toBe(true);
});

test('a crash between retire and removal is finished by the next run; removal is idempotent', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  const crashing = createActivities({
    ...deps,
    checkpoint: (name) => {
      if (name === 'identical_retired') throw new Error('killed');
    },
  });
  await expect(dedupe(cam, crashing)).rejects.toThrow('killed');
  expect(await row(n)).toMatchObject({ state: 'DUPLICATE', duplicate_removed_at: null });
  expect(await exists(await vaultPath(n))).toBe(true);
  // The retried activity (the keeper's) finishes the removal.
  expect(['KEPT', 'UNIQUE']).toContain(await dedupe(cam));
  expect(await exists(await vaultPath(n))).toBe(false);
  expect((await row(n)).duplicate_removed_at).toBeInstanceOf(Date);
  const c = ctx();
  expect(await removeRetired(c, n)).toBe('ALREADY_REMOVED');
  expect(await row(cam)).toMatchObject({ state: 'HASHED' });
});

test('removeRetired: logs both names and the rule; a file already gone is MISSING with 0 bytes', async () => {
  const cam = await hashed(await variant({}), { name: 'DSCF2223.JPG' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'DSCF2223_7f.JPG' });
  const m = await hashed(await variant({ Software: 'GIMP 2.10' }), { name: 'gimp.jpg' });
  const c = ctx();
  const retired = await withTx(pool, async (tx) => {
    const fam = await findFamily(c, tx, cam);
    expect([...fam].sort()).toEqual([cam, n, m].sort());
    return retireFamily(c, tx, fam);
  });
  expect(retired.keeper).toBe(cam);
  expect(retired.retired.map((r) => r.rule)).toEqual(['camera-original', 'camera-original']);
  expect(await removeRetired(c, n)).toBe('REMOVED');
  expect(c.logs.join('\n')).toMatch(/DSCF2223\.JPG/);
  expect(c.logs.join('\n')).toMatch(/DSCF2223_7f\.JPG/);
  expect(c.logs.join('\n')).toMatch(/camera-original/);
  expect(c.logs.join('\n')).toContain(vaultRel(n, 'jpg'));
  expect(c.logs.join('\n')).toContain(vaultRel(cam, 'jpg'));
  await unlink(await vaultPath(m));
  expect(await removeRetired(c, m)).toBe('MISSING');
  expect(await row(m)).toMatchObject({ duplicate_reclaimed_bytes: 0n });
  expect((await row(m)).duplicate_removed_at).toBeInstanceOf(Date);
  expect(await removeRetired(c, m)).toBe('ALREADY_REMOVED');
});

test('the Live Photo link moves to a keeper that has none, never over one it has', async () => {
  const cam = await hashed(await variant({}));
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { live: 'LIVE-1' });
  expect(await dedupe(n)).toBe('DUPLICATE');
  expect((await row(cam)).live_content_id).toBe('LIVE-1');

  await pgh.truncate();
  const cam2 = await hashed(await variant({}), { live: 'MINE' });
  const n2 = await hashed(await variant({ Software: 'Picasa 3.0' }), { live: 'LIVE-2' });
  expect(await dedupe(n2)).toBe('DUPLICATE');
  expect((await row(cam2)).live_content_id).toBe('MINE');
});

test('two identical newcomers at once: exactly one stays', async () => {
  const a = await hashed(await variant({}));
  const b = await hashed(await variant({ Software: 'Picasa 3.0' }));
  const out = await Promise.all([dedupe(a), dedupe(b)]);
  expect(out).toContain('DUPLICATE');
  expect(out.filter((o) => o !== 'DUPLICATE')).toHaveLength(1);
  expect((await row(a)).state).toBe('HASHED');
  expect(await row(b)).toMatchObject({ state: 'DUPLICATE', duplicate_of: a });
  expect(await exists(await vaultPath(b))).toBe(false);
  expect(await exists(await vaultPath(a))).toBe(true);
});

test('retiring a CLUSTERED content waits for the cluster lock (R6), heartbeating', async () => {
  const editor = await clustered(await variant({ Software: 'Picasa 3.0' }));
  const n = await hashed(await variant({}));
  const holder = await pool.connect();
  let done = false;
  const beats: unknown[] = [];
  try {
    await holder.query('BEGIN');
    await holder.query(`SELECT pg_advisory_xact_lock(hashtext('pm.cluster'))`);
    const c = ctx({ keepaliveMs: 100, beat: (d) => beats.push(d) });
    const run = dedupeIdenticalWith(c, n).then((o) => {
      done = true;
      return o;
    });
    await new Promise((r) => setTimeout(r, 1500));
    expect(done).toBe(false);
    expect((await row(editor)).state).toBe('CLUSTERED');
    expect(beats.filter((b) => b === 'waiting for lock').length).toBeGreaterThan(5);
    await holder.query('COMMIT');
    expect(await run).toBe('KEPT');
  } finally {
    holder.release();
  }
  expect((await row(editor)).state).toBe('DUPLICATE');
});

test('a content that is not HASHED or CLUSTERED fails visibly', async () => {
  const sha = 'e'.repeat(64);
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state) VALUES ($1, 'jpg', 'still', 1, 'INGESTED')`,
    [sha],
  );
  await expect(dedupe(sha)).rejects.toMatchObject({ type: 'UnexpectedState', nonRetryable: true });
  await expect(dedupe('0'.repeat(64))).rejects.toMatchObject({ type: 'UnexpectedState' });
});

test('recordHash on a retired content reports HASHED (a replayed workflow then dedupes again)', async () => {
  await hashed(await variant({}));
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }));
  expect(await dedupe(n)).toBe('DUPLICATE');
  const facts = { width: 640, height: 480, phash: '1', dhash: '1', mime: 'image/jpeg' };
  expect(await runActivity(acts.recordHash, [n, { facts, quarantineReason: null }])).toBe('HASHED');
});

// ---------------------------------------------------------------- the one-time cleanup

/** Every column of every contents row, to prove a dry run changed nothing. */
async function snapshot(): Promise<string> {
  const r = await pool.query(
    `SELECT (SELECT json_agg(c ORDER BY sha256) FROM contents c) AS c,
            (SELECT json_agg(f ORDER BY rel_path) FROM files f) AS f,
            (SELECT version::text FROM cluster_state WHERE id = 1) AS v`,
  );
  return JSON.stringify(r.rows[0]);
}

async function seedFamilies() {
  const cam = await hashed(await variant({}), { name: 'cam,1.jpg' });
  const pic = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic "x".jpg' });
  const lone = await hashed(await photo(await tmpDir(), 'jpg', { scene: 4 }), { name: 'lone.jpg' });
  return { cam, pic, lone };
}

test('planFamily picks keeper and losers without writing', async () => {
  const { cam, pic } = await seedFamilies();
  const before = await snapshot();
  const plan = await withTx(pool, (tx) => planFamily(ctx(), tx, [pic, cam]));
  expect(plan.keeper).toBe(cam);
  expect(plan.losers).toEqual([{ sha256: pic, rule: 'camera-original' }]);
  expect(await snapshot()).toBe(before);
});

test('dedupeIdenticalAll dry run: report rows and totals; only pixel_compares written', async () => {
  const { cam, pic } = await seedFamilies();
  const picSize = (await row(pic)).size_bytes;
  const before = await snapshot();
  const r = await dedupeIdenticalAll(ctx(), { apply: false });
  expect(r).toMatchObject({ families: 1, files: 1, bytes: Number(picSize), errors: 0 });
  expect(r.rows).toEqual([
    {
      keeper_sha256: cam,
      keeper_vault_path: vaultRel(cam, 'jpg'),
      keeper_names: 'cam,1.jpg',
      removed_sha256: pic,
      removed_vault_path: vaultRel(pic, 'jpg'),
      removed_names: 'pic "x".jpg',
      rule: 'camera-original',
      bytes: Number(picSize),
    },
  ]);
  expect(await snapshot()).toBe(before);
  expect(await exists(await vaultPath(pic))).toBe(true);
  expect((await compares()).map((c) => c.verdict)).toEqual(['identical']);
  const csv = reportCsv(r.rows);
  expect(csv.split('\n')[0]).toBe(
    'keeper_sha256,keeper_vault_path,keeper_names,removed_sha256,removed_vault_path,removed_names,rule,bytes',
  );
  expect(csv).toContain('"cam,1.jpg"');
  expect(csv).toContain('"pic ""x"".jpg"');
});

test('dedupeIdenticalAll apply retires and removes; a re-run is a no-op', async () => {
  const { cam, pic, lone } = await seedFamilies();
  const r = await dedupeIdenticalAll(ctx(), { apply: true });
  expect(r).toMatchObject({ families: 1, files: 1, errors: 0 });
  expect(await row(pic)).toMatchObject({ state: 'DUPLICATE', duplicate_of: cam });
  expect((await row(pic)).duplicate_removed_at).toBeInstanceOf(Date);
  expect(await exists(await vaultPath(pic))).toBe(false);
  expect(await exists(await vaultPath(cam))).toBe(true);
  expect((await row(lone)).state).toBe('HASHED');
  digests = [];
  const again = await dedupeIdenticalAll(ctx(), { apply: true });
  expect(again).toMatchObject({ families: 0, files: 0, bytes: 0, errors: 0, rows: [] });
  expect(digests).toEqual([]);
});

test('apply first removes a retired row whose unlink was lost; a dry run does not', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  const c = ctx();
  await withTx(pool, async (tx) => retireFamily(c, tx, await findFamily(c, tx, cam)));
  expect(await exists(await vaultPath(n))).toBe(true);
  await dedupeIdenticalAll(ctx(), { apply: false });
  expect(await exists(await vaultPath(n))).toBe(true);
  const r = await dedupeIdenticalAll(ctx(), { apply: true });
  expect(r.families).toBe(0);
  expect(await exists(await vaultPath(n))).toBe(false);
  expect((await row(n)).duplicate_removed_at).toBeInstanceOf(Date);
});

test('same key but different pixels is not a family; compared pairs are not decoded twice; errors count', async () => {
  const a = await hashed(await variant({}), { name: 'a.jpg' });
  const b = await hashed(await photo(await tmpDir(), 'jpg', { scene: 5 }), { name: 'b.jpg' });
  await sameKey(b, a);
  expect(await dedupeIdenticalAll(ctx(), { apply: false })).toMatchObject({ families: 0 });
  expect((await compares()).map((x) => x.verdict)).toEqual(['different']);
  digests = [];
  await dedupeIdenticalAll(ctx(), { apply: false });
  expect(digests).toEqual([]);
  await unlink(await vaultPath(b));
  await pool.query('DELETE FROM pixel_compares');
  const r = await dedupeIdenticalAll(ctx(), { apply: true });
  expect(r).toMatchObject({ families: 0, errors: 1 });
  expect((await compares()).map((x) => x.verdict)).toEqual(['error']);
});

test('RAW and non-still contents never form families', async () => {
  const a = await hashed(await variant({}), { name: 'a.jpg' });
  const b = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'b.jpg' });
  await pool.query(`UPDATE contents SET ext = 'cr2' WHERE sha256 = $1`, [b]);
  expect(await dedupeIdenticalAll(ctx(), { apply: true })).toMatchObject({ families: 0 });
  expect((await row(a)).state).toBe('HASHED');
});

/** A second family (another scene): a camera original and a Picasa re-save. */
async function secondFamily(): Promise<{ cam: string; pic: string }> {
  const p = await photo(await tmpDir(), 'jpg', { scene: 6 });
  const q = join(await tmpDir(), 'q.jpg');
  await copyFile(p, q);
  await et.write(q, { Software: 'Picasa 3.0' }, ['-overwrite_original']);
  return { cam: await hashed(p, { name: 'cam2.jpg' }), pic: await hashed(q, { name: 'pic2.jpg' }) };
}

test('an apply interrupted after the first family has already streamed its rows', async () => {
  const f1 = await seedFamilies();
  const f2 = await secondFamily();
  const seen: { removed_sha256: string | number; outcome?: string }[] = [];
  let calls = 0;
  const c = ctx({
    checkpoint: () => {
      calls += 1;
      if (calls === 2) throw new Error('killed');
    },
  });
  await expect(
    dedupeIdenticalAll(c, { apply: true, onRows: (rows) => void seen.push(...rows) }),
  ).rejects.toThrow('killed');
  expect(seen).toHaveLength(1);
  expect(seen[0]).toMatchObject({ outcome: 'REMOVED' });
  const gone = String(seen[0]!.removed_sha256);
  expect([f1.pic, f2.pic]).toContain(gone);
  expect(await exists(await vaultPath(gone))).toBe(false);
});

test('a resumed pending removal appears in the applied rows with its outcome', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  const c = ctx();
  await withTx(pool, async (tx) => retireFamily(c, tx, await findFamily(c, tx, cam)));
  const seen: unknown[] = [];
  const r = await dedupeIdenticalAll(ctx(), {
    apply: true,
    onRows: (rows) => void seen.push(...rows),
  });
  expect(seen).toEqual([
    expect.objectContaining({
      keeper_sha256: cam,
      removed_sha256: n,
      rule: 'camera-original',
      outcome: 'REMOVED',
    }),
  ]);
  expect(r).toMatchObject({ families: 0, files: 1, errors: 0 });
  expect(reportCsv(r.rows, true).split('\n')[0]).toMatch(/,bytes,outcome$/);
});

test('a loser that fails its checksum is MISMATCH: not counted as removed, counted as an error', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  const nPath = await vaultPath(n);
  const r = await dedupeIdenticalAll(
    ctx({
      checkpoint: async (name) => {
        if (name === 'identical_retired') await appendFile(nPath, Buffer.from('x'));
      },
    }),
    { apply: true },
  );
  expect(r).toMatchObject({ families: 1, files: 0, bytes: 0, errors: 1 });
  expect(r.rows).toEqual([
    expect.objectContaining({
      keeper_sha256: cam,
      removed_sha256: n,
      bytes: 0,
      outcome: 'MISMATCH',
    }),
  ]);
  expect(await exists(nPath)).toBe(true);
});

/** Retires the family of `cam` (row only: vault files untouched). */
async function retireOnly(cam: string): Promise<void> {
  const c = ctx();
  await withTx(pool, async (tx) => retireFamily(c, tx, await findFamily(c, tx, cam)));
}

test('a loser still linked elsewhere (a staging generation) is DEFERRED: kept, unmarked, removed once unlinked', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  await retireOnly(cam);
  const nPath = await vaultPath(n);
  const staged = join(await tmpDir(), 'staged.jpg');
  await link(nPath, staged);
  const c = ctx();
  expect(await removeRetired(c, n)).toBe('DEFERRED');
  expect(c.logs.join('\n')).toMatch(/deferred/);
  expect(await exists(nPath)).toBe(true);
  expect(await row(n)).toMatchObject({
    state: 'DUPLICATE',
    duplicate_removed_at: null,
    duplicate_mismatch: false,
  });
  // The pipeline step treats it as a normal outcome (pending, retried later).
  expect(['KEPT', 'UNIQUE']).toContain(await dedupe(cam));
  expect(await exists(nPath)).toBe(true);
  await unlink(staged);
  expect(await removeRetired(ctx(), n)).toBe('REMOVED');
  expect(await exists(nPath)).toBe(false);
  expect((await row(n)).duplicate_removed_at).toBeInstanceOf(Date);
});

test('apply: a DEFERRED removal is its own count (not removed, not an error); a later apply finishes it', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  const staged = join(await tmpDir(), 'staged.jpg');
  const r = await dedupeIdenticalAll(
    ctx({
      checkpoint: async (name) => {
        if (name === 'identical_retired') await link(await vaultPath(n), staged);
      },
    }),
    { apply: true },
  );
  expect(r).toMatchObject({ families: 1, files: 0, bytes: 0, deferred: 1, errors: 0 });
  expect(r.rows).toEqual([
    expect.objectContaining({ keeper_sha256: cam, removed_sha256: n, outcome: 'DEFERRED' }),
  ]);
  expect(reportCsv(r.rows, true)).toMatch(/,DEFERRED\n$/);
  expect(await exists(await vaultPath(n))).toBe(true);
  const still = await dedupeIdenticalAll(ctx(), { apply: true });
  expect(still).toMatchObject({ families: 0, files: 0, deferred: 1, errors: 0 });
  await unlink(staged);
  const done = await dedupeIdenticalAll(ctx(), { apply: true });
  expect(done).toMatchObject({ families: 0, files: 1, deferred: 0, errors: 0 });
  expect(done.rows).toEqual([expect.objectContaining({ removed_sha256: n, outcome: 'REMOVED' })]);
  expect(await exists(await vaultPath(n))).toBe(false);
});

test('a keeper whose bytes no longer match its sha256 keeps the loser: flagged, nothing deleted', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  await retireOnly(cam);
  const camPath = await vaultPath(cam);
  await writeFile(camPath, Buffer.from('not the keeper any more'));
  expect(await removeRetired(ctx(), n)).toBe('MISMATCH');
  expect(await exists(await vaultPath(n))).toBe(true);
  expect(await row(n)).toMatchObject({ duplicate_mismatch: true, duplicate_removed_at: null });
  expect(
    (await pool.query('SELECT last_error FROM contents WHERE sha256 = $1', [n])).rows[0],
  ).toMatchObject({ last_error: expect.stringMatching(/keeper vault sha256/) as unknown });
});

test('one family that fails to plan is an error, recorded with its members; the others still run', async () => {
  const f1 = await seedFamilies();
  const f2 = await secondFamily();
  const bad = await vaultPath(f1.cam);
  const c = ctx({
    metadata: {
      ...deps.metadata,
      keeperTags: (path: string) =>
        path === bad
          ? Promise.reject(new Error('exiftool exploded'))
          : deps.metadata.keeperTags(path),
    } as Deps['metadata'],
  });
  const errs: unknown[] = [];
  const r = await dedupeIdenticalAll(c, { apply: true, onError: (e) => void errs.push(e) });
  expect(r).toMatchObject({ families: 1, files: 1, errors: 1 });
  expect(r.errorRows).toEqual([
    {
      sha_a: '',
      sha_b: '',
      members: [f1.cam, f1.pic].sort().join(';'),
      error: expect.stringMatching(/exiftool exploded/) as unknown,
    },
  ]);
  expect(errs).toEqual(r.errorRows);
  expect(c.logs.join('\n')).toMatch(/family not processed/);
  // Nothing of the failed family was retired; the other family was.
  expect((await row(f1.pic)).state).toBe('HASHED');
  expect((await row(f1.cam)).state).toBe('HASHED');
  expect((await row(f2.pic)).state).toBe('DUPLICATE');
  expect(await exists(await vaultPath(f2.pic))).toBe(false);
});

// ---------------------------------------------------------------- purged contents (purge spec §6)

/** A vaulted content marked PURGED (vault file still there). */
async function purgedContent(src: string, name: string): Promise<string> {
  const sha = await hashed(src, { name });
  await pool.query(`UPDATE contents SET state = 'PURGED', purged_at = now() WHERE sha256 = $1`, [
    sha,
  ]);
  return sha;
}
interface PurgeRow {
  purge_removed_at: Date | null;
  purge_reclaimed_bytes: bigint | null;
  purge_mismatch: boolean;
  last_error: string | null;
}
const purgeRow = async (sha: string): Promise<PurgeRow> =>
  (
    await pool.query<PurgeRow>(
      `SELECT purge_removed_at, purge_reclaimed_bytes, purge_mismatch, last_error
         FROM contents WHERE sha256 = $1`,
      [sha],
    )
  ).rows[0]!;

test('removePurged: DEFERRED while a second link exists, REMOVED once it is gone', async () => {
  const p = await purgedContent(await variant({}), 'gone.jpg');
  const path = await vaultPath(p);
  const size = (await stat(path)).size;
  const staged = join(await tmpDir(), 'staged.jpg');
  await link(path, staged);
  const c = ctx();
  expect(await removePurged(c, p)).toBe('DEFERRED');
  expect(c.logs.join('\n')).toMatch(/purged photo removal deferred: still linked/);
  expect(await exists(path)).toBe(true);
  expect(await purgeRow(p)).toMatchObject({ purge_removed_at: null, purge_mismatch: false });
  await unlink(staged);
  expect(await removePurged(c, p)).toBe('REMOVED');
  expect(c.logs.join('\n')).toMatch(/purged photo removed/);
  expect(c.logs.join('\n')).toMatch(/gone\.jpg/);
  expect(await exists(path)).toBe(false);
  const r = await purgeRow(p);
  expect(r.purge_removed_at).toBeInstanceOf(Date);
  expect(r.purge_reclaimed_bytes).toBe(BigInt(size));
  expect(await removePurged(c, p)).toBe('ALREADY_REMOVED');
  // The duplicate columns are untouched.
  expect(await row(p)).toMatchObject({ duplicate_removed_at: null, duplicate_mismatch: false });
});

test('removePurged: altered bytes are flagged purge_mismatch and kept', async () => {
  const p = await purgedContent(await variant({}), 'x.jpg');
  const path = await vaultPath(p);
  await appendFile(path, Buffer.from('tampered'));
  const c = ctx();
  expect(await removePurged(c, p)).toBe('MISMATCH');
  expect(c.logs.join('\n')).toMatch(/purged photo not removed: mismatch flagged/);
  expect(await exists(path)).toBe(true);
  expect(await purgeRow(p)).toMatchObject({
    purge_mismatch: true,
    purge_removed_at: null,
    last_error: expect.stringMatching(/^purge_mismatch: vault sha256/) as unknown,
  });
  expect(await removePurged(c, p)).toBe('MISMATCH');
});

test('removePurged: an absent vault file is MISSING, recorded with 0 bytes', async () => {
  const p = await purgedContent(await variant({}), 'x.jpg');
  await unlink(await vaultPath(p));
  expect(await removePurged(ctx(), p)).toBe('MISSING');
  expect(await purgeRow(p)).toMatchObject({ purge_reclaimed_bytes: 0n });
  expect((await purgeRow(p)).purge_removed_at).toBeInstanceOf(Date);
});

test('removePurged refuses a content that is not PURGED', async () => {
  const s = await hashed(await variant({}));
  await expect(removePurged(ctx(), s)).rejects.toThrow(/not PURGED/);
  expect(await exists(await vaultPath(s))).toBe(true);
});

test('a retired duplicate whose keeper is PURGED is flagged, never removed (review focus 2)', async () => {
  const cam = await hashed(await variant({}), { name: 'cam.jpg' });
  const n = await hashed(await variant({ Software: 'Picasa 3.0' }), { name: 'pic.jpg' });
  await retireOnly(cam);
  await pool.query(`UPDATE contents SET state = 'PURGED', purged_at = now() WHERE sha256 = $1`, [
    cam,
  ]);
  expect(await removeRetired(ctx(), n)).toBe('MISMATCH');
  expect(await exists(await vaultPath(n))).toBe(true);
  expect(await row(n)).toMatchObject({
    state: 'DUPLICATE',
    duplicate_of: cam,
    duplicate_mismatch: true,
    duplicate_removed_at: null,
  });
  expect(
    (await pool.query('SELECT last_error FROM contents WHERE sha256 = $1', [n])).rows[0],
  ).toMatchObject({ last_error: expect.stringMatching(/keeper .* is PURGED/) as unknown });
});
