import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { existsSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import {
  chmod,
  mkdir,
  open,
  readdir,
  readFile,
  readlink,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { basename, dirname, join, relative } from 'node:path';
import type { Client } from '@temporalio/client';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { vaultRel, type DataLayout } from '../../src/fsops.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

/** Test seams: when set, `rmdir` / `lstat` / `unlink` call these first (they may throw). */
const fsHooks = vi.hoisted(() => ({
  rmdir: null as ((path: string) => void) | null,
  lstat: null as ((path: string) => void) | null,
  unlink: null as ((path: string) => void) | null,
}));
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    rmdir: async (...a: Parameters<typeof actual.rmdir>) => {
      fsHooks.rmdir?.(String(a[0]));
      return actual.rmdir(...a);
    },
    lstat: async (...a: Parameters<typeof actual.lstat>) => {
      fsHooks.lstat?.(String(a[0]));
      return actual.lstat(...a);
    },
    unlink: async (...a: Parameters<typeof actual.unlink>) => {
      fsHooks.unlink?.(String(a[0]));
      return actual.unlink(...a);
    },
  };
});
const fsError = (code: string) => Object.assign(new Error(`${code}: test`), { code });

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
}, 120_000);
afterAll(async () => {
  await close();
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
  // Each test starts from an empty DATA_DIR (the test owns this temp tree).
  for (const d of [layout.vault, layout.staging]) await rm(d, { recursive: true, force: true });
  await layout.ensure();
  acts = createActivities(deps);
});

afterEach(() => {
  fsHooks.rmdir = null;
  fsHooks.lstat = null;
  fsHooks.unlink = null;
});

const build = (force: boolean) => runActivity(acts.maybeBuildStaging, [force]);
const genDir = (n: number) => join(layout.staging, `.gen-${String(n)}`);

async function walk(dir: string): Promise<string[]> {
  const out: string[] = [];
  for (const e of await readdir(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) out.push(...(await walk(p)));
    else out.push(p);
  }
  return out.sort();
}

/** Every file under `dir` (followed through a symlinked root) as [relative path, sha256]. */
async function tree(dir: string): Promise<[string, string][]> {
  const root = join(dirname(dir), await readlink(dir).catch(() => relative(dirname(dir), dir)));
  return Promise.all(
    (await walk(root)).map(
      async (f) =>
        [
          relative(root, f),
          createHash('sha256')
            .update(await readFile(f))
            .digest('hex'),
        ] as [string, string],
    ),
  );
}

const fakeTemporal = (running: number) =>
  ({
    workflow: { count: () => Promise.resolve({ count: running, groups: [] }) },
  }) as unknown as Client;

/** Writes bytes read-only at `rel` under DATA_DIR; returns their sha256. */
async function put(rel: string, bytes: Buffer): Promise<void> {
  const abs = join(layout.root, rel);
  await mkdir(dirname(abs), { recursive: true });
  await writeFile(abs, bytes);
  await chmod(abs, 0o444);
}
const sha = (b: Buffer) => createHash('sha256').update(b).digest('hex');

async function content(
  bytes: Buffer,
  o: { ext: string; kind: string; state: string; w?: number; group?: number; live?: string },
): Promise<string> {
  const s = sha(bytes);
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, width, height, captured_at,
                           live_content_id, duration_s, state, copy_group_id)
     VALUES ($1, $2, $3, $4, $5, $6, '2021-10-03T14:02:11Z', $7, 2, $8, $9)`,
    [
      s,
      o.ext,
      o.kind,
      bytes.length,
      o.w ?? null,
      o.w ?? null,
      o.live ?? null,
      o.state,
      o.group ?? null,
    ],
  );
  return s;
}

async function file(relPath: string, s: string, ext: string, state = 'INGESTED'): Promise<bigint> {
  const r = await pool.query<{ id: bigint }>(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
     VALUES ($1, $2, 1, 0, $3, $4, $5) RETURNING id`,
    [relPath, relPath.split('/').pop(), ext, s, state],
  );
  return r.rows[0]!.id;
}

/** A Live still with a byte copy, a smaller JPEG copy, its MOV and an AAE sidecar. */
async function seedPipelineRows(): Promise<{ a: string; copyId: bigint }> {
  const [ba, bb, bm, bx] = [1, 2, 3, 4].map(() => randomBytes(64));
  await pool.query(`INSERT INTO groups (id, kind) VALUES (1, 'copy')`);
  const a = await content(ba!, {
    ext: 'heic',
    kind: 'still',
    state: 'CLUSTERED',
    w: 40,
    group: 1,
    live: 'L1',
  });
  const b = await content(bb!, { ext: 'jpg', kind: 'still', state: 'CLUSTERED', w: 20, group: 1 });
  const m = await content(bm!, { ext: 'mov', kind: 'motion', state: 'INGESTED', live: 'L1' });
  const x = await content(bx!, { ext: 'aae', kind: 'other', state: 'INGESTED' });
  await file('p/IMG_1.HEIC', a, 'heic');
  const copyId = await file('q/IMG_1.HEIC', a, 'heic', 'COPY');
  await file('p/IMG_1.JPG', b, 'jpg');
  await file('p/IMG_1.MOV', m, 'mov');
  await file('p/IMG_1.AAE', x, 'aae');
  await put(vaultRel(a, 'heic'), ba!);
  await put(`vault/.copies/${String(copyId)}.heic`, ba!);
  await put(vaultRel(b, 'jpg'), bb!);
  await put(vaultRel(m, 'mov'), bm!);
  await put(vaultRel(x, 'aae'), bx!);
  return { a, copyId };
}

test('build stages the planned tree', async () => {
  const { a } = await seedPipelineRows();
  const r = await build(true);
  expect(r).toEqual({ gen: 1, entries: 4, skipped: false });
  // The byte-identical COPY of IMG_1.HEIC is not staged (staging choice (a)).
  expect((await tree(join(layout.staging, 'current'))).map(([p]) => p)).toEqual([
    `copies/g-${a.slice(0, 8)}/IMG_1.JPG`,
    'other/p/IMG_1.AAE',
    'unique/2021/2021-10/20211003_140211_IMG_1.HEIC',
    'unique/2021/2021-10/20211003_140211_IMG_1.MOV',
  ]);
  const row = await pool.query<{ gen: bigint; cluster_version: bigint; entries: bigint }>(
    'SELECT gen, cluster_version, entries FROM staging_builds',
  );
  expect(row.rows).toEqual([{ gen: 1n, cluster_version: 0n, entries: 4n }]);
});

test('a removed duplicate never breaks a build, and no staged file is a .copies inode', async () => {
  const { copyId } = await seedPipelineRows();
  const copy = join(layout.vaultCopies, `${String(copyId)}.heic`);
  const copyIno = (await stat(copy)).ino;
  await build(true);
  for (const f of await walk(join(layout.staging, 'current')))
    expect((await stat(f)).ino, f).not.toBe(copyIno);
  await rm(copy, { force: true });
  await pool.query(
    `UPDATE files SET copy_removed_at = now(), copy_reclaimed_bytes = 64 WHERE id = $1`,
    [copyId],
  );
  expect(await build(false)).toMatchObject({ gen: 2, entries: 4, skipped: false });
});

test('build swaps atomically and is repeatable', async () => {
  await seedPipelineRows();
  const r1 = await build(true);
  const r2 = await build(true);
  expect(r2.gen).toBe(r1.gen! + 1);
  expect(await tree(join(layout.staging, 'current'))).toEqual(await tree(genDir(r1.gen!)));
  expect(await readlink(join(layout.staging, 'current'))).toBe(`.gen-${String(r2.gen)}`);
  expect((await readdir(layout.staging)).sort()).toEqual([
    `.gen-${String(r1.gen)}`,
    `.gen-${String(r2.gen)}`,
    'current',
  ]);
  const r3 = await build(true);
  expect((await readdir(layout.staging)).sort()).toEqual([
    `.gen-${String(r2.gen)}`,
    `.gen-${String(r3.gen)}`,
    'current',
  ]);
});

test('staged files share inode with vault', async () => {
  await seedPipelineRows();
  await build(true);
  const vaultInodes = new Set(
    await Promise.all((await walk(layout.vault)).map(async (f) => (await stat(f)).ino)),
  );
  const staged = await walk(join(layout.staging, 'current'));
  expect(staged.length).toBeGreaterThan(0);
  for (const f of staged) expect(vaultInodes.has((await stat(f)).ino), f).toBe(true);
});

test('orphan gen from a crash is removed', async () => {
  await seedPipelineRows();
  await mkdir(join(layout.staging, '.gen-99'), { recursive: true });
  const r = await build(true);
  expect(existsSync(join(layout.staging, '.gen-99'))).toBe(false);
  expect(r.gen).not.toBe(99);
});

test('a gen current points at survives a crash before its row, and a stale .current.tmp is replaced', async () => {
  await seedPipelineRows();
  await build(true);
  // crash after the rename but before the staging_builds insert of gen 2
  await mkdir(genDir(2));
  await symlink('.gen-2', join(layout.staging, '.current.tmp'));
  await rename(join(layout.staging, '.current.tmp'), join(layout.staging, 'current'));
  await symlink('.gen-1', join(layout.staging, '.current.tmp'));
  const r = await build(true);
  expect(r.gen).toBe(3);
  expect((await readdir(layout.staging)).sort()).toEqual(['.gen-2', '.gen-3', 'current']);
  expect(await readlink(join(layout.staging, 'current'))).toBe('.gen-3');
});

test('skips when unchanged; a cluster_state bump counts as a change', async () => {
  await seedPipelineRows();
  await build(true);
  expect(await build(false)).toMatchObject({ skipped: true, gen: null });
  await pool.query('UPDATE cluster_state SET version = version + 1');
  expect((await build(false)).skipped).toBe(false);
  expect(await build(false)).toMatchObject({ skipped: true, gen: null });
});

test('a write committed after a later write still triggers a rebuild', async () => {
  await seedPipelineRows();
  await build(true);
  const [t1, t2] = [await pool.connect(), await pool.connect()];
  try {
    await t1.query('BEGIN');
    await t1.query(`UPDATE files SET state = state WHERE rel_path = 'p/IMG_1.JPG'`); // writes t1
    await t2.query(`UPDATE files SET state = state WHERE rel_path = 'p/IMG_1.AAE'`); // t2 > t1, commits
    expect((await build(false)).skipped).toBe(false); // sees t2, not T1's uncommitted row
    await t1.query('COMMIT'); // max(updated_at) is still t2
    expect((await build(false)).skipped).toBe(false);
    expect((await build(false)).skipped).toBe(true);
  } finally {
    t1.release();
    t2.release();
  }
});

/** Two look-alike shots (set 9): A is the set best, with a smaller JPEG copy; then B. */
async function seedSimilarSet(): Promise<{ a: string; a2: string; b: string }> {
  const [ba, ba2, bb] = [1, 2, 3].map(() => randomBytes(64));
  await pool.query(
    `INSERT INTO groups (id, kind, similar_set_id) VALUES (1, 'copy', 9), (2, 'copy', 9)`,
  );
  const a = await content(ba!, { ext: 'heic', kind: 'still', state: 'CLUSTERED', w: 40, group: 1 });
  const a2 = await content(ba2!, {
    ext: 'jpg',
    kind: 'still',
    state: 'CLUSTERED',
    w: 20,
    group: 1,
  });
  const b = await content(bb!, { ext: 'heic', kind: 'still', state: 'CLUSTERED', w: 30, group: 2 });
  await file('p/A.HEIC', a, 'heic');
  await file('p/A.JPG', a2, 'jpg');
  await file('p/B.HEIC', b, 'heic');
  await put(vaultRel(a, 'heic'), ba!);
  await put(vaultRel(a2, 'jpg'), ba2!);
  await put(vaultRel(b, 'heic'), bb!);
  return { a, a2, b };
}
const candidates = async () =>
  (
    await pool.query<{ set_id: bigint; sha256: string }>(
      'SELECT set_id, sha256 FROM similar_candidates ORDER BY set_id, sha256',
    )
  ).rows;
const setNames = async () =>
  (await tree(join(layout.staging, 'current')))
    .map(([p]) => p)
    .filter((p) => p.startsWith('similar/'))
    .map((p) => basename(p));

test('a build records each similar set’s candidates, replacing the previous ones', async () => {
  const { a, b } = await seedSimilarSet();
  await pool.query(`INSERT INTO similar_candidates (set_id, sha256) VALUES (4, 'stale')`);
  await build(true);
  expect(await candidates()).toEqual([a, b].sort().map((sha256) => ({ set_id: 9n, sha256 })));
  expect(await setNames()).toEqual(['20211003_140211_A.HEIC', '20211003_140211_B.HEIC']);
});

const changedAt = async () =>
  (
    await pool.query<{ t: string | null }>(
      'SELECT candidates_changed_at::text AS t FROM similar_state WHERE id = 1',
    )
  ).rows[0]!.t;

test('the candidates stamp moves only when set membership changes', async () => {
  await seedSimilarSet();
  expect(await changedAt()).toBeNull();
  await build(true);
  const first = await changedAt();
  expect(first).not.toBeNull();
  await build(true); // identical sets: no bump
  expect(await changedAt()).toBe(first);
  // Group 2 leaves set 9: one copy group left, so no set at all.
  await pool.query(`UPDATE groups SET similar_set_id = NULL WHERE id = 2`);
  await build(true);
  expect(await candidates()).toEqual([]);
  const second = await changedAt();
  expect(second).not.toBeNull();
  expect(second! > first!).toBe(true);
});

test('a new user pick rebuilds and marks that file', async () => {
  const { a, b } = await seedSimilarSet();
  await build(true);
  expect(await build(false)).toMatchObject({ skipped: true });
  await pool.query(
    `INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best)
     VALUES (9, $1, $2, false)`,
    [b, [a, b]],
  );
  expect((await build(false)).skipped).toBe(false);
  expect(await setNames()).toEqual(['0-best_20211003_140211_B.HEIC', '20211003_140211_A.HEIC']);
  expect(await build(false)).toMatchObject({ skipped: true });
});

test('a later no-clear-best pick is not a user pick', async () => {
  const { a, b } = await seedSimilarSet();
  await pool.query(
    `INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best, decided_at)
     VALUES (9, $1, $2, false, now() - interval '1 day'), (9, NULL, $2, true, now())`,
    [b, [a, b]],
  );
  await build(true);
  expect(await setNames()).toEqual(['0-best_20211003_140211_B.HEIC', '20211003_140211_A.HEIC']);
});

test('a newly ranked model rebuilds and marks its top candidate', async () => {
  const { a, a2, b } = await seedSimilarSet();
  await build(true);
  // An unpromoted or unranked model's scores are never used.
  await pool.query(
    `INSERT INTO ranker_models (version, feature_version, picks_used, promoted, path, ranked_at)
     VALUES (1, 1, 0, true, 'm1', now()), (2, 1, 0, false, 'm2', now()), (3, 1, 0, true, 'm3', NULL)`,
  );
  await pool.query(
    `INSERT INTO photo_scores (sha256, model_version, score)
     VALUES ($1, 1, 0.9), ($2, 1, 0.2), ($3, 1, 0.99),
            ($1, 2, 0.1), ($2, 2, 0.95), ($1, 3, 0.1), ($2, 3, 0.95)`,
    [a, b, a2],
  );
  expect((await build(false)).skipped).toBe(false);
  expect(await setNames()).toEqual(['0-best_20211003_140211_A.HEIC', '20211003_140211_B.HEIC']);
  expect(await build(false)).toMatchObject({ skipped: true });
});

test('with the AI tables empty an unchanged build still skips', async () => {
  await seedSimilarSet();
  await build(true);
  expect(await build(false)).toMatchObject({ skipped: true, gen: null });
});

test('a first build runs even with an empty database', async () => {
  expect(await build(false)).toEqual({ gen: 1, entries: 0, skipped: false });
  expect(await readdir(join(layout.staging, 'current'))).toEqual([]);
  expect(await build(false)).toMatchObject({ skipped: true });
});

test('waits while FileWorkflows run, unless stale', async () => {
  await seedPipelineRows();
  await build(true);
  await pool.query(`UPDATE files SET state = state`); // trigger bumps updated_at
  const busy = (stale: number) =>
    createActivities({
      ...deps,
      temporal: fakeTemporal(3),
      settings: { ...deps.settings, stageMaxStalenessS: stale },
    });
  expect((await runActivity(busy(900).maybeBuildStaging, [false])).skipped).toBe(true);
  expect((await runActivity(busy(0).maybeBuildStaging, [false])).skipped).toBe(false);
  expect(
    (
      await runActivity(
        createActivities({ ...deps, temporal: fakeTemporal(0) }).maybeBuildStaging,
        [true],
      )
    ).skipped,
  ).toBe(false);
});

test('a failure inside the read transaction surfaces as is, and the next build works', async () => {
  await seedPipelineRows();
  await build(true);
  await pool.query(`UPDATE files SET state = state`);
  const down = {
    workflow: { count: () => Promise.reject(new Error('temporal down')) },
  } as unknown as Client;
  const a = createActivities({ ...deps, temporal: down });
  await expect(runActivity(a.maybeBuildStaging, [false])).rejects.toThrow('temporal down');
  expect((await build(false)).gen).toBe(2);
});

test('a missing source fails the build and leaves current untouched', async () => {
  await seedPipelineRows();
  await build(true);
  const ghost = randomBytes(32).toString('hex');
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state) VALUES ($1, 'aae', 'other', 1, 'INGESTED')`,
    [ghost],
  );
  await file('GHOST.AAE', ghost, 'aae');
  await expect(build(true)).rejects.toThrow(/GHOST\.AAE|ENOENT/);
  expect(await readlink(join(layout.staging, 'current'))).toBe('.gen-1');
  expect((await readdir(layout.staging)).sort()).toEqual(['.gen-1', 'current']);
});

test('heartbeats every 1,000 links', async () => {
  // 2,001 different contents (byte copies of one content would stage once).
  const shas = Array.from({ length: 2001 }, (_, i) => (i + 1).toString(16).padStart(64, '0'));
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state)
     SELECT s, 'aae', 'other', 1, 'INGESTED' FROM unnest($1::text[]) s`,
    [shas],
  );
  await pool.query(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
     SELECT 'd' || i || '/X.AAE', 'X.AAE', 1, 0, 'aae', s, 'INGESTED'
       FROM unnest($1::text[]) WITH ORDINALITY AS t(s, i)`,
    [shas],
  );
  for (const s of shas) await put(vaultRel(s, 'aae'), Buffer.from(s));
  let beats = 0;
  const r = await runActivity(acts.maybeBuildStaging, [true], { onHeartbeat: () => beats++ });
  expect(r.entries).toBe(2001);
  expect(beats).toBe(2);
});

test('heartbeats while another build holds the lock', async () => {
  const holder = await pool.connect();
  try {
    await holder.query(`SELECT pg_advisory_lock(hashtext('pm.stage'))`);
    const beats: unknown[] = [];
    const run = runActivity(acts.maybeBuildStaging, [true], { onHeartbeat: (d) => beats.push(d) });
    await expect.poll(() => beats.length, { timeout: 5000 }).toBeGreaterThan(1);
    expect(beats[0]).toBe('waiting for pm.stage');
    await holder.query(`SELECT pg_advisory_unlock(hashtext('pm.stage'))`);
    expect((await run).gen).toBe(1);
  } finally {
    holder.release();
  }
});

test('heartbeats while removing a large orphan generation', async () => {
  const orphan = join(genDir(7), 'unique');
  await mkdir(orphan, { recursive: true });
  await Promise.all(
    Array.from({ length: 1500 }, (_, i) => writeFile(join(orphan, `${String(i)}.x`), '')),
  );
  const beats: unknown[] = [];
  await runActivity(acts.maybeBuildStaging, [true], { onHeartbeat: (d) => beats.push(d) });
  expect(beats).toEqual(['rm .gen-7']);
  expect(existsSync(genDir(7))).toBe(false);
  // Single-link files are nobody's staging link: they were moved aside, not deleted.
  expect(await readdir(join(layout.staging, '.rescued', '7', 'unique'))).toHaveLength(1500);
});

test.skipIf(process.getuid?.() === 0)('staged file is read-only', async () => {
  await seedPipelineRows();
  await build(true);
  const f = (await walk(join(layout.staging, 'current', 'unique')))[0]!;
  await expect(open(f, 'r+')).rejects.toMatchObject({ code: 'EACCES' });
});

test('files the user saved inside staging are rescued when their generation goes, never deleted', async () => {
  await seedPipelineRows();
  const g1 = (await build(true)).gen!;
  const cur = join(layout.staging, 'current');
  await writeFile(join(cur, 'unique', 'my-edit.jpg'), 'edited by hand'); // nlink 1: only copy
  await mkdir(join(cur, 'mine'));
  await writeFile(join(cur, 'mine', 'notes.txt'), 'notes');
  await symlink('../unique', join(cur, 'mine', 'alias'));
  // A rescue never overwrites: an earlier rescue at the same path keeps its bytes.
  await mkdir(join(layout.staging, '.rescued', String(g1), 'unique'), { recursive: true });
  await writeFile(join(layout.staging, '.rescued', String(g1), 'unique', 'my-edit.jpg'), 'older');

  await build(true);
  await build(true); // gen g1 is now old and removed
  expect(existsSync(genDir(g1))).toBe(false);
  const rescued = join(layout.staging, '.rescued', String(g1));
  expect(await readFile(join(rescued, 'unique', 'my-edit.jpg'), 'utf8')).toBe('older');
  expect(await readFile(join(rescued, 'unique', 'my-edit.jpg.1'), 'utf8')).toBe('edited by hand');
  expect(await readFile(join(rescued, 'mine', 'notes.txt'), 'utf8')).toBe('notes');
  expect(await readlink(join(rescued, 'mine', 'alias'))).toBe('../unique');
  // Staging hardlinks themselves are removed, not rescued.
  expect((await walk(rescued)).map((f) => relative(rescued, f)).sort()).toEqual([
    'mine/alias',
    'mine/notes.txt',
    'unique/my-edit.jpg',
    'unique/my-edit.jpg.1',
  ]);
});

test('a file saved over a staging link while its generation is being removed is never unlinked', async () => {
  await seedPipelineRows();
  const g1 = (await build(true)).gen!;
  await build(true);
  // The staging link an app (that resolved `current` earlier) saves over atomically.
  const target = (await walk(join(genDir(g1), 'unique')))[0]!;
  const tmp = join(layout.staging, 'app-save.tmp');
  let raced = false;
  fsHooks.unlink = (p) => {
    // The race: the cleanup has examined the link and is about to unlink it, and the app's
    // rename-over lands first.
    if (!raced && p.startsWith(genDir(g1)) && basename(p) === basename(target)) {
      raced = true;
      writeFileSync(tmp, 'saved by app');
      renameSync(tmp, target);
    }
  };
  await build(true); // removes g1; the saved file keeps the unique/ directory alive
  expect(raced).toBe(true);
  fsHooks.unlink = null;
  await build(true); // the next sweep finishes g1
  expect(existsSync(genDir(g1))).toBe(false);
  const rescued = join(layout.staging, '.rescued', String(g1));
  expect(await readFile(join(rescued, relative(genDir(g1), target)), 'utf8')).toBe('saved by app');
  // Only the user's file was rescued: the staging links themselves were removed.
  expect((await walk(rescued)).map((f) => relative(rescued, f))).toEqual([
    relative(genDir(g1), target),
  ]);
});

test('an entry a crash left under the private name is rescued under its public path', async () => {
  const left = join(genDir(7), 'unique', `.pm-reap-${randomUUID()}`);
  await mkdir(left, { recursive: true });
  await writeFile(join(left, 'mine.txt'), 'mine');
  await build(true);
  expect(existsSync(genDir(7))).toBe(false);
  expect(await readFile(join(layout.staging, '.rescued', '7', 'unique', 'mine.txt'), 'utf8')).toBe(
    'mine',
  );
});

test('an entry that vanishes before the cleanup claims it is skipped', async () => {
  const dir = join(genDir(7), 'unique');
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, 'a.txt'), 'a');
  await writeFile(join(dir, 'b.txt'), 'b');
  // Each name's claim is a rename of the public path: the first claim removes the other name.
  fsHooks.lstat = (p) => {
    if (p.includes('.pm-reap-'))
      rmSync(join(dir, basename(p) === 'a.txt' ? 'b.txt' : 'a.txt'), { force: true });
  };
  await build(true);
  expect(existsSync(genDir(7))).toBe(false);
  expect(await readdir(join(layout.staging, '.rescued', '7', 'unique'))).toHaveLength(1);
});

test('a generation directory that gains an entry during cleanup is left for the next build', async () => {
  await seedPipelineRows();
  const g1 = (await build(true)).gen!;
  await build(true);
  let once = true;
  fsHooks.rmdir = (p) => {
    if (p === genDir(g1) && once) {
      once = false;
      throw fsError('ENOTEMPTY');
    }
  };
  await build(true); // removing g1 meets ENOTEMPTY: tolerated
  expect(existsSync(genDir(g1))).toBe(true);
  await build(true); // the next build's sweep finishes it
  expect(existsSync(genDir(g1))).toBe(false);
});

test('any other failure while removing a generation fails the build (retryable), keeping files', async () => {
  await seedPipelineRows();
  const g1 = (await build(true)).gen!;
  await writeFile(join(genDir(g1), 'unique', 'mine.txt'), 'keep me');
  await build(true);
  fsHooks.lstat = (p) => {
    if (p.includes('.rescued')) throw fsError('EACCES');
  };
  await expect(build(true)).rejects.toMatchObject({ code: 'EACCES' });
  expect(await readFile(join(genDir(g1), 'unique', 'mine.txt'), 'utf8')).toBe('keep me');
  fsHooks.lstat = null;
  fsHooks.rmdir = (p) => {
    if (p === genDir(g1)) throw fsError('EBUSY');
  };
  await expect(build(true)).rejects.toMatchObject({ code: 'EBUSY' });
  fsHooks.rmdir = null;
  await build(true);
  expect(
    await readFile(join(layout.staging, '.rescued', String(g1), 'unique', 'mine.txt'), 'utf8'),
  ).toBe('keep me');
});
