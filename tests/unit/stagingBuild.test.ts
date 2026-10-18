import { createHash, randomBytes } from 'node:crypto';
import { existsSync } from 'node:fs';
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
import { dirname, join, relative } from 'node:path';
import type { Client } from '@temporalio/client';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { vaultRel, type DataLayout } from '../../src/fsops.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

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
async function seedPipelineRows(): Promise<{ a: string }> {
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
  return { a };
}

test('build stages the planned tree', async () => {
  const { a } = await seedPipelineRows();
  const r = await build(true);
  expect(r).toEqual({ gen: 1, entries: 5, skipped: false });
  expect((await tree(join(layout.staging, 'current'))).map(([p]) => p)).toEqual([
    `copies/g-${a.slice(0, 8)}/IMG_1.HEIC`,
    `copies/g-${a.slice(0, 8)}/IMG_1.JPG`,
    'other/p/IMG_1.AAE',
    'unique/2021/2021-10/20211003_140211_IMG_1.HEIC',
    'unique/2021/2021-10/20211003_140211_IMG_1.MOV',
  ]);
  const row = await pool.query<{ gen: bigint; cluster_version: bigint; entries: bigint }>(
    'SELECT gen, cluster_version, entries FROM staging_builds',
  );
  expect(row.rows).toEqual([{ gen: 1n, cluster_version: 0n, entries: 5n }]);
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
  const bytes = randomBytes(16);
  const s = await content(bytes, { ext: 'aae', kind: 'other', state: 'INGESTED' });
  await put(vaultRel(s, 'aae'), bytes);
  await pool.query(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
     SELECT 'd' || i || '/X.AAE', 'X.AAE', 1, 0, 'aae', $1, 'INGESTED' FROM generate_series(1, 2001) i`,
    [s],
  );
  let beats = 0;
  const r = await runActivity(acts.maybeBuildStaging, [true], { onHeartbeat: () => beats++ });
  expect(r.entries).toBe(2001);
  expect(beats).toBe(2);
});

test.skipIf(process.getuid?.() === 0)('staged file is read-only', async () => {
  await seedPipelineRows();
  await build(true);
  const f = (await walk(join(layout.staging, 'current', 'unique')))[0]!;
  await expect(open(f, 'r+')).rejects.toMatchObject({ code: 'EACCES' });
});
