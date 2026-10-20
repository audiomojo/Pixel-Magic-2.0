import { chmod, lstat, mkdir, readdir, rename, rm, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { createHarvestActivities, type HarvestDeps } from '../../src/harvest/activities.js';
import { loadHarvestSettings } from '../../src/harvest/config.js';
import { readMake } from '../../src/harvest/exifMake.js';
import { createRun, runFolders } from '../../src/harvest/runs.js';
import { byName, rawRelPath, upsertBatch, type ScanFolderResult } from '../../src/harvest/scan.js';
import { registerSources, type SystemProbe } from '../../src/harvest/sources.js';
import { DataLayout } from '../../src/fsops.js';
import { jpegWithMake } from '../helpers/exifFixtures.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';
import { runActivity } from '../helpers/activityEnv.js';
import {
  lockReadOnly,
  makeSourceTree,
  snapshotTree,
  type TreeSpec,
} from '../helpers/sourceTree.js';

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(() => pgh.truncate());

const isRoot = process.getuid?.() === 0;
const FIXTURE: TreeSpec = {
  'raw.cr2': 500,
  'DCIM/a.jpg': 500,
  'DCIM/._a.jpg': 500,
  'DCIM/.DS_Store': 10,
  'docs/note.txt': 500,
  'docs/pic.png': 500,
  'docs/tiny.png': 10,
  'docs/exif.jpg': jpegWithMake('Canon', 300),
  'docs/noexif.jpg': jpegWithMake(null, 300),
  'proj/package.json': 20,
  'proj/img.png': 500,
  'music/art.png': 500,
  'node_modules/x.jpg': 500,
  'link.jpg': { symlink: 'raw.cr2' },
  linkdir: { symlink: 'DCIM' },
};

interface Env {
  root: string;
  sourceId: string;
  deps: HarvestDeps;
  /** A new run over one folder; runs the activity with `opts` merged into the deps. */
  scan: (
    relPath?: string,
    opts?: Partial<HarvestDeps>,
  ) => Promise<{ runId: string; folderId: string; res: ScanFolderResult }>;
  /** A queued run, for tests that drive the activity themselves. */
  newRun: (relPath?: string) => Promise<{ runId: string; folderId: string }>;
}

async function setup(
  spec: TreeSpec,
  extra: Partial<HarvestDeps> = {},
  mounted = true,
): Promise<Env> {
  const root = await makeSourceTree(spec);
  const volumes = `${dirname(root)}/`;
  const sys: SystemProbe = {
    mountOutput: () =>
      Promise.resolve(mounted ? `/dev/disk9s1 on ${root} (exfat, local, nodev)\n` : ''),
    diskutilInfo: () => Promise.resolve({ VolumeUUID: 'U-1', VolumeName: 'Drive' }),
    statfs: () => Promise.resolve({ blocks: 1n, bavail: 1n, bsize: 1n }),
  };
  const dataDir = await tmpDir();
  const settings = loadHarvestSettings(
    { DATA_DIR: dataDir, DATABASE_URL: pgh.url, HARVEST_MIN_SIZE: '100' },
    dataDir,
  );
  const sources = await registerSources(pgh.pool, [
    { identity: 'local:U-1', labelBase: 'Drive', kind: 'local', mountPath: root },
  ]);
  const sourceId = sources[0]!.id;
  const deps: HarvestDeps = {
    pool: pgh.pool,
    settings,
    layout: new DataLayout(dataDir),
    sys,
    detectOpts: { volumes },
    ...extra,
  };
  const newRun = async (relPath = '') => {
    const runId = await createRun(pgh.pool, [{ sourceId, relPath }], false);
    const [rf] = await runFolders(pgh.pool, runId);
    return { runId, folderId: rf!.folderId };
  };
  const scan = async (relPath = '', opts: Partial<HarvestDeps> = {}) => {
    const { runId, folderId } = await newRun(relPath);
    const { scanFolder } = createHarvestActivities({ ...deps, ...opts });
    return { runId, folderId, res: await runActivity(scanFolder, [runId, folderId]) };
  };
  return { root, sourceId, deps, scan, newRun };
}

interface ItemRow {
  rel_path: string;
  kind: string;
  state: string;
  size: string;
  mtime: string;
  first: string;
  last: string;
  rel_path_raw: Buffer | null;
}

interface CounterRow {
  state: string;
  dirs: number;
  photos: number;
  maybes: number;
  junk: Record<string, number>;
  errors: number;
  scan_cursor: string | null;
}

const items = async () =>
  (
    await pgh.pool.query<ItemRow>(
      `SELECT rel_path, kind, state, size_bytes::text AS size, mtime_ns::text AS mtime,
              first_seen_run::text AS first, last_seen_run::text AS last, rel_path_raw
       FROM harvest_items ORDER BY rel_path, size_bytes`,
    )
  ).rows;

const counters = async (runId: string) =>
  (
    await pgh.pool.query<CounterRow>(
      `SELECT state, dirs::int, photos::int, maybes::int, junk, errors::int, scan_cursor
       FROM harvest_run_folders WHERE run_id = $1`,
      [runId],
    )
  ).rows[0]!;

describe('scanFolder', () => {
  test('classifies and records a fixture tree', async () => {
    const env = await setup(FIXTURE);
    const before = await snapshotTree(env.root);
    const { runId, res } = await env.scan();
    expect(res).toEqual({
      outcome: 'DONE',
      dirs: 5,
      photos: 3,
      maybes: 2,
      junk: {
        appledouble: 1,
        'system-file': 1,
        'too-small': 1,
        'in-project': 1,
        'graphics-folder': 1,
      },
      errors: 0,
    });
    expect(await counters(runId)).toMatchObject({
      state: 'done',
      dirs: 5,
      photos: 3,
      maybes: 2,
      errors: 0,
    });
    const rows = await items();
    expect(rows.map((r) => [r.rel_path, r.kind, r.state])).toEqual([
      ['DCIM/a.jpg', 'photo', 'PLANNED'],
      ['docs/exif.jpg', 'photo', 'PLANNED'],
      ['docs/noexif.jpg', 'maybe', 'REVIEW'],
      ['docs/pic.png', 'maybe', 'REVIEW'],
      ['raw.cr2', 'photo', 'PLANNED'],
    ]);
    expect(
      rows.every((r) => r.first === runId && r.last === runId && r.rel_path_raw === null),
    ).toBe(true);
    expect(await snapshotTree(env.root)).toEqual(before);
  });

  test('scans a read-only source tree', async () => {
    const env = await setup(FIXTURE);
    const unlock = await lockReadOnly(env.root);
    try {
      const { res } = await env.scan();
      expect(res).toMatchObject({ outcome: 'DONE', photos: 3 });
    } finally {
      await unlock();
    }
  });

  test('rescan is idempotent', async () => {
    const env = await setup(FIXTURE);
    const a = await env.scan();
    const first = await items();
    const b = await env.scan();
    const second = await items();
    expect(second).toHaveLength(first.length);
    expect(b.res).toEqual(a.res);
    expect(second.every((r) => r.first === a.runId && r.last === b.runId)).toBe(true);
    expect(second.map((r) => [r.rel_path, r.state])).toEqual(
      first.map((r) => [r.rel_path, r.state]),
    );
  });

  test('an existing row keeps its state and gets last_seen_run', async () => {
    const env = await setup({ 'a.cr2': 500 });
    await env.scan();
    await pgh.pool.query("UPDATE harvest_items SET state = 'SENT'");
    const b = await env.scan();
    const rows = await items();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ state: 'SENT', last: b.runId });
  });

  test('rescan with a 1 h mtime shift reuses the row', async () => {
    const env = await setup({ 'a.cr2': 500, 'b.cr2': 500 });
    await env.scan();
    const [r0] = await items();
    const p = join(env.root, 'a.cr2');
    const st = await lstat(p);
    await utimes(p, st.atime, new Date(st.mtimeMs + 3_600_000 + 1000));
    const b = await env.scan();
    const rows = await items();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ rel_path: 'a.cr2', mtime: r0!.mtime, last: b.runId });
  });

  test('a changed file is a new PLANNED item', async () => {
    const env = await setup({ 'a.cr2': 500 });
    await env.scan();
    await writeFile(join(env.root, 'a.cr2'), Buffer.alloc(700));
    await env.scan();
    const rows = await items();
    expect(rows.map((r) => [r.size, r.state])).toEqual([
      ['500', 'PLANNED'],
      ['700', 'PLANNED'],
    ]);
  });

  test('resumes after the cursor with the same counts and no duplicates', async () => {
    const env = await setup({
      'a/1.cr2': 500,
      'a/b/2.cr2': 500,
      'a/b/x.png': 500,
      'a/b/._junk.jpg': 500,
      'a-b/3.cr2': 500,
      'a b/4.cr2': 500,
      'c/5.cr2': 500,
      'c/d/6.png': 500,
      'e/7.cr2': 500,
      '8.cr2': 500,
    });
    const whole = await env.scan();
    await pgh.pool.query('TRUNCATE harvest_items RESTART IDENTITY');

    let n = 0;
    const { runId, folderId } = await env.newRun();
    const crashing = createHarvestActivities({
      ...env.deps,
      checkpoint: () => {
        if (++n === 2) throw new Error('crash');
      },
    }).scanFolder;
    await expect(runActivity(crashing, [runId, folderId])).rejects.toThrow('crash');
    expect(await counters(runId)).toMatchObject({ state: 'scanning', dirs: 2 });

    const res = await runActivity(createHarvestActivities(env.deps).scanFolder, [runId, folderId]);
    expect(res).toEqual(whole.res);
    expect(await counters(runId)).toMatchObject({ state: 'done', dirs: whole.res.dirs });
    const rows = await items();
    expect(rows).toHaveLength(whole.res.photos + whole.res.maybes);
    expect(new Set(rows.map((r) => r.rel_path)).size).toBe(rows.length);
  });

  test('a crash after any directory resumes to the same result', async () => {
    // Compares names as components: 'a b' and 'a-b' sort before 'a/…' as strings, not as paths.
    const env = await setup({
      'a/x.cr2': 500,
      'a/z/q.cr2': 500,
      'a b/y.cr2': 500,
      'a-b/z.cr2': 500,
    });
    const whole = await env.scan();
    for (let stopAfter = 1; stopAfter <= whole.res.dirs; stopAfter++) {
      await pgh.pool.query('TRUNCATE harvest_items RESTART IDENTITY');
      let n = 0;
      const { runId, folderId } = await env.newRun();
      const crashing = createHarvestActivities({
        ...env.deps,
        checkpoint: () => {
          if (++n === stopAfter) throw new Error('crash');
        },
      }).scanFolder;
      await expect(runActivity(crashing, [runId, folderId])).rejects.toThrow('crash');
      const res = await runActivity(createHarvestActivities(env.deps).scanFolder, [
        runId,
        folderId,
      ]);
      expect(res).toEqual(whole.res);
      expect(await items()).toHaveLength(4);
    }
  });

  test('unreadable directory is counted and skipped', async () => {
    if (isRoot) return;
    const env = await setup({ 'ok/a.cr2': 500, 'locked/b.cr2': 500 });
    const locked = join(env.root, 'locked');
    await chmod(locked, 0o000);
    const logs: string[] = [];
    try {
      const { runId, folderId } = await env.newRun();
      const res = await runActivity(
        createHarvestActivities(env.deps).scanFolder,
        [runId, folderId],
        { onLog: (l, m) => logs.push(`${l} ${m}`) },
      );
      expect(res).toMatchObject({ outcome: 'DONE', photos: 1, errors: 1, dirs: 2 });
      expect(logs.some((l) => l.startsWith('WARN') && l.includes('locked'))).toBe(true);
      expect((await items()).map((r) => r.rel_path)).toEqual(['ok/a.cr2']);
    } finally {
      await chmod(locked, 0o755);
    }
  });

  test('resuming with an unreadable directory at the cursor keeps the counts', async () => {
    if (isRoot) return;
    const env = await setup({ 'a/1.cr2': 500, 'locked/2.cr2': 500, 'z/3.cr2': 500 });
    const locked = join(env.root, 'locked');
    await chmod(locked, 0o000);
    try {
      const whole = await env.scan();
      expect(whole.res).toMatchObject({ errors: 1, dirs: 3, photos: 2 });
      let n = 0;
      const { runId, folderId } = await env.newRun();
      const crashing = createHarvestActivities({
        ...env.deps,
        checkpoint: () => {
          if (++n === 3) throw new Error('crash');
        },
      }).scanFolder;
      await expect(runActivity(crashing, [runId, folderId])).rejects.toThrow('crash');
      expect((await counters(runId)).scan_cursor).toBe('locked');
      const res = await runActivity(createHarvestActivities(env.deps).scanFolder, [
        runId,
        folderId,
      ]);
      expect(res).toEqual(whole.res);
    } finally {
      await chmod(locked, 0o755);
    }
  });

  test('a file whose EXIF cannot be read is counted as an error and not recorded', async () => {
    if (isRoot) return;
    const env = await setup({ 'd/p.jpg': jpegWithMake('Canon', 300), 'd/q.cr2': 500 });
    const p = join(env.root, 'd/p.jpg');
    await chmod(p, 0o000);
    try {
      const { res } = await env.scan();
      expect(res).toMatchObject({ photos: 1, maybes: 0, errors: 1 });
    } finally {
      await chmod(p, 0o644);
    }
  });

  test('an unreadable ancestor of the folder is counted, not fatal', async () => {
    if (isRoot) return;
    const env = await setup({ 'locked/sub/a.cr2': 500 });
    const locked = join(env.root, 'locked');
    await chmod(locked, 0o000);
    try {
      const { res } = await env.scan('locked/sub');
      expect(res).toMatchObject({ outcome: 'DONE', photos: 0, errors: 1 });
    } finally {
      await chmod(locked, 0o755);
    }
  });

  test('symlinks are not followed and not candidates', async () => {
    const env = await setup({
      'real/a.cr2': 500,
      'link.cr2': { symlink: 'real/a.cr2' },
      dirlink: { symlink: 'real' },
      'dangling.cr2': { symlink: 'nowhere' },
    });
    const { res } = await env.scan();
    expect(res).toMatchObject({ photos: 1, dirs: 2, errors: 0 });
    expect((await items()).map((r) => r.rel_path)).toEqual(['real/a.cr2']);
  });

  test('FAILED below the cap and VANISHED rows seen again become PLANNED', async () => {
    const env = await setup({
      'a.cr2': 500,
      'b.cr2': 600,
      'c.cr2': 700,
      'd.png': 500,
      'e.cr2': 800,
      'f.cr2': 900,
    });
    await env.scan();
    const set = (name: string, state: string, attempts = 0) =>
      pgh.pool.query('UPDATE harvest_items SET state = $2, attempts = $3 WHERE rel_path = $1', [
        name,
        state,
        attempts,
      ]);
    await set('a.cr2', 'FAILED', 4);
    await set('b.cr2', 'FAILED', 5);
    await set('c.cr2', 'VANISHED');
    await set('d.png', 'VANISHED');
    await set('e.cr2', 'KNOWN');
    await set('f.cr2', 'REJECTED');
    await env.scan();
    const states = Object.fromEntries((await items()).map((r) => [r.rel_path, r.state]));
    expect(states).toEqual({
      'a.cr2': 'PLANNED',
      'b.cr2': 'FAILED',
      'c.cr2': 'PLANNED',
      'd.png': 'REVIEW',
      'e.cr2': 'KNOWN',
      'f.cr2': 'REJECTED',
    });
  });

  test('SOURCE_GONE when the mount is absent', async () => {
    const env = await setup(FIXTURE, {}, false);
    const { runId, res } = await env.scan();
    expect(res).toMatchObject({ outcome: 'SOURCE_GONE', reason: 'Drive unplugged' });
    expect(res.dirs).toBe(0);
    expect((await counters(runId)).state).toBe('scanning');
  });

  test('a missing folder on a live drive is an error, not SOURCE_GONE', async () => {
    const env = await setup({ 'a/x.cr2': 500 });
    for (const rel of ['missing', 'a/missing', 'nope/sub']) {
      const { runId, res } = await env.scan(rel);
      expect(res).toMatchObject({ outcome: 'DONE', dirs: 0, errors: 1 });
      expect(await counters(runId)).toMatchObject({ state: 'done', errors: 1 });
    }
  });

  test('SOURCE_GONE when the mount root itself is gone', async () => {
    const env = await setup({ 'a/x.cr2': 500 });
    await rm(env.root, { recursive: true });
    expect((await env.scan('a')).res.outcome).toBe('SOURCE_GONE');
    expect((await env.scan('')).res.outcome).toBe('SOURCE_GONE');
  });

  test('an unplug part-way through a directory commits nothing of it; a resume records it all', async () => {
    const spec: TreeSpec = { 'a/1.cr2': 500 };
    for (let i = 0; i < 40; i++) spec[`docs/p${i}.jpg`] = jpegWithMake('Canon', 300);
    const env = await setup(spec);
    const backup = join(await tmpDir(), 'Drive');
    const { runId, folderId } = await env.newRun();
    let unplugged = false;
    const act = createHarvestActivities({
      ...env.deps,
      readMake: async (p) => {
        if (!unplugged) {
          unplugged = true;
          await rename(env.root, backup);
        }
        return readMake(p);
      },
    }).scanFolder;
    const gone = await runActivity(act, [runId, folderId]);
    expect(gone.outcome).toBe('SOURCE_GONE');
    expect(gone.errors).toBe(0);
    expect((await items()).map((r) => r.rel_path)).toEqual(['a/1.cr2']);
    expect((await counters(runId)).scan_cursor).toBe('a');

    await rename(backup, env.root);
    const done = await runActivity(createHarvestActivities(env.deps).scanFolder, [runId, folderId]);
    expect(done).toMatchObject({ outcome: 'DONE', photos: 41, errors: 0 });
    expect(await items()).toHaveLength(41);
  });

  test('SOURCE_GONE when the drive disappears mid-walk, and the scan resumes later', async () => {
    const env = await setup({ 'a/1.cr2': 500, 'b/2.cr2': 500 });
    const backup = join(await tmpDir(), 'b');
    const { runId, folderId } = await env.newRun();
    let n = 0;
    const act = createHarvestActivities({
      ...env.deps,
      checkpoint: async () => {
        if (++n === 2) {
          await rename(join(env.root, 'b'), backup);
          await rm(env.root, { recursive: true });
        }
      },
    }).scanFolder;
    const gone = await runActivity(act, [runId, folderId]);
    expect(gone).toMatchObject({ outcome: 'SOURCE_GONE', dirs: 2 });
    await mkdir(join(env.root, 'a'), { recursive: true });
    await rename(backup, join(env.root, 'b'));
    const done = await runActivity(createHarvestActivities(env.deps).scanFolder, [runId, folderId]);
    expect(done).toMatchObject({ outcome: 'DONE', dirs: 3 });
  });

  test('a selected subfolder keeps its ancestors context; rel_path is source-relative', async () => {
    const env = await setup({
      'Pictures/trip/x.jpg': jpegWithMake(null, 300),
      'Pictures/trip/y.png': 500,
      'proj/package.json': 20,
      'proj/sub/i.png': 500,
      'node_modules/q/w.cr2': 500,
    });
    expect((await env.scan('Pictures/trip')).res).toMatchObject({
      photos: 1,
      maybes: 1,
      dirs: 1,
    });
    expect((await items()).map((r) => [r.rel_path, r.kind])).toEqual([
      ['Pictures/trip/x.jpg', 'photo'],
      ['Pictures/trip/y.png', 'maybe'],
    ]);
    const proj = await env.scan('proj/sub');
    expect(proj.res).toMatchObject({ photos: 0, maybes: 0, junk: { 'in-project': 1 } });
    const pruned = await env.scan('node_modules/q');
    expect(pruned.res).toMatchObject({ outcome: 'DONE', dirs: 0, photos: 0 });
    expect((await counters(pruned.runId)).state).toBe('done');
    expect(await items()).toHaveLength(2);
  });

  test('rejects a folder path that escapes the source', async () => {
    const env = await setup({ 'a.cr2': 500 });
    await expect(env.scan('../x')).rejects.toThrow(/invalid folder path/);
    await expect(env.scan('a//b')).rejects.toThrow(/invalid folder path/);
  });

  test('unknown folder or run-folder pairs fail', async () => {
    const env = await setup({ 'a.cr2': 500 });
    const { scanFolder } = createHarvestActivities(env.deps);
    await expect(runActivity(scanFolder, ['1', '999'])).rejects.toThrow(/not found/);
    const a = await env.newRun('');
    const b = await env.newRun('x');
    await expect(runActivity(scanFolder, [a.runId, b.folderId])).rejects.toThrow(/not part of run/);
  });

  test('heartbeats with dirs and the cursor', async () => {
    const env = await setup({ 'a/1.cr2': 500, 'b/2.cr2': 500 });
    const beats: unknown[] = [];
    const { runId, folderId } = await env.newRun();
    const act = createHarvestActivities({
      ...env.deps,
      scanHeartbeatMs: 5,
      checkpoint: () => sleep(30),
    }).scanFolder;
    await runActivity(act, [runId, folderId], { onHeartbeat: (d) => beats.push(d) });
    expect(beats.length).toBeGreaterThan(0);
    const last = beats.at(-1) as { dirs: number; cursor: string };
    expect(typeof last.dirs).toBe('number');
    expect(typeof last.cursor).toBe('string');
  });

  test('batches large directories (> 500 files)', async () => {
    const spec: TreeSpec = {};
    for (let i = 0; i < 1100; i++) spec[`big/f${i}.cr2`] = 100;
    const env = await setup(spec);
    const { res } = await env.scan();
    expect(res.photos).toBe(1100);
    expect(await items()).toHaveLength(1100);
  });

  test('an NFD name keeps its on-disk bytes in rel_path_raw (file and directory)', async () => {
    const nfd = 'Cafe\u0301';
    const env = await setup({ [`${nfd}/x.cr2`]: 500, [`p/${nfd}.cr2`]: 500, 'p/plain.cr2': 500 });
    // APFS returns names as created: readdir gives the NFD form back.
    expect((await readdir(join(env.root, 'p'))).includes(`${nfd}.cr2`)).toBe(true);
    await env.scan();
    const rows = await items();
    const byRel = new Map(rows.map((r) => [r.rel_path, r]));
    const nfc = nfd.normalize('NFC');
    expect(byRel.get(`${nfc}/x.cr2`)!.rel_path_raw).toEqual(Buffer.from(`${nfd}/x.cr2`));
    expect(byRel.get(`p/${nfc}.cr2`)!.rel_path_raw).toEqual(Buffer.from(`p/${nfd}.cr2`));
    expect(byRel.get('p/plain.cr2')!.rel_path_raw).toBeNull();
  });

  test('rel_path is NFC and path_key is lower-cased', async () => {
    const env = await setup({ 'café.CR2': 500 });
    await env.scan();
    expect((await items())[0]!.rel_path).toBe('café.CR2');
    const { rows } = await pgh.pool.query<{ path_key: string }>(
      'SELECT path_key FROM harvest_items',
    );
    expect(rows[0]!.path_key).toBe('café.cr2');
  });

  test('a non-UTF-8 name is stored with rel_path_raw when the filesystem allows it', async () => {
    const env = await setup({ 'd/ok.cr2': 500 });
    const tail = Buffer.concat([
      Buffer.from('bad'),
      Buffer.from([0xff, 0xfe]),
      Buffer.from('.cr2'),
    ]);
    try {
      await writeFile(
        Buffer.concat([Buffer.from(join(env.root, 'd') + '/'), tail]),
        Buffer.alloc(500),
      );
    } catch {
      return; // APFS rejects it; the encoder path is covered by the unit tests below
    }
    await env.scan();
    const bad = (await items()).find((r) => r.rel_path_raw);
    expect(bad!.rel_path_raw).toEqual(Buffer.concat([Buffer.from('d/'), tail]));
    expect(bad!.rel_path).toContain('�');
  });
});

describe('raw path encoding', () => {
  test('byName orders by code unit, then by raw bytes', () => {
    const e = (name: string, raw: number[] | null = null) => ({
      name,
      raw: raw && Buffer.from(raw),
      isDir: true,
      isFile: false,
    });
    const names = [e('b'), e('B'), e('a b'), e('a-b'), e('a')].sort(byName).map((x) => x.name);
    expect(names).toEqual(['B', 'a', 'a b', 'a-b', 'b']);
    expect(byName(e('x\uFFFD', [0xfe]), e('x\uFFFD', [0xff]))).toBeLessThan(0);
    expect(byName(e('x\uFFFD', [0xff]), e('x\uFFFD', [0xfe]))).toBeGreaterThan(0);
    expect(byName(e('x\uFFFD', [0xff]), e('x\uFFFD'))).toBeGreaterThan(0);
    expect(byName(e('x\uFFFD'), e('x\uFFFD', [0xff]))).toBeLessThan(0);
    expect(byName(e('x'), e('x'))).toBe(0);
  });

  test('rawRelPath keeps original bytes only when something is not UTF-8', () => {
    expect(rawRelPath('a', null, { name: 'x', raw: null })).toBeNull();
    const bad = Buffer.from([0x62, 0xff]);
    expect(rawRelPath('a/b', null, { name: 'b�', raw: bad })).toEqual(
      Buffer.concat([Buffer.from('a/b/'), bad]),
    );
    expect(rawRelPath('', null, { name: 'b�', raw: bad })).toEqual(bad);
    const parent = Buffer.from([0x70, 0xff]);
    expect(rawRelPath('p�', parent, { name: 'x.cr2', raw: null })).toEqual(
      Buffer.concat([parent, Buffer.from('/x.cr2')]),
    );
    expect(rawRelPath('', Buffer.from([0xff]), { name: 'x', raw: null })).toEqual(
      Buffer.concat([Buffer.from([0xff]), Buffer.from('/x')]),
    );
  });

  test('upsertBatch stores rel_path_raw bytes and ignores twin candidates', async () => {
    const sources = await registerSources(pgh.pool, [
      { identity: 'local:Z', labelBase: 'Z', kind: 'local', mountPath: '/Volumes/Z' },
    ]);
    const bad = Buffer.concat([Buffer.from('d/bad'), Buffer.from([0xff]), Buffer.from('.cr2')]);
    const cand = (rel: string, raw: Buffer | null) => ({
      rel,
      raw,
      key: rel.toLowerCase(),
      size: 500n,
      mtimeNs: 1_700_000_000_123_456_789n,
      kind: 'photo' as const,
    });
    const batch = [cand('d/bad�.cr2', bad), cand('d/bad�.cr2', bad), cand('d/ok.cr2', null)];
    const c = await pgh.pool.connect();
    try {
      await upsertBatch(c, sources[0]!.id, '7', 5, batch);
      await upsertBatch(c, sources[0]!.id, '8', 5, batch);
    } finally {
      c.release();
    }
    const rows = await items();
    expect(rows).toHaveLength(2);
    expect(rows.find((r) => r.rel_path_raw)!.rel_path_raw).toEqual(bad);
    expect(
      rows.every((r) => r.last === '8' && r.first === '7' && r.mtime === '1700000000123456789'),
    ).toBe(true);
  });
});

describe('runs', () => {
  test('runFolders returns selection order even when ids are reversed', async () => {
    const sources = await registerSources(pgh.pool, [
      { identity: 'local:P', labelBase: 'P', kind: 'local', mountPath: '/Volumes/P' },
    ]);
    const sid = sources[0]!.id;
    await createRun(pgh.pool, [{ sourceId: sid, relPath: 'low' }], false);
    const run = await createRun(
      pgh.pool,
      [
        { sourceId: sid, relPath: 'new' },
        { sourceId: sid, relPath: 'low' },
      ],
      false,
    );
    expect((await runFolders(pgh.pool, run)).map((f) => f.relPath)).toEqual(['new', 'low']);
  });

  test('createRun upserts folders and runFolders lists them', async () => {
    const sources = await registerSources(pgh.pool, [
      { identity: 'local:R', labelBase: 'R', kind: 'local', mountPath: '/Volumes/R' },
    ]);
    const sid = sources[0]!.id;
    const a = await createRun(
      pgh.pool,
      [
        { sourceId: sid, relPath: 'x' },
        { sourceId: sid, relPath: 'y' },
        { sourceId: sid, relPath: 'x' },
      ],
      true,
    );
    const b = await createRun(pgh.pool, [{ sourceId: sid, relPath: 'y' }], false);
    expect(await runFolders(pgh.pool, a)).toMatchObject([
      { sourceId: sid, relPath: 'x', state: 'queued' },
      { sourceId: sid, relPath: 'y', state: 'queued' },
    ]);
    expect((await runFolders(pgh.pool, b)).map((f) => f.relPath)).toEqual(['y']);
    const { rows } = await pgh.pool.query('SELECT dry_run, state FROM harvest_runs ORDER BY id');
    expect(rows).toEqual([
      { dry_run: true, state: 'queued' },
      { dry_run: false, state: 'queued' },
    ]);
    const n = await pgh.pool.query<{ n: number }>('SELECT count(*)::int AS n FROM harvest_folders');
    expect(n.rows[0]!.n).toBe(2);
  });
});
