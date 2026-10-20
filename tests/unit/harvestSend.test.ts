import { createHash, randomBytes } from 'node:crypto';
import { constants } from 'node:fs';
import * as fsp from 'node:fs/promises';
import {
  lstat,
  mkdir,
  readdir,
  readFile,
  rename,
  rmdir,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { dirname, join, relative } from 'node:path';
import { CancelledFailure } from '@temporalio/common';
import { MockActivityEnvironment } from '@temporalio/testing';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { createHarvestActivities, type HarvestDeps } from '../../src/harvest/activities.js';
import { loadHarvestSettings } from '../../src/harvest/config.js';
import { checkKnown } from '../../src/harvest/known.js';
import { realFsOps, type FsOps } from '../../src/harvest/paths.js';
import { createRun, runFolders, setRunState } from '../../src/harvest/runs.js';
import type { SendBatchResult } from '../../src/harvest/send.js';
import { registerSources, type SystemProbe } from '../../src/harvest/sources.js';
import { DataLayout } from '../../src/fsops.js';
import { runActivity } from '../helpers/activityEnv.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';
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

/** Read-only locks to undo so the temp trees can be removed. */
const unlocks: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const u of unlocks.splice(0)) await u();
});

const MUTATING = new Set([
  'mkdir',
  'open',
  'chmod',
  'utimes',
  'rename',
  'unlink',
  'copyFile',
  'link',
]);
interface Call {
  op: string;
  paths: string[];
  arg?: unknown;
}

/** Real filesystem ops that record every call (P6: no spying on ESM namespaces). */
function recording(base: FsOps = realFsOps): { ops: FsOps; calls: Call[] } {
  const calls: Call[] = [];
  const ops: FsOps = {
    mkdir: (p, o) => (calls.push({ op: 'mkdir', paths: [p] }), base.mkdir(p, o)),
    open: (p, f, m) => (calls.push({ op: 'open', paths: [p], arg: f }), base.open(p, f, m)),
    openRead: (p) => (calls.push({ op: 'openRead', paths: [p] }), base.openRead(p)),
    chmod: (p, m) => (calls.push({ op: 'chmod', paths: [p] }), base.chmod(p, m)),
    utimes: (p, a, m) => (calls.push({ op: 'utimes', paths: [p] }), base.utimes(p, a, m)),
    rename: (a, b) => (calls.push({ op: 'rename', paths: [a, b] }), base.rename(a, b)),
    unlink: (p) => (calls.push({ op: 'unlink', paths: [p] }), base.unlink(p)),
    link: (a, b) => (calls.push({ op: 'link', paths: [a, b] }), base.link(a, b)),
    copyFile: (a, b, m) => (
      calls.push({ op: 'copyFile', paths: [b], arg: { src: a, mode: m } }),
      base.copyFile(a, b, m)
    ),
    lstat: (p) => (calls.push({ op: 'lstat', paths: [p] }), base.lstat(p)),
    stat: (p) => (calls.push({ op: 'stat', paths: [p] }), base.stat(p)),
    realpath: (p) => base.realpath(p),
    mtimeNs: (p) => base.mtimeNs(p),
  };
  return { ops, calls };
}

const errno = (code: string): NodeJS.ErrnoException =>
  Object.assign(new Error(`${code}: simulated`), { code });

const OPEN_DISK = { blocks: 10n ** 12n, bavail: 10n ** 12n, bsize: 1n };

interface Env {
  root: string;
  sourceId: string;
  dz: string;
  deps: HarvestDeps;
  runId: string;
  folderId: string;
  /** Scans the run's folder (creates the PLANNED items). */
  scan: () => Promise<void>;
  send: (extra?: Partial<HarvestDeps>) => Promise<SendBatchResult>;
  /** A new run over the same folder, scanned; replaces runId. */
  rerun: () => Promise<void>;
}

async function setup(
  spec: TreeSpec,
  opts: {
    env?: Record<string, string>;
    statfs?: { blocks: bigint; bavail: bigint; bsize: bigint };
    lock?: boolean;
    modes?: Record<string, number>;
    mounted?: () => boolean;
    relPath?: string;
  } = {},
): Promise<Env> {
  const root = await makeSourceTree(spec);
  for (const [rel, m] of Object.entries(opts.modes ?? {})) await fsp.chmod(join(root, rel), m);
  const volumes = `${dirname(root)}/`;
  const sys: SystemProbe = {
    mountOutput: () =>
      Promise.resolve(
        (opts.mounted?.() ?? true) ? `/dev/disk9s1 on ${root} (exfat, local, nodev)\n` : '',
      ),
    diskutilInfo: () => Promise.resolve({ VolumeUUID: 'U-1', VolumeName: 'Drive' }),
    statfs: () => Promise.resolve(opts.statfs ?? OPEN_DISK),
  };
  const dataDir = await tmpDir();
  const layout = new DataLayout(dataDir);
  await layout.ensure();
  const settings = loadHarvestSettings(
    { DATA_DIR: dataDir, DATABASE_URL: pgh.url, HARVEST_MIN_SIZE: '100', ...opts.env },
    dataDir,
  );
  const [src] = await registerSources(pgh.pool, [
    { identity: 'local:U-1', labelBase: 'Drive', kind: 'local', mountPath: root },
  ]);
  const deps: HarvestDeps = {
    pool: pgh.pool,
    settings,
    layout,
    sys,
    detectOpts: { volumes },
  };
  if (opts.lock !== false) unlocks.push(await lockReadOnly(root));
  const env: Env = {
    root,
    sourceId: src!.id,
    dz: layout.dropzone,
    deps,
    runId: '',
    folderId: '',
    scan: async () => {
      const { scanFolder } = createHarvestActivities(deps);
      const r = await runActivity(scanFolder, [env.runId, env.folderId]);
      expect(r.outcome).toBe('DONE');
    },
    send: async (extra = {}) => {
      const { sendBatch } = createHarvestActivities({ ...deps, ...extra });
      return runActivity(sendBatch, [env.runId]);
    },
    rerun: async () => {
      env.runId = await createRun(
        pgh.pool,
        [{ sourceId: src!.id, relPath: opts.relPath ?? '' }],
        false,
      );
      env.folderId = (await runFolders(pgh.pool, env.runId))[0]!.folderId;
      await env.scan();
    },
  };
  await env.rerun();
  return env;
}

interface ItemRow {
  id: string;
  rel_path: string;
  state: string;
  dropzone_path: string | null;
  dropzone_mtime_ns: string | null;
  mtime_ns: string;
  size_bytes: string;
  attempts: number;
  last_error: string | null;
  duplicate_of: string | null;
  file_id: string | null;
  sent_at: Date | null;
}
const items = async (): Promise<ItemRow[]> =>
  (
    await pgh.pool.query<ItemRow>(
      `SELECT id::text AS id, rel_path, state, dropzone_path, dropzone_mtime_ns::text AS dropzone_mtime_ns,
              mtime_ns::text AS mtime_ns, size_bytes::text AS size_bytes, attempts, last_error,
              duplicate_of::text AS duplicate_of, file_id::text AS file_id, sent_at
       FROM harvest_items ORDER BY rel_path, id`,
    )
  ).rows;
const item = async (rel: string): Promise<ItemRow> => {
  const r = (await items()).filter((i) => i.rel_path === rel);
  return r[r.length - 1]!;
};

/** Every file under the drop zone, relative to it. */
async function listDz(dz: string): Promise<string[]> {
  const out: string[] = [];
  const walk = async (d: string): Promise<void> => {
    for (const e of await readdir(d, { withFileTypes: true })) {
      const p = join(d, e.name);
      if (e.isDirectory()) await walk(p);
      else out.push(relative(dz, p));
    }
  };
  await walk(dz);
  return out.sort();
}

const counters = async (runId: string) =>
  (
    await pgh.pool.query<{ sent: number; known: number; dups: number; bytes_sent: number }>(
      `SELECT sent::int, known::int, dups::int, bytes_sent::int FROM harvest_run_folders WHERE run_id = $1`,
      [runId],
    )
  ).rows[0]!;

const sha = (b: Buffer) => createHash('sha256').update(b).digest('hex');

describe('copy protocol', () => {
  test('sends photos atomically', async () => {
    const a = randomBytes(5000);
    const b = randomBytes(3000);
    const c = randomBytes(700);
    const e = await setup(
      { 'DCIM/a.cr2': a, 'DCIM/b.nef': b, 'x/c.cr2': c },
      { modes: { 'DCIM/a.cr2': 0o640, 'DCIM/b.nef': 0o604 } },
    );
    const beats: unknown[] = [];
    const { sendBatch } = createHarvestActivities(e.deps);
    const res = await runActivity(sendBatch, [e.runId], { onHeartbeat: (d) => beats.push(d) });
    expect(res).toMatchObject({ outcome: 'DONE', sent: 3, known: 0, dups: 0, failed: 0 });
    expect(beats.length).toBeGreaterThanOrEqual(3);
    for (const [rel, bytes] of [
      ['DCIM/a.cr2', a],
      ['DCIM/b.nef', b],
      ['x/c.cr2', c],
    ] as const) {
      const row = await item(rel);
      expect(row.state).toBe('SENT');
      expect(row.sent_at).not.toBeNull();
      expect(row.dropzone_path).toBe(`harvest/Drive/${rel}`);
      const dest = join(e.dz, row.dropzone_path!);
      expect(await readFile(dest)).toEqual(bytes);
      const ds = await lstat(dest, { bigint: true });
      const ss = await lstat(join(e.root, rel), { bigint: true });
      expect(ds.mtimeNs.toString()).toBe(row.dropzone_mtime_ns);
      const diff = ds.mtimeNs - ss.mtimeNs;
      expect(diff < 1000n && diff > -1000n).toBe(true);
      expect(Number(ds.mode) & 0o777).toBe((Number(ss.mode) & 0o777) | 0o600);
    }
    expect(Number((await lstat(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).mode) & 0o777).toBe(0o640);
    expect(Number((await lstat(join(e.dz, 'harvest/Drive/DCIM/b.nef'))).mode) & 0o777).toBe(0o604);
    expect((await listDz(e.dz)).filter((p) => p.endsWith('.part'))).toEqual([]);
    expect(await counters(e.runId)).toEqual({ sent: 3, known: 0, dups: 0, bytes_sent: 8700 });
  });

  test('sources untouched (snapshot, recorded ops, read-only opens)', async () => {
    const e = await setup({
      'DCIM/a.cr2': randomBytes(5000),
      'DCIM/sub/b.cr2': randomBytes(2000),
      'c.nef': randomBytes(900),
    });
    const before = await snapshotTree(e.root);
    // Byte copy path (clone refused) and clone path (real copyFile without the force flag).
    const noClone = recording({ ...realFsOps, copyFile: () => Promise.reject(errno('ENOTSUP')) });
    const res = await e.send({ fsOps: noClone.ops });
    expect(res.sent).toBe(3);
    const cloned = recording({
      ...realFsOps,
      copyFile: (s, d) => fsp.copyFile(s, d, constants.COPYFILE_EXCL),
    });
    await pgh.pool.query(`UPDATE harvest_items SET state = 'PLANNED', dropzone_path = NULL`);
    await fsp.rm(join(e.dz, 'harvest'), { recursive: true });
    expect((await e.send({ fsOps: cloned.ops })).sent).toBe(3);
    expect(await snapshotTree(e.root)).toEqual(before);
    for (const calls of [noClone.calls, cloned.calls]) {
      const bad = calls.filter(
        (c) => MUTATING.has(c.op) && c.paths.some((p) => p.startsWith(e.root)),
      );
      expect(bad).toEqual([]);
      for (const c of calls.filter((c) => c.op === 'open')) expect(c.arg).toBe('wx');
    }
    expect(noClone.calls.filter((c) => c.op === 'openRead').map((c) => c.paths[0])).toEqual([
      join(e.root, 'DCIM/a.cr2'),
      join(e.root, 'DCIM/sub/b.cr2'),
      join(e.root, 'c.nef'),
    ]);
  });

  test('no file in src/harvest except paths.ts imports a mutating fs function', async () => {
    const MUT =
      /\b(open|openSync|writeFile|writeFileSync|appendFile|rename|renameSync|chmod|lchmod|utimes|lutimes|futimes|unlink|unlinkSync|rm|rmSync|rmdir|mkdir|mkdirSync|mkdtemp|copyFile|copyFileSync|cp|link|symlink|truncate|ftruncate|createWriteStream|chown|lchown)\b/;
    const dir = 'src/harvest';
    const offenders: string[] = [];
    const files = (await readdir(dir, { recursive: true })).filter(
      (f) => /\.(ts|js)$/.test(f) && f !== 'paths.ts',
    );
    expect(files.length).toBeGreaterThan(5);
    for (const f of files) {
      const src = await readFile(join(dir, f), 'utf8');
      const re = /import\s+([^;]+?)\s+from\s+['"](?:node:)?fs(?:\/promises)?['"]/g;
      for (const m of src.matchAll(re)) {
        const clause = m[1]!;
        const ns = /\*\s+as\s+(\w+)/.exec(clause);
        if (ns) {
          const used = new RegExp(`\\b${ns[1]}\\.(\\w+)`, 'g');
          for (const u of src.matchAll(used)) if (MUT.test(u[1]!)) offenders.push(`${f}: ${u[0]}`);
          continue;
        }
        const named = /\{([^}]*)\}/.exec(clause)?.[1] ?? '';
        for (const n of named.split(',').map((s) => s.trim().split(/\s+as\s+/)[0]!)) {
          if (n && !n.startsWith('type ') && MUT.test(n)) offenders.push(`${f}: ${n}`);
        }
        if (!clause.trim().startsWith('{') && !clause.startsWith('type ') && !ns) {
          offenders.push(`${f}: default import ${clause}`);
        }
      }
      if (/import\(\s*['"](?:node:)?fs/.test(src)) offenders.push(`${f}: dynamic fs import`);
    }
    expect(offenders).toEqual([]);
  });

  test('case-colliding names get a suffix and never overwrite', async () => {
    const mine = randomBytes(4000);
    const e = await setup({ 'DCIM/IMG_1.CR2': mine, 'DCIM/B.CR2': randomBytes(1000) });
    // A pre-existing drop-zone file with another case, and another unlinked SENT item's name.
    const theirs = randomBytes(1234);
    await mkdir(join(e.dz, 'harvest/Drive/DCIM'), { recursive: true });
    await writeFile(join(e.dz, 'harvest/Drive/DCIM/img_1.cr2'), theirs);
    await pgh.pool.query(
      `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state, dropzone_path, sent_at)
       VALUES ($1, 'old/b.cr2', 'old/b.cr2', 1, 1, 'photo', 'SENT', 'harvest/Drive/DCIM/b.cr2', now())`,
      [e.sourceId],
    );
    let crashed = false;
    await expect(
      e.send({
        checkpoint: (n) => {
          if (n === 'send_renamed' && !crashed) {
            crashed = true;
            throw new Error('crash');
          }
        },
      }),
    ).rejects.toThrow('crash');
    const res = await e.send();
    expect(res).toMatchObject({ sent: 2 });
    expect((await item('DCIM/IMG_1.CR2')).dropzone_path).toBe('harvest/Drive/DCIM/IMG_1 (2).CR2');
    expect((await item('DCIM/B.CR2')).dropzone_path).toBe('harvest/Drive/DCIM/B (2).CR2');
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/img_1.cr2'))).toEqual(theirs);
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/IMG_1 (2).CR2'))).toEqual(mine);
    const listing = await listDz(e.dz);
    // Retry stable: nothing new on another batch or another run.
    expect((await e.send()).sent).toBe(0);
    await e.rerun();
    expect((await e.send()).sent).toBe(0);
    expect(await listDz(e.dz)).toEqual(listing);
    expect(listing.map((p) => p.normalize()).sort()).toEqual(
      [
        'harvest/Drive/DCIM/B (2).CR2',
        'harvest/Drive/DCIM/IMG_1 (2).CR2',
        'harvest/Drive/DCIM/img_1.cr2',
      ].sort(),
    );
  });

  test('a re-sent name whose earlier copy is still pending gets (2)', async () => {
    const first = randomBytes(2000);
    const e = await setup({ 'DCIM/a.cr2': first }, { lock: false });
    expect((await e.send()).sent).toBe(1);
    const second = randomBytes(2500);
    await writeFile(join(e.root, 'DCIM/a.cr2'), second);
    await e.rerun();
    expect((await e.send()).sent).toBe(1);
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(first);
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a (2).cr2'))).toEqual(second);
  });

  test('a publish that finds the name taken goes back for a new suffix', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(2000) });
    const intruder = randomBytes(10);
    const ops: FsOps = {
      ...realFsOps,
      link: async (a, b) => {
        if (b.endsWith('/a.cr2')) await writeFile(b, intruder, { flag: 'wx' });
        return realFsOps.link(a, b);
      },
    };
    expect((await e.send({ fsOps: ops })).sent).toBe(1);
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(intruder);
    expect((await item('DCIM/a.cr2')).dropzone_path).toBe('harvest/Drive/DCIM/a (2).cr2');
    expect((await listDz(e.dz)).filter((p) => p.endsWith('.part'))).toEqual([]);
  });
});

describe('crash recovery', () => {
  const crashAt = (name: string) => {
    let done = false;
    return (n: string) => {
      if (n === name && !done) {
        done = true;
        throw new Error(`crash at ${n}`);
      }
    };
  };

  test('crash after rename: the file still in the drop zone is recognised', async () => {
    const bytes = randomBytes(3000);
    const e = await setup({ 'DCIM/a.cr2': bytes });
    await expect(e.send({ checkpoint: crashAt('send_renamed') })).rejects.toThrow('crash');
    const mid = await item('DCIM/a.cr2');
    expect(mid.state).toBe('PLANNED');
    expect(mid.dropzone_mtime_ns).not.toBeNull();
    const rec = recording();
    const res = await e.send({ fsOps: rec.ops });
    expect(res).toMatchObject({ outcome: 'DONE', sent: 1 });
    expect(
      rec.calls.filter((c) => ['open', 'copyFile', 'openRead', 'link'].includes(c.op)),
    ).toEqual([]);
    expect((await item('DCIM/a.cr2')).state).toBe('SENT');
    expect(await listDz(e.dz)).toEqual(['harvest/Drive/DCIM/a.cr2']);
    expect(await counters(e.runId)).toMatchObject({ sent: 1, bytes_sent: 3000 });
  });

  test('crash after rename is recognised via the files table', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(3000) });
    await expect(e.send({ checkpoint: crashAt('send_renamed') })).rejects.toThrow('crash');
    const mid = await item('DCIM/a.cr2');
    // The pipeline claimed it: gone from the drop zone, a files row with the same facts.
    const away = join(await tmpDir(), 'claimed.cr2');
    await rename(join(e.dz, mid.dropzone_path!), away);
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state)
       VALUES ($1, 'a.cr2', 3000, $2, 'CLAIMED') RETURNING id::text AS id`,
      [mid.dropzone_path, mid.dropzone_mtime_ns],
    );
    const rec = recording();
    expect(await e.send({ fsOps: rec.ops })).toMatchObject({ sent: 1 });
    expect(rec.calls.filter((c) => ['open', 'copyFile', 'openRead'].includes(c.op))).toEqual([]);
    expect((await item('DCIM/a.cr2')).state).toBe('SENT');
    expect(await listDz(e.dz)).toEqual([]);
    const { linkFiles } = createHarvestActivities(e.deps);
    expect(await runActivity(linkFiles, [])).toBe(1);
    expect((await item('DCIM/a.cr2')).file_id).toBe(rows[0]!.id);
  });

  test('crash at send_part_written: the orphan .part is deleted and the file copied once', async () => {
    const bytes = randomBytes(3000);
    const e = await setup({ 'DCIM/a.cr2': bytes });
    await expect(e.send({ checkpoint: crashAt('send_part_written') })).rejects.toThrow('crash');
    expect((await listDz(e.dz)).filter((p) => p.endsWith('.part'))).toHaveLength(1);
    const rec = recording({ ...realFsOps, copyFile: () => Promise.reject(errno('ENOTSUP')) });
    expect(await e.send({ fsOps: rec.ops })).toMatchObject({ sent: 1 });
    expect(rec.calls.filter((c) => c.op === 'open')).toHaveLength(1);
    expect(await listDz(e.dz)).toEqual(['harvest/Drive/DCIM/a.cr2']);
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(bytes);
  });

  test('crash after the part is published but before it is unlinked', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(1500) });
    let linked = false;
    const ops: FsOps = {
      ...realFsOps,
      link: async (a, b) => {
        await realFsOps.link(a, b);
        linked = true;
      },
      unlink: (p) =>
        linked && p.endsWith('.part')
          ? Promise.reject(new Error('crash in unlink'))
          : realFsOps.unlink(p),
    };
    // The unlink error is an item error (the item stays PLANNED, attempts 1, part kept).
    expect(await e.send({ fsOps: ops })).toMatchObject({ sent: 0 });
    expect((await listDz(e.dz)).filter((p) => p.endsWith('.part'))).toHaveLength(1);
    expect(await e.send()).toMatchObject({ sent: 1 });
    expect(await listDz(e.dz)).toEqual(['harvest/Drive/DCIM/a.cr2']);
  });

  test('parent removed between mkdir and open is retried', async () => {
    const bytes = randomBytes(2000);
    const e = await setup({ 'DCIM/a.cr2': bytes });
    let removed = 0;
    const res = await e.send({
      checkpoint: async (n) => {
        if (n === 'send_dest_reserved' && removed === 0) {
          removed++;
          await rmdir(join(e.dz, 'harvest/Drive/DCIM'));
        }
      },
    });
    expect(removed).toBe(1);
    expect(res).toMatchObject({ sent: 1, failed: 0 });
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(bytes);
    expect((await item('DCIM/a.cr2')).attempts).toBe(0);
  });

  test('a parent that keeps vanishing is an item error after 3 retries', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(2000) });
    let opens = 0;
    const ops: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      open: async (p, f, m) => {
        opens++;
        await rmdir(dirname(p));
        return realFsOps.open(p, f, m);
      },
    };
    expect(await e.send({ fsOps: ops })).toMatchObject({ sent: 0 });
    expect(opens).toBe(4);
    const row = await item('DCIM/a.cr2');
    expect(row).toMatchObject({ state: 'PLANNED', attempts: 1 });
    expect(row.last_error).toMatch(/ENOENT/);
  });
});

describe('source changes', () => {
  test('source changed during the copy is not sent this time', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(2000) });
    const res = await e.send({
      checkpoint: async (n) => {
        if (n === 'send_part_written') await utimes(join(e.root, 'DCIM/a.cr2'), 5, 5);
      },
    });
    expect(res).toMatchObject({ sent: 0 });
    const row = await item('DCIM/a.cr2');
    expect(row.state).toBe('VANISHED');
    expect(row.last_error).toMatch(/changed/);
    expect(await listDz(e.dz)).toEqual([]);
  });

  test('a source changed or gone before its turn is VANISHED and its duplicates promoted', async () => {
    const same = randomBytes(2000);
    const e = await setup(
      { 'DCIM/a.cr2': same, 'DCIM/b.cr2': same, 'DCIM/c.cr2': randomBytes(900) },
      { lock: false },
    );
    // b becomes DUP of a (a still PLANNED, earlier) while a fails once.
    const failA: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      openRead: (p) =>
        p.endsWith('a.cr2') ? Promise.reject(errno('EACCES')) : realFsOps.openRead(p),
    };
    expect(await e.send({ fsOps: failA })).toMatchObject({ dups: 1, sent: 1 });
    expect((await item('DCIM/b.cr2')).state).toBe('DUP');
    await fsp.unlink(join(e.root, 'DCIM/a.cr2'));
    await utimes(join(e.root, 'DCIM/c.cr2'), 7, 7);
    await pgh.pool
      .query(`UPDATE harvest_items SET state = 'PLANNED', dropzone_path = NULL, dropzone_mtime_ns = NULL
       WHERE rel_path = 'DCIM/c.cr2'`);
    const res = await e.send();
    expect((await item('DCIM/a.cr2')).state).toBe('VANISHED');
    expect((await item('DCIM/c.cr2')).state).toBe('VANISHED');
    expect((await item('DCIM/b.cr2')).state).toBe('PLANNED');
    expect(res.outcome).toBe('MORE');
    expect(await e.send()).toMatchObject({ outcome: 'DONE', sent: 1 });
    expect((await item('DCIM/b.cr2')).state).toBe('SENT');
  });

  test('a name that is not valid UTF-8 is FAILED without a copy', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(500) });
    await pgh.pool.query(
      `UPDATE harvest_items SET rel_path_raw = '\\x44434d2f61ff2e637232'::bytea`,
    );
    expect(await e.send()).toMatchObject({ failed: 1, sent: 0 });
    expect((await item('DCIM/a.cr2')).state).toBe('FAILED');
  });

  test('unplugged before the batch: SOURCE_GONE, items stay PLANNED', async () => {
    let mounted = true;
    const e = await setup({ 'DCIM/a.cr2': randomBytes(500) }, { mounted: () => mounted });
    mounted = false;
    expect(await e.send()).toMatchObject({ outcome: 'SOURCE_GONE', reason: 'Drive unplugged' });
    expect(await item('DCIM/a.cr2')).toMatchObject({ state: 'PLANNED', attempts: 0 });
  });

  test('unplugged mid-copy: SOURCE_GONE, never FAILED, part removed', async () => {
    const e = await setup(
      { 'DCIM/a.cr2': randomBytes(500), 'DCIM/b.cr2': randomBytes(600) },
      {
        env: { HARVEST_MAX_ATTEMPTS: '1' },
      },
    );
    let gone = false;
    const ops: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      open: async (p, f, m) => {
        const fh = await realFsOps.open(p, f, m);
        gone = true;
        return fh;
      },
      // An errno outside the usual "gone" set: liveness, not the code, decides.
      openRead: (p) => (gone ? Promise.reject(errno('EBADF')) : realFsOps.openRead(p)),
      stat: (p) => (gone && p === e.root ? Promise.reject(errno('ENOENT')) : realFsOps.stat(p)),
    };
    // a: the part is opened, then the drive goes; b's read fails with EIO on a dead mount.
    const res = await e.send({ fsOps: ops });
    expect(res.outcome).toBe('SOURCE_GONE');
    for (const r of await items())
      expect(['PLANNED', 'SENT'].includes(r.state) && r.attempts === 0).toBe(true);
    expect((await listDz(e.dz)).filter((p) => p.endsWith('.part'))).toEqual([]);
  });
});

describe('errors and pauses', () => {
  test('ENOSPC pauses the run (clone path and byte copy)', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(4000) });
    const half: FsOps = {
      ...realFsOps,
      copyFile: async (_s, d) => {
        await writeFile(d, randomBytes(100), { flag: 'wx' });
        throw errno('ENOSPC');
      },
    };
    expect(await e.send({ fsOps: half })).toMatchObject({
      outcome: 'DISK_FULL',
      reason: 'disk full',
    });
    expect(await listDz(e.dz)).toEqual([]);
    expect(await item('DCIM/a.cr2')).toMatchObject({ state: 'PLANNED', attempts: 0 });

    const stream: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      open: async (p, f, m) => {
        const fh = await realFsOps.open(p, f, m);
        return new Proxy(fh, {
          get(t, k) {
            if (k === 'write')
              return async (...a: Parameters<typeof fh.write>) => {
                await t.write(...a);
                throw errno('ENOSPC');
              };
            const v = Reflect.get(t, k) as unknown;
            return typeof v === 'function' ? (v as (...x: unknown[]) => unknown).bind(t) : v;
          },
        });
      },
    };
    expect(await e.send({ fsOps: stream })).toMatchObject({ outcome: 'DISK_FULL' });
    expect(await listDz(e.dz)).toEqual([]);
    expect(await item('DCIM/a.cr2')).toMatchObject({ state: 'PLANNED', attempts: 0 });
  });

  test('a short copy is an item error and leaves no part', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(4000) });
    const short: FsOps = {
      ...realFsOps,
      copyFile: (_s, d) => writeFile(d, randomBytes(10), { flag: 'wx' }),
    };
    expect(await e.send({ fsOps: short })).toMatchObject({ outcome: 'MORE', sent: 0 });
    const row = await item('DCIM/a.cr2');
    expect(row).toMatchObject({ state: 'PLANNED', attempts: 1 });
    expect(row.last_error).toMatch(/short copy: 10 of 4000 bytes/);
    expect(await listDz(e.dz)).toEqual([]);
  });

  test('25 consecutive errors pause the run', async () => {
    const spec: TreeSpec = {};
    for (let i = 0; i < 26; i++) spec[`DCIM/f${String(i).padStart(2, '0')}.cr2`] = 200 + i;
    const e = await setup(spec);
    const ops: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      openRead: () => Promise.reject(errno('EACCES')),
    };
    const res = await e.send({ fsOps: ops });
    expect(res.outcome).toBe('PAUSED_ERRORS');
    expect(res.reason).toMatch(/DCIM\/f24\.cr2.*EACCES/);
    const rows = await items();
    expect(rows.filter((r) => r.attempts === 1)).toHaveLength(25);
    expect(rows[25]).toMatchObject({ attempts: 0, state: 'PLANNED' });
  });

  test('a success resets the consecutive error count', async () => {
    const spec: TreeSpec = {};
    for (let i = 0; i < 30; i++) spec[`DCIM/f${String(i).padStart(2, '0')}.cr2`] = 200 + i;
    const e = await setup(spec);
    const ops: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      openRead: (p) =>
        p.endsWith('f15.cr2') ? realFsOps.openRead(p) : Promise.reject(errno('EACCES')),
    };
    expect(await e.send({ fsOps: ops })).toMatchObject({ outcome: 'MORE', sent: 1 });
  });

  test('the attempts cap sets FAILED and promotes duplicates', async () => {
    const same = randomBytes(2000);
    const e = await setup(
      { 'DCIM/a.cr2': same, 'DCIM/b.cr2': same },
      { env: { HARVEST_MAX_ATTEMPTS: '2' } },
    );
    const failA: FsOps = {
      ...realFsOps,
      copyFile: () => Promise.reject(errno('ENOTSUP')),
      openRead: (p) =>
        p.endsWith('a.cr2') ? Promise.reject(errno('EACCES')) : realFsOps.openRead(p),
    };
    expect(await e.send({ fsOps: failA })).toMatchObject({ outcome: 'MORE', dups: 1, failed: 0 });
    expect(await e.send({ fsOps: failA })).toMatchObject({ outcome: 'MORE', failed: 1 });
    expect((await item('DCIM/a.cr2')).state).toBe('FAILED');
    expect(await item('DCIM/b.cr2')).toMatchObject({ state: 'PLANNED', duplicate_of: null });
    expect(await e.send({ fsOps: failA })).toMatchObject({ outcome: 'DONE', sent: 1 });
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/b.cr2'))).toEqual(same);
  });

  test('gate shut returns GATED with the reason and sends nothing', async () => {
    const e = await setup(
      { 'DCIM/a.cr2': randomBytes(500), 'DCIM/b.cr2': randomBytes(600) },
      { statfs: { blocks: 100n, bavail: 0n, bsize: 1n } },
    );
    const res = await e.send();
    expect(res).toMatchObject({ outcome: 'GATED', sent: 0, reason: 'disk 600.0 %' });
    expect(await listDz(e.dz)).toEqual([]);
    for (const r of await items())
      expect(r).toMatchObject({ state: 'PLANNED', dropzone_path: null });
  });
});

describe('known content', () => {
  test('known and duplicate items are not copied', async () => {
    const dup = randomBytes(2000);
    const vault = randomBytes(1800);
    const e = await setup({ 'DCIM/a.cr2': dup, 'DCIM/b.cr2': dup, 'DCIM/c.cr2': vault });
    await pgh.pool.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state) VALUES ($1, 'cr2', 'still', 1800, 'INGESTED')`,
      [sha(vault)],
    );
    expect(await e.send()).toMatchObject({ outcome: 'DONE', sent: 1, known: 1, dups: 1 });
    const a = await item('DCIM/a.cr2');
    expect(await item('DCIM/b.cr2')).toMatchObject({ state: 'DUP', duplicate_of: a.id });
    expect((await item('DCIM/c.cr2')).state).toBe('KNOWN');
    expect(await listDz(e.dz)).toEqual(['harvest/Drive/DCIM/a.cr2']);
    expect(await counters(e.runId)).toEqual({ sent: 1, known: 1, dups: 1, bytes_sent: 2000 });
  });

  test('a DUP whose target stopped being SENT/PLANNED is sent instead', async () => {
    const dup = randomBytes(2000);
    const e = await setup({ 'DCIM/a.cr2': dup, 'DCIM/b.cr2': dup });
    const res = await e.send({
      knownCheck: async (pool, it) => {
        const out = await checkKnown(pool, it);
        if (out.state === 'DUP')
          await pool.query(`UPDATE harvest_items SET state = 'VANISHED' WHERE id = $1`, [
            out.duplicateOf,
          ]);
        return out;
      },
    });
    expect(res).toMatchObject({ sent: 2, dups: 0 });
    expect((await item('DCIM/b.cr2')).state).toBe('SENT');
  });

  test('re-running a finished harvest sends 0', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(800), 'x/b.nef': randomBytes(900) });
    expect((await e.send()).sent).toBe(2);
    const listing = await listDz(e.dz);
    await e.rerun();
    expect(await e.send()).toMatchObject({ outcome: 'DONE', sent: 0, known: 0, dups: 0 });
    expect(await listDz(e.dz)).toEqual(listing);
  });

  test('only items under the run folders are sent, in folder order', async () => {
    const e = await setup(
      {
        'A/x.cr2': randomBytes(300),
        'A_b/y.cr2': randomBytes(300),
        'A%/z.cr2': randomBytes(300),
        'B/w.cr2': randomBytes(300),
      },
      { relPath: 'B' },
    );
    // Items exist for every folder (another run scanned the whole source).
    const whole = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    const { scanFolder } = createHarvestActivities(e.deps);
    await runActivity(scanFolder, [whole, (await runFolders(pgh.pool, whole))[0]!.folderId]);
    const run = await createRun(
      pgh.pool,
      [
        { sourceId: e.sourceId, relPath: 'B' },
        { sourceId: e.sourceId, relPath: 'A' },
      ],
      false,
    );
    const { sendBatch } = createHarvestActivities(e.deps);
    const beats: { item?: string }[] = [];
    const res = await runActivity(sendBatch, [run], {
      onHeartbeat: (d) => beats.push(d as { item?: string }),
    });
    expect(res.sent).toBe(2);
    expect(beats.map((b) => b.item).filter(Boolean)).toEqual(['B/w.cr2', 'A/x.cr2']);
    expect((await item('A_b/y.cr2')).state).toBe('PLANNED');
    expect((await item('A%/z.cr2')).state).toBe('PLANNED');
  });
});

describe('cancellation and clone', () => {
  test('cancellation deletes the part', async () => {
    const e = await setup({ 'DCIM/big.cr2': 64 * 1024 * 1024 });
    const env = new MockActivityEnvironment();
    env.on('heartbeat', (d: unknown) => {
      if ((d as { copying?: number }).copying) env.cancel();
    });
    const { sendBatch } = createHarvestActivities({
      ...e.deps,
      fsOps: { ...realFsOps, copyFile: () => Promise.reject(errno('ENOTSUP')) },
    });
    await expect(env.run(sendBatch, e.runId)).rejects.toBeInstanceOf(CancelledFailure);
    expect(await listDz(e.dz)).toEqual([]);
    expect(await item('DCIM/big.cr2')).toMatchObject({ state: 'PLANNED', attempts: 0 });
  });

  test('same APFS volume uses clone, falling back to a byte copy when cloning throws', async () => {
    const bytes = randomBytes(3000);
    const e = await setup({ 'DCIM/a.cr2': bytes, 'DCIM/b.cr2': randomBytes(1000) });
    // Real ops: macOS rejects FICLONE_FORCE (ENOSYS) → the byte copy is used.
    const rec = recording();
    expect((await e.send({ fsOps: rec.ops })).sent).toBe(2);
    const clones = rec.calls.filter((c) => c.op === 'copyFile');
    expect(clones).toHaveLength(2);
    for (const c of clones) {
      const mode = (c.arg as { mode: number }).mode;
      expect(mode & constants.COPYFILE_FICLONE_FORCE).toBe(constants.COPYFILE_FICLONE_FORCE);
      expect(mode & constants.COPYFILE_EXCL).toBe(constants.COPYFILE_EXCL);
    }
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(bytes);

    // Another device: no clone attempt at all.
    await pgh.pool.query(`UPDATE harvest_items SET state = 'PLANNED', dropzone_path = NULL`);
    await fsp.rm(join(e.dz, 'harvest'), { recursive: true });
    const other = recording({
      ...realFsOps,
      stat: async (p) => {
        const s = await realFsOps.stat(p);
        if (p !== e.dz) return s;
        const fake = Object.create(Object.getPrototypeOf(s) as object) as typeof s;
        return Object.assign(fake, s, { dev: s.dev + 1n });
      },
    });
    expect((await e.send({ fsOps: other.ops })).sent).toBe(2);
    expect(other.calls.filter((c) => c.op === 'copyFile')).toEqual([]);
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(bytes);
  });
});

describe('finishRun, linkFiles, setRunState', () => {
  test('finishRun writes folder history; linkFiles links by rel_path, size and mtime', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(800), 'DCIM/b.cr2': randomBytes(900) });
    expect((await e.send()).sent).toBe(2);
    const a = await item('DCIM/a.cr2');
    const b = await item('DCIM/b.cr2');
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state) VALUES
         ($1, 'a.cr2', 800, $2, 'INGESTED'), ($3, 'b.cr2', 900, $4, 'INGESTED')
       RETURNING id::text AS id`,
      // b's row carries the source mtime, not the drop-zone one: it must not link (T3-a).
      [
        a.dropzone_path,
        a.dropzone_mtime_ns,
        b.dropzone_path,
        (BigInt(b.dropzone_mtime_ns!) + 1n).toString(),
      ],
    );
    const { finishRun, linkFiles } = createHarvestActivities(e.deps);
    await runActivity(finishRun, [e.runId, 'done']);
    expect((await item('DCIM/a.cr2')).file_id).toBe(rows[0]!.id);
    expect((await item('DCIM/b.cr2')).file_id).toBeNull();
    expect(await runActivity(linkFiles, [])).toBe(0);
    const f = await pgh.pool.query<{
      last_harvested_at: Date | null;
      last_run_id: string;
      last_counts: Record<string, unknown>;
    }>(
      `SELECT last_harvested_at, last_run_id::text AS last_run_id, last_counts FROM harvest_folders`,
    );
    expect(f.rows[0]!.last_harvested_at).not.toBeNull();
    expect(f.rows[0]!.last_run_id).toBe(e.runId);
    expect(f.rows[0]!.last_counts).toMatchObject({
      photos: 2,
      sent: 2,
      known: 0,
      dups: 0,
      bytes_sent: 1700,
    });
    const r = await pgh.pool.query<{ state: string; finished_at: Date | null; error: null }>(
      `SELECT state, finished_at, error FROM harvest_runs WHERE id = $1`,
      [e.runId],
    );
    expect(r.rows[0]).toMatchObject({ state: 'done', error: null });
    expect(r.rows[0]!.finished_at).not.toBeNull();
    // Idempotent: a retry changes nothing.
    await runActivity(finishRun, [e.runId, 'done']);
  });

  test('cancelled and failed runs write only the run row; a dry run writes no history', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(800) });
    const { finishRun } = createHarvestActivities(e.deps);
    await runActivity(finishRun, [e.runId, 'cancelled']);
    const run2 = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    await runActivity(finishRun, [run2, 'failed', 'boom']);
    const dry = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], true);
    await runActivity(finishRun, [dry, 'done']);
    const f = await pgh.pool.query(`SELECT last_harvested_at, last_counts FROM harvest_folders`);
    expect(f.rows[0]).toMatchObject({ last_harvested_at: null, last_counts: null });
    const runs = await pgh.pool.query(`SELECT state, error FROM harvest_runs ORDER BY id`);
    expect(runs.rows).toEqual([
      { state: 'cancelled', error: null },
      { state: 'failed', error: 'boom' },
      { state: 'done', error: null },
    ]);
  });

  test('setRunState keeps paused_since within a reason class', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(800) });
    const since = async () =>
      (
        await pgh.pool.query<{
          s: Date | null;
          r: string | null;
          state: string;
          started: Date | null;
        }>(
          `SELECT paused_since AS s, paused_reason AS r, state, started_at AS started FROM harvest_runs WHERE id = $1`,
          [e.runId],
        )
      ).rows[0]!;
    const { setRunState: act } = createHarvestActivities(e.deps);
    await runActivity(act, [e.runId, 'sending']);
    expect((await since()).started).not.toBeNull();
    await setRunState(pgh.pool, e.runId, 'paused', 'disk 81.0 %');
    const s1 = (await since()).s!;
    expect(s1).not.toBeNull();
    await new Promise((r) => setTimeout(r, 20));
    await setRunState(pgh.pool, e.runId, 'paused', 'disk 82.5 %');
    expect((await since()).s).toEqual(s1);
    await setRunState(pgh.pool, e.runId, 'paused', 'stalled: the vault is filling the disk');
    expect((await since()).s).toEqual(s1);
    await setRunState(pgh.pool, e.runId, 'paused', 'backlog 20,000');
    const s2 = (await since()).s!;
    expect(s2.getTime()).toBeGreaterThan(s1.getTime());
    await setRunState(pgh.pool, e.runId, 'sending', null);
    expect(await since()).toMatchObject({ s: null, r: null, state: 'sending' });
  });
});
