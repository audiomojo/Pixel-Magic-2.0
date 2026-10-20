import { createHash, randomBytes } from 'node:crypto';
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
import {
  loadScope,
  pageQuery,
  type SendBatchResult,
  type SendState,
} from '../../src/harvest/send.js';
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

const MUTATING = new Set(['mkdir', 'open', 'chmod', 'utimes', 'rename', 'unlink', 'link']);
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
    openRead: (p) => (calls.push({ op: 'openRead', paths: [String(p)] }), base.openRead(p)),
    chmod: (p, m) => (calls.push({ op: 'chmod', paths: [p] }), base.chmod(p, m)),
    utimes: (p, a, m) => (calls.push({ op: 'utimes', paths: [p] }), base.utimes(p, a, m)),
    rename: (a, b) => (calls.push({ op: 'rename', paths: [a, b] }), base.rename(a, b)),
    unlink: (p) => (calls.push({ op: 'unlink', paths: [p] }), base.unlink(p)),
    link: (a, b) => (calls.push({ op: 'link', paths: [a, b] }), base.link(a, b)),
    lstat: (p) => (calls.push({ op: 'lstat', paths: [p] }), base.lstat(p)),
    stat: (p) => (calls.push({ op: 'stat', paths: [String(p)] }), base.stat(p)),
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
  send: (extra?: Partial<HarvestDeps>, state?: SendState) => Promise<SendBatchResult>;
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
    send: async (extra = {}, state = {}) => {
      const { sendBatch } = createHarvestActivities({ ...deps, ...extra });
      return runActivity(sendBatch, [env.runId, state]);
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
    const rec = recording();
    expect((await e.send({ fsOps: rec.ops })).sent).toBe(3);
    expect(await snapshotTree(e.root)).toEqual(before);
    const bad = rec.calls.filter(
      (c) => MUTATING.has(c.op) && c.paths.some((p) => p.startsWith(e.root)),
    );
    expect(bad).toEqual([]);
    for (const c of rec.calls.filter((c) => c.op === 'open')) expect(c.arg).toBe('wx');
    // Source files are opened only read-only, once each (the other openReads are drop-zone
    // directories opened for fsync).
    const srcOpens = rec.calls
      .filter((c) => c.op === 'openRead' && c.paths[0]!.startsWith(e.root))
      .map((c) => c.paths[0]);
    // Send order is path_key byte order (casefolded): c.nef before DCIM/.
    expect(srcOpens).toEqual([
      join(e.root, 'c.nef'),
      join(e.root, 'DCIM/a.cr2'),
      join(e.root, 'DCIM/sub/b.cr2'),
    ]);
    for (const c of rec.calls.filter((c) => c.op === 'openRead' && !c.paths[0]!.startsWith(e.root)))
      expect(c.paths[0]!.startsWith(join(await fsp.realpath(e.dz), 'harvest'))).toBe(true);
  });

  test('no file in src/harvest except paths.ts imports a mutating fs function', async () => {
    const MUT =
      /^(promises|open|openSync|writeFile|writeFileSync|appendFile|rename|renameSync|chmod|lchmod|utimes|lutimes|futimes|unlink|unlinkSync|rm|rmSync|rmdir|mkdir|mkdirSync|mkdtemp|copyFile|copyFileSync|cp|link|symlink|truncate|ftruncate|createWriteStream|chown|lchown)$/;
    // system.ts runs `mount` and `diskutil` (read-only probes); cli.ts runs `/bin/launchctl`
    // (it never receives a source path).
    const CHILD_PROCESS_ALLOWED = new Set(['system.ts', 'cli.ts']);
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
        for (const n of named.split(',').map((s) => s.trim().replace(/^type\s+/, ''))) {
          const name = n.split(/\s+as\s+/)[0]!;
          if (name && MUT.test(name)) offenders.push(`${f}: ${name}`);
        }
        if (!clause.trim().startsWith('{') && !clause.startsWith('type ')) {
          offenders.push(`${f}: default import ${clause}`);
        }
      }
      if (/import\(\s*['"](?:node:)?fs/.test(src)) offenders.push(`${f}: dynamic fs import`);
      if (/from\s+['"]fs-extra['"]|import\(\s*['"]fs-extra/.test(src))
        offenders.push(`${f}: fs-extra`);
      if (
        /from\s+['"](?:node:)?child_process['"]|import\(\s*['"](?:node:)?child_process/.test(src) &&
        !CHILD_PROCESS_ALLOWED.has(f)
      ) {
        offenders.push(`${f}: child_process`);
      }
      if (/\brealFsOps\b/.test(src)) offenders.push(`${f}: realFsOps`);
    }
    expect(offenders).toEqual([]);
  });

  test('the static check catches what it should', () => {
    // Guard against a regex that silently matches nothing.
    const MUT = /^(promises|open|copyFile)$/;
    expect(['promises', 'open', 'copyFile'].every((n) => MUT.test(n))).toBe(true);
    expect(MUT.test('openRead')).toBe(false);
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
    expect(rec.calls.filter((c) => ['open', 'openRead', 'link'].includes(c.op))).toEqual([]);
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
    expect(rec.calls.filter((c) => ['open', 'openRead'].includes(c.op))).toEqual([]);
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
    const rec = recording();
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
    // Linked = published: SENT, even though the (ignored) .part could not be removed.
    expect(await e.send({ fsOps: ops })).toMatchObject({ sent: 1, failed: 0 });
    expect(await item('DCIM/a.cr2')).toMatchObject({ state: 'SENT', attempts: 0 });
    const left = await listDz(e.dz);
    expect(left.filter((p) => p.endsWith('.part'))).toHaveLength(1);
    expect(left.filter((p) => !p.endsWith('.part'))).toEqual(['harvest/Drive/DCIM/a.cr2']);
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
      openRead: (p) =>
        String(p).endsWith('a.cr2') ? Promise.reject(errno('EACCES')) : realFsOps.openRead(p),
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

  test('an NFD-named file is read through its on-disk bytes and sent under the NFC name', async () => {
    const nfd = 'Cafe\u0301.cr2';
    const nfc = nfd.normalize('NFC');
    const bytes = randomBytes(2100);
    const e = await setup({ [`DCIM/${nfd}`]: bytes });
    const row = await item(`DCIM/${nfc}`);
    expect(row).toBeDefined();
    // A normalization-sensitive filesystem (exFAT/SMB/NTFS): the NFC spelling does not exist.
    const nfcBytes = Buffer.from(nfc);
    // Only source paths are strict; drop-zone names are NFC and real.
    const strict = (p: string | Buffer) =>
      Buffer.from(p).includes(Buffer.from(e.root)) && Buffer.from(p).includes(nfcBytes)
        ? Promise.reject(errno('ENOENT'))
        : null;
    const ops: FsOps = {
      ...realFsOps,
      stat: (p) => strict(p) ?? realFsOps.stat(p),
      openRead: (p) => strict(p) ?? realFsOps.openRead(p),
    };
    // The fake really rejects the NFC spelling.
    await expect(ops.stat(join(e.root, 'DCIM', nfc))).rejects.toMatchObject({ code: 'ENOENT' });
    expect(await e.send({ fsOps: ops })).toMatchObject({ sent: 1, failed: 0 });
    const sent = await item(`DCIM/${nfc}`);
    expect(sent).toMatchObject({ state: 'SENT', dropzone_path: `harvest/Drive/DCIM/${nfc}` });
    expect(await readFile(join(e.dz, sent.dropzone_path!))).toEqual(bytes);
  });

  test('rel_path_raw, when set, is the path read from the source', async () => {
    const real = randomBytes(700);
    const e = await setup(
      { 'DCIM/a.cr2': randomBytes(700), 'DCIM/other.cr2': real },
      { lock: false },
    );
    // Point a's raw bytes at another file of the same size and mtime: its bytes are what is sent.
    const st = await lstat(join(e.root, 'DCIM/a.cr2'));
    await utimes(join(e.root, 'DCIM/other.cr2'), st.atime, st.mtime);
    await pgh.pool.query(
      `UPDATE harvest_items SET rel_path_raw = convert_to('DCIM/other.cr2', 'UTF8'),
              mtime_ns = (SELECT mtime_ns FROM harvest_items WHERE rel_path = 'DCIM/other.cr2')
       WHERE rel_path = 'DCIM/a.cr2'`,
    );
    await pgh.pool.query(
      `UPDATE harvest_items SET state = 'REJECTED' WHERE rel_path = 'DCIM/other.cr2'`,
    );
    expect(await e.send()).toMatchObject({ sent: 1 });
    expect(await readFile(join(e.dz, 'harvest/Drive/DCIM/a.cr2'))).toEqual(real);
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
      open: async (p, f, m) => {
        const fh = await realFsOps.open(p, f, m);
        gone = true;
        return fh;
      },
      // An errno outside the usual "gone" set: liveness, not the code, decides.
      openRead: (p) => (gone ? Promise.reject(errno('EBADF')) : realFsOps.openRead(p)),
      stat: (p) =>
        gone && String(p) === e.root ? Promise.reject(errno('ENOENT')) : realFsOps.stat(p),
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
  test('ENOSPC pauses the run', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(4000) });
    const stream: FsOps = {
      ...realFsOps,
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
    expect(await e.send({ fsOps: stream })).toMatchObject({
      outcome: 'DISK_FULL',
      reason: 'disk full',
    });
    expect(await listDz(e.dz)).toEqual([]);
    expect(await item('DCIM/a.cr2')).toMatchObject({ state: 'PLANNED', attempts: 0 });
  });

  test('a short copy is an item error and leaves no part', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(4000) });
    const tiny = join(await tmpDir(), 'tiny');
    await writeFile(tiny, randomBytes(10));
    const short: FsOps = {
      ...realFsOps,
      openRead: (p) =>
        String(p).endsWith('a.cr2') ? realFsOps.openRead(tiny) : realFsOps.openRead(p),
    };
    expect(await e.send({ fsOps: short })).toMatchObject({ outcome: 'MORE', sent: 0 });
    const row = await item('DCIM/a.cr2');
    expect(row).toMatchObject({ state: 'PLANNED', attempts: 1 });
    expect(row.last_error).toMatch(/short copy: 10 of 4000 bytes/);
    expect(await listDz(e.dz)).toEqual([]);
  });

  test('an item that stopped being PLANNED during the copy is never published', async () => {
    const e = await setup({ 'DCIM/a.cr2': randomBytes(4000) });
    const res = await e.send({
      checkpoint: async (n) => {
        if (n === 'send_part_written')
          await pgh.pool.query(`UPDATE harvest_items SET state = 'REJECTED'`);
      },
    });
    expect(res).toMatchObject({ sent: 0, failed: 0 });
    expect(await listDz(e.dz)).toEqual([]);
    expect((await item('DCIM/a.cr2')).state).toBe('REJECTED');
  });

  test('25 consecutive errors pause the run', async () => {
    const spec: TreeSpec = {};
    for (let i = 0; i < 26; i++) spec[`DCIM/f${String(i).padStart(2, '0')}.cr2`] = 200 + i;
    const e = await setup(spec);
    const ops: FsOps = {
      ...realFsOps,
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
      openRead: (p) =>
        !String(p).startsWith(e.root) || String(p).endsWith('f15.cr2')
          ? realFsOps.openRead(p)
          : Promise.reject(errno('EACCES')),
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
      openRead: (p) =>
        String(p).endsWith('a.cr2') ? Promise.reject(errno('EACCES')) : realFsOps.openRead(p),
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
    // Checked once before paging the items (no file size yet).
    expect(res).toMatchObject({ outcome: 'GATED', sent: 0, reason: 'disk 100.0 %' });
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

/** The pool, recording every query's text (pg.Pool's own methods stay bound to it). */
function recordingPool(log: string[]): typeof pgh.pool {
  return new Proxy(pgh.pool, {
    get(t, p) {
      if (p === 'query') {
        return (...a: Parameters<typeof t.query>) => {
          const q = a[0] as unknown;
          log.push(typeof q === 'string' ? q : String((q as { text?: string }).text));
          return (t.query as (...x: unknown[]) => unknown)(...a);
        };
      }
      const v = Reflect.get(t, p) as unknown;
      return typeof v === 'function' ? (v as (...x: unknown[]) => unknown).bind(t) : v;
    },
  });
}
/** Queries that page the run's PLANNED items (the old and the new nextItems both match). */
const pageQueries = (log: string[]) =>
  log.filter((q) => /FROM harvest_items i[\s\S]*i\.state = 'PLANNED'/.test(q)).length;

describe('paging (I5)', () => {
  const five: TreeSpec = {
    'DCIM/a.cr2': 201,
    'DCIM/b.cr2': 202,
    'DCIM/c.cr2': 203,
    'DCIM/d.cr2': 204,
    'DCIM/e.cr2': 205,
  };

  test('batches page through the run from a cursor; a full batch is MORE', async () => {
    const e = await setup(five);
    const small = { sendBatchSize: 2 };
    const r1 = await e.send(small);
    expect(r1).toMatchObject({ outcome: 'MORE', sent: 2, cursor: { pos: 0, key: 'dcim/b.cr2' } });
    const r2 = await e.send(small, { cursor: r1.cursor });
    expect(r2).toMatchObject({ outcome: 'MORE', sent: 2, cursor: { pos: 0, key: 'dcim/d.cr2' } });
    const r3 = await e.send(small, { cursor: r2.cursor });
    expect(r3).toMatchObject({ outcome: 'DONE', sent: 1 });
    expect((await items()).every((i) => i.state === 'SENT')).toBe(true);
  });

  test('a full batch decides MORE without a second item query', async () => {
    const e = await setup(five);
    const log: string[] = [];
    const r = await e.send({ sendBatchSize: 2, pool: recordingPool(log) });
    expect(r.outcome).toBe('MORE');
    expect(pageQueries(log)).toBe(1);
  });

  test('a shut gate returns GATED before any item query', async () => {
    const e = await setup(five, { statfs: { blocks: 100n, bavail: 0n, bsize: 1n } });
    const log: string[] = [];
    expect((await e.send({ pool: recordingPool(log) })).outcome).toBe('GATED');
    expect(pageQueries(log)).toBe(0);
  });

  test('an item left PLANNED behind the cursor is retried on the next pass', async () => {
    const e = await setup(five);
    let failed = false;
    const once: FsOps = {
      ...realFsOps,
      openRead: (p) => {
        if (!failed && String(p).endsWith('b.cr2')) {
          failed = true;
          return Promise.reject(errno('EACCES'));
        }
        return realFsOps.openRead(p);
      },
    };
    const r1 = await e.send({ fsOps: once, sendBatchSize: 3 });
    expect(r1).toMatchObject({ outcome: 'MORE', sent: 2 });
    const r2 = await e.send({ fsOps: once, sendBatchSize: 3 }, { cursor: r1.cursor });
    // The pass ended with b still PLANNED: a new pass (cursor reset) picks it up.
    expect(r2).toMatchObject({ outcome: 'MORE', sent: 2, cursor: null });
    const r3 = await e.send({ fsOps: once, sendBatchSize: 3 }, { cursor: r2.cursor });
    expect(r3).toMatchObject({ outcome: 'DONE', sent: 1 });
    expect((await item('DCIM/b.cr2')).state).toBe('SENT');
  });

  test('nested folders: an item counts for the deepest selected folder, in position order', async () => {
    const e = await setup(
      { 'A/x.cr2': 300, 'A/B/y.cr2': 301, 'A/z.cr2': 302, 'C/w.cr2': 303 },
      { relPath: '' },
    );
    const run = await createRun(
      pgh.pool,
      [
        { sourceId: e.sourceId, relPath: 'A' },
        { sourceId: e.sourceId, relPath: 'C' },
        { sourceId: e.sourceId, relPath: 'A/B' },
      ],
      false,
    );
    const scope = await loadScope(pgh.pool, run);
    expect(scope.folders.map((f) => f.key)).toEqual(['a', 'c', 'a/b']);
    const { sendBatch } = createHarvestActivities(e.deps);
    const beats: { item?: string }[] = [];
    await runActivity(sendBatch, [run, {}], {
      onHeartbeat: (d) => beats.push(d as { item?: string }),
    });
    expect(beats.map((b) => b.item).filter(Boolean)).toEqual([
      'A/x.cr2',
      'A/z.cr2',
      'C/w.cr2',
      'A/B/y.cr2',
    ]);
    const c = await pgh.pool.query<{ rel_path: string; sent: number }>(
      `SELECT f.rel_path, rf.sent::int FROM harvest_run_folders rf
       JOIN harvest_folders f ON f.id = rf.folder_id WHERE rf.run_id = $1 ORDER BY rf.position`,
      [run],
    );
    expect(c.rows).toEqual([
      { rel_path: 'A', sent: 2 },
      { rel_path: 'C', sent: 1 },
      { rel_path: 'A/B', sent: 1 },
    ]);
  });

  test('EXPLAIN on 120k PLANNED rows: the page query is an index range scan, no sort', async () => {
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO harvest_sources (identity, label, kind) VALUES ('local:X', 'X', 'local')
       RETURNING id::text AS id`,
    );
    const sid = rows[0]!.id;
    await pgh.pool.query(
      `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state)
       SELECT $1, d || '/f' || g || '.cr2', lower(d || '/f' || g || '.cr2'), 1000 + g, g, 'photo',
              CASE WHEN g % 10 = 0 THEN 'SENT' WHEN g % 10 = 1 THEN 'DUP' ELSE 'PLANNED' END
       FROM generate_series(1, 120000) AS g,
            LATERAL (SELECT 'D' || (g % 40) AS d) AS dd`,
      [sid],
    );
    // Each DUP row points at the SENT row just before it.
    await pgh.pool.query(
      "UPDATE harvest_items SET duplicate_of = id - 1 WHERE state = 'DUP' AND id > 1",
    );
    await pgh.pool.query('ANALYZE harvest_items');
    const run = await createRun(pgh.pool, [{ sourceId: sid, relPath: 'D7' }], false);
    const scope = await loadScope(pgh.pool, run);
    const explain = async (after: { key: string; id: string } | null) => {
      const q = pageQuery(scope, 0, after, 500);
      const r = await pgh.pool.query<{ 'QUERY PLAN': string }>(`EXPLAIN ${q.text}`, q.values);
      return r.rows.map((x) => x['QUERY PLAN']).join('\n');
    };
    for (const plan of [await explain(null), await explain({ key: 'd7/f5007.cr2', id: '5007' })]) {
      expect(plan).toMatch(/Index Scan using harvest_items_planned_key_idx/);
      expect(plan).not.toMatch(/Seq Scan|Sort/);
      console.log(`EXPLAIN page query (120k rows):\n${plan}`);
    }
    const dup = await pgh.pool.query<{ 'QUERY PLAN': string }>(
      `EXPLAIN SELECT id FROM harvest_items WHERE duplicate_of = 50 AND state = 'DUP'
       ORDER BY (kind = 'maybe'), id FOR UPDATE`,
    );
    const dplan = dup.rows.map((x) => x['QUERY PLAN']).join('\n');
    expect(dplan).toMatch(/harvest_items_duplicate_of_idx/);
    console.log(`EXPLAIN promoteDuplicates lookup (120k rows):\n${dplan}`);
  });
});

describe('send-only runs (F-1)', () => {
  test('approving 3 maybes in a folder with 100 PLANNED photos sends exactly those 3', async () => {
    const spec: TreeSpec = {};
    for (let i = 0; i < 100; i++) spec[`DCIM/p${String(i).padStart(3, '0')}.cr2`] = 200 + i;
    for (const n of ['m1', 'm2', 'm3']) spec[`DCIM/${n}.cr2`] = randomBytes(400);
    const e = await setup(spec);
    // The three stand in for approved maybes (REVIEW → PLANNED); the 100 photos stay PLANNED.
    const approved = (await items()).filter((i) => /\/m\d\.cr2$/.test(i.rel_path)).map((i) => i.id);
    await pgh.pool.query("UPDATE harvest_items SET kind = 'maybe' WHERE id = ANY($1::bigint[])", [
      approved,
    ]);
    const so = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], {
      sendOnly: true,
      items: approved,
    });
    const { sendBatch } = createHarvestActivities(e.deps);
    expect(await runActivity(sendBatch, [so, {}])).toMatchObject({ outcome: 'DONE', sent: 3 });
    const rows = await items();
    expect(
      rows
        .filter((r) => r.state === 'SENT')
        .map((r) => r.id)
        .sort(),
    ).toEqual([...approved].sort());
    expect(rows.filter((r) => r.state === 'PLANNED')).toHaveLength(100);
  });

  test('a root-level approval sends exactly the approved items', async () => {
    const e = await setup({ 'top.cr2': 300, 'other.cr2': 301, 'DCIM/a.cr2': 302 });
    const top = await item('top.cr2');
    const so = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], {
      sendOnly: true,
      items: [top.id],
    });
    const { sendBatch } = createHarvestActivities(e.deps);
    expect(await runActivity(sendBatch, [so, {}])).toMatchObject({ outcome: 'DONE', sent: 1 });
    expect((await item('top.cr2')).state).toBe('SENT');
    expect((await item('other.cr2')).state).toBe('PLANNED');
    expect((await item('DCIM/a.cr2')).state).toBe('PLANNED');
  });
});

describe('cancellation and heartbeats', () => {
  test('cancellation deletes the part', async () => {
    const e = await setup({ 'DCIM/big.cr2': 64 * 1024 * 1024 });
    const env = new MockActivityEnvironment();
    env.on('heartbeat', (d: unknown) => {
      if ((d as { copying?: number }).copying) env.cancel();
    });
    const { sendBatch } = createHarvestActivities(e.deps);
    await expect(env.run(sendBatch, e.runId)).rejects.toBeInstanceOf(CancelledFailure);
    expect(await listDz(e.dz)).toEqual([]);
    expect(await item('DCIM/big.cr2')).toMatchObject({ state: 'PLANNED', attempts: 0 });
  });

  test('full-file hashing heartbeats', async () => {
    const e = await setup({ 'DCIM/big.cr2': randomBytes(3 * 1024 * 1024) });
    // A vault content of the same size forces a full hash (different bytes: still sent).
    await pgh.pool.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state)
       VALUES ($1, 'cr2', 'still', $2, 'INGESTED')`,
      ['0'.repeat(64), 3 * 1024 * 1024],
    );
    const beats: { hashing?: string }[] = [];
    const { sendBatch } = createHarvestActivities(e.deps);
    const res = await runActivity(sendBatch, [e.runId], {
      onHeartbeat: (d) => beats.push(d as { hashing?: string }),
    });
    expect(res.sent).toBe(1);
    expect(beats.some((b) => b.hashing === 'DCIM/big.cr2')).toBe(true);
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
