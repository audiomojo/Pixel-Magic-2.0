// Removal of confirmed byte-identical duplicates (spec §6.5): `reclaimCopy` and `sweepCopies`.
import { existsSync } from 'node:fs';
import { chmod, link, mkdir, readdir, rm, stat, unlink, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { DataLayout, sha256File, vaultRel } from '../../src/fsops.js';
import type { FileRef, IngestResult } from '../../src/types.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { JPEG_A } from '../helpers/images.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

class SimulatedCrash extends Error {}

/** Other bytes (still a JPEG to exiftool: the trailer is ignored). */
const JPEG_B = Buffer.concat([JPEG_A, Buffer.from('other bytes')]);

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
  await rm(layout.root, { recursive: true, force: true });
  await layout.ensure();
});

let run = 0;
async function ingest(name: string, bytes: Buffer): Promise<IngestResult> {
  const p = join(layout.dropzone, name);
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, bytes);
  const s = await stat(p, { bigint: true });
  const ref: FileRef = { relPath: name, size: String(s.size), mtimeNs: String(s.mtimeNs) };
  return runActivity(acts.ingestFile, [ref], { runId: `run-${String(++run)}` });
}

/** An owner and `n` byte-identical COPY files of `bytes`. */
async function copies(n: number, bytes = JPEG_A): Promise<IngestResult[]> {
  await ingest('owner.jpg', bytes);
  const out: IngestResult[] = [];
  for (let i = 0; i < n; i++) out.push(await ingest(`copy-${String(i)}.jpg`, bytes));
  for (const r of out) expect(r.state).toBe('COPY');
  return out;
}

const copyPath = (r: IngestResult) => join(layout.vaultCopies, `${r.fileId}.${r.ext!}`);
const vaultPath = (r: IngestResult) => join(layout.root, vaultRel(r.sha256!, r.ext!));

interface Marks {
  copy_removed_at: Date | null;
  copy_reclaimed_bytes: bigint | null;
  copy_mismatch: boolean;
  last_error: string | null;
  state: string;
}
async function marks(r: IngestResult): Promise<Marks> {
  return (
    await pool.query<Marks>(
      `SELECT copy_removed_at, copy_reclaimed_bytes, copy_mismatch, last_error, state
         FROM files WHERE id = $1`,
      [r.fileId],
    )
  ).rows[0]!;
}

const reclaim = (r: IngestResult, d: Deps = deps, onHeartbeat?: () => void) =>
  runActivity(createActivities(d).reclaimCopy, [r.fileId], onHeartbeat ? { onHeartbeat } : {});
const sweep = (limit: number, d: Deps = deps) =>
  runActivity(createActivities(d).sweepCopies, [limit]);

function crashAt(name: string): Deps {
  let fired = false;
  return {
    ...deps,
    checkpoint: (n) => {
      if (n === name && !fired) {
        fired = true;
        throw new SimulatedCrash(`crash at ${n}`);
      }
    },
  };
}

/** Overwrites a read-only file with `bytes`, keeping its mtime (a silent corruption). */
async function corrupt(path: string, bytes: Buffer): Promise<void> {
  const s = await stat(path);
  await chmod(path, 0o644);
  await writeFile(path, bytes);
  await utimes(path, s.atime, s.mtime);
  await chmod(path, 0o444);
}

test('a confirmed byte-identical copy is unlinked and its row marked removed', async () => {
  const [c] = await copies(1);
  expect(await reclaim(c!)).toBe('REMOVED');
  expect(existsSync(copyPath(c!))).toBe(false);
  expect(await sha256File(vaultPath(c!))).toBe(c!.sha256);
  const m = await marks(c!);
  expect(m.state).toBe('COPY');
  expect(m.copy_removed_at).toBeInstanceOf(Date);
  expect(m.copy_reclaimed_bytes).toBe(BigInt(JPEG_A.length));
  expect([m.copy_mismatch, m.last_error]).toEqual([false, null]);
});

test('the unlink is made durable before the row is marked', async () => {
  const [c] = await copies(1);
  let seen: { onDisk: boolean; marked: Date | null } | undefined;
  const d: Deps = {
    ...deps,
    checkpoint: async (n) => {
      if (n === 'reclaim_unlinked')
        seen = { onDisk: existsSync(copyPath(c!)), marked: (await marks(c!)).copy_removed_at };
    },
  };
  expect(await reclaim(c!, d)).toBe('REMOVED');
  expect(seen).toEqual({ onDisk: false, marked: null });
});

test('reclaim is idempotent on replay', async () => {
  const [c] = await copies(1);
  expect(await reclaim(c!)).toBe('REMOVED');
  const first = await marks(c!);
  expect(await reclaim(c!)).toBe('ALREADY_REMOVED');
  expect(await marks(c!)).toEqual(first);
  expect(await sha256File(vaultPath(c!))).toBe(c!.sha256);
});

test.each(['copy', 'vault'] as const)(
  'a mismatch on the %s side deletes nothing and flags the row',
  async (side) => {
    const [c] = await copies(1);
    const bad = Buffer.concat([JPEG_A.subarray(0, JPEG_A.length - 1), Buffer.from('!')]);
    await corrupt(side === 'copy' ? copyPath(c!) : vaultPath(c!), bad);
    expect(await reclaim(c!)).toBe('MISMATCH');
    expect(existsSync(copyPath(c!))).toBe(true);
    expect(existsSync(vaultPath(c!))).toBe(true);
    const m = await marks(c!);
    expect([m.copy_mismatch, m.copy_removed_at]).toEqual([true, null]);
    const observed = await sha256File(side === 'copy' ? copyPath(c!) : vaultPath(c!));
    expect(m.last_error).toContain('copy_mismatch');
    expect(m.last_error).toContain(observed);
    expect(m.last_error).toContain(c!.sha256);
    // Never retried: a flagged row answers at once, without hashing again.
    let beats = 0;
    expect(await reclaim(c!, deps, () => beats++)).toBe('MISMATCH');
    expect(beats).toBe(0);
  },
);

test('a missing vault file deletes nothing and is flagged as an anomaly', async () => {
  const [c] = await copies(1);
  await unlink(vaultPath(c!)); // test-only anomaly
  expect(await reclaim(c!)).toBe('MISMATCH');
  expect(await sha256File(copyPath(c!))).toBe(c!.sha256);
  const m = await marks(c!);
  expect(m.copy_mismatch).toBe(true);
  expect(m.last_error).toMatch(/vault file .* missing/);
});

test('a copy with another link (an older staging generation) is deferred, never unlinked', async () => {
  const [c] = await copies(1);
  const staged = join(layout.staging, '.gen-1', 'copies', 'x.jpg');
  await mkdir(dirname(staged), { recursive: true });
  await link(copyPath(c!), staged);
  expect(await reclaim(c!)).toBe('DEFERRED');
  expect(existsSync(copyPath(c!))).toBe(true);
  expect(await marks(c!)).toMatchObject({ copy_removed_at: null, copy_mismatch: false });
  await unlink(staged); // the generation is gone: now it can go
  expect(await reclaim(c!)).toBe('REMOVED');
});

test('reclaim deletes nothing for a row that is not a COPY', async () => {
  const owner = await ingest('a.jpg', JPEG_A);
  expect(await reclaim(owner)).toBe('NOT_COPY');
  expect(await sha256File(vaultPath(owner))).toBe(owner.sha256);
  expect(await runActivity(acts.reclaimCopy, ['999'])).toBe('NOT_COPY');
});

test.each(['copy', 'vault'] as const)(
  'the %s file changed while it is hashed: retried, never unlinked',
  async (side) => {
    const [c] = await copies(1);
    const path = side === 'copy' ? copyPath(c!) : vaultPath(c!);
    const d: Deps = {
      ...deps,
      checkpoint: async (n) => {
        if (n === 'reclaim_hashed') {
          await chmod(path, 0o644);
          await writeFile(path, Buffer.from(' more'), { flag: 'a' });
        }
      },
    };
    await expect(reclaim(c!, d)).rejects.toThrow(/changed while hashing/);
    expect(existsSync(copyPath(c!))).toBe(true);
    expect(await marks(c!)).toMatchObject({ copy_removed_at: null, copy_mismatch: false });
  },
);

test('something other than a regular file at the .copies path is flagged, not removed', async () => {
  const [c] = await copies(1);
  await unlink(copyPath(c!));
  await mkdir(copyPath(c!));
  expect(await reclaim(c!)).toBe('MISMATCH');
  expect(existsSync(copyPath(c!))).toBe(true);
  expect((await marks(c!)).last_error).toMatch(/not a regular file/);
});

test('reclaim heartbeats while it hashes', async () => {
  const big = Buffer.concat([JPEG_A, Buffer.alloc(3 << 20)]);
  const [c] = await copies(1, big);
  let beats = 0;
  expect(await reclaim(c!, deps, () => beats++)).toBe('REMOVED');
  expect(beats).toBeGreaterThanOrEqual(6); // both files, 4 MiB each
});

test('a crash between unlink and mark is finished by a retry', async () => {
  const [c] = await copies(1);
  await expect(reclaim(c!, crashAt('reclaim_unlinked'))).rejects.toThrow(SimulatedCrash);
  expect(existsSync(copyPath(c!))).toBe(false);
  expect((await marks(c!)).copy_removed_at).toBeNull();
  expect(await reclaim(c!)).toBe('REMOVED');
  expect((await marks(c!)).copy_reclaimed_bytes).toBe(BigInt(JPEG_A.length));
});

test('a crash between unlink and mark is finished by the sweep', async () => {
  const [c] = await copies(1);
  await expect(reclaim(c!, crashAt('reclaim_unlinked'))).rejects.toThrow(SimulatedCrash);
  expect(await sweep(10)).toEqual({
    examined: 1,
    removed: 1,
    mismatched: 0,
    deferred: 0,
    wrapped: true,
  });
  expect((await marks(c!)).copy_removed_at).toBeInstanceOf(Date);
});

test('copy gone, row unmarked and the vault file gone too: an anomaly, flagged, never hidden', async () => {
  const [c] = await copies(1);
  await expect(reclaim(c!, crashAt('reclaim_unlinked'))).rejects.toThrow(SimulatedCrash);
  await unlink(vaultPath(c!)); // test-only anomaly
  expect(await sweep(10)).toMatchObject({ examined: 1, removed: 0, mismatched: 1 });
  const m = await marks(c!);
  expect([m.copy_mismatch, m.copy_removed_at]).toEqual([true, null]);
  expect(m.last_error).toMatch(/missing/);
});

test('copy gone, row unmarked, vault bytes wrong: flagged, never marked removed', async () => {
  const [c] = await copies(1);
  await expect(reclaim(c!, crashAt('reclaim_unlinked'))).rejects.toThrow(SimulatedCrash);
  await corrupt(vaultPath(c!), JPEG_B);
  expect(await reclaim(c!)).toBe('MISMATCH');
  expect((await marks(c!)).last_error).toContain(await sha256File(vaultPath(c!)));
});

test('the sweep is bounded per call and resumes where it stopped', async () => {
  const cs = await copies(5);
  const r1 = await sweep(2);
  expect(r1).toEqual({ examined: 2, removed: 2, mismatched: 0, deferred: 0, wrapped: false });
  expect(await readdir(layout.vaultCopies)).toHaveLength(3);
  expect(await sweep(2)).toMatchObject({ examined: 2, removed: 2, wrapped: false });
  expect(await sweep(2)).toMatchObject({ examined: 1, removed: 1, wrapped: true });
  expect(await readdir(layout.vaultCopies)).toEqual([]);
  for (const c of cs) expect((await marks(c)).copy_removed_at).toBeInstanceOf(Date);
  expect(await sweep(2)).toEqual({
    examined: 0,
    removed: 0,
    mismatched: 0,
    deferred: 0,
    wrapped: true,
  });
});

test('the sweep skips removed and flagged rows and moves past deferred ones', async () => {
  const [done, flagged, held, plain] = await copies(4);
  expect(await reclaim(done!)).toBe('REMOVED');
  await corrupt(copyPath(flagged!), JPEG_B);
  expect(await reclaim(flagged!)).toBe('MISMATCH');
  await link(copyPath(held!), join(layout.root, 'elsewhere.jpg'));
  expect(await sweep(1)).toMatchObject({ examined: 1, deferred: 1, wrapped: false });
  expect(await sweep(1)).toMatchObject({ examined: 1, removed: 1, wrapped: false });
  expect(await sweep(1)).toMatchObject({ examined: 0, wrapped: true });
  expect((await marks(plain!)).copy_removed_at).toBeInstanceOf(Date);
  expect(existsSync(copyPath(flagged!))).toBe(true);
  expect(existsSync(copyPath(held!))).toBe(true);
  // The next pass starts over: the deferred row is looked at again.
  expect(await sweep(10)).toMatchObject({ examined: 1, deferred: 1, wrapped: true });
});

test('the vault owner and a different file are never touched by the sweep', async () => {
  const owner = await ingest('a.jpg', JPEG_A);
  const other = await ingest('b.jpg', JPEG_B);
  expect(await sweep(10)).toMatchObject({ examined: 0 });
  for (const r of [owner, other]) expect(await sha256File(vaultPath(r))).toBe(r.sha256);
});

/** Ingests `bytes` and JPEG_B, then retires the first content by hand (vault file gone). */
async function retire(bytes: Buffer): Promise<IngestResult> {
  const owner = await ingest('retired-owner.jpg', bytes);
  const survivor = await ingest('survivor.jpg', JPEG_B);
  await pool.query(
    `UPDATE contents SET state = 'DUPLICATE', duplicate_of = $2, duplicate_removed_at = now()
      WHERE sha256 = $1`,
    [owner.sha256, survivor.sha256],
  );
  await unlink(vaultPath(owner));
  return owner;
}

test('a retired content dropped again is COPY, reclaimed without a vault file, never re-vaulted', async () => {
  const owner = await retire(JPEG_A);
  const c = await ingest('again.jpg', JPEG_A);
  expect(c.state).toBe('COPY');
  expect(existsSync(vaultPath(owner))).toBe(false);
  expect(existsSync(copyPath(c))).toBe(true);
  expect(await reclaim(c)).toBe('REMOVED');
  expect(existsSync(copyPath(c))).toBe(false);
  expect(existsSync(vaultPath(owner))).toBe(false);
  expect((await marks(c)).copy_removed_at).toBeInstanceOf(Date);
  const st = await pool.query<{ state: string }>(`SELECT state FROM contents WHERE sha256 = $1`, [
    owner.sha256,
  ]);
  expect(st.rows[0]!.state).toBe('DUPLICATE');
});

test('a retired content whose .copies bytes differ is flagged, not deleted', async () => {
  const owner = await retire(JPEG_A);
  const c = await ingest('again.jpg', JPEG_A);
  await corrupt(copyPath(c), JPEG_B);
  expect(await reclaim(c)).toBe('MISMATCH');
  expect(existsSync(copyPath(c))).toBe(true);
  expect((await marks(c)).copy_mismatch).toBe(true);
  expect(owner.sha256).toBe(c.sha256);
});

test('a retired content whose .copies file is already gone is marked removed', async () => {
  await retire(JPEG_A);
  const c = await ingest('again.jpg', JPEG_A);
  await unlink(copyPath(c));
  expect(await reclaim(c)).toBe('REMOVED');
});

test('the sweep removes a retired content copy too', async () => {
  await retire(JPEG_A);
  const c = await ingest('again.jpg', JPEG_A);
  const r = await sweep(10);
  expect(r.removed).toBe(1);
  expect(existsSync(copyPath(c))).toBe(false);
});

/** Ingests `bytes` (the retired one) and a keeper, then retires the first by hand as `how` says. */
async function retireAs(
  bytes: Buffer,
  how: { removed: boolean; mismatch?: boolean; unlinkVault?: boolean; keeperGone?: boolean },
): Promise<{ owner: IngestResult; keeper: IngestResult }> {
  const owner = await ingest('retired-owner.jpg', bytes);
  const keeper = await ingest('keeper.jpg', JPEG_B);
  await pool.query(
    `UPDATE contents SET state = 'DUPLICATE', duplicate_of = $2,
                         duplicate_removed_at = CASE WHEN $3 THEN now() END,
                         duplicate_mismatch = $4
      WHERE sha256 = $1`,
    [owner.sha256, keeper.sha256, how.removed, how.mismatch ?? false],
  );
  if (how.unlinkVault) await unlink(vaultPath(owner));
  if (how.keeperGone) await unlink(vaultPath(keeper));
  return { owner, keeper };
}

test('a retired content flagged duplicate_mismatch takes the normal path: a bad vault file keeps the copy', async () => {
  const { owner } = await retireAs(JPEG_A, { removed: false, mismatch: true });
  const c = await ingest('again.jpg', JPEG_A);
  expect(c.state).toBe('COPY');
  await corrupt(vaultPath(owner), JPEG_B);
  expect(await reclaim(c)).toBe('MISMATCH');
  expect(existsSync(copyPath(c))).toBe(true);
  expect((await marks(c)).copy_mismatch).toBe(true);
});

test('a retired content flagged with its vault file gone: the copy is the only bytes, kept and flagged', async () => {
  await retireAs(JPEG_A, { removed: false, mismatch: true });
  const c = await ingest('again.jpg', JPEG_A);
  await unlink(vaultPath(c));
  expect(await reclaim(c)).toBe('MISMATCH');
  expect(existsSync(copyPath(c))).toBe(true);
});

test('a removed retired content whose keeper vault file is gone: the copy is kept and flagged', async () => {
  await retireAs(JPEG_A, { removed: true, unlinkVault: true, keeperGone: true });
  const c = await ingest('again.jpg', JPEG_A);
  expect(c.state).toBe('COPY');
  expect(await reclaim(c)).toBe('MISMATCH');
  expect(existsSync(copyPath(c))).toBe(true);
  expect((await marks(c)).last_error).toMatch(/vault file .* missing/);
});

test('a removed retired content whose keeper is gone, .copies file missing too: flagged, not marked', async () => {
  await retireAs(JPEG_A, { removed: true, unlinkVault: true, keeperGone: true });
  const c = await ingest('again.jpg', JPEG_A);
  await unlink(copyPath(c));
  expect(await reclaim(c)).toBe('MISMATCH');
  expect((await marks(c)).copy_removed_at).toBeNull();
});

test('a removed retired content with its keeper present: the copy is deleted as before', async () => {
  const { keeper } = await retireAs(JPEG_A, { removed: true, unlinkVault: true });
  const c = await ingest('again.jpg', JPEG_A);
  expect(await reclaim(c)).toBe('REMOVED');
  expect(existsSync(copyPath(c))).toBe(false);
  expect(existsSync(vaultPath(keeper))).toBe(true);
});

/** Retired, removal pending (vault file present), keeper in the vault. */
async function pendingRetired(name: string, bytes: Buffer, keeper: IngestResult) {
  const r = await ingest(name, bytes);
  await pool.query(`UPDATE contents SET state = 'DUPLICATE', duplicate_of = $2 WHERE sha256 = $1`, [
    r.sha256,
    keeper.sha256,
  ]);
  return r;
}
const retiredRow = async (r: IngestResult) =>
  (
    await pool.query<{ duplicate_removed_at: Date | null; duplicate_mismatch: boolean }>(
      'SELECT duplicate_removed_at, duplicate_mismatch FROM contents WHERE sha256 = $1',
      [r.sha256],
    )
  ).rows[0]!;

test('the sweep finishes pending retired removals: deferred while linked, removed once not', async () => {
  const keeper = await ingest('keeper.jpg', JPEG_B);
  const a = await pendingRetired('a.jpg', JPEG_A, keeper);
  const b = await pendingRetired('b.jpg', Buffer.concat([JPEG_A, Buffer.from('b')]), keeper);
  const staged = join(layout.root, 'staged-link.jpg');
  await link(vaultPath(a), staged);
  expect(await sweep(10)).toMatchObject({ removed: 1, deferred: 1, wrapped: true });
  expect(existsSync(vaultPath(a))).toBe(true);
  expect((await retiredRow(a)).duplicate_removed_at).toBeNull();
  expect(existsSync(vaultPath(b))).toBe(false);
  expect((await retiredRow(b)).duplicate_removed_at).toBeInstanceOf(Date);
  await unlink(staged);
  expect(await sweep(10)).toMatchObject({ removed: 1, deferred: 0 });
  expect(existsSync(vaultPath(a))).toBe(false);
  expect((await retiredRow(a)).duplicate_removed_at).toBeInstanceOf(Date);
  expect(existsSync(vaultPath(keeper))).toBe(true);
  expect(await sweep(10)).toMatchObject({ examined: 0, wrapped: true });
});

test('the retired pass is bounded and resumes at its own cursor', async () => {
  const keeper = await ingest('keeper.jpg', JPEG_B);
  const rs = [];
  for (let i = 0; i < 3; i++)
    rs.push(
      await pendingRetired(`r${String(i)}.jpg`, Buffer.concat([JPEG_A, Buffer.from([i])]), keeper),
    );
  for (const r of rs) await link(vaultPath(r), join(layout.root, `l-${r.sha256!}`));
  const first = await sweep(2);
  expect(first).toMatchObject({ deferred: 2, wrapped: false });
  const second = await sweep(2);
  expect(second).toMatchObject({ deferred: 1, wrapped: true });
  const third = await sweep(2);
  expect(third).toMatchObject({ deferred: 2, wrapped: false }); // back at the start
});
