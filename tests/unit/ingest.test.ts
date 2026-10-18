import { existsSync } from 'node:fs';
import { chmod, link, mkdir, readdir, rm, stat, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { MissingBytesError, StaleAttemptError } from '../../src/activities/ingest.js';
import { DataLayout, vaultRel } from '../../src/fsops.js';
import type { FileRef, IngestResult } from '../../src/types.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { JPEG_A, MOV_BYTES } from '../helpers/images.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

class SimulatedCrash extends Error {}

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

async function drop(name: string, bytes: Buffer, mtimeNs?: bigint): Promise<FileRef> {
  const p = join(layout.dropzone, name);
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, bytes);
  if (mtimeNs !== undefined) {
    const t = Number(mtimeNs) / 1e9;
    await utimes(p, t, t);
  }
  const s = await stat(p, { bigint: true });
  return { relPath: name, size: String(s.size), mtimeNs: String(s.mtimeNs) };
}

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

async function count(table: string, where = 'true'): Promise<number> {
  const r = await pool.query<{ n: string }>(
    `SELECT count(*)::text AS n FROM ${table} WHERE ${where}`,
  );
  return Number(r.rows[0]?.n);
}

test('new file lands in vault, read-only, drop zone and claimed empty', async () => {
  const r = await runActivity(acts.ingestFile, [await drop('IMG 1.jpg', JPEG_A)], { runId: 'r1' });
  expect([r.state, r.isNew, r.kind]).toEqual(['INGESTED', true, 'still']);
  const v = join(layout.root, vaultRel(r.sha256!, 'jpg'));
  expect((await stat(v)).mode & 0o777).toBe(0o444);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(existsSync(join(layout.dropzone, 'IMG 1.jpg'))).toBe(false);
});

test('byte-identical second file is COPY', async () => {
  const a = await runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: 'r1' });
  const b = await runActivity(acts.ingestFile, [await drop('b.jpg', JPEG_A)], { runId: 'r2' });
  expect([b.state, b.isNew, b.sha256]).toEqual(['COPY', false, a.sha256]);
  expect(existsSync(join(layout.vaultCopies, `${b.fileId}.jpg`))).toBe(true);
});

test.each(['claim', 'link', 'copies_rename'])('resume after crash at %s', async (point) => {
  if (point === 'copies_rename')
    await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], { runId: 'r0' });
  const ref = await drop('a.jpg', JPEG_A);
  const crashing = createActivities(crashAt(point));
  await expect(runActivity(crashing.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    SimulatedCrash,
  );
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe(point === 'copies_rename' ? 'COPY' : 'INGESTED');
  expect(await count('files', `rel_path = 'a.jpg'`)).toBe(1);
  expect(await readdir(layout.claimed)).toEqual([]);
});

test.each(['claim', 'link', 'copies_rename'])(
  'crash at %s converges: one row, one copy of the bytes, contents once',
  async (point) => {
    const first = await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], {
      runId: 'r0',
    });
    const ref = await drop('a.jpg', JPEG_A);
    await expect(
      runActivity(createActivities(crashAt(point)).ingestFile, [ref], { runId: 'r1' }),
    ).rejects.toThrow(SimulatedCrash);
    const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
    expect([r.state, r.isNew, r.sha256]).toEqual(['COPY', false, first.sha256]);
    expect(await readdir(layout.vaultCopies)).toEqual([`${r.fileId}.jpg`]);
    expect((await stat(join(layout.vaultCopies, `${r.fileId}.jpg`))).mode & 0o777).toBe(0o444);
    expect(await readdir(layout.dropzone)).toEqual([]);
    expect(await readdir(layout.claimed)).toEqual([]);
    expect([await count('files'), await count('contents')]).toEqual([2, 1]);
    expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(r);
  },
);

test('re-run after commit returns same outcome', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const first = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(first);
  expect(await count('files')).toBe(1);
});

test('re-drop after ingest becomes COPY', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  const ref2 = await drop('a.jpg', JPEG_A, BigInt(ref.mtimeNs));
  expect((await runActivity(acts.ingestFile, [ref2], { runId: 'r2' })).state).toBe('COPY');
  expect(await count('files')).toBe(2);
});

test('vanished file is quarantined', async () =>
  expect(
    await runActivity(acts.ingestFile, [{ relPath: 'gone.jpg', size: '1', mtimeNs: '1' }], {
      runId: 'r1',
    }),
  ).toMatchObject({ state: 'QUARANTINED', sha256: null }));

test('MOV is motion', async () =>
  expect(
    (await runActivity(acts.ingestFile, [await drop('IMG_1.MOV', MOV_BYTES)], { runId: 'r1' }))
      .kind,
  ).toBe('motion'));

test('heartbeats during hashing', async () => {
  let beats = 0;
  const ref = await drop('big.jpg', Buffer.concat([JPEG_A, Buffer.alloc(3 << 20)]));
  await runActivity(acts.ingestFile, [ref], { runId: 'r1', onHeartbeat: () => beats++ });
  expect(beats).toBeGreaterThanOrEqual(3);
});

test('vault owner is INGESTED even when a byte copy commits first', async () => {
  const ra = await drop('a.jpg', JPEG_A),
    rb = await drop('b.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('link')).ingestFile, [ra], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  const b = await runActivity(acts.ingestFile, [rb], { runId: 'r2' });
  expect([b.state, b.isNew]).toEqual(['COPY', false]);
  expect(await count('contents')).toBe(0);
  const a = await runActivity(acts.ingestFile, [ra], { runId: 'r1' });
  expect([a.state, a.isNew]).toEqual(['INGESTED', true]);
  expect(await count('contents')).toBe(1);
  expect(existsSync(join(layout.vaultCopies, `${b.fileId}.jpg`))).toBe(true);
});

test('a resumed stale attempt cannot regress a final row', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  let second: IngestResult | undefined;
  const stale = createActivities({
    ...deps,
    checkpoint: async (n) => {
      // attempt 1 "pauses" after linking; attempt 2 finishes meanwhile
      if (n === 'link' && !second)
        second = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
    },
  });
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    StaleAttemptError,
  );
  expect(second?.state).toBe('INGESTED');
  expect(
    (await pool.query<{ state: string }>(`SELECT state FROM files`)).rows.map((r) => r.state),
  ).toEqual(['INGESTED']);
});

test('hashed file with claimed gone but vault present commits INGESTED', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('link')).ingestFile, [ref], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  for (const f of await readdir(layout.claimed)) await rm(join(layout.claimed, f)); // test-only anomaly
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect([r.state, r.isNew, r.kind]).toEqual(['INGESTED', true, 'still']);
  expect(await count('contents')).toBe(1);
});

test('hashed file whose bytes are all gone is retried, never vanished', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('link')).ingestFile, [ref], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  const { sha256 } = (await pool.query<{ sha256: string }>(`SELECT sha256 FROM files`)).rows[0]!;
  for (const f of await readdir(layout.claimed)) await rm(join(layout.claimed, f));
  await rm(join(layout.root, vaultRel(sha256, 'jpg')), { force: true }); // test-only anomaly
  await expect(runActivity(acts.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    MissingBytesError,
  );
  expect(await count('files', `state = 'QUARANTINED'`)).toBe(0);
});

test('contents.mime comes from exiftool', async () => {
  const r = await runActivity(acts.ingestFile, [await drop('IMG_1.HEIC', JPEG_A)], {
    runId: 'r1',
  }); // JPEG bytes named .HEIC
  expect(r.ext).toBe('jpg');
  expect((await pool.query<{ mime: string }>('SELECT mime FROM contents')).rows[0]?.mime).toBe(
    'image/jpeg',
  );
});

test('a claimed file is never overwritten by a re-drop at the same path', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('claim')).ingestFile, [ref], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  const [claimedName] = await readdir(layout.claimed);
  const ino = (await stat(join(layout.claimed, claimedName!))).ino;
  await drop('a.jpg', Buffer.concat([JPEG_A, Buffer.from('other')])); // same path, new bytes
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe('INGESTED');
  expect((await stat(join(layout.root, vaultRel(r.sha256!, 'jpg')))).ino).toBe(ino);
  expect(existsSync(join(layout.dropzone, 'a.jpg'))).toBe(true); // left for the next scan
});

test('a crash after commit is finished by the replay (claimed unlinked, vault read-only)', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  const v = join(layout.root, vaultRel(r.sha256!, 'jpg'));
  // Simulate a crash between the commit and step 6: claimed still linked, vault writable.
  await link(v, join(layout.claimed, r.fileId));
  await chmod(v, 0o644);
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(r);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect((await stat(v)).mode & 0o777).toBe(0o444);
});

test('refuses to run without a workflow run id', async () => {
  await expect(
    runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: '' }),
  ).rejects.toThrow(/run id/);
  expect(await count('files')).toBe(0);
});

test('nested relPath and unknown type keep a usable ext', async () => {
  const r = await runActivity(acts.ingestFile, [await drop('a b/é.xyz', Buffer.from('hello'))], {
    runId: 'r1',
  });
  expect([r.state, r.kind]).toEqual(['INGESTED', 'other']);
  expect(r.ext).toMatch(/^[a-z0-9]+$/);
  expect(existsSync(join(layout.root, vaultRel(r.sha256!, r.ext!)))).toBe(true);
  expect(
    (await pool.query<{ original_name: string }>('SELECT original_name FROM files')).rows[0]
      ?.original_name,
  ).toBe('é.xyz');
});

test.each([
  ['blob', 'bin'],
  ['x.ABC', 'abc'],
  ['x.t#', 'bin'],
])('undetectable bytes named %s vault as .%s with no mime', async (name, ext) => {
  const r = await runActivity(acts.ingestFile, [await drop(name, Buffer.alloc(64))], {
    runId: 'r1',
  });
  expect([r.state, r.ext, r.kind]).toEqual(['INGESTED', ext, 'other']);
  expect((await pool.query<{ mime: string | null }>('SELECT mime FROM contents')).rows).toEqual([
    { mime: null },
  ]);
});

test('a claim failing other than ENOENT is retried, never quarantined', async () => {
  await drop('x.jpg', JPEG_A);
  const ref = { relPath: 'x.jpg/inner.jpg', size: '1', mtimeNs: '1' }; // ENOTDIR
  await expect(runActivity(acts.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(/ENOTDIR/);
  expect(await count('files', `state = 'DISCOVERED'`)).toBe(1);
});

test('a vanished outcome is replayed, not re-decided', async () => {
  const ref = { relPath: 'gone.jpg', size: '1', mtimeNs: '1' };
  const first = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(first);
  expect(await count('files', `last_error = 'vanished'`)).toBe(1);
});

test('replaying a committed row does not fail on a vault file removed since', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  await rm(join(layout.root, vaultRel(r.sha256!, 'jpg'))); // verify-vault's concern, not ingest's
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(r);
});

test('crash after hashing, before ext, with claimed gone is retried as missing bytes', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const failing = createActivities({
    ...deps,
    metadata: { read: () => Promise.reject(new SimulatedCrash('exiftool died')) } as never,
  });
  await expect(runActivity(failing.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    SimulatedCrash,
  );
  for (const f of await readdir(layout.claimed)) await rm(join(layout.claimed, f)); // test-only anomaly
  await expect(runActivity(acts.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    MissingBytesError,
  );
  expect(await count('files', `state = 'CLAIMED' AND ext IS NULL`)).toBe(1);
});

test('a crash during metadata resumes to INGESTED', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const failing = createActivities({
    ...deps,
    metadata: { read: () => Promise.reject(new SimulatedCrash('exiftool died')) } as never,
  });
  await expect(runActivity(failing.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    SimulatedCrash,
  );
  expect((await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).state).toBe('INGESTED');
  expect(await readdir(layout.claimed)).toEqual([]);
});
