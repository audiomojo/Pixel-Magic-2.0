import { existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import {
  chmod,
  link,
  mkdir,
  readFile,
  readdir,
  rm,
  stat,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { dirname, join } from 'node:path';
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import { createActivities, type Deps } from '../../src/activities/index.js';
import { MissingBytesError, StaleAttemptError } from '../../src/activities/ingest.js';
import { DataLayout, vaultRel } from '../../src/fsops.js';
import type { FileRef, IngestResult } from '../../src/types.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { JPEG_A, MOV_BYTES } from '../helpers/images.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

class SimulatedCrash extends Error {}

/** Every `fsyncDir` the activity makes, with what was already on disk / committed at that moment. */
const fsyncs = vi.hoisted(
  () => [] as { dir: string; entries: string[]; claimed: string[]; states: string[] }[],
);
let pool: pg.Pool;
vi.mock('../../src/fsops.js', async (orig) => {
  const real = await orig<typeof import('../../src/fsops.js')>();
  const { readdir: ls } = await import('node:fs/promises');
  return {
    ...real,
    fsyncDir: async (dir: string) => {
      const states = (await pool.query<{ state: string }>('SELECT state FROM files ORDER BY id'))
        .rows;
      fsyncs.push({
        dir,
        entries: await ls(dir),
        claimed: await ls(layout.claimed),
        states: states.map((r) => r.state),
      });
      await real.fsyncDir(dir);
    },
  };
});

let pgh: PgHandle;
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
  expect([r.state, r.kind]).toEqual(['INGESTED', 'still']);
  const v = join(layout.root, vaultRel(r.sha256!, 'jpg'));
  expect((await stat(v)).mode & 0o777).toBe(0o444);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(existsSync(join(layout.dropzone, 'IMG 1.jpg'))).toBe(false);
});

test('byte-identical second file is COPY', async () => {
  const a = await runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: 'r1' });
  const b = await runActivity(acts.ingestFile, [await drop('b.jpg', JPEG_A)], { runId: 'r2' });
  expect([b.state, b.sha256]).toEqual(['COPY', a.sha256]);
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
    expect([r.state, r.sha256]).toEqual(['COPY', first.sha256]);
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
  expect(b.state).toBe('COPY');
  expect(await count('contents')).toBe(0);
  const a = await runActivity(acts.ingestFile, [ra], { runId: 'r1' });
  expect(a.state).toBe('INGESTED');
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
  expect([r.state, r.kind]).toEqual(['INGESTED', 'still']);
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

test.each(['blob', 'x.ABC', 'x.t#'])(
  'undetectable bytes named %s vault as .bin with no mime (never by extension)',
  async (name) => {
    const r = await runActivity(acts.ingestFile, [await drop(name, Buffer.alloc(64))], {
      runId: 'r1',
    });
    expect([r.state, r.ext, r.kind]).toEqual(['INGESTED', 'bin', 'other']);
    expect((await pool.query<{ mime: string | null }>('SELECT mime FROM contents')).rows).toEqual([
      { mime: null },
    ]);
  },
);

test('identical undetectable bytes under different names dedup to one vault inode', async () => {
  const a = await runActivity(acts.ingestFile, [await drop('x.abc', Buffer.alloc(64))], {
    runId: 'r1',
  });
  const b = await runActivity(acts.ingestFile, [await drop('y.def', Buffer.alloc(64))], {
    runId: 'r2',
  });
  expect([a.state, b.state, b.sha256]).toEqual(['INGESTED', 'COPY', a.sha256]);
  expect(await count('contents')).toBe(1);
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

test('the .copies rename is fsynced before the COPY commit', async () => {
  await runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: 'r1' });
  fsyncs.length = 0;
  const b = await runActivity(acts.ingestFile, [await drop('b.jpg', JPEG_A)], { runId: 'r2' });
  expect(b.state).toBe('COPY');
  expect(fsyncs).toEqual([
    {
      dir: layout.vaultCopies,
      entries: [`${b.fileId}.jpg`],
      claimed: [],
      states: ['INGESTED', 'CLAIMED'],
    },
  ]);
});

test('a stale attempt that claimed a re-drop puts it back in the drop zone', async () => {
  fsyncs.length = 0;
  const ref = await drop('a.jpg', JPEG_A);
  const redrop = Buffer.concat([JPEG_A, Buffer.from('re-drop')]);
  let second: IngestResult | undefined;
  const stale = createActivities({
    ...deps,
    checkpoint: async (n) => {
      // attempt X stalls after its upsert (sha256 still null in its snapshot)…
      if (n === 'upsert' && !second) {
        second = await runActivity(acts.ingestFile, [ref], { runId: 'r1' }); // …Y commits
        await drop('a.jpg', redrop); // …and the user drops a new file at the same path
      }
    },
  });
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    StaleAttemptError,
  );
  expect(second?.state).toBe('INGESTED');
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(await readFile(join(layout.dropzone, 'a.jpg'))).toEqual(redrop);
  expect(await count('files')).toBe(1);
  // The link back into the drop zone was made durable while claimed/<id> still held the bytes.
  expect(fsyncs.filter((f) => f.dir === layout.dropzone)).toEqual([
    { dir: layout.dropzone, entries: ['a.jpg'], claimed: ['1'], states: ['INGESTED'] },
  ]);
});

test('a put-back onto an occupied drop-zone path fails loudly and keeps the bytes', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  let second: IngestResult | undefined;
  const stale = createActivities({
    ...deps,
    checkpoint: async (n) => {
      if (n === 'upsert' && !second) {
        second = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
        await drop('a.jpg', Buffer.from('re-drop 1'));
      }
      if (n === 'claim') await drop('a.jpg', Buffer.from('re-drop 2')); // path taken again
    },
  });
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(/occupied/);
  const held = await readdir(layout.claimed);
  expect(held).toHaveLength(1);
  expect(await readFile(join(layout.claimed, held[0]!), 'utf8')).toBe('re-drop 1');
  expect(await readFile(join(layout.dropzone, 'a.jpg'), 'utf8')).toBe('re-drop 2');
});

test('a stale unhashed attempt never marks a parked copy vanished', async () => {
  await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], { runId: 'r0' });
  const ref = await drop('a.jpg', JPEG_A);
  let crashed = false;
  const stale = createActivities({
    ...deps,
    checkpoint: async (n) => {
      if (n === 'upsert' && !crashed) {
        crashed = true; // Y gets as far as parking the bytes in .copies, then dies
        await expect(
          runActivity(createActivities(crashAt('copies_rename')).ingestFile, [ref], {
            runId: 'r1',
          }),
        ).rejects.toThrow(SimulatedCrash);
      }
    },
  });
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    StaleAttemptError,
  );
  expect(await count('files', `state = 'QUARANTINED'`)).toBe(0);
  expect((await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).state).toBe('COPY');
});

test('put-back never returns bytes the row already committed (no double count)', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  let second: IngestResult | undefined;
  const stale = createActivities({
    ...deps,
    checkpoint: async (n) => {
      // X claimed the file itself; Y reuses X's claimed file, commits, and dies before its unlink.
      if (n === 'claim' && !second) {
        second = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
        await link(join(layout.root, vaultRel(second.sha256!, 'jpg')), join(layout.claimed, '1'));
      }
    },
  });
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    StaleAttemptError,
  );
  expect(existsSync(join(layout.dropzone, 'a.jpg'))).toBe(false);
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(second);
  expect(await readdir(layout.claimed)).toEqual([]);
});

test('the claim-step .copies shortcut fsyncs .copies before committing COPY', async () => {
  await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], { runId: 'r0' });
  const ref = await drop('a.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('copies_rename')).ingestFile, [ref], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  fsyncs.length = 0;
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe('COPY');
  expect(fsyncs).toEqual([
    {
      dir: layout.vaultCopies,
      entries: [`${r.fileId}.jpg`],
      claimed: [],
      states: ['INGESTED', 'CLAIMED'],
    },
  ]);
});

test('a stale unhashed attempt cannot re-hash a re-drop over a parked copy', async () => {
  await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], { runId: 'r0' });
  const ref = await drop('a.jpg', JPEG_A);
  const redrop = Buffer.concat([JPEG_A, Buffer.from('re-drop')]);
  let parked = false;
  const stale = createActivities({
    ...deps,
    checkpoint: async (n) => {
      // X stalls after its upsert (sha null); Y parks the bytes in .copies and dies before its
      // commit; the user drops new bytes at the same path.
      if (n === 'upsert' && !parked) {
        parked = true;
        await expect(
          runActivity(createActivities(crashAt('copies_rename')).ingestFile, [ref], {
            runId: 'r1',
          }),
        ).rejects.toThrow(SimulatedCrash);
        await drop('a.jpg', redrop);
      }
    },
  });
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    StaleAttemptError,
  );
  expect(await readFile(join(layout.dropzone, 'a.jpg'))).toEqual(redrop);
  expect(await readdir(layout.claimed)).toEqual([]);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe('COPY');
  expect(await readdir(layout.vaultCopies)).toEqual([`${r.fileId}.jpg`]);
  expect([await count('files'), await count('contents')]).toEqual([2, 1]);
});

/** Regular files anywhere under the vault (including .copies). */
async function vaultFiles(): Promise<string[]> {
  return (await readdir(layout.vault, { recursive: true, withFileTypes: true }))
    .filter((d) => d.isFile())
    .map((d) => d.name);
}

test('a file changed between scan and claim is put back and quarantined, never vaulted', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const grown = Buffer.concat([JPEG_A, Buffer.from('still being written')]);
  await writeFile(join(layout.dropzone, 'a.jpg'), grown); // the copy was still running
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect([r.state, r.sha256]).toEqual(['QUARANTINED', null]);
  expect(await readFile(join(layout.dropzone, 'a.jpg'))).toEqual(grown);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(await vaultFiles()).toEqual([]);
  const row = (
    await pool.query<{ state: string; last_error: string; sha256: string | null }>(
      'SELECT state, last_error, sha256 FROM files',
    )
  ).rows;
  expect(row).toEqual([{ state: 'QUARANTINED', last_error: 'changed_before_claim', sha256: null }]);
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(r); // replay
});

test('a file written to while it is hashed is put back and retried, never vaulted', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  const writing = createActivities({
    ...deps,
    checkpoint: async (n) => {
      if (n === 'hashed') {
        const [id] = await readdir(layout.claimed);
        await writeFile(join(layout.claimed, id!), Buffer.from(' more'), { flag: 'a' });
      }
    },
  });
  await expect(runActivity(writing.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(
    /changed while hashing/,
  );
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(await readFile(join(layout.dropzone, 'a.jpg'))).toEqual(
    Buffer.concat([JPEG_A, Buffer.from(' more')]),
  );
  expect(await vaultFiles()).toEqual([]);
  expect(await count('files', 'sha256 IS NOT NULL')).toBe(0);
  // The retry sees a file that no longer matches the scan: quarantined, left for the next scan.
  expect((await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).state).toBe('QUARANTINED');
  expect(existsSync(join(layout.dropzone, 'a.jpg'))).toBe(true);
});

// Spec §6.1 with removal (§6.5): a COPY's .copies file disappears by design once confirmed.
test('replaying a COPY whose duplicate was removed returns the stored outcome', async () => {
  await runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: 'r1' });
  const ref = await drop('b.jpg', JPEG_A);
  const b = await runActivity(acts.ingestFile, [ref], { runId: 'r2' });
  expect(await runActivity(acts.reclaimCopy, [b.fileId])).toBe('REMOVED');
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r2' })).toEqual(b);
  expect(await readdir(layout.vaultCopies)).toEqual([]);
  expect(await count('files', `state = 'COPY' AND copy_removed_at IS NOT NULL`)).toBe(1);
});

test('a re-drop of bytes whose earlier copy was removed is a COPY again, then removed', async () => {
  await runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: 'r1' });
  const ref = await drop('b.jpg', JPEG_A);
  const b = await runActivity(acts.ingestFile, [ref], { runId: 'r2' });
  expect(await runActivity(acts.reclaimCopy, [b.fileId])).toBe('REMOVED');
  const again = await drop('b.jpg', JPEG_A, BigInt(ref.mtimeNs));
  const b2 = await runActivity(acts.ingestFile, [again], { runId: 'r3' });
  expect([b2.state, b2.sha256]).toEqual(['COPY', b.sha256]);
  expect(b2.fileId).not.toBe(b.fileId);
  expect(await runActivity(acts.reclaimCopy, [b2.fileId])).toBe('REMOVED');
  expect(await vaultFiles()).toHaveLength(1);
  expect([await count('files'), await count('contents')]).toEqual([3, 1]);
});

test('a parked copy whose COPY commit crashed is committed, never removed before it', async () => {
  await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], { runId: 'r0' });
  const ref = await drop('a.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('copies_rename')).ingestFile, [ref], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  const { id } = (await pool.query<{ id: bigint }>(`SELECT id FROM files WHERE state = 'CLAIMED'`))
    .rows[0]!;
  // The row is not COPY yet: removal must leave its parked bytes alone.
  expect(await runActivity(acts.reclaimCopy, [String(id)])).toBe('NOT_COPY');
  expect(await runActivity(acts.sweepCopies, [10])).toMatchObject({ examined: 0 });
  expect(await readdir(layout.vaultCopies)).toEqual([`${String(id)}.jpg`]);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe('COPY');
  expect(await runActivity(acts.reclaimCopy, [r.fileId])).toBe('REMOVED');
});

// ---- Purge spec §8–§9: blocked bytes are turned away; unblocked ones come back.

const shaOf = (b: Buffer) => createHash('sha256').update(b).digest('hex');
const vaultOf = (sha: string) => join(layout.root, vaultRel(sha, 'jpg'));

/** A `purged` row for `bytes`: blocked, or (with `unblocked`) unblocked since. */
async function purgedRow(bytes: Buffer, unblocked = false): Promise<void> {
  await pool.query(
    `INSERT INTO purged (sha256, size_bytes, names, purged_at, unblocked_at)
     VALUES ($1, $2, '{old.jpg}', now(), CASE WHEN $3 THEN now() END)`,
    [shaOf(bytes), bytes.length, unblocked],
  );
}

/** Ingests `bytes`, then purges its content; `removed`: its vault file is already gone too. */
async function purgedContent(bytes: Buffer, removed: boolean): Promise<string> {
  const r = await runActivity(acts.ingestFile, [await drop('old.jpg', bytes)], { runId: 'r-old' });
  await pool.query(`UPDATE contents SET state = 'PURGED', purged_at = now() WHERE sha256 = $1`, [
    r.sha256,
  ]);
  if (removed) {
    await rm(vaultOf(r.sha256!));
    await pool.query(
      `UPDATE contents SET purge_removed_at = now(), purge_reclaimed_bytes = $2 WHERE sha256 = $1`,
      [r.sha256, bytes.length],
    );
  }
  return r.sha256!;
}

async function contentRow(sha: string) {
  return (
    await pool.query<{
      state: string;
      purged_at: Date | null;
      purge_removed_at: Date | null;
      purge_reclaimed_bytes: bigint | null;
      purge_mismatch: boolean;
    }>(
      `SELECT state, purged_at, purge_removed_at, purge_reclaimed_bytes, purge_mismatch
         FROM contents WHERE sha256 = $1`,
      [sha],
    )
  ).rows[0];
}

const RESTORED = {
  state: 'INGESTED',
  purged_at: null,
  purge_removed_at: null,
  purge_reclaimed_bytes: null,
  purge_mismatch: false,
};

test('a blocked sha with no content row (wiped database) is deleted, BLOCKED, never vaulted', async () => {
  await purgedRow(JPEG_A);
  const ref = await drop('again.jpg', JPEG_A);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r).toEqual({
    fileId: r.fileId,
    sha256: shaOf(JPEG_A),
    ext: 'jpg',
    state: 'BLOCKED',
    kind: null,
  });
  expect(existsSync(join(layout.dropzone, 'again.jpg'))).toBe(false);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(await vaultFiles()).toEqual([]);
  expect(await count('contents')).toBe(0);
  expect(await count('files', `state = 'BLOCKED' AND original_name = 'again.jpg'`)).toBe(1);
  // The deletion was durable before BLOCKED was committed.
  const sync = fsyncs.find((f) => f.dir === layout.claimed && f.claimed.length === 0);
  expect(sync?.states).toEqual(['CLAIMED']);
});

test('a blocked file whose content is PURGED with its vault file still there is blocked; the vault is untouched (review focus 5)', async () => {
  const sha = await purgedContent(JPEG_A, false);
  await purgedRow(JPEG_A);
  const before = await stat(vaultOf(sha));
  const r = await runActivity(acts.ingestFile, [await drop('again.jpg', JPEG_A)], {
    runId: 'r1',
  });
  expect(r.state).toBe('BLOCKED');
  const after = await stat(vaultOf(sha));
  expect([after.ino, after.nlink]).toEqual([before.ino, 1]);
  expect(await readdir(layout.vaultCopies)).toEqual([]);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect((await contentRow(sha))?.state).toBe('PURGED');
  expect(await count('contents')).toBe(1);
});

test('a blocked file whose purged vault file is already removed stays out of the vault', async () => {
  const sha = await purgedContent(JPEG_A, true);
  await purgedRow(JPEG_A);
  const r = await runActivity(acts.ingestFile, [await drop('again.jpg', JPEG_A)], {
    runId: 'r1',
  });
  expect(r.state).toBe('BLOCKED');
  expect(await vaultFiles()).toEqual([]);
  expect((await contentRow(sha))?.purge_removed_at).toBeInstanceOf(Date);
});

test('re-running a BLOCKED ingest returns the stored outcome', async () => {
  await purgedRow(JPEG_A);
  const ref = await drop('again.jpg', JPEG_A);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(r);
  expect(await count('files')).toBe(1);
});

test('a crash after deleting a blocked file resumes to BLOCKED, never to the vault owner', async () => {
  await purgedContent(JPEG_A, false); // the vault file is there: recovery must not adopt it
  await purgedRow(JPEG_A);
  const ref = await drop('again.jpg', JPEG_A);
  await expect(
    runActivity(createActivities(crashAt('block_unlink')).ingestFile, [ref], { runId: 'r1' }),
  ).rejects.toThrow(SimulatedCrash);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe('BLOCKED');
  expect(await count('files', `state = 'INGESTED'`)).toBe(1); // only the first drop
});

test('an unblocked sha whose purged vault file was removed is vaulted again and its content restored', async () => {
  const sha = await purgedContent(JPEG_A, true);
  await purgedRow(JPEG_A, true);
  const ref = await drop('back.jpg', JPEG_A);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect([r.state, r.kind, r.sha256]).toEqual(['INGESTED', 'still', sha]);
  expect(await readFile(vaultOf(sha))).toEqual(JPEG_A);
  expect((await stat(vaultOf(sha))).mode & 0o777).toBe(0o444);
  expect(await readdir(layout.claimed)).toEqual([]);
  expect(await contentRow(sha)).toEqual(RESTORED);
  // A replay returns the same outcome.
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(r);
});

test('an unblocked sha whose purged vault file is still there is restored; the drop is a COPY of it', async () => {
  const sha = await purgedContent(JPEG_A, false);
  await purgedRow(JPEG_A, true);
  const r = await runActivity(acts.ingestFile, [await drop('back.jpg', JPEG_A)], { runId: 'r1' });
  expect([r.state, r.sha256]).toEqual(['COPY', sha]);
  expect(await contentRow(sha)).toEqual(RESTORED);
  // Its removal no longer applies: the vault file stays.
  expect(await runActivity(acts.removePurgedBatch, [[sha]])).toMatchObject({ examined: 0 });
  expect(await readFile(vaultOf(sha))).toEqual(JPEG_A);
});

test('a removal waiting on the content row while it is restored never deletes the vault file', async () => {
  const sha = await purgedContent(JPEG_A, false);
  await purgedRow(JPEG_A, true);
  let removal: Promise<unknown> | undefined;
  const restoring = createActivities({
    ...deps,
    checkpoint: async (n) => {
      if (n !== 'restore_link') return;
      // The removal starts now and must wait for the restore's row lock.
      removal = runActivity(acts.removePurgedBatch, [[sha]]).catch((e: unknown) => e);
      await vi.waitFor(async () => expect(await lockWaiters()).toBe(1));
    },
  });
  const r = await runActivity(restoring.ingestFile, [await drop('back.jpg', JPEG_A)], {
    runId: 'r1',
  });
  expect(r.state).toBe('COPY');
  expect(String(await removal)).toMatch(/not PURGED/);
  expect(await readFile(vaultOf(sha))).toEqual(JPEG_A);
  expect(await contentRow(sha)).toEqual(RESTORED);
});

test('a restore waiting on a removal in flight vaults the dropped bytes as the owner', async () => {
  const sha = await purgedContent(JPEG_A, false);
  await purgedRow(JPEG_A, true);
  // Stands in for removePurged: holds the row lock, removes the file, records it.
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query('SELECT 1 FROM contents WHERE sha256 = $1 FOR UPDATE', [sha]);
    const ingest = runActivity(acts.ingestFile, [await drop('back.jpg', JPEG_A)], {
      runId: 'r1',
    });
    await vi.waitFor(async () => expect(await lockWaiters()).toBe(1));
    await rm(vaultOf(sha));
    await c.query(
      `UPDATE contents SET purge_removed_at = now(), purge_reclaimed_bytes = 1 WHERE sha256 = $1`,
      [sha],
    );
    await c.query('COMMIT');
    const r = await ingest;
    expect([r.state, r.kind]).toEqual(['INGESTED', 'still']);
  } finally {
    c.release();
  }
  expect(await readFile(vaultOf(sha))).toEqual(JPEG_A);
  expect(await contentRow(sha)).toEqual(RESTORED);
});

test('an unblocked sha with no content row (wiped database) is ingested as new content', async () => {
  await purgedRow(JPEG_A, true);
  const r = await runActivity(acts.ingestFile, [await drop('back.jpg', JPEG_A)], { runId: 'r1' });
  expect([r.state, r.kind]).toEqual(['INGESTED', 'still']);
  expect((await contentRow(shaOf(JPEG_A)))?.state).toBe('INGESTED');
});

async function lockWaiters(): Promise<number> {
  return count('pg_stat_activity', `wait_event_type = 'Lock'`);
}
