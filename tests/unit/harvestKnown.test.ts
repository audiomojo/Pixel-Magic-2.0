import { createHash } from 'node:crypto';
import { lstat, mkdir, rm, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { checkKnown, promoteDuplicates, type KnownOptions } from '../../src/harvest/known.js';
import { createRun } from '../../src/harvest/runs.js';
import { registerSources } from '../../src/harvest/sources.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(() => pgh.truncate());

const sha = (b: Buffer) => createHash('sha256').update(b).digest('hex');
const KIB = 1024;
/** Deterministic bytes of `n` length; `tail` changes the last byte only. */
const bytes = (n: number, seed = 1, tail = 0) => {
  const b = Buffer.alloc(n, seed);
  if (n > 0) b[n - 1] = tail;
  return b;
};

interface Item {
  id: string;
  sourceId: string;
  absPath: string;
  sizeBytes: bigint;
  kind: 'photo' | 'maybe';
  sha256: string | null;
  headHash: string | null;
  pathKey: string;
  relPath: string;
  runId: string | null;
  /** Relative to the source root (and, for SENT, under harvest/Drive/ in the drop zone). */
  rel: string;
}

async function setup() {
  const root = await tmpDir();
  const dz = await tmpDir();
  const [s] = await registerSources(pgh.pool, [
    { identity: 'local:U-1', labelBase: 'Drive', kind: 'local', mountPath: root },
  ]);
  const sourceId = s!.id;
  // The run being sent: folder 'd' (every item below unless `dir` says otherwise).
  const runId = await createRun(pgh.pool, [{ sourceId, relPath: 'd' }], false);
  let n = 0;
  /**
   * Writes the file and inserts the item row (its real size and mtime). A SENT item also gets
   * its drop-zone copy (`copy: false` leaves it out) with `dropzone_mtime_ns` recorded.
   */
  const add = async (
    data: Buffer,
    o: {
      state?: string;
      kind?: 'photo' | 'maybe';
      sha?: string;
      head?: string;
      dup?: string;
      dir?: string;
      copy?: boolean;
    } = {},
  ): Promise<Item> => {
    const rel = `${o.dir ?? 'd'}/f${String(++n).padStart(2, '0')}.jpg`;
    const abs = join(root, rel);
    await mkdir(dirname(abs), { recursive: true });
    await writeFile(abs, data);
    const mtime = (await lstat(abs, { bigint: true })).mtimeNs;
    const kind = o.kind ?? 'photo';
    const state = o.state ?? 'PLANNED';
    let dzPath: string | null = null;
    let dzMtime: bigint | null = null;
    if (state === 'SENT') {
      dzPath = `harvest/Drive/${rel}`;
      const copy = join(dz, dzPath);
      await mkdir(dirname(copy), { recursive: true });
      await writeFile(copy, data);
      dzMtime = (await lstat(copy, { bigint: true })).mtimeNs;
      if (o.copy === false) await rm(copy);
    }
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state,
                                  sha256, head_hash, duplicate_of, dropzone_path, dropzone_mtime_ns)
       VALUES ($1, $2, lower($2), $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id::text`,
      [
        sourceId,
        rel,
        data.length,
        mtime.toString(),
        kind,
        state,
        o.sha ?? null,
        o.head ?? null,
        o.dup ?? null,
        dzPath,
        dzMtime?.toString() ?? null,
      ],
    );
    return {
      id: rows[0]!.id,
      sourceId,
      absPath: abs,
      sizeBytes: BigInt(data.length),
      kind,
      sha256: o.sha ?? null,
      headHash: o.head ?? null,
      pathKey: rel.toLowerCase(),
      relPath: rel,
      runId,
      rel,
    };
  };
  const addContent = (data: Buffer, hash = sha(data)) =>
    pgh.pool.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state) VALUES ($1, 'jpg', 'still', $2, 'INGESTED')`,
      [hash, data.length],
    );
  const row = async (id: string) =>
    (
      await pgh.pool.query<{
        sha256: string | null;
        head_hash: string | null;
        last_error: string | null;
        state: string;
        duplicate_of: string | null;
      }>(
        'SELECT sha256, head_hash, last_error, state, duplicate_of::text FROM harvest_items WHERE id = $1',
        [id],
      )
    ).rows[0]!;
  /** checkKnown with the sendBatch wiring: the source mounted at `root`, the drop zone `dz`. */
  const ck = (item: Item, opts: KnownOptions = {}) =>
    checkKnown(pgh.pool, item, {
      mountOf: (id) => Promise.resolve(id === sourceId ? root : null),
      dropzone: dz,
      ...opts,
    });
  return { root, dz, runId, sourceId, add, addContent, row, ck };
}

describe('checkKnown', () => {
  test('a unique size is SEND with no hashing and nothing stored', async () => {
    const { add, addContent, row, ck } = await setup();
    await addContent(bytes(100, 9));
    await add(bytes(300, 2));
    const item = await add(bytes(200, 3));
    const hash = vi.fn();
    const out = await ck(item, { sha256File: hash });
    expect(out).toEqual({ state: 'SEND', sha256: null, headHash: null });
    expect(hash).not.toHaveBeenCalled();
    expect(await row(item.id)).toMatchObject({ sha256: null, head_hash: null, last_error: null });
  });

  test('size and sha in contents is KNOWN; the hash is stored', async () => {
    const { add, addContent, row, ck } = await setup();
    const data = bytes(200 * KIB, 4);
    await addContent(data);
    const item = await add(data);
    expect(await ck(item)).toEqual({ state: 'KNOWN', sha256: sha(data) });
    expect(await row(item.id)).toMatchObject({ sha256: sha(data) });
  });

  test('size matches contents but the sha differs: SEND, hash kept', async () => {
    const { add, addContent, row, ck } = await setup();
    await addContent(bytes(200 * KIB, 4));
    const mine = bytes(200 * KIB, 5);
    const item = await add(mine);
    expect(await ck(item)).toEqual({
      state: 'SEND',
      sha256: sha(mine),
      headHash: null,
    });
    expect(await row(item.id)).toMatchObject({ sha256: sha(mine) });
  });

  test('a contents row with the same sha but another size is not KNOWN', async () => {
    const { add, addContent, ck } = await setup();
    const data = bytes(1000, 6);
    await addContent(bytes(1000, 7)); // same size, different content
    await addContent(data, sha(data)); // right sha …
    await pgh.pool.query('UPDATE contents SET size_bytes = 5 WHERE sha256 = $1', [sha(data)]); // … wrong size
    const out = await ck(await add(data));
    expect(out.state).toBe('SEND');
  });

  test('same size and head but different tail: full hashes differ, SEND', async () => {
    const { add, row, ck } = await setup();
    const other = await add(bytes(200 * KIB, 8, 1), { state: 'SENT' });
    const item = await add(bytes(200 * KIB, 8, 2));
    const out = await ck(item);
    expect(out.state).toBe('SEND');
    // Heads collided, so both full hashes were computed and stored.
    expect((await row(item.id)).sha256).toHaveLength(64);
    expect((await row(other.id)).sha256).toHaveLength(64);
    expect((await row(item.id)).sha256).not.toBe((await row(other.id)).sha256);
  });

  test('different heads: no full hash is computed', async () => {
    const { add, row, ck } = await setup();
    const other = await add(bytes(200 * KIB, 1), { state: 'SENT' });
    const item = await add(bytes(200 * KIB, 2));
    expect((await ck(item)).state).toBe('SEND');
    expect(await row(item.id)).toMatchObject({ sha256: null });
    expect((await row(item.id)).head_hash).toHaveLength(64);
    expect((await row(other.id)).head_hash).toHaveLength(64);
  });

  test('an identical earlier SENT item gives DUP', async () => {
    const { add, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const winner = await add(data, { state: 'SENT' });
    const item = await add(data);
    expect(await ck(item)).toEqual({
      state: 'DUP',
      sha256: sha(data),
      duplicateOf: winner.id,
    });
    expect((await row(item.id)).state).toBe('PLANNED'); // checkKnown never changes state
  });

  test('a SENT item wins even when its id is later', async () => {
    const { add, ck } = await setup();
    const data = bytes(10, 3);
    const item = await add(data);
    const sent = await add(data, { state: 'SENT' });
    expect(await ck(item)).toMatchObject({ state: 'DUP', duplicateOf: sent.id });
  });

  test('small files: head hash is the whole file, stored in both columns', async () => {
    const { add, row, ck } = await setup();
    const data = bytes(1000, 3);
    const winner = await add(data, { state: 'SENT' });
    const item = await add(data);
    expect(await ck(item)).toMatchObject({
      state: 'DUP',
      duplicateOf: winner.id,
    });
    for (const id of [item.id, winner.id]) {
      expect(await row(id)).toMatchObject({ sha256: sha(data), head_hash: sha(data) });
    }
  });

  test('a later PLANNED item is not a winner; an earlier PLANNED one is', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    const first = await add(data);
    const second = await add(data);
    expect((await ck(first)).state).toBe('SEND');
    expect(await ck(second)).toMatchObject({
      state: 'DUP',
      duplicateOf: first.id,
    });
  });

  test('photo before maybe: a maybe is a DUP of a later-id photo, not the reverse', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    const maybe = await add(data, { kind: 'maybe' });
    const photo = await add(data, { kind: 'photo' });
    expect(await ck(maybe)).toMatchObject({
      state: 'DUP',
      duplicateOf: photo.id,
    });
    expect((await ck(photo)).state).toBe('SEND');
  });

  test('a KNOWN ledger item is not a candidate (KNOWN needs a contents row)', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    await add(data, { state: 'KNOWN', sha: sha(data) });
    expect((await ck(await add(data))).state).toBe('SEND');
  });

  test('a small candidate with a stored sha but no head is compared without a read', async () => {
    const { add, ck } = await setup();
    const data = bytes(1000, 3);
    const winner = await add(data, { state: 'SENT', sha: sha(data) });
    await pgh.pool.query('UPDATE harvest_sources SET last_mount_path = NULL');
    const item = await add(data);
    expect(await ck(item)).toMatchObject({
      state: 'DUP',
      duplicateOf: winner.id,
    });
  });

  test('the earliest of several identical winners is chosen (photo first, then id)', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    await add(data, { state: 'SENT', kind: 'maybe' });
    const p2 = await add(data, { state: 'SENT' });
    await add(data, { state: 'SENT' });
    const out = await ck(await add(data));
    expect(out).toMatchObject({ state: 'DUP', duplicateOf: p2.id });
  });

  test('a DUP item resolves to its target when the target qualifies', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    const target = await add(data, { state: 'SENT' });
    await add(data, { state: 'DUP', dup: target.id, sha: sha(data) });
    const out = await ck(await add(data));
    expect(out).toMatchObject({ state: 'DUP', duplicateOf: target.id });
  });

  test('a DUP item whose target is gone does not qualify', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    const failed = await add(data, { state: 'FAILED' });
    await add(data, { state: 'DUP', dup: failed.id, sha: sha(data) });
    expect((await ck(await add(data))).state).toBe('SEND');
  });

  test('FAILED, VANISHED, REVIEW items are not candidates', async () => {
    const { add, ck } = await setup();
    const data = bytes(100 * KIB, 3);
    for (const state of ['FAILED', 'VANISHED', 'REVIEW', 'REJECTED']) await add(data, { state });
    expect((await ck(await add(data))).state).toBe('SEND');
  });

  test('a full hash reports progress through onChunk', async () => {
    const { add, addContent, ck } = await setup();
    const data = bytes(3 * 1024 * KIB, 9);
    await addContent(bytes(3 * 1024 * KIB, 8)); // same size, other bytes: hashed, not known
    const item = await add(data);
    let chunks = 0;
    const out = await ck(item, { onChunk: () => chunks++ });
    expect(out.state).toBe('SEND');
    expect(chunks).toBeGreaterThanOrEqual(3);
  });

  test('a candidate whose on-disk name differs from rel_path is read through rel_path_raw', async () => {
    const { add, ck } = await setup();
    const data = bytes(200 * KIB, 4);
    const winner = await add(data); // an earlier PLANNED item: read from its source
    // The stored rel_path no longer opens the file; its raw bytes do.
    await pgh.pool.query(
      `UPDATE harvest_items SET rel_path_raw = convert_to(rel_path, 'UTF8'), rel_path = 'd/a-nope.jpg'
       WHERE id = $1`,
      [winner.id],
    );
    const item = await add(data);
    expect(await ck(item)).toMatchObject({
      state: 'DUP',
      duplicateOf: winner.id,
    });
  });

  test('stored hashes are reused: no second read', async () => {
    const { add, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    await add(data, { state: 'SENT' });
    const item = await add(data);
    const spy = vi.fn((p: string | Buffer) =>
      import('../../src/fsops.js').then((m) => m.sha256File(p)),
    );
    const first = await ck(item, { sha256File: spy });
    expect(first.state).toBe('DUP');
    expect(spy).toHaveBeenCalledTimes(2);
    // Re-run with a missing file: stored hashes (ours and the winner's) suffice.
    const stored = await pgh.pool.query<{ sha256: string; head_hash: string }>(
      'SELECT sha256, head_hash FROM harvest_items WHERE id = $1',
      [item.id],
    );
    const again = {
      ...item,
      absPath: '/nonexistent',
      sha256: stored.rows[0]!.sha256,
      headHash: stored.rows[0]!.head_hash,
    };
    spy.mockClear();
    expect(await ck(again, { sha256File: spy })).toEqual(first);
    expect(spy).not.toHaveBeenCalled();
  });

  test('a hash read error is SEND with the error recorded, and does not throw', async () => {
    const { add, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    await add(data, { state: 'SENT' });
    const item = { ...(await add(data)), absPath: '/nonexistent/file.jpg' };
    const out = await ck(item);
    expect(out).toEqual({ state: 'SEND', sha256: null, headHash: null });
    expect((await row(item.id)).last_error).toMatch(/ENOENT/);
  });

  test('a full-hash read error on our file is SEND with the error recorded', async () => {
    const { add, addContent, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    await addContent(data);
    const item = await add(data);
    const out = await ck(item, {
      sha256File: () => Promise.reject(new Error('EIO: input/output error')),
    });
    expect(out.state).toBe('SEND');
    expect((await row(item.id)).last_error).toMatch(/EIO/);
  });

  test('an unreadable candidate is skipped, not an error', async () => {
    const { add, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const gone = await add(data);
    await pgh.pool.query("UPDATE harvest_items SET rel_path = 'missing.jpg' WHERE id = $1", [
      gone.id,
    ]);
    const item = await add(data);
    expect((await ck(item)).state).toBe('SEND');
    expect((await row(item.id)).last_error).toBeNull();
  });

  test('a PLANNED candidate whose source is not mounted is skipped', async () => {
    const { add, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    await add(data);
    const item = await add(data);
    expect((await ck(item, { mountOf: () => Promise.resolve(null) })).state).toBe('SEND');
  });

  // ---- I2 (a): a SENT candidate is never read from its source -------------------------------
  test('a SENT candidate is read through its drop-zone copy, never its source', async () => {
    const { add, root, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const winner = await add(data, { state: 'SENT' });
    // Its source was edited in place afterwards (same size): the copy still holds `data`.
    await writeFile(join(root, winner.rel), bytes(200 * KIB, 9));
    const item = await add(data);
    expect(await ck(item)).toMatchObject({ state: 'DUP', duplicateOf: winner.id });
    // And the reverse: the source matches, but the copy (what was sent) does not.
    const other = await add(bytes(200 * KIB, 5), { state: 'SENT' });
    await writeFile(join(root, other.rel), bytes(200 * KIB, 6));
    expect((await ck(await add(bytes(200 * KIB, 6)))).state).toBe('SEND');
  });

  test('an unlinked SENT candidate whose copy is gone or changed does not match', async () => {
    const { add, dz, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const gone = await add(data, { state: 'SENT', copy: false });
    expect((await ck(await add(data))).state).toBe('SEND');
    expect(await row(gone.id)).toMatchObject({ sha256: null, head_hash: null });
    // A copy with another mtime is not the file we wrote (the pipeline may have replaced it).
    const data2 = bytes(300 * KIB, 4);
    const touched = await add(data2, { state: 'SENT' });
    await utimes(join(dz, `harvest/Drive/${touched.rel}`), 1, 1);
    expect((await ck(await add(data2))).state).toBe('SEND');
    expect(await row(touched.id)).toMatchObject({ sha256: null, head_hash: null });
  });

  test('a linked SENT candidate is compared by files.sha256', async () => {
    const { add, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const linked = await add(data, { state: 'SENT', copy: false });
    const f = await pgh.pool.query<{ id: string }>(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state, sha256)
       VALUES ('harvest/Drive/x.jpg', 'x.jpg', $1, 1, 'INGESTED', $2) RETURNING id::text AS id`,
      [data.length, sha(data)],
    );
    await pgh.pool.query('UPDATE harvest_items SET file_id = $2 WHERE id = $1', [
      linked.id,
      f.rows[0]!.id,
    ]);
    const first = await add(data);
    expect(await ck(first)).toMatchObject({ state: 'DUP', duplicateOf: linked.id });
    await pgh.pool.query("UPDATE harvest_items SET state = 'DUP' WHERE id = $1", [first.id]);
    // Not yet hashed by the pipeline: nothing to compare with (the copy is the pipeline's now).
    await pgh.pool.query('UPDATE files SET sha256 = NULL');
    expect((await ck(await add(data))).state).toBe('SEND');
  });

  // ---- I2 (b): a PLANNED candidate is read only while it is still the scanned file ---------
  test('a PLANNED candidate edited in place (same size, new mtime) is not hashed: SEND', async () => {
    const { add, root, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const edited = await add(bytes(200 * KIB, 8));
    // Rewritten with our bytes: same size, another mtime than the scan recorded.
    await writeFile(join(root, edited.rel), data);
    await utimes(join(root, edited.rel), 2_000_000_000, 2_000_000_000);
    const item = await add(data);
    expect((await ck(item)).state).toBe('SEND');
    expect(await row(edited.id)).toMatchObject({ sha256: null, head_hash: null });
  });

  test('a PLANNED candidate on the wrong volume (its identity is not mounted) is no match', async () => {
    const { add, row, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    // last_mount_path still points at the old mount point, where another volume now holds an
    // identical file; detection by identity says the candidate's source is not mounted.
    const cand = await add(data);
    const item = await add(data);
    expect((await ck(item, { mountOf: () => Promise.resolve(null) })).state).toBe('SEND');
    expect(await row(cand.id)).toMatchObject({ sha256: null, head_hash: null });
  });

  // ---- I2 (c): a PLANNED candidate must be this run's and earlier in its send order --------
  test('a PLANNED item outside the run is not a winner', async () => {
    const { add, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    await add(data, { dir: 'elsewhere' }); // PLANNED, earlier id, but not in the run's folders
    expect((await ck(await add(data))).state).toBe('SEND');
  });

  test('send order is folder position, then key: a lower id later in the run is no winner', async () => {
    const { add, sourceId, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const late = await add(data, { dir: 'b' }); // lower id, second folder
    const early = await add(data, { dir: 'a' });
    const run = await createRun(
      pgh.pool,
      [
        { sourceId, relPath: 'a' },
        { sourceId, relPath: 'b' },
      ],
      false,
    );
    expect((await ck({ ...early, runId: run })).state).toBe('SEND');
    expect(await ck({ ...late, runId: run })).toMatchObject({
      state: 'DUP',
      duplicateOf: early.id,
    });
  });

  test('a send-only run: a PLANNED item it was not given is not a winner', async () => {
    const { add, sourceId, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    const other = await add(data);
    const mine = await add(data);
    const so = await createRun(pgh.pool, [{ sourceId, relPath: 'd' }], {
      sendOnly: true,
      items: [mine.id],
    });
    expect((await ck({ ...mine, runId: so })).state).toBe('SEND');
    const both = await createRun(pgh.pool, [{ sourceId, relPath: 'd' }], {
      sendOnly: true,
      items: [mine.id, other.id],
    });
    expect(await ck({ ...mine, runId: both })).toMatchObject({
      state: 'DUP',
      duplicateOf: other.id,
    });
  });

  test('no run: no PLANNED candidate qualifies', async () => {
    const { add, ck } = await setup();
    const data = bytes(200 * KIB, 3);
    await add(data);
    expect((await ck({ ...(await add(data)), runId: null })).state).toBe('SEND');
  });
});

describe('promoteDuplicates', () => {
  test('winner lost: the earliest DUP becomes PLANNED and the rest point at it', async () => {
    const { add, row } = await setup();
    const data = bytes(100, 3);
    const lost = await add(data, { state: 'FAILED' });
    const maybe = await add(data, { state: 'DUP', dup: lost.id, kind: 'maybe' });
    const d2 = await add(data, { state: 'DUP', dup: lost.id });
    const d3 = await add(data, { state: 'DUP', dup: lost.id });
    expect(await promoteDuplicates(pgh.pool, lost.id)).toBe(3);
    expect(await row(d2.id)).toMatchObject({ state: 'PLANNED', duplicate_of: null });
    expect(await row(d3.id)).toMatchObject({ state: 'DUP', duplicate_of: d2.id });
    expect(await row(maybe.id)).toMatchObject({ state: 'DUP', duplicate_of: d2.id });
  });

  test('nothing points at the item: 0; non-DUP pointers are untouched', async () => {
    const { add, row } = await setup();
    const lost = await add(bytes(10), { state: 'VANISHED' });
    expect(await promoteDuplicates(pgh.pool, lost.id)).toBe(0);
    const other = await add(bytes(10), { state: 'KNOWN', dup: lost.id });
    expect(await promoteDuplicates(pgh.pool, lost.id)).toBe(0);
    expect(await row(other.id)).toMatchObject({ state: 'KNOWN', duplicate_of: lost.id });
  });
});
