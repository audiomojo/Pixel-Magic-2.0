// Purging a similar set (spec §3, §5, §9): the closure, the purge transaction and unblock.
import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
import { currentShown, purgeClosure, purgeSet, SHOWN_CANDIDATE, unblock } from '../../src/purge.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let pgh: PgHandle;
let pool: pg.Pool;
beforeAll(async () => {
  pgh = await startPg();
  pool = pgh.pool;
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
});

const sha = (c: string) => c.repeat(64);
const SET = '100';
const OTHER_SET = '200';
const MOTION = sha('6');

async function group(id: number, setId: string | null, mergedInto: number | null = null) {
  await pool.query(
    `INSERT INTO groups (id, kind, merged_into, similar_set_id) VALUES ($1, 'copy', $2, $3)`,
    [id, mergedInto, setId],
  );
}

interface C {
  kind?: 'still' | 'motion';
  state?: string;
  group?: number | null;
  live?: string | null;
  size?: number;
  names?: string[];
  candidateOf?: string;
  described?: boolean | 'failed';
}
async function content(s: string, o: C = {}) {
  const state = o.state ?? 'CLUSTERED';
  await pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state, copy_group_id, live_content_id,
                           duplicate_of, purged_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [
      s,
      o.kind === 'motion' ? 'mov' : 'jpg',
      o.kind ?? 'still',
      o.size ?? 1000,
      state,
      o.group ?? null,
      o.live ?? null,
      state === 'DUPLICATE' ? sha('f') : null,
      state === 'PURGED' ? new Date() : null,
    ],
  );
  for (const [i, n] of (o.names ?? [`${s.slice(0, 4)}.jpg`]).entries())
    await pool.query(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
       VALUES ($1, $2, $3, 1, 'jpg', $4, 'INGESTED')`,
      [`d/${s.slice(0, 4)}-${String(i)}/${n}`, n, o.size ?? 1000, s],
    );
  if (o.candidateOf !== undefined) {
    await pool.query('INSERT INTO similar_candidates (set_id, sha256) VALUES ($1, $2)', [
      o.candidateOf,
      s,
    ]);
    if (o.described !== false)
      await pool.query(
        `INSERT INTO photo_features (sha256, feature_version, embedding, quality, faces, failed)
         VALUES ($1, 1, '{}', '{}', '[]', $2)`,
        [s, o.described === 'failed' ? 'boom' : null],
      );
  }
}

/**
 * Set 100: groups 1 and 2 (shown a, b); group 1 also holds a hidden copy c; group 3 was merged
 * into group 2 and still holds d; a's Live Photo video m; a retired duplicate x in group 1.
 * Set 200: group 9 with e (shown) — never touched.
 */
async function seed() {
  await group(1, SET);
  await group(2, SET);
  await group(3, null, 2);
  await group(9, OTHER_SET);
  await content(sha('a'), { group: 1, live: 'LIVE-A', candidateOf: SET, size: 3000 });
  await content(sha('b'), { group: 2, candidateOf: SET, size: 2000, names: ['b1.jpg', 'b2.jpg'] });
  await content(sha('c'), { group: 1, state: 'HASHED', size: 500 });
  await content(sha('d'), { group: 3, size: 400 });
  await content(MOTION, { kind: 'motion', state: 'INGESTED', live: 'LIVE-A', size: 7000 });
  await content(sha('7'), { group: 1, state: 'DUPLICATE', size: 300 });
  await content(sha('e'), { group: 9, candidateOf: OTHER_SET, live: 'LIVE-E' });
  await content(sha('8'), { kind: 'motion', state: 'INGESTED', live: 'LIVE-E' });
}
test('the shown predicate is a reusable SQL fragment over similar_candidates sc', () => {
  expect(SHOWN_CANDIDATE).toMatch(/\bsc\.sha256\b/);
  expect(SHOWN_CANDIDATE).toMatch(/failed IS NULL/);
  expect(SHOWN_CANDIDATE).toMatch(/PURGED/);
  expect(SHOWN_CANDIDATE).toMatch(/DUPLICATE/);
});

test('currentShown: described, not failed, not PURGED/DUPLICATE, ascending', async () => {
  await seed();
  await content(sha('1'), { group: 2, candidateOf: SET, described: false });
  await content(sha('2'), { group: 2, candidateOf: SET, described: 'failed' });
  await content(sha('3'), { group: 2, candidateOf: SET, state: 'PURGED' });
  await content(sha('4'), { group: 2, candidateOf: SET, state: 'DUPLICATE' });
  expect(await currentShown(pool, SET)).toEqual([sha('a'), sha('b')]);
  expect(await currentShown(pool, OTHER_SET)).toEqual([sha('e')]);
  expect(await currentShown(pool, '999')).toEqual([]);
});

test('closure: hidden copies, a merged group, the Live video; not other sets or DUPLICATEs', async () => {
  await seed();
  const s = await purgeClosure(pool, SET, [sha('a'), sha('b')]);
  if (s === 'stale') throw new Error('stale');
  expect(s.shas).toEqual([sha('a'), sha('b'), sha('c'), sha('d'), MOTION].sort());
  expect(s).toMatchObject({
    shown: 2,
    copies: 2,
    live: 1,
    bytes: 3000n + 2000n + 500n + 400n + 7000n,
  });
});

test('purgeSet marks the closure PURGED and upserts purged rows', async () => {
  await seed();
  const s = await purgeSet(pool, SET, [sha('b'), sha('a')]);
  expect(s).not.toBe('stale');
  if (s === 'stale') return;
  expect(s).toMatchObject({ shown: 2, copies: 2, live: 1 });
  const cs = await pool.query<{ sha256: string; state: string; purged_at: Date | null }>(
    'SELECT sha256, state, purged_at FROM contents ORDER BY sha256',
  );
  const purged = cs.rows.filter((r) => r.state === 'PURGED').map((r) => r.sha256);
  expect(purged).toEqual(s.shas);
  expect(cs.rows.find((r) => r.sha256 === sha('e'))!.state).toBe('CLUSTERED');
  expect(cs.rows.find((r) => r.sha256 === sha('7'))!.state).toBe('DUPLICATE');
  const p = await pool.query<{
    sha256: string;
    size_bytes: bigint;
    names: string[];
    set_id: bigint;
    unblocked_at: Date | null;
  }>('SELECT sha256, size_bytes, names, set_id, unblocked_at FROM purged ORDER BY sha256');
  expect(p.rows.map((r) => r.sha256)).toEqual(s.shas);
  const b = p.rows.find((r) => r.sha256 === sha('b'))!;
  expect(b).toMatchObject({ size_bytes: 2000n, names: ['b1.jpg', 'b2.jpg'], set_id: 100n });
  expect(b.unblocked_at).toBeNull();
  // A purge is not a pick.
  expect((await pool.query('SELECT 1 FROM picks')).rowCount).toBe(0);
});

test('purgeSet is stale when a shown sha is missing or extra; nothing changes', async () => {
  await seed();
  expect(await purgeSet(pool, SET, [sha('a')])).toBe('stale');
  expect(await purgeSet(pool, SET, [sha('a'), sha('b'), sha('c')])).toBe('stale');
  expect(await purgeSet(pool, SET, [])).toBe('stale');
  expect((await pool.query("SELECT 1 FROM contents WHERE state = 'PURGED'")).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM purged')).rowCount).toBe(0);
});

test('a second purgeSet returns the same summary and does not re-stamp purged_at', async () => {
  await seed();
  const first = await purgeSet(pool, SET, [sha('a'), sha('b')]);
  const at = (await pool.query<{ purged_at: Date }>('SELECT purged_at FROM purged ORDER BY sha256'))
    .rows;
  const second = await purgeSet(pool, SET, [sha('a'), sha('b')]);
  expect(second).toEqual(first);
  const again = (
    await pool.query<{ purged_at: Date }>('SELECT purged_at FROM purged ORDER BY sha256')
  ).rows;
  expect(again).toEqual(at);
});

test('a purge after an unblock re-blocks: purged_at renewed, unblocked_at cleared', async () => {
  await seed();
  await pool.query(
    `INSERT INTO purged (sha256, size_bytes, names, purged_at, unblocked_at, set_id)
     VALUES ($1, 2000, '{old.jpg}', '2020-01-01', '2020-02-01', 5)`,
    [sha('b')],
  );
  expect(await purgeSet(pool, SET, [sha('a'), sha('b')])).not.toBe('stale');
  const r = (
    await pool.query<{ purged_at: Date; unblocked_at: Date | null; set_id: bigint }>(
      'SELECT purged_at, unblocked_at, set_id FROM purged WHERE sha256 = $1',
      [sha('b')],
    )
  ).rows[0]!;
  expect(r.unblocked_at).toBeNull();
  expect(r.set_id).toBe(100n);
  expect(r.purged_at.getFullYear()).toBeGreaterThan(2020);
});

test('unblock sets unblocked_at, re-plans KNOWN/BLOCKED items and the SENT ones of a purged content', async () => {
  await seed();
  await purgeSet(pool, SET, [sha('a'), sha('b')]);
  await pool.query(`INSERT INTO harvest_sources (id, identity, label) VALUES (1, 'u', 's')`);
  const file = await pool.query<{ id: string }>(
    `SELECT id::text AS id FROM files WHERE sha256 = $1 LIMIT 1`,
    [sha('a')],
  );
  const item = async (
    path: string,
    state: string,
    s: string | null,
    fileId: string | null = null,
    sentAt = new Date('2020-01-01'),
  ) =>
    pool.query(
      `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state,
                                  sha256, file_id, dropzone_path, dropzone_mtime_ns, sent_at,
                                  attempts, last_error)
       VALUES (1, $1, $1, 1, 1, 'photo', $2, $3, $4, $5, $6, $7, 2, 'old')`,
      [
        path,
        state,
        s,
        fileId,
        state === 'SENT' ? `harvest/s/${path}` : null,
        state === 'SENT' ? 5 : null,
        state === 'SENT' ? sentAt : null,
      ],
    );
  await item('k.jpg', 'KNOWN', sha('a'));
  await item('bl.jpg', 'BLOCKED', sha('a'));
  // Linked: its bytes are the pipeline's files.sha256 (no sha stored on the item).
  await item('sent.jpg', 'SENT', null, file.rows[0]!.id);
  await item('sent2.jpg', 'SENT', sha('a'));
  await item('other.jpg', 'KNOWN', sha('e'));
  await item('bsent.jpg', 'SENT', sha('b'));
  // Sent again after the purge (an earlier unblock): its copy is on its way in, keep it.
  await item('resent.jpg', 'SENT', sha('a'), null, new Date(Date.now() + 60_000));
  // sha c was never purged-and-unblocked by this call; 9 is not purged at all.
  await item('c.jpg', 'KNOWN', sha('c'));
  await item('nine.jpg', 'KNOWN', sha('9'));
  expect(await unblock(pool, [sha('a'), sha('9')])).toBe(1);
  const p = await pool.query<{ sha256: string; unblocked_at: Date | null }>(
    'SELECT sha256, unblocked_at FROM purged ORDER BY sha256',
  );
  expect(p.rows.find((r) => r.sha256 === sha('a'))!.unblocked_at).toBeInstanceOf(Date);
  expect(p.rows.find((r) => r.sha256 === sha('b'))!.unblocked_at).toBeNull();
  const items = await pool.query<{
    rel_path: string;
    state: string;
    file_id: string | null;
    dropzone_path: string | null;
    dropzone_mtime_ns: string | null;
    sent_at: Date | null;
    attempts: number;
    last_error: string | null;
  }>(
    `SELECT rel_path, state, file_id::text AS file_id, dropzone_path,
            dropzone_mtime_ns::text AS dropzone_mtime_ns, sent_at, attempts, last_error
       FROM harvest_items ORDER BY rel_path`,
  );
  const by = Object.fromEntries(items.rows.map((r) => [r.rel_path, r]));
  expect(Object.fromEntries(items.rows.map((r) => [r.rel_path, r.state]))).toEqual({
    'bl.jpg': 'PLANNED',
    'bsent.jpg': 'SENT',
    'c.jpg': 'KNOWN',
    'k.jpg': 'PLANNED',
    'nine.jpg': 'KNOWN',
    'other.jpg': 'KNOWN',
    'resent.jpg': 'SENT',
    'sent.jpg': 'PLANNED',
    'sent2.jpg': 'PLANNED',
  });
  // A re-planned SENT item forgets its copy: sendItem must not take the old file as published.
  for (const r of ['sent.jpg', 'sent2.jpg'])
    expect(by[r]).toMatchObject({
      file_id: null,
      dropzone_path: null,
      dropzone_mtime_ns: null,
      sent_at: null,
      attempts: 0,
      last_error: null,
    });
  expect(by['bsent.jpg']!.dropzone_path).not.toBeNull();
  // Already unblocked: nothing changes.
  expect(await unblock(pool, [sha('a')])).toBe(0);
});

test('unblock does not re-plan SENT items once the content is back in the vault', async () => {
  await seed();
  await purgeSet(pool, SET, [sha('a'), sha('b')]);
  await pool.query(`INSERT INTO harvest_sources (id, identity, label) VALUES (1, 'u', 's')`);
  await pool.query(
    `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state,
                                sha256, dropzone_path, dropzone_mtime_ns)
     VALUES (1, 's.jpg', 's.jpg', 1, 1, 'photo', 'SENT', $1, 'harvest/s.jpg', 5)`,
    [sha('a')],
  );
  // Restored by a re-drop between the unblock and a retried unblock request.
  await pool.query(`UPDATE contents SET state = 'INGESTED', purged_at = NULL WHERE sha256 = $1`, [
    sha('a'),
  ]);
  await pool.query('UPDATE purged SET unblocked_at = now() WHERE sha256 = $1', [sha('a')]);
  expect(await unblock(pool, [sha('a')])).toBe(0);
  const r = await pool.query<{ state: string }>('SELECT state FROM harvest_items');
  expect(r.rows[0]!.state).toBe('SENT');
});

const clusterVersion = async () =>
  (await pool.query<{ v: bigint }>('SELECT version AS v FROM cluster_state WHERE id = 1')).rows[0]!
    .v;

test('purging CLUSTERED rows bumps cluster_state.version once; the idempotent repeat does not', async () => {
  await seed();
  const v0 = await clusterVersion();
  expect(await purgeSet(pool, SET, [sha('a'), sha('b')])).not.toBe('stale');
  expect(await clusterVersion()).toBe(v0 + 1n);
  await purgeSet(pool, SET, [sha('a'), sha('b')]);
  expect(await clusterVersion()).toBe(v0 + 1n);
});

test('a stale purge leaves cluster_state.version alone', async () => {
  await seed();
  const v0 = await clusterVersion();
  expect(await purgeSet(pool, SET, [sha('a')])).toBe('stale');
  expect(await clusterVersion()).toBe(v0);
});

test('purgeSet waits for pm.cluster before touching the closure rows', async () => {
  await seed();
  const holder = await pool.connect();
  try {
    await holder.query('BEGIN');
    await holder.query(`SELECT pg_advisory_xact_lock(hashtext('pm.cluster'))`);
    let done = false;
    const p = purgeSet(pool, SET, [sha('a'), sha('b')]).then((r) => {
      done = true;
      return r;
    });
    await new Promise((r) => setTimeout(r, 300));
    expect(done).toBe(false);
    expect((await pool.query("SELECT 1 FROM contents WHERE state = 'PURGED'")).rowCount).toBe(0);
    await holder.query('COMMIT');
    expect(await p).not.toBe('stale');
  } finally {
    holder.release();
  }
});

test('a set that changes while purge waits on pm.cluster is stale; nothing is purged', async () => {
  await seed();
  const holder = await pool.connect();
  try {
    await holder.query('BEGIN');
    await holder.query(`SELECT pg_advisory_xact_lock(hashtext('pm.cluster'))`);
    const p = purgeSet(pool, SET, [sha('a'), sha('b')]);
    await new Promise((r) => setTimeout(r, 300));
    // A cluster step commits under pm.cluster: a new described candidate joins the set.
    await holder.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state, copy_group_id)
       VALUES ($1, 'jpg', 'still', 10, 'CLUSTERED', 2)`,
      [sha('5')],
    );
    await holder.query('INSERT INTO similar_candidates (set_id, sha256) VALUES ($1, $2)', [
      SET,
      sha('5'),
    ]);
    await holder.query(
      `INSERT INTO photo_features (sha256, feature_version, embedding, quality, faces)
       VALUES ($1, 1, '{}', '{}', '[]')`,
      [sha('5')],
    );
    await holder.query('COMMIT');
    expect(await p).toBe('stale');
    expect((await pool.query("SELECT 1 FROM contents WHERE state = 'PURGED'")).rowCount).toBe(0);
    expect((await pool.query('SELECT 1 FROM purged')).rowCount).toBe(0);
  } finally {
    holder.release();
  }
});

// R6: the closure is the copy groups of the shown candidates only; a group of the set the
// Picks screen never showed (an undescribed candidate, or a group linked after staging) is
// neither counted nor purged.
test('an undescribed candidate of the set is not in the closure and survives the purge', async () => {
  await seed();
  await group(4, SET);
  await content(sha('9'), { group: 4, candidateOf: SET, described: false, size: 111 });
  await content(sha('5'), { group: 4, state: 'HASHED', size: 222 });
  const preview = await purgeClosure(pool, SET, [sha('a'), sha('b')]);
  expect(preview).not.toBe('stale');
  if (preview === 'stale') return;
  expect(preview.shas).not.toContain(sha('9'));
  expect(preview).toMatchObject({ shown: 2, copies: 2, live: 1 });
  const s = await purgeSet(pool, SET, [sha('a'), sha('b')]);
  expect(s).not.toBe('stale');
  if (s === 'stale') return;
  expect(s.shas).toEqual(preview.shas);
  const left = await pool.query<{ sha256: string; state: string }>(
    'SELECT sha256, state FROM contents WHERE sha256 = ANY($1::text[]) ORDER BY sha256',
    [[sha('9'), sha('5')]],
  );
  expect(left.rows.map((r) => r.state)).toEqual(['HASHED', 'CLUSTERED']);
  expect((await pool.query('SELECT 1 FROM purged WHERE sha256 = $1', [sha('9')])).rowCount).toBe(0);
  // The repeat (idempotent path) still reports the same closure.
  expect(await purgeSet(pool, SET, [sha('a'), sha('b')])).toEqual(s);
});

test('a copy group linked to the set after staging is not in the closure', async () => {
  await seed();
  // Linked by a cluster step after the last staging build: no similar_candidates row.
  await group(5, SET);
  await content(sha('0'), { group: 5, size: 999 });
  const s = await purgeSet(pool, SET, [sha('a'), sha('b')]);
  expect(s).not.toBe('stale');
  if (s === 'stale') return;
  expect(s.shas).not.toContain(sha('0'));
  expect(s).toMatchObject({ shown: 2, copies: 2, live: 1 });
  const r = await pool.query<{ state: string }>('SELECT state FROM contents WHERE sha256 = $1', [
    sha('0'),
  ]);
  expect(r.rows[0]!.state).toBe('CLUSTERED');
});

test('stale when a shown sha is no longer in a copy group of the set', async () => {
  await seed();
  // A cluster step moved b's group to the other set after staging.
  await pool.query('UPDATE groups SET similar_set_id = $1 WHERE id = 2', [OTHER_SET]);
  expect(await purgeClosure(pool, SET, [sha('a'), sha('b')])).toBe('stale');
  expect(await purgeSet(pool, SET, [sha('a'), sha('b')])).toBe('stale');
  expect((await pool.query("SELECT 1 FROM contents WHERE state = 'PURGED'")).rowCount).toBe(0);
});

test('stale when a shown sha is not HASHED/CLUSTERED', async () => {
  await seed();
  await pool.query(`UPDATE contents SET state = 'QUARANTINED' WHERE sha256 = $1`, [sha('b')]);
  expect(await purgeSet(pool, SET, [sha('a'), sha('b')])).toBe('stale');
  expect((await pool.query("SELECT 1 FROM contents WHERE state = 'PURGED'")).rowCount).toBe(0);
});

test('a shown still whose group was merged into another set group takes the whole root group', async () => {
  await seed();
  // b sits in group 6, which was merged into group 2 (a legacy non-root copy_group_id).
  await group(6, null, 2);
  await pool.query('UPDATE contents SET copy_group_id = 6 WHERE sha256 = $1', [sha('b')]);
  const s = await purgeClosure(pool, SET, [sha('a'), sha('b')]);
  expect(s).not.toBe('stale');
  if (s === 'stale') return;
  expect(s.shas).toEqual([sha('a'), sha('b'), sha('c'), sha('d'), MOTION].sort());
});
