import { createHash } from 'node:crypto';
import { join } from 'node:path';
import type pg from 'pg';
import { sha256File } from '../fsops.js';
import { openReadOnly } from './paths.js';

/** The head hash covers the first 64 KiB; a file no larger has the whole file as its head. */
export const HEAD_BYTES = 64 * 1024;

export type KnownOutcome =
  | { state: 'KNOWN'; sha256: string }
  | { state: 'DUP'; sha256: string; duplicateOf: string }
  | { state: 'SEND'; sha256: string | null; headHash: string | null };

export interface KnownItem {
  id: string;
  sourceId: string;
  absPath: string;
  sizeBytes: bigint;
  kind: 'photo' | 'maybe';
  sha256: string | null;
  headHash: string | null;
}

/** Test seam: the full-hash reader. */
export interface KnownOptions {
  sha256File?: (path: string) => Promise<string>;
}

interface Candidate {
  id: string;
  state: 'SENT' | 'PLANNED';
  kind: 'photo' | 'maybe';
  sha256: string | null;
  head_hash: string | null;
  rel_path: string;
  mount: string | null;
}

/** Send order for ties: photo before maybe, then item id ascending. */
const order = (kind: string, id: string): [number, bigint] => [
  kind === 'photo' ? 0 : 1,
  BigInt(id),
];
const cmp = (a: [number, bigint], b: [number, bigint]) =>
  a[0] !== b[0] ? a[0] - b[0] : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0;

/** sha256 of the first 64 KiB (opened read-only). */
async function headOf(path: string, size: bigint): Promise<string> {
  const fh = await openReadOnly(path);
  try {
    const want = Number(size < BigInt(HEAD_BYTES) ? size : BigInt(HEAD_BYTES));
    const buf = Buffer.alloc(want);
    let got = 0;
    while (got < want) {
      const { bytesRead } = await fh.read(buf, got, want - got, got);
      if (bytesRead === 0) break;
      got += bytesRead;
    }
    return createHash('sha256').update(buf.subarray(0, got)).digest('hex');
  } finally {
    await fh.close();
  }
}

async function store(
  pool: pg.Pool,
  id: string,
  sha256: string | null,
  headHash: string | null,
): Promise<void> {
  await pool.query(
    `UPDATE harvest_items SET sha256 = COALESCE($2, sha256), head_hash = COALESCE($3, head_hash)
     WHERE id = $1`,
    [id, sha256, headHash],
  );
}

/**
 * Pre-send dedupe (spec §7.4). Hashes a file only when its size could match, stores every
 * hash it computes (even for a SEND), and never changes the item's state (sendBatch does).
 * A read error on this item means SEND, with the error recorded. Sources are only read.
 */
export async function checkKnown(
  pool: pg.Pool,
  item: KnownItem,
  opts: KnownOptions = {},
): Promise<KnownOutcome> {
  const fullHash = opts.sha256File ?? sha256File;
  const small = item.sizeBytes <= BigInt(HEAD_BYTES);
  const cur: { sha: string | null; head: string | null } = {
    sha: item.sha256,
    head: item.headHash,
  };
  if (small) {
    cur.sha ??= cur.head;
    cur.head ??= cur.sha;
  }

  /** Full hash of this item, stored at once. */
  const ensureSha = async (): Promise<string> => {
    if (cur.sha === null) {
      const h = await fullHash(item.absPath);
      cur.sha = h;
      if (small) cur.head = h;
      await store(pool, item.id, h, small ? h : null);
    }
    return cur.sha;
  };
  const ensureHead = async (): Promise<string> => {
    if (cur.head === null) {
      if (small) return ensureSha();
      cur.head = await headOf(item.absPath, item.sizeBytes);
      await store(pool, item.id, null, cur.head);
    }
    return cur.head;
  };

  try {
    // 1. A vault content with this size and sha.
    const sized = await pool.query('SELECT 1 FROM contents WHERE size_bytes = $1 LIMIT 1', [
      item.sizeBytes.toString(),
    ]);
    if (sized.rowCount) {
      const s = await ensureSha();
      const hit = await pool.query('SELECT 1 FROM contents WHERE sha256 = $1 AND size_bytes = $2', [
        s,
        item.sizeBytes.toString(),
      ]);
      if (hit.rowCount) return { state: 'KNOWN', sha256: s };
    }

    // 2. Another ledger item with this size. DUP rows are left out: each is subsumed by its
    // target, which has the same bytes and is itself a candidate when it qualifies.
    const { rows } = await pool.query<Candidate>(
      `SELECT i.id::text AS id, i.state, i.kind, i.sha256, i.head_hash, i.rel_path,
              s.last_mount_path AS mount
       FROM harvest_items i JOIN harvest_sources s ON s.id = i.source_id
       WHERE i.size_bytes = $1 AND i.id <> $2 AND i.state IN ('SENT','PLANNED')`,
      [item.sizeBytes.toString(), item.id],
    );
    const mine = order(item.kind, item.id);
    const candidates = rows
      .filter((c) => c.state !== 'PLANNED' || cmp(order(c.kind, c.id), mine) < 0)
      .sort((a, b) => cmp(order(a.kind, a.id), order(b.kind, b.id)));

    for (const c of candidates) {
      if (
        await sameBytes(pool, c, item.sizeBytes, { cur, ensureHead, ensureSha, theirs: fullHash })
      ) {
        const s = cur.sha!;
        return { state: 'DUP', sha256: s, duplicateOf: c.id };
      }
    }
  } catch (e) {
    await pool.query('UPDATE harvest_items SET last_error = $2 WHERE id = $1', [
      item.id,
      (e as Error).message,
    ]);
  }
  // 3. Otherwise copy it.
  return { state: 'SEND', sha256: cur.sha, headHash: cur.head };
}

/**
 * Whether candidate `c` holds this item's bytes. Compares stored full hashes when both
 * exist; otherwise heads first, and full hashes only where heads collide. Errors reading
 * this item propagate; an unreadable candidate just does not match (the item is then sent).
 */
async function sameBytes(
  pool: pg.Pool,
  c: Candidate,
  size: bigint,
  me: {
    cur: { sha: string | null; head: string | null };
    ensureHead: () => Promise<string>;
    ensureSha: () => Promise<string>;
    theirs: (path: string) => Promise<string>;
  },
): Promise<boolean> {
  const small = size <= BigInt(HEAD_BYTES);
  let theirSha = c.sha256;
  let theirHead = c.head_hash;
  const path = c.mount === null ? null : join(c.mount, c.rel_path);
  if (me.cur.sha !== null && theirSha !== null) return me.cur.sha === theirSha;

  if (small) theirHead ??= theirSha;
  const myHead = await me.ensureHead();
  if (theirHead === null) {
    if (path === null) return false;
    try {
      if (small) {
        theirSha = await me.theirs(path);
        theirHead = theirSha;
        await store(pool, c.id, theirSha, theirHead);
      } else {
        theirHead = await headOf(path, size);
        await store(pool, c.id, null, theirHead);
      }
    } catch {
      return false;
    }
  }
  if (myHead !== theirHead) return false;
  const mine = await me.ensureSha();
  if (theirSha === null) {
    if (path === null) return false;
    try {
      theirSha = await me.theirs(path);
      await store(pool, c.id, theirSha, null);
    } catch {
      return false;
    }
  }
  return mine === theirSha;
}

/**
 * Lost winner (spec §7.4): the item `lostItemId` ended FAILED or VANISHED. Of the DUP items
 * pointing at it, the earliest (photo first, then id) becomes PLANNED, and the others point at
 * it. One transaction; returns the number of items promoted or repointed.
 */
export async function promoteDuplicates(pool: pg.Pool, lostItemId: string): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ id: string }>(
      `SELECT id::text AS id FROM harvest_items
       WHERE duplicate_of = $1 AND state = 'DUP'
       ORDER BY (kind = 'maybe'), id FOR UPDATE`,
      [lostItemId],
    );
    if (rows.length > 0) {
      const winner = rows[0]!.id;
      await client.query(
        `UPDATE harvest_items SET state = 'PLANNED', duplicate_of = NULL
         WHERE id = $1 AND state = 'DUP'`,
        [winner],
      );
      if (rows.length > 1) {
        await client.query(
          `UPDATE harvest_items SET duplicate_of = $1
           WHERE id = ANY($2::bigint[]) AND state = 'DUP'`,
          [winner, rows.slice(1).map((r) => r.id)],
        );
      }
    }
    await client.query('COMMIT');
    return rows.length;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}
