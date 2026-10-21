import { createHash } from 'node:crypto';
import { join } from 'node:path';
import type pg from 'pg';
import { withTx } from '../db.js';
import { sha256File } from '../fsops.js';
import { openReadOnly, readFsOps, sameMtime, sourcePath, type SourcePath } from './paths.js';
import { folderOf, loadScope, type SendScope } from './runs.js';

/** The head hash covers the first 64 KiB; a file no larger has the whole file as its head. */
const HEAD_BYTES = 64 * 1024;

export type KnownOutcome =
  | { state: 'KNOWN'; sha256: string }
  | { state: 'DUP'; sha256: string; duplicateOf: string }
  | { state: 'SEND'; sha256: string | null; headHash: string | null };

export interface KnownItem {
  id: string;
  sourceId: string;
  /** The source file (a Buffer when its on-disk name bytes differ from the NFC rel_path). */
  absPath: SourcePath;
  sizeBytes: bigint;
  kind: 'photo' | 'maybe';
  sha256: string | null;
  headHash: string | null;
  /** NFC + casefold of the item's rel_path: the run folder it is sent under. */
  pathKey: string;
  /** The item's rel_path: its place in the folder's send order. */
  relPath: string;
  /** The run being sent (PLANNED candidates qualify only inside its scope), or null. */
  runId: string | null;
}

export interface KnownOptions {
  /** Test seam: the full-hash reader. */
  sha256File?: (path: SourcePath, onChunk?: () => void) => Promise<string>;
  /** Called after each 1 MiB read of a full hash (sendBatch heartbeats from it). */
  onChunk?: () => void;
  /**
   * A source's current mount, found by its identity (detection, not `last_mount_path`), or
   * null when it is not mounted. Without it no PLANNED candidate is read.
   */
  mountOf?: (sourceId: string) => Promise<string | null>;
  /** The drop-zone root. Without it no unlinked SENT candidate is read. */
  dropzone?: string;
}

interface Candidate {
  id: string;
  source_id: string;
  state: 'SENT' | 'PLANNED';
  kind: 'photo' | 'maybe';
  /** The stored sha256, else (a linked SENT item) the pipeline's `files.sha256`. */
  sha256: string | null;
  head_hash: string | null;
  rel_path: string;
  rel_path_raw: Buffer | null;
  path_key: string;
  mtime_ns: string;
  dropzone_path: string | null;
  dropzone_mtime_ns: string | null;
  linked: boolean;
  /** For a send-only run: the candidate is one of its approved items. */
  in_run: boolean;
}

/** Tie order among candidates: photo before maybe, then item id ascending. */
const order = (kind: string, id: string): [number, bigint] => [
  kind === 'photo' ? 0 : 1,
  BigInt(id),
];
const cmp = (a: [number, bigint], b: [number, bigint]) =>
  a[0] !== b[0] ? a[0] - b[0] : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0;

/**
 * Send order within a run (sendBatch's pageQuery): photo before maybe, then folder position,
 * part (0: spelled exactly under the folder's rel_path, ordered by rel_path bytes; 1: another
 * spelling, ordered by path_key bytes), then id.
 */
type SendOrder = [number, number, number, Buffer, bigint];
function sendOrder(
  scope: SendScope,
  kind: string,
  pos: number,
  rel: string,
  key: string,
  id: string,
): SendOrder {
  const folderRel = scope.folders[pos]!.relPath;
  const part = folderRel === '' || rel.startsWith(`${folderRel}/`) ? 0 : 1;
  return [kind === 'photo' ? 0 : 1, pos, part, Buffer.from(part === 0 ? rel : key), BigInt(id)];
}
const before = (a: SendOrder, b: SendOrder): boolean => {
  for (let i = 0; i < 3; i++) if (a[i] !== b[i]) return (a[i] as number) < (b[i] as number);
  const k = Buffer.compare(a[3], b[3]);
  if (k !== 0) return k < 0;
  return a[4] < b[4];
};

/** sha256 of the first 64 KiB (opened read-only). */
async function headOf(path: SourcePath, size: bigint): Promise<string> {
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
  const hashFile = opts.sha256File ?? sha256File;
  const fullHash = (p: SourcePath) => hashFile(p, opts.onChunk);
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

  /**
   * Whether candidate `c` holds this item's bytes. Compares stored full hashes when both
   * exist; otherwise heads first, and full hashes only where heads collide. Errors reading
   * this item propagate; an unreadable candidate just does not match (the item is then sent),
   * and nothing is stored for it.
   */
  const sameBytes = async (c: Candidate): Promise<boolean> => {
    let theirSha = c.sha256;
    let theirHead = c.head_hash;
    if (cur.sha !== null && theirSha !== null) return cur.sha === theirSha;

    if (small) theirHead ??= theirSha;
    // Their full hash but no head (a linked SENT item): our full hash decides, no read of theirs.
    if (theirHead === null && theirSha !== null) return (await ensureSha()) === theirSha;
    const myHead = await ensureHead();
    let resolved: SourcePath | null | undefined;
    const pathOnce = async () =>
      resolved === undefined ? (resolved = await candidatePath(c, item.sizeBytes, opts)) : resolved;
    if (theirHead === null) {
      const path = await pathOnce();
      if (path === null) return false;
      try {
        if (small) {
          theirSha = await fullHash(path);
          theirHead = theirSha;
          await store(pool, c.id, theirSha, theirHead);
        } else {
          theirHead = await headOf(path, item.sizeBytes);
          await store(pool, c.id, null, theirHead);
        }
      } catch {
        return false;
      }
    }
    if (myHead !== theirHead) return false;
    const mine = await ensureSha();
    if (theirSha === null) {
      const path = await pathOnce();
      if (path === null) return false;
      try {
        theirSha = await fullHash(path);
        await store(pool, c.id, theirSha, null);
      } catch {
        return false;
      }
    }
    return mine === theirSha;
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
      `SELECT i.id::text AS id, i.source_id::text AS source_id, i.state, i.kind,
              COALESCE(i.sha256, f.sha256) AS sha256, i.head_hash, i.rel_path, i.rel_path_raw,
              i.path_key, i.mtime_ns::text AS mtime_ns, i.dropzone_path,
              i.dropzone_mtime_ns::text AS dropzone_mtime_ns, i.file_id IS NOT NULL AS linked,
              EXISTS (SELECT 1 FROM harvest_run_items ri
                      WHERE ri.run_id = $3::bigint AND ri.item_id = i.id) AS in_run
       FROM harvest_items i LEFT JOIN files f ON f.id = i.file_id
       WHERE i.size_bytes = $1 AND i.id <> $2 AND i.state IN ('SENT','PLANNED')`,
      [item.sizeBytes.toString(), item.id, item.runId],
    );
    // A PLANNED candidate wins only if this run sends it, and sends it before this item: it is
    // in the run's scope (a send-only run: one of its items), and earlier in its send order.
    let scope: SendScope | null = null;
    if (item.runId !== null && rows.some((c) => c.state === 'PLANNED')) {
      scope = await loadScope(pool, item.runId);
    }
    const myPos = scope ? folderOf(scope, item.sourceId, item.pathKey) : null;
    const qualifies = (c: Candidate): boolean => {
      if (c.state === 'SENT') return true;
      if (scope === null || myPos === null) return false;
      if (scope.sendOnly && !c.in_run) return false;
      const pos = folderOf(scope, c.source_id, c.path_key);
      if (pos === null) return false;
      return before(
        sendOrder(scope, c.kind, pos, c.rel_path, c.path_key, c.id),
        sendOrder(scope, item.kind, myPos, item.relPath, item.pathKey, item.id),
      );
    };
    const candidates = rows
      .filter(qualifies)
      .sort((a, b) => cmp(order(a.kind, a.id), order(b.kind, b.id)));

    for (const c of candidates) {
      if (await sameBytes(c)) return { state: 'DUP', sha256: cur.sha!, duplicateOf: c.id };
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
 * The file that holds a candidate's bytes, or null when none may be read (spec §7.4, I2):
 * - SENT: never its source (it may have changed since the copy). An unlinked one is read through
 *   its drop-zone copy while that is still the file we wrote (size and `dropzone_mtime_ns`); a
 *   linked one belongs to the pipeline now (its `files.sha256` stands in).
 * - PLANNED: its source on the mount found by the source's identity, only while it is still the
 *   scanned file (a regular file, same size, same mtime up to the exFAT/SMB tolerance).
 */
async function candidatePath(
  c: Candidate,
  size: bigint,
  opts: KnownOptions,
): Promise<SourcePath | null> {
  let path: SourcePath;
  if (c.state === 'SENT') {
    if (c.linked || !opts.dropzone || c.dropzone_path === null || c.dropzone_mtime_ns === null) {
      return null;
    }
    path = join(opts.dropzone, c.dropzone_path);
  } else {
    const mount = opts.mountOf ? await opts.mountOf(c.source_id).catch(() => null) : null;
    if (mount === null) return null;
    path = sourcePath(mount, c.rel_path, c.rel_path_raw);
  }
  const st = await readFsOps.stat(path).catch(() => null);
  if (!st?.isFile() || st.size !== size) return null;
  const same =
    c.state === 'SENT'
      ? st.mtimeNs === BigInt(c.dropzone_mtime_ns!)
      : sameMtime(st.mtimeNs, BigInt(c.mtime_ns));
  return same ? path : null;
}

/**
 * Lost winner (spec §7.4): the item `lostItemId` ended FAILED or VANISHED. Of the DUP items
 * pointing at it, the earliest (photo first, then id) becomes PLANNED, and the others point at
 * it. One transaction; returns the number of items promoted or repointed.
 */
export async function promoteDuplicates(pool: pg.Pool, lostItemId: string): Promise<number> {
  return withTx(pool, async (client) => {
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
    return rows.length;
  });
}
