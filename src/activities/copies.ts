// Removal of confirmed byte-identical duplicates (spec §6.5). A COPY file's bytes sit twice on
// disk: in its vault file and in `vault/.copies/<id>.<ext>`. The `.copies` file is unlinked only
// after a fresh re-hash of BOTH files equals `files.sha256`; anything else deletes nothing.
import { lstat, unlink } from 'node:fs/promises';
import { heartbeat, log } from '@temporalio/activity';
import { fsyncDir, sha256File, unlessEnoent } from '../fsops.js';
import type { ReclaimOutcome, SweepResult } from '../types.js';
import { removePurged, removeRetired, type IdenticalCtx, type RemoveOutcome } from './identical.js';
import type { Deps } from './index.js';
import { ChangedWhileHashingError } from './ingest.js';

interface CopyRow {
  id: bigint;
  state: string;
  sha256: string | null;
  ext: string | null;
  size_bytes: bigint;
  copy_removed_at: Date | null;
  copy_mismatch: boolean;
}

const ROW_COLUMNS = 'id, state, sha256, ext, size_bytes, copy_removed_at, copy_mismatch';
const lstatOrNull = (path: string) => unlessEnoent(lstat(path, { bigint: true }));

type Stats = NonNullable<Awaited<ReturnType<typeof lstatOrNull>>>;
/**
 * Unchanged since `a` was taken: same size and mtime. Not the inode number: Docker Desktop's
 * virtiofs renumbers inodes when its cache evicts them (see `sameFile`), and a large file's hash
 * can take long enough for that; the hashes, not the inode, prove the bytes.
 */
const same = (a: Stats, b: Stats): boolean => a.size === b.size && a.mtimeNs === b.mtimeNs;

/** Adds one removal outcome to a sweep's counts (`examined` included). */
export function tally(out: SweepResult, o: RemoveOutcome): void {
  out.examined++;
  if (o === 'REMOVED' || o === 'MISSING') out.removed++;
  else if (o === 'MISMATCH') out.mismatched++;
  else if (o === 'DEFERRED') out.deferred++;
}

/**
 * The sweep's passes over contents with a vault file still to remove: `sweepRetired` (identical
 * duplicates) and `sweepPurged` (purged contents), each with its cursor in `copy_sweep_state`.
 */
const PENDING_PASSES = [
  {
    state: 'DUPLICATE',
    removedAt: 'duplicate_removed_at',
    mismatch: 'duplicate_mismatch',
    cursor: 'retired_after',
    remove: removeRetired,
  },
  {
    state: 'PURGED',
    removedAt: 'purge_removed_at',
    mismatch: 'purge_mismatch',
    cursor: 'purged_after',
    remove: removePurged,
  },
] as const;

export function makeCopies(deps: Deps) {
  const { pool, layout } = deps;
  const beat = () => heartbeat();

  /** Marks the row removed (`bytes` reclaimed); guarded, so a concurrent mark is a no-op. */
  async function markRemoved(id: bigint, bytes: bigint): Promise<ReclaimOutcome> {
    const r = await pool.query(
      `UPDATE files SET copy_removed_at = clock_timestamp(), copy_reclaimed_bytes = $2
        WHERE id = $1 AND state = 'COPY' AND copy_removed_at IS NULL AND NOT copy_mismatch`,
      [id, bytes],
    );
    return r.rowCount === 1 ? 'REMOVED' : 'ALREADY_REMOVED';
  }

  /** Flags the row (never retried); both files are left exactly as they are. */
  async function flag(id: bigint, error: string): Promise<ReclaimOutcome> {
    await pool.query(
      `UPDATE files SET copy_mismatch = true, last_error = $2
        WHERE id = $1 AND state = 'COPY' AND copy_removed_at IS NULL`,
      [id, `copy_mismatch: ${error}`],
    );
    log.warn('duplicate not removed: mismatch flagged', { fileId: String(id), error });
    return 'MISMATCH';
  }

  /** Throws ChangedWhileHashingError unless `path` is still `before` (same size and mtime). */
  async function assertUnchanged(path: string, before: Stats): Promise<void> {
    const now = await lstatOrNull(path);
    if (!now || !same(before, now))
      throw new ChangedWhileHashingError(`${layout.rel(path)} changed while hashing`);
  }

  /** Hashes `path` and checks it did not change meanwhile. */
  async function freshHash(path: string, before: Stats): Promise<string> {
    const h = await sha256File(path, beat);
    await assertUnchanged(path, before);
    return h;
  }

  async function reclaimRow(row: CopyRow): Promise<ReclaimOutcome> {
    if (row.state !== 'COPY' || row.sha256 === null || row.ext === null) return 'NOT_COPY';
    if (row.copy_removed_at !== null) return 'ALREADY_REMOVED';
    if (row.copy_mismatch) return 'MISMATCH';
    const { id, sha256: sha } = row;
    const copy = layout.copiesFile(String(id), row.ext);
    const vault = layout.vaultFile(sha, row.ext);

    // A retired content whose vault file was removed (and not flagged) has no vault file by
    // design: the .copies file alone is checked — but only while its keeper's vault file is
    // there, so the bytes stay somewhere. Any other retired content (removal pending, flagged,
    // keeper gone) takes the normal path: against its own vault file, flagged when missing.
    const k = (
      await pool.query<{ sha256: string; ext: string }>(
        `SELECT k.sha256, k.ext FROM contents d JOIN contents k ON k.sha256 = d.duplicate_of
          WHERE d.sha256 = $1 AND d.state = 'DUPLICATE' AND d.duplicate_removed_at IS NOT NULL
            AND NOT d.duplicate_mismatch`,
        [sha],
      )
    ).rows[0];
    const kept = k ? await lstatOrNull(layout.vaultFile(k.sha256, k.ext)) : null;
    // A purged content whose vault file was removed (and not flagged): its bytes are meant to be
    // gone, so the .copies file alone is checked and deleted, never flagged for the missing
    // vault file. Pending or flagged purged contents take the normal path.
    const purgedGone =
      (
        await pool.query(
          `SELECT 1 FROM contents WHERE sha256 = $1 AND state = 'PURGED'
              AND purge_removed_at IS NOT NULL AND NOT purge_mismatch`,
          [sha],
        )
      ).rowCount === 1;
    const vaultless = kept?.isFile() === true || purgedGone;
    const c = await lstatOrNull(copy);
    const v = vaultless ? null : await lstatOrNull(vault);
    if (!c) {
      if (vaultless) return markRemoved(id, row.size_bytes);
      // COPY is committed only after the rename into .copies is durable, and nothing but this
      // step removes it: a crash came between our unlink and our mark. Mark it once the vault
      // still holds the bytes; otherwise the bytes are nowhere — an anomaly, flagged.
      if (!v || !v.isFile())
        return flag(id, `.copies file and vault file ${layout.rel(vault)} both missing`);
      const hv = await sha256File(vault, beat);
      if (hv !== sha) return flag(id, `.copies file missing; vault sha ${hv}, expected ${sha}`);
      return markRemoved(id, row.size_bytes);
    }
    if (!c.isFile()) return flag(id, `${layout.rel(copy)} is not a regular file`);
    // Another name holds these bytes too (an older staging generation's hardlink, or a link the
    // user made): unlinking would free nothing, and the other name would lose its twin. Later.
    if (c.nlink > 1n) return 'DEFERRED';
    if (!vaultless && (!v || !v.isFile()))
      return flag(id, `vault file ${layout.rel(vault)} missing`);

    const [hc, hv] = await Promise.all([
      freshHash(copy, c),
      v ? freshHash(vault, v) : Promise.resolve(sha),
    ]);
    await deps.checkpoint?.('reclaim_hashed');
    if (hc !== sha || hv !== sha)
      return flag(id, `.copies sha ${hc}, vault sha ${hv}, expected ${sha}`);
    // Both still the files that were hashed, right before the unlink.
    await assertUnchanged(copy, c);
    if (v) await assertUnchanged(vault, v);
    await unlessEnoent(unlink(copy)); // ENOENT: a concurrent reclaim of this row got there first
    await fsyncDir(layout.vaultCopies); // the unlink is durable before the row says so
    await deps.checkpoint?.('reclaim_unlinked');
    return markRemoved(id, c.size);
  }

  /** Queue `io`, heartbeating. Right after a COPY outcome (FileWorkflow). Idempotent. */
  async function reclaimCopy(fileId: string): Promise<ReclaimOutcome> {
    const r = await pool.query<CopyRow>(`SELECT ${ROW_COLUMNS} FROM files WHERE id = $1`, [fileId]);
    const row = r.rows[0];
    return row ? reclaimRow(row) : 'NOT_COPY';
  }

  const ctx: IdenticalCtx = { ...deps, beat, log };

  /**
   * One pass over contents whose vault file is not yet dealt with (removal DEFERRED while a
   * staging generation linked it, or a crash's leftover): up to `limit` of them after the
   * persisted sha256 cursor, each removed with `remove`. True when the pass reached the end
   * (cursor reset).
   */
  async function sweepPending(
    p: (typeof PENDING_PASSES)[number],
    limit: number,
    out: SweepResult,
    after: string,
  ): Promise<boolean> {
    const rows = (
      await pool.query<{ sha256: string }>(
        `SELECT sha256 FROM contents
          WHERE state = '${p.state}' AND ${p.removedAt} IS NULL AND NOT ${p.mismatch}
            AND sha256 > $1
          ORDER BY sha256 LIMIT $2`,
        [after, limit],
      )
    ).rows;
    for (const { sha256 } of rows) {
      tally(out, await p.remove(ctx, sha256));
      heartbeat(out);
    }
    const wrapped = rows.length < limit;
    await pool.query(`UPDATE copy_sweep_state SET ${p.cursor} = $1 WHERE id = 1`, [
      wrapped ? '' : rows[rows.length - 1]!.sha256,
    ]);
    return wrapped;
  }

  /**
   * Queue `io`, heartbeating (`CopySweepWorkflow`). Up to `limit` COPY rows neither removed nor
   * flagged, after the persisted cursor in id order: crash leftovers, deferred copies and the
   * one-off pass over data ingested before removal existed. Then up to `limit` pending retired
   * identical duplicates (`sweepRetired`) and up to `limit` pending purged contents
   * (`sweepPurged`), each on its own cursor. `wrapped`: every pass reached the end and its
   * cursor went back to the start.
   */
  async function sweepCopies(limit: number): Promise<SweepResult> {
    const state = (
      await pool.query<{ after_id: bigint; retired_after: string; purged_after: string }>(
        'SELECT after_id, retired_after, purged_after FROM copy_sweep_state WHERE id = 1',
      )
    ).rows[0]!;
    const after = state.after_id;
    const rows = (
      await pool.query<CopyRow>(
        `SELECT ${ROW_COLUMNS} FROM files
          WHERE state = 'COPY' AND copy_removed_at IS NULL AND NOT copy_mismatch AND id > $1
          ORDER BY id LIMIT $2`,
        [after, limit],
      )
    ).rows;
    const out: SweepResult = {
      examined: 0,
      removed: 0,
      mismatched: 0,
      deferred: 0,
      wrapped: false,
    };
    for (const row of rows) {
      const o = await reclaimRow(row);
      out.examined++;
      if (o === 'REMOVED') out.removed++;
      else if (o === 'MISMATCH') out.mismatched++;
      else if (o === 'DEFERRED') out.deferred++;
      heartbeat(out);
    }
    const copiesWrapped = rows.length < limit;
    const next = copiesWrapped ? 0n : rows[rows.length - 1]!.id;
    await pool.query('UPDATE copy_sweep_state SET after_id = $1 WHERE id = 1', [next]);
    let pendingWrapped = true;
    for (const p of PENDING_PASSES)
      if (!(await sweepPending(p, limit, out, state[p.cursor]))) pendingWrapped = false;
    out.wrapped = copiesWrapped && pendingWrapped;
    if (out.examined > 0) log.info('duplicate sweep', { ...out });
    return out;
  }

  return { reclaimCopy, sweepCopies };
}
