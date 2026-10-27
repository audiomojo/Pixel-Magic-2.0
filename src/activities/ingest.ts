import { chmod, link, mkdir, rename, stat, unlink } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import { Context, heartbeat } from '@temporalio/activity';
import type pg from 'pg';
import { withTx } from '../db.js';
import {
  errCode,
  fsyncDir,
  isEnoent,
  linkIntoVault,
  sameFile,
  sha256File,
  unlessEnoent,
} from '../fsops.js';
import type { Metadata } from '../metadata.js';
import type { FileRef, IngestResult } from '../types.js';
import type { Deps } from './index.js';

/** Another attempt moved the row on (a guarded update matched nothing). Retryable. */
export class StaleAttemptError extends Error {
  override name = 'StaleAttemptError';
}

/** A hashed file's bytes are in none of the places they must be. Retryable, never hidden. */
export class MissingBytesError extends Error {
  override name = 'MissingBytesError';
}

/** A file changed while it was hashed (a writer still has it open). Retryable. */
export class ChangedWhileHashingError extends Error {
  override name = 'ChangedWhileHashingError';
}

type Kind = IngestResult['kind'];
interface Row {
  id: string;
  sha256: string | null;
  ext: string | null;
}

const statOrNull = (path: string) => unlessEnoent(stat(path));

/**
 * The detected extension, else `bin`. Never the dropped name's: identical bytes must map to one
 * vault path whatever they were called, or byte dedup breaks.
 */
function extOf(meta: Metadata): string {
  return /^[a-z0-9]+$/.test(meta.ext) ? meta.ext : 'bin';
}

export function makeIngestFile(deps: Deps) {
  const { pool, layout, metadata } = deps;
  /** Where a hashed row's bytes are kept: its .copies file for a COPY, else the vault file. */
  const keptPath = (id: string, state: string, sha: string, ext: string) =>
    state === 'COPY' ? layout.copiesFile(id, ext) : layout.vaultFile(sha, ext);

  /** `UPDATE files … WHERE id AND state IN (allowed)`; throws StaleAttemptError if no row moved. */
  async function guarded(
    db: pg.Pool | pg.PoolClient,
    id: string,
    set: string,
    allowed: string[],
    params: unknown[] = [],
    where = 'true',
  ): Promise<void> {
    const r = await db.query(
      `UPDATE files SET ${set} WHERE id = $1 AND state = ANY($2::text[]) AND ${where}`,
      [id, allowed, ...params],
    );
    if (r.rowCount !== 1) throw new StaleAttemptError(`file ${id}: not in ${allowed.join('|')}`);
  }

  /** Tail of steps 6–7 (also re-run on replay): drop claimed/<id> only if it is the file held at `keep` (`sameFile`). */
  async function finish(id: string, keep: string, sha: string): Promise<void> {
    const k = await statOrNull(keep);
    if (!k) return;
    if (await sameFile(layout.claimedFile(id), keep, sha, () => heartbeat()))
      await unlink(layout.claimedFile(id));
    if ((k.mode & 0o777) !== 0o444) await chmod(keep, 0o444);
  }

  /** An unhashed row ends QUARANTINED with `reason`: nothing of it was ever vaulted. */
  async function quarantineUnhashed(
    id: string,
    reason: string,
    allowed: string[],
  ): Promise<IngestResult> {
    await guarded(
      pool,
      id,
      `state = 'QUARANTINED', last_error = $3`,
      allowed,
      [reason],
      'sha256 IS NULL',
    );
    return { fileId: id, sha256: null, ext: null, state: 'QUARANTINED', kind: null };
  }

  async function commitCopy(row: Row, sha: string, ext: string): Promise<IngestResult> {
    await guarded(pool, row.id, `state = 'COPY'`, ['CLAIMED']);
    await finish(row.id, layout.copiesFile(row.id, ext), sha);
    return { fileId: row.id, sha256: sha, ext, state: 'COPY', kind: null };
  }

  /** Moves the claimed file into .copies (durably), then commits COPY. */
  async function parkAsCopy(
    row: Row,
    claimed: string,
    sha: string,
    ext: string,
  ): Promise<IngestResult> {
    await rename(claimed, layout.copiesFile(row.id, ext));
    await fsyncDir(layout.vaultCopies); // the rename is durable before COPY is committed
    await deps.checkpoint?.('copies_rename');
    return commitCopy(row, sha, ext);
  }

  /** Step 6 for the vault owner: contents (idempotent) and the guarded INGESTED, in one transaction. */
  async function commitOwner(
    row: Row,
    sha: string,
    ext: string,
    meta: Metadata,
    size: bigint,
  ): Promise<IngestResult> {
    await withTx(pool, async (c) => {
      await c.query(
        `INSERT INTO contents (sha256, ext, kind, mime, size_bytes, captured_at, subsec, camera,
                               burst_uuid, live_content_id, duration_s, state)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'INGESTED')
         ON CONFLICT (sha256) DO NOTHING`,
        [
          sha,
          ext,
          meta.kind,
          meta.mime || null,
          size,
          meta.capturedAt,
          meta.subsec,
          meta.camera,
          meta.burstUuid,
          meta.liveContentId,
          meta.durationS,
        ],
      );
      await guarded(c, row.id, `state = 'INGESTED'`, ['CLAIMED']);
    });
    await finish(row.id, layout.vaultFile(sha, ext), sha);
    return { fileId: row.id, sha256: sha, ext, state: 'INGESTED', kind: meta.kind };
  }

  /** R1: a run whose row is already final returns the stored outcome (after re-running the tail of steps 6–7). */
  async function storedOutcome(runId: string): Promise<IngestResult | null> {
    const r = await pool.query<{
      id: bigint;
      sha256: string | null;
      ext: string | null;
      state: string;
      kind: Kind;
    }>(
      `SELECT f.id, f.sha256, f.ext, f.state, c.kind
         FROM files f LEFT JOIN contents c ON c.sha256 = f.sha256
        WHERE f.workflow_run_id = $1`,
      [runId],
    );
    const row = r.rows[0];
    if (!row || (row.state !== 'INGESTED' && row.state !== 'COPY' && row.state !== 'QUARANTINED'))
      return null;
    const id = String(row.id);
    if (row.sha256 && row.ext)
      await finish(id, keptPath(id, row.state, row.sha256, row.ext), row.sha256);
    return {
      fileId: id,
      sha256: row.sha256,
      ext: row.ext,
      state: row.state,
      kind: row.state === 'INGESTED' ? row.kind : null,
    };
  }

  async function upsert(ref: FileRef, runId: string): Promise<Row> {
    const r = await pool.query<{ id: bigint; sha256: string | null; ext: string | null }>(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state, workflow_run_id)
       VALUES ($1, $2, $3, $4, 'DISCOVERED', $5)
       ON CONFLICT (rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED')
       DO UPDATE SET workflow_run_id = EXCLUDED.workflow_run_id, attempts = files.attempts + 1
       RETURNING id, sha256, ext`,
      [ref.relPath, basename(ref.relPath), BigInt(ref.size), BigInt(ref.mtimeNs), runId],
    );
    const row = r.rows[0]!;
    return { id: String(row.id), sha256: row.sha256, ext: row.ext };
  }

  /**
   * This attempt's own rename moved a file into claimed/<id>, but the row has moved on without it
   * (a newer attempt committed). That file is a later drop at the same path: hardlink it back into
   * the drop zone for the next scan, and only then unlink claimed/<id>. If claimed/<id> is the
   * inode the row committed, it belongs to the committing attempt, which unlinks it itself.
   */
  async function putBack(ref: FileRef, id: string): Promise<void> {
    const claimed = layout.claimedFile(id);
    const c = await statOrNull(claimed);
    if (!c) return;
    const r = await pool.query<{ state: string; sha256: string | null; ext: string | null }>(
      'SELECT state, sha256, ext FROM files WHERE id = $1',
      [id],
    );
    const row = r.rows[0];
    if (row?.sha256 && row.ext) {
      const keep = keptPath(id, row.state, row.sha256, row.ext);
      if (await sameFile(claimed, keep, row.sha256, () => heartbeat())) return;
    }
    const dest = join(layout.dropzone, ref.relPath);
    await mkdir(dirname(dest), { recursive: true });
    try {
      await link(claimed, dest);
    } catch (e) {
      if (errCode(e) === 'EEXIST')
        throw new Error(
          `${claimed} holds a later drop of ${ref.relPath} but that path is occupied; left in place`,
          { cause: e },
        );
      throw e;
    }
    await fsyncDir(dirname(dest)); // the put-back link is durable before claimed/<id> goes
    await unlink(claimed);
  }

  /**
   * Neither the drop-zone file nor claimed/<id> exists. Without a hash nothing was ever claimed:
   * the file vanished. With a hash it is never "vanished": its bytes are in .copies (COPY), in the
   * vault (commit as owner), or the attempt fails retryably.
   */
  async function recoverUnclaimed(ref: FileRef, row: Row): Promise<IngestResult> {
    if (!row.sha256) {
      // Only an unhashed, unclaimed row can vanish: CLAIMED always carries a hash.
      return quarantineUnhashed(row.id, 'vanished', ['DISCOVERED']);
    }
    if (row.ext) {
      const vault = layout.vaultFile(row.sha256, row.ext);
      const v = await statOrNull(vault);
      if (v)
        return commitOwner(row, row.sha256, row.ext, await metadata.read(vault), BigInt(v.size));
    }
    throw new MissingBytesError(
      `file ${row.id} (${ref.relPath}, sha ${row.sha256}): bytes in neither drop zone, claimed nor vault`,
    );
  }

  return async function ingestFile(ref: FileRef): Promise<IngestResult> {
    const runId = Context.current().info.workflowExecution?.runId;
    if (!runId) throw new Error('ingestFile needs a workflow run id (its idempotency key)');
    const done = await storedOutcome(runId);
    if (done) return done;

    // Step 1: the row for this drop, keyed among non-final rows.
    const row = await upsert(ref, runId);
    const claimed = layout.claimedFile(row.id);
    await deps.checkpoint?.('upsert');

    // Step 2: claim. An existing claimed file is never overwritten (a re-drop at the same path
    // waits for the next scan), and a copy already parked in .copies is committed as such.
    let claimedHere = false;
    if (!(await statOrNull(claimed))) {
      if (row.sha256 && row.ext && (await statOrNull(layout.copiesFile(row.id, row.ext)))) {
        await fsyncDir(layout.vaultCopies); // durable before COPY is committed
        return commitCopy(row, row.sha256, row.ext);
      }
      try {
        await rename(join(layout.dropzone, ref.relPath), claimed);
        claimedHere = true;
      } catch (e) {
        if (!isEnoent(e)) throw e;
        if (!(await statOrNull(claimed))) return recoverUnclaimed(ref, row);
      }
    }
    await deps.checkpoint?.('claim');

    // The scan's stability check may be minutes old: claim only the file the scan saw. A file
    // still being written (a slow or repeated copy) goes back to the drop zone for a later scan,
    // under a new size/mtime and so a new workflow. The row ends like a vanished one. A hashed
    // row was already checked by the attempt that hashed it.
    const before = await stat(claimed, { bigint: true });
    if (
      row.sha256 === null &&
      (before.size !== BigInt(ref.size) || before.mtimeNs !== BigInt(ref.mtimeNs))
    ) {
      await putBack(ref, row.id);
      return quarantineUnhashed(row.id, 'changed_before_claim', ['DISCOVERED', 'CLAIMED']);
    }

    // Step 3: hash, heartbeating per 1 MiB chunk. Commit CLAIMED + sha256.
    const size = before.size;
    const sha = await sha256File(claimed, () => heartbeat());
    await deps.checkpoint?.('hashed');
    const after = await stat(claimed, { bigint: true });
    if (after.size !== before.size || after.mtimeNs !== before.mtimeNs) {
      // The hash covers bytes that are no longer the file's: never vault them under it.
      await putBack(ref, row.id);
      throw new ChangedWhileHashingError(`${ref.relPath} changed while hashing; put back`);
    }
    try {
      // A row already hashed to other bytes belongs to an earlier claim (e.g. parked in .copies by
      // a newer attempt): this attempt must not re-attribute it to whatever it claimed.
      await guarded(
        pool,
        row.id,
        `state = 'CLAIMED', sha256 = $3`,
        ['DISCOVERED', 'CLAIMED'],
        [sha],
        '(sha256 IS NULL OR sha256 = $3)',
      );
    } catch (e) {
      if (e instanceof StaleAttemptError && claimedHere) await putBack(ref, row.id);
      throw e;
    }

    // Step 4: detect the type by content. Commit ext.
    const meta = await metadata.read(claimed);
    const ext = extOf(meta);
    await guarded(pool, row.id, `ext = $3`, ['CLAIMED'], [ext]);

    // A retired (pixel-identical) content has no vault file on purpose: these exact bytes are a
    // copy of it, never linked back into the vault.
    const retired = await pool.query(
      `SELECT 1 FROM contents WHERE sha256 = $1 AND state = 'DUPLICATE'`,
      [sha],
    );
    if (retired.rowCount === 1) return parkAsCopy(row, claimed, sha, ext);

    // Step 5: vault link. A different inode already there means a byte-identical copy.
    const outcome = await linkIntoVault(claimed, layout.vaultFile(sha, ext), sha, {
      onChunk: () => heartbeat(),
    });
    await deps.checkpoint?.('link');
    if (outcome === 'EXISTS_OTHER') return parkAsCopy(row, claimed, sha, ext);
    // Steps 6–7.
    return commitOwner(row, sha, ext, meta, size);
  };
}
