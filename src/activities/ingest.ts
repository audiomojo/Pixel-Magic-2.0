import { chmod, rename, stat, unlink } from 'node:fs/promises';
import { basename, extname, join } from 'node:path';
import { Context, heartbeat } from '@temporalio/activity';
import type pg from 'pg';
import { withTx } from '../db.js';
import { linkIntoVault, sha256File, vaultRel } from '../fsops.js';
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

type Kind = IngestResult['kind'];
interface Row {
  id: string;
  sha256: string | null;
  ext: string | null;
}

const isEnoent = (e: unknown): boolean => (e as NodeJS.ErrnoException).code === 'ENOENT';

async function statOrNull(path: string) {
  try {
    return await stat(path);
  } catch (e) {
    if (isEnoent(e)) return null;
    throw e;
  }
}

/** Lowercased detected extension; falls back to the dropped name's extension, then `bin`. */
function extOf(meta: Metadata, relPath: string): string {
  const fromName = extname(relPath).slice(1).toLowerCase();
  const ext = meta.ext || fromName;
  return /^[a-z0-9]+$/.test(ext) ? ext : 'bin';
}

export function makeIngestFile(deps: Deps) {
  const { pool, layout, metadata } = deps;
  const checkpoint = async (name: string) => {
    await deps.checkpoint?.(name);
  };
  const claimedPath = (id: string) => join(layout.claimed, id);
  const copiesPath = (id: string, ext: string) => join(layout.vaultCopies, `${id}.${ext}`);
  const vaultPath = (sha: string, ext: string) => join(layout.root, vaultRel(sha, ext));

  /** `UPDATE files … WHERE id AND state IN (allowed)`; throws StaleAttemptError if no row moved. */
  async function guarded(
    db: pg.Pool | pg.PoolClient,
    id: string,
    set: string,
    allowed: string[],
    params: unknown[] = [],
  ): Promise<void> {
    const r = await db.query(`UPDATE files SET ${set} WHERE id = $1 AND state = ANY($2::text[])`, [
      id,
      allowed,
      ...params,
    ]);
    if (r.rowCount !== 1) throw new StaleAttemptError(`file ${id}: not in ${allowed.join('|')}`);
  }

  /** Tail of steps 6–7 (also re-run on replay): drop claimed/<id> only if the bytes' inode is held at `keep`. */
  async function finish(id: string, keep: string): Promise<void> {
    const [c, k] = await Promise.all([statOrNull(claimedPath(id)), statOrNull(keep)]);
    if (!k) return;
    if (c && c.dev === k.dev && c.ino === k.ino) await unlink(claimedPath(id));
    if ((k.mode & 0o777) !== 0o444) await chmod(keep, 0o444);
  }

  async function commitCopy(row: Row, sha: string, ext: string): Promise<IngestResult> {
    await guarded(pool, row.id, `state = 'COPY'`, ['CLAIMED']);
    await finish(row.id, copiesPath(row.id, ext));
    return { fileId: row.id, sha256: sha, ext, state: 'COPY', isNew: false, kind: null };
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
    await finish(row.id, vaultPath(sha, ext));
    return { fileId: row.id, sha256: sha, ext, state: 'INGESTED', isNew: true, kind: meta.kind };
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
      await finish(
        id,
        row.state === 'COPY' ? copiesPath(id, row.ext) : vaultPath(row.sha256, row.ext),
      );
    return {
      fileId: id,
      sha256: row.sha256,
      ext: row.ext,
      state: row.state,
      isNew: row.state === 'INGESTED',
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
   * Neither the drop-zone file nor claimed/<id> exists. Without a hash nothing was ever claimed:
   * the file vanished. With a hash it is never "vanished": its bytes are in .copies (COPY), in the
   * vault (commit as owner), or the attempt fails retryably.
   */
  async function recoverUnclaimed(ref: FileRef, row: Row): Promise<IngestResult> {
    if (!row.sha256) {
      await guarded(pool, row.id, `state = 'QUARANTINED', last_error = 'vanished'`, [
        'DISCOVERED',
        'CLAIMED',
      ]);
      return {
        fileId: row.id,
        sha256: null,
        ext: null,
        state: 'QUARANTINED',
        isNew: false,
        kind: null,
      };
    }
    if (row.ext) {
      const vault = vaultPath(row.sha256, row.ext);
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
    const claimed = claimedPath(row.id);

    // Step 2: claim. An existing claimed file is never overwritten (a re-drop at the same path
    // waits for the next scan), and a copy already parked in .copies is committed as such.
    if (!(await statOrNull(claimed))) {
      if (row.sha256 && row.ext && (await statOrNull(copiesPath(row.id, row.ext))))
        return commitCopy(row, row.sha256, row.ext);
      try {
        await rename(join(layout.dropzone, ref.relPath), claimed);
      } catch (e) {
        if (!isEnoent(e)) throw e;
        if (!(await statOrNull(claimed))) return recoverUnclaimed(ref, row);
      }
    }
    await checkpoint('claim');

    // Step 3: hash, heartbeating per 1 MiB chunk. Commit CLAIMED + sha256.
    const size = BigInt((await stat(claimed)).size);
    const sha = await sha256File(claimed, () => heartbeat());
    await guarded(pool, row.id, `state = 'CLAIMED', sha256 = $3`, ['DISCOVERED', 'CLAIMED'], [sha]);

    // Step 4: detect the type by content. Commit ext.
    const meta = await metadata.read(claimed);
    const ext = extOf(meta, ref.relPath);
    await guarded(pool, row.id, `ext = $3`, ['CLAIMED'], [ext]);

    // Step 5: vault link. A different inode already there means a byte-identical copy.
    const outcome = await linkIntoVault(claimed, vaultPath(sha, ext));
    await checkpoint('link');
    if (outcome === 'EXISTS_OTHER') {
      await rename(claimed, copiesPath(row.id, ext));
      await checkpoint('copies_rename');
      return commitCopy(row, sha, ext);
    }
    // Steps 6–7.
    return commitOwner(row, sha, ext, meta, size);
  };
}
