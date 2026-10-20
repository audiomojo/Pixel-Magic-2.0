import { dirname, join } from 'node:path';
import { Context } from '@temporalio/activity';
import type pg from 'pg';
import type { HarvestDeps } from './activities.js';
import { checkKnown, promoteDuplicates } from './known.js';
import {
  dropzoneName,
  GuardedFs,
  partPath,
  pathKey,
  readFsOps,
  sameMtime,
  sourcePath,
  type ReadFsOps,
  type SourcePath,
} from './paths.js';
import { finishRunRows, runFolders } from './runs.js';
import { resolveMount } from './sources.js';
import { checkGate } from './throttle.js';

export interface SendBatchResult {
  outcome: 'MORE' | 'DONE' | 'GATED' | 'SOURCE_GONE' | 'PAUSED_ERRORS' | 'DISK_FULL';
  sent: number;
  known: number;
  dups: number;
  failed: number;
  reason?: string;
}

export const SEND_BATCH = 500;
export const MAX_CONSECUTIVE_ERRORS = 25;
/** The pipeline's empty-folder cleanup may remove a parent between mkdir and open. */
const MKDIR_RETRIES = 3;
/** Publishing found the name taken (a race): choose another name, at most this often. */
const PUBLISH_RETRIES = 5;
const COPY_HEARTBEAT_MS = 1000;
const COPY_HEARTBEAT_BYTES = 8 * 1024 * 1024;
const DISK_FULL_CODES = new Set(['ENOSPC', 'EDQUOT']);

const code = (e: unknown): string => (e as NodeJS.ErrnoException | null)?.code ?? '';
const errText = (e: unknown): string => (e instanceof Error ? e.message : String(e));

/** A throw from a test checkpoint: a simulated crash, never handled as an item error. */
class Crash extends Error {
  constructor(cause: unknown) {
    super('checkpoint crash', { cause });
  }
}

interface Item {
  id: string;
  source_id: string;
  rel_path: string;
  /** The on-disk bytes of rel_path when they differ from its NFC string (see scan listDir). */
  raw: Buffer | null;
  size_bytes: string;
  mtime_ns: string;
  kind: 'photo' | 'maybe';
  sha256: string | null;
  head_hash: string | null;
  dropzone_path: string | null;
  dropzone_mtime_ns: string | null;
  label: string;
  folder_id: string;
}

type Stop = { stop: 'GATED' | 'SOURCE_GONE' | 'DISK_FULL'; reason: string };
/** `error`: an item error under the attempts cap; `capped`: one that reached it (FAILED). */
type ItemOutcome =
  'sent' | 'known' | 'dup' | 'vanished' | 'failed' | 'error' | 'capped' | 'skipped' | Stop;

const escapeLike = (s: string): string => s.replace(/[\\%_]/g, (c) => `\\${c}`);

/**
 * The run's next PLANNED items (spec §7.1): under one of its folders by `path_key` (the folder
 * key itself or a `key/…` prefix), ordered by folder position, then `rel_path` (byte order, as
 * the scan walks). An item under
 * nested selected folders counts for the deepest one.
 */
async function nextItems(pool: pg.Pool, runId: string, limit: number): Promise<Item[]> {
  const folders = await runFolders(pool, runId);
  if (folders.length === 0) return [];
  const keys = folders.map((f) => pathKey(f.relPath));
  const { rows } = await pool.query<Item>(
    `WITH rf AS (
       SELECT * FROM unnest($2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::int[])
         AS t(folder_id, source_id, key, pat, pos)
     ), m AS (
       SELECT DISTINCT ON (i.id) i.*, rf.folder_id AS run_folder_id, rf.pos
       FROM harvest_items i JOIN rf ON rf.source_id = i.source_id
         AND (rf.key = '' OR i.path_key = rf.key OR i.path_key LIKE rf.pat ESCAPE '\\')
       WHERE i.state = 'PLANNED'
       ORDER BY i.id, length(rf.key) DESC
     )
     SELECT m.id::text AS id, m.source_id::text AS source_id, m.rel_path,
            m.rel_path_raw AS raw, m.size_bytes::text AS size_bytes,
            m.mtime_ns::text AS mtime_ns, m.kind, m.sha256, m.head_hash, m.dropzone_path,
            m.dropzone_mtime_ns::text AS dropzone_mtime_ns, s.label,
            m.run_folder_id::text AS folder_id
     FROM m JOIN harvest_sources s ON s.id = m.source_id
     ORDER BY m.pos, m.rel_path COLLATE "C", m.id
     LIMIT $1`,
    [
      limit,
      folders.map((f) => f.folderId),
      folders.map((f) => f.sourceId),
      keys,
      keys.map((k) => `${escapeLike(k)}/%`),
      folders.map((_, i) => i),
    ],
  );
  return rows;
}

/**
 * One idempotent item transition from PLANNED plus its run-folder counter, in one statement.
 * `extra` adds a condition (the DUP target check). Returns whether the item moved.
 */
async function settle(
  pool: pg.Pool,
  runId: string,
  it: Item,
  counter: 'sent' | 'known' | 'dups',
  set: string,
  params: unknown[],
  extra = '',
): Promise<boolean> {
  const bytes =
    counter === 'sent'
      ? ', bytes_sent = bytes_sent + COALESCE((SELECT sum(size_bytes) FROM u), 0)'
      : '';
  const { rows } = await pool.query<{ n: number }>(
    `WITH u AS (
       UPDATE harvest_items SET ${set}
       WHERE id = $1 AND state = 'PLANNED' ${extra}
       RETURNING size_bytes
     ), c AS (
       UPDATE harvest_run_folders SET ${counter} = ${counter} + (SELECT count(*) FROM u)${bytes}
       WHERE run_id = $2 AND folder_id = $3
     )
     SELECT count(*)::int AS n FROM u`,
    [it.id, runId, it.folder_id, ...params],
  );
  return rows[0]!.n > 0;
}

export function makeSendBatch(deps: HarvestDeps) {
  const { pool, settings, layout } = deps;
  // Outside GuardedFs the send path only reads (stat, read-only open of the sources).
  const ops: ReadFsOps = deps.fsOps ?? readFsOps;
  const known = deps.knownCheck ?? checkKnown;

  async function sendBatch(runId: string): Promise<SendBatchResult> {
    const ctx = Context.current();
    const signal = ctx.cancellationSignal;
    const gfs = new GuardedFs([join(layout.dropzone, 'harvest')], deps.fsOps);
    const dz = layout.dropzone;
    const res: SendBatchResult = { outcome: 'DONE', sent: 0, known: 0, dups: 0, failed: 0 };
    const counts = () => ({ sent: res.sent, known: res.known, dups: res.dups, failed: res.failed });

    const checkpoint = async (name: string): Promise<void> => {
      try {
        await deps.checkpoint?.(name);
      } catch (e) {
        throw new Crash(e);
      }
    };
    const removeIfExists = async (p: string): Promise<void> => {
      try {
        await gfs.unlink(p);
      } catch {
        // Missing is fine; a failed cleanup leaves an orphan .part the next attempt removes.
      }
    };
    const statOrNull = (p: SourcePath) =>
      ops.stat(p).catch((e: unknown) => {
        if (code(e) === 'ENOENT') return null;
        throw e;
      });

    // Mounts are resolved once per batch, with the root's device id (an unplugged drive can
    // leave a plain directory at its mount point: "exists" is not "still mounted").
    const mounts = new Map<string, { path: string; dev: bigint } | null>();
    const mountOf = async (sourceId: string) => {
      if (!mounts.has(sourceId)) {
        const p = await resolveMount(pool, deps.sys, sourceId, settings.dataDir, deps.detectOpts);
        const dev =
          p === null
            ? null
            : await ops.stat(p).then(
                (s) => s.dev,
                () => null,
              );
        mounts.set(sourceId, p !== null && dev !== null ? { path: p, dev } : null);
      }
      return mounts.get(sourceId)!;
    };
    const alive = (m: { path: string; dev: bigint }): Promise<boolean> =>
      ops.stat(m.path).then(
        (s) => s.dev === m.dev,
        () => false,
      );

    const vanish = async (it: Item, why: string): Promise<'vanished'> => {
      const r = await pool.query(
        `UPDATE harvest_items SET state = 'VANISHED', last_error = $2
         WHERE id = $1 AND state = 'PLANNED'`,
        [it.id, why],
      );
      if (r.rowCount) await promoteDuplicates(pool, it.id);
      return 'vanished';
    };
    let lastError = '';
    const fail = async (it: Item, why: string): Promise<'capped' | 'error'> => {
      lastError = why;
      const { rows } = await pool.query<{ state: string }>(
        `UPDATE harvest_items SET attempts = attempts + 1, last_error = $2,
           state = CASE WHEN attempts + 1 >= $3 THEN 'FAILED' ELSE state END
         WHERE id = $1 AND state = 'PLANNED' RETURNING state`,
        [it.id, why, settings.maxAttempts],
      );
      if (rows[0]?.state !== 'FAILED') return 'error';
      await promoteDuplicates(pool, it.id);
      return 'capped';
    };
    const markSent = async (
      it: Item,
      dzPath: string,
      dzMtime: string,
    ): Promise<'sent' | 'skipped'> =>
      (await settle(
        pool,
        runId,
        it,
        'sent',
        `state = 'SENT', sent_at = clock_timestamp(), dropzone_path = $4, dropzone_mtime_ns = $5,
         last_error = NULL`,
        [dzPath, dzMtime],
      ))
        ? 'sent'
        : 'skipped';

    /** Crash recognition: our stored destination is in the drop zone, or the pipeline has it. */
    const alreadyPublished = async (it: Item): Promise<boolean> => {
      const s = await statOrNull(join(dz, it.dropzone_path!));
      const size = BigInt(it.size_bytes);
      const mtime = BigInt(it.dropzone_mtime_ns!);
      if (s?.isFile() && s.size === size && s.mtimeNs === mtime) return true;
      const f = await pool.query(
        'SELECT 1 FROM files WHERE rel_path = $1 AND size_bytes = $2 AND mtime_ns = $3 LIMIT 1',
        [it.dropzone_path, it.size_bytes, it.dropzone_mtime_ns],
      );
      return (f.rowCount ?? 0) > 0;
    };

    /**
     * Step 1: the first free name — not in the drop zone (lstat: casefolded on APFS) and not
     * held by another unlinked SENT item or a PLANNED one that reserved it — stored on the row
     * (still PLANNED) before any byte is written. Null if the item is no longer PLANNED.
     */
    const chooseDest = async (it: Item): Promise<string | null> => {
      for (let k = 1; ; k++) {
        const cand = dropzoneName(it.label, it.rel_path, k);
        if (await gfs.exists(join(dz, cand))) continue;
        const taken = await pool.query(
          `SELECT 1 FROM harvest_items
           WHERE file_id IS NULL AND lower(dropzone_path) = lower($2) AND id <> $1
             AND state IN ('SENT', 'PLANNED') LIMIT 1`,
          [it.id, cand],
        );
        if (taken.rowCount) continue;
        const r = await pool.query(
          `UPDATE harvest_items SET dropzone_path = $2, dropzone_mtime_ns = NULL
           WHERE id = $1 AND state = 'PLANNED'`,
          [it.id, cand],
        );
        return r.rowCount ? cand : null;
      }
    };

    /** A heartbeat at most every second (or 8 MiB) during long reads, then a cancel check. */
    const beater = (detail: Record<string, unknown>) => {
      let lastBeat = 0;
      let lastBytes = 0;
      return (bytes = 0) => {
        if (
          bytes - lastBytes >= COPY_HEARTBEAT_BYTES ||
          Date.now() - lastBeat >= COPY_HEARTBEAT_MS
        ) {
          ctx.heartbeat({ ...counts(), ...detail, ...(bytes ? { copying: bytes } : {}) });
          lastBeat = Date.now();
          lastBytes = bytes;
        }
      };
    };

    /**
     * Steps 2, 4 and 6: mkdir -p (retried when the parent vanishes), the byte copy into `.part`,
     * then mode and mtime on the open handle and fsync. Returns the part's on-disk mtime.
     */
    const copyToPart = async (
      src: SourcePath,
      part: string,
      st: { mode: bigint; mtimeNs: bigint },
      rel: string,
    ): Promise<bigint> => {
      const beat = beater({ file: rel });
      const onChunk = (written: number) => {
        beat(written);
        signal.throwIfAborted();
      };
      for (let i = 0; ; i++) {
        await gfs.mkdirp(dirname(part));
        if (i === 0) await checkpoint('send_dest_reserved');
        try {
          // BSD flags are never copied: only the permission bits, with owner read/write.
          return await gfs.copyInto(src, part, {
            mode: (Number(st.mode) & 0o777) | 0o600,
            mtimeNs: st.mtimeNs,
            onChunk,
          });
        } catch (e) {
          if (code(e) === 'ENOENT' && i < MKDIR_RETRIES && !(await gfs.exists(dirname(part)))) {
            continue;
          }
          throw e;
        }
      }
    };

    const sendItem = async (it: Item): Promise<ItemOutcome> => {
      // 0. A destination stored by an earlier attempt: already published (crash between the
      // publish and SENT)? Else remove our own orphaned .part.
      if (it.dropzone_path !== null) {
        const part = partPath(join(dz, it.dropzone_path), it.id);
        if (it.dropzone_mtime_ns !== null && (await alreadyPublished(it))) {
          await removeIfExists(part);
          return markSent(it, it.dropzone_path, it.dropzone_mtime_ns);
        }
        await removeIfExists(part);
      }
      const mount = await mountOf(it.source_id);
      if (mount === null) return { stop: 'SOURCE_GONE', reason: `${it.label} unplugged` };
      // Read through the on-disk bytes when they differ from the NFC rel_path; the drop-zone
      // name is always the NFC rel_path.
      const src = sourcePath(mount.path, it.rel_path, it.raw);
      const size = BigInt(it.size_bytes);

      // Throttle gate, before every file.
      const gate = await checkGate(deps, runId, size);
      if (!gate.open) return { stop: 'GATED', reason: gate.reason };

      // Known content (read errors on the source are handled inside: the item is sent).
      const k = await known(
        pool,
        {
          id: it.id,
          sourceId: it.source_id,
          absPath: src,
          sizeBytes: size,
          kind: it.kind,
          sha256: it.sha256,
          headHash: it.head_hash,
        },
        { onChunk: beater({ hashing: it.rel_path }) },
      );
      signal.throwIfAborted();
      if (k.state === 'KNOWN') {
        const ok = await settle(pool, runId, it, 'known', `state = 'KNOWN', sha256 = $4`, [
          k.sha256,
        ]);
        return ok ? 'known' : 'skipped';
      }
      if (k.state === 'DUP') {
        // Only while the target is still SENT or PLANNED, in the same statement; else send.
        const ok = await settle(
          pool,
          runId,
          it,
          'dups',
          `state = 'DUP', sha256 = $4, duplicate_of = $5`,
          [k.sha256, k.duplicateOf],
          `AND EXISTS (SELECT 1 FROM harvest_items t
                       WHERE t.id = $5 AND t.state IN ('SENT', 'PLANNED'))`,
        );
        if (ok) return 'dup';
      }

      try {
        // 3. The source still matches the row (an exFAT DST shift is the same file).
        const st = await ops.stat(src);
        if (!st.isFile() || st.size !== size || !sameMtime(st.mtimeNs, BigInt(it.mtime_ns))) {
          return vanish(it, 'changed on the source since the scan');
        }

        for (let round = 0; ; round++) {
          const dzPath = await chooseDest(it);
          if (dzPath === null) return 'skipped';
          const finalAbs = join(dz, dzPath);
          const part = partPath(finalAbs, it.id);
          try {
            const dzMtime = (await copyToPart(src, part, st, it.rel_path)).toString();
            await checkpoint('send_part_written');
            // 5. Complete, and the source unchanged while we read it.
            const now = await ops.stat(src);
            if (now.size !== st.size || now.mtimeNs !== st.mtimeNs || now.ino !== st.ino) {
              await removeIfExists(part);
              return vanish(it, 'changed on the source during the copy');
            }
            const ps = await ops.stat(part);
            if (ps.size !== st.size) {
              throw new Error(`short copy: ${ps.size} of ${st.size} bytes`);
            }
            // The on-disk mtime (step 6 ran on the handle) is stored before publishing so a crash
            // after the publish can recognise the file. No longer PLANNED: never publish.
            const kept = await pool.query(
              `UPDATE harvest_items SET dropzone_mtime_ns = $2 WHERE id = $1 AND state = 'PLANNED'`,
              [it.id, dzMtime],
            );
            if (!kept.rowCount) {
              await removeIfExists(part);
              return 'skipped';
            }
            // 7. Publish without ever replacing: a taken name goes back to step 1. Once linked
            // it is published, even if the .part could not be unlinked (it is ignored).
            try {
              if (!(await gfs.publishNoReplace(part, finalAbs))) await removeIfExists(part);
            } catch (e) {
              if (code(e) === 'EEXIST' && round < PUBLISH_RETRIES) {
                await removeIfExists(part);
                continue;
              }
              throw e;
            }
            await checkpoint('send_renamed');
            // 8.
            return markSent(it, dzPath, dzMtime);
          } catch (e) {
            if (!(e instanceof Crash)) await removeIfExists(part);
            throw e;
          }
        }
      } catch (e) {
        if (e instanceof Crash || signal.aborted) throw e;
        if (DISK_FULL_CODES.has(code(e))) return { stop: 'DISK_FULL', reason: 'disk full' };
        // Whatever errno a dying mount produces, an unplug never costs the item an attempt.
        if (!(await alive(mount))) return { stop: 'SOURCE_GONE', reason: `${it.label} unplugged` };
        if (code(e) === 'ENOENT' && (await statOrNull(src)) === null) {
          return vanish(it, 'gone from the source');
        }
        return fail(it, `${it.rel_path}: ${errText(e)}`);
      }
    };

    let consecutive = 0;
    const batch = await nextItems(pool, runId, SEND_BATCH);
    for (const it of batch) {
      signal.throwIfAborted();
      let out: ItemOutcome;
      try {
        out = await sendItem(it);
      } catch (e) {
        throw e instanceof Crash ? e.cause : e;
      }
      if (typeof out === 'object') return { ...res, outcome: out.stop, reason: out.reason };
      if (out === 'sent') res.sent++;
      else if (out === 'known') res.known++;
      else if (out === 'dup') res.dups++;
      else if (out === 'failed' || out === 'capped') res.failed++;
      consecutive = out === 'error' || out === 'capped' ? consecutive + 1 : 0;
      ctx.heartbeat({ ...counts(), item: it.rel_path });
      if (consecutive >= MAX_CONSECUTIVE_ERRORS) {
        return { ...res, outcome: 'PAUSED_ERRORS', reason: lastError };
      }
    }
    const more = (await nextItems(pool, runId, 1)).length > 0;
    return { ...res, outcome: more ? 'MORE' : 'DONE' };
  }

  return sendBatch;
}

/**
 * Links SENT items to the pipeline's `files` rows (spec §5): same `rel_path`, size and the
 * drop-zone copy's own mtime (`dropzone_mtime_ns`). Returns the number linked.
 */
export async function linkFiles(pool: pg.Pool): Promise<number> {
  const r = await pool.query(
    `WITH m AS (
       SELECT DISTINCT ON (i.id) i.id AS item_id, f.id AS file_id
       FROM harvest_items i
       JOIN files f ON f.rel_path = i.dropzone_path AND f.size_bytes = i.size_bytes
                   AND f.mtime_ns = i.dropzone_mtime_ns
       WHERE i.state = 'SENT' AND i.file_id IS NULL
       ORDER BY i.id, f.id DESC
     )
     UPDATE harvest_items i SET file_id = m.file_id
     FROM m WHERE i.id = m.item_id AND i.file_id IS NULL`,
  );
  return r.rowCount ?? 0;
}

export function makeFinishRun(deps: Pick<HarvestDeps, 'pool'>) {
  return async function finishRun(
    runId: string,
    outcome: 'done' | 'cancelled' | 'failed',
    error?: string,
  ): Promise<void> {
    await finishRunRows(deps.pool, runId, outcome, error ?? null);
    await linkFiles(deps.pool);
  };
}
