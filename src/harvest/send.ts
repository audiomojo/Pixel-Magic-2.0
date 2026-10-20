import { dirname, join } from 'node:path';
import { Context } from '@temporalio/activity';
import type pg from 'pg';
import type { HarvestDeps } from './activities.js';
import { checkKnown, promoteDuplicates } from './known.js';
import {
  dropzoneName,
  GuardedFs,
  partPath,
  readFsOps,
  sameMtime,
  sourcePath,
  type ReadFsOps,
  type SourcePath,
} from './paths.js';
import { finishRunRows, loadScope, setFolderState, underKey, type SendScope } from './runs.js';
import { resolveMount } from './sources.js';
import { checkGate } from './throttle.js';

/** Where the send pass stands: the run folder (index in position order) and the last item. */
export interface SendCursor {
  pos: number;
  /** 0: items spelled exactly under the folder; 1: other spellings (see pageQuery). */
  part: 0 | 1;
  /** The last item's rel_path (part 0) or path_key (part 1): byte order, then id. */
  at: string;
  id: string;
}

/** What the workflow carries from one sendBatch to the next (and across continue-as-new). */
export interface SendState {
  cursor?: SendCursor | null;
  /** Consecutive item errors so far (25 pause the run), carried across batches. */
  consecutive?: number;
  /**
   * Sent while the run is still scanning: the run folder states belong to the scan (a folder
   * marked `done` would be skipped after continue-as-new), so they are left alone, and the end
   * of a pass is DONE (the workflow waits, then starts another pass for what the scan added).
   */
  whileScanning?: boolean;
}

export interface SendBatchResult {
  outcome: 'MORE' | 'DONE' | 'GATED' | 'SOURCE_GONE' | 'PAUSED_ERRORS' | 'DISK_FULL';
  sent: number;
  known: number;
  dups: number;
  failed: number;
  reason?: string;
  /** The cursor for the next call (null: start a new pass from the first folder). */
  cursor: SendCursor | null;
  consecutive: number;
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
/**
 * Errors of a live but flaky share or network: they cost no attempt (a NAS blip must not end in
 * FAILED forever) and count toward the consecutive-error pause instead. EIO only on a network
 * source (on a local drive it is a real read error).
 */
const TRANSIENT_CODES = new Set([
  'ETIMEDOUT',
  'EAGAIN',
  'ECONNRESET',
  'EHOSTDOWN',
  'EHOSTUNREACH',
  'ENETDOWN',
  'ENETUNREACH',
]);
const isTransient = (e: unknown, sourceKind: string | null): boolean =>
  TRANSIENT_CODES.has(code(e)) || (code(e) === 'EIO' && sourceKind === 'network');

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
  path_key: string;
  size_bytes: string;
  mtime_ns: string;
  kind: 'photo' | 'maybe';
  sha256: string | null;
  head_hash: string | null;
  dropzone_path: string | null;
  dropzone_mtime_ns: string | null;
  label: string;
  source_kind: string | null;
  folder_id: string;
  /** Index of the item's run folder in position order, and the page part (see pageQuery). */
  pos: number;
  part: 0 | 1;
}

type Stop = { stop: 'GATED' | 'SOURCE_GONE' | 'DISK_FULL'; reason: string };
/**
 * `error`: an item error under the attempts cap; `capped`: one that reached it (FAILED);
 * `transient`: a flaky-share error that cost no attempt.
 */
type ItemOutcome =
  | 'sent'
  | 'known'
  | 'dup'
  | 'vanished'
  | 'failed'
  | 'error'
  | 'capped'
  | 'transient'
  | 'skipped'
  | Stop;

/**
 * One page of a run folder's PLANNED items (spec §7.3) in send order, after `after` (keyset).
 * Membership is the `path_key` prefix `key/` (the deepest selected folder wins; a send-only run
 * takes only its own approved items, `harvest_run_items`). Two parts:
 * - part 0: items spelled exactly under the folder's rel_path, in `rel_path` byte order (as the
 *   scan walks), then id: a range scan of `harvest_items_planned_rel_idx`, no sort. After the
 *   run's scan (which refreshes spellings) that is every item.
 * - part 1: the rest under the key (another case spelling of the folder, renamed since), in
 *   `path_key` byte order, then id: a range scan of `harvest_items_planned_key_idx`. It runs
 *   once part 0 is exhausted, when the key range holds little more than these.
 */
export function pageQuery(
  scope: SendScope,
  pos: number,
  part: 0 | 1,
  after: { at: string; id: string } | null,
  limit: number,
): { text: string; values: unknown[] } {
  const f = scope.folders[pos]!;
  const values: unknown[] = [f.sourceId];
  const p = (v: unknown): string => `$${values.push(v)}`;
  const where = ['i.source_id = $1', "i.state = 'PLANNED'"];
  if (f.key !== '') {
    // An item spelled under `rel/` has a path_key under `key/` (rel is NFC, and '/' ends the
    // folder name's case context), so part 0 needs no path_key condition: it would only make
    // the planner misjudge the range (the two columns are correlated).
    const exact = `i.rel_path COLLATE "C" >= ${p(`${f.relPath}/`)}
                   AND i.rel_path COLLATE "C" < ${p(`${f.relPath}0`)}`;
    if (part === 0) where.push(exact);
    else {
      where.push(`i.path_key COLLATE "C" >= ${p(`${f.key}/`)}`);
      where.push(`i.path_key COLLATE "C" < ${p(`${f.key}0`)}`);
      where.push(`NOT (${exact})`);
    }
  }
  const deeper = scope.folders.filter(
    (d, i) => i !== pos && d.sourceId === f.sourceId && d.key !== f.key && underKey(d.key, f.key),
  );
  if (deeper.length) {
    where.push(
      `NOT EXISTS (SELECT 1 FROM unnest(${p(deeper.map((d) => `${d.key}/`))}::text[],
                                        ${p(deeper.map((d) => `${d.key}0`))}::text[]) AS d(lo, hi)
                   WHERE i.path_key COLLATE "C" >= d.lo AND i.path_key COLLATE "C" < d.hi)`,
    );
  }
  if (scope.sendOnly) {
    where.push(
      `EXISTS (SELECT 1 FROM harvest_run_items ri
               WHERE ri.run_id = ${p(scope.runId)} AND ri.item_id = i.id)`,
    );
  }
  const col = part === 0 ? 'i.rel_path' : 'i.path_key';
  if (after) {
    where.push(
      `(${col} COLLATE "C", i.id) > (${p(after.at)}::text COLLATE "C", ${p(after.id)}::bigint)`,
    );
  }
  const text = `/* harvest:next-items */
     SELECT i.id::text AS id, i.source_id::text AS source_id, i.rel_path, i.rel_path_raw AS raw,
            i.path_key, i.size_bytes::text AS size_bytes, i.mtime_ns::text AS mtime_ns, i.kind,
            i.sha256, i.head_hash, i.dropzone_path,
            i.dropzone_mtime_ns::text AS dropzone_mtime_ns, s.label, s.kind AS source_kind
     FROM harvest_items i JOIN harvest_sources s ON s.id = i.source_id
     WHERE ${where.join('\n       AND ')}
     ORDER BY ${col} COLLATE "C", i.id
     LIMIT ${p(limit)}`;
  return { text, values };
}

/** The (folder, part) steps of a pass in order: part 1 only for a folder other than the root. */
function* steps(scope: SendScope, from: SendCursor | null): Generator<[number, 0 | 1]> {
  for (let pos = from?.pos ?? 0; pos < scope.folders.length; pos++) {
    for (const part of [0, 1] as const) {
      if (part === 1 && scope.folders[pos]!.key === '') continue;
      if (from && pos === from.pos && part < from.part) continue;
      yield [pos, part];
    }
  }
}

/** The run's next ≤ `limit` PLANNED items from `cursor`, folder by folder in position order. */
async function nextItems(
  pool: pg.Pool,
  scope: SendScope,
  cursor: SendCursor | null,
  limit: number,
): Promise<Item[]> {
  const out: Item[] = [];
  for (const [pos, part] of steps(scope, cursor)) {
    if (out.length >= limit) break;
    const after = cursor && pos === cursor.pos && part === cursor.part ? cursor : null;
    const q = pageQuery(scope, pos, part, after, limit - out.length);
    const { rows } = await pool.query<Omit<Item, 'folder_id' | 'pos' | 'part'>>(q.text, q.values);
    for (const r of rows) out.push({ ...r, folder_id: scope.folders[pos]!.folderId, pos, part });
  }
  return out;
}

/** Whether any item of the run's scope is still PLANNED (one LIMIT 1 page per folder). */
async function anyPlanned(pool: pg.Pool, scope: SendScope): Promise<boolean> {
  for (const [pos, part] of steps(scope, null)) {
    const q = pageQuery(scope, pos, part, null, 1);
    if ((await pool.query(q.text, q.values)).rowCount) return true;
  }
  return false;
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

  async function sendBatch(runId: string, state: SendState = {}): Promise<SendBatchResult> {
    const ctx = Context.current();
    const signal = ctx.cancellationSignal;
    const gfs = new GuardedFs([join(layout.dropzone, 'harvest')], deps.fsOps);
    const dz = layout.dropzone;
    const res = { sent: 0, known: 0, dups: 0, failed: 0 };
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
    /** A transient error: recorded on the row, the attempt count untouched. */
    const flaky = async (it: Item, why: string): Promise<'transient'> => {
      lastError = why;
      await pool.query(
        `UPDATE harvest_items SET last_error = $2 WHERE id = $1 AND state = 'PLANNED'`,
        [it.id, why],
      );
      return 'transient';
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
          pathKey: it.path_key,
          relPath: it.rel_path,
          runId,
        },
        {
          onChunk: beater({ hashing: it.rel_path }),
          // Candidates are read only on the mount found by identity, or from our own copy.
          mountOf: async (sourceId) => (await mountOf(sourceId))?.path ?? null,
          dropzone: dz,
        },
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
        if (isTransient(e, it.source_kind)) return flaky(it, `${it.rel_path}: ${errText(e)}`);
        return fail(it, `${it.rel_path}: ${errText(e)}`);
      }
    };

    // The gate first (cheap): a shut gate never pages the items, however often it is retried.
    const gate = await checkGate(deps, runId, 0n);
    if (!gate.open) {
      return {
        ...res,
        outcome: 'GATED',
        reason: gate.reason,
        cursor: state.cursor ?? null,
        consecutive: state.consecutive ?? 0,
      };
    }
    let consecutive = state.consecutive ?? 0;
    let cursor: SendCursor | null = state.cursor ?? null;
    const scope = await loadScope(pool, runId);
    const limit = deps.sendBatchSize ?? SEND_BATCH;
    const batch = await nextItems(pool, scope, cursor, limit);
    const stopped = (outcome: SendBatchResult['outcome'], reason: string): SendBatchResult => ({
      ...res,
      outcome,
      reason,
      cursor,
      consecutive,
    });
    // Run folder states (spec §9): the folder being sent is `sending`, one whose drive is gone
    // `waiting_source`, every other `done` once its turn has passed.
    const scanning = state.whileScanning === true;
    let current: string | null = null;
    for (const it of batch) {
      signal.throwIfAborted();
      if (!scanning && it.folder_id !== current) {
        await pool.query(
          `UPDATE harvest_run_folders
           SET state = CASE WHEN folder_id = $2 THEN 'sending' ELSE 'done' END
           WHERE run_id = $1 AND (folder_id = $2 OR state IN ('sending', 'waiting_source'))`,
          [runId, it.folder_id],
        );
        current = it.folder_id;
      }
      let out: ItemOutcome;
      try {
        out = await sendItem(it);
      } catch (e) {
        throw e instanceof Crash ? e.cause : e;
      }
      // A stop leaves the cursor before this item: it is tried again first.
      if (typeof out === 'object') {
        if (out.stop === 'SOURCE_GONE' && !scanning) {
          await setFolderState(pool, runId, it.folder_id, 'waiting_source');
        }
        return stopped(out.stop, out.reason);
      }
      cursor = {
        pos: it.pos,
        part: it.part,
        at: it.part === 0 ? it.rel_path : it.path_key,
        id: it.id,
      };
      if (out === 'sent') res.sent++;
      else if (out === 'known') res.known++;
      else if (out === 'dup') res.dups++;
      else if (out === 'failed' || out === 'capped') res.failed++;
      consecutive =
        out === 'error' || out === 'capped' || out === 'transient' ? consecutive + 1 : 0;
      ctx.heartbeat({ ...counts(), item: it.rel_path });
      if (consecutive >= MAX_CONSECUTIVE_ERRORS) return stopped('PAUSED_ERRORS', lastError);
    }
    // A full batch: more may follow the cursor. Otherwise the pass is over; items left PLANNED
    // behind it (an error under the cap, an approval, a promoted duplicate) start a new pass.
    if (batch.length === limit) return { ...res, outcome: 'MORE', cursor, consecutive };
    if (scanning) return { ...res, outcome: 'DONE', cursor: null, consecutive };
    const again = await anyPlanned(pool, scope);
    if (!again) {
      await pool.query(
        `UPDATE harvest_run_folders SET state = 'done' WHERE run_id = $1 AND state <> 'done'`,
        [runId],
      );
    }
    return { ...res, outcome: again ? 'MORE' : 'DONE', cursor: null, consecutive };
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
