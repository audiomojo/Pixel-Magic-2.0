import type { Dirent } from 'node:fs';
import { lstat, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { Context, heartbeat, log } from '@temporalio/activity';
import type pg from 'pg';
import { withTx } from '../db.js';
import type { HarvestDeps } from './activities.js';
import {
  childContext,
  classify,
  isJudged,
  keepDir,
  ROOT_CONTEXT,
  type DirContext,
  type Verdict,
} from './classify.js';
import { readMake } from './exifMake.js';
import { pathKey, sameMtime, type SourcePath } from './paths.js';
import { liveMount } from './sources.js';

interface Counters {
  dirs: number;
  photos: number;
  maybes: number;
  junk: Record<string, number>;
  errors: number;
}

export interface ScanFolderResult extends Counters {
  /**
   * `UNREADABLE` (F-2): the folder's root, an ancestor or the source root cannot be read for a
   * permission reason (EPERM/EACCES, typically no Full Disk Access): nothing was read, and the
   * workflow pauses the run with `reason` until the user resumes.
   */
  outcome: 'DONE' | 'SOURCE_GONE' | 'UNREADABLE';
  /** For SOURCE_GONE `<label> unplugged`, for UNREADABLE `cannot read <label>/<path>: …`. */
  reason?: string;
}

const SCAN_HEARTBEAT_MS = 5000;
const BATCH = 500;
const STAT_CONCURRENCY = 16;
const GONE_CODES = new Set(['ENOENT', 'EIO', 'ENOTCONN', 'ENXIO', 'ETIMEDOUT']);

const isGone = (e: unknown): boolean => GONE_CODES.has((e as NodeJS.ErrnoException).code ?? '');
const DENIED_CODES = new Set(['EPERM', 'EACCES']);
const isDenied = (e: unknown): boolean => DENIED_CODES.has((e as NodeJS.ErrnoException).code ?? '');
const errText = (e: unknown): string => (e instanceof Error ? e.message : String(e));

interface Entry {
  name: string;
  /** The name's on-disk bytes when they are not valid UTF-8 or not NFC, else null. */
  raw: Buffer | null;
  isDir: boolean;
  isFile: boolean;
}

/** The name's on-disk bytes when the NFC string would not reopen it exactly, else null. */
const rawOf = (name: string): Buffer | null =>
  name.normalize('NFC') === name ? null : Buffer.from(name, 'utf8');

/**
 * Entries of one directory. A name that is not valid UTF-8, or not NFC (macOS writes NFD
 * accents onto exFAT/SMB/NTFS, which may not match the NFC spelling), keeps its bytes (`raw`):
 * the stored rel_path is NFC, and the source is reopened through those bytes.
 */
export async function listDir(abs: SourcePath): Promise<Entry[]> {
  const ents = await readdir(abs, { withFileTypes: true });
  if (!ents.some((e) => e.name.includes('�'))) {
    return ents.map((e) => toEntry(e.name, rawOf(e.name), e));
  }
  const bufs = await readdir(abs, { withFileTypes: true, encoding: 'buffer' });
  return bufs.map((e) => {
    const buf = e.name as unknown as Buffer;
    const name = buf.toString('utf8');
    return toEntry(name, Buffer.from(name, 'utf8').equals(buf) ? rawOf(name) : buf, e);
  });
}

function toEntry(name: string, raw: Buffer | null, d: Dirent<string> | Dirent<Buffer>): Entry {
  return { name, raw, isDir: d.isDirectory(), isFile: d.isFile() };
}

export const byName = (a: Entry, b: Entry): number =>
  a.name < b.name
    ? -1
    : a.name > b.name
      ? 1
      : a.raw && b.raw
        ? Buffer.compare(a.raw, b.raw)
        : a.raw
          ? 1
          : b.raw
            ? -1
            : 0;

/** `parent` + '/' + a child name, as a path Node can open (a Buffer when bytes are raw). */
function childPath(parent: SourcePath, e: Entry): SourcePath {
  if (typeof parent === 'string' && !e.raw) return join(parent, e.name);
  return Buffer.concat([Buffer.from(parent), Buffer.from('/'), e.raw ?? Buffer.from(e.name)]);
}

/** The filesystem calls resolveComponent makes (tests fake a normalization-sensitive share). */
export interface ResolveIo {
  lstat(path: SourcePath): Promise<{ isDirectory(): boolean }>;
  listDir(path: SourcePath): Promise<Entry[]>;
}

/**
 * The on-disk entry of a selected folder's component `name` (NFC) inside `parent`: the name
 * itself when it opens as a directory, else (on a normalization-sensitive share, where the NFD
 * name on disk does not open by its NFC spelling) the directory entry whose NFC form is `name`.
 * Null when there is none. Errors other than ENOENT (permission, I/O) propagate.
 */
export async function resolveComponent(
  parent: SourcePath,
  name: string,
  io: ResolveIo,
): Promise<Entry | null> {
  const direct: Entry = { name, raw: null, isDir: true, isFile: false };
  try {
    return (await io.lstat(childPath(parent, direct))).isDirectory() ? direct : null;
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
  }
  return (
    (await io.listDir(parent)).find((e) => e.isDir && e.name.normalize('NFC') === name) ?? null
  );
}

/** The on-disk bytes of a source-relative path, or null when every component is NFC UTF-8. */
export function rawRelPath(
  parentRel: string,
  parentRaw: Buffer | null,
  e: Pick<Entry, 'name' | 'raw'>,
): Buffer | null {
  if (!parentRaw && !e.raw) return null;
  const parts = [parentRaw ?? Buffer.from(parentRel)];
  if (parentRel !== '' || parentRaw) parts.push(Buffer.from('/'));
  parts.push(e.raw ?? Buffer.from(e.name));
  return Buffer.concat(parts);
}

const joinRel = (comps: string[], name?: string): string =>
  (name === undefined ? comps : [...comps, name]).join('/');

/** True if `a` is `b` or an ancestor of it. */
const isPrefix = (a: string[], b: string[]): boolean => a.every((c, i) => b[i] === c);

interface Frame {
  comps: string[];
  abs: SourcePath;
  raw: Buffer | null;
  /** Context of the directory itself (the folder root has it up front). */
  ctx?: DirContext;
  parentCtx?: DirContext;
}

export interface Candidate {
  rel: string;
  raw: Buffer | null;
  key: string;
  size: bigint;
  mtimeNs: bigint;
  kind: 'photo' | 'maybe';
}

interface ExistingRow {
  id: string;
  size_bytes: string | bigint;
  mtime_ns: string | bigint;
  state: string;
  kind: string;
  path_key: string;
}

/**
 * State an existing row takes when seen again (spec §5, §7.2); other states are left alone. A
 * FAILED row seen again unchanged is PLANNED with its attempts reset (I6), so a later run
 * retries it instead of it staying FAILED forever.
 */
function seenAgain(row: ExistingRow): string {
  if (row.state === 'FAILED') return 'PLANNED';
  if (row.state === 'VANISHED') return row.kind === 'maybe' ? 'REVIEW' : 'PLANNED';
  return row.state;
}

/** Upserts one batch (≤ BATCH candidates) in the caller's transaction. */
export async function upsertBatch(
  c: pg.PoolClient,
  sourceId: string,
  runId: string,
  batch: Candidate[],
): Promise<void> {
  const { rows } = await c.query<ExistingRow>(
    `SELECT id::text AS id, path_key, size_bytes, mtime_ns, state, kind
     FROM harvest_items WHERE source_id = $1 AND path_key = ANY($2::text[])`,
    [sourceId, batch.map((b) => b.key)],
  );
  const byKey = new Map<string, ExistingRow[]>();
  for (const r of rows) byKey.set(r.path_key, [...(byKey.get(r.path_key) ?? []), r]);

  const upd = {
    id: [] as string[],
    old: [] as string[],
    next: [] as string[],
    rel: [] as string[],
    raw: [] as (string | null)[],
  };
  const ins: Candidate[] = [];
  const seen = new Set<string>();
  for (const b of batch) {
    const same = (byKey.get(b.key) ?? []).filter((r) => BigInt(r.size_bytes) === b.size);
    const hit =
      same.find((r) => BigInt(r.mtime_ns) === b.mtimeNs) ??
      same.find((r) => sameMtime(BigInt(r.mtime_ns), b.mtimeNs));
    if (hit) {
      if (seen.has(hit.id)) continue;
      seen.add(hit.id);
      upd.id.push(hit.id);
      upd.old.push(hit.state);
      upd.next.push(seenAgain(hit));
      upd.rel.push(b.rel);
      upd.raw.push(b.raw?.toString('hex') ?? null);
    } else {
      const k = `${b.key}\0${b.size}\0${b.mtimeNs}`;
      if (seen.has(k)) continue; // an NFC/NFD twin in the same directory
      seen.add(k);
      ins.push(b);
    }
  }
  // The spelling is refreshed from the scan: an NFC <-> NFD (or case) respell keeps the key, and
  // stale raw bytes would no longer reopen the file.
  if (upd.id.length) {
    await c.query(
      `UPDATE harvest_items i
       SET last_seen_run = $1::bigint,
           state = CASE WHEN i.state = u.old THEN u.next ELSE i.state END,
           attempts = CASE WHEN i.state = 'FAILED' AND u.old = 'FAILED' THEN 0 ELSE i.attempts END,
           rel_path = u.rel, rel_path_raw = decode(u.raw, 'hex')
       FROM unnest($2::bigint[], $3::text[], $4::text[], $5::text[], $6::text[])
         AS u(id, old, next, rel, raw)
       WHERE i.id = u.id`,
      [runId, upd.id, upd.old, upd.next, upd.rel, upd.raw],
    );
  }
  if (ins.length) {
    await c.query(
      `INSERT INTO harvest_items
         (source_id, rel_path, rel_path_raw, path_key, size_bytes, mtime_ns, kind, state,
          first_seen_run, last_seen_run)
       SELECT $1::bigint, t.rel, decode(t.raw, 'hex'), t.key, t.size, t.mtime, t.kind,
              CASE WHEN t.kind = 'photo' THEN 'PLANNED' ELSE 'REVIEW' END, $2::bigint, $2::bigint
       FROM unnest($3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[], $8::text[])
         AS t(rel, raw, key, size, mtime, kind)
       ON CONFLICT (source_id, path_key, size_bytes, mtime_ns)
       DO UPDATE SET last_seen_run = EXCLUDED.last_seen_run,
                     rel_path = EXCLUDED.rel_path, rel_path_raw = EXCLUDED.rel_path_raw`,
      [
        sourceId,
        runId,
        ins.map((b) => b.rel),
        ins.map((b) => b.raw?.toString('hex') ?? null),
        ins.map((b) => b.key),
        ins.map((b) => b.size.toString()),
        ins.map((b) => b.mtimeNs.toString()),
        ins.map((b) => b.kind),
      ],
    );
  }
}

export function makeScanFolder(deps: HarvestDeps) {
  const { pool, settings } = deps;

  async function scanFolder(runId: string, folderId: string): Promise<ScanFolderResult> {
    const f = await pool.query<{ source_id: string; rel_path: string; label: string }>(
      `SELECT f.source_id::text AS source_id, f.rel_path, s.label
       FROM harvest_folders f JOIN harvest_sources s ON s.id = f.source_id WHERE f.id = $1`,
      [folderId],
    );
    const folder = f.rows[0];
    if (!folder) throw new Error(`harvest folder ${folderId} not found`);
    const folderComps = folder.rel_path === '' ? [] : folder.rel_path.split('/');
    if (folderComps.some((p) => p === '' || p === '.' || p === '..')) {
      throw new Error(`invalid folder path: ${folder.rel_path}`);
    }

    // Resume: a null cursor starts from scratch (counters zeroed), else counters come with it.
    const start = await pool.query<{
      scan_cursor: string | null;
      dirs: string | bigint;
      photos: string | bigint;
      maybes: string | bigint;
      junk: Record<string, number>;
      errors: string | bigint;
    }>(
      `UPDATE harvest_run_folders SET state = 'scanning',
         root_ok = CASE WHEN scan_cursor IS NULL THEN true ELSE root_ok END,
         dirs = CASE WHEN scan_cursor IS NULL THEN 0 ELSE dirs END,
         photos = CASE WHEN scan_cursor IS NULL THEN 0 ELSE photos END,
         maybes = CASE WHEN scan_cursor IS NULL THEN 0 ELSE maybes END,
         junk = CASE WHEN scan_cursor IS NULL THEN '{}'::jsonb ELSE junk END,
         errors = CASE WHEN scan_cursor IS NULL THEN 0 ELSE errors END
       WHERE run_id = $1 AND folder_id = $2
       RETURNING scan_cursor, dirs, photos, maybes, junk, errors`,
      [runId, folderId],
    );
    const st = start.rows[0];
    if (!st) throw new Error(`folder ${folderId} is not part of run ${runId}`);
    const cursor: string[] | null =
      st.scan_cursor === null ? null : st.scan_cursor === '' ? [] : st.scan_cursor.split('/');
    const total: Counters = {
      dirs: Number(st.dirs),
      photos: Number(st.photos),
      maybes: Number(st.maybes),
      junk: { ...st.junk },
      errors: Number(st.errors),
    };
    const result = (outcome: ScanFolderResult['outcome']): ScanFolderResult => ({
      outcome,
      ...total,
      junk: { ...total.junk },
      ...(outcome === 'SOURCE_GONE' ? { reason: `${folder.label} unplugged` } : {}),
    });

    const live = await liveMount(deps, folder.source_id);
    if (live === null) return result('SOURCE_GONE');
    const mount = live.path;
    /** A "gone" error on a drive that is no longer mounted: the source was unplugged. */
    const unplugged = async (e: unknown): Promise<boolean> => isGone(e) && !(await live.alive());

    /**
     * Ends the folder without walking: state done. `rootOk` false (a missing or unreadable
     * root): nothing was read, so finishRun writes no folder history for it (F-2).
     */
    const finishNow = async (rootOk = true): Promise<ScanFolderResult> => {
      await pool.query(
        `UPDATE harvest_run_folders
         SET state = 'done', scan_cursor = $3, errors = $4, junk = $5::jsonb, root_ok = $6
         WHERE run_id = $1 AND folder_id = $2`,
        [runId, folderId, folder.rel_path, total.errors, JSON.stringify(total.junk), rootOk],
      );
      return result('DONE');
    };
    /** Nothing can be read (permission): the run pauses; the folder is not done (F-2). */
    const unreadable = async (rel: string): Promise<ScanFolderResult> => {
      await pool.query(
        'UPDATE harvest_run_folders SET root_ok = false WHERE run_id = $1 AND folder_id = $2',
        [runId, folderId],
      );
      const what = rel === '' ? folder.label : `${folder.label}/${rel}`;
      log.warn(`harvest scan: cannot read ${what}: permission denied`);
      return { ...result('UNREADABLE'), reason: `cannot read ${what}: permission denied` };
    };
    /** The folder (or an ancestor) is not there on a live drive: one error, done (T5-c). */
    const notFound = (what: string, e?: unknown): Promise<ScanFolderResult> => {
      if (cursor === null) total.errors++; // counted once, not again when a retry resumes
      log.warn(`harvest scan: folder not found ${what}${e ? `: ${errText(e)}` : ''}`);
      return finishNow(false);
    };

    /** A component (or its listing) failed: unplugged, unreadable at `deniedAt`, or not found. */
    const componentFailed = async (
      e: unknown,
      deniedAt: string,
      rel: string,
    ): Promise<ScanFolderResult> => {
      if (await unplugged(e)) return result('SOURCE_GONE');
      if (isDenied(e)) return unreadable(deniedAt);
      return notFound(rel, e);
    };

    // Each component of the folder, resolved against the names on disk (an NFD name may not
    // open by its NFC spelling), and the DirContext from the source root down to the folder's
    // parent; visit derives the folder's own.
    let ctx: DirContext = ROOT_CONTEXT;
    let rootAbs: SourcePath = mount;
    let rootRaw: Buffer | null = null;
    const io: ResolveIo = { lstat: (p) => lstat(p), listDir };
    for (const [i, name] of folderComps.entries()) {
      if (!keepDir(ctx, name)) return finishNow();
      const parentRel = joinRel(folderComps.slice(0, i));
      const rel = joinRel(folderComps.slice(0, i + 1));
      let ent: Entry | null;
      try {
        ent = await resolveComponent(rootAbs, name, io);
      } catch (e) {
        return componentFailed(e, parentRel, rel);
      }
      if (ent === null) return notFound(rel);
      rootRaw = rawRelPath(parentRel, rootRaw, ent);
      rootAbs = childPath(rootAbs, ent);
      if (i === folderComps.length - 1) break;
      let own: Entry[];
      try {
        own = await listDir(rootAbs);
      } catch (e) {
        return componentFailed(e, rel, rel); // nothing below an unreadable ancestor is reachable
      }
      ctx = childContext(ctx, name, own);
    }

    let current: string | null = cursor === null ? null : cursor.join('/');
    const timer = setInterval(
      () => heartbeat({ dirs: total.dirs, cursor: current }),
      deps.scanHeartbeatMs ?? SCAN_HEARTBEAT_MS,
    );
    timer.unref();
    const signal = Context.current().cancellationSignal;

    /** Lists, classifies and upserts one directory; returns its sorted, kept subdirectories. */
    const visit = async (fr: Frame): Promise<Entry[] | 'gone' | 'unreadable'> => {
      // Already committed by a previous attempt: the cursor or an ancestor of it.
      const before = cursor !== null && isPrefix(fr.comps, cursor);
      const isRoot = fr.comps.length === folderComps.length;
      let entries: Entry[];
      try {
        entries = await listDir(fr.abs);
      } catch (e) {
        if (await unplugged(e)) return 'gone';
        // The folder's own root unreadable for a permission reason: nothing can be read.
        if (isRoot && isDenied(e)) return 'unreadable';
        if (before) return [];
        total.errors++;
        const what = isGone(e) && isRoot ? 'folder not found' : 'cannot read directory';
        log.warn(`harvest scan: ${what} ${joinRel(fr.comps)}: ${errText(e)}`);
        await commit(fr, [], isRoot ? false : undefined);
        return [];
      }
      const dirCtx = fr.ctx ?? childContext(fr.parentCtx!, fr.comps[fr.comps.length - 1]!, entries);
      fr.ctx = dirCtx;
      const subdirs = entries.filter((e) => e.isDir && keepDir(dirCtx, e.name)).sort(byName);

      if (!before) {
        const delta: Counters = { dirs: 1, photos: 0, maybes: 0, junk: {}, errors: 0 };
        const cands: Candidate[] = [];
        const files = entries.filter((e) => e.isFile).sort(byName);
        for (let i = 0; i < files.length; i += STAT_CONCURRENCY) {
          const chunk = files.slice(i, i + STAT_CONCURRENCY);
          const outs = await Promise.all(chunk.map((e) => judge(fr, dirCtx, e)));
          if (outs.some((o) => o && 'gone' in o)) return 'gone'; // nothing of this directory is committed
          for (const o of outs) {
            if (!o || 'gone' in o) continue;
            if ('error' in o) {
              delta.errors++;
              log.warn(`harvest scan: ${o.error}`);
            } else if (o.verdict.kind === 'junk') {
              delta.junk[o.verdict.reason] = (delta.junk[o.verdict.reason] ?? 0) + 1;
            } else if (o.cand) {
              cands.push(o.cand);
              if (o.cand.kind === 'photo') delta.photos++;
              else delta.maybes++;
            }
          }
        }
        total.dirs += delta.dirs;
        total.photos += delta.photos;
        total.maybes += delta.maybes;
        total.errors += delta.errors;
        for (const [k, n] of Object.entries(delta.junk)) total.junk[k] = (total.junk[k] ?? 0) + n;
        await commit(fr, cands);
      }
      return subdirs.filter(
        (e) =>
          cursor === null ||
          !isPrefix(fr.comps, cursor) ||
          fr.comps.length === cursor.length ||
          e.name >= cursor[fr.comps.length]!,
      );
    };

    const judge = async (
      fr: Frame,
      dirCtx: DirContext,
      e: Entry,
    ): Promise<
      { error: string } | { gone: true } | { verdict: Verdict; cand: Candidate | null } | null
    > => {
      // Names that are not candidates are dropped before the stat: a drive holds far more of them.
      if (!isJudged(e.name)) return null;
      const rel = joinRel(fr.comps, e.name);
      const abs = childPath(fr.abs, e);
      try {
        const s = await lstat(abs, { bigint: true });
        const verdict = await classify(
          { name: e.name, size: Number(s.size) },
          dirCtx,
          settings.minSize,
          () => (deps.readMake ?? readMake)(abs),
        );
        if (verdict.kind !== 'photo' && verdict.kind !== 'maybe') return { verdict, cand: null };
        const nfc = rel.normalize('NFC');
        return {
          verdict,
          cand: {
            rel: nfc,
            raw: rawRelPath(joinRel(fr.comps), fr.raw, e),
            key: pathKey(rel),
            size: s.size,
            mtimeNs: s.mtimeNs,
            kind: verdict.kind,
          },
        };
      } catch (err) {
        if (await unplugged(err)) return { gone: true };
        return { error: `cannot read ${rel}: ${errText(err)}` };
      }
    };

    /** One transaction: the directory's upserts plus counters and cursor (and `rootOk`). */
    const commit = async (fr: Frame, cands: Candidate[], rootOk?: boolean): Promise<void> => {
      const rel = joinRel(fr.comps);
      await withTx(pool, async (c) => {
        for (let i = 0; i < cands.length; i += BATCH) {
          await upsertBatch(c, folder.source_id, runId, cands.slice(i, i + BATCH));
        }
        await c.query(
          `UPDATE harvest_run_folders
           SET dirs = $3, photos = $4, maybes = $5, junk = $6::jsonb, errors = $7, scan_cursor = $8,
               root_ok = COALESCE($9::boolean, root_ok)
           WHERE run_id = $1 AND folder_id = $2`,
          [
            runId,
            folderId,
            total.dirs,
            total.photos,
            total.maybes,
            JSON.stringify(total.junk),
            total.errors,
            rel,
            rootOk ?? null,
          ],
        );
      });
      current = rel;
      await deps.checkpoint?.('scan_dir_done');
    };

    try {
      const stack: Frame[] = [
        folderComps.length
          ? { comps: folderComps, abs: rootAbs, raw: rootRaw, parentCtx: ctx }
          : { comps: folderComps, abs: rootAbs, raw: null, ctx },
      ];
      while (stack.length) {
        signal.throwIfAborted();
        const fr = stack.pop()!;
        const subs = await visit(fr);
        if (subs === 'gone') return result('SOURCE_GONE');
        if (subs === 'unreadable') return await unreadable(folder.rel_path);
        for (let i = subs.length - 1; i >= 0; i--) {
          const e = subs[i]!;
          stack.push({
            comps: [...fr.comps, e.name],
            abs: childPath(fr.abs, e),
            raw: rawRelPath(joinRel(fr.comps), fr.raw, e),
            parentCtx: fr.ctx!,
          });
        }
      }
    } finally {
      clearInterval(timer);
    }
    await pool.query(
      `UPDATE harvest_run_folders SET state = 'done' WHERE run_id = $1 AND folder_id = $2`,
      [runId, folderId],
    );
    return result('DONE');
  }

  return scanFolder;
}
