// HTTP API and static files of the harvester (spec §8). Local only: 127.0.0.1, no login, so
// every request is checked against DNS rebinding (Host) and every POST against cross-site
// requests (X-Pixel-Magic, Origin). The only disk writes are thumbnails, through GuardedFs.
import { lstat, readdir, readFile, realpath, stat } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { extname, join, sep } from 'node:path';
import type pg from 'pg';
import { z } from 'zod';
import { logger } from '../logger.js';
import { errCode, errText, isMissing } from '../fsops.js';
import type { RunControl } from './activities.js';
import { childContext, keepDir, ROOT_CONTEXT, type DirContext } from './classify.js';
import type { HarvestSettings } from './config.js';
import { GuardedFs, sourcePath } from './paths.js';
import {
  ACTIVE_STATES_SQL,
  addRunItems,
  createRun,
  finishRunRows,
  startNextQueued,
} from './runs.js';
import {
  detectSources,
  registerDetected,
  type UnidentifiedVolume,
  type DetectOptions,
  type SourceRow,
  type SystemProbe,
} from './sources.js';
import { measureBacklog } from './throttle.js';
import { withTimeout } from './timeout.js';
import { evictThumbs, makeThumb, placeholderJpeg } from './thumbs.js';

export interface HarvestServerDeps {
  pool: pg.Pool;
  settings: HarvestSettings;
  sys: SystemProbe;
  control: RunControl;
  /** Directory of the static page (`src/harvest/ui`). */
  staticDir: string;
  /** A lightweight Temporal call succeeded (the host passes a real one; tests fake it). */
  temporalHealthy: () => Promise<boolean>;
  /** Passed to detectSources; tests point `volumes` at a temp directory. */
  detectOpts?: DetectOptions;
  /** How long a drive detection is reused by GET /api/state (default 10 s). */
  sourcesTtlMs?: number;
  /** Per-drive statfs cap for /api/state (default 2 s). */
  statfsTimeoutMs?: number;
  /** Tree listing timeout (default 10 s). */
  treeTimeoutMs?: number;
  /** Directory listing seam (tests inject hangs and faults). */
  listDir?: (path: string) => Promise<{ name: string; isDirectory(): boolean }[]>;
}

const BODY_LIMIT = 1024 * 1024;
/** A database id in a path or body. */
const ID_RE = /^\d{1,15}$/;

const isDenied = (e: unknown): boolean => ['EPERM', 'EACCES'].includes(errCode(e) ?? '');

/** What the page shows when macOS refuses a listing (README: granting drive access). */
const PERMISSION_HINT =
  'macOS does not let the harvester read this drive (permission denied). In System Settings → ' +
  'Privacy & Security → Files & Folders, turn on Removable Volumes (or Network Volumes) for ' +
  'pixel-magic-harvester, then run make harvest-stop and make harvest-start.';

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
};

/** A folder path inside a source: NFC, no leading '/', no '..', no NUL; '' is the whole source. */
function normaliseRelPath(raw: string): string {
  const rel = raw.normalize('NFC');
  if (rel.startsWith('/') || rel.includes('\0')) throw new HttpError(400, `invalid path: ${raw}`);
  const parts: string[] = [];
  for (const p of rel.split('/')) {
    if (p === '' || p === '.') continue;
    if (p === '..' || p.includes('\\')) throw new HttpError(400, `invalid path: ${raw}`);
    parts.push(p);
  }
  return parts.join('/');
}

const relPathSchema = z.string().transform((s, ctx) => {
  try {
    return normaliseRelPath(s);
  } catch {
    ctx.addIssue({ code: 'custom', message: `invalid path: ${s}` });
    return z.NEVER;
  }
});
const idSchema = z.union([z.string().regex(ID_RE), z.number().int().min(0)]).transform(String);

const runsBody = z.object({
  folders: z.array(z.object({ sourceId: idSchema, relPath: relPathSchema })).min(1),
  dryRun: z.boolean().default(false),
});
const reviewBody = z.union([
  z.object({
    itemIds: z.array(idSchema).min(1).max(10000),
    action: z.enum(['send', 'skip']),
  }),
  z.object({ sourceId: idSchema, path: relPathSchema, action: z.enum(['send', 'skip']) }),
]);

const parentDir = (rel: string): string =>
  rel.includes('/') ? rel.slice(0, rel.lastIndexOf('/')) : '';
/** SQL for the parent folder of a rel_path column. */
const parentSql = (col: string): string => `regexp_replace(${col}, '/?[^/]*$', '')`;

type Json = Record<string, unknown>;

function send(res: http.ServerResponse, status: number, body: Json): void {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(text),
    'Cache-Control': 'no-store',
  });
  res.end(text);
}

/** The JSON body, up to 1 MB; a larger body is read to the end and discarded, then 413. */
function readBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let tooBig = false;
    req.on('data', (c: Buffer) => {
      size += c.length;
      if (size > BODY_LIMIT) {
        tooBig = true;
        chunks.length = 0;
      } else if (!tooBig) chunks.push(c);
    });
    req.on('error', reject);
    req.on('end', () => {
      if (tooBig) return reject(new HttpError(413, 'request body too large (limit 1 MB)'));
      const text = Buffer.concat(chunks).toString('utf8');
      if (text.trim() === '') return resolve({});
      try {
        resolve(JSON.parse(text));
      } catch {
        reject(new HttpError(400, 'invalid JSON'));
      }
    });
  });
}

function parse<T extends z.ZodType>(schema: T, value: unknown): z.output<T> {
  const r = schema.safeParse(value);
  if (!r.success) {
    throw new HttpError(
      400,
      r.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`.replace(/^: /, '')).join('; '),
    );
  }
  return r.data;
}

const timeout = <T>(work: Promise<T>, ms: number, what: string): Promise<T> =>
  withTimeout(work, ms, () => {
    throw new HttpError(504, `${what} timed out`);
  });

export function createHarvestServer(d: HarvestServerDeps): http.Server {
  const { pool, settings, sys, control } = d;
  const cacheRoot = join(settings.dataDir, 'harvest-cache');
  const thumbDir = join(cacheRoot, 'thumbs');
  const cacheFs = new GuardedFs([cacheRoot]);
  const listDir = d.listDir ?? ((p) => readdir(p, { withFileTypes: true }));
  const treeTimeoutMs = d.treeTimeoutMs ?? 10_000;
  const ttl = d.sourcesTtlMs ?? 10_000;
  const statfsTimeoutMs = d.statfsTimeoutMs ?? 2000;

  // ---- drives -------------------------------------------------------------------------
  interface Detection {
    rows: SourceRow[];
    unidentified: UnidentifiedVolume[];
  }
  let sourcesCache: ({ at: number } & Detection) | undefined;
  let detecting: Promise<Detection> | undefined;
  async function detect(force = false): Promise<Detection> {
    if (!force && sourcesCache && Date.now() - sourcesCache.at < ttl) return sourcesCache;
    detecting ??= (async () => {
      try {
        const found = await registerDetected(
          pool,
          await detectSources(sys, settings.dataDir, d.detectOpts),
        );
        sourcesCache = { at: Date.now(), ...found };
        return found;
      } finally {
        detecting = undefined;
      }
    })();
    return detecting;
  }
  const getSources = async (force = false): Promise<SourceRow[]> => (await detect(force)).rows;

  /**
   * Used and total bytes of a mounted drive, cached for the sources TTL. statfs on a hung SMB
   * mount can block forever: it is capped at 2 s, and a call still pending is reused, never
   * piled up (each one holds a libuv thread).
   */
  const sizes = new Map<string, { at: number; size: { used: number; total: number } | null }>();
  const sizePending = new Map<string, Promise<{ used: number; total: number } | null>>();
  async function sizeOf(
    id: string,
    mount: string,
  ): Promise<{ used: number; total: number } | null> {
    const hit = sizes.get(id);
    if (hit && Date.now() - hit.at < ttl) return hit.size;
    let call = sizePending.get(id);
    if (!call) {
      call = sys.statfs(mount).then(
        (f) => ({
          total: Number(f.blocks * f.bsize),
          used: Number((f.blocks - f.bavail) * f.bsize),
        }),
        () => null, // a drive that cannot be stat'ed just shows no size
      );
      sizePending.set(id, call);
      void call.finally(() => sizePending.delete(id));
    }
    const size = await withTimeout(call, statfsTimeoutMs, () => null);
    sizes.set(id, { at: Date.now(), size });
    return size;
  }

  async function sourceViews(rows: SourceRow[]): Promise<Json[]> {
    const agg = await pool.query<{ source_id: string; sent: string; review: string }>(
      `SELECT source_id::text AS source_id,
              count(*) FILTER (WHERE state = 'SENT')::text AS sent,
              count(*) FILTER (WHERE state = 'REVIEW')::text AS review
       FROM harvest_items GROUP BY source_id`,
    );
    const last = await pool.query<{ source_id: string; at: Date }>(
      `SELECT source_id::text AS source_id, max(last_harvested_at) AS at
       FROM harvest_folders GROUP BY source_id`,
    );
    const a = new Map(agg.rows.map((r) => [r.source_id, r]));
    const l = new Map(last.rows.map((r) => [r.source_id, r.at]));
    return Promise.all(
      rows.map(async (s) => {
        const size = s.mounted && s.lastMountPath ? await sizeOf(s.id, s.lastMountPath) : null;
        return {
          ...s,
          sentCount: Number(a.get(s.id)?.sent ?? 0),
          reviewCount: Number(a.get(s.id)?.review ?? 0),
          lastHarvestedAt: l.get(s.id) ?? null,
          usedBytes: size?.used ?? null,
          totalBytes: size?.total ?? null,
        };
      }),
    );
  }

  // ---- runs ---------------------------------------------------------------------------
  async function runViews(where: string, params: unknown[], tail = ''): Promise<Json[]> {
    const runs = await pool.query<Json & { id: string }>(
      `SELECT id::text AS id, dry_run, state, paused_reason, paused_since, created_at,
              started_at, finished_at, error
       FROM harvest_runs WHERE ${where} ${tail}`,
      params,
    );
    if (runs.rows.length === 0) return [];
    const folders = await pool.query<Json & { run_id: string }>(
      `SELECT rf.run_id::text AS run_id, rf.folder_id::text AS "folderId",
              f.source_id::text AS "sourceId", s.label, f.rel_path AS "relPath", rf.state,
              rf.dirs::text, rf.photos::text, rf.maybes::text, rf.junk, rf.errors::text,
              rf.sent::text, rf.known::text, rf.dups::text, rf.bytes_sent::text AS "bytesSent"
       FROM harvest_run_folders rf JOIN harvest_folders f ON f.id = rf.folder_id
       JOIN harvest_sources s ON s.id = f.source_id
       WHERE rf.run_id = ANY($1::bigint[]) ORDER BY rf.run_id, rf.position`,
      [runs.rows.map((r) => r.id)],
    );
    return runs.rows.map((r) => ({
      ...r,
      folders: folders.rows
        .filter((f) => f.run_id === r.id)
        .map((f) => {
          const rest: Json = { ...f };
          delete rest['run_id'];
          return rest;
        }),
    }));
  }

  async function gateStatus(): Promise<Json> {
    const f = await sys.statfs(settings.dataDir);
    const used = f.blocks - f.bavail;
    const { backlog, pendingBytes } = await measureBacklog(pool);
    return {
      diskPct: f.blocks > 0n ? Number((used * 1000n) / f.blocks) / 10 : 0,
      diskLimit: settings.maxDiskPct,
      backlog,
      backlogLimit: settings.maxPendingFiles,
      pendingBytes,
      pendingBytesLimit: settings.maxPendingBytes,
    };
  }

  async function state(): Promise<Json> {
    const db = await pool.query('SELECT 1').then(
      () => true,
      () => false,
    );
    const temporal = await d.temporalHealthy().catch(() => false);
    if (!db) {
      return {
        stack: { db, temporal },
        sources: [],
        activeRun: null,
        queue: [],
        reviewCount: 0,
        gate: null,
        unidentified: [],
      };
    }
    const found = await detect();
    const sources = await sourceViews(found.rows);
    const [activeRun] = await runViews(`state IN ${ACTIVE_STATES_SQL}`, [], 'ORDER BY id LIMIT 1');
    const queue = await runViews("state = 'queued'", [], 'ORDER BY id');
    const review = await pool.query<{ n: string }>(
      "SELECT count(*)::text AS n FROM harvest_items WHERE state = 'REVIEW'",
    );
    return {
      stack: { db, temporal },
      sources,
      activeRun: activeRun ?? null,
      queue,
      reviewCount: Number(review.rows[0]!.n),
      gate: activeRun ? await gateStatus() : null,
      unidentified: found.unidentified,
    };
  }

  async function requireRun(id: string): Promise<{ id: string; state: string }> {
    const { rows } = ID_RE.test(id)
      ? await pool.query<{ id: string; state: string }>(
          'SELECT id::text AS id, state FROM harvest_runs WHERE id = $1',
          [id],
        )
      : { rows: [] };
    if (!rows[0]) throw new HttpError(404, `no such run: ${id}`);
    return rows[0];
  }

  const viaTemporal = async <T>(what: string, work: () => Promise<T>): Promise<T> => {
    try {
      return await work();
    } catch (e) {
      throw new HttpError(502, `${what} failed: ${errText(e)}`);
    }
  };

  async function postRuns(body: unknown): Promise<Json> {
    const b = parse(runsBody, body);
    const folders = [...new Map(b.folders.map((f) => [`${f.sourceId}\0${f.relPath}`, f])).values()];
    const ids = [...new Set(folders.map((f) => f.sourceId))];
    const known = await pool.query('SELECT 1 FROM harvest_sources WHERE id = ANY($1::bigint[])', [
      ids,
    ]);
    if (known.rowCount !== ids.length) throw new HttpError(400, 'unknown source');
    for (const f of folders) {
      // Pruned or symlinked components are refused; an unmounted source can only be checked by name.
      const mount = await mountOf(f.sourceId);
      await checkFolder(mount, f.relPath).catch((e: unknown) => {
        throw e instanceof HttpError && e.status === 404
          ? new HttpError(400, `invalid folder: ${f.relPath}`)
          : e;
      });
    }
    const runId = await createRun(pool, folders, b.dryRun);
    const startedId = await viaTemporal('starting the run (it stays queued)', () =>
      startNextQueued(pool, control),
    );
    // `started`: its workflow was started; the row says queued until that workflow moves it on.
    return { runId, state: (await requireRun(runId)).state, started: startedId === runId };
  }

  async function runAction(id: string, action: string): Promise<Json> {
    const run = await requireRun(id);
    if (['done', 'cancelled', 'failed'].includes(run.state)) {
      throw new HttpError(409, `run ${id} is already ${run.state}`);
    }
    if (run.state === 'queued' && action !== 'cancel') {
      throw new HttpError(409, `run ${id} has not started`);
    }
    if (action === 'cancel') {
      if (run.state === 'queued') {
        // Not yet running (or just offered to Temporal): remove it from the queue.
        await finishRunRows(pool, id, 'cancelled');
        await control.cancel(id).catch(() => {});
      } else {
        const outcome = await viaTemporal('cancel', () => control.cancel(id));
        if (outcome === 'gone') {
          // No workflow left to finish the row (it ended without doing so): finish it here, so
          // the queue is not stuck behind it.
          await finishRunRows(pool, id, 'cancelled');
          await viaTemporal('starting the next run', () => startNextQueued(pool, control));
        }
      }
    } else {
      await viaTemporal(action, () => control.signal(id, action as 'pause' | 'resume'));
    }
    return { ok: true };
  }

  async function runDetail(id: string): Promise<Json> {
    await requireRun(id);
    const [run] = await runViews('id = $1', [id]);
    const errors = await pool.query(
      `SELECT i.id::text AS id, s.label AS source, i.rel_path AS "relPath", i.last_error AS error
       FROM harvest_items i JOIN harvest_sources s ON s.id = i.source_id
       WHERE i.state = 'FAILED' AND i.last_seen_run = $1 ORDER BY i.id LIMIT 200`,
      [id],
    );
    return { run: run!, errors: errors.rows };
  }

  /** The source's current mount from the TTL-cached detection (no per-request mount/diskutil). */
  async function mountOf(sourceId: string): Promise<string | null> {
    if (!ID_RE.test(sourceId)) throw new HttpError(404, `no such source: ${sourceId}`);
    let row = (await getSources()).find((r) => r.id === sourceId);
    row ??= (await getSources(true)).find((r) => r.id === sourceId);
    if (!row) throw new HttpError(404, `no such source: ${sourceId}`);
    return row.mounted ? row.lastMountPath : null;
  }

  /**
   * Walks `rel` below `mount`: every component must be kept by the pruning rules (a hidden or
   * pruned ancestor like .Trashes is refused) and must not be a symlink (it could lead out of
   * the source). Returns the folder's directory context, or 'missing' if a component does not
   * exist.
   */
  async function checkFolder(mount: string | null, rel: string): Promise<DirContext | 'missing'> {
    let ctx = ROOT_CONTEXT;
    let cur = mount;
    for (const part of rel === '' ? [] : rel.split('/')) {
      if (!keepDir(ctx, part)) throw new HttpError(404, `no such folder: ${rel}`);
      ctx = childContext(ctx, part, []);
      if (cur === null) continue;
      cur = join(cur, part);
      const st = await timeout(lstat(cur), treeTimeoutMs, 'checking the folder').catch(
        (e: unknown) => {
          if (isMissing(e)) return null;
          throw e;
        },
      );
      if (st === null) return 'missing';
      if (st.isSymbolicLink()) throw new HttpError(404, `no such folder: ${rel}`);
    }
    return ctx;
  }

  // ---- tree ---------------------------------------------------------------------------
  async function tree(sourceId: string, rawPath: string): Promise<Json> {
    const path = normaliseRelPath(rawPath);
    const mount = await mountOf(sourceId);
    if (mount === null) throw new HttpError(409, 'source is not mounted');
    const ctx = await checkFolder(mount, path);
    if (ctx === 'missing') throw new HttpError(404, `no such folder: ${path}`);
    let entries;
    try {
      entries = await timeout(listDir(join(mount, path)), treeTimeoutMs, 'listing the folder');
    } catch (e) {
      if (isMissing(e)) throw new HttpError(404, `no such folder: ${path}`);
      if (isDenied(e)) throw new HttpError(403, PERMISSION_HINT);
      throw e;
    }
    const names = entries
      .filter((e) => e.isDirectory() && keepDir(ctx, e.name))
      .map((e) => e.name.normalize('NFC'))
      .sort((a, b) => {
        const x = a.toLowerCase();
        const y = b.toLowerCase();
        return x < y ? -1 : x > y ? 1 : a < b ? -1 : a > b ? 1 : 0;
      });
    const child = (n: string): string => (path === '' ? n : `${path}/${n}`);
    const parts = path === '' ? [] : path.split('/');
    const ancestors = ['', ...parts.map((_, i) => parts.slice(0, i + 1).join('/'))];
    const hist = await pool.query<{ rel_path: string; at: Date | null; counts: unknown }>(
      `SELECT rel_path, last_harvested_at AS at, last_counts AS counts FROM harvest_folders
       WHERE source_id = $1 AND rel_path = ANY($2::text[])`,
      [sourceId, [...names.map(child), ...ancestors]],
    );
    const byPath = new Map(hist.rows.map((r) => [r.rel_path, r]));
    const covering = ancestors.reverse().find((a) => byPath.get(a)?.at);
    return {
      path,
      entries: names.map((name) => {
        const h = byPath.get(child(name));
        return {
          name,
          relPath: child(name),
          ...(h ? { lastHarvestedAt: h.at, lastCounts: h.counts } : {}),
        };
      }),
      coveredBy:
        covering === undefined
          ? null
          : { relPath: covering, lastHarvestedAt: byPath.get(covering)!.at },
    };
  }

  // ---- review -------------------------------------------------------------------------
  async function reviewGet(q: URLSearchParams): Promise<Json> {
    const sid = q.get('sourceId');
    if (sid === null) {
      const { rows } = await pool.query<{
        source_id: string;
        label: string;
        path: string;
        n: string;
        bytes: string;
      }>(
        `SELECT i.source_id::text AS source_id, s.label, ${parentSql('i.rel_path')} AS path,
                count(*)::text AS n, sum(i.size_bytes)::text AS bytes
         FROM harvest_items i JOIN harvest_sources s ON s.id = i.source_id
         WHERE i.state = 'REVIEW' GROUP BY 1, 2, 3 ORDER BY s.label, 3`,
      );
      return {
        groups: rows.map((r) => ({
          sourceId: r.source_id,
          label: r.label,
          path: r.path,
          count: Number(r.n),
          bytes: Number(r.bytes),
        })),
      };
    }
    const sourceId = parse(idSchema, sid);
    const path = normaliseRelPath(q.get('path') ?? '');
    const { rows } = await pool.query<{
      id: string;
      rel_path: string;
      size_bytes: string;
      mtime_ns: string;
    }>(
      `SELECT id::text AS id, rel_path, size_bytes::text, mtime_ns::text FROM harvest_items
       WHERE state = 'REVIEW' AND source_id = $1 AND ${parentSql('rel_path')} = $2 ORDER BY rel_path, id`,
      [sourceId, path],
    );
    const label =
      (
        await pool.query<{ label: string }>('SELECT label FROM harvest_sources WHERE id = $1', [
          sourceId,
        ])
      ).rows[0]?.label ?? `source ${String(sourceId)}`;
    return {
      label,
      problem: rows.length === 0 ? null : await sourceProblem(String(sourceId), label),
      files: rows.map((r) => ({
        id: r.id,
        name: r.rel_path.slice(r.rel_path.lastIndexOf('/') + 1),
        relPath: r.rel_path,
        sizeBytes: Number(r.size_bytes),
        mtime: new Date(Number(BigInt(r.mtime_ns) / 1_000_000n)).toISOString(),
      })),
    };
  }

  /**
   * Why the files of a source cannot be shown (their thumbnails would be grey): not connected,
   * or macOS refuses to list its top folder. Null when it can be read (or another error, which
   * the thumbnails then show as before).
   */
  async function sourceProblem(
    sourceId: string,
    label: string,
  ): Promise<{ tile: string; detail: string } | null> {
    const mount = await mountOf(sourceId);
    if (mount === null)
      return { tile: `${label} is not connected`, detail: `${label} is not connected.` };
    try {
      await timeout(listDir(mount), treeTimeoutMs, 'listing the drive');
      return null;
    } catch (e) {
      if (!isDenied(e)) return null;
      return { tile: `can't read ${label}: permission denied`, detail: PERMISSION_HINT };
    }
  }

  async function reviewPost(body: unknown): Promise<Json> {
    const b = parse(reviewBody, body);
    const to = b.action === 'send' ? 'PLANNED' : 'REJECTED';
    const sql = `UPDATE harvest_items SET state = '${to}' WHERE state = 'REVIEW'`;
    const ret = 'RETURNING id::text AS id, source_id::text AS source_id, rel_path';
    type Approved = { id: string; source_id: string; rel_path: string };
    const { rows } =
      'itemIds' in b
        ? await pool.query<Approved>(`${sql} AND id = ANY($1::bigint[]) ${ret}`, [b.itemIds])
        : await pool.query<Approved>(
            `${sql} AND source_id = $1 AND ${parentSql('rel_path')} = $2 ${ret}`,
            [b.sourceId, b.path],
          );
    if (b.action === 'skip' || rows.length === 0) return { changed: rows.length };
    const ids = rows.map((r) => r.id);
    // Approved. A live (not dry) run whose folders cover every approved item sends them when
    // signalled (a send-only one records them first: it sends only its own items); otherwise a
    // send-only run over the items' folders (no scan) is queued with exactly these items.
    const active = await pool.query<{ id: string; dry_run: boolean; send_only: boolean }>(
      `SELECT id::text AS id, dry_run, send_only FROM harvest_runs
       WHERE state IN ${ACTIVE_STATES_SQL} ORDER BY id LIMIT 1`,
    );
    const act = active.rows[0];
    if (act && !act.dry_run) {
      const covers = await pool.query<{ source_id: string; rel_path: string }>(
        `SELECT f.source_id::text AS source_id, f.rel_path FROM harvest_run_folders rf
         JOIN harvest_folders f ON f.id = rf.folder_id WHERE rf.run_id = $1`,
        [act.id],
      );
      const covered = rows.every((r) =>
        covers.rows.some(
          (c) =>
            c.source_id === r.source_id &&
            (c.rel_path === '' || r.rel_path.startsWith(`${c.rel_path}/`)),
        ),
      );
      if (covered) {
        if (act.send_only) await addRunItems(pool, act.id, ids);
        await viaTemporal('signalling the run', () => control.signal(act.id, 'itemsApproved'));
        return { changed: rows.length, runId: act.id };
      }
    }
    const folders = new Map<string, { sourceId: string; relPath: string }>();
    for (const r of rows) {
      const relPath = parentDir(r.rel_path);
      folders.set(`${r.source_id}\0${relPath}`, { sourceId: r.source_id, relPath });
    }
    const runId = await createRun(pool, [...folders.values()], { sendOnly: true, items: ids });
    await viaTemporal('starting the run (it stays queued)', () => startNextQueued(pool, control));
    return { changed: rows.length, runId };
  }

  // ---- where --------------------------------------------------------------------------
  async function where(q: string): Promise<Json> {
    const term = q.trim();
    if (term === '') throw new HttpError(400, 'q is required');
    const isSha = /^[0-9a-f]{64}$/i.test(term);
    const cond = isSha
      ? 'i.sha256 = $1 OR f.sha256 = $1'
      : `lower(substring(i.rel_path from '[^/]*$')) = lower($1)
         OR lower(substring(i.dropzone_path from '[^/]*$')) = lower($1)
         OR lower(f.original_name) = lower($1)`;
    const { rows } = await pool.query(
      `SELECT i.id::text AS id, i.source_id::text AS "sourceId", s.label AS source,
              i.rel_path AS "relPath", i.state, i.kind, i.size_bytes::float8 AS "sizeBytes",
              coalesce(i.sha256, f.sha256) AS sha256, i.dropzone_path AS "dropzonePath",
              i.sent_at AS "sentAt", to_timestamp(i.mtime_ns / 1e9) AS mtime
       FROM harvest_items i JOIN harvest_sources s ON s.id = i.source_id
       LEFT JOIN files f ON f.id = i.file_id
       WHERE ${cond} ORDER BY s.label, i.rel_path, i.id LIMIT 200`,
      [isSha ? term.toLowerCase() : term],
    );
    return { items: rows };
  }

  // ---- thumbnails ---------------------------------------------------------------------
  const inflight = new Map<string, Promise<Buffer | null>>();
  let evicting = false;
  const scheduleEvict = (): void => {
    if (evicting) return;
    evicting = true;
    evictThumbs(thumbDir, settings.thumbCacheMb * 1024 * 1024, cacheFs)
      .catch((e: unknown) => logger.warn({ err: e }, 'harvest: thumbnail eviction failed'))
      .finally(() => {
        evicting = false;
      });
  };

  async function thumbBytes(id: string): Promise<Buffer | null> {
    // A cached thumbnail is served first: no database or drive detection for repeat views.
    const out = join(thumbDir, `${id}.jpg`);
    const cached = await readFile(out).catch(() => null);
    if (cached) return cached;
    const { rows } = await pool.query<{ source_id: string; rel_path: string; raw: Buffer | null }>(
      'SELECT source_id::text AS source_id, rel_path, rel_path_raw AS raw FROM harvest_items WHERE id = $1',
      [id],
    );
    const it = rows[0];
    if (!it) throw new HttpError(404, `no such item: ${id}`);
    const mount = await mountOf(it.source_id);
    if (mount === null) return null;
    try {
      await makeThumb(sourcePath(mount, it.rel_path, it.raw), out, cacheFs);
      const bytes = await readFile(out);
      scheduleEvict();
      return bytes;
    } catch (e) {
      logger.debug({ err: e, itemId: id }, 'harvest: no thumbnail');
      return null;
    }
  }

  async function thumb(id: string, res: http.ServerResponse): Promise<void> {
    if (!ID_RE.test(id)) throw new HttpError(404, 'no such item');
    let p = inflight.get(id);
    if (!p) {
      p = thumbBytes(id).finally(() => inflight.delete(id));
      inflight.set(id, p);
    }
    const jpeg = await p;
    res.writeHead(200, {
      'Content-Type': 'image/jpeg',
      'Cache-Control': jpeg ? 'private, max-age=3600' : 'no-store',
    });
    res.end(jpeg ?? (await placeholderJpeg()));
  }

  // ---- static files -------------------------------------------------------------------
  async function serveStatic(
    pathname: string,
    head: boolean,
    res: http.ServerResponse,
  ): Promise<void> {
    let decoded: string;
    try {
      decoded = decodeURIComponent(pathname);
    } catch {
      throw new HttpError(400, 'bad URL');
    }
    if (decoded.includes('\0') || decoded.includes('\\') || decoded.split('/').includes('..')) {
      throw new HttpError(403, 'forbidden');
    }
    const rel = decoded === '/' ? 'index.html' : decoded.replace(/^\/+/, '');
    let file: string;
    try {
      const root = await realpath(d.staticDir);
      file = await realpath(join(root, rel));
      if (!file.startsWith(root + sep)) throw new HttpError(403, 'forbidden');
      if (!(await stat(file)).isFile()) throw new HttpError(404, 'not found');
    } catch (e) {
      if (e instanceof HttpError) throw e;
      throw new HttpError(404, 'not found');
    }
    const body = await readFile(file);
    res.writeHead(200, {
      'Content-Type': MIME[extname(file).toLowerCase()] ?? 'application/octet-stream',
      'Content-Length': body.length,
      'Cache-Control': 'no-cache',
    });
    res.end(head ? undefined : body);
  }

  // ---- routing ------------------------------------------------------------------------
  async function route(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const port = (req.socket.localPort ?? (server.address() as AddressInfo | null)?.port) as number;
    const host = (req.headers.host ?? '').toLowerCase();
    if (host !== `127.0.0.1:${port}` && host !== `localhost:${port}`) {
      throw new HttpError(403, 'forbidden: unexpected Host');
    }
    const method = req.method ?? 'GET';
    const url = new URL(req.url ?? '/', 'http://localhost');
    const path = url.pathname;
    const isApi = path.startsWith('/api/');

    if (method === 'POST') {
      if (req.headers['x-pixel-magic'] !== '1')
        throw new HttpError(403, 'forbidden: missing X-Pixel-Magic');
      const origin = req.headers.origin;
      if (
        origin !== undefined &&
        origin !== `http://127.0.0.1:${port}` &&
        origin !== `http://localhost:${port}`
      ) {
        throw new HttpError(403, 'forbidden: unexpected Origin');
      }
    }
    if (method !== 'GET' && method !== 'POST' && !(method === 'HEAD' && !isApi)) {
      throw new HttpError(405, 'method not allowed');
    }
    if (!isApi) {
      if (method === 'POST') throw new HttpError(404, 'not found');
      return serveStatic(path, method === 'HEAD', res);
    }

    const body = method === 'POST' ? await readBody(req) : null;
    const q = url.searchParams;
    let m: RegExpMatchArray | null;
    const ok = (b: Json): void => send(res, 200, b);

    if (method === 'GET') {
      if (path === '/api/state') return ok(await state());
      if ((m = path.match(/^\/api\/sources\/([^/]+)\/tree$/)))
        return ok(await tree(m[1]!, q.get('path') ?? ''));
      if (path === '/api/runs') {
        const raw = q.get('limit') ?? '50';
        if (!/^\d{1,4}$/.test(raw) || Number(raw) < 1) throw new HttpError(400, 'invalid limit');
        return ok({ runs: await runViews('true', [], `ORDER BY id DESC LIMIT ${Number(raw)}`) });
      }
      if ((m = path.match(/^\/api\/runs\/([^/]+)$/))) return ok(await runDetail(m[1]!));
      if (path === '/api/review') return ok(await reviewGet(q));
      if (path === '/api/where') return ok(await where(q.get('q') ?? ''));
      if ((m = path.match(/^\/api\/thumb\/([^/]+)$/))) return thumb(m[1]!, res);
    } else {
      if (path === '/api/sources/refresh') {
        const found = await detect(true);
        return ok({ sources: await sourceViews(found.rows), unidentified: found.unidentified });
      }
      if (path === '/api/runs') return ok(await postRuns(body));
      if ((m = path.match(/^\/api\/runs\/([^/]+)\/(pause|resume|cancel)$/))) {
        return ok(await runAction(m[1]!, m[2]!));
      }
      if (path === '/api/review') return ok(await reviewPost(body));
    }
    throw new HttpError(404, 'not found');
  }

  const server: http.Server = http.createServer((req, res) => {
    route(req, res).catch((e: unknown) => {
      if (res.headersSent) return void res.destroy();
      if (e instanceof HttpError) return send(res, e.status, { error: e.message });
      logger.error({ err: e, url: req.url }, 'harvest: request failed');
      send(res, 500, { error: e instanceof Error ? e.message : 'internal error' });
    });
  });
  return server;
}
