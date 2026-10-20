// HTTP API and static files of the harvester (spec §8). Local only: 127.0.0.1, no login, so
// every request is checked against DNS rebinding (Host) and every POST against cross-site
// requests (X-Pixel-Magic, Origin). The only disk writes are thumbnails, through GuardedFs.
import { readdir, readFile, realpath, stat } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { extname, join, sep } from 'node:path';
import type pg from 'pg';
import { z } from 'zod';
import type { DataLayout } from '../fsops.js';
import { logger } from '../logger.js';
import type { RunControl } from './activities.js';
import { childContext, keepDir, ROOT_CONTEXT } from './classify.js';
import type { HarvestSettings } from './config.js';
import { GuardedFs, sourcePath } from './paths.js';
import { createRun, finishRunRows, startNextQueued } from './runs.js';
import {
  detectSources,
  registerSources,
  resolveMount,
  type DetectOptions,
  type SourceRow,
  type SystemProbe,
} from './sources.js';
import { evictThumbs, makeThumb, placeholderJpeg } from './thumbs.js';

export interface HarvestServerDeps {
  pool: pg.Pool;
  settings: HarvestSettings;
  sys: SystemProbe;
  layout: DataLayout;
  control: RunControl;
  /** Directory of the static page (`src/harvest/ui`). */
  staticDir: string;
  /** A lightweight Temporal call succeeded (the host passes a real one; tests fake it). */
  temporalHealthy: () => Promise<boolean>;
  /** Passed to detectSources / resolveMount; tests point `volumes` at a temp directory. */
  detectOpts?: DetectOptions;
  /** How long a drive detection is reused by GET /api/state (default 10 s). */
  sourcesTtlMs?: number;
  /** Tree listing timeout (default 10 s). */
  treeTimeoutMs?: number;
  /** Directory listing seam (tests inject hangs and faults). */
  listDir?: (path: string) => Promise<{ name: string; isDirectory(): boolean }[]>;
}

const BODY_LIMIT = 1024 * 1024;
const ACTIVE_STATES = "('scanning', 'sending', 'paused')";

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
export function normaliseRelPath(raw: string): string {
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
const idSchema = z
  .union([z.string().regex(/^\d{1,15}$/), z.number().int().min(0)])
  .transform(String);

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
const PARENT_SQL = "regexp_replace(rel_path, '/?[^/]*$', '')";

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

const timeout = <T>(work: Promise<T>, ms: number, what: string): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  const limit = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new HttpError(504, `${what} timed out`)), ms);
  });
  return Promise.race([work, limit]).finally(() => clearTimeout(timer));
};

export function createHarvestServer(d: HarvestServerDeps): http.Server {
  const { pool, settings, sys, control } = d;
  const cacheRoot = join(settings.dataDir, 'harvest-cache');
  const thumbDir = join(cacheRoot, 'thumbs');
  const cacheFs = new GuardedFs([cacheRoot]);
  const listDir = d.listDir ?? ((p) => readdir(p, { withFileTypes: true }));
  const treeTimeoutMs = d.treeTimeoutMs ?? 10_000;
  const ttl = d.sourcesTtlMs ?? 10_000;

  // ---- drives -------------------------------------------------------------------------
  let sourcesCache: { at: number; rows: SourceRow[] } | undefined;
  let detecting: Promise<SourceRow[]> | undefined;
  async function getSources(force = false): Promise<SourceRow[]> {
    if (!force && sourcesCache && Date.now() - sourcesCache.at < ttl) return sourcesCache.rows;
    detecting ??= (async () => {
      try {
        const rows = await registerSources(
          pool,
          await detectSources(sys, settings.dataDir, d.detectOpts),
        );
        sourcesCache = { at: Date.now(), rows };
        return rows;
      } finally {
        detecting = undefined;
      }
    })();
    return detecting;
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
        let usedBytes: number | null = null;
        let totalBytes: number | null = null;
        if (s.mounted && s.lastMountPath) {
          try {
            const f = await sys.statfs(s.lastMountPath);
            totalBytes = Number(f.blocks * f.bsize);
            usedBytes = Number((f.blocks - f.bavail) * f.bsize);
          } catch {
            // a drive that cannot be stat'ed just shows no size
          }
        }
        return {
          ...s,
          sentCount: Number(a.get(s.id)?.sent ?? 0),
          reviewCount: Number(a.get(s.id)?.review ?? 0),
          lastHarvestedAt: l.get(s.id) ?? null,
          usedBytes,
          totalBytes,
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
    const { rows } = await pool.query<{ files: string; sent: string }>(
      `SELECT s.pending_files::text AS files,
              (SELECT count(*) FROM harvest_items
                WHERE state = 'SENT' AND file_id IS NULL
                  AND sent_at > now() - interval '1 hour')::text AS sent
       FROM scan_stats s WHERE s.id = 1`,
    );
    return {
      diskPct: f.blocks > 0n ? Number((used * 1000n) / f.blocks) / 10 : 0,
      diskLimit: settings.maxDiskPct,
      backlog: Math.max(Number(rows[0]?.files ?? 0), Number(rows[0]?.sent ?? 0)),
      backlogLimit: settings.maxPendingFiles,
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
      };
    }
    const sources = await sourceViews(await getSources());
    const [activeRun] = await runViews(`state IN ${ACTIVE_STATES}`, [], 'ORDER BY id LIMIT 1');
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
    };
  }

  async function requireRun(id: string): Promise<{ id: string; state: string }> {
    const { rows } = /^\d{1,15}$/.test(id)
      ? await pool.query<{ id: string; state: string }>(
          'SELECT id::text AS id, state FROM harvest_runs WHERE id = $1',
          [id],
        )
      : { rows: [] };
    if (!rows[0]) throw new HttpError(404, `no such run: ${id}`);
    return rows[0];
  }

  const viaTemporal = async (what: string, work: () => Promise<unknown>): Promise<void> => {
    try {
      await work();
    } catch (e) {
      throw new HttpError(502, `${what} failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  async function postRuns(body: unknown): Promise<Json> {
    const b = parse(runsBody, body);
    const seen = new Set<string>();
    const folders = b.folders.filter((f) => {
      const k = `${f.sourceId}\0${f.relPath}`;
      return seen.has(k) ? false : (seen.add(k), true);
    });
    const ids = [...new Set(folders.map((f) => f.sourceId))];
    const known = await pool.query('SELECT 1 FROM harvest_sources WHERE id = ANY($1::bigint[])', [
      ids,
    ]);
    if (known.rowCount !== ids.length) throw new HttpError(400, 'unknown source');
    const runId = await createRun(pool, folders, b.dryRun);
    await viaTemporal('starting the run (it stays queued)', () => startNextQueued(pool, control));
    return { runId, state: (await requireRun(runId)).state };
  }

  async function runAction(id: string, action: string): Promise<Json> {
    const run = await requireRun(id);
    if (['done', 'cancelled', 'failed'].includes(run.state)) {
      throw new HttpError(409, `run ${id} is already ${run.state}`);
    }
    if (action === 'cancel') {
      if (run.state === 'queued') {
        // Not yet running (or just offered to Temporal): remove it from the queue.
        await finishRunRows(pool, id, 'cancelled');
        await control.cancel(id).catch(() => {});
      } else await viaTemporal('cancel', () => control.cancel(id));
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

  // ---- tree ---------------------------------------------------------------------------
  async function tree(sourceId: string, rawPath: string): Promise<Json> {
    const path = normaliseRelPath(rawPath);
    const src = /^\d{1,15}$/.test(sourceId)
      ? await pool.query('SELECT 1 FROM harvest_sources WHERE id = $1', [sourceId])
      : { rowCount: 0 };
    if (!src.rowCount) throw new HttpError(404, `no such source: ${sourceId}`);
    const mount = await resolveMount(pool, sys, sourceId, settings.dataDir, d.detectOpts);
    if (mount === null) throw new HttpError(409, 'source is not mounted');

    let ctx = ROOT_CONTEXT;
    for (const part of path === '' ? [] : path.split('/')) ctx = childContext(ctx, part, []);
    let entries;
    try {
      entries = await timeout(listDir(join(mount, path)), treeTimeoutMs, 'listing the folder');
    } catch (e) {
      const code = (e as NodeJS.ErrnoException).code;
      if (code === 'ENOENT' || code === 'ENOTDIR')
        throw new HttpError(404, `no such folder: ${path}`);
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
    const ancestors: string[] = [''];
    path
      .split('/')
      .filter(Boolean)
      .reduce((acc, p) => {
        const next = acc === '' ? p : `${acc}/${p}`;
        ancestors.push(next);
        return next;
      }, '');
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
        `SELECT i.source_id::text AS source_id, s.label, ${PARENT_SQL.replace('rel_path', 'i.rel_path')} AS path,
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
       WHERE state = 'REVIEW' AND source_id = $1 AND ${PARENT_SQL} = $2 ORDER BY rel_path, id`,
      [sourceId, path],
    );
    return {
      files: rows.map((r) => ({
        id: r.id,
        name: r.rel_path.slice(r.rel_path.lastIndexOf('/') + 1),
        relPath: r.rel_path,
        sizeBytes: Number(r.size_bytes),
        mtime: new Date(Number(BigInt(r.mtime_ns) / 1_000_000n)).toISOString(),
      })),
    };
  }

  async function reviewPost(body: unknown): Promise<Json> {
    const b = parse(reviewBody, body);
    const to = b.action === 'send' ? 'PLANNED' : 'REJECTED';
    const sql = `UPDATE harvest_items SET state = '${to}' WHERE state = 'REVIEW'`;
    const { rows } =
      'itemIds' in b
        ? await pool.query<{ source_id: string; rel_path: string }>(
            `${sql} AND id = ANY($1::bigint[]) RETURNING source_id::text AS source_id, rel_path`,
            [b.itemIds],
          )
        : await pool.query<{ source_id: string; rel_path: string }>(
            `${sql} AND source_id = $1 AND ${PARENT_SQL} = $2 RETURNING source_id::text AS source_id, rel_path`,
            [b.sourceId, b.path],
          );
    if (b.action === 'skip' || rows.length === 0) return { changed: rows.length };
    // Approved: the active run sends them (signal), else a send-only run over their folders.
    const active = await pool.query<{ id: string }>(
      `SELECT id::text AS id FROM harvest_runs WHERE state IN ${ACTIVE_STATES} ORDER BY id LIMIT 1`,
    );
    if (active.rows[0]) {
      const runId = active.rows[0].id;
      await viaTemporal('signalling the run', () => control.signal(runId, 'itemsApproved'));
      return { changed: rows.length, runId };
    }
    const folders = new Map<string, { sourceId: string; relPath: string }>();
    for (const r of rows) {
      const relPath = parentDir(r.rel_path);
      folders.set(`${r.source_id}\0${relPath}`, { sourceId: r.source_id, relPath });
    }
    const runId = await createRun(pool, [...folders.values()], false);
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
    const { rows } = await pool.query<{ source_id: string; rel_path: string; raw: Buffer | null }>(
      'SELECT source_id::text AS source_id, rel_path, rel_path_raw AS raw FROM harvest_items WHERE id = $1',
      [id],
    );
    const it = rows[0];
    if (!it) throw new HttpError(404, `no such item: ${id}`);
    const mount = await resolveMount(pool, sys, it.source_id, settings.dataDir, d.detectOpts);
    if (mount === null) return null;
    const out = join(thumbDir, `${id}.jpg`);
    const cached = await readFile(out).catch(() => null);
    if (cached) return cached;
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
    if (!/^\d{1,15}$/.test(id)) throw new HttpError(404, 'no such item');
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
        return ok({ sources: await sourceViews(await getSources(true)) });
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
