// The Picks tab's API (similar-picks spec §8): which look-alike set to teach next, storing the
// user's pick against the model's hidden one, the scoreboard, the describe control and large
// previews of vault files. server.ts runs its Host/Origin/X-Pixel-Magic checks first.
import { createHmac, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type http from 'node:http';
import { join } from 'node:path';
import type pg from 'pg';
import { z } from 'zod';
import { rawPreview } from '../decode.js';
import { RAW_MIME } from '../formats.js';
import { vaultRel } from '../fsops.js';
import { logger } from '../logger.js';
import type { AiControl } from './aiControl.js';
import type { HarvestSettings } from './config.js';
import { HttpError, parse, send, type Json } from './http.js';
import { GuardedFs } from './paths.js';
import { evictThumbs, placeholderJpeg, readSource, renderJpeg, writeCacheFile } from './thumbs.js';

export const DAILY_GOAL = 10;
/** Share of sets drawn uniformly at random (`random_draw`), the honest sample for the score. */
const RANDOM_SHARE = 0.2;
const TRAIN_EVERY = 10;
const HEADLINE_PICKS = 50;
const DISAGREEMENTS = 20;
const TOKEN_TTL_MS = 24 * 3600_000;
/** A token "issued" further than this in the future is refused too (clock skew allowance). */
const TOKEN_SKEW_MS = 5 * 60_000;
const PREVIEW_WIDTHS = new Set(['400', '1600']);
export const REASONS = ['sharper', 'eyes open', 'expression', 'framing', 'exposure'] as const;

const SHA_RE = /^[0-9a-f]{64}$/;
const shaSchema = z.string().regex(SHA_RE, 'expected a lowercase sha256');

/** What a token carries: the set as shown and the model's hidden pick. */
export interface PickToken {
  setId: string;
  shown: string[];
  modelVersion: number | null;
  modelPick: string | null;
  randomDraw: boolean;
  /** Issued at, ms since the epoch. */
  iat: number;
}
const tokenSchema = z.object({
  setId: z.string().regex(/^\d{1,19}$/),
  shown: z.array(shaSchema).min(2).max(1000),
  modelVersion: z.number().int().nullable(),
  modelPick: shaSchema.nullable(),
  randomDraw: z.boolean(),
  iat: z.number(),
});

const mac = (secret: Buffer, payload: string): Buffer =>
  createHmac('sha256', secret).update(payload).digest();

/** base64url(JSON) + "." + base64url(HMAC-SHA256): tamper-proof, but not secret. */
export function signToken(secret: Buffer, t: PickToken): string {
  const payload = Buffer.from(JSON.stringify(t)).toString('base64url');
  return `${payload}.${mac(secret, payload).toString('base64url')}`;
}

/** The token's payload, or null when it is malformed, forged, from another process or stale. */
export function verifyToken(secret: Buffer, token: string, now = Date.now()): PickToken | null {
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [payload, sig] = parts as [string, string];
  const want = mac(secret, payload);
  const got = Buffer.from(sig, 'base64url');
  if (got.length !== want.length || !timingSafeEqual(got, want)) return null;
  let json: unknown;
  try {
    json = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
  const r = tokenSchema.safeParse(json);
  if (!r.success) return null;
  const age = now - r.data.iat;
  return age > TOKEN_TTL_MS || age < -TOKEN_SKEW_MS ? null : r.data;
}

/**
 * The model's pick among `shas`: the top-scored one when at least two are scored and the top is
 * strictly ahead of the second; else null.
 */
function modelPickOf(shas: readonly string[], scores: ReadonlyMap<string, number>): string | null {
  const ranked = shas
    .filter((s) => scores.has(s))
    .map((s) => ({ s, score: scores.get(s)! }))
    .sort((a, b) => b.score - a.score);
  const [top, second] = ranked;
  return top && second && top.score > second.score ? top.s : null;
}

/** Confidence of a set: the gap between its top two scores; −∞ with fewer than two scored. */
function confidence(shas: readonly string[], scores: ReadonlyMap<string, number>): number {
  const s = shas
    .map((x) => scores.get(x))
    .filter((x) => x !== undefined)
    .sort((a, b) => b - a);
  return s.length < 2 ? -Infinity : s[0]! - s[1]!;
}

const sameList = (a: readonly string[], b: readonly string[]): boolean => {
  if (a.length !== b.length) return false;
  const [x, y] = [[...a].sort(), [...b].sort()];
  return x.every((v, i) => v === y[i]);
};

const bySetId = (a: string, b: string): number => {
  const [x, y] = [BigInt(a), BigInt(b)];
  return x < y ? -1 : x > y ? 1 : 0;
};

const pickBody = z
  .object({
    token: z.string().max(200_000),
    chosen: shaSchema.optional(),
    noClearBest: z.literal(true).optional(),
    reasons: z.array(z.enum(REASONS)).max(REASONS.length).default([]),
  })
  .refine((b) => (b.chosen === undefined) !== (b.noClearBest === undefined), {
    message: 'give exactly one of chosen and noClearBest',
  });

export interface PicksDeps {
  pool: pg.Pool;
  settings: HarvestSettings;
  ai: AiControl;
  /** HMAC key of the next-set tokens: random per process, so a restart invalidates them. */
  secret: Buffer;
  /** Random draws and shuffles (tests make them deterministic). */
  rng?: () => number;
}

export type PicksRoutes = (
  method: string,
  path: string,
  query: URLSearchParams,
  body: unknown,
  res: http.ServerResponse,
) => Promise<boolean>;

export function createPicksRoutes(d: PicksDeps): PicksRoutes {
  const { pool, settings, ai, secret } = d;
  const rng = d.rng ?? Math.random;
  const cacheRoot = join(settings.dataDir, 'harvest-cache');
  const previewDir = join(cacheRoot, 'previews');
  const cacheFs = new GuardedFs([cacheRoot]);

  /** The current ranked model (as staging reads it), or null before the first one. */
  async function currentModel(): Promise<number | null> {
    const { rows } = await pool.query<{ v: number | null }>(
      'SELECT max(version) AS v FROM ranker_models WHERE promoted AND ranked_at IS NOT NULL',
    );
    return rows[0]?.v ?? null;
  }

  async function scoresOf(version: number | null, shas: string[]): Promise<Map<string, number>> {
    if (version === null || shas.length === 0) return new Map();
    const { rows } = await pool.query<{ sha256: string; score: number }>(
      'SELECT sha256, score FROM photo_scores WHERE model_version = $1 AND sha256 = ANY($2::text[])',
      [version, shas],
    );
    return new Map(rows.map((r) => [r.sha256, r.score]));
  }

  /** Picks made on the server's local calendar day. */
  async function doneToday(): Promise<number> {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date(start);
    end.setDate(end.getDate() + 1);
    const { rows } = await pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM picks WHERE decided_at >= $1 AND decided_at < $2',
      [start, end],
    );
    return rows[0]!.n;
  }

  // ---- next ---------------------------------------------------------------------------
  async function next(): Promise<Json> {
    const version = await currentModel();
    // The described (non-failed) candidates of every set: what a set shows.
    const cands = await pool.query<{ set_id: string; sha256: string; score: number | null }>(
      `SELECT sc.set_id::text AS set_id, sc.sha256, ps.score
         FROM similar_candidates sc
         LEFT JOIN photo_scores ps ON ps.sha256 = sc.sha256 AND ps.model_version = $1
        WHERE EXISTS (SELECT 1 FROM photo_features pf
                       WHERE pf.sha256 = sc.sha256 AND pf.failed IS NULL)`,
      [version],
    );
    const sets = new Map<string, string[]>();
    const scores = new Map<string, number>();
    for (const r of cands.rows) {
      const list = sets.get(r.set_id);
      if (list) list.push(r.sha256);
      else sets.set(r.set_id, [r.sha256]);
      if (r.score !== null) scores.set(r.sha256, r.score);
    }
    // The latest pick covering each set: its chosen photo (or, for no clear best, any photo it
    // showed) is a current candidate.
    const covering = await pool.query<{ set_id: string; shown: string[] }>(
      `SELECT DISTINCT ON (sc.set_id) sc.set_id::text AS set_id, p.shown_sha256 AS shown
         FROM picks p JOIN similar_candidates sc
           ON sc.sha256 = p.chosen_sha256 OR (p.no_clear_best AND sc.sha256 = ANY (p.shown_sha256))
        ORDER BY sc.set_id, p.decided_at DESC, p.id DESC`,
    );
    const lastShown = new Map(covering.rows.map((r) => [r.set_id, r.shown]));
    // Eligible: two or more described candidates, and never picked, or picked when the set
    // showed other photos (offered once more: the new pick then shows the current list).
    const eligible = [...sets]
      .filter(([id, shas]) => {
        if (shas.length < 2) return false;
        const shown = lastShown.get(id);
        return shown === undefined || !sameList(shown, shas);
      })
      .sort(([a], [b]) => bySetId(a, b));
    if (eligible.length === 0) return { set: null };

    let chosen: [string, string[]];
    const randomDraw = rng() < RANDOM_SHARE;
    if (randomDraw) {
      chosen = eligible[Math.min(eligible.length - 1, Math.floor(rng() * eligible.length))]!;
    } else {
      // Lowest confidence first; the sort above makes ties go to the lowest set id.
      chosen = eligible[0]!;
      let best = confidence(chosen[1], scores);
      for (const e of eligible) {
        const c = confidence(e[1], scores);
        if (c < best) [chosen, best] = [e, c];
      }
    }
    const [setId, shas] = chosen;
    const shown = [...shas];
    for (let i = shown.length - 1; i > 0; i--) {
      const j = Math.min(i, Math.floor(rng() * (i + 1)));
      [shown[i], shown[j]] = [shown[j]!, shown[i]!];
    }
    const token = signToken(secret, {
      setId,
      shown,
      modelVersion: version,
      modelPick: modelPickOf(shown, scores),
      randomDraw,
      iat: Date.now(),
    });
    return {
      set: { id: setId, candidates: await details(shown), randomDraw },
      token,
      today: { done: await doneToday(), goal: DAILY_GOAL },
    };
  }

  /**
   * Name, capture date and size of each candidate. The name is the content's naming file, as
   * staging chooses it: its INGESTED or COPY files in (rel_path, mtime_ns, id) order.
   */
  async function details(shas: string[]): Promise<Json[]> {
    const { rows } = await pool.query<{
      sha256: string;
      name: string | null;
      ext: string;
      captured_at: Date | null;
      size: number;
    }>(
      `SELECT c.sha256, c.ext, c.captured_at, c.size_bytes::float8 AS size,
              (SELECT f.original_name FROM files f
                WHERE f.sha256 = c.sha256 AND f.state IN ('INGESTED', 'COPY') AND f.ext IS NOT NULL
                ORDER BY f.rel_path COLLATE "C", f.mtime_ns, f.id LIMIT 1) AS name
         FROM contents c WHERE c.sha256 = ANY($1)`,
      [shas],
    );
    const by = new Map(rows.map((r) => [r.sha256, r]));
    return shas.map((s) => {
      const r = by.get(s);
      return {
        sha256: s,
        name: r?.name ?? `${s.slice(0, 8)}${r ? `.${r.ext}` : ''}`,
        capturedAt: r?.captured_at ?? null,
        sizeBytes: r?.size ?? null,
      };
    });
  }

  // ---- storing a pick -----------------------------------------------------------------
  async function startTrainLogged(why: string): Promise<void> {
    try {
      await ai.startTrain();
    } catch (err) {
      logger.warn({ err }, `picks: starting TrainWorkflow (${why}) failed`);
    }
  }

  async function postPick(body: unknown): Promise<Json> {
    const b = parse(pickBody, body);
    const t = verifyToken(secret, b.token);
    if (!t) throw new HttpError(409, 'reload the page');
    if (b.chosen !== undefined && !t.shown.includes(b.chosen))
      throw new HttpError(400, 'chosen: not one of the photos shown');
    const chosen = b.chosen ?? null;
    await pool.query(
      `INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best, random_draw,
                          model_version, model_pick_sha256, reasons)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        t.setId,
        chosen,
        t.shown,
        chosen === null,
        t.randomDraw,
        t.modelVersion,
        t.modelPick,
        [...new Set(b.reasons)],
      ],
    );
    const { rows } = await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM picks');
    if (rows[0]!.n % TRAIN_EVERY === 0) await startTrainLogged(`pick ${String(rows[0]!.n)}`);
    return {
      modelPick: t.modelPick,
      modelVersion: t.modelVersion,
      // Ungraded when the model had no pick, or the user saw no clear best.
      matched: t.modelPick === null || chosen === null ? null : chosen === t.modelPick,
    };
  }

  async function sessionEnd(): Promise<Json> {
    const { rows } = await pool.query<{ untrained: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM picks
          WHERE decided_at > coalesce((SELECT max(created_at) FROM ranker_models), '-infinity')
       ) AS untrained`,
    );
    if (!rows[0]!.untrained) return { training: 'none' };
    try {
      return { training: await ai.startTrain() };
    } catch (e) {
      throw new HttpError(
        502,
        `starting training failed: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  async function setPaused(paused: boolean): Promise<Json> {
    try {
      await ai.setDescribePaused(paused);
    } catch (e) {
      throw new HttpError(
        502,
        `${paused ? 'pausing' : 'resuming'} describing failed: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
    return { paused };
  }

  // ---- scoreboard ---------------------------------------------------------------------
  const GRADED = 'random_draw AND model_pick_sha256 IS NOT NULL AND chosen_sha256 IS NOT NULL';

  async function scoreboard(): Promise<Json> {
    const headline = await pool.query<{ matched: number; of: number }>(
      `SELECT count(*) FILTER (WHERE chosen_sha256 = model_pick_sha256)::int AS matched,
              count(*)::int AS of
         FROM (SELECT chosen_sha256, model_pick_sha256 FROM picks WHERE ${GRADED}
                ORDER BY decided_at DESC, id DESC LIMIT ${String(HEADLINE_PICKS)}) latest`,
    );
    const byVersion = await pool.query<{ version: number; matched: number; of: number }>(
      `SELECT model_version AS version,
              count(*) FILTER (WHERE chosen_sha256 = model_pick_sha256)::int AS matched,
              count(*)::int AS of
         FROM picks WHERE ${GRADED} AND model_version IS NOT NULL
        GROUP BY model_version ORDER BY model_version`,
    );
    const totals = await pool.query<{ picks: number; failed: number; backlog: number }>(
      `SELECT (SELECT count(*)::int FROM picks) AS picks,
              (SELECT count(*)::int FROM photo_features WHERE failed IS NOT NULL) AS failed,
              (SELECT count(DISTINCT sc.sha256)::int FROM similar_candidates sc
                WHERE NOT EXISTS (SELECT 1 FROM photo_features pf WHERE pf.sha256 = sc.sha256))
                AS backlog`,
    );
    const disagreements = await pool.query<{ setId: string; chosen: string; modelPick: string }>(
      `SELECT set_id::text AS "setId", chosen_sha256 AS chosen, model_pick_sha256 AS "modelPick"
         FROM picks
        WHERE chosen_sha256 IS NOT NULL AND model_pick_sha256 IS NOT NULL
          AND chosen_sha256 <> model_pick_sha256
        ORDER BY decided_at DESC, id DESC LIMIT ${String(DISAGREEMENTS)}`,
    );
    const paused = await ai.describePaused().catch((err: unknown) => {
      logger.debug({ err }, 'picks: reading the describe schedule failed');
      return null;
    });
    const t = totals.rows[0]!;
    return {
      headline: headline.rows[0]!,
      byVersion: byVersion.rows,
      totalPicks: t.picks,
      changedByLatest: await changedByLatest(),
      disagreements: disagreements.rows,
      failedPhotos: t.failed,
      describe: { paused, backlog: t.backlog },
    };
  }

  /**
   * Sets whose model pick the current model changed: of each set's latest pick graded by an older
   * model, those where the current model (scoring the same shown photos) now picks another one.
   * photo_scores keeps only the current version, so the older pick comes from the pick row.
   * Null before the first ranked model.
   */
  async function changedByLatest(): Promise<number | null> {
    const version = await currentModel();
    if (version === null) return null;
    const { rows } = await pool.query<{ shown: string[]; pick: string }>(
      `SELECT DISTINCT ON (set_id) shown_sha256 AS shown, model_pick_sha256 AS pick
         FROM picks
        WHERE model_pick_sha256 IS NOT NULL AND model_version IS NOT NULL AND model_version <> $1
        ORDER BY set_id, decided_at DESC, id DESC`,
      [version],
    );
    const scores = await scoresOf(version, [...new Set(rows.flatMap((r) => r.shown))]);
    return rows.filter((r) => {
      const now = modelPickOf(r.shown, scores);
      return now !== null && now !== r.pick;
    }).length;
  }

  // ---- previews -----------------------------------------------------------------------
  const inflight = new Map<string, Promise<Buffer | null>>();
  let evicting = false;
  const scheduleEvict = (): void => {
    if (evicting) return;
    evicting = true;
    evictThumbs(previewDir, settings.thumbCacheMb * 1024 * 1024, cacheFs)
      .catch((err: unknown) => logger.warn({ err }, 'harvest: preview eviction failed'))
      .finally(() => {
        evicting = false;
      });
  };

  async function previewBytes(sha: string, w: string): Promise<Buffer | null> {
    const out = join(previewDir, `${sha}-${w}.jpg`);
    const cached = await readFile(out).catch(() => null);
    if (cached) return cached;
    const { rows } = await pool.query<{ ext: string }>(
      'SELECT ext FROM contents WHERE sha256 = $1',
      [sha],
    );
    const ext = rows[0]?.ext;
    if (ext === undefined) throw new HttpError(404, `no such photo: ${sha}`);
    const src = join(settings.dataDir, vaultRel(sha, ext));
    try {
      const input = RAW_MIME.has(ext.toLowerCase()) ? await rawPreview(src) : await readSource(src);
      const jpeg = await renderJpeg(input, Number(w));
      await writeCacheFile(out, jpeg, cacheFs);
      scheduleEvict();
      return jpeg;
    } catch (err) {
      logger.debug({ err, sha256: sha }, 'harvest: no preview');
      return null;
    }
  }

  async function preview(sha: string, q: URLSearchParams, res: http.ServerResponse): Promise<void> {
    if (!SHA_RE.test(sha)) throw new HttpError(404, 'no such photo');
    const w = q.get('w') ?? '';
    if (!PREVIEW_WIDTHS.has(w)) throw new HttpError(400, 'w must be 400 or 1600');
    const key = `${sha}-${w}`;
    let p = inflight.get(key);
    if (!p) {
      p = previewBytes(sha, w).finally(() => inflight.delete(key));
      inflight.set(key, p);
    }
    const jpeg = await p;
    res.writeHead(200, {
      'Content-Type': 'image/jpeg',
      'Cache-Control': jpeg ? 'private, max-age=3600' : 'no-store',
    });
    res.end(jpeg ?? (await placeholderJpeg()));
  }

  // ---- routing ------------------------------------------------------------------------
  return async (method, path, query, body, res) => {
    const ok = (b: Json): true => {
      send(res, 200, b);
      return true;
    };
    let m: RegExpMatchArray | null;
    if (method === 'GET') {
      if (path === '/api/picks/next') return ok(await next());
      if (path === '/api/picks/scoreboard') return ok(await scoreboard());
      if ((m = path.match(/^\/api\/preview\/([^/]+)$/))) {
        await preview(m[1]!, query, res);
        return true;
      }
    } else if (method === 'POST') {
      if (path === '/api/picks') return ok(await postPick(body));
      if (path === '/api/picks/session-end') return ok(await sessionEnd());
      if ((m = path.match(/^\/api\/picks\/describe\/(pause|resume)$/)))
        return ok(await setPaused(m[1] === 'pause'));
    }
    return false;
  };
}
