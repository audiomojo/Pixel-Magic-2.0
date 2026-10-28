/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-argument -- the JSON bodies under test are untyped */
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { dirname, join } from 'node:path';
import sharp from 'sharp';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { DataLayout, vaultRel } from '../../src/fsops.js';
import type { RunControl } from '../../src/harvest/activities.js';
import type { AiControl } from '../../src/harvest/aiControl.js';
import { loadHarvestSettings } from '../../src/harvest/config.js';
import { signToken, verifyToken, type PickToken } from '../../src/harvest/picks.js';
import { temporalPipelineControl } from '../../src/harvest/pipelineControl.js';
import { createHarvestServer, type HarvestServerDeps } from '../../src/harvest/server.js';
import type { SystemProbe } from '../../src/harvest/sources.js';
import { placeholderJpeg } from '../../src/harvest/thumbs.js';
import { HEIC_SCENE0 } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
  await pgh.pool.query(
    'TRUNCATE picks, photo_features, photo_scores, ranker_models, similar_candidates RESTART IDENTITY',
  );
});

const servers: http.Server[] = [];
afterEach(async () => {
  for (const s of servers.splice(0)) {
    s.closeAllConnections();
    await new Promise((r) => s.close(r));
  }
});

interface Res {
  status: number;
  headers: http.IncomingHttpHeaders;
  body: Buffer;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  json: any;
}
interface ReqOpts {
  headers?: Record<string, string>;
  json?: unknown;
  raw?: boolean; // no default Host / X-Pixel-Magic
}

interface FakeAi extends AiControl {
  calls: string[];
  paused: boolean;
  failTrain: boolean;
}

interface Env {
  port: number;
  dataDir: string;
  ai: FakeAi;
  sync: { calls: string[][]; fail: boolean };
  req(method: string, path: string, o?: ReqOpts): Promise<Res>;
  get(path: string): Promise<Res>;
  post(path: string, body?: unknown): Promise<Res>;
}

/** The servers' token key, so tests can mint fresh tokens (each one is single-use). */
const SECRET = Buffer.alloc(32, 42);
let minted = 0;
/** Another single-use token for the same shown set (a distinct iat, so a distinct MAC). */
function reissue(token: string): string {
  const t = JSON.parse(Buffer.from(token.split('.')[0]!, 'base64url').toString()) as PickToken;
  return signToken(SECRET, { ...t, iat: t.iat - ++minted });
}

function fakeAi(): FakeAi {
  const ai: FakeAi = {
    calls: [],
    paused: false,
    failTrain: false,
    startTrain() {
      ai.calls.push('startTrain');
      return ai.failTrain ? Promise.reject(new Error('temporal down')) : Promise.resolve('started');
    },
    setDescribePaused(p) {
      ai.calls.push(p ? 'pause' : 'resume');
      ai.paused = p;
      return Promise.resolve();
    },
    describePaused: () => Promise.resolve(ai.paused),
  };
  return ai;
}

async function setup(
  o: { deps?: Partial<HarvestServerDeps>; dataDir?: string } = {},
): Promise<Env> {
  const sys: SystemProbe = {
    mountOutput: () => Promise.resolve(''),
    diskutilInfo: () => Promise.resolve({}),
    statfs: () => Promise.resolve({ blocks: 1000n, bavail: 600n, bsize: 4096n }),
  };
  const dataDir = o.dataDir ?? (await tmpDir());
  await new DataLayout(dataDir).ensure();
  const settings = loadHarvestSettings({ DATA_DIR: dataDir, DATABASE_URL: pgh.url }, dataDir);
  const control: RunControl = {
    start: () => Promise.resolve('started' as const),
    describe: () => Promise.resolve('running' as const),
    signal: () => Promise.resolve(),
    cancel: () => Promise.resolve('requested' as const),
  };
  const ai = fakeAi();
  const sync = { calls: [] as string[][], fail: false };
  const staticDir = await tmpDir();
  await writeFile(join(staticDir, 'index.html'), '<h1>hi</h1>');
  const server = createHarvestServer({
    pool: pgh.pool,
    settings,
    sys,
    control,
    ai,
    pipeline: {
      startPurgeSync(shas) {
        sync.calls.push(shas);
        return sync.fail ? Promise.reject(new Error('temporal down')) : Promise.resolve();
      },
    },
    staticDir,
    temporalHealthy: () => Promise.resolve(true),
    detectOpts: { volumes: `${dataDir}/no-volumes/` },
    picksSecret: SECRET,
    picksRng: () => 0.5, // never a random draw; a fixed shuffle
    ...o.deps,
  });
  servers.push(server);
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as AddressInfo).port;
  const req = (method: string, path: string, ro: ReqOpts = {}): Promise<Res> =>
    new Promise((resolve, reject) => {
      const headers: Record<string, string> = ro.raw
        ? { ...ro.headers }
        : {
            host: `127.0.0.1:${port}`,
            ...(method === 'POST' ? { 'x-pixel-magic': '1' } : {}),
            ...ro.headers,
          };
      let body: string | undefined;
      if (ro.json !== undefined) {
        body = JSON.stringify(ro.json);
        headers['content-type'] = 'application/json';
        headers['content-length'] = String(Buffer.byteLength(body));
      }
      const r = http.request({ host: '127.0.0.1', port, method, path, headers }, (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          const buf = Buffer.concat(chunks);
          let json: unknown = null;
          if (String(res.headers['content-type']).includes('json'))
            json = JSON.parse(buf.toString());
          resolve({ status: res.statusCode!, headers: res.headers, body: buf, json });
        });
      });
      r.on('error', reject);
      r.end(body);
    });
  return {
    port,
    dataDir,
    ai,
    sync,
    req,
    get: (p) => req('GET', p),
    post: (p, json) => req('POST', p, { json: json ?? {} }),
  };
}

// ---- fixtures -----------------------------------------------------------------------------
const sha = (n: number): string => n.toString(16).padStart(64, '0');

async function content(
  s: string,
  o: { ext?: string; size?: number; captured?: string; name?: string } = {},
): Promise<void> {
  await pgh.pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state, captured_at)
     VALUES ($1, $2, 'still', $3, 'CLUSTERED', $4) ON CONFLICT DO NOTHING`,
    [s, o.ext ?? 'jpg', o.size ?? 1000, o.captured ?? '2024-05-01T10:00:00Z'],
  );
  if (o.name !== undefined) {
    await pgh.pool.query(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
       VALUES ($1, $2, $3, 1, $4, $5, 'INGESTED')`,
      [`dir/${o.name}`, o.name, o.size ?? 1000, o.ext ?? 'jpg', s],
    );
  }
}

/** A set of candidates; `described` (default all) get a photo_features row. */
async function set(id: number, shas: string[], described: string[] = shas): Promise<void> {
  for (const s of shas) {
    await content(s);
    await pgh.pool.query('INSERT INTO similar_candidates (set_id, sha256) VALUES ($1, $2)', [
      id,
      s,
    ]);
  }
  for (const s of described) await describeOk(s);
}

async function describeOk(s: string, failed: string | null = null): Promise<void> {
  await pgh.pool.query(
    `INSERT INTO photo_features (sha256, feature_version, embedding, quality, faces, failed)
     VALUES ($1, 1, '{}', '{}', '[]', $2)`,
    [s, failed],
  );
}

async function rankedModel(scores: Record<string, number>): Promise<number> {
  const { rows } = await pgh.pool.query<{ version: number }>(
    `INSERT INTO ranker_models (feature_version, picks_used, promoted, path, ranked_at)
     VALUES (1, 0, true, 'm', now()) RETURNING version`,
  );
  const v = rows[0]!.version;
  for (const [s, score] of Object.entries(scores))
    await pgh.pool.query(
      'INSERT INTO photo_scores (sha256, model_version, score) VALUES ($1, $2, $3)',
      [s, v, score],
    );
  return v;
}

async function pickRow(o: {
  setId: number;
  chosen: string | null;
  shown: string[];
  random?: boolean;
  modelPick?: string | null;
  modelVersion?: number | null;
  decidedAt?: string;
}): Promise<void> {
  await pgh.pool.query(
    `INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best, random_draw,
                        model_version, model_pick_sha256, decided_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, coalesce($8::timestamptz, now()))`,
    [
      o.setId,
      o.chosen,
      o.shown,
      o.chosen === null,
      o.random ?? false,
      o.modelVersion ?? null,
      o.modelPick ?? null,
      o.decidedAt ?? null,
    ],
  );
}

const pickCount = async (): Promise<number> =>
  Number((await pgh.pool.query('SELECT count(*) AS n FROM picks')).rows[0].n);

const [A, B, C, D, E, F, G] = [1, 2, 3, 4, 5, 6, 7].map(sha) as [
  string,
  string,
  string,
  string,
  string,
  string,
  string,
];

// ---- next --------------------------------------------------------------------------------
describe('GET /api/picks/next', () => {
  test('offers a set with ≥ 2 described candidates, never a picked or under-described one', async () => {
    const e = await setup();
    await content(A, { name: 'IMG_1.JPG', size: 1234, captured: '2024-05-01T10:00:00Z' });
    await set(1, [A, B]);
    await set(2, [C, D], [C]); // one described candidate only
    await set(3, [E, F]);
    await pickRow({ setId: 3, chosen: E, shown: [E, F] });
    const r = await e.get('/api/picks/next');
    expect(r.status).toBe(200);
    expect(r.json.set.id).toBe('1');
    expect(r.json.set.randomDraw).toBe(false);
    expect(r.json.set.candidates.map((c: { sha256: string }) => c.sha256).sort()).toEqual([A, B]);
    const a = r.json.set.candidates.find((c: { sha256: string }) => c.sha256 === A);
    expect(a).toEqual({
      sha256: A,
      name: 'IMG_1.JPG',
      capturedAt: '2024-05-01T10:00:00.000Z',
      sizeBytes: 1234,
    });
    expect(typeof r.json.token).toBe('string');
    expect(r.json.today).toEqual({ done: 1, goal: 10 });
    // picking set 1 leaves nothing to offer
    expect((await e.post('/api/picks', { token: r.json.token, chosen: A })).status).toBe(200);
    expect((await e.get('/api/picks/next')).json).toEqual({ set: null });
  });

  test('skip leaves the listed sets out; a bad skip is a 400', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await set(2, [C, D]);
    await rankedModel({ [A]: 5, [B]: 1, [C]: 2, [D]: 1.5 }); // set 2 is the less confident
    expect((await e.get('/api/picks/next')).json.set.id).toBe('2');
    expect((await e.get('/api/picks/next?skip=2')).json.set.id).toBe('1');
    expect((await e.get('/api/picks/next?skip=2,1')).json).toEqual({ set: null });
    for (const bad of [
      'x',
      '1,,2',
      '-1',
      '1.5',
      Array.from({ length: 51 }, (_, i) => i).join(','),
    ]) {
      const r = await e.get(`/api/picks/next?skip=${bad}`);
      expect(r.status).toBe(400);
      expect(r.json.error).toBe('invalid skip');
    }
  });

  test('a failed description does not count as described', async () => {
    const e = await setup();
    await set(1, [A, B], [A]);
    await describeOk(B, 'decode error');
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a set whose candidates changed after the pick comes back once', async () => {
    const e = await setup();
    await set(3, [E, F]);
    await pickRow({ setId: 3, chosen: E, shown: [E, F] });
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
    await set(3, [G]);
    const r = await e.get('/api/picks/next');
    expect(r.json.set.id).toBe('3');
    expect(r.json.set.candidates.map((c: { sha256: string }) => c.sha256).sort()).toEqual([
      E,
      F,
      G,
    ]);
    expect((await e.post('/api/picks', { token: r.json.token, chosen: G })).status).toBe(200);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a set fully covered by a pick (no retirement) is not offered again', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await pickRow({ setId: 1, chosen: A, shown: [A, B] });
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
    // the same for a no-clear-best pick, which compares the whole shown list
    await set(2, [C, D]);
    await pickRow({ setId: 2, chosen: null, shown: [C, D] });
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  /** Retires `loser` into `keeper` (the row only; the pick API reads rows). */
  async function retire(loser: string, keeper: string): Promise<void> {
    await pgh.pool.query(
      `UPDATE contents SET state = 'DUPLICATE', duplicate_of = $2 WHERE sha256 = $1`,
      [loser, keeper],
    );
  }

  test('a set whose picked photo was retired into its keeper is not offered again', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await pickRow({ setId: 1, chosen: C, shown: [C, B] });
    await content(C);
    await retire(C, A);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a retired shown member whose keeper was also shown is not a change', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await content(C);
    await pickRow({ setId: 1, chosen: B, shown: [A, B, C] });
    await retire(C, A);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a no-clear-best pick that showed a retired photo still covers the set', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await content(C);
    await pickRow({ setId: 1, chosen: null, shown: [C, B] });
    await retire(C, A);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a genuinely new candidate still brings a set back after a retire', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await content(C);
    await pickRow({ setId: 1, chosen: C, shown: [C, B] });
    await retire(C, A);
    await set(1, [D]);
    expect((await e.get('/api/picks/next')).json.set.id).toBe('1');
  });

  const purge = async (s: string): Promise<void> => {
    await pgh.pool.query(
      `UPDATE contents SET state = 'PURGED', purged_at = now() WHERE sha256 = $1`,
      [s],
    );
  };

  test('a purged candidate is not shown, before any staging rebuild', async () => {
    const e = await setup();
    await set(1, [A, B, C]);
    await purge(C);
    const r = await e.get('/api/picks/next');
    expect(r.json.set.candidates.map((c: { sha256: string }) => c.sha256).sort()).toEqual(
      [A, B].sort(),
    );
  });

  test('a set left with fewer than two candidates by a purge is not offered', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await purge(B);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a set that only lost a purged shown member is not offered again', async () => {
    const e = await setup();
    await set(1, [A, B, C]);
    await pickRow({ setId: 1, chosen: A, shown: [A, B, C] });
    await purge(C);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('a pick whose chosen photo was purged still covers the set by the rest it showed', async () => {
    const e = await setup();
    await set(1, [A, B, C]);
    await pickRow({ setId: 1, chosen: C, shown: [A, B, C] });
    await purge(C);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
  });

  test('no clear best for the same candidates is never offered; changed candidates come back', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await pickRow({ setId: 1, chosen: null, shown: [B, A] });
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
    await set(1, [C]);
    expect((await e.get('/api/picks/next')).json.set.id).toBe('1');
  });

  test('lowest confidence first (unscored first), ties by set id; a random draw', async () => {
    const e = await setup();
    await set(1, [A, B]);
    await set(2, [C, D]);
    await set(3, [E, F]);
    await set(4, [G, sha(8)]);
    await rankedModel({ [A]: 0.9, [B]: 0.1, [C]: 0.5, [D]: 0.45, [E]: 0.2, [F]: 0.0, [G]: 0.3 });
    // set 4 has one scored candidate: confidence −∞
    const order: string[] = [];
    for (let i = 0; i < 4; i++) {
      const r = await e.get('/api/picks/next');
      order.push(r.json.set.id);
      const chosen = r.json.set.candidates[0].sha256;
      expect((await e.post('/api/picks', { token: r.json.token, chosen })).status).toBe(200);
    }
    expect(order).toEqual(['4', '2', '3', '1']);

    await pgh.pool.query('TRUNCATE picks');
    const vals = [0.1, 0.99]; // a random draw, then index floor(0.99 * 4) = 3
    let i = 0;
    const r2 = await setup({ deps: { picksRng: () => vals[i++] ?? 0.5 } });
    const r = await r2.get('/api/picks/next');
    expect(r.json.set).toMatchObject({ id: '4', randomDraw: true });
  });
});

// ---- post --------------------------------------------------------------------------------
describe('POST /api/picks', () => {
  test("stores the token's model pick and reveals it", async () => {
    const e = await setup();
    await set(1, [A, B, C]);
    const v = await rankedModel({ [A]: 0.2, [B]: 0.7, [C]: 0.1 });
    const n = (await e.get('/api/picks/next')).json;
    const r = await e.post('/api/picks', { token: n.token, chosen: B, reasons: ['sharper'] });
    expect(r.status).toBe(200);
    expect(r.json).toEqual({ modelPick: B, modelVersion: v, matched: true });
    const { rows } = await pgh.pool.query('SELECT * FROM picks');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      set_id: 1n,
      chosen_sha256: B,
      no_clear_best: false,
      random_draw: false,
      model_version: v,
      model_pick_sha256: B,
      reasons: ['sharper'],
    });
    expect([...rows[0].shown_sha256].sort()).toEqual([A, B, C]);

    const m = await e.post('/api/picks', { token: reissue(n.token), chosen: A });
    expect(m.json).toEqual({ modelPick: B, modelVersion: v, matched: false });
  });

  test('no model, or a tie at the top: no model pick, matched null', async () => {
    const e = await setup();
    await set(1, [A, B]);
    const n = (await e.get('/api/picks/next')).json;
    expect((await e.post('/api/picks', { token: n.token, chosen: A })).json).toEqual({
      modelPick: null,
      modelVersion: null,
      matched: null,
    });
    await pgh.pool.query('TRUNCATE picks');
    const v = await rankedModel({ [A]: 0.5, [B]: 0.5 });
    const n2 = (await e.get('/api/picks/next')).json;
    expect((await e.post('/api/picks', { token: n2.token, noClearBest: true })).json).toEqual({
      modelPick: null,
      modelVersion: v,
      matched: null,
    });
    const { rows } = await pgh.pool.query('SELECT no_clear_best, chosen_sha256 FROM picks');
    expect(rows).toEqual([{ no_clear_best: true, chosen_sha256: null }]);
  });

  test('a forged, tampered or other-process token → 409 and no row', async () => {
    const e = await setup();
    const other = await setup({ deps: { picksSecret: Buffer.alloc(32, 9) } });
    await set(1, [A, B]);
    const n = (await e.get('/api/picks/next')).json;
    const foreign = (await other.get('/api/picks/next')).json;
    const [payload, mac] = n.token.split('.');
    const forged = JSON.parse(Buffer.from(payload, 'base64url').toString());
    forged.shown = [A, C];
    const tampered = `${Buffer.from(JSON.stringify(forged)).toString('base64url')}.${mac}`;
    for (const token of [foreign.token, tampered, 'garbage', `${payload}.`, `${payload}.${mac}x`]) {
      const r = await e.post('/api/picks', { token, chosen: A });
      expect(r.status).toBe(409);
      expect(r.json.error).toBe('reload the page');
    }
    expect(await pickCount()).toBe(0);
  });

  test('a token is single-use: a repeat returns the first response and stores nothing', async () => {
    const e = await setup();
    await set(1, [A, B]);
    for (let i = 0; i < 8; i++) await pickRow({ setId: 9, chosen: E, shown: [E, F] });
    const v = await rankedModel({ [A]: 0.9, [B]: 0.1 });
    const { token } = (await e.get('/api/picks/next')).json;
    const first = await e.post('/api/picks', { token, chosen: B }); // 9th
    expect(first.json).toEqual({ modelPick: A, modelVersion: v, matched: false });
    // a double-click (concurrent) and a later retry, even with another answer
    const again = await Promise.all([
      e.post('/api/picks', { token, chosen: B }),
      e.post('/api/picks', { token, chosen: A }),
      e.post('/api/picks', { token, noClearBest: true }),
    ]);
    for (const r of again) {
      expect(r.status).toBe(200);
      expect(r.json).toEqual(first.json);
    }
    expect(await pickCount()).toBe(9);
    expect(e.ai.calls).toEqual([]); // the repeats did not reach the 10th pick
  });

  test('a re-spelled MAC of a used token is the same token', async () => {
    const e = await setup();
    await set(1, [A, B]);
    const { token } = (await e.get('/api/picks/next')).json;
    expect((await e.post('/api/picks', { token, chosen: A })).status).toBe(200);
    const [payload, sig] = token.split('.') as [string, string];
    // 32 bytes are 43 base64url characters: the last one's low 2 bits are padding.
    const ALPHA = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
    const last = ALPHA.indexOf(sig.at(-1)!);
    const variants = [1, 2, 3].map((x) => `${payload}.${sig.slice(0, -1)}${ALPHA[last ^ x]!}`);
    variants.push(`${payload}.${sig}=`);
    for (const t of variants) {
      expect(Buffer.from(t.split('.')[1]!, 'base64url').equals(Buffer.from(sig, 'base64url'))).toBe(
        true,
      );
      const r = await e.post('/api/picks', { token: t, chosen: B });
      expect(r.status).toBe(200);
      expect(r.json).toEqual({ modelPick: null, modelVersion: null, matched: null });
    }
    const { rows } = await pgh.pool.query('SELECT chosen_sha256 FROM picks');
    expect(rows).toEqual([{ chosen_sha256: A }]);
  });

  test('a first POST refused with 400 does not use the token up', async () => {
    const e = await setup();
    await set(1, [A, B]);
    const { token } = (await e.get('/api/picks/next')).json;
    expect((await e.post('/api/picks', { token, chosen: C })).status).toBe(400);
    expect((await e.post('/api/picks', { token, chosen: A })).json.matched).toBeNull();
    expect(await pickCount()).toBe(1);
  });

  test('tokens expire after 24 h', () => {
    const secret = Buffer.alloc(32, 7);
    const now = Date.UTC(2026, 0, 2);
    const p = { setId: '1', shown: [A, B], modelVersion: null, modelPick: null, randomDraw: false };
    const fresh = signToken(secret, { ...p, iat: now - 23 * 3600_000 });
    const stale = signToken(secret, { ...p, iat: now - 25 * 3600_000 });
    expect(verifyToken(secret, fresh, now)).toMatchObject({ setId: '1', shown: [A, B] });
    expect(verifyToken(secret, stale, now)).toBeNull();
    expect(verifyToken(Buffer.alloc(32, 8), fresh, now)).toBeNull();
  });

  test('chosen outside shown, bad bodies and unknown reasons → 400', async () => {
    const e = await setup();
    await set(1, [A, B]);
    const { token } = (await e.get('/api/picks/next')).json;
    for (const body of [
      { token, chosen: C },
      { token },
      { token, chosen: A, noClearBest: true },
      { token, chosen: A, reasons: ['prettier'] },
      { token, chosen: 'nope' },
    ]) {
      expect((await e.post('/api/picks', body)).status).toBe(400);
    }
    expect(await pickCount()).toBe(0);
  });

  test('every 10th stored pick starts training; a failing start still stores the pick', async () => {
    const e = await setup();
    await set(1, [A, B]);
    for (let i = 0; i < 8; i++) await pickRow({ setId: 9, chosen: E, shown: [E, F] });
    const { token } = (await e.get('/api/picks/next')).json;
    await e.post('/api/picks', { token, chosen: A }); // 9th
    expect(e.ai.calls).toEqual([]);
    await e.post('/api/picks', { token: reissue(token), chosen: A }); // 10th
    expect(e.ai.calls).toEqual(['startTrain']);
    for (let i = 0; i < 9; i++) await e.post('/api/picks', { token: reissue(token), chosen: A }); // 11th–19th
    expect(e.ai.calls).toEqual(['startTrain']);
    e.ai.failTrain = true;
    const r = await e.post('/api/picks', { token: reissue(token), chosen: B }); // 20th
    expect(r.status).toBe(200);
    expect(e.ai.calls).toEqual(['startTrain', 'startTrain']);
    expect(await pickCount()).toBe(20);
  });
});

// ---- session end, describe control ------------------------------------------------------
describe('training and describe control', () => {
  test('session-end trains only when a pick is newer than the newest model', async () => {
    const e = await setup();
    expect((await e.post('/api/picks/session-end')).status).toBe(200);
    expect(e.ai.calls).toEqual([]);
    await pickRow({ setId: 1, chosen: A, shown: [A, B], decidedAt: '2026-01-01T00:00:00Z' });
    await e.post('/api/picks/session-end'); // no model yet: any pick counts
    expect(e.ai.calls).toEqual(['startTrain']);
    await rankedModel({}); // created now, after the pick
    await e.post('/api/picks/session-end');
    expect(e.ai.calls).toEqual(['startTrain']);
    await pickRow({ setId: 1, chosen: A, shown: [A, B], decidedAt: '2999-01-01T00:00:00Z' });
    await e.post('/api/picks/session-end');
    expect(e.ai.calls).toEqual(['startTrain', 'startTrain']);
  });

  test('describe pause and resume', async () => {
    const e = await setup();
    expect((await e.post('/api/picks/describe/pause')).json).toEqual({ paused: true });
    expect((await e.get('/api/picks/scoreboard')).json.describe.paused).toBe(true);
    expect((await e.post('/api/picks/describe/resume')).json).toEqual({ paused: false });
    expect(e.ai.calls).toEqual(['pause', 'resume']);
    expect((await e.post('/api/picks/describe/stop')).status).toBe(404);
  });
});

// ---- scoreboard --------------------------------------------------------------------------
describe('GET /api/picks/scoreboard', () => {
  test('headline counts only random, graded picks', async () => {
    const e = await setup();
    const v = await rankedModel({ [A]: 0.9, [B]: 0.1 });
    for (let i = 0; i < 3; i++)
      await pickRow({
        setId: 1,
        chosen: A,
        shown: [A, B],
        random: true,
        modelPick: A,
        modelVersion: v,
      });
    await pickRow({
      setId: 2,
      chosen: B,
      shown: [A, B],
      random: true,
      modelPick: A,
      modelVersion: v,
    });
    for (let i = 0; i < 5; i++)
      await pickRow({ setId: 3, chosen: B, shown: [A, B], modelPick: A, modelVersion: v });
    await pickRow({ setId: 4, chosen: A, shown: [A, B], random: true }); // ungraded
    await set(5, [C, D], [C]);
    await describeOk(D, 'boom');
    await set(6, [E]); // described
    await pgh.pool.query('INSERT INTO similar_candidates VALUES (6, $1)', [F]); // not described
    const s = (await e.get('/api/picks/scoreboard')).json;
    expect(s.headline).toEqual({ matched: 3, of: 4 });
    expect(s.byVersion).toEqual([{ version: v, matched: 3, of: 4 }]);
    expect(s.totalPicks).toBe(10);
    expect(s.failedPhotos).toBe(1);
    expect(s.describe).toEqual({ paused: false, backlog: 1 });
    expect(s.disagreements).toHaveLength(6);
    expect(s.disagreements[0]).toEqual({ setId: '3', chosen: B, modelPick: A });
    expect(s.changedByLatest === null || typeof s.changedByLatest === 'number').toBe(true);
  });
});

// ---- previews ----------------------------------------------------------------------------
describe('GET /api/preview', () => {
  async function vaultFile(dataDir: string, s: string, ext: string, bytes: Buffer): Promise<void> {
    const p = join(dataDir, vaultRel(s, ext));
    await mkdir(dirname(p), { recursive: true });
    await writeFile(p, bytes);
    await content(s, { ext });
  }

  test('JPEG and HEIC previews fit the requested width and are cached', async () => {
    const e = await setup();
    const jpg = await sharp({
      create: { width: 2000, height: 1000, channels: 3, background: '#336699' },
    })
      .jpeg()
      .toBuffer();
    await vaultFile(e.dataDir, A, 'jpg', jpg);
    await vaultFile(e.dataDir, B, 'heic', await readFile(HEIC_SCENE0));
    for (const [s, w] of [
      [A, 400],
      [A, 1600],
      [B, 400],
    ] as const) {
      const r = await e.get(`/api/preview/${s}?w=${w}`);
      expect(r.status).toBe(200);
      expect(r.headers['content-type']).toBe('image/jpeg');
      const meta = await sharp(r.body).metadata();
      expect(meta.format).toBe('jpeg');
      expect(meta.width).toBeLessThanOrEqual(w);
    }
    expect(
      await sharp((await e.get(`/api/preview/${A}?w=1600`)).body)
        .metadata()
        .then((m) => m.width),
    ).toBe(1600);
    expect((await readdir(join(e.dataDir, 'harvest-cache', 'previews'))).sort()).toEqual(
      [`${A}-1600.jpg`, `${A}-400.jpg`, `${B}-400.jpg`].sort(),
    );
  });

  test('a corrupt or missing vault file → the placeholder; bad width → 400; unknown → 404', async () => {
    const e = await setup();
    await vaultFile(e.dataDir, A, 'jpg', Buffer.from('not a jpeg'));
    await content(B, { ext: 'jpg' }); // no vault file
    for (const s of [A, B]) {
      const r = await e.get(`/api/preview/${s}?w=400`);
      expect(r.status).toBe(200);
      expect(r.headers['content-type']).toBe('image/jpeg');
      expect(r.body.equals(await placeholderJpeg())).toBe(true);
    }
    expect((await e.get(`/api/preview/${A}?w=999`)).status).toBe(400);
    expect((await e.get(`/api/preview/${A}`)).status).toBe(400);
    expect((await e.get(`/api/preview/${C}?w=400`)).status).toBe(404);
    expect((await e.get('/api/preview/xyz?w=400')).status).toBe(404);
  });
});

// ---- protection --------------------------------------------------------------------------
describe('protection', () => {
  test('picks and preview routes enforce Host, Origin and X-Pixel-Magic', async () => {
    const e = await setup();
    await set(1, [A, B]);
    for (const path of ['/api/picks/next', '/api/picks/scoreboard', `/api/preview/${A}?w=400`]) {
      const r = await e.req('GET', path, { headers: { host: 'evil.example' } });
      expect(r.status).toBe(403);
    }
    const { token } = (await e.get('/api/picks/next')).json;
    for (const path of ['/api/picks', '/api/picks/session-end', '/api/picks/describe/pause']) {
      const noHeader = await e.req('POST', path, {
        raw: true,
        headers: { host: `127.0.0.1:${e.port}` },
        json: { token, chosen: A },
      });
      expect(noHeader.status).toBe(403);
      const badOrigin = await e.req('POST', path, {
        headers: { origin: 'http://evil.example' },
        json: { token, chosen: A },
      });
      expect(badOrigin.status).toBe(403);
    }
    expect(await pickCount()).toBe(0);
    expect(e.ai.calls).toEqual([]);
  });
});

// ---- purge ---------------------------------------------------------------------------------
describe('purge API', () => {
  /** A set whose copy group is linked to similar set `id`; `extra` are hidden copies. */
  async function purgeSet(id: number, shown: string[], extra: string[] = []): Promise<void> {
    await set(id, shown);
    await pgh.pool.query(`INSERT INTO groups (id, kind, similar_set_id) VALUES ($1, 'copy', $2)`, [
      id,
      id,
    ]);
    for (const s of [...shown, ...extra]) {
      if (extra.includes(s)) await content(s, { name: `${s.slice(0, 4)}.jpg` });
      await pgh.pool.query('UPDATE contents SET copy_group_id = $2 WHERE sha256 = $1', [s, id]);
    }
  }
  const state = async (s: string): Promise<string> =>
    (await pgh.pool.query('SELECT state FROM contents WHERE sha256 = $1', [s])).rows[0]
      .state as string;
  const tokenFor = async (e: Env): Promise<string> =>
    (await e.get('/api/picks/next')).json.token as string;

  test('preview counts the closure; stale and bad tokens are refused', async () => {
    const e = await setup();
    await content(A, { name: 'a.jpg', size: 100 });
    await content(C, { name: 'c.jpg', size: 50 });
    await purgeSet(1, [A, B], [C]);
    const token = await tokenFor(e);
    const r = await e.get(`/api/picks/purge-preview?token=${encodeURIComponent(token)}`);
    expect(r.status).toBe(200);
    expect(r.json).toEqual({ shown: 2, copies: 1, live: 0, bytes: 1150 });
    expect((await e.get('/api/picks/purge-preview?token=junk')).status).toBe(400);
    await describeOk(C); // not shown yet: make the set grow
    await pgh.pool.query('INSERT INTO similar_candidates (set_id, sha256) VALUES (1, $1)', [C]);
    const stale = await e.get(`/api/picks/purge-preview?token=${encodeURIComponent(token)}`);
    expect(stale.status).toBe(409);
    expect(stale.json).toEqual({ error: 'set changed' });
  });

  test('purge marks the closure PURGED, starts the sync, and is not a pick', async () => {
    const e = await setup();
    await purgeSet(1, [A, B], [C]);
    const next = await e.get('/api/picks/next');
    const before = next.json.today;
    const r = await e.post('/api/picks/purge', { token: next.json.token });
    expect(r.status).toBe(200);
    expect(r.json.purged).toEqual({
      shown: 2,
      copies: 1,
      live: 0,
      bytes: 3000,
      shas: [A, B, C].sort(),
    });
    expect(e.sync.calls).toEqual([[A, B, C].sort()]);
    for (const s of [A, B, C]) expect(await state(s)).toBe('PURGED');
    expect(await pickCount()).toBe(0);
    expect(e.ai.calls).toEqual([]);
    expect((await e.get('/api/picks/next')).json.set).toBeNull();
    expect(before).toEqual({ done: 0, goal: 10 });
  });

  test('the same token twice purges once and returns the same summary', async () => {
    const e = await setup();
    await purgeSet(1, [A, B]);
    const token = await tokenFor(e);
    const r1 = await e.post('/api/picks/purge', { token });
    const r2 = await e.post('/api/picks/purge', { token });
    expect(r2.status).toBe(200);
    expect(r2.json).toEqual(r1.json);
    expect((await pgh.pool.query('SELECT count(*)::int AS n FROM purged')).rows[0].n).toBe(2);
  });

  test('a stale set is a 409 and nothing changes', async () => {
    const e = await setup();
    await purgeSet(1, [A, B]);
    const token = await tokenFor(e);
    await pgh.pool.query(
      `UPDATE contents SET state = 'PURGED', purged_at = now() WHERE sha256 = $1`,
      [A],
    );
    const r = await e.post('/api/picks/purge', { token });
    expect(r.status).toBe(409);
    expect(r.json).toEqual({ error: 'set changed' });
    expect(await state(B)).toBe('CLUSTERED');
    expect(e.sync.calls).toEqual([]);
    expect((await e.post('/api/picks/purge', { token: 'junk' })).status).toBe(400);
  });

  test('a sync that cannot start is reported, not an error', async () => {
    const e = await setup();
    await purgeSet(1, [A, B]);
    e.sync.fail = true;
    const r = await e.post('/api/picks/purge', { token: await tokenFor(e) });
    expect(r.status).toBe(200);
    expect(r.json.syncStarted).toBe(false);
    expect(await state(A)).toBe('PURGED');
  });

  test('the purged list groups by set, newest first; a null set is one entry per sha', async () => {
    const e = await setup();
    await content(A, { name: 'a.jpg', size: 10 });
    await content(B, { name: 'b.jpg', size: 20 });
    await content(C, { name: 'c.jpg', size: 30 });
    await content(D, { name: 'd.jpg', size: 40 });
    await pgh.pool.query(
      `INSERT INTO purged (sha256, size_bytes, names, purged_at, unblocked_at, set_id) VALUES
       ($1, 10, '{a.jpg,x.jpg}', '2026-01-01', NULL, 1),
       ($2, 20, '{b.jpg,x.jpg}', '2026-01-01', NULL, 1),
       ($3, 30, '{c.jpg}', '2026-02-01', now(), NULL),
       ($4, 40, '{d.jpg}', '2026-03-01', NULL, NULL)`,
      [A, B, C, D],
    );
    const r = await e.get('/api/picks/purged');
    expect(r.status).toBe(200);
    const p = r.json.purges;
    expect(p).toHaveLength(3);
    expect(p[0]).toMatchObject({ setId: null, names: ['d.jpg'], files: 1, bytes: 40, shas: [D] });
    expect(p[0].unblocked).toBe(false);
    expect(p[1]).toMatchObject({ setId: null, files: 1, unblocked: true, shas: [C] });
    expect(p[2]).toMatchObject({ setId: '1', files: 2, bytes: 30, unblocked: false });
    expect(p[2].names).toEqual(['a.jpg', 'b.jpg', 'x.jpg']);
    expect(p[2].shas).toEqual([A, B]);
    expect(typeof p[2].purgedAt).toBe('string');
  });

  test('unblock validates, unblocks, and asks for a sync', async () => {
    const e = await setup();
    await content(A, { size: 10 });
    await pgh.pool.query(
      `INSERT INTO purged (sha256, size_bytes, names, purged_at) VALUES ($1, 10, '{}', now())`,
      [A],
    );
    for (const bad of [{}, { shas: [] }, { shas: ['nope'] }, { shas: Array(1001).fill(A) }])
      expect((await e.post('/api/picks/unblock', bad)).status).toBe(400);
    expect(e.sync.calls).toEqual([]);
    const r = await e.post('/api/picks/unblock', { shas: [A] });
    expect(r.status).toBe(200);
    expect(r.json).toEqual({ unblocked: 1 });
    expect(e.sync.calls).toEqual([[]]);
    expect((await e.get('/api/picks/purged')).json.purges[0].unblocked).toBe(true);
  });

  test('an unblock whose sync cannot start is reported, not an error', async () => {
    const e = await setup();
    await content(A, { size: 10 });
    await pgh.pool.query(
      `INSERT INTO purged (sha256, size_bytes, names, purged_at) VALUES ($1, 10, '{}', now())`,
      [A],
    );
    e.sync.fail = true;
    const r = await e.post('/api/picks/unblock', { shas: [A] });
    expect(r.status).toBe(200);
    expect(r.json).toEqual({ unblocked: 1, syncStarted: false });
  });

  test('PurgeSyncWorkflow ids are unique per start (no clash within a millisecond)', async () => {
    const started: { type: string; id: string; args: unknown[] }[] = [];
    const client = {
      workflow: {
        start: (type: string, o: { workflowId: string; args: unknown[] }) => {
          started.push({ type, id: o.workflowId, args: o.args });
          return Promise.resolve();
        },
      },
    };
    const pc = temporalPipelineControl(client as never);
    await Promise.all([pc.startPurgeSync(['a']), pc.startPurgeSync([])]);
    expect(started.map((x) => x.type)).toEqual(['PurgeSyncWorkflow', 'PurgeSyncWorkflow']);
    for (const x of started) expect(x.id).toMatch(/^pm-purge-[0-9a-f-]{36}$/);
    expect(started[0]!.id).not.toBe(started[1]!.id);
    expect(started.map((x) => x.args)).toEqual([[['a']], [[]]]);
  });

  test('purge routes enforce the POST header', async () => {
    const e = await setup();
    const r = await e.req('POST', '/api/picks/purge', {
      json: {},
      raw: true,
      headers: { host: `127.0.0.1:${e.port}` },
    });
    expect(r.status).toBeGreaterThanOrEqual(400);
  });
});
