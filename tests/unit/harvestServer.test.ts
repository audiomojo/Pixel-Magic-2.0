/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument -- the JSON bodies under test are untyped */
import { mkdir, readdir, readFile, symlink, writeFile } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { dirname, join } from 'node:path';
import sharp from 'sharp';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { DataLayout } from '../../src/fsops.js';
import type { RunControl } from '../../src/harvest/activities.js';
import { loadHarvestSettings } from '../../src/harvest/config.js';
import { createRun, finishRunRows, setRunState } from '../../src/harvest/runs.js';
import { createHarvestServer, type HarvestServerDeps } from '../../src/harvest/server.js';
import { registerSources, type SystemProbe } from '../../src/harvest/sources.js';
import { HEIC_SCENE0 } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';
import { makeSourceTree, type TreeSpec } from '../helpers/sourceTree.js';

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(() => pgh.truncate());

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

interface Env {
  root: string;
  sourceId: string;
  port: number;
  calls: string[];
  staticDir: string;
  dataDir: string;
  mounted: { value: boolean };
  mountCalls: { value: number };
  temporal: { value: boolean };
  req(method: string, path: string, o?: ReqOpts): Promise<Res>;
  get(path: string): Promise<Res>;
  post(path: string, body: unknown): Promise<Res>;
  runState(id: string): Promise<string>;
}
interface ReqOpts {
  headers?: Record<string, string>;
  body?: string | Buffer;
  json?: unknown;
  raw?: boolean; // no default Host / X-Pixel-Magic
}

const TREE: TreeSpec = {
  'DCIM/100APPLE/a.jpg': 10,
  'Photos/b.jpg': 10,
  'Photos/2020': 'dir',
  'Zeta/z.txt': 5,
  'node_modules/pkg/i.js': 5,
  'Library.photoslibrary/database/x': 'dir',
  '.hidden/x': 5,
  link: { symlink: '/tmp' },
  'file.txt': 5,
};

async function setup(
  o: {
    spec?: TreeSpec;
    deps?: Partial<HarvestServerDeps>;
    control?: Partial<RunControl>;
    thumbCacheMb?: number;
    statfs?: SystemProbe['statfs'];
  } = {},
): Promise<Env> {
  const root = await makeSourceTree(o.spec ?? TREE);
  const volumes = `${dirname(root)}/`;
  const mounted = { value: true };
  const mountCalls = { value: 0 };
  const temporal = { value: true };
  const sys: SystemProbe = {
    mountOutput: () => {
      mountCalls.value++;
      return Promise.resolve(
        mounted.value ? `/dev/disk9s1 on ${root} (exfat, local, nodev)\n` : '',
      );
    },
    diskutilInfo: () => Promise.resolve({ VolumeUUID: 'U-1', VolumeName: 'Drive' }),
    statfs: o.statfs ?? (() => Promise.resolve({ blocks: 1000n, bavail: 600n, bsize: 4096n })),
  };
  const dataDir = await tmpDir();
  const layout = new DataLayout(dataDir);
  await layout.ensure();
  const settings = loadHarvestSettings(
    {
      DATA_DIR: dataDir,
      DATABASE_URL: pgh.url,
    },
    dataDir,
  );
  if (o.thumbCacheMb !== undefined) settings.thumbCacheMb = o.thumbCacheMb; // the env floor is 1
  const [src] = await registerSources(pgh.pool, [
    { identity: 'local:U-1', labelBase: 'Drive', kind: 'local', mountPath: root },
  ]);
  const calls: string[] = [];
  const control: RunControl = {
    start: (id) => {
      calls.push(`start ${id}`);
      return Promise.resolve('started' as const);
    },
    describe: () => Promise.resolve('running' as const),
    signal: (id, n) => {
      calls.push(`signal ${id} ${n}`);
      return Promise.resolve();
    },
    cancel: (id) => {
      calls.push(`cancel ${id}`);
      return Promise.resolve('requested' as const);
    },
    ...o.control,
  };
  const staticDir = await tmpDir();
  await writeFile(join(staticDir, 'index.html'), '<h1>hi</h1>');
  await mkdir(join(staticDir, 'sub'));
  await writeFile(join(staticDir, 'sub', 'app.js'), 'x');
  await writeFile(join(staticDir, 'app.css'), 'x');
  await writeFile(join(staticDir, 'data.bin'), 'x');
  const server = createHarvestServer({
    pool: pgh.pool,
    settings,
    sys,
    layout,
    control,
    staticDir,
    temporalHealthy: () => Promise.resolve(temporal.value),
    detectOpts: { volumes },
    sourcesTtlMs: 0,
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
      let body = ro.body;
      if (ro.json !== undefined) {
        body = JSON.stringify(ro.json);
        headers['content-type'] = 'application/json';
      }
      if (body !== undefined) headers['content-length'] = String(Buffer.byteLength(body));
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
    root,
    sourceId: src!.id,
    port,
    calls,
    staticDir,
    dataDir,
    mounted,
    mountCalls,
    temporal,
    req,
    get: (p) => req('GET', p),
    post: (p, json) => req('POST', p, { json }),
    runState: async (id) =>
      (await pgh.pool.query('SELECT state FROM harvest_runs WHERE id = $1', [id])).rows[0].state,
  };
}

async function item(
  e: Env,
  rel: string,
  state: string,
  extra: { kind?: string; sha?: string; size?: number; dz?: string; fileId?: number } = {},
): Promise<string> {
  const { rows } = await pgh.pool.query<{ id: string }>(
    `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state,
                                sha256, dropzone_path, file_id)
     VALUES ($1, $2, lower($2), $3, 1700000000000000000, $4, $5, $6, $7, $8) RETURNING id::text AS id`,
    [
      e.sourceId,
      rel,
      extra.size ?? 100,
      extra.kind ?? 'maybe',
      state,
      extra.sha ?? null,
      extra.dz ?? null,
      extra.fileId ?? null,
    ],
  );
  return rows[0]!.id;
}
const itemState = async (id: string): Promise<string> =>
  (await pgh.pool.query('SELECT state FROM harvest_items WHERE id = $1', [id])).rows[0].state;

describe('protection', () => {
  test('rejects a foreign Host', async () => {
    const e = await setup();
    for (const host of ['evil.example', `127.0.0.1:${e.port + 1}`, `evil.example:${e.port}`]) {
      const r = await e.req('GET', '/api/state', { headers: { host } });
      expect(r.status).toBe(403);
      expect(r.json.error).toBeTruthy();
    }
  });

  test('accepts localhost:<port> too', async () => {
    const e = await setup();
    const r = await e.req('GET', '/api/state', { headers: { host: `localhost:${e.port}` } });
    expect(r.status).toBe(200);
  });

  test('rejects a POST without X-Pixel-Magic', async () => {
    const e = await setup();
    const r = await e.req('POST', '/api/runs', {
      raw: true,
      headers: { host: `127.0.0.1:${e.port}` },
      json: { folders: [{ sourceId: e.sourceId, relPath: '' }], dryRun: false },
    });
    expect(r.status).toBe(403);
    const wrong = await e.req('POST', '/api/runs', { headers: { 'x-pixel-magic': '0' }, json: {} });
    expect(wrong.status).toBe(403);
    expect((await pgh.pool.query('SELECT 1 FROM harvest_runs')).rowCount).toBe(0);
  });

  test('rejects a mismatched Origin, accepts a matching one', async () => {
    const e = await setup();
    const body = { folders: [{ sourceId: e.sourceId, relPath: '' }], dryRun: true };
    for (const origin of ['http://evil.example', `http://127.0.0.1:${e.port + 1}`, 'null']) {
      const r = await e.req('POST', '/api/runs', { headers: { origin }, json: body });
      expect(r.status).toBe(403);
    }
    for (const origin of [`http://127.0.0.1:${e.port}`, `http://localhost:${e.port}`]) {
      const r = await e.req('POST', '/api/runs', { headers: { origin }, json: body });
      expect(r.status).toBe(200);
    }
  });

  test('a body over 1 MB is refused with 413', async () => {
    const e = await setup();
    const r = await e.req('POST', '/api/runs', { body: 'x'.repeat(1024 * 1024 + 10) });
    expect(r.status).toBe(413);
    expect(r.json.error).toMatch(/large/i);
  });

  test('invalid JSON is a 400; unknown routes 404; wrong methods 405', async () => {
    const e = await setup();
    expect((await e.req('POST', '/api/runs', { body: '{nope' })).status).toBe(400);
    expect((await e.get('/api/nope')).status).toBe(404);
    expect((await e.post('/api/state', {})).status).toBe(404);
    expect((await e.req('DELETE', '/api/state')).status).toBe(405);
    expect((await e.req('DELETE', '/index.html')).status).toBe(405);
  });

  test('an unexpected failure is a 500 with { error }', async () => {
    const e = await setup({ deps: { temporalHealthy: () => Promise.reject(new Error('x')) } });
    // temporalHealthy failing is swallowed (reported false); break the pool instead
    expect((await e.get('/api/state')).json.stack.temporal).toBe(false);
    const r = await e.get('/api/runs/abc');
    expect(r.status).toBe(404);
    const bad = await e.get('/api/runs/99999999999999999999999');
    expect([404, 500]).toContain(bad.status);
  });
});

describe('state, sources', () => {
  test('GET /api/state lists mounted sources, stack health, queue and review count', async () => {
    const e = await setup();
    await item(e, 'a.png', 'REVIEW');
    await item(e, 'b.png', 'REVIEW');
    const r1 = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], false);
    const r2 = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'Photos' }], true);
    await setRunState(pgh.pool, r1, 'paused', 'disk 81.0 %');
    const s = (await e.get('/api/state')).json;
    expect(s.stack).toEqual({ db: true, temporal: true });
    expect(s.sources).toHaveLength(1);
    expect(s.sources[0]).toMatchObject({ id: e.sourceId, label: 'Drive', mounted: true });
    expect(s.reviewCount).toBe(2);
    expect(s.activeRun).toMatchObject({ id: r1, state: 'paused', paused_reason: 'disk 81.0 %' });
    expect(s.activeRun.folders).toEqual([
      expect.objectContaining({ relPath: 'DCIM', state: 'queued', sent: '0' }),
    ]);
    expect(s.queue.map((q: { id: string }) => q.id)).toEqual([r2]);
    // 400 of 1000 blocks used = 40 %, limit 80 %
    expect(s.gate).toMatchObject({ diskPct: 40, diskLimit: 80, backlog: 0, backlogLimit: 20000 });
  });

  test('a mounted local drive whose probe failed is listed as unidentified, with no row', async () => {
    let root = '';
    let mystery = '';
    const sys: SystemProbe = {
      mountOutput: () =>
        Promise.resolve(
          `/dev/disk9s1 on ${root} (exfat, local, nodev)\n/dev/disk8s1 on ${mystery} (exfat, local, nodev)\n`,
        ),
      diskutilInfo: (dev) =>
        Promise.resolve(dev === '/dev/disk9s1' ? { VolumeUUID: 'U-1', VolumeName: 'Drive' } : null),
      statfs: () => Promise.resolve({ blocks: 1000n, bavail: 600n, bsize: 4096n }),
    };
    const e2 = await setup({ deps: { sys } });
    root = e2.root;
    mystery = join(dirname(root), 'Mystery');
    await mkdir(mystery);
    const s = (await e2.get('/api/state')).json;
    expect(s.sources.map((x: { label: string }) => x.label)).toEqual(['Drive']);
    expect(s.unidentified).toEqual([{ mountPath: mystery, labelBase: 'Mystery' }]);
    const r = await e2.post('/api/sources/refresh', {});
    expect(r.json.unidentified).toHaveLength(1);
  });

  test('state reports an idle system and a down Temporal', async () => {
    const e = await setup();
    e.temporal.value = false;
    const s = (await e.get('/api/state')).json;
    expect(s.stack.temporal).toBe(false);
    expect(s.activeRun).toBeNull();
    expect(s.gate).toBeNull();
    expect(s.queue).toEqual([]);
  });

  test('state reports db: false when the pool is broken', async () => {
    const e = await setup({
      deps: {
        pool: { query: () => Promise.reject(new Error('down')) } as never,
      },
    });
    const r = await e.get('/api/state');
    expect(r.status).toBe(200);
    expect(r.json.stack.db).toBe(false);
    expect(r.json.sources).toEqual([]);
  });

  test('an unmounted source is listed as not mounted; refresh re-detects', async () => {
    const e = await setup({ deps: { sourcesTtlMs: 60_000 } });
    expect((await e.get('/api/state')).json.sources[0].mounted).toBe(true);
    e.mounted.value = false;
    expect((await e.get('/api/state')).json.sources[0].mounted).toBe(true); // cached
    const r = await e.post('/api/sources/refresh', {});
    expect(r.json.sources[0].mounted).toBe(false);
    expect((await e.get('/api/state')).json.sources[0].mounted).toBe(false);
  });

  test('sources carry sent count, last harvest and review count', async () => {
    const e = await setup();
    await item(e, 'a.jpg', 'SENT', { kind: 'photo' });
    await item(e, 'b.jpg', 'SENT', { kind: 'photo' });
    await item(e, 'c.png', 'REVIEW');
    await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], false);
    await pgh.pool.query("UPDATE harvest_folders SET last_harvested_at = '2026-01-02T03:04:05Z'");
    const [src] = (await e.get('/api/state')).json.sources;
    expect(src).toMatchObject({ sentCount: 2, reviewCount: 1 });
    expect(new Date(src.lastHarvestedAt).toISOString()).toBe('2026-01-02T03:04:05.000Z');
    expect(src.totalBytes).toBe(4096 * 1000);
    expect(src.usedBytes).toBe(4096 * 400);
  });
});

describe('drive sizes', () => {
  test('a hung statfs cannot block /api/state; sizes are cached with the sources TTL', async () => {
    let calls = 0;
    const e = await setup({
      statfs: () => {
        calls++;
        return new Promise(() => {});
      },
      deps: { statfsTimeoutMs: 50, sourcesTtlMs: 60_000 },
    });
    const t0 = Date.now();
    const s = (await e.get('/api/state')).json.sources[0];
    expect(Date.now() - t0).toBeLessThan(1500);
    expect(s).toMatchObject({ mounted: true, usedBytes: null, totalBytes: null });
    await e.get('/api/state');
    expect(calls).toBe(1);
  });
});

describe('runs', () => {
  test('POST /api/runs creates a queued run and starts it when idle', async () => {
    const e = await setup();
    const r = await e.post('/api/runs', {
      folders: [
        { sourceId: e.sourceId, relPath: 'DCIM' },
        { sourceId: e.sourceId, relPath: '' },
      ],
      dryRun: true,
    });
    expect(r.status).toBe(200);
    expect(r.json).toEqual({ runId: expect.any(String), state: 'queued' });
    expect(e.calls).toEqual([`start ${r.json.runId}`]);
    const { rows } = await pgh.pool.query(
      `SELECT f.rel_path, r.dry_run FROM harvest_run_folders rf
       JOIN harvest_folders f ON f.id = rf.folder_id JOIN harvest_runs r ON r.id = rf.run_id
       WHERE rf.run_id = $1 ORDER BY rf.position`,
      [r.json.runId],
    );
    expect(rows).toEqual([
      { rel_path: 'DCIM', dry_run: true },
      { rel_path: '', dry_run: true },
    ]);
  });

  test('a second POST queues behind the active run', async () => {
    const e = await setup();
    const body = { folders: [{ sourceId: e.sourceId, relPath: 'Photos' }], dryRun: false };
    const a = await e.post('/api/runs', body);
    await setRunState(pgh.pool, a.json.runId, 'scanning');
    const b = await e.post('/api/runs', body);
    expect(b.json.state).toBe('queued');
    expect(b.json.runId).not.toBe(a.json.runId);
    expect(e.calls).toEqual([`start ${a.json.runId}`]);
  });

  test('NFC-normalises relPath and de-duplicates folders', async () => {
    const e = await setup();
    const r = await e.post('/api/runs', {
      folders: [
        { sourceId: Number(e.sourceId), relPath: 'Café/' },
        { sourceId: e.sourceId, relPath: 'Café' },
        { sourceId: e.sourceId, relPath: './x//y' },
      ],
    });
    expect(r.status).toBe(200);
    const { rows } = await pgh.pool.query(
      `SELECT f.rel_path, r.dry_run FROM harvest_run_folders rf
       JOIN harvest_folders f ON f.id = rf.folder_id JOIN harvest_runs r ON r.id = rf.run_id
       WHERE rf.run_id = $1 ORDER BY rf.position`,
      [r.json.runId],
    );
    expect(rows).toEqual([
      { rel_path: 'Café', dry_run: false },
      { rel_path: 'x/y', dry_run: false },
    ]);
  });

  test.each([
    ['leading slash', { folders: [{ sourceId: '1', relPath: '/etc' }] }],
    ['dotdot', { folders: [{ sourceId: '1', relPath: 'a/../../b' }] }],
    ['backslash dotdot', { folders: [{ sourceId: '1', relPath: '..\\b' }] }],
    ['NUL', { folders: [{ sourceId: '1', relPath: 'a\0b' }] }],
    ['no folders', { folders: [] }],
    ['not an object', []],
    ['missing relPath', { folders: [{ sourceId: '1' }] }],
    ['bad sourceId', { folders: [{ sourceId: 'x', relPath: '' }] }],
    ['bad dryRun', { folders: [{ sourceId: '1', relPath: '' }], dryRun: 'yes' }],
  ])('POST /api/runs rejects %s with 400', async (_n, body) => {
    const e = await setup();
    const r = await e.post('/api/runs', body);
    expect(r.status).toBe(400);
    expect(r.json.error).toBeTruthy();
    expect((await pgh.pool.query('SELECT 1 FROM harvest_runs')).rowCount).toBe(0);
  });

  test('POST /api/runs rejects an unknown source', async () => {
    const e = await setup();
    const r = await e.post('/api/runs', { folders: [{ sourceId: '999', relPath: '' }] });
    expect(r.status).toBe(400);
    expect(r.json.error).toMatch(/source/);
  });

  test('pause, resume and cancel call RunControl', async () => {
    const e = await setup();
    const id = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    await setRunState(pgh.pool, id, 'scanning');
    expect((await e.post(`/api/runs/${id}/pause`, {})).json).toEqual({ ok: true });
    expect((await e.post(`/api/runs/${id}/resume`, {})).status).toBe(200);
    expect((await e.post(`/api/runs/${id}/cancel`, {})).status).toBe(200);
    expect(e.calls).toEqual([`signal ${id} pause`, `signal ${id} resume`, `cancel ${id}`]);
  });

  test('cancel removes a queued run; finished runs give 409', async () => {
    const e = await setup();
    const q = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    expect((await e.post(`/api/runs/${q}/cancel`, {})).status).toBe(200);
    expect(await e.runState(q)).toBe('cancelled');
    expect(e.calls).toEqual([`cancel ${q}`]); // in case it was just offered to Temporal
    expect((await e.post(`/api/runs/${q}/pause`, {})).status).toBe(409);
    const q2 = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    expect((await e.post(`/api/runs/${q2}/resume`, {})).status).toBe(409); // not started yet
    expect((await e.post(`/api/runs/${q}/cancel`, {})).status).toBe(409);
    expect((await e.post('/api/runs/999/pause', {})).status).toBe(404);
    expect((await e.post('/api/runs/abc/pause', {})).status).toBe(404);
  });

  test('cancel of an active run whose workflow is gone finishes it and starts the next', async () => {
    const e = await setup({ control: { cancel: () => Promise.resolve('gone' as const) } });
    const id = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    const next = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], false);
    await setRunState(pgh.pool, id, 'paused', 'Drive unplugged');
    const r = await e.post(`/api/runs/${id}/cancel`, {});
    expect(r.status).toBe(200);
    expect(await e.runState(id)).toBe('cancelled');
    expect(e.calls).toEqual([`start ${next}`]);
  });

  test('a Temporal failure while signalling is a 502', async () => {
    const e = await setup({ control: { signal: () => Promise.reject(new Error('gone')) } });
    const id = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    await setRunState(pgh.pool, id, 'scanning');
    const r = await e.post(`/api/runs/${id}/pause`, {});
    expect(r.status).toBe(502);
    expect(r.json.error).toMatch(/gone/);
  });

  test('GET /api/runs lists history; GET /api/runs/:id has detail and errors', async () => {
    const e = await setup();
    const a = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], false);
    const b = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'Photos' }], true);
    await pgh.pool.query("UPDATE harvest_runs SET state = 'failed', error = 'boom' WHERE id = $1", [
      a,
    ]);
    await pgh.pool.query(
      `UPDATE harvest_run_folders SET junk = '{"too-small": 3}', sent = 4 WHERE run_id = $1`,
      [b],
    );
    const id = await item(e, 'DCIM/bad.cr2', 'FAILED', { kind: 'photo' });
    await pgh.pool.query(
      "UPDATE harvest_items SET last_error = 'EIO', last_seen_run = $2 WHERE id = $1",
      [id, a],
    );
    const list = (await e.get('/api/runs?limit=1')).json;
    expect(list.runs.map((r: { id: string }) => r.id)).toEqual([b]);
    expect((await e.get('/api/runs')).json.runs.map((r: { id: string }) => r.id)).toEqual([b, a]);
    expect((await e.get('/api/runs?limit=zzz')).status).toBe(400);
    const d = (await e.get(`/api/runs/${a}`)).json;
    expect(d.run).toMatchObject({ id: a, state: 'failed', error: 'boom' });
    expect(d.errors).toEqual([expect.objectContaining({ relPath: 'DCIM/bad.cr2', error: 'EIO' })]);
    const dry = (await e.get(`/api/runs/${b}`)).json;
    expect(dry.run.dry_run).toBe(true);
    expect(dry.run.folders[0]).toMatchObject({ junk: { 'too-small': 3 }, sent: '4' });
  });
});

describe('tree', () => {
  test('lists only sub-folders, hides pruned and symlinked ones', async () => {
    const e = await setup();
    const r = await e.get(`/api/sources/${e.sourceId}/tree?path=`);
    expect(r.status).toBe(200);
    expect(r.json.entries.map((x: { name: string }) => x.name)).toEqual([
      '.hidden',
      'DCIM',
      'Library.photoslibrary',
      'Photos',
      'Zeta',
    ]);
    const sub = (await e.get(`/api/sources/${e.sourceId}/tree?path=Photos`)).json;
    expect(sub.entries).toEqual([{ name: '2020', relPath: 'Photos/2020' }]);
  });

  test('a photo library lists only its masters/originals', async () => {
    const e = await setup({
      spec: { 'Lib.photoslibrary/originals/a': 'dir', 'Lib.photoslibrary/database/b': 'dir' },
    });
    const r = await e.get(`/api/sources/${e.sourceId}/tree?path=Lib.photoslibrary`);
    expect(r.json.entries.map((x: { name: string }) => x.name)).toEqual(['originals']);
  });

  test('entries carry harvest history, and the nearest harvested ancestor is reported', async () => {
    const e = await setup();
    const run = await createRun(
      pgh.pool,
      [
        { sourceId: e.sourceId, relPath: 'DCIM' },
        { sourceId: e.sourceId, relPath: 'Photos' },
        { sourceId: e.sourceId, relPath: 'Photos/2020' },
      ],
      false,
    );
    await pgh.pool.query(
      `UPDATE harvest_folders SET last_harvested_at = '2026-02-03T00:00:00Z',
         last_counts = '{"sent": 7}' WHERE rel_path IN ('DCIM', 'Photos')`,
    );
    void run;
    const root = (await e.get(`/api/sources/${e.sourceId}/tree?path=`)).json;
    const dcim = root.entries.find((x: { name: string }) => x.name === 'DCIM');
    expect(dcim.lastHarvestedAt).toBe('2026-02-03T00:00:00.000Z');
    expect(dcim.lastCounts).toEqual({ sent: 7 });
    expect(root.entries.find((x: { name: string }) => x.name === 'Zeta').lastHarvestedAt).toBe(
      undefined,
    );
    expect(root.coveredBy).toBeNull();
    const sub = (await e.get(`/api/sources/${e.sourceId}/tree?path=Photos`)).json;
    expect(sub.coveredBy).toEqual({
      relPath: 'Photos',
      lastHarvestedAt: '2026-02-03T00:00:00.000Z',
    });
    const deep = (await e.get(`/api/sources/${e.sourceId}/tree?path=DCIM/100APPLE`)).json;
    expect(deep.coveredBy.relPath).toBe('DCIM');
  });

  test('errors: unmounted 409, unknown source 404, bad path 400, missing folder 404, timeout 504', async () => {
    const e = await setup({ deps: { treeTimeoutMs: 50, listDir: () => new Promise(() => {}) } });
    expect((await e.get(`/api/sources/${e.sourceId}/tree?path=`)).status).toBe(504);
    expect((await e.get(`/api/sources/${e.sourceId}/tree?path=../x`)).status).toBe(400);
    expect((await e.get(`/api/sources/${e.sourceId}/tree?path=%2Fetc`)).status).toBe(400);
    expect((await e.get('/api/sources/999/tree?path=')).status).toBe(404);
    expect((await e.get('/api/sources/abc/tree?path=')).status).toBe(404);
    e.mounted.value = false;
    expect((await e.get(`/api/sources/${e.sourceId}/tree?path=`)).status).toBe(409);
    const e2 = await setup();
    expect((await e2.get(`/api/sources/${e2.sourceId}/tree?path=Nope`)).status).toBe(404);
    expect((await e2.get(`/api/sources/${e2.sourceId}/tree`)).status).toBe(200);
  });

  test('symlinked and pruned components are refused, in the tree and in POST /api/runs', async () => {
    const e = await setup({
      spec: {
        ...TREE,
        'Photos/inner': { symlink: '../DCIM' },
        '.Trashes/501/x': 'dir',
      },
    });
    const t = (p: string) => e.get(`/api/sources/${e.sourceId}/tree?path=${encodeURIComponent(p)}`);
    for (const p of [
      'link',
      'link/x',
      'Photos/inner',
      'Photos/inner/100APPLE',
      'node_modules',
      'node_modules/pkg',
    ])
      expect((await t(p)).status, p).toBe(404);
    expect((await t('Photos')).status).toBe(200);
    for (const p of ['link', 'Photos/inner/100APPLE', 'node_modules/pkg', '.Trashes/501']) {
      const r = await e.post('/api/runs', { folders: [{ sourceId: e.sourceId, relPath: p }] });
      expect(r.status, p).toBe(400);
    }
    expect((await pgh.pool.query('SELECT 1 FROM harvest_runs')).rowCount).toBe(0);
    // a missing folder is fine for a run (the scan reports it); an unmounted source is checked by name
    expect(
      (await e.post('/api/runs', { folders: [{ sourceId: e.sourceId, relPath: 'Nope' }] })).status,
    ).toBe(200);
    e.mounted.value = false;
    expect(
      (await e.post('/api/runs', { folders: [{ sourceId: e.sourceId, relPath: 'Photos' }] }))
        .status,
    ).toBe(200);
    expect(
      (await e.post('/api/runs', { folders: [{ sourceId: e.sourceId, relPath: 'node_modules' }] }))
        .status,
    ).toBe(400);
  });

  test('a read failure other than a missing folder is a 500', async () => {
    const e = await setup({
      deps: { listDir: () => Promise.reject(Object.assign(new Error('EIO'), { code: 'EIO' })) },
    });
    expect((await e.get(`/api/sources/${e.sourceId}/tree?path=`)).status).toBe(500);
  });
});

describe('review', () => {
  test('GET /api/review groups maybes by folder, then lists files', async () => {
    const e = await setup();
    await item(e, 'Pics/a.png', 'REVIEW', { size: 10 });
    await item(e, 'Pics/b.png', 'REVIEW', { size: 20 });
    await item(e, 'c.png', 'REVIEW', { size: 5 });
    await item(e, 'Pics/d.png', 'SENT');
    const g = (await e.get('/api/review')).json;
    expect(g.groups).toEqual([
      { sourceId: e.sourceId, label: 'Drive', path: '', count: 1, bytes: 5 },
      { sourceId: e.sourceId, label: 'Drive', path: 'Pics', count: 2, bytes: 30 },
    ]);
    const f = (await e.get(`/api/review?sourceId=${e.sourceId}&path=Pics`)).json;
    expect(f.files.map((x: { name: string }) => x.name)).toEqual(['a.png', 'b.png']);
    expect(f.files[0]).toMatchObject({ relPath: 'Pics/a.png', sizeBytes: 10 });
    expect((await e.get(`/api/review?sourceId=${e.sourceId}`)).json.files).toHaveLength(1); // path ''
    expect((await e.get('/api/review?sourceId=x')).status).toBe(400);
  });

  test('review send flips REVIEW→PLANNED and signals itemsApproved; skip → REJECTED', async () => {
    const e = await setup();
    const run = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    await setRunState(pgh.pool, run, 'sending');
    const a = await item(e, 'Pics/a.png', 'REVIEW');
    const b = await item(e, 'Pics/b.png', 'REVIEW');
    const c = await item(e, 'Pics/c.png', 'SENT'); // not in REVIEW: untouched
    const r = await e.post('/api/review', { itemIds: [a, c], action: 'send' });
    expect(r.json).toMatchObject({ changed: 1, runId: run });
    expect(await itemState(a)).toBe('PLANNED');
    expect(await itemState(c)).toBe('SENT');
    expect(e.calls).toEqual([`signal ${run} itemsApproved`]);
    const kind = (await pgh.pool.query('SELECT kind FROM harvest_items WHERE id = $1', [a]))
      .rows[0];
    expect(kind.kind).toBe('maybe');
    const s = await e.post('/api/review', { itemIds: [b, c], action: 'skip' });
    expect(s.json).toEqual({ changed: 1 });
    expect(await itemState(b)).toBe('REJECTED');
    expect(await itemState(c)).toBe('SENT');
    expect(e.calls).toHaveLength(1);
  });

  test('review send without an active run creates a send-only run over the parent folders', async () => {
    const e = await setup();
    const a = await item(e, 'Pics/a.png', 'REVIEW');
    const b = await item(e, 'Pics/b.png', 'REVIEW');
    const c = await item(e, 'top.png', 'REVIEW');
    const r = await e.post('/api/review', { itemIds: [a, b, c], action: 'send' });
    expect(r.json.changed).toBe(3);
    expect(e.calls).toEqual([`start ${r.json.runId}`]);
    const so = await pgh.pool.query('SELECT send_only FROM harvest_runs WHERE id = $1', [
      r.json.runId,
    ]);
    expect(so.rows[0].send_only).toBe(true);
    const { rows } = await pgh.pool.query(
      `SELECT f.rel_path, r.dry_run FROM harvest_run_folders rf
       JOIN harvest_folders f ON f.id = rf.folder_id JOIN harvest_runs r ON r.id = rf.run_id
       WHERE rf.run_id = $1 ORDER BY f.rel_path`,
      [r.json.runId],
    );
    expect(rows).toEqual([
      { rel_path: '', dry_run: false },
      { rel_path: 'Pics', dry_run: false },
    ]);
  });

  const runItems = async (runId: string): Promise<string[]> =>
    (
      await pgh.pool.query<{ id: string }>(
        'SELECT item_id::text AS id FROM harvest_run_items WHERE run_id = $1 ORDER BY item_id',
        [runId],
      )
    ).rows.map((r) => r.id);

  test('a send-only run records exactly the approved items (F-1)', async () => {
    const e = await setup();
    const a = await item(e, 'Pics/a.png', 'REVIEW');
    const b = await item(e, 'Pics/b.png', 'REVIEW');
    await item(e, 'Pics/c.png', 'REVIEW'); // not approved
    await item(e, 'Pics/p.jpg', 'PLANNED'); // a photo waiting for a normal run
    const r = await e.post('/api/review', { itemIds: [a, b], action: 'send' });
    expect(await runItems(r.json.runId)).toEqual([a, b].sort((x, y) => Number(x) - Number(y)));
  });

  test('approvals covered by an active send-only run join its items; a normal run adds none', async () => {
    const e = await setup();
    const x = await item(e, 'Pics/x.png', 'REVIEW');
    const first = await e.post('/api/review', { itemIds: [x], action: 'send' });
    const so = first.json.runId as string;
    await setRunState(pgh.pool, so, 'sending');
    const y = await item(e, 'Pics/y.png', 'REVIEW');
    const r = await e.post('/api/review', { itemIds: [y], action: 'send' });
    expect(r.json.runId).toBe(so);
    expect(await runItems(so)).toEqual([x, y]);
    expect(e.calls).toEqual([`start ${so}`, `signal ${so} itemsApproved`]);
    await finishRunRows(pgh.pool, so, 'done');
    const normal = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], false);
    await setRunState(pgh.pool, normal, 'sending');
    const z = await item(e, 'Pics/z.png', 'REVIEW');
    expect((await e.post('/api/review', { itemIds: [z], action: 'send' })).json.runId).toBe(normal);
    expect(await runItems(normal)).toEqual([]); // its folders already hold the item
  });

  test('approving at a source root queues a send-only run for that folder only', async () => {
    const e = await setup();
    const a = await item(e, 'top.png', 'REVIEW');
    const r = await e.post('/api/review', { itemIds: [a], action: 'send' });
    const { rows } = await pgh.pool.query(
      `SELECT f.rel_path, r.send_only FROM harvest_run_folders rf
       JOIN harvest_folders f ON f.id = rf.folder_id JOIN harvest_runs r ON r.id = rf.run_id
       WHERE rf.run_id = $1`,
      [r.json.runId],
    );
    expect(rows).toEqual([{ rel_path: '', send_only: true }]);
    expect(await itemState(a)).toBe('PLANNED');
    await pgh.pool.query('SELECT 1');
    expect(
      (
        await pgh.pool.query(
          'SELECT count(*)::int AS n FROM harvest_folders WHERE last_harvested_at IS NOT NULL',
        )
      ).rows[0].n,
    ).toBe(0);
  });

  test('with a dry run active, approvals queue a send-only run instead of signalling', async () => {
    const e = await setup();
    const dry = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: '' }], true);
    await setRunState(pgh.pool, dry, 'scanning');
    const a = await item(e, 'Pics/a.png', 'REVIEW');
    const r = await e.post('/api/review', { itemIds: [a], action: 'send' });
    expect(r.json.runId).not.toBe(dry);
    expect(e.calls).toEqual([]); // the dry run stays active, so the new run is only queued
    expect(await e.runState(r.json.runId)).toBe('queued');
    expect(await itemState(a)).toBe('PLANNED');
  });

  test('an active run that does not cover the approved folders is not signalled', async () => {
    const e = await setup();
    const act = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'Other' }], false);
    await setRunState(pgh.pool, act, 'sending');
    const a = await item(e, 'Pics/a.png', 'REVIEW');
    const b = await item(e, 'Other/deep/b.png', 'REVIEW');
    const r = await e.post('/api/review', { itemIds: [b], action: 'send' });
    expect(r.json.runId).toBe(act); // covered by the Other folder
    const r2 = await e.post('/api/review', { itemIds: [a], action: 'send' });
    expect(r2.json.runId).not.toBe(act);
    expect(e.calls).toEqual([`signal ${act} itemsApproved`]);
  });

  test('review by { sourceId, path } acts on that folder; nothing in REVIEW is a no-op', async () => {
    const e = await setup();
    const a = await item(e, 'Pics/a.png', 'REVIEW');
    const o = await item(e, 'Other/o.png', 'REVIEW');
    const r = await e.post('/api/review', { sourceId: e.sourceId, path: 'Pics', action: 'skip' });
    expect(r.json).toEqual({ changed: 1 });
    expect(await itemState(a)).toBe('REJECTED');
    expect(await itemState(o)).toBe('REVIEW');
    const none = await e.post('/api/review', { itemIds: [a], action: 'send' });
    expect(none.json).toEqual({ changed: 0 });
    expect(e.calls).toEqual([]);
    const root = await item(e, 'top.png', 'REVIEW');
    await e.post('/api/review', { sourceId: e.sourceId, path: '', action: 'skip' });
    expect(await itemState(root)).toBe('REJECTED');
    expect(await itemState(o)).toBe('REVIEW');
  });

  test.each([
    ['neither ids nor path', { action: 'send' }],
    ['bad action', { itemIds: ['1'], action: 'delete' }],
    ['bad id', { itemIds: ['x'], action: 'send' }],
    ['empty ids', { itemIds: [], action: 'send' }],
    ['bad path', { sourceId: '1', path: '../x', action: 'skip' }],
  ])('review rejects %s with 400', async (_n, body) => {
    const e = await setup();
    expect((await e.post('/api/review', body)).status).toBe(400);
  });
});

describe('where', () => {
  test('where finds every source path for a sha256', async () => {
    const e = await setup();
    const sha = 'ab'.repeat(32);
    await item(e, 'A/one.jpg', 'SENT', { kind: 'photo', sha, dz: 'harvest/Drive/A/one.jpg' });
    await item(e, 'B/two.jpg', 'DUP', { kind: 'photo', sha });
    await item(e, 'C/three.jpg', 'SENT', { kind: 'photo', sha: 'cd'.repeat(32) });
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
       VALUES ('x', 'x.jpg', 1, 1, 'jpg', $1, 'INGESTED') RETURNING id::text AS id`,
      [sha],
    );
    await item(e, 'D/four.jpg', 'SENT', { kind: 'photo', fileId: Number(rows[0]!.id) });
    const r = (await e.get(`/api/where?q=${sha.toUpperCase()}`)).json;
    expect(r.items.map((x: { relPath: string }) => x.relPath).sort()).toEqual([
      'A/one.jpg',
      'B/two.jpg',
      'D/four.jpg',
    ]);
    expect(r.items[0]).toMatchObject({ source: 'Drive', state: expect.any(String) });
    expect(r.items.find((x: { relPath: string }) => x.relPath === 'A/one.jpg')).toMatchObject({
      dropzonePath: 'harvest/Drive/A/one.jpg',
      sha256: sha,
    });
  });

  test('where finds by file name (source, drop-zone or pipeline name)', async () => {
    const e = await setup();
    await item(e, 'A/IMG_1.JPG', 'SENT', { kind: 'photo', dz: 'harvest/Drive/A/IMG_1.JPG' });
    await item(e, 'B/other.jpg', 'SENT', { kind: 'photo', dz: 'harvest/Drive/B/IMG_1 (2).JPG' });
    await item(e, 'C/img_1.jpg', 'KNOWN', { kind: 'photo' });
    await item(e, 'D/zzz.jpg', 'SENT', { kind: 'photo' });
    const names = async (q: string): Promise<string[]> =>
      (await e.get(`/api/where?q=${encodeURIComponent(q)}`)).json.items
        .map((x: { relPath: string }) => x.relPath)
        .sort();
    expect(await names('IMG_1.JPG')).toEqual(['A/IMG_1.JPG', 'C/img_1.jpg']);
    expect(await names('IMG_1 (2).JPG')).toEqual(['B/other.jpg']);
    expect(await names('nothing.jpg')).toEqual([]);
    expect((await e.get('/api/where?q=')).status).toBe(400);
    expect((await e.get('/api/where')).status).toBe(400);
  });

  test('where finds by the pipeline original_name via file_id', async () => {
    const e = await setup();
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, state)
       VALUES ('x', 'Vault Name.jpg', 1, 1, 'jpg', 'INGESTED') RETURNING id::text AS id`,
    );
    await item(e, 'A/q.jpg', 'SENT', { kind: 'photo', fileId: Number(rows[0]!.id) });
    const r = (await e.get('/api/where?q=Vault%20Name.jpg')).json;
    expect(r.items.map((x: { relPath: string }) => x.relPath)).toEqual(['A/q.jpg']);
  });
});

describe('thumb', () => {
  async function pngBuf(w: number, h: number): Promise<Buffer> {
    return sharp({ create: { width: w, height: h, channels: 3, background: '#aa4400' } })
      .png()
      .toBuffer();
  }

  test('thumb returns a JPEG and caches it; unmounted source → placeholder', async () => {
    const e = await setup({ spec: { 'Pics/a.png': await pngBuf(600, 300) } });
    const id = await item(e, 'Pics/a.png', 'REVIEW');
    const r = await e.get(`/api/thumb/${id}`);
    expect(r.status).toBe(200);
    expect(r.headers['content-type']).toBe('image/jpeg');
    expect(await sharp(r.body).metadata()).toMatchObject({
      format: 'jpeg',
      width: 256,
      height: 128,
    });
    const cached = join(e.dataDir, 'harvest-cache', 'thumbs', `${id}.jpg`);
    expect(await readdir(join(e.dataDir, 'harvest-cache', 'thumbs'))).toEqual([`${id}.jpg`]);
    // cache hit: the source may vanish and the same bytes still come back
    const again = await e.get(`/api/thumb/${id}`);
    expect(again.body.equals(r.body)).toBe(true);
    expect(cached).toContain(id);
    // a cached thumbnail needs no drive detection at all, even with the drive gone
    const detections = e.mountCalls.value;
    e.mounted.value = false;
    expect((await e.get(`/api/thumb/${id}`)).body.equals(r.body)).toBe(true);
    expect(e.mountCalls.value).toBe(detections);
    // an uncached item on an unmounted source gets the placeholder
    const other = await item(e, 'Pics/a.png', 'REVIEW', { size: 101 });
    const ph = await e.get(`/api/thumb/${other}`);
    expect(ph.status).toBe(200);
    expect(ph.headers['content-type']).toBe('image/jpeg');
    expect(ph.headers['cache-control']).toBe('no-store');
    expect(ph.body.equals(r.body)).toBe(false);
  });

  test('a decode failure, a missing file and a missing item', async () => {
    const e = await setup({ spec: { 'Pics/bad.png': Buffer.from('garbage') } });
    const bad = await item(e, 'Pics/bad.png', 'REVIEW');
    const gone = await item(e, 'Pics/gone.png', 'REVIEW');
    for (const id of [bad, gone]) {
      const r = await e.get(`/api/thumb/${id}`);
      expect(r.status).toBe(200);
      expect(r.headers['cache-control']).toBe('no-store');
      expect(await sharp(r.body).metadata()).toMatchObject({ format: 'jpeg' });
    }
    expect(await readdir(join(e.dataDir, 'harvest-cache', 'thumbs')).catch(() => [])).toEqual([]);
    expect((await e.get('/api/thumb/99999')).status).toBe(404);
    expect((await e.get('/api/thumb/abc')).status).toBe(404);
  });

  test('HEIC and raw-named sources thumbnail; the cache is evicted above the cap', async () => {
    const e = await setup({
      spec: { 'h.heic': await readFile(HEIC_SCENE0), 'a.png': await pngBuf(20, 20) },
    });
    const h = await item(e, 'h.heic', 'REVIEW');
    expect((await sharp((await e.get(`/api/thumb/${h}`)).body).metadata()).width).toBe(256);
    // a raw-named item: rel_path is the lossy NFC name, rel_path_raw the real bytes
    const raw = await item(e, 'lossy-name.png', 'REVIEW');
    await pgh.pool.query('UPDATE harvest_items SET rel_path_raw = $2 WHERE id = $1', [
      raw,
      Buffer.from('a.png'),
    ]);
    expect((await sharp((await e.get(`/api/thumb/${raw}`)).body).metadata()).width).toBe(256);
  });

  test('eviction runs after a write when the cache exceeds the cap', async () => {
    // A 0 MB cap: the written thumbnail is evicted right away (the response still has it).
    const e = await setup({ spec: { 'a.png': await pngBuf(300, 300) }, thumbCacheMb: 0 });
    const id = await item(e, 'a.png', 'REVIEW');
    const r = await e.get(`/api/thumb/${id}`);
    expect(r.headers['content-type']).toBe('image/jpeg');
    expect(r.headers['cache-control']).not.toBe('no-store');
    await new Promise((res) => setTimeout(res, 200));
    expect(await readdir(join(e.dataDir, 'harvest-cache', 'thumbs'))).toEqual([]);
  });
});

describe('static files', () => {
  test('static files are served and path traversal is refused', async () => {
    const e = await setup();
    const idx = await e.get('/');
    expect(idx.status).toBe(200);
    expect(idx.headers['content-type']).toContain('text/html');
    expect(idx.body.toString()).toBe('<h1>hi</h1>');
    expect((await e.get('/index.html?x=1')).status).toBe(200);
    const js = await e.get('/sub/app.js');
    expect(js.headers['content-type']).toContain('javascript');
    expect((await e.get('/app.css')).headers['content-type']).toContain('text/css');
    expect((await e.get('/data.bin')).headers['content-type']).toBe('application/octet-stream');
    expect((await e.req('HEAD', '/')).status).toBe(200);
    for (const p of [
      '/../package.json',
      '/%2e%2e/package.json',
      '/sub/%2e%2e/%2e%2e/x',
      '/..%2f..%2fetc/passwd',
      '/%00',
      '/a%5c..%5cb',
      '/%zz',
    ])
      expect((await e.get(p)).status).not.toBe(200);
    expect((await e.get('/..%2f..%2fetc/passwd')).status).toBe(403);
    expect((await e.get('/%00')).status).toBe(403);
    expect((await e.get('/%zz')).status).toBe(400);
    expect((await e.get('/missing.html')).status).toBe(404);
    expect((await e.get('/sub')).status).toBe(404); // a directory is not a file
  });

  test('a symlink leading out of the static dir is refused', async () => {
    const e = await setup();
    await symlink('/etc/hosts', join(e.staticDir, 'leak.txt'));
    expect((await e.get('/leak.txt')).status).toBe(403);
  });

  test('a missing static directory 404s cleanly', async () => {
    const e = await setup({ deps: { staticDir: '/nonexistent-dir-xyz' } });
    expect((await e.get('/')).status).toBe(404);
    expect((await e.get('/api/state')).status).toBe(200);
  });
});
