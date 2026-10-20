import { readFileSync } from 'node:fs';
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { startPg, type PgHandle } from '../helpers/pg.js';

const DASH = 'infra/grafana/dashboards/pipeline-live.json';
const REQUIRED = [
  'Drop zone',
  'In flight',
  'Backlog by stage',
  'Throughput / min',
  'ETA',
  'Copy groups',
  'Similar groups',
  'Vault',
  'Duplicates removed',
  'Copy mismatches',
  'Quarantined by reason',
  'Failed',
  'Recent errors',
  'Last staging build',
  'Harvest sent',
  'Already in vault',
  'Harvest run',
  'Harvester heartbeat',
  'Possible photos awaiting review',
  'Harvest / min',
  '① Scan',
  '② Check & copy',
  '③ Drop zone',
  '④ Ingest',
  '⑤ Decode',
  '⑥ Cluster',
];

interface Target {
  refId: string;
  rawSql: string;
  datasource: { uid: string };
}
interface Panel {
  title: string;
  type: string;
  targets: Target[];
}
interface Dashboard {
  title: string;
  uid: string;
  refresh: string;
  panels: Panel[];
}

const loadDashboard = (): Dashboard => JSON.parse(readFileSync(DASH, 'utf8')) as Dashboard;
const loadTargets = () => loadDashboard().panels.flatMap((p) => p.targets);
function targetByTitle(title: string): Target {
  const t = loadDashboard().panels.find((p) => p.title === title)?.targets[0];
  if (!t) throw new Error(`no panel ${title}`);
  return t;
}

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
});

async function seedPipelineRows(): Promise<void> {
  const { pool } = pgh;
  const h = (n: number) => String(n).padStart(64, '0');
  const states = ['INGESTED', 'HASHED', 'CLUSTERED', 'QUARANTINED', 'FAILED'];
  for (const [i, state] of states.entries())
    await pool.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state, quarantine_reason, last_error,
                             hashed_at, clustered_at)
       VALUES ($1,'jpg','still',100,$2,$3,$4, now(), now())`,
      [
        h(i),
        state,
        state === 'QUARANTINED' ? 'truncated' : null,
        state === 'FAILED' ? 'boom' : null,
      ],
    );
  await pool.query(`INSERT INTO groups (kind) VALUES ('copy'), ('similar')`);
  await pool.query(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state, last_error)
     VALUES ('a.jpg','a.jpg',5,1,'COPY','file err'), ('b.jpg','b.jpg',5,2,'DISCOVERED',NULL)`,
  );
  await pool.query(`UPDATE scan_stats SET pending_files = 3, pending_bytes = 99 WHERE id = 1`);
  await pool.query(
    `INSERT INTO staging_builds (gen, watermark, built_at, entries) VALUES (1, now(), now(), 4)`,
  );
}

test('every pixelmagic panel query runs against a seeded DB', async () => {
  await seedPipelineRows();
  const targets = loadTargets().filter((t) => t.datasource.uid === 'pixelmagic');
  expect(targets.length).toBeGreaterThanOrEqual(10);
  for (const t of targets) {
    expect(t.rawSql).not.toMatch(/\$__/); // no Grafana macros: the test runs them verbatim
    await expect(pgh.pool.query(t.rawSql)).resolves.toBeDefined();
  }
});

test('queries return the seeded numbers', async () => {
  await seedPipelineRows();
  const value = async (title: string) =>
    Number((await pgh.pool.query<{ value: unknown }>(targetByTitle(title).rawSql)).rows[0]!.value);
  expect(await value('Failed')).toBe(1);
  expect(await value('Copy groups')).toBe(0); // no content shares a copy group here
  expect(await value('Similar groups')).toBe(0); // a similar row with no copy groups is no set (below)
  // backlog = INGESTED + HASHED stills (2); 2 clustered rows in the last 10 min -> ... finite ETA
  expect(Number.isFinite(await value('ETA'))).toBe(true);
  const dz = (await pgh.pool.query<Record<string, unknown>>(targetByTitle('Drop zone').rawSql))
    .rows[0]!;
  expect([Number(dz.pending_files), Number(dz.pending_bytes)]).toEqual([3, 99]);
});

test('visibility panel queries run against a stub executions_visibility', async () => {
  const vis = loadTargets().filter((t) => t.datasource.uid === 'temporal_visibility');
  expect(vis.length).toBeGreaterThan(0);
  const c = await pgh.pool.connect();
  try {
    await c.query(`CREATE SCHEMA vis; SET search_path = vis; CREATE TABLE executions_visibility (namespace_id text, run_id text, workflow_id text,
                   workflow_type_name text, status int, start_time timestamptz, close_time timestamptz)`);
    await c.query(
      `INSERT INTO executions_visibility VALUES ('n','r1','file-1','FileWorkflow',1,now(),NULL),
        ('n','r2','file-2','FileWorkflow',2,now(),now()), ('n','r3','pm-scan','ScanWorkflow',1,now(),NULL)`,
    );
    for (const t of vis) {
      const r = await c.query<{ value: unknown }>(t.rawSql);
      expect(Number(r.rows[0]!.value)).toBe(1);
    }
  } finally {
    await c.query('RESET search_path; DROP SCHEMA vis CASCADE');
    c.release();
  }
});

// Task 13 chaos: copy groups linked as a similar set can later merge into one copy group. That
// set holds a single group and stages as unique (stagingPlan), so it is not counted either.
test('Similar groups counts only sets of two or more root copy groups', async () => {
  const { pool } = pgh;
  const id = async (sql: string, args: unknown[] = []) =>
    (await pool.query<{ id: bigint }>(`${sql} RETURNING id`, args)).rows[0]!.id;
  const real = await id(`INSERT INTO groups (kind) VALUES ('similar')`);
  for (let i = 0; i < 2; i++)
    await id(`INSERT INTO groups (kind, similar_set_id) VALUES ('copy', $1)`, [real]);
  const lone = await id(`INSERT INTO groups (kind) VALUES ('similar')`);
  const root = await id(`INSERT INTO groups (kind, similar_set_id) VALUES ('copy', $1)`, [lone]);
  await id(`INSERT INTO groups (kind, merged_into, similar_set_id) VALUES ('copy', $1, $2)`, [
    root,
    lone,
  ]);
  const r = await pool.query<{ value: unknown }>(targetByTitle('Similar groups').rawSql);
  expect(Number(r.rows[0]!.value)).toBe(1);
});

const row = async (title: string) =>
  (await pgh.pool.query<Record<string, unknown>>(targetByTitle(title).rawSql)).rows[0]!;

async function copyRow(o: { size: number; removed?: number; mismatch?: boolean; state?: string }) {
  await pgh.pool.query(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state, copy_removed_at,
                        copy_reclaimed_bytes, copy_mismatch)
     VALUES ('x', 'x', $1, $2, $3, $4, $5, $6)`,
    [
      o.size,
      Math.floor(Math.random() * 1e12),
      o.state ?? 'COPY',
      o.removed === undefined ? null : new Date(),
      o.removed ?? null,
      o.mismatch ?? false,
    ],
  );
}

test('Duplicates removed: count and bytes reclaimed, plus the bytes still pending', async () => {
  await copyRow({ size: 100, removed: 100 });
  await copyRow({ size: 50, removed: 50 });
  await copyRow({ size: 30 }); // pending removal
  await copyRow({ size: 20, mismatch: true }); // kept: never counted as reclaimable
  await copyRow({ size: 999, state: 'INGESTED' });
  const r = await row('Duplicates removed');
  expect([r.removed, r.reclaimed_bytes, r.pending_bytes].map(Number)).toEqual([2, 150, 30]);
});

test('Duplicates removed is all zeros on an empty database', async () => {
  const r = await row('Duplicates removed');
  expect([r.removed, r.reclaimed_bytes, r.pending_bytes].map(Number)).toEqual([0, 0, 0]);
});

test('Copy mismatches counts flagged rows (and reads 0 normally)', async () => {
  expect(Number((await row('Copy mismatches')).value)).toBe(0);
  await copyRow({ size: 20, mismatch: true });
  await copyRow({ size: 20, removed: 20 });
  expect(Number((await row('Copy mismatches')).value)).toBe(1);
});

test('Copy groups counts only groups of two or more contents', async () => {
  const { pool } = pgh;
  const g = async () =>
    (await pool.query<{ id: bigint }>(`INSERT INTO groups (kind) VALUES ('copy') RETURNING id`))
      .rows[0]!.id;
  const pair = await g();
  const lone = await g();
  const h = (n: number) => String(n).padStart(64, '0');
  for (const [i, group] of [pair, pair, lone].entries())
    await pool.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state, copy_group_id)
       VALUES ($1, 'jpg', 'still', 10, 'CLUSTERED', $2)`,
      [h(i), group],
    );
  expect(Number((await row('Copy groups')).value)).toBe(1);
});

test('Vault counts every stored content and its bytes', async () => {
  expect([(await row('Vault')).files, (await row('Vault')).bytes].map(Number)).toEqual([0, 0]);
  await seedPipelineRows(); // 5 contents of 100 bytes
  const r = await row('Vault');
  expect([r.files, r.bytes].map(Number)).toEqual([5, 500]);
});

async function harvestItem(state: string, n: number, sentAt: string | null = null) {
  const { pool } = pgh;
  const src = await pool.query<{ id: string }>(
    `INSERT INTO harvest_sources (identity, label, kind) VALUES ('i', 'Exp', 'local')
     ON CONFLICT (identity) DO UPDATE SET kind = 'local' RETURNING id`,
  );
  await pool.query(
    `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state, sent_at)
     VALUES ($1, $2, $2, 1, $3, 'photo', $4, $5)`,
    [src.rows[0]!.id, `f${String(n)}`, n, state, sentAt],
  );
}

test('Harvest tiles read 0 / idle on a database without harvest rows', async () => {
  expect(
    [(await row('Harvest sent')).today, (await row('Harvest sent')).total].map(Number),
  ).toEqual([0, 0]);
  expect(Number((await row('Already in vault')).value)).toBe(0);
  expect((await row('Harvest run')).value).toBe('idle');
  expect(Number((await row('Harvester heartbeat')).value)).toBe(0);
  expect(Number((await row('Possible photos awaiting review')).value)).toBe(0);
});

test('Harvest tiles count items by state and report the active run', async () => {
  await harvestItem('SENT', 1, new Date().toISOString());
  await harvestItem('SENT', 2, new Date().toISOString());
  await harvestItem('SENT', 3, '2020-01-01T00:00:00Z'); // sent long ago: total only
  await harvestItem('KNOWN', 4);
  await harvestItem('KNOWN', 5);
  await harvestItem('REVIEW', 6);
  await harvestItem('PLANNED', 7);
  const sent = await row('Harvest sent');
  expect([sent.today, sent.total].map(Number)).toEqual([2, 3]);
  expect(Number((await row('Already in vault')).value)).toBe(2);
  expect(Number((await row('Possible photos awaiting review')).value)).toBe(1);

  const { pool } = pgh;
  await pool.query(`INSERT INTO harvest_runs (state) VALUES ('done')`);
  expect((await row('Harvest run')).value).toBe('idle'); // a finished run is not active
  await pool.query(
    `INSERT INTO harvest_runs (state, paused_reason) VALUES ('paused', 'disk 81.0 %')`,
  );
  expect((await row('Harvest run')).value).toBe('paused (disk 81.0 %)');
  await pool.query(`UPDATE harvest_runs SET state = 'sending', paused_reason = NULL WHERE id = 2`);
  expect((await row('Harvest run')).value).toBe('sending');
});

test('Harvester heartbeat: seconds old while a run is active, 0 when idle, red above 300', async () => {
  const { pool } = pgh;
  const hb = async () => Number((await row('Harvester heartbeat')).value);
  await pool.query(`UPDATE harvest_host_state SET heartbeat_at = now() - interval '10 minutes'`);
  expect(await hb()).toBe(0); // no active run: a stale heartbeat is not an alarm
  await pool.query(`INSERT INTO harvest_runs (state) VALUES ('scanning')`);
  expect(await hb()).toBeGreaterThan(590);
  await pool.query(`UPDATE harvest_host_state SET heartbeat_at = now()`);
  expect(await hb()).toBeLessThan(5);
  await pool.query(`UPDATE harvest_host_state SET heartbeat_at = NULL`);
  expect(await hb()).toBeGreaterThan(300); // never beat while a run is active: red
  const panel = loadDashboard().panels.find(
    (p) => p.title === 'Harvester heartbeat',
  ) as unknown as {
    fieldConfig: { defaults: { thresholds: { steps: { color: string; value: number | null }[] } } };
  };
  expect(panel.fieldConfig.defaults.thresholds.steps).toEqual([
    { color: 'green', value: null },
    { color: 'red', value: 300 },
  ]);
});

test('the Harvest page link panel points at the local page', () => {
  const text = JSON.stringify(loadDashboard().panels.filter((p) => p.type === 'text'));
  expect(text).toContain('http://127.0.0.1:3020');
});

test('dashboard has required panels and 5s refresh', () => {
  const d = loadDashboard();
  expect(d.title).toBe('Pixel Magic: Pipeline Live');
  expect(d.panels.map((p) => p.title)).toEqual(expect.arrayContaining(REQUIRED));
  expect(d.refresh).toBe('5s');
});

describe('Now strip', () => {
  const text = async (title: string) => String((await row(title)).value);
  const NOW = ['① Scan', '② Check & copy', '③ Drop zone', '④ Ingest', '⑤ Decode', '⑥ Cluster'];

  /** A run over the whole of source `Exp` (rel_path ''), with its folder counters. */
  async function run(
    state: string,
    o: { folder?: string; counters?: Partial<Record<string, number>>; dryRun?: boolean } = {},
  ) {
    const { pool } = pgh;
    const src = await pool.query<{ id: string }>(
      `INSERT INTO harvest_sources (identity, label, kind) VALUES ('i', 'Exp', 'local')
       ON CONFLICT (identity) DO UPDATE SET kind = 'local' RETURNING id`,
    );
    const folder = await pool.query<{ id: string }>(
      `INSERT INTO harvest_folders (source_id, rel_path) VALUES ($1, '') RETURNING id`,
      [src.rows[0]!.id],
    );
    const r = await pool.query<{ id: string }>(
      `INSERT INTO harvest_runs (state, dry_run) VALUES ($1, $2) RETURNING id`,
      [state, o.dryRun ?? false],
    );
    const c = { dirs: 0, photos: 0, sent: 0, known: 0, dups: 0, ...o.counters };
    await pool.query(
      `INSERT INTO harvest_run_folders (run_id, folder_id, position, state, dirs, photos, sent, known, dups)
       VALUES ($1, $2, 0, $3, $4, $5, $6, $7, $8)`,
      [
        r.rows[0]!.id,
        folder.rows[0]!.id,
        o.folder ?? 'sending',
        c.dirs,
        c.photos,
        c.sent,
        c.known,
        c.dups,
      ],
    );
    return r.rows[0]!.id;
  }

  /** `n` harvest items in `state`, decided `ago` (a Postgres interval) or undecided (null). */
  async function items(state: string, n: number, ago: string | null, from = 0) {
    const { pool } = pgh;
    await harvestItem(state, from); // makes the source
    await pool.query(
      `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state)
       SELECT (SELECT id FROM harvest_sources), 'f' || g, 'f' || g, 1, g, 'photo', $1
       FROM generate_series($2::int + 1, $3::int - 1) g`,
      [state, from, from + n],
    );
    if (ago !== null)
      await pool.query(
        `UPDATE harvest_items SET decided_at = now() - $1::interval
         WHERE mtime_ns >= $2 AND mtime_ns < $3`,
        [ago, from, from + n],
      );
  }

  test('every tile reads idle or empty on an empty database, in grey', async () => {
    expect(await Promise.all(NOW.map(text))).toEqual([
      'idle',
      'idle',
      'empty',
      'idle',
      'idle',
      'idle',
    ]);
  });

  test('scan: folders and photos while scanning, the reason while paused, done after', async () => {
    const id = await run('scanning', {
      folder: 'scanning',
      counters: { dirs: 74821, photos: 622804 },
    });
    expect(await text('① Scan')).toBe('scanning · 74,821 folders · 622,804 photos');
    await pgh.pool.query(
      `UPDATE harvest_runs SET state = 'paused', paused_reason = 'source unplugged' WHERE id = $1`,
      [id],
    );
    expect(await text('① Scan')).toBe(
      'paused (source unplugged) · 74,821 folders · 622,804 photos',
    );
    await pgh.pool.query(`UPDATE harvest_runs SET state = 'sending', paused_reason = NULL`);
    await pgh.pool.query(`UPDATE harvest_run_folders SET state = 'sending'`);
    expect(await text('① Scan')).toBe('done · 622,804 photos found');
  });

  test('check & copy: % done, left, rate over the last 10 min and the time left', async () => {
    await run('sending', { counters: { sent: 100, known: 200, dups: 0 } });
    await items('PLANNED', 300, null);
    await items('SENT', 100, '9 minutes 30 seconds', 300);
    await items('KNOWN', 200, '30 seconds', 400);
    await items('KNOWN', 50, '2 hours', 600); // outside the window: not in the rate
    // 300 over the 9.5 minutes since the oldest one: 31.6/min (31 if the test runs slowly)
    expect(await text('② Check & copy')).toMatch(/^working · 50% · 300 left · 3[12]\/min · ~10m$/);
  });

  test('check & copy: the rate counts from the first decision when the window is not full yet', async () => {
    await run('sending', { counters: { known: 120 } });
    await items('PLANNED', 7140, null);
    await items('KNOWN', 120, '30 seconds', 7140); // a window under a minute counts as one
    expect(await text('② Check & copy')).toBe('working · 1% · 7,140 left · 120/min · ~1h 0m');
  });

  test('check & copy while scanning: what is left so far, no time left (the total grows)', async () => {
    await run('scanning', { folder: 'scanning' });
    await items('PLANNED', 40, null);
    expect(await text('② Check & copy')).toBe('waiting · 40 left');
    await items('SENT', 20, '30 seconds', 40);
    expect(await text('② Check & copy')).toBe('working while scanning · 40 left so far · 20/min');
  });

  test('check & copy: waiting for the scan, paused with its reason, dry run', async () => {
    const id = await run('scanning', { folder: 'scanning' });
    expect(await text('② Check & copy')).toBe('waiting for the scan to find photos');
    await items('PLANNED', 3, null);
    await pgh.pool.query(
      `UPDATE harvest_runs SET state = 'paused', paused_reason = 'disk 81.0 %' WHERE id = $1`,
      [id],
    );
    expect(await text('② Check & copy')).toBe('paused (disk 81.0 %) · 3 left');
    await pgh.pool.query(`UPDATE harvest_runs SET dry_run = true`);
    expect(await text('② Check & copy')).toBe('dry run (nothing is copied)');
  });

  test("check & copy counts only PLANNED items under the run's folder", async () => {
    const { pool } = pgh;
    await run('sending');
    await pool.query(`UPDATE harvest_folders SET rel_path = 'DCIM'`);
    const src = (await pool.query<{ id: string }>(`SELECT id FROM harvest_sources`)).rows[0]!.id;
    for (const [rel, key] of [
      ['DCIM/a.jpg', 'dcim/a.jpg'],
      ['dcim/B.jpg', 'dcim/b.jpg'], // another case spelling of the folder: in
      ['DCIMX/c.jpg', 'dcimx/c.jpg'], // a sibling sharing the prefix: out
      ['Other/d.jpg', 'other/d.jpg'],
    ])
      await pool.query(
        `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state)
         VALUES ($1, $2, $3, 1, 1, 'photo', 'PLANNED')`,
        [src, rel, key],
      );
    expect(await text('② Check & copy')).toBe('waiting · 2 left');
  });

  test('drop zone: files and size, or empty', async () => {
    await pgh.pool.query(`UPDATE scan_stats SET pending_files = 1240, pending_bytes = 4194304`);
    expect(await text('③ Drop zone')).toBe('queued · 1,240 files · 4096 kB');
  });

  test('pipeline stages: working with a rate, waiting with a backlog, idle', async () => {
    const { pool } = pgh;
    const h = (n: number) => String(n).padStart(64, '0');
    for (let i = 0; i < 10; i++)
      await pool.query(
        `INSERT INTO contents (sha256, ext, kind, size_bytes, state, hashed_at)
         VALUES ($1, 'jpg', 'still', 1, $2, $3)`,
        [h(i), i < 4 ? 'INGESTED' : 'HASHED', i < 4 ? null : new Date()],
      );
    // 6 decoded in the last 5 minutes -> 2/min (rounded up); 4 still waiting
    expect(await text('⑤ Decode')).toBe('working · 4 waiting · 2/min');
    expect(await text('⑥ Cluster')).toBe('waiting · 6 queued'); // 6 HASHED, none clustered
    await pool.query(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state)
       VALUES ('a.jpg', 'a.jpg', 1, 1, 'DISCOVERED'), ('b.jpg', 'b.jpg', 1, 2, 'INGESTED')`,
    );
    expect(await text('④ Ingest')).toBe('working · 1 waiting · 1/min');
    await pool.query(`DELETE FROM files WHERE state = 'INGESTED'`); // nothing finished lately
    expect(await text('④ Ingest')).toBe('waiting · 1 queued');
  });

  test('tiles colour by their first word: green working, orange waiting/paused, grey idle', () => {
    const color = (title: string, value: string) => {
      const p = loadDashboard().panels.find((x) => x.title === title) as unknown as {
        fieldConfig: {
          defaults: { mappings: { options: { pattern: string; result: { color: string } } }[] };
        };
      };
      const m = p.fieldConfig.defaults.mappings.find((x) =>
        new RegExp(x.options.pattern).test(value),
      );
      return m?.options.result.color ?? 'green';
    };
    for (const t of NOW) {
      expect(color(t, 'working · 3 left')).toBe('green');
      expect(color(t, 'scanning · 1 folders · 2 photos')).toBe('green');
      expect(color(t, 'queued · 1 files · 1 kB')).toBe('green');
      expect(color(t, 'waiting · 3 left')).toBe('orange');
      expect(color(t, 'paused (disk 81.0 %) · 3 left')).toBe('orange');
      for (const grey of [
        'idle',
        'empty',
        'done · 3 photos found',
        'skipped (sending approved photos)',
      ])
        expect(color(t, grey)).toBe('#8e8e8e');
    }
  });

  test('Harvest / min: decisions per minute by outcome', async () => {
    await items('SENT', 2, '1 minute');
    await items('KNOWN', 3, '1 minute', 2);
    await items('DUP', 1, '1 minute', 5);
    await items('KNOWN', 4, '2 hours', 6); // older than the chart
    await items('PLANNED', 1, null, 10);
    const r = await pgh.pool.query<{ metric: string; value: string }>(
      targetByTitle('Harvest / min').rawSql,
    );
    const by = Object.fromEntries(r.rows.map((x) => [x.metric, Number(x.value)]));
    expect(by).toEqual({ copied: 2, 'already in vault': 3, duplicate: 1 });
  });

  test('layout: the Now strip on top, then Harvest, then Pipeline; no panels overlap', () => {
    const panels = loadDashboard().panels as unknown as (Panel & {
      gridPos: { x: number; y: number; w: number; h: number };
    })[];
    const rows = panels.filter((p) => p.type === 'row').map((p) => [p.title, p.gridPos.y]);
    expect(rows).toEqual([
      ['Now: each step, in order', 0],
      ['Harvest', 4],
      ['Pipeline', 17],
    ]);
    for (const t of NOW) expect(panels.find((p) => p.title === t)!.gridPos.y).toBe(1);
    const cells = new Set<string>();
    for (const p of panels) {
      const g = p.gridPos;
      expect(g.x + g.w).toBeLessThanOrEqual(24);
      for (let x = g.x; x < g.x + g.w; x++)
        for (let y = g.y; y < g.y + g.h; y++) {
          const k = `${x},${y}`;
          expect(cells.has(k), `${p.title} overlaps at ${k}`).toBe(false);
          cells.add(k);
        }
    }
  });
});
