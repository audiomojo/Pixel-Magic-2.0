import { readFileSync } from 'node:fs';
import { afterAll, beforeAll, beforeEach, expect, test } from 'vitest';
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
