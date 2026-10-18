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
  'Similar sets',
  '≈ Reclaimable',
  'Quarantined by reason',
  'Failed',
  'Recent errors',
  'Last staging build',
];

interface Target {
  refId: string;
  rawSql: string;
  datasource: { uid: string };
}
interface Panel {
  title: string;
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
  expect(await value('Copy groups')).toBe(1);
  expect(await value('Similar sets')).toBe(0); // a similar row with no copy groups is no set (below)
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
test('Similar sets counts only sets of two or more root copy groups', async () => {
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
  const r = await pool.query<{ value: unknown }>(targetByTitle('Similar sets').rawSql);
  expect(Number(r.rows[0]!.value)).toBe(1);
});

test('≈ Reclaimable uses the approximate formula', async () => {
  const { pool } = pgh;
  const g = (
    await pool.query<{ id: bigint }>(`INSERT INTO groups (kind) VALUES ('copy') RETURNING id`)
  ).rows[0]!.id;
  // a merged (non-root) group must not count
  await pool.query(`INSERT INTO groups (kind, merged_into) VALUES ('copy', $1)`, [g]);
  for (const [i, size] of [100, 40].entries())
    await pool.query(
      `INSERT INTO contents (sha256, ext, kind, size_bytes, state, copy_group_id)
       VALUES ($1,'jpg','still',$2,'CLUSTERED',$3)`,
      [String(i).padStart(64, '0'), size, g],
    );
  await pool.query(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state)
     VALUES ('c.jpg','c.jpg',100,1,'COPY')`,
  );
  const r = await pool.query<{ value: unknown }>(targetByTitle('≈ Reclaimable').rawSql);
  expect(Number(r.rows[0]!.value)).toBe(140);
});

test('≈ Reclaimable is 0 on an empty database', async () => {
  const r = await pgh.pool.query<{ value: unknown }>(targetByTitle('≈ Reclaimable').rawSql);
  expect(Number(r.rows[0]!.value)).toBe(0);
});

test('dashboard has required panels and 5s refresh', () => {
  const d = loadDashboard();
  expect(d.title).toBe('Pixel Magic: Pipeline Live');
  expect(d.panels.map((p) => p.title)).toEqual(expect.arrayContaining(REQUIRED));
  expect(d.refresh).toBe('5s');
});
