// Harvest tier (spec §10): the harvester's activities and workflow run in this process against
// a real throwaway compose stack, over a fixture "drive" cloned from the corpus. The vault must
// hold each distinct photo once, duplicates and junk must never be sent, possible photos wait
// for review, the source stays untouched, and a second harvest sends nothing.
import { randomBytes } from 'node:crypto';
import { mkdir, rm, statfs, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Client, Connection } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import type pg from 'pg';
import { afterAll, beforeAll, expect, test } from 'vitest';
import { createPool } from '../../src/db.js';
import { DataLayout } from '../../src/fsops.js';
import { childContext, classify, ROOT_CONTEXT } from '../../src/harvest/classify.js';
import { createHarvestActivities } from '../../src/harvest/activities.js';
import { loadHarvestSettings, type HarvestSettings } from '../../src/harvest/config.js';
import { HARVEST_TASK_QUEUE, temporalRunControl } from '../../src/harvest/control.js';
import { createRun, startNextQueued } from '../../src/harvest/runs.js';
import {
  detectSources,
  registerSources,
  type DetectOptions,
  type SystemProbe,
} from '../../src/harvest/sources.js';
import { PATHS, loadManifest, seed, selectSubsets, type SeedFile } from '../../tools/corpus/lib.js';
import { hasCorpus } from '../helpers/corpus.js';
import { lockReadOnly, snapshotTree } from '../helpers/sourceTree.js';
import { Stack } from '../lib/stack.js';

const REPO = fileURLToPath(new URL('../../', import.meta.url));
const SUBSET_SEED = 7;
const KIB = 1024;
const RUN_TIMEOUT_MS = 20 * 60_000;

let stack: Stack;
let pool: pg.Pool;
let settings: HarvestSettings;
let sourceId: string;
let drive: string;
let unlock: () => Promise<void>;
let before: string[];
let failed = false;
let worker: Worker;
let workerDone: Promise<void>;
const closers: (() => Promise<unknown>)[] = [];
let control: ReturnType<typeof temporalRunControl>;
const runId = `harvest-${Date.now().toString(36)}`;
const srcParent = join(REPO, 'data-test', `${runId}-src`);

/** Expected ledger numbers, derived from the fixture rather than from the pipeline. */
const expected = {
  photoSha: new Set<string>(), // distinct sha256 of every file the harvester must treat as a photo
  photoFiles: 0, // photo files in the fixture, copies included
  maybeFiles: 0, // possible photos in the fixture
  junk: {} as Record<string, number>,
};

const fakeSystem = (mount: string): SystemProbe => ({
  mountOutput: () => Promise.resolve(`/dev/disk99s1 on ${mount} (apfs, local, journaled)\n`),
  diskutilInfo: () =>
    Promise.resolve({ BusProtocol: 'USB', VolumeUUID: 'TEST-UUID', VolumeName: 'Drive' }),
  statfs: async (path) => {
    const s = await statfs(path, { bigint: true });
    return { blocks: s.blocks, bavail: s.bavail, bsize: s.bsize };
  },
});

const bump = (m: Record<string, number>, k: string) => {
  m[k] = (m[k] ?? 0) + 1;
};

async function buildFixture(): Promise<void> {
  const manifest = await loadManifest();
  const names = selectSubsets(manifest, SUBSET_SEED).smoke;
  drive = join(srcParent, 'Drive');
  const photos = join(drive, 'Photos');
  const first = await seed({ corpusDir: PATHS.corpus, names, dest: photos });
  const twins = await seed({ corpusDir: PATHS.corpus, names, dest: join(photos, 'copy') });

  // Ground truth for the corpus files, by the §6.3 rules (Photos/ is a photo folder).
  const photoDir = childContext(ROOT_CONTEXT, 'Photos', []);
  const verdicts = async (files: SeedFile[], dir: typeof photoDir) => {
    for (const f of files) {
      const v = await classify({ name: f.name, size: Number(f.size) }, dir, 20 * KIB, () =>
        Promise.resolve(null),
      );
      if (v.kind === 'photo') {
        expected.photoFiles++;
        expected.photoSha.add(f.sha256);
      } else if (v.kind === 'maybe') expected.maybeFiles++;
      else if (v.kind === 'junk') bump(expected.junk, v.reason);
    }
  };
  await verdicts(first.files, photoDir);
  await verdicts(twins.files, childContext(photoDir, 'copy', []));

  const junk = async (rel: string, bytes: number) => {
    const p = join(drive, rel);
    await mkdir(join(p, '..'), { recursive: true });
    await writeFile(p, randomBytes(bytes));
  };
  await junk('Photos/._x.jpg', 25 * KIB);
  bump(expected.junk, 'appledouble');
  await junk('Photos/tiny.png', KIB);
  bump(expected.junk, 'too-small');
  await junk('Photos/cover.jpg', 25 * KIB);
  bump(expected.junk, 'album-art');
  await junk('Photos/Screenshot 2020.png', 25 * KIB);
  bump(expected.junk, 'screenshot');
  await junk('proj/package.json', 2);
  await junk('proj/icon.png', 25 * KIB);
  bump(expected.junk, 'in-project');
  await junk('misc/odd.png', 25 * KIB); // outside a photo folder: a possible photo
  expected.maybeFiles++;
  unlock = await lockReadOnly(srcParent);
  before = await snapshotTree(drive);
}

async function waitState(id: string, state: string): Promise<void> {
  const deadline = Date.now() + RUN_TIMEOUT_MS;
  for (;;) {
    const r = await pool.query<{ state: string; error: string | null }>(
      'SELECT state, error FROM harvest_runs WHERE id = $1',
      [id],
    );
    const s = r.rows[0]?.state;
    if (s === state) return;
    if (s === 'failed' || s === 'cancelled')
      throw new Error(`run ${id} ${s}: ${r.rows[0]?.error ?? ''}`);
    if (Date.now() > deadline) throw new Error(`run ${id} still ${String(s)} after the timeout`);
    await sleep(1000);
  }
}

/** A new run over the whole drive (as the server's "harvest" button does); returns when done. */
async function harvest(): Promise<string> {
  const id = await createRun(pool, [{ sourceId, relPath: '' }], false);
  await startNextQueued(pool, control);
  await waitState(id, 'done');
  await stack.waitIdle();
  return id;
}

interface VaultNumbers {
  files: number;
  bytes: number;
  contents: number;
}
async function vault(): Promise<VaultNumbers> {
  const f = await pool.query<{ n: string; b: string | null }>(
    'SELECT count(*)::text AS n, sum(size_bytes)::text AS b FROM files',
  );
  const c = await pool.query<{ n: string }>('SELECT count(*)::text AS n FROM contents');
  return {
    files: Number(f.rows[0]!.n),
    bytes: Number(f.rows[0]!.b ?? 0),
    contents: Number(c.rows[0]!.n),
  };
}

const counts = async (id: string) =>
  (
    await pool.query<Record<string, string> & { junk: Record<string, number> }>(
      `SELECT photos::text, maybes::text, sent::text, known::text, dups::text, errors::text,
              bytes_sent::text, junk FROM harvest_run_folders WHERE run_id = $1`,
      [id],
    )
  ).rows[0]!;

beforeAll(async () => {
  if (!hasCorpus) throw new Error('the harvest tier needs the corpus (test-automation/)');
  await buildFixture();
  stack = new Stack(runId, join(REPO, 'data-test', runId));
  try {
    await stack.up();
  } catch (e) {
    failed = true;
    throw e;
  }
  settings = loadHarvestSettings({
    DATA_DIR: stack.dataDir,
    DATABASE_URL: stack.dbUrl,
    TEMPORAL_ADDRESS: stack.temporalAddress,
  });
  pool = createPool(settings.databaseUrl, 8);
  closers.push(() => pool.end());
  const detectOpts: DetectOptions = { volumes: `${srcParent}/` };
  const sys = fakeSystem(drive);
  const detected = await detectSources(sys, settings.dataDir, detectOpts);
  expect(detected.map((d) => d.identity)).toEqual(['local:TEST-UUID']);
  const row = (await registerSources(pool, detected)).find((r) => r.identity === 'local:TEST-UUID');
  sourceId = row!.id;

  const connection = await Connection.connect({ address: settings.temporalAddress });
  closers.push(() => connection.close());
  control = temporalRunControl(new Client({ connection, namespace: settings.temporalNamespace }));
  const native = await NativeConnection.connect({ address: settings.temporalAddress });
  closers.push(() => native.close());
  const layout = new DataLayout(settings.dataDir);
  await layout.ensure();
  worker = await Worker.create({
    connection: native,
    namespace: settings.temporalNamespace,
    taskQueue: HARVEST_TASK_QUEUE,
    workflowsPath: join(REPO, 'dist', 'workflows.js'),
    activities: createHarvestActivities({
      pool,
      settings,
      layout,
      sys,
      temporal: control,
      detectOpts,
    }),
  });
  workerDone = worker.run();
}, 60 * 60_000);

afterAll(async () => {
  worker?.shutdown();
  await workerDone?.catch(() => {});
  for (const c of closers.reverse()) await c().catch(() => {});
  if (unlock) await unlock();
  await rm(srcParent, { recursive: true, force: true });
  if (stack) await stack.down(failed);
}, 10 * 60_000);

let first: string;
let afterFirst: VaultNumbers;

test('harvest 1: each distinct photo is in the vault once; dups and junk were not sent', async () => {
  try {
    first = await harvest();
    const c = await counts(first);
    const distinct = expected.photoSha.size;
    process.stdout.write(`[harvest 1] ${JSON.stringify({ ...c, distinct })}\n`);
    expect(distinct).toBeGreaterThan(50);
    expect(Number(c['photos'])).toBe(expected.photoFiles);
    expect(Number(c['sent'])).toBe(distinct);
    expect(Number(c['dups'])).toBe(expected.photoFiles - distinct);
    expect(Number(c['known'])).toBe(0);
    expect(Number(c['errors'])).toBe(0);
    expect(c.junk).toEqual(expected.junk);
    expect(Number(c['maybes'])).toBe(expected.maybeFiles);

    // The vault: every distinct photo exactly once, nothing else.
    const contents = await pool.query<{ sha256: string }>('SELECT sha256 FROM contents');
    expect(contents.rows.map((r) => r.sha256).sort()).toEqual([...expected.photoSha].sort());
    const files = await pool.query<{ sha256: string | null; n: string }>(
      'SELECT sha256, count(*)::text AS n FROM files GROUP BY sha256',
    );
    expect(files.rows.length).toBe(distinct);
    expect(files.rows.every((r) => r.n === '1')).toBe(true);
    const states = await pool.query<{ state: string; n: string }>(
      'SELECT state, count(*)::text AS n FROM files GROUP BY state',
    );
    expect(states.rows.filter((r) => r.state !== 'INGESTED' && r.state !== 'QUARANTINED')).toEqual(
      [],
    );

    // The ledger.
    const items = await pool.query<{ state: string; n: string }>(
      'SELECT state, count(*)::text AS n FROM harvest_items GROUP BY state',
    );
    const by = Object.fromEntries(items.rows.map((r) => [r.state, Number(r.n)]));
    expect(by).toEqual({
      SENT: distinct,
      DUP: expected.photoFiles - distinct,
      REVIEW: expected.maybeFiles,
    });
    const unsent = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM harvest_items
        WHERE state IN ('DUP', 'REVIEW') AND (dropzone_path IS NOT NULL OR sent_at IS NOT NULL)`,
    );
    expect(unsent.rows[0]!.n).toBe('0');
    const odd = await pool.query<{ state: string }>(
      "SELECT state FROM harvest_items WHERE rel_path = 'misc/odd.png'",
    );
    expect(odd.rows).toEqual([{ state: 'REVIEW' }]);
    const twinWins = await pool.query<{ n: string }>(
      "SELECT count(*)::text AS n FROM harvest_items WHERE state = 'DUP' AND rel_path NOT LIKE 'Photos/copy/%'",
    );
    expect(twinWins.rows[0]!.n).toBe('0'); // the copy/ twins are the DUP side

    expect(await snapshotTree(drive)).toEqual(before);
    afterFirst = await vault();
    expect(afterFirst.contents).toBe(distinct);
  } catch (e) {
    failed = true;
    throw e;
  }
});

test('harvest 2: nothing is sent and the vault is unchanged', async () => {
  try {
    const second = await harvest();
    expect(second).not.toBe(first);
    const c = await counts(second);
    expect(Number(c['sent'])).toBe(0);
    expect(Number(c['bytes_sent'])).toBe(0);
    expect(Number(c['errors'])).toBe(0);
    expect(await vault()).toEqual(afterFirst);
    expect(await snapshotTree(drive)).toEqual(before);
  } catch (e) {
    failed = true;
    throw e;
  }
});
