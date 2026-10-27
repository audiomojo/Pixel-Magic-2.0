// The harvester page in a real browser (Chromium via Playwright): the real server and page
// files against a throwaway Postgres and a fake drive. Temporal is a recording fake.
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, type Browser, type BrowserContext, type Page } from 'playwright';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test } from 'vitest';
import { DataLayout } from '../../src/fsops.js';
import type { RunControl } from '../../src/harvest/activities.js';
import { loadHarvestSettings } from '../../src/harvest/config.js';
import { createRun, finishRunRows, setRunState } from '../../src/harvest/runs.js';
import { createHarvestServer } from '../../src/harvest/server.js';
import { registerSources, type SystemProbe } from '../../src/harvest/sources.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';
import { makeSourceTree, type TreeSpec } from '../helpers/sourceTree.js';

const UI_DIR = fileURLToPath(new URL('../../src/harvest/ui', import.meta.url));

let pgh: PgHandle;
let browser: Browser;
beforeAll(async () => {
  [pgh, browser] = await Promise.all([startPg(), chromium.launch()]);
}, 120_000);
afterAll(async () => {
  await browser.close();
  await pgh.stop();
});
beforeEach(() => pgh.truncate());

const servers: http.Server[] = [];
const contexts: BrowserContext[] = [];
/** Uncaught page errors and console errors: every test ends with none. */
let pageErrors: string[] = [];
afterEach(async () => {
  await Promise.all(contexts.splice(0).map((c) => c.close()));
  for (const s of servers.splice(0)) {
    s.closeAllConnections();
    await new Promise((r) => s.close(r));
  }
  const errors = pageErrors;
  pageErrors = [];
  expect(errors).toEqual([]);
});

const TREE: TreeSpec = {
  'DCIM/100APPLE/a.jpg': 10,
  'Photos/b.jpg': 10,
  'Pics/a.png': 10,
  'Pics/b.png': 10,
  'node_modules/pkg/i.js': 5,
};

interface Env {
  page: Page;
  sourceId: string;
  /** Calls made to the fake Temporal control, e.g. `start 1`, `signal 1 pause`. */
  calls: string[];
  /** Opens a page route (`#/review`, …) and waits for the first paint. */
  open(hash?: string): Promise<void>;
}

async function setup(o: { spec?: TreeSpec; temporal?: boolean } = {}): Promise<Env> {
  const root = await makeSourceTree(o.spec ?? TREE);
  const sys: SystemProbe = {
    mountOutput: () => Promise.resolve(`/dev/disk9s1 on ${root} (exfat, local, nodev)\n`),
    diskutilInfo: () => Promise.resolve({ VolumeUUID: 'U-1', VolumeName: 'Drive' }),
    statfs: () => Promise.resolve({ blocks: 1000n, bavail: 600n, bsize: 4096n }),
  };
  const dataDir = await tmpDir();
  await new DataLayout(dataDir).ensure();
  const settings = loadHarvestSettings({ DATA_DIR: dataDir, DATABASE_URL: pgh.url }, dataDir);
  const [src] = await registerSources(pgh.pool, [
    { identity: 'local:U-1', labelBase: 'Drive', kind: 'local', mountPath: root },
  ]);
  const calls: string[] = [];
  const control: RunControl = {
    // As the real workflow does first: the started run leaves the queue.
    start: async (id) => {
      calls.push(`start ${id}`);
      await setRunState(pgh.pool, id, 'scanning');
      return 'started' as const;
    },
    describe: () => Promise.resolve('running' as const),
    signal: (id, name) => {
      calls.push(`signal ${id} ${name}`);
      return Promise.resolve();
    },
    cancel: (id) => {
      calls.push(`cancel ${id}`);
      return Promise.resolve('requested' as const);
    },
  };
  const server = createHarvestServer({
    pool: pgh.pool,
    settings,
    sys,
    control,
    staticDir: UI_DIR,
    temporalHealthy: () => Promise.resolve(o.temporal ?? true),
    detectOpts: { volumes: `${dirname(root)}/` },
    sourcesTtlMs: 0,
  });
  servers.push(server);
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/`;

  const context = await browser.newContext();
  contexts.push(context);
  const page = await context.newPage();
  page.setDefaultTimeout(10_000);
  page.on('pageerror', (e) => pageErrors.push(e.message));
  page.on('console', (m) => {
    if (m.type() === 'error') pageErrors.push(m.text());
  });
  return {
    page,
    sourceId: src!.id,
    calls,
    open: async (hash = '') => {
      await page.goto(`${base}${hash}`);
    },
  };
}

/** Adds a harvest item (a possible photo waiting for review, by default). */
async function item(e: Env, rel: string, state = 'REVIEW'): Promise<string> {
  const { rows } = await pgh.pool.query<{ id: string }>(
    `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state)
     VALUES ($1, $2, lower($2), 2000, 1700000000000000000, 'maybe', $3) RETURNING id::text AS id`,
    [e.sourceId, rel, state],
  );
  return rows[0]!.id;
}

const itemStates = async (): Promise<string[]> =>
  (await pgh.pool.query<{ state: string }>('SELECT state FROM harvest_items ORDER BY id')).rows.map(
    (r) => r.state,
  );

/** Waits (polling) until `read` returns `want`. */
async function eventually<T>(read: () => Promise<T> | T, want: T): Promise<void> {
  await expect.poll(read, { timeout: 10_000 }).toEqual(want);
}

test('the sources list shows the mounted drive and opens its folder tree', async () => {
  const e = await setup();
  await e.open();
  await e.page.getByRole('heading', { name: 'Sources' }).waitFor();
  await e.page.getByRole('link', { name: 'Drive' }).click();
  await e.page.getByRole('heading', { name: 'Drive' }).waitFor();
  await e.page.getByText('Whole source (Drive)').waitFor();
  for (const name of ['DCIM', 'Photos', 'Pics']) {
    await e.page.getByRole('checkbox', { name, exact: true }).waitFor();
  }
  // Pruned folders (code, libraries) are never offered.
  expect(await e.page.getByText('node_modules').count()).toBe(0);
});

test('expanding a folder lists its sub-folders', async () => {
  const e = await setup();
  await e.open(`#/source/${e.sourceId}`);
  await e.page.getByRole('button', { name: 'Expand DCIM' }).click();
  await e.page.getByRole('checkbox', { name: '100APPLE' }).waitFor();
});

test('a ticked folder includes everything below it', async () => {
  const e = await setup();
  await e.open(`#/source/${e.sourceId}`);
  const summary = e.page.getByText(/ticked$/);
  await eventually(() => summary.textContent(), 'Nothing ticked');
  expect(await e.page.getByRole('button', { name: 'Start harvest' }).isDisabled()).toBe(true);

  await e.page.getByRole('checkbox', { name: 'Whole source (Drive)' }).check();
  const dcim = e.page.getByRole('checkbox', { name: 'DCIM', exact: true });
  expect(await dcim.isChecked()).toBe(true);
  expect(await dcim.isDisabled()).toBe(true);
  await eventually(() => summary.textContent(), '1 folder ticked');
  expect(await e.page.getByRole('button', { name: 'Start harvest' }).isEnabled()).toBe(true);
});

test('starting a harvest shows it in the run panel', async () => {
  const e = await setup();
  await e.open(`#/source/${e.sourceId}`);
  await e.page.getByRole('checkbox', { name: 'DCIM', exact: true }).check();
  await e.page.getByRole('checkbox', { name: 'Photos', exact: true }).check();
  await eventually(() => e.page.getByText(/ticked$/).textContent(), '2 folders ticked');
  await e.page.getByRole('button', { name: 'Start harvest' }).click();

  await e.page.getByText('Harvest #1 started.').waitFor();
  expect(e.calls).toContain('start 1');
  await e.page.locator('#run-panel').getByRole('heading', { name: 'Harvest #1' }).waitFor();
  // The ticks are cleared once the run is created.
  await eventually(() => e.page.getByText(/ticked$/).textContent(), 'Nothing ticked');
});

test('harvesting the whole drive asks first', async () => {
  const e = await setup();
  await e.open(`#/source/${e.sourceId}`);
  await e.page.getByRole('checkbox', { name: 'Whole source (Drive)' }).check();
  await e.page.getByRole('button', { name: 'Start harvest' }).click();
  const dialog = e.page.getByRole('dialog');
  await dialog.getByText('Harvest all of Drive?').waitFor();
  await dialog.getByRole('button', { name: 'Back' }).click();
  expect(e.calls).toEqual([]);
});

test('pause and resume reach the run, and a pause shows its reason', async () => {
  const e = await setup();
  const runId = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], false);
  await setRunState(pgh.pool, runId, 'sending');
  await e.open();
  const panel = e.page.locator('#run-panel');
  await panel.getByRole('button', { name: 'Pause' }).click();
  await eventually(() => e.calls, [`signal ${runId} pause`]);

  await setRunState(pgh.pool, runId, 'paused', 'disk 91.0 %');
  await panel.getByText('disk 91.0 %').waitFor(); // the page polls every 2 s
  await panel.getByRole('button', { name: 'Resume' }).click();
  await eventually(() => e.calls.at(-1), `signal ${runId} resume`);
});

test('possible photos: the badge counts them and "Send all" approves a folder', async () => {
  const e = await setup();
  await item(e, 'Pics/a.png');
  await item(e, 'Pics/b.png');
  await e.open();
  await eventually(() => e.page.locator('#review-badge').textContent(), '2');

  await e.page.getByRole('link', { name: '2 possible photos waiting for review' }).click();
  await e.page.getByRole('heading', { name: 'Possible photos' }).waitFor();
  await e.page.getByText('Drive/Pics').waitFor();
  await e.page.getByRole('button', { name: 'Send all' }).click();
  await e.page.getByText(/^2 files approved/).waitFor();
  await eventually(itemStates, ['PLANNED', 'PLANNED']);
  await eventually(() => e.page.locator('#review-badge').isHidden(), true);
});

test('possible photos: one file can be skipped from the folder view', async () => {
  const e = await setup();
  await item(e, 'Pics/a.png');
  await item(e, 'Pics/b.png');
  await e.open(`#/review/${e.sourceId}/Pics`);
  const figures = e.page.locator('figure');
  await eventually(() => figures.count(), 2);
  await figures.filter({ hasText: 'a.png' }).getByRole('button', { name: 'Skip' }).click();
  await e.page.getByText('a.png: skipped.').waitFor();
  await eventually(() => figures.count(), 1);
  await eventually(itemStates, ['REJECTED', 'REVIEW']);
});

test('a banner says when the pipeline stack is down', async () => {
  const e = await setup({ temporal: false });
  await e.open();
  await eventually(
    () => e.page.locator('#banner').textContent(),
    'Pipeline stack down (Temporal unreachable). Nothing is sent until it is back.',
  );
});

test('history lists finished runs', async () => {
  const e = await setup();
  const runId = await createRun(pgh.pool, [{ sourceId: e.sourceId, relPath: 'DCIM' }], true);
  await finishRunRows(pgh.pool, runId, 'done');
  await e.open('#/history');
  await e.page.getByText(`Dry run #${runId}`).waitFor();
  await e.page.getByText(/^done · /).waitFor();
});

test('find a photo says when nothing matches', async () => {
  const e = await setup();
  await e.open('#/where');
  await e.page.getByRole('searchbox', { name: 'sha256 or file name' }).fill('nothing-like-this');
  await e.page.getByRole('button', { name: 'Find' }).click();
  await e.page.getByText('No source path holds those bytes.').waitFor();
});

test('folder names are shown as text, never run as HTML', async () => {
  const name = '<img src=x onerror="window.pwned=1">';
  const e = await setup({ spec: { [`${name}/a.jpg`]: 10 } });
  await e.open(`#/source/${e.sourceId}`);
  await e.page.getByRole('checkbox', { name }).waitFor();
  expect(await e.page.evaluate(() => (globalThis as { pwned?: number }).pwned)).toBeUndefined();
});
