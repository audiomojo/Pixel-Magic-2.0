// Smoke tier (spec §10.3): ~150 corpus files through the real compose stack, then the golden
// invariant, ground-truth expectations, the pinned cases and the golden staging tree.
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { afterAll, afterEach, beforeAll, expect, test } from 'vitest';
import { components } from '../../tools/corpus/lib.js';
import { diffExpectations, queries } from '../lib/expectations.js';
import { checkInvariant } from '../lib/invariant.js';
import { readGolden, stagedEntries, treeSnapshot, writeGolden } from '../lib/snapshot.js';
import { runTier, type TierRun } from '../lib/stack.js';

let run: TierRun;
let failed = false;
let q: ReturnType<typeof queries>;

beforeAll(async () => {
  run = await runTier('smoke');
  try {
    await run.runToIdle();
  } catch (e) {
    failed = true;
    throw e;
  }
  q = queries(run.dbUrl, run.dataDir);
}, 45 * 60_000);

afterEach((ctx) => {
  if (ctx.task.result?.state === 'fail') failed = true;
});

afterAll(async () => {
  if (run) await run.stack.down(failed);
}, 10 * 60_000);

test('smoke: invariant, expectations, golden tree', async () => {
  expect(await checkInvariant(...run.ctx)).toEqual([]);
  expect(await diffExpectations(...run.exp)).toEqual([]);
  const tree = await treeSnapshot(...run.snap);
  if (process.env['UPDATE_GOLDEN'] === '1') await writeGolden('smoke_tree.txt', tree);
  expect(tree).toEqual(await readGolden('smoke_tree.txt'));
});

// Controller ruling R12: the one-time cleanup, run for real in the stack's worker after the
// pipeline settled, finds nothing left to retire (the pipeline step already did it all).
test('dedupe-identical dry run in the worker: exit 0, families 0, a report', async () => {
  // `compose exec` rejects on a non-zero exit.
  const stdout = await run.stack.compose(
    'exec',
    '-T',
    'worker',
    'node',
    'dist/cli.js',
    'dedupe-identical',
  );
  expect(stdout).toMatch(/^families 0, files to remove 0, bytes to free 0, errors 0$/m);
  const reports = (await readdir(join(run.dataDir, 'reports'))).filter((n) =>
    /^dedupe-identical-.*\.csv$/.test(n),
  );
  expect(reports.length, String(reports)).toBe(1);
  expect(reports[0]).not.toMatch(/-applied\.csv$/);
  expect(stdout).toContain(`report: /data/reports/${reports[0]!}`);
});

test('pinned cases', async () => {
  expect(await q.kindOf('IMG_E8719.HEIC')).toBe('still');
  expect(await q.mimeOf('IMG_E8719.HEIC')).toBe('image/jpeg');
  expect(await q.kindOf('IMG_0004.MOV')).toBe('motion');
  expect(await q.sameCopyGroup('IMG_6941 1.HEIC', 'IMG_E6941.JPG')).toBe(true);
  expect(await q.sameCopyGroup('IMG_0034.HEIC', 'IMG_0034.JPG')).toBe(false); // same name, different photos
  expect(await q.sameCopyGroup('IMG_2834.HEIC', 'IMG_2834 1.HEIC')).toBe(false);
  expect(new Set(await q.states(['PSWU5412.MP4', 'PSWU5412 1.MP4']))).toEqual(
    new Set(['INGESTED', 'COPY']),
  );
  expect(await q.beside('IMG_2834 1.MOV', 'IMG_2834 1.HEIC')).toBe(true);
});

/** A dated primary placement (spec §6.4): `unique/` or a similar set, `<YYYYMMDD_HHMMSS>_<name>`. */
const primary = (name: string) =>
  new RegExp(
    String.raw`^(unique/\d{4}/\d{4}-\d{2}|similar/\d{4}/\d{4}-\d{2}/s-[0-9a-f]{8})/\d{8}_\d{6}_` +
      name.replace(/[.]/g, '\\.') +
      '$',
  );

// Spec §10.3 pins, asserted wherever the tier's subset (from the manifest) holds them.
test('spec-pinned destinations and burst sets', async () => {
  const seeded = new Set(run.seedManifest.files.map((f) => f.name));
  const has = (...names: string[]) =>
    names.every((n) => seeded.has(n) && run.manifest.records.some((r) => r.name === n));
  for (const n of ['VERP1022.TIF', 'AUXA4949.BMP'])
    if (has(n)) expect(await q.stagedOf(n), n).toEqual([expect.stringMatching(primary(n))]);
  if (has('IMG_0433.HEIC', 'IMG_E0433.HEIC')) {
    // An edit and its original, same capture key: one copy group, the best copy placed by date
    // and the other under the group's copies folder (named after the best copy's sha8).
    expect(await q.sameCopyGroup('IMG_0433.HEIC', 'IMG_E0433.HEIC')).toBe(true);
    const names = ['IMG_0433.HEIC', 'IMG_E0433.HEIC'];
    const placed = (await stagedEntries(run.dataDir)).filter((e) =>
      names.some((n) => e.dest.endsWith(n)),
    );
    const best = placed.filter((e) => /^(unique|similar)\//.test(e.dest));
    expect(best.length, JSON.stringify(placed)).toBe(1);
    const other = names.find((n) => !best[0]!.dest.endsWith(n))!;
    expect(best[0]!.dest).toMatch(primary(names.find((n) => n !== other)!));
    expect(placed.map((e) => e.dest)).toContain(`copies/g-${best[0]!.sha256.slice(0, 8)}/${other}`);
  }
  const bursts = components(run.manifest.relations.burst).filter(
    (g) => g.filter((n) => seeded.has(n)).length >= 2,
  );
  expect(bursts.length).toBeGreaterThanOrEqual(5);
});
