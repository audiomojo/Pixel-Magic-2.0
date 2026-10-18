// Smoke tier (spec §10.3): ~150 corpus files through the real compose stack, then the golden
// invariant, ground-truth expectations, the pinned cases and the golden staging tree.
import { afterAll, afterEach, beforeAll, expect, test } from 'vitest';
import { diffExpectations, queries } from '../lib/expectations.js';
import { checkInvariant } from '../lib/invariant.js';
import { readGolden, treeSnapshot, writeGolden } from '../lib/snapshot.js';
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
