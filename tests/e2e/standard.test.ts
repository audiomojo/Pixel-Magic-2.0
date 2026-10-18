// Standard tier (spec §10.3): ~2k corpus files plus synthetic variants, junk and corrupt extras.
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { afterAll, afterEach, beforeAll, expect, test } from 'vitest';
import { isIgnored } from '../../src/fsops.js';
import type { Manifest } from '../../tools/corpus/lib.js';
import { diffExpectations, livePairs, moduloAmbiguity, queries } from '../lib/expectations.js';
import { checkInvariant, vaultUsage } from '../lib/invariant.js';
import { stagedEntries } from '../lib/snapshot.js';
import { runTier, type TierRun } from '../lib/stack.js';

let run: TierRun;
let manifest: Manifest;
let failed = false;
let q: ReturnType<typeof queries>;

beforeAll(async () => {
  run = await runTier('standard');
  manifest = run.manifest;
  try {
    await run.runToIdle();
  } catch (e) {
    failed = true;
    throw e;
  }
  q = queries(run.dbUrl, run.dataDir);
}, 60 * 60_000);

afterEach((ctx) => {
  if (ctx.task.result?.state === 'fail') failed = true;
});

afterAll(async () => {
  if (run) await run.stack.down(failed);
}, 10 * 60_000);

test('standard: invariant and expectations', async () => {
  expect(await checkInvariant(...run.ctx)).toEqual([]);
  expect(await diffExpectations(...run.exp)).toEqual([]);
});

test('standard: no burst pair shares a copy group', async () => {
  const seeded = new Set(run.seedManifest.files.map((f) => f.name));
  const pairs = manifest.relations.burst.filter(([a, b]) => seeded.has(a) && seeded.has(b));
  expect(pairs.length).toBeGreaterThan(0);
  for (const [a, b] of pairs) expect(await q.sameCopyGroup(a, b), `${a} ${b}`).toBe(false);
});

test('standard: synthetic resized/re-encoded variants join their seed’s copy group', async () => {
  const copies = run.seedManifest.files.filter(
    (f) => f.origin === 'synthetic' && f.expect === 'copy',
  );
  expect(copies.length).toBeGreaterThan(0);
  for (const v of copies) expect(await q.sameCopyGroup(v.name, v.seedName!), v.name).toBe(true);
});

test('every Live Photo pair staged together', async () => {
  const pairs = livePairs(manifest, new Set(run.seedManifest.files.map((f) => f.name)));
  expect(pairs.length).toBeGreaterThanOrEqual(20);
  for (const [still, mov] of pairs)
    expect(await q.beside(mov, still), `${still} ${mov}`).toBe(true);
});

// Ignored names (`.DS_Store`, partial downloads) are never discovered and stay in the drop zone;
// other junk (an unknown type such as a .txt) is filed under `other/` like any unknown file.
test('junk never discovered', async () => {
  const junk = run.seedManifest.files.filter((f) => f.origin === 'junk');
  const ignored = junk.filter((f) => isIgnored(f.name));
  expect(ignored.length).toBeGreaterThan(0);
  expect(await q.fileRowCount(ignored.map((f) => f.relPath))).toBe(0);
  for (const f of ignored)
    expect(existsSync(join(run.dataDir, 'dropzone', f.relPath)), f.relPath).toBe(true);
  const staged = await stagedEntries(run.dataDir);
  for (const f of junk.filter((f) => !isIgnored(f.name))) {
    expect(await q.stateOf(f.relPath), f.relPath).toBe('INGESTED');
    expect(
      staged.some((e) => e.sha256 === f.sha256 && e.dest.startsWith('other/')),
      f.relPath,
    ).toBe(true);
  }
});

test('corrupt extras quarantined with expected reasons', async () => {
  const bad = run.seedManifest.files.filter((f) => f.origin === 'corrupt');
  expect(bad.length).toBeGreaterThan(0);
  for (const f of bad) expect(await q.quarantineReasonOf(f.relPath), f.relPath).toBe(f.reason);
});

test('nested Unicode layout works', async () => {
  const nested = await runTier('standard', { layout: 'nested', runId: `${run.runId}-nested` });
  let ok = false;
  try {
    await nested.runToIdle();
    expect(
      nested.seedManifest.files.some((f) => f.relPath.includes('/') && /[^ -~]/.test(f.relPath)),
    ).toBe(true);
    expect(await checkInvariant(...nested.ctx)).toEqual([]);
    // Partitions only: a byte twin's staged name comes from its lowest rel_path, which nesting changes.
    const m = await moduloAmbiguity(nested, run);
    expect(m.partitions[0]).toEqual(m.partitions[1]);
    ok = true;
  } finally {
    await nested.stack.down(!ok);
  }
});

test('re-seed under new paths is all COPY and stores nothing new', async () => {
  // Last: it mutates the run.
  const before = await vaultUsage(run.dataDir);
  const again = (await run.reseed('again/')).files.filter(
    (f) => f.relPath.startsWith('again/') && f.origin === 'corpus',
  );
  await run.runToIdle();
  expect(again.length).toBeGreaterThan(0);
  for (const f of again) expect(await q.stateOf(f.relPath), f.relPath).toBe('COPY');
  // Every re-dropped file was a confirmed duplicate and removed: the vault is byte for byte
  // what it was (spec §6.5).
  expect(await vaultUsage(run.dataDir)).toEqual(before);
  expect(await checkInvariant(...run.ctx)).toEqual([]);
});
