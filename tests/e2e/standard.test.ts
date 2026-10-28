// Standard tier (spec §10.3): ~2k corpus files plus synthetic variants, junk and corrupt extras.
import { existsSync } from 'node:fs';
import { copyFile, mkdir, readdir, readFile, rename } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { Client, Connection } from '@temporalio/client';
import { afterAll, afterEach, beforeAll, expect, test } from 'vitest';
import { isIgnored, shaOfVaultName } from '../../src/fsops.js';
import type { DoctorReport } from '../../src/operator.js';
import { currentShown, purgeClosure, purgeSet } from '../../src/purge.js';
import { parseLog, purgeLogPath } from '../../src/purgeLog.js';
import type { PurgeSyncWorkflow } from '../../src/workflows.js';
import type { Manifest, SeedFile } from '../../tools/corpus/lib.js';
import { poolFor } from '../lib/db.js';
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

// Identical-dedupe spec §9: a metadata-only re-save (`meta`, EXIF:Software = Picasa 3.0) and a
// pixel-identical lossless PNG are retired; only the family's keeper is staged.
test('standard: pixel-identical variants are retired and only the keeper is staged', async () => {
  const dups = run.seedManifest.files.filter(
    (f) => f.origin === 'synthetic' && f.expect === 'duplicate',
  );
  expect(dups.some((f) => f.variant === 'meta')).toBe(true);
  for (const seedName of new Set(dups.map((f) => f.seedName!))) {
    const family = [seedName, ...dups.filter((f) => f.seedName === seedName).map((f) => f.name)];
    const placed = await Promise.all(family.map(async (n) => (await q.stagedOf(n)).length));
    // Exactly one member placed: the keeper.
    expect(placed.filter((n) => n > 0).length, `${family.join(', ')}: ${String(placed)}`).toBe(1);
    const keeper = dups.find((f) => f.seedName === seedName)!.keeper;
    if (keeper !== undefined) {
      const name = keeper === seedName ? seedName : keeper.slice(keeper.lastIndexOf('/') + 1);
      expect(placed[family.indexOf(name)], `${name} staged`).toBeGreaterThan(0);
    }
  }
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
  // Mutates the run: only the purge test below comes after it.
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

/** Vault files (not `.copies`) by the sha256 they are named after. */
async function vaultFiles(dataDir: string): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  for (const d of await readdir(join(dataDir, 'vault'), { recursive: true, withFileTypes: true }))
    if (d.isFile() && !d.parentPath.split('/').includes('.copies'))
      out.set(shaOfVaultName(d.name), join(d.parentPath, d.name));
  return out;
}

/** `cli <args>` inside the stack's worker; its last stdout line. */
async function cli(...args: string[]): Promise<string> {
  const stdout = await run.stack.compose('exec', '-T', 'worker', 'node', 'dist/cli.js', ...args);
  return stdout.trim().split('\n').at(-1)!;
}

// Purge spec §11 end-to-end: purge an expected similar set as the Picks API does, then drop its
// files again (none re-enters the vault), also after the block list was wiped and rebuilt from
// purged.csv. Last: it mutates the run.
test('purged set leaves the vault and stays out, also after a wiped block list', async () => {
  const pool = poolFor(run.dbUrl);
  const seeded = run.seedManifest.files.filter((f) => !isIgnored(f.name));
  const corpusSha = new Map(
    seeded.filter((f) => f.origin === 'corpus').map((f) => [f.name, f.sha256]),
  );
  // An expected similar set: one holding a synthetic variant and its seed. Preferred: one with a
  // Live video, then a `similar` variant, then hidden copies, so the whole closure is exercised.
  const pairs = seeded
    .filter((f) => f.origin === 'synthetic' && f.expect !== 'duplicate')
    .map((v) => ({ v, seed: corpusSha.get(v.seedName ?? '') }))
    .filter((x): x is { v: SeedFile; seed: string } => x.seed !== undefined);
  const sets = (
    await pool.query<{ set_id: string; shas: string[] }>(
      `SELECT set_id::text, array_agg(sha256 ORDER BY sha256) AS shas
         FROM similar_candidates GROUP BY set_id ORDER BY set_id`,
    )
  ).rows;
  const ranked = [];
  for (const s of sets) {
    const has = new Set(s.shas);
    const held = pairs.filter((p) => has.has(p.v.sha256) && has.has(p.seed));
    if (held.length === 0) continue;
    const preview = await purgeClosure(pool, s.set_id, s.shas);
    if (preview === 'stale') throw new Error(`set ${s.set_id}: stale`);
    // A candidate outside the held pairs can stay undescribed (not shown) while two or more
    // are shown: it must survive the purge (R6, the closure is the shown copy groups only).
    const pairShas = new Set(held.flatMap((p) => [p.v.sha256, p.seed]));
    const spare = s.shas.length >= 3 ? s.shas.find((sha) => !pairShas.has(sha)) : undefined;
    const score =
      (spare !== undefined ? 8 : 0) +
      (preview.live > 0 ? 4 : 0) +
      (held.some((p) => p.v.expect === 'similar') ? 2 : 0) +
      (preview.copies > 0 ? 1 : 0);
    ranked.push({ setId: s.set_id, candidates: s.shas, score, live: preview.live > 0, spare });
  }
  expect(ranked.length, 'no similar set holds a synthetic variant and its seed').toBeGreaterThan(0);
  const chosen = ranked.sort((a, b) => b.score - a.score)[0]!;
  // The AI worker (not part of the stack) describes candidates; the Picks screen shows only
  // described ones. Stand in for it on this set, leaving the spare candidate (if any) undescribed.
  const described = chosen.candidates.filter((sha) => sha !== chosen.spare);
  await pool.query(
    `INSERT INTO photo_features (sha256, feature_version, embedding, quality, faces)
     SELECT unnest($1::text[]), 1, '{}'::real[], '{}'::jsonb, '[]'::jsonb
     ON CONFLICT DO NOTHING`,
    [described],
  );
  const shown = await currentShown(pool, chosen.setId);
  expect(shown).toEqual(described);
  const spareState = async (): Promise<string | undefined> =>
    chosen.spare === undefined
      ? undefined
      : (
          await pool.query<{ state: string }>('SELECT state FROM contents WHERE sha256 = $1', [
            chosen.spare,
          ])
        ).rows[0]?.state;
  const spareBefore = await spareState();

  // Keep the closure's bytes to drop them again later (synthetic variants exist nowhere else).
  const preview = await purgeClosure(pool, chosen.setId, shown);
  if (preview === 'stale') throw new Error(`set ${chosen.setId}: stale`);
  const saved = join(run.dataDir, 'purge-e2e', 'bytes');
  await mkdir(saved, { recursive: true });
  const before = await vaultFiles(run.dataDir);
  for (const sha of preview.shas) await copyFile(before.get(sha)!, join(saved, sha));

  // As POST /api/picks/purge: the transaction, then PurgeSyncWorkflow on `main`.
  const summary = await purgeSet(pool, chosen.setId, shown);
  if (summary === 'stale') throw new Error(`set ${chosen.setId}: stale`);
  const purged = new Set(summary.shas);
  process.stdout.write(
    `purge: set ${chosen.setId}, ${String(summary.shown)} shown, ${String(summary.copies)} ` +
      `copies, ${String(summary.live)} live, ${String(summary.bytes)} bytes ` +
      `(of ${String(ranked.length)} eligible sets, ${String(ranked.filter((r) => r.live).length)} with a Live video, ` +
      `${String(ranked.filter((r) => r.spare !== undefined).length)} with a spare candidate; ` +
      `undescribed spare: ${chosen.spare ?? 'none'})\n`,
  );
  if (chosen.spare !== undefined) expect(purged.has(chosen.spare), 'spare purged').toBe(false);
  expect(summary.shas).toEqual(preview.shas);
  expect(summary.shown).toBe(shown.length);
  for (const sha of shown) expect(purged.has(sha), sha).toBe(true);
  const connection = await Connection.connect({ address: run.stack.temporalAddress });
  try {
    const client = new Client({ connection, namespace: 'default' });
    await client.workflow.execute<typeof PurgeSyncWorkflow>('PurgeSyncWorkflow', {
      taskQueue: 'main',
      workflowId: `pm-purge-e2e-${String(Date.now())}`,
      args: [summary.shas],
    });
  } finally {
    await connection.close();
  }
  // Staging keeps the previous generation, which still links the files: two more builds drop
  // every link, and the copy sweep finishes the deferred removals.
  await run.stack.stage();
  await run.stack.stage();
  await cli('sweep-copies');

  const vault = await vaultFiles(run.dataDir);
  expect(summary.shas.filter((sha) => vault.has(sha))).toEqual([]);
  // The undescribed candidate was never shown: still in the vault, state unchanged.
  if (chosen.spare !== undefined) {
    expect(vault.has(chosen.spare), 'spare left the vault').toBe(true);
    expect(await spareState()).toBe(spareBefore);
  }
  const staged = await stagedEntries(run.dataDir);
  expect(staged.filter((e) => purged.has(e.sha256))).toEqual([]);
  const csv = await readFile(purgeLogPath(run.dataDir), 'utf8');
  const log = parseLog(csv);
  expect(log.bad).toEqual([]);
  expect(
    log.events
      .filter((e) => e.action === 'purge')
      .map((e) => e.sha256)
      .sort(),
  ).toEqual(summary.shas);
  const doctor = JSON.parse(await cli('doctor')) as DoctorReport;
  expect(doctor.purgeMismatched).toBe(0);
  expect(doctor.purged).toBe(summary.shas.length);
  expect(await checkInvariant(...run.ctx, { purged })).toEqual([]);
  expect(await diffExpectations(...run.exp, { purged })).toEqual([]);

  // Drop the set's original files again (every seeded name, byte twins included).
  const originals = seeded.filter((f) => purged.has(f.sha256) && !f.relPath.startsWith('again/'));
  expect(originals.length).toBeGreaterThanOrEqual(summary.shas.length);
  const redrop = async (prefix: string): Promise<string[]> => {
    const paths = originals.map((f) => `${prefix}/${f.relPath}`);
    for (const [i, f] of originals.entries()) {
      // Copied beside the drop zone, then renamed in: the scanner never sees a partial file.
      const tmp = join(run.dataDir, 'purge-e2e', paths[i]!);
      await mkdir(dirname(tmp), { recursive: true });
      await copyFile(join(saved, f.sha256), tmp);
      const dest = join(run.dataDir, 'dropzone', paths[i]!);
      await mkdir(dirname(dest), { recursive: true });
      await rename(tmp, dest);
    }
    return paths;
  };
  const usage = await vaultUsage(run.dataDir);
  const again = await redrop('purged-again');
  await run.runToIdle();
  for (const p of again) {
    expect(await q.stateOf(p), p).toBe('BLOCKED');
    expect(existsSync(join(run.dataDir, 'dropzone', p)), p).toBe(false);
  }
  expect(await vaultUsage(run.dataDir)).toEqual(usage);

  // A wiped block list: the restarted worker rebuilds it from purged.csv before taking files in.
  await pool.query('TRUNCATE purged');
  await run.stack.restart('worker');
  const blocked = async () =>
    (
      await pool.query<{ sha256: string; size_bytes: string }>(
        `SELECT sha256, size_bytes::text FROM purged WHERE unblocked_at IS NULL ORDER BY sha256`,
      )
    ).rows;
  const deadline = Date.now() + 5 * 60_000;
  while ((await blocked()).length < summary.shas.length && Date.now() < deadline) await sleep(2000);
  const sizes = new Map(seeded.map((f) => [f.sha256, f.size]));
  expect(await blocked()).toEqual(
    summary.shas.map((sha) => ({ sha256: sha, size_bytes: sizes.get(sha) })),
  );
  // Database and file agreed again: nothing was appended.
  expect(await readFile(purgeLogPath(run.dataDir), 'utf8')).toBe(csv);
  const afterWipe = await redrop('purged-after-wipe');
  await run.runToIdle();
  for (const p of afterWipe) {
    expect(await q.stateOf(p), p).toBe('BLOCKED');
  }
  expect(await vaultUsage(run.dataDir)).toEqual(usage);
  expect(await checkInvariant(...run.ctx, { purged })).toEqual([]);
});
