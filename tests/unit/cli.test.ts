import { chmod, mkdir, readdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import type { Client } from '@temporalio/client';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import type { Deps } from '../../src/activities/index.js';
import { main, run } from '../../src/cli.js';
import { DataLayout, vaultRel } from '../../src/fsops.js';
import { testDeps } from '../helpers/activityEnv.js';
import { photo } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

interface Executed {
  workflowType: unknown;
  opts: { taskQueue: string; workflowId: string; args: unknown[] };
}

function fakeClient() {
  const executed: Executed[] = [];
  const client = {
    workflow: {
      execute: (workflowType: unknown, opts: Executed['opts']) => {
        executed.push({ workflowType, opts });
        return Promise.resolve({ gen: 4, entries: 12, skipped: false });
      },
    },
  };
  return { client: client as unknown as Client, executed };
}

function capture(stream: 'stdout' | 'stderr'): () => string {
  let buf = '';
  vi.spyOn(process[stream], 'write').mockImplementation((chunk: string | Uint8Array) => {
    buf += String(chunk);
    return true;
  });
  return () => buf;
}

afterEach(() => {
  vi.restoreAllMocks();
});

test('stage runs StagingWorkflow on main and prints its result', async () => {
  const f = fakeClient();
  const out = capture('stdout');
  expect(await main(['stage'], { temporal: f.client })).toBe(0);
  expect(f.executed).toHaveLength(1);
  expect(f.executed[0]!.workflowType).toBe('StagingWorkflow');
  expect(f.executed[0]!.opts.taskQueue).toBe('main');
  expect(f.executed[0]!.opts.args).toEqual([false]);
  expect(f.executed[0]!.opts.workflowId).toMatch(/^pm-stage-cli-\d+$/);
  expect(out()).toContain('"gen":4');
});

test('stage --force forces a rebuild', async () => {
  const f = fakeClient();
  capture('stdout');
  expect(await main(['stage', '--force'], { temporal: f.client })).toBe(0);
  expect(f.executed[0]!.opts.args).toEqual([true]);
});

test('unknown command exits 2 with usage', async () => {
  const err = capture('stderr');
  expect(await main(['nope'], { temporal: fakeClient().client })).toBe(2);
  expect(err()).toMatch(/usage/i);
});

test('unknown option exits 2 with usage', async () => {
  const err = capture('stderr');
  expect(await main(['stage', '--bogus'], { temporal: fakeClient().client })).toBe(2);
  expect(err()).toMatch(/usage/i);
});

// ---- commands that touch Postgres and the data directory ----

const sha = (bytes: string) => createHash('sha256').update(bytes).digest('hex');

let pgh: PgHandle;
let deps: Deps;
let layout: DataLayout;
let closeDeps: () => Promise<void>;

beforeAll(async () => {
  pgh = await startPg();
  ({ deps, layout, close: closeDeps } = await testDeps(pgh));
}, 120_000);

afterAll(async () => {
  await closeDeps();
  await pgh.stop();
});

beforeEach(async () => {
  await pgh.truncate();
  await rm(layout.vault, { recursive: true, force: true });
  await rm(layout.claimed, { recursive: true, force: true });
  await rm(join(layout.root, 'reports'), { recursive: true, force: true });
  await layout.ensure();
});

/** A vault file holding `bytes`, named by its true hash unless `nameSha` says otherwise. */
async function writeVault(bytes: string, opts: { nameSha?: string } = {}): Promise<string> {
  const name = opts.nameSha ?? sha(bytes);
  const path = join(layout.root, vaultRel(name, 'jpg'));
  await mkdir(join(path, '..'), { recursive: true });
  await writeFile(path, bytes);
  await chmod(path, 0o444);
  return name;
}

let seq = 0;
async function seedContent(o: { state: string; lastError?: string | null; kind?: string }) {
  seq += 1;
  const hash = sha(`content-${String(seq)}`);
  await pgh.pool.query(
    `INSERT INTO contents (sha256, ext, kind, size_bytes, state, last_error)
     VALUES ($1, 'jpg', $2, 10, $3, $4)`,
    [hash, o.kind ?? 'still', o.state, o.lastError ?? null],
  );
  return hash;
}

function recordingClient() {
  const started: {
    workflowType: unknown;
    workflowId: string;
    taskQueue: string;
    args: unknown[];
  }[] = [];
  const client = {
    workflow: {
      start: (
        workflowType: unknown,
        opts: { workflowId: string; taskQueue: string; args: unknown[] },
      ) => {
        started.push({ workflowType, ...opts });
        return Promise.resolve({});
      },
    },
  };
  return { client: client as unknown as Client, started };
}

test('verify-vault detects mismatch', async () => {
  await writeVault('a');
  await writeVault('b', { nameSha: '0'.repeat(64) });
  const out = capture('stdout');
  expect(await main(['verify-vault'], deps)).toBe(1);
  expect(out()).toContain('0'.repeat(64));
});

test('verify-vault skips .copies and reports the count', async () => {
  await writeVault('a');
  await writeFile(join(layout.vaultCopies, '7.jpg'), 'named by file id');
  const out = capture('stdout');
  expect(await main(['verify-vault'], deps)).toBe(0);
  expect(out()).toContain('skipped 1 .copies files');
});

test('verify-vault --sample N checks at most N files', async () => {
  for (const b of ['a', 'b', 'c']) await writeVault(b);
  const out = capture('stdout');
  expect(await main(['verify-vault', '--sample', '2'], deps)).toBe(0);
  expect(out()).toContain('checked 2 ');
});

test('verify-vault --sample larger than the vault checks everything', async () => {
  await writeVault('a');
  const out = capture('stdout');
  expect(await main(['verify-vault', '--sample', '50'], deps)).toBe(0);
  expect(out()).toContain('checked 1 ');
});

test('verify-vault rejects a bad --sample', async () => {
  const err = capture('stderr');
  expect(await main(['verify-vault', '--sample', '0'], deps)).toBe(2);
  expect(err()).toMatch(/usage/i);
});

test('retry-failed starts a ContentWorkflow per FAILED content, which reopens it itself', async () => {
  await seedContent({ state: 'FAILED', lastError: 'x' });
  await seedContent({ state: 'FAILED', lastError: 'decode_killed: child got SIGKILL' });
  await seedContent({ state: 'CLUSTERED' });
  const f = recordingClient();
  const out = capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: f.client, poisonAttempts: 3 })).toBe(0);
  // The CLI changes no row: a crash between listing and starting leaves nothing half-done.
  const states = (await pgh.pool.query<{ state: string }>('SELECT state FROM contents')).rows;
  expect(states.map((r) => r.state).sort()).toEqual(['CLUSTERED', 'FAILED', 'FAILED']);
  expect(f.started.map((w) => w.workflowType)).toEqual(['ContentWorkflow', 'ContentWorkflow']);
  expect(f.started.every((w) => /^content-[0-9a-f]{64}-retry-\d+$/.test(w.workflowId))).toBe(true);
  expect(f.started.every((w) => w.taskQueue === 'main')).toBe(true);
  expect(f.started[0]!.args).toEqual([
    expect.stringMatching(/^[0-9a-f]{64}$/),
    'jpg',
    { poisonAttempts: 3 },
  ]);
  expect(out()).toContain('started 2');
});

test('retry-failed leaves a row FAILED when its workflow cannot start', async () => {
  const hash = await seedContent({ state: 'FAILED', lastError: 'boom' });
  const client = {
    workflow: { start: () => Promise.reject(new Error('temporal down')) },
  } as unknown as Client;
  const err = capture('stderr');
  capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: client, poisonAttempts: 5 })).toBe(1);
  const row = (
    await pgh.pool.query<{ state: string; last_error: string }>(
      'SELECT state, last_error FROM contents WHERE sha256 = $1',
      [hash],
    )
  ).rows[0]!;
  expect(row).toEqual({ state: 'FAILED', last_error: 'boom' });
  expect(err()).toContain('temporal down');
});

test('retry-failed reports a non-Error start failure', async () => {
  await seedContent({ state: 'FAILED' });
  const client = {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- the point: a non-Error rejection
    workflow: { start: () => Promise.reject('plain string') },
  } as unknown as Client;
  const err = capture('stderr');
  capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: client, poisonAttempts: 5 })).toBe(1);
  expect(err()).toContain('plain string');
});

test('retry-failed with nothing failed starts nothing', async () => {
  const f = recordingClient();
  const out = capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: f.client, poisonAttempts: 5 })).toBe(0);
  expect(f.started).toEqual([]);
  expect(out()).toContain('0');
});

test('a failing command prints one error line (no stack trace) and exits 1', async () => {
  const err = capture('stderr');
  const missing = new DataLayout(join(layout.root, 'no-such-dir'));
  expect(await run(['verify-vault'], { layout: missing })).toBe(1);
  expect(err()).toMatch(/^error: .*ENOENT.*\n$/);
  expect(err()).not.toMatch(/\n\s+at /);
});

async function seedFile(state: string, attempts = 0): Promise<string> {
  seq += 1;
  const r = await pgh.pool.query<{ id: bigint }>(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, state, attempts)
     VALUES ($1, 'x', 1, $2, $3, $4) RETURNING id`,
    [`f${String(seq)}.jpg`, seq, state, attempts],
  );
  return String(r.rows[0]!.id);
}

test('doctor counts orphaned claimed files and stuck work', async () => {
  const orphan = await seedFile('INGESTED');
  const live = await seedFile('CLAIMED', 1);
  await seedFile('CLAIMED', 9);
  for (const id of [orphan, live]) await writeFile(join(layout.claimed, id), 'bytes');
  const hash = await seedContent({ state: 'HASHED' });
  // The updated_at trigger would overwrite the backdating, so switch it off for the update.
  await pgh.pool.query('ALTER TABLE contents DISABLE TRIGGER contents_updated_at');
  await pgh.pool.query(
    `UPDATE contents SET updated_at = now() - interval '1 hour' WHERE sha256=$1`,
    [hash],
  );
  await pgh.pool.query('ALTER TABLE contents ENABLE TRIGGER contents_updated_at');
  const out = capture('stdout');
  expect(await main(['doctor'], deps)).toBe(0);
  expect(JSON.parse(out())).toEqual({
    orphanedClaimed: 1,
    filesStuckRetrying: 1,
    contentsStale: 1,
    copyMismatches: 0,
    duplicatesRetired: 0,
    duplicatesMismatched: 0,
  });
});

test('doctor reports zeros on a clean system', async () => {
  const out = capture('stdout');
  expect(await main(['doctor'], deps)).toBe(0);
  expect(JSON.parse(out())).toEqual({
    orphanedClaimed: 0,
    filesStuckRetrying: 0,
    contentsStale: 0,
    copyMismatches: 0,
    duplicatesRetired: 0,
    duplicatesMismatched: 0,
  });
});

/** A COPY row of a.jpg's bytes at `relPath`: removed, flagged a mismatch, or still present. */
async function seedCopy(relPath: string, status: 'removed' | 'mismatch' | 'present') {
  const r = await pgh.pool.query<{ id: bigint }>(
    `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state,
                        copy_removed_at, copy_reclaimed_bytes, copy_mismatch, last_error,
                        discovered_at)
     VALUES ($1, $2, 7, 1600000000000000000, 'jpg', $3, 'COPY', $4, $5, $6, $7,
             '2026-10-18T10:00:00Z')
     RETURNING id`,
    [
      relPath,
      relPath.split('/').pop(),
      sha('a'),
      status === 'removed' ? new Date('2026-10-18T11:00:00Z') : null,
      status === 'removed' ? 7 : null,
      status === 'mismatch',
      status === 'mismatch' ? 'copy_mismatch: .copies sha x, vault sha y, expected z' : null,
    ],
  );
  return String(r.rows[0]!.id);
}

test('doctor surfaces copy mismatches', async () => {
  await seedCopy('x/a.jpg', 'mismatch');
  await seedCopy('y/a.jpg', 'removed');
  const out = capture('stdout');
  expect(await main(['doctor'], deps)).toBe(0);
  expect(JSON.parse(out())).toMatchObject({ copyMismatches: 1 });
});

test('verify-vault reports removed and still-present duplicates', async () => {
  await writeVault('a');
  await writeFile(join(layout.vaultCopies, `${await seedCopy('p/a.jpg', 'present')}.jpg`), 'a');
  await seedCopy('r/a.jpg', 'removed');
  await seedCopy('s/a.jpg', 'removed');
  const out = capture('stdout');
  expect(await main(['verify-vault'], deps)).toBe(0);
  expect(out()).toContain('skipped 1 .copies files still present; 2 duplicates removed');
});

test('copies lists every duplicate drop with its mtime, drop time, vault sha and status', async () => {
  await seedCopy('y/a.jpg', 'removed');
  await seedCopy('x/a.jpg', 'mismatch');
  await seedCopy('z/a.jpg', 'present');
  const out = capture('stdout');
  expect(await main(['copies'], deps)).toBe(0);
  const mtime = '2020-09-13T12:26:40.000Z';
  const at = '2026-10-18T10:00:00.000Z';
  expect(out().split('\n')).toEqual([
    'rel_path\tmtime\tdropped_at\tvault_sha256\tstatus',
    `x/a.jpg\t${mtime}\t${at}\t${sha('a')}\tmismatch`,
    `y/a.jpg\t${mtime}\t${at}\t${sha('a')}\tremoved`,
    `z/a.jpg\t${mtime}\t${at}\t${sha('a')}\tpresent`,
    '',
  ]);
});

test('sweep-copies runs CopySweepWorkflow on main and prints its result', async () => {
  const f = fakeClient();
  const out = capture('stdout');
  expect(await main(['sweep-copies'], { temporal: f.client })).toBe(0);
  expect(f.executed[0]!.workflowType).toBe('CopySweepWorkflow');
  expect(f.executed[0]!.opts).toMatchObject({ taskQueue: 'main', args: [] });
  expect(f.executed[0]!.opts.workflowId).toMatch(/^pm-copies-cli-\d+$/);
  expect(out()).toContain('"gen":4'); // the fake's canned result, printed as is
});

test('doctor counts retired duplicates and identical-duplicate mismatches', async () => {
  const keeper = await seedContent({ state: 'CLUSTERED' });
  const retired = await seedContent({ state: 'CLUSTERED' });
  const flagged = await seedContent({ state: 'CLUSTERED' });
  await pgh.pool.query(
    `UPDATE contents SET state = 'DUPLICATE', duplicate_of = $2 WHERE sha256 IN ($1, $3)`,
    [retired, keeper, flagged],
  );
  await pgh.pool.query('UPDATE contents SET duplicate_mismatch = true WHERE sha256 = $1', [
    flagged,
  ]);
  const out = capture('stdout');
  expect(await main(['doctor'], deps)).toBe(0);
  expect(JSON.parse(out())).toMatchObject({ duplicatesRetired: 2, duplicatesMismatched: 1 });
});

// ---- dedupe-identical ----

/** Two vaulted JPEGs with identical pixels (the second has a trailing byte) and a lone one. */
async function seedDupes(): Promise<{ keep: string; lose: string }> {
  const src = await readFile(await photo(await tmpDir(), 'jpg'));
  const seed = async (bytes: Buffer, name: string) => {
    const h = createHash('sha256').update(bytes).digest('hex');
    const path = layout.vaultFile(h, 'jpg');
    await mkdir(join(path, '..'), { recursive: true });
    await writeFile(path, bytes);
    await pgh.pool.query(
      `INSERT INTO contents (sha256, ext, kind, mime, size_bytes, state, phash, dhash, width, height)
       VALUES ($1, 'jpg', 'still', 'image/jpeg', $2, 'HASHED', 1, 1, 640, 480)`,
      [h, bytes.length],
    );
    await pgh.pool.query(
      `INSERT INTO files (rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state)
       VALUES ($1, $2, $3, 1, 'jpg', $4, 'INGESTED')`,
      [`drop/${name}`, name, bytes.length, h],
    );
    return h;
  };
  const keep = await seed(src, 'IMG_1.jpg');
  const lose = await seed(Buffer.concat([src, Buffer.from('x')]), 'IMG_1 copy.jpg');
  return { keep, lose };
}
const reports = async () => {
  const dir = join(layout.root, 'reports');
  return (await readdir(dir).catch(() => [])).sort().map((f) => join(dir, f));
};
const exists = (p: string) =>
  stat(p).then(
    () => true,
    () => false,
  );

test('dedupe-identical rejects unknown flags and --apply elsewhere', async () => {
  const err = capture('stderr');
  expect(await main(['dedupe-identical', '--bogus'], deps)).toBe(2);
  expect(await main(['dedupe-identical', 'x'], deps)).toBe(2);
  expect(await main(['stage', '--apply'], deps)).toBe(2);
  expect(err()).toMatch(/usage/i);
});

test('dedupe-identical dry run writes a report and totals, deletes nothing', async () => {
  const { keep, lose } = await seedDupes();
  const out = capture('stdout');
  expect(await main(['dedupe-identical'], { ...deps, identical: deps })).toBe(0);
  expect(out()).toMatch(/^families 1, files to remove 1, bytes to free \d+, errors 0$/m);
  const files = await reports();
  expect(files).toHaveLength(1);
  expect(files[0]).toMatch(/dedupe-identical-\d{8}-\d{6}\.csv$/);
  const csv = await readFile(files[0]!, 'utf8');
  expect(csv.split('\n')[0]).toBe(
    'keeper_sha256,keeper_vault_path,keeper_names,removed_sha256,removed_vault_path,removed_names,rule,bytes',
  );
  expect(csv).toContain(vaultRel(lose, 'jpg'));
  expect(csv).toContain(keep);
  expect(await exists(layout.vaultFile(lose, 'jpg'))).toBe(true);
  expect(
    (await pgh.pool.query('SELECT state FROM contents WHERE state = $1', ['DUPLICATE'])).rowCount,
  ).toBe(0);
});

test('dedupe-identical --apply retires, removes, names the report -applied; re-run is a no-op', async () => {
  const { keep, lose } = await seedDupes();
  const out = capture('stdout');
  expect(await main(['dedupe-identical', '--apply'], { ...deps, identical: deps })).toBe(0);
  expect(out()).toMatch(/families 1, files to remove 1, bytes to free \d+, errors 0/);
  // A full tie falls to ingest time then sha: exactly one of the two stays.
  const left = [keep, lose].filter((h) => existsSync(layout.vaultFile(h, 'jpg')));
  expect(left).toHaveLength(1);
  expect((await reports())[0]).toMatch(/dedupe-identical-\d{8}-\d{6}-applied\.csv$/);
  const again = capture('stdout');
  expect(await main(['dedupe-identical', '--apply'], { ...deps, identical: deps })).toBe(0);
  expect(again()).toMatch(/families 0, files to remove 0, bytes to free 0, errors 0/);
});
