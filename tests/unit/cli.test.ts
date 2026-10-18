import { chmod, mkdir, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import type { Client } from '@temporalio/client';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import type { Deps } from '../../src/activities/index.js';
import { main } from '../../src/cli.js';
import { vaultRel, type DataLayout } from '../../src/fsops.js';
import { testDeps } from '../helpers/activityEnv.js';
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

test('retry-failed resets and starts', async () => {
  await seedContent({ state: 'FAILED', lastError: 'x' });
  await seedContent({ state: 'FAILED', lastError: 'decode_killed: child got SIGKILL' });
  await seedContent({ state: 'CLUSTERED' });
  const f = recordingClient();
  const out = capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: f.client })).toBe(0);
  const states = (await pgh.pool.query<{ state: string }>('SELECT state FROM contents')).rows;
  expect(states.map((r) => r.state).sort()).toEqual(['CLUSTERED', 'INGESTED', 'INGESTED']);
  expect(f.started.map((w) => w.workflowType)).toEqual(['ContentWorkflow', 'ContentWorkflow']);
  expect(f.started.every((w) => /^content-[0-9a-f]{64}-retry-\d+$/.test(w.workflowId))).toBe(true);
  expect(f.started.every((w) => w.taskQueue === 'main')).toBe(true);
  expect(f.started[0]!.args).toEqual([expect.stringMatching(/^[0-9a-f]{64}$/), 'jpg']);
  expect(out()).toContain('2');
});

test('retry-failed puts a row back to FAILED when its workflow cannot start', async () => {
  const hash = await seedContent({ state: 'FAILED', lastError: 'boom' });
  const client = {
    workflow: { start: () => Promise.reject(new Error('temporal down')) },
  } as unknown as Client;
  const err = capture('stderr');
  capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: client })).toBe(1);
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
  expect(await main(['retry-failed'], { ...deps, temporal: client })).toBe(1);
  expect(err()).toContain('plain string');
});

test('retry-failed with nothing failed starts nothing', async () => {
  const f = recordingClient();
  const out = capture('stdout');
  expect(await main(['retry-failed'], { ...deps, temporal: f.client })).toBe(0);
  expect(f.started).toEqual([]);
  expect(out()).toContain('0');
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
  });
});

test('doctor reports zeros on a clean system', async () => {
  const out = capture('stdout');
  expect(await main(['doctor'], deps)).toBe(0);
  expect(JSON.parse(out())).toEqual({
    orphanedClaimed: 0,
    filesStuckRetrying: 0,
    contentsStale: 0,
  });
});
