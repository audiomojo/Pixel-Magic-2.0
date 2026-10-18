import { appendFile, chmod, mkdir, rm, stat, unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { WorkflowExecutionAlreadyStartedError, type Client } from '@temporalio/client';
import type pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import { createActivities, type Deps } from '../../src/activities/index.js';
import type { Settings } from '../../src/config.js';
import type { DataLayout } from '../../src/fsops.js';
import { fileWorkflowId } from '../../src/ids.js';
import type { FileRef } from '../../src/types.js';
import { runActivity, testDeps } from '../helpers/activityEnv.js';
import { JPEG_A } from '../helpers/images.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

/** Test seam: when set, the scanner's readdir awaits this first (a slow drop-zone listing). */
const slowReaddir = vi.hoisted(() => ({ gate: null as (() => Promise<void>) | null }));
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    readdir: async (...a: Parameters<typeof actual.readdir>) => {
      const gate = slowReaddir.gate;
      slowReaddir.gate = null;
      if (gate) await gate();
      return actual.readdir(...a);
    },
  };
});

interface Start {
  workflowType: unknown;
  opts: {
    taskQueue: string;
    workflowId: string;
    args: [FileRef, { poisonAttempts: number }];
    workflowIdReusePolicy: string;
    workflowIdConflictPolicy: string;
  };
}

/** A Temporal client double: records starts, reports `running` FileWorkflows, fails chosen starts. */
function makeFakeClient() {
  let running = 0;
  let counts: string[] = [];
  let starts: Start[] = [];
  let failures = new Map<string, Error>();
  let onStart: ((relPath: string) => Promise<void>) | null = null;
  const client = {
    workflow: {
      count: (query: string) => {
        counts.push(query);
        return Promise.resolve({ count: running, groups: [] });
      },
      start: async (workflowType: unknown, opts: Start['opts']) => {
        const relPath = opts.args[0].relPath;
        await onStart?.(relPath);
        const err = failures.get(relPath);
        if (err) throw err;
        starts.push({ workflowType, opts });
        return { workflowId: opts.workflowId };
      },
    },
  };
  return {
    client: client as unknown as Client,
    counts: () => counts,
    starts: () => starts,
    startedRelPaths: () => starts.map((s) => s.opts.args[0].relPath),
    setRunning: (n: number) => {
      running = n;
    },
    failStartFor: (relPath: string, err: Error) => failures.set(relPath, err),
    beforeStart: (fn: (relPath: string) => Promise<void>) => {
      onStart = fn;
    },
    reset: () => {
      running = 0;
      counts = [];
      starts = [];
      failures = new Map();
      onStart = null;
    },
  };
}

const fakeClient = makeFakeClient();
let pgh: PgHandle;
let pool: pg.Pool;
let deps: Deps;
let layout: DataLayout;
let settings: Settings;
let close: () => Promise<void>;
let actsWithFakeClient: ReturnType<typeof createActivities>;
const growers: NodeJS.Timeout[] = [];

beforeAll(async () => {
  pgh = await startPg();
  pool = pgh.pool;
  ({ deps, layout, close } = await testDeps(pgh, { temporal: fakeClient.client }));
  deps.settings.stableSeconds = 1; // keep the two snapshots close together in tests
  deps.settings.maxInFlight = 3;
  settings = deps.settings;
  actsWithFakeClient = createActivities(deps);
}, 120_000);
afterAll(async () => {
  await close();
  await pgh.stop();
});
beforeEach(async () => {
  await pgh.truncate();
  fakeClient.reset();
  await rm(layout.dropzone, { recursive: true, force: true });
  await layout.ensure();
});
afterEach(() => {
  for (const t of growers.splice(0)) clearInterval(t);
});

/** Creates the file now and keeps appending to it, so its size never holds still. */
function growFileDuringScan(path: string): void {
  void writeFile(path, 'start');
  growers.push(setInterval(() => void appendFile(path, 'more'), 100));
}

const scan = () => runActivity(actsWithFakeClient.scanAndStart, []);
const stats = async () =>
  (
    await pool.query<{ pending_files: bigint; pending_bytes: bigint; scanned_at: Date | null }>(
      'SELECT pending_files, pending_bytes, scanned_at FROM scan_stats WHERE id = 1',
    )
  ).rows[0]!;

test('scan starts only stable, non-ignored files', async () => {
  growFileDuringScan(join(layout.dropzone, 'big.mov'));
  await writeFile(join(layout.dropzone, 'ok.jpg'), JPEG_A);
  await writeFile(join(layout.dropzone, '.DS_Store'), 'x');
  const r = await runActivity(actsWithFakeClient.scanAndStart, []);
  expect(fakeClient.startedRelPaths()).toEqual(['ok.jpg']);
  expect(r.pendingFiles).toBe(2);
});

test('scan starts FileWorkflow on main with the id and policies of spec §4.3', async () => {
  await mkdir(join(layout.dropzone, 'trip', 'day 1'), { recursive: true });
  const path = join(layout.dropzone, 'trip', 'day 1', 'é.jpg');
  await writeFile(path, JPEG_A);
  const st = await stat(path, { bigint: true });
  const r = await scan();
  const ref: FileRef = {
    relPath: 'trip/day 1/é.jpg',
    size: String(st.size),
    mtimeNs: String(st.mtimeNs),
  };
  expect(r).toEqual({ started: 1, pendingFiles: 1, pendingBytes: String(JPEG_A.length) });
  const [s] = fakeClient.starts();
  expect(s!.workflowType).toBe('FileWorkflow');
  expect(s!.opts).toEqual({
    taskQueue: 'main',
    workflowId: fileWorkflowId(ref),
    args: [ref, { poisonAttempts: settings.poisonAttempts }],
    workflowIdReusePolicy: 'ALLOW_DUPLICATE',
    workflowIdConflictPolicy: 'FAIL',
  });
  expect(fakeClient.counts()).toEqual([
    "WorkflowType='FileWorkflow' AND ExecutionStatus='Running'",
  ]);
});

test('scan writes scan_stats with pending files and bytes', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A);
  await mkdir(join(layout.dropzone, 'sub'));
  await writeFile(join(layout.dropzone, 'sub', 'b.bin'), 'abc');
  await mkdir(join(layout.dropzone, 'empty'));
  await writeFile(join(layout.dropzone, 'sub', 'x.part'), 'partial');
  const r = await scan();
  const s = await stats();
  expect(r.pendingFiles).toBe(2);
  expect(r.pendingBytes).toBe(String(JPEG_A.length + 3));
  expect(s.pending_files).toBe(2n);
  expect(s.pending_bytes).toBe(BigInt(JPEG_A.length + 3));
  expect(s.scanned_at).not.toBeNull();
});

test('scan heartbeats between its snapshots', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A);
  const beats: unknown[] = [];
  await runActivity(actsWithFakeClient.scanAndStart, [], { onHeartbeat: (d) => beats.push(d) });
  expect(beats.length).toBeGreaterThan(0);
});

test('scan keeps heartbeating through a slow drop-zone listing', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A);
  // The listing only finishes once a heartbeat arrives while it is pending (or after 20 s,
  // which is well past the 30 s heartbeat timeout's comfort zone and fails the assertion).
  let pending = false;
  let beatDuringListing = false;
  let release: () => void = () => undefined;
  slowReaddir.gate = () =>
    new Promise<void>((resolve) => {
      pending = true;
      release = () => {
        if (!pending) return;
        pending = false;
        beatDuringListing = true;
        resolve();
      };
      setTimeout(() => {
        pending = false;
        resolve();
      }, 20_000);
    });
  await runActivity(actsWithFakeClient.scanAndStart, [], { onHeartbeat: () => release() });
  expect(beatDuringListing).toBe(true);
  expect(fakeClient.startedRelPaths()).toEqual(['a.jpg']);
}, 30_000);

test('scan tolerates WorkflowExecutionAlreadyStartedError', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A);
  await writeFile(join(layout.dropzone, 'b.jpg'), JPEG_A);
  fakeClient.failStartFor(
    'a.jpg',
    new WorkflowExecutionAlreadyStartedError('already running', 'file-x', 'FileWorkflow'),
  );
  const r = await runActivity(actsWithFakeClient.scanAndStart, []);
  expect(r.started).toBe(1);
  expect(fakeClient.startedRelPaths()).toEqual(['b.jpg']);
});

test('scan surfaces any other start failure', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A);
  fakeClient.failStartFor('a.jpg', new Error('temporal unavailable'));
  await expect(scan()).rejects.toThrow('temporal unavailable');
});

test('scan backpressure', async () => {
  fakeClient.setRunning(settings.maxInFlight);
  await writeFile(join(layout.dropzone, 'ok.jpg'), JPEG_A);
  const r = await runActivity(actsWithFakeClient.scanAndStart, []);
  expect(r.started).toBe(0);
  expect(fakeClient.startedRelPaths()).toEqual([]);
  expect((await stats()).pending_files).toBe(1n);
});

test('scan starts at most maxInFlight - running, in path order', async () => {
  fakeClient.setRunning(settings.maxInFlight - 2);
  for (const n of ['d.jpg', 'a.jpg', 'c.jpg', 'b.jpg'])
    await writeFile(join(layout.dropzone, n), JPEG_A);
  const r = await scan();
  expect(r.started).toBe(2);
  expect(fakeClient.startedRelPaths()).toEqual(['a.jpg', 'b.jpg']);
});

test('scan skips a file claimed (or changed) after its snapshots', async () => {
  // A FileWorkflow from an earlier scan may claim the file between this scan's snapshots and
  // its start call; starting then would re-run a finished workflow id on a vanished file.
  for (const n of ['a.jpg', 'b.jpg', 'c.jpg']) await writeFile(join(layout.dropzone, n), JPEG_A);
  await mkdir(join(layout.dropzone, 'd'));
  await writeFile(join(layout.dropzone, 'd', 'e.jpg'), JPEG_A);
  fakeClient.beforeStart(async (relPath) => {
    if (relPath === 'a.jpg') {
      await unlink(join(layout.dropzone, 'b.jpg'));
      await writeFile(join(layout.dropzone, 'b.jpg'), 'a different drop');
      await unlink(join(layout.dropzone, 'c.jpg'));
      await rm(join(layout.dropzone, 'd'), { recursive: true });
      await writeFile(join(layout.dropzone, 'd'), 'now a file');
    }
  });
  const r = await scan();
  expect(fakeClient.startedRelPaths()).toEqual(['a.jpg']);
  expect(r.started).toBe(1);
});

test('scan without a drop zone directory reports nothing pending', async () => {
  await rm(layout.dropzone, { recursive: true });
  expect(await scan()).toEqual({ started: 0, pendingFiles: 0, pendingBytes: '0' });
});

test('scan surfaces a drop zone it cannot read', async () => {
  await rm(layout.dropzone, { recursive: true });
  await writeFile(layout.dropzone, 'not a directory');
  try {
    await expect(scan()).rejects.toMatchObject({ code: 'ENOTDIR' });
  } finally {
    await rm(layout.dropzone);
  }
});

test('scan surfaces a file it cannot stat before starting it', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A);
  await mkdir(join(layout.dropzone, 'z'));
  await writeFile(join(layout.dropzone, 'z', 'f.jpg'), JPEG_A);
  fakeClient.beforeStart(async (relPath) => {
    if (relPath === 'a.jpg') await chmod(join(layout.dropzone, 'z'), 0o000);
  });
  try {
    await expect(scan()).rejects.toMatchObject({ code: 'EACCES' });
  } finally {
    await chmod(join(layout.dropzone, 'z'), 0o755);
  }
});

test('scan needs a Temporal client', async () => {
  const acts = createActivities({ ...deps, temporal: undefined } as unknown as Deps);
  await expect(runActivity(acts.scanAndStart, [])).rejects.toThrow('needs a Temporal client');
});

test('scan of an empty drop zone starts nothing', async () => {
  const r = await scan();
  expect(r).toEqual({ started: 0, pendingFiles: 0, pendingBytes: '0' });
});
