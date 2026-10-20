import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { ApplicationFailure } from '@temporalio/activity';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { bundleWorkflowCode, DefaultLogger, Runtime, Worker } from '@temporalio/worker';
import {
  WorkflowFailedError,
  type WorkflowHandleWithFirstExecutionRunId,
} from '@temporalio/client';
import { afterAll, beforeAll, expect, test } from 'vitest';
import type { ScanFolderResult } from '../../src/harvest/scan.js';
import type { SendBatchResult } from '../../src/harvest/send.js';
import { STALLED_REASON } from '../../src/harvest/throttle.js';
import type { HarvestWorkflow } from '../../src/harvest/workflow.js';

const SCAN_DONE: ScanFolderResult = {
  outcome: 'DONE',
  dirs: 1,
  photos: 1,
  maybes: 0,
  junk: {},
  errors: 0,
};
const batch = (outcome: SendBatchResult['outcome'], reason?: string): SendBatchResult => ({
  outcome,
  sent: 0,
  known: 0,
  dups: 0,
  failed: 0,
  ...(reason ? { reason } : {}),
});

interface Fakes {
  calls: string[];
  scan: (runId: string, folderId: string) => Promise<ScanFolderResult> | ScanFolderResult;
  send: () => Promise<SendBatchResult> | SendBatchResult;
  dryRun: boolean;
  folders: { folderId: string; state: string }[];
}
function fakes(o: Partial<Fakes> = {}): Fakes {
  return {
    calls: [],
    scan: () => SCAN_DONE,
    send: () => batch('DONE'),
    dryRun: false,
    folders: [
      { folderId: 'f1', state: 'queued' },
      { folderId: 'f2', state: 'queued' },
    ],
    ...o,
  };
}

let env: TestWorkflowEnvironment;
let cur: Fakes = fakes();
let worker: Worker;
let running: Promise<unknown>;

const activities = {
  loadRun: (runId: string) => {
    cur.calls.push(`loadRun ${runId}`);
    return Promise.resolve({
      dryRun: cur.dryRun,
      folders: cur.folders.map((f) => ({ sourceId: 's', relPath: '', ...f })),
    });
  },
  scanFolder: async (runId: string, folderId: string) => {
    cur.calls.push(`scanFolder ${folderId}`);
    return cur.scan(runId, folderId);
  },
  sendBatch: async () => {
    cur.calls.push('sendBatch');
    return cur.send();
  },
  setRunState: (_runId: string, state: string, reason?: string | null) => {
    cur.calls.push(reason ? `state ${state}: ${reason}` : `state ${state}`);
    return Promise.resolve();
  },
  finishRun: (_runId: string, outcome: string, error?: string) => {
    cur.calls.push(error ? `finishRun ${outcome}: ${error}` : `finishRun ${outcome}`);
    return Promise.resolve();
  },
  startNextQueued: () => {
    cur.calls.push('startNextQueued');
    return Promise.resolve(null);
  },
  linkFiles: () => Promise.resolve(0),
};

beforeAll(async () => {
  Runtime.install({
    logger: new DefaultLogger('ERROR'),
    telemetryOptions: { logging: { filter: { core: 'ERROR', other: 'ERROR' }, forward: {} } },
  });
  env = await TestWorkflowEnvironment.createTimeSkipping();
  const workflowBundle = await bundleWorkflowCode({
    workflowsPath: fileURLToPath(new URL('../../src/workflows.ts', import.meta.url)),
    logger: new DefaultLogger('ERROR'),
  });
  worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: 'harvest',
    workflowBundle,
    activities,
  });
  running = worker.run();
}, 120_000);

afterAll(async () => {
  worker.shutdown();
  await running;
  await env.teardown();
});

type Handle = WorkflowHandleWithFirstExecutionRunId<typeof HarvestWorkflow>;
async function begin(f: Fakes, input: Parameters<typeof HarvestWorkflow>[0] = { runId: '7' }) {
  cur = f;
  return (await env.client.workflow.start<typeof HarvestWorkflow>('HarvestWorkflow', {
    taskQueue: 'harvest',
    workflowId: `harvest-${input.runId}-${randomUUID()}`,
    args: [input],
  })) as Handle;
}
const run = async (f: Fakes, input?: Parameters<typeof HarvestWorkflow>[0]) =>
  (await begin(f, input)).result();
/** Runs a workflow whose send/scan step answers `first` once, then DONE; returns the server-time gap. */
async function gapAfter(first: SendBatchResult): Promise<number> {
  const times: number[] = [];
  let n = 0;
  const f = fakes({
    send: async () => {
      times.push(await env.currentTimeMs());
      return n++ === 0 ? first : batch('DONE');
    },
  });
  await run(f);
  return times[1]! - times[0]!;
}
/** Waits (real time) until the fakes have recorded a call. */
async function until(f: Fakes, pred: (calls: string[]) => boolean): Promise<void> {
  for (let i = 0; i < 400 && !pred(f.calls); i++) await new Promise((r) => setTimeout(r, 25));
  expect(pred(f.calls)).toBe(true);
}
const count = (f: Fakes, name: string) => f.calls.filter((c) => c === name).length;

test('scans every folder then sends until DONE then finishes', async () => {
  const f = fakes({
    send: (() => {
      let n = 0;
      return () => batch(++n < 3 ? 'MORE' : 'DONE');
    })(),
  });
  expect(await run(f)).toBe('done');
  expect(f.calls).toEqual([
    'loadRun 7',
    'state scanning',
    'scanFolder f1',
    'scanFolder f2',
    'state sending',
    'sendBatch',
    'sendBatch',
    'sendBatch',
    'finishRun done',
    'startNextQueued',
  ]);
});

test('folders already done are not rescanned', async () => {
  const f = fakes({
    folders: [
      { folderId: 'f1', state: 'done' },
      { folderId: 'f2', state: 'scanning' },
    ],
  });
  await run(f);
  expect(f.calls.filter((c) => c.startsWith('scanFolder'))).toEqual(['scanFolder f2']);
});

test('dry run never calls sendBatch', async () => {
  const f = fakes({ dryRun: true });
  expect(await run(f)).toBe('done');
  expect(count(f, 'sendBatch')).toBe(0);
  expect(f.calls.slice(-2)).toEqual(['finishRun done', 'startNextQueued']);
});

test('GATED sleeps 30 s and retries', async () => {
  let n = 0;
  const f = fakes({ send: () => (n++ === 0 ? batch('GATED', 'disk 81 %') : batch('DONE')) });
  const t0 = Date.now();
  await run(f);
  expect(Date.now() - t0).toBeLessThan(20_000); // time skipping, not real waiting
  const i = f.calls.indexOf('state paused: disk 81 %');
  expect(i).toBeGreaterThan(0);
  expect(f.calls.slice(i, i + 3)).toEqual([
    'state paused: disk 81 %',
    'state sending',
    'sendBatch',
  ]);
});

test('a stalled gate waits 5 minutes', async () => {
  const gap = await gapAfter(batch('GATED', STALLED_REASON));
  expect(gap).toBeGreaterThanOrEqual(5 * 60_000);
  expect(gap).toBeLessThan(6 * 60_000);
});

test('a plain gate waits 30 seconds', async () => {
  const gap = await gapAfter(batch('GATED', 'disk 81 %'));
  expect(gap).toBeGreaterThanOrEqual(30_000);
  expect(gap).toBeLessThan(60_000);
});

test('pause blocks sending until resume', async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  let n = 0;
  const f = fakes({
    send: async () => {
      if (n++ === 0) {
        await gate;
        return batch('MORE');
      }
      return batch('DONE');
    },
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('sendBatch'));
  await h.signal('pause');
  release();
  await until(f, (c) => c.includes('state paused: paused by you'));
  await new Promise((r) => setTimeout(r, 300));
  expect(count(f, 'sendBatch')).toBe(1);
  expect(await h.query('status')).toEqual({ phase: 'send', paused: true });
  await h.signal('resume');
  expect(await h.result()).toBe('done');
  expect(count(f, 'sendBatch')).toBe(2);
});

test('SOURCE_GONE during send pauses and resumes when the source returns', async () => {
  const gap = await gapAfter(batch('SOURCE_GONE', 'Drive unplugged'));
  expect(gap).toBeGreaterThanOrEqual(60_000);
  expect(gap).toBeLessThan(120_000);
});

test('SOURCE_GONE during send shows the unplugged reason', async () => {
  let n = 0;
  const f = fakes({
    send: () => (n++ === 0 ? batch('SOURCE_GONE', 'Drive unplugged') : batch('DONE')),
  });
  await run(f);
  expect(f.calls).toContain('state paused: Drive unplugged');
  expect(f.calls.filter((c) => c === 'state sending')).toHaveLength(2);
  expect(count(f, 'sendBatch')).toBe(2);
});

test('SOURCE_GONE during scan pauses and retries the same folder', async () => {
  let n = 0;
  const f = fakes({
    scan: (_r, folder) =>
      folder === 'f1' && n++ === 0
        ? { ...SCAN_DONE, outcome: 'SOURCE_GONE', reason: 'Drive unplugged' }
        : SCAN_DONE,
  });
  await run(f);
  const i = f.calls.indexOf('scanFolder f1');
  expect(f.calls.slice(i, i + 4)).toEqual([
    'scanFolder f1',
    'state paused: Drive unplugged',
    'state scanning',
    'scanFolder f1',
  ]);
});

test('DISK_FULL waits for resume', async () => {
  let n = 0;
  const f = fakes({ send: () => (n++ === 0 ? batch('DISK_FULL', 'disk full') : batch('DONE')) });
  const h = await begin(f);
  await until(f, (c) => c.includes('state paused: disk full'));
  await new Promise((r) => setTimeout(r, 300));
  expect(count(f, 'sendBatch')).toBe(1);
  await h.signal('resume');
  expect(await h.result()).toBe('done');
  expect(count(f, 'sendBatch')).toBe(2);
});

test('PAUSED_ERRORS waits for resume', async () => {
  let n = 0;
  const f = fakes({
    send: () => (n++ === 0 ? batch('PAUSED_ERRORS', '25 errors in a row') : batch('DONE')),
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('state paused: 25 errors in a row'));
  await h.signal('resume');
  expect(await h.result()).toBe('done');
});

test('cancel runs finishRun(cancelled) and startNextQueued', async () => {
  const f = fakes({ send: () => batch('DISK_FULL', 'disk full') });
  const h = await begin(f);
  await until(f, (c) => c.includes('state paused: disk full'));
  await h.cancel();
  await expect(h.result()).rejects.toBeInstanceOf(WorkflowFailedError);
  expect(f.calls.slice(-2)).toEqual(['finishRun cancelled', 'startNextQueued']);
});

test('an unexpected failure finishes the run as failed and starts the next', async () => {
  const f = fakes({
    scan: () => {
      throw ApplicationFailure.nonRetryable('boom');
    },
  });
  await expect(run(f)).rejects.toBeInstanceOf(WorkflowFailedError);
  expect(f.calls.slice(-2)).toEqual(['finishRun failed: boom', 'startNextQueued']);
});

test('itemsApproved after DONE triggers one more pass', async () => {
  let n = 0;
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const f = fakes({
    send: async () => {
      if (n++ === 0) await gate;
      return batch('DONE');
    },
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('sendBatch'));
  await h.signal('itemsApproved');
  release();
  expect(await h.result()).toBe('done');
  expect(count(f, 'sendBatch')).toBe(2);
});

test('continues as new after 50 batches with phase send', async () => {
  let n = 0;
  const f = fakes({ send: () => batch(++n < 120 ? 'MORE' : 'DONE') });
  const h = await begin(f);
  expect(await h.result()).toBe('done');
  expect(count(f, 'sendBatch')).toBe(120);
  expect(count(f, 'scanFolder f1')).toBe(1);
  expect(count(f, 'finishRun done')).toBe(1);
  const d = await h.describe();
  expect(d.runId).not.toBe(h.firstExecutionRunId);
});

test('starting with phase send skips the scan', async () => {
  const f = fakes();
  await run(f, { runId: '9', phase: 'send', batches: 0 });
  expect(f.calls.some((c) => c.startsWith('scanFolder'))).toBe(false);
  expect(count(f, 'sendBatch')).toBe(1);
});
