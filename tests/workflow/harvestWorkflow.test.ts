import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { ApplicationFailure, Context } from '@temporalio/activity';
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
  cursor: null,
  consecutive: 0,
  ...(reason ? { reason } : {}),
});

interface Fakes {
  calls: string[];
  /** The state argument of each sendBatch call. */
  sendStates: unknown[];
  scan: (runId: string, folderId: string) => Promise<ScanFolderResult> | ScanFolderResult;
  send: () => Promise<SendBatchResult> | SendBatchResult;
  dryRun: boolean;
  sendOnly: boolean;
  folders: { folderId: string; state: string }[];
}
function fakes(o: Partial<Fakes> = {}): Fakes {
  return {
    calls: [],
    sendStates: [],
    scan: () => SCAN_DONE,
    send: () => batch('DONE'),
    dryRun: false,
    sendOnly: false,
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
      sendOnly: cur.sendOnly,
      folders: cur.folders.map((f) => ({ sourceId: 's', relPath: '', ...f })),
    });
  },
  scanFolder: async (runId: string, folderId: string) => {
    cur.calls.push(`scanFolder ${folderId}`);
    return cur.scan(runId, folderId);
  },
  sendBatch: async (_runId: string, state?: unknown) => {
    cur.calls.push('sendBatch');
    cur.sendStates.push(state);
    return cur.send();
  },
  setFolderState: (_runId: string, folderId: string, state: string) => {
    cur.calls.push(`folder ${folderId} ${state}`);
    return Promise.resolve();
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

test('a send-only run never scans and sends until DONE', async () => {
  const f = fakes({ sendOnly: true });
  expect(await run(f)).toBe('done');
  expect(f.calls).toEqual([
    'loadRun 7',
    'state sending',
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
    'sendBatch',
    'state sending',
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
  expect(f.calls.slice(i, i + 5)).toEqual([
    'scanFolder f1',
    'folder f1 waiting_source', // spec §9 (M1)
    'state paused: Drive unplugged',
    'scanFolder f1',
    'state scanning',
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

test('repeated GATED with one reason writes one paused row and no sending until progress', async () => {
  let n = 0;
  const f = fakes({ send: () => (n++ < 3 ? batch('GATED', 'disk 81 %') : batch('MORE')) });
  const after = f.send;
  f.send = () => (n >= 5 ? batch('DONE') : after());
  await run(f);
  const i = f.calls.indexOf('state paused: disk 81 %');
  expect(f.calls.filter((c) => c.startsWith('state paused'))).toHaveLength(1);
  expect(f.calls.slice(i, i + 5)).toEqual([
    'state paused: disk 81 %',
    'sendBatch',
    'sendBatch',
    'sendBatch',
    'state sending',
  ]);
});

test('a pause during a gate sleep is honoured before the next batch', async () => {
  let n = 0;
  const f = fakes({ send: () => (n++ === 0 ? batch('GATED', 'disk 81 %') : batch('DONE')) });
  const h = await begin(f);
  await until(f, (c) => c.includes('state paused: disk 81 %'));
  await h.signal('pause');
  await until(f, (c) => c.includes('state paused: paused by you'));
  await new Promise((r) => setTimeout(r, 300));
  expect(count(f, 'sendBatch')).toBe(1);
  await h.signal('resume');
  expect(await h.result()).toBe('done');
});

test('cancel waits for the in-flight sendBatch before finishRun(cancelled)', async () => {
  const f = fakes({
    send: async () => {
      const ctx = Context.current();
      const beat = setInterval(() => ctx.heartbeat(), 50);
      try {
        await ctx.cancelled;
      } catch {
        // cancelled: clean up slowly
      } finally {
        clearInterval(beat);
      }
      await new Promise((r) => setTimeout(r, 400));
      f.calls.push('sendBatch returned');
      return batch('DONE');
    },
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('sendBatch'));
  await h.cancel();
  await expect(h.result()).rejects.toBeInstanceOf(WorkflowFailedError);
  expect(f.calls.slice(-3)).toEqual([
    'sendBatch returned',
    'finishRun cancelled',
    'startNextQueued',
  ]);
});

test('a non-Temporal error finishes the run failed and fails the workflow', async () => {
  const f = fakes({ send: () => batch('WEIRD' as SendBatchResult['outcome']) });
  await expect(run(f)).rejects.toBeInstanceOf(WorkflowFailedError);
  expect(f.calls.slice(-2)).toEqual([
    'finishRun failed: unexpected sendBatch outcome: WEIRD',
    'startNextQueued',
  ]);
});

test('a long unplug during the scan continues as new before the history limit', async () => {
  let n = 0;
  const f = fakes({
    scan: (_r, folder) =>
      folder === 'f1' && n++ < 30
        ? { ...SCAN_DONE, outcome: 'SOURCE_GONE', reason: 'Drive unplugged' }
        : SCAN_DONE,
  });
  const h = await begin(f, { runId: '7', maxHistory: 100 });
  expect(await h.result()).toBe('done');
  expect(count(f, 'scanFolder f1')).toBe(31);
  expect(count(f, 'finishRun done')).toBe(1);
  const d = await h.describe();
  expect(d.runId).not.toBe(h.firstExecutionRunId);
  // Every new execution starts in the scan phase (done folders are skipped, the cursor is in the DB).
  expect(count(f, 'loadRun 7')).toBeGreaterThan(1);
});

test('the send cursor and error count are carried between batches and across continue-as-new', async () => {
  let n = 0;
  const f = fakes({
    send: () => {
      n++;
      if (n === 3) {
        return {
          ...batch('PAUSED_ERRORS', '25 errors in a row'),
          cursor: { pos: 0, key: 'k3', id: '3' },
          consecutive: 25,
        };
      }
      if (n >= 60) return batch('DONE');
      return { ...batch('MORE'), cursor: { pos: 0, key: `k${n}`, id: String(n) }, consecutive: 1 };
    },
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('state paused: 25 errors in a row'));
  await h.signal('resume');
  expect(await h.result()).toBe('done');
  const st = f.sendStates as { cursor: { key: string } | null; consecutive: number }[];
  expect(st[0]).toEqual({ cursor: null, consecutive: 0 });
  expect(st[1]).toEqual({ cursor: { pos: 0, key: 'k1', id: '1' }, consecutive: 1 });
  // After a resume from PAUSED_ERRORS the count starts again; the cursor is kept.
  expect(st[3]).toEqual({ cursor: { pos: 0, key: 'k3', id: '3' }, consecutive: 0 });
  // Batch 51 runs in a new execution with the cursor of batch 50.
  expect(st[50]).toEqual({ cursor: { pos: 0, key: 'k50', id: '50' }, consecutive: 1 });
  const d = await h.describe();
  expect(d.runId).not.toBe(h.firstExecutionRunId);
});

test('DONE with an approval starts a new pass from the first folder', async () => {
  let n = 0;
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const f = fakes({
    send: async () => {
      if (n++ === 0) {
        await gate;
        return { ...batch('DONE'), cursor: { pos: 1, key: 'z', id: '9' } };
      }
      return batch('DONE');
    },
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('sendBatch'));
  await h.signal('itemsApproved');
  release();
  expect(await h.result()).toBe('done');
  expect(f.sendStates[1]).toEqual({ cursor: null, consecutive: 0 });
});

test('UNREADABLE during the scan pauses with the reason until resume, then rescans the folder', async () => {
  let n = 0;
  const f = fakes({
    scan: (_r, folder) =>
      folder === 'f1' && n++ === 0
        ? {
            ...SCAN_DONE,
            outcome: 'UNREADABLE',
            reason: 'cannot read Drive/DCIM: permission denied',
          }
        : SCAN_DONE,
  });
  const h = await begin(f);
  await until(f, (c) => c.includes('state paused: cannot read Drive/DCIM: permission denied'));
  await env.sleep('10m'); // no timer: it waits for the user, not the 60 s source re-check
  await new Promise((r) => setTimeout(r, 300));
  expect(count(f, 'scanFolder f1')).toBe(1);
  await h.signal('resume');
  expect(await h.result()).toBe('done');
  expect(count(f, 'scanFolder f1')).toBe(2);
  expect(f.calls.slice(-2)).toEqual(['finishRun done', 'startNextQueued']);
});
