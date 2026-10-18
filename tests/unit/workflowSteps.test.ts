// The workflow functions' branching, run directly in Node with `proxyActivities` faked. The
// time-skipping tests (tests/workflow) prove the same paths against a real Temporal server; this
// file also pins the activity options table, which only the proxies ever see.
import {
  ActivityFailure,
  ApplicationFailure,
  CancelledFailure,
  RetryState,
} from '@temporalio/common';
import { beforeEach, expect, test, vi } from 'vitest';
import type { ActivityOptions } from '@temporalio/workflow';

type Fake = (...a: unknown[]) => unknown;
const h = vi.hoisted(() => {
  const impl: Record<string, Fake> = {};
  const calls: { name: string; args: unknown[]; opts: unknown }[] = [];
  return { impl, calls };
});

vi.mock('@temporalio/workflow', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@temporalio/workflow')>();
  return {
    ...actual,
    log: { warn: () => undefined },
    sleep: () => Promise.resolve(),
    proxyActivities: (opts: unknown) =>
      new Proxy(
        {},
        {
          get:
            (_t, name: string) =>
            (...args: unknown[]) =>
              new Promise((resolve) => {
                h.calls.push({ name, args, opts });
                const fn = h.impl[name];
                if (!fn) throw new Error(`no fake for ${name}`);
                resolve(fn(...args)); // a throwing fake rejects, like a failed activity
              }),
        },
      ),
  };
});

const wf = await import('../../src/workflows.js');

const SHA = 'c'.repeat(64);
const DECODED = {
  facts: { width: 1, height: 1, phash: '1', dhash: '1', mime: 'image/jpeg' },
  quarantineReason: null,
};
const STILL = {
  fileId: '1',
  sha256: SHA,
  ext: 'jpg',
  state: 'INGESTED',
  isNew: true,
  kind: 'still',
};
const REF = { relPath: 'a.jpg', size: '1', mtimeNs: '1' };
const activityFailure = (cause: Error) =>
  new ActivityFailure(
    'Activity task failed',
    'decodeContent',
    '1',
    RetryState.MAXIMUM_ATTEMPTS_REACHED,
    'id',
    cause,
  );

beforeEach(() => {
  h.calls.length = 0;
  h.impl = {
    ingestFile: () => STILL,
    decodeContent: () => DECODED,
    recordHash: () => 'HASHED',
    clusterContent: () => 1,
    markFailed: () => undefined,
    maybeBuildStaging: (force) => ({ gen: force ? 1 : null, entries: 0, skipped: !force }),
    scanAndStart: () => ({ started: 1, pendingFiles: 1, pendingBytes: '1' }),
  };
});

const names = () => h.calls.map((c) => c.name);
const optsOf = (name: string) => h.calls.find((c) => c.name === name)?.opts as ActivityOptions;

test('a new still runs every step, each with its queue, timeouts and retry policy', async () => {
  expect(await wf.FileWorkflow(REF as never)).toBe('CLUSTERED');
  expect(names()).toEqual(['ingestFile', 'decodeContent', 'recordHash', 'clusterContent']);
  const forever = { initialInterval: '1s', backoffCoefficient: 2, maximumInterval: '5m' };
  expect(optsOf('ingestFile')).toEqual({
    taskQueue: 'io',
    startToCloseTimeout: '1h',
    heartbeatTimeout: '30s',
    retry: forever,
  });
  expect(optsOf('decodeContent')).toEqual({
    taskQueue: 'cpu',
    startToCloseTimeout: '2m',
    retry: { ...forever, maximumAttempts: 1 }, // the workflow retries and counts attempts
  });
  expect(optsOf('recordHash')).toEqual({
    taskQueue: 'io',
    startToCloseTimeout: '1m',
    retry: forever,
  });
  expect(optsOf('clusterContent')).toEqual({
    taskQueue: 'cluster',
    startToCloseTimeout: '15m', // a cache reload at 300k+ contents
    heartbeatTimeout: '30s',
    retry: forever,
  });
});

test('every activity has a start-to-close timeout; only decode caps its attempts', () => {
  const o = wf.ACTIVITY_OPTIONS;
  const all: [string, ActivityOptions][] = Object.entries(o);
  expect(all.map(([k]) => k).sort()).toEqual([
    'clusterContent',
    'decodeContent',
    'ingestFile',
    'markFailed',
    'maybeBuildStaging',
    'recordHash',
    'scanAndStart',
  ]);
  for (const [k, v] of all) {
    expect(v.startToCloseTimeout, k).toBeDefined();
    if (k !== 'decodeContent') expect(v.retry?.maximumAttempts, k).toBeUndefined();
  }
  expect(o.markFailed).toMatchObject({ taskQueue: 'io', startToCloseTimeout: '1m' });
  expect(o.maybeBuildStaging).toMatchObject({
    taskQueue: 'io',
    startToCloseTimeout: '2h',
    heartbeatTimeout: '30s',
  });
  expect(o.scanAndStart).toMatchObject({
    taskQueue: 'io',
    startToCloseTimeout: '10m',
    heartbeatTimeout: '30s',
  });
});

test('poisonAttempts caps poison decode attempts', async () => {
  h.impl['decodeContent'] = () => {
    throw activityFailure(ApplicationFailure.retryable('timed out', 'Poison'));
  };
  expect(await wf.FileWorkflow(REF, { poisonAttempts: 3 })).toBe('FAILED');
  expect(names().filter((n) => n === 'decodeContent')).toHaveLength(3);
});

test('a decode killed every time is FAILED decode_killed after MAX_DECODE_ATTEMPTS', async () => {
  h.impl['decodeContent'] = () => {
    throw activityFailure(ApplicationFailure.retryable('SIGKILL', 'DecodeKilled'));
  };
  expect(await wf.ContentWorkflow(SHA, 'jpg')).toBe('FAILED');
  expect(names().filter((n) => n === 'decodeContent')).toHaveLength(wf.MAX_DECODE_ATTEMPTS);
  expect(h.calls.at(-1)).toMatchObject({
    name: 'markFailed',
    args: [SHA, 'decode_killed: SIGKILL'],
  });
});

test('decode exhaustion marks FAILED with the cause', async () => {
  h.impl['decodeContent'] = () => {
    throw activityFailure(ApplicationFailure.retryable('timed out', 'Poison'));
  };
  expect(await wf.ContentWorkflow(SHA, 'jpg')).toBe('FAILED');
  expect(h.calls.at(-1)).toMatchObject({ name: 'markFailed', args: [SHA, 'timed out'] });
});

test('decode cancellation is not poison', async () => {
  h.impl['decodeContent'] = () => {
    throw activityFailure(new CancelledFailure('cancelled'));
  };
  await expect(wf.ContentWorkflow(SHA, 'jpg')).rejects.toBeInstanceOf(ActivityFailure);
  expect(names()).not.toContain('markFailed');
});

test('a non-activity error from decode propagates', async () => {
  h.impl['decodeContent'] = () => {
    throw new TypeError('bug');
  };
  await expect(wf.runContentSteps(SHA, 'jpg', 5)).rejects.toThrow('bug');
});

test('quarantine ends before clustering', async () => {
  h.impl['recordHash'] = () => 'QUARANTINED';
  expect(await wf.runContentSteps(SHA, 'jpg', 5)).toBe('QUARANTINED');
  expect(names()).not.toContain('clusterContent');
});

test.each([
  ['COPY', { ...STILL, state: 'COPY', isNew: false, kind: null }],
  ['QUARANTINED', { ...STILL, state: 'QUARANTINED', sha256: null, ext: null, kind: null }],
  ['INGESTED', { ...STILL, kind: 'motion' }],
  ['INGESTED', { ...STILL, kind: 'other' }],
])('ingest outcome %s without new still content ends the workflow', async (state, r) => {
  h.impl['ingestFile'] = () => r;
  expect(await wf.FileWorkflow(REF as never)).toBe(state);
  expect(names()).toEqual(['ingestFile']);
});

test('an INGESTED still without sha256 fails visibly', async () => {
  h.impl['ingestFile'] = () => ({ ...STILL, sha256: null });
  await expect(wf.FileWorkflow(REF as never)).rejects.toMatchObject({ type: 'UnexpectedState' });
});

test('ScanWorkflow and StagingWorkflow delegate to their activities', async () => {
  expect(await wf.ScanWorkflow()).toEqual({ started: 1, pendingFiles: 1, pendingBytes: '1' });
  expect(await wf.StagingWorkflow()).toEqual({ gen: null, entries: 0, skipped: true });
  expect(await wf.StagingWorkflow(true)).toEqual({ gen: 1, entries: 0, skipped: false });
});
