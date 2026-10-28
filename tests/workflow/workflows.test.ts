import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { ApplicationFailure } from '@temporalio/activity';
import { WorkflowFailedError } from '@temporalio/client';
import { temporal } from '@temporalio/proto';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import {
  bundleWorkflowCode,
  DefaultLogger,
  Runtime,
  Worker,
  type WorkflowBundleWithSourceMap,
} from '@temporalio/worker';
import { afterAll, beforeAll, expect, test } from 'vitest';
import type { Activities } from '../../src/activities/index.js';
import { fileWorkflowId } from '../../src/ids.js';
import type {
  BuildResult,
  DecodeResult,
  FileRef,
  IngestResult,
  ReclaimOutcome,
  ScanResult,
  SweepResult,
} from '../../src/types.js';
import type {
  ContentWorkflow,
  CopySweepWorkflow,
  FileWorkflow,
  PurgeSyncWorkflow,
  ScanWorkflow,
  StagingWorkflow,
} from '../../src/workflows.js';

type Fakeable<T> = T | ((...args: never[]) => T | Promise<T>);
interface Overrides {
  ingest?: Fakeable<IngestResult>;
  decode?: Fakeable<DecodeResult>;
  record?: Fakeable<'HASHED' | 'QUARANTINED'>;
  dedupe?: Fakeable<'UNIQUE' | 'KEPT' | 'DUPLICATE'>;
  cluster?: Fakeable<number | null>;
  stage?: (force: boolean) => BuildResult;
  scan?: Fakeable<ScanResult>;
  reclaim?: Fakeable<ReclaimOutcome>;
  sweep?: Fakeable<SweepResult>;
  removePurged?: Fakeable<SweepResult>;
}
type Impl = Pick<
  Activities,
  | 'ingestFile'
  | 'decodeContent'
  | 'recordHash'
  | 'dedupeIdentical'
  | 'markFailed'
  | 'reopenFailed'
  | 'clusterContent'
  | 'maybeBuildStaging'
  | 'scanAndStart'
  | 'reclaimCopy'
  | 'sweepCopies'
  | 'syncPurgeLog'
  | 'removePurgedBatch'
>;
interface Fakes {
  impl: Impl;
  calls: string[];
  args: Record<string, unknown[][]>;
}

const SHA = 'f'.repeat(64);
const REF: FileRef = { relPath: 'trip/IMG_1.HEIC', size: '1234', mtimeNs: '1700000000000000000' };
const INGESTED_STILL: IngestResult = {
  fileId: '1',
  sha256: SHA,
  ext: 'heic',
  state: 'INGESTED',
  kind: 'still',
};
const INGESTED_MOTION: IngestResult = { ...INGESTED_STILL, ext: 'mov', kind: 'motion' };
// Task 6: COPY owns no content, so its kind is null.
const COPY: IngestResult = { ...INGESTED_STILL, state: 'COPY', kind: null };
const DECODED: DecodeResult = {
  facts: { width: 4, height: 3, phash: '1', dhash: '2', mime: 'image/heic' },
  quarantineReason: null,
};

const alwaysPoison = (): never => {
  throw ApplicationFailure.retryable('decode timed out', 'Poison');
};
/** Fails like an outage (an ordinary, retryable error) n times, then returns value. */
function failNTimes<T>(n: number, value: T): () => T {
  let left = n;
  return () => {
    if (left-- > 0) throw new Error(`outage (${String(left + 1)} left)`);
    return value;
  };
}

function fakes(o: Overrides): Fakes {
  const calls: string[] = [];
  const args: Record<string, unknown[][]> = {};
  const value = async <T>(f: Fakeable<T> | undefined, def: T): Promise<T> =>
    typeof f === 'function' ? (f as () => T | Promise<T>)() : (f ?? def);
  /** Records the call and its arguments, then answers with `fn(...args)`. */
  const rec =
    <R>(name: string, fn: (...a: never[]) => Promise<R>) =>
    (...a: unknown[]): Promise<R> => {
      calls.push(name);
      (args[name] ??= []).push(a);
      return (fn as (...x: unknown[]) => Promise<R>)(...a);
    };
  return {
    calls,
    args,
    impl: {
      ingestFile: rec('ingestFile', () => value(o.ingest, INGESTED_STILL)),
      decodeContent: rec('decodeContent', () => value(o.decode, DECODED)),
      recordHash: rec('recordHash', () => value<'HASHED' | 'QUARANTINED'>(o.record, 'HASHED')),
      dedupeIdentical: rec('dedupeIdentical', () =>
        value<'UNIQUE' | 'KEPT' | 'DUPLICATE'>(o.dedupe, 'UNIQUE'),
      ),
      markFailed: rec('markFailed', () => Promise.resolve()),
      reopenFailed: rec('reopenFailed', () => Promise.resolve()),
      clusterContent: rec('clusterContent', () => value<number | null>(o.cluster, 7)),
      maybeBuildStaging: rec('maybeBuildStaging', (force: boolean) =>
        Promise.resolve(o.stage ? o.stage(force) : { gen: 1, entries: 2, skipped: false }),
      ),
      scanAndStart: rec('scanAndStart', () =>
        value(o.scan, { started: 0, pendingFiles: 0, pendingBytes: '0' }),
      ),
      reclaimCopy: rec('reclaimCopy', () => value<ReclaimOutcome>(o.reclaim, 'REMOVED')),
      sweepCopies: rec('sweepCopies', () =>
        value(o.sweep, { examined: 0, removed: 0, mismatched: 0, deferred: 0, wrapped: true }),
      ),
      syncPurgeLog: rec('syncPurgeLog', () => Promise.resolve({ appended: 3 })),
      removePurgedBatch: rec('removePurgedBatch', () =>
        value(o.removePurged, {
          examined: 2,
          removed: 1,
          mismatched: 0,
          deferred: 1,
          wrapped: true,
        }),
      ),
    },
  };
}

let env: TestWorkflowEnvironment;
let current: Fakes = fakes({});
const workers: Worker[] = [];
let running: Promise<unknown>;
let workflowBundle: WorkflowBundleWithSourceMap;

const NAMES = [
  'ingestFile',
  'decodeContent',
  'recordHash',
  'dedupeIdentical',
  'markFailed',
  'reopenFailed',
  'clusterContent',
  'maybeBuildStaging',
  'scanAndStart',
  'reclaimCopy',
  'sweepCopies',
  'syncPurgeLog',
  'removePurgedBatch',
] as const;
/** Every activity forwards to the current test's fakes, so one set of workers serves the file. */
const activities = Object.fromEntries(
  NAMES.map((n) => [
    n,
    (...a: unknown[]) => (current.impl[n] as (...x: unknown[]) => Promise<unknown>)(...a),
  ]),
);

beforeAll(async () => {
  // Activity failures are the point of these tests; keep the SDK's warnings out of the output.
  // (The time-skipping server also lacks a capability core warns about once per worker.)
  Runtime.install({
    logger: new DefaultLogger('ERROR'),
    telemetryOptions: { logging: { filter: { core: 'ERROR', other: 'ERROR' }, forward: {} } },
  });
  env = await TestWorkflowEnvironment.createTimeSkipping();
  workflowBundle = await bundleWorkflowCode({
    workflowsPath: fileURLToPath(new URL('../../src/workflows.ts', import.meta.url)),
    logger: new DefaultLogger('ERROR'),
  });
  workers.push(
    await Worker.create({ connection: env.nativeConnection, taskQueue: 'main', workflowBundle }),
  );
  for (const taskQueue of ['io', 'cpu', 'cluster'])
    workers.push(await Worker.create({ connection: env.nativeConnection, taskQueue, activities }));
  running = Promise.all(workers.map((w) => w.run()));
}, 120_000);

afterAll(async () => {
  for (const w of workers) w.shutdown();
  await running;
  await env.teardown();
});

const start = { taskQueue: 'main', workflowId: '' };
const id = () => ({ ...start, workflowId: `test-${randomUUID()}` });

function runFile(f: Fakes, opts?: { poisonAttempts?: number }): Promise<string> {
  current = f;
  return env.client.workflow.execute<typeof FileWorkflow>('FileWorkflow', {
    ...id(),
    args: opts ? [REF, opts] : [REF],
  });
}
function runContent(f: Fakes, sha: string, ext: string): Promise<string> {
  current = f;
  return env.client.workflow.execute<typeof ContentWorkflow>('ContentWorkflow', {
    ...id(),
    args: [sha, ext],
  });
}

test('new still runs all steps', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_STILL }))).toBe('CLUSTERED'));

test('new still passes each step its inputs', async () => {
  const f = fakes({ ingest: INGESTED_STILL });
  await runFile(f);
  expect(f.calls).toEqual([
    'ingestFile',
    'decodeContent',
    'recordHash',
    'dedupeIdentical',
    'clusterContent',
  ]);
  expect(f.args['ingestFile']).toEqual([[REF]]);
  expect(f.args['decodeContent']).toEqual([[SHA, 'heic']]);
  expect(f.args['recordHash']).toEqual([[SHA, DECODED]]);
  expect(f.args['dedupeIdentical']).toEqual([[SHA]]);
  expect(f.args['clusterContent']).toEqual([[SHA]]);
});

test('COPY removes the confirmed duplicate, then stops', async () => {
  const f = fakes({ ingest: COPY });
  expect(await runFile(f)).toBe('COPY');
  expect(f.calls).toEqual(['ingestFile', 'reclaimCopy']);
  expect(f.args['reclaimCopy']).toEqual([[COPY.fileId]]);
});

test('the reclaim step retries through an outage', async () =>
  expect(await runFile(fakes({ ingest: COPY, reclaim: failNTimes(10, 'REMOVED') }))).toBe('COPY'));

test('CopySweepWorkflow is bounded: it stops after its batch cap or once the pass wraps', async () => {
  const run = () =>
    env.client.workflow.execute<typeof CopySweepWorkflow>('CopySweepWorkflow', {
      ...id(),
      args: [],
    });
  const batch = { examined: 3, removed: 3, mismatched: 0, deferred: 0, wrapped: false };
  current = fakes({ sweep: batch });
  expect(await run()).toMatchObject({ wrapped: false });
  const capped = current.calls.length;
  expect(capped).toBeGreaterThan(1);
  let n = 0;
  current = fakes({ sweep: () => ({ ...batch, wrapped: ++n === 2 }) });
  expect(await run()).toEqual({ ...batch, examined: 6, removed: 6, wrapped: true });
  expect(current.calls).toEqual(['sweepCopies', 'sweepCopies']);
  expect(capped).toBeLessThanOrEqual(100);
});

test('motion stops after ingest', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_MOTION }))).toBe('INGESTED'));

test('a quarantined file stops after ingest', async () => {
  const f = fakes({
    ingest: { ...INGESTED_STILL, state: 'QUARANTINED', sha256: null, kind: null },
  });
  expect(await runFile(f)).toBe('QUARANTINED');
  expect(f.calls).toEqual(['ingestFile']);
});

// Purge spec §8: a dropped file whose bytes are blocked is turned away; nothing else runs.
test('a blocked file ends the workflow after ingest, without content steps', async () => {
  const f = fakes({ ingest: { ...INGESTED_STILL, state: 'BLOCKED', kind: null } });
  expect(await runFile(f)).toBe('BLOCKED');
  expect(f.calls).toEqual(['ingestFile']);
});

test('a pixel-identical duplicate ends after dedupe: DUPLICATE, never clustered', async () => {
  const f = fakes({ ingest: INGESTED_STILL, dedupe: 'DUPLICATE' });
  expect(await runFile(f)).toBe('DUPLICATE');
  expect(f.calls).toEqual(['ingestFile', 'decodeContent', 'recordHash', 'dedupeIdentical']);
});

test.each(['UNIQUE', 'KEPT'] as const)('dedupe %s continues to clustering', async (dedupe) => {
  const f = fakes({ ingest: INGESTED_STILL, dedupe });
  expect(await runFile(f)).toBe('CLUSTERED');
  expect(f.calls.slice(-2)).toEqual(['dedupeIdentical', 'clusterContent']);
});

test('a content retired before clustering (clusterContent null) ends DUPLICATE', async () =>
  expect(await runContent(fakes({ cluster: () => null }), SHA, 'heic')).toBe('DUPLICATE'));

test('the dedupe step retries through an outage', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_STILL, dedupe: failNTimes(5, 'KEPT') }))).toBe(
    'CLUSTERED',
  ));

test('a FileWorkflow history recorded before dedupe-identical replays without the step', async () => {
  const bytes = await readFile(
    fileURLToPath(
      new URL('../fixtures/fileWorkflow-pre-dedupe-identical.history.bin', import.meta.url),
    ),
  );
  const history = temporal.api.history.v1.History.decode(bytes);
  const scheduled = (history.events ?? []).flatMap((e) => {
    const name = e.activityTaskScheduledEventAttributes?.activityType?.name;
    return name ? [name] : [];
  });
  // Recorded from the workflow code before the patch: no dedupe step, no patch marker.
  expect(scheduled).toEqual(['ingestFile', 'decodeContent', 'recordHash', 'clusterContent']);
  await Worker.runReplayHistory({ workflowBundle }, history);
});

// Purge spec §9: an unblocked purged content restored while its vault file was still there
// comes back as a COPY that carries the content's kind; a still then runs the content steps.
test('a restored COPY of a still removes the copy, then runs the content steps', async () => {
  const f = fakes({ ingest: { ...COPY, kind: 'still' } });
  expect(await runFile(f)).toBe('CLUSTERED');
  expect(f.calls).toEqual([
    'ingestFile',
    'reclaimCopy',
    'decodeContent',
    'recordHash',
    'dedupeIdentical',
    'clusterContent',
  ]);
  expect(f.args['decodeContent']).toEqual([[SHA, 'heic']]);
});

test('a restored COPY of a motion stops after the reclaim, like a new motion', async () => {
  const f = fakes({ ingest: { ...COPY, ext: 'mov', kind: 'motion' } });
  expect(await runFile(f)).toBe('COPY');
  expect(f.calls).toEqual(['ingestFile', 'reclaimCopy']);
});

test('a FileWorkflow COPY history recorded before restored-copy replays unchanged', async () => {
  const bytes = await readFile(
    fileURLToPath(
      new URL('../fixtures/fileWorkflow-copy-pre-restored-copy.history.bin', import.meta.url),
    ),
  );
  const history = temporal.api.history.v1.History.decode(bytes);
  const events = history.events ?? [];
  const scheduled = events.flatMap((e) => {
    const name = e.activityTaskScheduledEventAttributes?.activityType?.name;
    return name ? [name] : [];
  });
  // Recorded from the workflow code before the patch: ingest (COPY, kind null), then reclaim.
  expect(scheduled).toEqual(['ingestFile', 'reclaimCopy']);
  const patches = events.flatMap((e) => {
    const d = e.markerRecordedEventAttributes?.details?.['patch_data']?.payloads?.[0]?.data;
    return d ? [Buffer.from(d).toString()] : [];
  });
  expect(patches.join()).not.toContain('restored-copy');
  await Worker.runReplayHistory({ workflowBundle }, history);
});

test('quarantine stops before cluster', async () => {
  const f = fakes({ ingest: INGESTED_STILL, record: 'QUARANTINED' });
  expect(await runFile(f)).toBe('QUARANTINED');
  expect(f.calls).not.toContain('clusterContent');
});

test('decode poison marks FAILED after 5', async () => {
  const f = fakes({ ingest: INGESTED_STILL, decode: alwaysPoison });
  expect(await runFile(f)).toBe('FAILED');
  expect(f.calls.filter((c) => c === 'decodeContent')).toHaveLength(5);
  expect(f.calls.at(-1)).toBe('markFailed');
  const [sha, error] = f.args['markFailed']![0]!;
  expect(sha).toBe(SHA);
  expect(error).toContain('decode timed out');
});

/** Throws the scripted decode failures in order ('K' killed by a signal, 'P' poison), then decodes. */
function scripted(script: string): () => DecodeResult {
  let i = 0;
  return () => {
    const step = script[i++];
    if (step === 'K') throw ApplicationFailure.retryable('child got SIGKILL', 'DecodeKilled');
    if (step === 'P') throw ApplicationFailure.retryable('decode timed out', 'Poison');
    return DECODED;
  };
}

test('a killed decode child is transient: a kill, then success, clusters', async () => {
  const f = fakes({ ingest: INGESTED_STILL, decode: scripted('KKK') });
  expect(await runFile(f)).toBe('CLUSTERED');
  expect(f.calls.filter((c) => c === 'decodeContent')).toHaveLength(4);
  expect(f.calls).not.toContain('markFailed');
});

test('decode kills are capped: 10 attempts in all, then FAILED decode_killed', async () => {
  const f = fakes({ ingest: INGESTED_STILL, decode: scripted('K'.repeat(30)) });
  expect(await runFile(f)).toBe('FAILED');
  expect(f.calls.filter((c) => c === 'decodeContent')).toHaveLength(10);
  expect(f.calls.at(-1)).toBe('markFailed');
  expect(f.args['markFailed']![0]![1]).toMatch(/^decode_killed: /);
});

test('mixed kills and poison never reset the caps: FAILED within 10 attempts', async () => {
  // Four poison, then a kill, repeated: the fifth poison (attempt 6) ends it.
  const f = fakes({ ingest: INGESTED_STILL, decode: scripted('PPPPK'.repeat(6)) });
  expect(await runFile(f)).toBe('FAILED');
  expect(f.calls.filter((c) => c === 'decodeContent')).toHaveLength(6);
  expect(f.args['markFailed']![0]![1]).toContain('decode timed out');
  // Kills and poison alternating: total attempts hit 10 before poison reaches 5.
  const g = fakes({ ingest: INGESTED_STILL, decode: scripted('KPKPKPKPKKKKK') });
  expect(await runFile(g)).toBe('FAILED');
  expect(g.calls.filter((c) => c === 'decodeContent').length).toBeLessThanOrEqual(10);
});

test('poisonAttempts option caps decode attempts', async () => {
  const f = fakes({ ingest: INGESTED_STILL, decode: alwaysPoison });
  expect(await runFile(f, { poisonAttempts: 2 })).toBe('FAILED');
  expect(f.calls.filter((c) => c === 'decodeContent')).toHaveLength(2);
});

test('decode recovering within its attempts still clusters', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_STILL, decode: failNTimes(4, DECODED) }))).toBe(
    'CLUSTERED',
  ));

test('recordHash outage never fails content', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_STILL, record: failNTimes(40, 'HASHED') }))).toBe(
    'CLUSTERED',
  ));

test('ingest retries through outage', async () =>
  expect(await runFile(fakes({ ingest: failNTimes(20, INGESTED_STILL) }))).toBe('CLUSTERED'));

test('cluster retries through outage', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_STILL, cluster: failNTimes(15, 3) }))).toBe(
    'CLUSTERED',
  ));

test('UnexpectedState from cluster fails the workflow visibly', async () => {
  const unexpected = (): never => {
    throw ApplicationFailure.nonRetryable('content is FAILED, not HASHED', 'UnexpectedState');
  };
  const f = fakes({ ingest: INGESTED_STILL, cluster: unexpected });
  const err: unknown = await runFile(f).catch((e: unknown) => e);
  expect(err).toBeInstanceOf(WorkflowFailedError);
  expect(JSON.stringify((err as WorkflowFailedError).cause?.cause)).toContain('UnexpectedState');
  expect(f.calls).not.toContain('markFailed');
});

test('UnexpectedState from recordHash fails the workflow visibly', async () => {
  const unexpected = (): never => {
    throw ApplicationFailure.nonRetryable('content is missing', 'UnexpectedState');
  };
  const err: unknown = await runFile(fakes({ ingest: INGESTED_STILL, record: unexpected })).catch(
    (e: unknown) => e,
  );
  expect(err).toBeInstanceOf(WorkflowFailedError);
});

test('ContentWorkflow runs content steps only', async () => {
  const f = fakes({});
  expect(await runContent(f, 'a'.repeat(64), 'heic')).toBe('CLUSTERED');
  expect(f.calls).toEqual([
    'reopenFailed',
    'decodeContent',
    'recordHash',
    'dedupeIdentical',
    'clusterContent',
  ]);
  expect(f.args['reopenFailed']).toEqual([['a'.repeat(64)]]);
});

test('ContentWorkflow marks FAILED on poison', async () => {
  const f = fakes({ decode: alwaysPoison });
  expect(await runContent(f, 'a'.repeat(64), 'heic')).toBe('FAILED');
});

test('ScanWorkflow returns the scan result', async () => {
  const scan: ScanResult = { started: 3, pendingFiles: 4, pendingBytes: '99' };
  current = fakes({ scan });
  expect(
    await env.client.workflow.execute<typeof ScanWorkflow>('ScanWorkflow', { ...id(), args: [] }),
  ).toEqual(scan);
  expect(current.calls).toEqual(['scanAndStart']);
});

test('StagingWorkflow passes force through', async () => {
  current = fakes({ stage: (force) => ({ gen: force ? 9 : null, entries: 0, skipped: !force }) });
  const run = (args: [] | [boolean]) =>
    env.client.workflow.execute<typeof StagingWorkflow>('StagingWorkflow', { ...id(), args });
  expect(await run([true])).toEqual({ gen: 9, entries: 0, skipped: false });
  expect(await run([])).toEqual({ gen: null, entries: 0, skipped: true });
  expect(current.args['maybeBuildStaging']).toEqual([[true], [false]]);
});

test('PurgeSyncWorkflow: sync the log, rebuild staging (forced), then remove the files', async () => {
  current = fakes({});
  const shas = ['a'.repeat(64), 'b'.repeat(64)];
  const workflowId = `test-${randomUUID()}`;
  expect(
    await env.client.workflow.execute<typeof PurgeSyncWorkflow>('PurgeSyncWorkflow', {
      ...start,
      workflowId,
      args: [shas],
    }),
  ).toEqual({ appended: 3, removed: 1, deferred: 1 });
  expect(current.calls).toEqual(['syncPurgeLog', 'maybeBuildStaging', 'removePurgedBatch']);
  expect(current.args['maybeBuildStaging']).toEqual([[true]]);
  expect(current.args['removePurgedBatch']).toEqual([[shas]]);
  const child = env.client.workflow.getHandle(`pm-stage-purge-${workflowId}`);
  expect((await child.describe()).type).toBe('StagingWorkflow');
});

test('PurgeSyncWorkflow: the removal step retries through an outage', async () => {
  current = fakes({
    removePurged: failNTimes(3, {
      examined: 1,
      removed: 1,
      mismatched: 0,
      deferred: 0,
      wrapped: true,
    }),
  });
  expect(
    await env.client.workflow.execute<typeof PurgeSyncWorkflow>('PurgeSyncWorkflow', {
      ...id(),
      args: [['a'.repeat(64)]],
    }),
  ).toEqual({ appended: 3, removed: 1, deferred: 0 });
});

test('fileWorkflowId is stable and sensitive', () => {
  const r = { relPath: 'a b/é.jpg', size: '1', mtimeNs: '2' };
  expect(fileWorkflowId(r)).toBe(fileWorkflowId({ ...r }));
  expect(fileWorkflowId(r)).not.toBe(fileWorkflowId({ ...r, mtimeNs: '3' }));
  expect(fileWorkflowId(r)).not.toBe(fileWorkflowId({ ...r, size: '9' }));
  expect(fileWorkflowId(r)).toMatch(/^file-[0-9a-f]{40}$/);
});
