// Workflow code: deterministic, bundled for the workflow sandbox. Only `@temporalio/workflow` and
// type-only imports belong here.
import {
  ActivityFailure,
  ApplicationFailure,
  isCancellation,
  log,
  proxyActivities,
  type ActivityOptions,
  type RetryPolicy,
} from '@temporalio/workflow';
import type { Activities } from './activities/index.js';
import type { BuildResult, FileRef, ScanResult } from './types.js';

/** Spec §7 transient retry: 1 s, ×2, capped at 5 min, unlimited attempts (no `maximumAttempts`). */
export const TRANSIENT_RETRY: RetryPolicy = {
  initialInterval: '1s',
  backoffCoefficient: 2,
  maximumInterval: '5m',
};

export const DEFAULT_POISON_ATTEMPTS = 5;

/** Every proxied activity's queue, timeouts and retry policy (spec §4.2, §7). */
export const ACTIVITY_OPTIONS = {
  ingestFile: {
    taskQueue: 'io',
    startToCloseTimeout: '1h',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
  /** Only decode is capped (it has no database access, so its failures are never outages). */
  decodeContent: (poisonAttempts: number): ActivityOptions => ({
    taskQueue: 'cpu',
    startToCloseTimeout: '2m',
    retry: { ...TRANSIENT_RETRY, maximumAttempts: poisonAttempts },
  }),
  recordHash: { taskQueue: 'io', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
  clusterContent: { taskQueue: 'cluster', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
  markFailed: { taskQueue: 'io', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
  maybeBuildStaging: {
    taskQueue: 'io',
    startToCloseTimeout: '2h',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
  scanAndStart: {
    taskQueue: 'io',
    startToCloseTimeout: '10m',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
} satisfies Record<keyof Activities, ActivityOptions | ((n: number) => ActivityOptions)>;

const { ingestFile } = proxyActivities<Activities>(ACTIVITY_OPTIONS.ingestFile);
const { recordHash } = proxyActivities<Activities>(ACTIVITY_OPTIONS.recordHash);
const { clusterContent } = proxyActivities<Activities>(ACTIVITY_OPTIONS.clusterContent);
const { markFailed } = proxyActivities<Activities>(ACTIVITY_OPTIONS.markFailed);
const { maybeBuildStaging } = proxyActivities<Activities>(ACTIVITY_OPTIONS.maybeBuildStaging);
const { scanAndStart } = proxyActivities<Activities>(ACTIVITY_OPTIONS.scanAndStart);

export type ContentOutcome = 'CLUSTERED' | 'QUARANTINED' | 'FAILED';

/**
 * decode (cpu) → record (io) → cluster (cluster). Decode exhausting its attempts is poison:
 * `markFailed`, then `FAILED`. Any other activity failure (only non-retryable ones can reach
 * here, e.g. `UnexpectedState`) fails the workflow, visibly.
 */
export async function runContentSteps(
  sha256: string,
  ext: string,
  poisonAttempts: number,
): Promise<ContentOutcome> {
  const { decodeContent } = proxyActivities<Activities>(
    ACTIVITY_OPTIONS.decodeContent(poisonAttempts),
  );
  let decoded;
  try {
    decoded = await decodeContent(sha256, ext);
  } catch (e) {
    if (!(e instanceof ActivityFailure) || isCancellation(e)) throw e;
    const error = e.cause?.message ?? e.message;
    log.warn('decode attempts exhausted; marking FAILED', { sha256, error });
    await markFailed(sha256, error);
    return 'FAILED';
  }
  if ((await recordHash(sha256, decoded)) === 'QUARANTINED') return 'QUARANTINED';
  await clusterContent(sha256);
  return 'CLUSTERED';
}

/** One dropped file: ingest, then the content steps if it brought new still-image content. */
export async function FileWorkflow(
  ref: FileRef,
  opts: { poisonAttempts?: number } = {},
): Promise<string> {
  const r = await ingestFile(ref);
  if (r.state !== 'INGESTED' || r.kind !== 'still') return r.state;
  if (r.sha256 === null || r.ext === null)
    throw ApplicationFailure.nonRetryable(
      `ingest of ${ref.relPath} returned an INGESTED still without sha256/ext`,
      'UnexpectedState',
    );
  return runContentSteps(r.sha256, r.ext, opts.poisonAttempts ?? DEFAULT_POISON_ATTEMPTS);
}

/** The content steps alone (used by `retry-failed`). */
export async function ContentWorkflow(
  sha256: string,
  ext: string,
  opts: { poisonAttempts?: number } = {},
): Promise<string> {
  return runContentSteps(sha256, ext, opts.poisonAttempts ?? DEFAULT_POISON_ATTEMPTS);
}

/** Scheduled (`pm-scan`): find stable drops and start their FileWorkflows. */
export async function ScanWorkflow(): Promise<ScanResult> {
  return scanAndStart();
}

/** Scheduled (`pm-stage`) or on demand (`cli stage [--force]`). */
export async function StagingWorkflow(force = false): Promise<BuildResult> {
  return maybeBuildStaging(force);
}
