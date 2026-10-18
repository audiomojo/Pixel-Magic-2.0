// Workflow code: deterministic, bundled for the workflow sandbox. Only `@temporalio/workflow` and
// type-only imports belong here.
import {
  ActivityFailure,
  ApplicationFailure,
  isCancellation,
  log,
  proxyActivities,
  sleep,
  type ActivityOptions,
  type RetryPolicy,
} from '@temporalio/workflow';
import type { Activities } from './activities/index.js';
import type { BuildResult, DecodeKilledType, FileRef, ScanResult } from './types.js';

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
  /**
   * Only decode is capped (it has no database access, so its failures are never outages). Each
   * activity call is one attempt: `runContentSteps` retries and counts attempts across kinds.
   */
  decodeContent: {
    taskQueue: 'cpu',
    startToCloseTimeout: '2m',
    retry: { ...TRANSIENT_RETRY, maximumAttempts: 1 },
  },
  recordHash: { taskQueue: 'io', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
  /** 15 min: a cache reload after a worker restart scans every clustered content (heartbeats). */
  clusterContent: {
    taskQueue: 'cluster',
    startToCloseTimeout: '15m',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
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
} satisfies Record<keyof Activities, ActivityOptions>;

const { ingestFile } = proxyActivities<Activities>(ACTIVITY_OPTIONS.ingestFile);
const { recordHash } = proxyActivities<Activities>(ACTIVITY_OPTIONS.recordHash);
const { clusterContent } = proxyActivities<Activities>(ACTIVITY_OPTIONS.clusterContent);
const { markFailed } = proxyActivities<Activities>(ACTIVITY_OPTIONS.markFailed);
const { maybeBuildStaging } = proxyActivities<Activities>(ACTIVITY_OPTIONS.maybeBuildStaging);
const { scanAndStart } = proxyActivities<Activities>(ACTIVITY_OPTIONS.scanAndStart);

const DECODE_KILLED: DecodeKilledType = 'DecodeKilled';
/** Decode attempts in all (kills included) before a content is FAILED `decode_killed`. */
export const MAX_DECODE_ATTEMPTS = 10;
const MAX_BACKOFF_S = 300;

export type ContentOutcome = 'CLUSTERED' | 'QUARANTINED' | 'FAILED';

/**
 * decode (cpu) → record (io) → cluster (cluster). Decode is retried here, one attempt per call,
 * with the transient backoff (1 s ×2, max 5 min). `poisonAttempts` failures other than a killed
 * child are poison; `MAX_DECODE_ATTEMPTS` attempts of any kind (a child killed again and again
 * by the OOM killer) are `decode_killed`. Either way: `markFailed`, then `FAILED`, which
 * `retry-failed` can reset. Any other activity failure (only non-retryable ones can reach here,
 * e.g. `UnexpectedState`) fails the workflow, visibly.
 */
export async function runContentSteps(
  sha256: string,
  ext: string,
  poisonAttempts: number,
): Promise<ContentOutcome> {
  const { decodeContent } = proxyActivities<Activities>(ACTIVITY_OPTIONS.decodeContent);
  const maxAttempts = Math.max(MAX_DECODE_ATTEMPTS, poisonAttempts);
  let decoded;
  for (let attempts = 1, poison = 0; ; attempts++) {
    try {
      decoded = await decodeContent(sha256, ext);
      break;
    } catch (e) {
      if (!(e instanceof ActivityFailure) || isCancellation(e)) throw e;
      const error = e.cause?.message ?? e.message;
      const killed = e.cause instanceof ApplicationFailure && e.cause.type === DECODE_KILLED;
      if (!killed) poison++;
      if (poison >= poisonAttempts || attempts >= maxAttempts) {
        const reason = poison >= poisonAttempts ? error : `decode_killed: ${error}`;
        log.warn('decode attempts exhausted; marking FAILED', { sha256, error: reason, attempts });
        await markFailed(sha256, reason);
        return 'FAILED';
      }
      log.warn('decode failed; retrying', { sha256, error, attempts, killed });
      await sleep(Math.min(2 ** (attempts - 1), MAX_BACKOFF_S) * 1000);
    }
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
