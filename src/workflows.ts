// Workflow code: deterministic, bundled for the workflow sandbox. Only `@temporalio/workflow` and
// type-only imports belong here.
import {
  ActivityFailure,
  ApplicationFailure,
  isCancellation,
  log,
  patched,
  proxyActivities,
  sleep,
  type ActivityOptions,
  type RetryPolicy,
} from '@temporalio/workflow';
import type { Activities } from './activities/index.js';
import type { BuildResult, DecodeKilledType, FileRef, ScanResult, SweepResult } from './types.js';

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
  /**
   * Decodes (cpu) every not-yet-compared suspect pair under one transaction, heartbeating
   * between pairs; then deletes the retired vault files.
   */
  dedupeIdentical: {
    taskQueue: 'cpu',
    startToCloseTimeout: '10m',
    heartbeatTimeout: '60s',
    retry: TRANSIENT_RETRY,
  },
  /** 15 min: a cache reload after a worker restart scans every clustered content (heartbeats). */
  clusterContent: {
    taskQueue: 'cluster',
    startToCloseTimeout: '15m',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
  markFailed: { taskQueue: 'io', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
  reopenFailed: { taskQueue: 'io', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
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
  /** Re-hashes a duplicate and its vault file (a large video takes a while): heartbeats. */
  reclaimCopy: {
    taskQueue: 'io',
    startToCloseTimeout: '1h',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
  sweepCopies: {
    taskQueue: 'io',
    startToCloseTimeout: '1h',
    heartbeatTimeout: '30s',
    retry: TRANSIENT_RETRY,
  },
} satisfies Record<keyof Activities, ActivityOptions>;

const { ingestFile } = proxyActivities<Activities>(ACTIVITY_OPTIONS.ingestFile);
const { recordHash } = proxyActivities<Activities>(ACTIVITY_OPTIONS.recordHash);
const { dedupeIdentical } = proxyActivities<Activities>(ACTIVITY_OPTIONS.dedupeIdentical);
const { clusterContent } = proxyActivities<Activities>(ACTIVITY_OPTIONS.clusterContent);
const { markFailed } = proxyActivities<Activities>(ACTIVITY_OPTIONS.markFailed);
const { reopenFailed } = proxyActivities<Activities>(ACTIVITY_OPTIONS.reopenFailed);
const { maybeBuildStaging } = proxyActivities<Activities>(ACTIVITY_OPTIONS.maybeBuildStaging);
const { scanAndStart } = proxyActivities<Activities>(ACTIVITY_OPTIONS.scanAndStart);
const { reclaimCopy } = proxyActivities<Activities>(ACTIVITY_OPTIONS.reclaimCopy);
const { sweepCopies } = proxyActivities<Activities>(ACTIVITY_OPTIONS.sweepCopies);

const DECODE_KILLED: DecodeKilledType = 'DecodeKilled';
/** Decode attempts in all (kills included) before a content is FAILED `decode_killed`. */
export const MAX_DECODE_ATTEMPTS = 10;
const MAX_BACKOFF_S = 300;

export type ContentOutcome = 'CLUSTERED' | 'QUARANTINED' | 'FAILED' | 'DUPLICATE';

/**
 * decode (cpu) → record (io) → dedupe (cpu) → cluster (cluster). A content retired as a
 * pixel-identical duplicate ends `DUPLICATE`, never clustered; `patched`: histories started
 * before the dedupe step existed replay without it, exactly as before. Decode is retried here,
 * one attempt per call, with the transient backoff (1 s ×2, max 5 min). `poisonAttempts`
 * failures other than a killed child are poison; `MAX_DECODE_ATTEMPTS` attempts of any kind (a
 * child killed again and again by the OOM killer) are `decode_killed`. Either way:
 * `markFailed`, then `FAILED`, which `retry-failed` can reset. Any other activity failure (only non-retryable ones can reach here,
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
  if (patched('dedupe-identical')) {
    if ((await dedupeIdentical(sha256)) === 'DUPLICATE') return 'DUPLICATE';
    // null: retired by a later newcomer's dedupe between the two steps.
    return (await clusterContent(sha256)) === null ? 'DUPLICATE' : 'CLUSTERED';
  }
  await clusterContent(sha256);
  return 'CLUSTERED';
}

/**
 * One dropped file: ingest, then the content steps if it brought new still-image content. A
 * byte-identical COPY has its `.copies` file removed once confirmed (spec §6.5); a mismatch is
 * an outcome (flagged), so the workflow ends normally either way. `patched`: histories started
 * before removal existed replay without the step (the sweep covers those files).
 */
export async function FileWorkflow(
  ref: FileRef,
  opts: { poisonAttempts?: number } = {},
): Promise<string> {
  const r = await ingestFile(ref);
  if (r.state === 'COPY' && patched('reclaim-copy')) await reclaimCopy(r.fileId);
  if (r.state !== 'INGESTED' || r.kind !== 'still') return r.state;
  if (r.sha256 === null || r.ext === null)
    throw ApplicationFailure.nonRetryable(
      `ingest of ${ref.relPath} returned an INGESTED still without sha256/ext`,
      'UnexpectedState',
    );
  return runContentSteps(r.sha256, r.ext, opts.poisonAttempts ?? DEFAULT_POISON_ATTEMPTS);
}

/**
 * `retry-failed`: reopen the FAILED content (a no-op for any other state), then the content
 * steps. The reopen is inside the workflow so the CLI changes no row and a rerun is safe.
 * `patched`: histories started before the reopen step existed replay without it.
 */
export async function ContentWorkflow(
  sha256: string,
  ext: string,
  opts: { poisonAttempts?: number } = {},
): Promise<string> {
  if (patched('content-reopen-failed')) await reopenFailed(sha256);
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

/** Rows per `sweepCopies` call, and calls per run: a run examines at most 10,000 COPY rows. */
export const SWEEP_BATCH = 200;
export const MAX_SWEEP_BATCHES = 50;

/**
 * Scheduled (`pm-copies`) or on demand (`cli sweep-copies`): removes confirmed duplicates that
 * FileWorkflow did not (crash leftovers, deferred copies, files ingested before removal existed).
 * Bounded: `MAX_SWEEP_BATCHES` batches of `SWEEP_BATCH`; the next run resumes at the cursor.
 */
export async function CopySweepWorkflow(): Promise<SweepResult> {
  const total: SweepResult = {
    examined: 0,
    removed: 0,
    mismatched: 0,
    deferred: 0,
    wrapped: false,
  };
  for (let i = 0; i < MAX_SWEEP_BATCHES && !total.wrapped; i++) {
    const r = await sweepCopies(SWEEP_BATCH);
    total.examined += r.examined;
    total.removed += r.removed;
    total.mismatched += r.mismatched;
    total.deferred += r.deferred;
    total.wrapped = r.wrapped;
  }
  return total;
}

export * from './harvest/workflow.js';
