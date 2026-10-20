// Workflow code: deterministic, bundled for the workflow sandbox. Only `@temporalio/workflow` and
// type-only imports (plus pure constants) belong here.
import {
  ActivityCancellationType,
  ApplicationFailure,
  CancellationScope,
  condition,
  ContinueAsNew,
  continueAsNew,
  defineQuery,
  defineSignal,
  isCancellation,
  TemporalFailure,
  proxyActivities,
  setHandler,
  workflowInfo,
  type ActivityOptions,
  type RetryPolicy,
} from '@temporalio/workflow';
import type { createHarvestActivities } from './activities.js';
import type { SendCursor } from './send.js';
import { STALLED_REASON } from './constants.js';

export const pauseSignal = defineSignal('pause');
export const resumeSignal = defineSignal('resume');
export const itemsApprovedSignal = defineSignal('itemsApproved');
export const statusQuery = defineQuery<{ phase: 'scan' | 'send' | 'done'; paused: boolean }>(
  'status',
);

/** The spec §7 transient retry (same values as `TRANSIENT_RETRY`; not imported: workflows.ts re-exports this file). */
const TRANSIENT_RETRY: RetryPolicy = {
  initialInterval: '1s',
  backoffCoefficient: 2,
  maximumInterval: '5m',
};

export const HARVEST_ACTIVITY_OPTIONS = {
  scanFolder: {
    taskQueue: 'harvest',
    startToCloseTimeout: '24h',
    heartbeatTimeout: '2m',
    // The run must not be finished (and the next started) while the activity is still working.
    cancellationType: ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    retry: TRANSIENT_RETRY,
  },
  sendBatch: {
    taskQueue: 'harvest',
    startToCloseTimeout: '2h',
    heartbeatTimeout: '2m',
    // The run must not be finished (and the next started) while the activity is still working.
    cancellationType: ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    retry: TRANSIENT_RETRY,
  },
  other: { taskQueue: 'harvest', startToCloseTimeout: '1m', retry: TRANSIENT_RETRY },
} satisfies Record<string, ActivityOptions>;

type A = ReturnType<typeof createHarvestActivities>;
const { scanFolder } = proxyActivities<Pick<A, 'scanFolder'>>(HARVEST_ACTIVITY_OPTIONS.scanFolder);
const { sendBatch } = proxyActivities<Pick<A, 'sendBatch'>>(HARVEST_ACTIVITY_OPTIONS.sendBatch);
const { loadRun, setRunState, setFolderState, finishRun, startNextQueued } = proxyActivities<
  Pick<A, 'loadRun' | 'setRunState' | 'setFolderState' | 'finishRun' | 'startNextQueued'>
>(HARVEST_ACTIVITY_OPTIONS.other);

export const BATCHES_PER_RUN = 50;
/** Safety net: long stalls add timer events; restart well before the history limit. */
const MAX_HISTORY = 10_000;
const GATE_RECHECK = '30s';
const STALL_RECHECK = '5m';
const SOURCE_RECHECK = '60s';
/** Shown at once when a pause arrives during a scan step (it takes effect after the step). */
export const PAUSE_REQUESTED = 'pause requested — after the current folder step';

export interface HarvestInput {
  runId: string;
  phase?: 'scan' | 'send';
  batches?: number;
  /** The send pass position and the consecutive error count (see sendBatch). */
  cursor?: SendCursor | null;
  consecutive?: number;
  /** History length that triggers continue-as-new (default 10 000; tests lower it). */
  maxHistory?: number;
}

export async function HarvestWorkflow(input: HarvestInput): Promise<'done' | 'cancelled'> {
  const { runId } = input;
  const maxHistory = input.maxHistory ?? MAX_HISTORY;
  /** Carried into the next execution (only set when not the default). */
  const carry = input.maxHistory === undefined ? {} : { maxHistory };
  let phase: 'scan' | 'send' | 'done' = input.phase ?? 'scan';
  let paused = false;
  let resumes = 0;
  let approved = false;
  /** What the run row currently says, to skip writes that change nothing. */
  let shown = '';

  setHandler(pauseSignal, () => {
    paused = true;
  });
  setHandler(resumeSignal, () => {
    paused = false;
    resumes++;
  });
  setHandler(itemsApprovedSignal, () => {
    approved = true;
  });
  setHandler(statusQuery, () => ({ phase, paused }));

  async function show(state: 'scanning' | 'sending' | 'paused', reason?: string): Promise<void> {
    const key = `${state}|${reason ?? ''}`;
    if (key === shown) return;
    await setRunState(runId, state, reason);
    shown = key;
  }
  /** Honours a user pause before the next activity. */
  async function userPause(work: 'scanning' | 'sending'): Promise<void> {
    if (paused) {
      await show('paused', 'paused by you');
      await condition(() => !paused);
      await show(work);
    }
  }
  /** Shows the pause, then waits for the timer or (promptly) a resume signal. */
  async function waitPaused(
    work: 'scanning' | 'sending',
    reason: string,
    timeout?: string,
  ): Promise<void> {
    await show('paused', reason);
    const seen = resumes;
    const woke = () => resumes !== seen;
    if (timeout === undefined) {
      await condition(woke);
      await show(work);
    } else await condition(() => woke() || paused, timeout);
  }

  /**
   * One scanFolder call. A pause during it is shown at once ("pause requested …") and honoured
   * after it. The activity is always awaited to its end, also on cancellation
   * (WAIT_CANCELLATION_COMPLETED): the run must not finish while it still works.
   */
  async function scanStep(folderId: string) {
    let settled = false;
    const step = scanFolder(runId, folderId).finally(() => {
      settled = true;
    });
    try {
      await condition(() => settled || paused);
      if (!settled) await show('paused', PAUSE_REQUESTED);
    } catch (e) {
      await step.catch(() => undefined);
      throw e;
    }
    return await step;
  }

  async function body(): Promise<'done'> {
    if (phase === 'scan') {
      const run = await loadRun(runId);
      if (run.sendOnly) {
        // Review approvals: the items are already PLANNED; nothing is scanned.
        phase = 'send';
      } else {
        await show('scanning');
        for (const folder of run.folders) {
          if (folder.state === 'done') continue;
          for (;;) {
            await userPause('scanning');
            // A long unplug retries every 60 s (~11 events each): restart before the history
            // limit. Done folders are skipped and the scan cursor is in the database.
            if (workflowInfo().historyLength > maxHistory) {
              return await continueAsNew<typeof HarvestWorkflow>({
                runId,
                phase: 'scan',
                ...carry,
              });
            }
            const res = await scanStep(folder.folderId);
            if (res.outcome === 'DONE') {
              if (!paused) await show('scanning');
              break;
            }
            if (res.outcome === 'UNREADABLE') {
              // Nothing could be read (no Full Disk Access, say): wait for the user to resume.
              await waitPaused('scanning', res.reason ?? 'cannot read the folder');
            } else {
              // T5-b: the scan only reports SOURCE_GONE; the folder waits for its drive (§9).
              await setFolderState(runId, folder.folderId, 'waiting_source');
              await waitPaused('scanning', res.reason ?? 'source unplugged', SOURCE_RECHECK);
            }
          }
        }
        if (run.dryRun) {
          phase = 'done';
          return 'done';
        }
        phase = 'send';
      }
    }

    let batches = input.batches ?? 0;
    let send = { cursor: input.cursor ?? null, consecutive: input.consecutive ?? 0 };
    await show('sending');
    for (;;) {
      await userPause('sending');
      if (batches >= BATCHES_PER_RUN || workflowInfo().historyLength > maxHistory) {
        return await continueAsNew<typeof HarvestWorkflow>({
          runId,
          phase: 'send',
          batches: 0,
          ...send,
          ...carry,
        });
      }
      approved = false;
      const res = await sendBatch(runId, send);
      send = { cursor: res.cursor, consecutive: res.consecutive };
      batches++;
      switch (res.outcome) {
        case 'MORE':
          await show('sending');
          break;
        case 'DONE':
          await show('sending');
          if (approved) {
            // Approved items may lie anywhere in the run: a new pass from the first folder.
            approved = false;
            send = { ...send, cursor: null };
            break;
          }
          phase = 'done';
          return 'done';
        case 'GATED':
          await waitPaused(
            'sending',
            res.reason ?? 'waiting for the pipeline',
            res.reason === STALLED_REASON ? STALL_RECHECK : GATE_RECHECK,
          );
          break;
        case 'SOURCE_GONE':
          await waitPaused('sending', res.reason ?? 'source unplugged', SOURCE_RECHECK);
          break;
        case 'DISK_FULL':
          await waitPaused('sending', res.reason ?? 'disk full');
          break;
        case 'PAUSED_ERRORS':
          await waitPaused('sending', res.reason ?? 'too many errors');
          send = { ...send, consecutive: 0 };
          break;
        default:
          throw new Error(`unexpected sendBatch outcome: ${String(res.outcome)}`);
      }
    }
  }

  try {
    await body();
    await finishRun(runId, 'done');
    await startNextQueued();
    return 'done';
  } catch (err) {
    if (err instanceof ContinueAsNew) throw err;
    const cancelled = isCancellation(err);
    await CancellationScope.nonCancellable(async () => {
      if (cancelled) await finishRun(runId, 'cancelled');
      else {
        const e = err as { message?: string; cause?: { message?: string } };
        await finishRun(runId, 'failed', e.cause?.message ?? e.message ?? String(err));
      }
      await startNextQueued();
    });
    // A Temporal failure (cancellation, activity failure) already fails the workflow; any other
    // error is a bug, which would otherwise retry the workflow task forever.
    if (cancelled || err instanceof TemporalFailure) throw err;
    throw ApplicationFailure.nonRetryable(err instanceof Error ? err.message : String(err));
  }
}
