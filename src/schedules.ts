import {
  ScheduleAlreadyRunning,
  ScheduleOverlapPolicy,
  type Client,
  type ScheduleOptions,
  type ScheduleUpdateOptions,
} from '@temporalio/client';
import type { Settings } from './config.js';

export const STAGE_INTERVAL_S = 300;
/** The duplicate sweep (spec §6.5): low frequency, each run bounded (`CopySweepWorkflow`). */
export const COPY_SWEEP_INTERVAL_S = 900;

/** The schedules this deployment wants (spec §4.3), from the current settings. */
export function scheduleOptions(s: Settings): ScheduleOptions[] {
  const every = (n: number) => ({ intervals: [{ every: `${String(n)}s` }] });
  const policies = { overlap: ScheduleOverlapPolicy.SKIP };
  return [
    {
      scheduleId: 'pm-scan',
      spec: every(s.scanIntervalS),
      action: {
        type: 'startWorkflow',
        workflowType: 'ScanWorkflow',
        workflowId: 'pm-scan',
        taskQueue: 'main',
        args: [],
      },
      policies,
    },
    {
      // Also re-runs a build that `maybeBuildStaging` skipped while FileWorkflows were running.
      scheduleId: 'pm-stage',
      spec: every(STAGE_INTERVAL_S),
      action: {
        type: 'startWorkflow',
        workflowType: 'StagingWorkflow',
        workflowId: 'pm-stage',
        taskQueue: 'main',
        args: [false],
      },
      policies,
    },
    {
      // Crash leftovers, deferred copies and the one-off pass over data from before removal.
      scheduleId: 'pm-copies',
      spec: every(COPY_SWEEP_INTERVAL_S),
      action: {
        type: 'startWorkflow',
        workflowType: 'CopySweepWorkflow',
        workflowId: 'pm-copies',
        taskQueue: 'main',
        args: [],
      },
      policies,
    },
  ];
}

/** Creates the schedules, or updates existing ones in place so changed settings take effect. */
export async function ensureSchedules(client: Client, s: Settings): Promise<void> {
  for (const opts of scheduleOptions(s)) {
    try {
      await client.schedule.create(opts);
    } catch (e) {
      if (!(e instanceof ScheduleAlreadyRunning)) throw e;
      const { scheduleId, ...rest } = opts;
      // Keep the live state (an operator may have paused it); replace spec, action and policies.
      await client.schedule
        .getHandle(scheduleId)
        .update((prev): ScheduleUpdateOptions => ({ ...rest, state: prev.state }));
    }
  }
}
