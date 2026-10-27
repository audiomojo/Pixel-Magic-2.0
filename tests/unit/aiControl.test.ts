import type { Client } from '@temporalio/client';
import { WorkflowNotFoundError } from '@temporalio/common';
import { expect, test } from 'vitest';
import { temporalAiControl } from '../../src/harvest/aiControl.js';

interface Calls {
  schedule: string[];
  cancelled: { workflowId: string; runId?: string; firstExecutionRunId?: string }[];
}

/** A fake client: the `ai-describe` schedule with `running` actions; `cancelErr` fails cancels. */
function fakeClient(
  running: { workflowId: string; firstExecutionRunId: string }[],
  cancelErr?: Error,
) {
  const calls: Calls = { schedule: [], cancelled: [] };
  const client = {
    schedule: {
      getHandle: (id: string) => ({
        pause: (note?: string) => {
          calls.schedule.push(`pause ${id} ${note ?? ''}`);
          return Promise.resolve();
        },
        unpause: (note?: string) => {
          calls.schedule.push(`unpause ${id} ${note ?? ''}`);
          return Promise.resolve();
        },
        describe: () =>
          Promise.resolve({
            state: { paused: false },
            info: {
              runningActions: running.map((workflow) => ({ type: 'startWorkflow', workflow })),
            },
          }),
      }),
    },
    workflow: {
      getHandle: (
        workflowId: string,
        runId?: string,
        options?: { firstExecutionRunId?: string },
      ) => ({
        cancel: () => {
          if (cancelErr) return Promise.reject(cancelErr);
          calls.cancelled.push({
            workflowId,
            ...(runId === undefined ? {} : { runId }),
            ...(options?.firstExecutionRunId === undefined
              ? {}
              : { firstExecutionRunId: options.firstExecutionRunId }),
          });
          return Promise.resolve();
        },
      }),
    },
  } as unknown as Client;
  return { client, calls };
}

test('pausing describing pauses the schedule and cancels the run it started', async () => {
  const { client, calls } = fakeClient([
    { workflowId: 'ai-describe-x', firstExecutionRunId: 'r1' },
  ]);
  await temporalAiControl(client).setDescribePaused(true);
  expect(calls.schedule).toEqual(['pause ai-describe paused from the Picks tab']);
  // The latest run of that chain (after continue-as-new), never another chain's.
  expect(calls.cancelled).toEqual([{ workflowId: 'ai-describe-x', firstExecutionRunId: 'r1' }]);
});

test('pausing with nothing running only pauses', async () => {
  const { client, calls } = fakeClient([]);
  await temporalAiControl(client).setDescribePaused(true);
  expect(calls.schedule).toEqual(['pause ai-describe paused from the Picks tab']);
  expect(calls.cancelled).toEqual([]);
});

test('a run that closed meanwhile is not an error', async () => {
  const { client } = fakeClient(
    [{ workflowId: 'ai-describe-x', firstExecutionRunId: 'r1' }],
    new WorkflowNotFoundError('gone', 'ai-describe-x', 'r1'),
  );
  await expect(temporalAiControl(client).setDescribePaused(true)).resolves.toBeUndefined();
});

test('resuming only unpauses', async () => {
  const { client, calls } = fakeClient([
    { workflowId: 'ai-describe-x', firstExecutionRunId: 'r1' },
  ]);
  await temporalAiControl(client).setDescribePaused(false);
  expect(calls.schedule).toEqual(['unpause ai-describe resumed from the Picks tab']);
  expect(calls.cancelled).toEqual([]);
});
