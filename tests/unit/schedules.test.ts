import {
  ScheduleAlreadyRunning,
  ScheduleOverlapPolicy,
  type Client,
  type ScheduleDescription,
} from '@temporalio/client';
import { expect, test } from 'vitest';
import { loadSettings } from '../../src/config.js';
import { ensureSchedules } from '../../src/schedules.js';

const settings = loadSettings({ DATABASE_URL: 'postgres://unused' });

/** `create()` throws ScheduleAlreadyRunning for `existing` ids; `update()` results are recorded. */
function fakeScheduleClient({ existing }: { existing: string[] }) {
  const created = new Map<string, Record<string, unknown>>();
  const updated = new Map<string, unknown>();
  const client = {
    schedule: {
      create: (opts: { scheduleId: string }) => {
        if (existing.includes(opts.scheduleId))
          return Promise.reject(new ScheduleAlreadyRunning('exists', opts.scheduleId));
        created.set(opts.scheduleId, opts);
        return Promise.resolve({ scheduleId: opts.scheduleId });
      },
      getHandle: (id: string) => ({
        update: (fn: (prev: ScheduleDescription) => unknown) => {
          updated.set(id, fn({} as ScheduleDescription));
          return Promise.resolve();
        },
      }),
    },
  };
  return { client: client as unknown as Client, created, updated };
}

test('ensureSchedules creates both schedules', async () => {
  const sc = fakeScheduleClient({ existing: [] });
  await ensureSchedules(sc.client, settings);
  expect([...sc.created.keys()].sort()).toEqual(['pm-scan', 'pm-stage']);
});

test('pm-scan runs ScanWorkflow every scanIntervalS, pm-stage StagingWorkflow every 300 s', async () => {
  const sc = fakeScheduleClient({ existing: [] });
  await ensureSchedules(sc.client, settings);
  expect(sc.created.get('pm-scan')).toMatchObject({
    spec: { intervals: [{ every: '30s' }] },
    action: { type: 'startWorkflow', workflowType: 'ScanWorkflow', taskQueue: 'main', args: [] },
    policies: { overlap: ScheduleOverlapPolicy.SKIP },
  });
  expect(sc.created.get('pm-stage')).toMatchObject({
    spec: { intervals: [{ every: '300s' }] },
    action: {
      type: 'startWorkflow',
      workflowType: 'StagingWorkflow',
      taskQueue: 'main',
      args: [false],
    },
    policies: { overlap: ScheduleOverlapPolicy.SKIP },
  });
});

test('ensureSchedules updates an existing schedule to the current interval', async () => {
  const sc = fakeScheduleClient({ existing: ['pm-scan', 'pm-stage'] });
  await ensureSchedules(sc.client, { ...settings, scanIntervalS: 45 });
  expect(sc.updated.get('pm-scan')).toMatchObject({
    spec: { intervals: [{ every: '45s' }] },
    policies: { overlap: ScheduleOverlapPolicy.SKIP },
  });
  expect(sc.updated.get('pm-scan')).not.toHaveProperty('scheduleId');
  expect(sc.updated.get('pm-stage')).toMatchObject({ spec: { intervals: [{ every: '300s' }] } });
});

test('ensureSchedules surfaces other create failures', async () => {
  const sc = fakeScheduleClient({ existing: [] });
  const client = {
    schedule: { ...sc.client.schedule, create: () => Promise.reject(new Error('unavailable')) },
  } as unknown as Client;
  await expect(ensureSchedules(client, settings)).rejects.toThrow('unavailable');
});
