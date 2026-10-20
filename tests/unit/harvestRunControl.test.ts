import { WorkflowExecutionAlreadyStartedError, type Client } from '@temporalio/client';
import { WorkflowNotFoundError } from '@temporalio/common';
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { createHarvestActivities, type RunControl } from '../../src/harvest/activities.js';
import { temporalRunControl } from '../../src/harvest/control.js';
import {
  createRun,
  finishRunRows,
  loadRun,
  setRunState,
  startNextQueued,
} from '../../src/harvest/runs.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});
beforeEach(() => pgh.truncate());

/** A fake control; `closed` lists runs whose workflow already closed (start and describe say so). */
function control(closed: string[] = []): RunControl & { started: string[] } {
  const started: string[] = [];
  return {
    started,
    start: (id) => {
      if (closed.includes(id)) return Promise.resolve('closed' as const);
      started.push(id);
      return Promise.resolve('started' as const);
    },
    describe: (id) => Promise.resolve(closed.includes(id) ? 'closed' : 'running'),
    signal: () => Promise.resolve(),
    cancel: () => Promise.resolve('requested' as const),
  };
}
const runRow = async (id: string) =>
  (
    await pgh.pool.query<{ state: string; error: string | null }>(
      'SELECT state, error FROM harvest_runs WHERE id = $1',
      [id],
    )
  ).rows[0]!;

async function queuedRun(dry = false): Promise<string> {
  const { rows } = await pgh.pool.query<{ id: string }>(
    `INSERT INTO harvest_sources (identity, label, kind) VALUES ('id-' || gen_random_uuid(), 'Drive-' || gen_random_uuid(), 'local') RETURNING id::text AS id`,
  );
  return createRun(pgh.pool, [{ sourceId: rows[0]!.id, relPath: 'a' }], dry);
}

describe('startNextQueued', () => {
  test('with no active run it starts the oldest queued run', async () => {
    const first = await queuedRun();
    await queuedRun();
    const c = control();
    expect(await startNextQueued(pgh.pool, c)).toBe(first);
    expect(c.started).toEqual([first]);
  });

  test.each(['scanning', 'sending', 'paused'] as const)(
    'with a %s run it starts nothing',
    async (state) => {
      const active = await queuedRun();
      await queuedRun();
      await setRunState(pgh.pool, active, state, state === 'paused' ? 'disk full' : null);
      const c = control();
      expect(await startNextQueued(pgh.pool, c)).toBeNull();
      expect(c.started).toEqual([]);
    },
  );

  test('finished runs do not block, and an empty queue starts nothing', async () => {
    const done = await queuedRun();
    await pgh.pool.query("UPDATE harvest_runs SET state = 'done' WHERE id = $1", [done]);
    const c = control();
    expect(await startNextQueued(pgh.pool, c)).toBeNull();
    const next = await queuedRun();
    expect(await startNextQueued(pgh.pool, c)).toBe(next);
  });

  test('a queued run whose workflow already closed is failed and the next one starts', async () => {
    const stale = await queuedRun();
    const next = await queuedRun();
    const c = control([stale]);
    expect(await startNextQueued(pgh.pool, c)).toBe(next);
    expect(c.started).toEqual([next]);
    expect(await runRow(stale)).toEqual({ state: 'failed', error: 'workflow closed unexpectedly' });
  });

  test.each(['scanning', 'sending', 'paused'] as const)(
    'a %s run whose workflow is gone no longer blocks the queue',
    async (state) => {
      const dead = await queuedRun();
      const next = await queuedRun();
      await setRunState(pgh.pool, dead, state, state === 'paused' ? 'Drive unplugged' : null);
      const c = control([dead]);
      expect(await startNextQueued(pgh.pool, c)).toBe(next);
      expect(await runRow(dead)).toEqual({
        state: 'failed',
        error: 'workflow closed unexpectedly',
      });
    },
  );

  test('host startup: with no queued run nothing is started (spec §10.7)', async () => {
    // The host calls startNextQueued once at startup: it may only start an already-queued run.
    const c = control();
    expect(await startNextQueued(pgh.pool, c)).toBeNull();
    for (const state of ['done', 'cancelled', 'failed']) {
      const id = await queuedRun();
      await pgh.pool.query('UPDATE harvest_runs SET state = $2 WHERE id = $1', [id, state]);
    }
    expect(await startNextQueued(pgh.pool, c)).toBeNull();
    expect(c.started).toEqual([]);
  });

  test('the activity needs run control', async () => {
    const acts = createHarvestActivities({ pool: pgh.pool } as never);
    expect(() => acts.startNextQueued()).toThrow(/run control/);
    const c = control();
    const run = await queuedRun();
    const withControl = createHarvestActivities({ pool: pgh.pool, temporal: c } as never);
    expect(await withControl.startNextQueued()).toBe(run);
  });
});

describe('loadRun', () => {
  test('returns the dry-run flag and folders', async () => {
    const id = await queuedRun(true);
    expect(await loadRun(pgh.pool, id)).toMatchObject({
      dryRun: true,
      folders: [{ relPath: 'a', state: 'queued' }],
    });
    await expect(loadRun(pgh.pool, '999999')).rejects.toThrow(/not found/);
  });
});

describe('send-only runs', () => {
  test('loadRun reports sendOnly; finishing one leaves folder history untouched', async () => {
    const { rows } = await pgh.pool.query<{ id: string }>(
      `INSERT INTO harvest_sources (identity, label, kind) VALUES ('i', 'D', 'local') RETURNING id::text AS id`,
    );
    const folder = [{ sourceId: rows[0]!.id, relPath: 'a' }];
    const so = await createRun(pgh.pool, folder, { sendOnly: true });
    const normal = await createRun(pgh.pool, folder, { dryRun: false });
    expect(await loadRun(pgh.pool, so)).toMatchObject({ sendOnly: true, dryRun: false });
    expect(await loadRun(pgh.pool, normal)).toMatchObject({ sendOnly: false });
    await finishRunRows(pgh.pool, so, 'done');
    const h = async (): Promise<{ last_harvested_at: Date | null; last_run_id: string | null }> =>
      (
        await pgh.pool.query<{ last_harvested_at: Date | null; last_run_id: string | null }>(
          'SELECT last_harvested_at, last_run_id FROM harvest_folders',
        )
      ).rows[0]!;
    expect(await h()).toEqual({ last_harvested_at: null, last_run_id: null });
    await finishRunRows(pgh.pool, normal, 'done');
    expect((await h()).last_harvested_at).not.toBeNull();
  });
});

describe('temporalRunControl', () => {
  function fakeClient(
    startError?: Error,
    o: { status?: string; describeError?: Error; cancelError?: Error } = {},
  ) {
    const signal = vi.fn(() => Promise.resolve());
    const cancel = vi.fn(() => (o.cancelError ? Promise.reject(o.cancelError) : Promise.resolve()));
    const start = vi.fn(() => (startError ? Promise.reject(startError) : Promise.resolve()));
    const describe = vi.fn(() =>
      o.describeError
        ? Promise.reject(o.describeError)
        : Promise.resolve({ status: { name: o.status ?? 'RUNNING' } }),
    );
    const getHandle = vi.fn(() => ({ signal, cancel, describe }));
    return {
      client: { workflow: { start, getHandle } } as unknown as Client,
      signal,
      cancel,
      start,
      getHandle,
    };
  }
  const notFound = () => new WorkflowNotFoundError('workflow execution already completed', 'h', '');

  test('start uses workflow id harvest-<runId> on the harvest queue, rejecting duplicates', async () => {
    const f = fakeClient();
    await temporalRunControl(f.client).start('12');
    expect(f.start).toHaveBeenCalledWith(
      'HarvestWorkflow',
      expect.objectContaining({
        taskQueue: 'harvest',
        workflowId: 'harvest-12',
        args: [{ runId: '12' }],
        workflowIdReusePolicy: 'REJECT_DUPLICATE',
      }),
    );
  });

  test('an already-started workflow is success; other errors propagate', async () => {
    const dup = new WorkflowExecutionAlreadyStartedError('x', 'harvest-1', 'HarvestWorkflow');
    await expect(temporalRunControl(fakeClient().client).start('1')).resolves.toBe('started');
    await expect(temporalRunControl(fakeClient(dup).client).start('1')).resolves.toBe('running');
    await expect(
      temporalRunControl(fakeClient(new Error('down')).client).start('1'),
    ).rejects.toThrow('down');
  });

  test('a duplicate whose workflow already closed is reported closed', async () => {
    const dup = new WorkflowExecutionAlreadyStartedError('x', 'harvest-1', 'HarvestWorkflow');
    for (const status of ['COMPLETED', 'FAILED', 'TERMINATED', 'TIMED_OUT', 'CANCELLED']) {
      const c = temporalRunControl(fakeClient(dup, { status }).client);
      await expect(c.start('1')).resolves.toBe('closed');
    }
  });

  test('describe: running, closed, or missing', async () => {
    await expect(temporalRunControl(fakeClient().client).describe('1')).resolves.toBe('running');
    const closed = fakeClient(undefined, { status: 'TERMINATED' });
    await expect(temporalRunControl(closed.client).describe('1')).resolves.toBe('closed');
    const gone = fakeClient(undefined, { describeError: notFound() });
    await expect(temporalRunControl(gone.client).describe('1')).resolves.toBe('missing');
    const down = fakeClient(undefined, { describeError: new Error('down') });
    await expect(temporalRunControl(down.client).describe('1')).rejects.toThrow('down');
  });

  test('cancel of a workflow that no longer exists or already closed is "gone"', async () => {
    const gone = fakeClient(undefined, { cancelError: notFound() });
    await expect(temporalRunControl(gone.client).cancel('1')).resolves.toBe('gone');
    await expect(temporalRunControl(fakeClient().client).cancel('1')).resolves.toBe('requested');
    const down = fakeClient(undefined, { cancelError: new Error('down') });
    await expect(temporalRunControl(down.client).cancel('1')).rejects.toThrow('down');
  });

  test('signal and cancel go to the run workflow', async () => {
    const f = fakeClient();
    const c = temporalRunControl(f.client);
    await c.signal('3', 'resume');
    await c.cancel('3');
    expect(f.getHandle).toHaveBeenCalledWith('harvest-3');
    expect(f.signal).toHaveBeenCalledWith('resume');
    expect(f.cancel).toHaveBeenCalled();
  });
});
