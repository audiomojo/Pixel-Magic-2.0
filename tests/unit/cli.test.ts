import type { Client } from '@temporalio/client';
import { afterEach, expect, test, vi } from 'vitest';
import { main } from '../../src/cli.js';

interface Executed {
  workflowType: unknown;
  opts: { taskQueue: string; workflowId: string; args: unknown[] };
}

function fakeClient() {
  const executed: Executed[] = [];
  const client = {
    workflow: {
      execute: (workflowType: unknown, opts: Executed['opts']) => {
        executed.push({ workflowType, opts });
        return Promise.resolve({ gen: 4, entries: 12, skipped: false });
      },
    },
  };
  return { client: client as unknown as Client, executed };
}

function capture(stream: 'stdout' | 'stderr'): () => string {
  let buf = '';
  vi.spyOn(process[stream], 'write').mockImplementation((chunk: string | Uint8Array) => {
    buf += String(chunk);
    return true;
  });
  return () => buf;
}

afterEach(() => {
  vi.restoreAllMocks();
});

test('stage runs StagingWorkflow on main and prints its result', async () => {
  const f = fakeClient();
  const out = capture('stdout');
  expect(await main(['stage'], { temporal: f.client })).toBe(0);
  expect(f.executed).toHaveLength(1);
  expect(f.executed[0]!.workflowType).toBe('StagingWorkflow');
  expect(f.executed[0]!.opts.taskQueue).toBe('main');
  expect(f.executed[0]!.opts.args).toEqual([false]);
  expect(f.executed[0]!.opts.workflowId).toMatch(/^pm-stage-cli-\d+$/);
  expect(out()).toContain('"gen":4');
});

test('stage --force forces a rebuild', async () => {
  const f = fakeClient();
  capture('stdout');
  expect(await main(['stage', '--force'], { temporal: f.client })).toBe(0);
  expect(f.executed[0]!.opts.args).toEqual([true]);
});

test('unknown command exits 2 with usage', async () => {
  const err = capture('stderr');
  expect(await main(['nope'], { temporal: fakeClient().client })).toBe(2);
  expect(err()).toMatch(/usage/i);
});

test('unknown option exits 2 with usage', async () => {
  const err = capture('stderr');
  expect(await main(['stage', '--bogus'], { temporal: fakeClient().client })).toBe(2);
  expect(err()).toMatch(/usage/i);
});
