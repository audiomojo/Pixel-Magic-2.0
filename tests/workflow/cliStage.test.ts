// `cli stage` end to end: it connects with TEMPORAL_ADDRESS, runs StagingWorkflow on `main`, and
// the workflow's maybeBuildStaging (faked here) gets the --force flag.
import { createServer } from 'node:net';
import { fileURLToPath } from 'node:url';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { bundleWorkflowCode, DefaultLogger, Runtime, Worker } from '@temporalio/worker';
import { afterAll, beforeAll, expect, test, vi } from 'vitest';
import { main } from '../../src/cli.js';

async function freePort(): Promise<number> {
  const srv = createServer();
  await new Promise<void>((resolve) => srv.listen(0, '127.0.0.1', resolve));
  const addr = srv.address();
  await new Promise((resolve) => srv.close(resolve));
  if (addr === null || typeof addr === 'string') throw new Error('no port');
  return addr.port;
}

let env: TestWorkflowEnvironment;
let port: number;
const workers: Worker[] = [];
let running: Promise<unknown>;
const forces: boolean[] = [];

beforeAll(async () => {
  Runtime.install({
    logger: new DefaultLogger('ERROR'),
    telemetryOptions: { logging: { filter: { core: 'ERROR', other: 'ERROR' }, forward: {} } },
  });
  port = await freePort();
  env = await TestWorkflowEnvironment.createTimeSkipping({ server: { port } });
  const workflowBundle = await bundleWorkflowCode({
    workflowsPath: fileURLToPath(new URL('../../src/workflows.ts', import.meta.url)),
    logger: new DefaultLogger('ERROR'),
  });
  const activities = {
    maybeBuildStaging: (force: boolean) => {
      forces.push(force);
      return Promise.resolve({ gen: 3, entries: 5, skipped: false });
    },
  };
  workers.push(
    await Worker.create({ connection: env.nativeConnection, taskQueue: 'main', workflowBundle }),
    await Worker.create({ connection: env.nativeConnection, taskQueue: 'io', activities }),
  );
  running = Promise.all(workers.map((w) => w.run()));
}, 120_000);

afterAll(async () => {
  for (const w of workers) w.shutdown();
  await running;
  await env.teardown();
  vi.unstubAllEnvs();
});

test('cli stage --force runs StagingWorkflow through TEMPORAL_ADDRESS', async () => {
  vi.stubEnv('DATABASE_URL', 'postgres://unused');
  vi.stubEnv('TEMPORAL_ADDRESS', `127.0.0.1:${String(port)}`);
  let out = '';
  const spy = vi.spyOn(process.stdout, 'write').mockImplementation((chunk: string | Uint8Array) => {
    out += String(chunk);
    return true;
  });
  try {
    expect(await main(['stage', '--force'])).toBe(0);
  } finally {
    spy.mockRestore();
  }
  expect(forces).toEqual([true]);
  expect(JSON.parse(out)).toEqual({ gen: 3, entries: 5, skipped: false });
});
