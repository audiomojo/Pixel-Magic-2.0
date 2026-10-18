import { MockActivityEnvironment, type MockActivityEnvironmentOptions } from '@temporalio/testing';
import type pg from 'pg';
import type { Deps } from '../../src/activities/index.js';
import { loadSettings } from '../../src/config.js';
import { DataLayout } from '../../src/fsops.js';
import { MetadataReader } from '../../src/metadata.js';
import { tmpDir } from './layout.js';

const noop = () => undefined;
/** Activity failures are the point of many tests; keep the mock's logger quiet. */
const silent: NonNullable<MockActivityEnvironmentOptions['logger']> = {
  log: noop,
  trace: noop,
  debug: noop,
  info: noop,
  warn: noop,
  error: noop,
};

/** Runs an activity function inside a mock Activity Context whose workflow run id is `runId`. */
export async function runActivity<A extends unknown[], R>(
  fn: (...a: A) => Promise<R>,
  args: A,
  opts: { runId?: string; onHeartbeat?: (d: unknown) => void } = {},
): Promise<R> {
  const env = new MockActivityEnvironment(
    {
      workflowExecution: { workflowId: 'test', runId: opts.runId ?? 'test-run' },
    },
    { logger: silent },
  );
  const onHeartbeat = opts.onHeartbeat;
  if (onHeartbeat) env.on('heartbeat', (d: unknown) => onHeartbeat(d));
  return env.run(fn, ...args);
}

/** Activity deps over a fresh, ensured DataLayout and the given Postgres; `close()` ends what it created. */
export async function testDeps(
  pg: { url: string; pool: pg.Pool },
  overrides: Partial<Deps> = {},
): Promise<{ deps: Deps; layout: DataLayout; close: () => Promise<void> }> {
  const layout = new DataLayout(await tmpDir());
  await layout.ensure();
  const settings = loadSettings({ DATABASE_URL: pg.url, DATA_DIR: layout.root });
  const metadata = new MetadataReader();
  const deps: Deps = { pool: pg.pool, settings, layout, metadata, ...overrides };
  return {
    deps,
    layout,
    close: () => metadata.end(),
  };
}
