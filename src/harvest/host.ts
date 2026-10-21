// Harvester host process (startup wiring only): `npm run harvester`. One Temporal worker on the
// `harvest` queue, the local HTTP server, and the host heartbeat. Excluded from coverage.
import './hostLogEnv.js'; // must stay first: points the logger at DATA_DIR/logs/harvester.log
import { readFileSync } from 'node:fs';
import type { Server } from 'node:http';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client, Connection } from '@temporalio/client';
import {
  DefaultLogger,
  NativeConnection,
  Runtime,
  Worker,
  type LogLevel,
} from '@temporalio/worker';
import { createPool } from '../db.js';
import { DataLayout } from '../fsops.js';
import { logDestination, logger } from '../logger.js';
import { createHarvestActivities } from './activities.js';
import { loadHarvestSettings } from './config.js';
import { HARVEST_TASK_QUEUE, temporalRunControl } from './control.js';
import { rotateIfLarge } from './logRotate.js';
import { startNextQueued } from './runs.js';
import { linkFiles } from './send.js';
import { createHarvestServer } from './server.js';
import { realSystem } from './system.js';

const here = (file: string) => fileURLToPath(new URL(file, import.meta.url));
const LOG_MAX_BYTES = 20 * 1024 * 1024;
const LOG_KEEP = 5;

let stopping = false;
let wake!: () => void;
const stopped = new Promise<void>((r) => {
  wake = r;
});
/** Sleeps `ms`, or until a stop signal arrives. */
const sleep = (ms: number) => Promise.race([new Promise<void>((r) => setTimeout(r, ms)), stopped]);

/** SIGTERM/SIGINT before the worker runs (stack down, still connecting): abort startup. */
class StopRequested extends Error {}
for (const sig of ['SIGTERM', 'SIGINT'] as const) {
  process.on(sig, () => {
    // Once the worker runs, Runtime's own handler shuts it down and run() returns.
    stopping = true;
    wake();
  });
}

/**
 * Retries `connect` with backoff (1 s doubling to 30 s) until it works or the process is
 * stopping. Logs the first failure and the recovery, not every attempt.
 */
async function connectWithRetry<T>(what: string, connect: () => Promise<T>): Promise<T> {
  let delay = 1000;
  let failed = false;
  for (;;) {
    try {
      const c = await connect();
      if (stopping) throw new StopRequested();
      if (failed) logger.info(`${what} is back`);
      return c;
    } catch (err) {
      if (stopping) throw new StopRequested();
      if (!failed) logger.warn({ err }, `${what} unavailable, retrying with backoff`);
      failed = true;
      await sleep(delay);
      if (stopping) throw new StopRequested();
      delay = Math.min(delay * 2, 30_000);
    }
  }
}

/** Runs `fn` every `ms`, never overlapping itself; failures are logged, never thrown. */
function every(ms: number, what: string, fn: () => Promise<unknown>): NodeJS.Timeout {
  let running = false;
  return setInterval(() => {
    if (running) return;
    running = true;
    fn()
      .catch((err: unknown) => logger.warn({ err }, `harvest: ${what} failed`))
      .finally(() => {
        running = false;
      });
  }, ms);
}

async function main(): Promise<void> {
  const level = (process.env['LOG_LEVEL'] ?? 'info').toUpperCase() as LogLevel;
  Runtime.install({
    logger: new DefaultLogger(level, ({ level: l, message, meta }) =>
      logger[l.toLowerCase() as Lowercase<LogLevel>](meta ?? {}, message),
    ),
  });
  const settings = loadHarvestSettings();
  const version = (
    JSON.parse(readFileSync(here('../../package.json'), 'utf8')) as { version: string }
  ).version;
  const layout = new DataLayout(settings.dataDir);
  await layout.ensure();

  // Everything opened from here on is closed on every exit path, startup failures included.
  const cleanup: (() => Promise<unknown>)[] = [];
  const timers: NodeJS.Timeout[] = [];
  try {
    const pool = createPool(settings.databaseUrl, 8);
    cleanup.push(() => pool.end());
    await connectWithRetry('postgres', () => pool.query('SELECT 1'));

    const clientConnection = await connectWithRetry('temporal', () =>
      Connection.connect({ address: settings.temporalAddress }),
    );
    cleanup.push(() => clientConnection.close());
    const client = new Client({
      connection: clientConnection,
      namespace: settings.temporalNamespace,
    });
    const control = temporalRunControl(client);
    const connection = await connectWithRetry('temporal worker connection', () =>
      NativeConnection.connect({ address: settings.temporalAddress }),
    );
    cleanup.push(() => connection.close());

    const activities = createHarvestActivities({
      pool,
      settings,
      layout,
      sys: realSystem,
      temporal: control,
    });
    const worker = await Worker.create({
      connection,
      namespace: settings.temporalNamespace,
      taskQueue: HARVEST_TASK_QUEUE,
      workflowsPath: here('../workflows.js'),
      activities,
      maxConcurrentActivityTaskExecutions: 4,
      shutdownGraceTime: '30s',
    });

    const temporalHealthy = async (): Promise<boolean> => {
      try {
        await Promise.race([
          clientConnection.workflowService.getSystemInfo({}),
          sleep(3000).then(() => Promise.reject(new Error('timeout'))),
        ]);
        return true;
      } catch {
        return false;
      }
    };
    const server: Server = createHarvestServer({
      pool,
      settings,
      sys: realSystem,
      control,
      staticDir: here('../../src/harvest/ui'),
      temporalHealthy,
    });
    cleanup.push(
      () =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
          server.closeAllConnections();
        }),
    );
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(settings.uiPort, '127.0.0.1', () => resolve());
    });
    server.on('error', (err) => logger.error({ err }, 'harvest: http server error'));

    const heartbeat = () =>
      pool.query(
        'UPDATE harvest_host_state SET heartbeat_at = clock_timestamp(), version = $1 WHERE id = 1',
        [version],
      );
    await heartbeat().catch((err: unknown) => logger.warn({ err }, 'harvest: heartbeat failed'));
    timers.push(every(30_000, 'heartbeat', heartbeat));
    timers.push(every(60_000, 'linkFiles', () => linkFiles(pool)));
    const logFile = join(settings.dataDir, 'logs', 'harvester.log');
    timers.push(
      every(60_000, 'log rotation', async () => {
        if (logDestination && (await rotateIfLarge(logFile, LOG_MAX_BYTES, LOG_KEEP))) {
          logDestination.reopen();
        }
      }),
    );

    logger.info(
      { dataDir: settings.dataDir, port: settings.uiPort, queue: HARVEST_TASK_QUEUE },
      'harvester started',
    );
    if (stopping) throw new StopRequested();
    const running = worker.run();
    startNextQueued(pool, control).catch((err: unknown) =>
      logger.warn({ err }, 'harvest: startNextQueued at startup failed'),
    );
    // SIGTERM/SIGINT make the worker shut down (Runtime's default signals), so run() returns.
    await running;
  } catch (err) {
    if (!(err instanceof StopRequested)) throw err;
    logger.info('stop requested during startup');
  } finally {
    stopping = true;
    for (const t of timers) clearInterval(t);
    await Promise.allSettled(cleanup.reverse().map((close) => close()));
    logger.info('harvester stopped');
  }
}

main()
  .then(() => process.exit(0))
  .catch((err: unknown) => {
    logger.fatal({ err }, 'harvester failed');
    // Give the async log destination a moment to flush before exiting.
    setTimeout(() => process.exit(1), 200);
  });
