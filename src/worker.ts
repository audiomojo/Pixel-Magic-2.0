// Worker entry (startup wiring only): `node dist/worker.js`. Serves the queues in WORKER_QUEUES.
import { fileURLToPath } from 'node:url';
import { Client, Connection } from '@temporalio/client';
import {
  DefaultLogger,
  NativeConnection,
  Runtime,
  Worker,
  type LogLevel,
  type WorkerOptions,
} from '@temporalio/worker';
import { createDecodePool, type WorkerPool } from './activities/hashing.js';
import { createActivities } from './activities/index.js';
import { loadSettings } from './config.js';
import { crashAt } from './crashAt.js';
import { createPool } from './db.js';
import { checkSameDevice, DataLayout } from './fsops.js';
import { logger } from './logger.js';
import { MetadataReader } from './metadata.js';
import { reconcilePurgeLog } from './purgeLog.js';
import { ensureSchedules } from './schedules.js';

const QUEUES = ['main', 'io', 'cpu', 'cluster'] as const;
type Queue = (typeof QUEUES)[number];
const here = (file: string) => fileURLToPath(new URL(file, import.meta.url));

/** Stand-in when `cpu` isn't served: no decode is ever routed to this worker. */
const noDecodePool = {
  exec: () => {
    throw new Error('this worker does not serve the cpu queue');
  },
} as unknown as WorkerPool;

async function main(): Promise<void> {
  const level = (process.env['LOG_LEVEL'] ?? 'info').toUpperCase() as LogLevel;
  Runtime.install({
    logger: new DefaultLogger(level, ({ level: l, message, meta }) =>
      logger[l.toLowerCase() as Lowercase<LogLevel>](meta ?? {}, message),
    ),
  });

  const settings = loadSettings();
  const queues = settings.workerQueues;
  const unknown = queues.filter((q) => !(QUEUES as readonly string[]).includes(q));
  if (unknown.length > 0) throw new Error(`WORKER_QUEUES: unknown queue(s) ${unknown.join(', ')}`);
  const serves = (q: Queue) => queues.includes(q);

  const layout = new DataLayout(settings.dataDir);
  await layout.ensure();
  await checkSameDevice(layout);
  // Everything opened from here on is closed on every exit path, startup failures included.
  const cleanup: (() => Promise<unknown>)[] = [];
  try {
    const metadata = new MetadataReader();
    cleanup.push(() => metadata.end());
    await metadata.read(fileURLToPath(import.meta.url)); // exiftool works before any claim
    const pool = createPool(settings.databaseUrl, settings.ioConcurrency + 4);
    cleanup.push(() => pool.end());
    // A worker never ingests without its block list: a failed reconcile is fatal at start.
    try {
      await reconcilePurgeLog(pool, settings.dataDir, {
        info: (m, meta) => logger.info(meta ?? {}, m),
        warn: (m, meta) => logger.warn(meta ?? {}, m),
      });
    } catch (err) {
      logger.fatal({ err }, 'purged.csv reconcile failed');
      throw err;
    }
    let decodePool = noDecodePool;
    if (serves('cpu')) {
      const p = createDecodePool(here('./decodeWorker.js'), settings.cpuConcurrency);
      cleanup.push(async () => {
        await p.terminate();
      }); // runs the child's onTerminate (ends its exiftool)
      decodePool = p;
    }

    const clientConnection = await Connection.connect({ address: settings.temporalAddress });
    cleanup.push(() => clientConnection.close());
    const temporal = new Client({
      connection: clientConnection,
      namespace: settings.temporalNamespace,
    });
    const connection = await NativeConnection.connect({ address: settings.temporalAddress });
    cleanup.push(() => connection.close());
    // Test seam only (chaos tier): unset in production, so no checkpoint is installed.
    const checkpoint = crashAt(process.env['CRASH_AT'], settings.dataDir);
    const activities = createActivities({
      pool,
      settings,
      layout,
      metadata,
      decodePool,
      temporal,
      ...(checkpoint === undefined ? {} : { checkpoint }),
    });

    // Under compose's stop_grace_period (60 s), so cleanup runs before a SIGKILL.
    const base = { connection, namespace: settings.temporalNamespace, shutdownGraceTime: '50s' };
    const perQueue: Record<Queue, Partial<WorkerOptions>> = {
      main: { workflowsPath: here('./workflows.js') },
      io: { activities, maxConcurrentActivityTaskExecutions: settings.ioConcurrency },
      cpu: { activities, maxConcurrentActivityTaskExecutions: settings.cpuConcurrency },
      // One per worker; the advisory lock makes clustering single-active across workers.
      cluster: { activities, maxConcurrentActivityTaskExecutions: 1 },
    };
    const workers: Worker[] = [];
    for (const q of QUEUES)
      if (serves(q)) workers.push(await Worker.create({ ...base, taskQueue: q, ...perQueue[q] }));

    if (serves('cluster')) await ensureSchedules(temporal, settings);
    logger.info(
      { queues, dataDir: settings.dataDir, cpuConcurrency: settings.cpuConcurrency },
      'worker started',
    );

    // SIGTERM/SIGINT shut every worker down (Runtime's default signals), so each run() returns.
    // If one worker fails, stop the rest too.
    await Promise.all(
      workers.map((w) =>
        w.run().catch((e: unknown) => {
          for (const other of workers) if (other.getState() === 'RUNNING') other.shutdown();
          throw e;
        }),
      ),
    );
  } finally {
    await Promise.allSettled(cleanup.reverse().map((close) => close()));
    logger.info('worker stopped');
  }
}

main().catch((err: unknown) => {
  logger.fatal({ err }, 'worker failed');
  process.exit(1);
});
