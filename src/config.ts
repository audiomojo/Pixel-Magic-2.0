import { availableParallelism } from 'node:os';
import { z } from 'zod';

const int = (def: number) => z.coerce.number().int().default(def);

const schema = z.object({
  DATA_DIR: z.string().default('/data'),
  DATABASE_URL: z.string().min(1),
  TEMPORAL_ADDRESS: z.string().default('temporal:7233'),
  TEMPORAL_NAMESPACE: z.string().default('default'),
  WORKER_QUEUES: z.string().default('main,io,cpu,cluster'),
  SCAN_INTERVAL_S: int(30),
  STABLE_SECONDS: int(5),
  MAX_IN_FLIGHT: int(5000),
  IO_CONCURRENCY: int(8),
  CPU_CONCURRENCY: z.coerce.number().int().positive().optional(),
  DECODE_TIMEOUT_S: int(60),
  POISON_ATTEMPTS: int(5),
  LOOK_ALIKE_PHASH: int(8),
  LOOK_ALIKE_DHASH: int(10),
  COPY_PHASH: int(4),
  COPY_DHASH: int(6),
  LIVE_MAX_DURATION_S: int(5),
  STAGE_MAX_STALENESS_S: int(900),
});

export interface Settings {
  dataDir: string;
  databaseUrl: string;
  temporalAddress: string;
  temporalNamespace: string;
  workerQueues: string[];
  scanIntervalS: number;
  stableSeconds: number;
  maxInFlight: number;
  ioConcurrency: number;
  cpuConcurrency: number;
  decodeTimeoutS: number;
  poisonAttempts: number;
  lookAlikePhash: number;
  lookAlikeDhash: number;
  copyPhash: number;
  copyDhash: number;
  liveMaxDurationS: number;
  stageMaxStalenessS: number;
}

export function loadSettings(env: NodeJS.ProcessEnv = process.env): Settings {
  const r = schema.safeParse(env);
  if (!r.success) {
    const msg = r.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ');
    throw new Error(`Invalid configuration: ${msg}`);
  }
  const e = r.data;
  return {
    dataDir: e.DATA_DIR,
    databaseUrl: e.DATABASE_URL,
    temporalAddress: e.TEMPORAL_ADDRESS,
    temporalNamespace: e.TEMPORAL_NAMESPACE,
    workerQueues: e.WORKER_QUEUES.split(',')
      .map((q) => q.trim())
      .filter((q) => q.length > 0),
    scanIntervalS: e.SCAN_INTERVAL_S,
    stableSeconds: e.STABLE_SECONDS,
    maxInFlight: e.MAX_IN_FLIGHT,
    ioConcurrency: e.IO_CONCURRENCY,
    cpuConcurrency: e.CPU_CONCURRENCY ?? availableParallelism(),
    decodeTimeoutS: e.DECODE_TIMEOUT_S,
    poisonAttempts: e.POISON_ATTEMPTS,
    lookAlikePhash: e.LOOK_ALIKE_PHASH,
    lookAlikeDhash: e.LOOK_ALIKE_DHASH,
    copyPhash: e.COPY_PHASH,
    copyDhash: e.COPY_DHASH,
    liveMaxDurationS: e.LIVE_MAX_DURATION_S,
    stageMaxStalenessS: e.STAGE_MAX_STALENESS_S,
  };
}
