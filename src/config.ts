import { availableParallelism, totalmem } from 'node:os';
import { z } from 'zod';

/** compose forwards an unset variable as `''` (`${NAME:-}`): that means "use the default". */
const unsetIfEmpty = (v: unknown): unknown => (v === '' ? undefined : v);

/** An integer ≥ `min` (1 unless 0 is meaningful: a 0 queue size or poison cap breaks the pipeline). */
const int = (def: number, min = 1) =>
  z.preprocess(unsetIfEmpty, z.coerce.number().int().min(min).default(def));

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
  CPU_CONCURRENCY: z.preprocess(unsetIfEmpty, z.coerce.number().int().min(1).optional()),
  DECODE_MEM_BUDGET_MB: int(1280),
  DECODE_TIMEOUT_S: int(60),
  POISON_ATTEMPTS: int(5),
  LOOK_ALIKE_PHASH: int(8, 0),
  LOOK_ALIKE_DHASH: int(10, 0),
  COPY_PHASH: int(4, 0),
  COPY_DHASH: int(6, 0),
  LIVE_MAX_DURATION_S: int(5),
  STAGE_MAX_STALENESS_S: int(900),
});

/** Not forwarded: compose sets these itself (paths, addresses, queues per service, the namespace it creates). */
const SET_BY_COMPOSE = new Set([
  'DATA_DIR',
  'DATABASE_URL',
  'TEMPORAL_ADDRESS',
  'TEMPORAL_NAMESPACE',
  'WORKER_QUEUES',
]);

/** Every tunable env name, which docker-compose.yml forwards to the workers as `${NAME:-}`. */
export const TUNABLE_ENV: readonly string[] = Object.keys(schema.shape).filter(
  (n) => !SET_BY_COMPOSE.has(n),
);

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
  /** Memory one decode may need (a ~60 MP HEIC peaks near 1 GB); sizes the default `cpuConcurrency`. */
  decodeMemBudgetMb: number;
  decodeTimeoutS: number;
  poisonAttempts: number;
  lookAlikePhash: number;
  lookAlikeDhash: number;
  copyPhash: number;
  copyDhash: number;
  liveMaxDurationS: number;
  stageMaxStalenessS: number;
}

/** Memory available to this process: the cgroup limit when one is set and below RAM, else RAM. */
export function memoryBytes(
  constrained: number = process.constrainedMemory(),
  total: number = totalmem(),
): number {
  return constrained > 0 ? Math.min(constrained, total) : total;
}

/** Default decode parallelism: one per CPU, but no more than memory / budget, and at least 1. */
export function decodeConcurrency(cpus: number, memBytes: number, budgetMb: number): number {
  return Math.max(1, Math.min(cpus, Math.floor(memBytes / (budgetMb * 1024 * 1024))));
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
    cpuConcurrency:
      e.CPU_CONCURRENCY ??
      decodeConcurrency(availableParallelism(), memoryBytes(), e.DECODE_MEM_BUDGET_MB),
    decodeMemBudgetMb: e.DECODE_MEM_BUDGET_MB,
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
