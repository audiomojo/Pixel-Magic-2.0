import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';

/** An empty variable means "use the default" (same rule as src/config.ts). */
const unsetIfEmpty = (v: unknown): unknown => (v === '' ? undefined : v);

const int = (def: number, min = 1, max?: number) => {
  let n = z.coerce.number().int().min(min);
  if (max !== undefined) n = n.max(max);
  return z.preprocess(unsetIfEmpty, n.default(def));
};

const schema = z.object({
  DATA_DIR: z.preprocess(unsetIfEmpty, z.string().optional()),
  DATABASE_URL: z.preprocess(unsetIfEmpty, z.string().min(1).optional()),
  POSTGRES_PASSWORD: z.preprocess(unsetIfEmpty, z.string().optional()),
  TEMPORAL_ADDRESS: z.string().default('127.0.0.1:7233'),
  TEMPORAL_NAMESPACE: z.string().default('default'),
  HARVEST_UI_PORT: int(3020, 1, 65535),
  HARVEST_MAX_DISK_PCT: int(80, 1, 99),
  HARVEST_MAX_PENDING_FILES: int(20000),
  HARVEST_MAX_PENDING_BYTES: z.preprocess(unsetIfEmpty, z.coerce.number().int().min(1).optional()),
  HARVEST_MIN_SIZE: int(20480, 0),
  HARVEST_MAX_ATTEMPTS: int(5),
  HARVEST_THUMB_CACHE_MB: int(1024),
});

export interface HarvestSettings {
  dataDir: string;
  databaseUrl: string;
  temporalAddress: string;
  temporalNamespace: string;
  uiPort: number;
  maxDiskPct: number;
  maxPendingFiles: number;
  maxPendingBytes: number | null;
  minSize: number;
  maxAttempts: number;
  thumbCacheMb: number;
}

/** src/harvest or dist/harvest: two levels up is the repository root. */
const DEFAULT_REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/** Host-side settings: defaults point at the local compose stack (published on 127.0.0.1). */
export function loadHarvestSettings(
  env: NodeJS.ProcessEnv = process.env,
  repoRoot: string = DEFAULT_REPO_ROOT,
): HarvestSettings {
  const e = schema.parse(env);
  return {
    dataDir: resolve(repoRoot, e.DATA_DIR ?? 'data'),
    databaseUrl:
      e.DATABASE_URL ??
      `postgres://postgres:${e.POSTGRES_PASSWORD ?? 'pixelmagic'}@127.0.0.1:5432/pixelmagic`,
    temporalAddress: e.TEMPORAL_ADDRESS,
    temporalNamespace: e.TEMPORAL_NAMESPACE,
    uiPort: e.HARVEST_UI_PORT,
    maxDiskPct: e.HARVEST_MAX_DISK_PCT,
    maxPendingFiles: e.HARVEST_MAX_PENDING_FILES,
    maxPendingBytes: e.HARVEST_MAX_PENDING_BYTES ?? null,
    minSize: e.HARVEST_MIN_SIZE,
    maxAttempts: e.HARVEST_MAX_ATTEMPTS,
    thumbCacheMb: e.HARVEST_THUMB_CACHE_MB,
  };
}
