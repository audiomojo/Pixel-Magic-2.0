import type pg from 'pg';
import type { DataLayout } from '../fsops.js';
import type { HarvestSettings } from './config.js';
import type { KnownItem, KnownOutcome } from './known.js';
import type { FsOps } from './paths.js';
import { setRunState, type RunState } from './runs.js';
import { makeScanFolder } from './scan.js';
import { linkFiles, makeFinishRun, makeSendBatch } from './send.js';
import type { DetectOptions, SystemProbe } from './sources.js';

/** Starts, signals and cancels harvest workflows (implemented by Task 8). */
export interface RunControl {
  start(runId: string): Promise<void>;
  signal(runId: string, name: 'pause' | 'resume' | 'itemsApproved'): Promise<void>;
  cancel(runId: string): Promise<void>;
}

export interface HarvestDeps {
  pool: pg.Pool;
  settings: HarvestSettings;
  layout: DataLayout;
  sys: SystemProbe;
  temporal?: RunControl;
  /** Crash points for tests: called with a name; a throw simulates a crash there. */
  checkpoint?: (name: string) => void | Promise<void>;
  /** Passed to resolveMount; tests point `volumes` at a temp directory. */
  detectOpts?: DetectOptions;
  /** EXIF Make reader override (tests inject faults). */
  readMake?: (path: string) => Promise<string | null>;
  /** Scan heartbeat period (default 5 s); tests shorten it. */
  scanHeartbeatMs?: number;
  /** Filesystem seam for the send path (tests record calls or inject faults). */
  fsOps?: FsOps;
  /** Known-content check override (tests interleave state changes). */
  knownCheck?: (pool: pg.Pool, item: KnownItem) => Promise<KnownOutcome>;
}

export function createHarvestActivities(deps: HarvestDeps) {
  return {
    scanFolder: makeScanFolder(deps),
    sendBatch: makeSendBatch(deps),
    finishRun: makeFinishRun(deps),
    linkFiles: (): Promise<number> => linkFiles(deps.pool),
    setRunState: (runId: string, state: RunState, reason?: string | null): Promise<void> =>
      setRunState(deps.pool, runId, state, reason ?? null),
  };
}
