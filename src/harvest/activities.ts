import type pg from 'pg';
import type { DataLayout } from '../fsops.js';
import type { HarvestSettings } from './config.js';
import type { KnownItem, KnownOptions, KnownOutcome } from './known.js';
import type { FsOps, SourcePath } from './paths.js';
import {
  loadRun,
  setFolderState,
  setRunState,
  startNextQueued,
  type FolderState,
  type RunState,
} from './runs.js';
import { makeScanFolder } from './scan.js';
import { linkFiles, makeFinishRun, makeSendBatch } from './send.js';
import type { DetectOptions, SystemProbe } from './sources.js';

/** Starts, signals and cancels harvest workflows (implemented by Task 8). */
export interface RunControl {
  /**
   * Starts the run's workflow. `running`: it already runs (a duplicate start); `closed`: a
   * workflow with that id already ended, so the run can never be started again.
   */
  start(runId: string): Promise<'started' | 'running' | 'closed'>;
  /** Whether the run's workflow is running, has ended, or does not exist (any more). */
  describe(runId: string): Promise<'running' | 'closed' | 'missing'>;
  signal(runId: string, name: 'pause' | 'resume' | 'itemsApproved'): Promise<void>;
  /** `gone`: there is no running workflow to cancel (never started, ended, or deleted). */
  cancel(runId: string): Promise<'requested' | 'gone'>;
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
  readMake?: (path: SourcePath) => Promise<string | null>;
  /** Scan heartbeat period (default 5 s); tests shorten it. */
  scanHeartbeatMs?: number;
  /** Filesystem seam for the send path (tests record calls or inject faults). */
  fsOps?: FsOps;
  /** Items per sendBatch (default 500); tests lower it. */
  sendBatchSize?: number;
  /** Known-content check override (tests interleave state changes). */
  knownCheck?: (pool: pg.Pool, item: KnownItem, opts?: KnownOptions) => Promise<KnownOutcome>;
}

export function createHarvestActivities(deps: HarvestDeps) {
  return {
    scanFolder: makeScanFolder(deps),
    sendBatch: makeSendBatch(deps),
    finishRun: makeFinishRun(deps),
    linkFiles: (): Promise<number> => linkFiles(deps.pool),
    setRunState: (runId: string, state: RunState, reason?: string | null): Promise<void> =>
      setRunState(deps.pool, runId, state, reason ?? null),
    loadRun: (runId: string) => loadRun(deps.pool, runId),
    setFolderState: (runId: string, folderId: string, state: FolderState): Promise<void> =>
      setFolderState(deps.pool, runId, folderId, state),
    startNextQueued: (): Promise<string | null> => {
      if (!deps.temporal) throw new Error('startNextQueued: no Temporal run control configured');
      return startNextQueued(deps.pool, deps.temporal);
    },
  };
}
