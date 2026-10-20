import type pg from 'pg';
import type { DataLayout } from '../fsops.js';
import type { HarvestSettings } from './config.js';
import { makeScanFolder } from './scan.js';
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
}

export function createHarvestActivities(deps: HarvestDeps) {
  return { scanFolder: makeScanFolder(deps) };
}
