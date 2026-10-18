import type { Client } from '@temporalio/client';
import type pg from 'pg';
import type { Settings } from '../config.js';
import type { DataLayout } from '../fsops.js';
import type { MetadataReader } from '../metadata.js';
import { makeClusterContent } from './cluster.js';
import { makeCopies } from './copies.js';
import { makeHashing, type WorkerPool } from './hashing.js';
import { makeIngestFile } from './ingest.js';
import { makeScan } from './scan.js';
import { makeStaging } from './staging.js';

export interface Deps {
  pool: pg.Pool;
  settings: Settings;
  layout: DataLayout;
  metadata: MetadataReader;
  /** Decode child processes; owned (and terminated) by whoever created it, never by activities. */
  decodePool: WorkerPool;
  /** Required by `scanAndStart`; `maybeBuildStaging` uses it to see running FileWorkflows. */
  temporal?: Client;
  /** Test seam: awaited at named crash points inside activities. */
  checkpoint?: (name: string) => void | Promise<void>;
}

export function createActivities(deps: Deps) {
  return {
    ingestFile: makeIngestFile(deps),
    ...makeHashing(deps),
    clusterContent: makeClusterContent(deps),
    ...makeCopies(deps),
    maybeBuildStaging: makeStaging(deps),
    scanAndStart: makeScan(deps),
  };
}

export type Activities = ReturnType<typeof createActivities>;
