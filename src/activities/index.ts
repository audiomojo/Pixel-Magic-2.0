import type { Client } from '@temporalio/client';
import type pg from 'pg';
import type { Settings } from '../config.js';
import type { DataLayout } from '../fsops.js';
import type { MetadataReader } from '../metadata.js';
import { makeIngestFile } from './ingest.js';

export interface Deps {
  pool: pg.Pool;
  settings: Settings;
  layout: DataLayout;
  metadata: MetadataReader;
  temporal?: Client;
  /** Test seam: awaited at named crash points inside activities. */
  checkpoint?: (name: string) => void | Promise<void>;
}

export function createActivities(deps: Deps) {
  return {
    ingestFile: makeIngestFile(deps),
  };
}

export type Activities = ReturnType<typeof createActivities>;
