// Operator commands behind `cli retry-failed | verify-vault | doctor` (spec §7, §8).
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import type { Client } from '@temporalio/client';
import type pg from 'pg';
import type { DataLayout } from './fsops.js';
import { sha256File } from './fsops.js';

export interface RetryResult {
  reset: number;
  failed: { sha256: string; error: string }[];
}

/**
 * Resets every FAILED content to INGESTED and starts a `ContentWorkflow` for it. A row whose
 * workflow cannot be started goes back to FAILED (with its error), so a later run finds it again.
 */
export async function retryFailed(
  pool: pg.Pool,
  client: Client,
  now: () => number = Date.now,
): Promise<RetryResult> {
  const rows = (
    await pool.query<{ sha256: string; ext: string; last_error: string | null }>(
      `WITH failed AS (SELECT sha256, last_error FROM contents WHERE state = 'FAILED' FOR UPDATE)
       UPDATE contents c SET state = 'INGESTED', last_error = NULL
       FROM failed WHERE c.sha256 = failed.sha256
       RETURNING c.sha256, c.ext, failed.last_error`,
    )
  ).rows;
  const result: RetryResult = { reset: 0, failed: [] };
  for (const row of rows) {
    try {
      await client.workflow.start('ContentWorkflow', {
        taskQueue: 'main',
        workflowId: `content-${row.sha256}-retry-${String(now())}`,
        args: [row.sha256, row.ext],
      });
      result.reset += 1;
    } catch (e) {
      await pool.query(
        `UPDATE contents SET state = 'FAILED', last_error = $2
         WHERE sha256 = $1 AND state = 'INGESTED'`,
        [row.sha256, row.last_error],
      );
      result.failed.push({ sha256: row.sha256, error: e instanceof Error ? e.message : String(e) });
    }
  }
  return result;
}

export interface VaultReport {
  checked: number;
  skippedCopies: number;
  mismatches: { path: string; expected: string; actual: string }[];
}

async function listVault(layout: DataLayout): Promise<{ files: string[]; skippedCopies: number }> {
  const files: string[] = [];
  let skippedCopies = 0;
  const walk = async (dir: string): Promise<void> => {
    for (const e of await readdir(dir, { withFileTypes: true })) {
      const path = join(dir, e.name);
      if (path === layout.vaultCopies) {
        // Named by file id, not hash: nothing to verify here.
        skippedCopies += (await readdir(path, { recursive: true, withFileTypes: true })).filter(
          (f) => f.isFile(),
        ).length;
      } else if (e.isDirectory()) await walk(path);
      else if (e.isFile()) files.push(path);
    }
  };
  await walk(layout.vault);
  return { files, skippedCopies };
}

/** Re-hashes vault files (all, or a random `sample`) and reports those not named by their hash. Never repairs. */
export async function verifyVault(layout: DataLayout, sample?: number): Promise<VaultReport> {
  const { files, skippedCopies } = await listVault(layout);
  let chosen = files;
  if (sample !== undefined && sample < files.length) {
    chosen = [...files].sort(() => Math.random() - 0.5).slice(0, sample);
  }
  const mismatches: VaultReport['mismatches'] = [];
  for (const path of chosen) {
    const name = path.slice(path.lastIndexOf('/') + 1);
    const expected = name.split('.')[0]!;
    const actual = await sha256File(path);
    if (actual !== expected) mismatches.push({ path: layout.rel(path), expected, actual });
  }
  return { checked: chosen.length, skippedCopies, mismatches };
}

export interface DoctorReport {
  /** `work/claimed/<id>` files whose `files` row is already final: leftovers of a failed put-back. */
  orphanedClaimed: number;
  /** Files still DISCOVERED/CLAIMED after many ingest attempts. */
  filesStuckRetrying: number;
  /** Still-image contents sitting in INGESTED/HASHED with no update for a long time. */
  contentsStale: number;
}

export const STUCK_ATTEMPTS = 5;
export const STALE_MINUTES = 15;

/** Cheap consistency counts for the operator; read-only. */
export async function doctor(pool: pg.Pool, layout: DataLayout): Promise<DoctorReport> {
  const ids = (await readdir(layout.claimed)).filter((n) => /^\d+$/.test(n));
  const orphans = await pool.query<{ n: bigint }>(
    `SELECT count(*) AS n FROM files
     WHERE id = ANY($1::bigint[]) AND state IN ('INGESTED','COPY','QUARANTINED')`,
    [ids],
  );
  const stuck = await pool.query<{ n: bigint }>(
    `SELECT count(*) AS n FROM files WHERE state IN ('DISCOVERED','CLAIMED') AND attempts >= $1`,
    [STUCK_ATTEMPTS],
  );
  const stale = await pool.query<{ n: bigint }>(
    `SELECT count(*) AS n FROM contents
     WHERE kind = 'still' AND state IN ('INGESTED','HASHED')
       AND updated_at < now() - make_interval(mins => $1)`,
    [STALE_MINUTES],
  );
  return {
    orphanedClaimed: Number(orphans.rows[0]!.n),
    filesStuckRetrying: Number(stuck.rows[0]!.n),
    contentsStale: Number(stale.rows[0]!.n),
  };
}
