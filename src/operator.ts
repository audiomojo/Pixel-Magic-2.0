// Operator commands behind `cli retry-failed | verify-vault | doctor` (spec §7, §8).
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import type { Client } from '@temporalio/client';
import type pg from 'pg';
import type { DataLayout } from './fsops.js';
import { errText, sha256File, shaOfVaultName } from './fsops.js';
import type { ContentWorkflow } from './workflows.js';

export interface RetryResult {
  started: number;
  failed: { sha256: string; error: string }[];
}

/**
 * Starts a `ContentWorkflow` for every FAILED content; the workflow itself reopens the row
 * (`reopenFailed`) before decoding. This command changes no row, so a crash part-way leaves
 * nothing half-done and a rerun is always safe (a row still FAILED is simply started again).
 */
export async function retryFailed(
  pool: pg.Pool,
  client: Client,
  opts: { poisonAttempts: number },
  now: () => number = Date.now,
): Promise<RetryResult> {
  const rows = (
    await pool.query<{ sha256: string; ext: string }>(
      `SELECT sha256, ext FROM contents WHERE state = 'FAILED' ORDER BY sha256`,
    )
  ).rows;
  const result: RetryResult = { started: 0, failed: [] };
  for (const row of rows) {
    try {
      await client.workflow.start<typeof ContentWorkflow>('ContentWorkflow', {
        taskQueue: 'main',
        workflowId: `content-${row.sha256}-retry-${String(now())}`,
        args: [row.sha256, row.ext, { poisonAttempts: opts.poisonAttempts }],
      });
      result.started += 1;
    } catch (e) {
      result.failed.push({ sha256: row.sha256, error: errText(e) });
    }
  }
  return result;
}

export interface VaultReport {
  checked: number;
  /** `.copies` files still present (named by file id: not verified here). */
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
    const expected = shaOfVaultName(name);
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
  /** COPY files not removed because a re-hash disagreed or a file was missing (spec §6.5). */
  copyMismatches: number;
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
  const mismatches = await pool.query<{ n: bigint }>(
    `SELECT count(*) AS n FROM files WHERE copy_mismatch`,
  );
  return {
    orphanedClaimed: Number(orphans.rows[0]!.n),
    filesStuckRetrying: Number(stuck.rows[0]!.n),
    contentsStale: Number(stale.rows[0]!.n),
    copyMismatches: Number(mismatches.rows[0]!.n),
  };
}

/** COPY rows whose duplicate `.copies` file was removed (spec §6.5). */
export async function removedCopies(pool: pg.Pool): Promise<number> {
  const r = await pool.query<{ n: bigint }>(
    `SELECT count(*) AS n FROM files WHERE copy_removed_at IS NOT NULL`,
  );
  return Number(r.rows[0]!.n);
}

export interface CopyListing {
  relPath: string;
  mtime: Date;
  droppedAt: Date;
  sha256: string;
  status: 'removed' | 'mismatch' | 'present';
}

/** Every duplicate drop (COPY row), by drop-zone path: what was dropped twice, and when. */
export async function listCopies(pool: pg.Pool): Promise<CopyListing[]> {
  const r = await pool.query<{
    rel_path: string;
    mtime_ns: bigint;
    discovered_at: Date;
    sha256: string;
    copy_removed_at: Date | null;
    copy_mismatch: boolean;
  }>(
    `SELECT rel_path, mtime_ns, discovered_at, sha256, copy_removed_at, copy_mismatch
       FROM files WHERE state = 'COPY' ORDER BY rel_path COLLATE "C", id`,
  );
  return r.rows.map((x) => ({
    relPath: x.rel_path,
    mtime: new Date(Number(x.mtime_ns / 1_000_000n)),
    droppedAt: x.discovered_at,
    sha256: x.sha256,
    status: x.copy_removed_at !== null ? 'removed' : x.copy_mismatch ? 'mismatch' : 'present',
  }));
}
