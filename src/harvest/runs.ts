import type pg from 'pg';
import { withTx } from '../db.js';

/**
 * Creates a queued run over the given folders: upserts `harvest_folders`, then the run and its
 * `harvest_run_folders` rows (all `queued`). Returns the run id.
 */
export async function createRun(
  pool: pg.Pool,
  folders: { sourceId: string; relPath: string }[],
  dryRun: boolean,
): Promise<string> {
  return withTx(pool, async (c) => {
    const ids: string[] = [];
    for (const f of folders) {
      const { rows } = await c.query<{ id: string }>(
        `INSERT INTO harvest_folders (source_id, rel_path) VALUES ($1, $2)
         ON CONFLICT (source_id, rel_path) DO UPDATE SET rel_path = EXCLUDED.rel_path
         RETURNING id::text AS id`,
        [f.sourceId, f.relPath],
      );
      ids.push(rows[0]!.id);
    }
    const run = await c.query<{ id: string }>(
      `INSERT INTO harvest_runs (dry_run, state) VALUES ($1, 'queued') RETURNING id::text AS id`,
      [dryRun],
    );
    const runId = run.rows[0]!.id;
    for (const [position, folderId] of ids.entries()) {
      await c.query(
        `INSERT INTO harvest_run_folders (run_id, folder_id, position, state)
         VALUES ($1, $2, $3, 'queued') ON CONFLICT DO NOTHING`,
        [runId, folderId, position],
      );
    }
    return runId;
  });
}

export interface RunFolder {
  folderId: string;
  sourceId: string;
  relPath: string;
  state: string;
}

/**
 * The run's folders in selection order.
 */
export async function runFolders(pool: pg.Pool, runId: string): Promise<RunFolder[]> {
  const { rows } = await pool.query<{
    folder_id: string;
    source_id: string;
    rel_path: string;
    state: string;
  }>(
    `SELECT rf.folder_id::text AS folder_id, f.source_id::text AS source_id, f.rel_path, rf.state
     FROM harvest_run_folders rf JOIN harvest_folders f ON f.id = rf.folder_id
     WHERE rf.run_id = $1 ORDER BY rf.position`,
    [runId],
  );
  return rows.map((r) => ({
    folderId: r.folder_id,
    sourceId: r.source_id,
    relPath: r.rel_path,
    state: r.state,
  }));
}

export type RunState =
  'queued' | 'scanning' | 'sending' | 'paused' | 'done' | 'cancelled' | 'failed';

/**
 * Reason class: the word before the first space; `stalled…` belongs to the disk class (a disk
 * pause that waited long enough to be called stalled is still the same pause).
 */
const REASON_CLASS = `CASE WHEN split_part(%s, ' ', 1) IN ('stalled', 'stalled:') THEN 'disk'
                           ELSE split_part(%s, ' ', 1) END`;
const reasonClass = (expr: string) => REASON_CLASS.replaceAll('%s', expr);

/**
 * Sets a run's state and pause reason. `paused_since` is kept while the new reason is in the
 * same class as the old one (`disk 80.4 %` → `disk 81.0 %` → `stalled: …`), set when the class
 * changes, and cleared with the reason when `reason` is null. A finished run is left alone.
 */
export async function setRunState(
  pool: pg.Pool,
  runId: string,
  state: RunState,
  reason: string | null = null,
): Promise<void> {
  await pool.query(
    `UPDATE harvest_runs SET
       state = $2,
       paused_reason = $3::text,
       paused_since = CASE
         WHEN $3::text IS NULL THEN NULL
         WHEN paused_reason IS NOT NULL AND paused_since IS NOT NULL
              AND ${reasonClass('paused_reason')} = ${reasonClass('$3::text')} THEN paused_since
         ELSE clock_timestamp() END,
       started_at = CASE WHEN started_at IS NULL AND $2 IN ('scanning', 'sending')
                         THEN clock_timestamp() ELSE started_at END
     WHERE id = $1 AND state NOT IN ('done', 'cancelled', 'failed')`,
    [runId, state, reason],
  );
}

/**
 * Ends a run (one transaction): its state, `finished_at` and error; for `done` (not a dry run)
 * also each folder's history — `last_harvested_at`, `last_run_id` and `last_counts` from the
 * run's counters. Returns false if the run had already finished (a retry changes nothing).
 */
export async function finishRunRows(
  pool: pg.Pool,
  runId: string,
  outcome: 'done' | 'cancelled' | 'failed',
  error: string | null = null,
): Promise<boolean> {
  return withTx(pool, async (c) => {
    const run = await c.query<{ dry_run: boolean }>(
      `UPDATE harvest_runs
       SET state = $2, finished_at = clock_timestamp(), error = $3,
           paused_reason = NULL, paused_since = NULL
       WHERE id = $1 AND state NOT IN ('done', 'cancelled', 'failed')
       RETURNING dry_run`,
      [runId, outcome, error],
    );
    const row = run.rows[0];
    if (!row) return false;
    if (outcome === 'done' && !row.dry_run) {
      await c.query(
        `UPDATE harvest_folders f
         SET last_harvested_at = clock_timestamp(), last_run_id = rf.run_id,
             last_counts = jsonb_build_object(
               'dirs', rf.dirs, 'photos', rf.photos, 'maybes', rf.maybes, 'junk', rf.junk,
               'errors', rf.errors, 'sent', rf.sent, 'known', rf.known, 'dups', rf.dups,
               'bytes_sent', rf.bytes_sent)
         FROM harvest_run_folders rf
         WHERE rf.run_id = $1 AND rf.folder_id = f.id`,
        [runId],
      );
    }
    return true;
  });
}
