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
