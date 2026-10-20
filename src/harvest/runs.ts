import type pg from 'pg';
import { withTx } from '../db.js';
import type { RunControl } from './activities.js';
import { pathKey } from './paths.js';

/**
 * Creates a queued run over the given folders: upserts `harvest_folders`, then the run and its
 * `harvest_run_folders` rows (all `queued`). Returns the run id. `sendOnly` (review approvals):
 * nothing is scanned and only `items` (the approved ids, recorded in `harvest_run_items`) are
 * sent, while still PLANNED; the folders are their parent directories, for display and counters.
 */
export async function createRun(
  pool: pg.Pool,
  folders: { sourceId: string; relPath: string }[],
  options: boolean | { dryRun?: boolean; sendOnly?: boolean; items?: string[] },
): Promise<string> {
  const {
    dryRun = false,
    sendOnly = false,
    items = [],
  } = typeof options === 'boolean' ? { dryRun: options } : options;
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
      `INSERT INTO harvest_runs (dry_run, send_only, state) VALUES ($1, $2, 'queued')
       RETURNING id::text AS id`,
      [dryRun, sendOnly],
    );
    const runId = run.rows[0]!.id;
    for (const [position, folderId] of ids.entries()) {
      await c.query(
        `INSERT INTO harvest_run_folders (run_id, folder_id, position, state)
         VALUES ($1, $2, $3, 'queued') ON CONFLICT DO NOTHING`,
        [runId, folderId, position],
      );
    }
    if (sendOnly) await addRunItems(c, runId, items);
    return runId;
  });
}

/** Records approved items for a send-only run (F-1); a send-only run sends only these. */
export async function addRunItems(
  c: pg.Pool | pg.PoolClient,
  runId: string,
  items: string[],
): Promise<void> {
  if (items.length === 0) return;
  await c.query(
    `INSERT INTO harvest_run_items (run_id, item_id)
     SELECT $1, unnest($2::bigint[]) ON CONFLICT DO NOTHING`,
    [runId, items],
  );
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

export interface ScopeFolder {
  folderId: string;
  sourceId: string;
  /** pathKey of the folder's rel_path ('' = the whole source). */
  key: string;
}

/** A run's send scope: its folders in position order, and whether only its own items count. */
export interface SendScope {
  runId: string;
  sendOnly: boolean;
  folders: ScopeFolder[];
}

export async function loadScope(pool: pg.Pool, runId: string): Promise<SendScope> {
  const run = await pool.query<{ send_only: boolean }>(
    'SELECT send_only FROM harvest_runs WHERE id = $1',
    [runId],
  );
  const folders = await runFolders(pool, runId);
  return {
    runId,
    sendOnly: run.rows[0]?.send_only ?? false,
    folders: folders.map((f) => ({
      folderId: f.folderId,
      sourceId: f.sourceId,
      key: pathKey(f.relPath),
    })),
  };
}

/** True if `key` lies under the folder key `folder` ('' = everything). */
export const underKey = (key: string, folder: string): boolean =>
  folder === '' || key.startsWith(`${folder}/`);

/**
 * The run folder an item of `sourceId` / `key` is sent under: the deepest selected folder
 * holding it (the first by position among equal ones), or null when outside the run.
 */
export function folderOf(scope: SendScope, sourceId: string, key: string): number | null {
  let best: number | null = null;
  for (const [i, f] of scope.folders.entries()) {
    if (f.sourceId !== sourceId || !underKey(key, f.key)) continue;
    if (best === null || f.key.length > scope.folders[best]!.key.length) best = i;
  }
  return best;
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
 * Ends a run (one transaction): its state, `finished_at` and error; for `done` (unless a dry or
 * send-only run) also each folder's history — `last_harvested_at`, `last_run_id` and
 * `last_counts` from the run's counters — except for folders whose root the scan could not read
 * (`root_ok` false: not found, unreadable; F-2). Returns false if the run had already finished
 * (a retry changes nothing).
 */
export async function finishRunRows(
  pool: pg.Pool,
  runId: string,
  outcome: 'done' | 'cancelled' | 'failed',
  error: string | null = null,
): Promise<boolean> {
  return withTx(pool, async (c) => {
    const run = await c.query<{ dry_run: boolean; send_only: boolean }>(
      `UPDATE harvest_runs
       SET state = $2, finished_at = clock_timestamp(), error = $3,
           paused_reason = NULL, paused_since = NULL
       WHERE id = $1 AND state NOT IN ('done', 'cancelled', 'failed')
       RETURNING dry_run, send_only`,
      [runId, outcome, error],
    );
    const row = run.rows[0];
    if (!row) return false;
    if (outcome === 'done' && !row.dry_run && !row.send_only) {
      await c.query(
        `UPDATE harvest_folders f
         SET last_harvested_at = clock_timestamp(), last_run_id = rf.run_id,
             last_counts = jsonb_build_object(
               'dirs', rf.dirs, 'photos', rf.photos, 'maybes', rf.maybes, 'junk', rf.junk,
               'errors', rf.errors, 'sent', rf.sent, 'known', rf.known, 'dups', rf.dups,
               'bytes_sent', rf.bytes_sent)
         FROM harvest_run_folders rf
         WHERE rf.run_id = $1 AND rf.folder_id = f.id AND rf.root_ok`,
        [runId],
      );
    }
    return true;
  });
}

/** What the workflow needs to plan a run: the dry-run flag and its folders in order. */
export async function loadRun(
  pool: pg.Pool,
  runId: string,
): Promise<{ dryRun: boolean; sendOnly: boolean; folders: RunFolder[] }> {
  const { rows } = await pool.query<{ dry_run: boolean; send_only: boolean }>(
    'SELECT dry_run, send_only FROM harvest_runs WHERE id = $1',
    [runId],
  );
  if (!rows[0]) throw new Error(`harvest run ${runId} not found`);
  return {
    dryRun: rows[0].dry_run,
    sendOnly: rows[0].send_only,
    folders: await runFolders(pool, runId),
  };
}

/** The error a run gets when its workflow ended without finishing the run row. */
export const WORKFLOW_CLOSED = 'workflow closed unexpectedly';

/**
 * Starts the oldest queued run unless one is already active (scanning, sending or paused).
 * Returns the started run's id, or null. Callers are serialised by an advisory lock; a run the
 * workflow has not yet moved out of `queued` may be offered twice, which `control.start` absorbs
 * (workflow id `harvest-<runId>`, duplicates rejected and treated as success).
 *
 * A run whose workflow already ended (terminated at the history limit, say) can never finish
 * its own row: an active one whose workflow is not running, or a queued one whose start finds a
 * closed workflow, is marked failed ("workflow closed unexpectedly") and the queue moves on.
 */
export async function startNextQueued(pool: pg.Pool, control: RunControl): Promise<string | null> {
  for (;;) {
    const pick = await withTx(pool, async (c) => {
      await c.query("SELECT pg_advisory_xact_lock(hashtext('harvest_start_next'))");
      const active = await c.query<{ id: string }>(
        `SELECT id::text AS id FROM harvest_runs WHERE state IN ('scanning', 'sending', 'paused')
         ORDER BY id LIMIT 1`,
      );
      if (active.rows[0]) return { id: active.rows[0].id, active: true };
      const q = await c.query<{ id: string }>(
        "SELECT id::text AS id FROM harvest_runs WHERE state = 'queued' ORDER BY id LIMIT 1",
      );
      return q.rows[0] ? { id: q.rows[0].id, active: false } : null;
    });
    if (pick === null) return null;
    if (pick.active) {
      if ((await control.describe(pick.id)) === 'running') return null;
    } else {
      const started = await control.start(pick.id);
      if (started !== 'closed') return pick.id;
    }
    // finishRunRows changes nothing if the run finished meanwhile (its workflow just ended).
    await finishRunRows(pool, pick.id, 'failed', WORKFLOW_CLOSED);
  }
}
