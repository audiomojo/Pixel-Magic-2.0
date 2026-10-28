// Purging a similar set (purge spec §3, §5, §9): the closure, the purge transaction and unblock.
// Database only: the harvester's Picks API calls these; the worker owns `purged.csv` and the
// vault (PurgeSyncWorkflow).
import type pg from 'pg';
import { withTx } from './db.js';

export interface PurgeSummary {
  /** Closure stills the Picks screen showed. */
  shown: number;
  /** The other closure stills (hidden lesser copies). */
  copies: number;
  /** Live Photo videos of closure stills. */
  live: number;
  bytes: bigint;
  /** Every closure sha256, ascending. */
  shas: string[];
}

/**
 * A `similar_candidates sc` row the Picks screen shows: described (a non-failed
 * `photo_features` row) and its content neither purged nor retired. `next` and `currentShown`
 * must agree on this, or a purge would be refused (or would purge something not shown).
 */
export const SHOWN_CANDIDATE = `EXISTS (SELECT 1 FROM photo_features pf
                WHERE pf.sha256 = sc.sha256 AND pf.failed IS NULL)
     AND NOT EXISTS (SELECT 1 FROM contents sx
                      WHERE sx.sha256 = sc.sha256 AND sx.state IN ('PURGED', 'DUPLICATE'))`;

type Db = pg.Pool | pg.PoolClient;

/** The set's candidates as the Picks screen shows them, ascending. */
export async function currentShown(c: Db, setId: string): Promise<string[]> {
  const r = await c.query<{ sha256: string }>(
    `SELECT sc.sha256 FROM similar_candidates sc
      WHERE sc.set_id = $1 AND ${SHOWN_CANDIDATE}
      ORDER BY sc.sha256`,
    [setId],
  );
  return r.rows.map((x) => x.sha256);
}

/**
 * The closure's rows: stills of every copy group of the set (and of groups merged into one,
 * transitively) in `stillStates`, plus `kind = 'motion'` contents sharing a Live Photo id with
 * one of those stills, in `motionStates`.
 */
async function closureRows(
  c: Db,
  setId: string,
  stillStates: string[],
  motionStates: string[],
): Promise<{ sha256: string; kind: string; size_bytes: bigint }[]> {
  const r = await c.query<{ sha256: string; kind: string; size_bytes: bigint }>(
    `WITH RECURSIVE g(id) AS (
       SELECT id FROM groups WHERE similar_set_id = $1
        UNION
       SELECT x.id FROM groups x JOIN g ON x.merged_into = g.id),
     stills AS (
       SELECT c.sha256, c.kind, c.size_bytes, c.live_content_id FROM contents c
        WHERE c.copy_group_id IN (SELECT id FROM g) AND c.kind <> 'motion'
          AND c.state = ANY($2::text[]))
     SELECT sha256, kind, size_bytes FROM stills
      UNION
     SELECT m.sha256, m.kind, m.size_bytes FROM contents m
      WHERE m.kind = 'motion' AND m.state = ANY($3::text[])
        AND m.live_content_id IN (SELECT live_content_id FROM stills
                                   WHERE live_content_id IS NOT NULL)
      ORDER BY sha256`,
    [setId, stillStates, motionStates],
  );
  return r.rows;
}

function summarize(
  rows: { sha256: string; kind: string; size_bytes: bigint }[],
  shown: string[],
): PurgeSummary {
  const seen = new Set(shown);
  const out: PurgeSummary = { shown: 0, copies: 0, live: 0, bytes: 0n, shas: [] };
  for (const r of rows) {
    if (r.kind === 'motion') out.live++;
    else if (seen.has(r.sha256)) out.shown++;
    else out.copies++;
    out.bytes += r.size_bytes;
    out.shas.push(r.sha256);
  }
  return out;
}

const LIVE_STATES = ['HASHED', 'CLUSTERED'];
/** Any motion not already purged (motions never go past INGESTED; DUPLICATE is stills only). */
const LIVE_MOTION_STATES = ['INGESTED', 'HASHED', 'CLUSTERED', 'QUARANTINED', 'FAILED'];

/** The set's purge closure, from the current database (never from client input). */
export async function purgeClosure(c: Db, setId: string, shown: string[]): Promise<PurgeSummary> {
  return summarize(await closureRows(c, setId, LIVE_STATES, LIVE_MOTION_STATES), shown);
}

const sameList = (a: string[], b: string[]): boolean =>
  a.length === b.length && a.every((x, i) => x === b[i]);

/**
 * Purges the set in one transaction (spec §5 step 2). Idempotent: when every `shown` sha is
 * already PURGED the stored summary is returned and nothing is written. `'stale'` when the set's
 * shown candidates are no longer exactly `shown` (or `shown` is empty); nothing is changed then.
 */
export async function purgeSet(
  pool: pg.Pool,
  setId: string,
  shown: string[],
): Promise<PurgeSummary | 'stale'> {
  const want = [...new Set(shown)].sort();
  if (want.length === 0) return 'stale';
  return withTx(pool, async (c) => {
    // A double-click's two requests: the second waits, then finds the set purged.
    await c.query(`SELECT pg_advisory_xact_lock(hashtext('pm.purge-set:' || $1))`, [setId]);
    const states = await c.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM contents WHERE sha256 = ANY($1::text[]) AND state = 'PURGED'`,
      [want],
    );
    if (states.rows[0]!.n === want.length)
      return summarize(await closureRows(c, setId, ['PURGED'], ['PURGED']), want);
    if (!sameList(await currentShown(c, setId), want)) return 'stale';

    const rows = await closureRows(c, setId, LIVE_STATES, LIVE_MOTION_STATES);
    const shas = rows.map((r) => r.sha256);
    await c.query(
      'SELECT 1 FROM contents WHERE sha256 = ANY($1::text[]) ORDER BY sha256 FOR UPDATE',
      [shas],
    );
    // Under the row locks: the closure as it is now (a concurrent step may have moved a row).
    const locked = await closureRows(c, setId, LIVE_STATES, LIVE_MOTION_STATES);
    const lockedShas = locked.map((r) => r.sha256);
    if (!sameList(lockedShas, shas)) return 'stale';
    await c.query(
      `UPDATE contents SET state = 'PURGED', purged_at = now() WHERE sha256 = ANY($1::text[])`,
      [shas],
    );
    await c.query(
      `INSERT INTO purged (sha256, size_bytes, names, purged_at, unblocked_at, set_id)
       SELECT c.sha256, c.size_bytes,
              ARRAY(SELECT DISTINCT f.original_name FROM files f
                     WHERE f.sha256 = c.sha256 ORDER BY 1),
              now(), NULL, $2
         FROM contents c WHERE c.sha256 = ANY($1::text[])
       ON CONFLICT (sha256) DO UPDATE
         SET size_bytes = EXCLUDED.size_bytes, names = EXCLUDED.names,
             purged_at = EXCLUDED.purged_at, unblocked_at = NULL, set_id = EXCLUDED.set_id`,
      [shas, setId],
    );
    return summarize(rows, want);
  });
}

/**
 * Unblocks these shas (spec §9): `unblocked_at = now()` where currently blocked, and their
 * harvest ledger items in KNOWN/BLOCKED go back to PLANNED so the next run sends them. Returns
 * the number of `purged` rows unblocked.
 */
export async function unblock(pool: pg.Pool, shas: string[]): Promise<number> {
  return withTx(pool, async (c) => {
    const r = await c.query(
      `UPDATE purged SET unblocked_at = now()
        WHERE sha256 = ANY($1::text[]) AND unblocked_at IS NULL`,
      [shas],
    );
    await c.query(
      `UPDATE harvest_items SET state = 'PLANNED'
        WHERE sha256 = ANY($1::text[]) AND state IN ('KNOWN', 'BLOCKED')`,
      [shas],
    );
    return r.rowCount ?? 0;
  });
}
