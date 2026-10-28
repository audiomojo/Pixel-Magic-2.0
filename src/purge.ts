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
 * The root copy groups of `shown` (following `merged_into` up) and every group merged into one
 * of them (following it down): "the shown stills' copy groups". A SQL prefix for `WITH RECURSIVE`.
 */
const SHOWN_GROUPS = `up(id, parent) AS (
       SELECT g.id, g.merged_into FROM groups g
        WHERE g.id IN (SELECT copy_group_id FROM contents WHERE sha256 = ANY($1::text[]))
        UNION
       SELECT x.id, x.merged_into FROM groups x JOIN up ON x.id = up.parent),
     g(id) AS (
       SELECT id FROM up WHERE parent IS NULL
        UNION
       SELECT x.id FROM groups x JOIN g ON x.merged_into = g.id)`;

/**
 * The closure's rows (R6): stills of the copy groups of the shown stills (their hidden lesser
 * copies, groups merged into them included) in `stillStates`, plus `kind = 'motion'` contents
 * sharing a Live Photo id with one of those stills, in `motionStates`. A copy group of the set
 * the Picks screen never showed (an undescribed candidate, a group linked after staging) is not
 * in it.
 */
async function closureRows(
  c: Db,
  shown: string[],
  stillStates: string[],
  motionStates: string[],
): Promise<{ sha256: string; kind: string; size_bytes: bigint }[]> {
  const r = await c.query<{ sha256: string; kind: string; size_bytes: bigint }>(
    `WITH RECURSIVE ${SHOWN_GROUPS},
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
    [shown, stillStates, motionStates],
  );
  return r.rows;
}

/**
 * True when every `shown` sha is a HASHED/CLUSTERED still whose root copy group belongs to the
 * set now. False means the set changed since staging recorded the candidates (a set merge, a
 * group moved, a content quarantined), so a purge or preview would not be what was shown.
 */
async function shownInSet(c: Db, setId: string, shown: string[]): Promise<boolean> {
  const r = await c.query<{ n: number }>(
    `WITH RECURSIVE up(sha256, id, parent) AS (
       SELECT c.sha256, g.id, g.merged_into FROM contents c JOIN groups g ON g.id = c.copy_group_id
        WHERE c.sha256 = ANY($1::text[]) AND c.kind <> 'motion'
          AND c.state IN ('HASHED', 'CLUSTERED')
        UNION
       SELECT up.sha256, x.id, x.merged_into FROM groups x JOIN up ON x.id = up.parent)
     SELECT count(DISTINCT up.sha256)::int AS n FROM up JOIN groups r ON r.id = up.id
      WHERE up.parent IS NULL AND r.similar_set_id = $2`,
    [shown, setId],
  );
  return r.rows[0]!.n === new Set(shown).size;
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

/**
 * The set's purge closure, from the current database: the copy groups of `shown` (which the
 * caller has checked against `currentShown`). `'stale'` when a shown sha is no longer a live
 * still in a copy group of the set.
 */
export async function purgeClosure(
  c: Db,
  setId: string,
  shown: string[],
): Promise<PurgeSummary | 'stale'> {
  if (shown.length === 0 || !(await shownInSet(c, setId, shown))) return 'stale';
  return summarize(await closureRows(c, shown, LIVE_STATES, LIVE_MOTION_STATES), shown);
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
    // A double-click's two requests: the second waits, then finds the set purged. The idempotent
    // answer is read before pm.cluster: it only reads PURGED rows, which nothing but a purge
    // (serialized here) creates, so a repeat need not wait behind a cluster-cache reload.
    await c.query(`SELECT pg_advisory_xact_lock(hashtext('pm.purge-set:' || $1))`, [setId]);
    const states = await c.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM contents WHERE sha256 = ANY($1::text[]) AND state = 'PURGED'`,
      [want],
    );
    if (states.rows[0]!.n === want.length)
      return summarize(await closureRows(c, want, ['PURGED'], ['PURGED']), want);
    // Lock order purge-set → pm.cluster → rows (dedupe: suspect key → pm.cluster → rows;
    // clusterContent: pm.cluster → rows), so none of them deadlock. Holding pm.cluster keeps a
    // concurrent cluster step from committing a cache that still holds a purged row, and the
    // groups and candidates from changing under the stale check and the closure (R6, as
    // `retireFamily`): both are read after it is taken, so what is purged is what was shown.
    await c.query(`SELECT pg_advisory_xact_lock(hashtext('pm.cluster'))`);
    if (!sameList(await currentShown(c, setId), want)) return 'stale';
    // The candidates are staging's snapshot; the groups are now. Every shown sha must still be a
    // live still in a group of the set, or the closure would differ from what was shown.
    if (!(await shownInSet(c, setId, want))) return 'stale';
    const rows = await closureRows(c, want, LIVE_STATES, LIVE_MOTION_STATES);
    const shas = rows.map((r) => r.sha256);
    await c.query(
      'SELECT 1 FROM contents WHERE sha256 = ANY($1::text[]) ORDER BY sha256 FOR UPDATE',
      [shas],
    );
    // Under the row locks: the closure as it is now (a concurrent step may have moved a row).
    const locked = await closureRows(c, want, LIVE_STATES, LIVE_MOTION_STATES);
    const lockedShas = locked.map((r) => r.sha256);
    if (!sameList(lockedShas, shas)) return 'stale';
    const upd = await c.query<{ was: string }>(
      `UPDATE contents n SET state = 'PURGED', purged_at = now()
         FROM contents o WHERE o.sha256 = n.sha256 AND n.sha256 = ANY($1::text[])
       RETURNING o.state AS was`,
      [shas],
    );
    // A purged CLUSTERED row must leave every worker's in-memory cluster index.
    if (upd.rows.some((r) => r.was === 'CLUSTERED'))
      await c.query('UPDATE cluster_state SET version = version + 1 WHERE id = 1');
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
 * Unblocks these shas (spec §9): `unblocked_at = now()` where currently blocked. The harvest
 * ledger items of every sha that is unblocked now (not of other shas passed) go back to PLANNED
 * so the next run sends them: KNOWN/BLOCKED ones, and SENT ones sent before the (still `PURGED`)
 * content was purged — their copy was purged, and a SENT item would otherwise stay SENT through
 * rescans and make every other copy settle as its DUP. One re-sent since is left alone. A re-planned SENT item
 * forgets its drop-zone copy and link (`sendItem` would otherwise take the old copy's `files`
 * row as already published). Returns the number of `purged` rows unblocked.
 */
export async function unblock(pool: pg.Pool, shas: string[]): Promise<number> {
  return withTx(pool, async (c) => {
    const r = await c.query(
      `UPDATE purged SET unblocked_at = now()
        WHERE sha256 = ANY($1::text[]) AND unblocked_at IS NULL`,
      [shas],
    );
    const open = (
      await c.query<{ sha256: string }>(
        `SELECT sha256 FROM purged WHERE sha256 = ANY($1::text[]) AND unblocked_at IS NOT NULL`,
        [shas],
      )
    ).rows.map((x) => x.sha256);
    if (open.length === 0) return r.rowCount ?? 0;
    await c.query(
      `UPDATE harvest_items SET state = 'PLANNED'
        WHERE sha256 = ANY($1::text[]) AND state IN ('KNOWN', 'BLOCKED')`,
      [open],
    );
    await c.query(
      `UPDATE harvest_items i
          SET state = 'PLANNED', file_id = NULL, dropzone_path = NULL, dropzone_mtime_ns = NULL,
              sent_at = NULL, attempts = 0, last_error = NULL
        WHERE i.state = 'SENT'
          AND EXISTS (SELECT 1 FROM contents c
                       WHERE c.sha256 = COALESCE(i.sha256,
                               (SELECT f.sha256 FROM files f WHERE f.id = i.file_id))
                         AND c.sha256 = ANY($1::text[]) AND c.state = 'PURGED'
                         AND (i.sent_at IS NULL OR i.sent_at <= c.purged_at))`,
      [open],
    );
    return r.rowCount ?? 0;
  });
}
