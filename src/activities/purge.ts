// The worker's side of a purge (purge spec §5 step 3, §7): the durable log and the vault files.
import { heartbeat, log } from '@temporalio/activity';
import { reconcilePurgeLog } from '../purgeLog.js';
import type { SweepResult } from '../types.js';
import { tally } from './copies.js';
import { removePurged, type IdenticalCtx } from './identical.js';
import type { Deps } from './index.js';

export function makePurge(deps: Deps) {
  const { pool } = deps;

  /** Queue `io`. Brings `purged.csv` and the `purged` table level (appends what the file lacks). */
  async function syncPurgeLog(): Promise<{ appended: number }> {
    const { appended } = await reconcilePurgeLog(pool, deps.settings.dataDir, log);
    return { appended };
  }

  /**
   * Queue `io`, heartbeating. Removes the vault files of these purged contents (`removePurged`
   * each). A sha no longer pending — removed, flagged, or not PURGED any more (unblocked and
   * dropped again) — is skipped; DEFERRED ones are left to the copy sweep.
   */
  async function removePurgedBatch(shas: string[]): Promise<SweepResult> {
    const ctx: IdenticalCtx = { ...deps, beat: (d?: unknown) => heartbeat(d), log };
    const pending = (
      await pool.query<{ sha256: string }>(
        `SELECT sha256 FROM contents
          WHERE sha256 = ANY($1::text[]) AND state = 'PURGED'
            AND purge_removed_at IS NULL AND NOT purge_mismatch
          ORDER BY sha256`,
        [shas],
      )
    ).rows;
    const out: SweepResult = { examined: 0, removed: 0, mismatched: 0, deferred: 0, wrapped: true };
    for (const { sha256 } of pending) {
      tally(out, await removePurged(ctx, sha256));
      heartbeat(out);
    }
    if (out.examined > 0) log.info('purged files removed', { ...out });
    return out;
  }

  return { syncPurgeLog, removePurgedBatch };
}
