import type { HarvestDeps } from './activities.js';

export type Gate = { open: true } | { open: false; reason: string; stalled: boolean };

const STALL_AFTER_MS = 10 * 60_000;
const SENT_WINDOW_MS = 60 * 60_000;
export const STALLED_REASON = 'stalled: the vault is filling the disk';

function formatBytes(n: number): string {
  if (n < 1000) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n;
  let u = -1;
  while (v >= 1000 && u < units.length - 1) {
    v /= 1000;
    u++;
  }
  return `${v.toFixed(1)} ${units[u]}`;
}

/**
 * The throttle gate checked before every file copy (spec §7.5). A pure read: the workflow
 * writes `paused_reason` / `paused_since`; this reads them to measure the 10-minute stall.
 */
export async function checkGate(
  deps: Pick<HarvestDeps, 'pool' | 'settings' | 'sys'>,
  runId: string,
  fileSize: bigint,
  now: Date = new Date(),
): Promise<Gate> {
  const { pool, settings, sys } = deps;

  const { blocks, bavail, bsize } = await sys.statfs(settings.dataDir);
  if (blocks > 0n) {
    const used = blocks - bavail + (fileSize + bsize - 1n) / bsize;
    if (used * 100n > BigInt(settings.maxDiskPct) * blocks) {
      const pct = ((Number(used) / Number(blocks)) * 100).toFixed(1);
      if (await isStalled(deps, runId, now)) {
        return { open: false, reason: STALLED_REASON, stalled: true };
      }
      return { open: false, reason: `disk ${pct} %`, stalled: false };
    }
  }

  const { rows } = await pool.query<{ files: string; bytes: string; sent: string }>(
    `SELECT s.pending_files::text AS files, s.pending_bytes::text AS bytes,
            (SELECT count(*) FROM harvest_items
              WHERE state = 'SENT' AND file_id IS NULL AND sent_at > $1)::text AS sent
     FROM scan_stats s WHERE s.id = 1`,
    [new Date(now.getTime() - SENT_WINDOW_MS)],
  );
  const pendingFiles = Number(rows[0]?.files ?? 0);
  const pendingBytes = Number(rows[0]?.bytes ?? 0);
  const backlog = Math.max(pendingFiles, Number(rows[0]?.sent ?? 0));
  if (backlog >= settings.maxPendingFiles) {
    return { open: false, reason: `backlog ${backlog.toLocaleString('en-US')}`, stalled: false };
  }
  if (settings.maxPendingBytes !== null && pendingBytes >= settings.maxPendingBytes) {
    return { open: false, reason: `backlog ${formatBytes(pendingBytes)}`, stalled: false };
  }
  return { open: true };
}

/**
 * With the disk shut, waiting helps only if something in flight will free space. Nothing is
 * in flight (no pending drop-zone files, none of ours on their way, nothing awaiting claim
 * or COPY reclaim) for 10 minutes since the run was paused for the disk: stalled.
 */
async function isStalled(
  deps: Pick<HarvestDeps, 'pool'>,
  runId: string,
  now: Date,
): Promise<boolean> {
  const { rows } = await deps.pool.query<{ stalled: boolean }>(
    `SELECT (
        (SELECT pending_files FROM scan_stats WHERE id = 1) = 0
        AND NOT EXISTS (SELECT 1 FROM files WHERE state IN ('DISCOVERED','CLAIMED'))
        AND NOT EXISTS (SELECT 1 FROM files
                         WHERE state = 'COPY' AND copy_removed_at IS NULL AND NOT copy_mismatch)
        AND NOT EXISTS (SELECT 1 FROM harvest_items
                         WHERE state = 'SENT' AND file_id IS NULL AND sent_at > $2)
        AND (r.paused_reason LIKE 'disk%' OR r.paused_reason LIKE 'stalled%')
        AND r.paused_since <= $3
      ) AS stalled
     FROM harvest_runs r WHERE r.id = $1`,
    [runId, new Date(now.getTime() - SENT_WINDOW_MS), new Date(now.getTime() - STALL_AFTER_MS)],
  );
  return rows[0]?.stalled === true;
}
