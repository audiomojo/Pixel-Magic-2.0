import type { Dirent } from 'node:fs';
import { lstat, readdir, rmdir, stat, unlink } from 'node:fs/promises';
import { join, relative, sep } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { heartbeat, log } from '@temporalio/activity';
import {
  type Client,
  WorkflowExecutionAlreadyStartedError,
  WorkflowIdConflictPolicy,
  WorkflowIdReusePolicy,
} from '@temporalio/client';
import { isIgnored } from '../fsops.js';
import { fileWorkflowId } from '../ids.js';
import type { FileRef, ScanResult } from '../types.js';
import type { FileWorkflow } from '../workflows.js';
import type { Deps } from './index.js';

export const RUNNING_FILE_WORKFLOWS = "WorkflowType='FileWorkflow' AND ExecutionStatus='Running'";
const HEARTBEAT_MS = 1000;
/** Heartbeats run on a timer for the whole scan: one recursive readdir can outlast the timeout. */
export const HEARTBEAT_INTERVAL_MS = 5000;
const STAT_BATCH = 64;
/**
 * An empty drop-zone folder is removed only once unchanged this long: a Finder copy creates a
 * folder before its files, and an app may be about to save into it.
 */
export const EMPTY_DIR_GRACE_S = 60;
/** Finder litter that alone does not keep a folder: `.DS_Store` and AppleDouble `._*` files. */
const isLitter = (name: string): boolean => name === '.DS_Store' || name.startsWith('._');

/** rel_path → (size, mtime_ns) of every non-ignored regular file under the drop zone. */
type Snapshot = Map<string, { size: bigint; mtimeNs: bigint }>;

const isEnoent = (e: unknown): boolean => (e as NodeJS.ErrnoException).code === 'ENOENT';
const isNotEmpty = (e: unknown): boolean =>
  ['ENOTEMPTY', 'EEXIST'].includes((e as NodeJS.ErrnoException).code ?? '');
/** The path is gone: removed, or a parent directory was replaced by a file. */
const isGone = (e: unknown): boolean =>
  isEnoent(e) || (e as NodeJS.ErrnoException).code === 'ENOTDIR';

async function statOrNull(path: string) {
  try {
    return await stat(path, { bigint: true });
  } catch (e) {
    if (isGone(e)) return null;
    throw e;
  }
}

export function makeScan(deps: Deps) {
  const { pool, layout, settings } = deps;

  /** The drop zone's files, plus how many symlinks it skipped (they are never followed). */
  async function snapshot(): Promise<{ files: Snapshot; symlinks: number }> {
    const out: Snapshot = new Map();
    let entries: Dirent[];
    try {
      entries = await readdir(layout.dropzone, { recursive: true, withFileTypes: true });
    } catch (e) {
      if (isEnoent(e)) return { files: out, symlinks: 0 };
      throw e;
    }
    const symlinks = entries.filter((d) => d.isSymbolicLink()).length;
    const files = entries.filter((d) => d.isFile() && !isIgnored(d.name));
    // Bounded parallel stats: a bind-mounted drop zone answers each one slowly.
    for (let i = 0; i < files.length; i += STAT_BATCH) {
      const batch = files.slice(i, i + STAT_BATCH).map((d) => join(d.parentPath, d.name));
      const stats = await Promise.all(batch.map((abs) => statOrNull(abs)));
      batch.forEach((abs, j) => {
        const st = stats[j]; // null: claimed between readdir and stat
        if (!st) return;
        const rel = relative(layout.dropzone, abs).split(sep).join('/');
        out.set(rel, { size: st.size, mtimeNs: st.mtimeNs });
      });
    }
    return { files: out, symlinks };
  }

  /** Waits `stableSeconds`, heartbeating so a long wait never looks like a dead worker. */
  async function settle(): Promise<void> {
    const until = Date.now() + settings.stableSeconds * 1000;
    for (;;) {
      heartbeat('waiting for stable files');
      const left = until - Date.now();
      if (left <= 0) return;
      await sleep(Math.min(left, HEARTBEAT_MS));
    }
  }

  /** The file still is what both snapshots saw (an earlier FileWorkflow may have claimed it since). */
  async function unchanged(rel: string, size: bigint, mtimeNs: bigint): Promise<boolean> {
    const st = await statOrNull(join(layout.dropzone, rel));
    return st !== null && st.isFile() && st.size === size && st.mtimeNs === mtimeNs;
  }

  /**
   * Removes drop-zone folders (never the drop zone itself) that hold nothing but Finder litter
   * and have been unchanged for `EMPTY_DIR_GRACE_S`, deepest first so emptied parents go too.
   * Only litter files are ever unlinked; any other entry (a file, even an ignored partial
   * download, a link, a folder) keeps the folder. `rmdir` refuses a folder that filled up
   * meanwhile, and a put-back recreates any folder it needs.
   */
  async function pruneEmptyDirs(): Promise<number> {
    let dirs: Dirent[];
    try {
      dirs = (await readdir(layout.dropzone, { recursive: true, withFileTypes: true })).filter(
        (d) => d.isDirectory(),
      );
    } catch (e) {
      if (isEnoent(e)) return 0;
      throw e;
    }
    const paths = dirs
      .map((d) => join(d.parentPath, d.name))
      .sort((a, b) => b.split(sep).length - a.split(sep).length);
    const cutoff = Date.now() - EMPTY_DIR_GRACE_S * 1000;
    // Ages are taken before anything is removed: removing a child touches its parent's mtime.
    const old = new Set<string>();
    for (const dir of paths) {
      const st = await lstat(dir).catch((e: unknown) => {
        if (isGone(e)) return null;
        throw e;
      });
      if (st?.isDirectory() && st.mtimeMs <= cutoff) old.add(dir);
    }
    let removed = 0;
    for (const dir of paths) {
      if (!old.has(dir)) continue;
      try {
        const entries = await readdir(dir, { withFileTypes: true });
        if (!entries.every((e) => e.isFile() && isLitter(e.name))) continue;
        for (const e of entries)
          await unlink(join(dir, e.name)).catch((err: unknown) => {
            if (!isEnoent(err)) throw err;
          });
        await rmdir(dir);
        removed++;
      } catch (e) {
        if (isGone(e) || isNotEmpty(e)) continue; // gone already, or something arrived: keep it
        throw e;
      }
    }
    if (removed > 0) log.info(`removed ${String(removed)} empty drop-zone folders`);
    return removed;
  }

  async function scan(temporal: Client): Promise<ScanResult> {
    const { files: first } = await snapshot();
    await settle();
    const { files: second, symlinks } = await snapshot();
    heartbeat('snapshots taken');
    if (symlinks > 0)
      log.warn(
        `drop zone holds ${String(symlinks)} symlinks, which are never ingested: drop real files, not links or aliases`,
      );

    let pendingBytes = 0n;
    for (const f of second.values()) pendingBytes += f.size;
    await pool.query(
      `UPDATE scan_stats SET pending_files = $1, pending_bytes = $2, scanned_at = now() WHERE id = 1`,
      [second.size, pendingBytes],
    );

    const { count: running } = await temporal.workflow.count(RUNNING_FILE_WORKFLOWS);
    let budget = Math.max(0, settings.maxInFlight - running);
    let started = 0;
    const stable = [...second.entries()]
      .filter(([rel, f]) => {
        const a = first.get(rel);
        return a !== undefined && a.size === f.size && a.mtimeNs === f.mtimeNs;
      })
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

    for (const [rel, f] of stable) {
      if (budget <= 0) break;
      // Without this re-check, a workflow that claimed and finished this very file after the
      // snapshots would be started again under ALLOW_DUPLICATE and report the file vanished.
      if (!(await unchanged(rel, f.size, f.mtimeNs))) continue;
      const ref: FileRef = { relPath: rel, size: String(f.size), mtimeNs: String(f.mtimeNs) };
      try {
        await temporal.workflow.start<typeof FileWorkflow>('FileWorkflow', {
          taskQueue: 'main',
          workflowId: fileWorkflowId(ref),
          args: [ref, { poisonAttempts: settings.poisonAttempts }],
          workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
          workflowIdConflictPolicy: WorkflowIdConflictPolicy.FAIL,
        });
        started++;
        budget--;
      } catch (e) {
        if (!(e instanceof WorkflowExecutionAlreadyStartedError)) throw e;
        // Still running from an earlier scan: nothing to do (and it already counts as running).
      }
      heartbeat(`started ${String(started)}`);
    }
    await pruneEmptyDirs();
    return { started, pendingFiles: second.size, pendingBytes: String(pendingBytes) };
  }

  /**
   * Queue `io`. Two snapshots `stableSeconds` apart; a file whose size and mtime held still is
   * stable and gets a FileWorkflow, up to `maxInFlight` running ones (spec §4.3).
   */
  return async function scanAndStart(): Promise<ScanResult> {
    const temporal = deps.temporal;
    if (!temporal) throw new Error('scanAndStart needs a Temporal client');
    const beat = setInterval(() => heartbeat('scanning'), HEARTBEAT_INTERVAL_MS);
    try {
      return await scan(temporal);
    } finally {
      clearInterval(beat);
    }
  };
}
