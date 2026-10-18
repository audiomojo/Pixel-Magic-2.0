import {
  link,
  lstat,
  mkdir,
  readdir,
  readlink,
  rename,
  rmdir,
  symlink,
  unlink,
} from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { dirname, join, relative, sep } from 'node:path';
import { heartbeat, log } from '@temporalio/activity';
import type pg from 'pg';
import { fsyncDir, vaultRel } from '../fsops.js';
import { planStaging, type Entry, type StageContent, type StageFile } from '../stagingPlan.js';
import type { BuildResult } from '../types.js';
import type { Deps } from './index.js';

const GEN = /^\.gen-(\d+)$/;
/** `removeGen`'s private directory inside a generation directory being removed. */
const REAP = /^\.pm-reap-[0-9a-f-]{36}$/;
const HEARTBEAT_EVERY = 1000;
const LOCK_POLL_MS = 250;

/** A planned source is missing: the vault disagrees with the database. Retryable, never hidden. */
export class MissingSourceError extends Error {
  override name = 'MissingSourceError';
}

interface Snapshot {
  /** `greatest(max(files.updated_at), max(contents.updated_at))` as text (full µs precision). */
  watermark: string;
  version: bigint;
  /** `pg_current_snapshot()` of the read transaction: which commits these facts include. */
  snapshot: string;
  changed: boolean;
  lastGen: number | null;
  sinceLastS: number | null;
}

const isEnoent = (e: unknown): boolean => (e as NodeJS.ErrnoException).code === 'ENOENT';
const isNotEmpty = (e: unknown): boolean => (e as NodeJS.ErrnoException).code === 'ENOTEMPTY';

/** `path`, or `path.1`, `path.2`, … : the first name nothing occupies (a rescue never overwrites). */
async function freeName(path: string): Promise<string> {
  for (let i = 0; ; i++) {
    const p = i === 0 ? path : `${path}.${String(i)}`;
    try {
      await lstat(p);
    } catch (e) {
      if (isEnoent(e)) return p;
      throw e;
    }
  }
}

export function makeStaging(deps: Deps) {
  const { pool, layout, settings } = deps;
  const genPath = (n: number) => join(layout.staging, `.gen-${String(n)}`);
  const currentPath = join(layout.staging, 'current');
  const tmpLink = join(layout.staging, '.current.tmp');

  async function snapshot(c: pg.PoolClient): Promise<Snapshot> {
    const r = await c.query<{
      watermark: string;
      version: bigint;
      snapshot: string;
      changed: boolean;
      last_gen: bigint | null;
      since_s: number | null;
    }>(
      `WITH w AS (
         SELECT coalesce(greatest((SELECT max(updated_at) FROM files),
                                  (SELECT max(updated_at) FROM contents)),
                         'epoch'::timestamptz) AS watermark,
                (SELECT version FROM cluster_state WHERE id = 1) AS version,
                pg_current_snapshot() AS snapshot),
       b AS (SELECT * FROM staging_builds ORDER BY gen DESC LIMIT 1)
       SELECT w.watermark::text AS watermark, w.version, w.snapshot::text AS snapshot,
              (b.gen IS NULL OR b.snapshot IS NULL OR w.version <> b.cluster_version
                 OR w.watermark > b.watermark
                 OR EXISTS (SELECT 1 FROM files
                             WHERE NOT pg_visible_in_snapshot(xmin::text::xid8, b.snapshot))
                 OR EXISTS (SELECT 1 FROM contents
                             WHERE NOT pg_visible_in_snapshot(xmin::text::xid8, b.snapshot))
              ) AS changed,
              b.gen AS last_gen,
              extract(epoch FROM clock_timestamp() - b.built_at)::float8 AS since_s
         FROM w LEFT JOIN b ON true`,
    );
    const row = r.rows[0]!;
    return {
      watermark: row.watermark,
      version: row.version,
      snapshot: row.snapshot,
      changed: row.changed,
      lastGen: row.last_gen === null ? null : Number(row.last_gen),
      sinceLastS: row.since_s,
    };
  }

  async function runningFileWorkflows(): Promise<number> {
    if (!deps.temporal) return 0;
    const r = await deps.temporal.workflow.count(
      `WorkflowType = 'FileWorkflow' AND ExecutionStatus = 'Running'`,
    );
    return r.count;
  }

  async function loadFacts(
    c: pg.PoolClient,
  ): Promise<{ contents: StageContent[]; files: StageFile[] }> {
    const cs = await c.query<{
      sha256: string;
      kind: string;
      ext: string;
      mime: string | null;
      width: number | null;
      height: number | null;
      size_bytes: bigint;
      captured_at: Date | null;
      live_content_id: string | null;
      duration_s: number | null;
      state: string;
      quarantine_reason: string | null;
      copy_group_id: bigint | null;
      similar_set_id: bigint | null;
      copy_ambiguous: boolean;
    }>(
      `SELECT c.sha256, c.kind, c.ext, c.mime, c.width, c.height, c.size_bytes, c.captured_at,
              c.live_content_id, c.duration_s, c.state, c.quarantine_reason, c.copy_group_id,
              g.similar_set_id, c.copy_ambiguous
         FROM contents c LEFT JOIN groups g ON g.id = c.copy_group_id`,
    );
    const fs = await c.query<{
      id: bigint;
      rel_path: string;
      original_name: string;
      mtime_ns: bigint;
      sha256: string;
      state: string;
      ext: string;
    }>(
      `SELECT id, rel_path, original_name, mtime_ns, sha256, state, ext FROM files
        WHERE state IN ('INGESTED', 'COPY') AND sha256 IS NOT NULL AND ext IS NOT NULL`,
    );
    const num = (v: bigint | null) => (v === null ? null : Number(v));
    return {
      contents: cs.rows.map((r) => ({
        sha256: r.sha256,
        kind: r.kind,
        ext: r.ext,
        mime: r.mime,
        width: r.width,
        height: r.height,
        sizeBytes: r.size_bytes,
        capturedAt: r.captured_at,
        liveContentId: r.live_content_id,
        durationS: r.duration_s,
        state: r.state,
        quarantineReason: r.quarantine_reason,
        copyGroup: num(r.copy_group_id),
        similarSet: num(r.similar_set_id),
        ambiguous: r.copy_ambiguous,
      })),
      files: fs.rows.map((r) => ({
        fileId: r.id,
        relPath: r.rel_path,
        originalName: r.original_name,
        mtimeNs: r.mtime_ns,
        sha256: r.sha256,
        state: r.state,
        srcRel:
          r.state === 'COPY'
            ? layout.rel(join(layout.vaultCopies, `${String(r.id)}.${r.ext}`))
            : vaultRel(r.sha256, r.ext),
      })),
    };
  }

  /** The generation `current` points at, if any. */
  async function currentGen(): Promise<number | null> {
    try {
      const m = GEN.exec(await readlink(currentPath));
      return m ? Number(m[1]) : null;
    } catch (e) {
      if (isEnoent(e)) return null;
      throw e;
    }
  }

  async function gensOnDisk(): Promise<number[]> {
    return (await readdir(layout.staging))
      .map((n) => GEN.exec(n))
      .filter((m) => m !== null)
      .map((m) => Number(m[1]));
  }

  /** Hardlinks every entry into `dir` (read-only inodes are shared, never copied or modified). */
  async function linkAll(dir: string, entries: Entry[]): Promise<void> {
    const made = new Set<string>();
    let n = 0;
    for (const e of entries) {
      const dest = join(dir, e.dest);
      const parent = dirname(dest);
      if (!made.has(parent)) {
        await mkdir(parent, { recursive: true });
        made.add(parent);
      }
      try {
        await link(join(layout.root, e.src), dest);
      } catch (err) {
        if (isEnoent(err))
          throw new MissingSourceError(`staging ${e.dest}: source ${e.src} is missing`, {
            cause: err,
          });
        throw err;
      }
      if (++n % HEARTBEAT_EVERY === 0) heartbeat(n);
    }
  }

  /**
   * Removes `.gen-<n>` bottom-up, heartbeating every 1,000 entries. Only staging links are
   * unlinked: regular files with another name (the vault's). Anything else is something a user
   * saved or made inside staging, whose bytes may exist nowhere else: it is moved, never
   * deleted, to `staging/.rescued/<n>/<same relative path>`.
   *
   * Each entry is first renamed into a private `.pm-reap-<uuid>/` directory beside it, and only
   * that private name is examined and unlinked. An app that saves over the public name meanwhile
   * (rename-over, through an old `current`) makes a new entry there instead, which the next
   * sweep rescues: there is no window between the `lstat` and the `unlink`.
   */
  async function removeGen(n: number): Promise<void> {
    const root = genPath(n);
    let count = 0;
    let rescued = 0;
    const tick = () => {
      if (++count % HEARTBEAT_EVERY === 0) heartbeat(`rm .gen-${String(n)}`);
    };
    /** Moves `held` (the private name of the entry once at `p`) to `.rescued`. */
    const rescue = async (held: string, p: string): Promise<void> => {
      // A crash can leave a private directory behind: rescue under the public path all the same.
      const rel = relative(root, p)
        .split(sep)
        .filter((c) => !REAP.test(c))
        .join(sep);
      const dest = join(layout.staging, '.rescued', String(n), rel);
      await mkdir(dirname(dest), { recursive: true });
      await rename(held, await freeName(dest));
      rescued++;
    };
    /** Renames `p` to the private `held`; false when `p` is gone meanwhile (nothing to remove). */
    const claim = async (p: string, held: string): Promise<boolean> => {
      try {
        await rename(p, held);
        return true;
      } catch (err) {
        if (isEnoent(err)) return false;
        throw err;
      }
    };
    const walk = async (dir: string): Promise<void> => {
      let reap: string | null = null;
      for (const e of await readdir(dir, { withFileTypes: true })) {
        const p = join(dir, e.name);
        if (e.isDirectory()) await walk(p);
        else {
          if (reap === null) {
            reap = join(dir, `.pm-reap-${randomUUID()}`);
            await mkdir(reap);
          }
          const held = join(reap, e.name);
          if (await claim(p, held))
            try {
              const st = await lstat(held);
              if (st.isFile() && st.nlink >= 2) await unlink(held);
              else await rescue(held, p);
            } catch (err) {
              // Never leave the entry under a private name: put it back (beside anything new).
              await rename(held, await freeName(p));
              throw err;
            }
        }
        tick();
      }
      for (const d of reap === null ? [dir] : [reap, dir])
        try {
          await rmdir(d);
        } catch (e) {
          // Something appeared meanwhile: leave the directory for the next build's sweep.
          if (!isNotEmpty(e)) throw e;
        }
    };
    try {
      await walk(root);
    } catch (e) {
      if (!isEnoent(e)) throw e;
    } finally {
      if (rescued > 0)
        log.warn(`staging: moved ${String(rescued)} files not made by staging to .rescued`, {
          gen: n,
          rescued,
        });
    }
  }

  /** Takes the session advisory lock, heartbeating while another build holds it. */
  async function lock(c: pg.PoolClient): Promise<void> {
    for (;;) {
      const r = await c.query<{ ok: boolean }>(
        `SELECT pg_try_advisory_lock(hashtext('pm.stage')) AS ok`,
      );
      if (r.rows[0]!.ok) return;
      heartbeat('waiting for pm.stage');
      await sleep(LOCK_POLL_MS);
    }
  }

  async function buildLocked(c: pg.PoolClient, force: boolean): Promise<BuildResult> {
    await c.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    let snap: Snapshot;
    let facts: Awaited<ReturnType<typeof loadFacts>>;
    try {
      snap = await snapshot(c);
      const skip =
        !force &&
        (!snap.changed ||
          (snap.sinceLastS !== null &&
            snap.sinceLastS < settings.stageMaxStalenessS &&
            (await runningFileWorkflows()) > 0));
      if (skip) {
        await c.query('COMMIT');
        return { gen: null, entries: 0, skipped: true };
      }
      facts = await loadFacts(c);
      await c.query('COMMIT');
    } catch (e) {
      await c.query('ROLLBACK').catch(() => undefined); // never mask the original error
      throw e;
    }

    // Orphans: generations no build recorded, except the one `current` still points at.
    const recorded = new Set(
      (await c.query<{ gen: bigint }>('SELECT gen FROM staging_builds')).rows.map((r) =>
        Number(r.gen),
      ),
    );
    const live = await currentGen();
    for (const g of await gensOnDisk()) if (!recorded.has(g) && g !== live) await removeGen(g);
    await unlink(tmpLink).catch((e: unknown) => {
      if (!isEnoent(e)) throw e;
    });

    const gen = Math.max(0, snap.lastGen ?? 0, live ?? 0, ...(await gensOnDisk())) + 1;
    const entries = planStaging(facts.contents, facts.files, settings.liveMaxDurationS);
    try {
      await mkdir(genPath(gen));
      await linkAll(genPath(gen), entries);
    } catch (e) {
      await removeGen(gen); // never recorded: an orphan
      throw e;
    }
    await symlink(`.gen-${String(gen)}`, tmpLink);
    await rename(tmpLink, currentPath);
    await fsyncDir(layout.staging);
    await c.query(
      `INSERT INTO staging_builds (gen, watermark, cluster_version, snapshot, built_at, entries)
       VALUES ($1, $2::timestamptz, $3, $4::pg_snapshot, clock_timestamp(), $5)`,
      [gen, snap.watermark, snap.version, snap.snapshot, entries.length],
    );
    // Keep the new generation and the previous one (still open in someone's browser, maybe).
    for (const g of await gensOnDisk()) if (g !== gen && g !== live) await removeGen(g);
    return { gen, entries: entries.length, skipped: false };
  }

  /** Queue `io`. Rebuilds `staging/current` from the database when anything changed (or `force`). */
  return async function maybeBuildStaging(force: boolean): Promise<BuildResult> {
    const c = await pool.connect();
    let failure: Error | undefined;
    try {
      await lock(c);
      try {
        return await buildLocked(c, force);
      } finally {
        await c.query(`SELECT pg_advisory_unlock(hashtext('pm.stage'))`);
      }
    } catch (e) {
      failure = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      c.release(failure); // a failed client is discarded, not returned to the pool
    }
  };
}
