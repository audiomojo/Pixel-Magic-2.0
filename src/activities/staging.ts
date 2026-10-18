import { link, mkdir, readdir, readlink, rename, rm, symlink, unlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { heartbeat } from '@temporalio/activity';
import type pg from 'pg';
import { fsyncDir, vaultRel } from '../fsops.js';
import { planStaging, type Entry, type StageContent, type StageFile } from '../stagingPlan.js';
import type { BuildResult } from '../types.js';
import type { Deps } from './index.js';

const GEN = /^\.gen-(\d+)$/;
const HEARTBEAT_EVERY = 1000;

/** A planned source is missing: the vault disagrees with the database. Retryable, never hidden. */
export class MissingSourceError extends Error {
  override name = 'MissingSourceError';
}

interface Snapshot {
  /** `greatest(max(files.updated_at), max(contents.updated_at))` as text (full µs precision). */
  watermark: string;
  version: bigint;
  changed: boolean;
  lastGen: number | null;
  sinceLastS: number | null;
}

const isEnoent = (e: unknown): boolean => (e as NodeJS.ErrnoException).code === 'ENOENT';

export function makeStaging(deps: Deps) {
  const { pool, layout, settings } = deps;
  const genPath = (n: number) => join(layout.staging, `.gen-${String(n)}`);
  const currentPath = join(layout.staging, 'current');
  const tmpLink = join(layout.staging, '.current.tmp');

  async function snapshot(c: pg.PoolClient): Promise<Snapshot> {
    const r = await c.query<{
      watermark: string;
      version: bigint;
      changed: boolean;
      last_gen: bigint | null;
      since_s: number | null;
    }>(
      `WITH w AS (
         SELECT coalesce(greatest((SELECT max(updated_at) FROM files),
                                  (SELECT max(updated_at) FROM contents)),
                         'epoch'::timestamptz) AS watermark,
                (SELECT version FROM cluster_state WHERE id = 1) AS version),
       b AS (SELECT * FROM staging_builds ORDER BY gen DESC LIMIT 1)
       SELECT w.watermark::text AS watermark, w.version,
              (b.gen IS NULL OR w.watermark > b.watermark OR w.version <> b.cluster_version) AS changed,
              b.gen AS last_gen,
              extract(epoch FROM clock_timestamp() - b.built_at)::float8 AS since_s
         FROM w LEFT JOIN b ON true`,
    );
    const row = r.rows[0]!;
    return {
      watermark: row.watermark,
      version: row.version,
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
      if (skip) return { gen: null, entries: 0, skipped: true };
      facts = await loadFacts(c);
    } finally {
      await c.query('COMMIT');
    }

    // Orphans: generations no build recorded, except the one `current` still points at.
    const recorded = new Set(
      (await c.query<{ gen: bigint }>('SELECT gen FROM staging_builds')).rows.map((r) =>
        Number(r.gen),
      ),
    );
    const live = await currentGen();
    for (const g of await gensOnDisk())
      if (!recorded.has(g) && g !== live) await rm(genPath(g), { recursive: true, force: true });
    await unlink(tmpLink).catch((e: unknown) => {
      if (!isEnoent(e)) throw e;
    });

    const gen = Math.max(0, snap.lastGen ?? 0, live ?? 0, ...(await gensOnDisk())) + 1;
    const entries = planStaging(facts.contents, facts.files, settings.liveMaxDurationS);
    try {
      await mkdir(genPath(gen));
      await linkAll(genPath(gen), entries);
    } catch (e) {
      await rm(genPath(gen), { recursive: true, force: true }); // never recorded: an orphan
      throw e;
    }
    await symlink(`.gen-${String(gen)}`, tmpLink);
    await rename(tmpLink, currentPath);
    await fsyncDir(layout.staging);
    await c.query(
      `INSERT INTO staging_builds (gen, watermark, cluster_version, built_at, entries)
       VALUES ($1, $2::timestamptz, $3, clock_timestamp(), $4)`,
      [gen, snap.watermark, snap.version, entries.length],
    );
    // Keep the new generation and the previous one (still open in someone's browser, maybe).
    for (const g of await gensOnDisk())
      if (g !== gen && g !== live) await rm(genPath(g), { recursive: true, force: true });
    return { gen, entries: entries.length, skipped: false };
  }

  /** Queue `io`. Rebuilds `staging/current` from the database when anything changed (or `force`). */
  return async function maybeBuildStaging(force: boolean): Promise<BuildResult> {
    const c = await pool.connect();
    try {
      await c.query(`SELECT pg_advisory_lock(hashtext('pm.stage'))`);
      try {
        return await buildLocked(c, force);
      } finally {
        await c.query(`SELECT pg_advisory_unlock(hashtext('pm.stage'))`);
      }
    } finally {
      c.release();
    }
  };
}
