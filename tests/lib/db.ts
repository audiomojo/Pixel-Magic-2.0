// Read-only views of a test stack's `pixelmagic` database for the e2e assertions.
import type pg from 'pg';
import { createPool } from '../../src/db.js';

const pools = new Map<string, pg.Pool>();

/** One small pool per database URL, shared by every helper. */
export function poolFor(url: string): pg.Pool {
  let p = pools.get(url);
  if (!p) {
    p = createPool(url, 4);
    pools.set(url, p);
  }
  return p;
}

export async function closePool(url: string): Promise<void> {
  const p = pools.get(url);
  pools.delete(url);
  await p?.end();
}

export interface FileRow {
  id: bigint;
  relPath: string;
  originalName: string;
  state: string;
  sha256: string | null;
  sizeBytes: bigint;
  /** Set once its byte-identical `.copies` file was removed (spec §6.5). */
  copyRemovedAt: Date | null;
  copyReclaimedBytes: bigint | null;
  copyMismatch: boolean;
  lastError: string | null;
}

export interface ContentRow {
  sha256: string;
  kind: string;
  mime: string | null;
  state: string;
  quarantineReason: string | null;
  lastError: string | null;
  capturedAt: Date | null;
  subsec: string | null;
  burstUuid: string | null;
  /** Stored (signed) 64-bit hashes, null until hashed. */
  phash: bigint | null;
  dhash: bigint | null;
  ambiguous: boolean;
  /** Root copy group (resolved through `merged_into`), or null. */
  copyGroup: number | null;
  /** Root similar set of the root copy group, or null. */
  similarSet: number | null;
}

/** Every file and content row, with group ids resolved to their roots. */
export interface World {
  files: FileRow[];
  contents: Map<string, ContentRow>;
}

export async function loadWorld(url: string): Promise<World> {
  const pool = poolFor(url);
  const groups = await pool.query<{
    id: bigint;
    merged_into: bigint | null;
    similar_set_id: bigint | null;
  }>('SELECT id, merged_into, similar_set_id FROM groups');
  const parent = new Map(groups.rows.map((g) => [Number(g.id), g.merged_into]));
  const similarOf = new Map(groups.rows.map((g) => [Number(g.id), g.similar_set_id]));
  const root = (id: number): number => {
    for (let x = id, hops = 0; ; hops++) {
      const p = parent.get(x);
      if (p === undefined || p === null) return x;
      if (hops > parent.size) throw new Error(`groups: merged_into cycle at ${String(id)}`);
      x = Number(p);
    }
  };
  const files = await pool.query<{
    id: bigint;
    rel_path: string;
    original_name: string;
    state: string;
    sha256: string | null;
    size_bytes: bigint;
    copy_removed_at: Date | null;
    copy_reclaimed_bytes: bigint | null;
    copy_mismatch: boolean;
    last_error: string | null;
  }>(
    `SELECT id, rel_path, original_name, state, sha256, size_bytes, copy_removed_at,
            copy_reclaimed_bytes, copy_mismatch, last_error FROM files ORDER BY id`,
  );
  const contents = await pool.query<{
    sha256: string;
    kind: string;
    mime: string | null;
    state: string;
    quarantine_reason: string | null;
    last_error: string | null;
    captured_at: Date | null;
    subsec: string | null;
    burst_uuid: string | null;
    phash: bigint | null;
    dhash: bigint | null;
    copy_ambiguous: boolean;
    copy_group_id: bigint | null;
  }>(
    `SELECT sha256, kind, mime, state, quarantine_reason, last_error, captured_at, subsec,
            burst_uuid, phash, dhash, copy_ambiguous, copy_group_id FROM contents`,
  );
  return {
    files: files.rows.map((r) => ({
      id: r.id,
      relPath: r.rel_path,
      originalName: r.original_name,
      state: r.state,
      sha256: r.sha256,
      sizeBytes: r.size_bytes,
      copyRemovedAt: r.copy_removed_at,
      copyReclaimedBytes: r.copy_reclaimed_bytes,
      copyMismatch: r.copy_mismatch,
      lastError: r.last_error,
    })),
    contents: new Map(
      contents.rows.map((r) => {
        const copyGroup = r.copy_group_id === null ? null : root(Number(r.copy_group_id));
        const s = copyGroup === null ? null : (similarOf.get(copyGroup) ?? null);
        return [
          r.sha256,
          {
            sha256: r.sha256,
            kind: r.kind,
            mime: r.mime,
            state: r.state,
            quarantineReason: r.quarantine_reason,
            lastError: r.last_error,
            capturedAt: r.captured_at,
            subsec: r.subsec,
            burstUuid: r.burst_uuid,
            phash: r.phash,
            dhash: r.dhash,
            ambiguous: r.copy_ambiguous,
            copyGroup,
            similarSet: s === null ? null : root(Number(s)),
          },
        ];
      }),
    ),
  };
}

/** Members of each root copy group. */
export function copyGroups(w: World): Map<number, ContentRow[]> {
  const out = new Map<number, ContentRow[]>();
  for (const c of w.contents.values()) {
    if (c.copyGroup === null) continue;
    out.set(c.copyGroup, [...(out.get(c.copyGroup) ?? []), c]);
  }
  return out;
}
