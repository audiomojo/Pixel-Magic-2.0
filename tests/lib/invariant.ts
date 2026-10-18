// The golden invariant (spec §10.4), plus the burst rule: one violation per line, empty = holds.
import { lstat, readdir } from 'node:fs/promises';
import { join, sep } from 'node:path';
import { isIgnored } from '../../src/fsops.js';
import { hashFiles, type SeedManifest } from '../../tools/corpus/lib.js';
import { copyGroups, loadWorld, type ContentRow } from './db.js';
import { stagedEntries } from './snapshot.js';

const FINAL_FILE_STATES = new Set(['INGESTED', 'COPY', 'QUARANTINED']);
const FINAL_STILL_STATES = new Set(['CLUSTERED', 'QUARANTINED', 'FAILED']);

async function filesUnder(root: string): Promise<string[]> {
  try {
    return (await readdir(root, { recursive: true, withFileTypes: true }))
      .filter((d) => d.isFile())
      .map((d) => join(d.parentPath, d.name));
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw e;
  }
}

/** Regular files under `vault/` (`.copies` included) and their bytes, each inode counted once. */
export async function vaultUsage(dataDir: string): Promise<{ files: number; bytes: bigint }> {
  const seen = new Set<string>();
  let bytes = 0n;
  for (const p of await filesUnder(join(dataDir, 'vault'))) {
    const st = await lstat(p, { bigint: true });
    const key = `${String(st.dev)}:${String(st.ino)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    bytes += st.size;
  }
  return { files: seen.size, bytes };
}

const stemOf = (path: string): string => {
  const base = path.slice(path.lastIndexOf('/') + 1);
  const dot = base.indexOf('.');
  return dot < 0 ? base : base.slice(0, dot);
};

/** `(captured_at at whole seconds, subsec)`; null when unknown. */
const captureKey = (c: ContentRow) =>
  c.capturedAt === null ? null : { s: Math.floor(c.capturedAt.getTime() / 1000), sub: c.subsec };

/** Two different known captures (same rule as the grouping's refusal, spec §6.3). */
const conflict = (
  a: NonNullable<ReturnType<typeof captureKey>>,
  b: NonNullable<ReturnType<typeof captureKey>>,
) => a.s !== b.s || (a.sub !== null && b.sub !== null && a.sub !== b.sub);

/**
 * - every seeded file has exactly one final row (ignored names: none, and no row is left
 *   DISCOVERED/CLAIMED); every still content is final; any FAILED content has a `last_error`
 * - `sha256(vault file) == name` for every vault file; every distinct seeded sha256 is in the
 *   vault exactly once; `work/claimed/` is empty
 * - after idle `.copies` holds no confirmed duplicate: every COPY row is marked removed (its
 *   size reclaimed), none is flagged `copy_mismatch`, and `.copies` is empty (spec §6.5)
 * - staging holds each non-motion content once: a byte-identical COPY is never staged (a paired
 *   MOV is staged beside every placement of its still, by design)
 * - no copy group holds two different known `(captured_at, subsec)` values, or two contents with
 *   the same `burst_uuid` (burst frames are never copies of each other)
 */
export async function checkInvariant(
  dbUrl: string,
  dataDir: string,
  sm: SeedManifest,
): Promise<string[]> {
  const w = await loadWorld(dbUrl);
  const out: string[] = [];

  const rowsByPath = new Map<string, string[]>();
  for (const f of w.files)
    rowsByPath.set(f.relPath, [...(rowsByPath.get(f.relPath) ?? []), f.state]);
  for (const f of sm.files) {
    const states = rowsByPath.get(f.relPath) ?? [];
    if (isIgnored(f.name)) {
      if (states.length > 0)
        out.push(`${f.relPath}: ignored name has files rows [${String(states)}]`);
    } else if (states.length !== 1 || !FINAL_FILE_STATES.has(states[0]!))
      out.push(`${f.relPath}: expected one final files row, got [${states.join(', ')}]`);
  }
  for (const f of w.files)
    if (!FINAL_FILE_STATES.has(f.state)) out.push(`files ${String(f.id)} ${f.relPath}: ${f.state}`);
  for (const c of w.contents.values()) {
    if (c.kind === 'still' && !FINAL_STILL_STATES.has(c.state))
      out.push(`content ${c.sha256}: still left ${c.state}`);
    if (c.state === 'FAILED' && !c.lastError)
      out.push(`content ${c.sha256}: FAILED without last_error`);
    if (c.state === 'CLUSTERED' && c.copyGroup === null)
      out.push(`content ${c.sha256}: CLUSTERED without a copy group`);
  }

  // Vault integrity and completeness: each distinct seeded sha exactly once.
  const vault = join(dataDir, 'vault');
  const copiesDir = join(vault, '.copies') + sep;
  const all = await filesUnder(vault);
  const hashes = await hashFiles(all);
  const held = new Map<string, number>();
  const shaOfId = new Map(w.files.map((f) => [String(f.id), f.sha256]));
  all.forEach((p, i) => {
    const actual = hashes[i]!;
    const inCopies = p.startsWith(copiesDir);
    if (!inCopies) held.set(actual, (held.get(actual) ?? 0) + 1);
    const expected = inCopies ? shaOfId.get(stemOf(p)) : stemOf(p);
    if (actual !== expected)
      out.push(`${p.slice(dataDir.length + 1)}: sha256 ${actual} ≠ expected ${String(expected)}`);
    if (inCopies) out.push(`${p.slice(dataDir.length + 1)}: a duplicate still in .copies`);
  });
  for (const sha of new Set(sm.files.filter((f) => !isIgnored(f.name)).map((f) => f.sha256))) {
    const n = held.get(sha) ?? 0;
    if (n !== 1) out.push(`bytes ${sha}: in the vault ${String(n)} times, expected exactly once`);
  }
  for (const f of w.files) {
    if (f.state !== 'COPY') continue;
    const at = `files ${String(f.id)} ${f.relPath}`;
    if (f.copyMismatch) out.push(`${at}: copy_mismatch (${String(f.lastError)})`);
    else if (f.copyRemovedAt === null) out.push(`${at}: COPY not removed after idle`);
    else if (f.copyReclaimedBytes !== f.sizeBytes)
      out.push(`${at}: reclaimed ${String(f.copyReclaimedBytes)} bytes of ${String(f.sizeBytes)}`);
  }
  const claimed = await filesUnder(join(dataDir, 'work', 'claimed'));
  if (claimed.length > 0) out.push(`work/claimed holds ${String(claimed.length)} files`);

  // Staging choice (a): a non-motion content is staged once, so no byte-identical COPY is.
  const placed = new Map<string, string[]>();
  for (const e of await stagedEntries(dataDir))
    placed.set(e.sha256, [...(placed.get(e.sha256) ?? []), e.dest]);
  for (const [sha, dests] of placed)
    if (dests.length > 1 && w.contents.get(sha)?.kind !== 'motion')
      out.push(`content ${sha} staged ${String(dests.length)} times: ${dests.join(', ')}`);

  // Copy groups never mix shots.
  for (const [id, members] of copyGroups(w)) {
    const keys = members.map(captureKey).filter((k) => k !== null);
    if (keys.some((a, i) => keys.slice(i + 1).some((b) => conflict(a, b))))
      out.push(
        `copy group ${String(id)} holds different captures: ${members.map((m) => m.sha256).join(', ')}`,
      );
    const bursts = members.map((m) => m.burstUuid).filter((b) => b !== null);
    if (new Set(bursts).size !== bursts.length)
      out.push(
        `copy group ${String(id)} holds two frames of one burst: ${members.map((m) => m.sha256).join(', ')}`,
      );
  }
  return out;
}
