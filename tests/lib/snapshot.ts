// The staged tree as `dest → content sha256`, and the (local, git-ignored) golden trees.
import { mkdir, readFile, readdir, readlink, writeFile } from 'node:fs/promises';
import { hashFiles } from '../../tools/corpus/lib.js';
import { dirname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { poolFor } from './db.js';

const GOLDEN_DIR = fileURLToPath(new URL('../e2e/golden/', import.meta.url));

export interface StagedEntry {
  /** Path under `staging/current/`. */
  dest: string;
  sha256: string;
}

async function walkFiles(root: string): Promise<string[]> {
  const out: string[] = [];
  let entries;
  try {
    entries = await readdir(root, { recursive: true, withFileTypes: true });
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return out;
    throw e;
  }
  for (const d of entries) if (d.isFile()) out.push(join(d.parentPath, d.name));
  return out;
}

const posixRel = (from: string, to: string) => relative(from, to).split(sep).join('/');

const cache = new Map<string, StagedEntry[]>();

/**
 * Every file under `staging/current/` with the sha256 of its bytes, hashed (not matched by
 * inode: Docker Desktop's virtiofs may renumber inodes). Cached per staging generation, since
 * `current` names a new `.gen-<n>` on every build.
 */
export async function stagedEntries(dataDir: string): Promise<StagedEntry[]> {
  const current = join(dataDir, 'staging', 'current');
  let gen: string;
  try {
    gen = await readlink(current);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw e;
  }
  const key = `${dataDir}\0${gen}`;
  const hit = cache.get(key);
  if (hit) return hit;
  const paths = await walkFiles(current);
  const shas = await hashFiles(paths);
  const out = paths
    .map((p, i) => ({ dest: posixRel(current, p), sha256: shas[i]! }))
    .sort((a, b) => (a.dest < b.dest ? -1 : a.dest > b.dest ? 1 : 0));
  cache.set(key, out);
  return out;
}

/**
 * Sorted `dest\tsha256` lines of the staged tree. Where a `copy_ambiguous` content lands depends
 * on arrival order (spec §12 R3), so its entries read `AMBIGUOUS\t<sha256>`.
 */
export async function treeSnapshot(dataDir: string, dbUrl: string): Promise<string[]> {
  const ambiguous = new Set(
    (
      await poolFor(dbUrl).query<{ sha256: string }>(
        'SELECT sha256 FROM contents WHERE copy_ambiguous',
      )
    ).rows.map((r) => r.sha256),
  );
  return (await stagedEntries(dataDir))
    .map((e) => `${ambiguous.has(e.sha256) ? 'AMBIGUOUS' : e.dest}\t${e.sha256}`)
    .sort();
}

const goldenPath = (name: string) => join(GOLDEN_DIR, name);

/** The local golden `tests/e2e/golden/<name>` (git-ignored: it holds personal file names). */
export async function readGolden(name: string): Promise<string[]> {
  let text: string;
  try {
    text = await readFile(goldenPath(name), 'utf8');
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
    throw new Error(
      `golden ${name} does not exist on this machine: run \`make golden-smoke\` ` +
        '(= `make update-golden`) once to generate it, review it, then re-run',
      { cause: e },
    );
  }
  return text.split('\n').filter((l) => l !== '');
}

/** `make update-golden` (UPDATE_GOLDEN=1): replaces the local golden with `lines`. */
export async function writeGolden(name: string, lines: string[]): Promise<void> {
  await mkdir(dirname(goldenPath(name)), { recursive: true });
  await writeFile(goldenPath(name), lines.join('\n') + '\n');
}
