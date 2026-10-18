// The staged tree as `dest → content sha256`, and the (local, git-ignored) golden trees.
import { mkdir, readFile, readdir, stat, writeFile } from 'node:fs/promises';
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

/**
 * Every file under `staging/current/` with the sha256 of the bytes it links to. Staged files are
 * hardlinks, so the inode identifies the vault file (named by its sha256) or the `.copies` file
 * (named by its file id) it came from; a staged inode found in neither is a defect, reported as
 * sha256 `UNKNOWN`.
 */
export async function stagedEntries(dataDir: string, dbUrl: string): Promise<StagedEntry[]> {
  const vault = join(dataDir, 'vault');
  const copies = join(vault, '.copies');
  const shaOfIno = new Map<bigint, string>();
  const shaOfId = new Map(
    (
      await poolFor(dbUrl).query<{ id: bigint; sha256: string }>(
        'SELECT id, sha256 FROM files WHERE sha256 IS NOT NULL',
      )
    ).rows.map((r) => [String(r.id), r.sha256]),
  );
  for (const p of await walkFiles(vault)) {
    const base = p.slice(p.lastIndexOf('/') + 1);
    const stem = base.slice(0, base.indexOf('.') < 0 ? base.length : base.indexOf('.'));
    const sha = p.startsWith(copies + sep) ? shaOfId.get(stem) : stem;
    if (sha !== undefined) shaOfIno.set((await stat(p, { bigint: true })).ino, sha);
  }
  const current = join(dataDir, 'staging', 'current');
  const out: StagedEntry[] = [];
  for (const p of await walkFiles(current)) {
    const ino = (await stat(p, { bigint: true })).ino;
    out.push({ dest: posixRel(current, p), sha256: shaOfIno.get(ino) ?? 'UNKNOWN' });
  }
  return out.sort((a, b) => (a.dest < b.dest ? -1 : a.dest > b.dest ? 1 : 0));
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
  return (await stagedEntries(dataDir, dbUrl))
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
