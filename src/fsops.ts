import { createReadStream } from 'node:fs';
import { link, mkdir, open, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, join, relative } from 'node:path';

export const errCode = (e: unknown): string | undefined => (e as NodeJS.ErrnoException).code;
export const isEnoent = (e: unknown): boolean => errCode(e) === 'ENOENT';

/** `p`'s value, or null when it rejected with ENOENT (any other error propagates). */
export async function unlessEnoent<T>(p: Promise<T>): Promise<T | null> {
  try {
    return await p;
  } catch (e) {
    if (isEnoent(e)) return null;
    throw e;
  }
}

export class SameDeviceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SameDeviceError';
  }
}

/** The DATA_DIR layout. All of it must live on one filesystem so hardlinks and renames work. */
export class DataLayout {
  readonly dropzone: string;
  readonly claimed: string;
  readonly vault: string;
  readonly vaultCopies: string;
  readonly staging: string;

  constructor(readonly root: string) {
    this.dropzone = join(root, 'dropzone');
    this.claimed = join(root, 'work', 'claimed');
    this.vault = join(root, 'vault');
    this.vaultCopies = join(this.vault, '.copies');
    this.staging = join(root, 'staging');
  }

  async ensure(): Promise<void> {
    for (const d of [this.dropzone, this.claimed, this.vaultCopies, this.staging]) {
      await mkdir(d, { recursive: true });
    }
  }

  /** Path relative to the data root, as stored in the database. */
  rel(abs: string): string {
    return relative(this.root, abs);
  }
}

type DevStat = (path: string) => Promise<{ dev: number }>;

/** Throws SameDeviceError unless every layout directory is on the same device as the root. */
export async function checkSameDevice(layout: DataLayout, statFn: DevStat = stat): Promise<void> {
  const rootDev = (await statFn(layout.root)).dev;
  const dirs: [string, string][] = [
    ['dropzone', layout.dropzone],
    ['claimed', layout.claimed],
    ['vault', layout.vault],
    ['vaultCopies', layout.vaultCopies],
    ['staging', layout.staging],
  ];
  for (const [name, dir] of dirs) {
    const dev = (await statFn(dir)).dev;
    if (dev !== rootDev) {
      throw new SameDeviceError(`${name} (${dir}) is on device ${dev}, root is on ${rootDev}`);
    }
  }
}

export function vaultRel(sha256: string, ext: string): string {
  return `vault/${sha256.slice(0, 2)}/${sha256.slice(2, 4)}/${sha256}.${ext}`;
}

const CHUNK = 1 << 20;

export async function sha256File(path: string | Buffer, onChunk?: () => void): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path, { highWaterMark: CHUNK })) {
    h.update(chunk as Buffer);
    onChunk?.();
  }
  return h.digest('hex');
}

/**
 * Whether `a` and `b` are names of one file. Equal `(dev, ino)` decides. Docker Desktop's
 * virtiofs renumbers an inode when its cache evicts it, though, so two names of one file can
 * report different numbers. On a mismatch, two multiply-linked files with equal size and mtime
 * whose bytes both hash to `sha256` count as the same file. `onChunk` runs per MiB hashed (a
 * caller's heartbeat: a large video on virtiofs takes longer than a heartbeat timeout).
 */
export async function sameFile(
  a: string,
  b: string,
  sha256: string | null,
  onChunk?: () => void,
): Promise<boolean> {
  const [x, y] = await Promise.all([
    unlessEnoent(stat(a, { bigint: true })),
    unlessEnoent(stat(b, { bigint: true })),
  ]);
  if (!x || !y) return false;
  if (x.dev === y.dev && x.ino === y.ino) return true;
  if (sha256 === null || x.nlink < 2n || y.nlink < 2n) return false;
  if (x.size !== y.size || x.mtimeNs !== y.mtimeNs) return false;
  const [ha, hb] = await Promise.all([sha256File(a, onChunk), sha256File(b, onChunk)]);
  return ha === sha256 && hb === sha256;
}

export type LinkOutcome = 'LINKED' | 'ALREADY_LINKED' | 'EXISTS_OTHER';

/** fsync a directory so a link or rename inside it survives power loss. */
export async function fsyncDir(dir: string): Promise<void> {
  const fh = await open(dir, 'r');
  try {
    await fh.sync();
  } finally {
    await fh.close();
  }
}

/**
 * Hardlink src to dest (never overwrites, never unlinks); `sha256` is the bytes' hash. Once
 * linked (or found already linked), dest's directory and its two parents (`vault/ab/cd`,
 * `vault/ab`, `vault`) are fsynced on every call, plus the parent of any higher level it had
 * to create. A replay redoes them all: the crash may have come after `mkdir` or `link` but
 * before their fsyncs, and on a replay `mkdir` creates nothing to say so.
 */
export async function linkIntoVault(
  src: string,
  dest: string,
  sha256: string,
  opts: { onChunk?: () => void; fsync?: (dir: string) => Promise<void> } = {},
): Promise<LinkOutcome> {
  const sync = opts.fsync ?? fsyncDir;
  const dir = dirname(dest);
  const first = await mkdir(dir, { recursive: true });
  /** Directories to fsync, all ancestors-or-self of `dir`: deduplicated, ordered top-down. */
  const levels = new Set([dirname(dirname(dir)), dirname(dir), dir]);
  if (first !== undefined)
    for (let d = dir; ; d = dirname(d)) {
      levels.add(dirname(d));
      if (d === first || dirname(d) === d) break;
    }
  const durable = async (): Promise<void> => {
    for (const d of [...levels].sort((a, b) => a.length - b.length)) await sync(d);
  };
  try {
    await link(src, dest);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    // `sameFile`, not a bare inode compare: virtiofs may have renumbered the vault link.
    if (!(await sameFile(src, dest, sha256, opts.onChunk))) return 'EXISTS_OTHER';
    await durable();
    return 'ALREADY_LINKED';
  }
  await durable();
  return 'LINKED';
}

const IGNORED_SUFFIXES = ['.part', '.crdownload', '.tmp', '.download'];

export function isIgnored(name: string): boolean {
  return (
    name === '.DS_Store' ||
    name.startsWith('._') ||
    name.startsWith('~$') ||
    IGNORED_SUFFIXES.some((s) => name.toLowerCase().endsWith(s))
  );
}
