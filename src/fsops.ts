import { createReadStream } from 'node:fs';
import { link, mkdir, open, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, join, relative } from 'node:path';

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

export async function sha256File(path: string, onChunk?: () => void): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path, { highWaterMark: CHUNK })) {
    h.update(chunk as Buffer);
    onChunk?.();
  }
  return h.digest('hex');
}

async function statOrNull(path: string) {
  try {
    return await stat(path, { bigint: true });
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw e;
  }
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
  const [x, y] = await Promise.all([statOrNull(a), statOrNull(b)]);
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
 * Hardlink src to dest (never overwrites, never unlinks); `sha256` is the bytes' hash. Every
 * directory level it creates is made durable in its parent, and so is the link, also when a
 * replay finds it already there (the crash may have come before its fsync).
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
  if (first !== undefined) {
    const created: string[] = [];
    for (let d = dir; ; d = dirname(d)) {
      created.unshift(d);
      if (d === first || dirname(d) === d) break;
    }
    for (const d of created) await sync(dirname(d));
  }
  try {
    await link(src, dest);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    // `sameFile`, not a bare inode compare: virtiofs may have renumbered the vault link.
    if (!(await sameFile(src, dest, sha256, opts.onChunk))) return 'EXISTS_OTHER';
    await sync(dir);
    return 'ALREADY_LINKED';
  }
  await sync(dir);
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
