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

export type LinkOutcome = 'LINKED' | 'ALREADY_LINKED' | 'EXISTS_OTHER';

async function fsyncDir(dir: string): Promise<void> {
  const fh = await open(dir, 'r');
  try {
    await fh.sync();
  } finally {
    await fh.close();
  }
}

/** Hardlink src to dest (never overwrites, never unlinks). */
export async function linkIntoVault(src: string, dest: string): Promise<LinkOutcome> {
  await mkdir(dirname(dest), { recursive: true });
  try {
    await link(src, dest);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    const [a, b] = await Promise.all([stat(src), stat(dest)]);
    return a.dev === b.dev && a.ino === b.ino ? 'ALREADY_LINKED' : 'EXISTS_OTHER';
  }
  await fsyncDir(dirname(dest));
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
