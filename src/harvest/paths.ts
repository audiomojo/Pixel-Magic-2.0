import * as fsp from 'node:fs/promises';
import { basename, dirname, extname, join, resolve, sep } from 'node:path';

/** NFC + lowercase: APFS ignores normalization and case, SMB can deliver NFD/NFC twins. */
export function pathKey(rel: string): string {
  return rel.normalize('NFC').toLowerCase();
}

const SEC = 1_000_000_000n;
const TOL = 2n * SEC;
const HOUR = 3600n * SEC;

/** Same file by mtime: SMB rounds (≤ 2 s), FAT/exFAT shift by exactly one hour across DST (± 2 s). */
export function sameMtime(a: bigint, b: bigint): boolean {
  const d = a > b ? a - b : b - a;
  if (d <= TOL) return true;
  const h = d > HOUR ? d - HOUR : HOUR - d;
  return h <= TOL;
}

/** Drop-zone relative name; the k-th claimant (k ≥ 2) gets ` (k)` before the extension. */
export function dropzoneName(label: string, rel: string, k: number): string {
  if (k < 2) return `harvest/${label}/${rel}`;
  const ext = extname(rel);
  return `harvest/${label}/${rel.slice(0, rel.length - ext.length)} (${k})${ext}`;
}

/** The in-progress name: `.part` suffix, never visible to the pipeline as a finished file. */
export function partPath(destAbs: string, itemId: string): string {
  return join(dirname(destAbs), `.${basename(destAbs)}.h${itemId}.part`);
}

export class OutsideAllowedRootError extends Error {
  constructor(public readonly target: string) {
    super(`refusing to write outside the allowed roots: ${target}`);
    this.name = 'OutsideAllowedRootError';
  }
}

/** Every filesystem call GuardedFs makes goes through this seam (tests can record them). */
export interface FsOps {
  mkdir(path: string, opts: { recursive: true }): Promise<unknown>;
  open(path: string, flags: string, mode: number): Promise<fsp.FileHandle>;
  chmod(path: string, mode: number): Promise<void>;
  utimes(path: string, atime: number, mtime: number): Promise<void>;
  rename(from: string, to: string): Promise<void>;
  unlink(path: string): Promise<void>;
  lstat(path: string): Promise<{ isSymbolicLink(): boolean }>;
  realpath(path: string): Promise<string>;
}

export const realFsOps: FsOps = {
  mkdir: (p, o) => fsp.mkdir(p, o),
  open: (p, f, m) => fsp.open(p, f, m),
  chmod: (p, m) => fsp.chmod(p, m),
  utimes: (p, a, m) => fsp.utimes(p, a, m),
  rename: (a, b) => fsp.rename(a, b),
  unlink: (p) => fsp.unlink(p),
  lstat: (p) => fsp.lstat(p),
  realpath: (p) => fsp.realpath(p),
};

/** The only way harvest code writes to disk: each mutation is confined to the allowed roots. */
export class GuardedFs {
  private roots?: Promise<string[]>;

  constructor(
    private readonly allowedRoots: string[],
    protected readonly ops: FsOps = realFsOps,
  ) {}

  /** Real path of the nearest existing ancestor + the not-yet-existing tail. */
  protected async realTarget(path: string): Promise<string> {
    if (path.split(/[\\/]/).includes('..')) throw new OutsideAllowedRootError(path);
    const abs = resolve(path);
    const tail: string[] = [];
    let cur = abs;
    for (;;) {
      try {
        return join(await this.ops.realpath(cur), ...tail);
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
        // A dangling symlink is not "missing": it could be followed out of the root.
        const st = await this.ops.lstat(cur).catch(() => null);
        if (st?.isSymbolicLink()) throw new OutsideAllowedRootError(path);
        const parent = dirname(cur);
        if (parent === cur) throw e;
        tail.unshift(basename(cur));
        cur = parent;
      }
    }
  }

  private rootList(): Promise<string[]> {
    this.roots ??= Promise.all(this.allowedRoots.map((r) => this.realTarget(r)));
    return this.roots;
  }

  /** Resolves `path` and returns it (real, tail appended) if under an allowed root. */
  protected async check(path: string): Promise<string> {
    const real = await this.realTarget(path);
    for (const r of await this.rootList()) {
      if (real === r || real.startsWith(r.endsWith(sep) ? r : r + sep)) return real;
    }
    throw new OutsideAllowedRootError(path);
  }

  async mkdirp(dir: string): Promise<void> {
    await this.ops.mkdir(await this.check(dir), { recursive: true });
  }

  /** Exclusive create (`wx`): never overwrites. */
  async openForWrite(path: string): Promise<fsp.FileHandle> {
    return this.ops.open(await this.check(path), 'wx', 0o644);
  }

  async chmod(path: string, mode: number): Promise<void> {
    await this.ops.chmod(await this.check(path), mode);
  }

  /** Sets atime and mtime to `mtimeNs` (the double-precision seconds Node takes: ~µs). */
  async utimesNs(path: string, mtimeNs: bigint): Promise<void> {
    const s = Number(mtimeNs) / 1e9;
    await this.ops.utimes(await this.check(path), s, s);
  }

  async rename(from: string, to: string): Promise<void> {
    const f = await this.check(from);
    await this.ops.rename(f, await this.check(to));
  }

  async unlink(path: string): Promise<void> {
    await this.ops.unlink(await this.check(path));
  }

  /** Read-only (lstat): no guard needed. */
  async exists(path: string): Promise<boolean> {
    try {
      await this.ops.lstat(path);
      return true;
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw e;
    }
  }
}
