import type { BigIntStats } from 'node:fs';
import { constants } from 'node:fs';
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

/**
 * Every filesystem call GuardedFs and the send path make goes through this seam (tests can
 * record them). The only way to open a file without a write guard is `openRead` (O_RDONLY).
 */
export interface FsOps {
  mkdir(path: string, opts: { recursive: true }): Promise<unknown>;
  open(path: string, flags: string, mode: number): Promise<fsp.FileHandle>;
  /** Read-only open (O_RDONLY): the only way a source file is opened. */
  openRead(path: string): Promise<fsp.FileHandle>;
  chmod(path: string, mode: number): Promise<void>;
  utimes(path: string, atime: number, mtime: number): Promise<void>;
  rename(from: string, to: string): Promise<void>;
  unlink(path: string): Promise<void>;
  link(existing: string, newPath: string): Promise<void>;
  copyFile(src: string, dest: string, mode: number): Promise<void>;
  lstat(path: string): Promise<{ isSymbolicLink(): boolean }>;
  /** lstat with bigint fields (exact ns times, dev). */
  stat(path: string): Promise<BigIntStats>;
  realpath(path: string): Promise<string>;
  /** mtime as exact nanoseconds. */
  mtimeNs(path: string): Promise<bigint>;
}

/** Opens `path` read-only. Harvest code reads files through this (or FsOps.openRead). */
export const openReadOnly = (path: string): Promise<fsp.FileHandle> =>
  fsp.open(path, constants.O_RDONLY);

export const realFsOps: FsOps = {
  mkdir: (p, o) => fsp.mkdir(p, o),
  open: (p, f, m) => fsp.open(p, f, m),
  openRead: openReadOnly,
  chmod: (p, m) => fsp.chmod(p, m),
  utimes: (p, a, m) => fsp.utimes(p, a, m),
  rename: (a, b) => fsp.rename(a, b),
  unlink: (p) => fsp.unlink(p),
  link: (a, b) => fsp.link(a, b),
  copyFile: (a, b, m) => fsp.copyFile(a, b, m),
  lstat: (p) => fsp.lstat(p),
  stat: (p) => fsp.lstat(p, { bigint: true }),
  realpath: (p) => fsp.realpath(p),
  mtimeNs: async (p) => (await fsp.lstat(p, { bigint: true })).mtimeNs,
};

const COPY_CHUNK = 1024 * 1024;
const errCode = (e: unknown): string | undefined => (e as NodeJS.ErrnoException).code;
/** A failed clone with one of these is a real outcome, not "cloning unsupported". */
const NO_FALLBACK = new Set(['EEXIST', 'ENOENT', 'ENOSPC', 'EDQUOT']);

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

  /**
   * Sets atime and mtime to about `mtimeNs`. Node's utimes takes double seconds, so the result
   * can differ by < 1 µs: returns the mtime actually on disk (read back), which is what an exact
   * `mtime_ns` join against the pipeline's `files` row needs.
   */
  async utimesNs(path: string, mtimeNs: bigint): Promise<bigint> {
    const real = await this.check(path);
    const s = Number(mtimeNs) / 1e9;
    await this.ops.utimes(real, s, s);
    return this.ops.mtimeNs(real);
  }

  async rename(from: string, to: string): Promise<void> {
    const f = await this.check(from);
    await this.ops.rename(f, await this.check(to));
  }

  async unlink(path: string): Promise<void> {
    await this.ops.unlink(await this.check(path));
  }

  /**
   * Copies `src` (opened O_RDONLY only; never written) into a new file `dest` (exclusive
   * create, guarded). `clone` tries `copyFile` with COPYFILE_FICLONE_FORCE first and falls back
   * to a streamed byte copy when cloning is refused (ENOSYS, ENOTSUP, EXDEV …). `onChunk` is
   * called with the bytes written so far after each 1 MiB (it may throw to abort the copy).
   * On failure `dest` may be left behind: the caller removes it.
   */
  async copyInto(
    src: string,
    dest: string,
    opts: { clone: boolean; onChunk?: (written: number) => void },
  ): Promise<void> {
    const d = await this.check(dest);
    if (opts.clone) {
      try {
        await this.ops.copyFile(src, d, constants.COPYFILE_EXCL | constants.COPYFILE_FICLONE_FORCE);
        return;
      } catch (e) {
        if (NO_FALLBACK.has(errCode(e) ?? '')) throw e;
        // A refused clone leaves nothing behind, but make sure before the exclusive create.
        await this.ops.unlink(d).catch((u: unknown) => {
          if (errCode(u) !== 'ENOENT') throw u;
        });
      }
    }
    const from = await this.ops.openRead(src);
    try {
      const to = await this.ops.open(d, 'wx', 0o600);
      try {
        const buf = Buffer.allocUnsafe(COPY_CHUNK);
        let written = 0;
        for (;;) {
          const { bytesRead } = await from.read(buf, 0, COPY_CHUNK, written);
          if (bytesRead === 0) break;
          let off = 0;
          while (off < bytesRead) {
            const { bytesWritten } = await to.write(buf, off, bytesRead - off, written + off);
            off += bytesWritten;
          }
          written += bytesRead;
          opts.onChunk?.(written);
        }
        await to.datasync();
      } finally {
        await to.close();
      }
    } finally {
      await from.close();
    }
  }

  /**
   * Publishes `part` under `final` without ever replacing an existing file: link (fails with
   * EEXIST if `final` exists — the caller picks another name), then unlink the part.
   */
  async publishNoReplace(part: string, final: string): Promise<void> {
    const p = await this.check(part);
    await this.ops.link(p, await this.check(final));
    await this.ops.unlink(p);
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
