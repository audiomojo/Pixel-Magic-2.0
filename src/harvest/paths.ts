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

/** A path Node can open: a string, or the exact on-disk bytes. */
export type SourcePath = string | Buffer;

/**
 * Every filesystem call GuardedFs and the send path make goes through this seam (tests can
 * record them). The only way to open a file without a write guard is `openRead` (O_RDONLY).
 */
export interface FsOps {
  mkdir(path: string, opts: { recursive: true }): Promise<unknown>;
  open(path: string, flags: string, mode: number): Promise<fsp.FileHandle>;
  /** Read-only open (O_RDONLY): the only way a source file is opened. */
  openRead(path: SourcePath): Promise<fsp.FileHandle>;
  chmod(path: string, mode: number): Promise<void>;
  utimes(path: string, atime: number, mtime: number): Promise<void>;
  rename(from: string, to: string): Promise<void>;
  unlink(path: string): Promise<void>;
  link(existing: string, newPath: string): Promise<void>;
  lstat(path: string): Promise<{ isSymbolicLink(): boolean }>;
  /** lstat with bigint fields (exact ns times, dev). */
  stat(path: SourcePath): Promise<BigIntStats>;
  realpath(path: string): Promise<string>;
  /** mtime as exact nanoseconds. */
  mtimeNs(path: string): Promise<bigint>;
}

/** Opens `path` read-only. Harvest code reads files through this (or FsOps.openRead). */
export const openReadOnly = (path: SourcePath): Promise<fsp.FileHandle> =>
  fsp.open(path, constants.O_RDONLY);

/** The read-only part of FsOps: what code outside paths.ts may call directly. */
export type ReadFsOps = Pick<FsOps, 'stat' | 'openRead'>;
export const readFsOps: ReadFsOps = {
  stat: (p) => fsp.lstat(p, { bigint: true }),
  openRead: openReadOnly,
};

/**
 * A source file's path: `<mount>/<rel_path>`, or — when the on-disk name is not the NFC
 * `rel_path` (not UTF-8, or not NFC) — the mount's bytes + '/' + `rel_path_raw`.
 */
export function sourcePath(mount: string, relPath: string, raw: Buffer | null): SourcePath {
  if (raw === null) return join(mount, relPath);
  return Buffer.concat([Buffer.from(mount.endsWith('/') ? mount : `${mount}/`), raw]);
}

export const realFsOps: FsOps = {
  mkdir: (p, o) => fsp.mkdir(p, o),
  open: (p, f, m) => fsp.open(p, f, m),
  openRead: openReadOnly,
  chmod: (p, m) => fsp.chmod(p, m),
  utimes: (p, a, m) => fsp.utimes(p, a, m),
  rename: (a, b) => fsp.rename(a, b),
  unlink: (p) => fsp.unlink(p),
  link: (a, b) => fsp.link(a, b),
  lstat: (p) => fsp.lstat(p),
  stat: readFsOps.stat,
  realpath: (p) => fsp.realpath(p),
  mtimeNs: async (p) => (await fsp.lstat(p, { bigint: true })).mtimeNs,
};

const COPY_CHUNK = 1024 * 1024;

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
   * Copies `src` (opened O_RDONLY only, never written; a Buffer for on-disk name bytes) into a
   * new file `dest` (exclusive create, mode 0600, guarded) with a streamed byte copy. On the
   * open write handle it then sets `mode` and the mtime (`mtimeNs`, about: see utimesNs) and
   * fsyncs, so a published copy is complete on disk. `onChunk` is called with the bytes written
   * so far after each 1 MiB (it may throw to abort). Returns the mtime actually on disk.
   * On failure `dest` may be left behind: the caller removes it.
   */
  async copyInto(
    src: SourcePath,
    dest: string,
    opts: { mode: number; mtimeNs: bigint; onChunk?: (written: number) => void },
  ): Promise<bigint> {
    const d = await this.check(dest);
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
        const s = Number(opts.mtimeNs) / 1e9;
        await to.chmod(opts.mode);
        await to.utimes(s, s);
        await to.sync();
      } finally {
        await to.close();
      }
    } finally {
      await from.close();
    }
    return this.ops.mtimeNs(d);
  }

  /**
   * Publishes `part` under `final` without ever replacing an existing file: link (fails with
   * EEXIST if `final` exists — the caller picks another name), fsync the directory so the new
   * name is durable, then unlink the part. Once the link exists the file is published: a
   * failed unlink only leaves the (ignored) `.part` behind, and returns false.
   */
  async publishNoReplace(part: string, final: string): Promise<boolean> {
    const p = await this.check(part);
    const f = await this.check(final);
    await this.ops.link(p, f);
    const dir = await this.ops.openRead(dirname(f));
    try {
      await dir.sync();
    } finally {
      await dir.close();
    }
    try {
      await this.ops.unlink(p);
      return true;
    } catch {
      return false;
    }
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
