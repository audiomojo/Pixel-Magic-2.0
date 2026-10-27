import { readdir, stat } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import heicDecode from 'heic-decode';
import sharp from 'sharp';
import { isHeifHead } from '../formats.js';
import { unlessEnoent } from '../fsops.js';
import { GuardedFs, readFsOps, type SourcePath } from './paths.js';

const THUMB_PX = 256;
/** Sources bigger than this are not thumbnailed (the whole file is read into memory). */
const MAX_SOURCE_BYTES = 256 * 1024 * 1024;

/** Reads a whole source file read-only; refuses one above 256 MB. */
export async function readSource(src: SourcePath): Promise<Buffer> {
  const fh = await readFsOps.openRead(src);
  try {
    if ((await fh.stat()).size > MAX_SOURCE_BYTES) throw new Error('source too large to thumbnail');
    return await fh.readFile();
  } finally {
    await fh.close();
  }
}

/** A JPEG of `input` (HEIC or anything sharp decodes) fitting inside `px` × `px`, oriented. */
export async function renderJpeg(input: Buffer, px: number): Promise<Buffer> {
  if (isHeifHead(input)) {
    const { width, height, data } = await heicDecode({ buffer: input });
    return sharp(Buffer.from(data.buffer, data.byteOffset, data.byteLength), {
      raw: { width, height, channels: 4 },
    })
      .resize(px, px, { fit: 'inside' })
      .jpeg()
      .toBuffer();
  }
  return sharp(input).rotate().resize(px, px, { fit: 'inside' }).jpeg().toBuffer();
}

/**
 * Writes a 256 px JPEG of `srcAbs` (read-only) to `outAbs` through `fs`: a unique `.part` first,
 * then a rename inside the cache. Throws on a decode failure, leaving nothing behind.
 */
export async function makeThumb(srcAbs: SourcePath, outAbs: string, fs: GuardedFs): Promise<void> {
  await writeCacheFile(outAbs, await renderJpeg(await readSource(srcAbs), THUMB_PX), fs);
}

/** Writes `bytes` to `outAbs` through `fs`: a unique `.part` first, then a rename. */
export async function writeCacheFile(outAbs: string, bytes: Buffer, fs: GuardedFs): Promise<void> {
  await fs.mkdirp(dirname(outAbs));
  const part = join(dirname(outAbs), `.${basename(outAbs)}.${process.pid}.${Date.now()}.part`);
  try {
    const fh = await fs.openForWrite(part);
    try {
      await fh.writeFile(bytes);
    } finally {
      await fh.close();
    }
    await fs.rename(part, outAbs);
  } catch (e) {
    await fs.unlink(part).catch(() => {});
    throw e;
  }
}

/**
 * Deletes the oldest thumbnails (by mtime) until the directory holds at most `capBytes`.
 * In-progress `.part` files are left alone. Returns the bytes removed.
 */
export async function evictThumbs(
  dir: string,
  capBytes: number,
  fs: GuardedFs = new GuardedFs([dir]),
): Promise<number> {
  const names = (await unlessEnoent(readdir(dir))) ?? [];
  const files: { path: string; size: number; mtime: number }[] = [];
  for (const n of names) {
    if (n.endsWith('.part')) continue;
    const st = await stat(join(dir, n)).catch(() => null);
    if (st?.isFile()) files.push({ path: join(dir, n), size: st.size, mtime: st.mtimeMs });
  }
  let total = files.reduce((a, f) => a + f.size, 0);
  let freed = 0;
  for (const f of files.sort((a, b) => a.mtime - b.mtime)) {
    if (total <= capBytes) break;
    await fs.unlink(f.path).catch(() => {});
    total -= f.size;
    freed += f.size;
  }
  return freed;
}

let placeholder: Promise<Buffer> | undefined;
/** A small grey JPEG for unmounted sources and undecodable files (made once). */
export function placeholderJpeg(): Promise<Buffer> {
  placeholder ??= sharp({
    create: { width: THUMB_PX, height: THUMB_PX, channels: 3, background: '#8a8a8a' },
  })
    .jpeg({ quality: 60 })
    .toBuffer();
  return placeholder;
}
