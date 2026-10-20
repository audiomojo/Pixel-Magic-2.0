import { readdir, stat } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import heicDecode from 'heic-decode';
import sharp from 'sharp';
import { GuardedFs, readFsOps, type SourcePath } from './paths.js';

const THUMB_PX = 256;
/** Sources bigger than this are not thumbnailed (the whole file is read into memory). */
const MAX_SOURCE_BYTES = 256 * 1024 * 1024;
const HEIF_BRANDS = new Set(['heic', 'heix', 'hevc', 'hevx', 'heim', 'heis', 'mif1', 'msf1']);

/** HEIC/HEIF by content (`ftyp` box), since extensions lie. */
function isHeif(b: Buffer): boolean {
  return (
    b.length > 12 &&
    b.toString('latin1', 4, 8) === 'ftyp' &&
    HEIF_BRANDS.has(b.toString('latin1', 8, 12))
  );
}

async function readSource(src: SourcePath): Promise<Buffer> {
  const fh = await readFsOps.openRead(src);
  try {
    if ((await fh.stat()).size > MAX_SOURCE_BYTES) throw new Error('source too large to thumbnail');
    return await fh.readFile();
  } finally {
    await fh.close();
  }
}

async function render(input: Buffer): Promise<Buffer> {
  if (isHeif(input)) {
    const { width, height, data } = await heicDecode({ buffer: input });
    return sharp(Buffer.from(data.buffer, data.byteOffset, data.byteLength), {
      raw: { width, height, channels: 4 },
    })
      .resize(THUMB_PX, THUMB_PX, { fit: 'inside' })
      .jpeg()
      .toBuffer();
  }
  return sharp(input).rotate().resize(THUMB_PX, THUMB_PX, { fit: 'inside' }).jpeg().toBuffer();
}

/**
 * Writes a 256 px JPEG of `srcAbs` (read-only) to `outAbs` through `fs`: a unique `.part` first,
 * then a rename inside the cache. Throws on a decode failure, leaving nothing behind.
 */
export async function makeThumb(srcAbs: SourcePath, outAbs: string, fs: GuardedFs): Promise<void> {
  const jpeg = await render(await readSource(srcAbs));
  await fs.mkdirp(dirname(outAbs));
  const part = join(dirname(outAbs), `.${basename(outAbs)}.${process.pid}.${Date.now()}.part`);
  try {
    const fh = await fs.openForWrite(part);
    try {
      await fh.writeFile(jpeg);
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
  const names = await readdir(dir).catch((e: NodeJS.ErrnoException) => {
    if (e.code === 'ENOENT') return [] as string[];
    throw e;
  });
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
