import { mkdtemp, open, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ExifTool } from 'exiftool-vendored';
import heicDecode from 'heic-decode';
import sharp, { type Sharp, type SharpOptions } from 'sharp';
import { unlessEnoent } from './fsops.js';
import { isHeifHead, RAW_MIME, STILL_EXTS } from './formats.js';
import { dhashFromGray, phashFromGray } from './hashing.js';
import type { WireFacts } from './types.js';

export interface ImageFacts {
  width: number;
  height: number;
  phash: bigint;
  dhash: bigint;
  mime: string;
}

export type QuarantineReason = 'decode_error' | 'truncated' | 'decompression_bomb' | 'unsupported';

/**
 * A permanent decode failure. `name` and `reason` are own properties so they survive
 * workerpool's error serialization; across the pool, test `name`/`reason`, not `instanceof`.
 */
export class QuarantineError extends Error {
  override name = 'QuarantineError';
  constructor(
    readonly reason: QuarantineReason,
    message: string = reason,
  ) {
    super(message);
  }
}

/** Larger inputs are treated as decompression bombs (spec §6.2). */
const PIXEL_LIMIT = 200_000_000;

const SHARP_MIME: Partial<Record<string, string>> = {
  jpeg: 'image/jpeg',
  png: 'image/png',
  webp: 'image/webp',
  tiff: 'image/tiff',
  gif: 'image/gif',
  heif: 'image/avif', // prebuilt libvips decodes only AV1 HEIF; HEVC goes through heic-decode
};

let exiftool: ExifTool | undefined;

/** Ends the lazily created RAW-preview ExifTool, if any. */
export async function endDecodeResources(): Promise<void> {
  const et = exiftool;
  exiftool = undefined;
  await et?.end();
}

/**
 * Decodes a still image (sniffing the content, not trusting `ext` beyond picking the
 * family) and returns its oriented original dimensions, pHash and dHash.
 * Throws `QuarantineError` for permanent failures; anything else is transient.
 */
export async function computeImageFacts(absPath: string, ext: string): Promise<ImageFacts> {
  const e = ext.toLowerCase();
  const rawMime = RAW_MIME.get(e);
  if (rawMime) return fromRawPreview(absPath, rawMime);
  if (!STILL_EXTS.has(e)) throw new QuarantineError('unsupported', `unsupported extension .${ext}`);
  const head = await readHead(absPath, 32);
  if (isHeifHead(head)) return fromHeic(absPath);
  if (head.toString('latin1', 0, 2) === 'BM') return fromBmp(absPath);
  return factsFrom(absPath);
}

export async function computeImageFactsWire(absPath: string, ext: string): Promise<WireFacts> {
  const f = await computeImageFacts(absPath, ext);
  return { ...f, phash: f.phash.toString(), dhash: f.dhash.toString() };
}

async function readHead(absPath: string, n: number): Promise<Buffer> {
  const fh = await open(absPath, 'r');
  try {
    const buf = Buffer.alloc(n);
    const { bytesRead } = await fh.read(buf, 0, n, 0);
    return buf.subarray(0, bytesRead);
  } finally {
    await fh.close();
  }
}

/** Runs the sharp pipeline (with `.rotate()` for EXIF orientation) and hashes the result. */
async function factsFrom(
  input: string | Buffer,
  opts: SharpOptions = {},
  mime?: string,
): Promise<ImageFacts> {
  try {
    const img = sharp(input, { limitInputPixels: PIXEL_LIMIT, failOn: 'truncated', ...opts });
    const meta = await img.metadata();
    const m = mime ?? SHARP_MIME[meta.format];
    if (!m) throw new QuarantineError('unsupported', `unsupported image format ${meta.format}`);
    const [p, d] = await Promise.all([gray(img, 32, 32), gray(img, 9, 8)]);
    return {
      width: meta.autoOrient.width,
      height: meta.autoOrient.height,
      phash: phashFromGray(p),
      dhash: dhashFromGray(d),
      mime: m,
    };
  } catch (err) {
    throw toQuarantine(err);
  }
}

async function gray(img: Sharp, w: number, h: number): Promise<Uint8Array> {
  const { data, info } = await img
    .clone()
    .rotate()
    .removeAlpha()
    .toColourspace('b-w')
    .resize(w, h, { fit: 'fill' })
    .raw()
    .toBuffer({ resolveWithObject: true });
  if (info.channels !== 1 || data.length !== w * h)
    throw new Error(`expected ${w}×${h}×1 greyscale, got ${info.channels} channels`);
  return data;
}

/** Maps libvips error messages to quarantine reasons; unknown errors pass through (transient). */
function toQuarantine(err: unknown): unknown {
  if (err instanceof QuarantineError || !(err instanceof Error)) return err;
  const msg = err.message;
  if (/pixel limit/i.test(msg)) return new QuarantineError('decompression_bomb', msg);
  if (/premature end|truncated/i.test(msg)) return new QuarantineError('truncated', msg);
  if (/unsupported image format|corrupt header/i.test(msg))
    return new QuarantineError('decode_error', msg);
  return err;
}

type HeifImages = {
  width: number;
  height: number;
  decode(): Promise<{ width: number; height: number; data: Uint8ClampedArray }>;
}[] & { dispose(): void };

async function fromHeic(absPath: string): Promise<ImageFacts> {
  const buffer = await readFile(absPath);
  let decoded: { width: number; height: number; data: Uint8ClampedArray };
  try {
    // At runtime `all()` also exposes each image's size and a `dispose()`; the typings omit both.
    const images = (await heicDecode.all({ buffer })) as unknown as HeifImages;
    try {
      const primary = images[0];
      if (!primary) throw new Error('HEIF image not found');
      // Size check before decoding so a bomb never allocates its RGBA buffer.
      checkPixels(primary.width, primary.height);
      decoded = await primary.decode();
    } finally {
      images.dispose();
    }
  } catch (err) {
    if (err instanceof QuarantineError) throw err;
    // The WASM decoder failing to grow its memory says nothing about the file: transient.
    if (err instanceof RangeError || /memory/i.test(String(err))) throw err;
    throw new QuarantineError('decode_error', `HEIC: ${String(err)}`);
  }
  const { width, height, data } = decoded;
  return factsFrom(
    Buffer.from(data.buffer, data.byteOffset, data.byteLength),
    { raw: { width, height, channels: 4 } },
    'image/heic',
  );
}

function checkPixels(width: number, height: number): void {
  if (width * height > PIXEL_LIMIT)
    throw new QuarantineError('decompression_bomb', `${width}×${height} exceeds pixel limit`);
}

/**
 * Prebuilt libvips has no BMP loader, so uncompressed (BI_RGB) 24/32-bit BMPs are decoded
 * here and handed to sharp as raw RGB. Anything else is `unsupported`.
 */
async function fromBmp(absPath: string): Promise<ImageFacts> {
  const buf = await readFile(absPath);
  if (buf.length < 54) throw new QuarantineError('truncated', 'BMP header cut short');
  const offset = buf.readUInt32LE(10);
  const dibSize = buf.readUInt32LE(14);
  const width = buf.readInt32LE(18);
  const rawHeight = buf.readInt32LE(22);
  const bpp = buf.readUInt16LE(28);
  const compression = buf.readUInt32LE(30);
  if (dibSize < 40 || (bpp !== 24 && bpp !== 32) || compression !== 0 || width <= 0 || !rawHeight)
    throw new QuarantineError('unsupported', `BMP ${bpp}-bit compression ${compression}`);
  const height = Math.abs(rawHeight);
  checkPixels(width, height);
  const bytesPP = bpp / 8;
  const stride = Math.ceil((width * bytesPP) / 4) * 4;
  if (offset + stride * height > buf.length)
    throw new QuarantineError('truncated', 'BMP pixel data cut short');
  const rgb = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    const row = offset + (rawHeight > 0 ? height - 1 - y : y) * stride;
    for (let x = 0; x < width; x++) {
      const [s, d] = [row + x * bytesPP, (y * width + x) * 3];
      rgb[d] = buf.readUInt8(s + 2);
      rgb[d + 1] = buf.readUInt8(s + 1);
      rgb[d + 2] = buf.readUInt8(s);
    }
  }
  return factsFrom(rgb, { raw: { width, height, channels: 3 } }, 'image/bmp');
}

/**
 * RAW files are hashed from their embedded preview, extracted to a temp file and removed.
 * Width and height are the RAW's own (oriented) image size, not the smaller preview's, so a RAW
 * outranks the camera's JPEG on pixels (best-copy rule 1); a size below the preview's is not
 * believed.
 */
async function fromRawPreview(absPath: string, mime: string): Promise<ImageFacts> {
  return withRawPreview(absPath, async (preview, { Orientation, ImageWidth, ImageHeight }) => {
    const facts = { ...(await factsFrom(preview)), mime };
    if (!isDim(ImageWidth) || !isDim(ImageHeight)) return facts;
    const swap = typeof Orientation === 'number' && Orientation >= 5 && Orientation <= 8;
    const [width, height] = swap ? [ImageHeight, ImageWidth] : [ImageWidth, ImageHeight];
    return width * height >= facts.width * facts.height ? { ...facts, width, height } : facts;
  });
}

/**
 * The embedded preview JPEG of a RAW file, carrying the RAW's orientation (so a `.rotate()`
 * pipeline shows it upright). Throws `QuarantineError` when the RAW has no preview.
 */
export async function rawPreview(absPath: string): Promise<Buffer> {
  return withRawPreview(absPath, (preview) => readFile(preview));
}

/**
 * Extracts the RAW's embedded preview to a temp file, hands it to `use`, then removes it.
 * Previews usually carry no orientation (it lives in the RAW's IFD0), so the RAW's numeric
 * Orientation is copied onto the preview first. Only a missing preview is permanent; any other
 * exiftool failure (spawn, timeout) is transient.
 */
async function withRawPreview<T>(
  absPath: string,
  use: (
    preview: string,
    tags: { Orientation?: unknown; ImageWidth?: unknown; ImageHeight?: unknown },
  ) => Promise<T>,
): Promise<T> {
  await stat(absPath); // a missing or unreadable source is transient, not the file's fault
  exiftool ??= new ExifTool({ maxProcs: 1 });
  const dir = await mkdtemp(join(tmpdir(), 'pm-raw-'));
  try {
    const preview = join(dir, 'preview.jpg');
    try {
      await exiftool.extractPreview(absPath, preview);
    } catch (err) {
      if (err instanceof Error && NO_PREVIEW.test(err.message))
        throw new QuarantineError('decode_error', `RAW has no preview: ${err.message}`);
      throw err;
    }
    if (await isMissing(preview)) throw new QuarantineError('decode_error', 'RAW has no preview');
    const tags = await exiftool.readRaw(absPath, {
      readArgs: ['-Orientation', '-ImageWidth', '-ImageHeight', '-n'],
    });
    const { Orientation } = tags;
    if (typeof Orientation === 'number' && Orientation !== 1)
      await exiftool.write(
        preview,
        {},
        {
          writeArgs: [`-Orientation#=${Orientation}`, '-overwrite_original'],
        },
      );
    return await use(preview, tags);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

const isDim = (v: unknown): v is number => typeof v === 'number' && Number.isInteger(v) && v > 0;

/** exiftool-vendored's explicit "nothing extracted" result for a binary tag. */
const NO_PREVIEW = /\b0 output files? created/i;

/** True only for ENOENT; any other stat failure propagates (transient). */
const isMissing = async (path: string): Promise<boolean> =>
  (await unlessEnoent(stat(path))) === null;
