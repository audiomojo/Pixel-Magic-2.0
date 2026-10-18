import { randomBytes as cryptoRandomBytes } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { deflateSync } from 'node:zlib';
import { ExifTool } from 'exiftool-vendored';
import sharp from 'sharp';

const SCENE_W = 320;
const SCENE_H = 240;
let seq = 0;
const nextPath = (dir: string, ext: string): string => join(dir, `img-${++seq}.${ext}`);

/** Deterministic PRNG (mulberry32), so a scene number always renders the same picture. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A smooth "photo": a background gradient plus a few soft colour blobs, as raw RGB. */
function sceneRgb(scene: number): Buffer {
  const rnd = prng(scene * 7919 + 1);
  const blobs = Array.from({ length: 7 }, () => ({
    cx: rnd() * SCENE_W,
    cy: rnd() * SCENE_H,
    r: 25 + rnd() * 70,
    rgb: [rnd() * 255, rnd() * 255, rnd() * 255] as const,
  }));
  const tilt = rnd() * 2 - 1;
  const out = Buffer.alloc(SCENE_W * SCENE_H * 3);
  for (let y = 0; y < SCENE_H; y++)
    for (let x = 0; x < SCENE_W; x++) {
      const bg = 60 + 60 * ((x / SCENE_W) * tilt + (y / SCENE_H) * (1 - Math.abs(tilt)));
      const px = [bg, bg, bg];
      for (const b of blobs) {
        const w = Math.exp(-((x - b.cx) ** 2 + (y - b.cy) ** 2) / (2 * b.r * b.r));
        for (let c = 0; c < 3; c++) px[c] = (px[c] ?? 0) * (1 - w) + (b.rgb[c] ?? 0) * w;
      }
      for (let c = 0; c < 3; c++) out[(y * SCENE_W + x) * 3 + c] = Math.round(px[c] ?? 0);
    }
  return out;
}

const scene = (n: number) =>
  sharp(sceneRgb(n), { raw: { width: SCENE_W, height: SCENE_H, channels: 3 } });

/**
 * A synthetic photo of `scene` (default 0) written as `ext` (jpg, jpeg, png, webp, tif, gif).
 * `size` is the displayed size (default 640×480), multiplied by `scale`. `orientation: 6`
 * stores the pixels rotated 270° and tags EXIF Orientation 6, so it displays as the plain photo.
 */
export async function photo(
  dir: string,
  ext: string,
  opts: { scale?: number; size?: [number, number]; scene?: number; orientation?: 6 } = {},
): Promise<string> {
  const [w0, h0] = opts.size ?? [640, 480];
  const s = opts.scale ?? 1;
  const [w, h] = [Math.round(w0 * s), Math.round(h0 * s)];
  let img = sharp(
    await scene(opts.scene ?? 0)
      .resize(w, h, { fit: 'fill' })
      .png()
      .toBuffer(),
  );
  if (opts.orientation === 6) img = img.rotate(270).withMetadata({ orientation: 6 });
  const p = nextPath(dir, ext);
  switch (ext) {
    case 'jpg':
    case 'jpeg':
      await img.jpeg({ quality: 90 }).toFile(p);
      break;
    case 'png':
      await img.png().toFile(p);
      break;
    case 'webp':
      await img.webp({ quality: 90 }).toFile(p);
      break;
    case 'tif':
      await img.tiff().toFile(p);
      break;
    case 'gif':
      await img.gif().toFile(p);
      break;
    default:
      throw new Error(`photo: no encoder for .${ext}`);
  }
  return p;
}

/** An uncompressed BI_RGB BMP of scene 0 at 64×48 (24- or 32-bit, bottom-up unless `topDown`). */
export async function bmpFile(
  dir: string,
  opts: { bpp: 24 | 32; topDown?: boolean },
): Promise<string> {
  const [w, h] = [64, 48];
  const rgb = await scene(0).resize(w, h, { fit: 'fill' }).raw().toBuffer();
  const bytesPP = opts.bpp / 8;
  const stride = Math.ceil((w * bytesPP) / 4) * 4;
  const pixels = Buffer.alloc(stride * h);
  for (let y = 0; y < h; y++) {
    const row = opts.topDown ? y : h - 1 - y;
    for (let x = 0; x < w; x++) {
      const [s, d] = [(y * w + x) * 3, row * stride + x * bytesPP];
      pixels[d] = rgb[s + 2] ?? 0;
      pixels[d + 1] = rgb[s + 1] ?? 0;
      pixels[d + 2] = rgb[s] ?? 0;
      if (bytesPP === 4) pixels[d + 3] = 0xff;
    }
  }
  const header = Buffer.alloc(54);
  header.write('BM', 0, 'latin1');
  header.writeUInt32LE(54 + pixels.length, 2);
  header.writeUInt32LE(54, 10);
  header.writeUInt32LE(40, 14);
  header.writeInt32LE(w, 18);
  header.writeInt32LE(opts.topDown ? -h : h, 22);
  header.writeUInt16LE(1, 26);
  header.writeUInt16LE(opts.bpp, 28);
  header.writeUInt32LE(0, 30);
  header.writeUInt32LE(pixels.length, 34);
  const p = nextPath(dir, 'bmp');
  await writeFile(p, Buffer.concat([header, pixels]));
  return p;
}

/** A JPEG cut off halfway through its entropy-coded data. */
export async function truncatedJpeg(dir: string): Promise<string> {
  const full = await readFile(await photo(dir, 'jpg'));
  const p = nextPath(dir, 'jpg');
  await writeFile(p, full.subarray(0, Math.floor(full.length / 2)));
  return p;
}

/** A real JPEG's first 200 bytes, then 4 KiB of random bytes: a JPEG whose header is corrupt. */
export async function garbageJpeg(dir: string): Promise<string> {
  const full = await readFile(await photo(dir, 'jpg'));
  const p = nextPath(dir, 'jpg');
  await writeFile(p, Buffer.concat([full.subarray(0, 200), cryptoRandomBytes(4096)]));
  return p;
}

/** 4 KiB of random bytes: no image format at all. */
export async function randomBytes(dir: string): Promise<string> {
  const p = nextPath(dir, 'jpg');
  await writeFile(p, cryptoRandomBytes(4096));
  return p;
}

function pngChunk(type: string, data: Buffer): Buffer {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, 'latin1'), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}

function crc32(buf: Buffer): number {
  let c = 0xffffffff;
  for (const b of buf) {
    c ^= b;
    for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0xedb88320 : c >>> 1;
  }
  return (c ^ 0xffffffff) >>> 0;
}

/** A tiny PNG whose header claims 30000×30000 (900 Mpx), well over the 200 Mpx decode limit. */
export async function bombPng(dir: string): Promise<string> {
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(30_000, 0);
  ihdr.writeUInt32BE(30_000, 4);
  ihdr.set([8, 0, 0, 0, 0], 8); // 8-bit greyscale, deflate, no filter/interlace
  const idat = deflateSync(Buffer.alloc(30_001 * 4)); // the first rows; the limit trips before decode
  const png = Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk('IHDR', ihdr),
    pngChunk('IDAT', idat),
    pngChunk('IEND', Buffer.alloc(0)),
  ]);
  const p = nextPath(dir, 'png');
  await writeFile(p, png);
  return p;
}

/**
 * 320×240 HEIC of scene 0. Prebuilt sharp can't encode HEVC, so this was encoded once with
 * macOS `sips -s format heic` from `photo(dir, 'png', { size: [320, 240] })` and committed.
 */
export const HEIC_SCENE0 = fileURLToPath(new URL('../fixtures/scene0.heic', import.meta.url));

/**
 * A stand-in RAW: a TIFF of scene 3 named `.dng` whose embedded PreviewImage is scene 0.
 * With `orientation: 6`, IFD0 carries Orientation 6 and the preview (like a camera's) carries
 * no orientation tag and holds the pixels un-rotated (270°), so it displays as scene 0 upright.
 */
export async function rawWithPreview(dir: string, opts: { orientation?: 6 } = {}): Promise<string> {
  const p = nextPath(dir, 'dng');
  await sharp(await readFile(await photo(dir, 'tif', { scene: 3 })))
    .tiff()
    .toFile(p);
  let preview = await photo(dir, 'jpg');
  const args = ['-overwrite_original'];
  if (opts.orientation === 6) {
    const stored = nextPath(dir, 'jpg');
    await sharp(await readFile(preview))
      .rotate(270)
      .jpeg({ quality: 90 })
      .toFile(stored);
    preview = stored;
    args.push('-IFD0:Orientation#=6');
  }
  const et = new ExifTool({ maxProcs: 1 });
  try {
    await et.write(p, {}, { writeArgs: [`-PreviewImage<=${preview}`, ...args] });
  } finally {
    await et.end();
  }
  return p;
}

/** A small fixed JPEG (32×24, scene 7), byte-identical on every run. Used by Tasks 6 and 9. */
export const JPEG_A: Buffer = Buffer.from(
  [
    '/9j/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGB',
    'IUFRT/2wBDAQMEBAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU',
    'FBQUFBT/wAARCAAYACADASIAAhEBAxEB/8QAGQAAAgMBAAAAAAAAAAAAAAAAAAUDBAYH/8QAIBAAAgEEAgMBAAAAAA',
    'AAAAAAAQIAAwQFESExBhIUQf/EABgBAQADAQAAAAAAAAAAAAAAAAcEBQYI/8QAIBEAAAUFAAMAAAAAAAAAAAAAAAEC',
    'AwQFBhETIRIiQf/aAAwDAQACEQMRAD8A1lPKo1rsEHic+zOcavkGpnrcixGcqVUVWJ0eI8Hiy5FfpGt63ES1apHSs9',
    'pBJtRtqky9zpZyM8zbHMpVlBaMcpQ+Jyn6IsVvdppa3U2H0YQEm5KtHkM+g0d1ibWyxiVUce2txbaecPZ02ohtjqEI',
    'VTyKM4k2ecHPFvSnltr81Z79Cq9yzZGqWJ7k1nTLEbhCRdziy6YuZkl1XDMf/9k=',
  ].join(''),
  'base64',
);

/** QuickTime box: 32-bit size, 4-char type, payload. */
const qtBox = (type: string, payload: Buffer): Buffer => {
  const head = Buffer.alloc(8);
  head.writeUInt32BE(8 + payload.length, 0);
  head.write(type, 4, 'latin1');
  return Buffer.concat([head, payload]);
};

/**
 * A minimal QuickTime movie: a `ftyp` box (major brand `qt  `, minor 0, compatible `qt  `)
 * followed by an empty `mdat`. exiftool reports `FileType MOV`, `MIMEType video/quicktime`.
 */
export const MOV_BYTES: Buffer = Buffer.concat([
  qtBox(
    'ftyp',
    Buffer.concat([Buffer.from('qt  ', 'latin1'), Buffer.alloc(4), Buffer.from('qt  ', 'latin1')]),
  ),
  qtBox('mdat', Buffer.alloc(0)),
]);
