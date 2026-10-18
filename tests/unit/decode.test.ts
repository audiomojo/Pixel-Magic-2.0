import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import sharp from 'sharp';
import { afterAll, expect, test } from 'vitest';
import workerpool from 'workerpool';
import {
  computeImageFacts,
  computeImageFactsWire,
  endDecodeResources,
  QuarantineError,
} from '../../src/decode.js';
import { hamming } from '../../src/hashing.js';
import {
  bmpFile,
  bombPng,
  HEIC_SCENE0,
  JPEG_A,
  photo,
  randomBytes,
  rawWithPreview,
  truncatedJpeg,
} from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';

afterAll(endDecodeResources);

test('hash stable across format and size', async () => {
  const d = await tmpDir();
  const base = await computeImageFacts(await photo(d, 'jpg'), 'jpg');
  for (const [p, e] of [
    [await photo(d, 'png'), 'png'],
    [await photo(d, 'jpg', { scale: 0.5 }), 'jpg'],
    [await photo(d, 'webp'), 'webp'],
  ] as const)
    expect(hamming(base.phash, (await computeImageFacts(p, e)).phash)).toBeLessThanOrEqual(4);
});

test('EXIF orientation applied', async () => {
  const d = await tmpDir();
  const up = await computeImageFacts(await photo(d, 'jpg'), 'jpg');
  const tagged = await computeImageFacts(await photo(d, 'jpg', { orientation: 6 }), 'jpg');
  expect(hamming(up.phash, tagged.phash)).toBeLessThanOrEqual(4);
  expect([tagged.width, tagged.height]).toEqual([up.width, up.height]);
});

test('dimensions are oriented original, not the 32×32 resize', async () => {
  const f = await computeImageFacts(
    await photo(await tmpDir(), 'jpg', { size: [4032, 3024] }),
    'jpg',
  );
  expect([f.width, f.height]).toEqual([4032, 3024]);
});

test('different photos are far apart', async () => {
  const d = await tmpDir();
  const [a, b] = [
    await computeImageFacts(await photo(d, 'jpg', { scene: 1 }), 'jpg'),
    await computeImageFacts(await photo(d, 'jpg', { scene: 2 }), 'jpg'),
  ];
  expect(hamming(a.phash, b.phash)).toBeGreaterThan(16);
  expect(hamming(a.dhash, b.dhash)).toBeGreaterThan(16);
});

test('wire form carries hashes as decimal strings', async () => {
  const p = await photo(await tmpDir(), 'png');
  const [f, w] = [await computeImageFacts(p, 'png'), await computeImageFactsWire(p, 'png')];
  expect(w).toEqual({ ...f, phash: f.phash.toString(), dhash: f.dhash.toString() });
});

test('QuarantineError keeps name and reason as own properties', () => {
  const e = new QuarantineError('truncated');
  expect(Object.keys(e)).toEqual(expect.arrayContaining(['name', 'reason']));
  expect([e.name, e.reason]).toEqual(['QuarantineError', 'truncated']);
});

test.each([
  [truncatedJpeg, 'truncated'],
  [randomBytes, 'decode_error'],
  [bombPng, 'decompression_bomb'],
] as const)('quarantine reason %#', async (make, reason) =>
  expect(computeImageFacts(await make(await tmpDir()), 'jpg')).rejects.toMatchObject({ reason }),
);

test.each([
  ['jpeg', 'image/jpeg'],
  ['png', 'image/png'],
  ['webp', 'image/webp'],
  ['tif', 'image/tiff'],
  ['gif', 'image/gif'],
])('.%s decodes as %s', async (ext, mime) => {
  const d = await tmpDir();
  const base = await computeImageFacts(await photo(d, 'png'), 'png');
  const f = await computeImageFacts(await photo(d, ext), ext.toUpperCase());
  expect(f).toMatchObject({ width: 640, height: 480, mime });
  expect(hamming(base.phash, f.phash)).toBeLessThanOrEqual(4);
});

test.each([
  [24, false],
  [32, false],
  [24, true],
] as const)('%i-bit BMP (top-down %s) decodes in-process', async (bpp, topDown) => {
  const d = await tmpDir();
  const png = await computeImageFacts(await photo(d, 'png', { size: [64, 48] }), 'png');
  const f = await computeImageFacts(await bmpFile(d, { bpp, topDown }), 'bmp');
  expect(f).toMatchObject({ width: 64, height: 48, mime: 'image/bmp' });
  expect(hamming(png.phash, f.phash)).toBeLessThanOrEqual(4);
  expect(hamming(png.dhash, f.dhash)).toBeLessThanOrEqual(6);
});

test('HEIC decodes through heic-decode', async () => {
  const png = await computeImageFacts(
    await photo(await tmpDir(), 'png', { size: [320, 240] }),
    'png',
  );
  const f = await computeImageFacts(HEIC_SCENE0, 'HEIC');
  expect(f).toMatchObject({ width: 320, height: 240, mime: 'image/heic' });
  expect(hamming(png.phash, f.phash)).toBeLessThanOrEqual(4);
});

test('RAW is hashed from its embedded preview, not the main image', async () => {
  const d = await tmpDir();
  const preview = await computeImageFacts(await photo(d, 'jpg'), 'jpg');
  const f = await computeImageFacts(await rawWithPreview(d), 'dng');
  expect(f).toMatchObject({ width: 640, height: 480, mime: 'image/x-adobe-dng' });
  expect(hamming(preview.phash, f.phash)).toBeLessThanOrEqual(4);
}, 30_000);

test('a missing RAW source is a transient error, not a quarantine', async () => {
  const err: unknown = await computeImageFacts(join(await tmpDir(), 'gone.dng'), 'dng').catch(
    (e: unknown) => e,
  );
  expect(err).toMatchObject({ code: 'ENOENT' });
  expect(err).not.toBeInstanceOf(QuarantineError);
});

test('a BMP over the pixel limit is a decompression bomb', async () => {
  const d = await tmpDir();
  const h = bmpHeader(24, 0);
  h.writeInt32LE(20_000, 18);
  h.writeInt32LE(20_000, 22);
  await writeFile(join(d, 'big.bmp'), h);
  await expect(computeImageFacts(join(d, 'big.bmp'), 'bmp')).rejects.toMatchObject({
    reason: 'decompression_bomb',
  });
});

test('BMP variants the in-process decoder cannot read are unsupported', async () => {
  const d = await tmpDir();
  const rle = bmpHeader(8, 1); // 8-bit RLE8
  await writeFile(join(d, 'rle.bmp'), rle);
  await expect(computeImageFacts(join(d, 'rle.bmp'), 'bmp')).rejects.toMatchObject({
    name: 'QuarantineError',
    reason: 'unsupported',
  });
});

test('a BMP whose pixel data is cut short is truncated', async () => {
  const d = await tmpDir();
  const full = await readFile(await bmpFile(d, { bpp: 24 }));
  await writeFile(join(d, 'short.bmp'), full.subarray(0, full.length - 100));
  await expect(computeImageFacts(join(d, 'short.bmp'), 'bmp')).rejects.toMatchObject({
    reason: 'truncated',
  });
});

test('content is sniffed: a JPEG named .heic or .bmp still decodes as JPEG', async () => {
  const d = await tmpDir();
  for (const ext of ['heic', 'bmp']) {
    const p = join(d, `a.${ext}`);
    await writeFile(p, JPEG_A);
    expect(await computeImageFacts(p, ext)).toMatchObject({
      width: 32,
      height: 24,
      mime: 'image/jpeg',
    });
  }
});

test('unknown extensions and non-photo formats are unsupported', async () => {
  const d = await tmpDir();
  const p = join(d, 'a.mov');
  await writeFile(p, JPEG_A);
  await expect(computeImageFacts(p, 'mov')).rejects.toMatchObject({ reason: 'unsupported' });
  const svg = join(d, 'a.png');
  await writeFile(svg, '<svg xmlns="http://www.w3.org/2000/svg" width="8" height="8"/>');
  await expect(computeImageFacts(svg, 'png')).rejects.toMatchObject({ reason: 'unsupported' });
});

test('a RAW file without an embedded preview is a decode_error', async () => {
  const d = await tmpDir();
  const p = join(d, 'a.dng');
  await writeFile(p, await sharp(JPEG_A).png().toBuffer());
  await expect(computeImageFacts(p, 'dng')).rejects.toMatchObject({ reason: 'decode_error' });
});

test('a corrupt HEIC container is a decode_error', async () => {
  const d = await tmpDir();
  const p = join(d, 'a.heic');
  const ftyp = Buffer.from('00000018667479706865696300000000686569636d696631', 'hex');
  await writeFile(p, Buffer.concat([ftyp, Buffer.alloc(64, 7)]));
  await expect(computeImageFacts(p, 'heic')).rejects.toMatchObject({ reason: 'decode_error' });
});

test('decodeWorker serves computeImageFacts from a process pool', async () => {
  const worker = fileURLToPath(new URL('../../src/decodeWorker.ts', import.meta.url));
  const pool = workerpool.pool(worker, {
    workerType: 'process',
    maxWorkers: 1,
    forkOpts: { execArgv: ['--import', 'tsx'] },
  });
  try {
    const d = await tmpDir();
    const p = await photo(d, 'png');
    expect(await pool.exec('computeImageFacts', [p, 'png'])).toEqual(
      await computeImageFactsWire(p, 'png'),
    );
    await expect(
      pool.exec('computeImageFacts', [await truncatedJpeg(d), 'jpg']),
    ).rejects.toMatchObject({ name: 'QuarantineError', reason: 'truncated' });
  } finally {
    await pool.terminate();
  }
}, 30_000);

/** A bare BMP header (no pixels) with the given bit depth and compression. */
function bmpHeader(bpp: number, compression: number): Buffer {
  const h = Buffer.alloc(54);
  h.write('BM', 0, 'latin1');
  h.writeUInt32LE(54, 2);
  h.writeUInt32LE(54, 10);
  h.writeUInt32LE(40, 14);
  h.writeInt32LE(4, 18);
  h.writeInt32LE(4, 22);
  h.writeUInt16LE(1, 26);
  h.writeUInt16LE(bpp, 28);
  h.writeUInt32LE(compression, 30);
  return h;
}
