import { mkdir, readdir, readFile, stat, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import sharp from 'sharp';
import { describe, expect, test } from 'vitest';
import { GuardedFs, OutsideAllowedRootError } from '../../src/harvest/paths.js';
import { evictThumbs, makeThumb } from '../../src/harvest/thumbs.js';
import { HEIC_SCENE0 } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';

const png = (w: number, h: number): Promise<Buffer> =>
  sharp({ create: { width: w, height: h, channels: 3, background: '#3377cc' } })
    .png()
    .toBuffer();

async function cacheEnv() {
  const d = await tmpDir();
  const cache = join(d, 'harvest-cache', 'thumbs');
  return { d, cache, fs: new GuardedFs([join(d, 'harvest-cache')]) };
}

describe('makeThumb', () => {
  test('writes a 256 px JPEG that fits inside 256x256', async () => {
    const e = await cacheEnv();
    const src = join(e.d, 'a.png');
    await writeFile(src, await png(800, 400));
    const out = join(e.cache, '1.jpg');
    await makeThumb(src, out, e.fs);
    const meta = await sharp(out).metadata();
    expect(meta).toMatchObject({ format: 'jpeg', width: 256, height: 128 });
    expect(await readdir(e.cache)).toEqual(['1.jpg']); // no .part left behind
  });

  test('decodes HEIC through heic-decode', async () => {
    const e = await cacheEnv();
    const out = join(e.cache, '2.jpg');
    await makeThumb(HEIC_SCENE0, out, e.fs);
    expect(await sharp(out).metadata()).toMatchObject({ format: 'jpeg', width: 256, height: 192 });
  });

  test('accepts a Buffer path (raw on-disk name)', async () => {
    const e = await cacheEnv();
    const src = join(e.d, 'b.png');
    await writeFile(src, await png(50, 50));
    const out = join(e.cache, '3.jpg');
    await makeThumb(Buffer.from(src), out, e.fs);
    expect((await sharp(out).metadata()).width).toBe(256);
  });

  test('a decode failure throws and leaves no file or part', async () => {
    const e = await cacheEnv();
    const src = join(e.d, 'bad.png');
    await writeFile(src, Buffer.from('not an image at all'));
    await expect(makeThumb(src, join(e.cache, '4.jpg'), e.fs)).rejects.toThrow();
    const bad = join(e.d, 'bad.heic');
    await writeFile(
      bad,
      Buffer.concat([Buffer.alloc(4), Buffer.from('ftypheic'), Buffer.alloc(40)]),
    );
    await expect(makeThumb(bad, join(e.cache, '5.jpg'), e.fs)).rejects.toThrow();
    expect(await readdir(e.cache).catch(() => [])).toEqual([]);
  });

  test('refuses to write outside the guarded root', async () => {
    const e = await cacheEnv();
    const src = join(e.d, 'a.png');
    await writeFile(src, await png(10, 10));
    await expect(makeThumb(src, join(e.d, 'elsewhere.jpg'), e.fs)).rejects.toBeInstanceOf(
      OutsideAllowedRootError,
    );
  });
});

test('a failed publish removes the part and rethrows', async () => {
  const e = await cacheEnv();
  const src = join(e.d, 'a.png');
  await writeFile(src, await png(10, 10));
  await mkdir(join(e.cache, 'taken.jpg'), { recursive: true }); // rename onto a directory fails
  await expect(makeThumb(src, join(e.cache, 'taken.jpg'), e.fs)).rejects.toThrow();
  expect(await readdir(e.cache)).toEqual(['taken.jpg']);
});

describe('evictThumbs', () => {
  test('a path that is not a directory is an error, not "empty"', async () => {
    const e = await cacheEnv();
    const file = join(e.d, 'plain');
    await writeFile(file, 'x');
    await expect(evictThumbs(file, 0)).rejects.toThrow();
  });

  async function fill(dir: string, n: number, size: number): Promise<void> {
    await mkdir(dir, { recursive: true });
    for (let i = 0; i < n; i++) {
      const p = join(dir, `${i}.jpg`);
      await writeFile(p, Buffer.alloc(size));
      const t = new Date(Date.now() - (n - i) * 10_000); // 0 is the oldest
      await utimes(p, t, t);
    }
  }

  test('keeps the cache under the cap, oldest first', async () => {
    const e = await cacheEnv();
    await fill(e.cache, 10, 100);
    const freed = await evictThumbs(e.cache, 450, e.fs);
    expect(freed).toBe(600);
    expect((await readdir(e.cache)).sort()).toEqual(['6.jpg', '7.jpg', '8.jpg', '9.jpg']);
  });

  test('does nothing under the cap, and for a missing directory', async () => {
    const e = await cacheEnv();
    await fill(e.cache, 3, 100);
    expect(await evictThumbs(e.cache, 1000, e.fs)).toBe(0);
    expect(await readdir(e.cache)).toHaveLength(3);
    expect(await evictThumbs(join(e.d, 'nope'), 10)).toBe(0);
  });

  test('a cap of 0 empties the cache except parts; default guard covers the directory', async () => {
    const e = await cacheEnv();
    await fill(e.cache, 3, 10);
    await writeFile(join(e.cache, 'x.part'), 'p'); // in-progress writes are never evicted
    expect(await evictThumbs(e.cache, 0)).toBe(30);
    expect(await readdir(e.cache)).toEqual(['x.part']);
    expect((await stat(e.cache)).isDirectory()).toBe(true);
    expect(await readFile(HEIC_SCENE0).then((b) => b.length)).toBeGreaterThan(0);
  });
});
