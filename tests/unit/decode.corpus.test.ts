import { afterAll, describe, expect, test } from 'vitest';
import { computeImageFacts, endDecodeResources } from '../../src/decode.js';
import { hamming } from '../../src/hashing.js';
import { C, corpusNames, hasCorpus } from '../helpers/corpus.js';

afterAll(endDecodeResources);

const median = (xs: number[]): number => {
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? (s[m] ?? NaN) : ((s[m - 1] ?? NaN) + (s[m] ?? NaN)) / 2;
};

describe.skipIf(!hasCorpus)('decode on the sample corpus', () => {
  // IMG_0034.HEIC/.JPG share a name only (different cameras and years).
  // The corpus's one HEIC/JPG capture twin is "IMG_6941 1.HEIC" and its edit IMG_E6941.JPG
  // (same capture time and camera; the JPG is a smaller export).
  test('HEIC and its JPG twin hash within copy range', async () =>
    expect(
      hamming(
        (await computeImageFacts(C('IMG_6941 1.HEIC'), 'heic')).phash,
        (await computeImageFacts(C('IMG_E6941.JPG'), 'jpg')).phash,
      ),
    ).toBeLessThanOrEqual(4));

  test('IMG_0034 HEIC and JPG are different photos, not twins', async () =>
    expect(
      hamming(
        (await computeImageFacts(C('IMG_0034.HEIC'), 'heic')).phash,
        (await computeImageFacts(C('IMG_0034.JPG'), 'jpg')).phash,
      ),
    ).toBeGreaterThan(16));

  test('HEIC dimensions are the oriented original (irot applied)', async () =>
    expect(await computeImageFacts(C('IMG_0034.HEIC'), 'heic')).toMatchObject({
      width: 3024, // stored 4032×3024 with irot 90°
      height: 4032,
      mime: 'image/heic',
    }));

  test('misnamed JPEG decodes', async () =>
    expect((await computeImageFacts(C('IMG_E8719.HEIC'), 'jpg')).mime).toBe('image/jpeg'));

  test.each([
    ['VERP1022.TIF', 'tif'],
    ['AUXA4949.BMP', 'bmp'],
  ])('%s decodes', async (n, e) => {
    const f = await computeImageFacts(C(n), e);
    expect(f.width).toBeGreaterThan(0);
    expect(f.height).toBeGreaterThan(0);
  });

  test('HEIC decode benchmark', async () => {
    const names = corpusNames((n) => n.endsWith('.HEIC') && !n.startsWith('IMG_E')).slice(0, 20);
    const ms: number[] = [];
    for (const n of names) {
      const t = performance.now();
      await computeImageFacts(C(n), 'heic');
      ms.push(performance.now() - t);
    }
    console.log(`median HEIC ms: ${median(ms)}`);
    expect(names).toHaveLength(20);
    expect(median(ms)).toBeLessThanOrEqual(1500);
  }, 120_000);
});
