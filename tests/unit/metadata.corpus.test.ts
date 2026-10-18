import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { MetadataReader } from '../../src/metadata.js';
import { C, hasCorpus } from '../helpers/corpus.js';

describe.skipIf(!hasCorpus)('metadata (real corpus)', () => {
  let reader: MetadataReader;
  beforeAll(() => {
    reader = new MetadataReader();
  });
  afterAll(async () => {
    await reader.end();
  });

  test.each([
    ['IMG_E8719.HEIC', 'still', 'jpg'],
    ['IMG_0004.MOV', 'motion', 'mov'],
    ['VERP1022.TIF', 'still', 'tif'],
    ['AUXA4949.BMP', 'still', 'bmp'],
  ])('real %s', async (n, kind, ext) => {
    const m = await reader.read(C(n));
    expect([m.kind, m.ext]).toEqual([kind, ext]);
  });
  test('AAE is other', async () =>
    expect((await reader.read(C('IMG_0066.AAE'))).kind).toBe('other'));
  test('a Live Photo pair shares a content id', async () => {
    const [h, m] = [
      await reader.read(C('IMG_2834 1.HEIC')),
      await reader.read(C('IMG_2834 1.MOV')),
    ];
    expect(h.liveContentId).not.toBeNull();
    expect(m.liveContentId).toBe(h.liveContentId);
  });
  test('HEIC/JPG twin share capturedAt', async () => {
    // IMG_0034.HEIC/.JPG are NOT twins (different phones, years apart)
    const [h, j] = [await reader.read(C('IMG_6941 1.HEIC')), await reader.read(C('IMG_E6941.JPG'))];
    expect(h.capturedAt).not.toBeNull();
    expect(j.capturedAt?.getTime()).toBe(h.capturedAt?.getTime());
  });
});
