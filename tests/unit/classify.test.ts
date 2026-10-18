import { expect, test } from 'vitest';
import { captureConflict, classify, looksAlike, type Facts } from '../../src/classify.js';
import { F, NOEXIF, T } from '../helpers/facts.js';

test.each<[Facts, Facts, 'copy' | 'similar']>([
  [F('a', { burstUuid: 'B1' }), F('b', { phash: 1n, burstUuid: 'B1' }), 'similar'], // same burst → never copy
  [F('a'), F('b', { captured: { s: 1_600_000_001, sub: '123' } }), 'similar'], // different second
  [F('a'), F('b', { captured: { s: 1_600_000_000, sub: '456' } }), 'similar'], // different subsec
  [F('a'), F('b', { camera: 'Canon' }), 'similar'], // different camera
  [F('a'), F('b', { phash: 0b111n, width: 2000, height: 1500, mime: 'image/jpeg' }), 'copy'], // same capture incl. subsec
  [
    F('a', { captured: { s: 1, sub: null } }),
    F('b', { phash: 3n, captured: { s: 1, sub: null } }),
    'similar',
  ], // no subsec, same dims+mime
  [
    F('a', { captured: { s: 1, sub: null } }),
    F('b', { phash: 3n, width: 2000, height: 1500, captured: { s: 1, sub: null } }),
    'copy',
  ],
  [F('a'), F('b', { phash: 0b1111n, dhash: 0b111111n, ...NOEXIF }), 'copy'], // EXIF-less export, tight
  [F('a'), F('b', { phash: 0b11111n, ...NOEXIF }), 'similar'], // EXIF-less, not tight
  [F('a', NOEXIF), F('b', { phash: 1n, ...NOEXIF }), 'similar'], // both EXIF-less, same dims
  [F('a', NOEXIF), F('b', { phash: 1n, width: 10, height: 10, ...NOEXIF }), 'copy'],
  // extra rows: table order and edge cases
  [F('a', { burstUuid: 'B1' }), F('b', { width: 2000, height: 1500, burstUuid: 'B1' }), 'similar'], // burst beats same capture
  [F('a', { burstUuid: 'B1' }), F('b', { width: 2000, burstUuid: 'B2' }), 'copy'], // different bursts don't block
  [F('a', { burstUuid: 'B1' }), F('b', { width: 2000 }), 'copy'], // one-sided burst doesn't block
  [F('a'), F('b', { camera: null, width: 2000 }), 'copy'], // camera missing on one side
  [F('a'), F('b', { captured: { s: 1_600_000_000, sub: null }, mime: 'image/jpeg' }), 'copy'], // subsec missing, mime differs
  [F('a'), F('b', { captured: { s: 1_600_000_000, sub: null } }), 'similar'], // subsec missing, same dims+mime
  [F('a'), F('b', { phash: 1n, dhash: 0b1111111n, ...NOEXIF }), 'similar'], // one-sided, dHash not tight
  [
    F('a', { captured: null }),
    F('b', { camera: 'Canon', captured: null, mime: 'image/jpeg' }),
    'similar',
  ], // cameras differ, no dates
  [F('a', NOEXIF), F('b', { phash: 0b11111n, mime: 'image/png', ...NOEXIF }), 'similar'], // neither, not tight
])('classify %#', (a, b, want) => {
  expect(classify(a, b, T)).toBe(want);
  expect(classify(b, a, T)).toBe(want); // symmetric
});

test('looksAlike bounds', () => {
  expect(looksAlike(F('a'), F('b', { phash: 0xffn, dhash: 0x3ffn }), T)).toBe(true);
  expect(looksAlike(F('a'), F('b', { phash: 0x1ffn }), T)).toBe(false);
  expect(looksAlike(F('a'), F('b', { dhash: 0x7ffn }), T)).toBe(false);
});

test('captureConflict', () => {
  expect(captureConflict({ s: 1, sub: '1' }, { s: 1, sub: '1' })).toBe(false);
  expect(captureConflict({ s: 1, sub: '1' }, { s: 1, sub: null })).toBe(false);
  expect(captureConflict({ s: 1, sub: null }, { s: 1, sub: null })).toBe(false);
  expect(captureConflict({ s: 1, sub: '1' }, { s: 1, sub: '2' })).toBe(true);
  expect(captureConflict({ s: 1, sub: null }, { s: 2, sub: null })).toBe(true);
});

test('ten burst frames are never copies of each other', () => {
  const frames = Array.from({ length: 10 }, (_, i) =>
    F(`f${i}`, { phash: BigInt(i & 3), burstUuid: 'BURST', captured: { s: 1, sub: String(i) } }),
  );
  for (const a of frames)
    for (const b of frames) if (a !== b) expect(classify(a, b, T)).toBe('similar');
});
