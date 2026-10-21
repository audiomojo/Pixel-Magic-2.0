import { expect, test } from 'vitest';
import { looksAlike } from '../../src/classify.js';
import { HashIndex } from '../../src/hashIndex.js';
import { F, T } from '../helpers/facts.js';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);
/** Well-spread unsigned 64-bit values. */
const mix = (i: number) => BigInt.asUintN(64, BigInt(i + 1) * 0x9e3779b97f4a7c15n);

test('index matches within thresholds', () => {
  const idx = new HashIndex();
  idx.append([F('a'), F('b', { phash: 0x1ffn })]);
  expect(idx.matches(F('n'), T).map((m) => m.sha256)).toEqual(['a']);
});

test('index grows past initial capacity', () => {
  const rows = range(10_000).map((i) => F(`s${i}`, { phash: mix(i), dhash: mix(i + 20_000) }));
  const idx = new HashIndex();
  idx.append(rows);
  expect(
    idx.matches(F('q', { phash: mix(5000), dhash: mix(25_000) }), T).map((m) => m.sha256),
  ).toEqual(['s5000']);
});

test('index compares both 32-bit halves of both hashes', () => {
  const hi = 0xff00_0000_0000_0000n; // 8 high bits: at the pHash limit, over a tighter one
  const idx = new HashIndex();
  idx.append([
    F('p-hi', { phash: hi }),
    F('p-lo', { phash: 0x1ffn }),
    F('d-hi', { dhash: 0xffe0_0000_0000_0000n }),
    F('d-lo', { dhash: 0x7ffn }),
  ]);
  expect(idx.matches(F('n'), T).map((m) => m.sha256)).toEqual(['p-hi']);
  expect(idx.matches(F('n'), { ...T, lookAlikePhash: 7 })).toEqual([]);
});

test('index agrees with looksAlike, appends in batches, and clears', () => {
  const rows = range(3000).map((i) => F(`r${i}`, { phash: mix(i % 50), dhash: mix(i % 7) }));
  const idx = new HashIndex();
  for (let i = 0; i < rows.length; i += 700) {
    idx.append(rows.slice(i, i + 700));
  }
  const q = F('q', { phash: mix(3) ^ 0b101n, dhash: mix(3) });
  const want = rows.filter((r) => looksAlike(q, r, T)).map((r) => r.sha256);
  expect(want.length).toBeGreaterThan(0);
  expect(idx.matches(q, T).map((m) => m.sha256)).toEqual(want);
  expect(idx.size).toBe(3000);
  idx.clear();
  expect(idx.size).toBe(0);
  expect(idx.matches(q, T)).toEqual([]);
});
