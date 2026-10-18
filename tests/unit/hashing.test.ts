import fc from 'fast-check';
import { expect, test } from 'vitest';
import { dhashFromGray, hamming, phashFromGray, toSigned, toUnsigned } from '../../src/hashing.js';

test('toSigned/toUnsigned round-trip', () =>
  fc.assert(
    fc.property(
      fc.bigInt({ min: 0n, max: 2n ** 64n - 1n }),
      (u) =>
        toUnsigned(toSigned(u)) === u && toSigned(u) >= -(2n ** 63n) && toSigned(u) < 2n ** 63n,
    ),
  ));

test('toSigned maps the top bit to negative', () => {
  expect(toSigned(2n ** 64n - 1n)).toBe(-1n);
  expect(toSigned(2n ** 63n - 1n)).toBe(2n ** 63n - 1n);
  expect(toUnsigned(-1n)).toBe(2n ** 64n - 1n);
});

test('hamming', () => {
  expect(hamming(0n, 2n ** 64n - 1n)).toBe(64);
  expect(hamming(0b1011n, 0b0001n)).toBe(2);
  expect(hamming(2n ** 63n, 2n ** 31n)).toBe(2);
  expect(hamming(0xdeadbeefcafebaben, 0xdeadbeefcafebaben)).toBe(0);
});

test('hamming matches a bit-by-bit count', () =>
  fc.assert(
    fc.property(
      fc.bigInt({ min: 0n, max: 2n ** 64n - 1n }),
      fc.bigInt({ min: 0n, max: 2n ** 64n - 1n }),
      (a, b) => hamming(a, b) === [...(a ^ b).toString(2)].filter((c) => c === '1').length,
    ),
  ));

/**
 * 32×32 separable gradient g(x)·h(y) with g = (x/31)², h = (y/31)³. Its DCT is
 * C[v][u] = G[u]·H[v], and both factors of an increasing power ramp alternate in sign
 * (+, −, +, …), so sign(C[v][u]) = (−1)^(u+v): 32 positive and 32 negative coefficients.
 * The median sits between them, so bit = (u+v even) → rows 0xaa, 0x55, 0xaa, … (hand check).
 */
const gradient32 = (): Uint8Array =>
  Uint8Array.from({ length: 32 * 32 }, (_, i) => {
    const [x, y] = [i % 32, Math.floor(i / 32)];
    return Math.floor(255 * (x / 31) ** 2 * (y / 31) ** 3);
  });

/** 9×8 raster whose every row strictly increases left to right. */
const rampRows9x8 = (): Uint8Array => Uint8Array.from({ length: 9 * 8 }, (_, i) => (i % 9) * 20);

const GOLDEN_PHASH = 0xaa55aa55aa55aa55n;

test('phash of known gradient is stable (golden)', () =>
  expect(phashFromGray(gradient32())).toBe(GOLDEN_PHASH));
test('dhash bit order', () => expect(dhashFromGray(rampRows9x8())).toBe(0xffffffffffffffffn));

test('dhash: first bit is the MSB, row-major', () => {
  const px = new Uint8Array(9 * 8); // all equal → no bit set
  px[1] = 1; // row 0, x=0: px[1] > px[0]
  expect(dhashFromGray(px)).toBe(1n << 63n);
  const last = new Uint8Array(9 * 8);
  last[7 * 9 + 8] = 1; // row 7, x=7: px[8] > px[7]
  expect(dhashFromGray(last)).toBe(1n);
});

test('phash is invariant to uniform brightness offset and gain', () => {
  const g = gradient32();
  expect(phashFromGray(g.map((v) => Math.floor(v / 2) + 40))).toBe(phashFromGray(g));
});

test('hash functions reject wrong raster sizes', () => {
  expect(() => phashFromGray(new Uint8Array(31 * 32))).toThrow(/1024/);
  expect(() => dhashFromGray(new Uint8Array(8 * 8))).toThrow(/72/);
});
