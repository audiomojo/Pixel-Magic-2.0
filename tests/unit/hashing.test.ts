import fc from 'fast-check';
import { expect, test } from 'vitest';
import { hamming, toSigned, toUnsigned } from '../../src/hashing.js';

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
