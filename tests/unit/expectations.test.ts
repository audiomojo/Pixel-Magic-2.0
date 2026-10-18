import { expect, test } from 'vitest';
import {
  copyIfTightSummary,
  resolveCopyIfTight,
  TIGHT_BY_CONSTRUCTION,
} from '../lib/expectations.js';

test('a copy-if-tight variant of a kind tight by construction must be a copy, however it measures', () => {
  expect([...TIGHT_BY_CONSTRUCTION]).toEqual(['png']);
  expect(resolveCopyIfTight('png', true)).toBe('copy');
  expect(resolveCopyIfTight('png', false)).toBe('copy');
});

test('other copy-if-tight variants follow their measured distance', () => {
  for (const v of ['resize50', 'q60-noexif', 'bridge'] as const) {
    expect(resolveCopyIfTight(v, true)).toBe('copy');
    expect(resolveCopyIfTight(v, false)).toBe('similar');
  }
  expect(resolveCopyIfTight(undefined, false)).toBe('similar');
});

test('the copy-if-tight summary counts the variants that came out similar, by kind', () => {
  expect(copyIfTightSummary([])).toBe('copy-if-tight: 0 of 0 variants resolved similar');
  expect(
    copyIfTightSummary([
      { variant: 'resize50', tight: false, at: 'abcd1234 pHash 2 dHash 7' },
      { variant: 'png', tight: true, at: 'x' },
      { variant: 'q60-noexif', tight: false, at: 'ef015678 pHash 5 dHash 9' },
      { variant: 'resize50', tight: true, at: 'y' },
    ]),
  ).toBe(
    'copy-if-tight: 2 of 4 variants resolved similar (q60-noexif 1, resize50 1): ' +
      'q60-noexif ef015678 pHash 5 dHash 9; resize50 abcd1234 pHash 2 dHash 7',
  );
});
