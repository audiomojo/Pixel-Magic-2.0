import { expect, test } from 'vitest';
import {
  copyIfTightSummary,
  duplicateFamilyFindings,
  resolveCopyIfTight,
  TIGHT_BY_CONSTRUCTION,
  type FamilyMember,
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

const seedM: FamilyMember = { at: 'S.JPG', sha256: 's', state: 'CLUSTERED', duplicateOf: null };
const pngM: FamilyMember = { at: 'synth-png', sha256: 'p', state: 'DUPLICATE', duplicateOf: 's' };
const metaM: FamilyMember = { at: 'synth-meta', sha256: 'm', state: 'DUPLICATE', duplicateOf: 's' };

test('a pixel-identical family holds: the expected keeper stays CLUSTERED, every other member is its DUPLICATE', () => {
  expect(duplicateFamilyFindings([seedM, pngM, metaM], 's')).toEqual([]);
  // The keeper can be a variant (a lossless PNG larger than its JPEG seed wins `larger`).
  const pngKept = [
    { ...seedM, state: 'DUPLICATE', duplicateOf: 'p' },
    { ...pngM, state: 'CLUSTERED', duplicateOf: null },
    { ...metaM, duplicateOf: 'p' },
  ];
  expect(duplicateFamilyFindings(pngKept, 'p')).toEqual([]);
  // No expected keeper (only ingest order would decide): any one survivor will do.
  expect(duplicateFamilyFindings(pngKept, null)).toEqual([]);
});

test('a pixel-identical family that was not retired as expected is reported', () => {
  expect(
    duplicateFamilyFindings([seedM, { ...pngM, state: 'CLUSTERED', duplicateOf: null }], 's'),
  ).toEqual([
    'identical family [S.JPG, synth-png]: expected one survivor, found 2 (S.JPG, synth-png)',
  ]);
  expect(
    duplicateFamilyFindings(
      [
        { ...seedM, state: 'DUPLICATE', duplicateOf: 'p' },
        { ...pngM, state: 'CLUSTERED', duplicateOf: null },
      ],
      's',
    ),
  ).toEqual(['identical family [S.JPG, synth-png]: expected S.JPG to stay, synth-png did']);
  expect(duplicateFamilyFindings([seedM, { ...metaM, duplicateOf: 'x' }], 's')).toEqual([
    'synth-meta: DUPLICATE of x, expected of S.JPG',
  ]);
  expect(duplicateFamilyFindings([{ ...seedM, state: 'HASHED' }, metaM], 's')).toEqual([
    'S.JPG: kept, expected CLUSTERED, got HASHED',
  ]);
  expect(
    duplicateFamilyFindings(
      [
        { ...seedM, state: 'DUPLICATE', duplicateOf: 'm' },
        { ...metaM, duplicateOf: 's' },
      ],
      null,
    ),
  ).toEqual(['identical family [S.JPG, synth-meta]: expected one survivor, found 0 ()']);
});
