import { expect, test } from 'vitest';
import type { ContentRow } from '../lib/db.js';
import { duplicateRowFindings, FINAL_STILL_STATES, vaultCountFinding } from '../lib/invariant.js';

const row = (sha256: string, o: Partial<ContentRow> = {}): ContentRow => ({
  sha256,
  kind: 'still',
  mime: 'image/jpeg',
  state: 'CLUSTERED',
  quarantineReason: null,
  lastError: null,
  capturedAt: null,
  subsec: null,
  burstUuid: null,
  phash: 1n,
  dhash: 1n,
  ambiguous: false,
  copyGroup: 1,
  similarSet: null,
  duplicateOf: null,
  duplicateRemovedAt: null,
  duplicateMismatch: false,
  ...o,
});
const AT = new Date('2026-10-27T00:00:00Z');

test('a seeded sha is in the vault exactly once, or zero times once retired and removed', () => {
  expect(vaultCountFinding('a', 1, row('a'))).toBeNull();
  expect(vaultCountFinding('a', 0, row('a'))).toBe(
    'bytes a: in the vault 0 times, expected exactly once',
  );
  expect(vaultCountFinding('a', 2, undefined)).toBe(
    'bytes a: in the vault 2 times, expected exactly once',
  );
  const retired = row('a', { state: 'DUPLICATE', duplicateOf: 'k', duplicateRemovedAt: AT });
  expect(vaultCountFinding('a', 0, retired)).toBeNull();
  expect(vaultCountFinding('a', 1, retired)).toBe(
    'bytes a: in the vault 1 times, expected zero times (DUPLICATE, removed)',
  );
  // Retired but not yet removed: the file must still be there (once).
  const pending = row('a', { state: 'DUPLICATE', duplicateOf: 'k' });
  expect(vaultCountFinding('a', 1, pending)).toBeNull();
  expect(vaultCountFinding('a', 0, pending)).toBe(
    'bytes a: in the vault 0 times, expected exactly once',
  );
});

test('after idle every retired content is removed, unflagged, and points at a kept content', () => {
  const kept = row('k');
  const ok = row('a', { state: 'DUPLICATE', duplicateOf: 'k', duplicateRemovedAt: AT });
  expect(
    duplicateRowFindings(
      new Map([
        ['k', kept],
        ['a', ok],
      ]),
    ),
  ).toEqual([]);
  const contents = new Map([
    ['k', kept],
    ['b', row('b', { state: 'DUPLICATE', duplicateOf: 'k' })],
    [
      'c',
      row('c', {
        state: 'DUPLICATE',
        duplicateOf: 'k',
        duplicateRemovedAt: AT,
        duplicateMismatch: true,
      }),
    ],
    ['d', row('d', { state: 'DUPLICATE', duplicateOf: 'b', duplicateRemovedAt: AT })],
    ['e', row('e', { state: 'DUPLICATE', duplicateOf: 'zz', duplicateRemovedAt: AT })],
  ]);
  expect(duplicateRowFindings(contents)).toEqual([
    'content b: DUPLICATE of k, vault file not removed after idle',
    'content c: DUPLICATE of k, flagged duplicate_mismatch',
    'content d: DUPLICATE of b, which is itself DUPLICATE',
    'content e: DUPLICATE of zz, which has no contents row',
  ]);
});

test('a retired duplicate is a final still state (it is never clustered)', () => {
  expect([...FINAL_STILL_STATES].sort()).toEqual([
    'CLUSTERED',
    'DUPLICATE',
    'FAILED',
    'QUARANTINED',
  ]);
});
