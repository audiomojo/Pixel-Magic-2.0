import { expect, test } from 'vitest';
import type { ContentRow } from '../lib/db.js';
import {
  blockedFileFinding,
  duplicateRowFindings,
  FINAL_STILL_STATES,
  purgedRowFindings,
  vaultCountFinding,
} from '../lib/invariant.js';

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
  purgedAt: null,
  purgeRemovedAt: null,
  purgeMismatch: false,
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

test('an expected purge, removed, is in the vault zero times; any other purge keeps the once rule', () => {
  const purged = row('a', { state: 'PURGED', purgedAt: AT, purgeRemovedAt: AT });
  const expected = new Set(['a']);
  expect(vaultCountFinding('a', 0, purged, expected)).toBeNull();
  expect(vaultCountFinding('a', 1, purged, expected)).toBe(
    'bytes a: in the vault 1 times, expected zero times (PURGED, removed)',
  );
  // Not an expected purge: its missing vault file is reported, not explained away.
  expect(vaultCountFinding('a', 0, purged)).toBe(
    'bytes a: in the vault 0 times, expected exactly once',
  );
  // Purged but removal pending: the file is still there, once.
  const pending = row('a', { state: 'PURGED', purgedAt: AT });
  expect(vaultCountFinding('a', 1, pending, expected)).toBeNull();
});

test('after idle exactly the expected purges are PURGED, removed and unflagged', () => {
  const ok = row('a', { state: 'PURGED', purgedAt: AT, purgeRemovedAt: AT });
  expect(purgedRowFindings(new Map([['a', ok]]), new Set(['a']))).toEqual([]);
  const contents = new Map([
    ['a', ok],
    ['b', row('b', { state: 'PURGED', purgedAt: AT })],
    ['c', row('c', { state: 'PURGED', purgedAt: AT, purgeRemovedAt: AT, purgeMismatch: true })],
    ['d', row('d')],
  ]);
  expect(purgedRowFindings(contents, new Set(['b', 'c', 'd', 'e']))).toEqual([
    'content a: PURGED, but in no expected purge',
    'content b: PURGED, vault file not removed after idle',
    'content c: PURGED, flagged purge_mismatch',
    'content d: expected PURGED, got CLUSTERED',
    'content e: expected PURGED, has no contents row',
  ]);
});

test('a BLOCKED drop is final only for the bytes of an expected purge', () => {
  const f = { id: 7n, relPath: 'x/a.jpg', state: 'BLOCKED', sha256: 'a' };
  expect(blockedFileFinding(f, new Set(['a']))).toBeNull();
  expect(blockedFileFinding(f, new Set())).toBe(
    'files 7 x/a.jpg: BLOCKED, but its sha a is in no expected purge',
  );
  expect(blockedFileFinding({ ...f, state: 'INGESTED' }, new Set())).toBeNull();
});
