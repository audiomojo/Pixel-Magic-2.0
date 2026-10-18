// I1: a cluster cache reload must stay linear in the number of contents. It used to run one
// full look-alike scan per key-less content (O(key-less × all)), inside the cluster
// transaction, which at 300k+ contents outlasts both the activity and the idle-tx timeouts.
import { afterEach, expect, test, vi } from 'vitest';
import { rebuildCache, type ContentRow } from '../../src/activities/cluster.js';
import { DEFAULT_THRESHOLDS } from '../../src/classify.js';
import { HashIndex } from '../../src/hashIndex.js';

afterEach(() => vi.restoreAllMocks());

const hex = (i: number) => i.toString(16).padStart(64, '0');
const mix = (i: number): bigint => BigInt.asUintN(63, BigInt(i + 1) * 0x9e3779b97f4a7c15n);

/** `n` clustered contents, every one key-less (the worst case for the old reload), one group each. */
function rows(n: number): {
  groups: { id: bigint; merged_into: null; similar_set_id: null }[];
  contents: ContentRow[];
} {
  const contents = Array.from({ length: n }, (_, i): ContentRow => ({
    sha256: hex(i),
    state: 'CLUSTERED',
    phash: mix(i),
    dhash: mix(i + n),
    width: 100,
    height: 100,
    mime: 'image/jpeg',
    captured_at: null,
    subsec: null,
    camera: null,
    burst_uuid: null,
    copy_group_id: BigInt(i + 1),
    copy_ambiguous: false,
  }));
  return {
    groups: contents.map((_, i) => ({
      id: BigInt(i + 1),
      merged_into: null,
      similar_set_id: null,
    })),
    contents,
  };
}

test('perf: a reload of 5,000 key-less contents runs no look-alike scan and stays fast', () => {
  const scans = vi.spyOn(HashIndex.prototype, 'matches');
  const { groups, contents } = rows(5000);
  const beats: number[] = [];
  const t0 = performance.now();
  const cache = rebuildCache(
    groups,
    contents,
    DEFAULT_THRESHOLDS,
    () => 1,
    (n) => beats.push(n),
  );
  const ms = performance.now() - t0;
  expect(scans).not.toHaveBeenCalled(); // was 5,000 full scans of 5,000 rows
  expect(cache.index.size).toBe(5000);
  expect(beats).toEqual([1000, 2000, 3000, 4000, 5000]);
  expect(ms).toBeLessThan(2000);
});

test('copy edges of a reloaded content are computed on demand from the index', () => {
  const { groups, contents } = rows(3);
  // Content 1 is a resized copy of content 0 (same hashes, other dimensions); 2 is unrelated.
  contents[1] = {
    ...contents[1]!,
    phash: contents[0]!.phash,
    dhash: contents[0]!.dhash,
    width: 50,
  };
  const cache = rebuildCache(groups, contents, DEFAULT_THRESHOLDS, () => 1);
  expect([...cache.edgesOf(hex(0))]).toEqual([hex(1)]);
  expect([...cache.edgesOf(hex(2))]).toEqual([]);
  expect([...cache.edgesOf('unknown')]).toEqual([]);
});
