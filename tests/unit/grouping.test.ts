import fc from 'fast-check';
import { expect, test } from 'vitest';
import { classify, looksAlike, type Facts } from '../../src/classify.js';
import { Grouper, type Assignment } from '../../src/grouping.js';
import { F, NOEXIF, T } from '../helpers/facts.js';

const range = (n: number): number[] => Array.from({ length: n }, (_, i) => i);
const mix = (i: number): bigint => BigInt.asUintN(64, BigInt(i + 1) * 0x9e3779b97f4a7c15n);

/** Id allocator for Grouper; starts high so fresh ids never undercut ids loaded from "the DB". */
const counter = (start = 1) => {
  let n = start;
  return () => n++;
};

function permutations<X>(xs: readonly X[]): X[][] {
  if (xs.length <= 1) return [[...xs]];
  return xs.flatMap((x, i) =>
    permutations([...xs.slice(0, i), ...xs.slice(i + 1)]).map((p) => [x, ...p]),
  );
}

const key = (i: number, j: number) => (i < j ? `${i}-${j}` : `${j}-${i}`);

/** Adds facts in `order`, matching each against every earlier one that is an edge (like HashIndex would). */
function runGrouper(edges: ReadonlySet<string>, facts: readonly Facts[], order: readonly number[]) {
  const g = new Grouper(counter());
  const seen: number[] = [];
  for (const i of order) {
    const f = facts[i]!;
    const matches = seen
      .filter((j) => edges.has(key(i, j)))
      .map((j) => [facts[j]!.sha256, classify(f, facts[j]!, T)] as const);
    g.add(f, matches);
    seen.push(i);
  }
  return g;
}

function lookAlikeEdges(facts: readonly Facts[]): Set<string> {
  const edges = new Set<string>();
  for (const i of range(facts.length))
    for (const j of range(i)) if (looksAlike(facts[i]!, facts[j]!, T)) edges.add(key(i, j));
  return edges;
}

/** Scene k: two burst frames (f0, f1) and a resized JPEG export of each (c0, c1). Index = 4k + role. */
const FACTS: Facts[] = range(12).map((i) => {
  const scene = i >> 2;
  const frame = i & 1;
  const isCopy = (i & 2) !== 0;
  const captured = { s: 1_600_000_000 + scene, sub: String(frame) };
  const phash = mix(scene) ^ BigInt(1 << (frame + (isCopy ? 2 : 0)));
  const dhash = mix(scene + 100);
  return isCopy
    ? F(`s${scene}c${frame}`, {
        phash,
        dhash,
        captured,
        width: 2000,
        height: 1500,
        mime: 'image/jpeg',
      })
    : F(`s${scene}f${frame}`, { phash, dhash, captured, burstUuid: `B${scene}` });
});
const EDGES = lookAlikeEdges(FACTS);
const EXPECTED_COPY = new Set(
  range(3).flatMap((s) => range(2).map((fr) => [`s${s}f${fr}`, `s${s}c${fr}`].sort().join(','))),
);
const EXPECTED_SIMILAR = new Set(
  range(3).map((s) => [`s${s}f0`, `s${s}f1`, `s${s}c0`, `s${s}c1`].sort().join(',')),
);

test('fixture: every pair within a scene looks alike, no pair across scenes does', () => {
  expect(EDGES.size).toBe(3 * 6);
  for (const e of EDGES) {
    const [i, j] = e.split('-').map(Number);
    expect(i! >> 2).toBe(j! >> 2);
  }
});

test('bridge export never merges conflicting shots', () => {
  const facts = {
    A: F('A', { captured: { s: 1, sub: '1' } }),
    B: F('B', { captured: { s: 2, sub: '1' } }),
    X: F('X', NOEXIF),
  };
  const verdict = (p: string, q: string): 'copy' | 'similar' =>
    [p, q].sort().join('') === 'AB' ? 'similar' : 'copy';
  for (const order of permutations(['A', 'B', 'X'] as const)) {
    const g = new Grouper(counter());
    const seen: string[] = [];
    const flagged: boolean[] = [];
    for (const s of order) {
      flagged.push(
        g.add(
          facts[s],
          seen.map((o) => [o, verdict(s, o)] as const),
        ).ambiguous,
      );
      seen.push(s);
    }
    expect([...g.copyPartition()].some((grp) => grp.includes('A') && grp.includes('B'))).toBe(
      false,
    );
    expect(g.similarPartition()).toEqual(new Set(['A,B,X']));
    expect(flagged.filter(Boolean)).toHaveLength(1);
  }
});

test('partitions are order-independent without conflicts', () =>
  fc.assert(
    fc.property(fc.shuffledSubarray(range(12), { minLength: 12, maxLength: 12 }), (perm) => {
      const g = runGrouper(EDGES, FACTS, perm);
      expect(g.similarPartition()).toEqual(EXPECTED_SIMILAR);
      expect(g.copyPartition()).toEqual(EXPECTED_COPY);
    }),
  ));

test('a 10-frame burst with an export of each frame stays 10 copy groups in one similar set', () => {
  const facts = range(20).map((i) => {
    const frame = i % 10;
    const captured = { s: 1_600_000_000, sub: String(100 + frame) };
    const phash = BigInt(frame & 3) | (i >= 10 ? 4n : 0n);
    return i < 10
      ? F(`f${frame}`, { phash, captured, burstUuid: 'BURST' })
      : F(`c${frame}`, { phash, captured, width: 1024, height: 768, mime: 'image/jpeg' });
  });
  const edges = lookAlikeEdges(facts);
  expect(edges.size).toBe((20 * 19) / 2);
  fc.assert(
    fc.property(fc.shuffledSubarray(range(20), { minLength: 20, maxLength: 20 }), (perm) => {
      const g = runGrouper(edges, facts, perm);
      expect(g.copyPartition()).toEqual(new Set(range(10).map((fr) => `c${fr},f${fr}`)));
      expect(g.similarPartition().size).toBe(1);
    }),
  );
});

test('unrelated contents are singleton copy groups and singleton similar components', () => {
  const g = new Grouper(counter());
  const a = g.add(F('a'), []);
  expect(a).toEqual<Assignment>({
    copyGroup: 1,
    ambiguous: false,
    created: [[1, 'copy']],
    copyMerges: [],
    similarMerges: [],
    similarLinks: new Map(),
  });
  g.add(F('b', { phash: mix(1) }), []);
  expect(g.copyPartition()).toEqual(new Set(['a', 'b']));
  expect(g.similarPartition()).toEqual(new Set(['a', 'b']));
});

test('assignment: copy merges into the older group; a similar match creates and links one set', () => {
  const g = new Grouper(counter());
  g.add(F('a'), []);
  expect(g.add(F('c', { width: 2000 }), [['a', 'copy']])).toEqual<Assignment>({
    copyGroup: 1,
    ambiguous: false,
    created: [[2, 'copy']],
    copyMerges: [[2, 1]],
    similarMerges: [],
    similarLinks: new Map(),
  });
  const b = F('b', { captured: { s: 1_600_000_000, sub: '999' } });
  expect(
    g.add(b, [
      ['a', 'similar'],
      ['c', 'similar'],
    ]),
  ).toEqual<Assignment>({
    copyGroup: 3,
    ambiguous: false,
    created: [
      [3, 'copy'],
      [4, 'similar'],
    ],
    copyMerges: [],
    similarMerges: [],
    similarLinks: new Map([
      [3, 4],
      [1, 4],
    ]),
  });
  expect(g.copyPartition()).toEqual(new Set(['a,c', 'b']));
  expect(g.similarPartition()).toEqual(new Set(['a,b,c']));
});

const row = (
  sha: string,
  copyGroup: number,
  similarSet: number | null,
  captured = F(sha).captured,
) => ({
  sha256: sha,
  copyGroup,
  similarSet,
  captured,
  ambiguous: false,
});

test('loaded state: a new look-alike unions two existing similar sets, smaller into larger', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, 20));
  g.load(row('a2', 10, 20));
  g.load(row('b', 11, 21));
  const asg = g.add(F('n', { captured: { s: 5, sub: null } }), [
    ['a', 'similar'],
    ['b', 'similar'],
  ]);
  expect(asg.created).toEqual([[100, 'copy']]);
  expect(asg.similarMerges).toEqual([[21, 20]]);
  expect(asg.similarLinks).toEqual(new Map([[100, 20]]));
  expect(g.similarPartition()).toEqual(new Set(['a,a2,b,n']));
  expect(g.copyPartition()).toEqual(new Set(['a,a2', 'b', 'n']));
});

test('loaded state: similar-set size tie goes to the lower id', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, 21));
  g.load(row('b', 11, 20));
  const asg = g.add(F('n', { captured: { s: 5, sub: null } }), [
    ['a', 'similar'],
    ['b', 'similar'],
  ]);
  expect(asg.similarMerges).toEqual([[21, 20]]);
  expect(asg.similarLinks).toEqual(new Map([[100, 20]]));
});

test('copy merge keeps the winner set when the loser has none', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, 20));
  g.load(row('b', 11, null, null));
  const asg = g.add(F('n', NOEXIF), [
    ['a', 'copy'],
    ['b', 'copy'],
  ]);
  expect(asg).toMatchObject({
    copyGroup: 10,
    ambiguous: false,
    copyMerges: [
      [100, 10],
      [11, 10],
    ],
  });
  expect(asg.created).toEqual([[100, 'copy']]);
  expect(asg.similarMerges).toEqual([]);
  expect(asg.similarLinks).toEqual(new Map());
  expect(g.copyPartition()).toEqual(new Set(['a,b,n']));
  expect(g.similarPartition()).toEqual(new Set(['a,b,n']));
});

test('copy merge inherits the loser set when the winner has none', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null));
  g.load(row('a2', 10, null));
  g.load(row('b', 11, 21, null));
  const asg = g.add(F('n', NOEXIF), [
    ['a', 'copy'],
    ['a2', 'copy'],
    ['b', 'copy'],
  ]);
  expect(asg.copyMerges).toEqual([
    [100, 10],
    [11, 10],
  ]);
  expect(asg.similarMerges).toEqual([]);
  expect(asg.similarLinks).toEqual(new Map([[10, 21]]));
  expect(g.similarPartition()).toEqual(new Set(['a,a2,b,n']));
});

test('copy merge of two groups with different sets unions the sets', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, 20));
  g.load(row('a2', 10, 20));
  g.load(row('b', 11, 21, null));
  g.load(row('z', 12, 21, null));
  const asg = g.add(F('n', NOEXIF), [
    ['a', 'copy'],
    ['b', 'copy'],
  ]);
  expect(asg.copyGroup).toBe(10);
  expect(asg.similarMerges).toEqual([[21, 20]]);
  expect(asg.similarLinks).toEqual(new Map());
  expect(g.copyPartition()).toEqual(new Set(['a,a2,b,n', 'z']));
  expect(g.similarPartition()).toEqual(new Set(['a,a2,b,n,z']));
});

test('refused union is flagged ambiguous and still joins the similar set', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null, { s: 1, sub: '1' }));
  g.load(row('b', 11, null, { s: 1, sub: '2' }));
  const asg = g.add(F('n', { captured: { s: 1, sub: '1' } }), [
    ['a', 'copy'],
    ['b', 'copy'],
  ]);
  expect(asg.ambiguous).toBe(true);
  expect(asg.copyGroup).toBe(10);
  expect(asg.created).toEqual([
    [100, 'copy'],
    [101, 'similar'],
  ]);
  expect(asg.similarLinks).toEqual(
    new Map([
      [10, 101],
      [11, 101],
    ]),
  );
  expect(g.copyPartition()).toEqual(new Set(['a,n', 'b']));
  expect(g.similarPartition()).toEqual(new Set(['a,b,n']));
});

test('a content conflicting with its only copy match keeps its own group', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null, { s: 1, sub: '1' }));
  const asg = g.add(F('n', { captured: { s: 2, sub: '1' } }), [['a', 'copy']]);
  expect(asg).toMatchObject({ copyGroup: 100, ambiguous: true, copyMerges: [] });
  expect(g.copyPartition()).toEqual(new Set(['a', 'n']));
  expect(g.similarPartition()).toEqual(new Set(['a,n']));
});

test('rejects unknown matches and duplicate contents', () => {
  const g = new Grouper(counter());
  g.add(F('a'), []);
  expect(() => g.add(F('a'), [])).toThrow(/already grouped/);
  expect(() => g.add(F('b'), [['nope', 'copy']])).toThrow(/unknown/);
  expect(() => g.load(row('a', 5, null))).toThrow(/already grouped/);
});
