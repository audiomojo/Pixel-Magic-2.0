import fc from 'fast-check';
import { expect, test } from 'vitest';
import { captureConflict, classify, looksAlike, type Facts } from '../../src/classify.js';
import { Grouper, type Assignment } from '../../src/grouping.js';
import { F, NOEXIF, T } from '../helpers/facts.js';

const range = (n: number): number[] => Array.from({ length: n }, (_, i) => i);
const mix = (i: number): bigint => BigInt.asUintN(64, BigInt(i + 1) * 0x9e3779b97f4a7c15n);

/** Sequential id allocator for Grouper; tests that load rows pass a start above the loaded ids, like the DB sequence. */
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
    for (const s of order) {
      g.add(
        facts[s],
        seen.map((o) => [o, verdict(s, o)] as const),
      );
      seen.push(s);
    }
    expect([...g.copyPartition()].some((grp) => grp.includes('A') && grp.includes('B'))).toBe(
      false,
    );
    expect(g.similarPartition()).toEqual(new Set(['A,B,X']));
    // Spec §12 R3: the bridging export is the flagged content, whichever arrived first.
    expect(g.ambiguousContents()).toEqual(new Set(['X']));
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
    flagged: [],
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
    flagged: [],
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
    flagged: [],
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
  burstUuid: string | null = null,
) => ({
  sha256: sha,
  copyGroup,
  similarSet,
  captured,
  burstUuid,
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

test('a keyed content whose union is refused is never flagged and still joins the similar set', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null, { s: 1, sub: '1' }));
  g.load(row('b', 11, null, { s: 1, sub: '2' }));
  const asg = g.add(F('n', { captured: { s: 1, sub: '1' } }), [
    ['a', 'copy'],
    ['b', 'copy'],
  ]);
  expect([asg.ambiguous, asg.flagged]).toEqual([false, []]);
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
  expect(asg).toMatchObject({ copyGroup: 100, ambiguous: false, flagged: [], copyMerges: [] });
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

test('two frames of one burst with non-conflicting capture keys are never bridged into a copy', () => {
  const facts = {
    A: F('A', { captured: { s: 1, sub: '1' }, burstUuid: 'B' }),
    C: F('C', { captured: { s: 1, sub: null }, burstUuid: 'B' }),
    X: F('X', { phash: 1n, ...NOEXIF }),
  };
  for (const order of permutations(['A', 'C', 'X'] as const)) {
    const g = new Grouper(counter());
    const seen: Array<keyof typeof facts> = [];
    for (const s of order) {
      g.add(
        facts[s],
        seen.map((o) => [o, classify(facts[s], facts[o], T)] as const),
      );
      seen.push(s);
    }
    expect([...g.copyPartition()].some((grp) => grp.includes('A') && grp.includes('C'))).toBe(
      false,
    );
    expect(g.similarPartition()).toEqual(new Set(['A,C,X']));
  }
});

test('a loaded group refuses a second frame of its burst', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null, { s: 1, sub: '1' }, 'B'));
  g.load(row('x', 10, null, null));
  const asg = g.add(F('n', { captured: { s: 1, sub: null }, burstUuid: 'B' }), [['x', 'copy']]);
  // The key-less x bridges the burst frames: x is flagged, never the frame n.
  expect(asg).toMatchObject({ copyGroup: 100, ambiguous: false, flagged: ['x'], copyMerges: [] });
  expect(g.ambiguousContents()).toEqual(new Set(['x']));
  expect(g.copyPartition()).toEqual(new Set(['a,x', 'n']));
  expect(g.similarPartition()).toEqual(new Set(['a,n,x']));
});

/** Random mixes of burst frames, partial capture keys and EXIF-less tight exports that bridge them. */
const shotArb = fc.record({
  exifless: fc.boolean(),
  phash: fc.bigInt({ min: 0n, max: 15n }),
  dhash: fc.bigInt({ min: 0n, max: 7n }),
  s: fc.integer({ min: 1, max: 2 }),
  sub: fc.constantFrom(null, '0', '1', '2'),
  noCapture: fc.boolean(),
  burstUuid: fc.constantFrom(null, 'B', 'C'),
  width: fc.constantFrom(10, 20),
  mime: fc.constantFrom('image/heic', 'image/jpeg'),
  camera: fc.constantFrom(null, 'cam'),
});
const fixtureArb = fc.array(shotArb, { minLength: 3, maxLength: 10 }).chain((shots) =>
  fc.tuple(
    fc.constant(shots),
    fc.shuffledSubarray(range(shots.length), {
      minLength: shots.length,
      maxLength: shots.length,
    }),
  ),
);

function components(facts: readonly Facts[]): Set<string> {
  const parent = range(facts.length);
  const root = (x: number): number => (parent[x] === x ? x : (parent[x] = root(parent[x]!)));
  for (const i of range(facts.length))
    for (const j of range(i)) if (looksAlike(facts[i]!, facts[j]!, T)) parent[root(i)] = root(j);
  const comps = new Map<number, string[]>();
  for (const i of range(facts.length))
    comps.set(root(i), [...(comps.get(root(i)) ?? []), facts[i]!.sha256]);
  return new Set([...comps.values()].map((l) => l.sort().join(',')));
}

test('copy groups never mix shots, and similar sets are the look-alike components, in any order', () =>
  fc.assert(
    fc.property(fixtureArb, ([shots, order]) => {
      const facts = shots.map((x, i) =>
        x.exifless
          ? F(`n${i}`, { phash: x.phash, dhash: x.dhash, width: x.width, mime: x.mime, ...NOEXIF })
          : F(`n${i}`, {
              phash: x.phash,
              dhash: x.dhash,
              width: x.width,
              mime: x.mime,
              camera: x.camera,
              captured: x.noCapture ? null : { s: x.s, sub: x.sub },
              burstUuid: x.burstUuid,
            }),
      );
      const g = runGrouper(lookAlikeEdges(facts), facts, order);
      const bySha = new Map(facts.map((f) => [f.sha256, f]));
      for (const grp of g.copyPartition()) {
        const members = grp.split(',').map((sha) => bySha.get(sha)!);
        for (const p of members)
          for (const q of members) {
            if (p === q) continue;
            if (p.captured !== null && q.captured !== null)
              expect(captureConflict(p.captured, q.captured)).toBe(false);
            if (p.burstUuid !== null) expect(q.burstUuid).not.toBe(p.burstUuid);
          }
      }
      expect(g.similarPartition()).toEqual(components(facts));
    }),
    { numRuns: 500 },
  ));

test('a key-less member of a burst is never flagged', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null, { s: 1, sub: '1' }, 'B'));
  const asg = g.add(F('n', { ...NOEXIF, burstUuid: 'B' }), [['a', 'copy']]);
  expect(asg).toMatchObject({ copyGroup: 100, ambiguous: false, flagged: [] });
  expect(g.ambiguousContents()).toEqual(new Set());
});

test('loaded copy_ambiguous rows stay flagged and are not reported again', () => {
  const g = new Grouper(counter(100));
  g.load(row('a', 10, null, { s: 1, sub: '1' }));
  g.load({ ...row('x', 10, null, null), ambiguous: true });
  const asg = g.add(F('b', { captured: { s: 2, sub: '1' } }), [['x', 'copy']]);
  expect([asg.ambiguous, asg.flagged, asg.copyGroup]).toEqual([false, [], 100]);
  expect(g.ambiguousContents()).toEqual(new Set(['x']));
});

/**
 * Two burst frames A and B, a keyed copy of each (A2, B2: same capture key, other encoding), a
 * key-less copy of A only (Y), and two key-less exports that copy-match both frames (X1, X2).
 */
const BRIDGED = {
  A: F('A', { captured: { s: 1, sub: '1' }, burstUuid: 'U' }),
  B: F('B', { captured: { s: 1, sub: '2' }, burstUuid: 'U' }),
  A2: F('A2', { captured: { s: 1, sub: '1' }, width: 2000, mime: 'image/jpeg' }),
  B2: F('B2', { captured: { s: 1, sub: '2' }, width: 2000, mime: 'image/jpeg' }),
  Y: F('Y', { ...NOEXIF, width: 1000 }),
  X1: F('X1', { ...NOEXIF, width: 3000 }),
  X2: F('X2', { ...NOEXIF, width: 3000 }),
};
type Bridged = keyof typeof BRIDGED;
const KEYED: Bridged[] = ['A', 'B', 'A2', 'B2'];
const bridgedVerdict = (p: Bridged, q: Bridged): 'copy' | 'similar' => {
  const pair = new Set([p, q]);
  const frame = (n: Bridged) =>
    n === 'A' || n === 'A2' ? 'A' : n === 'B' || n === 'B2' ? 'B' : null;
  if (KEYED.includes(p) && KEYED.includes(q)) return frame(p) === frame(q) ? 'copy' : 'similar';
  if (pair.has('Y')) return [...pair].some((n) => frame(n) === 'A') ? 'copy' : 'similar';
  return KEYED.includes(p) || KEYED.includes(q) ? 'copy' : 'similar'; // X1/X2 vs a frame
};

test('bridges are flagged and the unflagged copy partition is order-independent', () =>
  fc.assert(
    fc.property(
      fc.shuffledSubarray(Object.keys(BRIDGED) as Bridged[], { minLength: 7, maxLength: 7 }),
      (order) => {
        const g = new Grouper(counter());
        const seen: Bridged[] = [];
        for (const n of order) {
          g.add(
            BRIDGED[n],
            seen.map((o) => [o, bridgedVerdict(n, o)] as const),
          );
          seen.push(n);
        }
        const flagged = g.ambiguousContents();
        expect(flagged).toEqual(new Set(['X1', 'X2']));
        const unflagged = new Set(
          [...g.copyPartition()]
            .map((grp) =>
              grp
                .split(',')
                .filter((sha) => !flagged.has(sha))
                .join(','),
            )
            .filter((grp) => grp !== ''),
        );
        expect(unflagged).toEqual(new Set(['A,A2,Y', 'B,B2']));
        expect(g.similarPartition()).toEqual(new Set(['A,A2,B,B2,X1,X2,Y']));
      },
    ),
    { numRuns: 500 },
  ));
