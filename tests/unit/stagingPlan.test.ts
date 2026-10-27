import { basename } from 'node:path';
import fc from 'fast-check';
import { expect, test } from 'vitest';
import { vaultRel } from '../../src/fsops.js';
import {
  bestCopyKey,
  compareBestCopy,
  pairLivePhotos,
  planStaging,
  similarSets,
  type Entry,
  type StageContent,
  type StageFile,
} from '../../src/stagingPlan.js';
import { NO_PICKS, type PickInputs } from '../../src/similarPick.js';

const C = (sha: string, o: Partial<StageContent> = {}): StageContent => ({
  sha256: sha,
  kind: 'still',
  ext: 'heic',
  mime: 'image/heic',
  width: 4032,
  height: 3024,
  sizeBytes: 1000n,
  capturedAt: new Date('2021-10-03T14:02:11Z'),
  liveContentId: null,
  durationS: null,
  state: 'CLUSTERED',
  quarantineReason: null,
  copyGroup: 1,
  similarSet: null,
  ambiguous: false,
  ...o,
});
const Fl = (
  fileId: bigint,
  relPath: string,
  c: StageContent,
  o: Partial<StageFile> = {},
): StageFile => {
  const state = o.state ?? 'INGESTED';
  return {
    fileId,
    relPath,
    originalName: basename(relPath),
    mtimeNs: 1_600_000_000_000_000_000n,
    sha256: c.sha256,
    state,
    ...o,
  };
};
const dests = (entries: Entry[]): string[] => entries.map((e) => e.dest).sort();

// Staging choice (a), 2026-10-18: copies/ shows only different files of the same shot. A
// byte-identical COPY file is never staged (removed from .copies or not); it is only counted.
test('byte-identical COPY files are never staged; every link comes from the vault', () => {
  const heic = C('h'.repeat(64), { ext: 'heic', width: 4032, height: 3024 });
  const jpg = C('j'.repeat(64), { ext: 'jpg', width: 2016, height: 1512 });
  const files = [
    Fl(1n, 'IMG_0034.HEIC', heic),
    Fl(2n, 'again/IMG_0034.HEIC', heic, { state: 'COPY' }),
    Fl(3n, 'IMG_0034.JPG', jpg),
    Fl(4n, 'again/IMG_0034.JPG', jpg, { state: 'COPY' }),
    Fl(5n, 'more/IMG_0034.JPG', jpg, { state: 'COPY' }),
  ];
  expect(planStaging([heic, jpg], files, 5)).toEqual([
    { dest: 'copies/g-hhhhhhhh/IMG_0034.JPG', src: vaultRel(jpg.sha256, 'jpg') },
    {
      dest: 'unique/2021/2021-10/20211003_140211_IMG_0034.HEIC',
      src: vaultRel(heic.sha256, 'heic'),
    },
  ]);
});

test('a content named by a COPY file still links from the vault', () => {
  const c = C('a'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const best = C('b'.repeat(64));
  const files = [
    Fl(1n, 'z/B.HEIC', best),
    Fl(2n, 'z/A.JPG', c),
    Fl(3n, 'a/A.JPG', c, { state: 'COPY' }),
  ];
  expect(planStaging([best, c], files, 5)[0]).toEqual({
    dest: 'copies/g-bbbbbbbb/A.JPG',
    src: vaultRel(c.sha256, 'jpg'),
  });
});

test('unique best copy and content-named copies folder', () => {
  const heic = C('h'.repeat(64), { ext: 'heic', width: 4032, height: 3024, copyGroup: 1 });
  const jpg = C('j'.repeat(64), { ext: 'jpg', width: 2016, height: 1512, copyGroup: 1 });
  expect(
    dests(
      planStaging([heic, jpg], [Fl(1n, 'IMG_0034.HEIC', heic), Fl(2n, 'IMG_0034.JPG', jpg)], 5),
    ),
  ).toEqual([
    'copies/g-hhhhhhhh/IMG_0034.JPG',
    'unique/2021/2021-10/20211003_140211_IMG_0034.HEIC',
  ]);
});

test('burst set keeps every shot in similar/', () => {
  const a = C('a'.repeat(64), { copyGroup: 1, similarSet: 9 }),
    b = C('b'.repeat(64), { copyGroup: 2, similarSet: 9 });
  expect(
    dests(planStaging([a, b], [Fl(1n, 'A.HEIC', a), Fl(2n, 'B.HEIC', b)], 5)).every((d) =>
      d.startsWith('similar/2021/2021-10/s-'),
    ),
  ).toBe(true);
});

// Chaos (Task 13): copy groups already linked as a similar set can later merge into one copy
// group, leaving a set that holds only it; had the copies merged first there'd be no set at all.
test('a similar set holding a single copy group stages as unique', () => {
  const a = C('a'.repeat(64), { copyGroup: 1, similarSet: 9 });
  const a2 = C('c'.repeat(64), { copyGroup: 1, similarSet: 9, width: 2016, height: 1512 });
  const asSet = planStaging([a, a2], [Fl(1n, 'A.HEIC', a), Fl(2n, 'A2.HEIC', a2)], 5);
  const noSet = planStaging(
    [a, a2].map((c) => ({ ...c, similarSet: null })),
    [Fl(1n, 'A.HEIC', a), Fl(2n, 'A2.HEIC', a2)],
    5,
  );
  expect(dests(asSet)).toEqual([
    'copies/g-aaaaaaaa/A2.HEIC',
    'unique/2021/2021-10/20211003_140211_A.HEIC',
  ]);
  expect(dests(asSet)).toEqual(dests(noSet));
});

test('similar folder is named and dated after the set best, across months', () => {
  const a = C('a'.repeat(64), { copyGroup: 1, similarSet: 9, width: 10, height: 10 });
  const b = C('b'.repeat(64), {
    copyGroup: 2,
    similarSet: 9,
    capturedAt: new Date('2021-11-01T00:00:01Z'),
  });
  expect(dests(planStaging([a, b], [Fl(1n, 'A.HEIC', a), Fl(2n, 'B.HEIC', b)], 5))).toEqual([
    'similar/2021/2021-11/s-bbbbbbbb/20211003_140211_A.HEIC',
    'similar/2021/2021-11/s-bbbbbbbb/20211101_000001_B.HEIC',
  ]);
});

/** A user pick of each sha, later ones later. */
const userPicked = (...shas: string[]): PickInputs => ({
  userPicks: new Map(shas.map((s, i) => [s, i])),
  scores: new Map(),
});

/** Three look-alike shots, one of them with a smaller JPEG copy (never a candidate). */
function threeShots(): { cs: StageContent[]; fs: StageFile[] } {
  const a = C('a'.repeat(64), { copyGroup: 1, similarSet: 9 });
  const a2 = C('d'.repeat(64), { copyGroup: 1, similarSet: 9, ext: 'jpg', width: 10, height: 10 });
  const b = C('b'.repeat(64), { copyGroup: 2, similarSet: 9, width: 10, height: 10 });
  const c = C('c'.repeat(64), { copyGroup: 3, similarSet: 9, width: 20, height: 10 });
  return {
    cs: [a, a2, b, c],
    fs: [Fl(1n, 'A.HEIC', a), Fl(2n, 'A.JPG', a2), Fl(3n, 'B.HEIC', b), Fl(4n, 'C.HEIC', c)],
  };
}

test('with no picks the plan is unchanged', () => {
  const { cs, fs } = threeShots();
  expect(planStaging(cs, fs, 5, NO_PICKS)).toEqual(planStaging(cs, fs, 5));
  expect(dests(planStaging(cs, fs, 5))).toEqual([
    'copies/g-aaaaaaaa/A.JPG',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_A.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_B.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_C.HEIC',
  ]);
});

test('the pick of a set is named 0-best_ in the same set folder', () => {
  const { cs, fs } = threeShots();
  expect(dests(planStaging(cs, fs, 5, userPicked('b'.repeat(64))))).toEqual([
    'copies/g-aaaaaaaa/A.JPG',
    'similar/2021/2021-10/s-aaaaaaaa/0-best_20211003_140211_B.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_A.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_C.HEIC',
  ]);
});

test('a changed pick renames two files and no folder', () => {
  const { cs, fs } = threeShots();
  const before = dests(planStaging(cs, fs, 5, userPicked('b'.repeat(64))));
  const after = dests(planStaging(cs, fs, 5, userPicked('b'.repeat(64), 'c'.repeat(64))));
  const folders = (ds: string[]) => [...new Set(ds.map((d) => d.slice(0, d.lastIndexOf('/'))))];
  expect(folders(after)).toEqual(folders(before));
  expect(before.filter((d) => !after.includes(d))).toEqual([
    'similar/2021/2021-10/s-aaaaaaaa/0-best_20211003_140211_B.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_C.HEIC',
  ]);
  expect(after.filter((d) => !before.includes(d))).toEqual([
    'similar/2021/2021-10/s-aaaaaaaa/0-best_20211003_140211_C.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_B.HEIC',
  ]);
});

test('a pick of a copy group’s non-placed content marks nothing', () => {
  const { cs, fs } = threeShots();
  expect(planStaging(cs, fs, 5, userPicked('d'.repeat(64)))).toEqual(planStaging(cs, fs, 5));
});

test('similar sets: the placed best of each copy group, never its other contents', () => {
  const { cs, fs } = threeShots();
  const lone = C('e'.repeat(64), { copyGroup: 4, similarSet: 10 }); // a set of one copy group
  expect(similarSets([...cs, lone], [...fs, Fl(5n, 'E.HEIC', lone)], 5)).toEqual(
    new Map([[9, ['a'.repeat(64), 'b'.repeat(64), 'c'.repeat(64)]]]),
  );
});

test('a Live motion beside a picked still is never a candidate and shares its base name', () => {
  const a = C('a'.repeat(64), { copyGroup: 1, similarSet: 9, liveContentId: 'L' });
  const b = C('b'.repeat(64), { copyGroup: 2, similarSet: 9 });
  const m = C('f'.repeat(64), {
    kind: 'motion',
    ext: 'mov',
    state: 'INGESTED',
    liveContentId: 'L',
    copyGroup: null,
  });
  const fs = [Fl(1n, 'A.HEIC', a), Fl(2n, 'B.HEIC', b), Fl(3n, 'A.MOV', m)];
  expect(similarSets([a, b, m], fs, 5)).toEqual(new Map([[9, ['a'.repeat(64), 'b'.repeat(64)]]]));
  expect(dests(planStaging([a, b, m], fs, 5, userPicked('a'.repeat(64))))).toEqual([
    'similar/2021/2021-10/s-aaaaaaaa/0-best_20211003_140211_A.HEIC',
    'similar/2021/2021-10/s-aaaaaaaa/0-best_20211003_140211_A.MOV',
    'similar/2021/2021-10/s-aaaaaaaa/20211003_140211_B.HEIC',
  ]);
});

test('Live MOV beside still with same base name', () => {
  const s = C('5'.repeat(64), { liveContentId: 'L1' }),
    m = C('6'.repeat(64), {
      kind: 'motion',
      ext: 'mov',
      liveContentId: 'L1',
      durationS: 2,
      copyGroup: null,
      state: 'INGESTED',
    });
  expect(
    dests(planStaging([s, m], [Fl(1n, 'x/IMG_9.HEIC', s), Fl(2n, 'y/OTHER.MOV', m)], 5)),
  ).toEqual([
    'unique/2021/2021-10/20211003_140211_IMG_9.HEIC',
    'unique/2021/2021-10/20211003_140211_IMG_9.MOV',
  ]); // MOV keeps its own extension
});

test('Live MOV links from the vault, and its byte copies are never staged', () => {
  const s = C('5'.repeat(64), { liveContentId: 'L1' });
  const m = C('6'.repeat(64), {
    kind: 'motion',
    ext: 'mov',
    liveContentId: 'L1',
    copyGroup: null,
    state: 'INGESTED',
  });
  const out = planStaging(
    [s, m],
    [Fl(1n, 'IMG_9.HEIC', s), Fl(2n, 'IMG_9.MOV', m), Fl(3n, 'z/IMG_9.MOV', m, { state: 'COPY' })],
    5,
  );
  expect(out).toEqual([
    { dest: 'unique/2021/2021-10/20211003_140211_IMG_9.HEIC', src: vaultRel(s.sha256, 'heic') },
    { dest: 'unique/2021/2021-10/20211003_140211_IMG_9.MOV', src: vaultRel(m.sha256, 'mov') },
  ]);
});

test('a MOV paired to an unstaged still goes to other/', () => {
  const s = C('5'.repeat(64), { liveContentId: 'L1', state: 'HASHED', copyGroup: null });
  const m = C('6'.repeat(64), {
    kind: 'motion',
    ext: 'mov',
    liveContentId: 'L1',
    copyGroup: null,
    state: 'INGESTED',
  });
  expect(dests(planStaging([s, m], [Fl(1n, 'd/A.HEIC', s), Fl(2n, 'd/A.MOV', m)], 5))).toEqual([
    'other/d/A.MOV',
  ]);
});

test('a paired MOV follows a quarantined still', () => {
  const s = C('5'.repeat(64), {
    liveContentId: 'L1',
    state: 'QUARANTINED',
    quarantineReason: 'truncated',
    copyGroup: null,
  });
  const m = C('6'.repeat(64), {
    kind: 'motion',
    ext: 'mov',
    liveContentId: 'L1',
    copyGroup: null,
    state: 'INGESTED',
  });
  expect(dests(planStaging([s, m], [Fl(1n, 'A.HEIC', s), Fl(2n, 'B.MOV', m)], 5))).toEqual([
    'quarantine/truncated/A.HEIC',
    'quarantine/truncated/A.MOV',
  ]);
});

for (const [state, reason, folder] of [
  ['QUARANTINED', 'truncated', 'quarantine/truncated'],
  ['FAILED', null, 'quarantine/failed'],
] as const)
  test(`a paired MOV follows a ${state} still whose sha sorts after the motion's`, () => {
    const s = C('6'.repeat(64), {
      liveContentId: 'L1',
      state,
      quarantineReason: reason,
      copyGroup: null,
    });
    const m = C('5'.repeat(64), {
      kind: 'motion',
      ext: 'mov',
      liveContentId: 'L1',
      copyGroup: null,
      state: 'INGESTED',
    });
    expect(dests(planStaging([s, m], [Fl(1n, 'A.HEIC', s), Fl(2n, 'B.MOV', m)], 5))).toEqual([
      `${folder}/A.HEIC`,
      `${folder}/A.MOV`,
    ]);
  });

test('naming file: lowest (relPath, mtimeNs, fileId) — a re-drop with an older mtime names it', () => {
  const c = C('a'.repeat(64), { capturedAt: null });
  const at = (iso: string) => BigInt(Date.parse(iso)) * 1_000_000n;
  const files = (newId: bigint, oldId: bigint) => [
    Fl(newId, 'N.HEIC', c, { mtimeNs: at('2020-03-01T00:00:00Z') }),
    Fl(oldId, 'N.HEIC', c, { mtimeNs: at('2020-01-01T00:00:00Z'), state: 'COPY' }),
  ];
  for (const [n, o] of [
    [1n, 2n],
    [2n, 1n],
  ] as const)
    expect(planStaging([c], files(n, o), 5)).toEqual([
      { dest: 'unique/2020/2020-01/20200101_000000_N.HEIC', src: vaultRel(c.sha256, 'heic') },
    ]);
});

test('stem fallback is exact and duration-bounded', () => {
  const s1 = C('1'.repeat(64)),
    s2 = C('2'.repeat(64));
  const m1 = C('3'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 3 }),
    long = C('4'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 30 });
  const files = [
    Fl(1n, 'd/IMG_2834.HEIC', s1),
    Fl(2n, 'd/IMG_2834 1.HEIC', s2),
    Fl(3n, 'd/IMG_2834 1.MOV', m1),
    Fl(4n, 'd/IMG_2834.MOV', long),
  ];
  expect(pairLivePhotos([s1, s2, m1, long], files, 5)).toEqual(
    new Map([['2'.repeat(64), '3'.repeat(64)]]),
  );
});

test('stem fallback is case-insensitive, same-directory only, and needs a duration', () => {
  const s = C('1'.repeat(64));
  const m = C('3'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 3 });
  const noDur = C('4'.repeat(64), { kind: 'motion', ext: 'mov', durationS: null });
  expect(pairLivePhotos([s, m], [Fl(1n, 'd/img_1.heic', s), Fl(2n, 'd/IMG_1.MOV', m)], 5)).toEqual(
    new Map([[s.sha256, m.sha256]]),
  );
  expect(pairLivePhotos([s, m], [Fl(1n, 'd/IMG_1.HEIC', s), Fl(2n, 'e/IMG_1.MOV', m)], 5)).toEqual(
    new Map(),
  );
  expect(
    pairLivePhotos([s, noDur], [Fl(1n, 'd/IMG_1.HEIC', s), Fl(2n, 'd/IMG_1.MOV', noDur)], 5),
  ).toEqual(new Map());
});

test('content-id pairing picks the lowest motion sha and ignores duration', () => {
  const s = C('1'.repeat(64), { liveContentId: 'L' });
  const m9 = C('9'.repeat(64), { kind: 'motion', ext: 'mov', liveContentId: 'L', durationS: 99 });
  const m7 = C('7'.repeat(64), { kind: 'motion', ext: 'mov', liveContentId: 'L', durationS: 99 });
  expect(pairLivePhotos([m9, s, m7], [], 5)).toEqual(new Map([[s.sha256, m7.sha256]]));
});

test('collision gets _sha8, then _2', () => {
  // Three different contents of one shot, all named IMG_1.JPG; two share their first 8 hex.
  const best = C('a'.repeat(64));
  const small = (sha: string) => C(sha, { ext: 'jpg', width: 10, height: 10 });
  const [j, k, l] = [
    'b'.repeat(64),
    'b'.repeat(8) + 'c'.repeat(56),
    'b'.repeat(8) + 'd'.repeat(56),
  ];
  const [cj, ck, cl] = [small(j), small(k), small(l)];
  const files = [
    Fl(1n, 'IMG_1.HEIC', best),
    Fl(2n, 'x/IMG_1.JPG', cj),
    Fl(3n, 'y/IMG_1.JPG', ck),
    Fl(4n, 'z/IMG_1.JPG', cl),
  ];
  expect(
    dests(planStaging([best, cj, ck, cl], files, 5)).filter((d) => d.startsWith('copies/')),
  ).toEqual([
    'copies/g-aaaaaaaa/IMG_1.JPG',
    'copies/g-aaaaaaaa/IMG_1_bbbbbbbb.JPG',
    'copies/g-aaaaaaaa/IMG_1_bbbbbbbb_2.JPG',
  ]);
});

test('collisions are case-insensitive and suffix names without an extension', () => {
  const best = C('a'.repeat(64));
  const j = C('b'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const k = C('c'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const r1 = C('1'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const r2 = C('2'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const files = [
    Fl(1n, 'IMG_1.HEIC', best),
    Fl(2n, 'x/img_1.jpg', j),
    Fl(3n, 'y/IMG_1.JPG', k),
    Fl(4n, 'p/README', r1),
    Fl(5n, 'q/README', r2),
  ];
  expect(
    dests(planStaging([best, j, k, r1, r2], files, 5)).filter((d) => d.startsWith('copies/')),
  ).toEqual([
    'copies/g-aaaaaaaa/IMG_1_cccccccc.JPG',
    'copies/g-aaaaaaaa/README',
    'copies/g-aaaaaaaa/README_22222222',
    'copies/g-aaaaaaaa/img_1.jpg',
  ]);
});

test('two Live pairs with one base name do not collide', () => {
  const a = C('a'.repeat(64), { liveContentId: 'LA', copyGroup: 1 });
  const b = C('b'.repeat(64), { liveContentId: 'LB', copyGroup: 2 });
  const mov = (sha: string, id: string) =>
    C(sha, { kind: 'motion', ext: 'mov', liveContentId: id, copyGroup: null, state: 'INGESTED' });
  const ma = mov('1'.repeat(64), 'LA'),
    mb = mov('2'.repeat(64), 'LB');
  const files = [
    Fl(1n, 'x/IMG.HEIC', a),
    Fl(2n, 'y/IMG.HEIC', b),
    Fl(3n, 'x/IMG.MOV', ma),
    Fl(4n, 'y/IMG.MOV', mb),
  ];
  expect(dests(planStaging([a, b, ma, mb], files, 5))).toEqual([
    'unique/2021/2021-10/20211003_140211_IMG.HEIC',
    'unique/2021/2021-10/20211003_140211_IMG.MOV',
    'unique/2021/2021-10/20211003_140211_IMG_bbbbbbbb.HEIC',
    'unique/2021/2021-10/20211003_140211_IMG_bbbbbbbb.MOV',
  ]);
});

test('ambiguous content is never best', () => {
  const amb = C('c'.repeat(64), { width: 8000, height: 6000, ambiguous: true }),
    ok = C('d'.repeat(64));
  expect(
    dests(planStaging([amb, ok], [Fl(1n, 'AMB.HEIC', amb), Fl(2n, 'OK.HEIC', ok)], 5)),
  ).toEqual(['copies/g-dddddddd/AMB.HEIC', 'unique/2021/2021-10/20211003_140211_OK.HEIC']);
});

test('best-copy ties: earliest capture, then lowest sha', () => {
  const early = C('9'.repeat(64), { capturedAt: new Date('2021-10-03T14:02:10Z') }),
    late = C('1'.repeat(64));
  expect(compareBestCopy(bestCopyKey(early, false), bestCopyKey(late, false))).toBeLessThan(0);
  expect(
    compareBestCopy(bestCopyKey(C('1'.repeat(64)), false), bestCopyKey(C('2'.repeat(64)), false)),
  ).toBeLessThan(0);
  expect(
    compareBestCopy(bestCopyKey(C('2'.repeat(64)), true), bestCopyKey(C('1'.repeat(64)), false)),
  ).toBeLessThan(0); // hasLive beats sha
});

test('best-copy key order: ambiguity, pixels, capture, live, format, size', () => {
  const k = (o: Partial<StageContent>, live = false) => bestCopyKey(C('5'.repeat(64), o), live);
  const better = (a: ReturnType<typeof k>, b: ReturnType<typeof k>) =>
    expect([compareBestCopy(a, b) < 0, compareBestCopy(b, a) > 0]).toEqual([true, true]);
  better(k({ width: 1, height: 1 }), k({ ambiguous: true }));
  better(k({ capturedAt: null }), k({ width: 1, height: 1 }));
  better(k({}), k({ capturedAt: null }, true));
  better(k({ ext: 'jpg' }, true), k({ ext: 'dng' }));
  for (const [hi, lo] of [
    ['dng', 'tif'],
    ['tif', 'png'],
    ['png', 'heic'],
    ['heic', 'jpg'],
    ['jpg', 'webp'],
    ['webp', 'gif'],
    ['gif', 'bmp'],
    ['bmp', 'xyz'],
  ] as const)
    better(k({ ext: hi }), k({ ext: lo, sizeBytes: 9999n }));
  better(k({ sizeBytes: 2000n }), k({ capturedAt: new Date(0) }));
  expect(compareBestCopy(k({}), k({}))).toBe(0);
  expect(k({ width: null, height: null, capturedAt: null })).toEqual([
    1,
    0,
    0,
    0,
    5,
    1000n,
    0,
    '5'.repeat(64),
  ]);
});

test('quarantine, failed and other buckets', () => {
  const q = C('e'.repeat(64), {
    state: 'QUARANTINED',
    quarantineReason: 'truncated',
    copyGroup: null,
  });
  const f = C('f'.repeat(64), { state: 'FAILED', copyGroup: null });
  const aae = C('0'.repeat(64), {
    kind: 'other',
    ext: 'aae',
    mime: 'application/xml',
    state: 'INGESTED',
    copyGroup: null,
  });
  expect(
    dests(
      planStaging(
        [q, f, aae],
        [Fl(1n, 'd/BAD.JPG', q), Fl(2n, 'd/HANG.JPG', f), Fl(3n, 'd/e/IMG_1.AAE', aae)],
        5,
      ),
    ),
  ).toEqual(['other/d/e/IMG_1.AAE', 'quarantine/failed/HANG.JPG', 'quarantine/truncated/BAD.JPG']);
});

test('quarantine byte copies are not staged, an odd reason is made path-safe', () => {
  const q = C('e'.repeat(64), {
    state: 'QUARANTINED',
    quarantineReason: '../bad reason',
    copyGroup: null,
  });
  const n = C('d'.repeat(64), { state: 'QUARANTINED', quarantineReason: null, copyGroup: null });
  expect(
    dests(
      planStaging(
        [q, n],
        [Fl(1n, 'BAD.JPG', q), Fl(2n, 'x/BAD.JPG', q, { state: 'COPY' }), Fl(3n, 'N.JPG', n)],
        5,
      ),
    ),
  ).toEqual(['quarantine/.._bad_reason/BAD.JPG', 'quarantine/unknown/N.JPG']);
});

test('names are independent of group ids', () => {
  const mk = (g: number, set: number) => [
    C('a'.repeat(64), { copyGroup: g, similarSet: set }),
    C('b'.repeat(64), { copyGroup: g + 1, similarSet: set }),
  ];
  const files = (cs: StageContent[]) =>
    cs.map((c, i) => Fl(BigInt(i + 1), `${c.sha256[0]}.HEIC`, c));
  const [p, q] = [mk(1, 9), mk(500, 77)];
  expect(planStaging(p, files(p), 5)).toEqual(planStaging(q, files(q), 5));
});

test('name comes from lowest (relPath, fileId), not arrival order', () => {
  const m = C('1'.repeat(64), { kind: 'motion', ext: 'mp4', copyGroup: null, state: 'INGESTED' });
  const a = planStaging(
    [m],
    [Fl(1n, 'PSWU5412 1.MP4', m), Fl(2n, 'PSWU5412.MP4', m, { state: 'COPY' })],
    5,
  );
  const b = planStaging(
    [m],
    [Fl(1n, 'PSWU5412.MP4', m), Fl(2n, 'PSWU5412 1.MP4', m, { state: 'COPY' })],
    5,
  );
  expect(dests(a)).toEqual(dests(b));
  expect(dests(a)).toEqual(['other/PSWU5412 1.MP4']);
});

test('dated names are UTC', () => {
  const c = C('a'.repeat(64), { capturedAt: new Date('2021-12-31T23:30:00Z') });
  expect(dests(planStaging([c], [Fl(1n, 'N.HEIC', c)], 5))).toEqual([
    'unique/2021/2021-12/20211231_233000_N.HEIC',
  ]);
});

test('without captured_at the date is the naming file mtime (UTC)', () => {
  const c = C('a'.repeat(64), { capturedAt: null });
  const ns = BigInt(Date.parse('2019-02-03T04:05:06.789Z')) * 1_000_000n + 123_456n;
  expect(dests(planStaging([c], [Fl(1n, 'N.HEIC', c, { mtimeNs: ns })], 5))).toEqual([
    'unique/2019/2019-02/20190203_040506_N.HEIC',
  ]);
});

test('unclustered stills are not staged; INGESTED motion/other are', () => {
  const still = C('2'.repeat(64), { state: 'HASHED', copyGroup: null });
  const mov = C('3'.repeat(64), {
    kind: 'motion',
    ext: 'mov',
    state: 'INGESTED',
    copyGroup: null,
    durationS: 30,
  });
  const aae = C('4'.repeat(64), { kind: 'other', ext: 'aae', state: 'INGESTED', copyGroup: null });
  expect(
    dests(
      planStaging(
        [still, mov, aae],
        [Fl(1n, 'S.HEIC', still), Fl(2n, 'V.MOV', mov), Fl(3n, 'X.AAE', aae)],
        5,
      ),
    ),
  ).toEqual(['other/V.MOV', 'other/X.AAE']);
});

test('files without a known content are ignored; output is sorted by dest', () => {
  const c = C('a'.repeat(64));
  const ghost = C('f'.repeat(64));
  const out = planStaging([c], [Fl(2n, 'Z.HEIC', c), Fl(1n, 'A.HEIC', ghost)], 5);
  expect(out.map((e) => e.dest)).toEqual(['unique/2021/2021-10/20211003_140211_Z.HEIC']);
});

// ---------------------------------------------------------------- determinism (property)

interface World {
  contents: StageContent[];
  files: StageFile[];
}

const SHAS = '0123456789abcdef'.split('').map((h) => h.repeat(64));
const NAMES = ['IMG_1.HEIC', 'IMG_1.JPG', 'img_1.heic', 'IMG_1 1.HEIC', 'IMG_1.MOV', 'X.AAE', 'V'];
const DIRS = ['', 'd/', 'e/'];
const DATES = [null, new Date('2021-10-03T14:02:11Z'), new Date('2021-10-03T14:02:12Z')];

/** A random but internally consistent world: CLUSTERED stills have a group; a group has one set. */
const world: fc.Arbitrary<World> = fc
  .record({
    n: fc.integer({ min: 1, max: 10 }),
    groupSet: fc.array(fc.option(fc.integer({ min: 0, max: 2 }), { nil: null }), {
      minLength: 4,
      maxLength: 4,
    }),
    rows: fc.array(
      fc.record({
        kind: fc.constantFrom('still', 'still', 'motion', 'other'),
        ext: fc.constantFrom('heic', 'jpg', 'png', 'mov'),
        px: fc.constantFrom(1, 2),
        date: fc.integer({ min: 0, max: 2 }),
        live: fc.option(fc.constantFrom('L1', 'L2'), { nil: null }),
        dur: fc.option(fc.constantFrom(2, 30), { nil: null }),
        state: fc.constantFrom('CLUSTERED', 'CLUSTERED', 'HASHED', 'QUARANTINED', 'FAILED'),
        group: fc.integer({ min: 0, max: 3 }),
        ambiguous: fc.boolean(),
        size: fc.constantFrom(1n, 2n),
        files: fc.array(
          fc.record({
            name: fc.constantFrom(...NAMES),
            dir: fc.constantFrom(...DIRS),
            copy: fc.boolean(),
            day: fc.constantFrom(0, 40),
            redrop: fc.boolean(),
          }),
          { minLength: 1, maxLength: 3 },
        ),
      }),
      { minLength: 10, maxLength: 10 },
    ),
  })
  .map(({ n, groupSet, rows }) => {
    const contents: StageContent[] = [];
    const files: StageFile[] = [];
    let id = 1n;
    rows.slice(0, n).forEach((r, i) => {
      const still = r.kind === 'still';
      const state = still ? r.state : 'INGESTED';
      const c = C(SHAS[i]!, {
        kind: r.kind,
        ext: r.ext,
        width: 100 * r.px,
        height: 100,
        capturedAt: DATES[r.date]!,
        liveContentId: r.live,
        durationS: r.dur,
        state,
        quarantineReason: state === 'QUARANTINED' ? 'truncated' : null,
        copyGroup: still && state === 'CLUSTERED' ? r.group : null,
        similarSet: still && state === 'CLUSTERED' ? (groupSet[r.group] ?? null) : null,
        ambiguous: r.ambiguous,
        sizeBytes: r.size,
      });
      contents.push(c);
      let prev = '';
      r.files.forEach((f, j) => {
        // a re-drop of the same bytes at the previous drop's path (new row, maybe a new mtime)
        const relPath = j > 0 && f.redrop ? prev : f.dir + f.name;
        prev = relPath;
        // mtime varies per drop, independently of the path: two drops at one path with equal
        // bytes may differ in mtime (and, without captured_at, in staged date)
        const mtimeNs =
          BigInt(Date.parse('2020-01-01T00:00:00Z') + f.day * 86_400_000) * 1_000_000n;
        files.push(Fl(id++, relPath, c, { state: j > 0 && f.copy ? 'COPY' : 'INGESTED', mtimeNs }));
      });
    });
    return { contents, files };
  });

/** Renumbers file ids in a new arrival order and relabels group / set ids. */
function permute(w: World, order: number[], shift: number): World {
  const files = order.map((k, i) => {
    const f = w.files[k]!;
    return { ...f, fileId: BigInt(i + 1) };
  });
  const contents = [...w.contents].reverse().map((c) => ({
    ...c,
    copyGroup: c.copyGroup === null ? null : 1000 - c.copyGroup * shift,
    similarSet: c.similarSet === null ? null : 77 + c.similarSet * shift,
  }));
  return { contents, files };
}

/** The tree as a viewer sees it: every dest with the bytes (content sha) it links to. */
function tree(w: World, entries: Entry[]): [string, string][] {
  const bySrc = new Map(w.contents.map((c) => [vaultRel(c.sha256, c.ext), c.sha256]));
  return entries.map((e) => [e.dest, bySrc.get(e.src)!]);
}

test('the tree is independent of arrival order and group ids (property)', () => {
  fc.assert(
    fc.property(
      world.chain((w) =>
        fc.tuple(
          fc.constant(w),
          fc.shuffledSubarray(
            w.files.map((_, i) => i),
            { minLength: w.files.length, maxLength: w.files.length },
          ),
          fc.integer({ min: 1, max: 5 }),
        ),
      ),
      ([w, order, shift]) => {
        const a = planStaging(w.contents, w.files, 5);
        const p = permute(w, order, shift);
        const b = planStaging(p.contents, p.files, 5);
        expect(tree(p, b)).toEqual(tree(w, a));
        const keys = a.map((e) => e.dest.toLowerCase());
        expect(new Set(keys).size).toBe(keys.length); // never two links at one (APFS) path
        expect(a.map((e) => e.dest)).toEqual(a.map((e) => e.dest).sort());
      },
    ),
    { numRuns: 1000 },
  );
});

test('at most one picked photo per set folder; folders never depend on picks (property)', () => {
  fc.assert(
    fc.property(
      world,
      fc.array(fc.tuple(fc.constantFrom(...SHAS), fc.integer({ min: 0, max: 9 }))),
      fc.array(fc.tuple(fc.constantFrom(...SHAS), fc.constantFrom(0.1, 0.5, 0.9))),
      (w, up, sc) => {
        const picks: PickInputs = { userPicks: new Map(up), scores: new Map(sc) };
        const plain = planStaging(w.contents, w.files, 5);
        const picked = planStaging(w.contents, w.files, 5, picks);
        const folder = (d: string) => d.slice(0, d.lastIndexOf('/'));
        expect(picked.map((e) => folder(e.dest))).toEqual(plain.map((e) => folder(e.dest)));
        // A Live MOV beside the pick takes its name too; count the stills.
        const stills = new Set(
          w.contents.filter((c) => c.kind === 'still').map((c) => vaultRel(c.sha256, c.ext)),
        );
        const best = new Map<string, number>();
        for (const { dest, src } of picked) {
          if (!dest.slice(dest.lastIndexOf('/') + 1).startsWith('0-best_')) continue;
          expect(dest.startsWith('similar/')).toBe(true);
          if (stills.has(src)) best.set(folder(dest), (best.get(folder(dest)) ?? 0) + 1);
        }
        for (const n of best.values()) expect(n).toBe(1);
      },
    ),
    { numRuns: 1000 },
  );
});

test('perf: a 40,000-content similar set plans in linear time', () => {
  // Thousands of look-alike dark or document shots form one set; accumulating its members by
  // copying the list on every push made this O(S²) (≈ 1.6 s at 40k). Compare growth, not wall
  // time, so a slower machine or coverage instrumentation does not fail it: 4× the input
  // should take about 4× as long (16× if quadratic).
  const planSet = (n: number): number => {
    const cs = Array.from({ length: n }, (_, i) =>
      C(i.toString(16).padStart(64, '0'), {
        ext: 'jpg',
        copyGroup: i + 1,
        similarSet: 1,
        capturedAt: new Date(1e12 + i * 1000),
      }),
    );
    const fs = cs.map((c, i) => Fl(BigInt(i + 1), `p/${String(i)}.jpg`, c));
    const t0 = performance.now();
    expect(planStaging(cs, fs, 5)).toHaveLength(n);
    return performance.now() - t0;
  };
  planSet(2_000); // warm up the JIT
  const small = planSet(10_000);
  const large = planSet(40_000);
  expect(large / small).toBeLessThan(8);
});
