import { basename } from 'node:path';
import fc from 'fast-check';
import { expect, test } from 'vitest';
import { vaultRel } from '../../src/fsops.js';
import {
  bestCopyKey,
  compareBestCopy,
  pairLivePhotos,
  planStaging,
  type Entry,
  type StageContent,
  type StageFile,
} from '../../src/stagingPlan.js';

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
    srcRel: state === 'COPY' ? `vault/.copies/${fileId}.${c.ext}` : vaultRel(c.sha256, c.ext),
    ...o,
  };
};
const dests = (entries: Entry[]): string[] => entries.map((e) => e.dest).sort();

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

test('Live MOV links from the vault, and its byte copies go to copies/v-', () => {
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
    { dest: 'copies/v-66666666/IMG_9.MOV', src: 'vault/.copies/3.mov' },
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
      { dest: 'copies/g-aaaaaaaa/N.HEIC', src: vaultRel(c.sha256, 'heic') },
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
  const best = C('a'.repeat(64)),
    j = C('b'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const files = [
    Fl(1n, 'IMG_1.HEIC', best),
    Fl(2n, 'x/IMG_1.JPG', j),
    Fl(3n, 'y/IMG_1.JPG', j, { state: 'COPY' }),
    Fl(4n, 'z/IMG_1.JPG', j, { state: 'COPY' }),
  ];
  expect(dests(planStaging([best, j], files, 5)).filter((d) => d.startsWith('copies/'))).toEqual([
    'copies/g-aaaaaaaa/IMG_1.JPG',
    'copies/g-aaaaaaaa/IMG_1_bbbbbbbb.JPG',
    'copies/g-aaaaaaaa/IMG_1_bbbbbbbb_2.JPG',
  ]);
});

test('collisions are case-insensitive and suffix names without an extension', () => {
  const best = C('a'.repeat(64));
  const j = C('b'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const k = C('c'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const files = [
    Fl(1n, 'IMG_1.HEIC', best),
    Fl(2n, 'x/img_1.jpg', j),
    Fl(3n, 'y/IMG_1.JPG', k),
    Fl(4n, 'p/README', j, { state: 'COPY' }),
    Fl(5n, 'q/README', j, { state: 'COPY' }),
  ];
  expect(dests(planStaging([best, j, k], files, 5)).filter((d) => d.startsWith('copies/'))).toEqual(
    [
      'copies/g-aaaaaaaa/IMG_1_cccccccc.JPG',
      'copies/g-aaaaaaaa/README',
      'copies/g-aaaaaaaa/README_bbbbbbbb',
      'copies/g-aaaaaaaa/img_1.jpg',
    ],
  );
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

test('quarantine byte copies go to copies/v-, an odd reason is made path-safe', () => {
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
  ).toEqual([
    'copies/v-eeeeeeee/BAD.JPG',
    'quarantine/.._bad_reason/BAD.JPG',
    'quarantine/unknown/N.JPG',
  ]);
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
  expect(dests(a)).toEqual(['copies/v-11111111/PSWU5412.MP4', 'other/PSWU5412 1.MP4']);
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
    const c = w.contents.find((x) => x.sha256 === f.sha256)!;
    const fileId = BigInt(i + 1);
    return {
      ...f,
      fileId,
      srcRel: f.state === 'COPY' ? `vault/.copies/${fileId}.${c.ext}` : f.srcRel,
    };
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
  const byCopy = new Map(w.files.map((f) => [f.srcRel, f.sha256]));
  return entries.map((e) => [e.dest, byCopy.get(e.src)!]);
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
