import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import {
  appendFile,
  copyFile,
  mkdir,
  readdir,
  readFile,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { basename, join } from 'node:path';
import sharp from 'sharp';
import { beforeAll, describe, expect, test, vi } from 'vitest';
import {
  PINNED,
  buildManifest,
  buildRelations,
  defaultCopier,
  expectedRelation,
  synthesize,
  type Copier,
  type TruthFacts,
  loadManifest,
  seed,
  selectSubsets,
  verify,
  type Manifest,
  type ManifestRecord,
} from '../../tools/corpus/lib.js';
import { HEIC_SCENE0 } from '../helpers/images.js';
import { tmpDir } from '../helpers/layout.js';

let tmp: string;
beforeAll(async () => {
  tmp = await tmpDir();
});

/** All synthetic records share one capture instant unless overridden (no subsec, so no bursts). */
const T0 = '2021-01-01T00:00:00.000Z';

function rec(name: string, overrides: Partial<ManifestRecord> = {}): ManifestRecord {
  return {
    name,
    size: '100',
    mtimeNs: '1000000000',
    sha256: `sha-${name}`,
    kind: /\.(mov|mp4)$/i.test(name) ? 'motion' : 'still',
    extMismatch: false,
    width: 10,
    height: 10,
    capturedAt: T0,
    subsec: null,
    camera: null,
    burstUuid: null,
    contentId: null,
    durationS: null,
    ...overrides,
  };
}

function fakeManifest(n: number, opts: { omit?: string[] } = {}): Manifest {
  const names = Array.from({ length: n }, (_, i) => `F${String(i).padStart(4, '0')}.JPG`);
  const records = [...names, ...PINNED.filter((p) => !opts.omit?.includes(p))].map((x) => rec(x));
  // Live pairs and burst sets for selectSubsets to choose from.
  for (let i = 0; i < 30; i++) {
    records.push(rec(`L${i}.HEIC`, { contentId: `c${i}` }));
    records.push(rec(`L${i}.MOV`, { contentId: `c${i}`, durationS: 2 }));
  }
  for (let i = 0; i < 8; i++)
    for (let j = 0; j < 3; j++) records.push(rec(`B${i}_${j}.HEIC`, { burstUuid: `b${i}` }));
  return { version: 1, records, relations: buildRelations(records) };
}

let corpusSeq = 0;
const exifAt = (date: string, subsec: string) => ({
  IFD0: { Make: 'Apple', Model: 'iPhone 12' },
  IFD2: { DateTimeOriginal: date, SubSecTimeOriginal: subsec },
});

/** A tmp "corpus": a.jpg, b.jpg and c.jpg (b and c are a burst), d.png, e.heic. */
async function makeCorpus(): Promise<string> {
  const dir = join(tmp, `corpus-${++corpusSeq}`);
  const img = (r: number) =>
    sharp({ create: { width: 64, height: 48, channels: 3, background: { r, g: 90, b: 160 } } });
  await mkdir(dir, { recursive: true });
  await img(10).jpeg().toFile(join(dir, 'a.jpg'));
  await img(80).withExif(exifAt('2021:10:03 14:02:11', '120')).jpeg().toFile(join(dir, 'b.jpg'));
  await img(90).withExif(exifAt('2021:10:03 14:02:12', '480')).jpeg().toFile(join(dir, 'c.jpg'));
  await img(200).png().toFile(join(dir, 'd.png'));
  await copyFile(HEIC_SCENE0, join(dir, 'e.heic'));
  return dir;
}

async function snapshot(dir: string): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const n of (await readdir(dir)).sort()) {
    const s = await stat(join(dir, n), { bigint: true });
    out[n] =
      `${s.size}:${s.mtimeNs}:${s.mode}:${(await readFile(join(dir, n))).toString('base64')}`;
  }
  return out;
}

const manifestOf = (dir: string) =>
  buildManifest(dir, { cachePath: join(tmp, `cache-${++corpusSeq}.json`) });

describe('relations', () => {
  test('relations from names', () => {
    const r = buildRelations(
      [
        'IMG_0034.HEIC',
        'IMG_0034.JPG',
        'IMG_E0433.HEIC',
        'IMG_0433.HEIC',
        'IMG_2834 1.HEIC',
        'IMG_2834.HEIC',
      ].map((n) => rec(n)),
    );
    expect(r.heicJpgTwin).toContainEqual(['IMG_0034.HEIC', 'IMG_0034.JPG']);
    expect(r.editOf).toContainEqual(['IMG_E0433.HEIC', 'IMG_0433.HEIC']);
    expect(r.spaceSuffixCopy).toContainEqual(['IMG_2834 1.HEIC', 'IMG_2834.HEIC']);
  });

  test('a same-name HEIC/JPG with different capture times is not a twin', () => {
    const r = buildRelations([
      rec('IMG_0034.HEIC'),
      rec('IMG_0034.JPG', { capturedAt: '2019-05-05T10:00:00.000Z' }),
      rec('X.HEIC', { capturedAt: null }),
      rec('X.JPG', { capturedAt: null }),
    ]);
    expect(r.heicJpgTwin).toEqual([]);
  });

  test('burst relation needs same camera and different subsec', () => {
    const at = (s: number) => `2021-10-03T14:02:${s}.000Z`,
      cam = 'Apple iPhone 12';
    const r = buildRelations([
      rec('A.HEIC', { capturedAt: at(11), subsec: '1', camera: cam }),
      rec('B.HEIC', { capturedAt: at(12), subsec: '5', camera: cam }),
      rec('C.HEIC', { capturedAt: at(11), subsec: '7', camera: 'Canon' }),
      rec('D.HEIC', { capturedAt: at(11), subsec: '1', camera: cam }),
    ]);
    expect(r.burst).toContainEqual(['A.HEIC', 'B.HEIC']);
    expect(r.burst.flat()).not.toContain('C.HEIC');
    expect(r.burst).not.toContainEqual(['A.HEIC', 'D.HEIC']);
  });

  test('burst: shared BurstUUID, and nothing beyond 2 s', () => {
    const cam = 'Apple iPhone 12';
    const r = buildRelations([
      rec('P.HEIC', { burstUuid: 'u1', capturedAt: '2021-10-03T14:00:00.000Z' }),
      rec('Q.HEIC', { burstUuid: 'u1', capturedAt: '2021-10-03T14:09:00.000Z' }),
      rec('R.HEIC', { capturedAt: '2021-10-03T15:00:00.000Z', subsec: '1', camera: cam }),
      rec('S.HEIC', { capturedAt: '2021-10-03T15:00:03.000Z', subsec: '2', camera: cam }),
    ]);
    expect(r.burst).toEqual([['P.HEIC', 'Q.HEIC']]);
  });

  test('exact groups, edits across extensions, live photos', () => {
    const r = buildRelations([
      rec('A.JPG', { sha256: 'x' }),
      rec('A copy.JPG', { sha256: 'x' }),
      rec('0.JPG', { sha256: 'y' }),
      rec('0 copy.JPG', { sha256: 'y' }),
      rec('IMG_E6941.JPG'),
      rec('IMG_6941.HEIC'),
      rec('IMG_E9999.HEIC'),
      rec('IMG_1.HEIC', { contentId: 'cid' }),
      rec('other.MOV', { kind: 'motion', contentId: 'cid', durationS: 30 }),
      rec('IMG_2.HEIC'),
      rec('IMG_2.MOV', { kind: 'motion', durationS: 3 }),
      rec('IMG_3.HEIC'),
      rec('IMG_3.MOV', { kind: 'motion', durationS: 12 }),
      rec('IMG_4.HEIC'),
      rec('IMG_4.MOV', { kind: 'motion', durationS: null }),
    ]);
    expect(r.exactGroups).toEqual([
      ['0 copy.JPG', '0.JPG'],
      ['A copy.JPG', 'A.JPG'],
    ]);
    expect(r.editOf).toEqual([['IMG_E6941.JPG', 'IMG_6941.HEIC']]);
    expect(r.livePhoto).toEqual([
      ['IMG_1.HEIC', 'other.MOV'],
      ['IMG_2.HEIC', 'IMG_2.MOV'],
    ]);
  });
});

describe('expectedRelation (§6.3 for a look-alike pair; copy-if-tight: the rows needing copy distances)', () => {
  const base: TruthFacts = {
    capturedAt: '2021-10-03T14:02:11.000Z',
    subsec: '120',
    camera: 'Apple iPhone 12',
    burstUuid: null,
    width: 4032,
    height: 3024,
    mime: 'image/heic',
  };
  const jpgHalf = { width: 2016, height: 1512, mime: 'image/jpeg' };
  const none = { capturedAt: null, subsec: null, camera: null };
  test.each<
    [string, Partial<TruthFacts>, Partial<TruthFacts>, 'copy' | 'copy-if-tight' | 'similar']
  >([
    ['same BurstUUID', { burstUuid: 'u' }, { burstUuid: 'u', ...jpgHalf }, 'similar'],
    ['different second', {}, { capturedAt: '2021-10-03T14:02:12.000Z' }, 'similar'],
    ['same second, ms ignored', {}, { capturedAt: '2021-10-03T14:02:11.900Z' }, 'copy'],
    ['different subsec', {}, { subsec: '480' }, 'similar'],
    ['different camera', {}, { camera: 'Canon', ...jpgHalf }, 'similar'],
    ['same capture and subsec', {}, {}, 'copy'],
    ['subsec missing, other encoding', {}, { subsec: null, ...jpgHalf }, 'copy-if-tight'],
    ['subsec missing, same encoding', {}, { subsec: null }, 'similar'],
    ['one side has no capture', {}, { ...none }, 'copy-if-tight'],
    ['neither has capture, other mime', none, { ...none, mime: 'image/jpeg' }, 'copy-if-tight'],
    ['neither has capture, other size', none, { ...none, width: 2016 }, 'copy-if-tight'],
    ['neither has capture, same encoding', none, { ...none }, 'similar'],
  ])('%s', (_, a, b, want) => {
    expect(expectedRelation({ ...base, ...a }, { ...base, ...b })).toBe(want);
    expect(expectedRelation({ ...base, ...b }, { ...base, ...a })).toBe(want);
  });
});

describe('subsets', () => {
  test('subsets are deterministic and include pins', () => {
    const m = fakeManifest(500);
    expect(selectSubsets(m, 7)).toEqual(selectSubsets(m, 7));
    expect(selectSubsets(m, 7).smoke).toEqual(expect.arrayContaining([...PINNED]));
  });

  test('subset shape: sizes, pins in both, ≥ 20 live pairs and ≥ 5 bursts, seed matters', () => {
    const m = fakeManifest(3000);
    const { smoke, standard } = selectSubsets(m, 7);
    expect(smoke).toHaveLength(150);
    expect(standard).toHaveLength(2000);
    expect(new Set(standard).size).toBe(standard.length);
    expect(standard).toEqual(expect.arrayContaining([...PINNED, ...smoke]));
    const live = m.relations.livePhoto.filter(([a, b]) => smoke.includes(a) && smoke.includes(b));
    expect(live.length).toBeGreaterThanOrEqual(20);
    const bursts = new Set(smoke.filter((n) => n.startsWith('B')).map((n) => n.split('_')[0]));
    expect(bursts.size).toBeGreaterThanOrEqual(5);
    expect(selectSubsets(m, 8).smoke).not.toEqual(smoke);
  });

  test('a small corpus yields every file', () => {
    const records = PINNED.map((p) => rec(p));
    const m: Manifest = { version: 1, records, relations: buildRelations(records) };
    expect(selectSubsets(m, 1).standard.sort()).toEqual([...PINNED].sort());
  });

  test('smoke takes BurstUUID-backed bursts first (the hard "bursts never collapse" case)', () => {
    const base = fakeManifest(500).records.filter((r) => r.burstUuid === null);
    const timed = Array.from({ length: 20 }, (_, i) =>
      ['1', '2', '3'].map((sub) =>
        rec(`T${String(i)}_${sub}.JPG`, {
          camera: 'cam',
          subsec: sub,
          capturedAt: new Date(Date.parse(T0) + i * 60_000).toISOString(),
        }),
      ),
    ).flat();
    const uuid = [0, 1].flatMap((i) =>
      [0, 1, 2].map((j) => rec(`U${String(i)}_${String(j)}.HEIC`, { burstUuid: `u${String(i)}` })),
    );
    const records = [...base, ...timed, ...uuid];
    const m: Manifest = { version: 1, records, relations: buildRelations(records) };
    for (const seed of [1, 2, 3, 7, 42]) {
      const smoke = new Set(selectSubsets(m, seed).smoke);
      expect(
        uuid.every((r) => smoke.has(r.name)),
        `seed ${String(seed)}`,
      ).toBe(true);
    }
  });

  test('selectSubsets throws when a pinned file is missing', () =>
    expect(() => selectSubsets(fakeManifest(500, { omit: ['IMG_2834 1.MOV'] }), 7)).toThrow(
      /IMG_2834 1\.MOV/,
    ));
});

describe('manifest', () => {
  test('manifest numbers are decimal strings', async () => {
    const rec0 = (await manifestOf(await makeCorpus())).records[0]!;
    expect(rec0.size).toMatch(/^\d+$/);
    expect(rec0.mtimeNs).toMatch(/^\d+$/);
  });

  test('loadManifest reads a written manifest back', async () => {
    const m = fakeManifest(3);
    const p = join(tmp, 'm.json');
    await writeFile(p, JSON.stringify(m));
    expect(await loadManifest(p)).toEqual(m);
  });

  test('records carry independent facts; the cache is reused', async () => {
    const src = await makeCorpus();
    await writeFile(join(src, 'notes.jpg'), 'plain text, not a JPEG');
    await writeFile(join(src, '.DS_Store'), 'ignored');
    await writeFile(join(src, 'empty.jpg'), '');
    await writeFile(join(src, 'trunc.heic'), (await readFile(HEIC_SCENE0)).subarray(0, 300));
    const cachePath = join(tmp, 'reuse-cache.json');
    const m = await buildManifest(src, { cachePath });
    const by = new Map(m.records.map((r) => [r.name, r]));
    expect([...by.keys()]).toEqual([
      'a.jpg',
      'b.jpg',
      'c.jpg',
      'd.png',
      'e.heic',
      'empty.jpg',
      'notes.jpg',
      'trunc.heic',
    ]);
    expect(by.get('b.jpg')).toMatchObject({
      kind: 'still',
      extMismatch: false,
      width: 64,
      height: 48,
      capturedAt: '2021-10-03T14:02:11.000Z',
      subsec: '120',
      camera: 'Apple iPhone 12',
    });
    const digest = createHash('sha256')
      .update(await readFile(join(src, 'b.jpg')))
      .digest('hex');
    expect(by.get('b.jpg')!.sha256).toBe(digest);
    expect(by.get('e.heic')).toMatchObject({ kind: 'still', width: 320, height: 240 });
    expect(by.get('d.png')!.capturedAt).toBeNull();
    expect(by.get('notes.jpg')).toMatchObject({ kind: 'other', extMismatch: true, width: null });
    expect(by.get('empty.jpg')).toMatchObject({ kind: 'other', extMismatch: true });
    expect(by.get('trunc.heic')).toMatchObject({ width: null, height: null });
    expect(m.relations.burst).toEqual([['b.jpg', 'c.jpg']]);

    // A poisoned cache entry with matching (name, size, mtimeNs) is trusted: proof of reuse.
    const cache = JSON.parse(await readFile(cachePath, 'utf8')) as {
      entries: Record<string, ManifestRecord>;
    };
    cache.entries['a.jpg']!.sha256 = 'from-cache';
    await writeFile(cachePath, JSON.stringify(cache));
    const again = await buildManifest(src, { cachePath });
    expect(again.records.find((r) => r.name === 'a.jpg')!.sha256).toBe('from-cache');
  });

  test('verify detects drift', async () => {
    const src = await makeCorpus();
    const m = await manifestOf(src);
    expect(await verify(src, m, true)).toEqual([]);
    await appendFile(join(src, 'a.jpg'), 'x');
    await rm(join(src, 'b.jpg'));
    const drift = await verify(src, m, true);
    expect(drift).toHaveLength(2);
    expect(drift).toEqual(
      expect.arrayContaining([expect.stringContaining('a.jpg'), expect.stringContaining('b.jpg')]),
    );
  });

  test('verify: same-size rewrite needs deep; unexpected files are drift', async () => {
    const src = await makeCorpus();
    const p = join(src, 'd.png');
    await utimes(p, 1_600_000_000, 1_600_000_000); // whole seconds survive a utimes round trip
    const m = await manifestOf(src);
    const buf = await readFile(p);
    buf[buf.length - 1] = buf[buf.length - 1]! ^ 0xff;
    await writeFile(p, buf);
    await utimes(p, 1_600_000_000, 1_600_000_000);
    await writeFile(join(src, 'new.jpg'), 'x');
    const shallow = await verify(src, m, false);
    expect(shallow).toEqual([expect.stringContaining('new.jpg')]);
    const deep = await verify(src, m, true);
    expect(deep).toHaveLength(2);
    expect(deep).toEqual(
      expect.arrayContaining([expect.stringMatching(/sha256.*d\.png|d\.png.*sha256/)]),
    );
  });
});

describe('seed', () => {
  test('seed clones without touching source', async () => {
    const src = await makeCorpus();
    const before = await snapshot(src);
    await seed({ corpusDir: src, names: ['a.jpg'], dest: `${tmp}/dz` });
    expect(await snapshot(src)).toEqual(before);
    expect(existsSync(`${tmp}/dz/a.jpg`)).toBe(true);
  });

  test('seed manifest: hashes, mtimes kept, nested layout, extras, synthetic variants', async () => {
    const src = await makeCorpus();
    const before = await snapshot(src);
    const m = await manifestOf(src);
    const dest = join(tmp, 'dz-nested');
    const sm = await seed({
      corpusDir: src,
      names: ['a.jpg', 'b.jpg', 'c.jpg', 'e.heic'],
      dest,
      layout: 'nested',
      shuffle: 3,
      extras: ['junk', 'corrupt'],
      synthetic: true,
      prefix: 'z-',
    });
    expect(await snapshot(src)).toEqual(before);
    expect(sm.dest).toBe(dest);
    const truth = new Map(m.records.map((r) => [r.name, r]));
    for (const f of sm.files) {
      const s = await stat(join(dest, f.relPath), { bigint: true });
      expect([f.size, f.mtimeNs]).toEqual([String(s.size), String(s.mtimeNs)]);
    }
    const corpus = sm.files.filter((f) => f.origin === 'corpus');
    expect(corpus.map((f) => f.name).sort()).toEqual(['a.jpg', 'b.jpg', 'c.jpg', 'e.heic']);
    for (const f of corpus) {
      expect(f.relPath).toMatch(/^d\d Été\/e\d 写真\//); // nested Unicode directories
      expect(f.sha256).toBe(truth.get(f.name)!.sha256);
      expect(f.mtimeNs).toBe(truth.get(f.name)!.mtimeNs);
    }
    expect(sm.files.filter((f) => f.origin === 'junk').length).toBeGreaterThan(0);
    expect(
      sm.files
        .filter((f) => f.origin === 'corrupt')
        .map((f) => f.reason)
        .sort(),
    ).toEqual(['decode_error', 'truncated']);

    const synth = sm.files.filter((f) => f.origin === 'synthetic');
    const variants = (seedName: string) =>
      synth
        .filter((f) => f.seedName === seedName)
        .map((f) => f.variant)
        .sort();
    expect(variants('a.jpg')).toEqual(['png', 'q60-noexif', 'resize50']);
    expect(variants('e.heic')).toEqual(['png', 'q60-noexif', 'resize50']);
    expect(variants('b.jpg')).toEqual(['bridge', 'png', 'q60-noexif', 'resize50']);
    expect(synth.every((f) => f.name.startsWith('z-'))).toBe(true);
    const truthOf = (seedName: string, variant: string) =>
      synth
        .filter((f) => f.seedName === seedName && f.variant === variant)
        .map((f) => [f.expect, f.bridges ?? null]);
    // a.jpg has no capture time: a same-size, same-mime re-encode is only "similar" (§6.3 row 8).
    // Rows 7–8 (and 6) need the copy distances, which the oracle can't measure: copy-if-tight.
    expect(truthOf('a.jpg', 'resize50')).toEqual([['copy-if-tight', null]]);
    expect(truthOf('a.jpg', 'q60-noexif')).toEqual([['similar', null]]);
    expect(truthOf('a.jpg', 'png')).toEqual([['copy-if-tight', null]]);
    expect(truthOf('e.heic', 'q60-noexif')).toEqual([['copy-if-tight', null]]);
    // b.jpg and c.jpg are a burst: EXIF-kept variants are copies, EXIF-less ones are bridges.
    expect(truthOf('b.jpg', 'resize50')).toEqual([['copy', null]]);
    expect(truthOf('b.jpg', 'png')).toEqual([['copy', null]]);
    expect(truthOf('b.jpg', 'q60-noexif')).toEqual([['copy-or-ambiguous', ['b.jpg', 'c.jpg']]]);
    expect(truthOf('b.jpg', 'bridge')).toEqual([['copy-or-ambiguous', ['b.jpg', 'c.jpg']]]);
    expect(truthOf('c.jpg', 'q60-noexif')).toEqual([['copy-or-ambiguous', ['c.jpg', 'b.jpg']]]);
    const resized = synth.find((f) => f.seedName === 'b.jpg' && f.variant === 'resize50')!;
    const meta = await sharp(join(dest, resized.relPath)).metadata();
    expect([meta.width, meta.height, meta.exif !== undefined]).toEqual([32, 24, true]);
    const bridge = synth.find((f) => f.variant === 'bridge')!;
    expect((await sharp(join(dest, bridge.relPath)).metadata()).exif).toBeUndefined();
  });

  test('shuffle changes only the copy order, never the content', async () => {
    const src = await makeCorpus();
    const names = ['a.jpg', 'b.jpg', 'c.jpg', 'd.png'];
    const one = await seed({ corpusDir: src, names, dest: join(tmp, 's1'), shuffle: 1 });
    const two = await seed({ corpusDir: src, names, dest: join(tmp, 's2'), shuffle: 2 });
    const key = (f: { relPath: string; sha256: string }) => `${f.relPath}:${f.sha256}`;
    expect(one.files.map(key).sort()).toEqual(two.files.map(key).sort());
  });

  test('same-stem HEIC and JPG seeds get distinct variant names', async () => {
    const src = await makeCorpus();
    await copyFile(join(src, 'a.jpg'), join(src, 'e.jpg'));
    const dest = join(tmp, 'dz-stem');
    const files = await synthesize(src, ['e.heic', 'e.jpg'], dest);
    expect(files.map((f) => f.name).sort()).toEqual([
      'synth-png-e_heic.png',
      'synth-png-e_jpg.png',
      'synth-q60-noexif-e_heic.jpg',
      'synth-q60-noexif-e_jpg.jpg',
      'synth-resize50-e_heic.jpg',
      'synth-resize50-e_jpg.jpg',
    ]);
  });

  test('a tmp dir on APFS clones: no fallback copies', async () => {
    const src = await makeCorpus();
    const sm = await seed({ corpusDir: src, names: ['a.jpg', 'b.jpg'], dest: join(tmp, 'cl') });
    expect(sm.fallbackCopies).toBe(0);
    expect(await defaultCopier.canClone(src, join(tmp, 'cl'))).toBe(true);
  });

  test('byte copy only where cloning is unsupported, loudly counted', async () => {
    const src = await makeCorpus();
    const calls: string[] = [];
    const copier: Copier = {
      canClone: () => Promise.resolve(false),
      clone: () => Promise.reject(new Error('must not clone')),
      copy: (a, b) => (calls.push(basename(a)), defaultCopier.copy(a, b)),
    };
    const warn = vi.spyOn(console, 'warn').mockImplementation((m: string) => void calls.push(m));
    try {
      const dest = join(tmp, 'fb');
      const sm = await seed({ corpusDir: src, names: ['a.jpg', 'b.jpg'], dest }, copier);
      expect(sm.fallbackCopies).toBe(2);
      // Warned up front, before the first byte is copied (a big tier copies for a long time).
      expect(calls[0]).toMatch(/2 .*byte cop/);
      expect(calls.slice(1).sort()).toEqual(['a.jpg', 'b.jpg']);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(await readFile(join(dest, 'a.jpg'))).toEqual(await readFile(join(src, 'a.jpg')));
      const [s, d] = await Promise.all([stat(join(src, 'a.jpg')), stat(join(dest, 'a.jpg'))]);
      expect(Math.abs(d.mtimeMs - s.mtimeMs)).toBeLessThan(0.01);
    } finally {
      warn.mockRestore();
    }
  });

  test('byte copies beyond the smoke size need ALLOW_BYTE_COPY=1, refused before any copy', async () => {
    const src = await makeCorpus();
    const names = Array.from({ length: 151 }, (_, i) => `many-${String(i)}.jpg`);
    for (const n of names) await writeFile(join(src, n), n);
    const copies: string[] = [];
    const copier: Copier = {
      canClone: () => Promise.resolve(false),
      clone: () => Promise.reject(new Error('must not clone')),
      copy: (a, b) => (copies.push(a), defaultCopier.copy(a, b)),
    };
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    try {
      await expect(
        seed({ corpusDir: src, names, dest: join(tmp, 'big'), allowByteCopy: false }, copier),
      ).rejects.toThrow(/ALLOW_BYTE_COPY=1/);
      expect(copies).toEqual([]);
      const sm = await seed(
        { corpusDir: src, names, dest: join(tmp, 'big-ok'), allowByteCopy: true },
        copier,
      );
      expect(sm.fallbackCopies).toBe(151);
    } finally {
      warn.mockRestore();
    }
  });

  test('a clone failure where cloning is supported is fatal, never a silent copy', async () => {
    const src = await makeCorpus();
    const copier: Copier = {
      canClone: () => Promise.resolve(true),
      clone: () => Promise.reject(new Error('EIO: clone broke')),
      copy: () => Promise.reject(new Error('must not copy')),
    };
    await expect(
      seed({ corpusDir: src, names: ['a.jpg'], dest: join(tmp, 'fatal') }, copier),
    ).rejects.toThrow(/clone broke/);
  });

  test('destinations inside the corpus are refused', async () => {
    const src = await makeCorpus();
    const before = await snapshot(src);
    await expect(seed({ corpusDir: src, names: ['a.jpg'], dest: join(src, 'dz') })).rejects.toThrow(
      /inside the corpus/,
    );
    await expect(
      seed({ corpusDir: src, names: ['a.jpg'], dest: join(src, 'deep', 'er', 'dz') }),
    ).rejects.toThrow(/inside the corpus/);
    const link = join(tmp, `link-${corpusSeq}`);
    await symlink(src, link);
    await expect(
      seed({ corpusDir: src, names: ['a.jpg'], dest: join(link, 'x', 'dz') }),
    ).rejects.toThrow(/inside the corpus/);
    await expect(synthesize(src, ['a.jpg'], join(src, 'syn'))).rejects.toThrow(/inside the corpus/);
    expect(await snapshot(src)).toEqual(before);
  });

  test('seed rejects a missing source and an existing destination file', async () => {
    const src = await makeCorpus();
    await expect(
      seed({ corpusDir: src, names: ['nope.jpg'], dest: join(tmp, 'x1') }),
    ).rejects.toThrow(/nope\.jpg/);
    const dest = join(tmp, 'x2');
    await seed({ corpusDir: src, names: ['a.jpg'], dest });
    await expect(seed({ corpusDir: src, names: ['a.jpg'], dest })).rejects.toThrow(/a\.jpg/);
  });
});
