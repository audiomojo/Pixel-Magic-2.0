/**
 * Corpus ground truth: an oracle that is independent of the pipeline (full SHA-256, `sips`
 * dimensions, the exiftool CLI called directly), deterministic subsets, a copy-on-write seeder
 * and a drift verifier. The corpus is only ever read and cloned from, never written.
 */
import { execFile as execFileCb } from 'node:child_process';
import { constants } from 'node:fs';
import { availableParallelism, tmpdir } from 'node:os';
import {
  copyFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  stat,
  statfs,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { basename, dirname, extname, isAbsolute, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { Worker } from 'node:worker_threads';
import { exiftoolPath } from 'exiftool-vendored';
import sharp, { type Sharp } from 'sharp';
import { isIgnored } from '../../src/fsops.js';

const execFile = promisify(execFileCb);

const REPO = fileURLToPath(new URL('../../', import.meta.url));
/** Default locations: the private corpus, and the git-ignored manifest and hash cache. */
export const PATHS = {
  corpus: join(REPO, 'test-automation/sample-images'),
  manifest: join(REPO, 'tests/corpus/manifest.json'),
  cache: join(REPO, 'tests/corpus/.hash-cache.json'),
} as const;

export interface ManifestRecord {
  name: string;
  /** Decimal string (sizes and mtimes are never `number`). */
  size: string;
  /** Decimal string, nanoseconds. */
  mtimeNs: string;
  sha256: string;
  kind: 'still' | 'motion' | 'other';
  /** The extension disagrees with the sniffed file type. */
  extMismatch: boolean;
  width: number | null;
  height: number | null;
  /** ISO 8601, UTC. */
  capturedAt: string | null;
  subsec: string | null;
  camera: string | null;
  burstUuid: string | null;
  contentId: string | null;
  durationS: number | null;
}

type Pair = [string, string];

export interface Relations {
  /** Byte-identical files (same SHA-256), each group sorted. */
  exactGroups: string[][];
  /** Same stem as HEIC and JPG and the same capture instant. */
  heicJpgTwin: Pair[];
  /** `[edit, original]`: `IMG_Ennnn.*` and `IMG_nnnn.*` of the same kind. */
  editOf: Pair[];
  /** Name-only: `"X 1.ext"` vs `"X.ext"`. Never implies "copy". */
  spaceSuffixCopy: Pair[];
  /** Same BurstUUID, or ≤ 2 s apart from the same camera with a different capture key. */
  burst: Pair[];
  /** Still + motion sharing a content ID, or the same stem with a MOV ≤ 5 s. */
  livePhoto: Pair[];
}

export interface Manifest {
  version: 1;
  records: ManifestRecord[];
  relations: Relations;
}

export interface SeedOptions {
  corpusDir: string;
  names: string[];
  dest: string;
  layout?: 'flat' | 'nested';
  shuffle?: number;
  extras?: Array<'junk' | 'corrupt'>;
  synthetic?: boolean;
  prefix?: string;
  /**
   * Allow byte copies (cloning unsupported) of more than the smoke tier's file count. Default:
   * `ALLOW_BYTE_COPY=1` in the environment. Without it a big tier fails before copying anything.
   */
  allowByteCopy?: boolean;
}

export interface SeedFile {
  relPath: string;
  name: string;
  sha256: string;
  size: string;
  mtimeNs: string;
  origin: 'corpus' | 'synthetic' | 'junk' | 'corrupt';
  seedName?: string;
  variant?: 'resize50' | 'q60-noexif' | 'png' | 'bridge';
  /**
   * Synthetic ground truth vs `seedName` (§6.3). `copy-if-tight`: a §6.3 row that is a copy only
   * within the copy distances, which this oracle can't measure; the e2e check takes them from
   * the pair's stored hashes. `copy-or-ambiguous` marks an EXIF-less copy of a burst frame
   * (spec §12 R3): it ends up a copy of one of `bridges` or `copy_ambiguous`, and the two
   * `bridges` frames are never in the same copy group.
   */
  expect?: 'copy' | 'copy-if-tight' | 'similar' | 'copy-or-ambiguous';
  /** `[seedName, its burst partner]` when `expect` is `copy-or-ambiguous`. */
  bridges?: [string, string];
  reason?: string;
}

export interface SeedManifest {
  dest: string;
  files: SeedFile[];
  /** Corpus files byte-copied because the destination can't clone (0 on one APFS volume). */
  fallbackCopies: number;
}

/** Spec §10.3: in both subsets. Only files that exist (there is no `IMG_2834.MOV`). */
export const PINNED: readonly string[] = Object.freeze([
  'IMG_E8719.HEIC',
  'IMG_0004.MOV',
  'PSWU5412.MP4',
  'PSWU5412 1.MP4',
  'IMG_0034.HEIC',
  'IMG_0034.JPG',
  'IMG_6941 1.HEIC',
  'IMG_E6941.JPG',
  'IMG_0433.HEIC',
  'IMG_E0433.HEIC',
  'IMG_2834.HEIC',
  'IMG_2834 1.HEIC',
  'IMG_2834 1.MOV',
  'VERP1022.TIF',
  'AUXA4949.BMP',
]);

const SUBSETS = {
  smoke: { size: 150, livePairs: 20, bursts: 5 },
  standard: { size: 2000, livePairs: 100, bursts: 20 },
} as const;
/** Frames taken from one burst set (a big burst would otherwise swamp the smoke tier). */
const MAX_BURST_FRAMES = 6;
const BURST_WINDOW_S = 2;
const LIVE_MAX_DURATION_S = 5;

// ---------------------------------------------------------------------------------------------
// Hashing: SHA-256 on a worker_threads pool.

const HASH_WORKER = `
const { parentPort } = require('node:worker_threads');
const { createReadStream } = require('node:fs');
const { createHash } = require('node:crypto');
parentPort.on('message', ({ id, path }) => {
  const h = createHash('sha256');
  createReadStream(path, { highWaterMark: 1 << 20 })
    .on('data', (c) => h.update(c))
    .on('error', (e) => parentPort.postMessage({ id, error: String(e) }))
    .on('end', () => parentPort.postMessage({ id, sha: h.digest('hex') }));
});
`;

type HashReply = { id: number; sha?: string; error?: string };

/** SHA-256 of every path, in order, hashed in parallel on `availableParallelism()` workers. */
export async function hashFiles(paths: string[]): Promise<string[]> {
  const out = new Array<string>(paths.length);
  if (paths.length === 0) return out;
  const n = Math.min(availableParallelism(), paths.length);
  let next = 0;
  const run = (w: Worker) =>
    new Promise<void>((resolve, reject) => {
      const send = () => {
        if (next >= paths.length) return resolve();
        const id = next++;
        w.postMessage({ id, path: paths[id] });
      };
      w.on('message', (m: HashReply) => {
        if (m.sha === undefined) return reject(new Error(`hash ${paths[m.id]}: ${m.error}`));
        out[m.id] = m.sha;
        send();
      });
      w.on('error', reject);
      send();
    });
  const workers = Array.from({ length: n }, () => new Worker(HASH_WORKER, { eval: true }));
  try {
    await Promise.all(workers.map(run));
  } finally {
    await Promise.all(workers.map((w) => w.terminate()));
  }
  return out;
}

// ---------------------------------------------------------------------------------------------
// Metadata: exiftool CLI and sips, both batched.

const EXIF_TAGS = [
  '-File:FileType',
  '-File:FileTypeExtension',
  '-File:MIMEType',
  '-EXIF:DateTimeOriginal',
  '-EXIF:OffsetTimeOriginal',
  '-EXIF:SubSecTimeOriginal',
  '-EXIF:Orientation',
  '-EXIF:Make',
  '-EXIF:Model',
  '-MakerNotes:BurstUUID',
  '-MakerNotes:ContentIdentifier',
  '-QuickTime:ContentIdentifier',
  '-QuickTime:CreateDate',
  '-QuickTime:Duration',
];
const BATCH = 400;
const MAX_BUFFER = 256 * 1024 * 1024;

type Tags = Record<string, unknown>;

async function inBatches<T>(
  items: string[],
  size: number,
  parallel: number,
  fn: (batch: string[]) => Promise<T[]>,
): Promise<T[]> {
  const batches: string[][] = [];
  for (let i = 0; i < items.length; i += size) batches.push(items.slice(i, i + size));
  const results = new Array<T[]>(batches.length);
  let next = 0;
  const lane = async () => {
    while (next < batches.length) {
      const i = next++;
      results[i] = await fn(batches[i]!);
    }
  };
  await Promise.all(Array.from({ length: parallel }, lane));
  return results.flat();
}

/** Raw `-G -n` tags per path (same order as `paths`). */
async function readExif(paths: string[]): Promise<Tags[]> {
  const bin = await exiftoolPath();
  const lanes = Math.max(1, Math.min(4, availableParallelism() >> 1));
  return inBatches(paths, BATCH, lanes, async (batch) => {
    // exiftool exits 1 when some file can't be read (e.g. empty); its JSON is still complete.
    const { stdout } = await execFile(bin, ['-j', '-n', '-G', ...EXIF_TAGS, ...batch], {
      maxBuffer: MAX_BUFFER,
    }).catch((err: unknown) => {
      const out = (err as { stdout?: string }).stdout;
      if (!out) throw err;
      return { stdout: out };
    });
    const byName = new Map(
      (JSON.parse(stdout) as Tags[]).map((t) => [basename(String(t['SourceFile'])), t]),
    );
    return batch.map((p) => byName.get(basename(p)) ?? {});
  });
}

/** Pixel dimensions from `sips` (null when sips can't read the file; it still exits 0). */
async function readDims(paths: string[]): Promise<Array<[number, number] | null>> {
  return inBatches(paths, BATCH, 4, async (batch) => {
    const { stdout } = await execFile('sips', ['-g', 'pixelWidth', '-g', 'pixelHeight', ...batch], {
      maxBuffer: MAX_BUFFER,
    });
    const dims = new Map<string, { w?: number; h?: number }>();
    let cur: { w?: number; h?: number } = {};
    for (const line of stdout.split('\n')) {
      const m = /^\s+pixel(Width|Height): (\d+)$/.exec(line);
      if (m) cur[m[1] === 'Width' ? 'w' : 'h'] = Number(m[2]);
      else if (line !== '' && !/^\s/.test(line)) dims.set(basename(line), (cur = {}));
    }
    return batch.map((p) => {
      const d = dims.get(basename(p));
      return d?.w !== undefined && d.h !== undefined ? [d.w, d.h] : null;
    });
  });
}

const STILL_TYPES = new Set(['JPEG', 'PNG', 'HEIC', 'HEIF', 'WEBP', 'TIFF', 'GIF', 'BMP', 'DNG']);
const EXT_ALIASES: Record<string, string> = { jpeg: 'jpg', tiff: 'tif', heif: 'heic' };
const normExt = (e: string) => EXT_ALIASES[e.toLowerCase()] ?? e.toLowerCase();

const tagStr = (v: unknown): string | null => {
  if (typeof v === 'number') return String(v);
  return typeof v === 'string' && v.trim() !== '' ? v.trim() : null;
};

const EXIF_DATE = /^(\d{4}):(\d{2}):(\d{2}) (\d{2}):(\d{2}):(\d{2})/;

/** `YYYY:MM:DD HH:MM:SS` with an optional `±HH:MM` offset (none = UTC), as ISO UTC. */
function exifIso(raw: unknown, offset: unknown): string | null {
  const m = EXIF_DATE.exec(tagStr(raw) ?? '');
  if (!m || m[1] === '0000') return null;
  const o = /^([+-])(\d{2}):(\d{2})$/.exec(tagStr(offset) ?? '');
  const off = o ? (o[1] === '-' ? -1 : 1) * (Number(o[2]) * 60 + Number(o[3])) : 0;
  const ms = Date.UTC(+m[1]!, +m[2]! - 1, +m[3]!, +m[4]!, +m[5]!, +m[6]!) - off * 60_000;
  return Number.isNaN(ms) ? null : new Date(ms).toISOString();
}

function cameraOf(make: string | null, model: string | null): string | null {
  if (make && model)
    return model.toLowerCase().startsWith(make.toLowerCase()) ? model : `${make} ${model}`;
  return model ?? make;
}

type Facts = Omit<ManifestRecord, 'name' | 'size' | 'mtimeNs' | 'sha256' | 'width' | 'height'>;

function factsOf(name: string, t: Tags): Facts {
  const fileType = tagStr(t['File:FileType']) ?? '';
  const mime = tagStr(t['File:MIMEType']) ?? '';
  const typeExt = tagStr(t['File:FileTypeExtension']);
  const kind =
    mime.startsWith('image/') && STILL_TYPES.has(fileType)
      ? 'still'
      : mime.startsWith('video/')
        ? 'motion'
        : 'other';
  const duration = t['QuickTime:Duration'];
  return {
    kind,
    extMismatch: typeExt === null || normExt(typeExt) !== normExt(extname(name).slice(1)),
    capturedAt:
      exifIso(t['EXIF:DateTimeOriginal'], t['EXIF:OffsetTimeOriginal']) ??
      exifIso(t['QuickTime:CreateDate'], null),
    subsec: tagStr(t['EXIF:SubSecTimeOriginal']),
    camera: cameraOf(tagStr(t['EXIF:Make']), tagStr(t['EXIF:Model'])),
    burstUuid: tagStr(t['MakerNotes:BurstUUID']),
    contentId:
      tagStr(t['MakerNotes:ContentIdentifier']) ?? tagStr(t['QuickTime:ContentIdentifier']),
    durationS: typeof duration === 'number' ? duration : null,
  };
}

type Dims = { width: number | null; height: number | null };
/** What the pipeline compares beyond the record: exiftool MIME type and EXIF orientation. */
type Encoding = { mime: string; orientation: number | null };

/**
 * Record facts (without size/mtime/sha) for `names` in `dir`: exiftool facts plus sips
 * (stored) dimensions, and in `enc` each file's encoding.
 */
async function describe(
  dir: string,
  names: string[],
): Promise<{ facts: Array<Facts & Dims>; enc: Encoding[] }> {
  const tags = await readExif(names.map((n) => join(dir, n)));
  const facts = names.map((n, i) => factsOf(n, tags[i]!));
  const stills = names.filter((_, i) => facts[i]!.kind === 'still');
  const dims = new Map(
    (await readDims(stills.map((n) => join(dir, n)))).map((d, i) => [stills[i]!, d]),
  );
  return {
    facts: facts.map((f, i) => {
      const d = dims.get(names[i]!) ?? null;
      return { ...f, width: d?.[0] ?? null, height: d?.[1] ?? null };
    }),
    enc: tags.map((t) => ({
      mime: tagStr(t['File:MIMEType']) ?? '',
      orientation: typeof t['EXIF:Orientation'] === 'number' ? t['EXIF:Orientation'] : null,
    })),
  };
}

// ---------------------------------------------------------------------------------------------
// Manifest.

interface Cache {
  version: 1;
  entries: Record<string, ManifestRecord>;
}

async function readCache(path: string): Promise<Cache> {
  try {
    const c = JSON.parse(await readFile(path, 'utf8')) as Cache;
    if (c.version === 1) return c;
  } catch {
    // missing or unreadable: start fresh
  }
  return { version: 1, entries: {} };
}

type Listing = { name: string; size: string; mtimeNs: string };

/** Regular, non-ignored files directly in `dir`, sorted by name, with bigint-exact stats. */
async function listDir(dir: string): Promise<Listing[]> {
  const ents = await readdir(dir, { withFileTypes: true });
  const names = ents
    .filter((e) => e.isFile() && !isIgnored(e.name))
    .map((e) => e.name)
    .sort();
  return Promise.all(
    names.map(async (name) => {
      const s = await stat(join(dir, name), { bigint: true });
      return { name, size: String(s.size), mtimeNs: String(s.mtimeNs) };
    }),
  );
}

/**
 * Builds the ground-truth manifest. With `cachePath`, records whose `(name, size, mtimeNs)` match
 * the cache are reused as-is; everything else is hashed and described afresh, and the cache is
 * rewritten.
 */
export async function buildManifest(
  corpusDir: string,
  opts: { cachePath?: string } = {},
): Promise<Manifest> {
  const cachePath = opts.cachePath;
  const cache: Cache =
    cachePath === undefined ? { version: 1, entries: {} } : await readCache(cachePath);
  const listing = await listDir(corpusDir);
  const fresh = listing.filter((l) => {
    const c = cache.entries[l.name];
    return c?.size !== l.size || c.mtimeNs !== l.mtimeNs;
  });
  const [shas, { facts }] = await Promise.all([
    hashFiles(fresh.map((l) => join(corpusDir, l.name))),
    describe(
      corpusDir,
      fresh.map((l) => l.name),
    ),
  ]);
  fresh.forEach((l, i) => {
    cache.entries[l.name] = { ...l, sha256: shas[i]!, ...facts[i]! };
  });
  const records = listing.map((l) => cache.entries[l.name]!);
  if (cachePath !== undefined) {
    const entries = Object.fromEntries(records.map((r) => [r.name, r]));
    await mkdir(dirname(cachePath), { recursive: true });
    await writeFile(cachePath, JSON.stringify({ version: 1, entries } satisfies Cache));
  }
  return { version: 1, records, relations: buildRelations(records) };
}

export async function loadManifest(path: string = PATHS.manifest): Promise<Manifest> {
  return JSON.parse(await readFile(path, 'utf8')) as Manifest;
}

// ---------------------------------------------------------------------------------------------
// Relations.

const stemOf = (name: string) => name.slice(0, name.length - extname(name).length);
const extOf = (name: string) => normExt(extname(name).slice(1));
const sortPair = (a: string, b: string): Pair => (a < b ? [a, b] : [b, a]);
const byFirst = (a: string[], b: string[]) => (a[0]! < b[0]! ? -1 : 1);
const cmpPair = (x: Pair, y: Pair) =>
  x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : x[1] < y[1] ? -1 : x[1] > y[1] ? 1 : 0;

function uniqPairs(pairs: Pair[]): Pair[] {
  const seen = new Set<string>();
  return pairs
    .filter((p) => {
      const k = p.join('\0');
      return !seen.has(k) && (seen.add(k), true);
    })
    .sort(cmpPair);
}

function groupBy<T>(items: T[], key: (t: T) => string | null): Map<string, T[]> {
  const m = new Map<string, T[]>();
  for (const it of items) {
    const k = key(it);
    if (k !== null) m.set(k, [...(m.get(k) ?? []), it]);
  }
  return m;
}

function allPairs(names: string[]): Pair[] {
  const out: Pair[] = [];
  for (let i = 0; i < names.length; i++)
    for (let j = i + 1; j < names.length; j++) out.push(sortPair(names[i]!, names[j]!));
  return out;
}

const captureS = (r: ManifestRecord) =>
  r.capturedAt === null ? null : Math.floor(Date.parse(r.capturedAt) / 1000);

function burstPairs(recs: ManifestRecord[]): Pair[] {
  const pairs: Pair[] = [];
  for (const g of groupBy(recs, (r) => r.burstUuid).values())
    pairs.push(...allPairs(g.map((r) => r.name)));
  // Time-window bursts: same camera, ≤ 2 s apart, both subsecs known, different capture key.
  const timed = recs
    .filter((r) => r.kind === 'still' && r.camera !== null && r.subsec !== null)
    .map((r) => ({ r, s: captureS(r) }))
    .filter((x): x is { r: ManifestRecord; s: number } => x.s !== null)
    .sort((a, b) => a.s - b.s);
  for (let i = 0; i < timed.length; i++) {
    const a = timed[i]!;
    for (let j = i + 1; j < timed.length && timed[j]!.s - a.s <= BURST_WINDOW_S; j++) {
      const b = timed[j]!;
      const sameKey = a.s === b.s && a.r.subsec === b.r.subsec;
      if (a.r.camera === b.r.camera && !sameKey) pairs.push(sortPair(a.r.name, b.r.name));
    }
  }
  return pairs;
}

function livePairs(recs: ManifestRecord[]): Pair[] {
  const pairs: Pair[] = [];
  for (const g of groupBy(recs, (r) => r.contentId).values())
    for (const s of g.filter((r) => r.kind === 'still'))
      for (const m of g.filter((r) => r.kind === 'motion')) pairs.push(sortPair(s.name, m.name));
  for (const g of groupBy(recs, (r) => stemOf(r.name)).values())
    for (const s of g.filter((r) => r.kind === 'still'))
      for (const m of g.filter(
        (r) =>
          extOf(r.name) === 'mov' && r.durationS !== null && r.durationS <= LIVE_MAX_DURATION_S,
      ))
        pairs.push(sortPair(s.name, m.name));
  return pairs;
}

export function buildRelations(recs: ManifestRecord[]): Relations {
  const byName = new Map(recs.map((r) => [r.name, r]));
  const byStem = groupBy(recs, (r) => stemOf(r.name));

  const exactGroups = [...groupBy(recs, (r) => r.sha256).values()]
    .filter((g) => g.length > 1)
    .map((g) => g.map((r) => r.name).sort())
    .sort(byFirst);

  const heicJpgTwin: Pair[] = [];
  for (const g of byStem.values())
    for (const h of g.filter((r) => extOf(r.name) === 'heic'))
      for (const j of g.filter((r) => extOf(r.name) === 'jpg'))
        if (h.capturedAt !== null && h.capturedAt === j.capturedAt)
          heicJpgTwin.push(sortPair(h.name, j.name));

  const editOf: Pair[] = [];
  const spaceSuffixCopy: Pair[] = [];
  for (const r of recs) {
    const e = /^(.*)_E(\d+)$/.exec(stemOf(r.name));
    if (e)
      for (const o of byStem.get(`${e[1]}_${e[2]}`) ?? [])
        if (o.kind === r.kind) editOf.push([r.name, o.name]);
    const s = /^(.*) \d+$/.exec(stemOf(r.name));
    const original = s ? `${s[1]}${extname(r.name)}` : null;
    if (original !== null && byName.has(original)) spaceSuffixCopy.push(sortPair(r.name, original));
  }

  return {
    exactGroups,
    heicJpgTwin: uniqPairs(heicJpgTwin),
    editOf: uniqPairs(editOf),
    spaceSuffixCopy: uniqPairs(spaceSuffixCopy),
    burst: uniqPairs(burstPairs(recs)),
    livePhoto: uniqPairs(livePairs(recs)),
  };
}

/** Connected components of a pair list, each sorted, ordered by first member. */
export function components(pairs: Pair[]): string[][] {
  const parent = new Map<string, string>();
  const find = (x: string): string => {
    let p = parent.get(x) ?? x;
    while (p !== (parent.get(p) ?? p)) p = parent.get(p) ?? p;
    parent.set(x, p);
    return p;
  };
  for (const [a, b] of pairs) {
    const [ra, rb] = [find(a), find(b)];
    if (ra !== rb) parent.set(ra < rb ? rb : ra, ra < rb ? ra : rb);
  }
  return [...groupBy([...new Set(pairs.flat())], find).values()].map((g) => g.sort()).sort(byFirst);
}

// ---------------------------------------------------------------------------------------------
// Subsets.

/** Deterministic PRNG (mulberry32). */
export function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function shuffled<T>(items: readonly T[], rnd: () => number): T[] {
  const a = [...items];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [a[i], a[j]] = [a[j]!, a[i]!];
  }
  return a;
}

/**
 * Deterministic subsets for `seed`. Both start with every PINNED file (throws naming any that
 * is missing), then Live Photo pairs and burst sets, then random files up to the tier size.
 * `standard` is a superset of `smoke`.
 */
export function selectSubsets(m: Manifest, seed: number): { smoke: string[]; standard: string[] } {
  const names = new Set(m.records.map((r) => r.name));
  const missing = PINNED.filter((p) => !names.has(p));
  if (missing.length > 0)
    throw new Error(`pinned files missing from manifest: ${missing.join(', ')}`);
  const rnd = prng(seed);
  const lives = shuffled(m.relations.livePhoto, rnd);
  // BurstUUID-backed bursts first (stable sort keeps the shuffle within each kind), so even the
  // smoke tier exercises the hard "burst frames never collapse" case on real burst metadata.
  const uuidOf = new Map(m.records.map((r) => [r.name, r.burstUuid]));
  const hasUuid = (g: string[]) => g.some((n) => (uuidOf.get(n) ?? null) !== null);
  const bursts = shuffled(components(m.relations.burst), rnd)
    .sort((a, b) => Number(hasUuid(b)) - Number(hasUuid(a)))
    .map((g) => g.slice(0, MAX_BURST_FRAMES));
  const rest = shuffled([...names].sort(), rnd);

  const pick = (base: string[], tier: (typeof SUBSETS)[keyof typeof SUBSETS]): string[] => {
    const out = new Set(base);
    const live = lives.filter(([a, b]) => out.has(a) && out.has(b)).length;
    for (const p of lives
      .filter(([a, b]) => !(out.has(a) && out.has(b)))
      .slice(0, tier.livePairs - live))
      p.forEach((n) => out.add(n));
    const have = bursts.filter((g) => g.every((n) => out.has(n))).length;
    for (const g of bursts.filter((g) => !g.every((n) => out.has(n))).slice(0, tier.bursts - have))
      g.forEach((n) => out.add(n));
    for (const n of rest) {
      if (out.size >= tier.size) break;
      out.add(n);
    }
    return [...out];
  };
  const smoke = pick([...PINNED], SUBSETS.smoke);
  return { smoke, standard: pick(smoke, SUBSETS.standard) };
}

// ---------------------------------------------------------------------------------------------
// Seeding.

/** Headroom kept free on the destination volume. */
const MIN_FREE_BYTES = 1n << 30n;

async function freeBytes(dir: string): Promise<bigint> {
  const s = await statfs(dir, { bigint: true });
  return s.bavail * s.bsize;
}

/** `statfs().type` of APFS on macOS. */
const APFS_FSTYPE = 26;

/**
 * How corpus bytes reach a destination. `clone` must be a real copy-on-write clone; `copy` is
 * the byte copy used only where `canClone` says cloning is unsupported. Injectable for tests.
 */
export interface Copier {
  canClone(srcDir: string, destDir: string): Promise<boolean>;
  clone(src: string, dest: string): Promise<void>;
  copy(src: string, dest: string): Promise<void>;
}

/**
 * macOS `cp -c` silently falls back to a byte copy when `clonefile(2)` is unsupported (another
 * volume, HFS+), and Node's `COPYFILE_FICLONE_FORCE` is ENOSYS on macOS. So support is decided
 * up front: cloning works only within one APFS volume. Then `cp -c -p -n` clones, and any
 * error it reports is fatal.
 */
export const defaultCopier: Copier = {
  async canClone(srcDir, destDir) {
    const [a, b, fs] = await Promise.all([stat(srcDir), stat(destDir), statfs(destDir)]);
    return a.dev === b.dev && fs.type === APFS_FSTYPE;
  },
  async clone(src, dest) {
    await execFile('cp', ['-c', '-p', '-n', src, dest]);
  },
  /** Byte copy stamped with the source times (mtime to sub-microsecond precision). */
  async copy(src, dest) {
    const s = await stat(src, { bigint: true });
    if ((await freeBytes(dirname(dest))) < s.size + MIN_FREE_BYTES)
      throw new Error(`not enough free space to copy ${basename(src)}`);
    await copyFile(src, dest, constants.COPYFILE_EXCL);
    const secs = (ns: bigint) => Number(ns) / 1e9;
    await utimes(dest, secs(s.atimeNs), secs(s.mtimeNs));
  },
};

/** `p` resolved through symlinks, even when its tail doesn't exist yet. */
async function realpathLoose(p: string): Promise<string> {
  const abs = resolve(p);
  try {
    return await realpath(abs);
  } catch {
    const parent = dirname(abs);
    return parent === abs ? abs : join(await realpathLoose(parent), basename(abs));
  }
}

/** Throws unless `dest` and `dest/..` (where manifests are written) lie outside the corpus. */
export async function assertOutsideCorpus(corpusDir: string, dest: string): Promise<void> {
  const corpus = await realpath(corpusDir);
  for (const p of [dest, dirname(resolve(dest))]) {
    const rel = relative(corpus, await realpathLoose(p));
    if (rel === '' || (!rel.startsWith('..') && !isAbsolute(rel)))
      throw new Error(`refusing to write inside the corpus: ${p}`);
  }
}

/** Stable small bucket for the nested layout: non-ASCII directory names with spaces, on purpose. */
function bucketOf(name: string): string {
  let h = 0x811c9dc5;
  for (const c of Buffer.from(name)) h = Math.imul(h ^ c, 0x01000193) >>> 0;
  return `d${h % 8} Été/e${(h >>> 3) % 4} 写真`;
}

async function seedFileOf(
  dest: string,
  relPath: string,
  extra: Omit<SeedFile, 'relPath' | 'name' | 'sha256' | 'size' | 'mtimeNs'>,
  sha256: string,
): Promise<SeedFile> {
  const s = await stat(join(dest, relPath), { bigint: true });
  return {
    relPath,
    name: basename(relPath),
    sha256,
    size: String(s.size),
    mtimeNs: String(s.mtimeNs),
    ...extra,
  };
}

async function hashed(
  dest: string,
  items: Array<
    { relPath: string } & Omit<SeedFile, 'relPath' | 'name' | 'sha256' | 'size' | 'mtimeNs'>
  >,
): Promise<SeedFile[]> {
  const shas = await hashFiles(items.map((i) => join(dest, i.relPath)));
  return Promise.all(
    items.map(({ relPath, ...extra }, i) => seedFileOf(dest, relPath, extra, shas[i]!)),
  );
}

/**
 * Clones `names` from the corpus into `dest` (flat or nested), in an order shuffled by
 * `shuffle`, then adds junk / corrupt extras and synthetic variants. Never overwrites, never
 * writes inside the corpus. Byte copies happen only where cloning is unsupported; they are
 * warned about and counted in `fallbackCopies`.
 */
export async function seed(
  opts: SeedOptions,
  copier: Copier = defaultCopier,
): Promise<SeedManifest> {
  const { corpusDir, dest } = opts;
  const prefix = opts.prefix ?? '';
  await assertOutsideCorpus(corpusDir, dest);
  await mkdir(dest, { recursive: true });
  if ((await freeBytes(dest)) < MIN_FREE_BYTES) throw new Error(`less than 1 GiB free at ${dest}`);
  for (const n of opts.names) await stat(join(corpusDir, n)); // fail early, naming the file

  const order =
    opts.shuffle === undefined ? [...opts.names] : shuffled(opts.names, prng(opts.shuffle));
  const rel = (n: string) => (opts.layout === 'nested' ? `${bucketOf(n)}/${n}` : n);
  const clones = await copier.canClone(corpusDir, dest);
  if (!clones) {
    const allow = opts.allowByteCopy ?? process.env['ALLOW_BYTE_COPY'] === '1';
    if (order.length > SUBSETS.smoke.size && !allow)
      throw new Error(
        `${dest} can't clone from ${corpusDir} (not one APFS volume): refusing ${order.length} byte copies; set ALLOW_BYTE_COPY=1 to copy anyway`,
      );
    console.warn(
      `WARNING: ${dest} can't clone from ${corpusDir} (not one APFS volume): making ${order.length} byte copies`,
    );
  }
  for (const n of order) {
    const target = join(dest, rel(n));
    await mkdir(dirname(target), { recursive: true });
    if (await exists(target)) throw new Error(`refusing to overwrite ${target}`);
    await (clones
      ? copier.clone(join(corpusDir, n), target)
      : copier.copy(join(corpusDir, n), target));
  }
  const fallbackCopies = clones ? 0 : order.length;
  const items: Parameters<typeof hashed>[1] = order.map((n) => ({
    relPath: rel(n),
    origin: 'corpus',
  }));
  if (opts.extras?.includes('junk')) items.push(...(await writeJunk(dest, prefix)));
  if (opts.extras?.includes('corrupt')) items.push(...(await writeCorrupt(dest, prefix)));
  const files = await hashed(dest, items);
  if (opts.synthetic) files.push(...(await synthesize(corpusDir, opts.names, dest, prefix)));
  return { dest, files, fallbackCopies };
}

async function exists(p: string): Promise<boolean> {
  try {
    await stat(p);
    return true;
  } catch {
    return false;
  }
}

type Item = Parameters<typeof hashed>[1][number];

/** Files the pipeline must ignore (`.DS_Store`, partial downloads) or file under `other/`. */
async function writeJunk(dest: string, prefix: string): Promise<Item[]> {
  const files: Record<string, string> = {
    '.DS_Store': 'Bud1 junk',
    [`${prefix}junk-notes.txt`]: 'not a photo\n',
    [`${prefix}junk-download.jpg.part`]: 'partial download',
  };
  for (const [n, body] of Object.entries(files))
    await writeFile(join(dest, n), body, { flag: 'wx' });
  return Object.keys(files).map((relPath) => ({ relPath, origin: 'junk' }));
}

/**
 * A truncated JPEG and a JPEG whose header is followed by random bytes, with the quarantine
 * reason each must get. (Bare random bytes have no detectable type: they are filed under
 * `other/`, not decoded.)
 */
async function writeCorrupt(dest: string, prefix: string): Promise<Item[]> {
  const full = await sharp({
    create: { width: 256, height: 192, channels: 3, background: { r: 30, g: 120, b: 200 } },
  })
    .jpeg({ quality: 95 })
    .toBuffer();
  const random = Buffer.alloc(4096);
  const rnd = prng(4096);
  for (let i = 0; i < random.length; i++) random[i] = Math.floor(rnd() * 256);
  const truncated = `${prefix}corrupt-truncated.jpg`;
  const garbage = `${prefix}corrupt-garbage.jpg`;
  await writeFile(join(dest, truncated), full.subarray(0, full.length >> 1), { flag: 'wx' });
  await writeFile(join(dest, garbage), Buffer.concat([full.subarray(0, 200), random]), {
    flag: 'wx',
  });
  return [
    { relPath: truncated, origin: 'corrupt', reason: 'truncated' },
    { relPath: garbage, origin: 'corrupt', reason: 'decode_error' },
  ];
}

// ---------------------------------------------------------------------------------------------
// Synthetic variants.

/** At most this many still seeds get the three variants. */
const SYNTH_SEEDS = 8;
const SYNTH_STILL_EXT = new Set(['jpg', 'heic', 'png', 'tif']);

/**
 * A sharp-decodable path for `src`: itself, or a sips JPEG conversion in `tmp` (EXIF kept),
 * made once per seed.
 */
async function decodable(src: string, tmp: string): Promise<string> {
  if (extOf(src) !== 'heic') return src;
  const out = join(tmp, `${basename(src)}.jpg`);
  if (!(await exists(out)))
    await execFile('sips', [
      '-s',
      'format',
      'jpeg',
      '-s',
      'formatOptions',
      '95',
      src,
      '--out',
      out,
    ]);
  return out;
}

/** The facts §6.3 compares for a pair; `width`/`height` are as displayed (EXIF-oriented). */
export interface TruthFacts {
  capturedAt: string | null;
  subsec: string | null;
  camera: string | null;
  burstUuid: string | null;
  width: number | null;
  height: number | null;
  mime: string;
}

/**
 * The §6.3 classification of a look-alike pair (a synthetic variant and its seed). Rows top to
 * bottom, first match wins. Rows 6–8 are copies only within the copy hash distances, which this
 * oracle doesn't compute: `copy-if-tight`.
 */
export function expectedRelation(
  a: TruthFacts,
  b: TruthFacts,
): 'copy' | 'copy-if-tight' | 'similar' {
  const both = <T>(x: T | null, y: T | null) => x !== null && y !== null;
  const sec = (t: string) => Math.floor(Date.parse(t) / 1000);
  const otherEncoding = a.width !== b.width || a.height !== b.height || a.mime !== b.mime;
  if (both(a.burstUuid, b.burstUuid) && a.burstUuid === b.burstUuid) return 'similar';
  const captured = a.capturedAt !== null && b.capturedAt !== null;
  if (captured && sec(a.capturedAt!) !== sec(b.capturedAt!)) return 'similar';
  if (both(a.subsec, b.subsec) && a.subsec !== b.subsec) return 'similar';
  if (both(a.camera, b.camera) && a.camera !== b.camera) return 'similar';
  if (captured && both(a.subsec, b.subsec)) return 'copy';
  if (captured) return otherEncoding ? 'copy-if-tight' : 'similar';
  if (a.capturedAt !== null || b.capturedAt !== null) return 'copy-if-tight';
  return otherEncoding ? 'copy-if-tight' : 'similar';
}

function truthFacts(f: Facts & Dims, e: Encoding): TruthFacts {
  const swap = e.orientation !== null && e.orientation >= 5 && e.orientation <= 8;
  return {
    capturedAt: f.capturedAt,
    subsec: f.subsec,
    camera: f.camera,
    burstUuid: f.burstUuid,
    width: swap ? f.height : f.width,
    height: swap ? f.width : f.height,
    mime: e.mime,
  };
}

/**
 * Variants of real seeds: 50% resize keeping EXIF, q60 with EXIF stripped, PNG keeping EXIF,
 * and an EXIF-stripped export of one burst frame (the bridge case). Each variant's `expect` is
 * derived from §6.3 on the seed's and the variant's own exiftool/sips facts; an EXIF-less copy
 * of a burst frame whose partner is also seeded is `copy-or-ambiguous` with `bridges`.
 */
export async function synthesize(
  corpusDir: string,
  names: string[],
  dest: string,
  prefix = '',
): Promise<SeedFile[]> {
  await assertOutsideCorpus(corpusDir, dest);
  await mkdir(dest, { recursive: true });
  const stills = names.filter((n) => SYNTH_STILL_EXT.has(extOf(n))).sort();
  if (stills.length === 0) return [];
  const { facts, enc } = await describe(corpusDir, stills);
  const recs = stills.map((name, i) => ({
    name,
    size: '0',
    mtimeNs: '0',
    sha256: name,
    ...facts[i]!,
  }));
  const truth = new Map(stills.map((n, i) => [n, truthFacts(facts[i]!, enc[i]!)]));
  const burst = buildRelations(recs.filter((r) => r.kind === 'still')).burst;
  const partnerOf = (n: string) => burst.find((p) => p.includes(n))?.find((x) => x !== n);
  const seeds = recs.filter((r) => r.kind === 'still').slice(0, SYNTH_SEEDS);
  const tmp = await mkdtemp(join(tmpdir(), 'pm-synth-'));
  const made: Array<{
    relPath: string;
    seedName: string;
    variant: NonNullable<SeedFile['variant']>;
  }> = [];
  try {
    const emit = async (
      seedName: string,
      variant: NonNullable<SeedFile['variant']>,
      ext: string,
      render: (img: Sharp, width: number) => Sharp,
    ) => {
      const relPath = `${prefix}synth-${variant}-${seedName.replaceAll('.', '_')}.${ext}`;
      const input = await decodable(join(corpusDir, seedName), tmp);
      const { autoOrient } = await sharp(input).metadata();
      const out = await render(sharp(input).rotate(), autoOrient.width).toBuffer();
      await writeFile(join(dest, relPath), out, { flag: 'wx' });
      // The seed's times, not "now": an EXIF-less variant that becomes a best copy is staged
      // under its mtime, and two runs must get identical inputs (calm vs chaos, spec §10.4).
      const st = await stat(join(corpusDir, seedName), { bigint: true });
      const secs = (ns: bigint) => Number(ns) / 1e9;
      await utimes(join(dest, relPath), secs(st.atimeNs), secs(st.mtimeNs));
      made.push({ relPath, seedName, variant });
    };
    for (const { name } of seeds) {
      await emit(name, 'resize50', 'jpg', (img, w) =>
        img
          .resize({ width: Math.round(w / 2) })
          .keepExif()
          .jpeg({ quality: 90 }),
      );
      await emit(name, 'q60-noexif', 'jpg', (img) => img.jpeg({ quality: 60 }));
      await emit(name, 'png', 'png', (img) => img.keepExif().png());
    }
    const frame = burst[0]?.[0];
    if (frame !== undefined) await emit(frame, 'bridge', 'jpg', (img) => img.jpeg({ quality: 85 }));
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
  const out = await describe(
    dest,
    made.map((m) => m.relPath),
  );
  const items: Item[] = made.map((m, i) => {
    const v = truthFacts(out.facts[i]!, out.enc[i]!);
    const rel = expectedRelation(truth.get(m.seedName)!, v);
    const partner = partnerOf(m.seedName);
    return rel !== 'similar' && v.capturedAt === null && partner !== undefined
      ? { ...m, origin: 'synthetic', expect: 'copy-or-ambiguous', bridges: [m.seedName, partner] }
      : { ...m, origin: 'synthetic', expect: rel };
  });
  return hashed(dest, items);
}

// ---------------------------------------------------------------------------------------------
// Verify.

/**
 * Drift between the corpus and its manifest, one line per file: missing, changed size/mtime,
 * unexpected, and (with `deep`) a SHA-256 mismatch.
 */
export async function verify(corpusDir: string, m: Manifest, deep: boolean): Promise<string[]> {
  const listing = new Map((await listDir(corpusDir)).map((l) => [l.name, l]));
  const drift: string[] = [];
  const unchanged: ManifestRecord[] = [];
  for (const r of m.records) {
    const l = listing.get(r.name);
    if (!l) drift.push(`missing: ${r.name}`);
    else if (l.size !== r.size || l.mtimeNs !== r.mtimeNs)
      drift.push(
        `changed: ${r.name} (size ${r.size} → ${l.size}, mtimeNs ${r.mtimeNs} → ${l.mtimeNs})`,
      );
    else unchanged.push(r);
  }
  const known = new Set(m.records.map((r) => r.name));
  for (const n of listing.keys()) if (!known.has(n)) drift.push(`unexpected: ${n}`);
  if (deep) {
    const shas = await hashFiles(unchanged.map((r) => join(corpusDir, r.name)));
    unchanged.forEach((r, i) => {
      if (shas[i] !== r.sha256) drift.push(`sha256 mismatch: ${r.name}`);
    });
  }
  return drift;
}
