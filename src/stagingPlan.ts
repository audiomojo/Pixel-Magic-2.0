import { vaultRel } from './fsops.js';

/** One `contents` row as staging sees it. `copyGroup` and `similarSet` are root group ids. */
export interface StageContent {
  sha256: string;
  kind: string;
  ext: string;
  mime: string | null;
  width: number | null;
  height: number | null;
  sizeBytes: bigint;
  capturedAt: Date | null;
  liveContentId: string | null;
  durationS: number | null;
  state: string;
  quarantineReason: string | null;
  copyGroup: number | null;
  similarSet: number | null;
  ambiguous: boolean;
}

/** One `files` row holding bytes (`INGESTED` or `COPY`). `srcRel` is its bytes' path under DATA_DIR. */
export interface StageFile {
  fileId: bigint;
  relPath: string;
  originalName: string;
  mtimeNs: bigint;
  sha256: string;
  state: string;
  srcRel: string;
}

/** One hardlink: `dest` relative to the generation directory, `src` relative to DATA_DIR. */
export interface Entry {
  dest: string;
  src: string;
}

export type BestCopyKey = readonly [
  notAmbiguous: number,
  pixels: number,
  hasCaptured: number,
  hasLive: number,
  formatRank: number,
  sizeBytes: bigint,
  negEpochMs: number,
  sha256: string,
];

const RAW = ['dng', 'cr2', 'cr3', 'nef', 'arw', 'orf', 'rw2', 'raf', 'pef', 'srw'];
const FORMAT_RANK = new Map<string, number>([
  ...RAW.map((e) => [e, 8] as const),
  ['tif', 7],
  ['tiff', 7],
  ['png', 6],
  ['heic', 5],
  ['heif', 5],
  ['jpg', 4],
  ['jpeg', 4],
  ['webp', 3],
  ['gif', 2],
  ['bmp', 1],
]);

/** Spec §6.4 best-copy key, highest first (see `compareBestCopy`). */
export function bestCopyKey(c: StageContent, hasLive: boolean): BestCopyKey {
  return [
    c.ambiguous ? 0 : 1,
    (c.width ?? 0) * (c.height ?? 0),
    c.capturedAt ? 1 : 0,
    hasLive ? 1 : 0,
    FORMAT_RANK.get(c.ext.toLowerCase()) ?? 0,
    c.sizeBytes,
    c.capturedAt ? 0 - c.capturedAt.getTime() : 0,
    c.sha256,
  ];
}

/** Code-unit order: locale-free, identical on every machine. */
const cmpStr = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);
const cmpDesc = (a: number | bigint, b: number | bigint): number => (a > b ? -1 : a < b ? 1 : 0);

/** Negative when `a` is the better copy: every element descending, except the final sha ascending. */
export function compareBestCopy(a: BestCopyKey, b: BestCopyKey): number {
  for (let i = 0; i < 7; i++) {
    const d = cmpDesc(a[i] as number | bigint, b[i] as number | bigint);
    if (d !== 0) return d;
  }
  return cmpStr(a[7], b[7]);
}

/** Naming-file order `(relPath, mtimeNs, fileId)`: `fileId` (arrival order) only breaks true ties. */
const cmpFile = (a: StageFile, b: StageFile): number =>
  cmpStr(a.relPath, b.relPath) || cmpDesc(b.mtimeNs, a.mtimeNs) || cmpDesc(b.fileId, a.fileId);

const lastSlash = (p: string): number => p.lastIndexOf('/');
const dirOf = (relPath: string): string => relPath.slice(0, Math.max(0, lastSlash(relPath)));
const dotAt = (name: string): number => {
  const i = name.lastIndexOf('.');
  return i > 0 ? i : -1;
};
const stemOf = (name: string): string => (dotAt(name) < 0 ? name : name.slice(0, dotAt(name)));
const extOf = (name: string): string => (dotAt(name) < 0 ? '' : name.slice(dotAt(name)));

/** Groups byte-holding files by content, each list sorted by `(relPath, fileId)`. */
function filesByContent(files: readonly StageFile[]): Map<string, StageFile[]> {
  const out = new Map<string, StageFile[]>();
  for (const f of files) {
    if (f.state !== 'INGESTED' && f.state !== 'COPY') continue;
    const list = out.get(f.sha256);
    if (list) list.push(f);
    else out.set(f.sha256, [f]);
  }
  for (const list of out.values()) list.sort(cmpFile);
  return out;
}

const bySha = (a: StageContent, b: StageContent): number => cmpStr(a.sha256, b.sha256);

/**
 * Live Photo pairs, still sha → motion sha. A still takes the lowest-sha motion with its
 * `liveContentId`; failing that, the lowest-sha motion (not claimed by any content id) whose file
 * shares a directory and the full stem (case-insensitive) with one of the still's files and runs
 * at most `maxDurationS`.
 */
export function pairLivePhotos(
  contents: readonly StageContent[],
  files: readonly StageFile[],
  maxDurationS: number,
): Map<string, string> {
  const motions = contents.filter((c) => c.kind === 'motion').sort(bySha);
  const stills = contents.filter((c) => c.kind === 'still').sort(bySha);
  const byLiveId = new Map<string, StageContent>();
  for (const m of motions)
    if (m.liveContentId !== null && !byLiveId.has(m.liveContentId))
      byLiveId.set(m.liveContentId, m);

  const pairs = new Map<string, string>();
  const claimed = new Set<string>();
  for (const s of stills) {
    const m = s.liveContentId === null ? undefined : byLiveId.get(s.liveContentId);
    if (m) {
      pairs.set(s.sha256, m.sha256);
      claimed.add(m.sha256);
    }
  }

  const fileMap = filesByContent(files);
  const stemKeys = (sha: string): string[] =>
    (fileMap.get(sha) ?? []).map(
      (f) => `${dirOf(f.relPath)}\0${stemOf(f.originalName).toLowerCase()}`,
    );
  const byStem = new Map<string, string>(); // first (lowest-sha) eligible motion per key
  for (const m of motions) {
    if (claimed.has(m.sha256) || m.durationS === null || m.durationS > maxDurationS) continue;
    for (const k of stemKeys(m.sha256)) if (!byStem.has(k)) byStem.set(k, m.sha256);
  }
  for (const s of stills) {
    if (pairs.has(s.sha256)) continue;
    const found = stemKeys(s.sha256)
      .map((k) => byStem.get(k))
      .filter((m): m is string => m !== undefined)
      .sort(cmpStr)[0];
    if (found !== undefined) pairs.set(s.sha256, found);
  }
  return pairs;
}

const pad = (n: number, w = 2): string => String(n).padStart(w, '0');

/** UTC `YYYY/YYYY-MM` and `YYYYMMDD_HHMMSS`. */
function dateParts(d: Date): { dir: string; stamp: string } {
  const [y, mo] = [pad(d.getUTCFullYear(), 4), pad(d.getUTCMonth() + 1)];
  return {
    dir: `${y}/${y}-${mo}`,
    stamp: `${y}${mo}${pad(d.getUTCDate())}_${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`,
  };
}

/** A quarantine reason as one safe path segment. */
function reasonDir(reason: string | null): string {
  const r = (reason ?? '').replace(/[^A-Za-z0-9._-]/g, '_');
  return /^\.*$/.test(r) ? 'unknown' : r;
}

const sha8 = (sha: string): string => sha.slice(0, 8);

interface Request {
  folder: string;
  name: string;
  sha: string;
  relPath: string;
  mtimeNs: bigint;
  fileId: bigint;
  src: string;
}

/** Collision order `(sha256, relPath)`, then the naming-file tie-breaks, so a content's naming file is its first claimant. */
const cmpRequest = (a: Request, b: Request): number =>
  cmpStr(a.sha, b.sha) ||
  cmpStr(a.relPath, b.relPath) ||
  cmpDesc(b.mtimeNs, a.mtimeNs) ||
  cmpDesc(b.fileId, a.fileId);

/** Collision-free names: dests compare NFC + case-insensitively (APFS), whatever the host. */
class Namer {
  private readonly taken = new Set<string>();
  readonly entries: Entry[] = [];

  /** Places `r` (callers go in `(sha256, relPath)` order); returns its final dest. */
  place(r: Request): string {
    const i = dotAt(r.name);
    const [stem, ext] = i < 0 ? [r.name, ''] : [r.name.slice(0, i), r.name.slice(i)];
    for (let n = 0; ; n++) {
      const name =
        n === 0 ? r.name : `${stem}_${sha8(r.sha)}${n === 1 ? '' : `_${String(n)}`}${ext}`;
      const dest = `${r.folder}/${name}`;
      const key = dest.normalize('NFC').toLowerCase();
      if (this.taken.has(key)) continue;
      this.taken.add(key);
      this.entries.push({ dest, src: r.src });
      return dest;
    }
  }
}

/** Appends `v` to `map[k]` in place (linear, unlike rebuilding the list on every push). */
function push<K, V>(map: Map<K, V[]>, k: K, v: V): void {
  let list = map.get(k);
  if (list === undefined) map.set(k, (list = []));
  list.push(v);
}

/**
 * The full staged tree (spec §6.4, §12 R5/R8) from database facts, sorted by `dest`. Only facts
 * decide it — never group ids, file ids beyond the last tie-break, or input order.
 */
export function planStaging(
  contents: readonly StageContent[],
  files: readonly StageFile[],
  maxDurationS: number,
): Entry[] {
  const fileMap = filesByContent(files);
  const staged = [...contents].filter((c) => fileMap.has(c.sha256)).sort(bySha);
  const pairs = pairLivePhotos(staged, files, maxDurationS);
  const key = (c: StageContent) => bestCopyKey(c, pairs.has(c.sha256));
  const better = (a: StageContent, b: StageContent) => compareBestCopy(key(a), key(b));
  const filesOf = (c: StageContent): StageFile[] => fileMap.get(c.sha256)!;
  const namingFile = (c: StageContent): StageFile => filesOf(c)[0]!;
  const dateOf = (c: StageContent) =>
    dateParts(c.capturedAt ?? new Date(Number(namingFile(c).mtimeNs / 1_000_000n)));

  const requests: Request[] = [];
  /** Each content's own placement (the request carrying its naming file), for Live MOVs. */
  const placementOf = new Map<string, Request>();
  const fileRequest = (folder: string, name: string, f: StageFile): Request => ({
    folder,
    name,
    sha: f.sha256,
    relPath: f.relPath,
    mtimeNs: f.mtimeNs,
    fileId: f.fileId,
    src: f.srcRel,
  });
  /** The content's placement from the vault, and every other file of it into `copiesFolder`. */
  const place = (c: StageContent, folder: string, name: string, copiesFolder: string) => {
    const [first, ...rest] = filesOf(c);
    const r = { ...fileRequest(folder, name, first!), src: vaultRel(c.sha256, c.ext) };
    requests.push(r);
    placementOf.set(c.sha256, r);
    for (const f of rest) requests.push(fileRequest(copiesFolder, f.originalName, f));
  };

  // Copy groups and similar sets of CLUSTERED stills.
  const groups = new Map<number, StageContent[]>();
  const sets = new Map<number, StageContent[]>();
  for (const c of staged) {
    if (c.kind !== 'still' || c.state !== 'CLUSTERED' || c.copyGroup === null) continue;
    push(groups, c.copyGroup, c);
    if (c.similarSet !== null) push(sets, c.similarSet, c);
  }
  // A set is two or more look-alike copy groups (spec §6.3). One that holds a single copy group
  // (its groups merged after they were linked) stages as unique: had the copies merged first,
  // there'd be no set, and the tree must not depend on arrival order.
  const setBest = new Map(
    [...sets]
      .filter(([, cs]) => new Set(cs.map((c) => c.copyGroup)).size > 1)
      .map(([id, cs]) => [id, [...cs].sort(better)[0]!]),
  );
  for (const members of groups.values()) {
    const [best, ...others] = [...members].sort(better);
    const copies = `copies/g-${sha8(best!.sha256)}`;
    const sb = best!.similarSet === null ? undefined : setBest.get(best!.similarSet);
    const folder = sb
      ? `similar/${dateOf(sb).dir}/s-${sha8(sb.sha256)}`
      : `unique/${dateOf(best!).dir}`;
    place(best!, folder, `${dateOf(best!).stamp}_${namingFile(best!).originalName}`, copies);
    for (const o of others) {
      const [first, ...rest] = filesOf(o);
      const r = fileRequest(copies, first!.originalName, first!);
      requests.push(r);
      placementOf.set(o.sha256, r);
      for (const f of rest) requests.push(fileRequest(copies, f.originalName, f));
    }
  }

  // Quarantine, FAILED, and INGESTED motion / other.
  const stillsOf = new Map<string, string[]>();
  for (const [still, motion] of pairs) push(stillsOf, motion, still);
  const vcopies = (c: StageContent) => `copies/v-${sha8(c.sha256)}`;
  for (const c of staged) {
    const name = namingFile(c).originalName;
    if (c.state === 'QUARANTINED')
      place(c, `quarantine/${reasonDir(c.quarantineReason)}`, name, vcopies(c));
    else if (c.state === 'FAILED') place(c, 'quarantine/failed', name, vcopies(c));
  }
  // Every still placement is known now, so a motion's home never depends on sha order.
  const besideMotions: StageContent[] = [];
  for (const c of staged) {
    if (c.kind === 'still' || c.state !== 'INGESTED') continue;
    if (c.kind === 'motion' && (stillsOf.get(c.sha256) ?? []).some((s) => placementOf.has(s)))
      besideMotions.push(c);
    else {
      const dir = dirOf(namingFile(c).relPath);
      place(c, dir === '' ? 'other' : `other/${dir}`, namingFile(c).originalName, vcopies(c));
    }
  }

  const namer = new Namer();
  const finalDest = new Map<Request, string>();
  for (const r of requests.sort(cmpRequest)) finalDest.set(r, namer.place(r));

  // Paired MOVs: beside every placement of their stills, under the still's final base name.
  const beside: Request[] = [];
  for (const m of besideMotions) {
    const [first, ...rest] = filesOf(m);
    const ext = extOf(first!.originalName) || `.${m.ext}`;
    for (const still of stillsOf.get(m.sha256)!) {
      const p = placementOf.get(still);
      if (!p) continue;
      const dest = finalDest.get(p)!;
      const r = fileRequest(
        dest.slice(0, lastSlash(dest)),
        stemOf(dest.slice(lastSlash(dest) + 1)) + ext,
        first!,
      );
      beside.push({ ...r, relPath: dest, src: vaultRel(m.sha256, m.ext) });
    }
    for (const f of rest) beside.push(fileRequest(`copies/v-${sha8(m.sha256)}`, f.originalName, f));
  }
  for (const r of beside.sort(cmpRequest)) namer.place(r);

  return namer.entries.sort((a, b) => cmpStr(a.dest, b.dest));
}
