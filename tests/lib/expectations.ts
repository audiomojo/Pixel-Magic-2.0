// Ground-truth expectations for an e2e run, and the lookups the e2e tests assert with.
import { DEFAULT_THRESHOLDS } from '../../src/classify.js';
import { isIgnored } from '../../src/fsops.js';
import { hamming, toUnsigned } from '../../src/hashing.js';
import type { Manifest, SeedFile, SeedManifest } from '../../tools/corpus/lib.js';
import { loadWorld, poolFor, type ContentRow, type World } from './db.js';
import { stagedEntries, treeSnapshot } from './snapshot.js';

const dataDirs = new Map<string, string>();

/** Lets `queries(dbUrl)` find the run's data dir (the Stack registers it once it's up). */
export function registerDataDir(dbUrl: string, dataDir: string): void {
  dataDirs.set(dbUrl, dataDir);
}

const stemOf = (name: string): string => {
  const i = name.lastIndexOf('.');
  return i > 0 ? name.slice(0, i) : name;
};
const dirOf = (p: string): string => p.slice(0, Math.max(0, p.lastIndexOf('/')));
const baseOf = (p: string): string => p.slice(p.lastIndexOf('/') + 1);

/** The one content every file with this original name holds (throws if none or several). */
function shaOfName(w: World, name: string): string {
  const shas = new Set(
    w.files.filter((f) => f.originalName === name && f.sha256 !== null).map((f) => f.sha256!),
  );
  if (shas.size !== 1)
    throw new Error(`${name}: expected files of one content, found ${String(shas.size)}`);
  return [...shas][0]!;
}

function contentOf(w: World, sha: string): ContentRow {
  const c = w.contents.get(sha);
  if (!c) throw new Error(`no contents row for ${sha}`);
  return c;
}

function sameGroup(w: World, a: string, b: string): boolean {
  if (a === b) return true;
  const ga = contentOf(w, a).copyGroup;
  return ga !== null && ga === contentOf(w, b).copyGroup;
}

/** The last files row of a drop-zone-relative path. */
function rowOf(w: World, relPath: string) {
  const rows = w.files.filter((f) => f.relPath === relPath);
  const row = rows[rows.length - 1];
  if (!row) throw new Error(`no files row for ${relPath}`);
  return row;
}

/** Lookups by original file name or drop-zone path, each reading the database fresh. */
export function queries(dbUrl: string, dataDir: string | undefined = dataDirs.get(dbUrl)) {
  const world = () => loadWorld(dbUrl);
  return {
    kindOf: async (name: string) => {
      const w = await world();
      return contentOf(w, shaOfName(w, name)).kind;
    },
    mimeOf: async (name: string) => {
      const w = await world();
      return contentOf(w, shaOfName(w, name)).mime;
    },
    sameCopyGroup: async (a: string, b: string) => {
      const w = await world();
      return sameGroup(w, shaOfName(w, a), shaOfName(w, b));
    },
    /** Every files row's state, for all files with these original names. */
    states: async (names: string[]) => {
      const w = await world();
      return w.files.filter((f) => names.includes(f.originalName)).map((f) => f.state);
    },
    stateOf: async (relPath: string) => rowOf(await world(), relPath).state,
    quarantineReasonOf: async (relPath: string) => {
      const w = await world();
      const row = rowOf(w, relPath);
      return row.sha256 === null ? null : contentOf(w, row.sha256).quarantineReason;
    },
    fileRowCount: async (relPaths: string[]) => {
      const r = await poolFor(dbUrl).query<{ n: bigint }>(
        'SELECT count(*) AS n FROM files WHERE rel_path = ANY($1::text[])',
        [relPaths],
      );
      return Number(r.rows[0]!.n);
    },
    /** Every staged path of the content this original name holds, sorted. */
    stagedOf: async (name: string) => {
      if (dataDir === undefined) throw new Error(`no data dir registered for ${dbUrl}`);
      const sha = shaOfName(await world(), name);
      return (await stagedEntries(dataDir)).filter((e) => e.sha256 === sha).map((e) => e.dest);
    },
    /** Some staged path of the MOV has the dirname and stem of a staged path of the still's content. */
    beside: async (movName: string, stillName: string) => {
      if (dataDir === undefined) throw new Error(`no data dir registered for ${dbUrl}`);
      const w = await world();
      const [mov, still] = [shaOfName(w, movName), shaOfName(w, stillName)];
      const staged = await stagedEntries(dataDir);
      const key = (dest: string) => `${dirOf(dest)}\0${stemOf(baseOf(dest))}`;
      const stillKeys = new Set(staged.filter((e) => e.sha256 === still).map((e) => key(e.dest)));
      return staged.some((e) => e.sha256 === mov && stillKeys.has(key(e.dest)));
    },
  };
}

/**
 * The manifest's Live Photo pairs with both files seeded, keeping each MOV's real partner only.
 * The manifest also pairs by stem, so one MOV can have several stills (`IMG_9999.HEIC` and an
 * unrelated `IMG_9999.JPG`): a still sharing the MOV's content identifier wins, and otherwise
 * the pair counts only when the MOV has a single candidate (spec §6.4 pairing).
 */
export function livePairs(manifest: Manifest, seeded: Set<string>): Array<[string, string]> {
  const cid = new Map(manifest.records.map((r) => [r.name, r.contentId]));
  const stillsOf = new Map<string, string[]>();
  for (const [still, mov] of manifest.relations.livePhoto)
    if (seeded.has(still) && seeded.has(mov))
      stillsOf.set(mov, [...(stillsOf.get(mov) ?? []), still]);
  const out: Array<[string, string]> = [];
  for (const [mov, stills] of stillsOf) {
    const id = cid.get(mov) ?? null;
    const byId = id === null ? [] : stills.filter((s) => cid.get(s) === id);
    const real = byId.length > 0 ? byId : stills.length === 1 ? stills : [];
    for (const s of real) out.push([s, mov]);
  }
  return out;
}

const FINAL_FILE_STATES = new Set(['INGESTED', 'COPY', 'QUARANTINED']);
const sha8 = (s: string) => s.slice(0, 8);

/**
 * Every way the run disagrees with ground truth, one readable line per finding. Copy-ness comes
 * only from sha256 equality and synthetic ground truth (`SeedFile.expect`), never from names:
 * `IMG_2834` and `"IMG_2834 1"` are different shots, and a false copy is the one harmful error.
 */
export async function diffExpectations(
  dbUrl: string,
  sm: SeedManifest,
  manifest: Manifest,
): Promise<string[]> {
  const w = await loadWorld(dbUrl);
  const out: string[] = [];
  const records = new Map(manifest.records.map((r) => [r.name, r]));
  const rowsByPath = new Map<string, World['files']>();
  for (const f of w.files) rowsByPath.set(f.relPath, [...(rowsByPath.get(f.relPath) ?? []), f]);
  const seeded = sm.files.filter((f) => !isIgnored(f.name));
  /** The seeded file's final row, or null (reported). */
  const rowFor = (f: SeedFile) => {
    const rows = (rowsByPath.get(f.relPath) ?? []).filter((r) => FINAL_FILE_STATES.has(r.state));
    if (rows.length !== 1) {
      out.push(`${f.relPath}: expected one final files row, found ${String(rows.length)}`);
      return null;
    }
    return rows[0]!;
  };
  const contentFor = (f: SeedFile): ContentRow | null => {
    const row = rowFor(f);
    if (!row) return null;
    if (row.sha256 !== f.sha256) {
      out.push(`${f.relPath}: recorded sha256 ${String(row.sha256)} ≠ seeded ${f.sha256}`);
      return null;
    }
    const c = w.contents.get(f.sha256);
    if (!c) out.push(`${f.relPath}: no contents row for ${sha8(f.sha256)}`);
    return c ?? null;
  };

  // Byte-identical files: exactly one holds the content (INGESTED), every other one is a COPY.
  const bySha = new Map<string, SeedFile[]>();
  for (const f of seeded) bySha.set(f.sha256, [...(bySha.get(f.sha256) ?? []), f]);
  for (const [sha, fs] of bySha) {
    const states = fs.map((f) => rowFor(f)?.state ?? 'MISSING').sort();
    const expected = ['INGESTED', ...Array<string>(fs.length - 1).fill('COPY')].sort();
    if (states.join() !== expected.join())
      out.push(
        `${sha8(sha)} (${fs.map((f) => f.relPath).join(', ')}): expected states ` +
          `[${expected.join(', ')}], got [${states.join(', ')}]`,
      );
  }

  for (const f of seeded.filter((f) => f.origin === 'corpus')) {
    const c = contentFor(f);
    const r = records.get(f.name);
    if (c && r && c.kind !== r.kind)
      out.push(`${f.relPath}: kind ${c.kind}, manifest says ${r.kind}`);
  }

  for (const f of seeded.filter((f) => f.origin === 'corrupt')) {
    const c = contentFor(f);
    if (c && (c.state !== 'QUARANTINED' || c.quarantineReason !== (f.reason ?? null)))
      out.push(
        `${f.relPath}: expected QUARANTINED(${String(f.reason)}), got ` +
          `${c.state}(${String(c.quarantineReason)})`,
      );
  }

  // Synthetic ground truth against the seed (and, for bridges, the seed's burst partner).
  const seedSha = new Map(
    seeded.filter((f) => f.origin === 'corpus').map((f) => [f.name, f.sha256]),
  );
  const group = (sha: string) => w.contents.get(sha)?.copyGroup ?? null;
  const same = (a: string, b: string) => a === b || (group(a) !== null && group(a) === group(b));
  const tight = (a: string, b: string): boolean => {
    const [x, y] = [w.contents.get(a), w.contents.get(b)];
    if (!x || !y || x.phash === null || y.phash === null || x.dhash === null || y.dhash === null)
      return false;
    const t = DEFAULT_THRESHOLDS; // the stacks run with the default thresholds
    return (
      hamming(toUnsigned(x.phash), toUnsigned(y.phash)) <= t.copyPhash &&
      hamming(toUnsigned(x.dhash), toUnsigned(y.dhash)) <= t.copyDhash
    );
  };
  for (const v of seeded.filter((f) => f.origin === 'synthetic')) {
    const c = contentFor(v);
    const s = seedSha.get(v.seedName ?? '');
    if (!c || s === undefined) {
      if (s === undefined) out.push(`${v.relPath}: seed ${String(v.seedName)} was not seeded`);
      continue;
    }
    if (c.state !== 'CLUSTERED') {
      out.push(`${v.relPath}: expected CLUSTERED, got ${c.state}`);
      continue;
    }
    // Rows 6–8 of §6.3: a copy exactly when the pair is within the copy distances (measured on
    // the stored hashes), otherwise only similar.
    const expect =
      v.expect === 'copy-if-tight' ? (tight(v.sha256, s) ? 'copy' : 'similar') : v.expect;
    if (expect === 'copy' && !same(v.sha256, s))
      out.push(`${v.relPath}: expected a copy of ${String(v.seedName)}, not in its copy group`);
    if (expect === 'similar') {
      // Within the look-alike distances by construction: always in the seed's similar set. Its
      // copy group only through another variant of the same seed that is a copy of both (an
      // EXIF-less re-encode bridging them, spec §6.3 transitive union).
      const sc = w.contents.get(s);
      if (!same(v.sha256, s) && (c.similarSet === null || c.similarSet !== sc?.similarSet))
        out.push(`${v.relPath}: expected similar to ${String(v.seedName)}, not in its similar set`);
      const bridged = seeded.some(
        (o) =>
          o !== v && o.origin === 'synthetic' && o.seedName === v.seedName && same(o.sha256, s),
      );
      if (same(v.sha256, s) && !bridged)
        out.push(`${v.relPath}: expected only similar to ${String(v.seedName)}, but a copy`);
    }
    if (v.expect === 'copy-or-ambiguous') {
      const [a, b] = (v.bridges ?? []).map((n) => seedSha.get(n));
      if (a === undefined || b === undefined) {
        out.push(`${v.relPath}: bridged frames ${String(v.bridges)} not both seeded`);
        continue;
      }
      if (same(a, b)) out.push(`${v.relPath}: burst frames ${String(v.bridges)} share a group`);
      if (!c.ambiguous && !same(v.sha256, a) && !same(v.sha256, b))
        out.push(`${v.relPath}: neither copy_ambiguous nor a copy of ${String(v.bridges)}`);
    }
  }

  // Burst frames are never copies of each other.
  for (const [a, b] of manifest.relations.burst) {
    const [sa, sb] = [seedSha.get(a), seedSha.get(b)];
    if (sa !== undefined && sb !== undefined && same(sa, sb))
      out.push(`burst frames ${a} and ${b} share copy group ${String(group(sa))}`);
  }
  return out;
}

/** Each set as its sorted member sha256s, comma-joined. */
const canon = (groups: Iterable<string[]>): Set<string> =>
  new Set([...groups].map((g) => [...g].sort().join(',')));

export interface Partitions {
  similar: Set<string>;
  copy: Set<string>;
}

/**
 * The similar-set and copy-group partitions of the clustered contents, comparable across runs.
 * `copy` excludes `copy_ambiguous` contents (which group they join depends on arrival order);
 * both leave out the `exclude` contents.
 */
export async function partitions(
  dbUrl: string,
  exclude: ReadonlySet<string> = new Set(),
): Promise<Partitions> {
  const w = await loadWorld(dbUrl);
  const similar = new Map<string, string[]>();
  const copy = new Map<number, string[]>();
  for (const c of w.contents.values()) {
    if (c.state !== 'CLUSTERED' || c.copyGroup === null || exclude.has(c.sha256)) continue;
    const s = c.similarSet === null ? `g${String(c.copyGroup)}` : `s${String(c.similarSet)}`;
    similar.set(s, [...(similar.get(s) ?? []), c.sha256]);
    if (!c.ambiguous) copy.set(c.copyGroup, [...(copy.get(c.copyGroup) ?? []), c.sha256]);
  }
  return { similar: canon(similar.values()), copy: canon(copy.values()) };
}

/** A finished run to compare: its database and its `treeSnapshot(...snap)` arguments. */
export interface ComparableRun {
  dbUrl: string;
  snap: [string, string];
}

/**
 * Two runs' partitions and staged trees with every content flagged `copy_ambiguous` in EITHER
 * run left out (and its staged entries): which EXIF-less copies get flagged can depend on
 * processing order (spec §12 R3, as amended). `toEqual` the `[a, b]` pairs.
 */
export async function moduloAmbiguity(
  a: ComparableRun,
  b: ComparableRun,
): Promise<{
  partitions: [Partitions, Partitions];
  tree: [string[], string[]];
  ambiguous: number;
  /** Contents flagged only in `a` / only in `b` (sorted): order-dependent flags, to print. */
  onlyA: string[];
  onlyB: string[];
}> {
  const flagged = async (r: ComparableRun) =>
    (
      await poolFor(r.dbUrl).query<{ sha256: string }>(
        'SELECT sha256 FROM contents WHERE copy_ambiguous ORDER BY sha256',
      )
    ).rows.map((x) => x.sha256);
  const [fa, fb] = [await flagged(a), await flagged(b)];
  const [sa, sb] = [new Set(fa), new Set(fb)];
  const either = new Set([...fa, ...fb]);
  const tree = async (r: ComparableRun) =>
    (await treeSnapshot(...r.snap)).filter((l) => !either.has(l.slice(l.lastIndexOf('\t') + 1)));
  return {
    partitions: [await partitions(a.dbUrl, either), await partitions(b.dbUrl, either)],
    tree: [await tree(a), await tree(b)],
    ambiguous: either.size,
    onlyA: fa.filter((s) => !sb.has(s)),
    onlyB: fb.filter((s) => !sa.has(s)),
  };
}
