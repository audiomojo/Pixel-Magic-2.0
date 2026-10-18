import { captureConflict, type CaptureKey, type Facts } from './classify.js';

export type GroupKind = 'copy' | 'similar';
export type Verdict = 'copy' | 'similar';

/** The group changes caused by one `Grouper.add`, for the caller to persist. */
export interface Assignment {
  /** Root copy group of the new content after all merges. */
  copyGroup: number;
  /** A copy union was refused: it would mix two different capture keys or two frames of one burst. */
  ambiguous: boolean;
  /** Groups allocated by this add: always one copy group, at most one similar set. */
  created: Array<[id: number, kind: GroupKind]>;
  copyMerges: Array<[loser: number, winner: number]>;
  similarMerges: Array<[loser: number, winner: number]>;
  /** Copy groups whose similar set changed (copy group → root similar set). */
  similarLinks: Map<number, number>;
}

export interface LoadedRow {
  sha256: string;
  copyGroup: number;
  similarSet: number | null;
  captured: CaptureKey | null;
  burstUuid: string | null;
  /** Ignored by grouping: the flag is persisted in `contents.copy_ambiguous` and never changes unions. */
  ambiguous: boolean;
}

interface CopyGroup {
  parent: number;
  /** Member SHA-256s (roots only; emptied once merged away). */
  members: string[];
  /** Distinct non-null capture keys of the members (roots only). */
  captures: CaptureKey[];
  /** Burst UUIDs of the members (roots only); a copy group holds at most one frame per burst. */
  bursts: Set<string>;
  /** Similar set id, possibly not a root (roots only). */
  similar: number | null;
}

interface SimilarSet {
  parent: number;
  /** Number of contents in the set (roots only). */
  size: number;
}

const sameKey = (a: CaptureKey, b: CaptureKey): boolean => a.s === b.s && a.sub === b.sub;

/** Would merging these groups mix two shots: conflicting capture keys, or two frames of one burst? */
const mustNotMerge = (a: CopyGroup, b: CopyGroup): boolean =>
  a.captures.some((x) => b.captures.some((y) => captureConflict(x, y))) ||
  [...a.bursts].some((u) => b.bursts.has(u));

/** Larger wins; on a tie the lower id wins. */
const pick = (a: [id: number, size: number], b: [id: number, size: number]): [number, number] =>
  a[1] > b[1] || (a[1] === b[1] && a[0] < b[0]) ? [a[0], b[0]] : [b[0], a[0]];

/**
 * Capture-consistent union-find over copy groups and their similar sets (spec §6.3).
 * A copy union that would put two conflicting capture keys in one group is refused, so burst
 * frames are never collapsed into one copy group, even through an EXIF-less bridge.
 */
export class Grouper {
  private readonly copy = new Map<number, CopyGroup>();
  private readonly similar = new Map<number, SimilarSet>();
  private readonly groupOf = new Map<string, number>();

  constructor(private readonly newId: (kind: GroupKind) => number) {}

  /** Restore one already-clustered content from the database. */
  load(row: LoadedRow): void {
    this.claim(row.sha256, row.copyGroup);
    let g = this.copy.get(row.copyGroup);
    if (g === undefined) {
      g = { parent: row.copyGroup, members: [], captures: [], bursts: new Set(), similar: null };
      this.copy.set(row.copyGroup, g);
    }
    g.members.push(row.sha256);
    if (row.captured !== null) addCapture(g, row.captured);
    if (row.burstUuid !== null) g.bursts.add(row.burstUuid);
    if (row.similarSet !== null) {
      if (!this.similar.has(row.similarSet))
        this.similar.set(row.similarSet, { parent: row.similarSet, size: 0 });
      g.similar ??= row.similarSet;
      this.sim(g.similar).size += 1;
    }
  }

  /** Group a new content given its look-alike matches and their verdicts. */
  add(f: Facts, matches: ReadonlyArray<readonly [sha256: string, verdict: Verdict]>): Assignment {
    const resolved = matches.map(([sha, verdict]) => {
      const id = this.groupOf.get(sha);
      if (id === undefined) throw new Error(`Grouper: unknown match ${sha}`);
      return [this.findCopy(id), verdict] as const;
    });

    const gid = this.newId('copy');
    this.claim(f.sha256, gid);
    this.copy.set(gid, {
      parent: gid,
      members: [f.sha256],
      captures: f.captured === null ? [] : [f.captured],
      bursts: new Set(f.burstUuid === null ? [] : [f.burstUuid]),
      similar: null,
    });
    const asg: Assignment = {
      copyGroup: gid,
      ambiguous: false,
      created: [[gid, 'copy']],
      copyMerges: [],
      similarMerges: [],
      similarLinks: new Map(),
    };

    const copyTargets = [...new Set(resolved.filter(([, v]) => v === 'copy').map(([r]) => r))];
    copyTargets.sort((a, b) => size(this.group(b)) - size(this.group(a)) || a - b);
    const pendingSets: number[] = [];
    for (const target of copyTargets) {
      const root = this.findCopy(gid);
      if (mustNotMerge(this.group(root), this.group(target))) {
        asg.ambiguous = true;
        continue;
      }
      this.mergeCopy(root, target, asg, pendingSets);
    }

    const root = this.findCopy(gid);
    asg.copyGroup = root;
    const similarTargets = [...new Set(resolved.map(([r]) => this.findCopy(r)))].filter(
      (r) => r !== root,
    );
    if (similarTargets.length > 0 || pendingSets.length > 0)
      this.joinSimilar([root, ...similarTargets], pendingSets, asg);
    for (const [g, s] of asg.similarLinks) asg.similarLinks.set(g, this.findSim(s));
    return asg;
  }

  /** Copy groups as sorted, comma-joined SHA-256 lists. */
  copyPartition(): Set<string> {
    return this.partition((root) => root);
  }

  /** Connected components of the look-alike graph (similar sets, plus unlinked copy groups). */
  similarPartition(): Set<string> {
    return this.partition((root) => {
      const s = this.group(root).similar;
      return s === null ? `c${root}` : `s${this.findSim(s)}`;
    });
  }

  private partition(keyOf: (copyRoot: number) => string | number): Set<string> {
    const parts = new Map<string | number, string[]>();
    for (const [sha, id] of this.groupOf) {
      const k = keyOf(this.findCopy(id));
      const list = parts.get(k);
      if (list === undefined) parts.set(k, [sha]);
      else list.push(sha);
    }
    return new Set([...parts.values()].map((l) => l.sort().join(',')));
  }

  private mergeCopy(a: number, b: number, asg: Assignment, pendingSets: number[]): void {
    const [winner, loser] = pick([a, size(this.group(a))], [b, size(this.group(b))]);
    const w = this.group(winner);
    const l = this.group(loser);
    if (l.similar !== null && w.similar === null) {
      w.similar = l.similar;
      this.sim(w.similar).size += w.members.length;
      asg.similarLinks.set(winner, w.similar);
    } else if (l.similar === null && w.similar !== null) {
      this.sim(w.similar).size += l.members.length;
    } else if (l.similar !== null && w.similar !== null) {
      pendingSets.push(l.similar);
    }
    w.members.push(...l.members);
    for (const c of l.captures) addCapture(w, c);
    for (const u of l.bursts) w.bursts.add(u);
    l.parent = winner;
    l.members = [];
    l.captures = [];
    l.bursts = new Set();
    l.similar = null;
    asg.copyMerges.push([loser, winner]);
  }

  private joinSimilar(roots: readonly number[], pendingSets: readonly number[], asg: Assignment) {
    const sets = new Set(pendingSets.map((s) => this.findSim(s)));
    for (const r of roots) {
      const s = this.group(r).similar;
      if (s !== null) sets.add(this.findSim(s));
    }
    if (sets.size === 0) {
      const sid = this.newId('similar');
      this.similar.set(sid, { parent: sid, size: 0 });
      asg.created.push([sid, 'similar']);
      sets.add(sid);
    }
    const ordered = [...sets].sort((a, b) => this.sim(b).size - this.sim(a).size || a - b);
    const winner = ordered[0]!;
    const w = this.sim(winner);
    for (const loser of ordered.slice(1)) {
      const l = this.sim(loser);
      w.size += l.size;
      l.parent = winner;
      l.size = 0;
      asg.similarMerges.push([loser, winner]);
    }
    for (const r of roots) {
      const g = this.group(r);
      if (g.similar !== null) continue;
      g.similar = winner;
      w.size += g.members.length;
      asg.similarLinks.set(r, winner);
    }
  }

  private claim(sha: string, id: number): void {
    if (this.groupOf.has(sha)) throw new Error(`Grouper: ${sha} is already grouped`);
    this.groupOf.set(sha, id);
  }

  private group(id: number): CopyGroup {
    const g = this.copy.get(id);
    if (g === undefined) throw new Error(`Grouper: unknown copy group ${id}`);
    return g;
  }

  private simNode(id: number): SimilarSet {
    const s = this.similar.get(id);
    if (s === undefined) throw new Error(`Grouper: unknown similar set ${id}`);
    return s;
  }

  /** Root similar set of `id`. */
  private sim(id: number): SimilarSet {
    return this.simNode(this.findSim(id));
  }

  private findCopy(id: number): number {
    return find(id, (x) => this.group(x));
  }

  private findSim(id: number): number {
    return find(id, (x) => this.simNode(x));
  }
}

const size = (g: CopyGroup): number => g.members.length;

function addCapture(g: CopyGroup, key: CaptureKey): void {
  if (!g.captures.some((c) => sameKey(c, key))) g.captures.push(key);
}

/** Union-find root with path compression. */
function find(id: number, node: (id: number) => { parent: number }): number {
  let root = id;
  while (node(root).parent !== root) root = node(root).parent;
  for (let x = id; x !== root;) {
    const n = node(x);
    x = n.parent;
    n.parent = root;
  }
  return root;
}
