/** Identical-picture dedupe: which of several pixel-identical vault files stays. */

/** Case-insensitive substrings of EXIF:Software / XMP:CreatorTool that mark an editor. */
export const EDITOR_PATTERNS: readonly string[] = [
  'picasa',
  'photoshop',
  'lightroom',
  'gimp',
  'iphoto',
  'aperture',
  'photos',
  'windows photo',
  'photo gallery',
  'acdsee',
  'snapseed',
  'google',
];

export function isEditor(software: string | null, creatorTool: string | null): boolean {
  const hay = [software, creatorTool].map((v) => (v ?? '').toLowerCase());
  return hay.some((h) => h !== '' && EDITOR_PATTERNS.some((p) => h.includes(p)));
}

export interface KeeperFacts {
  sha256: string;
  software: string | null;
  creatorTool: string | null;
  metaCount: number;
  sizeBytes: bigint;
  ingestedAt: Date;
}

export type KeeperRule = 'camera-original' | 'more-metadata' | 'larger' | 'earlier';

/** First rule separating `a` from `b`, and whether `a` wins on it. `earlier` always separates. */
function compare(a: KeeperFacts, b: KeeperFacts): { rule: KeeperRule; aWins: boolean } {
  const ae = isEditor(a.software, a.creatorTool);
  const be = isEditor(b.software, b.creatorTool);
  if (ae !== be) return { rule: 'camera-original', aWins: !ae };
  if (a.metaCount !== b.metaCount)
    return { rule: 'more-metadata', aWins: a.metaCount > b.metaCount };
  if (a.sizeBytes !== b.sizeBytes) return { rule: 'larger', aWins: a.sizeBytes > b.sizeBytes };
  const at = a.ingestedAt.getTime();
  const bt = b.ingestedAt.getTime();
  if (at !== bt) return { rule: 'earlier', aWins: at < bt };
  return { rule: 'earlier', aWins: a.sha256 < b.sha256 };
}

/** The keeper wins every comparison; each loser records the first rule that separates it from it. */
export function chooseKeeper(members: KeeperFacts[]): {
  keeper: string;
  losers: { sha256: string; rule: KeeperRule }[];
} {
  if (members.length === 0) throw new Error('chooseKeeper: no members');
  let best = members[0]!;
  for (const m of members.slice(1)) if (compare(m, best).aWins) best = m;
  const losers = members
    .filter((m) => m !== best)
    .map((m) => ({ sha256: m.sha256, rule: compare(best, m).rule }))
    .sort((x, y) => (x.sha256 < y.sha256 ? -1 : 1));
  return { keeper: best.sha256, losers };
}
