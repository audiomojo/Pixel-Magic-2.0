/**
 * What decides the pick of a similar set (spec §6 "Pick of a set"), read from the database at
 * staging build time.
 */
export interface PickInputs {
  /** Chosen sha256 → decision order (higher = later). `no_clear_best` picks are not here. */
  userPicks: ReadonlyMap<string, number>;
  /** sha256 → score of the current ranked model. */
  scores: ReadonlyMap<string, number>;
}

export const NO_PICKS: PickInputs = { userPicks: new Map(), scores: new Map() };

/**
 * The pick among a set's candidates: the latest user pick that is still a candidate; else the
 * top-scoring candidate when at least two are scored and the top is strictly higher than the
 * second; else none.
 */
export function choosePick(candidates: readonly string[], p: PickInputs): string | null {
  let latest: string | null = null;
  let order = -Infinity;
  for (const s of candidates) {
    const o = p.userPicks.get(s);
    if (o !== undefined && o > order) [latest, order] = [s, o];
  }
  if (latest !== null) return latest;

  let top: string | null = null;
  let first = -Infinity;
  let second = -Infinity;
  let scored = 0;
  for (const s of candidates) {
    const v = p.scores.get(s);
    if (v === undefined) continue;
    scored++;
    if (v > first) [top, second, first] = [s, first, v];
    else if (v > second) second = v;
  }
  return scored >= 2 && first > second ? top : null;
}
