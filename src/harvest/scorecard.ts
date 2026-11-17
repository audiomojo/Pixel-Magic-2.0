// The scorecard's arithmetic: the Wilson interval, the exact sign test, the verdict and the
// picks-needed estimate, all pure. scoreboard() in picks.ts feeds it ranker_models rows; the
// page only renders what comes out.

/** Agreement starts being learnt at this many usable picks (ranker.MIN_TRAIN_PICKS). */
export const LEARNING_STARTS = 25;
/** The paired test's bar: a split this unlikely by luck counts as a real difference. */
export const SIGNIFICANCE = 0.05;
const Z = 1.959964; // 95%
const MAX_PICKS = 100_000;

/** A ranker_models row as the scorecard reads it (the baselines are null before the backfill). */
export interface ModelFigures {
  version: number;
  promoted: boolean;
  picksUsed: number;
  cvAgreement: number | null;
  cvN: number | null;
  chanceAgreement: number | null;
  ruleAgreement: number | null;
  ruleTies: number | null;
  modelWins: number | null;
  ruleWins: number | null;
}

export interface Interval {
  lo: number;
  hi: number;
}

/**
 * The 95% Wilson interval for a proportion of `successes` in `n`. The success count may be
 * fractional (tie credits). With n = 0 it is the whole range.
 */
export function wilson(successes: number, n: number): Interval {
  if (n <= 0) return { lo: 0, hi: 1 };
  const p = successes / n;
  const z2 = Z * Z;
  const centre = (p + z2 / (2 * n)) / (1 + z2 / n);
  const half = (Z * Math.sqrt((p * (1 - p)) / n + z2 / (4 * n * n))) / (1 + z2 / n);
  return { lo: Math.max(0, centre - half), hi: Math.min(1, centre + half) };
}

const logFactorial = (n: number): number => {
  let s = 0;
  for (let k = 2; k <= n; k++) s += Math.log(k);
  return s;
};

/**
 * The two-sided exact binomial (sign) test on the picks where the model and the rule differ:
 * the chance of a split at least this lopsided when neither is better (p = ½). 1 with none.
 */
export function signTest(wins: number, losses: number): number {
  const n = wins + losses;
  if (n === 0) return 1;
  const low = Math.min(wins, losses);
  let tail = 0;
  for (let k = 0; k <= low; k++)
    tail += Math.exp(logFactorial(n) - logFactorial(k) - logFactorial(n - k) - n * Math.LN2);
  return Math.min(1, 2 * tail);
}

/** A probability as "1 time in N": whole numbers below 10, two significant figures above. */
export function oneIn(p: number): number {
  const n = 1 / p;
  if (n < 10) return Math.max(1, Math.round(n));
  const scale = 10 ** (Math.floor(Math.log10(n)) - 1);
  return Math.round(n / scale) * scale;
}

/**
 * More picks until the sign test clears SIGNIFICANCE, if the model keeps winning and losing
 * at today's rates per pick. 0 when it already does; null when the model is not ahead.
 */
export function picksNeeded(wins: number, losses: number, n: number): number | null {
  if (wins <= losses || n <= 0) return null;
  for (let total = n; total <= MAX_PICKS; total++) {
    const w = Math.round((wins / n) * total);
    const l = Math.round((losses / n) * total);
    if (signTest(w, l) < SIGNIFICANCE) return total - n;
  }
  return null;
}

export type VerdictState =
  | 'no-model'
  | 'no-baselines'
  | 'rule-only'
  | 'not-above-chance'
  | 'learning'
  | 'worse'
  | 'undecided';

export interface Verdict {
  state: VerdictState;
  text: string;
}

const pct = (x: number): string => `${Math.round(x * 100)}%`;
const picks = (n: number): string => `${n} pick${n === 1 ? '' : 's'}`;

/** The verdict in plain words, by the rules in the spec (§8), in this order. */
export function verdict(f: ModelFigures | null): Verdict {
  if (f === null) return { state: 'no-model', text: 'No model yet.' };
  if (
    f.cvN === null ||
    f.chanceAgreement === null ||
    f.ruleAgreement === null ||
    f.modelWins === null ||
    f.ruleWins === null
  )
    return {
      state: 'no-baselines',
      text: `Baselines not recorded yet for model v${f.version}. Run the backfill: cd ai && uv run python -m pixelmagic_ai.scorecard backfill`,
    };
  if (f.picksUsed === 0)
    return {
      state: 'rule-only',
      text: `Still the rule score: ${f.cvN} usable picks so far; learning starts at ${LEARNING_STARTS}.`,
    };
  const cv = f.cvAgreement ?? 0;
  const band = wilson(cv * f.cvN, f.cvN);
  if (band.lo <= f.chanceAgreement)
    return {
      state: 'not-above-chance',
      text: `Not beating chance yet: the model's range (${pct(band.lo)}–${pct(band.hi)}) includes guessing (${pct(f.chanceAgreement)}).`,
    };
  const p = signTest(f.modelWins, f.ruleWins);
  const luck = `about 1 time in ${oneIn(p)}`;
  if (p < SIGNIFICANCE && f.modelWins > f.ruleWins)
    return {
      state: 'learning',
      text: `Learning: beat the rule score on ${picks(f.modelWins)}, lost on ${f.ruleWins}; that split would happen by luck ${luck}.`,
    };
  if (p < SIGNIFICANCE && f.ruleWins > f.modelWins)
    return {
      state: 'worse',
      text: `Worse than the rule score: it beat the model on ${picks(f.ruleWins)}, lost on ${f.modelWins}; that split would happen by luck ${luck}.`,
    };
  const more = picksNeeded(f.modelWins, f.ruleWins, f.cvN);
  const tail =
    more === null
      ? 'The model is not ahead, so no number of picks will separate them yet.'
      : `About ${more} more picks should separate them.`;
  const split =
    f.modelWins + f.ruleWins === 0
      ? 'they have agreed on every pick so far'
      : `beat it on ${picks(f.modelWins)}, lost on ${f.ruleWins}; that split happens by luck ${luck}`;
  return {
    state: 'undecided',
    text: `Not yet distinguishable from the rule score: ${split}. ${tail}`,
  };
}

export interface Scorecard {
  version: number | null;
  n: number | null;
  model: { pct: number; lo: number; hi: number } | null;
  rule: { pct: number; lo: number; hi: number; ties: number } | null;
  chance: number | null;
  paired: { modelWins: number; ruleWins: number; p: number } | null;
  verdict: Verdict;
}

/** The current model's three rows and verdict. */
export function buildScorecard(f: ModelFigures | null): Scorecard {
  const n = f?.cvN ?? null;
  const row = (agreement: number | null) =>
    f !== null && n !== null && agreement !== null
      ? { pct: agreement, ...wilson(agreement * n, n) }
      : null;
  const rule = row(f?.ruleAgreement ?? null);
  return {
    version: f?.version ?? null,
    n,
    model: row(f?.cvAgreement ?? null),
    rule:
      rule && f?.ruleTies !== null && f?.ruleTies !== undefined
        ? { ...rule, ties: f.ruleTies }
        : null,
    chance: f?.chanceAgreement ?? null,
    paired:
      f !== null && f.modelWins !== null && f.ruleWins !== null
        ? { modelWins: f.modelWins, ruleWins: f.ruleWins, p: signTest(f.modelWins, f.ruleWins) }
        : null,
    verdict: verdict(f),
  };
}

export interface CurvePoint {
  version: number;
  /** Held-out picks; for a fitted version from before the backfill, the picks it trained on. */
  cvN: number | null;
  /** For a fitted version its CV; for a rule-only version the rule's own figure. */
  cv: number | null;
  lo: number | null;
  hi: number | null;
  rule: number | null;
  chance: number | null;
  promoted: boolean;
  fitted: boolean;
}

/** One learning-curve entry per version; the page decides what to draw from it. */
export function curvePoint(f: ModelFigures): CurvePoint {
  const fitted = f.picksUsed > 0;
  const cvN = f.cvN ?? (fitted ? f.picksUsed : null);
  const band = cvN !== null && f.cvAgreement !== null ? wilson(f.cvAgreement * cvN, cvN) : null;
  return {
    version: f.version,
    cvN,
    cv: f.cvAgreement,
    lo: band?.lo ?? null,
    hi: band?.hi ?? null,
    rule: f.ruleAgreement,
    chance: f.chanceAgreement,
    promoted: f.promoted,
    fitted,
  };
}
