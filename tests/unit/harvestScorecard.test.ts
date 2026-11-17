import { describe, expect, test } from 'vitest';
import {
  buildScorecard,
  curvePoint,
  oneIn,
  picksNeeded,
  signTest,
  verdict,
  wilson,
  type ModelFigures,
} from '../../src/harvest/scorecard.js';

const fitted: ModelFigures = {
  version: 14,
  promoted: true,
  picksUsed: 76,
  cvAgreement: 0.62,
  cvN: 77,
  chanceAgreement: 0.448,
  ruleAgreement: 0.539,
  ruleTies: 19,
  modelWins: 20,
  ruleWins: 11,
};

describe('wilson', () => {
  test('matches known values', () => {
    const half = wilson(50, 100);
    expect(half.lo).toBeCloseTo(0.4038, 3);
    expect(half.hi).toBeCloseTo(0.5962, 3);
    const all = wilson(10, 10);
    expect(all.lo).toBeCloseTo(0.7225, 3);
    expect(all.hi).toBeCloseTo(1, 6);
  });
  test('takes a fractional success count and an empty sample', () => {
    const i = wilson(47.5, 77);
    expect(i.lo).toBeGreaterThan(0.5);
    expect(i.hi).toBeLessThan(0.72);
    expect(wilson(0, 0)).toEqual({ lo: 0, hi: 1 });
  });
});

describe('signTest', () => {
  test('two-sided exact binomial on the picks that differ', () => {
    expect(signTest(20, 5)).toBeCloseTo(0.0041, 3);
    expect(signTest(5, 20)).toBeCloseTo(0.0041, 3);
    expect(signTest(20, 11)).toBeCloseTo(0.15, 2);
    expect(signTest(0, 0)).toBe(1);
    expect(signTest(3, 3)).toBe(1);
  });
  test('oneIn rounds a probability to "1 time in N"', () => {
    expect(oneIn(0.0041)).toBe(240);
    expect(oneIn(0.15)).toBe(7);
    expect(oneIn(1)).toBe(1);
  });
});

describe('picksNeeded', () => {
  test('more picks at the current win and loss rates until p < 0.05', () => {
    const more = picksNeeded(20, 11, 77);
    expect(more).not.toBeNull();
    expect(more!).toBeGreaterThanOrEqual(40);
    expect(more!).toBeLessThanOrEqual(60);
  });
  test('null when the model is not ahead', () => {
    expect(picksNeeded(10, 10, 77)).toBeNull();
    expect(picksNeeded(5, 20, 77)).toBeNull();
  });
  test('zero when the split is already significant', () => {
    expect(picksNeeded(20, 5, 77)).toBe(0);
  });
});

describe('verdict', () => {
  test('1: baselines not recorded', () => {
    const v = verdict({ ...fitted, cvN: null, chanceAgreement: null });
    expect(v.state).toBe('no-baselines');
    expect(v.text).toMatch(/backfill/);
  });
  test('2: still the rule score', () => {
    const v = verdict({ ...fitted, picksUsed: 0, cvN: 12, modelWins: 0, ruleWins: 0 });
    expect(v.state).toBe('rule-only');
    expect(v.text).toBe('Still the rule score: 12 usable picks so far; learning starts at 25.');
  });
  test('3: not beating chance when the interval reaches down to chance', () => {
    const v = verdict({ ...fitted, cvAgreement: 0.5, cvN: 30, chanceAgreement: 0.45 });
    expect(v.state).toBe('not-above-chance');
    expect(v.text).toMatch(/guessing/);
  });
  test('4: learning', () => {
    const v = verdict({ ...fitted, modelWins: 20, ruleWins: 5 });
    expect(v.state).toBe('learning');
    expect(v.text).toBe(
      'Learning: beat the rule score on 20 picks, lost on 5; that split would happen by luck about 1 time in 250.',
    );
  });
  test('5: worse than the rule score', () => {
    const v = verdict({ ...fitted, cvAgreement: 0.6, modelWins: 5, ruleWins: 20 });
    expect(v.state).toBe('worse');
    expect(v.text).toMatch(/^Worse than the rule score: it beat the model on 20 picks, lost on 5/);
  });
  test('6: not yet distinguishable, with the picks needed', () => {
    const v = verdict(fitted);
    expect(v.state).toBe('undecided');
    expect(v.text).toMatch(
      /^Not yet distinguishable from the rule score: beat it on 20 picks, lost on 11; that split happens by luck about 1 time in 7\. About \d+ more picks should separate them\.$/,
    );
  });
  test('6: not ahead, so no number', () => {
    const v = verdict({ ...fitted, modelWins: 9, ruleWins: 11 });
    expect(v.state).toBe('undecided');
    expect(v.text).toMatch(/The model is not ahead, so no number of picks will separate them yet/);
  });
});

describe('buildScorecard and curvePoint', () => {
  test('rows with intervals, the chance row without, and the paired counts', () => {
    const s = buildScorecard(fitted);
    expect(s.version).toBe(14);
    expect(s.n).toBe(77);
    expect(s.model!.pct).toBeCloseTo(0.62);
    expect(s.model!.lo).toBeLessThan(0.62);
    expect(s.model!.hi).toBeGreaterThan(0.62);
    expect(s.rule!.pct).toBeCloseTo(0.539);
    expect(s.rule!.ties).toBe(19);
    expect(s.chance).toBeCloseTo(0.448);
    expect(s.paired).toMatchObject({ modelWins: 20, ruleWins: 11 });
    expect(s.paired!.p).toBeCloseTo(0.15, 2);
    expect(s.verdict.state).toBe('undecided');
  });
  test('null baselines leave the rows null and the verdict asks for the backfill', () => {
    const s = buildScorecard({
      ...fitted,
      cvN: null,
      chanceAgreement: null,
      ruleAgreement: null,
      ruleTies: null,
      modelWins: null,
      ruleWins: null,
    });
    expect(s.rule).toBeNull();
    expect(s.chance).toBeNull();
    expect(s.paired).toBeNull();
    expect(s.model).toBeNull(); // no n to put an interval on
    expect(s.verdict.state).toBe('no-baselines');
  });
  test('no model at all', () => {
    const s = buildScorecard(null);
    expect(s.version).toBeNull();
    expect(s.verdict.state).toBe('no-model');
  });
  test('a curve point carries the band and both baselines; a rule-only version has no dot', () => {
    const p = curvePoint(fitted);
    expect(p).toMatchObject({ version: 14, cvN: 77, cv: 0.62, promoted: true, fitted: true });
    expect(p.lo).toBeLessThan(0.62);
    expect(p.rule).toBeCloseTo(0.539);
    expect(p.chance).toBeCloseTo(0.448);
    const r = curvePoint({ ...fitted, picksUsed: 0, cvAgreement: 0.54, version: 3 });
    expect(r.fitted).toBe(false);
    expect(r.cv).toBe(0.54); // the rule's own figure, used for the rule line
  });
  test('a fitted version with null baselines keeps its dot at the picks it trained on', () => {
    const p = curvePoint({ ...fitted, cvN: null, chanceAgreement: null, ruleAgreement: null });
    expect(p).toMatchObject({ cvN: 76, cv: 0.62, fitted: true, rule: null, chance: null });
    expect(p.lo).toBeLessThan(0.62);
    const r = curvePoint({ ...fitted, picksUsed: 0, cvN: null, ruleAgreement: null });
    expect(r.cvN).toBeNull(); // a rule-only version without cv_n has nowhere to go
  });
});
