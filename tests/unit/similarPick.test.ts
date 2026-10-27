import { expect, test } from 'vitest';
import { choosePick, NO_PICKS, type PickInputs } from '../../src/similarPick.js';

const [A, B, C] = ['a', 'b', 'c'].map((h) => h.repeat(64)) as [string, string, string];
const inputs = (
  userPicks: [string, number][] = [],
  scores: [string, number][] = [],
): PickInputs => ({ userPicks: new Map(userPicks), scores: new Map(scores) });

test('a user pick wins over a higher score', () => {
  expect(
    choosePick(
      [A, B],
      inputs(
        [[B, 0]],
        [
          [A, 0.9],
          [B, 0.1],
        ],
      ),
    ),
  ).toBe(B);
});

test('the later of two user picks wins', () => {
  expect(
    choosePick(
      [A, B, C],
      inputs([
        [A, 2],
        [C, 1],
      ]),
    ),
  ).toBe(A);
  expect(
    choosePick(
      [A, B, C],
      inputs([
        [A, 1],
        [C, 2],
      ]),
    ),
  ).toBe(C);
});

test('a user pick not among the candidates is ignored', () => {
  expect(
    choosePick(
      [A, B],
      inputs(
        [[C, 5]],
        [
          [A, 0.2],
          [B, 0.7],
        ],
      ),
    ),
  ).toBe(B);
  expect(choosePick([A, B], inputs([[C, 5]]))).toBeNull();
});

test('one scored candidate gives no pick', () => {
  expect(choosePick([A, B], inputs([], [[A, 0.9]]))).toBeNull();
});

test('a tie on the top score gives no pick', () => {
  expect(
    choosePick(
      [A, B, C],
      inputs(
        [],
        [
          [A, 0.5],
          [B, 0.5],
          [C, 0.1],
        ],
      ),
    ),
  ).toBeNull();
});

test('the top score wins when strictly greater than the second', () => {
  expect(
    choosePick(
      [A, B, C],
      inputs(
        [],
        [
          [A, 0.5],
          [B, 0.6],
          [C, 0.6000001],
        ],
      ),
    ),
  ).toBe(C);
});

test('no inputs give no pick', () => {
  expect(choosePick([A, B], NO_PICKS)).toBeNull();
  expect(NO_PICKS.userPicks.size + NO_PICKS.scores.size).toBe(0);
});
