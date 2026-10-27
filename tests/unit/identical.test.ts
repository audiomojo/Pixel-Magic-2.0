import { describe, expect, test } from 'vitest';
import { chooseKeeper, EDITOR_PATTERNS, isEditor, type KeeperFacts } from '../../src/identical.js';

const f = (sha256: string, o: Partial<KeeperFacts> = {}): KeeperFacts => ({
  sha256,
  software: null,
  creatorTool: null,
  metaCount: 2,
  sizeBytes: 100n,
  ingestedAt: new Date('2024-01-02T00:00:00Z'),
  ...o,
});

describe('isEditor', () => {
  test.each([
    ['Picasa 3.0', null, true],
    [null, 'Adobe Photoshop Lightroom Classic', true],
    ['Photos 9.0', null, true],
    ['17.1', null, false],
    ['Digital Camera FinePix S7000 Ver1.00', null, false],
    [null, null, false],
  ])('%s / %s', (s, c, want) => expect(isEditor(s, c)).toBe(want));
  test('editor list verbatim', () =>
    expect([...EDITOR_PATTERNS]).toEqual([
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
    ]));
});

describe('chooseKeeper', () => {
  test('camera-original beats a larger Picasa file', () => {
    const r = chooseKeeper([f('a', { software: 'Picasa 3.0', sizeBytes: 999n }), f('b')]);
    expect(r).toEqual({ keeper: 'b', losers: [{ sha256: 'a', rule: 'camera-original' }] });
  });
  test('more-metadata', () => {
    const r = chooseKeeper([f('a', { metaCount: 1, sizeBytes: 999n }), f('b', { metaCount: 3 })]);
    expect(r).toEqual({ keeper: 'b', losers: [{ sha256: 'a', rule: 'more-metadata' }] });
  });
  test('larger', () => {
    const r = chooseKeeper([f('a'), f('b', { sizeBytes: 200n })]);
    expect(r).toEqual({ keeper: 'b', losers: [{ sha256: 'a', rule: 'larger' }] });
  });
  test('earlier', () => {
    const r = chooseKeeper([f('a'), f('b', { ingestedAt: new Date('2024-01-01T00:00:00Z') })]);
    expect(r).toEqual({ keeper: 'b', losers: [{ sha256: 'a', rule: 'earlier' }] });
  });
  test('full tie: lower sha wins, rule earlier', () => {
    const r = chooseKeeper([f('b'), f('a')]);
    expect(r).toEqual({ keeper: 'a', losers: [{ sha256: 'b', rule: 'earlier' }] });
  });
  test('three members: each loser gets its own first separating rule', () => {
    const r = chooseKeeper([f('c', { software: 'GIMP 2.10' }), f('a'), f('b', { sizeBytes: 50n })]);
    expect(r.keeper).toBe('a');
    expect(r.losers).toEqual(
      expect.arrayContaining([
        { sha256: 'c', rule: 'camera-original' },
        { sha256: 'b', rule: 'larger' },
      ]),
    );
    expect(r.losers).toHaveLength(2);
  });
  test('order independent', () => {
    const m = [f('a', { sizeBytes: 1n }), f('b', { sizeBytes: 5n }), f('c', { sizeBytes: 3n })];
    expect(chooseKeeper([...m].reverse())).toEqual(chooseKeeper(m));
  });
});
