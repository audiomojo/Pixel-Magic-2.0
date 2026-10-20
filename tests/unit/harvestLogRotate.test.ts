import { chmod, readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { rotateIfLarge } from '../../src/harvest/logRotate.js';
import { tmpDir } from '../helpers/layout.js';

test('rotates a large file to .1, shifting older ones and keeping 5', async () => {
  const d = await tmpDir();
  const log = join(d, 'harvester.log');
  for (const [i, c] of ['a', 'b', 'c', 'd', 'e'].entries()) await writeFile(`${log}.${i + 1}`, c);
  await writeFile(log, 'x'.repeat(100));
  expect(await rotateIfLarge(log, 50, 5)).toBe(true);
  expect(await readFile(`${log}.1`, 'utf8')).toBe('x'.repeat(100));
  expect(await Promise.all([2, 3, 4, 5].map((n) => readFile(`${log}.${n}`, 'utf8')))).toEqual([
    'a',
    'b',
    'c',
    'd',
  ]); // 'e' was dropped
  expect((await readdir(d)).sort()).toEqual([1, 2, 3, 4, 5].map((n) => `harvester.log.${n}`));
});

test('leaves a small or missing file alone', async () => {
  const d = await tmpDir();
  const log = join(d, 'harvester.log');
  expect(await rotateIfLarge(log, 50, 5)).toBe(false);
  await writeFile(log, 'tiny');
  expect(await rotateIfLarge(log, 50, 5)).toBe(false);
  expect(await readdir(d)).toEqual(['harvester.log']);
});

test('gaps in the numbered files are fine; a real rename failure is thrown', async () => {
  const d = await tmpDir();
  const log = join(d, 'harvester.log');
  await writeFile(log, 'x'.repeat(100));
  await writeFile(`${log}.3`, 'c');
  expect(await rotateIfLarge(log, 50, 5)).toBe(true);
  expect(await readdir(d).then((n) => n.sort())).toEqual(['harvester.log.1', 'harvester.log.4']);

  const d2 = await tmpDir();
  const log2 = join(d2, 'harvester.log');
  await writeFile(log2, 'x'.repeat(100));
  await writeFile(`${log2}.1`, 'old');
  await chmod(d2, 0o500); // nothing can be renamed in a read-only directory
  try {
    await expect(rotateIfLarge(log2, 50, 5)).rejects.toThrow();
  } finally {
    await chmod(d2, 0o700);
  }
});
