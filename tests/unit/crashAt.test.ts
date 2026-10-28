import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { expect, test, vi } from 'vitest';
import { crashAt } from '../../src/crashAt.js';
import { tmpDir } from '../helpers/layout.js';

test('no checkpoint name: no seam', () => {
  expect(crashAt(undefined, '/nowhere')).toBeUndefined();
  expect(crashAt('', '/nowhere')).toBeUndefined();
});

test('exits hard the first time the named checkpoint is reached, once per data dir', async () => {
  const dir = await tmpDir();
  const exit = vi.fn();
  const one = crashAt('identical_retired', dir, exit)!;
  await one('upsert');
  expect(exit).not.toHaveBeenCalled();
  await one('identical_retired');
  expect(exit).toHaveBeenCalledTimes(1);
  expect(existsSync(join(dir, '.crash-at-identical_retired'))).toBe(true);
  // A restarted worker (or a second one on the same data dir) passes the checkpoint.
  const two = crashAt('identical_retired', dir, exit)!;
  await two('identical_retired');
  await one('identical_retired');
  expect(exit).toHaveBeenCalledTimes(1);
});
