import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
  existsSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, expect, test } from 'vitest';
import {
  GuardedFs,
  OutsideAllowedRootError,
  dropzoneName,
  partPath,
  pathKey,
  sameMtime,
  type FsOps,
} from '../../src/harvest/paths.js';

const S = 1_000_000_000n;

test('pathKey is NFC + lowercase', () => {
  expect(pathKey('Café/IMG.JPG')).toBe('café/img.jpg');
});
test.each([
  [0n, true],
  [2n, true],
  [3n, false],
  [3600n, true],
  [3602n, true],
  [3603n, false],
  [7200n, false],
  [-3601n, true],
])('sameMtime %s s -> %s', (sec, ok) => {
  expect(sameMtime(1000n * S, 1000n * S + sec * S)).toBe(ok);
  expect(sameMtime(1000n * S + sec * S, 1000n * S)).toBe(ok);
});
test('dropzoneName', () => {
  expect(dropzoneName('Expansion', 'a/b.JPG', 1)).toBe('harvest/Expansion/a/b.JPG');
  expect(dropzoneName('Expansion', 'a/b.JPG', 2)).toBe('harvest/Expansion/a/b (2).JPG');
  expect(dropzoneName('E', 'x', 2)).toBe('harvest/E/x (2)');
  expect(dropzoneName('E', 'd.v/x', 3)).toBe('harvest/E/d.v/x (3)');
  expect(dropzoneName('E', '.hidden', 2)).toBe('harvest/E/.hidden (2)');
});
test('partPath', () => {
  expect(partPath('/a/b/c.jpg', '42')).toBe('/a/b/.c.jpg.h42.part');
});

const tmp = mkdtempSync(join(tmpdir(), 'gfs-'));
const base = realpathSync(tmp);
const root = join(base, 'root');
const outside = join(base, 'root-sibling');
mkdirSync(root);
mkdirSync(outside);
symlinkSync(outside, join(root, 'link'));
const g = new GuardedFs([root]);
afterAll(() => {});

test('refuses sibling, .. escape and symlink escape', async () => {
  await expect(g.mkdirp(join(outside, 'x'))).rejects.toBeInstanceOf(OutsideAllowedRootError);
  await expect(g.mkdirp(join(root, '..', 'root-sibling', 'x'))).rejects.toBeInstanceOf(
    OutsideAllowedRootError,
  );
  await expect(g.mkdirp(join(root, 'link', 'x'))).rejects.toBeInstanceOf(OutsideAllowedRootError);
  await expect(g.openForWrite(join(root, 'link', 'f'))).rejects.toBeInstanceOf(
    OutsideAllowedRootError,
  );
  await expect(g.unlink(join(outside, 'f'))).rejects.toBeInstanceOf(OutsideAllowedRootError);
  await expect(g.chmod(join(outside, 'f'), 0o444)).rejects.toBeInstanceOf(OutsideAllowedRootError);
  await expect(g.utimesNs(join(outside, 'f'), 1n)).rejects.toBeInstanceOf(OutsideAllowedRootError);
  await expect(g.rename(join(root, 'a'), join(outside, 'a'))).rejects.toBeInstanceOf(
    OutsideAllowedRootError,
  );
  await expect(g.rename(join(outside, 'a'), join(root, 'a'))).rejects.toBeInstanceOf(
    OutsideAllowedRootError,
  );
  expect(existsSync(join(outside, 'x'))).toBe(false);
});
test('allows nested new dirs, write, chmod, utimes, rename, unlink, exists', async () => {
  const d = join(root, 'n', 'm');
  await g.mkdirp(d);
  const fh = await g.openForWrite(join(d, 'f.part'));
  await fh.writeFile('hi');
  await fh.close();
  await g.chmod(join(d, 'f.part'), 0o644);
  await g.utimesNs(join(d, 'f.part'), 1_500_000_000_123_000_000n);
  await g.rename(join(d, 'f.part'), join(d, 'f'));
  expect(readFileSync(join(d, 'f'), 'utf8')).toBe('hi');
  expect(await g.exists(join(d, 'f'))).toBe(true);
  expect(await g.exists(join(d, 'nope'))).toBe(false);
  await expect(g.openForWrite(join(d, 'f'))).rejects.toMatchObject({ code: 'EEXIST' });
  await g.unlink(join(d, 'f'));
  expect(await g.exists(join(d, 'f'))).toBe(false);
});
test('a non-existent allowed root is still usable once created; all calls go through FsOps', async () => {
  const calls: string[] = [];
  const ops: FsOps = {
    mkdir: async (p, o) => (calls.push('mkdir'), (await import('node:fs/promises')).mkdir(p, o)),
    open: async (p, f, m) => (calls.push('open'), (await import('node:fs/promises')).open(p, f, m)),
    chmod: async (p, m) => (calls.push('chmod'), (await import('node:fs/promises')).chmod(p, m)),
    utimes: async (p, a, m) => (
      calls.push('utimes'),
      (await import('node:fs/promises')).utimes(p, a, m)
    ),
    rename: async (a, b) => (calls.push('rename'), (await import('node:fs/promises')).rename(a, b)),
    unlink: async (p) => (calls.push('unlink'), (await import('node:fs/promises')).unlink(p)),
    lstat: async (p) => (calls.push('lstat'), (await import('node:fs/promises')).lstat(p)),
    realpath: async (p) => (calls.push('realpath'), (await import('node:fs/promises')).realpath(p)),
  };
  const g2 = new GuardedFs([root], ops);
  await g2.mkdirp(join(root, 'q'));
  const fh = await g2.openForWrite(join(root, 'q', 'a'));
  await fh.close();
  await g2.chmod(join(root, 'q', 'a'), 0o600);
  await g2.utimesNs(join(root, 'q', 'a'), 2_000_000_000n);
  await g2.rename(join(root, 'q', 'a'), join(root, 'q', 'b'));
  await g2.unlink(join(root, 'q', 'b'));
  await g2.exists(join(root, 'q'));
  for (const c of ['mkdir', 'open', 'chmod', 'utimes', 'rename', 'unlink', 'lstat', 'realpath'])
    expect(calls).toContain(c);
});
test('a dangling symlink inside the root is refused, not treated as missing', async () => {
  symlinkSync(join(outside, 'not-there'), join(root, 'dangling'));
  await expect(g.openForWrite(join(root, 'dangling', 'f'))).rejects.toBeInstanceOf(
    OutsideAllowedRootError,
  );
});
test('non-ENOENT errors propagate; exists rethrows them', async () => {
  const file = join(root, 'plainfile');
  writeFileSync(file, 'x');
  await expect(g.mkdirp(join(file, 'sub'))).rejects.toMatchObject({ code: 'ENOTDIR' });
  await expect(g.exists(join(file, 'sub'))).rejects.toMatchObject({ code: 'ENOTDIR' });
});
test('a root that does not exist anywhere under an existing ancestor is resolved by its tail', async () => {
  const g3 = new GuardedFs([join(base, 'later')]);
  await g3.mkdirp(join(base, 'later', 'a'));
  expect(existsSync(join(base, 'later', 'a'))).toBe(true);
});
