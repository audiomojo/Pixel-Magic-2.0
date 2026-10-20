import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
  existsSync,
  lstatSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, describe, expect, test } from 'vitest';
import {
  GuardedFs,
  OutsideAllowedRootError,
  dropzoneName,
  partPath,
  pathKey,
  realFsOps,
  sameMtime,
  sourcePath,
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
  await expect(g.mkdirp(`${root}/../root-sibling/x`)).rejects.toBeInstanceOf(
    OutsideAllowedRootError,
  );
  await expect(g.mkdirp(`${root}/link/../x`)).rejects.toBeInstanceOf(OutsideAllowedRootError);
  expect(existsSync(join(base, 'x'))).toBe(false);
  expect(existsSync(join(root, 'x'))).toBe(false);
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
  const want = 1_700_000_000_123_456_789n;
  const got = await g.utimesNs(join(d, 'f.part'), want);
  const disk = lstatSync(join(d, 'f.part'), { bigint: true }).mtimeNs;
  expect(got).toBe(disk);
  expect(got > want ? got - want : want - got).toBeLessThan(1000n);
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
    mtimeNs: async (p) => (
      calls.push('mtimeNs'),
      (await import('node:fs/promises')).lstat(p, { bigint: true }).then((s) => s.mtimeNs)
    ),
    realpath: async (p) => (calls.push('realpath'), (await import('node:fs/promises')).realpath(p)),
    openRead: (p) => (calls.push('openRead'), realFsOps.openRead(p)),
    link: (a, b) => (calls.push('link'), realFsOps.link(a, b)),
    stat: (p) => (calls.push('stat'), realFsOps.stat(p)),
  };
  const g2 = new GuardedFs([root], ops);
  await g2.mkdirp(join(root, 'q'));
  const fh = await g2.openForWrite(join(root, 'q', 'a'));
  await fh.close();
  await g2.chmod(join(root, 'q', 'a'), 0o600);
  await g2.utimesNs(join(root, 'q', 'a'), 2_000_000_000n);
  expect(calls).toContain('mtimeNs');
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

describe('copyInto and publishNoReplace', () => {
  const src = join(outside, 'src.bin');
  const bytes = Buffer.from('0123456789'.repeat(300_000)); // 3 MB: several 1 MiB chunks
  writeFileSync(src, bytes);
  const MT = 1_700_000_000_123_456_789n;

  test('a byte copy reads read-only, sets mode and mtime on the handle, never replaces', async () => {
    const seen: number[] = [];
    const calls: string[] = [];
    const ops: FsOps = {
      ...realFsOps,
      openRead: (p) => (calls.push(`openRead ${String(p)}`), realFsOps.openRead(p)),
      open: async (p, f, m) => {
        const fh = await realFsOps.open(p, f, m);
        for (const k of ['chmod', 'utimes', 'sync', 'close'] as const) {
          const orig = (fh[k] as (...a: unknown[]) => Promise<void>).bind(fh);
          (fh as unknown as Record<string, unknown>)[k] = (...a: unknown[]) => (
            calls.push(`fh.${k}`),
            orig(...a)
          );
        }
        return fh;
      },
      chmod: () => Promise.reject(new Error('path chmod not used')),
      utimes: () => Promise.reject(new Error('path utimes not used')),
    };
    const dest = join(root, 'c1.part');
    const got = await new GuardedFs([root], ops).copyInto(src, dest, {
      mode: 0o640,
      mtimeNs: MT,
      onChunk: (n) => seen.push(n),
    });
    // Buffer.equals: a deep toEqual over 3 MB walks every byte (seconds under coverage).
    expect(readFileSync(dest).equals(bytes)).toBe(true);
    expect(seen).toEqual([1048576, 2097152, 3000000]);
    const st = lstatSync(dest, { bigint: true });
    expect(got).toBe(st.mtimeNs);
    expect(got > MT ? got - MT : MT - got).toBeLessThan(1000n);
    expect(Number(st.mode) & 0o777).toBe(0o640);
    expect(calls).toEqual([`openRead ${src}`, 'fh.chmod', 'fh.utimes', 'fh.sync', 'fh.close']);
    await expect(g.copyInto(src, dest, { mode: 0o600, mtimeNs: MT })).rejects.toMatchObject({
      code: 'EEXIST',
    });
    await expect(
      g.copyInto(src, join(outside, 'x'), { mode: 0o600, mtimeNs: MT }),
    ).rejects.toBeInstanceOf(OutsideAllowedRootError);
    expect(existsSync(join(outside, 'x'))).toBe(false);
  });

  test('copyInto reads a source given as raw bytes', async () => {
    const dest = join(root, 'c2.part');
    await g.copyInto(Buffer.from(src), dest, { mode: 0o600, mtimeNs: MT });
    // Buffer.equals: a deep toEqual over 3 MB walks every byte (seconds under coverage).
    expect(readFileSync(dest).equals(bytes)).toBe(true);
  });

  test('sourcePath joins the mount and the raw bytes exactly', () => {
    const raw = Buffer.concat([
      Buffer.from('d/bad'),
      Buffer.from([0xff, 0xfe]),
      Buffer.from('.cr2'),
    ]);
    expect(sourcePath('/Volumes/X', 'd/bad\uFFFD\uFFFD.cr2', raw)).toEqual(
      Buffer.concat([Buffer.from('/Volumes/X/'), raw]),
    );
    expect(sourcePath('/Volumes/X/', 'a', Buffer.from('a'))).toEqual(Buffer.from('/Volumes/X/a'));
    expect(sourcePath('/Volumes/X', 'd/a.cr2', null)).toBe('/Volumes/X/d/a.cr2');
  });

  test('publishNoReplace links, fsyncs the directory, unlinks; never replaces', async () => {
    const part = join(root, '.p.h1.part');
    writeFileSync(part, 'new');
    writeFileSync(join(root, 'taken'), 'old');
    await expect(g.publishNoReplace(part, join(root, 'taken'))).rejects.toMatchObject({
      code: 'EEXIST',
    });
    expect(readFileSync(join(root, 'taken'), 'utf8')).toBe('old');
    expect(readFileSync(part, 'utf8')).toBe('new');
    const synced: string[] = [];
    const ops: FsOps = {
      ...realFsOps,
      openRead: async (p) => {
        const fh = await realFsOps.openRead(p);
        const sync = fh.sync.bind(fh);
        fh.sync = () => (synced.push(String(p)), sync());
        return fh;
      },
    };
    expect(await new GuardedFs([root], ops).publishNoReplace(part, join(root, 'free'))).toBe(true);
    expect(synced).toEqual([root]);
    expect(readFileSync(join(root, 'free'), 'utf8')).toBe('new');
    expect(existsSync(part)).toBe(false);
    await expect(g.publishNoReplace(join(root, 'free'), join(outside, 'f'))).rejects.toBeInstanceOf(
      OutsideAllowedRootError,
    );
  });

  test('publishNoReplace: a failed unlink after the link still means published', async () => {
    const part = join(root, '.q2.h2.part');
    writeFileSync(part, 'q');
    const ops: FsOps = { ...realFsOps, unlink: () => Promise.reject(new Error('busy')) };
    expect(await new GuardedFs([root], ops).publishNoReplace(part, join(root, 'q2'))).toBe(false);
    expect(readFileSync(join(root, 'q2'), 'utf8')).toBe('q');
  });
});
