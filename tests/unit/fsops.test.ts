import { createHash } from 'node:crypto';
import { link, stat, utimes, writeFile } from 'node:fs/promises';
import { describe, expect, test } from 'vitest';
import {
  DataLayout,
  SameDeviceError,
  checkSameDevice,
  isIgnored,
  linkIntoVault,
  sameFile,
  sha256File,
  vaultRel,
} from '../../src/fsops.js';
import { tmpDir } from '../helpers/layout.js';

test('vaultRel', () =>
  expect(vaultRel('abcd' + '0'.repeat(60), 'heic')).toBe(`vault/ab/cd/abcd${'0'.repeat(60)}.heic`));

test.each([
  ['.DS_Store', true],
  ['._IMG_1.HEIC', true],
  ['a.part', true],
  ['a.crdownload', true],
  ['~$lock', true],
  ['IMG_2834 1.HEIC', false],
  ['café.jpg', false],
])('isIgnored(%s)', (n, want) => expect(isIgnored(n)).toBe(want));

test('linkIntoVault outcomes', async () => {
  const d = await tmpDir();
  await writeFile(`${d}/a`, '1');
  await writeFile(`${d}/b`, '1');
  const dest = `${d}/v/ab/cd/x.jpg`;
  expect(await linkIntoVault(`${d}/a`, dest)).toBe('LINKED');
  expect(await linkIntoVault(`${d}/a`, dest)).toBe('ALREADY_LINKED');
  expect(await linkIntoVault(`${d}/b`, dest)).toBe('EXISTS_OTHER');
  expect((await stat(`${d}/a`)).ino).toBe((await stat(dest)).ino);
});

test('linkIntoVault rethrows non-EEXIST errors', async () => {
  const d = await tmpDir();
  await expect(linkIntoVault(`${d}/missing`, `${d}/v/x`)).rejects.toThrow(/ENOENT/);
});

test('sha256File streams and calls onChunk per MiB', async () => {
  const p = `${await tmpDir()}/f`;
  const buf = Buffer.alloc(3 << 20, 'x');
  await writeFile(p, buf);
  let calls = 0;
  expect(await sha256File(p, () => calls++)).toBe(createHash('sha256').update(buf).digest('hex'));
  expect(calls).toBe(3);
});

test('sha256File works without onChunk', async () => {
  const p = `${await tmpDir()}/f`;
  await writeFile(p, 'abc');
  expect(await sha256File(p)).toBe(createHash('sha256').update('abc').digest('hex'));
});

test('DataLayout paths, ensure and rel', async () => {
  const root = await tmpDir();
  const l = new DataLayout(root);
  await l.ensure();
  expect(l.dropzone).toBe(`${root}/dropzone`);
  expect(l.claimed).toBe(`${root}/work/claimed`);
  expect(l.vault).toBe(`${root}/vault`);
  expect(l.vaultCopies).toBe(`${root}/vault/.copies`);
  expect(l.staging).toBe(`${root}/staging`);
  for (const p of [l.dropzone, l.claimed, l.vault, l.vaultCopies, l.staging]) {
    expect((await stat(p)).isDirectory()).toBe(true);
  }
  expect(l.rel(`${root}/vault/ab/x.jpg`)).toBe('vault/ab/x.jpg');
});

test('checkSameDevice passes on one fs', async () => {
  const l = new DataLayout(await tmpDir());
  await l.ensure();
  await expect(checkSameDevice(l)).resolves.toBeUndefined();
});

test('checkSameDevice throws SameDeviceError when a dir is on another device', async () => {
  const l = new DataLayout(await tmpDir());
  await l.ensure();
  const fake = async (p: string) => {
    const st = await stat(p);
    return { dev: p === l.staging ? st.dev + 1 : st.dev };
  };
  await expect(checkSameDevice(l, fake)).rejects.toBeInstanceOf(SameDeviceError);
  await expect(checkSameDevice(l, fake)).rejects.toThrow(/staging/);
});

describe('sameFile', () => {
  const sha = (body: string) => createHash('sha256').update(body).digest('hex');

  test('hardlinks of one inode', async () => {
    const d = await tmpDir();
    await writeFile(`${d}/a`, 'bytes');
    await link(`${d}/a`, `${d}/b`);
    expect(await sameFile(`${d}/a`, `${d}/b`, sha('bytes'))).toBe(true);
  });

  test('a lone file with equal bytes is not the same file', async () => {
    const d = await tmpDir();
    await writeFile(`${d}/a`, 'bytes');
    await writeFile(`${d}/b`, 'bytes');
    for (const f of ['a', 'b']) await utimes(`${d}/${f}`, 1_700_000_000, 1_700_000_000);
    expect(await sameFile(`${d}/a`, `${d}/b`, sha('bytes'))).toBe(false);
  });

  // Docker Desktop's virtiofs renumbers inodes when its cache evicts them, so two names of one
  // file can report different inode numbers. Multiply-linked, equal size and mtime, bytes hashing
  // to the expected sha256: the same file.
  test('renumbered inode: multiply-linked twins with the expected bytes', async () => {
    const d = await tmpDir();
    await writeFile(`${d}/a`, 'bytes');
    await writeFile(`${d}/b`, 'bytes');
    await link(`${d}/a`, `${d}/a2`);
    await link(`${d}/b`, `${d}/b2`);
    for (const f of ['a', 'b']) await utimes(`${d}/${f}`, 1_700_000_000, 1_700_000_000);
    expect(await sameFile(`${d}/a`, `${d}/b`, sha('bytes'))).toBe(true);
    expect(await sameFile(`${d}/a`, `${d}/b`, sha('other'))).toBe(false);
    expect(await sameFile(`${d}/a`, `${d}/b`, null)).toBe(false);
    await utimes(`${d}/b`, 1_700_000_000, 1_700_000_005);
    expect(await sameFile(`${d}/a`, `${d}/b`, sha('bytes'))).toBe(false);
  });

  test('a missing path is never the same file', async () => {
    const d = await tmpDir();
    await writeFile(`${d}/a`, 'bytes');
    expect(await sameFile(`${d}/a`, `${d}/nope`, sha('bytes'))).toBe(false);
  });
});
