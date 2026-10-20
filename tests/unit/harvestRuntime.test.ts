import { mkdir, mkdtemp, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, expect, test } from 'vitest';
import {
  defaultRuntimeDir,
  installRuntime,
  removeRuntime,
  RUNTIME_NAME,
  runtimeMatches,
} from '../../src/harvest/runtime.js';
import { runVersion } from '../../src/harvest/cli.js';

let dir: string;
let checked: string[];
const check = (p: string) => (checked.push(p), Promise.resolve('v0'));

/** A Homebrew-shaped install: bin/node, lib/libnode.<n>.dylib. */
async function fakeNode(libs: Record<string, string> = { 'libnode.137.dylib': 'lib137' }) {
  const root = join(dir, 'Cellar', 'node');
  await mkdir(join(root, 'bin'), { recursive: true });
  await mkdir(join(root, 'lib'), { recursive: true });
  await writeFile(join(root, 'bin', 'node'), 'node-v1');
  for (const [n, v] of Object.entries(libs)) await writeFile(join(root, 'lib', n), v);
  return join(root, 'bin', 'node');
}

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'harvest-runtime-'));
  checked = [];
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

test('copies node as pixel-magic-harvester (0755) and libnode from ../lib, then checks it', async () => {
  const node = await fakeNode();
  const rt = join(dir, 'runtime');
  const r = await installRuntime(node, rt, check);
  expect(r).toEqual({ path: join(rt, RUNTIME_NAME), changed: true });
  expect((await readdir(rt)).sort()).toEqual(['libnode.137.dylib', RUNTIME_NAME]);
  expect((await stat(r.path)).mode & 0o777).toBe(0o755);
  expect(checked).toEqual([r.path]);
});

test('a reinstall with the same bytes leaves the files alone (the grant survives)', async () => {
  const node = await fakeNode();
  const rt = join(dir, 'runtime');
  await installRuntime(node, rt, check);
  const ino = (await stat(join(rt, RUNTIME_NAME))).ino;
  expect((await installRuntime(node, rt, check)).changed).toBe(false);
  expect((await stat(join(rt, RUNTIME_NAME))).ino).toBe(ino);
});

test('a new Node replaces the copy and drops the old libnode', async () => {
  const node = await fakeNode();
  const rt = join(dir, 'runtime');
  await installRuntime(node, rt, check);
  await rm(join(dir, 'Cellar'), { recursive: true });
  const node2 = await fakeNode({ 'libnode.141.dylib': 'lib141' });
  await writeFile(node2, 'node-v2');
  expect((await installRuntime(node2, rt, check)).changed).toBe(true);
  expect((await readdir(rt)).sort()).toEqual(['libnode.141.dylib', RUNTIME_NAME]);
  expect(await runtimeMatches(node2, rt)).toBe(true);
});

test('follows a symlinked node; a libnode next to the binary wins over ../lib', async () => {
  const node = await fakeNode();
  await writeFile(join(dir, 'Cellar', 'node', 'bin', 'libnode.137.dylib'), 'beside');
  const link = join(dir, 'node-link');
  await symlink(node, link);
  const rt = join(dir, 'runtime');
  await installRuntime(link, rt, check);
  expect(await runtimeMatches(link, rt)).toBe(true);
  const { readFile } = await import('node:fs/promises');
  expect(await readFile(join(rt, 'libnode.137.dylib'), 'utf8')).toBe('beside');
});

test('a copy that does not start fails the install', async () => {
  const node = await fakeNode();
  await expect(
    installRuntime(node, join(dir, 'runtime'), () => Promise.reject(new Error('no dylib'))),
  ).rejects.toThrow('no dylib');
});

test('removeRuntime removes only the runtime files; a missing directory is fine', async () => {
  const node = await fakeNode();
  const rt = join(dir, 'runtime');
  await installRuntime(node, rt, check);
  await writeFile(join(rt, 'keep.txt'), 'x');
  await removeRuntime(rt);
  expect(await readdir(rt)).toEqual(['keep.txt']);
  await removeRuntime(join(dir, 'nowhere'));
});

test('runtimeMatches is false for different bytes or no runtime', async () => {
  const node = await fakeNode();
  const rt = join(dir, 'runtime');
  expect(await runtimeMatches(node, rt)).toBe(false);
  await installRuntime(node, rt, check);
  await writeFile(node, 'node-v1-patched');
  expect(await runtimeMatches(node, rt)).toBe(false);
});

test('the default directory is under Application Support', () => {
  expect(defaultRuntimeDir()).toMatch(/Library\/Application Support\/PixelMagic\/harvester$/);
});

test('a copy of this Node starts (real binary, real libnode)', async () => {
  const rt = join(dir, 'runtime');
  const r = await installRuntime(process.execPath, rt, runVersion);
  expect(await runVersion(r.path)).toBe(process.version);
}, 120_000);

test('runVersion rejects a binary that cannot start', async () => {
  const bad = join(dir, 'bad');
  await writeFile(bad, 'not a program', { mode: 0o755 });
  await expect(runVersion(bad)).rejects.toThrow('does not start');
});
