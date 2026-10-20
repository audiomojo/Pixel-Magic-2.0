// The harvester's own copy of the Node runtime (least privilege). macOS grants drive access per
// executable, so the agent runs a copy that nothing else uses: the Removable Volumes and Network
// Volumes grants go to it alone, not to the Node every other script runs. Homebrew's node loads
// libnode from @loader_path (then ../lib), so the libnode dylibs are copied next to it. Every
// write is confined to the runtime directory by a GuardedFs; this file runs no program.
import { readdir, realpath, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { sha256File } from '../fsops.js';
import { GuardedFs } from './paths.js';

/** The executable's name: what System Settings lists under Files & Folders. */
export const RUNTIME_NAME = 'pixel-magic-harvester';

export const defaultRuntimeDir = (): string =>
  join(homedir(), 'Library', 'Application Support', 'PixelMagic', 'harvester');

const isLibnode = (name: string) => /^libnode(\.\d+)*\.dylib$/.test(name);

/** Checks that the copy starts (cli.ts runs `<path> --version`); rejects when it cannot. */
export type RuntimeCheck = (path: string) => Promise<string>;

async function libnodes(dir: string): Promise<string[]> {
  const names = await readdir(dir).catch(() => [] as string[]);
  return names.filter(isLibnode).map((n) => join(dir, n));
}

const sameBytes = async (a: string, b: string): Promise<boolean> => {
  const [sa, sb] = await Promise.all([stat(a), stat(b).catch(() => null)]);
  if (!sb || sa.size !== sb.size) return false;
  return (await sha256File(a)) === (await sha256File(b));
};

/**
 * Copies the Node binary `node` (resolved through symlinks) and its libnode dylibs into `dir`,
 * the binary as RUNTIME_NAME. A file whose bytes already match is left alone, so a reinstall
 * keeps the grant; a changed one is replaced atomically (copy to `.part`, rename). Stale
 * libnode dylibs are removed. Returns the runtime's path and whether anything changed.
 */
export async function installRuntime(
  node: string,
  dir: string,
  check: RuntimeCheck,
): Promise<{ path: string; changed: boolean }> {
  const fs = new GuardedFs([dir]);
  await fs.mkdirp(dir);
  const real = await realpath(node);
  const here = dirname(real);
  const libs = new Map<string, string>(); // name in dir → source; @loader_path wins
  for (const l of [...(await libnodes(resolve(here, '..', 'lib'))), ...(await libnodes(here))])
    libs.set(basename(l), l);
  const files: [string, string, number][] = [
    [real, RUNTIME_NAME, 0o755],
    ...[...libs].map(([name, src]): [string, string, number] => [src, name, 0o644]),
  ];
  let changed = false;
  for (const [src, name, mode] of files) {
    const dest = join(dir, name);
    if (await sameBytes(src, dest)) continue;
    const part = `${dest}.part`;
    await fs.unlink(part).catch(() => undefined);
    const { mtimeNs } = await stat(src, { bigint: true });
    await fs.copyInto(src, part, { mode, mtimeNs });
    await fs.rename(part, dest);
    changed = true;
  }
  for (const old of await libnodes(dir))
    if (!libs.has(basename(old))) {
      await fs.unlink(old);
      changed = true;
    }
  const path = join(dir, RUNTIME_NAME);
  await check(path);
  return { path, changed };
}

/** Removes the runtime directory's files (uninstall). Missing files are fine. */
export async function removeRuntime(dir: string): Promise<void> {
  const fs = new GuardedFs([dir]);
  const names = await readdir(dir).catch(() => [] as string[]);
  for (const n of names)
    if (n === RUNTIME_NAME || isLibnode(n) || n.endsWith('.part')) await fs.unlink(join(dir, n));
}

/** Whether the runtime in `dir` still has the same bytes as `node` (status: out of date?). */
export async function runtimeMatches(node: string, dir: string): Promise<boolean> {
  try {
    return await sameBytes(await realpath(node), join(dir, RUNTIME_NAME));
  } catch {
    return false;
  }
}
