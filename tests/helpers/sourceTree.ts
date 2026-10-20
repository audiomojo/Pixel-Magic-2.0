import { execFile } from 'node:child_process';
import { chmod, lstat, mkdir, readdir, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { promisify } from 'node:util';
import { tmpDir } from './layout.js';

const run = promisify(execFile);

export type TreeSpec = Record<string, Buffer | number | 'dir' | { symlink: string }>;

/**
 * Builds a source tree in a fresh temp directory and returns its root. A key is a relative
 * path; the value is the file contents, a size (zero-filled file), 'dir', or a symlink target.
 */
export async function makeSourceTree(spec: TreeSpec): Promise<string> {
  const root = join(await tmpDir(), 'Drive');
  await mkdir(root, { recursive: true });
  for (const [rel, v] of Object.entries(spec)) {
    const p = join(root, rel);
    if (v === 'dir') {
      await mkdir(p, { recursive: true });
      continue;
    }
    await mkdir(dirname(p), { recursive: true });
    if (typeof v === 'object' && !Buffer.isBuffer(v)) await symlink(v.symlink, p);
    else await writeFile(p, typeof v === 'number' ? Buffer.alloc(v) : v);
  }
  return root;
}

/** One line per entry (path, type, size, mtime ns, ctime ns, mode), sorted: equal snapshots mean untouched. */
export async function snapshotTree(root: string): Promise<string[]> {
  const out: string[] = [];
  const walk = async (dir: string, rel: string): Promise<void> => {
    const s = await lstat(dir, { bigint: true });
    const type = s.isDirectory() ? 'd' : s.isSymbolicLink() ? 'l' : 'f';
    out.push(`${rel}|${type}|${s.size}|${s.mtimeNs}|${s.ctimeNs}|${s.mode.toString(8)}`);
    if (!s.isDirectory()) return;
    for (const name of await readdir(dir)) await walk(join(dir, name), `${rel}/${name}`);
  };
  await walk(root, '');
  return out.sort();
}

/** `chmod -R a-w`; resolves to a function that restores write access so the tree can be removed. */
export async function lockReadOnly(root: string): Promise<() => Promise<void>> {
  await run('chmod', ['-R', 'a-w', root]);
  return async () => {
    await run('chmod', ['-R', 'u+w', root]);
  };
}

/** Restores access to a directory made unreadable with chmod 000. */
export const restoreMode = (p: string): Promise<void> => chmod(p, 0o755);
