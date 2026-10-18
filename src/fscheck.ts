import {
  link,
  mkdir,
  mkdtemp,
  readlink,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { join } from 'node:path';

/** Verifies hardlink, rename and symlink-swap semantics on the DATA_DIR filesystem. */
async function main(): Promise<void> {
  const dataDir = process.env['DATA_DIR'] ?? '/data';
  await mkdir(dataDir, { recursive: true });
  const tmp = await mkdtemp(join(dataDir, '.fs-check-'));
  try {
    const a = join(tmp, 'a');
    const b = join(tmp, 'b');
    const c = join(tmp, 'c');
    await writeFile(a, 'fs-check');
    await link(a, b);
    const [sa, sb] = await Promise.all([stat(a), stat(b)]);
    if (sa.ino !== sb.ino || sa.dev !== sb.dev || sa.nlink !== 2)
      throw new Error('link: inode not shared');
    await rename(b, c);
    if ((await stat(c)).ino !== sa.ino) throw new Error('rename: inode changed');

    // staging/current swap: symlink to .current.tmp then rename over the existing symlink
    await symlink('gen-1', join(tmp, 'current'));
    await symlink('gen-2', join(tmp, '.current.tmp'));
    await rename(join(tmp, '.current.tmp'), join(tmp, 'current'));
    if ((await readlink(join(tmp, 'current'))) !== 'gen-2') throw new Error('symlink swap failed');
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
  console.log('fs-check OK');
}

main().catch((err: unknown) => {
  console.error('fs-check FAILED:', err);
  process.exit(1);
});
