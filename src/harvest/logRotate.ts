// Size-based rotation of the harvester's own log (spec §3). These writes are the harvester's
// log files under DATA_DIR/logs, not a source and not the drop zone, so they bypass GuardedFs:
// tests/unit/harvestSend.test.ts allow-lists this file for that reason.
import { rename, rm, stat } from 'node:fs/promises';

/**
 * If `path` is larger than `maxBytes`, shifts `path.1` … `path.<keep-1>` up by one (dropping
 * `path.<keep>`) and renames `path` to `path.1`. Returns whether it rotated. The caller reopens
 * its destination afterwards.
 */
export async function rotateIfLarge(
  path: string,
  maxBytes: number,
  keep: number,
): Promise<boolean> {
  const size = await stat(path).then(
    (s) => s.size,
    () => 0,
  );
  if (size <= maxBytes) return false;
  await rm(`${path}.${keep}`, { force: true });
  for (let i = keep - 1; i >= 1; i--) {
    await rename(`${path}.${i}`, `${path}.${i + 1}`).catch((e: NodeJS.ErrnoException) => {
      if (e.code !== 'ENOENT') throw e;
    });
  }
  await rename(path, `${path}.1`);
  return true;
}
