// Size-based rotation of the harvester's own log (spec §3). Renames and removals go through a
// GuardedFs confined to the log directory (DATA_DIR/logs), like every other harvester write.
import { stat } from 'node:fs/promises';
import { dirname } from 'node:path';
import { GuardedFs } from './paths.js';

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
  const fs = new GuardedFs([dirname(path)]);
  const size = await stat(path).then(
    (s) => s.size,
    () => 0,
  );
  if (size <= maxBytes) return false;
  const quiet = (e: NodeJS.ErrnoException): void => {
    if (e.code !== 'ENOENT') throw e;
  };
  await fs.unlink(`${path}.${keep}`).catch(quiet);
  for (let i = keep - 1; i >= 1; i--)
    await fs.rename(`${path}.${i}`, `${path}.${i + 1}`).catch(quiet);
  await fs.rename(path, `${path}.1`);
  return true;
}
