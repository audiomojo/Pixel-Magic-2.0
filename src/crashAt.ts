// Test seam for the chaos tier: `CRASH_AT=<checkpoint>` makes a worker exit hard the first time
// any worker on this data dir reaches that named checkpoint (e.g. `identical_retired`, between
// the retire commit and the vault unlink). Unset in production: no checkpoint at all.
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';

/**
 * The `Deps.checkpoint` for `name`, or undefined when `name` is unset. The marker file
 * `<dataDir>/.crash-at-<name>` (created exclusively before exiting) makes it fire once per data
 * dir, so the restarted worker, and any other, passes the checkpoint and the run can finish.
 * The exit is `process.exit` (no graceful shutdown): a signal to the container's PID 1 would
 * be ignored.
 */
export function crashAt(
  name: string | undefined,
  dataDir: string,
  exit: () => void = () => process.exit(137),
): ((checkpoint: string) => Promise<void>) | undefined {
  if (name === undefined || name === '') return undefined;
  const marker = join(dataDir, `.crash-at-${name}`);
  return async (checkpoint) => {
    if (checkpoint !== name) return;
    try {
      await writeFile(marker, `${new Date().toISOString()}\n`, { flag: 'wx' });
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'EEXIST') return;
      throw e;
    }
    process.stderr.write(`CRASH_AT: exiting hard at checkpoint ${name}\n`);
    exit();
  };
}
