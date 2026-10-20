import { execFile } from 'node:child_process';
import { statfs } from 'node:fs/promises';
import type { SystemProbe } from './sources.js';

const DISKUTIL_TIMEOUT_MS = 15_000;

function run(
  cmd: string,
  args: string[],
  input?: string,
  timeout = DISKUTIL_TIMEOUT_MS,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = execFile(cmd, args, { timeout, maxBuffer: 16 * 1024 * 1024 }, (err, stdout) =>
      err ? reject(new Error(err.message)) : resolve(stdout),
    );
    if (input !== undefined) child.stdin?.end(input);
  });
}

/** The real macOS probe: thin by design, exercised by the host and the e2e tests only. */
export const realSystem: SystemProbe = {
  mountOutput: () => run('/sbin/mount', [], undefined, 30_000),

  /** `diskutil info -plist` converted to JSON by plutil; null on any failure or timeout. */
  async diskutilInfo(device) {
    try {
      const plist = await run('/usr/sbin/diskutil', ['info', '-plist', device]);
      const json = await run('/usr/bin/plutil', ['-convert', 'json', '-o', '-', '-'], plist);
      const info: unknown = JSON.parse(json);
      return typeof info === 'object' && info !== null && !Array.isArray(info)
        ? (info as Record<string, unknown>)
        : null;
    } catch {
      return null;
    }
  },

  async statfs(path) {
    const s = await statfs(path, { bigint: true });
    return { blocks: s.blocks, bavail: s.bavail, bsize: s.bsize };
  },
};
