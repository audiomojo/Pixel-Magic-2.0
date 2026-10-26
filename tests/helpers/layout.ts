import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll } from 'vitest';

const created: string[] = [];

afterAll(async () => {
  await Promise.all(created.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

/** True when os.tmpdir() is on a case-insensitive volume (macOS default; not Linux). */
export const tmpIsCaseInsensitive: boolean = (() => {
  const d = mkdtempSync(join(tmpdir(), 'pm-case-'));
  try {
    writeFileSync(join(d, 'a'), '');
    return existsSync(join(d, 'A'));
  } finally {
    rmSync(d, { recursive: true, force: true });
  }
})();

/** Fresh temp directory under os.tmpdir(), removed after the test file finishes. */
export async function tmpDir(): Promise<string> {
  const d = await mkdtemp(join(tmpdir(), 'pm-test-'));
  created.push(d);
  return d;
}
