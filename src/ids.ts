import { createHash } from 'node:crypto';
import type { FileRef } from './types.js';

/** `file-<sha1(rel_path|size|mtime_ns)>` (spec §4.3): one id per observed drop of a file. */
export function fileWorkflowId(ref: FileRef): string {
  const key = `${ref.relPath}|${ref.size}|${ref.mtimeNs}`;
  return 'file-' + createHash('sha1').update(key, 'utf8').digest('hex');
}
