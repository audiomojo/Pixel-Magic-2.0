/** A file in the drop zone. `relPath` is relative to `dropzone/`; sizes and times are decimal strings. */
export interface FileRef {
  relPath: string;
  size: string;
  mtimeNs: string;
}

export interface IngestResult {
  fileId: string;
  sha256: string | null;
  ext: string | null;
  state: 'INGESTED' | 'COPY' | 'QUARANTINED' | 'BLOCKED';
  /** The owned content's kind; null for COPY (owns no content), QUARANTINED and BLOCKED. */
  kind: 'still' | 'motion' | 'other' | null;
}

/** `ImageFacts` as it crosses JSON (the decode pool's IPC, Temporal): hashes as unsigned decimal strings. */
export interface WireFacts {
  width: number;
  height: number;
  phash: string;
  dhash: string;
  mime: string;
}

/** `decode_content`'s outcome. Hashes are unsigned decimal strings (bigints can't cross Temporal JSON). */
export interface DecodeResult {
  facts: WireFacts | null;
  quarantineReason: string | null;
}

/** `maybeBuildStaging`'s outcome: the new generation, or `skipped` (then `gen` is null). */
export interface BuildResult {
  gen: number | null;
  entries: number;
  skipped: boolean;
}

/** `scanAndStart`'s outcome. `pendingBytes` is a decimal string (bigint can't cross Temporal JSON). */
export interface ScanResult {
  started: number;
  pendingFiles: number;
  pendingBytes: string;
}

/**
 * `ApplicationFailure.type` of a decode whose child process was killed by a signal (OOM, an
 * operator, a crash): transient, retried by the workflow up to its total decode-attempt cap. A
 * type (not a value) so workflow code can share it through a type-only import.
 */
export type DecodeKilledType = 'DecodeKilled';

/**
 * `reclaimCopy`'s outcome for one file (spec §6.5). `DEFERRED`: the `.copies` file has another
 * name too (an older staging generation), so it is left for a later sweep. `MISMATCH`: flagged,
 * nothing deleted, never retried.
 */
export type ReclaimOutcome = 'REMOVED' | 'ALREADY_REMOVED' | 'MISMATCH' | 'DEFERRED' | 'NOT_COPY';

/**
 * `sweepCopies`' outcome for one bounded batch (COPY rows, then pending retired identical
 * duplicates); `wrapped`: both passes reached their last row.
 */
export interface SweepResult {
  examined: number;
  removed: number;
  mismatched: number;
  deferred: number;
  wrapped: boolean;
}
