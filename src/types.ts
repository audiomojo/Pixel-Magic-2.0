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
  state: 'INGESTED' | 'COPY' | 'QUARANTINED';
  /** This file owns the vault inode (it inserted, or may insert, the `contents` row). */
  isNew: boolean;
  /** The owned content's kind; null for COPY (owns no content) and QUARANTINED. */
  kind: 'still' | 'motion' | 'other' | null;
}

/** `decode_content`'s outcome. Hashes are unsigned decimal strings (bigints can't cross Temporal JSON). */
export interface DecodeResult {
  facts: { width: number; height: number; phash: string; dhash: string; mime: string } | null;
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
