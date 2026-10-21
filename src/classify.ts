import { hamming } from './hashing.js';

/** How two look-alike contents relate: a copy of one picture, or merely similar. */
export type Verdict = 'copy' | 'similar';

/** Capture instant at whole-second precision (`s`, Unix seconds) plus the EXIF sub-second string. */
export type CaptureKey = { s: number; sub: string | null };

/** What the dedupe needs to know about one still content. Hashes are unsigned 64-bit. */
export interface Facts {
  sha256: string;
  phash: bigint;
  dhash: bigint;
  width: number;
  height: number;
  mime: string;
  captured: CaptureKey | null;
  camera: string | null;
  burstUuid: string | null;
}

export interface Thresholds {
  lookAlikePhash: number;
  lookAlikeDhash: number;
  copyPhash: number;
  copyDhash: number;
}

export const DEFAULT_THRESHOLDS: Readonly<Thresholds> = Object.freeze({
  lookAlikePhash: 8,
  lookAlikeDhash: 10,
  copyPhash: 4,
  copyDhash: 6,
});

/** Candidate filter: both hashes within the look-alike distances. */
export function looksAlike(a: Facts, b: Facts, t: Thresholds): boolean {
  return (
    hamming(a.phash, b.phash) <= t.lookAlikePhash && hamming(a.dhash, b.dhash) <= t.lookAlikeDhash
  );
}

/** True if the two keys cannot be the same shot: different seconds, or two known, different subsecs. */
export function captureConflict(a: CaptureKey, b: CaptureKey): boolean {
  return a.s !== b.s || (a.sub !== null && b.sub !== null && a.sub !== b.sub);
}

const bothDiffer = (x: string | null, y: string | null): boolean =>
  x !== null && y !== null && x !== y;

const differentEncoding = (a: Facts, b: Facts): boolean =>
  a.width !== b.width || a.height !== b.height || a.mime !== b.mime;

/**
 * Spec §6.3 table, first match wins, for a pair that already `looksAlike`.
 * Only a provable re-encoding of the same shot is a copy; anything uncertain is similar.
 */
export function classify(a: Facts, b: Facts, t: Thresholds): Verdict {
  if (a.burstUuid !== null && a.burstUuid === b.burstUuid) return 'similar';
  const ca = a.captured;
  const cb = b.captured;
  if (ca !== null && cb !== null && captureConflict(ca, cb)) return 'similar';
  if (bothDiffer(a.camera, b.camera)) return 'similar';
  const tight =
    hamming(a.phash, b.phash) <= t.copyPhash && hamming(a.dhash, b.dhash) <= t.copyDhash;
  if (ca !== null && cb !== null) {
    if (ca.sub !== null && cb.sub !== null) return 'copy';
    // Same second without subsecs: two burst frames look exactly like this, so a re-encoding
    // must also be within the copy distances (spec §6.3 row 6).
    return tight && differentEncoding(a, b) ? 'copy' : 'similar';
  }
  if (!tight) return 'similar';
  if (ca !== null || cb !== null) return 'copy';
  return differentEncoding(a, b) ? 'copy' : 'similar';
}
