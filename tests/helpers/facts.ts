import { DEFAULT_THRESHOLDS, type Facts } from '../../src/classify.js';

export const T = DEFAULT_THRESHOLDS;
export const F = (sha: string, o: Partial<Facts> = {}): Facts => ({
  sha256: sha,
  phash: 0n,
  dhash: 0n,
  width: 4000,
  height: 3000,
  mime: 'image/heic',
  captured: { s: 1_600_000_000, sub: '123' },
  camera: 'Apple iPhone 12',
  burstUuid: null,
  ...o,
});
export const NOEXIF = { captured: null, camera: null } as const;
