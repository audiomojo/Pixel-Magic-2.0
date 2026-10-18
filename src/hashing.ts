const MASK32 = 0xffff_ffffn;

/** Population count of a 32-bit unsigned integer (SWAR). */
export function popcount32(x: number): number {
  x = x - ((x >>> 1) & 0x5555_5555);
  x = (x & 0x3333_3333) + ((x >>> 2) & 0x3333_3333);
  return Math.imul((x + (x >>> 4)) & 0x0f0f_0f0f, 0x0101_0101) >>> 24;
}

/** Hamming distance between two unsigned 64-bit hashes (popcount over the two 32-bit halves). */
export function hamming(a: bigint, b: bigint): number {
  const x = a ^ b;
  return popcount32(Number(x & MASK32)) + popcount32(Number((x >> 32n) & MASK32));
}

/** Unsigned 64-bit hash → signed int64 (the Postgres `bigint` representation). */
export const toSigned = (u: bigint): bigint => BigInt.asIntN(64, u);

/** Signed int64 from Postgres → unsigned 64-bit hash. */
export const toUnsigned = (s: bigint): bigint => BigInt.asUintN(64, s);

const N = 32;
const K = 8;

/** Orthonormal DCT-II basis, first K rows: DCT[k][n] = c(k)·cos(π(2n+1)k / 2N). */
const DCT: Float64Array[] = Array.from({ length: K }, (_, k) =>
  Float64Array.from(
    { length: N },
    (_, n) => Math.sqrt((k === 0 ? 1 : 2) / N) * Math.cos((Math.PI * (2 * n + 1) * k) / (2 * N)),
  ),
);

function bitsToHash(bits: boolean[]): bigint {
  let h = 0n;
  for (const b of bits) h = (h << 1n) | (b ? 1n : 0n);
  return h;
}

// Indexed reads below are in bounds (`!`): each hash checks the raster length first.

/**
 * pHash of a 32×32 row-major greyscale raster: 2-D DCT-II, keep the top-left 8×8
 * coefficients, bit = coefficient > their median. Row-major, first bit = MSB.
 */
export function phashFromGray(px: Uint8Array): bigint {
  if (px.length !== N * N) throw new Error(`phashFromGray needs ${N * N} pixels, got ${px.length}`);
  // rows[v][x] = Σ_y DCT[v][y]·px[y][x]  (vertical pass, 8×32)
  const rows = DCT.map((basis) => {
    const r = new Float64Array(N);
    for (let y = 0; y < N; y++) {
      const w = basis[y]!;
      for (let x = 0; x < N; x++) r[x]! += w * px[y * N + x]!;
    }
    return r;
  });
  // coef[v][u] = Σ_x rows[v][x]·DCT[u][x]  (horizontal pass, 8×8)
  const coef = rows.flatMap((r) => DCT.map((basis) => basis.reduce((s, w, x) => s + w * r[x]!, 0)));
  const sorted = [...coef].sort((a, b) => a - b);
  const median = (sorted[31]! + sorted[32]!) / 2;
  return bitsToHash(coef.map((c) => c > median));
}

/** dHash of a 9×8 row-major greyscale raster: bit = px[x+1] > px[x]. Row-major, first bit = MSB. */
export function dhashFromGray(px: Uint8Array): bigint {
  if (px.length !== 9 * 8) throw new Error(`dhashFromGray needs 72 pixels, got ${px.length}`);
  const bits: boolean[] = [];
  for (let y = 0; y < 8; y++)
    for (let x = 0; x < 8; x++) bits.push(px[y * 9 + x + 1]! > px[y * 9 + x]!);
  return bitsToHash(bits);
}
