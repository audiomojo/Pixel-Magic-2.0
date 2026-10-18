const MASK32 = 0xffff_ffffn;

/** Population count of a 32-bit unsigned integer (SWAR). */
function popcount32(x: number): number {
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
