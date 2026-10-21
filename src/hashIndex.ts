import type { Facts, Thresholds } from './classify.js';
import { hi, lo, popcount32 } from './hashing.js';

const INITIAL_CAPACITY = 1024;

/**
 * The cluster worker's in-memory look-alike index (spec §6.3): every stored pHash and dHash as
 * hi/lo `Uint32Array` pairs (growing by doubling), scanned in full with XOR + 32-bit popcount.
 */
export class HashIndex {
  private n = 0;
  private pHi = new Uint32Array(INITIAL_CAPACITY);
  private pLo = new Uint32Array(INITIAL_CAPACITY);
  private dHi = new Uint32Array(INITIAL_CAPACITY);
  private dLo = new Uint32Array(INITIAL_CAPACITY);
  private facts: Facts[] = [];

  get size(): number {
    return this.n;
  }

  clear(): void {
    this.n = 0;
    this.facts = [];
  }

  append(rows: readonly Facts[]): void {
    this.reserve(this.n + rows.length);
    rows.forEach((f, i) => {
      const at = this.n + i;
      this.pHi[at] = hi(f.phash);
      this.pLo[at] = lo(f.phash);
      this.dHi[at] = hi(f.dhash);
      this.dLo[at] = lo(f.dhash);
      this.facts[at] = f;
    });
    this.n += rows.length;
  }

  /** Every stored row within both look-alike distances of `f`, in append order. */
  matches(f: Facts, t: Thresholds): Facts[] {
    const [ph, pl, dh, dl] = [hi(f.phash), lo(f.phash), hi(f.dhash), lo(f.dhash)];
    const out: Facts[] = [];
    for (let i = 0; i < this.n; i++) {
      // Indexed reads are in bounds (`!`): i < n ≤ capacity.
      if (popcount32(this.pHi[i]! ^ ph) + popcount32(this.pLo[i]! ^ pl) > t.lookAlikePhash)
        continue;
      if (popcount32(this.dHi[i]! ^ dh) + popcount32(this.dLo[i]! ^ dl) > t.lookAlikeDhash)
        continue;
      out.push(this.facts[i]!);
    }
    return out;
  }

  private reserve(need: number): void {
    let cap = this.pHi.length;
    if (need <= cap) return;
    while (cap < need) cap *= 2;
    const grow = (a: Uint32Array<ArrayBuffer>): Uint32Array<ArrayBuffer> => {
      const b = new Uint32Array(cap);
      b.set(a.subarray(0, this.n));
      return b;
    };
    this.pHi = grow(this.pHi);
    this.pLo = grow(this.pLo);
    this.dHi = grow(this.dHi);
    this.dLo = grow(this.dLo);
  }
}
