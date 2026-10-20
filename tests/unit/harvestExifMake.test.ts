import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import fc from 'fast-check';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { parseMake, readMake } from '../../src/harvest/exifMake.js';
import {
  heicIlocBomb,
  heicWithBoxSize,
  heicWithMake,
  jpegHugeIfdCount,
  jpegTruncatedApp1,
  jpegWithMake,
  tiffWithMake,
} from '../helpers/exifFixtures.js';

const HEAD = 256 * 1024;

/** Run parseMake over a whole in-memory file, as readMake would, recording readAt calls. */
async function run(file: Buffer) {
  const calls: Array<[number, number]> = [];
  const readAt = (offset: number, length: number): Promise<Buffer> => {
    calls.push([offset, length]);
    return Promise.resolve(file.subarray(offset, offset + length));
  };
  const make = await parseMake(file.subarray(0, HEAD), file.length, readAt);
  return { make, calls };
}

describe('parseMake', () => {
  it('reads a JPEG Make, padding stripped', async () => {
    expect((await run(jpegWithMake('Canon'))).make).toBe('Canon');
    expect((await run(jpegWithMake('  Canon  '))).make).toBe('Canon');
  });

  it('reads TIFF in both byte orders, inline and out-of-line values', async () => {
    expect((await run(tiffWithMake('Nikon Corp', { endian: 'be' }))).make).toBe('Nikon Corp');
    expect((await run(tiffWithMake('Fuji'))).make).toBe('Fuji');
    expect((await run(tiffWithMake('Sony'))).make).toBe('Sony');
  });

  it('reads a TIFF with IFD0 beyond the head by one capped seek', async () => {
    const { make, calls } = await run(tiffWithMake('Pentax Corp', { farIfd0: true }));
    expect(make).toBe('Pentax Corp');
    expect(calls).toHaveLength(1);
    expect(calls[0]![1]).toBeLessThanOrEqual(64 * 1024);
  });

  it('fetches a Make value outside the IFD chunk with a second small read', async () => {
    const { make, calls } = await run(
      tiffWithMake('Hasselblad', { farIfd0: true, valuePad: 70 * 1024 }),
    );
    expect(make).toBe('Hasselblad');
    expect(calls).toHaveLength(2);
    expect(calls[1]![1]).toBeLessThanOrEqual(4 * 1024);
  });

  it('reads HEIC Make across box versions and construction methods', async () => {
    expect((await run(heicWithMake('Apple'))).make).toBe('Apple');
    expect(
      (await run(heicWithMake('Apple', { iinfVersion: 1, infeVersion: 3, ilocVersion: 2 }))).make,
    ).toBe('Apple');
    expect((await run(heicWithMake('Apple', { ilocVersion: 1 }))).make).toBe('Apple');
    expect((await run(heicWithMake('Apple', { ilocVersion: 1, constructionMethod: 1 }))).make).toBe(
      'Apple',
    );
    expect(
      (await run(heicWithMake('Apple', { ilocVersion: 1, constructionMethod: 2 }))).make,
    ).toBeNull();
  });

  it('returns null without an Exif block or Make', async () => {
    expect((await run(jpegWithMake(null))).make).toBeNull();
    expect((await run(heicWithMake('Apple', { withExif: false }))).make).toBeNull();
    expect((await run(Buffer.from('not an image at all'))).make).toBeNull();
    expect((await run(Buffer.alloc(0))).make).toBeNull();
    expect((await run(tiffWithMake(''))).make).toBeNull();
    expect((await run(tiffWithMake('   '))).make).toBeNull();
  });

  it('treats malformed TIFF as null', async () => {
    const bad = (mut: (b: Buffer) => void): Promise<string | null> => {
      const b = Buffer.from(tiffWithMake('Canon Inc.'));
      mut(b);
      return run(b).then((r) => r.make);
    };
    expect(await bad((b) => b.writeUInt16LE(41, 2))).toBeNull(); // magic
    expect(await bad((b) => b.writeUInt32LE(0x7fffffff, 4))).toBeNull(); // IFD0 past EOF
    expect(await bad((b) => b.writeUInt32LE(1 << 30, 8 + 2 + 12 + 4 + 4))).toBeNull(); // count huge
    expect(await bad((b) => b.writeUInt16LE(3, 8 + 2 + 12 + 2))).toBeNull(); // Make not ASCII
    expect(await bad((b) => b.writeUInt32LE(0x7fffffff, 8 + 2 + 12 + 8))).toBeNull(); // value offset past EOF
    expect(await bad((b) => b.writeUInt16LE(1, 8))).toBeNull(); // Make beyond the claimed entries
  });

  it('is null when a seek read comes back short', async () => {
    const file = tiffWithMake('Pentax', { farIfd0: true });
    const make = await parseMake(file.subarray(0, HEAD), file.length, () =>
      Promise.resolve(Buffer.alloc(3)),
    );
    expect(make).toBeNull();
    const far = tiffWithMake('Hasselblad', { farIfd0: true, valuePad: 70 * 1024 });
    let n = 0;
    const m2 = await parseMake(far.subarray(0, HEAD), far.length, (o, l) =>
      Promise.resolve(++n === 1 ? far.subarray(o, o + l) : Buffer.alloc(1)),
    );
    expect(m2).toBeNull();
  });

  it('propagates readAt failures', async () => {
    const file = tiffWithMake('Pentax', { farIfd0: true });
    await expect(
      parseMake(file.subarray(0, HEAD), file.length, () =>
        Promise.reject(Object.assign(new Error('boom'), { code: 'EIO' })),
      ),
    ).rejects.toThrow('boom');
  });

  describe('malformed input is null, and fast', () => {
    const cases: Array<[string, () => Buffer]> = [
      ['truncated APP1', jpegTruncatedApp1],
      ['IFD entry count 65535 past the buffer', jpegHugeIfdCount],
      ['iloc extent count huge', () => heicIlocBomb()],
      ['box size 0', () => heicWithBoxSize(0)],
      ['box size 1', () => heicWithBoxSize(1)],
      ['box size past EOF', () => heicWithBoxSize(0x7fffffff)],
    ];
    for (const [name, build] of cases) {
      it(name, async () => {
        const t0 = performance.now();
        const { make } = await run(build());
        expect(make).toBeNull();
        expect(performance.now() - t0).toBeLessThan(50);
      });
    }
  });

  it('treats a truncated head as malformed', async () => {
    for (const f of [jpegWithMake('Canon'), heicWithMake('Apple'), tiffWithMake('Canon Inc.')]) {
      for (let cut = 0; cut < f.length; cut += 7) {
        const r = await parseMake(f.subarray(0, cut), f.length, (o, l) =>
          Promise.resolve(f.subarray(o, o + l)),
        );
        expect(r === null || typeof r === 'string').toBe(true);
      }
    }
  });

  it('never throws on random buffers (fuzz)', async () => {
    const seeds = [
      jpegWithMake('Canon'),
      heicWithMake('Apple'),
      tiffWithMake('Canon Inc.', { endian: 'be' }),
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc.uint8Array({ maxLength: 512 }).map((a) => Buffer.from(a)),
          fc
            .tuple(
              fc.constantFrom(...seeds),
              fc.array(fc.tuple(fc.nat(600), fc.nat(255)), { maxLength: 6 }),
            )
            .map(([s, muts]) => {
              const b = Buffer.from(s);
              for (const [i, v] of muts) if (i < b.length) b[i] = v;
              return b;
            }),
        ),
        fc.nat(2000),
        async (buf, extra) => {
          const r = await parseMake(buf, buf.length + extra, (o, l) =>
            Promise.resolve(buf.subarray(o, o + l)),
          );
          expect(r === null || typeof r === 'string').toBe(true);
        },
      ),
      { numRuns: 2000 },
    );
  });
});

describe('readMake', () => {
  let dir: string;
  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), 'exifmake-'));
  });
  afterAll(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('reads files from disk, including a far IFD0', async () => {
    const a = join(dir, 'a.jpg');
    await writeFile(a, jpegWithMake('Canon'));
    expect(await readMake(a)).toBe('Canon');
    const b = join(dir, 'b.tif');
    await writeFile(b, tiffWithMake('Pentax', { farIfd0: true }));
    expect(await readMake(b)).toBe('Pentax');
    const c = join(dir, 'c.heic');
    await writeFile(c, heicWithMake('Apple'));
    expect(await readMake(c)).toBe('Apple');
  });

  it('returns null for an empty file and throws for a missing one', async () => {
    const e = join(dir, 'empty.jpg');
    await writeFile(e, '');
    expect(await readMake(e)).toBeNull();
    await expect(readMake(join(dir, 'nope.jpg'))).rejects.toMatchObject({ code: 'ENOENT' });
  });
});
