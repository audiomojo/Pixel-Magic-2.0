/** Bounded, read-only EXIF Make reader for JPEG, TIFF/DNG and HEIC (spec §6.4). */

import { openReadOnly } from './paths.js';

export const EXIF_READ_LIMIT = 256 * 1024;
const HEIC_EXIF_LIMIT = 64 * 1024;
const TIFF_IFD_LIMIT = 64 * 1024; // a TIFF IFD0 stored past the first read
const TIFF_VALUE_LIMIT = 4 * 1024; // a Make value outside that IFD chunk
const TAG_MAKE = 0x010f;
const TYPE_ASCII = 2;

type ReadAt = (offset: number, length: number) => Promise<Buffer>;
type Fetch = (offset: number, length: number) => Promise<Buffer>;

/** Thrown by the parsers for any structurally bad input; parseMake turns it into null. */
class Malformed extends Error {}

const fail = (): never => {
  throw new Malformed();
};

/** A 4- or 8-byte big-endian unsigned field (or 0 for size 0), as a safe integer. */
function uintBE(data: Buffer, pos: number, size: number): number {
  if (size === 0) return 0;
  if ((size !== 4 && size !== 8) || pos + size > data.length) return fail();
  const v = size === 4 ? data.readUInt32BE(pos) : data.readBigUInt64BE(pos);
  if (typeof v === 'bigint') return v > BigInt(Number.MAX_SAFE_INTEGER) ? fail() : Number(v);
  return v;
}

/** The stripped IFD0 Make from a TIFF stream, or null. `data` holds the start of the stream. */
async function tiffMake(data: Buffer, fetch?: Fetch): Promise<string | null> {
  if (data.length < 8) return null;
  const tag = data.toString('latin1', 0, 2);
  if (tag !== 'II' && tag !== 'MM') return null;
  const le = tag === 'II';
  const r16 = (b: Buffer, p: number): number => (le ? b.readUInt16LE(p) : b.readUInt16BE(p));
  const r32 = (b: Buffer, p: number): number => (le ? b.readUInt32LE(p) : b.readUInt32BE(p));
  if (r16(data, 2) !== 42) return null;
  const ifd = r32(data, 4);
  let buf = data; // buf holds stream bytes from offset base
  let base = 0;
  if (ifd + 2 > data.length) {
    if (!fetch) return null;
    buf = await fetch(ifd, TIFF_IFD_LIMIT);
    base = ifd;
  }
  const rel = ifd - base;
  if (rel + 2 > buf.length) return null;
  const count = r16(buf, rel);
  for (let i = 0; i < count; i++) {
    const entry = rel + 2 + 12 * i;
    if (entry + 12 > buf.length) return null;
    if (r16(buf, entry) !== TAG_MAKE) continue;
    if (r16(buf, entry + 2) !== TYPE_ASCII) return null;
    const n = r32(buf, entry + 4);
    let value: Buffer;
    if (n <= 4) {
      value = buf.subarray(entry + 8, entry + 8 + n);
    } else {
      const start = r32(buf, entry + 8);
      if (start + n <= data.length) {
        value = data.subarray(start, start + n);
      } else if (start >= base && start - base + n <= buf.length) {
        value = buf.subarray(start - base, start - base + n);
      } else if (fetch) {
        const want = Math.min(n, TIFF_VALUE_LIMIT);
        value = await fetch(start, want);
        if (value.length < want) return null;
      } else {
        return null;
      }
    }
    const nul = value.indexOf(0);
    const make = value.toString('latin1', 0, nul < 0 ? value.length : nul).trim();
    return make || null;
  }
  return null;
}

async function jpegMake(data: Buffer): Promise<string | null> {
  let i = 2;
  while (i + 4 <= data.length) {
    if (data[i] !== 0xff) return null;
    const marker = data[i + 1]!;
    if (marker === 0xff) {
      i += 1; // fill byte
      continue;
    }
    if (marker === 0x01 || (marker >= 0xd0 && marker <= 0xd7)) {
      i += 2; // standalone markers
      continue;
    }
    if (marker === 0xda || marker === 0xd9) return null; // SOS / EOI
    const length = data.readUInt16BE(i + 2);
    if (length < 2) return null;
    const seg = data.subarray(i + 4, i + 2 + length);
    if (marker === 0xe1 && seg.length >= 6 && seg.toString('latin1', 0, 6) === 'Exif\0\0') {
      return tiffMake(seg.subarray(6));
    }
    i += 2 + length;
  }
  return null;
}

interface Box {
  kind: string;
  start: number; // payload start
  end: number; // box end
}

/** ISO BMFF boxes in data[start:end]. A box running past `end` is reported truncated, then stops. */
function boxes(data: Buffer, start: number, end: number): Box[] {
  const out: Box[] = [];
  end = Math.min(end, data.length);
  let pos = start;
  while (pos + 8 <= end) {
    let size = data.readUInt32BE(pos);
    const kind = data.toString('latin1', pos + 4, pos + 8);
    let header = 8;
    if (size === 1) {
      if (pos + 16 > end) return out;
      const big = data.readBigUInt64BE(pos + 8);
      if (big > BigInt(Number.MAX_SAFE_INTEGER)) return out;
      size = Number(big);
      header = 16;
    } else if (size === 0) {
      size = end - pos;
    }
    if (size < header || pos + size > end) {
      out.push({ kind, start: pos + header, end });
      return out;
    }
    out.push({ kind, start: pos + header, end: pos + size });
    pos += size;
  }
  return out;
}

/** The absolute offset and length of the Exif item in a HEIC head, or null. */
function heicExifLocation(data: Buffer): { offset: number; length: number } | null {
  const meta = boxes(data, 0, data.length).find((b) => b.kind === 'meta');
  if (!meta) return null;
  let exifId: number | null = null;
  let iloc: Box | null = null;
  let idat: number | null = null;
  for (const b of boxes(data, meta.start + 4, meta.end)) {
    if (b.kind === 'iinf' && b.start + 4 <= b.end) {
      const pos = b.start + 4 + (data[b.start] === 0 ? 2 : 4);
      for (const ib of boxes(data, pos, b.end)) {
        if (ib.kind !== 'infe' || ib.start + 4 > ib.end) continue;
        const iver = data[ib.start]!;
        const p = ib.start + 4;
        let id: number;
        let type: string;
        if (iver === 2 && p + 8 <= ib.end) {
          id = data.readUInt16BE(p);
          type = data.toString('latin1', p + 4, p + 8);
        } else if (iver === 3 && p + 10 <= ib.end) {
          id = data.readUInt32BE(p);
          type = data.toString('latin1', p + 6, p + 10);
        } else continue;
        if (type === 'Exif') {
          exifId = id;
          break;
        }
      }
    } else if (b.kind === 'iloc') iloc = b;
    else if (b.kind === 'idat') idat = b.start;
  }
  if (exifId === null || !iloc) return null;
  const { start, end } = iloc;
  if (start + 8 > end) return null;
  const version = data[start]!;
  if (version > 2) return null;
  const offsetSize = data[start + 4]! >> 4;
  const lengthSize = data[start + 4]! & 0x0f;
  const baseOffsetSize = data[start + 5]! >> 4;
  const indexSize = version === 1 || version === 2 ? data[start + 5]! & 0x0f : 0;
  let pos = start + 6;
  const idSize = version < 2 ? 2 : 4;
  if (pos + idSize > end) return null;
  let count = uintBE4(data, pos, idSize);
  pos += idSize;
  const per = indexSize + offsetSize + lengthSize; // bytes per extent
  const minItem = idSize + (version === 1 || version === 2 ? 2 : 0) + 2 + baseOffsetSize + 2;
  count = Math.min(count, Math.floor((end - pos) / minItem));
  for (let n = 0; n < count; n++) {
    if (pos + idSize > end) return null;
    const itemId = uintBE4(data, pos, idSize);
    pos += idSize;
    let method = 0;
    if (version === 1 || version === 2) {
      if (pos + 2 > end) return null;
      method = data.readUInt16BE(pos) & 0x0f;
      pos += 2;
    }
    pos += 2; // data_reference_index
    const baseOff = uintBE(data, pos, baseOffsetSize);
    pos += baseOffsetSize;
    if (pos + 2 > end) return null;
    const extents = data.readUInt16BE(pos);
    pos += 2;
    if (pos + extents * per > end) return null;
    let first: { off: number; len: number } | null = null;
    if (extents) {
      pos += indexSize;
      const off = uintBE(data, pos, offsetSize);
      pos += offsetSize;
      const len = uintBE(data, pos, lengthSize);
      pos += lengthSize;
      first = { off, len };
      pos += (extents - 1) * per;
    }
    if (itemId !== exifId) continue;
    if (!first) return null;
    const off = baseOff + first.off;
    if (method === 0) return { offset: off, length: first.len };
    if (method === 1 && idat !== null) return { offset: idat + off, length: first.len };
    return null;
  }
  return null;
}

/** An unsigned big-endian integer of 2 or 4 bytes (the iloc id/count fields). */
function uintBE4(data: Buffer, pos: number, size: number): number {
  if (pos + size > data.length) return fail();
  return size === 2 ? data.readUInt16BE(pos) : data.readUInt32BE(pos);
}

/**
 * The stripped EXIF Make (tag 0x010F, IFD0) of a JPEG, TIFF/DNG or HEIC, or null.
 *
 * `head` is the start of the file (≤ 256 KiB), `fileSize` its total size, `readAt` a bounded
 * positional read. Malformed data of any kind is null; only `readAt` failures propagate.
 */
export async function parseMake(
  head: Buffer,
  fileSize: number,
  readAt: ReadAt,
): Promise<string | null> {
  const fetch: Fetch = async (offset, length) =>
    offset >= fileSize ? Buffer.alloc(0) : readAt(offset, length);
  try {
    if (head.length >= 2 && head[0] === 0xff && head[1] === 0xd8) return await jpegMake(head);
    const magic = head.toString('latin1', 0, 4);
    if (magic === 'II*\0' || magic === 'MM\0*') return await tiffMake(head, fetch);
    if (head.length >= 8 && head.toString('latin1', 4, 8) === 'ftyp') {
      const loc = heicExifLocation(head);
      if (!loc || loc.length < 8 || loc.offset + loc.length > fileSize) return null;
      const item = await fetch(loc.offset, Math.min(loc.length, HEIC_EXIF_LIMIT));
      if (item.length < 4) return null;
      return await tiffMake(item.subarray(4 + item.readUInt32BE(0)));
    }
  } catch (err) {
    // Bad data surfaces as Malformed or as a Buffer RangeError; filesystem errors propagate.
    if (err instanceof Malformed || err instanceof RangeError) return null;
    throw err;
  }
  return null;
}

/** Opens `path` read-only, reads ≤ 256 KiB and parses the Make. Filesystem errors throw. */
export async function readMake(path: string): Promise<string | null> {
  const fh = await openReadOnly(path);
  try {
    const size = (await fh.stat()).size;
    const head = Buffer.alloc(EXIF_READ_LIMIT);
    const { bytesRead } = await fh.read(head, 0, EXIF_READ_LIMIT, 0);
    return await parseMake(head.subarray(0, bytesRead), size, async (offset, length) => {
      const buf = Buffer.alloc(length);
      const r = await fh.read(buf, 0, length, offset);
      return buf.subarray(0, r.bytesRead);
    });
  } finally {
    await fh.close();
  }
}
