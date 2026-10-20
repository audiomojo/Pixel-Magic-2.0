// Builders for tiny synthetic JPEG / TIFF / HEIC files carrying an EXIF Make tag.
// A TypeScript port of find-all-images-wt/tests/exif_fixtures.py.

type Endian = 'le' | 'be';

const u16 = (v: number, e: Endian): Buffer => {
  const b = Buffer.alloc(2);
  if (e === 'le') b.writeUInt16LE(v);
  else b.writeUInt16BE(v);
  return b;
};
const u32 = (v: number, e: Endian): Buffer => {
  const b = Buffer.alloc(4);
  if (e === 'le') b.writeUInt32LE(v);
  else b.writeUInt32BE(v);
  return b;
};
const be16 = (v: number): Buffer => u16(v, 'be');
const be32 = (v: number): Buffer => u32(v, 'be');

export interface TiffOptions {
  endian?: Endian;
  /** Bytes of padding before IFD0 (IFD0 then sits at 8 + ifdPad). */
  ifdPad?: number;
  /** Bytes between IFD0 and an out-of-line Make value. */
  valuePad?: number;
  /** Put IFD0 beyond the 256 KiB head (a 300 KiB pad). */
  farIfd0?: boolean;
}

/** A TIFF stream whose IFD0 holds ImageWidth then Make (ASCII). */
export function tiffWithMake(make: string, opts: TiffOptions = {}): Buffer {
  const e = opts.endian ?? 'le';
  const ifdPad = opts.farIfd0 ? 300 * 1024 : (opts.ifdPad ?? 0);
  const valuePad = opts.valuePad ?? 0;
  const value = Buffer.concat([Buffer.from(make, 'latin1'), Buffer.from([0])]);
  const ifd0 = 8 + ifdPad;
  const dataOff = ifd0 + 2 + 2 * 12 + 4 + valuePad;
  const parts: Buffer[] = [
    Buffer.from(e === 'le' ? 'II' : 'MM', 'latin1'),
    u16(42, e),
    u32(ifd0, e),
    Buffer.alloc(ifdPad),
    u16(2, e),
    u16(0x0100, e),
    u16(3, e),
    u32(1, e),
    u16(640, e),
    u16(0, e),
    u16(0x010f, e),
    u16(2, e),
    u32(value.length, e),
  ];
  let tail = Buffer.alloc(0);
  if (value.length <= 4) {
    const inline = Buffer.alloc(4);
    value.copy(inline);
    parts.push(inline);
  } else {
    parts.push(u32(dataOff, e));
    tail = Buffer.concat([Buffer.alloc(valuePad), value]);
  }
  parts.push(u32(0, e), tail);
  return Buffer.concat(parts);
}

/** SOI, APP0 (JFIF), optional APP1 Exif, SOS, filler, EOI. */
export function jpegWithMake(make: string | null, pad = 0): Buffer {
  const app0 = Buffer.concat([
    Buffer.from('JFIF\0', 'latin1'),
    Buffer.from([1, 1, 0, 0, 1, 0, 1, 0, 0]),
  ]);
  const parts: Buffer[] = [
    Buffer.from([0xff, 0xd8]),
    Buffer.from([0xff, 0xe0]),
    be16(app0.length + 2),
    app0,
  ];
  if (make !== null) {
    const app1 = Buffer.concat([
      Buffer.from('Exif\0\0', 'latin1'),
      tiffWithMake(make, { endian: 'be' }),
    ]);
    parts.push(Buffer.from([0xff, 0xe1]), be16(app1.length + 2), app1);
  }
  const sos = Buffer.from([1, 1, 0, 0, 0x3f, 0]);
  parts.push(
    Buffer.from([0xff, 0xda]),
    be16(sos.length + 2),
    sos,
    Buffer.alloc(pad, 0x11),
    Buffer.from([0xff, 0xd9]),
  );
  return Buffer.concat(parts);
}

/** A JPEG whose APP1 length claims far more than the file holds. */
export function jpegTruncatedApp1(): Buffer {
  const full = jpegWithMake('Canon');
  const at = full.indexOf(Buffer.from([0xff, 0xe1]));
  return full.subarray(0, at + 30);
}

/** A JPEG whose APP1 Exif holds a TIFF IFD0 claiming 65535 entries. */
export function jpegHugeIfdCount(): Buffer {
  const full = Buffer.from(jpegWithMake('Canon'));
  const at = full.indexOf(Buffer.from('Exif\0\0', 'latin1')) + 6;
  // TIFF header is 8 bytes, IFD0 count follows (big-endian in the JPEG fixture).
  full.writeUInt16BE(0xffff, at + 8);
  full.writeUInt16BE(0x0110, at + 8 + 2 + 12); // Make renamed, so the scan runs off the buffer
  return full;
}

const box = (kind: string, payload: Buffer): Buffer =>
  Buffer.concat([be32(8 + payload.length), Buffer.from(kind, 'latin1'), payload]);
const fullBox = (kind: string, version: number, payload: Buffer, flags = 0): Buffer =>
  box(kind, Buffer.concat([be32(((version << 24) | flags) >>> 0), payload]));

const ftyp = (): Buffer =>
  box('ftyp', Buffer.concat([Buffer.from('heic'), be32(0), Buffer.from('mif1heic')]));

/** A HEIC whose iloc has zero-size fields and 65535 extents per item, and no matching Exif entry. */
export function heicIlocBomb(items = 40000): Buffer {
  const infe = fullBox(
    'infe',
    2,
    Buffer.concat([be16(1), be16(0), Buffer.from('Exif'), Buffer.from([0])]),
  );
  const iinf = fullBox('iinf', 0, Buffer.concat([be16(1), infe]));
  const rows: Buffer[] = [Buffer.from([0, 0]), be16(items)];
  const row = Buffer.concat([be16(2), be16(0), be16(0xffff)]);
  for (let i = 0; i < items; i++) rows.push(row);
  const iloc = fullBox('iloc', 0, Buffer.concat(rows));
  return Buffer.concat([ftyp(), fullBox('meta', 0, Buffer.concat([iinf, iloc]))]);
}

export interface HeicOptions {
  iinfVersion?: 0 | 1;
  infeVersion?: 2 | 3;
  ilocVersion?: 0 | 1 | 2;
  constructionMethod?: number;
  withExif?: boolean;
}

/** ftyp + meta[hdlr, iinf, iloc(, idat)] + mdat holding the Exif item. */
export function heicWithMake(make: string, opts: HeicOptions = {}): Buffer {
  const iinfVersion = opts.iinfVersion ?? 0;
  const infeVersion = opts.infeVersion ?? 2;
  const ilocVersion = opts.ilocVersion ?? 0;
  const method = opts.constructionMethod ?? 0;
  const withExif = opts.withExif ?? true;
  const exifItem = Buffer.concat([be32(6), Buffer.from('Exif\0\0', 'latin1'), tiffWithMake(make)]);
  const hdlr = fullBox(
    'hdlr',
    0,
    Buffer.concat([Buffer.alloc(4), Buffer.from('pict'), Buffer.alloc(13)]),
  );

  const idOf = (v: number): Buffer => (infeVersion === 2 ? be16(v) : be32(v));
  const infe = (id: number, type: string): Buffer =>
    fullBox(
      'infe',
      infeVersion,
      Buffer.concat([idOf(id), be16(0), Buffer.from(type), Buffer.from([0])]),
    );
  const items = [infe(1, 'hvc1'), ...(withExif ? [infe(2, 'Exif')] : [])];
  const count = iinfVersion === 0 ? be16(items.length) : be32(items.length);
  const iinf = fullBox('iinf', iinfVersion, Buffer.concat([count, ...items]));

  const iloc = (offset: number): Buffer => {
    const wide = (v: number): Buffer => (ilocVersion < 2 ? be16(v) : be32(v));
    const rows: Buffer[] = [Buffer.from([0x44, 0x40]), wide(2)];
    for (const [id, off, len] of [
      [1, 0, 0],
      [2, offset, exifItem.length],
    ] as const) {
      rows.push(wide(id));
      if (ilocVersion === 1 || ilocVersion === 2) rows.push(be16(id === 2 ? method : 0));
      rows.push(be16(0), be32(0), be16(1), be32(off), be32(len));
    }
    return fullBox('iloc', ilocVersion, Buffer.concat(rows));
  };

  if (method === 1) {
    const meta = fullBox('meta', 0, Buffer.concat([hdlr, iinf, iloc(0), box('idat', exifItem)]));
    return Buffer.concat([ftyp(), meta, box('mdat', Buffer.alloc(64))]);
  }
  const metaLen = fullBox('meta', 0, Buffer.concat([hdlr, iinf, iloc(0)])).length;
  const offset = ftyp().length + metaLen + 8;
  const meta = fullBox('meta', 0, Buffer.concat([hdlr, iinf, iloc(offset)]));
  return Buffer.concat([ftyp(), meta, box('mdat', exifItem)]);
}

/** A HEIC with the given size field patched into its first box (0, 1, or past EOF). */
export function heicWithBoxSize(size: number): Buffer {
  const b = Buffer.from(heicWithMake('Apple'));
  b.writeUInt32BE(size, 0);
  return b;
}
