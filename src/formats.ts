// The still formats the pipeline decodes, clusters and ranks: one row per exiftool FileType.
// Everything else (other RAWs such as PEF, SRW, DCR) is vaulted as kind `other`, never
// clustered (harvest spec §2). To support a format, add its row here.

interface StillFormat {
  /** Vault extensions (exiftool `FileTypeExtension`, plus common spellings). */
  readonly exts: readonly string[];
  /** Best-copy rank (spec §6.4), higher wins. */
  readonly rank: number;
  /** RAW files are decoded from their embedded preview, reported with this MIME type. */
  readonly rawMime?: string;
}

const RAW_RANK = 8;

const STILL_FORMATS: Readonly<Record<string, StillFormat>> = {
  DNG: { exts: ['dng'], rank: RAW_RANK, rawMime: 'image/x-adobe-dng' },
  CR2: { exts: ['cr2'], rank: RAW_RANK, rawMime: 'image/x-canon-cr2' },
  CR3: { exts: ['cr3'], rank: RAW_RANK, rawMime: 'image/x-canon-cr3' },
  NEF: { exts: ['nef'], rank: RAW_RANK, rawMime: 'image/x-nikon-nef' },
  ARW: { exts: ['arw'], rank: RAW_RANK, rawMime: 'image/x-sony-arw' },
  ORF: { exts: ['orf'], rank: RAW_RANK, rawMime: 'image/x-olympus-orf' },
  RW2: { exts: ['rw2'], rank: RAW_RANK, rawMime: 'image/x-panasonic-rw2' },
  RAF: { exts: ['raf'], rank: RAW_RANK, rawMime: 'image/x-fuji-raf' },
  TIFF: { exts: ['tif', 'tiff'], rank: 7 },
  PNG: { exts: ['png'], rank: 6 },
  HEIC: { exts: ['heic'], rank: 5 },
  HEIF: { exts: ['heif'], rank: 5 },
  JPEG: { exts: ['jpg', 'jpeg'], rank: 4 },
  WEBP: { exts: ['webp'], rank: 3 },
  GIF: { exts: ['gif'], rank: 2 },
  BMP: { exts: ['bmp'], rank: 1 },
};

const byExt = Object.values(STILL_FORMATS).flatMap((f) => f.exts.map((e) => [e, f] as const));

/** exiftool FileTypes that make a file kind `still`. */
export const STILL_FILE_TYPES: ReadonlySet<string> = new Set(Object.keys(STILL_FORMATS));

/** Non-RAW still extensions, decoded directly. */
export const STILL_EXTS: ReadonlySet<string> = new Set(
  byExt.filter(([, f]) => f.rawMime === undefined).map(([e]) => e),
);

/** RAW extension → the MIME type its decoded preview is reported as. */
export const RAW_MIME: ReadonlyMap<string, string> = new Map(
  byExt.flatMap(([e, f]) => (f.rawMime === undefined ? [] : [[e, f.rawMime] as const])),
);

/** Still extension → best-copy rank. */
export const FORMAT_RANK: ReadonlyMap<string, number> = new Map(byExt.map(([e, f]) => [e, f.rank]));

/** `ftyp` major brands of HEIC/HEIF files (sniffed, since extensions lie). */
const HEIF_BRANDS = new Set(['heic', 'heix', 'hevc', 'hevx', 'heim', 'heis', 'mif1', 'msf1']);

/** True when the file's first 12+ bytes are an HEIF `ftyp` box. */
export function isHeifHead(head: Buffer): boolean {
  return (
    head.length >= 12 &&
    head.toString('latin1', 4, 8) === 'ftyp' &&
    HEIF_BRANDS.has(head.toString('latin1', 8, 12))
  );
}
