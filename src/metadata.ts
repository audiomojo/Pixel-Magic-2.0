import { ExifTool } from 'exiftool-vendored';

export interface Metadata {
  /** exiftool `MIMEType`: the only source of `contents.mime`. */
  mime: string;
  ext: string;
  kind: 'still' | 'motion' | 'other';
  capturedAt: Date | null;
  subsec: string | null;
  camera: string | null;
  burstUuid: string | null;
  liveContentId: string | null;
  durationS: number | null;
}

const STILL_TYPES = new Set([
  'JPEG',
  'PNG',
  'HEIC',
  'HEIF',
  'WEBP',
  'TIFF',
  'GIF',
  'BMP',
  'DNG',
  'CR2',
  'CR3',
  'NEF',
  'ARW',
  'ORF',
  'RW2',
  'RAF',
]);

const str = (v: unknown): string | null => {
  if (typeof v === 'string') return v.trim() === '' ? null : v.trim();
  if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  return null;
};

const EXIF_DATE = /^(\d{4}):(\d{2}):(\d{2}) (\d{2}):(\d{2}):(\d{2})$/;
const OFFSET = /^([+-])(\d{2}):(\d{2})$/;

/** `YYYY:MM:DD HH:MM:SS` plus an optional `±HH:MM` offset (absent or unparseable means UTC). */
function parseDate(raw: unknown, offset: unknown): Date | null {
  const s = str(raw);
  const m = s === null ? null : EXIF_DATE.exec(s);
  if (!m) return null;
  const [y, mo, d, h, mi, se] = m.slice(1).map(Number) as [
    number,
    number,
    number,
    number,
    number,
    number,
  ];
  // A corrupt field must not become a plausible wrong capture key (they decide copy verdicts):
  // no rollover of out-of-range values (Date.UTC turns 30 Feb into 2 Mar, 24:00 into tomorrow).
  if (y === 0 || mo < 1 || mo > 12 || d < 1 || d > 31) return null;
  if (h > 23 || mi > 59 || se > 59) return null;
  const ms = Date.UTC(y, mo - 1, d, h, mi, se);
  if (new Date(ms).getUTCDate() !== d) return null;
  const o = OFFSET.exec(str(offset) ?? '');
  if (o && (Number(o[3]) > 59 || Number(o[2]) * 60 + Number(o[3]) > 14 * 60)) return null; // ±14:00 max
  const offsetMin = o ? (o[1] === '-' ? -1 : 1) * (Number(o[2]) * 60 + Number(o[3])) : 0;
  const t = new Date(ms - offsetMin * 60_000);
  return Number.isNaN(t.getTime()) ? null : t;
}

function camera(make: string | null, model: string | null): string | null {
  if (make && model)
    return model.toLowerCase().startsWith(make.toLowerCase()) ? model : `${make} ${model}`;
  return model ?? make;
}

/** Pure: turns exiftool `-G -n` tags into the typed facts the classifier consumes. */
export function parseMetadata(tags: Record<string, unknown>): Metadata {
  const fileType = str(tags['File:FileType']) ?? '';
  const mime = str(tags['File:MIMEType']) ?? '';
  const rawExt = (str(tags['File:FileTypeExtension']) ?? '').toLowerCase();
  const kind =
    mime.startsWith('image/') && STILL_TYPES.has(fileType)
      ? 'still'
      : mime.startsWith('video/')
        ? 'motion'
        : 'other';
  const duration = tags['QuickTime:Duration'];
  return {
    mime,
    ext: rawExt === 'jpeg' ? 'jpg' : rawExt,
    kind,
    capturedAt:
      parseDate(tags['EXIF:DateTimeOriginal'], tags['EXIF:OffsetTimeOriginal']) ??
      parseDate(tags['QuickTime:CreateDate'], null),
    subsec: str(tags['EXIF:SubSecTimeOriginal']),
    camera: camera(str(tags['EXIF:Make']), str(tags['EXIF:Model'])),
    burstUuid: str(tags['MakerNotes:BurstUUID']),
    liveContentId:
      str(tags['MakerNotes:ContentIdentifier']) ?? str(tags['QuickTime:ContentIdentifier']),
    durationS: typeof duration === 'number' && Number.isFinite(duration) ? duration : null,
  };
}

export class MetadataReader {
  private readonly exiftool = new ExifTool({ maxProcs: 2 });

  async read(absPath: string): Promise<Metadata> {
    const tags = await this.exiftool.readRaw(absPath, { readArgs: ['-G', '-n'] });
    return parseMetadata(tags as Record<string, unknown>);
  }

  async end(): Promise<void> {
    await this.exiftool.end();
  }
}
