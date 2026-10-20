// Harvest classification (spec §6): a pure port of find-all-images. No I/O: the scanner
// walks the tree and feeds in names, sizes and a lazy EXIF Make reader.

const set = (s: string): ReadonlySet<string> => new Set(s.split(' '));

export const RAW_EXTS = set(
  'cr2 cr3 crw nef nrw arw srf sr2 dng raf orf rw2 pef srw 3fr iiq x3f erf mos mrw kdc dcr',
);
/** May carry camera EXIF. */
export const TAGGED_EXTS = set('jpg jpeg jpe jfif heic heif tif tiff');
export const FOLDER_ONLY_EXTS = set('bmp');
export const MAYBE_EXTS = set('png webp gif avif jp2 j2k jpf jpx');

export type JunkReason =
  | 'appledouble'
  | 'system-file'
  | 'too-small'
  | 'screenshot'
  | 'album-art'
  | 'in-project'
  | 'graphics-folder';

export type Verdict =
  | { kind: 'photo' }
  | { kind: 'maybe' }
  | { kind: 'junk'; reason: JunkReason }
  | { kind: 'ignored' };

/**
 * What the path above a directory says about its contents. Flags are inherited downward.
 * `library: 'root'` means directly inside a Photos/iPhoto library (only originals/masters
 * may be entered); `'kept'` is anywhere below one of those, where normal rules apply.
 */
export interface DirContext {
  photoFolder: boolean;
  inProject: boolean;
  graphics: boolean;
  library: 'none' | 'root' | 'kept';
}

/** The source root itself: its own name never counts. */
export const ROOT_CONTEXT: DirContext = Object.freeze({
  photoFolder: false,
  inProject: false,
  graphics: false,
  library: 'none',
});

const SYSTEM_FILES = set('.ds_store thumbs.db');
const ALBUM_ART_STEMS = set('folder cover front back');
const PHOTO_DIRS = new Set(['photos', 'pictures', 'my pictures', 'dcim', 'camera roll']);
const PROJECT_FILES = set(
  'package.json pom.xml build.gradle build.gradle.kts pyproject.toml setup.py cargo.toml go.mod gemfile composer.json cmakelists.txt makefile .gitignore .git',
);
const PROJECT_FILE_SUFFIXES = ['.sln', '.csproj', '.vcxproj'];
const PROJECT_DIRS = set('.git .svn node_modules venv .venv');
const PROJECT_DIR_SUFFIXES = ['.xcodeproj', '.xcworkspace'];
const GRAPHICS_DIRS = new Set(['music', 'audio', 'itunes', 'ai images']);
const GRAPHICS_SUBSTRINGS = ['comfyui', 'stable diffusion', 'midjourney'];
const GRAPHICS_SUFFIXES = ['_files', '.band'];
const PRUNE_NAMES = new Set([
  'caches',
  'node_modules',
  '.git',
  '__macosx',
  '.spotlight-v100',
  '.fseventsd',
  '.trashes',
  '.temporaryitems',
  'appdata',
  'windows',
  'program files',
  'program files (x86)',
  '$recycle.bin',
  'system volume information',
]);
const PRUNE_SUFFIXES = ['.app', '.framework', '.bundle', '.plugin', '.lrdata', '.sparsebundle'];
const LIBRARY_KEEP = set('originals masters');

const endsWithAny = (s: string, suffixes: readonly string[]): boolean =>
  suffixes.some((x) => s.endsWith(x));

/** Lower-case extension without the dot; '' when there is none (a leading dot is not one). */
function extOf(name: string): string {
  const dot = name.lastIndexOf('.');
  return dot > 0 ? name.slice(dot + 1).toLowerCase() : '';
}

const isLibrary = (lower: string): boolean =>
  lower.endsWith('.photoslibrary') || lower.startsWith('iphoto library');

/** False if the directory `name` inside `parent` is never walked (spec §6.2). */
export function keepDir(parent: DirContext, name: string): boolean {
  const n = name.toLowerCase();
  if (PRUNE_NAMES.has(n) || endsWithAny(n, PRUNE_SUFFIXES)) return false;
  if (parent.library === 'root') return LIBRARY_KEEP.has(n);
  return true;
}

/** True if a directory's own entries include a source-code project marker (`.git` as file or directory). */
function hasProjectMarker(entries: readonly { name: string; isDir: boolean }[]): boolean {
  return entries.some((e) => {
    const n = e.name.toLowerCase();
    return e.isDir
      ? PROJECT_DIRS.has(n) || endsWithAny(n, PROJECT_DIR_SUFFIXES)
      : PROJECT_FILES.has(n) || endsWithAny(n, PROJECT_FILE_SUFFIXES);
  });
}

/**
 * The context for directory `name` inside `parent`. `entries` are the child's own entries,
 * checked for project markers (before pruning, so a pruned `node_modules` still marks it).
 */
export function childContext(
  parent: DirContext,
  name: string,
  entries: readonly { name: string; isDir: boolean }[],
): DirContext {
  const n = name.toLowerCase();
  const inLibrary = parent.library === 'root' && LIBRARY_KEEP.has(n);
  return {
    photoFolder:
      parent.photoFolder ||
      inLibrary ||
      PHOTO_DIRS.has(n) ||
      n.startsWith('camera') ||
      n.includes('iphone'),
    inProject: parent.inProject || n === 'source code' || hasProjectMarker(entries),
    graphics:
      parent.graphics ||
      GRAPHICS_DIRS.has(n) ||
      endsWithAny(n, GRAPHICS_SUFFIXES) ||
      GRAPHICS_SUBSTRINGS.some((w) => n.includes(w)),
    library: isLibrary(n) ? 'root' : parent.library === 'none' ? 'none' : 'kept',
  };
}

/**
 * Classify one regular file by the spec §6.3 rules, in order. Only candidate extensions
 * are judged; anything else is `ignored` without a count. `readMake` runs for rule 8 only.
 */
export async function classify(
  f: { name: string; size: number },
  dir: DirContext,
  minSize: number,
  readMake: () => Promise<string | null>,
): Promise<Verdict> {
  const lower = f.name.toLowerCase();
  // Rule 2 is by name alone, so it applies whatever the extension.
  if (SYSTEM_FILES.has(lower)) return { kind: 'junk', reason: 'system-file' };
  const ext = extOf(f.name);
  const raw = RAW_EXTS.has(ext);
  const tagged = TAGGED_EXTS.has(ext);
  const bmp = FOLDER_ONLY_EXTS.has(ext);
  if (!raw && !tagged && !bmp && !MAYBE_EXTS.has(ext)) return { kind: 'ignored' };

  if (f.name.startsWith('._')) return { kind: 'junk', reason: 'appledouble' };
  if (f.size < minSize) return { kind: 'junk', reason: 'too-small' };
  if (lower.startsWith('screenshot') || lower.startsWith('screen shot')) {
    return { kind: 'junk', reason: 'screenshot' };
  }
  const stem = lower.slice(0, lower.length - ext.length - 1);
  if (ALBUM_ART_STEMS.has(stem) || stem.startsWith('albumart')) {
    return { kind: 'junk', reason: 'album-art' };
  }

  if (raw) return { kind: 'photo' };
  if ((tagged || bmp) && dir.photoFolder) return { kind: 'photo' };
  if (tagged && (await readMake())) return { kind: 'photo' };
  if (dir.inProject) return { kind: 'junk', reason: 'in-project' };
  if (dir.graphics) return { kind: 'junk', reason: 'graphics-folder' };
  return { kind: 'maybe' };
}
