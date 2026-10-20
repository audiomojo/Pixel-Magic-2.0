import { describe, expect, test, vi } from 'vitest';
import {
  FOLDER_ONLY_EXTS,
  MAYBE_EXTS,
  RAW_EXTS,
  ROOT_CONTEXT,
  TAGGED_EXTS,
  childContext,
  classify,
  keepDir,
  type DirContext,
  type Verdict,
} from '../../src/harvest/classify.js';

const MIN = 20480;
const BIG = 1_000_000;
const words = (s: string): ReadonlySet<string> => new Set(s.split(' '));
const f = (name: string, isDir = false) => ({ name, isDir });

/** Descend from the root through directory names, with no marker entries. */
const ctxFor = (...path: string[]): DirContext =>
  path.reduce((c, n) => childContext(c, n, []), ROOT_CONTEXT);

const run = (
  name: string,
  dir: DirContext = ROOT_CONTEXT,
  make: string | null = null,
  size = BIG,
): Promise<Verdict> => classify({ name, size }, dir, MIN, () => Promise.resolve(make));

test('extension sets match the spec', () => {
  expect(RAW_EXTS).toEqual(
    words(
      'cr2 cr3 crw nef nrw arw srf sr2 dng raf orf rw2 pef srw 3fr iiq x3f erf mos mrw kdc dcr',
    ),
  );
  expect(TAGGED_EXTS).toEqual(words('jpg jpeg jpe jfif heic heif tif tiff'));
  expect(FOLDER_ONLY_EXTS).toEqual(words('bmp'));
  expect(MAYBE_EXTS).toEqual(words('png webp gif avif jp2 j2k jpf jpx'));
});

describe('rules apply in order', () => {
  const dcim = ctxFor('DCIM');
  const project = childContext(ROOT_CONTEXT, 'app', [f('package.json')]);
  const music = ctxFor('Music');
  const vm = vi.fn();
  test.each<[string, string, number, DirContext, string | null, Verdict]>([
    [
      '1 appledouble',
      '._a.jpg',
      BIG,
      ROOT_CONTEXT,
      'Canon',
      { kind: 'junk', reason: 'appledouble' },
    ],
    ['3 too-small', 'a.jpg', 20479, dcim, 'Canon', { kind: 'junk', reason: 'too-small' }],
    ['3 threshold kept', 'a.cr2', 20480, ROOT_CONTEXT, null, { kind: 'photo' }],
    [
      '4 screenshot',
      'Screen Shot 2020.png',
      BIG,
      ROOT_CONTEXT,
      null,
      { kind: 'junk', reason: 'screenshot' },
    ],
    ['5 album-art', 'cover.jpg', BIG, dcim, null, { kind: 'junk', reason: 'album-art' }],
    ['6 raw', 'x.cr3', BIG, ROOT_CONTEXT, null, { kind: 'photo' }],
    ['6 raw in project', 'x.CR3', BIG, project, null, { kind: 'photo' }],
    ['7 tagged in photo folder', 'x.jpg', BIG, dcim, null, { kind: 'photo' }],
    ['8 tagged with make', 'x.jpg', BIG, ROOT_CONTEXT, 'Apple', { kind: 'photo' }],
    ['9 bmp in photo folder', 'x.bmp', BIG, dcim, null, { kind: 'photo' }],
    ['10 in-project', 'x.png', BIG, project, null, { kind: 'junk', reason: 'in-project' }],
    ['11 graphics', 'x.png', BIG, music, null, { kind: 'junk', reason: 'graphics-folder' }],
    ['12 maybe', 'x.png', BIG, ROOT_CONTEXT, null, { kind: 'maybe' }],
    ['12 bmp elsewhere', 'x.bmp', BIG, ROOT_CONTEXT, null, { kind: 'maybe' }],
  ])('%s', async (_n, name, size, dir, make, want) => {
    vm.mockClear();
    vm.mockResolvedValue(make);
    expect(await classify({ name, size }, dir, MIN, vm)).toEqual(want);
  });

  test('readMake is called only for rule 8', async () => {
    const spy = vi.fn(() => Promise.resolve('Apple'));
    expect(await classify({ name: 'x.jpg', size: BIG }, dcim, MIN, spy)).toEqual({ kind: 'photo' });
    await classify({ name: 'x.cr2', size: BIG }, ROOT_CONTEXT, MIN, spy);
    await classify({ name: 'x.png', size: BIG }, ROOT_CONTEXT, MIN, spy);
    await classify({ name: 'x.bmp', size: BIG }, ROOT_CONTEXT, MIN, spy);
    await classify({ name: 'screenshot.jpg', size: BIG }, ROOT_CONTEXT, MIN, spy);
    expect(spy).not.toHaveBeenCalled();
    await classify({ name: 'x.jpg', size: BIG }, ROOT_CONTEXT, MIN, spy);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe('name and size rules', () => {
  test('no vector exemption: tiny candidate is too-small', async () => {
    expect(await run('tiny.png', ROOT_CONTEXT, null, 100)).toEqual({
      kind: 'junk',
      reason: 'too-small',
    });
  });
  test('too-small before screenshot', async () => {
    expect(await run('Screenshot.jpg', ROOT_CONTEXT, null, 10)).toEqual({
      kind: 'junk',
      reason: 'too-small',
    });
  });
  test.each([
    'Screenshot 2024-01-02 at 10.00.00.jpg',
    'screenshot.heic',
    'SCREENSHOT_1.JPG',
    'Screen Shot 2015-03-01 at 9.00.00 AM.jpg',
    'screen shot.tif',
    'SCREEN SHOT 2.JPG',
    'screenshot.CR2',
  ])('screenshot: %s', async (n) => {
    expect(await run(n)).toEqual({ kind: 'junk', reason: 'screenshot' });
  });
  test.each(['my screenshot.jpg', 'screen.jpg', 'screens hot.jpg', 'my screen shot.jpg'])(
    'not a screenshot: %s',
    async (n) => {
      expect(await run(n, ctxFor('DCIM'))).toEqual({ kind: 'photo' });
    },
  );
  test.each([
    'folder.jpg',
    'Folder.JPG',
    'cover.jpeg',
    'Front.jpg',
    'back.jpg',
    'AlbumArtSmall.jpg',
    'AlbumArt_{ABC}_Large.jpg',
    'albumart.bmp',
  ])('album art: %s', async (n) => {
    expect(await run(n, ctxFor('DCIM'))).toEqual({ kind: 'junk', reason: 'album-art' });
  });
  test.each(['folder2.jpg', 'covers.jpg', 'frontdoor.jpg', 'my cover.jpg', 'backyard.jpg'])(
    'not album art: %s',
    async (n) => {
      expect(await run(n, ctxFor('DCIM'))).toEqual({ kind: 'photo' });
    },
  );
});

describe('ignored extensions', () => {
  test.each([
    'a.psd',
    'b.mov',
    'c.svg',
    'd.ai',
    'e.eps',
    'f.psb',
    'g.ico',
    'h.icns',
    'i.xcf',
    'x.txt',
    'jpg',
    'noext',
    'x.jpg.zip',
    '.jpg',
    '.DS_Store',
    'Thumbs.db',
    '._a.psd',
  ])('%s', async (n) => {
    expect(await run(n, ROOT_CONTEXT, 'Canon', 10)).toEqual({ kind: 'ignored' });
  });
  test('every candidate extension is classified', async () => {
    for (const e of [...RAW_EXTS, ...TAGGED_EXTS, ...FOLDER_ONLY_EXTS, ...MAYBE_EXTS]) {
      expect((await run(`a.${e.toUpperCase()}`)).kind).not.toBe('ignored');
    }
  });
});

describe('tagged outside a photo folder needs a Make', () => {
  test('Make present is a photo', async () => {
    expect(await run('a.jpg', ctxFor('Docs'), 'Apple')).toEqual({ kind: 'photo' });
  });
  test('no Make is maybe', async () => {
    expect(await run('a.jpg', ctxFor('Docs'), null)).toEqual({ kind: 'maybe' });
    expect(await run('a.jpg', ctxFor('Docs'), '')).toEqual({ kind: 'maybe' });
  });
  test('no Make in a project is in-project; a Make still wins', async () => {
    const p = childContext(ROOT_CONTEXT, 'app', [f('package.json')]);
    expect(await run('a.jpg', p, null)).toEqual({ kind: 'junk', reason: 'in-project' });
    expect(await run('a.jpg', p, 'Canon')).toEqual({ kind: 'photo' });
  });
  test('no Make in Music is graphics-folder', async () => {
    expect(await run('a.jpg', ctxFor('Media', 'Music'), null)).toEqual({
      kind: 'junk',
      reason: 'graphics-folder',
    });
  });
  test('project beats graphics', async () => {
    const c = childContext(ctxFor('Music'), 'app', [f('Makefile')]);
    expect(await run('a.png', c)).toEqual({ kind: 'junk', reason: 'in-project' });
  });
});

describe('photo folder rules', () => {
  test.each([
    'Photos',
    'Pictures',
    'My Pictures',
    'DCIM',
    'Camera Roll',
    'camera uploads',
    'Camera',
    "Rich's iPhone",
    'IPHONE',
    'old iphone dump',
  ])('%s counts', (n) => {
    expect(ctxFor(n).photoFolder).toBe(true);
  });
  test.each([
    'Pics',
    'Documents',
    'myphotos',
    'Photos 2010',
    'Old Pictures',
    'dcim2',
    'my camera',
    'originals',
    'Masters',
  ])('%s does not', (n) => {
    expect(ctxFor(n).photoFolder).toBe(false);
  });
  test('inherited downward', async () => {
    const c = ctxFor('Rich Mac', 'Pictures', '2010', 'trip');
    expect(c.photoFolder).toBe(true);
    expect(await run('a.jpg', c)).toEqual({ kind: 'photo' });
  });
  test('originals/masters count only directly inside a library', () => {
    expect(ctxFor('Backup', 'Photos Library.photoslibrary', 'originals').photoFolder).toBe(true);
    expect(ctxFor('x', 'Old.photoslibrary', 'Originals', 'A').photoFolder).toBe(true);
    expect(ctxFor('iPhoto Library 2010', 'Masters', '2010').photoFolder).toBe(true);
    expect(ctxFor('Work', 'originals', 'A').photoFolder).toBe(false);
    expect(ctxFor('Old.photoslibrary').photoFolder).toBe(false);
  });
  test('the source root itself is not a photo folder', () => {
    expect(ROOT_CONTEXT).toEqual({
      photoFolder: false,
      inProject: false,
      graphics: false,
      library: 'none',
    });
  });
});

describe('library pruning', () => {
  test('library root keeps only originals/masters', () => {
    for (const lib of ['Photos Library.photoslibrary', 'iPhoto Library 2010', 'IPHOTO LIBRARY']) {
      const c = ctxFor('Pictures', lib);
      expect(c.library).toBe('root');
      expect(keepDir(ROOT_CONTEXT, lib)).toBe(true);
      expect(keepDir(c, 'originals')).toBe(true);
      expect(keepDir(c, 'Masters')).toBe(true);
      expect(keepDir(c, 'resources')).toBe(false);
      expect(keepDir(c, 'database')).toBe(false);
      expect(keepDir(c, 'Thumbnails')).toBe(false);
    }
  });
  test('below originals normal rules apply', () => {
    const c = ctxFor('x.photoslibrary', 'originals');
    expect(c.library).toBe('kept');
    expect(keepDir(c, 'A')).toBe(true);
    expect(keepDir(c, 'caches')).toBe(false);
    expect(ctxFor('x.photoslibrary', 'originals', 'A').library).toBe('kept');
  });
  test('files in the library root are classified normally', async () => {
    expect(await run('a.cr2', ctxFor('x.photoslibrary'))).toEqual({ kind: 'photo' });
  });
  test.each([
    'Caches',
    'node_modules',
    '.git',
    '__MACOSX',
    '.Spotlight-V100',
    '.fseventsd',
    '.Trashes',
    '.TemporaryItems',
    'AppData',
    'Windows',
    'Program Files',
    'Program Files (x86)',
    '$RECYCLE.BIN',
    'System Volume Information',
    'Foo.app',
    'X.framework',
    'Y.bundle',
    'Z.plugin',
    'Cat Previews.lrdata',
    'tm.sparsebundle',
  ])('pruned: %s', (n) => {
    expect(keepDir(ROOT_CONTEXT, n)).toBe(false);
  });
  test('normal dirs kept', () => {
    expect(keepDir(ROOT_CONTEXT, 'Pictures')).toBe(true);
  });
});

describe('project markers', () => {
  test.each([
    ['package.json', false],
    ['pom.xml', false],
    ['build.gradle', false],
    ['build.gradle.kts', false],
    ['pyproject.toml', false],
    ['setup.py', false],
    ['Cargo.toml', false],
    ['go.mod', false],
    ['GemFile', false],
    ['composer.json', false],
    ['CMakeLists.txt', false],
    ['Makefile', false],
    ['.gitignore', false],
    ['.git', false],
    ['.git', true],
    ['X.sln', false],
    ['a.CSPROJ', false],
    ['b.vcxproj', false],
    ['.svn', true],
    ['node_modules', true],
    ['venv', true],
    ['.venv', true],
    ['Foo.xcodeproj', true],
    ['Bar.xcworkspace', true],
  ])('%s (dir=%s) marks a project', (n, isDir) => {
    expect(childContext(ROOT_CONTEXT, 'p', [f(n, isDir)]).inProject).toBe(true);
  });
  test('negatives', () => {
    expect(
      childContext(ROOT_CONTEXT, 'p', [
        f('a.jpg'),
        f('notes.txt'),
        f('Pictures', true),
        f('src', true),
      ]).inProject,
    ).toBe(false);
    // directory-only markers do not count as files
    expect(childContext(ROOT_CONTEXT, 'p', [f('venv'), f('Foo.xcodeproj')]).inProject).toBe(false);
  });
  test('a folder named Source Code is a project, case-insensitively', () => {
    expect(ctxFor('source code').inProject).toBe(true);
  });
  test('inherited downward', () => {
    const p = childContext(ROOT_CONTEXT, 'app', [f('package.json')]);
    expect(childContext(p, 'assets', []).inProject).toBe(true);
    expect(childContext(childContext(p, 'assets', []), 'deep', []).inProject).toBe(true);
    expect(ctxFor('other').inProject).toBe(false);
  });
});

describe('graphics folders', () => {
  test.each([
    'Music',
    'ComfyUI_out',
    'page_files',
    'My Song.band',
    'AI Images',
    'AUDIO',
    'iTunes',
    'my stable diffusion',
    'Midjourney',
  ])('%s', (n) => {
    expect(ctxFor(n).graphics).toBe(true);
  });
  test('inherited; negatives', () => {
    expect(ctxFor('Media', 'Music', 'A').graphics).toBe(true);
    expect(ctxFor('Pictures', '2010').graphics).toBe(false);
  });
});
