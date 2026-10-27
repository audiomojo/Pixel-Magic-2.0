import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ExifTool } from 'exiftool-vendored';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { MetadataReader } from '../../src/metadata.js';
import { jpegWithMake } from '../helpers/exifFixtures.js';

describe('MetadataReader.keeperTags (real exiftool)', () => {
  let dir: string;
  let reader: MetadataReader;
  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), 'keeper-tags-'));
    reader = new MetadataReader();
  });
  afterAll(async () => {
    await reader.end();
    await rm(dir, { recursive: true, force: true });
  });

  test('reads Software and counts present metadata', async () => {
    const p = join(dir, 'a.jpg');
    await writeFile(p, jpegWithMake('Canon'));
    const et = new ExifTool({ maxProcs: 1 });
    try {
      await et.write(p, { Software: 'Picasa 3.0', Model: 'EOS' }, ['-overwrite_original']);
    } finally {
      await et.end();
    }
    expect(await reader.keeperTags(p)).toEqual({
      software: 'Picasa 3.0',
      creatorTool: null,
      metaCount: 2,
    });
  });
});
