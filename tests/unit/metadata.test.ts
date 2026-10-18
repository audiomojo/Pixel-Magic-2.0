import { describe, expect, test } from 'vitest';
import { parseMetadata } from '../../src/metadata.js';

const heic = {
  'File:FileType': 'HEIC',
  'File:MIMEType': 'image/heic',
  'File:FileTypeExtension': 'HEIC',
  'EXIF:DateTimeOriginal': '2021:10:03 14:02:11',
  'EXIF:OffsetTimeOriginal': '-07:00',
  'EXIF:SubSecTimeOriginal': 482,
  'EXIF:Make': 'Apple',
  'EXIF:Model': 'iPhone 12',
  'MakerNotes:BurstUUID': 'B-1',
  'MakerNotes:ContentIdentifier': 'C-1',
};
const jpeg = {
  'File:FileType': 'JPEG',
  'File:MIMEType': 'image/jpeg',
  'File:FileTypeExtension': 'JPG',
};

describe('parseMetadata', () => {
  test('HEIC still', () => {
    const m = parseMetadata(heic);
    expect([m.kind, m.ext, m.subsec, m.camera, m.burstUuid, m.liveContentId]).toEqual([
      'still',
      'heic',
      '482',
      'Apple iPhone 12',
      'B-1',
      'C-1',
    ]);
    expect(m.capturedAt?.toISOString()).toBe('2021-10-03T21:02:11.000Z');
  });
  test('camera dedupes make', () =>
    expect(parseMetadata({ ...heic, 'EXIF:Model': 'Apple iPhone 12' }).camera).toBe(
      'Apple iPhone 12',
    ));
  test('camera from make or model alone, else null', () => {
    expect(parseMetadata({ ...jpeg, 'EXIF:Make': 'Canon' }).camera).toBe('Canon');
    expect(parseMetadata({ ...jpeg, 'EXIF:Model': 'X100' }).camera).toBe('X100');
    expect(parseMetadata(jpeg).camera).toBeNull();
  });
  test('misnamed JPEG uses content type', () => expect(parseMetadata(jpeg).ext).toBe('jpg'));
  test('jpeg extension normalises to jpg', () =>
    expect(parseMetadata({ ...jpeg, 'File:FileTypeExtension': 'jpeg' }).ext).toBe('jpg'));
  test('Live Photo MOV', () => {
    const m = parseMetadata({
      'File:FileType': 'MOV',
      'File:MIMEType': 'video/quicktime',
      'File:FileTypeExtension': 'MOV',
      'QuickTime:ContentIdentifier': 'C-1',
      'QuickTime:Duration': 2.97,
    });
    expect([m.kind, m.liveContentId, m.durationS]).toEqual(['motion', 'C-1', 2.97]);
  });
  test('MOV falls back to QuickTime:CreateDate as UTC; bad duration is null', () => {
    const m = parseMetadata({
      'File:FileType': 'MOV',
      'File:MIMEType': 'video/quicktime',
      'File:FileTypeExtension': 'MOV',
      'QuickTime:CreateDate': '2021:10:03 14:02:11',
      'QuickTime:Duration': 'n/a',
    });
    expect(m.capturedAt?.toISOString()).toBe('2021-10-03T14:02:11.000Z');
    expect(m.durationS).toBeNull();
  });
  test('AAE sidecar and empty tags are other', () => {
    expect(
      parseMetadata({
        'File:FileType': 'XMP',
        'File:MIMEType': 'application/rdf+xml',
        'File:FileTypeExtension': 'XMP',
      }).kind,
    ).toBe('other');
    expect(parseMetadata({}).kind).toBe('other');
    expect(parseMetadata({ 'File:FileType': 'PDF', 'File:MIMEType': 'image/x-weird' }).kind).toBe(
      'other',
    );
  });
  test('no offset is treated as UTC', () =>
    expect(
      parseMetadata({
        ...jpeg,
        'EXIF:DateTimeOriginal': '2000:01:01 00:00:00',
      }).capturedAt?.toISOString(),
    ).toBe('2000-01-01T00:00:00.000Z'));
  test('Z offset and bad offset', () => {
    expect(
      parseMetadata({ ...heic, 'EXIF:OffsetTimeOriginal': 'Z' }).capturedAt?.toISOString(),
    ).toBe('2021-10-03T14:02:11.000Z');
    expect(
      parseMetadata({ ...heic, 'EXIF:OffsetTimeOriginal': 'junk' }).capturedAt?.toISOString(),
    ).toBe('2021-10-03T14:02:11.000Z');
  });
  test.each(['0000:00:00 00:00:00', 'garbage', ''])('invalid date %j becomes null', (d) =>
    expect(parseMetadata({ ...heic, 'EXIF:DateTimeOriginal': d }).capturedAt).toBeNull(),
  );
});
