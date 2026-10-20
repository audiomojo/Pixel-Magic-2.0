import { mkdirSync, mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import { logger } from '../../src/logger.js';
import { startPg, type PgHandle } from '../helpers/pg.js';
import {
  detectSources,
  parseMount,
  registerDetected,
  registerSources,
  resolveMount,
  type DetectedSource,
  type SystemProbe,
} from '../../src/harvest/sources.js';

const MOUNT = `/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)
devfs on /dev (devfs, local, nobrowse)
/dev/disk3s6 on /System/Volumes/VM (apfs, local, noexec, journaled, noatime, nobrowse)
/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled, nobrowse, protect, root data)
map auto_home on /System/Volumes/Data/home (autofs, automounted, nobrowse)
/dev/disk3s3 on /Volumes/Recovery (apfs, local, journaled, nobrowse)
/dev/disk5s1 on /Volumes/Untapped.gg Companion 3.11.0-latest-universal (apfs, local, nodev, nosuid, read-only, journaled, noowners, quarantine, mounted by rich)
/dev/disk7s1 on /Volumes/Untapped.gg Companion 3.11.0-latest-universal 1 (apfs, local, nodev, nosuid, read-only, journaled, noowners, quarantine, mounted by rich)
//GUEST:@NAS._smb._tcp.local/Backup on /Volumes/Backup (smbfs, nodev, nosuid, noowners, mounted by rich)
//GUEST@NAS._smb._tcp.local/KeePass on /Volumes/KeePass (smbfs, nodev, nosuid, noowners, mounted by rich)
//GUEST@NAS._smb._tcp.local/Public on /Volumes/Public (smbfs, nodev, nosuid, noowners, mounted by rich)
//GUEST@NAS._smb._tcp.local/Plex on /Volumes/Plex-1 (smbfs, nodev, nosuid, noowners, mounted by rich)
//rich@NAS._smb._tcp.local/Shared%20Pictures on /Volumes/Shared Pictures (smbfs, nodev, nosuid, noowners, mounted by rich)
//GUEST@NAS._smb._tcp.local/%2E%2E on /Volumes/dots (smbfs, nodev)
//GUEST@NAS._smb._tcp.local/ on /Volumes/empty (smbfs, nodev)
/dev/disk8s2 on /Volumes/Expansion (exfat, local, nodev, nosuid, noowners, noatime, fskit)
/dev/disk9s1 on /Volumes/My Passport (exfat, local, nodev, nosuid, noowners)
/dev/disk10s1 on /Volumes/Seagate NTFS (ntfs, local, nodev, nosuid, read-only, noowners)
/dev/disk3s1s1@/dev/x on /Volumes/TM Snapshot (apfs, local)
/dev/disk11s1 on /Volumes/Hidden (apfs, local, nobrowse)
/dev/disk12s1 on /Volumes/.hidden (apfs, local)
/dev/disk13s1 on /Volumes/com.apple.x (apfs, local)
/dev/disk14s1 on /Volumes/NoUuid (msdos, local)
/dev/disk15s1 on /Volumes/harvest (msdos, local)
not a mount line
/dev/disk16s1 on /Users/x (apfs, local)`;

const UUIDS: Record<string, string> = {
  disk8s2: '9316A285-353D-3BE8-9F20-4D5455A05E3C',
  disk9s1: 'AAAAAAAA-0000-0000-0000-000000000009',
  disk10s1: 'AAAAAAAA-0000-0000-0000-000000000010',
};

function fake(
  mountOutput = MOUNT,
  diskutil?: (d: string) => Promise<Record<string, unknown> | null>,
): SystemProbe {
  return {
    mountOutput: () => Promise.resolve(mountOutput),
    diskutilInfo:
      diskutil ??
      ((device) => {
        const id = device.replace('/dev/', '');
        if (id === 'disk5s1' || id === 'disk7s1')
          return Promise.resolve({ BusProtocol: 'Disk Image', VolumeName: 'Untapped' });
        const u = UUIDS[id];
        return Promise.resolve(u ? { BusProtocol: 'USB', VolumeUUID: u } : { BusProtocol: 'USB' });
      }),
    statfs: () => Promise.resolve({ blocks: 1n, bavail: 1n, bsize: 1n }),
  };
}

const NOPE = '/nonexistent-data-dir';

test('parseMount splits on the last " (" so mount points may contain spaces', () => {
  const e = parseMount('//a@h/s on /Volumes/My (odd) Disk (smbfs, nodev, mounted by rich)');
  expect(e).toEqual([
    {
      device: '//a@h/s',
      mountPoint: '/Volumes/My (odd) Disk',
      fsType: 'smbfs',
      options: ['nodev', 'mounted by rich'],
    },
  ]);
});

test('detects exactly NAS-Backup, NAS-KeePass, NAS-Public, NAS-Plex, NAS-Shared Pictures, Expansion, My Passport, the NTFS disk', async () => {
  const d = await detectSources(fake(), NOPE);
  expect(d.map((s) => s.labelBase).sort()).toEqual(
    [
      'NAS-Backup',
      'NAS-KeePass',
      'NAS-Public',
      'NAS-Plex',
      'NAS-Shared Pictures',
      'Expansion',
      'My Passport',
      'Seagate NTFS',
      'NoUuid',
    ].sort(),
  );
});

test('label uses the share name, not the mount point; identities follow the spec', async () => {
  const d = await detectSources(fake(), NOPE);
  const by = (l: string): DetectedSource => d.find((s) => s.labelBase === l)!;
  expect(by('NAS-Plex')).toMatchObject({
    identity: 'net:nas/plex',
    kind: 'network',
    mountPath: '/Volumes/Plex-1',
  });
  expect(by('NAS-Shared Pictures').identity).toBe('net:nas/shared pictures');
  expect(by('NAS-Backup').identity).toBe('net:nas/backup');
  expect(by('Expansion')).toMatchObject({
    identity: 'local:9316A285-353D-3BE8-9F20-4D5455A05E3C',
    kind: 'local',
  });
  expect(by('NoUuid').identity).toBe('local-name:NoUuid');
});

test('diskutil failure keeps the volume', async () => {
  const d = await detectSources(
    fake(MOUNT, () => Promise.reject(new Error('timeout'))),
    NOPE,
  );
  expect(d.map((s) => s.labelBase)).toContain('Expansion');
  // A failed probe never mints a name-based identity.
  expect(d.find((s) => s.labelBase === 'Expansion')).toMatchObject({
    identity: '',
    probeFailed: true,
  });
  const n = await detectSources(
    fake(MOUNT, () => Promise.resolve(null)),
    NOPE,
  );
  expect(n.map((s) => s.labelBase)).toContain('Untapped.gg Companion 3.11.0-latest-universal');
});

test('a source containing DATA_DIR is excluded, also by a case variant of the path', async () => {
  const out =
    '/dev/disk20s1 on /Volumes/Fake (exfat, local)\n/dev/disk21s1 on /Volumes/Fake2 (exfat, local)';
  expect((await detectSources(fake(out), '/Volumes/Fake/data')).map((s) => s.labelBase)).toEqual([
    'Fake2',
  ]);
  expect((await detectSources(fake(out), '/VOLUMES/fake/Data')).map((s) => s.labelBase)).toEqual([
    'Fake2',
  ]);
});

test('DATA_DIR containment is also found by device+inode (real directories)', async () => {
  const root = mkdtempSync(join(tmpdir(), 'hsrc-'));
  try {
    const mp = join(root, 'Vol');
    const other = join(root, 'Other');
    mkdirSync(join(mp, 'data', 'sub'), { recursive: true });
    mkdirSync(other);
    const link = join(root, 'link');
    symlinkSync(mp, link);
    const out = `/dev/disk20s1 on ${mp} (exfat, local)\n/dev/disk21s1 on ${other} (exfat, local)`;
    const dataDir = join(mp, 'data', 'sub');
    const names = async (d: string) =>
      (await detectSources(fake(out), d, { volumes: `${root}/` })).map((s) => s.labelBase);
    expect(await names(dataDir)).toEqual(['Other']);
    expect(await names(join(link, 'data'))).toEqual(['Other']);
    expect(await names(join(other, 'x'))).toEqual(['Vol']);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

beforeEach(() => {
  vi.spyOn(logger, 'warn').mockImplementation(() => undefined);
});
afterEach(() => {
  vi.restoreAllMocks();
});

test('a stale mount whose fs calls hang is skipped within the timeout', async () => {
  const out =
    '/dev/disk20s1 on /Volumes/Stale (exfat, local)\n/dev/disk21s1 on /Volumes/Fine (exfat, local)';
  const hang = new Promise<never>(() => {});
  const slow = {
    realpath: (p: string) => (p.includes('Stale') ? hang : Promise.resolve(p)),
    stat: () => Promise.reject(new Error('ENOENT')),
  };
  const t0 = Date.now();
  const d = await detectSources(fake(out), NOPE, { fsTimeoutMs: 50, fs: slow });
  expect(d.map((s) => s.labelBase)).toEqual(['Fine']);
  expect(Date.now() - t0).toBeLessThan(2000);
});

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh?.stop();
});
beforeEach(async () => {
  await pgh.truncate();
});

const exp = (path: string): DetectedSource => ({
  identity: 'local:U1',
  labelBase: 'Expansion',
  kind: 'local',
  mountPath: path,
});

test('remount at a new mount point keeps identity and label', async () => {
  const a = await registerSources(pgh.pool, [exp('/Volumes/Expansion')]);
  const b = await registerSources(pgh.pool, [
    { ...exp('/Volumes/Expansion 1'), labelBase: 'Expansion 1' },
  ]);
  expect(b).toHaveLength(1);
  expect(b[0]).toMatchObject({
    id: a[0]!.id,
    label: 'Expansion',
    lastMountPath: '/Volumes/Expansion 1',
    mounted: true,
  });
  expect(typeof b[0]!.id).toBe('string');
  expect(b[0]!.lastSeenAt).toBeInstanceOf(Date);
});

test('two volumes named the same get distinct labels; unmounted ones are listed', async () => {
  const mk = (i: string, p: string): DetectedSource => ({
    identity: `local:${i}`,
    labelBase: 'Backup',
    kind: 'local',
    mountPath: p,
  });
  const rows = await registerSources(pgh.pool, [
    mk('A', '/Volumes/Backup'),
    mk('B', '/Volumes/Backup 1'),
  ]);
  expect(rows.map((r) => r.label)).toEqual(['Backup', 'Backup (2)']);
  const more = await registerSources(pgh.pool, [
    mk('C', '/Volumes/Backup 2'),
    mk('A', '/Volumes/Backup'),
  ]);
  expect(more.map((r) => [r.label, r.mounted])).toEqual([
    ['Backup', true],
    ['Backup (2)', false],
    ['Backup (3)', true],
  ]);
});

test('reserved labels are rejected', async () => {
  for (const l of ['harvest', '.copies', '_logs', '.x']) {
    await expect(
      registerSources(pgh.pool, [{ ...exp('/Volumes/x'), labelBase: l }]),
    ).rejects.toThrow(/reserved/);
  }
});

test('resolveMount returns the current mount path via identity, or null', async () => {
  const rows = await registerSources(pgh.pool, [
    {
      identity: 'net:nas/plex',
      labelBase: 'NAS-Plex',
      kind: 'network',
      mountPath: '/Volumes/Plex',
    },
  ]);
  const id = rows[0]!.id;
  expect(await resolveMount(pgh.pool, fake(), id, NOPE)).toBe('/Volumes/Plex-1');
  expect(await resolveMount(pgh.pool, fake(''), id, NOPE)).toBeNull();
  expect(await resolveMount(pgh.pool, fake(), '999', NOPE)).toBeNull();
});

test('probe failure on a known volume reuses its row; unknown volume is skipped; no UUID gives local-name', async () => {
  const uuidOut = '/dev/disk8s2 on /Volumes/Expansion (exfat, local)';
  const good = fake(uuidOut, () => Promise.resolve({ VolumeUUID: 'U-1', VolumeName: 'Expansion' }));
  const a = await registerSources(pgh.pool, await detectSources(good, NOPE));
  expect(a[0]!.identity).toBe('local:U-1');

  const bad = fake(uuidOut, () => Promise.resolve(null));
  const detected = await detectSources(bad, NOPE);
  expect(detected[0]).toMatchObject({ probeFailed: true, identity: '' });
  const b = await registerSources(pgh.pool, detected);
  expect(b).toHaveLength(1);
  expect(b[0]).toMatchObject({
    id: a[0]!.id,
    label: 'Expansion',
    identity: 'local:U-1',
    mounted: true,
  });
  expect(await resolveMount(pgh.pool, bad, a[0]!.id, NOPE)).toBe('/Volumes/Expansion');

  const unknown = fake('/dev/disk9s1 on /Volumes/Other (exfat, local)', () =>
    Promise.resolve(null),
  );
  const c = await registerSources(pgh.pool, await detectSources(unknown, NOPE));
  expect(c).toHaveLength(1);
  expect(c[0]!.mounted).toBe(false);
  expect(c.map((r) => r.label)).toEqual(['Expansion']);

  const noUuid = fake('/dev/disk9s1 on /Volumes/Plain (msdos, local)', () =>
    Promise.resolve({ BusProtocol: 'USB', VolumeName: 'Plain' }),
  );
  const d = await detectSources(noUuid, NOPE);
  expect(d[0]).toMatchObject({ identity: 'local-name:Plain' });
  expect(d[0]!.probeFailed).toBeUndefined();
});

test('probe failure with two local rows at the same mount path is ambiguous: skipped, no new row', async () => {
  const mk = (identity: string, labelBase: string): DetectedSource => ({
    identity,
    labelBase,
    kind: 'local',
    mountPath: '/Volumes/Same',
  });
  await registerSources(pgh.pool, [mk('local:A', 'A')]);
  await registerSources(pgh.pool, [mk('local:B', 'B')]);
  const bad = fake('/dev/disk9s1 on /Volumes/Same (exfat, local)', () => Promise.resolve(null));
  const rows = await registerSources(pgh.pool, await detectSources(bad, NOPE));
  expect(rows.map((r) => [r.identity, r.mounted])).toEqual([
    ['local:A', false],
    ['local:B', false],
  ]);
  expect(logger.warn).toHaveBeenCalledWith(
    expect.objectContaining({ matches: 2 }),
    expect.stringContaining('ambiguous'),
  );
});

test('registerDetected returns a probe-failed volume with no matching row as unidentified, without a row', async () => {
  const bad = fake('/dev/disk9s1 on /Volumes/Mystery (exfat, local)', () => Promise.resolve(null));
  const { rows, unidentified } = await registerDetected(pgh.pool, await detectSources(bad, NOPE));
  expect(rows).toEqual([]);
  expect(unidentified).toEqual([{ mountPath: '/Volumes/Mystery', labelBase: 'Mystery' }]);
});
