import { resolve } from 'node:path';
import { expect, test } from 'vitest';
import { loadHarvestSettings } from '../../src/harvest/config.js';

const root = '/repo';

test('defaults', () => {
  expect(loadHarvestSettings({}, root)).toEqual({
    dataDir: '/repo/data',
    databaseUrl: 'postgres://postgres:pixelmagic@127.0.0.1:5432/pixelmagic',
    temporalAddress: '127.0.0.1:7233',
    temporalNamespace: 'default',
    uiPort: 3020,
    maxDiskPct: 80,
    maxPendingFiles: 20000,
    maxPendingBytes: null,
    minSize: 20480,
    maxAttempts: 5,
    thumbCacheMb: 1024,
  });
});
test('empty strings mean unset; password and overrides are honoured', () => {
  const s = loadHarvestSettings(
    {
      HARVEST_UI_PORT: '',
      POSTGRES_PASSWORD: 'pw',
      HARVEST_MAX_PENDING_BYTES: '1000',
      HARVEST_MAX_DISK_PCT: '99',
      TEMPORAL_ADDRESS: 'h:1',
      TEMPORAL_NAMESPACE: 'ns',
      DATABASE_URL: 'postgres://x',
    },
    root,
  );
  expect(s.uiPort).toBe(3020);
  expect(s.maxPendingBytes).toBe(1000);
  expect(s.maxDiskPct).toBe(99);
  expect(s.temporalAddress).toBe('h:1');
  expect(s.temporalNamespace).toBe('ns');
  expect(s.databaseUrl).toBe('postgres://x');
  expect(loadHarvestSettings({ POSTGRES_PASSWORD: 'pw' }, root).databaseUrl).toContain(':pw@');
});
test('DATA_DIR relative resolves against the repo root; absolute is kept; empty is default', () => {
  expect(loadHarvestSettings({ DATA_DIR: './data2' }, root).dataDir).toBe('/repo/data2');
  expect(loadHarvestSettings({ DATA_DIR: '/abs/d' }, root).dataDir).toBe('/abs/d');
  expect(loadHarvestSettings({ DATA_DIR: '' }, root).dataDir).toBe('/repo/data');
});
test('default repo root is the real repository root', () => {
  expect(loadHarvestSettings({}).dataDir).toBe(resolve(import.meta.dirname, '../../data'));
});
test('HARVEST_MAX_DISK_PCT is limited to 1-99', () => {
  expect(() => loadHarvestSettings({ HARVEST_MAX_DISK_PCT: '100' }, root)).toThrow();
  expect(() => loadHarvestSettings({ HARVEST_MAX_DISK_PCT: '0' }, root)).toThrow();
  expect(loadHarvestSettings({ HARVEST_MAX_DISK_PCT: '1' }, root).maxDiskPct).toBe(1);
});
