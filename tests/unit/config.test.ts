import { expect, test } from 'vitest';
import { loadSettings } from '../../src/config.js';

test('settings defaults', () => {
  const s = loadSettings({ DATABASE_URL: 'postgres://x' });
  expect([s.scanIntervalS, s.stableSeconds, s.maxInFlight]).toEqual([30, 5, 5000]);
  expect([s.lookAlikePhash, s.lookAlikeDhash, s.copyPhash, s.copyDhash]).toEqual([8, 10, 4, 6]);
  expect(s.workerQueues).toEqual(['main', 'io', 'cpu', 'cluster']);
});

test('missing DATABASE_URL throws', () => expect(() => loadSettings({})).toThrow(/DATABASE_URL/));

test('overrides and remaining defaults', () => {
  const s = loadSettings({
    DATABASE_URL: 'postgres://x',
    WORKER_QUEUES: ' io , cpu ,',
    SCAN_INTERVAL_S: '7',
    CPU_CONCURRENCY: '3',
  });
  expect(s.workerQueues).toEqual(['io', 'cpu']);
  expect(s.scanIntervalS).toBe(7);
  expect(s.cpuConcurrency).toBe(3);
  expect(s.dataDir).toBe('/data');
  expect(s.temporalAddress).toBe('temporal:7233');
  expect(s.temporalNamespace).toBe('default');
  expect(s.ioConcurrency).toBe(8);
  expect([s.decodeTimeoutS, s.poisonAttempts]).toEqual([60, 5]);
  expect([s.liveMaxDurationS, s.stageMaxStalenessS]).toEqual([5, 900]);
  expect(loadSettings({ DATABASE_URL: 'x' }).cpuConcurrency).toBeGreaterThan(0);
});

test('invalid numeric value throws', () => {
  expect(() => loadSettings({ DATABASE_URL: 'x', MAX_IN_FLIGHT: 'abc' })).toThrow(/MAX_IN_FLIGHT/);
});
