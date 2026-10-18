import { expect, test } from 'vitest';
import { decodeConcurrency, loadSettings, memoryBytes } from '../../src/config.js';

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

const MiB = 1024 * 1024;

test('decode concurrency: CPUs, capped by memory over the per-decode budget, at least 1', () => {
  expect(decodeConcurrency(18, 7.75 * 1024 * MiB, 1280)).toBe(6); // Docker Desktop's default VM
  expect(decodeConcurrency(4, 64 * 1024 * MiB, 1280)).toBe(4);
  expect(decodeConcurrency(8, 1000 * MiB, 1280)).toBe(1);
  expect(decodeConcurrency(8, 2560 * MiB, 1280)).toBe(2);
});

test('decode memory: the cgroup limit when set (and below RAM), else total RAM', () => {
  expect(memoryBytes(0, 8e9)).toBe(8e9);
  expect(memoryBytes(2e9, 8e9)).toBe(2e9);
  expect(memoryBytes(18446744073709552000, 8e9)).toBe(8e9); // "no limit" on cgroup v2
});

test('CPU_CONCURRENCY wins over the memory-aware default; the budget is configurable', () => {
  const s = loadSettings({ DATABASE_URL: 'x' });
  expect(s.decodeMemBudgetMb).toBe(1280);
  expect(loadSettings({ DATABASE_URL: 'x', CPU_CONCURRENCY: '64' }).cpuConcurrency).toBe(64);
  const tiny = loadSettings({ DATABASE_URL: 'x', DECODE_MEM_BUDGET_MB: '1000000000' });
  expect([tiny.decodeMemBudgetMb, tiny.cpuConcurrency]).toEqual([1_000_000_000, 1]);
  expect(() => loadSettings({ DATABASE_URL: 'x', DECODE_MEM_BUDGET_MB: '0' })).toThrow(
    /DECODE_MEM_BUDGET_MB/,
  );
});

const TUNABLE_INTS = [
  'SCAN_INTERVAL_S',
  'STABLE_SECONDS',
  'MAX_IN_FLIGHT',
  'IO_CONCURRENCY',
  'DECODE_MEM_BUDGET_MB',
  'DECODE_TIMEOUT_S',
  'POISON_ATTEMPTS',
  'LOOK_ALIKE_PHASH',
  'LOOK_ALIKE_DHASH',
  'COPY_PHASH',
  'COPY_DHASH',
  'LIVE_MAX_DURATION_S',
  'STAGE_MAX_STALENESS_S',
];

test("an empty value (compose's ${NAME:-} for an unset variable) means the default", () => {
  const env = Object.fromEntries(
    [...TUNABLE_INTS, 'CPU_CONCURRENCY'].map((n) => [n, '']),
  ) as Record<string, string>;
  const s = loadSettings({ DATABASE_URL: 'x', ...env });
  const d = loadSettings({ DATABASE_URL: 'x' });
  expect(s).toEqual(d);
  expect([s.poisonAttempts, s.ioConcurrency, s.decodeMemBudgetMb]).toEqual([5, 8, 1280]);
});

test.each([
  'SCAN_INTERVAL_S',
  'MAX_IN_FLIGHT',
  'IO_CONCURRENCY',
  'CPU_CONCURRENCY',
  'DECODE_TIMEOUT_S',
  'POISON_ATTEMPTS',
  'LIVE_MAX_DURATION_S',
])('%s=0 is rejected (it would disable a queue or FAIL every content at once)', (name) => {
  expect(() => loadSettings({ DATABASE_URL: 'x', [name]: '0' })).toThrow(new RegExp(name));
});

test('hash thresholds may be 0 but never negative', () => {
  const s = loadSettings({ DATABASE_URL: 'x', COPY_PHASH: '0', COPY_DHASH: '0' });
  expect([s.copyPhash, s.copyDhash]).toEqual([0, 0]);
  expect(() => loadSettings({ DATABASE_URL: 'x', LOOK_ALIKE_PHASH: '-1' })).toThrow(
    /LOOK_ALIKE_PHASH/,
  );
  expect(() => loadSettings({ DATABASE_URL: 'x', STABLE_SECONDS: '-5' })).toThrow(/STABLE_SECONDS/);
});
