import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, afterEach, beforeAll, beforeEach, expect, test, vi } from 'vitest';
import { makeLaunchctl, run, withDefaults, type LaunchctlRunner } from '../../src/harvest/cli.js';
import { startPg, type PgHandle } from '../helpers/pg.js';

let dir: string;
let calls: string[][];
let result: { code: number; stderr: string }[];
const runner: LaunchctlRunner = (args) => {
  calls.push(args);
  return Promise.resolve(result.shift() ?? { code: 0, stderr: '' });
};
const deps = () => ({
  launchctl: runner,
  agentsDir: join(dir, 'LaunchAgents'),
  uid: 501,
  node: '/opt/node/bin/node',
  repo: '/repo',
  logDir: join(dir, 'logs'),
  sleep: () => Promise.resolve(),
});
const plist = () => join(dir, 'LaunchAgents', 'com.pixelmagic.harvester.plist');

function capture(): () => { out: string; err: string } {
  let out = '';
  let err = '';
  vi.spyOn(process.stdout, 'write').mockImplementation((s) => ((out += String(s)), true));
  vi.spyOn(process.stderr, 'write').mockImplementation((s) => ((err += String(s)), true));
  return () => ({ out, err });
}

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'harvest-cli-'));
  calls = [];
  result = [];
});
afterEach(async () => {
  vi.restoreAllMocks();
  await rm(dir, { recursive: true, force: true });
});

test('install writes the plist, then boots out and bootstraps', async () => {
  const got = capture();
  expect(await run(['install'], deps())).toBe(0);
  expect(calls).toEqual([
    ['bootout', 'gui/501/com.pixelmagic.harvester'],
    ['bootstrap', 'gui/501', plist()],
  ]);
  const text = await readFile(plist(), 'utf8');
  expect(text).toContain('/opt/node/bin/node');
  expect(text).toContain(join(dir, 'logs', 'harvester.launchd.log'));
  expect(got().out).toContain('installed');
});

test('install is idempotent: a second run replaces the plist and succeeds', async () => {
  capture();
  expect(await run(['install'], deps())).toBe(0);
  await writeFile(plist(), 'stale', { flag: 'w' });
  result = [
    { code: 0, stderr: '' },
    { code: 0, stderr: '' },
  ];
  expect(await run(['install'], deps())).toBe(0);
  expect(await readFile(plist(), 'utf8')).toContain('<plist');
});

test('install ignores a bootout of an agent that is not loaded', async () => {
  capture();
  result = [{ code: 3, stderr: 'Boot-out failed: 3: No such process' }];
  expect(await run(['install'], deps())).toBe(0);
  expect(calls[1]![0]).toBe('bootstrap');
});

test('install treats "already loaded" from bootstrap as success', async () => {
  capture();
  result = [
    { code: 0, stderr: '' },
    { code: 5, stderr: 'Bootstrap failed: 5: Input/output error (already loaded)' },
  ];
  expect(await run(['install'], deps())).toBe(0);
});

test('install retries bootstrap on an Input/output error (bootout still tearing down)', async () => {
  capture();
  const sleeps: number[] = [];
  result = [
    { code: 0, stderr: '' },
    { code: 5, stderr: 'Bootstrap failed: 5: Input/output error' },
    { code: 0, stderr: '' },
  ];
  const d = { ...deps(), sleep: (ms: number) => (sleeps.push(ms), Promise.resolve()) };
  expect(await run(['install'], d)).toBe(0);
  expect(calls.filter((c) => c[0] === 'bootstrap')).toHaveLength(2);
  expect(sleeps).toEqual([1000]);
});

test('install gives up after 3 bootstrap attempts', async () => {
  const got = capture();
  const io = { code: 5, stderr: 'Input/output error' };
  result = [{ code: 0, stderr: '' }, io, io, io];
  expect(await run(['install'], deps())).toBe(1);
  expect(calls.filter((c) => c[0] === 'bootstrap')).toHaveLength(3);
  expect(got().err).toContain('Input/output error');
});

test('stop boots out and keeps the plist; start bootstraps it; start needs an install', async () => {
  const got = capture();
  expect(await run(['start'], deps())).toBe(1);
  expect(got().err).toContain('not installed');
  await run(['install'], deps());
  calls = [];
  expect(await run(['stop'], deps())).toBe(0);
  expect(calls).toEqual([['bootout', 'gui/501/com.pixelmagic.harvester']]);
  await expect(stat(plist())).resolves.toBeDefined();
  calls = [];
  expect(await run(['start'], deps())).toBe(0);
  expect(calls).toEqual([['bootstrap', 'gui/501', plist()]]);
});

test('stop tolerates a not-loaded agent; start and stop report real errors', async () => {
  const got = capture();
  result = [{ code: 3, stderr: 'No such process' }];
  expect(await run(['stop'], deps())).toBe(0);
  result = [{ code: 1, stderr: 'denied' }];
  expect(await run(['stop'], deps())).toBe(1);
  await run(['install'], deps());
  result = [{ code: 1, stderr: 'broken' }];
  expect(await run(['start'], deps())).toBe(1);
  expect(got().err).toContain('broken');
});

test('install fails when bootstrap fails for another reason', async () => {
  const got = capture();
  result = [
    { code: 0, stderr: '' },
    { code: 1, stderr: 'nope' },
  ];
  expect(await run(['install'], deps())).toBe(1);
  expect(got().err).toContain('nope');
});

test('uninstall boots out and removes the plist; twice is fine', async () => {
  capture();
  await run(['install'], deps());
  calls = [];
  expect(await run(['uninstall'], deps())).toBe(0);
  expect(calls).toEqual([['bootout', 'gui/501/com.pixelmagic.harvester']]);
  await expect(stat(plist())).rejects.toThrow();
  result = [{ code: 3, stderr: 'No such process' }];
  expect(await run(['uninstall'], deps())).toBe(0);
});

test('uninstall keeps the plist and fails on a real bootout error', async () => {
  capture();
  await run(['install'], deps());
  result = [{ code: 1, stderr: 'permission denied' }];
  expect(await run(['uninstall'], deps())).toBe(1);
  await expect(stat(plist())).resolves.toBeDefined();
});

test('unknown or missing subcommand prints usage', async () => {
  const got = capture();
  expect(await run([], deps())).toBe(1);
  expect(await run(['nope'], deps())).toBe(1);
  expect(got().err).toContain('usage');
});

test('the real runner calls /bin/launchctl and maps success, exit codes and spawn errors', async () => {
  type Cb = (e: (Error & { code?: unknown }) | null, o: string, s: string) => void;
  const seen: string[][] = [];
  const fake = (outcome: (Error & { code?: unknown }) | null, stderr = '') =>
    makeLaunchctl(((bin: string, args: string[], cb: Cb) => {
      seen.push([bin, ...args]);
      cb(outcome, '', stderr);
    }) as never);
  expect(await fake(null)(['print', 'x'])).toEqual({ code: 0, stderr: '' });
  expect(seen[0]).toEqual(['/bin/launchctl', 'print', 'x']);
  expect(await fake(Object.assign(new Error('x'), { code: 3 }), 'No such process')([])).toEqual({
    code: 3,
    stderr: 'No such process',
  });
  expect(await fake(Object.assign(new Error('spawn failed'), { code: 'ENOENT' }))([])).toEqual({
    code: 1,
    stderr: 'spawn failed',
  });
});

test('defaults: ~/Library/LaunchAgents, this uid and node, the repo and DATA_DIR/logs', () => {
  const d = withDefaults({});
  expect(d.agentsDir).toMatch(/Library\/LaunchAgents$/);
  expect(d.uid).toBe(process.getuid!());
  expect(d.node).toBe(process.execPath);
  expect(d.logDir).toMatch(/logs$/);
  expect(typeof d.launchctl).toBe('function');
  expect(d.repo).toBe(resolve(dirname(fileURLToPath(import.meta.url)), '..', '..'));
});

let pgh: PgHandle;
beforeAll(async () => {
  pgh = await startPg();
}, 120_000);
afterAll(async () => {
  await pgh.stop();
});

test('status on an empty database', async () => {
  await pgh.truncate();
  const got = capture();
  expect(await run(['status'], { ...deps(), pool: pgh.pool })).toBe(0);
  expect(got().out).toContain('no active run');
  expect(got().out).toContain('heartbeat: never');
});

test('status names the node binary to grant Full Disk Access (installed, else this one)', async () => {
  await pgh.truncate();
  let got = capture();
  expect(await run(['status'], { ...deps(), pool: pgh.pool })).toBe(0);
  expect(got().out).toContain('node: /opt/node/bin/node (not installed)');
  vi.restoreAllMocks();
  expect(await run(['install'], { ...deps(), node: '/usr/local/bin/node&x' })).toBe(0);
  got = capture();
  expect(await run(['status'], { ...deps(), pool: pgh.pool })).toBe(0);
  expect(got().out).toContain('node: /usr/local/bin/node&x (grant it Full Disk Access)');
});

test('status shows the active run, counts, gate and heartbeat', async () => {
  await pgh.truncate();
  const { pool } = pgh;
  const s = await pool.query<{ id: string }>(
    `INSERT INTO harvest_sources (identity, label, kind) VALUES ('i','Exp','local') RETURNING id`,
  );
  const sid = s.rows[0]!.id;
  await pool.query(
    `INSERT INTO harvest_runs (id, state, paused_reason, paused_since, started_at)
     VALUES (7, 'paused', 'disk 81.0 %', now(), now())`,
  );
  let n = 0;
  for (const [state, count] of [
    ['SENT', 3],
    ['KNOWN', 2],
    ['REVIEW', 1],
  ] as const)
    for (let i = 0; i < count; i++)
      await pool.query(
        `INSERT INTO harvest_items (source_id, rel_path, path_key, size_bytes, mtime_ns, kind, state)
         VALUES ($1, $2, $2, 1, $3, 'photo', $4)`,
        [sid, `f${String(n)}`, n++, state],
      );
  await pool.query(`UPDATE harvest_host_state SET heartbeat_at = now() WHERE id = 1`);
  const got = capture();
  expect(await run(['status'], { ...deps(), pool })).toBe(0);
  const out = got().out;
  expect(out).toContain('run 7: paused (disk 81.0 %)');
  expect(out).toMatch(/SENT\s+3/);
  expect(out).toMatch(/KNOWN\s+2/);
  expect(out).toMatch(/possible photos awaiting review: 1/);
  expect(out).toMatch(/heartbeat: \d+ s ago/);
});

test('status connects with DATABASE_URL when no pool is injected', async () => {
  const got = capture();
  vi.stubEnv('DATABASE_URL', pgh.url);
  try {
    expect(await run(['status'], deps())).toBe(0);
  } finally {
    vi.unstubAllEnvs();
  }
  expect(got().out).toContain('heartbeat');
});

test('status reports an unreachable database as an error', async () => {
  const got = capture();
  const bad = { query: () => Promise.reject(new Error('ECONNREFUSED')) };
  expect(await run(['status'], { ...deps(), pool: bad as never })).toBe(1);
  expect(got().err).toContain('ECONNREFUSED');
});
