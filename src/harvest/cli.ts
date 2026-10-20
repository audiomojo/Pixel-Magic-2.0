// Harvester operations: `install` / `uninstall` the launchd agent, `status` summary (spec §13).
// The plist is the one write outside DATA_DIR, so it goes through a GuardedFs confined to the
// LaunchAgents directory. `launchctl` is called through an injectable runner.
import { execFile } from 'node:child_process';
import { realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import type pg from 'pg';
import { createPool } from '../db.js';
import { loadHarvestSettings } from './config.js';
import { LABEL, renderPlist } from './launchd.js';
import { GuardedFs } from './paths.js';

export const LAUNCHCTL = '/bin/launchctl';

export type LaunchctlRunner = (args: string[]) => Promise<{ code: number; stderr: string }>;

export interface CliDeps {
  launchctl?: LaunchctlRunner;
  /** Where the plist goes; default `~/Library/LaunchAgents`. */
  agentsDir?: string;
  uid?: number;
  /** Absolute Node binary recorded in the plist; default `process.execPath`. */
  node?: string;
  repo?: string;
  logDir?: string;
  /** Delay between bootstrap retries; tests inject a no-op. */
  sleep?: (ms: number) => Promise<void>;
  pool?: Pick<pg.Pool, 'query'>;
}

const USAGE = 'usage: harvest/cli install | uninstall | stop | start | status\n';

/** The real runner; `exec` is injectable so tests never reach launchctl. */
export const makeLaunchctl =
  (exec: typeof execFile = execFile): LaunchctlRunner =>
  (args) =>
    new Promise((done) => {
      exec(LAUNCHCTL, args, (err, _stdout, stderr) => {
        const code = err === null ? 0 : typeof err.code === 'number' ? err.code : 1;
        done({ code, stderr: stderr || (err?.message ?? '') });
      });
    });

const out = (line: string) => process.stdout.write(line + '\n');
const fail = (line: string) => process.stderr.write(line + '\n');

/** "No such process" (3) or "Could not find service" (113): the agent was not loaded. */
const notLoaded = (r: { code: number; stderr: string }) =>
  r.code === 3 || r.code === 113 || /no such process|could not find|not loaded/i.test(r.stderr);

/** src/harvest or dist/harvest: two levels up is the repository root. */
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/** Fills the defaults: the real launchctl, `~/Library/LaunchAgents`, this process's uid and Node. */
export function withDefaults(d: CliDeps): Required<Omit<CliDeps, 'pool'>> {
  return {
    launchctl: d.launchctl ?? makeLaunchctl(),
    agentsDir: d.agentsDir ?? join(homedir(), 'Library', 'LaunchAgents'),
    uid: d.uid ?? process.getuid!(),
    node: d.node ?? process.execPath,
    repo: d.repo ?? REPO_ROOT,
    logDir: d.logDir ?? join(loadHarvestSettings().dataDir, 'logs'),
    sleep: d.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms))),
  };
}

const BOOTSTRAP_TRIES = 3;
const BOOTSTRAP_DELAY_MS = 1000;

/**
 * `launchctl bootstrap`, retried on "Input/output error" (exit 5: a bootout may still be tearing
 * down). "Already loaded" counts as success. Returns an error message, or null when loaded.
 */
async function bootstrap(
  d: ReturnType<typeof withDefaults>,
  target: string,
): Promise<string | null> {
  let r = { code: 0, stderr: '' };
  for (let i = 0; i < BOOTSTRAP_TRIES; i++) {
    if (i > 0) await d.sleep(BOOTSTRAP_DELAY_MS);
    r = await d.launchctl(['bootstrap', `gui/${String(d.uid)}`, target]);
    if (r.code === 0 || /already (loaded|bootstrapped)/i.test(r.stderr)) return null;
    if (r.code !== 5 && !/input\/output error/i.test(r.stderr)) break;
  }
  return r.stderr.trim();
}

async function install(deps: CliDeps): Promise<number> {
  const d = withDefaults(deps);
  const { launchctl, agentsDir: dir, uid, node, repo, logDir } = d;
  const target = join(dir, `${LABEL}.plist`);
  const domain = `gui/${String(uid)}`;

  await new GuardedFs([logDir]).mkdirp(logDir);
  const agents = new GuardedFs([dir]);
  await agents.mkdirp(dir);
  // Reinstall: replace the old plist (the guarded open never overwrites).
  await agents.unlink(target).catch((e: NodeJS.ErrnoException) => {
    if (e.code !== 'ENOENT') throw e;
  });
  const fh = await agents.openForWrite(target);
  try {
    await fh.writeFile(renderPlist({ node, repo, logDir }));
  } finally {
    await fh.close();
  }

  // Idempotent: unload whatever is loaded (failure = nothing was), then load the new plist.
  await launchctl(['bootout', `${domain}/${LABEL}`]);
  const err = await bootstrap(d, target);
  if (err !== null) {
    fail(`launchctl bootstrap failed: ${err}`);
    return 1;
  }
  out(`installed ${target}; the harvester starts now and at every login`);
  return 0;
}

async function uninstall(deps: CliDeps): Promise<number> {
  const { launchctl, agentsDir: dir, uid } = withDefaults(deps);
  const r = await launchctl(['bootout', `gui/${String(uid)}/${LABEL}`]);
  if (r.code !== 0 && !notLoaded(r)) {
    fail(`launchctl bootout failed: ${r.stderr.trim()}`);
    return 1;
  }
  await new GuardedFs([dir])
    .unlink(join(dir, `${LABEL}.plist`))
    .catch((e: NodeJS.ErrnoException) => {
      if (e.code !== 'ENOENT') throw e;
    });
  out('uninstalled the harvester agent');
  return 0;
}

/** Unloads the agent but keeps the plist (so `start` can load it again). */
async function stop(deps: CliDeps): Promise<number> {
  const { launchctl, uid } = withDefaults(deps);
  const r = await launchctl(['bootout', `gui/${String(uid)}/${LABEL}`]);
  if (r.code !== 0 && !notLoaded(r)) {
    fail(`launchctl bootout failed: ${r.stderr.trim()}`);
    return 1;
  }
  out('stopped the harvester agent (start it again with make harvest-start)');
  return 0;
}

/** Loads the plist that `install` wrote. */
async function start(deps: CliDeps): Promise<number> {
  const d = withDefaults(deps);
  const target = join(d.agentsDir, `${LABEL}.plist`);
  if (!(await new GuardedFs([d.agentsDir]).exists(target))) {
    fail('not installed: run make harvest-install first');
    return 1;
  }
  const err = await bootstrap(d, target);
  if (err !== null) {
    fail(`launchctl bootstrap failed: ${err}`);
    return 1;
  }
  out('started the harvester agent');
  return 0;
}

async function status(pool: Pick<pg.Pool, 'query'>): Promise<number> {
  const run = await pool.query<{ id: string; state: string; paused_reason: string | null }>(
    `SELECT id::text, state, paused_reason FROM harvest_runs
     WHERE state IN ('scanning','sending','paused') ORDER BY id LIMIT 1`,
  );
  const r = run.rows[0];
  if (r) {
    out(`run ${r.id}: ${r.state}${r.paused_reason ? ` (${r.paused_reason})` : ''}`);
    out(`gate: ${r.paused_reason ? `closed (${r.paused_reason})` : 'open'}`);
  } else {
    out('no active run');
  }
  const counts = await pool.query<{ state: string; n: string }>(
    'SELECT state, count(*)::text AS n FROM harvest_items GROUP BY state ORDER BY state',
  );
  for (const c of counts.rows) out(`  ${c.state.padEnd(9)} ${c.n}`);
  const review = counts.rows.find((c) => c.state === 'REVIEW')?.n ?? '0';
  out(`possible photos awaiting review: ${review}`);
  const hb = await pool.query<{ age: string | null }>(
    `SELECT extract(epoch FROM now() - heartbeat_at)::bigint::text AS age
     FROM harvest_host_state WHERE id = 1`,
  );
  const age = hb.rows[0]?.age;
  out(age == null ? 'heartbeat: never' : `heartbeat: ${age} s ago`);
  return 0;
}

/** Runs one subcommand; any failure is one `error:` line and exit code 1. */
export async function run(argv: string[], deps: CliDeps = {}): Promise<number> {
  try {
    switch (argv[0]) {
      case 'install':
        return await install(deps);
      case 'uninstall':
        return await uninstall(deps);
      case 'stop':
        return await stop(deps);
      case 'start':
        return await start(deps);
      case 'status': {
        if (deps.pool) return await status(deps.pool);
        const pool = createPool(loadHarvestSettings().databaseUrl, 2);
        try {
          return await status(pool);
        } finally {
          await pool.end();
        }
      }
      default:
        fail(USAGE.trim());
        return 1;
    }
  } catch (e) {
    fail(`error: ${e instanceof Error ? e.message : String(e)}`);
    return 1;
  }
}

const isEntry =
  process.argv[1] !== undefined &&
  realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
if (isEntry) process.exitCode = await run(process.argv.slice(2));
