// The real compose stack on an isolated project and data dir, and the one shared tier runner
// (Task 13's chaos suite reuses `runTier`).
import { execFile as execFileCb } from 'node:child_process';
import { mkdir, readdir, rm, writeFile } from 'node:fs/promises';
import { join, posix } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { isIgnored } from '../../src/fsops.js';
import {
  PATHS,
  loadManifest,
  prng,
  seed,
  selectSubsets,
  shuffled,
  type Manifest,
  type SeedManifest,
  type SeedOptions,
} from '../../tools/corpus/lib.js';
import { closePool, poolFor } from './db.js';
import { registerDataDir } from './expectations.js';

const execFile = promisify(execFileCb);
const REPO = fileURLToPath(new URL('../../', import.meta.url));
const REPORTS = join(REPO, 'tests', 'reports');
const POSTGRES_PASSWORD = process.env['POSTGRES_PASSWORD'] ?? 'pixelmagic';
const SERVICES = [
  'postgres',
  'temporal-admin',
  'temporal',
  'temporal-ns',
  'temporal-ui',
  'migrate',
  'worker',
  'grafana',
];
const POLL_MS = 5000;
/** Subset selection seed for every tier (deterministic file lists). */
const SUBSET_SEED = 7;

/** What `waitIdle` polls. Idle = every count is 0. */
export interface StackStatus {
  runningWorkflows: number;
  eligibleDropped: number;
  pendingFiles: number;
  pendingStills: number;
}

/** One isolated compose project (`pmtest_<runId>`) over `dataDir`, with random loopback ports. */
export class Stack {
  readonly project: string;
  private url: string | null = null;
  /** Replica counts that `up` keeps (`docker compose up` would reset them to the file's). */
  private readonly scales = new Map<string, number>();

  /** Per-stack compose override holding `workerEnv` (next to the data dir, not inside it). */
  private readonly envFile: string | null;

  /** `workerEnv`: extra environment for `worker` and `worker-extra` (e.g. `CPU_CONCURRENCY`). */
  constructor(
    readonly runId: string,
    readonly dataDir: string,
    readonly workerEnv: Readonly<Record<string, string>> = {},
  ) {
    this.project = `pmtest_${runId}`;
    this.envFile = Object.keys(workerEnv).length === 0 ? null : `${dataDir}.compose.yml`;
  }

  /** `postgres://…/pixelmagic` on the mapped host port (after `up`). */
  get dbUrl(): string {
    if (this.url === null) throw new Error(`stack ${this.project} is not up`);
    return this.url;
  }

  private dbUrlFor(db: string): string {
    return this.dbUrl.replace(/\/pixelmagic$/, `/${db}`);
  }

  /** `docker compose` with this project's name, data dir and the test override. */
  async compose(...args: string[]): Promise<string> {
    const { stdout } = await execFile(
      'docker',
      [
        'compose',
        '-f',
        'docker-compose.yml',
        '-f',
        'docker-compose.test.yml',
        ...(this.envFile === null ? [] : ['-f', this.envFile]),
        ...args,
      ],
      {
        cwd: REPO,
        env: { ...process.env, COMPOSE_PROJECT_NAME: this.project, DATA_DIR: this.dataDir },
        maxBuffer: 64 * 1024 * 1024,
      },
    );
    return stdout;
  }

  async up(): Promise<void> {
    await mkdir(join(this.dataDir, 'dropzone'), { recursive: true });
    if (this.envFile !== null) {
      const env = JSON.stringify(this.workerEnv); // JSON is YAML
      await writeFile(
        this.envFile,
        `services:\n  worker:\n    environment: ${env}\n  worker-extra:\n    environment: ${env}\n`,
      );
    }
    const scale = [...this.scales].flatMap(([svc, n]) => ['--scale', `${svc}=${String(n)}`]);
    await this.compose('up', '-d', '--build', '--wait', ...scale);
    await this.connect();
  }

  /** Runs `n` replicas of `svc` from now on (kept across `up`). */
  async scale(svc: string, n: number): Promise<void> {
    this.scales.set(svc, n);
    await this.compose(
      'up',
      '-d',
      '--wait',
      '--no-deps',
      '--no-recreate',
      '--scale',
      `${svc}=${String(n)}`,
      svc,
    );
  }

  private async connect(): Promise<void> {
    const mapped = (await this.compose('port', 'postgres', '5432')).trim();
    const port = mapped.slice(mapped.lastIndexOf(':') + 1);
    if (!/^\d+$/.test(port))
      throw new Error(`no host port for postgres: ${JSON.stringify(mapped)}`);
    const url = `postgres://postgres:${POSTGRES_PASSWORD}@127.0.0.1:${port}/pixelmagic`;
    if (this.url !== null && this.url !== url) {
      await closePool(this.url);
      await closePool(this.dbUrlFor('temporal_visibility'));
    }
    this.url = url;
    registerDataDir(url, this.dataDir);
  }

  /**
   * `keep`: leave everything for inspection after dumping counts and logs to
   * `tests/reports/<runId>/`. Otherwise remove the project, its volumes and the data dir.
   */
  async down(keep: boolean): Promise<void> {
    if (keep) {
      const dir = await this.dump().catch((e: unknown) => `(dump failed: ${String(e)})`);
      console.error(
        `KEPT ${this.project} (data ${this.dataDir}); report in ${dir}. ` +
          `Remove with: COMPOSE_PROJECT_NAME=${this.project} docker compose down -v`,
      );
    }
    if (this.url !== null) {
      await closePool(this.url);
      await closePool(this.dbUrlFor('temporal_visibility'));
    }
    if (keep) return;
    await this.compose('down', '-v', '--remove-orphans');
    await rm(this.dataDir, { recursive: true, force: true });
    if (this.envFile !== null) await rm(this.envFile, { force: true });
  }

  async kill(svc: string): Promise<void> {
    await this.compose('kill', '-s', 'SIGKILL', svc);
  }

  /** Starts a stopped or killed service (a no-op if its restart policy already did). */
  async start(svc: string): Promise<void> {
    await this.compose('start', svc);
  }

  async restart(svc: string): Promise<void> {
    await this.compose('restart', svc);
    if (svc === 'postgres') await this.connect();
  }

  async pause(svc: string, ms: number): Promise<void> {
    await this.compose('pause', svc);
    try {
      await sleep(ms);
    } finally {
      await this.compose('unpause', svc);
    }
  }

  /** `docker compose kill` of the whole stack (bring it back with `up`). */
  async killAll(): Promise<void> {
    await this.compose('kill', '-s', 'SIGKILL');
  }

  async status(): Promise<StackStatus> {
    const pm = poolFor(this.dbUrl);
    const vis = poolFor(this.dbUrlFor('temporal_visibility'));
    const [wf, files, stills] = await Promise.all([
      vis.query<{ n: bigint }>(
        `SELECT count(*) AS n FROM executions_visibility
          WHERE workflow_type_name IN ('FileWorkflow', 'ContentWorkflow') AND status = 1`,
      ),
      pm.query<{ n: bigint }>(
        `SELECT count(*) AS n FROM files WHERE state IN ('DISCOVERED', 'CLAIMED')`,
      ),
      pm.query<{ n: bigint }>(
        `SELECT count(*) AS n FROM contents WHERE kind = 'still' AND state IN ('INGESTED', 'HASHED')`,
      ),
    ]);
    const dropped = (
      await readdir(join(this.dataDir, 'dropzone'), { recursive: true, withFileTypes: true })
    ).filter((d) => d.isFile() && !isIgnored(d.name)).length;
    return {
      runningWorkflows: Number(wf.rows[0]!.n),
      eligibleDropped: dropped,
      pendingFiles: Number(files.rows[0]!.n),
      pendingStills: Number(stills.rows[0]!.n),
    };
  }

  /** Two consecutive polls (5 s apart) with nothing running, pending or eligible in the drop zone. */
  async waitIdle(timeoutMs = 30 * 60_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let calm = 0;
    let last: StackStatus | null = null;
    for (;;) {
      try {
        last = await this.status();
        calm = Object.values(last).every((n) => n === 0) ? calm + 1 : 0;
      } catch {
        calm = 0; // the database is restarting (chaos): keep polling
      }
      if (calm >= 2) return;
      if (Date.now() > deadline)
        throw new Error(
          `${this.project} not idle after ${String(timeoutMs / 1000)} s: ${JSON.stringify(last)}`,
        );
      await sleep(POLL_MS);
    }
  }

  /** Forces a staging build now (`cli stage --force`) and waits for it. */
  async stage(): Promise<void> {
    await this.compose('exec', '-T', 'worker', 'node', 'dist/cli.js', 'stage', '--force');
  }

  /** Per-state counts and the last 200 log lines of each service, under `tests/reports/<runId>/`. */
  async dump(): Promise<string> {
    const dir = join(REPORTS, this.runId);
    await mkdir(dir, { recursive: true });
    if (this.url !== null) {
      const counts = await poolFor(this.dbUrl).query<{ t: string; state: string; n: bigint }>(
        `SELECT 'files' AS t, state, count(*) AS n FROM files GROUP BY state
         UNION ALL SELECT 'contents', state, count(*) FROM contents GROUP BY state ORDER BY 1, 2`,
      );
      const text = counts.rows.map((r) => `${r.t}\t${r.state}\t${String(r.n)}`).join('\n');
      console.error(`${this.project} state counts:\n${text}`);
      await writeFile(join(dir, 'counts.tsv'), text + '\n');
    }
    for (const svc of SERVICES) {
      const logs = await this.compose('logs', '--no-color', '--tail', '200', svc).catch(
        (e: unknown) => `(logs failed: ${String(e)})`,
      );
      await writeFile(join(dir, `${svc}.log`), logs);
    }
    return dir;
  }
}

export type Tier = 'smoke' | 'standard';

/** One fault: SIGKILL a service, restart it, pause it for `ms` (≈ Mac sleep), or SIGKILL them all. */
export type ChaosAction =
  ['kill', string] | ['restart', string] | ['pause', string, number] | ['killAll'];

export interface ChaosOptions {
  /** PRNG seed: the action order and the waits are reproducible. */
  seed: number;
  /** Wait before each action, uniform in `[min, max]` seconds. */
  everyS: [min: number, max: number];
  /**
   * Stop once this fraction of the seeded stills (corpus stills and synthetic variants, by
   * content) is terminal: CLUSTERED, FAILED or QUARANTINED. A merely ingested file doesn't count,
   * so the chaos overlaps decode and clustering, not just ingest.
   */
  untilProgress: number;
  actions: ChaosAction[];
}

/** One applied action and the still progress just before it (null: the database was down). */
export interface AppliedAction {
  action: ChaosAction;
  stillProgress: number | null;
}

export interface ChaosRun {
  /** Human-readable lines, also printed as they happen. */
  log: string[];
  applied: AppliedAction[];
}

const TERMINAL_STILL_STATES = ['CLUSTERED', 'FAILED', 'QUARANTINED'];
/** How long a killed service stays down before it is started again (as a supervisor would). */
const KILL_DOWNTIME_MS = 5000;

export interface TierRun {
  tier: Tier;
  runId: string;
  stack: Stack;
  dataDir: string;
  dbUrl: string;
  manifest: Manifest;
  seedManifest: SeedManifest;
  /** `checkInvariant(...ctx)` */
  ctx: [string, string, SeedManifest];
  /** `treeSnapshot(...snap)` */
  snap: [string, string];
  /** `diffExpectations(...exp)` */
  exp: [string, SeedManifest, Manifest];
  /** `stack.waitIdle`, then `stack.stage()`. */
  runToIdle(timeoutMs?: number): Promise<void>;
  /** Seeds the tier's corpus names again under `dropzone/<prefix>`; merges them into `seedManifest`. */
  reseed(prefix: string): Promise<SeedManifest>;
  /**
   * Scales `worker-extra` to 1, then applies seeded-random `actions` at random `everyS` intervals
   * until `untilProgress` (of the seeded stills, see `ChaosOptions`) is reached and every action has been applied at least once (actions are
   * drawn as shuffled rounds of the whole list). Returns the log and each applied action with
   * the still progress it landed at.
   */
  runWithChaos(opts: ChaosOptions): Promise<ChaosRun>;
}

export interface TierOptions {
  shuffle?: number;
  layout?: 'flat' | 'nested';
  runId?: string;
  /** Extra environment for this run's workers only (e.g. `{ CPU_CONCURRENCY: '2' }`). */
  workerEnv?: Record<string, string>;
}

/**
 * Seeds the tier's subset into `data-test/<runId>/dropzone` (standard adds junk and corrupt
 * extras and synthetic variants) and brings the stack up. It does not wait for the pipeline.
 */
export async function runTier(tier: Tier, opts: TierOptions = {}): Promise<TierRun> {
  const runId = opts.runId ?? `${tier}-${Date.now().toString(36)}`;
  const dataDir = join(REPO, 'data-test', runId);
  const dropzone = join(dataDir, 'dropzone');
  const manifest = await loadManifest();
  const names = selectSubsets(manifest, SUBSET_SEED)[tier];
  const layout = opts.layout ?? 'flat';
  const seedOpts: SeedOptions = { corpusDir: PATHS.corpus, names, dest: dropzone, layout };
  if (opts.shuffle !== undefined) seedOpts.shuffle = opts.shuffle;
  if (tier === 'standard')
    Object.assign(seedOpts, { extras: ['junk', 'corrupt'], synthetic: true });
  const seedManifest = await seed(seedOpts);
  const saveSeedManifest = () =>
    writeFile(join(dataDir, 'seed-manifest.json'), JSON.stringify(seedManifest, null, 1));
  await saveSeedManifest();

  const stack = new Stack(runId, dataDir, opts.workerEnv);
  try {
    await stack.up();
  } catch (e) {
    await stack.down(true);
    throw e;
  }
  // Getters: a restarted postgres container may come back on another host port.
  return {
    tier,
    runId,
    stack,
    dataDir,
    get dbUrl() {
      return stack.dbUrl;
    },
    manifest,
    seedManifest,
    get ctx(): TierRun['ctx'] {
      return [stack.dbUrl, dataDir, seedManifest];
    },
    get snap(): TierRun['snap'] {
      return [dataDir, stack.dbUrl];
    },
    get exp(): TierRun['exp'] {
      return [stack.dbUrl, seedManifest, manifest];
    },
    async runToIdle(timeoutMs) {
      await stack.waitIdle(timeoutMs);
      await stack.stage();
    },
    runWithChaos: (opts) => runWithChaos(stack, seededStills(seedManifest, manifest), opts),
    async reseed(prefix) {
      const sm = await seed({
        corpusDir: PATHS.corpus,
        names,
        dest: join(dropzone, prefix),
        layout,
      });
      seedManifest.files.push(
        ...sm.files.map((f) => ({ ...f, relPath: posix.join(prefix, f.relPath) })),
      );
      await saveSeedManifest();
      return seedManifest;
    },
  };
}

/** Content SHA-256s of the seeded stills: corpus files the manifest calls stills, and every synthetic variant. */
function seededStills(sm: SeedManifest, manifest: Manifest): string[] {
  const still = new Set(manifest.records.filter((r) => r.kind === 'still').map((r) => r.name));
  return [
    ...new Set(
      sm.files
        .filter((f) => f.origin === 'synthetic' || (f.origin === 'corpus' && still.has(f.name)))
        .map((f) => f.sha256),
    ),
  ];
}

/** Fraction of `stills` in a terminal content state; null while the database is down. */
async function stillProgress(stack: Stack, stills: string[]): Promise<number | null> {
  try {
    const r = await poolFor(stack.dbUrl).query<{ n: bigint }>(
      // A content the pipeline didn't take for a still has no decode or cluster step left.
      `SELECT count(*) AS n FROM contents
        WHERE sha256 = ANY($1) AND (state = ANY($2) OR kind <> 'still')`,
      [stills, TERMINAL_STILL_STATES],
    );
    return Number(r.rows[0]!.n) / stills.length;
  } catch {
    return null;
  }
}

async function apply(stack: Stack, a: ChaosAction): Promise<void> {
  switch (a[0]) {
    case 'kill':
      await stack.kill(a[1]);
      await sleep(KILL_DOWNTIME_MS);
      await stack.start(a[1]);
      return;
    case 'restart':
      return stack.restart(a[1]);
    case 'pause':
      return stack.pause(a[1], a[2]);
    case 'killAll':
      await stack.killAll();
      return stack.up();
  }
}

async function runWithChaos(stack: Stack, stills: string[], opts: ChaosOptions): Promise<ChaosRun> {
  if (opts.actions.length === 0) throw new Error('runWithChaos: no actions');
  if (stills.length === 0) throw new Error('runWithChaos: no seeded stills');
  const rnd = prng(opts.seed);
  const [min, max] = opts.everyS;
  const t0 = Date.now();
  const log: string[] = [];
  const done: AppliedAction[] = [];
  const note = (msg: string) => {
    const line = `+${((Date.now() - t0) / 1000).toFixed(1)}s ${msg}`;
    log.push(line);
    process.stdout.write(`[chaos ${stack.project}] ${line}\n`); // shown even when the test passes
  };
  await stack.scale('worker-extra', 1);
  note('scaled worker-extra to 1');
  const applied = new Set<number>();
  let round: number[] = [];
  for (;;) {
    await sleep((min + rnd() * (max - min)) * 1000);
    // Measured after the wait, so no action is applied once the goal is already reached.
    const before = await stillProgress(stack, stills);
    if (applied.size === opts.actions.length && before !== null && before >= opts.untilProgress) {
      note(`done at still progress ${before.toFixed(3)}`);
      return { log, applied: done };
    }
    if (round.length === 0) round = shuffled([...opts.actions.keys()], rnd);
    const i = round.pop()!;
    const action = opts.actions[i]!;
    const started = Date.now();
    await apply(stack, action);
    applied.add(i);
    done.push({ action, stillProgress: before });
    note(
      `${action.join(' ')} (still progress ${before === null ? 'n/a' : before.toFixed(3)}, ` +
        `took ${((Date.now() - started) / 1000).toFixed(1)}s)`,
    );
  }
}
