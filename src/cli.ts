import { realpathSync } from 'node:fs';
import { appendFile, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Client, Connection } from '@temporalio/client';
import type pg from 'pg';
import { createDecodePool } from './activities/hashing.js';
import {
  dedupeIdenticalAll,
  reportHeader,
  reportLines,
  type CleanupSummary,
  type IdenticalCtx,
} from './activities/identical.js';
import type { Deps } from './activities/index.js';
import { loadSettings } from './config.js';
import { createPool } from './db.js';
import { DataLayout, errText } from './fsops.js';
import { MetadataReader } from './metadata.js';
import { doctor, listCopies, removedCopies, retryFailed, verifyVault } from './operator.js';
import type { CopySweepWorkflow, StagingWorkflow } from './workflows.js';

export interface CliDeps {
  temporal?: Client;
  pool?: pg.Pool;
  layout?: DataLayout;
  /** `POISON_ATTEMPTS` for retried contents; defaults to the configured value. */
  poisonAttempts?: number;
  /** `dedupe-identical`'s pool, settings, layout, metadata and decode pool (tests); else built and closed here. */
  identical?: Deps;
}

const USAGE =
  'usage: cli stage [--force] | retry-failed | verify-vault [--sample N] | doctor | copies | sweep-copies | dedupe-identical [--apply]\n';

async function connect(): Promise<{ client: Client; close: () => Promise<void> }> {
  const s = loadSettings();
  const connection = await Connection.connect({ address: s.temporalAddress });
  return {
    client: new Client({ connection, namespace: s.temporalNamespace }),
    close: () => connection.close(),
  };
}

const here = (file: string) => fileURLToPath(new URL(file, import.meta.url));
const out = (line: string) => process.stdout.write(line + '\n');

async function stage(client: Client, force: boolean): Promise<number> {
  const result = await client.workflow.execute<typeof StagingWorkflow>('StagingWorkflow', {
    taskQueue: 'main',
    workflowId: `pm-stage-cli-${String(Date.now())}`,
    args: [force],
  });
  out(JSON.stringify(result));
  return 0;
}

async function sweep(client: Client): Promise<number> {
  const result = await client.workflow.execute<typeof CopySweepWorkflow>('CopySweepWorkflow', {
    taskQueue: 'main',
    workflowId: `pm-copies-cli-${String(Date.now())}`,
    args: [],
  });
  out(JSON.stringify(result));
  return 0;
}

async function copies(pool: pg.Pool): Promise<number> {
  out('rel_path\tmtime\tdropped_at\tvault_sha256\tstatus');
  for (const c of await listCopies(pool))
    out(
      [c.relPath, c.mtime.toISOString(), c.droppedAt.toISOString(), c.sha256, c.status].join('\t'),
    );
  return 0;
}

async function retry(pool: pg.Pool, client: Client, poisonAttempts: number): Promise<number> {
  const r = await retryFailed(pool, client, { poisonAttempts });
  out(`started ${String(r.started)} failed contents again`);
  for (const f of r.failed) process.stderr.write(`could not start ${f.sha256}: ${f.error}\n`);
  return r.failed.length === 0 ? 0 : 1;
}

async function verify(
  layout: DataLayout,
  sample: number | undefined,
  pool: () => pg.Pool,
): Promise<number> {
  const r = await verifyVault(layout, sample);
  const removed = await removedCopies(pool());
  for (const m of r.mismatches) out(`MISMATCH ${m.path} expected ${m.expected} actual ${m.actual}`);
  out(
    `checked ${String(r.checked)} vault files, ${String(r.mismatches.length)} mismatches; ` +
      `skipped ${String(r.skippedCopies)} .copies files still present; ` +
      `${String(removed)} duplicates removed`,
  );
  return r.mismatches.length === 0 ? 0 : 1;
}

const stamp = (d: Date): string =>
  d
    .toISOString()
    .replace(/\.\d+Z$/, '')
    .replace(/[-:]/g, '')
    .replace('T', '-');

/** Dry run (default) or `--apply` of the identical-picture cleanup; writes the CSV report. */
async function dedupeIdentical(deps: Deps, apply: boolean): Promise<number> {
  const ctx: IdenticalCtx = {
    ...deps,
    beat: () => undefined,
    log: {
      info: (m, meta) => process.stderr.write(`${m} ${JSON.stringify(meta)}\n`),
      warn: (m, meta) => process.stderr.write(`warning: ${m} ${JSON.stringify(meta)}\n`),
    },
  };
  // The report exists from the first moment and grows family by family, so an interrupted
  // apply still leaves a record of every file it removed.
  const dir = join(deps.settings.dataDir, 'reports');
  await mkdir(dir, { recursive: true });
  const file = join(dir, `dedupe-identical-${stamp(new Date())}${apply ? '-applied' : ''}.csv`);
  await writeFile(file, reportHeader(apply));
  const r: CleanupSummary = await dedupeIdenticalAll(ctx, {
    apply,
    onRows: (rows) => appendFile(file, reportLines(rows, apply)),
  });
  out(
    `families ${String(r.families)}, ` +
      `${apply ? 'files removed' : 'files to remove'} ${String(r.files)}, ` +
      `${apply ? 'bytes freed' : 'bytes to free'} ${String(r.bytes)}, errors ${String(r.errors)}`,
  );
  out(`report: ${file}`);
  return 0;
}

/** Operator commands; returns the exit code (2 for usage errors). */
export async function main(argv: string[], deps: CliDeps = {}): Promise<number> {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        force: { type: 'boolean', default: false },
        apply: { type: 'boolean', default: false },
        sample: { type: 'string' },
      },
    });
  } catch {
    process.stderr.write(USAGE);
    return 2;
  }
  const [command, ...extra] = parsed.positionals;
  const { force, apply, sample: sampleArg } = parsed.values;
  const sample = sampleArg === undefined ? undefined : Number(sampleArg);
  const ok =
    extra.length === 0 &&
    (!force || command === 'stage') &&
    (!apply || command === 'dedupe-identical') &&
    (sampleArg === undefined ||
      (command === 'verify-vault' && Number.isInteger(sample) && sample! > 0));
  const commands = [
    'stage',
    'retry-failed',
    'verify-vault',
    'doctor',
    'copies',
    'sweep-copies',
    'dedupe-identical',
  ];
  if (!ok || !commands.includes(command ?? '')) {
    process.stderr.write(USAGE);
    return 2;
  }

  const cleanups: (() => Promise<void>)[] = [];
  try {
    const needPool = (): pg.Pool => {
      if (deps.pool) return deps.pool;
      const pool = createPool(loadSettings().databaseUrl, 2);
      cleanups.push(() => pool.end());
      return pool;
    };
    const needLayout = (): DataLayout => deps.layout ?? new DataLayout(loadSettings().dataDir);
    const needClient = async (): Promise<Client> => {
      if (deps.temporal) return deps.temporal;
      const c = await connect();
      cleanups.push(c.close);
      return c.client;
    };
    switch (command) {
      case 'stage':
        return await stage(await needClient(), force);
      case 'retry-failed':
        return await retry(
          needPool(),
          await needClient(),
          deps.poisonAttempts ?? loadSettings().poisonAttempts,
        );
      case 'verify-vault':
        return await verify(needLayout(), sample, needPool);
      case 'copies':
        return await copies(needPool());
      case 'sweep-copies':
        return await sweep(await needClient());
      case 'dedupe-identical': {
        if (deps.identical) return await dedupeIdentical(deps.identical, apply);
        const settings = loadSettings();
        const layout = needLayout();
        await layout.ensure();
        const metadata = new MetadataReader();
        cleanups.push(() => metadata.end());
        // This command owns its decode pool (the worker's is not reachable): terminate it on exit.
        const decodePool = createDecodePool(here('./decodeWorker.js'), settings.cpuConcurrency);
        cleanups.push(async () => {
          await decodePool.terminate();
        });
        return await dedupeIdentical(
          { pool: needPool(), settings, layout, metadata, decodePool },
          apply,
        );
      }
      default:
        out(JSON.stringify(await doctor(needPool(), needLayout())));
        return 0;
    }
  } finally {
    for (const c of cleanups) await c();
  }
}

/** `main`, with any failure reported as one `error:` line (no stack trace) and exit code 1. */
export async function run(argv: string[], deps: CliDeps = {}): Promise<number> {
  try {
    return await main(argv, deps);
  } catch (e) {
    process.stderr.write(`error: ${errText(e)}\n`);
    return 1;
  }
}

const isEntry =
  process.argv[1] !== undefined &&
  realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
if (isEntry) process.exitCode = await run(process.argv.slice(2));
