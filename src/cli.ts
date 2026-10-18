import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Client, Connection } from '@temporalio/client';
import type pg from 'pg';
import { loadSettings } from './config.js';
import { createPool } from './db.js';
import { DataLayout } from './fsops.js';
import { doctor, retryFailed, verifyVault } from './operator.js';
import type { StagingWorkflow } from './workflows.js';

export interface CliDeps {
  temporal?: Client;
  pool?: pg.Pool;
  layout?: DataLayout;
  /** `POISON_ATTEMPTS` for retried contents; defaults to the configured value. */
  poisonAttempts?: number;
}

const USAGE = 'usage: cli stage [--force] | retry-failed | verify-vault [--sample N] | doctor\n';

async function connect(): Promise<{ client: Client; close: () => Promise<void> }> {
  const s = loadSettings();
  const connection = await Connection.connect({ address: s.temporalAddress });
  return {
    client: new Client({ connection, namespace: s.temporalNamespace }),
    close: () => connection.close(),
  };
}

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

async function retry(pool: pg.Pool, client: Client, poisonAttempts: number): Promise<number> {
  const r = await retryFailed(pool, client, { poisonAttempts });
  out(`started ${String(r.started)} failed contents again`);
  for (const f of r.failed) process.stderr.write(`could not start ${f.sha256}: ${f.error}\n`);
  return r.failed.length === 0 ? 0 : 1;
}

async function verify(layout: DataLayout, sample: number | undefined): Promise<number> {
  const r = await verifyVault(layout, sample);
  for (const m of r.mismatches) out(`MISMATCH ${m.path} expected ${m.expected} actual ${m.actual}`);
  out(
    `checked ${String(r.checked)} vault files, ${String(r.mismatches.length)} mismatches; ` +
      `skipped ${String(r.skippedCopies)} .copies files`,
  );
  return r.mismatches.length === 0 ? 0 : 1;
}

/** Operator commands; returns the exit code (2 for usage errors). */
export async function main(argv: string[], deps: CliDeps = {}): Promise<number> {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: { force: { type: 'boolean', default: false }, sample: { type: 'string' } },
    });
  } catch {
    process.stderr.write(USAGE);
    return 2;
  }
  const [command, ...extra] = parsed.positionals;
  const { force, sample: sampleArg } = parsed.values;
  const sample = sampleArg === undefined ? undefined : Number(sampleArg);
  const ok =
    extra.length === 0 &&
    (force ? command === 'stage' : true) &&
    (sampleArg === undefined ||
      (command === 'verify-vault' && Number.isInteger(sample) && sample! > 0));
  if (!ok || !['stage', 'retry-failed', 'verify-vault', 'doctor'].includes(command ?? '')) {
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
        return await verify(needLayout(), sample);
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
    process.stderr.write(`error: ${e instanceof Error ? e.message : String(e)}\n`);
    return 1;
  }
}

const isEntry =
  process.argv[1] !== undefined &&
  realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
if (isEntry) process.exitCode = await run(process.argv.slice(2));
