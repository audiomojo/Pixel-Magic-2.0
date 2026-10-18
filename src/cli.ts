import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Client, Connection } from '@temporalio/client';
import { loadSettings } from './config.js';
import type { StagingWorkflow } from './workflows.js';

export interface CliDeps {
  temporal?: Client;
}

const USAGE = 'usage: cli stage [--force]\n';

async function connect(): Promise<{ client: Client; close: () => Promise<void> }> {
  const s = loadSettings();
  const connection = await Connection.connect({ address: s.temporalAddress });
  return {
    client: new Client({ connection, namespace: s.temporalNamespace }),
    close: () => connection.close(),
  };
}

async function stage(client: Client, force: boolean): Promise<number> {
  const result = await client.workflow.execute<typeof StagingWorkflow>('StagingWorkflow', {
    taskQueue: 'main',
    workflowId: `pm-stage-cli-${String(Date.now())}`,
    args: [force],
  });
  process.stdout.write(JSON.stringify(result) + '\n');
  return 0;
}

/** Operator commands; returns the exit code (2 for usage errors). */
export async function main(argv: string[], deps: CliDeps = {}): Promise<number> {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: { force: { type: 'boolean', default: false } },
    });
  } catch {
    process.stderr.write(USAGE);
    return 2;
  }
  const [command] = parsed.positionals;
  if (command !== 'stage' || parsed.positionals.length !== 1) {
    process.stderr.write(USAGE);
    return 2;
  }
  if (deps.temporal) return stage(deps.temporal, parsed.values.force);
  const { client, close } = await connect();
  try {
    return await stage(client, parsed.values.force);
  } finally {
    await close();
  }
}

const isEntry =
  process.argv[1] !== undefined &&
  realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
if (isEntry) process.exitCode = await main(process.argv.slice(2));
