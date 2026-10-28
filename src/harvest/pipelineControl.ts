import { randomUUID } from 'node:crypto';
import type { Client } from '@temporalio/client';
import type { PurgeSyncWorkflow } from '../workflows.js';

/** What the harvester's Picks API asks of the main pipeline worker (tests fake it). */
export interface PipelineControl {
  /**
   * Starts PurgeSyncWorkflow for these shas (purge spec §5 step 3): append the purge log, rebuild
   * staging, remove the vault files. `[]` = just append the log (after an unblock).
   */
  startPurgeSync(shas: string[]): Promise<void>;
}

/** Client-side pipeline control (workflow code is imported as a type only). */
export function temporalPipelineControl(client: Client): PipelineControl {
  return {
    async startPurgeSync(shas) {
      await client.workflow.start<typeof PurgeSyncWorkflow>('PurgeSyncWorkflow', {
        taskQueue: 'main',
        workflowId: `pm-purge-${randomUUID()}`,
        args: [shas],
      });
    },
  };
}
