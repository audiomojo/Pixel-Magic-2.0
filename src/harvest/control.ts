import { WorkflowExecutionAlreadyStartedError, type Client } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common';
import type { RunControl } from './activities.js';

export const HARVEST_TASK_QUEUE = 'harvest';
const workflowId = (runId: string): string => `harvest-${runId}`;

/** Client-side run control (never imported by workflow code). */
export function temporalRunControl(client: Client): RunControl {
  return {
    async start(runId) {
      try {
        await client.workflow.start('HarvestWorkflow', {
          taskQueue: HARVEST_TASK_QUEUE,
          workflowId: workflowId(runId),
          workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
          args: [{ runId }],
        });
      } catch (e) {
        if (!(e instanceof WorkflowExecutionAlreadyStartedError)) throw e;
      }
    },
    async signal(runId, name) {
      await client.workflow.getHandle(workflowId(runId)).signal(name);
    },
    async cancel(runId) {
      await client.workflow.getHandle(workflowId(runId)).cancel();
    },
  };
}
