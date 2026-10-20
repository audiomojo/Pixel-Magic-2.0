import { WorkflowExecutionAlreadyStartedError, type Client } from '@temporalio/client';
import { WorkflowIdReusePolicy, WorkflowNotFoundError } from '@temporalio/common';
import type { RunControl } from './activities.js';

export const HARVEST_TASK_QUEUE = 'harvest';
const workflowId = (runId: string): string => `harvest-${runId}`;

/** Client-side run control (never imported by workflow code). */
export function temporalRunControl(client: Client): RunControl {
  async function describe(runId: string): Promise<'running' | 'closed' | 'missing'> {
    try {
      const d = await client.workflow.getHandle(workflowId(runId)).describe();
      return d.status.name === 'RUNNING' ? 'running' : 'closed';
    } catch (e) {
      if (e instanceof WorkflowNotFoundError) return 'missing';
      throw e;
    }
  }
  return {
    async start(runId) {
      try {
        await client.workflow.start('HarvestWorkflow', {
          taskQueue: HARVEST_TASK_QUEUE,
          workflowId: workflowId(runId),
          workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
          args: [{ runId }],
        });
        return 'started';
      } catch (e) {
        if (!(e instanceof WorkflowExecutionAlreadyStartedError)) throw e;
        // REJECT_DUPLICATE also refuses an id whose workflow already ended.
        return (await describe(runId)) === 'running' ? 'running' : 'closed';
      }
    },
    describe,
    async signal(runId, name) {
      await client.workflow.getHandle(workflowId(runId)).signal(name);
    },
    async cancel(runId) {
      try {
        await client.workflow.getHandle(workflowId(runId)).cancel();
        return 'requested';
      } catch (e) {
        // Not found, or "workflow execution already completed".
        if (e instanceof WorkflowNotFoundError) return 'gone';
        throw e;
      }
    },
  };
}
