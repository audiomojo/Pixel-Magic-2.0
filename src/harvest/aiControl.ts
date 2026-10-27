import type { Client } from '@temporalio/client';
import {
  WorkflowIdConflictPolicy,
  WorkflowIdReusePolicy,
  WorkflowNotFoundError,
} from '@temporalio/common';

/** The Python AI worker's queue, training workflow and describe schedule (spec §5). */
export const AI_TASK_QUEUE = 'ai';
const TRAIN_WORKFLOW_ID = 'ai-train';
const DESCRIBE_SCHEDULE_ID = 'ai-describe';

/** What the harvester's Picks API asks of the AI worker (tests fake it). */
export interface AiControl {
  /** Starts TrainWorkflow; 'running' when one was already running (nothing new started). */
  startTrain(): Promise<'started' | 'running'>;
  setDescribePaused(paused: boolean): Promise<void>;
  describePaused(): Promise<boolean>;
}

/** Client-side AI control (never imported by workflow code). */
export function temporalAiControl(client: Client): AiControl {
  async function trainRunning(): Promise<boolean> {
    try {
      const d = await client.workflow.getHandle(TRAIN_WORKFLOW_ID).describe();
      return d.status.name === 'RUNNING';
    } catch (e) {
      if (e instanceof WorkflowNotFoundError) return false;
      throw e;
    }
  }
  const schedule = () => client.schedule.getHandle(DESCRIBE_SCHEDULE_ID);
  return {
    async startTrain() {
      if (await trainRunning()) return 'running';
      // USE_EXISTING: a run started between the describe and here is joined, not an error.
      await client.workflow.start('TrainWorkflow', {
        taskQueue: AI_TASK_QUEUE,
        workflowId: TRAIN_WORKFLOW_ID,
        workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
        workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
        args: [],
      });
      return 'started';
    },
    async setDescribePaused(paused) {
      if (paused) await schedule().pause('paused from the Picks tab');
      else await schedule().unpause('resumed from the Picks tab');
    },
    async describePaused() {
      return (await schedule().describe()).state.paused;
    },
  };
}
