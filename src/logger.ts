import { pino } from 'pino';

/**
 * With LOG_FILE set (the harvester host sets it, see harvest/hostLogEnv.ts) logs go to that file
 * through a reopenable destination, so the host can rotate it; otherwise to stdout as before.
 */
export const logDestination = process.env['LOG_FILE']
  ? pino.destination({ dest: process.env['LOG_FILE'], mkdir: true, sync: false })
  : undefined;

export const logger = pino({ level: process.env['LOG_LEVEL'] ?? 'info' }, logDestination);
