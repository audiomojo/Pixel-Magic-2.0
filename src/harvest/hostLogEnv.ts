// Imported first by host.ts: ES modules evaluate in import order, so this runs before
// ../logger.js and points it at DATA_DIR/logs/harvester.log (pino creates the directory).
import { join } from 'node:path';
import { loadHarvestSettings } from './config.js';

process.env['LOG_FILE'] ??= join(loadHarvestSettings().dataDir, 'logs', 'harvester.log');
