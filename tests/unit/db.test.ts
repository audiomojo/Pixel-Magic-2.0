import { afterEach, expect, test, vi } from 'vitest';
import { createPool } from '../../src/db.js';
import { logger } from '../../src/logger.js';

afterEach(() => {
  vi.restoreAllMocks();
});

// Chaos (Task 13): pg attaches the whole client to an idle-client error, and pino serialised it
// (≈ 5 KB per idle connection on every Postgres restart).
test('an idle client error logs its code and message, not the pg client', async () => {
  const warn = vi.spyOn(logger, 'warn').mockImplementation(() => undefined);
  const pool = createPool('postgres://postgres@127.0.0.1:1/none');
  const err = Object.assign(new Error('terminating connection due to administrator command'), {
    code: '57P01',
    client: { connectionParameters: { host: '127.0.0.1' }, _queryQueue: [] },
  });
  pool.emit('error', err, {});
  expect(warn).toHaveBeenCalledWith(
    { err: { code: '57P01', message: 'terminating connection due to administrator command' } },
    'idle postgres client error',
  );
  await pool.end();
});
