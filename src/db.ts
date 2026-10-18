import pg from 'pg';
import { logger } from './logger.js';

// int8 (OID 20) -> BigInt, so 64-bit hashes, sizes and mtime_ns never lose precision.
pg.types.setTypeParser(20, (v: string) => BigInt(v));

/**
 * Server-side bounds so a hung database call can't stall a worker forever (it fails, and the
 * activity retries): no statement runs longer than 60 s, and a session idle inside an open
 * transaction for 120 s is terminated. Every query in the pipeline is far below both.
 */
export const STATEMENT_TIMEOUT_MS = 60_000;
export const IDLE_IN_TRANSACTION_TIMEOUT_MS = 120_000;

export function createPool(url: string, max = 10): pg.Pool {
  const pool = new pg.Pool({
    connectionString: url,
    max,
    statement_timeout: STATEMENT_TIMEOUT_MS,
    idle_in_transaction_session_timeout: IDLE_IN_TRANSACTION_TIMEOUT_MS,
  });
  // An idle client whose backend goes away (Postgres restart, terminated session) is re-emitted
  // on the pool; unhandled, that 'error' event kills the process. The pool already discarded
  // the client, and the next query opens a fresh connection.
  pool.on('error', (err) => {
    logger.warn({ err }, 'idle postgres client error');
  });
  return pool;
}

export async function withTx<T>(pool: pg.Pool, fn: (c: pg.PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const out = await fn(c);
    await c.query('COMMIT');
    return out;
  } catch (e) {
    await c.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally {
    c.release();
  }
}
