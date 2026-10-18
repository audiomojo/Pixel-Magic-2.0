import pg from 'pg';

// int8 (OID 20) -> BigInt, so 64-bit hashes, sizes and mtime_ns never lose precision.
pg.types.setTypeParser(20, (v: string) => BigInt(v));

export function createPool(url: string, max = 10): pg.Pool {
  return new pg.Pool({ connectionString: url, max });
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
