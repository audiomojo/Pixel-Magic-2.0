import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { runner } from 'node-pg-migrate';
import type pg from 'pg';
import { createPool } from '../../src/db.js';

export interface PgHandle {
  url: string;
  pool: pg.Pool;
  truncate: () => Promise<void>;
  stop: () => Promise<void>;
}

/** Throwaway Postgres 16 with all migrations applied. Start once per test file. */
export async function startPg(): Promise<PgHandle> {
  const container = await new PostgreSqlContainer('postgres:16').start();
  const url = container.getConnectionUri();
  await runner({
    databaseUrl: url,
    dir: 'migrations',
    direction: 'up',
    migrationsTable: 'pgmigrations',
    log: () => {},
  });
  const pool = createPool(url, 5);
  return {
    url,
    pool,
    async truncate() {
      await pool.query('TRUNCATE files, contents, groups, staging_builds RESTART IDENTITY');
      await pool.query('UPDATE cluster_state SET version = 0 WHERE id = 1');
      await pool.query(
        'UPDATE scan_stats SET pending_files = 0, pending_bytes = 0, scanned_at = NULL WHERE id = 1',
      );
      await pool.query(
        'TRUNCATE harvest_run_folders, harvest_items, harvest_runs, harvest_folders, harvest_sources RESTART IDENTITY CASCADE',
      );
      await pool.query(
        'UPDATE harvest_host_state SET heartbeat_at = NULL, version = NULL WHERE id = 1',
      );
      await pool.query('UPDATE copy_sweep_state SET after_id = 0 WHERE id = 1');
    },
    async stop() {
      await pool.end();
      await container.stop();
    },
  };
}
