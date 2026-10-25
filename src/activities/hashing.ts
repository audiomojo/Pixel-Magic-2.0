import { ApplicationFailure } from '@temporalio/activity';
import workerpool, { type Pool } from 'workerpool';
import type { WireFacts } from '../types.js';
import { toSigned } from '../hashing.js';
import type { DecodeKilledType, DecodeResult } from '../types.js';
import type { Deps } from './index.js';

export type WorkerPool = Pool;

/** A process-worker decode pool. Whoever creates it owns `.terminate()`. */
export function createDecodePool(
  workerPath: string,
  maxWorkers: number,
  forkOpts?: { execArgv?: string[] },
): WorkerPool {
  return workerpool.pool(workerPath, {
    workerType: 'process',
    maxWorkers,
    ...(forkOpts && { forkOpts }),
  });
}

/** Own-property shape of an error after workerpool's serialization (no `instanceof` across processes). */
type WireError = { name?: unknown; message?: unknown; reason?: unknown };

/**
 * The pool's own failures: our timeout, or the child dying (crash, kill, cleanup on timeout).
 * workerpool reports both with these names / messages; anything else came from the decode itself.
 */
function isPoolFailure(e: WireError): boolean {
  if (e.name === 'TimeoutError') return true;
  return (
    typeof e.message === 'string' && /^Workerpool Worker|^Worker (is )?terminat/i.test(e.message)
  );
}

/**
 * The child was killed by a signal (the kernel's OOM killer, an operator, a crash). That says
 * nothing about the file: transient, retried by the workflow up to `MAX_DECODE_ATTEMPTS`, then
 * FAILED `decode_killed` (which `retry-failed` recovers).
 */
function isSignalDeath(e: WireError): boolean {
  return typeof e.message === 'string' && /signalCode: `?SIG[A-Z0-9]+/.test(e.message);
}

/** HASHED/CLUSTERED and QUARANTINED are where a recorded hash can already be. */
const RECORDED: Partial<Record<string, 'HASHED' | 'QUARANTINED'>> = {
  HASHED: 'HASHED',
  CLUSTERED: 'HASHED',
  QUARANTINED: 'QUARANTINED',
};

export function makeHashing(deps: Deps) {
  const { pool, layout } = deps;

  /** Queue `cpu`. No database access and no skip check (spec §12 R4): it always decodes. */
  async function decodeContent(sha256: string, ext: string): Promise<DecodeResult> {
    const abs = layout.vaultFile(sha256, ext);
    try {
      const facts = (await deps.decodePool
        .exec('computeImageFacts', [abs, ext])
        .timeout(deps.settings.decodeTimeoutS * 1000)) as WireFacts;
      return { facts, quarantineReason: null };
    } catch (err) {
      const e = (err ?? {}) as WireError;
      if (e.name === 'QuarantineError' && typeof e.reason === 'string')
        return { facts: null, quarantineReason: e.reason };
      if (e.name !== 'TimeoutError' && isSignalDeath(e))
        throw ApplicationFailure.retryable(
          `decode ${sha256}.${ext}: ${String(e.message)}`,
          'DecodeKilled' satisfies DecodeKilledType,
        );
      if (isPoolFailure(e))
        throw ApplicationFailure.retryable(
          `decode ${sha256}.${ext}: ${String(e.message)}`,
          'Poison',
        );
      throw err;
    }
  }

  /** Queue `io`. Records the decode outcome once; a later call reports the stored outcome. */
  async function recordHash(sha256: string, r: DecodeResult): Promise<'HASHED' | 'QUARANTINED'> {
    let res;
    if (r.facts) {
      const f = r.facts;
      res = await pool.query(
        `UPDATE contents SET phash = $2, dhash = $3, width = $4, height = $5, hashed_at = now(),
                             state = 'HASHED'
          WHERE sha256 = $1 AND state = 'INGESTED'`,
        [sha256, toSigned(BigInt(f.phash)), toSigned(BigInt(f.dhash)), f.width, f.height],
      );
    } else if (r.quarantineReason !== null) {
      res = await pool.query(
        `UPDATE contents SET quarantine_reason = $2, hashed_at = now(), state = 'QUARANTINED'
          WHERE sha256 = $1 AND state = 'INGESTED'`,
        [sha256, r.quarantineReason],
      );
    } else {
      throw ApplicationFailure.nonRetryable(
        `recordHash ${sha256}: neither facts nor a quarantine reason`,
        'BadDecodeResult',
      );
    }
    if (res.rowCount === 1) return r.facts ? 'HASHED' : 'QUARANTINED';
    const cur = await pool.query<{ state: string }>(
      'SELECT state FROM contents WHERE sha256 = $1',
      [sha256],
    );
    const state = cur.rows[0]?.state;
    const done = state === undefined ? undefined : RECORDED[state];
    if (done) return done;
    throw ApplicationFailure.nonRetryable(
      `recordHash ${sha256}: content is ${state ?? 'missing'}`,
      'UnexpectedState',
    );
  }

  /** Queue `io`. First step of a `retry-failed` ContentWorkflow: FAILED → INGESTED (else a no-op). */
  async function reopenFailed(sha256: string): Promise<void> {
    await pool.query(
      `UPDATE contents SET state = 'INGESTED', last_error = NULL WHERE sha256 = $1 AND state = 'FAILED'`,
      [sha256],
    );
  }

  /** Queue `io`. After decode attempts run out: FAILED, unless the row has already moved on. */
  async function markFailed(sha256: string, error: string): Promise<void> {
    await pool.query(
      `UPDATE contents SET state = 'FAILED', last_error = $2 WHERE sha256 = $1 AND state = 'INGESTED'`,
      [sha256, error],
    );
  }

  return { decodeContent, recordHash, markFailed, reopenFailed };
}
