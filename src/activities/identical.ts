// Identical-picture dedupe (spec §4–§5): a still whose full-resolution decode is pixel-for-pixel
// equal to another's is retired to DUPLICATE and its vault file deleted, keeping the better file.
// Nothing is unlinked unless a fresh sha256 of the vault file equals `contents.sha256`; any
// decode failure, missing file or checksum mismatch at compare time retires nothing.
import { lstat, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { ApplicationFailure, heartbeat, log as activityLog } from '@temporalio/activity';
import type pg from 'pg';
import { STATEMENT_TIMEOUT_MS, withTx } from '../db.js';
import { RAW_MIME } from '../formats.js';
import { errText, fsyncDir, sha256File, unlessEnoent } from '../fsops.js';
import { chooseKeeper, type KeeperFacts, type KeeperRule } from '../identical.js';
import type { Deps } from './index.js';

/**
 * What the dedupe functions need beyond `Deps`: a heartbeat and a logger. The activity passes
 * Temporal's; the `dedupe-identical` CLI (no activity context) passes its own.
 */
export interface IdenticalCtx extends Deps {
  beat: (details?: unknown) => void;
  log: {
    info: (message: string, meta?: Record<string, unknown>) => void;
    warn: (message: string, meta?: Record<string, unknown>) => void;
  };
  /** Heartbeat period during a lock wait or decode (default KEEPALIVE_MS; a test seam). */
  keepaliveMs?: number;
}

export type DedupeOutcome = 'UNIQUE' | 'KEPT' | 'DUPLICATE';
/** DEFERRED: the vault file has another link (a staging generation); nothing done, retried later. */
export type RemoveOutcome = 'REMOVED' | 'ALREADY_REMOVED' | 'MISSING' | 'MISMATCH' | 'DEFERRED';
export interface Retired {
  keeper: string;
  retired: { sha256: string; rule: KeeperRule }[];
}

/** RAW extensions: only an embedded preview decodes, so two RAWs can never be proven identical. */
const RAW_EXTS: string[] = [...RAW_MIME.keys()];

/**
 * Bounds for the dedupe transaction, just above the activity's 10-minute startToCloseTimeout:
 * it sits idle in the transaction while files decode, and may wait that long for a lock held by
 * a family compare or a cluster-cache reload. A half-open holder still releases within 11 min.
 */
const LONG_TX = '11min';

/** The suspect-key condition on `s` relative to `n` (spec §3: equal phash, dhash, width, height). */
const SAME_KEY = `s.kind = 'still' AND s.phash = n.phash AND s.dhash = n.dhash
                 AND s.width = n.width AND s.height = n.height`;
/** `s` is a suspect: comparable state, not RAW. */
const SUSPECT = `s.state IN ('HASHED','CLUSTERED') AND lower(s.ext) <> ALL($2::text[])`;
/** `s` was retired but its vault file is not yet dealt with. */
const PENDING = `s.state = 'DUPLICATE' AND s.duplicate_removed_at IS NULL AND NOT s.duplicate_mismatch`;

/** Heartbeat period while waiting on something slow (well under the 60 s heartbeat timeout). */
export const KEEPALIVE_MS = 15_000;

/** Awaits `p`, heartbeating every KEEPALIVE_MS meanwhile: a lock wait or a decode can be long. */
async function beating<T>(ctx: IdenticalCtx, what: string, p: Promise<T>): Promise<T> {
  const t = setInterval(() => ctx.beat(what), ctx.keepaliveMs ?? KEEPALIVE_MS);
  try {
    return await p;
  } finally {
    clearInterval(t);
  }
}

/** Runs `sql` (a lock acquisition) allowed to wait up to LONG_TX, then restores the bound. */
async function waitForLock(
  ctx: IdenticalCtx,
  c: pg.PoolClient,
  sql: string,
  params: unknown[] = [],
): Promise<void> {
  await c.query(`SET LOCAL statement_timeout = '${LONG_TX}'`);
  await beating(ctx, 'waiting for lock', c.query(sql, params));
  await c.query(`SET LOCAL statement_timeout = ${String(STATEMENT_TIMEOUT_MS)}`);
}

/** The transaction-scoped lock serializing every dedupe of one suspect key. */
export async function lockKey(ctx: IdenticalCtx, c: pg.PoolClient, sha256: string): Promise<void> {
  await waitForLock(
    ctx,
    c,
    `SELECT pg_advisory_xact_lock(hashtext('pm.identical:' || phash || ':' || dhash || ':' ||
                                           width || ':' || height))
       FROM contents WHERE sha256 = $1`,
    [sha256],
  );
}

interface Member {
  sha256: string;
  ext: string;
  state: string;
}

type Digest = { width: number; height: number; channels: number; sha256: string };
const show = (d: Digest): string =>
  `${String(d.width)}x${String(d.height)}x${String(d.channels)} ${d.sha256}`;

/**
 * The content plus every suspect proven pixel-identical to it. Pairs without a final verdict
 * (`identical` / `different`) are compared now and recorded in `pixel_compares`; `error` pairs
 * are retried. Run inside the caller's transaction, under the suspect-key lock.
 */
export async function findFamily(
  ctx: IdenticalCtx,
  c: pg.PoolClient,
  sha256: string,
): Promise<string[]> {
  const self = (
    await c.query<Member>('SELECT sha256, ext, state FROM contents WHERE sha256 = $1', [sha256])
  ).rows[0];
  if (!self) throw new Error(`findFamily ${sha256}: no such content`);
  if (RAW_EXTS.includes(self.ext.toLowerCase())) return [sha256];
  const suspects = (
    await c.query<Member & { verdict: string | null }>(
      `SELECT s.sha256, s.ext, s.state, p.verdict
         FROM contents n
         JOIN contents s ON ${SAME_KEY} AND ${SUSPECT} AND s.sha256 <> n.sha256
         LEFT JOIN pixel_compares p ON p.sha_a = least(n.sha256, s.sha256)
                                   AND p.sha_b = greatest(n.sha256, s.sha256)
        WHERE n.sha256 = $1
        ORDER BY s.sha256`,
      [sha256, RAW_EXTS],
    )
  ).rows;

  const digests = new Map<string, Promise<Digest>>();
  /** Fresh sha256 first (a file that is not its content proves nothing), then the decode. */
  const digestOf = (m: Member): Promise<Digest> => {
    let d = digests.get(m.sha256);
    if (!d) {
      d = (async () => {
        const abs = ctx.layout.vaultFile(m.sha256, m.ext);
        const actual = await sha256File(abs, () => ctx.beat());
        if (actual !== m.sha256)
          throw new Error(`${ctx.layout.rel(abs)}: sha256 ${actual}, expected ${m.sha256}`);
        const exec = ctx.decodePool
          .exec('pixelDigest', [abs, m.ext])
          .timeout(ctx.settings.decodeTimeoutS * 1000);
        return (await beating(ctx, 'decoding', Promise.resolve(exec))) as Digest;
      })();
      d.catch(() => undefined); // awaited per pair; never an unhandled rejection
      digests.set(m.sha256, d);
    }
    return d;
  };

  const family = [sha256];
  for (const s of suspects) {
    let verdict = s.verdict;
    if (verdict !== 'identical' && verdict !== 'different') {
      let detail: string | null = null;
      try {
        const [a, b] = await Promise.all([digestOf(self), digestOf(s)]);
        const same =
          a.width === b.width &&
          a.height === b.height &&
          a.channels === b.channels &&
          a.sha256 === b.sha256;
        verdict = same ? 'identical' : 'different';
        if (!same) detail = `${show(a)} vs ${show(b)}`;
      } catch (e) {
        verdict = 'error';
        detail = errText(e);
      }
      const [lo, hi] = sha256 < s.sha256 ? [sha256, s.sha256] : [s.sha256, sha256];
      await c.query(
        `INSERT INTO pixel_compares (sha_a, sha_b, verdict, detail) VALUES ($1, $2, $3, $4)
         ON CONFLICT (sha_a, sha_b)
         DO UPDATE SET verdict = EXCLUDED.verdict, detail = EXCLUDED.detail, compared_at = now()`,
        [lo, hi, verdict, detail],
      );
      if (verdict === 'error')
        ctx.log.warn('pixel compare failed; nothing retired', { a: lo, b: hi, detail });
      ctx.beat({ compared: s.sha256 });
    }
    if (verdict === 'identical') family.push(s.sha256);
  }
  return family;
}

interface MemberFacts extends Member {
  size_bytes: bigint;
  ingested_at: Date;
  live_content_id: string | null;
}

/** Who stays and who goes in a proven family, and by which rule (nothing is written). */
export interface FamilyPlan {
  keeper: string;
  losers: { sha256: string; rule: KeeperRule }[];
}

/**
 * The keeper rule applied to a family (spec §4): reads each member's facts (row + embedded
 * tags) and picks one. Writes nothing; `retireFamily` and the dry run both call it.
 */
export async function planFamily(
  ctx: IdenticalCtx,
  c: pg.PoolClient,
  members: string[],
): Promise<FamilyPlan> {
  if (new Set(members).size < 2) throw new Error('planFamily: a family has at least two members');
  const rows = (
    await c.query<MemberFacts>(
      `SELECT sha256, ext, state, size_bytes, ingested_at, live_content_id FROM contents
        WHERE sha256 = ANY($1::text[]) ORDER BY sha256`,
      [members],
    )
  ).rows;
  if (rows.length !== new Set(members).size)
    throw new Error(`planFamily: ${String(members.length - rows.length)} member(s) missing`);
  const facts: KeeperFacts[] = [];
  for (const r of rows) {
    const tags = await ctx.metadata.keeperTags(ctx.layout.vaultFile(r.sha256, r.ext));
    facts.push({ sha256: r.sha256, ...tags, sizeBytes: r.size_bytes, ingestedAt: r.ingested_at });
    ctx.beat();
  }
  return chooseKeeper(facts);
}

/**
 * One transaction (the caller's, under the suspect-key lock): the keeper rule picks one member;
 * every other becomes DUPLICATE of it, older duplicates of a loser are re-pointed to it, the
 * keeper takes a loser's Live Photo link when it has none, and `cluster_state.version` is bumped
 * when a CLUSTERED content was retired. Lock order is suspect key → `pm.cluster` → rows, the
 * same as `clusterContent`'s `pm.cluster` → rows, so the two never deadlock; holding `pm.cluster`
 * also keeps a concurrent cluster step from committing a cache that still holds a loser (R6).
 */
export async function retireFamily(
  ctx: IdenticalCtx,
  c: pg.PoolClient,
  members: string[],
): Promise<Retired> {
  if (new Set(members).size < 2) throw new Error('retireFamily: a family has at least two members');
  const { keeper, losers } = await planFamily(ctx, c, members);

  await waitForLock(ctx, c, `SELECT pg_advisory_xact_lock(hashtext('pm.cluster'))`);
  const locked = (
    await c.query<MemberFacts>(
      `SELECT sha256, ext, state, size_bytes, ingested_at, live_content_id FROM contents
        WHERE sha256 = ANY($1::text[]) ORDER BY sha256 FOR UPDATE`,
      [members],
    )
  ).rows;
  const bad = locked.filter((r) => r.state !== 'HASHED' && r.state !== 'CLUSTERED');
  if (bad.length > 0)
    throw new Error(`retireFamily: ${bad.map((r) => `${r.sha256} is ${r.state}`).join(', ')}`);
  const loserShas = losers.map((l) => l.sha256);
  for (const l of losers)
    await c.query(
      `UPDATE contents SET state = 'DUPLICATE', duplicate_of = $2, duplicate_rule = $3,
                           duplicate_at = clock_timestamp()
        WHERE sha256 = $1`,
      [l.sha256, keeper, l.rule],
    );
  await c.query(`UPDATE contents SET duplicate_of = $1 WHERE duplicate_of = ANY($2::text[])`, [
    keeper,
    loserShas,
  ]);
  const keeperRow = locked.find((r) => r.sha256 === keeper)!;
  const live = locked.find((r) => loserShas.includes(r.sha256) && r.live_content_id !== null);
  if (keeperRow.live_content_id === null && live)
    await c.query('UPDATE contents SET live_content_id = $2 WHERE sha256 = $1', [
      keeper,
      live.live_content_id,
    ]);
  if (locked.some((r) => loserShas.includes(r.sha256) && r.state === 'CLUSTERED'))
    await c.query('UPDATE cluster_state SET version = version + 1 WHERE id = 1');
  return { keeper, retired: losers };
}

interface RetiredRow {
  state: string;
  ext: string;
  size_bytes: bigint;
  duplicate_of: string | null;
  duplicate_rule: string | null;
  duplicate_removed_at: Date | null;
  duplicate_mismatch: boolean;
}

/** Original names of a content's files, for the removal log. */
async function namesOf(c: pg.PoolClient, sha256: string): Promise<string[]> {
  const r = await c.query<{ n: string }>(
    'SELECT DISTINCT original_name AS n FROM files WHERE sha256 = $1 ORDER BY 1',
    [sha256],
  );
  return r.rows.map((x) => x.n);
}

/**
 * Deletes a retired content's vault file once a fresh sha256 of it equals `contents.sha256` and
 * a fresh sha256 of the keeper's vault file equals `duplicate_of`, then records it. DEFERRED
 * (nothing done, nothing recorded) while another name links the file (R15). Safe to repeat and to run on a row
 * left retired-but-not-removed by a crash; concurrent calls for one content serialize on its
 * row. A file already gone is MISSING (0 bytes reclaimed); any mismatch is flagged and kept.
 */
export async function removeRetired(ctx: IdenticalCtx, sha256: string): Promise<RemoveOutcome> {
  return withTx(ctx.pool, async (c) => {
    await c.query(`SET LOCAL idle_in_transaction_session_timeout = '${LONG_TX}'`);
    const r = (
      await c.query<RetiredRow>(
        `SELECT state, ext, size_bytes, duplicate_of, duplicate_rule, duplicate_removed_at,
                duplicate_mismatch
           FROM contents WHERE sha256 = $1 FOR UPDATE`,
        [sha256],
      )
    ).rows[0];
    if (r?.state !== 'DUPLICATE' || r.duplicate_of === null)
      throw new Error(
        `removeRetired ${sha256}: content is ${r?.state ?? 'missing'}, not DUPLICATE`,
      );
    if (r.duplicate_removed_at !== null) return 'ALREADY_REMOVED';
    if (r.duplicate_mismatch) return 'MISMATCH';
    const keeper = (
      await c.query<{ ext: string; state: string }>(
        'SELECT ext, state FROM contents WHERE sha256 = $1',
        [r.duplicate_of],
      )
    ).rows[0];
    const path = ctx.layout.vaultFile(sha256, r.ext);
    const keptPath = keeper ? ctx.layout.vaultFile(r.duplicate_of, keeper.ext) : null;
    const where = {
      removed: ctx.layout.rel(path),
      removedNames: await namesOf(c, sha256),
      kept: keptPath && ctx.layout.rel(keptPath),
      keptNames: await namesOf(c, r.duplicate_of),
      rule: r.duplicate_rule,
    };
    const flag = async (error: string): Promise<RemoveOutcome> => {
      await c.query(
        `UPDATE contents SET duplicate_mismatch = true, last_error = $2 WHERE sha256 = $1`,
        [sha256, `duplicate_mismatch: ${error}`],
      );
      ctx.log.warn('identical duplicate not removed: mismatch flagged', { ...where, error });
      return 'MISMATCH';
    };
    const deferred = (): RemoveOutcome => {
      ctx.log.info('identical duplicate removal deferred: still linked', where);
      return 'DEFERRED';
    };
    const mark = async (bytes: bigint): Promise<void> => {
      await c.query(
        `UPDATE contents SET duplicate_removed_at = clock_timestamp(),
                             duplicate_reclaimed_bytes = $2
          WHERE sha256 = $1`,
        [sha256, bytes],
      );
    };

    const before = await unlessEnoent(lstat(path, { bigint: true }));
    if (!before) {
      await mark(0n);
      ctx.log.info('identical duplicate already gone', where);
      return 'MISSING';
    }
    if (!before.isFile()) return flag(`${where.removed} is not a regular file`);
    if (!keeper || !keptPath || keeper.state === 'DUPLICATE')
      return flag(`keeper ${r.duplicate_of} is ${keeper?.state ?? 'missing'}`);
    const kept = await unlessEnoent(lstat(keptPath));
    if (!kept?.isFile()) return flag(`keeper vault file ${String(where.kept)} missing`);
    // A staging generation (or a link the user made) still names these bytes: unlinking the vault
    // name would free nothing and leave the other name to be reaped as a user's file. Later: the
    // copy sweep and `dedupe-identical --apply` retry once the generation is gone.
    if (before.nlink > 1n) return deferred();
    const keptSha = await sha256File(keptPath, () => ctx.beat());
    if (keptSha !== r.duplicate_of)
      return flag(`keeper vault sha256 ${keptSha}, expected ${r.duplicate_of}`);
    const actual = await sha256File(path, () => ctx.beat());
    if (actual !== sha256) return flag(`vault sha256 ${actual}, expected ${sha256}`);
    const after = await unlessEnoent(lstat(path, { bigint: true }));
    if (!after || after.size !== before.size || after.mtimeNs !== before.mtimeNs)
      return flag(`${where.removed} changed while hashing`);
    if (after.nlink > 1n) return deferred();
    await unlessEnoent(unlink(path));
    await fsyncDir(dirname(path)); // the unlink is durable before the row says so
    await mark(before.size);
    ctx.log.info('identical duplicate removed', { ...where, bytes: String(before.size) });
    return 'REMOVED';
  });
}

interface Pre {
  state: string;
  kind: string;
  ext: string;
  phash: bigint | null;
  suspects: boolean;
  pending: boolean;
}

/** Retired contents of `sha256`'s suspect key whose vault file is not yet dealt with. */
async function pendingInKey(ctx: IdenticalCtx, sha256: string): Promise<string[]> {
  const r = await ctx.pool.query<{ sha256: string }>(
    `SELECT s.sha256 FROM contents n JOIN contents s ON ${SAME_KEY} AND ${PENDING}
      WHERE n.sha256 = $1 ORDER BY s.sha256`,
    [sha256],
  );
  return r.rows.map((x) => x.sha256);
}

/** `dedupeIdentical` over an explicit context (the activity wraps it). */
export async function dedupeIdenticalWith(
  ctx: IdenticalCtx,
  sha256: string,
): Promise<DedupeOutcome> {
  const pre = (
    await ctx.pool.query<Pre>(
      `SELECT n.state, n.kind, n.ext, n.phash,
              EXISTS (SELECT 1 FROM contents s
                       WHERE ${SAME_KEY} AND ${SUSPECT} AND s.sha256 <> n.sha256) AS suspects,
              EXISTS (SELECT 1 FROM contents s WHERE ${SAME_KEY} AND ${PENDING}) AS pending
         FROM contents n WHERE n.sha256 = $1`,
      [sha256, RAW_EXTS],
    )
  ).rows[0];
  if (pre?.state === 'DUPLICATE') {
    await removePending(ctx, [sha256]);
    return 'DUPLICATE';
  }
  if (pre?.state !== 'HASHED' && pre?.state !== 'CLUSTERED')
    throw ApplicationFailure.nonRetryable(
      `dedupeIdentical ${sha256}: content is ${pre?.state ?? 'missing'}, not HASHED`,
      'UnexpectedState',
    );
  if (pre.kind !== 'still' || pre.phash === null || RAW_EXTS.includes(pre.ext.toLowerCase()))
    return 'UNIQUE';
  if (!pre.suspects) {
    if (pre.pending) await removePending(ctx, await pendingInKey(ctx, sha256)); // a crash's leftovers
    return 'UNIQUE';
  }

  const out = await withTx(ctx.pool, async (c) => {
    // Idle in the transaction while files decode: see LONG_TX.
    await c.query(`SET LOCAL idle_in_transaction_session_timeout = '${LONG_TX}'`);
    await lockKey(ctx, c, sha256);
    const now = (
      await c.query<{ state: string }>('SELECT state FROM contents WHERE sha256 = $1', [sha256])
    ).rows[0]?.state;
    if (now === 'DUPLICATE') return null; // a concurrent newcomer of this key retired it
    const family = await findFamily(ctx, c, sha256);
    return family.length > 1 ? retireFamily(ctx, c, family) : { keeper: sha256, retired: [] };
  });
  if (out && out.retired.length > 0) await ctx.checkpoint?.('identical_retired');
  await removePending(ctx, await pendingInKey(ctx, sha256));
  if (out === null || out.retired.some((r) => r.sha256 === sha256)) return 'DUPLICATE';
  return out.retired.length > 0 ? 'KEPT' : 'UNIQUE';
}

/** `removeRetired` for each (a no-op for one already removed or flagged). */
async function removePending(ctx: IdenticalCtx, shas: string[]): Promise<void> {
  for (const s of shas) {
    await removeRetired(ctx, s);
    ctx.beat();
  }
}

// ---------------------------------------------------------------- the one-time cleanup (spec §8)

export const REPORT_COLUMNS = [
  'keeper_sha256',
  'keeper_vault_path',
  'keeper_names',
  'removed_sha256',
  'removed_vault_path',
  'removed_names',
  'rule',
  'bytes',
] as const;
export type ReportRow = Record<(typeof REPORT_COLUMNS)[number], string | number> & {
  /** Apply only: what `removeRetired` did with this file (a 9th column of the applied report). */
  outcome?: RemoveOutcome;
};

export interface CleanupSummary {
  families: number;
  /** Apply: files actually removed (outcome REMOVED); dry run: losers that would be. */
  files: number;
  /** Bytes freed (apply: actually reclaimed) or that would be. */
  bytes: number;
  /** Apply: removals DEFERRED (still linked by a staging generation; finished later). */
  deferred: number;
  /**
   * Pairs that failed to compare (retried next run), families that failed to plan or retire
   * (nothing retired; retried next run); under apply also MISMATCH/MISSING removals.
   */
  errors: number;
  rows: ReportRow[];
  errorRows: ErrorRow[];
}

/** One error of a cleanup run: a pair (sha_a, sha_b), a family (members) or a removal. */
export interface ErrorRow {
  sha_a: string;
  sha_b: string;
  /** A failed family's members, `;`-separated. */
  members: string;
  error: string;
}

export const ERROR_COLUMNS = ['sha_a', 'sha_b', 'members', 'error'] as const;

/** The errors file's header. */
export const errorsHeader = (): string => ERROR_COLUMNS.join(',') + '\n';

/** Errors file lines (no header), one per error. */
export const errorLines = (rows: ErrorRow[]): string =>
  rows.map((r) => ERROR_COLUMNS.map((k) => csvField(r[k])).join(',') + '\n').join('');

/** `field` quoted for CSV only when it must be. */
const csvField = (v: string | number): string => {
  const t = String(v);
  return /[",\n\r]/.test(t) ? `"${t.replaceAll('"', '""')}"` : t;
};

/** The report header; the applied report has a 9th column, `outcome`. */
export function reportHeader(applied: boolean): string {
  return [...REPORT_COLUMNS, ...(applied ? ['outcome'] : [])].join(',') + '\n';
}

/** Report lines (no header), one per file. */
export function reportLines(rows: ReportRow[], applied: boolean): string {
  return rows
    .map(
      (r) =>
        [...REPORT_COLUMNS.map((k) => csvField(r[k])), ...(applied ? [r.outcome ?? ''] : [])].join(
          ',',
        ) + '\n',
    )
    .join('');
}

/** The whole report: header plus one line per file. */
export function reportCsv(rows: ReportRow[], applied = false): string {
  return reportHeader(applied) + reportLines(rows, applied);
}

/** Report rows for a family's losers, read inside the family's transaction. */
async function reportRows(
  ctx: IdenticalCtx,
  c: pg.PoolClient,
  keeper: string,
  losers: { sha256: string; rule: string }[],
): Promise<ReportRow[]> {
  const info = async (sha: string) => {
    const r = (
      await c.query<{ ext: string; size_bytes: bigint }>(
        'SELECT ext, size_bytes FROM contents WHERE sha256 = $1',
        [sha],
      )
    ).rows[0]!;
    return {
      path: ctx.layout.rel(ctx.layout.vaultFile(sha, r.ext)),
      names: (await namesOf(c, sha)).join(';'),
      bytes: Number(r.size_bytes),
    };
  };
  const k = await info(keeper);
  const rows: ReportRow[] = [];
  for (const l of losers) {
    const x = await info(l.sha256);
    rows.push({
      keeper_sha256: keeper,
      keeper_vault_path: k.path,
      keeper_names: k.names,
      removed_sha256: l.sha256,
      removed_vault_path: x.path,
      removed_names: x.names,
      rule: l.rule,
      bytes: x.bytes,
    });
  }
  return rows;
}

/**
 * Every suspect family in the vault. Dry run (`apply: false`): finds families and picks keepers
 * but retires and deletes nothing; its only database writes are `pixel_compares` rows. Apply:
 * first finishes any retire-without-unlink left by a crash, then retires family by family (one
 * transaction each, under the suspect-key lock) and removes the losers' vault files.
 * Both modes take the suspect-key lock (a dry run holds it only while comparing that key), so a
 * running worker never races a scan. Re-runnable: a retired content is no longer a suspect.
 */
export async function dedupeIdenticalAll(
  ctx: IdenticalCtx,
  opts: {
    apply: boolean;
    onRows?: (rows: ReportRow[]) => void | Promise<void>;
    onError?: (row: ErrorRow) => void | Promise<void>;
  },
): Promise<CleanupSummary> {
  const out: CleanupSummary = {
    families: 0,
    files: 0,
    bytes: 0,
    deferred: 0,
    errors: 0,
    rows: [],
    errorRows: [],
  };
  const failed = new Set<string>();
  const error = (row: ErrorRow): void => {
    out.errorRows.push(row);
    out.errors += 1;
  };
  let flushed = 0;
  /** Hands the errors recorded since the last call to `onError`, in order. */
  const flush = async (): Promise<void> => {
    for (; flushed < out.errorRows.length; flushed++) await opts.onError?.(out.errorRows[flushed]!);
  };
  const run: IdenticalCtx = {
    ...ctx,
    log: {
      info: ctx.log.info,
      warn: (m, meta) => {
        const [a, b, detail] = [meta?.['a'], meta?.['b'], meta?.['detail']];
        if (typeof a === 'string' && typeof b === 'string' && !failed.has(`${a}:${b}`)) {
          failed.add(`${a}:${b}`);
          error({
            sha_a: a,
            sha_b: b,
            members: '',
            error: typeof detail === 'string' ? detail : m,
          });
        }
        ctx.log.warn(m, meta);
      },
    },
  };
  /**
   * Applied rows: only a real removal frees bytes or counts as removed; DEFERRED is its own
   * count (finished later); MISSING/MISMATCH are errors.
   */
  const settle = async (row: ReportRow): Promise<void> => {
    if (row.outcome === 'REMOVED') out.files += 1;
    else {
      row.bytes = 0;
      if (row.outcome === 'DEFERRED') out.deferred += 1;
      if (row.outcome === 'MISMATCH' || row.outcome === 'MISSING') {
        const why =
          row.outcome === 'MISMATCH'
            ? (
                await ctx.pool.query<{ e: string | null }>(
                  'SELECT last_error AS e FROM contents WHERE sha256 = $1',
                  [row.removed_sha256],
                )
              ).rows[0]?.e
            : null;
        error({
          sha_a: String(row.removed_sha256),
          sha_b: String(row.keeper_sha256),
          members: '',
          error: `removal ${row.outcome}${why ? `: ${why}` : ': vault file already gone'}`,
        });
      }
    }
    out.bytes += Number(row.bytes);
  };

  if (opts.apply) {
    const pending = await ctx.pool.query<{ sha256: string }>(
      `SELECT s.sha256 FROM contents s WHERE ${PENDING} ORDER BY s.sha256`,
    );
    for (const { sha256 } of pending.rows) {
      const c = await ctx.pool.connect();
      let row: ReportRow;
      try {
        const d = (
          await c.query<{ duplicate_of: string; duplicate_rule: string }>(
            'SELECT duplicate_of, duplicate_rule FROM contents WHERE sha256 = $1',
            [sha256],
          )
        ).rows[0]!;
        row = (await reportRows(run, c, d.duplicate_of, [{ sha256, rule: d.duplicate_rule }]))[0]!;
      } finally {
        c.release();
      }
      row.outcome = await removeRetired(run, sha256);
      await settle(row);
      ctx.beat();
      out.rows.push(row);
      await opts.onRows?.([row]);
      await flush();
    }
  }

  const keys = await ctx.pool.query<{ sha256: string; n: number }>(
    `SELECT min(s.sha256) AS sha256, count(*)::int AS n
       FROM contents s
      WHERE s.kind = 'still' AND s.phash IS NOT NULL AND ${SUSPECT.replace('$2', '$1')}
      GROUP BY s.phash, s.dhash, s.width, s.height
     HAVING count(*) > 1
      ORDER BY min(s.sha256)`,
    [RAW_EXTS],
  );
  for (const key of keys.rows) {
    const members = (
      await ctx.pool.query<{ sha256: string }>(
        `SELECT s.sha256 FROM contents n JOIN contents s ON ${SAME_KEY} AND ${SUSPECT}
          WHERE n.sha256 = $1 ORDER BY s.sha256`,
        [key.sha256, RAW_EXTS],
      )
    ).rows.map((r) => r.sha256);
    const handled = new Set<string>();
    for (const sha of members) {
      if (handled.has(sha)) continue;
      handled.add(sha);
      let family = [sha];
      let done;
      try {
        done = await withTx(ctx.pool, async (c) => {
          await c.query(`SET LOCAL idle_in_transaction_session_timeout = '${LONG_TX}'`);
          await lockKey(run, c, sha);
          const now = (
            await c.query<{ state: string }>('SELECT state FROM contents WHERE sha256 = $1', [sha])
          ).rows[0]?.state;
          if (now !== 'HASHED' && now !== 'CLUSTERED') return null; // retired meanwhile
          family = await findFamily(run, c, sha);
          for (const f of family) handled.add(f);
          if (family.length < 2) return null;
          const plan = opts.apply
            ? await retireFamily(run, c, family).then((r) => ({
                keeper: r.keeper,
                losers: r.retired,
              }))
            : await planFamily(run, c, family);
          return { plan, rows: await reportRows(run, c, plan.keeper, plan.losers) };
        });
      } catch (e) {
        // One family's failure (exiftool, a lock timeout, a member gone) retires nothing of it
        // (the transaction rolled back) and must not stop the others: recorded, retried next run.
        ctx.log.warn('family not processed; nothing retired', {
          members: family,
          error: errText(e),
        });
        error({ sha_a: '', sha_b: '', members: family.join(';'), error: errText(e) });
        await flush();
        continue;
      }
      await flush();
      if (!done) continue;
      out.families += 1;
      if (opts.apply) {
        await ctx.checkpoint?.('identical_retired');
        for (const row of done.rows) {
          row.outcome = await removeRetired(run, String(row.removed_sha256));
          await settle(row);
          ctx.beat();
        }
      } else {
        out.files += done.rows.length;
        out.bytes += done.rows.reduce((n, r) => n + Number(r.bytes), 0);
      }
      out.rows.push(...done.rows);
      await opts.onRows?.(done.rows);
      await flush();
    }
  }
  return out;
}

export function makeIdentical(deps: Deps) {
  /**
   * Queue `cpu` (it decodes), heartbeating. After `recordHash`, before `clusterContent`: no
   * suspects → UNIQUE (one indexed query); else, under the suspect-key lock, compares, retires
   * all but the keeper, and after commit deletes the losers' vault files. DUPLICATE when this
   * content itself was retired (also on a rerun, which finishes a pending removal).
   */
  async function dedupeIdentical(sha256: string): Promise<DedupeOutcome> {
    return dedupeIdenticalWith(
      { ...deps, beat: (d?: unknown) => heartbeat(d), log: activityLog },
      sha256,
    );
  }
  return { dedupeIdentical };
}
