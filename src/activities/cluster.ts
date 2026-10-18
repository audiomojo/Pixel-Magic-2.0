import { ApplicationFailure } from '@temporalio/activity';
import type pg from 'pg';
import { classify, type CaptureKey, type Facts, type Thresholds } from '../classify.js';
import { withTx } from '../db.js';
import { Grouper, type Assignment } from '../grouping.js';
import { HashIndex } from '../hashIndex.js';
import { toUnsigned } from '../hashing.js';
import type { Deps } from './index.js';

/** The one place DB capture columns become `Facts.captured`: whole UTC seconds plus the subsec. */
export function captureKey(capturedAt: Date | null, subsec: string | null): CaptureKey | null {
  return capturedAt === null ? null : { s: Math.floor(capturedAt.getTime() / 1000), sub: subsec };
}

interface ContentRow {
  sha256: string;
  state: string;
  phash: bigint | null;
  dhash: bigint | null;
  width: number | null;
  height: number | null;
  mime: string | null;
  captured_at: Date | null;
  subsec: string | null;
  camera: string | null;
  burst_uuid: string | null;
  copy_group_id: bigint | null;
  copy_ambiguous: boolean;
}

const COLS = `sha256, state, phash, dhash, width, height, mime, captured_at, subsec, camera,
              burst_uuid, copy_group_id, copy_ambiguous`;

function toFacts(r: ContentRow): Facts {
  if (r.phash === null || r.dhash === null || r.width === null || r.height === null)
    throw new Error(`content ${r.sha256} is ${r.state} but has no hashes or dimensions`);
  return {
    sha256: r.sha256,
    phash: toUnsigned(r.phash),
    dhash: toUnsigned(r.dhash),
    width: r.width,
    height: r.height,
    mime: r.mime ?? '', // unknown mime compares equal to unknown mime
    captured: captureKey(r.captured_at, r.subsec),
    camera: r.camera,
    burstUuid: r.burst_uuid,
  };
}

const num = (v: bigint | null): number | null => (v === null ? null : Number(v));

interface Cache {
  version: bigint;
  grouper: Grouper;
  index: HashIndex;
}

export function makeClusterContent(deps: Deps) {
  const { pool } = deps;
  const t: Thresholds = {
    lookAlikePhash: deps.settings.lookAlikePhash,
    lookAlikeDhash: deps.settings.lookAlikeDhash,
    copyPhash: deps.settings.copyPhash,
    copyDhash: deps.settings.copyDhash,
  };
  /** Group ids prefetched inside the lock; `Grouper`'s synchronous `newId` takes from here. */
  const ids: number[] = [];
  const newId = (): number => {
    const id = ids.shift();
    if (id === undefined) throw new Error('clusterContent: ran out of prefetched group ids');
    return id;
  };
  let cache: Cache | null = null;
  /** Calls on one worker run one at a time: the cache is shared and updated after COMMIT. */
  let tail: Promise<unknown> = Promise.resolve();

  /** Rebuilds index and grouper from the stored clustering, resolving every id to its root. */
  async function load(c: pg.PoolClient, version: bigint): Promise<Cache> {
    const groups = await c.query<{
      id: bigint;
      merged_into: bigint | null;
      similar_set_id: bigint | null;
    }>('SELECT id, merged_into, similar_set_id FROM groups');
    const parent = new Map(groups.rows.map((g) => [Number(g.id), num(g.merged_into)]));
    const similarOf = new Map(groups.rows.map((g) => [Number(g.id), num(g.similar_set_id)]));
    const root = (id: number): number => {
      for (let x = id, hops = 0; ; hops++) {
        const p = parent.get(x);
        if (p === undefined || p === null) return x;
        if (hops > parent.size) throw new Error(`groups: merged_into cycle at ${id}`);
        x = p;
      }
    };
    const rows = await c.query<ContentRow>(
      `SELECT ${COLS} FROM contents WHERE state = 'CLUSTERED' ORDER BY sha256`,
    );
    const grouper = new Grouper(newId);
    const facts: Facts[] = [];
    const copyGroups: number[] = [];
    for (const r of rows.rows) {
      const f = toFacts(r);
      if (r.copy_group_id === null)
        throw new Error(`content ${r.sha256} is CLUSTERED without a group`);
      const copyGroup = root(Number(r.copy_group_id));
      const s = similarOf.get(copyGroup) ?? null;
      grouper.load({
        sha256: r.sha256,
        copyGroup,
        similarSet: s === null ? null : root(s),
        captured: f.captured,
        camera: f.camera,
        burstUuid: f.burstUuid,
        ambiguous: r.copy_ambiguous,
      });
      facts.push(f);
      copyGroups.push(copyGroup);
    }
    const index = new HashIndex();
    index.append(facts, copyGroups);
    // Copy verdicts of key-less contents, so a later flag can propagate to copies of a bridge.
    for (const f of facts) {
      if (f.captured !== null || f.burstUuid !== null) continue;
      for (const m of index.matches(f, t))
        if (m.sha256 !== f.sha256 && classify(f, m, t) === 'copy')
          grouper.loadCopyEdge(f.sha256, m.sha256);
    }
    return { version, grouper, index };
  }

  /** Writes one `Assignment`, keeping every stored copy-group and similar-set id a root. */
  async function persist(c: pg.PoolClient, sha256: string, asg: Assignment): Promise<void> {
    for (const [id, kind] of asg.created)
      await c.query('INSERT INTO groups (id, kind) VALUES ($1, $2)', [id, kind]);
    for (const [loser, winner] of asg.copyMerges) {
      await c.query('UPDATE groups SET merged_into = $2 WHERE id = $1', [loser, winner]);
      await c.query('UPDATE contents SET copy_group_id = $2 WHERE copy_group_id = $1', [
        loser,
        winner,
      ]);
    }
    for (const [loser, winner] of asg.similarMerges) {
      await c.query('UPDATE groups SET merged_into = $2 WHERE id = $1', [loser, winner]);
      await c.query('UPDATE groups SET similar_set_id = $2 WHERE similar_set_id = $1', [
        loser,
        winner,
      ]);
    }
    if (asg.flagged.length > 0)
      await c.query('UPDATE contents SET copy_ambiguous = true WHERE sha256 = ANY($1::text[])', [
        asg.flagged,
      ]);
    for (const [group, set] of asg.similarLinks)
      await c.query('UPDATE groups SET similar_set_id = $2 WHERE id = $1', [group, set]);
    const r = await c.query(
      `UPDATE contents SET copy_group_id = $2, copy_ambiguous = $3, state = 'CLUSTERED',
                           clustered_at = now()
        WHERE sha256 = $1 AND state = 'HASHED'`,
      [sha256, asg.copyGroup, asg.ambiguous],
    );
    if (r.rowCount !== 1) throw new Error(`content ${sha256} left HASHED under the cluster lock`);
  }

  async function clusterOnce(sha256: string): Promise<number> {
    try {
      const out = await withTx(pool, async (c) => {
        await c.query(`SELECT pg_advisory_xact_lock(hashtext('pm.cluster'))`);
        const row = (
          await c.query<ContentRow>(`SELECT ${COLS} FROM contents WHERE sha256 = $1`, [sha256])
        ).rows[0];
        if (row?.state === 'CLUSTERED' && row.copy_group_id !== null)
          return { group: Number(row.copy_group_id), added: null };
        if (row?.state !== 'HASHED')
          throw ApplicationFailure.nonRetryable(
            `clusterContent ${sha256}: content is ${row?.state ?? 'missing'}, not HASHED`,
            'UnexpectedState',
          );
        const v = await c.query<{ version: bigint }>(
          'SELECT version FROM cluster_state WHERE id = 1',
        );
        const version = v.rows[0]!.version;
        if (cache?.version !== version) {
          cache = null;
          cache = await load(c, version);
        }
        const prefetched = await c.query<{ id: bigint }>(
          `SELECT nextval('groups_id_seq') AS id FROM generate_series(1, 2)`,
        );
        ids.splice(0, ids.length, ...prefetched.rows.map((p) => Number(p.id)));

        const f = toFacts(row);
        const matches = cache.index
          .matches(f, t)
          .map((m) => [m.sha256, classify(f, m, t)] as const);
        const asg = cache.grouper.add(f, matches); // the grouper is now ahead of the database
        await persist(c, sha256, asg);
        const bumped = await c.query<{ version: bigint }>(
          'UPDATE cluster_state SET version = version + 1 WHERE id = 1 RETURNING version',
        );
        await deps.checkpoint?.('cluster:persisted');
        return { group: asg.copyGroup, added: { f, cache, version: bumped.rows[0]!.version } };
      });
      if (out.added) {
        const { f, cache: c, version } = out.added;
        c.index.append([f], [out.group]);
        c.version = version;
      }
      return out.group;
    } catch (e) {
      cache = null; // the grouper may hold an uncommitted add: rebuild from the database
      throw e;
    }
  }

  /** Queue `cluster` (single-active). Returns the content's root copy group id. */
  return function clusterContent(sha256: string): Promise<number> {
    const run = tail.then(() => clusterOnce(sha256));
    tail = run.catch(() => undefined);
    return run;
  };
}
