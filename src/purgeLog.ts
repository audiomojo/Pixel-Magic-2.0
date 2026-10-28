// The durable purge record, `DATA_DIR/purged.csv` (spec §7): append-only, one line per event,
// merged both ways with the `purged` table at worker start. Only the worker writes it.
import { open, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type pg from 'pg';
import { fsyncDir, isEnoent } from './fsops.js';

export interface PurgeEvent {
  action: 'purge' | 'unblock';
  sha256: string;
  sizeBytes: bigint;
  at: Date;
  names: string[];
}

export interface PurgeLogger {
  info: (message: string, meta?: Record<string, unknown>) => void;
  warn: (message: string, meta?: Record<string, unknown>) => void;
}

const HEADER = 'action,sha256,size_bytes,at,names';

export function purgeLogPath(dataDir: string): string {
  return join(dataDir, 'purged.csv');
}

function field(s: string): string {
  return /[",\r\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}

/** One CSV line (no trailing newline); a newline inside a name stays inside a quoted field. */
export function formatEvent(e: PurgeEvent): string {
  return [e.action, e.sha256, e.sizeBytes.toString(), e.at.toISOString(), JSON.stringify(e.names)]
    .map(field)
    .join(',');
}

/**
 * RFC 4180 reader: records with the 1-based line each starts on. A raw line break always ends a
 * record, even inside quotes: `formatEvent` never writes one (names are JSON-escaped), so one
 * inside quotes can only be a torn last line, which must not swallow the event appended after it.
 */
function readRecords(text: string): { line: number; fields: string[] }[] {
  const out: { line: number; fields: string[] }[] = [];
  let fields: string[] = [];
  let cur = '';
  let quoted = false;
  let line = 1;
  let start = 1;
  let touched = false;
  const endField = (): void => {
    fields.push(cur);
    cur = '';
  };
  const endRecord = (): void => {
    if (touched || fields.length > 0) {
      endField();
      out.push({ line: start, fields });
    }
    fields = [];
    touched = false;
  };
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (c === '\n') line++;
    if (quoted && c !== '\n' && c !== '\r') {
      if (c !== '"') cur += c;
      else if (text[i + 1] === '"') {
        cur += '"';
        i++;
      } else quoted = false;
    } else if (c === '"' && cur === '') {
      quoted = true;
      touched = true;
    } else if (c === ',') {
      touched = true;
      endField();
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') {
        i++;
        line++;
      }
      quoted = false;
      endRecord();
      start = line;
    } else {
      touched = true;
      cur += c;
    }
  }
  endRecord();
  return out;
}

function toEvent(f: string[]): PurgeEvent | null {
  if (f.length !== 5) return null;
  const [action, sha256, size, at, names] = f as [string, string, string, string, string];
  if (action !== 'purge' && action !== 'unblock') return null;
  if (!/^[0-9a-f]{64}$/.test(sha256) || !/^\d+$/.test(size)) return null;
  const when = new Date(at);
  if (Number.isNaN(when.getTime())) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(names);
  } catch {
    return null;
  }
  if (!Array.isArray(parsed) || !parsed.every((n) => typeof n === 'string')) return null;
  return { action, sha256, sizeBytes: BigInt(size), at: when, names: parsed };
}

/** Parse the file; the header and blank lines are skipped, an unreadable record is reported in `bad`. */
export function parseLog(text: string): { events: PurgeEvent[]; bad: number[] } {
  const events: PurgeEvent[] = [];
  const bad: number[] = [];
  for (const r of readRecords(text)) {
    if (r.fields.length === 1 && r.fields[0] === '') continue;
    if (r.line === 1 && r.fields.join(',') === HEADER) continue;
    const e = toEvent(r.fields);
    if (e) events.push(e);
    else bad.push(r.line);
  }
  return { events, bad };
}

interface Row {
  sha256: string;
  size_bytes: string;
  names: string[];
  purged_at: Date;
  unblocked_at: Date | null;
}

/** A row's latest event: `unblock` when unblocked at/after the purge, else `purge`. */
function latestOf(r: Row): { action: PurgeEvent['action']; at: number } {
  return r.unblocked_at !== null && r.unblocked_at.getTime() >= r.purged_at.getTime()
    ? { action: 'unblock', at: r.unblocked_at.getTime() }
    : { action: 'purge', at: r.purged_at.getTime() };
}

async function appendLines(path: string, dir: string, lines: string[]): Promise<void> {
  let existing = '';
  try {
    existing = await readFile(path, 'utf8');
  } catch (err) {
    if (!isEnoent(err)) throw err;
  }
  const body =
    (existing === '' ? `${HEADER}\n` : existing.endsWith('\n') ? '' : '\n') +
    lines.map((l) => `${l}\n`).join('');
  const fh = await open(path, 'a');
  try {
    await fh.appendFile(body);
    await fh.sync();
  } finally {
    await fh.close();
  }
  if (existing === '') await fsyncDir(dir);
}

/**
 * Merge `purged.csv` and the `purged` table both ways (spec §7). Serialised across workers by a
 * session advisory lock; appends nothing when the two already agree.
 */
export async function reconcilePurgeLog(
  pool: pg.Pool,
  dataDir: string,
  log: PurgeLogger,
): Promise<{ imported: number; appended: number; bad: number }> {
  const client = await pool.connect();
  /** Set when the unlock failed: the connection may still hold the lock, so it is destroyed. */
  let broken: Error | undefined;
  try {
    await client.query("SELECT pg_advisory_lock(hashtext('pm.purge-log'))");
    try {
      const path = purgeLogPath(dataDir);
      let text = '';
      try {
        text = await readFile(path, 'utf8');
      } catch (err) {
        if (!isEnoent(err)) throw err;
      }
      const { events, bad } = parseLog(text);
      if (bad.length > 0) log.warn('purged.csv: unreadable lines skipped', { lines: bad });

      // Latest event per sha (by time, then file order) and the latest purge, for a lone unblock.
      const latest = new Map<string, PurgeEvent>();
      const lastPurge = new Map<string, PurgeEvent>();
      for (const e of events) {
        const cur = latest.get(e.sha256);
        if (!cur || e.at.getTime() >= cur.at.getTime()) latest.set(e.sha256, e);
        const p = lastPurge.get(e.sha256);
        if (e.action === 'purge' && (!p || e.at.getTime() >= p.at.getTime()))
          lastPurge.set(e.sha256, e);
      }

      const { rows } = await client.query<Row>(
        'SELECT sha256, size_bytes::text, names, purged_at, unblocked_at FROM purged',
      );
      const db = new Map(rows.map((r) => [r.sha256, r]));
      let imported = 0;
      for (const [sha, ev] of latest) {
        const row = db.get(sha);
        if (row && ev.at.getTime() <= latestOf(row).at) continue;
        const p = lastPurge.get(sha);
        const names = (p ?? ev).names;
        if (ev.action === 'purge') {
          await client.query(
            `INSERT INTO purged (sha256, size_bytes, names, purged_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT (sha256) DO UPDATE SET purged_at = $4, unblocked_at = NULL`,
            [sha, ev.sizeBytes.toString(), names, ev.at],
          );
        } else {
          await client.query(
            `INSERT INTO purged (sha256, size_bytes, names, purged_at, unblocked_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (sha256) DO UPDATE SET unblocked_at = $5`,
            [sha, ev.sizeBytes.toString(), names, (p ?? ev).at, ev.at],
          );
        }
        imported++;
      }

      const after =
        imported > 0
          ? (
              await client.query<Row>(
                'SELECT sha256, size_bytes::text, names, purged_at, unblocked_at FROM purged',
              )
            ).rows
          : rows;
      const lines: string[] = [];
      for (const r of after) {
        const want = latestOf(r);
        const have = latest.get(r.sha256);
        if (have && have.action === want.action && have.at.getTime() === want.at) continue;
        lines.push(
          formatEvent({
            action: want.action,
            sha256: r.sha256,
            sizeBytes: BigInt(r.size_bytes),
            at: new Date(want.at),
            names: r.names,
          }),
        );
      }
      if (lines.length > 0) await appendLines(path, dataDir, lines);
      if (imported > 0 || lines.length > 0)
        log.info('purged.csv reconciled', { imported, appended: lines.length, bad: bad.length });
      return { imported, appended: lines.length, bad: bad.length };
    } finally {
      try {
        await client.query("SELECT pg_advisory_unlock(hashtext('pm.purge-log'))");
      } catch (err) {
        broken = err instanceof Error ? err : new Error(String(err));
      }
    }
  } finally {
    client.release(broken);
  }
}
