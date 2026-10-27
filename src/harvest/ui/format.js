// Pure helpers of the harvester page (no DOM, no network): unit-tested from
// tests/unit/harvestUiFormat.test.ts and imported by app.js as an ES module.

const DASH = '–';

/** @param {unknown} v @returns {number | null} a finite number from a number or numeric string */
function num(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/** A byte size with decimal units, as Finder shows them: 999 B, 1.5 KB, 20 KB, 1.2 GB. */
export function bytes(n) {
  const v = num(n);
  if (v === null) return DASH;
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let x = Math.abs(v);
  let u = 0;
  while (x >= 1000 && u < units.length - 1) {
    x /= 1000;
    u++;
  }
  const text = u === 0 || x >= 10 ? String(Math.round(x)) : x.toFixed(1);
  return `${v < 0 ? '-' : ''}${text} ${units[u]}`;
}

/** A count with thousands separators (bigint columns arrive as strings). */
export function count(n) {
  const v = num(n);
  return v === null ? DASH : v.toLocaleString('en-US');
}

/** `2026-10-20` (UTC) or '' for a missing or invalid date. */
export function day(date) {
  const t = date ? new Date(date) : null;
  return t && !Number.isNaN(t.getTime()) ? t.toISOString().slice(0, 10) : '';
}

/** Relative time for recent dates, the date itself after 30 days, 'never' for none. */
export function ago(date, now = new Date()) {
  const t = date ? new Date(date) : null;
  if (!t || Number.isNaN(t.getTime())) return 'never';
  const s = (now.getTime() - t.getTime()) / 1000;
  if (s < 60) return 'just now';
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  if (s < 30 * 86400) return `${Math.floor(s / 86400)} d ago`;
  return day(t);
}

/**
 * `disk 74 % (limit 80 %) · backlog 12,400 (limit 20,000)`, plus the pending bytes when a
 * byte limit is set; '' when the server sent no gate.
 */
export function gateLine(state) {
  const g = state && state.gate;
  if (!g) return '';
  const limit = (v, fmt) => (num(v) === null ? '' : ` (limit ${fmt(v)})`);
  const parts = [
    `disk ${Math.round(Number(g.diskPct))} %${limit(g.diskLimit, (v) => `${v} %`)}`,
    `backlog ${count(g.backlog)}${limit(g.backlogLimit, count)}`,
  ];
  if (num(g.pendingBytesLimit) !== null) {
    parts.push(`${bytes(g.pendingBytes)}${limit(g.pendingBytesLimit, bytes)}`);
  }
  return parts.join(' · ');
}

/**
 * A folder's history line. `ancestors` run from the source root down to the folder's parent,
 * each `{name, lastHarvestedAt}` (name as shown, e.g. "Rich Mac/Photos/"); the nearest harvested
 * one is named when the folder itself was never harvested.
 */
export function folderStatus(folder, ancestors = []) {
  if (folder.lastHarvestedAt) {
    const sent = folder.lastCounts && num(folder.lastCounts.sent);
    return sent === null || sent === undefined
      ? day(folder.lastHarvestedAt)
      : `${day(folder.lastHarvestedAt)} · ${count(sent)} sent`;
  }
  for (let i = ancestors.length - 1; i >= 0; i--) {
    const a = ancestors[i];
    if (a && day(a.lastHarvestedAt)) return `covered by ${a.name} · ${day(a.lastHarvestedAt)}`;
  }
  return 'never';
}

/**
 * Whether a folder counts as not yet harvested: never harvested itself and not covered by a
 * harvested ancestor (`ancestors` as for folderStatus).
 */
export function defaultTicked(folder, ancestors = []) {
  return !folder.lastHarvestedAt && !ancestors.some((a) => a && day(a.lastHarvestedAt));
}

/** A paused run whose reason starts with "stalled" is shown in red. */
export function isStalled(reason) {
  return typeof reason === 'string' && reason.startsWith('stalled');
}

/** Sums the per-folder counters of a run (they arrive as strings; junk is a reason map). */
export function runTotals(folders) {
  const t = {
    dirs: 0,
    photos: 0,
    maybes: 0,
    junk: 0,
    errors: 0,
    sent: 0,
    known: 0,
    dups: 0,
    bytesSent: 0,
  };
  for (const f of folders || []) {
    for (const k of Object.keys(t)) {
      if (k !== 'junk') t[k] += num(f[k]) ?? 0;
    }
    for (const n of Object.values(f.junk || {})) t.junk += num(n) ?? 0;
  }
  return t;
}

/** `too-small 40, screenshot 3`: junk reasons across folders, largest first. */
export function junkSummary(folders) {
  const by = new Map();
  for (const f of folders || []) {
    for (const [k, n] of Object.entries(f.junk || {})) by.set(k, (by.get(k) ?? 0) + (num(n) ?? 0));
  }
  return [...by.entries()]
    .sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1))
    .map(([k, n]) => `${k} ${n}`)
    .join(', ');
}

/**
 * The page's route from `location.hash`: `#/source/<id>`, `#/review`, `#/review/<id>/<path>`,
 * `#/where`, `#/history`; anything else is the sources list.
 */
export function parseHash(hash) {
  const parts = hash
    .replace(/^#\/?/, '')
    .split('/')
    .filter(Boolean)
    .map((p) => {
      try {
        return decodeURIComponent(p);
      } catch {
        return p; // a malformed escape: keep it as typed
      }
    });
  const [a, b, ...rest] = parts;
  if (a === 'source' && b) return { name: 'source', id: b };
  if (a === 'review')
    return b ? { name: 'review-files', id: b, path: rest.join('/') } : { name: 'review' };
  if (a === 'where') return { name: 'where' };
  if (a === 'history') return { name: 'history' };
  return { name: 'sources' };
}

const RATE_WINDOW_MS = 60_000;

/**
 * Files per minute over about the last minute. `samples` are one run's `[time, sent]` pairs,
 * oldest first; returns the samples to keep (this one added) and the rate.
 */
export function filesPerMinute(samples, now, sent) {
  const kept = [...samples, [now, sent]];
  while (kept.length > 2 && now - kept[0][0] > RATE_WINDOW_MS) kept.shift();
  const [t0, sent0] = kept[0];
  const rate = kept.length > 1 && now > t0 ? ((sent - sent0) * RATE_WINDOW_MS) / (now - t0) : 0;
  return { samples: kept, rate };
}

/** A files-per-minute rate: whole numbers from 10 up, one decimal below. */
export function rateText(rate) {
  return rate >= 10 ? String(Math.round(rate)) : rate.toFixed(1);
}
