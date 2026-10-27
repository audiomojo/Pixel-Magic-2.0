import { describe, expect, it } from 'vitest';
import {
  ago,
  bytes,
  count,
  defaultTicked,
  filesPerMinute,
  folderStatus,
  gateLine,
  isStalled,
  junkSummary,
  parseHash,
  rateText,
  runTotals,
  scoreLine,
} from '../../src/harvest/ui/format.js';

describe('bytes', () => {
  it('scales with decimal units like Finder', () => {
    expect(bytes(0)).toBe('0 B');
    expect(bytes(999)).toBe('999 B');
    expect(bytes(1500)).toBe('1.5 KB');
    expect(bytes(20480)).toBe('20 KB');
    expect(bytes(2_500_000)).toBe('2.5 MB');
    expect(bytes(1_234_000_000)).toBe('1.2 GB');
    expect(bytes(3_400_000_000_000)).toBe('3.4 TB');
  });
  it('shows a dash for missing values', () => {
    expect(bytes(null)).toBe('–');
    expect(bytes(undefined)).toBe('–');
    expect(bytes(Number.NaN)).toBe('–');
  });
});

describe('count', () => {
  it('groups thousands', () => {
    expect(count(12400)).toBe('12,400');
    expect(count(0)).toBe('0');
    expect(count(null)).toBe('–');
  });
  it('accepts numeric strings (bigint columns)', () => {
    expect(count('1204')).toBe('1,204');
  });
});

describe('ago', () => {
  const now = new Date('2026-10-20T12:00:00Z');
  it('is relative for recent times', () => {
    expect(ago('2026-10-20T11:59:50Z', now)).toBe('just now');
    expect(ago('2026-10-20T11:55:00Z', now)).toBe('5 min ago');
    expect(ago('2026-10-20T09:00:00Z', now)).toBe('3 h ago');
    expect(ago('2026-10-18T12:00:00Z', now)).toBe('2 d ago');
  });
  it('is a date for old times, never for null', () => {
    expect(ago('2026-08-01T10:00:00Z', now)).toBe('2026-08-01');
    expect(ago(null, now)).toBe('never');
    expect(ago('garbage', now)).toBe('never');
  });
  it('treats a future time as just now', () => {
    expect(ago('2026-10-20T12:05:00Z', now)).toBe('just now');
  });
});

describe('gateLine', () => {
  it('shows disk and backlog against their limits', () => {
    expect(
      gateLine({ gate: { diskPct: 74.2, diskLimit: 80, backlog: 12400, backlogLimit: 20000 } }),
    ).toBe('disk 74 % (limit 80 %) · backlog 12,400 (limit 20,000)');
  });
  it('adds the pending bytes when a byte limit is set', () => {
    const gate = { diskPct: 50, diskLimit: 80, backlog: 10, backlogLimit: 20000 };
    expect(gateLine({ gate: { ...gate, pendingBytes: 1.2e9, pendingBytesLimit: null } })).toBe(
      'disk 50 % (limit 80 %) · backlog 10 (limit 20,000)',
    );
    expect(gateLine({ gate: { ...gate, pendingBytes: 1.2e9, pendingBytesLimit: 2e9 } })).toBe(
      'disk 50 % (limit 80 %) · backlog 10 (limit 20,000) · 1.2 GB (limit 2.0 GB)',
    );
  });
  it('is empty without a gate', () => {
    expect(gateLine({ gate: null })).toBe('');
    expect(gateLine({})).toBe('');
  });
});

describe('folderStatus', () => {
  it('says never for an unharvested folder', () => {
    expect(folderStatus({}, [])).toBe('never');
    expect(
      folderStatus({ lastHarvestedAt: null }, [{ name: 'Rich Mac/', lastHarvestedAt: null }]),
    ).toBe('never');
  });
  it('shows date and sent count for a harvested folder', () => {
    expect(
      folderStatus({ lastHarvestedAt: '2026-10-20T08:00:00Z', lastCounts: { sent: 1204 } }, []),
    ).toBe('2026-10-20 · 1,204 sent');
  });
  it('omits the count when unknown', () => {
    expect(folderStatus({ lastHarvestedAt: '2026-10-20T08:00:00Z' }, [])).toBe('2026-10-20');
  });
  it('names the nearest harvested ancestor (ancestors run root first)', () => {
    const ancestors = [
      { name: 'Rich Mac/', lastHarvestedAt: '2026-09-01T00:00:00Z' },
      { name: 'Rich Mac/Photos/', lastHarvestedAt: '2026-10-20T08:00:00Z' },
      { name: 'Rich Mac/Photos/2019/', lastHarvestedAt: null },
    ];
    expect(folderStatus({}, ancestors)).toBe('covered by Rich Mac/Photos/ · 2026-10-20');
  });
  it('prefers its own history over an ancestor', () => {
    expect(
      folderStatus({ lastHarvestedAt: '2026-10-21T00:00:00Z', lastCounts: { sent: 0 } }, [
        { name: 'Rich Mac/', lastHarvestedAt: '2026-10-20T00:00:00Z' },
      ]),
    ).toBe('2026-10-21 · 0 sent');
  });
});

describe('defaultTicked', () => {
  it('is true only for a folder never harvested', () => {
    expect(defaultTicked({})).toBe(true);
    expect(defaultTicked({ lastHarvestedAt: null })).toBe(true);
    expect(defaultTicked({ lastHarvestedAt: '2026-10-20T08:00:00Z' })).toBe(false);
  });
  it('is false for a folder covered by a harvested ancestor', () => {
    const done = { name: 'Mac/', lastHarvestedAt: '2026-10-20T08:00:00Z' };
    expect(defaultTicked({}, [done])).toBe(false);
    expect(defaultTicked({}, [{ name: 'Mac/', lastHarvestedAt: null }])).toBe(true);
    expect(defaultTicked({}, [])).toBe(true);
  });
});

describe('isStalled', () => {
  it('matches a reason starting with stalled', () => {
    expect(isStalled('stalled: disk 83 % above limit 80 %')).toBe(true);
    expect(isStalled('Expansion unplugged')).toBe(false);
    expect(isStalled(null)).toBe(false);
  });
});

describe('runTotals', () => {
  it('sums the folder counters (bigint strings)', () => {
    const t = runTotals([
      {
        dirs: '3',
        photos: '10',
        maybes: '2',
        junk: { 'too-small': 4 },
        errors: '1',
        sent: '5',
        known: '2',
        dups: '1',
        bytesSent: '100',
      },
      {
        dirs: '1',
        photos: '1',
        maybes: '0',
        junk: { 'too-small': 1, screenshot: 2 },
        errors: '0',
        sent: '1',
        known: '0',
        dups: '0',
        bytesSent: '50',
      },
    ]);
    expect(t).toEqual({
      dirs: 4,
      photos: 11,
      maybes: 2,
      junk: 7,
      errors: 1,
      sent: 6,
      known: 2,
      dups: 1,
      bytesSent: 150,
    });
  });
  it('is zero for no folders', () => {
    expect(runTotals(undefined).sent).toBe(0);
  });
});

describe('junkSummary', () => {
  it('lists reasons largest first', () => {
    expect(junkSummary([{ junk: { a: 1, b: 5 } }, { junk: { a: 2 } }])).toBe('b 5, a 3');
    expect(junkSummary([])).toBe('');
  });
});

describe('edge cases', () => {
  it('handles negative and huge sizes, odd inputs', () => {
    expect(bytes(-1500)).toBe('-1.5 KB');
    expect(bytes('2048')).toBe('2.0 KB');
    expect(bytes('')).toBe('–');
    expect(bytes(5e18)).toBe('5000 PB');
    expect(count(undefined)).toBe('–');
  });
  it('gateLine without limits', () => {
    expect(gateLine({ gate: { diskPct: 10, backlog: 3 } })).toBe('disk 10 % · backlog 3');
  });
  it('folderStatus ignores junk counts and bad dates', () => {
    expect(folderStatus({ lastHarvestedAt: '2026-10-20T08:00:00Z', lastCounts: {} }, [])).toBe(
      '2026-10-20',
    );
    expect(folderStatus({}, [{ name: 'X/', lastHarvestedAt: 'garbage' }])).toBe('never');
  });
  it('tolerates missing junk maps and breaks ties by name', () => {
    expect(runTotals([{ dirs: 'x' }]).dirs).toBe(0);
    expect(junkSummary([{}, { junk: { b: 2, a: 2 } }])).toBe('a 2, b 2');
    expect(junkSummary(undefined)).toBe('');
  });
});

describe('parseHash', () => {
  it('reads each route', () => {
    expect(parseHash('')).toEqual({ name: 'sources' });
    expect(parseHash('#/')).toEqual({ name: 'sources' });
    expect(parseHash('#/source/3')).toEqual({ name: 'source', id: '3' });
    expect(parseHash('#/review')).toEqual({ name: 'review' });
    expect(parseHash('#/where')).toEqual({ name: 'where' });
    expect(parseHash('#history')).toEqual({ name: 'history' });
    expect(parseHash('#/picks')).toEqual({ name: 'picks' });
  });
  it('decodes a review path, keeping its slashes', () => {
    expect(parseHash('#/review/3/My%20Pics/2020/caf%C3%A9')).toEqual({
      name: 'review-files',
      id: '3',
      path: 'My Pics/2020/café',
    });
  });
  it('keeps a malformed escape as typed', () => {
    expect(parseHash('#/review/3/100%')).toEqual({ name: 'review-files', id: '3', path: '100%' });
  });
  it('falls back to the sources list', () => {
    expect(parseHash('#/source')).toEqual({ name: 'sources' });
    expect(parseHash('#/nope/1')).toEqual({ name: 'sources' });
  });
});

describe('filesPerMinute', () => {
  it('is 0 until there are two samples', () => {
    expect(filesPerMinute([], 1000, 5)).toEqual({ samples: [[1000, 5]], rate: 0 });
  });
  it('measures from the oldest kept sample', () => {
    const { rate } = filesPerMinute([[0, 0]], 30_000, 10);
    expect(rate).toBe(20); // 10 files in 30 s
  });
  it('keeps about the last minute, never fewer than two samples', () => {
    const old = [
      [0, 0],
      [50_000, 100],
      [90_000, 160],
    ];
    const r = filesPerMinute(old, 120_000, 190);
    expect(r.samples).toEqual([
      [90_000, 160],
      [120_000, 190],
    ]);
    expect(r.rate).toBe(60); // 30 files in 30 s
    expect(filesPerMinute([[0, 0]], 600_000, 10).samples).toHaveLength(2);
  });
  it('does not change the samples it is given', () => {
    const s = [[0, 0]];
    filesPerMinute(s, 1000, 1);
    expect(s).toEqual([[0, 0]]);
  });
});

describe('rateText', () => {
  it('rounds from 10 up and keeps one decimal below', () => {
    expect(rateText(0)).toBe('0.0');
    expect(rateText(2.345)).toBe('2.3');
    expect(rateText(9.96)).toBe('10.0');
    expect(rateText(12.6)).toBe('13');
  });
});

describe('scoreLine', () => {
  it('states the match once 50 random sets are graded', () => {
    expect(scoreLine({ matched: 41, of: 50 })).toBe(
      'Matched your pick 41 of your last 50 random sets',
    );
  });
  it('states the count so far before that', () => {
    expect(scoreLine({ matched: 3, of: 7 })).toBe('7 random picks so far (the score needs 50)');
    expect(scoreLine({ matched: 0, of: 49 })).toBe('49 random picks so far (the score needs 50)');
  });
});
