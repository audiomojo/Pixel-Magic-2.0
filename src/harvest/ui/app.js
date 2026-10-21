// The harvester page: plain DOM, no framework, no build step, no external requests.
// Everything that comes from the server (file and folder names especially) is put in with
// textContent / attributes, never as HTML.
import {
  ago,
  bytes,
  count,
  day,
  defaultTicked,
  folderStatus,
  gateLine,
  isStalled,
  junkSummary,
  runTotals,
} from './format.js';

const POLL_MS = 2000;
const $ = (id) => document.getElementById(id);

/** Builds an element: attrs are properties (or aria-/data- attributes), children text or nodes. */
function h(tag, attrs, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else if (k.includes('-') || k === 'role') node.setAttribute(k, String(v));
    else node[k] = v;
  }
  for (const c of children.flat()) {
    if (c === undefined || c === null || c === false) continue;
    node.append(typeof c === 'string' || typeof c === 'number' ? String(c) : c);
  }
  return node;
}

async function api(path, body) {
  const opts =
    body === undefined
      ? {}
      : {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Pixel-Magic': '1' },
          body: JSON.stringify(body),
        };
  const res = await fetch(path, opts);
  let data = {};
  try {
    data = await res.json();
  } catch {
    // not JSON: keep the status text below
  }
  if (!res.ok) throw new Error(data.error || `${res.status} ${res.statusText}`);
  return data;
}
const post = (path, body = {}) => api(path, body);

function confirmDialog(title, text, yesLabel) {
  const dlg = $('confirm');
  $('confirm-title').textContent = title;
  $('confirm-text').textContent = text;
  $('confirm-yes').textContent = yesLabel;
  return new Promise((resolve) => {
    dlg.addEventListener('close', () => resolve(dlg.returnValue === 'yes'), { once: true });
    dlg.returnValue = 'no';
    dlg.showModal();
  });
}

const message = (text, bad = false) =>
  h('p', { className: bad ? 'msg bad' : 'msg', role: 'status' }, text);
const button = (label, onclick, cls = '', attrs = {}) =>
  h('button', { className: cls ? `btn ${cls}` : 'btn', type: 'button', onclick, ...attrs }, label);
const sourceName = (s) => s.label || `source ${s.id}`;
const runName = (dryRun, id) => `${dryRun ? 'Dry run' : 'Harvest'} #${id}`;
const folderList = (folders) => folders.map((f) => `${f.label}/${f.relPath}`).join(', ');
const decided = (action, r) =>
  `${action === 'send' ? 'approved' : 'skipped'}${r.runId ? ` (run #${r.runId})` : ''}`;
/** A `<dl class="counts">` of [label, value] pairs. */
const countItems = (items) => items.map(([k, v]) => h('div', {}, h('dt', {}, k), h('dd', {}, v)));

/**
 * Shows `head` and "Loading…", then fetches `url`. Returns the data, or null when the fetch
 * failed (the error is shown under `head`) or the user has moved to another view meanwhile.
 */
async function loadInto(routeName, head, url) {
  const m = main();
  m.replaceChildren(...head, h('p', { className: 'muted' }, 'Loading…'));
  try {
    const data = await api(url);
    return route.name === routeName ? data : null;
  } catch (e) {
    if (route.name === routeName) m.replaceChildren(...head, message(e.message, true));
    return null;
  }
}

// ---------------------------------------------------------------- state and polling
let state = null;
let stateError = null;
let route = { name: 'sources' };
const samples = []; // [time, sent] of the active run, for files per minute
let samplesRun = null;

async function refreshState() {
  try {
    state = await api('/api/state');
    stateError = null;
  } catch (e) {
    stateError = e.message;
  }
  try {
    paintChrome();
    if (route.name === 'sources') paintSources();
    if (route.name === 'source') paintSourceHeader();
  } catch (e) {
    console.error('paint failed', e);
  }
}

let timer;
/** Refreshes now, then again every POLL_MS while the page is visible. */
async function tick() {
  try {
    await refreshState();
  } finally {
    schedule();
  }
}
function schedule() {
  clearTimeout(timer);
  if (!document.hidden) timer = setTimeout(tick, POLL_MS);
}
document.addEventListener('visibilitychange', () =>
  document.hidden ? clearTimeout(timer) : tick(),
);

// ---------------------------------------------------------------- banner, nav badge
function paintChrome() {
  const banner = $('banner');
  const down = stateError
    ? `The harvester is not answering (${stateError}).`
    : state && (!state.stack.db || !state.stack.temporal)
      ? `Pipeline stack down (${[!state.stack.db && 'database', !state.stack.temporal && 'Temporal']
          .filter(Boolean)
          .join(', ')} unreachable). Nothing is sent until it is back.`
      : '';
  banner.hidden = !down;
  banner.textContent = down;
  const badge = $('review-badge');
  const n = state ? state.reviewCount : 0;
  badge.hidden = !n;
  badge.textContent = count(n);
  paintRunPanel();
}

// ---------------------------------------------------------------- run panel
const panel = {};
let queueKey = '';

function buildPanel() {
  const root = $('run-panel');
  panel.title = h('h2', {}, 'Run');
  panel.status = h('p', { role: 'status', 'aria-live': 'polite' });
  panel.reason = h('p', { className: 'bad' });
  panel.current = h('p', { className: 'muted' });
  panel.counts = h('dl', { className: 'counts' });
  panel.gate = h('p', { className: 'muted' });
  panel.pause = button('Pause', () => runAction('pause'));
  panel.resume = button('Resume', () => runAction('resume'), 'primary');
  panel.cancel = button('Cancel', cancelActive, 'danger');
  panel.errors = h(
    'details',
    {
      ontoggle: () => {
        if (panel.errors.open) loadErrors();
      },
    },
    h('summary', {}, 'Errors'),
    (panel.errorList = h('ul', { className: 'plain' })),
  );
  panel.queue = h('div');
  panel.err = h('p', { className: 'bad', role: 'alert' });
  root.append(
    panel.title,
    panel.status,
    panel.reason,
    panel.current,
    panel.counts,
    panel.gate,
    h('div', { className: 'row' }, panel.pause, panel.resume, panel.cancel),
    panel.err,
    panel.errors,
    panel.queue,
  );
}

/** Count labels, in the run panel's order. */
const COUNT_LABELS = {
  dirs: 'Folders scanned',
  photos: 'Photos',
  maybes: 'Possible photos',
  junk: 'Junk',
  sent: 'Sent',
  known: 'Already in vault',
  dups: 'Duplicates',
  errors: 'Failed',
};

function paintRunPanel() {
  const root = $('run-panel');
  const run = state && state.activeRun;
  const queue = (state && state.queue) || [];
  root.hidden = !run && queue.length === 0;
  if (root.hidden) {
    samples.length = 0;
    return;
  }
  if (!panel.title) buildPanel();
  root.classList.toggle('stalled', Boolean(run && isStalled(run.paused_reason)));
  panel.err.textContent = '';
  for (const el of [
    panel.status,
    panel.reason,
    panel.current,
    panel.counts,
    panel.gate,
    panel.pause,
    panel.resume,
    panel.cancel,
    panel.errors,
  ]) {
    el.hidden = !run;
  }
  if (run) {
    const t = runTotals(run.folders);
    panel.title.textContent = runName(run.dry_run, run.id);
    panel.status.textContent = `${run.state}${run.paused_since ? ` since ${ago(run.paused_since)}` : ''}`;
    const stalled = isStalled(run.paused_reason);
    panel.reason.hidden = !run.paused_reason;
    panel.reason.className = stalled ? 'bad' : 'muted';
    panel.reason.textContent = run.paused_reason
      ? stalled
        ? `${run.paused_reason}. Hint: free space or raise HARVEST_MAX_DISK_PCT.`
        : run.paused_reason
      : '';
    const cur = run.folders.find((f) => f.state !== 'done');
    panel.current.textContent = cur ? `Now: ${cur.label}/${cur.relPath} (${cur.state})` : '';
    const rate = filesPerMinute(run.id, t.sent);
    panel.counts.replaceChildren(
      ...countItems([
        ...Object.entries(COUNT_LABELS).map(([k, label]) => [label, count(t[k])]),
        ['Files / min', rate >= 10 ? String(Math.round(rate)) : rate.toFixed(1)],
        ['Bytes sent', bytes(t.bytesSent)],
      ]),
    );
    panel.gate.textContent = gateLine(state);
    const paused = run.state === 'paused';
    panel.pause.hidden = paused;
    panel.resume.hidden = !paused;
    panel.errors.firstChild.textContent = `Errors (${count(t.errors)})`;
    panel.errors.hidden = t.errors === 0;
    if (panel.errors.open && t.errors > 0) loadErrors();
  }
  paintQueue(queue);
}

/** Files per minute of run `runId`, over about the last minute of samples. */
function filesPerMinute(runId, sent) {
  const now = Date.now();
  if (samplesRun !== runId) {
    samples.length = 0;
    samplesRun = runId;
  }
  samples.push([now, sent]);
  while (samples.length > 2 && now - samples[0][0] > 60000) samples.shift();
  const first = samples[0];
  return samples.length > 1 && now > first[0] ? ((sent - first[1]) * 60000) / (now - first[0]) : 0;
}

/** The waiting runs, repainted only when the queue changed. */
function paintQueue(queue) {
  const key = JSON.stringify(queue.map((q) => q.id));
  if (key === queueKey) return;
  queueKey = key;
  panel.queue.replaceChildren(
    queue.length ? h('h3', {}, 'Waiting') : '',
    ...queue.map((q) =>
      h(
        'div',
        { className: 'row between' },
        h('span', {}, `${runName(q.dry_run, q.id)}: ${folderList(q.folders)}`),
        button('Remove', () => removeQueued(q.id), 'small'),
      ),
    ),
  );
}

async function loadErrors() {
  const run = state && state.activeRun;
  if (!run) return;
  try {
    const d = await api(`/api/runs/${run.id}`);
    panel.errorList.replaceChildren(
      ...d.errors.map((e) => h('li', {}, `${e.source}/${e.relPath}: ${e.error || 'failed'}`)),
    );
  } catch (e) {
    panel.errorList.replaceChildren(h('li', { className: 'bad' }, e.message));
  }
}

async function runAction(action) {
  try {
    await post(`/api/runs/${state.activeRun.id}/${action}`);
    await refreshState();
  } catch (e) {
    panel.err.textContent = e.message;
  }
}
async function cancelActive() {
  const run = state.activeRun;
  if (
    !(await confirmDialog(
      `Cancel run #${run.id}?`,
      'Files already sent stay sent. The run cannot be resumed.',
      'Cancel run',
    ))
  )
    return;
  await runAction('cancel');
}
async function removeQueued(id) {
  try {
    await post(`/api/runs/${id}/cancel`);
    queueKey = '';
    await refreshState();
  } catch (e) {
    panel.err.textContent = e.message;
  }
}

// ---------------------------------------------------------------- router
const main = () => $('main');
let sourcesFrame = null; // the sources view's fixed parts, rebuilt only on navigation
function parseHash() {
  const parts = location.hash
    .replace(/^#\/?/, '')
    .split('/')
    .filter(Boolean)
    .map((p) => {
      try {
        return decodeURIComponent(p);
      } catch {
        return p;
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
function navigate() {
  route = parseHash();
  const top = {
    sources: 'sources',
    source: 'sources',
    review: 'review',
    'review-files': 'review',
    where: 'where',
    history: 'history',
  }[route.name];
  for (const a of document.querySelectorAll('nav a')) {
    if (a.dataset.nav === top) a.setAttribute('aria-current', 'page');
    else a.removeAttribute('aria-current');
  }
  document.title = 'Harvest';
  sourcesFrame = null;
  main().replaceChildren();
  ({
    sources: paintSources,
    source: openSource,
    review: openReview,
    'review-files': openReviewFiles,
    where: openWhere,
    history: openHistory,
  })[route.name]();
}
window.addEventListener('hashchange', navigate);

// ---------------------------------------------------------------- 1. sources
async function refreshDrives(btn, msg) {
  btn.disabled = true;
  msg.replaceChildren();
  try {
    await post('/api/sources/refresh');
    await refreshState();
  } catch (e) {
    msg.replaceChildren(message(e.message, true));
  } finally {
    btn.disabled = false;
  }
}

/** The text of one source card, so the list is repainted only when something visible changed. */
function sourceCard(s) {
  return {
    id: s.id,
    mounted: s.mounted,
    name: sourceName(s),
    kind: s.kind,
    line: `${s.mounted ? `used ${bytes(s.usedBytes)} of ${bytes(s.totalBytes)}` : `last seen ${ago(s.lastSeenAt)}`} · last harvest ${ago(s.lastHarvestedAt)} · ${count(s.sentCount)} sent`,
    review: s.reviewCount > 0 ? `${count(s.reviewCount)} to review` : '',
  };
}

function paintSources() {
  if (route.name !== 'sources') return;
  const m = main();
  if (!state) return m.replaceChildren(h('p', { className: 'muted' }, 'Loading…'));
  if (!sourcesFrame) {
    const msg = h('div');
    const btn = button('Refresh drives', () => refreshDrives(btn, msg));
    sourcesFrame = { msg, list: h('div'), key: '' };
    m.replaceChildren(
      h('div', { className: 'row between' }, h('h2', {}, 'Sources'), btn),
      msg,
      sourcesFrame.list,
    );
  }
  const rows = [...state.sources].sort(
    (a, b) => Number(b.mounted) - Number(a.mounted) || sourceName(a).localeCompare(sourceName(b)),
  );
  const cards = rows.map(sourceCard);
  const unidentified = state.unidentified || [];
  const key = JSON.stringify([cards, unidentified, state.reviewCount]);
  if (key === sourcesFrame.key) return;
  sourcesFrame.key = key;
  sourcesFrame.list.replaceChildren(
    state.reviewCount > 0
      ? h(
          'p',
          {},
          h(
            'a',
            { href: '#/review' },
            `${count(state.reviewCount)} possible photos waiting for review`,
          ),
        )
      : '',
    rows.length === 0 && unidentified.length === 0
      ? h(
          'p',
          { className: 'muted' },
          'No sources found. Plug in a drive and press Refresh drives.',
        )
      : '',
    ...cards.map((c) => {
      const title = c.mounted
        ? h('a', { href: `#/source/${c.id}` }, c.name)
        : h('span', {}, `${c.name} (not mounted)`);
      return h(
        'div',
        { className: c.mounted ? 'card' : 'card off' },
        h(
          'div',
          { className: 'row between' },
          h('strong', {}, title),
          h('span', { className: 'muted' }, c.kind),
        ),
        h('div', { className: 'muted' }, c.line),
        c.review ? h('div', {}, h('a', { href: '#/review' }, c.review)) : '',
      );
    }),
    ...unidentified.map((u) =>
      h(
        'div',
        { className: 'card off' },
        h('strong', {}, u.labelBase),
        h('div', { className: 'muted' }, "Couldn't identify this drive. Press Refresh drives."),
      ),
    ),
  );
}

// ---------------------------------------------------------------- 2. one source
let src = null; // {id, ticked: Set, rows: Map<relPath, {input, entry, li}>}

function openSource() {
  src = { id: route.id, ticked: new Set(), rows: new Map() };
  main().replaceChildren(
    h('div', { id: 'src-head' }),
    h('div', { id: 'src-tree' }),
    h('div', { id: 'src-msg' }),
  );
  paintSourceHeader(); // starts the tree once the source is known and mounted
}

function currentSource() {
  return state && state.sources.find((s) => s.id === route.id);
}

function paintSourceHeader() {
  if (route.name !== 'source' || !src) return;
  const s = currentSource();
  const head = $('src-head');
  if (!head || !state) return;
  if (!s) {
    src.headKey = 'unknown';
    head.replaceChildren(
      h('p', { className: 'bad' }, 'Unknown source.'),
      h('a', { href: '#/' }, 'Back to sources'),
    );
    return;
  }
  if (!src.treeStarted && s.mounted) startTree();
  const key = JSON.stringify([sourceName(s), s.kind, s.mounted, s.usedBytes, s.totalBytes]);
  if (key === src.headKey) return;
  src.headKey = key;
  head.replaceChildren(
    h('p', {}, h('a', { href: '#/' }, '← Sources')),
    h('h2', {}, sourceName(s)),
    h(
      'p',
      { className: 'muted' },
      s.mounted
        ? `${s.kind} · used ${bytes(s.usedBytes)} of ${bytes(s.totalBytes)}`
        : 'Not mounted. Plug it in and refresh the drives.',
    ),
  );
  if (!s.mounted) {
    $('src-tree').replaceChildren();
    src.treeStarted = false;
  }
}

function startTree() {
  const s = currentSource();
  if (!s || !s.mounted || src.treeStarted) return;
  src.treeStarted = true;
  src.rows.clear();
  src.ticked.clear();
  const tree = $('src-tree');
  const ul = h('ul', { className: 'tree' });
  const actions = h('div', { className: 'sticky' });
  src.summary = h('span', { className: 'muted', 'aria-live': 'polite' });
  src.start = button('Start harvest', () => startRun(false), 'primary', { disabled: true });
  src.dry = button('Dry run', () => startRun(true), '', { disabled: true });
  const tickNewBtn = button('Tick folders not yet harvested', tickNew);
  actions.append(h('div', { className: 'row' }, src.start, src.dry, tickNewBtn, src.summary));
  tree.replaceChildren(ul, actions);
  const rootLi = nodeFor({ name: `Whole source (${sourceName(s)})`, relPath: '' }, true, [], '');
  ul.append(rootLi);
  expand(rootLi, '');
  syncTicks();
}

/** A tree row; `status` is its folder status line. */
function nodeFor(entry, isRoot, chain, status) {
  const input = h('input', {
    type: 'checkbox',
    onchange: () => {
      if (input.checked) src.ticked.add(entry.relPath);
      else src.ticked.delete(entry.relPath);
      syncTicks();
    },
  });
  const statusEl = h('span', { className: 'status' }, status);
  const li = h('li');
  const twisty = button('▸', () => toggle(li, entry.relPath, twisty), 'small twisty', {
    'aria-expanded': 'false',
    'aria-label': `Expand ${entry.name}`,
  });
  li.append(
    h(
      'div',
      { className: 'node' },
      isRoot ? h('span', { className: 'twisty' }) : twisty,
      h('label', {}, input, h('span', {}, entry.name)),
      statusEl,
    ),
  );
  src.rows.set(entry.relPath, { input, entry, li, isRoot, chain, statusEl });
  return li;
}

/** The twisty's state: aria-expanded and its glyph together. */
function setExpanded(twisty, open) {
  twisty.setAttribute('aria-expanded', String(open));
  twisty.textContent = open ? '▾' : '▸';
}

async function toggle(li, relPath, twisty) {
  const open = twisty.getAttribute('aria-expanded') === 'true';
  const sub = li.querySelector(':scope > ul');
  if (open) {
    setExpanded(twisty, false);
    if (sub) sub.hidden = true;
    return;
  }
  setExpanded(twisty, true);
  if (sub && !sub.dataset.failed) sub.hidden = false;
  else {
    if (sub) sub.remove(); // a failed load: try again
    if (!(await expand(li, relPath))) setExpanded(twisty, false);
  }
}

/** Loads one level below relPath and shows it under li. */
async function expand(li, relPath) {
  const s = currentSource();
  // This row's own status line, held now: another source may be open when the reply arrives.
  const statusEl = src.rows.get(relPath).statusEl;
  const ul = h('ul', {}, h('li', { className: 'muted' }, 'Loading…'));
  li.append(ul);
  let data;
  try {
    data = await api(
      `/api/sources/${encodeURIComponent(src.id)}/tree?path=${encodeURIComponent(relPath)}`,
    );
  } catch (e) {
    // The error row stays; expanding again retries.
    ul.dataset.failed = '1';
    ul.replaceChildren(h('li', { className: 'bad', role: 'alert' }, e.message));
    return false;
  }
  const cov = data.coveredBy;
  // The listed path's own status (root row only; sub-rows got theirs from their parent's listing).
  if (relPath === '') {
    const own = cov && cov.relPath === '' ? { lastHarvestedAt: cov.lastHarvestedAt } : {};
    statusEl.textContent = folderStatus(own, []);
  }
  const chain = cov
    ? [
        {
          name: `${sourceName(s)}/${cov.relPath ? `${cov.relPath}/` : ''}`,
          lastHarvestedAt: cov.lastHarvestedAt,
        },
      ]
    : [];
  ul.replaceChildren(
    ...(data.entries.length === 0 ? [h('li', { className: 'muted' }, 'No sub-folders.')] : []),
    ...data.entries.map((e) => nodeFor(e, false, chain, folderStatus(e, chain))),
  );
  syncTicks();
  return true;
}

/** Whether a ticked folder above `p` includes it. */
const includedByTick = (p) =>
  [...src.ticked].some((t) => t !== p && (t === '' || p.startsWith(`${t}/`)));

/** A ticked folder includes everything below it: its descendants show ticked and locked. */
function syncTicks() {
  if (!src) return;
  for (const [p, row] of src.rows) {
    const locked = includedByTick(p);
    row.input.checked = src.ticked.has(p) || locked;
    row.input.disabled = locked;
  }
  const picked = selection();
  src.start.disabled = picked.length === 0;
  src.dry.disabled = picked.length === 0;
  src.summary.textContent =
    picked.length === 0
      ? 'Nothing ticked'
      : `${picked.length} folder${picked.length === 1 ? '' : 's'} ticked`;
}

function selection() {
  return [...src.ticked].filter((p) => !includedByTick(p)).sort();
}

function tickNew() {
  for (const [p, row] of src.rows) {
    // Never the whole-source row: that would re-harvest everything.
    if (!row.isRoot && defaultTicked(row.entry, row.chain)) src.ticked.add(p);
  }
  syncTicks();
}

async function startRun(dryRun) {
  const s = currentSource();
  const folders = selection().map((relPath) => ({ sourceId: src.id, relPath }));
  const whole = folders.some((f) => f.relPath === '');
  if (!dryRun && whole) {
    const ok = await confirmDialog(
      `Harvest all of ${sourceName(s)}?`,
      `This scans the whole source (${bytes(s.usedBytes)} used) and can take many hours. Continue?`,
      'Start harvest',
    );
    if (!ok) return;
  }
  const msg = $('src-msg');
  src.start.disabled = src.dry.disabled = true;
  try {
    const r = await post('/api/runs', { folders, dryRun });
    msg.replaceChildren(
      message(
        `${runName(dryRun, r.runId)} ${r.state === 'queued' ? 'queued behind the active run' : 'started'}.`,
      ),
    );
    src.ticked.clear();
    await refreshState();
  } catch (e) {
    msg.replaceChildren(message(e.message, true));
  }
  syncTicks();
}

// ---------------------------------------------------------------- 4. review
async function openReview() {
  const d = await loadInto('review', [h('h2', {}, 'Possible photos')], '/api/review');
  if (d) paintReviewGroups(d.groups);
}

function paintReviewGroups(groups) {
  const m = main();
  const holder = h('div');
  m.replaceChildren(
    h('h2', {}, 'Possible photos'),
    h(
      'p',
      { className: 'muted' },
      'Files that might be photos. Send the ones you want kept; skip the rest.',
    ),
    holder,
  );
  if (groups.length === 0) return holder.append(h('p', {}, 'Nothing waiting for review.'));
  for (const g of groups) {
    const card = h('div', { className: 'card' });
    const msg = h('div');
    const act = (action) => async (e) => {
      const btn = e.currentTarget; // null after the first await
      btn.disabled = true;
      try {
        const r = await post('/api/review', { sourceId: g.sourceId, path: g.path, action });
        card.replaceChildren(
          message(`${r.changed} file${r.changed === 1 ? '' : 's'} ${decided(action, r)}.`),
        );
        refreshState();
      } catch (err) {
        btn.disabled = false;
        msg.replaceChildren(message(err.message, true));
      }
    };
    card.append(
      h('div', {}, h('strong', {}, `${g.label}/${g.path}`)),
      h('div', { className: 'muted' }, `${count(g.count)} files · ${bytes(g.bytes)}`),
      h(
        'div',
        { className: 'row' },
        button('Send all', act('send'), 'primary small'),
        button('Skip all', act('skip'), 'small'),
        h(
          'a',
          {
            href: `#/review/${encodeURIComponent(g.sourceId)}/${g.path.split('/').map(encodeURIComponent).join('/')}`,
          },
          'Review',
        ),
      ),
      msg,
    );
    holder.append(card);
  }
}

async function openReviewFiles() {
  const { id, path } = route;
  const back = h('p', {}, h('a', { href: '#/review' }, '← Possible photos'));
  const d = await loadInto(
    'review-files',
    [back, h('h2', {}, path || '(top of drive)')],
    `/api/review?sourceId=${encodeURIComponent(id)}&path=${encodeURIComponent(path)}`,
  );
  if (!d) return;
  const { files, label, problem } = d;
  const grid = h('div', { className: 'grid' });
  const msg = h('div');
  const decide = (item, fig, action) => async () => {
    const buttons = fig.querySelectorAll('button');
    for (const b of buttons) b.disabled = true;
    try {
      const r = await post('/api/review', { itemIds: [item.id], action });
      fig.remove();
      msg.replaceChildren(message(`${item.name}: ${decided(action, r)}.`));
      refreshState();
    } catch (e) {
      for (const b of buttons) b.disabled = false;
      msg.replaceChildren(message(e.message, true));
    }
  };
  for (const f of files) {
    const fig = h('figure');
    fig.append(
      problem
        ? h('div', { className: 'thumb-note' }, problem.tile)
        : h('img', {
            src: `/api/thumb/${encodeURIComponent(f.id)}`,
            loading: 'lazy',
            alt: f.name,
            width: 256,
            height: 256,
          }),
      h('figcaption', {}, `${f.name} · ${bytes(f.sizeBytes)}`),
      h(
        'div',
        { className: 'row' },
        button('Send', decide(f, fig, 'send'), 'small primary'),
        button('Skip', decide(f, fig, 'skip'), 'small'),
      ),
    );
    grid.append(fig);
  }
  main().replaceChildren(
    back,
    h('h2', {}, `${label} · ${path || '(top of drive)'}`),
    problem && files.length ? message(problem.detail, true) : '',
    msg,
    files.length ? grid : h('p', {}, 'No files left in this folder.'),
  );
}

// ---------------------------------------------------------------- 5. where
function openWhere() {
  const input = h('input', {
    type: 'search',
    placeholder: 'sha256 or file name',
    autocomplete: 'off',
    'aria-label': 'sha256 or file name',
  });
  const out = h('div', { 'aria-live': 'polite' });
  const form = h(
    'form',
    {
      className: 'row',
      onsubmit: async (e) => {
        e.preventDefault();
        const q = input.value.trim();
        if (!q) return;
        out.replaceChildren(h('p', { className: 'muted' }, 'Searching…'));
        try {
          const { items } = await api(`/api/where?q=${encodeURIComponent(q)}`);
          out.replaceChildren(
            items.length ? whereTable(items) : h('p', {}, 'No source path holds those bytes.'),
          );
        } catch (err) {
          out.replaceChildren(message(err.message, true));
        }
      },
    },
    input,
    h('button', { className: 'btn primary', type: 'submit' }, 'Find'),
  );
  main().replaceChildren(
    h('h2', {}, 'Find a photo'),
    h(
      'p',
      { className: 'muted' },
      'Paste a sha256, or a file name from the vault, staging or a source.',
    ),
    form,
    out,
  );
  input.focus();
}

function whereTable(items) {
  const th = (t) => h('th', { scope: 'col' }, t);
  return h(
    'div',
    { className: 'scroll-x' },
    h(
      'table',
      {},
      h(
        'thead',
        {},
        h('tr', {}, th('Source path'), th('State'), th('Size'), th('Sent'), th('Modified')),
      ),
      h(
        'tbody',
        {},
        ...items.map((i) =>
          h(
            'tr',
            {},
            h(
              'td',
              {},
              `${i.source}/${i.relPath}`,
              i.sha256 ? h('div', { className: 'muted' }, i.sha256.slice(0, 16) + '…') : '',
            ),
            h('td', {}, `${i.state} (${i.kind})`),
            h('td', {}, bytes(i.sizeBytes)),
            h('td', {}, i.sentAt ? ago(i.sentAt) : '–'),
            h('td', {}, day(i.mtime) || '–'),
          ),
        ),
      ),
    ),
  );
}

// ---------------------------------------------------------------- 6. history
async function openHistory() {
  const d = await loadInto('history', [h('h2', {}, 'History')], '/api/runs?limit=50');
  if (!d) return;
  main().replaceChildren(
    h('h2', {}, 'History'),
    d.runs.length === 0 ? h('p', { className: 'muted' }, 'No runs yet.') : '',
    ...d.runs.map(runCard),
  );
}

function runCard(run) {
  const t = runTotals(run.folders);
  const junk = junkSummary(run.folders);
  const keys = run.dry_run
    ? ['photos', 'known', 'dups', 'maybes', 'junk', 'dirs']
    : ['sent', 'known', 'dups', 'errors', 'maybes', 'junk'];
  const items = keys.map((k) => [k === 'photos' ? 'Would send' : COUNT_LABELS[k], count(t[k])]);
  return h(
    'div',
    { className: 'card' },
    h(
      'div',
      { className: 'row between' },
      h('strong', {}, runName(run.dry_run, run.id)),
      h(
        'span',
        { className: run.state === 'failed' ? 'bad' : 'muted' },
        `${run.state} · ${ago(run.created_at)}`,
      ),
    ),
    h('div', { className: 'muted' }, folderList(run.folders)),
    h('dl', { className: 'counts' }, ...countItems(items)),
    junk ? h('div', { className: 'muted' }, `Junk by reason: ${junk}`) : '',
    run.error ? h('div', { className: 'bad' }, run.error) : '',
  );
}

// ---------------------------------------------------------------- start
navigate();
tick();
