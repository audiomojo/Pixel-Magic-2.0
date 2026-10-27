// The Picks tab: teach the taste model which photo is best in each look-alike set (blind), see
// what it picked, and watch the scoreboard. Server text goes in with textContent only.
// Reasons travel with the pick POST (the API takes them there), so the reason chips are shown
// BEFORE confirming: choose, tick any reasons, then Confirm (or Enter) stores the pick and reveals.
import { paintScoreboard } from './scoreboard.js';

const REASONS = ['sharper', 'eyes open', 'expression', 'framing', 'exposure'];
const preview = (sha, w) => `/api/preview/${sha}?w=${w}`;

/** What the reveal says: the pick response against the user's choice. */
function revealText(r, none) {
  if (none) return 'No clear best stored.';
  if (r.modelPick === null) return 'No model pick yet for this set.';
  return r.matched ? 'It picked the same' : 'It picked this one';
}

/** `ui` = {main, h, api, post, button, message, routeIs}; app.js owns routing. */
export function openPicks(ui) {
  const { h, api, post, button, message, routeIs } = ui;
  const main = ui.main();
  const board = h('section', { className: 'panel', 'aria-label': 'Scoreboard' });
  const teach = h('section', { 'aria-label': 'Teach' });
  main.replaceChildren(h('h2', {}, 'Picks'), board, teach);

  let cur = null; // {set, token, today}
  let choice = null; // index of the chosen candidate, 'none', or null
  let reasons = new Set();
  let revealed = false;
  let busy = false;
  let zoomed = false;
  let done = 0;
  let goal = 10;
  let counter, live, grid, actions;
  let ended = false;
  let skipped = new Set();

  // ---- session end: once per visit, on leaving the tab and on pagehide ----------------
  const end = () => {
    if (ended) return;
    ended = true;
    document.removeEventListener('keydown', onKey);
    window.removeEventListener('hashchange', onHash);
    window.removeEventListener('pagehide', end);
    fetch('/api/picks/session-end', {
      method: 'POST',
      keepalive: true,
      headers: { 'X-Pixel-Magic': '1', 'Content-Type': 'application/json' },
      body: '{}',
    }).catch(() => {});
  };
  const onHash = () => {
    if (!routeIs('picks')) end();
  };
  window.addEventListener('hashchange', onHash);
  window.addEventListener('pagehide', end);

  const paintBoard = () => paintScoreboard(ui, board, guarded, () => ended);

  // ---- the teaching screen -----------------------------------------------------------------
  const guarded = async (fn) => {
    if (busy) return;
    busy = true;
    try {
      await fn();
    } catch (e) {
      teach.prepend(message(e.message, true));
    } finally {
      busy = false;
    }
  };

  async function load() {
    const data = await api(
      `/api/picks/next${skipped.size ? `?skip=${[...skipped].join(',')}` : ''}`,
    );
    if (ended) return;
    cur = data;
    choice = null;
    reasons = new Set();
    revealed = false;
    if (!data.set) {
      teach.replaceChildren(
        h(
          'p',
          { className: 'muted' },
          'No look-alike sets are waiting. Harvest more photos, or wait for describing to catch up.',
        ),
      );
      return;
    }
    done = data.today.done;
    goal = data.today.goal;
    paintSet();
  }

  function paintSet() {
    const { set } = cur;
    counter = h('p', { className: 'counter' }, `${done}/${goal} today`);
    live = h('p', { className: 'reveal', role: 'status', 'aria-live': 'polite' });
    actions = h('div', { className: 'row' });
    grid = h(
      'div',
      { className: `picks-grid${zoomed ? ' zoomed' : ''}` },
      set.candidates.map((c, i) => {
        const frame = h(
          'div',
          { className: 'frame', onscroll: (ev) => linkScroll(ev.currentTarget) },
          h('img', {
            src: preview(c.sha256, zoomed ? 1600 : 400),
            alt: c.name,
            onclick: toggleZoom,
          }),
        );
        return h(
          'figure',
          { 'data-sha': c.sha256 },
          frame,
          h(
            'figcaption',
            {},
            h('span', { className: 'name' }, c.name),
            ' ',
            i < 9 && h('kbd', {}, String(i + 1)),
          ),
          button(`Choose ${i < 9 ? i + 1 : ''}`.trim(), () => choose(i), '', {
            'aria-label': `Choose ${c.name}`,
          }),
        );
      }),
    );
    teach.replaceChildren(
      counter,
      grid,
      h(
        'p',
        { className: 'muted' },
        'Keys: 1-9 choose, N no clear best, S skip, Z zoom, Enter confirms or moves on.',
      ),
      actions,
      live,
    );
    paintActions();
  }

  function paintActions() {
    if (!cur?.set || !actions) return;
    actions.replaceChildren();
    for (const [i, f] of [...grid.children].entries()) f.classList.toggle('chosen', choice === i);
    if (revealed) {
      actions.append(button('Next', () => guarded(load), 'primary'));
    } else if (choice !== null) {
      actions.append(
        h(
          'span',
          { className: 'chips pending', role: 'group', 'aria-label': 'Why (optional)' },
          REASONS.map((r) =>
            h(
              'button',
              {
                type: 'button',
                className: 'btn chip',
                'aria-pressed': String(reasons.has(r)),
                'data-chip': r,
                onclick: (ev) => {
                  if (!reasons.delete(r)) reasons.add(r);
                  ev.currentTarget.setAttribute('aria-pressed', String(reasons.has(r)));
                },
              },
              r,
            ),
          ),
        ),
        button('Confirm', confirm, 'primary'),
      );
    } else {
      actions.append(
        button('No clear best', () => choose('none')),
        button('Skip', skip),
      );
    }
  }

  function choose(c) {
    if (revealed || busy || !cur?.set) return;
    choice = c;
    paintActions();
  }

  const skip = () => {
    if (revealed || !cur?.set) return;
    skipped.add(cur.set.id);
    guarded(load);
  };

  function confirm() {
    if (revealed || choice === null) return;
    guarded(async () => {
      const body = { token: cur.token, reasons: [...reasons] };
      if (choice === 'none') body.noClearBest = true;
      else body.chosen = cur.set.candidates[choice].sha256;
      const r = await post('/api/picks', body);
      revealed = true;
      done += 1;
      counter.textContent = `${done}/${goal} today`;
      const pick = r.modelPick;
      for (const f of grid.children) f.classList.toggle('model-pick', f.dataset.sha === pick);
      live.textContent = revealText(r, choice === 'none');
      paintActions();
      actions.querySelector('button')?.focus();
      await paintBoard();
    });
  }

  // ---- zoom, with the scroll position linked across candidates -------------------------------
  function toggleZoom() {
    if (!grid) return;
    zoomed = !zoomed;
    grid.classList.toggle('zoomed', zoomed);
    for (const img of grid.querySelectorAll('img')) {
      const sha = img.closest('figure').dataset.sha;
      img.src = preview(sha, zoomed ? 1600 : 400);
    }
  }
  let linking = false;
  function linkScroll(from) {
    if (linking) return;
    linking = true;
    const fx = from.scrollLeft / Math.max(1, from.scrollWidth - from.clientWidth);
    const fy = from.scrollTop / Math.max(1, from.scrollHeight - from.clientHeight);
    for (const f of grid.querySelectorAll('.frame')) {
      if (f === from) continue;
      f.scrollLeft = fx * (f.scrollWidth - f.clientWidth);
      f.scrollTop = fy * (f.scrollHeight - f.clientHeight);
    }
    requestAnimationFrame(() => (linking = false));
  }

  // ---- keys: only on this view, never while typing --------------------------------------------
  function onKey(ev) {
    if (!routeIs('picks') || ev.metaKey || ev.ctrlKey || ev.altKey) return;
    const t = ev.target;
    if (
      t instanceof HTMLElement &&
      (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))
    )
      return;
    const k = ev.key.toLowerCase();
    if (k === 'enter') {
      // A focused button handles its own Enter, except the reason chips (Enter confirms).
      if (t instanceof HTMLButtonElement && !t.dataset.chip) return;
      if (revealed) guarded(load);
      else confirm();
    } else if (/^[1-9]$/.test(k)) {
      if (Number(k) <= (cur?.set?.candidates.length ?? 0)) choose(Number(k) - 1);
    } else if (k === 'n') choose('none');
    else if (k === 's') skip();
    else if (k === 'z') toggleZoom();
    else return;
    ev.preventDefault();
  }
  document.addEventListener('keydown', onKey);

  paintBoard();
  guarded(load);
}
