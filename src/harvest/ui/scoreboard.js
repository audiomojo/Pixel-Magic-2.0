// The Picks tab's scoreboard: the scorecard (verdict, the model against its baselines, the
// blind check, the learning curve), then the headline match score, the trend by model version,
// the disagreement list and the describe pause/resume control.
import { scoreLine } from './format.js';

const preview = (sha, w) => `/api/preview/${sha}?w=${w}`;
const NUDGE_AFTER_MS = 7 * 86_400_000;
const BLIND_ENOUGH = 30;
const pct = (x) => `${Math.round(x * 100)}%`;
const range = (r) => `${pct(r.lo)}–${pct(r.hi)}`;
const SVG = 'http://www.w3.org/2000/svg';

/** Like h(), for SVG elements (createElement would make HTMLUnknownElements). */
function svg(tag, attrs, ...children) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {}))
    if (v !== null && v !== false) node.setAttribute(k, String(v));
  for (const c of children.flat(Infinity)) if (c) node.append(c);
  return node;
}

/** The three-row table: the model and the rule with 95% intervals, chance as an exact value. */
function rowsTable(h, card) {
  const row = (name, cells) =>
    h('tr', {}, h('th', { scope: 'row' }, name), ...cells.map((c) => h('td', {}, c)));
  return h(
    'table',
    { className: 'rows' },
    h('caption', {}, `Agreement with your picks, on ${card.n} held-out picks`),
    h(
      'tbody',
      {},
      row('AI model', card.model ? [pct(card.model.pct), range(card.model)] : ['—', '']),
      row(
        'Rule score alone',
        card.rule
          ? [
              pct(card.rule.pct),
              `${range(card.rule)} · no preference on ${card.rule.ties} of these`,
            ]
          : ['—', 'not recorded'],
      ),
      row(
        'Random guess',
        card.chance !== null ? [pct(card.chance), 'exact for these sets'] : ['—', ''],
      ),
    ),
  );
}

/** The blind check: random sets the model graded before you chose. */
function blindLine(h, b) {
  const by = (name, c) => (c.of > 0 ? `${name} ${c.matched} of ${c.of}` : null);
  const split = [by('by the rule score', b.byRuleOnly), by('by a fitted model', b.byFitted)].filter(
    Boolean,
  );
  return h(
    'p',
    { className: b.enough ? '' : 'muted' },
    `Blind check: matched ${b.matched} of your last ${b.of} random sets`,
    b.of > 0 ? ` (${range(b)}); guessing would match ${pct(b.chance)}` : '',
    split.length ? `; ${split.join(', ')}` : '',
    '. ',
    b.enough ? '' : `Too few to read yet (needs ${BLIND_ENOUGH}).`,
  );
}

/**
 * The learning curve: one dot per fitted version at (held-out picks, agreement), hollow when
 * unpromoted, in a light 95% band; the rule and chance baselines as lines through every
 * version's own values. Null when no version has anything to plot.
 */
function curveSvg(curve) {
  const plotted = curve.filter((c) => c.cvN !== null && c.cvN > 0);
  if (plotted.length === 0) return null;
  const W = 420,
    H = 180,
    L = 36,
    R = 44,
    T = 10,
    B = 24;
  const maxN = Math.max(...plotted.map((c) => c.cvN)) * 1.05;
  const x = (n) => L + ((W - L - R) * n) / maxN;
  const y = (v) => T + (H - T - B) * (1 - v);
  const byN = (a, b) => a.cvN - b.cvN;
  const line = (key, cls) => {
    const pts = plotted.filter((c) => c[key] !== null).sort(byN);
    if (pts.length === 0) return [];
    const d = pts
      .map((c, i) => `${i ? 'L' : 'M'}${x(c.cvN).toFixed(1)},${y(c[key]).toFixed(1)}`)
      .join(' ');
    const last = pts[pts.length - 1];
    return [
      svg('path', { d, class: cls, fill: 'none' }),
      svg('text', { x: x(last.cvN) + 4, y: y(last[key]) + 4, class: 'label' }, key),
    ];
  };
  const dots = plotted.filter((c) => c.fitted && c.cv !== null).sort(byN);
  const band = dots.filter((c) => c.lo !== null);
  const bandPath =
    band.length > 1
      ? [
          ...band.map((c) => `${x(c.cvN).toFixed(1)},${y(c.hi).toFixed(1)}`),
          ...band
            .slice()
            .reverse()
            .map((c) => `${x(c.cvN).toFixed(1)},${y(c.lo).toFixed(1)}`),
        ].join(' ')
      : null;
  return svg(
    'svg',
    { class: 'curve', viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': 'Learning curve' },
    [0, 0.5, 1].map((v) => [
      svg('line', { x1: L, x2: W - R, y1: y(v), y2: y(v), class: 'grid' }),
      svg('text', { x: L - 6, y: y(v) + 4, class: 'label', 'text-anchor': 'end' }, pct(v)),
    ]),
    svg(
      'text',
      { x: (L + W - R) / 2, y: H - 6, class: 'label', 'text-anchor': 'middle' },
      'picks the version was tested on',
    ),
    bandPath && svg('polygon', { points: bandPath, class: 'band' }),
    line('chance', 'chance'),
    line('rule', 'rule'),
    dots.length > 1 &&
      svg('path', {
        d: dots
          .map((c, i) => `${i ? 'L' : 'M'}${x(c.cvN).toFixed(1)},${y(c.cv).toFixed(1)}`)
          .join(' '),
        class: 'model',
        fill: 'none',
      }),
    dots.map((c) =>
      svg(
        'circle',
        { cx: x(c.cvN), cy: y(c.cv), r: 4, class: c.promoted ? 'dot' : 'dot hollow' },
        svg(
          'title',
          {},
          `v${c.version}: ${pct(c.cv)} on ${c.cvN} picks${c.promoted ? '' : ' (not promoted)'}`,
        ),
      ),
    ),
  );
}

/** The scorecard block: verdict, nudge, the rows, the blind check and the curve. */
function scorecard(h, s) {
  const card = s.scorecard;
  const stale = s.lastPickAt !== null && Date.now() - Date.parse(s.lastPickAt) > NUDGE_AFTER_MS;
  const curve = curveSvg(s.curve);
  return h(
    'div',
    { className: 'scorecard' },
    h('p', { className: 'headline' }, card.verdict.text),
    stale &&
      h(
        'p',
        { className: 'muted' },
        `No picks since ${s.lastPickAt.slice(0, 10)}. The model only learns from new picks.`,
      ),
    card.n !== null && rowsTable(h, card),
    blindLine(h, card.blind),
    curve,
    curve &&
      h(
        'p',
        { className: 'muted' },
        'Filled dots are promoted versions, hollow ones were kept unpromoted; the shaded band is the 95% interval. Consecutive versions share most of their picks, so the line is expected to look smooth: read the band, not the wiggle.',
      ),
  );
}

/** Fills `board` from /api/picks/scoreboard; `guarded` runs a button's action once at a time. */
export async function paintScoreboard(ui, board, guarded, isEnded) {
  const { h, api, post, button, message } = ui;
  try {
    const s = await api('/api/picks/scoreboard');
    if (isEnded()) return;
    const paused = s.describe.paused;
    const items = [
      scorecard(h, s),
      h('p', { className: 'headline' }, scoreLine(s.headline)),
      s.byVersion.length > 0 &&
        h(
          'p',
          { className: 'muted' },
          'By model version: ',
          s.byVersion.map((v) => `v${v.version} ${v.matched} of ${v.of}`).join(', '),
        ),
      h('p', { className: 'muted' }, `${s.totalPicks} picks in all`),
      s.changedByLatest !== null &&
        h(
          'p',
          { className: 'muted' },
          `${s.changedByLatest} sets the latest model picks differently`,
        ),
      s.failedPhotos > 0 &&
        h('p', { className: 'muted' }, `${s.failedPhotos} photos could not be read`),
      h(
        'p',
        { className: 'row' },
        `Describing ${paused === null ? 'status unknown' : paused ? 'is paused' : 'is running'}, ${s.describe.backlog} photos waiting. `,
        paused !== null &&
          button(paused ? 'Resume describing' : 'Pause describing', () =>
            guarded(async () => {
              await post(`/api/picks/describe/${paused ? 'resume' : 'pause'}`);
              await paintScoreboard(ui, board, guarded, isEnded);
            }),
          ),
      ),
      s.disagreements.length > 0 &&
        h(
          'details',
          {},
          h('summary', {}, `Where we disagree (${s.disagreements.length})`),
          h(
            'ul',
            { className: 'disagree' },
            s.disagreements.map((d) =>
              h(
                'li',
                {},
                `Set ${d.setId}: you chose `,
                h('img', {
                  src: preview(d.chosen, 400),
                  alt: d.chosen.slice(0, 8),
                  loading: 'lazy',
                }),
                ' it picked ',
                h('img', {
                  src: preview(d.modelPick, 400),
                  alt: d.modelPick.slice(0, 8),
                  loading: 'lazy',
                }),
              ),
            ),
          ),
        ),
    ];
    board.replaceChildren(...items);
  } catch (e) {
    board.replaceChildren(message(e.message, true));
  }
}
