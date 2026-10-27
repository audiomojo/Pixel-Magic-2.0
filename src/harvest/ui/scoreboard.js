// The Picks tab's scoreboard: the headline match score, the trend by model version, the
// disagreement list and the describe pause/resume control.
import { scoreLine } from './format.js';

const preview = (sha, w) => `/api/preview/${sha}?w=${w}`;

/** Fills `board` from /api/picks/scoreboard; `guarded` runs a button's action once at a time. */
export async function paintScoreboard(ui, board, guarded, isEnded) {
  const { h, api, post, button, message } = ui;
  try {
    const s = await api('/api/picks/scoreboard');
    if (isEnded()) return;
    const paused = s.describe.paused;
    const items = [
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
