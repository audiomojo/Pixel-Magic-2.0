# Prompt: a scorecard that shows whether the taste model is learning

Paste this into a Claude Code session opened in `pixel-magic-2.0`. It changes how the existing
similar-picks model is **measured and reported**. It adds no model and no new picking workflow,
and it does not change the features, the fit or the promotion rule. The one change to the
training code is the definition of "agreement" (ties, §1a below), which is a measurement.

---

## Who I am and how to work with me

I'm an engineering leader who is comfortable with software but new to hands-on ML. Treat this as
a teaching project: the first time an ML concept comes up (baseline, cross-validation, held-out
data, learning curve, confidence interval, paired test, tie credit), explain it in two or three
sentences. Keep code simple and in the style of the surrounding code.

Before writing code, read these and propose a short plan. Wait for my OK before you start.

- `docs/superpowers/specs/2026-10-26-similar-picks-design.md` (§6 and §8: the model and scoreboard)
- `ai/pixelmagic_ai/ranker.py`, `ai/pixelmagic_ai/activities.py`, `ai/pixelmagic_ai/db.py`, and
  `ai/tests/test_ranker.py`, `ai/tests/test_train.py`
- `src/harvest/picks.ts` (`scoreboard()`), `src/harvest/ui/scoreboard.js`, `src/harvest/ui/app.css`,
  and the scoreboard tests in `tests/unit/harvestPicks.test.ts` and `tests/ui/harvestPage.test.ts`

Write tests first, keep all suites passing, and commit once at the end. The suites are:
`npm test`, `npm run test:ui`, `npm run lint && npm run typecheck`, and `cd ai && uv run pytest`.
When you're done, tell me how to see the scorecard, what I should see, and what the numbers say
today.

## Where things stand (live database, 2026-11-17)

- **Picks:** 89, of which 12 are "no clear best", so 77 are usable. 61 of the 77 are two-photo
  sets, so for most picks a coin flip is right half the time. All picks were made on four days
  (2026-10-27, 10-28, 11-02, 11-03); none since 2026-11-03. No set has been picked twice.
- **Model:** 14 versions. v1–v5 are the rule score (v2–v5 kept unpromoted); v6–v14 are fitted
  models, trained on 27 → 76 picks. Their stored cross-validated agreement is 0.59–0.63, flat.
- **Chance on the same picks** (mean of 1/candidates shown): 44.8%.
- **The rule score has never been recorded.** An offline re-run of `ranker.py` on the 77 picks
  (same code, today's features) gives these approximate figures; the backfill below will produce
  the exact ones:
  - Rule score: **41.6%** when a tie at the top counts as a miss (the code's current definition),
    **53.9%** when a tie is split (1/k credit). The rule tied on 19 of 77 picks: only 22 of the
    200 candidates have a detected face, and for faceless pairs the rule is just
    "sharpness vs aesthetic", which often cancels to 0 = 0.
  - Model (best λ, which is 10, the top of the grid): **61–62%**, 95% interval about 50–72%.
    Re-shuffling the folds with other seeds moves this between 0.53 and 0.64, so the "flat
    0.59–0.63" curve is within fold noise.
  - Model vs chance: 47 of 77 right against 34.5 expected, about 3 standard deviations above
    chance. The model is real.
  - Model vs rule, paired on the same picks (tie-split): the model beat the rule on 20 picks and
    lost on 11; that split happens by luck about 1 time in 7 (p ≈ 0.15). **Not yet
    distinguishable from the rule.** If the gap holds, about 120–130 picks in all should settle it.
  - Dropping the 32 embedding features changes nothing (62–63%); the fitted weights sit almost
    entirely on `rule`, `eyes_open_min` and `sharpness_rel`.
- **Blind random picks graded on the scoreboard:** 21. The model matched 12 (57%); chance on
  those sets is 41.7%; 9 of the 21 were graded by the rule-only v1, 12 by a fitted model (8
  matched). Far too few to read.
- **Reasons I gave:** expression 35, framing 30, exposure 18, eyes open 9, sharper 6. The
  features measure sharpness, exposure and faces; almost nothing measures expression or framing.

So today: the model is clearly better than guessing, not yet shown to be better than the rule
score it started from, and the scoreboard says none of this. The scorecard should.

## What to build

### 1. Record baselines with every model

These go in `ranker.py`, `db.py`, `activities.py` and a migration `0012_scorecard.sql` with a
Down section.

**1a. One definition of agreement, with tie credit.** Today `_agrees` needs the chosen photo to
score *strictly* highest, so a tie at the top is a miss. That punishes the rule score (which
ties on a quarter of my picks) and would make "beats the rule" trivially true. Replace it with
a credit: 1 when the chosen photo alone scores highest, 1/k when it is one of k tied at the
top, 0 otherwise. Agreement = mean credit over the held-out picks. Use this everywhere in
`ranker.py` (CV, the rule figure, promotion). For a fitted model with continuous weights it
changes almost nothing (test: on synthetic picks the two definitions differ by at most one
pick's worth); for the rule it is the fair figure. Treat two scores as tied when they are within
`1e-9`. The live board's grading of `model_pick_sha256` is a different code path (a tie there
means no model pick, so the set is ungraded) and stays as it is.

**1b. Columns on `ranker_models`,** all nullable so older rows stay valid:

| Column | Meaning |
|---|---|
| `cv_n int` | held-out picks behind the figures (usable picks when the version was trained) |
| `chance_agreement real` | mean of 1/candidates over those picks |
| `rule_agreement real` | the rule score's tie-credit agreement over those picks |
| `rule_ties int` | how many of those picks the rule tied on |
| `model_wins int` | picks where the model's held-out credit beat the rule's |
| `rule_wins int` | picks where the rule's credit beat the model's |

`model_wins` and `rule_wins` are the paired counts: the model and the rule are graded on the
same picks, so the question "which is better" is answered by the picks where they differ, not
by comparing two separate percentages. Picks where both are right or both wrong carry no
information about the difference.

**1c. Compute everything on exactly the same held-out picks:**

- `cv_agreement` returns the per-pick credits (in pick order) along with the figure; `train()`
  returns a small result (model, cv, promoted, and the six figures above) that
  `activities._train_locked` stores. The rule credits come from `RULE_ONLY`, which has no
  fitting step, so it is evaluated on every usable pick directly.
- The per-pick model credits are those of the **chosen λ**. Compute wins/losses from them and
  the rule credits with the same `1e-9` tolerance.
- Below `MIN_TRAIN_PICKS`, where `train()` returns `RULE_ONLY` and reports the rule's agreement
  as `cv_agreement`, store `cv_n`, `chance_agreement`, `rule_agreement` and `rule_ties` over all
  usable picks and `model_wins = rule_wins = 0`. The UI tells these versions apart by
  `picks_used = 0`.

**1d. Fold the CV by set, not by pick.** The spec says "by set", and the code permutes picks.
This doesn't matter today, because no set has two picks, but keep the two in line so a second
pick on a set can never land in a different fold from the first (that would be the model seeing
the answer to a held-out question).

- Add `set_id` to `db.Pick`. It's already in the `picks` table, so this is still a read-only
  SELECT.
- Permute sets, ordered by their first pick. When every set has a single pick, this gives
  exactly today's folds, so today's CV figures don't move (other than by tie credit). Test that.

**1e. Backfill the existing versions** with a small one-off command, so the history has
baselines too: `cd ai && uv run python -m pixelmagic_ai.scorecard backfill`.

- Recompute each version with the picks that existed when it was created
  (`picks.decided_at < ranker_models.created_at`), using that version's `feature_version`.
- Only fill nulls, so running it twice changes nothing.
- Recompute the version's CV as a check. For a fitted version compare it with the stored
  `cv_agreement` and the recomputed `cv_n` with `picks_used`; log any mismatch (a stored strict
  figure may differ from the tie-credit one by up to `rule_ties / n`; a photo described after
  the version was trained can raise `cv_n` above `picks_used`). Never overwrite `cv_agreement`
  or `picks_used`.
- The recomputation can only use today's sets and features: sets merge over time and more
  photos get described. The `created_at` filter on picks is exact, but the inputs around
  those picks are today's. Say so in the spec.
- v1 was trained on zero picks (`cv_agreement = 0.0`, `cv_n = 0`): fill its baselines as null
  and skip it on the curve.

### 2. The scorecard (top of the Picks tab, extending `scoreboard.js`)

**A verdict line** in plain words, chosen by these rules, in this order:

1. **Baselines not recorded yet:** the current model's row has null baselines. Say so, and say
   to run the backfill.
2. **Still the rule score:** the current model has `picks_used = 0`. "N usable picks so far;
   learning starts at 25." (Today's live model is fitted, so this state is for a reset library.)
3. **Not beating chance:** the model's 95% interval includes or sits below the chance figure.
   State that plainly.
4. **Learning:** the paired test says the model beats the rule: `model_wins > rule_wins` and the
   two-sided exact binomial (sign) test on (`model_wins`, `rule_wins`) gives p < 0.05. Show the
   counts: "beat the rule score on 20 picks, lost on 5; that split would happen by luck about 1
   time in 250."
5. **Worse than the rule score:** the same test with `rule_wins > model_wins`. State it plainly.
6. **Not yet distinguishable from the rule score:** otherwise. Show the counts and the p as
   "about 1 time in N", and about how many more picks would separate them.
   - **Picks needed:** assume the current win and loss rates per pick hold (`model_wins/cv_n`,
     `rule_wins/cv_n`), and find the smallest total n at which the sign test gives p < 0.05.
     Report it as "about N more picks". If the model isn't ahead (`model_wins ≤ rule_wins`),
     say so instead of giving a number.

Why a paired test and not "the two intervals don't overlap": the model and the rule are graded
on the same picks, so most of the noise is shared. Two overlapping intervals can still hide a
clear win on the picks that differ; the paired test sees it with a third of the picks.

**A three-row table for the current model:**

| Row | Shows |
|---|---|
| AI model | `cv_agreement` |
| Rule score alone | `rule_agreement` and "no preference on `rule_ties` of these" |
| Random guess | `chance_agreement` |

- The model and rule rows show the % and a 95% interval (Wilson, using the credit sum as the
  success count). Show `n` held-out picks once.
- The chance row shows a single number with no interval. Given the picks that were shown,
  chance is an exact expected value, not a measured proportion, so an interval would mislead.
- **The blind random-pick check** as a separate row: "matched X of your last N random sets",
  with its Wilson interval and the chance figure on those same sets (the mean of
  1/`array_length(shown_sha256)` over the graded picks). Split the count by who graded: rule-only
  versions (`picks_used = 0`) and fitted ones. Below 30 graded picks, label the row "too few to
  read yet" and draw no conclusion from it. This is the cleanest test, because the model never
  trained on those picks before grading them; it is also the slowest to fill, because only 20%
  of sets are random draws.
- **Where the logic lives:** the Wilson interval, the exact binomial test, the verdict and the
  picks-needed estimate go in a small pure module, `src/harvest/scorecard.ts` (no dependency;
  n stays in the hundreds, so a plain loop over `k` with log-factorials is fine). `scoreboard()`
  returns:
  - a `scorecard` block (rows, intervals, n, verdict, the paired counts)
  - a `curve` array, one entry per version
  - `lastPickAt`

  The UI only renders these fields.

**A learning curve:**

- One point per fitted version (`picks_used > 0`), with x = `cv_n` and y = `cv_agreement`.
  Don't use `picks_used` for x: it is 0 for rule-only versions, which would stack them at the
  left edge. Promoted versions are filled dots and unpromoted ones are hollow.
- Draw the rule and chance baselines as lines joining each version's own values (rule-only
  versions contribute to these lines but get no model dot; their `cv_agreement` *is* the rule's
  figure). Draw the model's Wilson interval as a light band.
- A version with null baselines still gets its model point. Skip versions with `cv_n = 0`.
- Caption, in plain words: consecutive versions share most of their picks, so the line is
  expected to look smooth; read the band, not the wiggle.
- Use inline SVG and no new library. It must read well in both the light and dark themes, using
  only the existing CSS tokens (`--accent`, `--muted`, `--line`).
- Put the scorecard at the top of the board. The existing "By model version" text and the rest
  of the board stay below it.

**A one-line nudge** when no pick has been made for more than 7 days: "No picks since <date>. The
model only learns from new picks."

### 3. Tests

- **Python:**
  - tie credit: alone at the top → 1; one of k tied at the top → 1/k; else 0; `rule_ties`
    counts the tied picks
  - the baselines use exactly the held-out picks, and `model_wins + rule_wins` equals the number
    of picks where the two credits differ
  - chance equals the mean of 1/n
  - folds never split a set
  - with one pick per set, the folds equal today's per-pick folds
  - on synthetic picks, a fitted model's strict and tie-credit agreement differ by at most one
    pick's worth
  - the backfill uses only picks made before each version's `created_at`, fills only nulls, and
    leaves `cv_agreement` and `picks_used` alone
- **TypeScript/server (Vitest):**
  - Wilson intervals match known values
  - the exact binomial test matches known values: (20, 5) → p ≈ 0.004; (20, 11) → p ≈ 0.150;
    (0, 0) → 1
  - each verdict state in order, and the picks-needed estimate
  - `scoreboard()` returns the new fields, and older rows with null baselines don't break it
- **Playwright** (`tests/ui/harvestPage.test.ts`, with the API mocked): each verdict state
  renders, and the curve renders with 0, 1 and many versions.

## Constraints

- **Read only, outside the new columns and the migration:** the vault is never written, and the
  `picks` table is never changed.
- **The teaching screen stays as it is** (blind until you confirm; 80% least-confident sets,
  20% random).
- **Don't change the model** in this task: features, λ grid, PCA, fit and promotion rule stay.
  Once the scorecard is in, tell me what it shows and propose the cheapest next experiment, with
  numbers run offline on the live picks. Candidates, in the order I'd try them:
  - extend the λ grid upward (30, 100, 300): the best λ today is 10, the top of the grid, which
    says the data wants more shrinkage than it is allowed
  - no embedding features (12 features instead of 44): offline it scores the same or better
  - find out why only 22 of 200 candidates have a face (face `size` values near 0.003 suggest
    the faces are tiny or the detector's threshold is wrong) — half the hand features and most
    of the rule score depend on faces
- **Update the docs:** the spec (§6 agreement and CV folds, §8 scoreboard) and the README
  "Similar picks" section.
- **After the commit,** run the backfill on the live database and report the real numbers
  against the approximate ones above.

## Known limits (leave these as they are, but mention them when proposing the next experiment)

- **The reported CV is slightly optimistic.** It is the best of the 4 λ values, all measured on
  the same folds, and picking the best of several tries adds a little upward bias (a point or
  two here). Nested CV would fix this, but it changes training, so it is out of scope.
- **One fold split, one seed.** The stored CV is one shuffle of the picks into 5 folds. Other
  shuffles give 0.53–0.64 at n = 77. The Wilson band covers this roughly, but it is why the
  curve's wiggle means nothing yet.
- **80% of picks are the sets the model was least sure about.** That is good for learning and
  makes the CV a hard exam, which is fine for comparing the model with the rule on the same
  picks. It means the CV figure understates how often the model is right on a typical set, and
  the headline target (9 in 10) should only ever be read off the random picks.
- **The rule baseline is weak on this library** because it leans on faces that are rarely
  detected. Beating it is necessary, not sufficient.

## Later (not this session)

- **Repeated CV** (average over several fold seeds) to steady the curve; cheap at this n.
- **More random draws for a while** (say 50% instead of 20%) to fill the blind check faster:
  at 20% it takes about 250 picks to reach 50 graded random sets.
- **Features for what I actually pick on.** My reasons are mostly expression and framing;
  nothing in the 12 hand features measures either, and 32 PCA components of the embedding
  cannot be learnt from under 100 picks. Options: a handful of embedding directions chosen
  against the picks, better face detection, or the spec's phase 2 vision-language questions.
- **Nested CV** for an unbiased figure once there are a few hundred picks.
- **A trend test** (first half of the picks vs second half, each cross-validated on its own)
  once there are enough picks for two non-overlapping samples of 100+.

## How many picks it takes (so I can plan)

- Above chance: already shown with 77.
- Better than the rule score: at today's win/loss rates, about 120–130 picks in all, so 45–50
  more, or one week at 10 a day. If the gap is smaller than it looks, longer.
- A readable blind check (50 random graded picks with a ±13-point interval): about 250 picks at
  the current 20% random share.
- A learning *trend* (later versions beating earlier ones): two separate samples of 100+, so
  300+ picks, and the curve will not show it before then.
