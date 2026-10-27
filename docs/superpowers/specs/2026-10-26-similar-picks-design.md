# Pixel Magic 2.0 — Similar picks: design

Date: 2026-10-26. Status: written for review.
Parent spec: `docs/superpowers/specs/2026-10-17-pixel-magic-design.md` (the pipeline), whose §9
"Future AI hook" this design fills in. The page it extends is the harvester page
(`docs/superpowers/specs/2026-10-19-harvest-design.md` §8).

## 1. Purpose

In every similar set (a look-alike group: burst frames, retakes; 6,625 sets at the time of
writing), mark the photo the user would pick. A local model learns the user's taste from a few
picks a day until its pick matches theirs. Nothing is moved or deleted: every photo stays staged
as today; the pick is only marked.

## 2. Requirements

| # | Requirement |
|---|---|
| R1 | Each similar set with a pick has that photo marked in staging. Every member stays staged. |
| R2 | The user teaches by picking the best photo of a set, a few sets a day, with no fixed end. |
| R3 | The model's pick for a set matches the user's on sets it has not learned from: target **9 in 10**, shown on a scoreboard. |
| R4 | Every model runs locally on the Mac Studio (M5 Max, 64 GB). No photo or derived data leaves the machine. |
| R5 | The user's pick always wins over the model's. A new model version never overwrites a user pick. |
| R6 | The pipeline never waits on the AI: ingest, clustering and staging work with the AI worker stopped. |
| R7 | Nothing in the vault is written. The AI worker reads vault files only. |

### User decisions (2026-10-26)

- The pick is **marked; all photos are kept** (no keep/rest split, no deletion).
- Teaching is **pick the best per set** (not A/B comparisons, star ratings or written rules).
- Effort is **ongoing, a few sets a day**.
- Approach **C**: a small taste model trained on the user's picks makes the decisions (phase 1);
  a local vision-language model is added as a source of extra inputs and written reasons only if
  phase 1 stalls below target (phase 2).

### Settled defaults (overridable)

- Daily counter: 10 sets.
- Review mix: 80 % least-confident sets, 20 % random sets.
- Retrain: after every 10 picks and when the user leaves the Picks tab, plus nightly.
- Scoreboard window: the last 50 picks on random sets.
- Phase 2 trigger: headline below 90 % for three model versions in a row, each with at least
  50 random picks behind it.

## 3. Architecture

```
  Docker stack (as today)                 Native on macOS (new)
  ─────────────────────────               ──────────────────────────────────
  Postgres ◄──────────────────────────────  ai worker (Python, Temporal SDK, launchd)
    + photo_features, picks,                 ├─ DescribeWorkflow → describe_photo
      photo_scores, ranker_models            ├─ TrainWorkflow    → train_ranker
  Temporal ──── queue "ai" ───────────────►  └─ RankWorkflow     → rank_sets
  Staging build: marks each set's pick       (phase 2: vision-language model via MLX)
  Harvester page: new "Picks" tab
```

- **ai worker** (`ai/` at the repo root): Python on the host, not in Docker, because Docker on
  macOS has no GPU access and Apple's Vision framework is macOS-only. Installed as a launchd agent
  like the harvester (`make ai-install` / `ai-uninstall`). It shares only the `ai` task-queue name
  and JSON payloads with the TypeScript pipeline (parent spec R7).
- **Staging** (TypeScript, existing) reads stored picks and scores; it never calls the worker.
- **Harvester server** (TypeScript, existing, on the host) serves the Picks tab, its API and
  photo previews, and starts Train/Rank through its existing Temporal client.

### Candidates

The photos compared in a set are its **candidates**: the placed (best) file of each copy group in
the set, exactly the files staging puts in `similar/…/s-<sha8>/` today. Other members of a copy
group are copies of the same shot and are never candidates. Only candidates are described.

## 4. Data model (migration `0008_similar_picks.sql`)

```
photo_features(sha256 text, feature_version int, embedding real[], quality jsonb, faces jsonb,
               vlm jsonb NULL, failed text NULL, described_at timestamptz,
               PRIMARY KEY (sha256, feature_version))
picks(id bigserial PK, set_id bigint, chosen_sha256 text NULL, shown_sha256 text[],
      no_clear_best bool, random_draw bool, model_version int NULL, model_pick_sha256 text NULL,
      reasons text[], decided_at timestamptz)
photo_scores(sha256 text, model_version int, score real, PRIMARY KEY (sha256, model_version))
ranker_models(version serial PK, created_at timestamptz, feature_version int, picks_used int,
              cv_agreement real, promoted bool, path text)
```

- `photo_features.quality`: sharpness, exposure (clipped highlights/shadows), motion blur, noise,
  general aesthetic score. `faces`: per face: eyes open, smile, in focus, relative size.
  `failed`: why the photo could not be described (it is then left out of ranking).
- `picks`: one row per decision. `chosen_sha256` is null when `no_clear_best`. `model_pick_sha256`
  and `model_version` are what the model would have picked **when the set was shown**, recorded
  before the user chose: the scoreboard grades these, so every graded pick was unseen by the model
  that made it. `random_draw` marks sets drawn at random (the scoreboard's sample).
- `set_id` is the set's root group id when shown. Sets can later merge; a pick stays valid as
  training data ("chosen beat the others shown") whatever happens to the set.
- `ranker_models.path`: the model file under `DATA_DIR/ai/models/<version>`. Old versions are kept.
  Exactly one promoted version is current (the highest promoted `version`).

## 5. Workflows (queue `ai`)

- **DescribeWorkflow** (scheduled, every 15 min): backlog = candidates with no `photo_features`
  row for the current `feature_version`. `describe_photo(sha256)` reads the vault file, decodes it
  (HEIC and RAW via the same routes the pipeline uses), computes the SigLIP 2 embedding, the
  quality measures, the Apple Vision face measures and the aesthetic score, and upserts one row.
  Idempotent per `(sha256, feature_version)`. Low concurrency (default 2), pausable from the page.
- **TrainWorkflow** (after 10 picks, on leaving the tab, nightly): `train_ranker` fits a new model
  on all non-`no_clear_best` picks (§6), cross-validates it, writes the model file and a
  `ranker_models` row, and promotes it only if its cross-validated agreement is at least the
  current model's on the same picks. Then starts RankWorkflow if promoted.
- **RankWorkflow**: `rank_sets` scores every described candidate with the current model and
  writes `photo_scores` for that version. Minutes for the whole library.
- A new `feature_version` (new embedding model) makes a new Describe backlog; ranking keeps using
  the old version's features until the new backlog is empty.

## 6. Choosing and learning

- **Inputs** per candidate: the embedding, the quality and face measures, and the aesthetic
  score, mostly **relative to the set** (e.g. sharpness ÷ the set's sharpest, "only candidate
  with all eyes open"). The model ranks within a set; it never grades photos in absolute terms.
- **Day one** (no picks): a fixed rule score — faces in focus, eyes open, sharpness, exposure,
  aesthetic score — gives every described set a pick. The rule score stays one of the inputs, so
  early picks only have to correct it.
- **Model:** a regularised conditional choice model (softmax over a set's candidates, linear in
  the inputs): "the chosen photo beat each other shown". Regularisation strength by
  cross-validation. Its weights are readable ("eyes open matters far more than sharpness to you").
  Swappable for a stronger ranker (e.g. gradient-boosted trees) if it stalls after ~1,000 picks.
- **Cross-validation:** 5 folds by set; agreement = share of held-out picks where the model's top
  candidate is the chosen one.
- **Pick of a set**, in order: the user's latest pick whose chosen photo is still a candidate;
  else the current model's highest-scoring candidate, when at least two candidates are described;
  else none (nothing is marked).
- **Confidence** of a set = the gap between its top two candidate scores (current model).

## 7. Staging

- The picked candidate's staged name gets the prefix `0-best_`
  (`similar/YYYY/YYYY-MM/s-<sha8>/0-best_<YYYYMMDD_HHMMSS>_<name>`), so it sorts first in Finder.
- A set with no pick (§6) is staged exactly as today. Folder names (`s-<sha8>`, from the file
  rules) never depend on picks, so a changed pick renames one file, never a folder.
- `planStaging` gains an input: set → picked candidate sha256, read from the database at build
  time. With the AI tables empty the tree is byte-for-byte today's (golden tests unchanged).

## 8. The Picks tab and its API

### 8.1 Views

- **Teaching screen**: one set at a time; all candidates large, side by side; click for full size,
  with zoom linked across candidates. Keys: `1`–`9` pick, `N` no clear best, `S` skip for now,
  `Z` zoom. **Blind**: the model's pick is hidden until the user chooses, then shown ("it picked
  the same" / "it picked this one"). Optional reason chips after choosing: sharper, eyes open,
  expression, framing, exposure. A daily counter (default 10). A pause/resume control for
  describing.
- **Which set next**: sets without a user pick, with at least two described candidates; 80 % the
  lowest confidence, 20 % uniformly random (`random_draw`). Sets whose candidates changed since
  the user's pick come back once.
- **Scoreboard**: headline "matched your pick X of your last 50 random sets"; trend by model
  version; total picks; sets whose pick changed with the latest model; a "where we disagree" list.
  Before 50 random picks exist it shows the count so far.

### 8.2 API (harvester server; same Host/Origin/header checks as the existing API)

| Method | Path | Does |
|---|---|---|
| GET | `/api/picks/next` | the next set: candidates (sha256, name, date, size), `random_draw`, and an opaque token that records the model's hidden pick |
| POST | `/api/picks` | `{token, chosen \| noClearBest, reasons}` → stores the pick, returns the model's pick to reveal; starts TrainWorkflow every 10 picks |
| POST | `/api/picks/session-end` | starts TrainWorkflow if any picks are untrained |
| GET | `/api/picks/scoreboard` | the scoreboard figures |
| GET | `/api/preview/<sha256>?w=1600` | a JPEG preview of a vault file (cached under `DATA_DIR/harvest-cache/previews`, size-capped like thumbnails) |
| POST | `/api/picks/describe/(pause\|resume)` | pauses or resumes DescribeWorkflow |

## 9. Failures

| Case | Behaviour |
|---|---|
| ai worker down, Mac asleep | Work waits in Temporal and resumes; the pipeline and staging are unaffected (R6). |
| A candidate cannot be read or decoded | `photo_features.failed` is set; it is left out of ranking and listed on the scoreboard. |
| Fewer than two described candidates | No model pick for that set; staging unchanged. |
| Model file missing or unreadable | Rank uses the previous promoted version, else the day-one rule score. |
| New model scores worse in cross-validation | Kept, not promoted. |
| Memory pressure | Describe concurrency 2 and pausable; phase 2's model loads only while describing. |
| Temporal down when a pick is posted | The pick is stored; training starts at the next trigger. |

## 10. Phase 2 (only if the trigger in §2 fires)

1. Benchmark Qwen3-VL (~30B, 4-bit MLX) and Gemma 4 (26B/31B) on 100 of the user's picked sets:
   answer quality on fixed questions, agreement gained when added as inputs, seconds per photo.
   Confirm exact model names and MLX builds on the Hugging Face `mlx-community` org first.
2. The winner answers fixed per-photo questions (eyes closed, subject cut off, awkward
   expression, motion blur, …; refined from the "where we disagree" list and the reason chips),
   stored in `photo_features.vlm` by a new `feature_version`.
3. It writes a one-line reason under each model pick on the teaching screen.

## 11. Testing

- **Python unit:** quality and face measures on fixture photos; the ranker recovers known weights
  from synthetic picks; cross-validation and promotion; next-set selection (mix, confidence,
  changed sets); scoreboard arithmetic (only blind, random picks are graded).
- **Python workflow:** the Temporal Python test environment, activities faked at each boundary;
  each workflow applies each effect once.
- **TypeScript:** `planStaging` marks the right candidate in each precedence case and never renames
  a folder; empty AI tables leave the tree unchanged. Server API tests for every §8.2 route.
  Playwright: blind-then-reveal, keys, no clear best, the daily counter.
- **CI:** Python tests on Linux with a fake face detector; one macOS job runs the Apple Vision
  measures for real.

## 12. Build order (each step is useful on its own)

1. **Spike (thrown away):** on ~200 of the user's photos, measure describe speed, check the
   measures look right, confirm SigLIP 2 and Apple Vision (PyObjC) run under launchd; settle the
   exact Python packages and embedding size.
2. **ai worker:** migration, DescribeWorkflow, launchd install.
3. **Day-one picks:** the rule score, RankWorkflow, staging marks picks (§7).
4. **Picks tab:** teaching screen, `picks`, scoreboard, previews.
5. **Learning loop:** TrainWorkflow, promotion, retrain triggers.
6. **Phase 2** (§10), only if triggered.

## 13. Out of scope

Deleting, moving or archiving non-picked photos; more than one pick per set; scoring photos that
are not in a similar set; any hosted model or service.
