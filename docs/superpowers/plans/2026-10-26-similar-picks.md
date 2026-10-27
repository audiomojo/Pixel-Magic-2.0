# Similar Picks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mark the user's pick in every similar set, chosen by a local taste model that learns from a few picks a day (phase 1 of the spec; phase 2 is not in this plan).

**Architecture:** A native Python worker (`ai/`, Temporal queue `ai`, launchd) describes each set's candidates and trains/ranks a conditional choice model; results go to new Postgres tables. The TypeScript staging build records each set's candidates and marks the pick; the harvester server and page gain a Picks tab for blind teaching and a scoreboard.

**Tech Stack:** TypeScript (existing: node-pg-migrate, vitest, Playwright, Temporal TS client); Python 3.13 via uv, `temporalio`, `psycopg[binary]`, numpy, scipy, Pillow + `pillow-heif`, `rawpy`, `torch` + `transformers` (SigLIP 2), `aesthetic-predictor-v2-5`, `pyobjc-framework-Vision`, pytest, `testcontainers[postgres]`.

**Spec:** `docs/superpowers/specs/2026-10-26-similar-picks-design.md`

## Global Constraints

- Nothing in the vault is written; the worker opens vault files read-only (spec R7).
- The pipeline never waits on the AI: staging reads stored rows only; with the AI tables empty the staging tree is byte-for-byte today's (spec R6, §7).
- The user's pick always wins; a model version never overwrites a pick (spec R5).
- Everything runs locally; no hosted model or service (spec R4).
- Pick marker: staged name prefix `0-best_`; folder names never depend on picks (spec §7).
- Queue name `ai`. Workflow names `DescribeWorkflow`, `TrainWorkflow`. Workflow ids `ai-describe`, `ai-train`. Schedule ids `ai-describe` (every 15 min), `ai-train-nightly` (03:00 local).
- Defaults: daily counter 10; review mix 80 % least confident / 20 % random; retrain every 10 picks, on leaving the tab, nightly; scoreboard window 50 random picks; describe concurrency 2.
- Previews: `GET /api/preview/<sha256>?w=400|1600`, JPEG, cached under `DATA_DIR/harvest-cache/previews`, evicted with the thumbnail cap.
- New Python dependencies and Playwright-style pinning: exact versions, each released at least two weeks before it is added.

## Review Focus

1. A set whose candidates change after the user picked (a new burst frame arrives, two sets merge): the pick still applies if its photo is still a candidate, the set is offered again once, and staging never marks two files. Test in Task 3 and Task 11.
2. A candidate whose vault file is HEIC, RAW or corrupt: describe records `failed` instead of crashing the batch, the set ranks on the rest, previews fall back to the placeholder. Tests in Task 5 and Task 11.
3. Picks posted with a stale or forged token (server restarted, tab left open overnight): 409 "reload", nothing stored. Test in Task 11.
4. Two training triggers at once (10th pick while the nightly run is going): one TrainWorkflow runs; the other request is a no-op. Test in Task 10.
5. A set with one described candidate, or all candidates scoring equal: no model pick and no marker; it is not offered for teaching. Tests in Task 3 and Task 11.

---

### Task 1: Spike (throwaway): packages, speed and sanity on real photos

**Files:**
- Create (not committed): `ai-spike/` with one script

- [ ] **Step 1:** Using uv with Python 3.13, install `torch`, `transformers`, `aesthetic-predictor-v2-5`, `pillow-heif`, `rawpy`, `pyobjc-framework-Vision`. Load `google/siglip2-so400m-patch14-384` on `mps`.
- [ ] **Step 2:** On 200 candidates from `staging/current/similar/` (mixed JPEG/HEIC/RAW), time per photo: decode, SigLIP 2 embedding, aesthetic score, Apple Vision `VNDetectFaceLandmarksRequest` + `VNDetectFaceCaptureQualityRequest`. Run it once under a throwaway launchd agent to confirm Vision works without a GUI session.
- [ ] **Step 3:** Eyeball 20 sets: does the sharpest/eyes-open/highest-aesthetic photo look right?
- [ ] **Step 4:** Record in the PR description of Task 4: exact versions (each ≥ 2 weeks old), embedding size (expected 1152), photos/second, and whether aesthetic-predictor's own SigLIP can be dropped in favour of SigLIP 2 embeddings (fewer models). Later tasks use these. Delete `ai-spike/`.

### Task 2: Migration 0008

**Files:**
- Create: `migrations/0008_similar_picks.sql`
- Modify: `tests/helpers/pg.ts` (truncate), `tests/unit/schema.test.ts`

**Interfaces:**
- Produces tables (spec §4) plus `similar_candidates(set_id bigint, sha256 text, PRIMARY KEY (set_id, sha256))` written by staging, and `ranker_models.ranked_at timestamptz NULL` (set when ranking for that version finished). Indexes: `picks (decided_at)`, `picks (chosen_sha256)`, `photo_scores (model_version)`, `similar_candidates (sha256)`.
- `photo_features.failed` and `picks.chosen_sha256` nullable; CHECK `picks`: `no_clear_best = (chosen_sha256 IS NULL)`.

- [ ] **Step 1: Failing schema tests** in `schema.test.ts`: `picks` CHECK rejects `no_clear_best=false` with null `chosen_sha256` (code `23514`); `photo_features` PK rejects a duplicate `(sha256, feature_version)` (`23505`); `similar_candidates` PK likewise.
- [ ] **Step 2:** Run `npx vitest run --project unit tests/unit/schema.test.ts` — FAIL (relations missing).
- [ ] **Step 3:** Write the migration (Up and Down sections, as `0007`). Add the five tables to `truncate()` in `tests/helpers/pg.ts`.
- [ ] **Step 4:** Run the schema and dashboard SQL tests — PASS.
- [ ] **Step 5:** Commit `feat(db): similar picks tables (0008)`.

### Task 3: Staging records candidates and marks the pick

**Files:**
- Create: `src/similarPick.ts`, `tests/unit/similarPick.test.ts`
- Modify: `src/stagingPlan.ts`, `src/activities/staging.ts`, `tests/unit/stagingPlan.test.ts`, `tests/unit/stagingBuild.test.ts`

**Interfaces:**
- Produces in `src/similarPick.ts`:
  - `interface PickInputs { userPicks: ReadonlyMap<string, number>; scores: ReadonlyMap<string, number> }` — `userPicks`: chosen sha256 → decision order (higher = later); `scores`: sha256 → score of the current ranked model.
  - `const NO_PICKS: PickInputs` (both maps empty).
  - `choosePick(candidates: readonly string[], p: PickInputs): string | null` — spec §6 precedence: latest user pick among candidates; else the top score when ≥ 2 candidates are scored and the top is strictly greater than the second; else null.
- `planStaging(contents, files, maxDurationS, picks: PickInputs = NO_PICKS): Entry[]` and a new export `similarSets(contents, files): Map<number, string[]>` (root set id → candidate sha256s, exactly the files placed under `similar/`), used by `planStaging` itself.
- Staging activity: after a successful build, replaces `similar_candidates` in one transaction. Change detection (`snapshot()`) also fires when a `picks` or `ranker_models` row is not visible in the last build's snapshot (same `pg_visible_in_snapshot(xmin…)` test as `files`).
- Current ranked model: `SELECT max(version) FROM ranker_models WHERE promoted AND ranked_at IS NOT NULL`.

- [ ] **Step 1: Failing tests** (`similarPick.test.ts`): user pick wins over a higher score; the later of two user picks wins; a user pick not among candidates is ignored; one scored candidate → null; tie on top score → null; no inputs → null.
- [ ] **Step 2: Failing tests** (`stagingPlan.test.ts`): with `NO_PICKS` the plan equals today's (existing expectations unchanged); a set with a pick names that file `0-best_<stamp>_<name>` in the same `s-<sha8>` folder and leaves the other names unchanged; changing the pick to another candidate changes two file names and no folder; a copy group's non-placed contents are never candidates (`similarSets`). Property test (fast-check, as existing ones): at most one `0-best_` per set folder.
- [ ] **Step 3:** Run both — FAIL.
- [ ] **Step 4:** Implement `choosePick`, `similarSets`, the `picks` parameter.
- [ ] **Step 5: Failing tests** (`stagingBuild.test.ts`): after a build, `similar_candidates` holds each set's candidates; inserting a `picks` row makes the next unforced build run (not skip) and mark that file; inserting a promoted `ranker_models` row with `ranked_at` and `photo_scores` does likewise; empty AI tables leave the build skipped as before.
- [ ] **Step 6:** Implement in `staging.ts`: `loadPickInputs(c)` inside the REPEATABLE READ transaction (user picks ordered by `decided_at, id`; scores of the current ranked model), the snapshot clause, the candidates write.
- [ ] **Step 7:** `npm test` — PASS; `make test-smoke` still matches the golden tree.
- [ ] **Step 8:** Commit `feat(staging): record similar-set candidates and mark the pick`.

### Task 4: Python worker scaffold, database access, CI

**Files:**
- Create: `ai/pyproject.toml`, `ai/uv.lock`, `ai/pixelmagic_ai/{__init__,config,db}.py`, `ai/tests/conftest.py`, `ai/tests/test_db.py`
- Modify: `.github/workflows/ci.yml`, `.gitignore` (`ai/.venv/`), `Makefile` (`ai-test`)

**Interfaces:**
- `pyproject.toml`: core deps `temporalio`, `psycopg[binary]`, `numpy`, `scipy`, `pillow`, `pillow-heif`, `rawpy`; optional group `ml` (`torch`, `transformers`, `aesthetic-predictor-v2-5`); optional group `mac` (`pyobjc-framework-Vision`, marker `sys_platform == 'darwin'`); dev: `pytest`, `testcontainers[postgres]`. Versions from Task 1.
- `config.py`: `@dataclass(frozen=True) class Settings(data_dir: Path, database_url: str, temporal_address: str, temporal_namespace: str, describe_concurrency: int)`; `load_settings(env: Mapping[str, str], repo_root: Path) -> Settings` with the same variables and defaults as `src/harvest/config.ts` (`DATA_DIR` → `<repo>/data`, `DATABASE_URL` → `postgres://postgres:<POSTGRES_PASSWORD|pixelmagic>@127.0.0.1:5432/pixelmagic`, `TEMPORAL_ADDRESS` → `127.0.0.1:7233`, `TEMPORAL_NAMESPACE` → `default`), plus `AI_DESCRIBE_CONCURRENCY` → 2. Reads `.env` from the repo root when present.
- `db.py` (all functions take a `psycopg.Connection`):
  - `describe_backlog(conn, feature_version: int, limit: int) -> list[tuple[str, str]]` — `(sha256, ext)` of candidates with no `photo_features` row for that version.
  - `upsert_features(conn, row: FeatureRow) -> None`; `@dataclass FeatureRow(sha256, feature_version, embedding: list[float] | None, quality: dict | None, faces: list[dict] | None, failed: str | None)`.
  - `load_sets(conn, feature_version: int) -> dict[int, list[Described]]` — set id → described, non-failed candidates; `Described(sha256, embedding: np.ndarray, quality: dict, faces: list[dict])`.
  - `load_picks(conn) -> list[Pick]` — non-`no_clear_best`; `Pick(chosen: str, shown: list[str])`.
  - `insert_model(conn, feature_version, picks_used, cv_agreement, promoted, path) -> int`; `current_model(conn) -> ModelRow | None` (highest promoted); `write_scores(conn, version, scores: dict[str, float])`; `mark_ranked(conn, version)`.
- `tests/conftest.py`: session fixture starting Postgres 16 (testcontainers) and applying every `migrations/*.sql` Up section in order; per-test `conn` fixture that truncates the same tables as `tests/helpers/pg.ts`.

- [ ] **Step 1: Failing tests** (`test_db.py`): `describe_backlog` returns only candidates without features for the version; `upsert_features` twice keeps one row; `load_sets` skips failed rows; `load_picks` skips `no_clear_best`; `current_model` ignores unpromoted rows.
- [ ] **Step 2:** `cd ai && uv run pytest` — FAIL.
- [ ] **Step 3:** Implement `config.py` and `db.py`.
- [ ] **Step 4:** `uv run pytest` — PASS.
- [ ] **Step 5:** CI: a second job `ai` on `ubuntu-latest`: `astral-sh/setup-uv` (pinned), `uv sync --frozen` (core + dev only), `uv run pytest -m "not mac and not ml"`. Make target `ai-test`.
- [ ] **Step 6:** Commit `feat(ai): Python worker scaffold and database access`.

### Task 5: Image loading and quality measures

**Files:**
- Create: `ai/pixelmagic_ai/{imageio,quality}.py`, `ai/tests/test_quality.py`, fixtures under `ai/tests/fixtures/` (generated in the test where possible; `tests/fixtures/scene0.heic` reused)

**Interfaces:**
- `load_rgb(path: Path, ext: str, max_px: int = 1600) -> PIL.Image.Image` — EXIF-oriented RGB, longest side ≤ `max_px`. HEIC/HEIF via `pillow-heif`; RAW extensions (the RAW set of `src/formats.ts`) via the embedded JPEG preview (`rawpy` `extract_thumb`); everything else via Pillow. Raises `UndecodableError(reason: str)` for a permanent failure.
- `measure(img: Image.Image) -> dict[str, float]` with keys exactly: `sharpness` (variance of the Laplacian on grey at 1024 px), `brightness` (mean luminance 0–1), `highlights` (fraction ≥ 250/255), `shadows` (fraction ≤ 5/255), `noise` (median absolute deviation of a 3×3 high-pass, 0–1).

- [ ] **Step 1: Failing tests:** a blurred copy of an image has lower `sharpness` than the original; an all-white image has `highlights` ≈ 1; HEIC fixture loads with the right orientation; an EXIF-rotated JPEG comes back upright; a truncated JPEG raises `UndecodableError`; output longest side ≤ 1600.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(ai): image loading and quality measures`.

### Task 6: Face measures (Apple Vision, with a fake for Linux)

**Files:**
- Create: `ai/pixelmagic_ai/faces.py`, `ai/tests/test_faces.py`

**Interfaces:**
- `class FaceDetector(Protocol): def detect(self, img: Image.Image) -> list[dict[str, float]]` — each face: `eyes_open` (0–1, from eye landmark aspect ratio, ≥ 0.2 ratio = open), `smile` (0–1, mouth corners above mouth centre), `quality` (Vision face capture quality 0–1), `size` (face box area ÷ image area).
- `AppleVisionDetector()` (macOS only; imports Vision lazily). `FakeDetector(faces: list[dict])` returns its list.

- [ ] **Step 1: Failing tests:** `FakeDetector` round-trips; `@pytest.mark.mac` on a fixture portrait: one face, `size` in (0, 1), `quality` in [0, 1]; on a landscape with no people: `[]`.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** On the Mac `uv run pytest -m mac` — PASS.
- [ ] **Step 5:** CI: a small `ai-mac` job on `macos-latest` running `uv sync --extra mac` and `uv run pytest -m mac`.
- [ ] **Step 6:** Commit `feat(ai): face measures via Apple Vision`.

### Task 7: Embedding and aesthetic score

**Files:**
- Create: `ai/pixelmagic_ai/embed.py`, `ai/tests/test_embed.py`

**Interfaces:**
- `class Embedder(Protocol): def embed(self, img) -> np.ndarray` (float32, L2-normalised, length `EMBED_DIM`); `def aesthetic(self, img) -> float` (1–10).
- `SiglipEmbedder(device: str = "mps")` loads the models chosen in Task 1 once. `FakeEmbedder(dim: int = 8)` — deterministic from image bytes.
- `EMBED_DIM` and `FEATURE_VERSION = 1` constants live in `embed.py`.

- [ ] **Step 1: Failing tests:** `FakeEmbedder` deterministic and unit-length; `@pytest.mark.ml`: the same image twice → cosine ≥ 0.999; a flipped copy → cosine > 0.9; an unrelated image → lower; `aesthetic` in [1, 10].
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** `uv run --extra ml pytest -m ml` — PASS.
- [ ] **Step 5:** Commit `feat(ai): SigLIP 2 embeddings and aesthetic score`.

### Task 8: DescribeWorkflow, the worker process, launchd

**Files:**
- Create: `ai/pixelmagic_ai/{activities,workflows,worker,schedules}.py`, `ai/launchd/com.pixelmagic.ai.plist.in`, `ai/tests/test_describe.py`, `ai/tests/test_workflows.py`
- Modify: `Makefile` (`ai-install`, `ai-uninstall`, `ai-status`, `ai-worker`), `README.md`

**Interfaces:**
- Activity `describe_photo(sha256: str, ext: str) -> str` (`"ok"` or `"failed"`): opens `<data_dir>/vault/<aa>/<bb>/<sha>.<ext>` read-only, `load_rgb` → `measure`, `detect`, `embed`, `aesthetic` (stored as `quality["aesthetic"]`), `upsert_features`. `UndecodableError` → row with `failed`; other errors propagate (Temporal retries).
- Activity `describe_backlog_page(limit: int) -> list[tuple[str, str]]`.
- `DescribeWorkflow.run() -> int`: pages of 200; runs `describe_photo` for each with at most `describe_concurrency` in flight; continue-as-new every 5 pages; when it described ≥ 1 photo, runs `rank_sets` (Task 10) at the end. Returns photos described.
- `schedules.ensure_schedules(client)`: creates or updates `ai-describe` (every 15 min, overlap SKIP) and `ai-train-nightly` (03:00, overlap SKIP).
- `worker.py`: `main()` — loads settings, connects, `ensure_schedules`, runs a Worker on queue `ai` with both workflows and all activities. Shared `Deps(settings, pool, detector, embedder)` constructed once.
- launchd: label `com.pixelmagic.ai`, runs `uv run --extra ml --extra mac python -m pixelmagic_ai.worker` in `ai/`, `RunAtLoad`, `KeepAlive`, log `DATA_DIR/logs/ai.launchd.log`. `make ai-install` renders the template (absolute `uv` path, repo path, log dir) into `~/Library/LaunchAgents/` and bootstraps it; `ai-uninstall` boots it out and removes it.

- [ ] **Step 1: Failing tests** (`test_describe.py`, fakes for detector/embedder, a temp vault): a JPEG candidate gets a full row; a corrupt file gets `failed` and no exception; the vault file's bytes and mtime are unchanged afterwards; re-running is a no-op (backlog empty).
- [ ] **Step 2: Failing tests** (`test_workflows.py`, `WorkflowEnvironment.start_time_skipping`, activities faked): 450 backlog items are each described once across pages and continue-as-new; `rank_sets` runs once at the end only when something was described; concurrency never exceeds the setting.
- [ ] **Step 3:** Run — FAIL. **Step 4:** Implement. **Step 5:** Run — PASS.
- [ ] **Step 6:** On the Mac: `make ai-install`, confirm in Temporal UI that `ai-describe` runs and `photo_features` fills. README: a "Similar picks" section (install, pause, logs).
- [ ] **Step 7:** Commit `feat(ai): describe worker, schedules and launchd agent`.

### Task 9: Features, rule score and the choice model (pure)

**Files:**
- Create: `ai/pixelmagic_ai/ranker.py`, `ai/tests/test_ranker.py`

**Interfaces:**
- `FEATURE_NAMES` (fixed order): `rule`, `sharpness_rel`, `brightness_dev`, `clipped`, `noise_rel`, `faces_max`, `eyes_open_all`, `eyes_open_min`, `smile_mean`, `face_quality_mean`, `face_size_max`, `aesthetic_rel`, then `emb_0`…`emb_31`.
- `set_matrix(cands: list[Described], pca: np.ndarray) -> np.ndarray` (n × len(FEATURE_NAMES)), relative to the set: `sharpness_rel = log(s / max s)`, `brightness_dev = |b − 0.5|`, `clipped = highlights + shadows`, `noise_rel = noise − mean`, `faces_max = face count == set max`, face fields 0 when no faces, `aesthetic_rel = a − mean`, `emb_k` = (embedding − set mean embedding) · `pca[:, k]`. `rule` = z(face_quality_mean) + eyes_open_all + 0.5·z(sharpness_rel) + 0.5·z(aesthetic_rel) − clipped, where z standardises within the set (0 when the set's spread is 0).
- `fit_pca(sets) -> np.ndarray` (EMBED_DIM × 32, from set-centred embeddings).
- `@dataclass Model(weights: np.ndarray, lam: float, pca: np.ndarray, feature_version: int)`; `RULE_ONLY(pca)` = weight 1 on `rule`, 0 elsewhere.
- `fit(sets, picks, pca, lam) -> Model` — maximises Σ log softmax(Xw)[chosen] − lam·‖w‖² over the shown candidates of each pick that are still described (`scipy.optimize.minimize`, L-BFGS-B). Fewer than 20 usable picks → `RULE_ONLY`.
- `cv_agreement(sets, picks, pca, lam, folds=5, seed=7) -> float` — folds by pick; share of held-out picks whose top-scoring shown candidate is the chosen one.
- `train(sets, picks, current: Model | None) -> tuple[Model, float, bool]` — chooses `lam` from `{0.01, 0.1, 1, 10}` by CV; promoted when its CV agreement ≥ the current model's config (same `lam`, refit on the same folds); with no current model, promoted.
- `score(model, sets) -> dict[str, float]` — every described candidate.
- `save(model, path)` / `load(path) -> Model` (`.npz`).

- [ ] **Step 1: Failing tests:** synthetic picks generated from known weights (e.g. only `eyes_open_all` matters) → the fitted model ranks held-out sets ≥ 90 % right and its largest weight is `eyes_open_all`; < 20 picks → `RULE_ONLY`; `RULE_ONLY` prefers the eyes-open, sharper candidate in a hand-built set; a pick whose shown candidates are no longer described is skipped, not an error; `train` does not promote a model whose CV agreement is below the current config's; `save`/`load` round-trips.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(ai): rule score and conditional choice model`.

### Task 10: TrainWorkflow and ranking

**Files:**
- Modify: `ai/pixelmagic_ai/{activities,workflows,worker}.py`
- Create: `ai/tests/test_train.py`

**Interfaces:**
- Activity `train_ranker() -> int | None`: loads sets and picks, `train`, saves to `<data_dir>/ai/models/<version>.npz`, `insert_model`; returns the version when promoted, else None. With no model yet, inserts and promotes `RULE_ONLY` (picks_used 0).
- Activity `rank_sets() -> int`: ranks on the newest `feature_version` whose describe backlog is empty (spec §5: old features stay in use until the new ones are done); holds `pg_advisory_lock(hashtext('pm.ai.rank'))`; loads the current model (falls back to the previous promoted file, then `RULE_ONLY`, if the file is unreadable); `write_scores`; `mark_ranked`. Returns candidates scored.
- `TrainWorkflow.run() -> int | None`: `train_ranker`, then `rank_sets` when promoted. Started with workflow id `ai-train` and id-conflict policy "use existing" (a second trigger while one runs is a no-op).

- [ ] **Step 1: Failing tests:** first run with no picks creates and ranks `RULE_ONLY`; 30 consistent picks produce a promoted version 2 whose `photo_scores` exist and `ranked_at` is set; a worse candidate model is stored unpromoted and scores are not rewritten; a deleted model file falls back to the previous version; two `rank_sets` at once serialise (advisory lock); two TrainWorkflow starts with id `ai-train` run once.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(ai): train and rank`.

### Task 11: Picks API, previews and AI control (harvester server)

**Files:**
- Create: `src/harvest/picks.ts`, `src/harvest/aiControl.ts`, `tests/unit/harvestPicks.test.ts`
- Modify: `src/harvest/server.ts` (route delegation, deps), `src/harvest/thumbs.ts`, `src/decode.ts`, `src/harvest/host.ts`

**Interfaces:**
- `aiControl.ts`: `interface AiControl { startTrain(): Promise<'started' | 'running'>; setDescribePaused(paused: boolean): Promise<void>; describePaused(): Promise<boolean> }`; `temporalAiControl(client)` (workflow `TrainWorkflow`, id `ai-train`, queue `ai`; pause = Temporal schedule `ai-describe` pause/unpause).
- `HarvestServerDeps.ai: AiControl` (tests fake it).
- `picks.ts`: `createPicksRoutes(d: { pool; settings; ai: AiControl; secret: Buffer }) => (method, path, query, body, res) => Promise<boolean>` (true when it handled the request); `server.ts` calls it before its own 404.
  - `GET /api/picks/next` → `{set: {id, candidates: [{sha256, name, capturedAt, sizeBytes}], randomDraw}, token, today: {done, goal: 10}} | {set: null}`. Selection: sets in `similar_candidates` with ≥ 2 described non-failed candidates, no user pick among current candidates (or candidates changed since the latest pick, offered once), not `no_clear_best`ed for the same candidate list; draw random with probability 0.2, else lowest confidence (top-two score gap under the current ranked model; unscored sets first). Candidate order shuffled. The token is HMAC-SHA256 (`secret`, random per process) over `{setId, shown, modelVersion, modelPick, randomDraw}`, so the model's pick is hidden but tamper-proof.
  - `POST /api/picks` `{token, chosen?: sha256, noClearBest?: true, reasons?: string[]}` → stores the row, returns `{modelPick, modelVersion, matched}`; every 10th stored pick calls `ai.startTrain()` (failure logged, pick kept). Bad/stale token → 409 `reload the page`. `chosen` not in `shown` → 400. Reasons limited to `sharper | eyes open | expression | framing | exposure`.
  - `POST /api/picks/session-end` → `startTrain()` when picks exist after the newest model's `created_at`.
  - `GET /api/picks/scoreboard` → `{headline: {matched, of}, byVersion: [{version, matched, of}], totalPicks, changedByLatest, disagreements: [{setId, chosen, modelPick}] (latest 20), failedPhotos, describe: {paused, backlog}}`; `headline` over the latest 50 picks with `random_draw` and a non-null `model_pick_sha256`.
  - `POST /api/picks/describe/(pause|resume)`.
  - `GET /api/preview/<sha256>?w=400|1600` → JPEG of the vault file (via `contents.ext`), cached at `harvest-cache/previews/<sha>-<w>.jpg`, evicted with the thumbnail cap; undecodable → the placeholder.
- `thumbs.ts`: `renderJpeg(input: Buffer, px: number): Promise<Buffer>` replaces the private `render`; `decode.ts` exports `rawPreview(absPath: string): Promise<Buffer>` (the existing exiftool extraction) for RAW previews.

- [ ] **Step 1: Failing tests** (`harvestPicks.test.ts`, real Postgres, fake `AiControl`, rows inserted directly): next returns a set with ≥ 2 described candidates and never one the user picked; a set whose candidates changed after the pick is offered once; a set with one described candidate is never offered; posting stores `model_pick_sha256` from the token and returns `matched`; a forged or other-process token → 409 and no row; `chosen` outside `shown` → 400; the 10th pick calls `startTrain` once; a `startTrain` that throws still stores the pick and returns 200; scoreboard headline counts only random, graded picks (fixture: 3 random matched, 1 random missed, 5 non-random → `{matched: 3, of: 4}`); preview of a JPEG and of the HEIC fixture return `image/jpeg` ≤ requested width; corrupt file → placeholder; `w=999` → 400; all routes enforce the existing Host/Origin/`X-Pixel-Magic` checks.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement; wire `temporalAiControl` in `host.ts`. **Step 4:** `npm test` — PASS.
- [ ] **Step 5:** Commit `feat(harvest): picks API, previews and AI control`.

### Task 12: The Picks tab

**Files:**
- Create: `src/harvest/ui/picks.js`
- Modify: `src/harvest/ui/{app.js,index.html,app.css,format.js}`, `tests/unit/harvestUiFormat.test.ts`, `tests/ui/harvestPage.test.ts`

**Interfaces:**
- `index.html` nav: `<a href="#/picks" data-nav="picks">Picks</a>`. `parseHash` gains `{name: 'picks'}`.
- `picks.js`: `export function openPicks(ui)` where `ui = {main, h, api, post, button, message, routeIs}` (passed from `app.js`, which keeps owning routing).
- `format.js`: `scoreLine({matched, of})` → `"Matched your pick 41 of your last 50 random sets"`, or `"7 random picks so far (the score needs 50)"` when `of < 50`.
- Teaching screen per spec §8.1: candidates in a grid at `w=400`; click opens `w=1600` with zoom linked across candidates; keys `1`–`9`, `N`, `S`, `Z`; after choosing, reveal "It picked the same" / "It picked this one" (outline on its candidate) and the five reason chips; Next button and Enter advance; the daily counter `done/10`; `pagehide` and leaving the tab call `/api/picks/session-end` (`navigator.sendBeacon` not usable: POST needs the header, so `fetch(..., {keepalive: true})`). Scoreboard above the screen; describe pause/resume button.

- [ ] **Step 1: Failing tests:** `scoreLine` both forms (unit). Playwright (rows inserted directly, as the existing UI tests do): the model's pick is not in the DOM before choosing; pressing `2` picks the second candidate, then the reveal shows match/mismatch correctly; `N` stores no-clear-best; `S` shows another set without storing; the counter goes `0/10 → 1/10`; leaving to `#/history` calls session-end (fake `AiControl` records `startTrain`); scoreboard text after seeded picks; no page errors.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** `npm test && npm run test:ui` — PASS.
- [ ] **Step 5:** Commit `feat(harvest-ui): Picks tab for teaching and the scoreboard`.

### Task 13: Rollout on the user's library

- [ ] **Step 1:** `make ai-install`; wait for `ai-describe` to empty the backlog (scoreboard shows it). Check `failedPhotos` and sample a few failures.
- [ ] **Step 2:** Confirm a `RULE_ONLY` model is ranked and the next staging build shows `0-best_` files under `similar/`; spot-check 10 sets with the user.
- [ ] **Step 3:** User teaches 20 sets; confirm a retrain runs after the 10th and 20th pick, the scoreboard updates, and the user's picks are marked in staging.
- [ ] **Step 4:** Full local tiers: `make test-smoke test-standard test-harvest chaos` — PASS (the AI tables are empty there, so the golden tree is unchanged).
