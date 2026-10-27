# Identical-Picture Dedupe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The vault never keeps two files whose decoded pictures are pixel-identical; the better file stays, the other is deleted with its record kept — for new drops and, once, for the existing vault.

**Architecture:** After `recordHash`, a new `dedupeIdentical` activity looks for suspects (same phash, dhash, width, height), compares full-resolution pixel digests in the decode pool, picks a keeper by the keeper rule, retires the rest (`contents.state = 'DUPLICATE'`) and unlinks their vault files after a checksum check. A CLI runs the same logic over the existing vault with a dry-run report.

**Tech Stack:** TypeScript (Node 24, ESM, strict), Temporal TS SDK, Postgres + node-pg-migrate, sharp / heic-decode / workerpool, exiftool-vendored, vitest + testcontainers.

**Spec:** `docs/superpowers/specs/2026-10-27-identical-dedupe-design.md`

## Global Constraints

- Identical = equal width, height, channel count and every pixel byte of the full-resolution decode with EXIF orientation applied, using the hashing decode paths (sharp `.rotate()`, heic-decode for HEVC, the BMP parser). Formats may differ. RAW and motion are never candidates.
- Suspect = state `HASHED` or `CLUSTERED`, kind `still`, not RAW, equal `phash`, `dhash`, `width`, `height`.
- Keeper rule, first separating rule decides: `camera-original` (Software / CreatorTool not matching the editor list) → `more-metadata` (count of `DateTimeOriginal`, `Make`, `Model`, `GPSLatitude`) → `larger` (`size_bytes`) → `earlier` (`ingested_at`, then sha256 ascending).
- Editor list (case-insensitive substring of `EXIF:Software` or `XMP:CreatorTool`): `picasa`, `photoshop`, `lightroom`, `gimp`, `iphoto`, `aperture`, `photos`, `windows photo`, `photo gallery`, `acdsee`, `snapseed`, `google`.
- Nothing is unlinked unless a fresh sha256 of the vault file equals `contents.sha256`; mismatch → `duplicate_mismatch = true`, no delete. Decode error / missing file → `pixel_compares.verdict = 'error'`, no retire.
- Workflow changes are replay-safe: the new step is behind `patched('dedupe-identical')`.
- The live vault is touched only by Task 9, and only after the user approves the dry-run report.
- `npm test`, `npm run lint`, `npm run typecheck`, `npm run format:check` stay green after every task.

## Review Focus

1. Two pixel-identical newcomers processed at the same time → exactly one survives (advisory lock keyed on the suspect key); test in Task 4.
2. A `HASHED` content retired by another workflow before its own `clusterContent` runs → its workflow ends `DUPLICATE`, never `UnexpectedState`; test in Task 4.
3. Same pixels but different EXIF orientation tags → not identical (orientation applied before digest); test in Task 2.
4. The retired vault file is still hardlinked from the current staging generation → unlink succeeds, staging keeps serving until the next build, `verify-vault` reports nothing; test in Task 6.
5. A pick the user made of the retired photo → staging marks the keeper; test in Task 6.

---

### Task 1: Migration 0009

**Files:**
- Create: `migrations/0009_identical_dedupe.sql`
- Test: `tests/unit/schema.test.ts`

**Interfaces:**
- Produces: `contents.duplicate_of char(64)`, `duplicate_rule text`, `duplicate_at timestamptz`, `duplicate_removed_at timestamptz`, `duplicate_reclaimed_bytes bigint`, `duplicate_mismatch boolean NOT NULL DEFAULT false`; `contents_state_check` allowing `'DUPLICATE'`; CHECK `contents_duplicate_check ((state = 'DUPLICATE') = (duplicate_of IS NOT NULL))`; index `contents_suspect_idx ON contents (phash, dhash, width, height) WHERE kind = 'still'`; table `pixel_compares(sha_a char(64), sha_b char(64), verdict text CHECK IN ('identical','different','error'), detail text, compared_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (sha_a, sha_b), CHECK (sha_a < sha_b))`.

- [ ] **Step 1:** Add schema tests: inserting a `DUPLICATE` content without `duplicate_of` fails; with it succeeds; a non-duplicate with `duplicate_of` fails; `pixel_compares` rejects `sha_a >= sha_b` and an unknown verdict.
- [ ] **Step 2:** Run `npx vitest run tests/unit/schema.test.ts` — FAIL.
- [ ] **Step 3:** Write the migration (drop and re-add `contents_state_check` with `'DUPLICATE'` added).
- [ ] **Step 4:** Run the schema tests — PASS; full `npm test` green.
- [ ] **Step 5:** Commit `feat(db): identical-dedupe columns and pixel_compares`.

### Task 2: Pixel digest in the decode pool

**Files:**
- Modify: `src/decode.ts`, `src/decodeWorker.ts`, `src/activities/hashing.ts` (export a pool call helper if useful)
- Test: `tests/unit/decode.test.ts`

**Interfaces:**
- Produces: `pixelDigest(absPath: string, ext: string): Promise<PixelDigest>` in `src/decode.ts`, `interface PixelDigest { width: number; height: number; channels: number; sha256: string }` — sha256 over the raw oriented full-resolution buffer (8-bit per channel); registered in the decode worker as `pixelDigest`. Throws `QuarantineError('unsupported', …)` for RAW extensions.

- [ ] **Step 1:** Tests (use `tests/helpers/images.ts` builders): a JPEG and a lossless PNG of its decoded pixels → same digest; the PNG with one pixel changed → different; same pixels with orientation 1 vs a copy carrying orientation 6 → different dimensions/digest; a JPEG and a byte-copy with only an EXIF Software tag changed → same digest; HEIC fixture `scene0.heic` digests deterministically twice; a RAW ext → `QuarantineError`.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement by reusing the existing sniff/dispatch of `computeImageFacts` (HEIC → heic-decode RGBA into sharp, BMP parser, else sharp with `.rotate()`), ending in `.raw().toBuffer({ resolveWithObject: true })` and hashing `data`. Keep alpha only if the source has it (no `removeAlpha`), so channel count is part of identity.
- [ ] **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(decode): full-resolution pixel digest`.

### Task 3: Keeper rule and keeper tags

**Files:**
- Create: `src/identical.ts`
- Modify: `src/metadata.ts` (`MetadataReader` gains `keeperTags`)
- Test: `tests/unit/identical.test.ts`, `tests/unit/metadata.test.ts`

**Interfaces:**
- Produces in `src/identical.ts`:
  - `EDITOR_PATTERNS: readonly string[]` (the Global Constraints list, verbatim).
  - `isEditor(software: string | null, creatorTool: string | null): boolean`.
  - `interface KeeperFacts { sha256: string; software: string | null; creatorTool: string | null; metaCount: number; sizeBytes: bigint; ingestedAt: Date }`.
  - `type KeeperRule = 'camera-original' | 'more-metadata' | 'larger' | 'earlier'`.
  - `chooseKeeper(members: KeeperFacts[]): { keeper: string; losers: { sha256: string; rule: KeeperRule }[] }` — each loser's rule is the first rule separating it from the keeper.
- Produces in `src/metadata.ts`: `MetadataReader.keeperTags(absPath: string): Promise<{ software: string | null; creatorTool: string | null; metaCount: number }>`; `metaCount` counts present `EXIF:DateTimeOriginal`, `EXIF:Make`, `EXIF:Model`, `EXIF:GPSLatitude` (or `Composite:GPSLatitude`).

- [ ] **Step 1:** Tests: `isEditor('Picasa 3.0', null)` true; `isEditor(null, 'Adobe Photoshop Lightroom Classic')` true; `isEditor('Photos 9.0', null)` true; `isEditor('17.1', null)` false; `isEditor('Digital Camera FinePix S7000 Ver1.00', null)` false; `isEditor(null, null)` false. `chooseKeeper`: each rule deciding once (camera-original beats a larger Picasa file; more-metadata; larger; earlier; full tie → lower sha); three members → one keeper, two losers with their own rules. `keeperTags` on an `exifFixtures` JPEG with Software set returns it and the count.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(identical): keeper rule and keeper tags`.

### Task 4: `dedupeIdentical` activity and workflow step

**Files:**
- Create: `src/activities/identical.ts`
- Modify: `src/activities/index.ts`, `src/workflows.ts`, `src/activities/cluster.ts`, `src/activities/hashing.ts` (`RECORDED` maps `DUPLICATE` → `'HASHED'`), `src/worker.ts` if activity/queue registration lists names
- Test: `tests/unit/identicalActivities.test.ts` (testcontainers Postgres + temp layout, like `hashClusterActivities.test.ts`), `tests/workflow/workflows.test.ts`

**Interfaces:**
- Consumes: `pixelDigest` (Task 2, via `deps.decodePool.exec('pixelDigest', …)` with `decodeTimeoutS`), `chooseKeeper`, `keeperTags` (Task 3), `layout.vaultFile`, `sha256File`, `fsyncDir`, `withTx`.
- Produces from `makeIdentical(deps)`:
  - `findFamily(c, sha256): Promise<string[]>` — internal: the content plus every suspect proven identical (comparing, and recording in `pixel_compares`, pairs not yet `identical`/`different`).
  - `retireFamily(c, members: string[]): Promise<{ keeper: string; retired: { sha256: string; rule: KeeperRule }[] }>` — one transaction: losers → `DUPLICATE` (`duplicate_of` = keeper, re-pointing older duplicates of a loser to the keeper), Live link moved to keeper when it has none, `cluster_state.version` bumped when any loser was `CLUSTERED`.
  - `removeRetired(sha256): Promise<'REMOVED' | 'ALREADY_REMOVED' | 'MISSING' | 'MISMATCH'>` — fresh sha256 of the vault file, unlink + `fsyncDir`, set `duplicate_removed_at` / `duplicate_reclaimed_bytes` (0 for MISSING), or `duplicate_mismatch = true`.
  - Activity `dedupeIdentical(sha256: string): Promise<'UNIQUE' | 'KEPT' | 'DUPLICATE'>` — queue `cpu`; no suspects → `UNIQUE` with one query; else under `pg_advisory_xact_lock(hashtext('pm.identical:' || phash || ':' || dhash || ':' || width || ':' || height))` finds the family, retires, then (after commit) `removeRetired` for each loser; returns `DUPLICATE` if `sha256` itself was retired. Idempotent: a content already `DUPLICATE` returns `DUPLICATE` (and finishes a pending `removeRetired`).
  - `ContentOutcome` gains `'DUPLICATE'`; `runContentSteps` calls `dedupeIdentical` between `recordHash` and `clusterContent` when `patched('dedupe-identical')`, returning `'DUPLICATE'` without clustering.
  - `clusterContent` on a content now `DUPLICATE` returns without error (Review Focus 2).
- Logs each removal: kept path + original name, removed path + original name, rule.

- [ ] **Step 1:** Integration tests: newcomer loses (no clustering, vault file gone, row `DUPLICATE` with rule); newcomer wins over a `CLUSTERED` suspect (old retired, `cluster_state.version` bumped, newcomer then clusters into the same similar set as the old one's partner); three identical; one-pixel-different suspect → `different` recorded, nothing retired, and a second run does not decode it again; RAW suspect ignored; decode error → `error` verdict, nothing retired; checksum mismatch → `duplicate_mismatch`, file kept; Live link moved; two newcomers run concurrently → exactly one non-duplicate; a `HASHED` content retired before its `clusterContent` → `clusterContent` returns cleanly.
- [ ] **Step 2:** Workflow test (time-skipping env, mocked activities): `dedupeIdentical` → `DUPLICATE` ends the workflow with `'DUPLICATE'` and no `clusterContent` call; `UNIQUE`/`KEPT` continue to clustering; a history recorded before the patch replays.
- [ ] **Step 3:** Run — FAIL.
- [ ] **Step 4:** Implement.
- [ ] **Step 5:** Run — PASS; full `npm test`.
- [ ] **Step 6:** Commit `feat(pipeline): retire pixel-identical duplicates after hashing`.

### Task 5: A retired file dropped again

**Files:**
- Modify: `src/activities/ingest.ts`, `src/activities/copies.ts`
- Test: `tests/unit/ingest.test.ts`, `tests/unit/copies.test.ts`

**Interfaces:**
- Consumes: `contents.state = 'DUPLICATE'` (Task 1).
- Produces: in `ingestFile`, before step 5 (vault link), a content row with this sha256 in state `DUPLICATE` → rename to `.copies` and `commitCopy` (never linked back into the vault). In `reclaimRow`, when the file's content is `DUPLICATE`, the `.copies` file is unlinked if its fresh sha256 equals the row's sha256 (no vault comparison; the vault file is gone by design); mismatch flags as today.

- [ ] **Step 1:** Tests: drop the exact bytes of a retired content → file `COPY`, `.copies` file removed by `reclaimCopy`, no vault file recreated, `contents` row still `DUPLICATE`; a `.copies` file of a retired content whose bytes differ → flagged, not deleted.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(ingest): a retired duplicate dropped again is removed, not re-vaulted`.

### Task 6: Staging, picks and operator views skip retired contents

**Files:**
- Modify: `src/activities/staging.ts` (contents query and `loadPickInputs`), `src/harvest/picks.ts` (candidate / "changed since your pick" checks), `src/operator.ts` (`doctor` counts, `verify-vault` unaffected but tested)
- Test: `tests/unit/stagingBuild.test.ts`, `tests/unit/harvestPicks.test.ts`, `tests/unit/cli.test.ts` or the operator test that covers `verify-vault`/`doctor`

**Interfaces:**
- Produces: staging loads no `DUPLICATE` content and stages no file of one; `loadPickInputs` maps `chosen_sha256` of a retired content to its `duplicate_of`; the Picks API treats a set whose only change is a retired member replaced by its keeper as unchanged; `doctor` reports `duplicatesRetired` and `duplicatesMismatched`.

- [ ] **Step 1:** Tests: a retired content in a similar set → not staged, the set's other members staged; a pick of the retired sha → the keeper gets `0-best_`; retired vault file unlinked while the current generation still links it → current tree readable, next build drops it, `verify-vault` reports no mismatch (Review Focus 4); Picks API re-offer rule with a retired member; `doctor` counts.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `feat(staging): retired duplicates leave staging; picks follow the keeper`.

### Task 7: `dedupe-identical` CLI over the existing vault

**Files:**
- Modify: `src/cli.ts`, `src/activities/identical.ts` (export a non-activity runner), `Makefile`, `README.md`
- Test: `tests/unit/cli.test.ts`, `tests/unit/identicalActivities.test.ts`

**Interfaces:**
- Consumes: `findFamily`, `retireFamily`, `removeRetired`, `chooseKeeper` (Tasks 3–4).
- Produces: `node dist/cli.js dedupe-identical [--apply]`; `make dedupe-identical [APPLY=1]` running it in the worker container (like `make stage`). Steps: first `removeRetired` every `DUPLICATE` with `duplicate_removed_at IS NULL AND NOT duplicate_mismatch` (resume); then every suspect key with more than one `HASHED`/`CLUSTERED` non-RAW still, families found as in Task 4, under the same advisory lock. Dry run writes `DATA_DIR/reports/dedupe-identical-<YYYYMMDD-HHMMSS>.csv` with columns `keeper_sha256,keeper_vault_path,keeper_names,removed_sha256,removed_vault_path,removed_names,rule,bytes` (names = `files.original_name` joined by `;`) and prints `families N, files to remove N, bytes to free X, errors N`; `--apply` writes the same report (`…-applied.csv`) and retires + removes per family. Dry run writes nothing to the database except `pixel_compares` rows.

- [ ] **Step 1:** Tests: dry run on a seeded database → report rows and totals, no state change, no unlink; apply → rows retired, files gone; apply re-run → no-op; a retired row with no `duplicate_removed_at` (simulated crash) is removed first on the next run; usage error for unknown flags.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** README section "Identical-picture dedupe" (what is removed, the keeper rule, dry run first, then `APPLY=1`, then `make stage FORCE=1`).
- [ ] **Step 6:** Commit `feat(cli): dedupe-identical with a dry-run report`.

### Task 8: End-to-end and chaos tiers

**Files:**
- Modify: `tools/corpus/lib.ts` (`synthesize`: new `meta` variant; `png` expectation), `tests/lib/expectations.ts`, `tests/lib/invariant.ts`, `tests/chaos/chaos.test.ts`, `tests/e2e/smoke.test.ts`; `SeedFile['variant']` type
- Test: the tiers themselves plus `tests/unit/expectations.test.ts`, `tests/unit/corpusTools.test.ts`

**Interfaces:**
- Produces: `meta` variant — for JPEG seeds only, the seed's bytes copied verbatim with `EXIF:Software` set to `Picasa 3.0` via exiftool-vendored (`-overwrite_original`), expectation `duplicate` (the seed is kept). Expectation kind `duplicate` = the variant's content ends `DUPLICATE` with `duplicate_of` = the seed's sha256 (or, for `png`, decided by measured pixel digests: identical → `duplicate` of the seed, else today's `copy-if-tight`). Invariant: "every distinct seeded sha256 is in the vault exactly once" becomes "exactly once, or zero times when its content is `DUPLICATE` with `duplicate_removed_at` set". The chaos tier adds a kill during `dedupeIdentical` (checkpoint `identical_retired`, between retire commit and unlink).

- [ ] **Step 1:** Unit tests for the new expectation and invariant clauses.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement. **Step 4:** Run — PASS.
- [ ] **Step 5:** `make update-golden` (local, git-ignored), then `make test-smoke test-standard test-harvest chaos` — PASS; corpus check reports 0 drifted.
- [ ] **Step 6:** Commit `test(e2e): metadata-only and lossless duplicates are retired`.

### Task 9: Rollout on the live library (user approval required)

- [ ] **Step 1:** `make up` (applies 0009, restarts the worker), DB backup first (`pg_dump -Fc` to `data/backups/pre-0009-<ts>.dump`).
- [ ] **Step 2:** `make dedupe-identical`; share the report path, totals and a sample of rows with the user. Stop for approval.
- [ ] **Step 3:** After approval: `make dedupe-identical APPLY=1`, then `make stage FORCE=1`; report families retired, bytes freed, and the new count of similar sets and of tied sets.
