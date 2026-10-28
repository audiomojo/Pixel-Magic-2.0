# Purge a Similar Set Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** From the Picks screen the user purges a whole similar set after a confirmation; its photos (with hidden copies and Live videos) leave the vault, each file's checksum and names are recorded in the database and in `DATA_DIR/purged.csv`, and a purged file arriving again — by harvest or drop, even into a wiped database — is blocked. A purge can be unblocked.

**Architecture:** The harvester's Picks API (native on the Mac) marks the closure `PURGED` and upserts `purged` rows in one transaction, then starts `PurgeSyncWorkflow` on the worker. The worker owns the file side: it appends to `purged.csv` (reconciled both ways at worker start), rebuilds staging, and deletes vault files with the removal steps the identical-dedupe built (deferred while staged, re-hash before unlink); the copy sweep finishes deferred ones. Ingest and the harvester's known-check consult `purged`.

**Tech Stack:** TypeScript (Node 24, ESM, strict), Temporal TS SDK, Postgres + node-pg-migrate, vitest + testcontainers, Playwright (ui project).

**Spec:** `docs/superpowers/specs/2026-10-28-purge-sets-design.md`

## Global Constraints

- Blocking is by exact sha256 (+ size) only — never by name or look-alike.
- Purge closure = the copy groups of the shown candidates (shown stills and their hidden lesser copies in `HASHED`/`CLUSTERED`; groups merged into them per `merged_into`) plus Live motions of those stills (`kind = 'motion'` contents sharing a non-null `live_content_id` with a closure still). Computed on the server; the token's shown list only selects the groups, and a shown sha that is not a `HASHED`/`CLUSTERED` still in a copy group of the set makes the purge stale.
- A purge is refused (HTTP 409 `set changed`) when the set's current shown candidates (described, not `PURGED`, not `DUPLICATE`) differ from the token's `shown`.
- A purge is not a pick: no `picks` row, no change to today's count, no training trigger.
- No vault file is unlinked unless nlink is 1 and a fresh sha256 equals `contents.sha256`; mismatch → `purge_mismatch = true`, file kept.
- `purged.csv` line: `action,sha256,size_bytes,at,names` — `action` ∈ `purge|unblock`, `at` ISO-8601 UTC, `names` a JSON array of strings, RFC 4180 quoting of every field that needs it. Header line `action,sha256,size_bytes,at,names`. Append-only; fsync after append.
- Only the worker writes `purged.csv` and the vault; the harvester process writes only the database and starts workflows.
- Workflow changes are replay-safe (new branches only for new activity results; existing histories replay unchanged — the replay test in `tests/workflow` stays green).
- UI copy, verbatim: button `Purge set…`; dialog `Delete these N photos, M hidden copies and K Live Photo videos (X MB) from the vault? Your originals on the source drives are not touched. These files will be blocked from coming back.`; buttons `Purge` / `Cancel`; unblocked row note `unblocked — returns on next harvest`. Counts with zero are left out of the sentence (`Delete these 2 photos (14.2 MB) from the vault? …`).
- `npm test`, `npm run lint`, `npm run typecheck`, `npm run format:check` stay green after every task; `npm run test:ui` after Tasks 7–8.
- The live system is not touched by this plan; rollout happens after the branch is finished, on the user's say-so.

## Review Focus

1. Double-click / retried `POST /api/picks/purge` with the same token → one purge, the second returns the same summary (idempotent on already-`PURGED` closure); test in Task 7.
2. A purged sha that is also the keeper of retired `DUPLICATE`s → the duplicates stay retired (their record points at a purged keeper); their re-drop is still parked as COPY, and `removeRetired`'s keeper check treats a `PURGED` keeper as "keeper gone" (flag, never delete the duplicate's record wrongly); test in Task 3.
3. `purged.csv` has a line with a name containing a comma, quote or newline → round-trips exactly; test in Task 2.
4. Worker restarts while a purge's files are DEFERRED → the copy sweep removes them after staging drops its links; test in Task 3.
5. A blocked file dropped while its content row still exists as `PURGED` with the vault file not yet removed → still blocked (no link into the vault, no COPY of a doomed file); test in Task 5.

---

### Task 1: Migration 0011

**Files:**
- Create: `migrations/0011_purge.sql`
- Modify: `tests/helpers/pg.ts` (truncate list gains `purged`)
- Test: `tests/unit/schema.test.ts`

**Interfaces:**
- Produces: table `purged(sha256 char(64) PRIMARY KEY, size_bytes bigint NOT NULL, names text[] NOT NULL DEFAULT '{}', purged_at timestamptz NOT NULL, unblocked_at timestamptz, set_id bigint)`; `contents` state check gains `'PURGED'`, columns `purged_at timestamptz`, `purge_removed_at timestamptz`, `purge_reclaimed_bytes bigint`, `purge_mismatch boolean NOT NULL DEFAULT false`, CHECK `contents_purge_check ((state = 'PURGED') = (purged_at IS NOT NULL))`; `files` state check gains `'BLOCKED'`; `harvest_items` state check gains `'BLOCKED'`; `harvest_run_folders.blocked bigint NOT NULL DEFAULT 0` (beside `known`); `copy_sweep_state.purged_after text NOT NULL DEFAULT ''`.

- [ ] **Step 1:** Schema tests: `PURGED` content without `purged_at` fails, with it succeeds; non-PURGED with `purged_at` fails; `files.state = 'BLOCKED'` accepted; `harvest_items.state = 'BLOCKED'` accepted; `purged` row insert/upsert works.
- [ ] **Step 2:** Run `npx vitest run tests/unit/schema.test.ts` — FAIL.
- [ ] **Step 3:** Write the migration (drop and re-add each state check with the new value, keeping every existing value).
- [ ] **Step 4:** Schema tests PASS; `npm test` green.
- [ ] **Step 5:** Commit `feat(db): purge table, PURGED contents, BLOCKED files and items`.

### Task 2: The purge log file

**Files:**
- Create: `src/purgeLog.ts`
- Modify: `src/worker.ts` (call reconcile at start, after `layout.ensure()`, before any `Worker.create`)
- Test: `tests/unit/purgeLog.test.ts`

**Interfaces:**
- Produces in `src/purgeLog.ts`:
  - `interface PurgeEvent { action: 'purge' | 'unblock'; sha256: string; sizeBytes: bigint; at: Date; names: string[] }`
  - `formatEvent(e: PurgeEvent): string` (one line, no trailing newline), `parseLog(text: string): { events: PurgeEvent[]; bad: number[] }` (bad = 1-based line numbers skipped)
  - `purgeLogPath(dataDir: string): string` → `<dataDir>/purged.csv`
  - `reconcilePurgeLog(pool: pg.Pool, dataDir: string, log: Logger): Promise<{ imported: number; appended: number; bad: number }>` — under `pg_advisory_lock(hashtext('pm.purge-log'))`: read the file (missing = empty; create with header on first append); latest event per sha (by `at`, then file order) upserted into `purged` when newer than the row's latest (`greatest(purged_at, unblocked_at)`), setting `purged_at`/`unblocked_at` accordingly (`purge` after `unblock` clears `unblocked_at`); then for every `purged` row whose latest state the file's latest event for that sha does not equal, append the event (`purge` with `purged_at`, or `unblock` with `unblocked_at`), fsync.

- [ ] **Step 1:** Tests: format/parse round-trip incl. names with `,`, `"`, `|`, a newline and unicode; header skipped; a garbage line → in `bad`, others kept; reconcile on empty DB + file → rows imported (`purge` then `unblock` → `unblocked_at` set; `unblock` then `purge` → null); DB rows missing from file → appended once (second run appends nothing); missing file → created with header only when something is appended; two concurrent reconciles → no duplicate lines.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement (names field = `JSON.stringify(names)`, CSV-quoted; parse with a small RFC 4180 reader — no new dependency).
- [ ] **Step 4:** Wire into `src/worker.ts`; failure to reconcile is fatal at start (logged) — a worker never ingests without its block list.
- [ ] **Step 5:** Tests PASS; commit `feat(purge): durable purged.csv with both-way reconcile at worker start`.

### Task 3: Purge core, removal and sync workflow

**Files:**
- Create: `src/purge.ts`
- Modify: `src/activities/identical.ts` (extract the shared removal), `src/activities/copies.ts` (`sweepPurged` pass), `src/activities/index.ts`, `src/workflows.ts` (`PurgeSyncWorkflow`), `src/operator.ts` (doctor)
- Test: `tests/unit/purge.test.ts`, `tests/unit/identicalActivities.test.ts`, `tests/unit/copies.test.ts`, `tests/workflow/workflows.test.ts`

**Interfaces:**
- Produces in `src/purge.ts`:
  - `interface PurgeSummary { shown: number; copies: number; live: number; bytes: bigint; shas: string[] }`
  - `currentShown(c: pg.Pool | pg.PoolClient, setId: string): Promise<string[]>` — the set's candidates as the Picks API shows them (in `similar_candidates`, described not failed, state not `PURGED`/`DUPLICATE`), ascending.
  - `purgeClosure(c, setId: string, shown: string[]): Promise<PurgeSummary>` — per Global Constraints; `shown` count = closure stills in `shown`, `copies` = other stills, `live` = motions.
  - `purgeSet(pool: pg.Pool, setId: string, shown: string[]): Promise<PurgeSummary | 'stale'>` — one transaction: if every `shown` sha is already `PURGED` → return the stored summary (idempotent); if `currentShown` ≠ sorted `shown` → `'stale'`; lock closure rows `FOR UPDATE`; set `state = 'PURGED', purged_at = now()`; upsert `purged` (`names` = distinct `files.original_name` of the sha, `set_id`, `purged_at = now()`, `unblocked_at = NULL`).
  - `unblock(pool: pg.Pool, shas: string[]): Promise<number>` — sets `unblocked_at = now()` where currently blocked; resets `harvest_items` with that sha in `KNOWN`/`BLOCKED` to `PLANNED`; returns rows changed.
- Produces in `src/activities/identical.ts`: `removePurged(ctx: IdenticalCtx, sha256: string): Promise<RemoveOutcome>` sharing one internal removal routine with `removeRetired` (no keeper check for purge; columns `purge_removed_at`, `purge_reclaimed_bytes`, `purge_mismatch`; log messages `purged photo removed` / `… removal deferred: still linked` / `… not removed: mismatch flagged`). `removeRetired` treats a keeper in state `PURGED` as gone (flag).
- Produces activities (queue `io`): `syncPurgeLog(): Promise<{ appended: number }>` (calls `reconcilePurgeLog`), `removePurgedBatch(shas: string[]): Promise<SweepResult>`.
- Produces `PurgeSyncWorkflow(shas: string[]): Promise<{ appended: number; removed: number; deferred: number }>` — `syncPurgeLog` → child `StagingWorkflow(false)` (id `pm-stage-purge-<workflowId>`) → `removePurgedBatch(shas)`.
- `sweepCopies` gains a third pass `sweepPurged` with cursor `copy_sweep_state.purged_after`, same shape as `sweepRetired`.
- Doctor gains `purged: number` and `purgeMismatched: number`.

- [ ] **Step 1:** Tests (testcontainers PG, temp vault): closure includes hidden copies, a merged group's members, a Live motion, excludes other sets and `DUPLICATE`s; `purgeSet` stale when a shown sha is missing / extra; second call returns same summary, no second upsert of `purged_at`; `unblock` sets `unblocked_at` and resets a KNOWN ledger item to PLANNED; `removePurged` DEFERRED with a second link, REMOVED after it's gone, MISMATCH on altered bytes, MISSING on absent file; Review Focus 2 (purged keeper of a DUPLICATE → `removeRetired` flags); Review Focus 4 (sweep removes deferred purged after the link is dropped); workflow test: `PurgeSyncWorkflow` calls the three steps in order with fakes; doctor counts.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; full `npm test` green (replay test included).
- [ ] **Step 5:** Commit `feat(purge): purge a set, remove its vault files, sync workflow and sweep`.

### Task 4: Everything that lists contents skips PURGED

**Files:**
- Modify: `src/activities/staging.ts` (`loadFacts` contents and files filters), `src/harvest/picks.ts` (`next` candidates exclude `PURGED`), `src/activities/staging.ts` `loadPickInputs` (picks naming a purged sha are ignored)
- Test: `tests/unit/stagingBuild.test.ts`, `tests/unit/harvestPicks.test.ts`

**Interfaces:** consumes Task 1 columns; produces no new symbols.

- [ ] **Step 1:** Tests: a `PURGED` content and its files are not staged (and not under `similar/`); its set with one remaining candidate stages as unique and leaves `similar_candidates`; `next` never offers a set whose shown list would include a purged sha, and a set left with < 2 candidates is not offered — before any staging rebuild; a pick whose chosen sha is purged no longer marks `0-best_`.
- [ ] **Step 2:** Run — FAIL. **Step 3:** Implement (`state NOT IN ('DUPLICATE','PURGED')` and matching `NOT EXISTS`). **Step 4:** PASS.
- [ ] **Step 5:** Commit `feat(purge): staging, sets and picks skip purged photos`.

### Task 5: Ingest blocks purged bytes and restores unblocked ones

**Files:**
- Modify: `src/activities/ingest.ts`, `src/types.ts` (`IngestResult.state` gains `'BLOCKED'`), `src/workflows.ts` (FileWorkflow ends on `BLOCKED` with no content steps)
- Test: `tests/unit/ingest.test.ts`, `tests/workflow/workflows.test.ts`

**Interfaces:**
- After hashing and before the retired-duplicate check: if `purged` has the sha with `unblocked_at IS NULL` → unlink the claimed file, fsync its dir, set `files.state = 'BLOCKED'` (guarded from `CLAIMED`), log `blocked purged file` with name and sha, return `{ state: 'BLOCKED', … }`.
- If the content is `PURGED` and the sha is unblocked (or has no `purged` row): link into the vault as an owner and, in `commitOwner`'s transaction, `UPDATE contents SET state = 'INGESTED', purged_at = NULL, purge_removed_at = NULL, purge_reclaimed_bytes = NULL, purge_mismatch = false WHERE sha256 = $1 AND state = 'PURGED'`; the workflow then runs content steps as for a new content.

- [ ] **Step 1:** Tests: blocked sha → claimed file gone, row `BLOCKED`, no vault file, no contents row created (wiped-DB case: `purged` row only); Review Focus 5 (content still `PURGED` with vault file present → blocked, vault untouched); unblocked sha with `PURGED` content → vault file linked, content `INGESTED`, purge columns cleared; workflow: an ingest result `BLOCKED` ends the FileWorkflow without content steps; re-running a BLOCKED ingest returns the stored outcome (R1 path).
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** PASS, replay test green.
- [ ] **Step 5:** Commit `feat(purge): ingest blocks purged bytes, restores unblocked ones`.

### Task 6: Harvester skips blocked files

**Files:**
- Modify: `src/harvest/known.ts` (contents match ignores `PURGED`; new outcome `{ state: 'BLOCKED'; sha256: string }` when `purged` has size + sha blocked), `src/harvest/send.ts` (settle `BLOCKED`, counter `blocked`), `src/harvest/runs.ts` (summary JSON gains `blocked`), `src/harvest/ui/app.js` (show `blocked` beside `known` when non-zero)
- Test: `tests/unit/harvestKnown.test.ts`, `tests/unit/harvestSend.test.ts`, `tests/unit/harvestServer.test.ts` (runs summary)

- [ ] **Step 1:** Tests: item whose sha is blocked → `BLOCKED`, not sent, counter incremented; unblocked → `SEND` even though a `PURGED` content exists; a `PURGED` content with no `purged` row (should not happen, defensive) → `SEND`; run summary includes `blocked`.
- [ ] **Step 2:** FAIL. **Step 3:** Implement (size pre-check like the contents path to avoid hashing every file). **Step 4:** PASS.
- [ ] **Step 5:** Commit `feat(harvest): blocked purged files are not sent`.

### Task 7: Picks API — preview, purge, list, unblock

**Files:**
- Modify: `src/harvest/picks.ts` (routes), `src/harvest/aiControl.ts` or a new `src/harvest/pipelineControl.ts` (`startPurgeSync(shas: string[]): Promise<void>` — starts `PurgeSyncWorkflow` on queue `main`, id `pm-purge-<Date.now()>`), `src/harvest/server.ts` (wire the control into `PicksDeps`)
- Test: `tests/unit/harvestPicks.test.ts`

**Interfaces:**
- `PicksDeps` gains `pipeline: { startPurgeSync(shas: string[]): Promise<void> }` (tests fake it).
- `GET /api/picks/purge-preview?token=…` → `{ shown, copies, live, bytes }` (bytes as a number) or 409 `{ error: 'set changed' }`; 400 on a bad token.
- `POST /api/picks/purge { token }` → `{ purged: PurgeSummary-as-json }` after commit, then `startPurgeSync(shas)` (a failure to start is logged and returned as `{ purged, syncStarted: false }` — the sweep and next worker start reconcile anyway); 409 on stale.
- `GET /api/picks/purged` → `{ purges: [{ setId, purgedAt, names: string[], files, bytes, unblocked: boolean, shas: string[] }] }` grouped by `set_id` (null set → one entry per sha), newest first, max 500.
- `POST /api/picks/unblock { shas: string[] }` (1–1000 valid shas) → `{ unblocked: n }` then `startPurgeSync([])`.

- [ ] **Step 1:** Tests: preview counts; purge → contents PURGED, fake `startPurgeSync` called with the closure; Review Focus 1 (same token twice → one purge, same summary, sync started each time is fine); stale → 409, nothing changed; purge does not change `/api/picks/next`'s today count; purged list grouping and order; unblock validates input and calls sync.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** PASS.
- [ ] **Step 5:** Commit `feat(picks): purge and unblock API`.

### Task 8: Picks UI — Purge button, dialog, Purged list

**Files:**
- Modify: `src/harvest/ui/picks.js`, `src/harvest/ui/format.js` (pure `purgeSentence({shown,copies,live,bytes})`), `src/harvest/ui/app.css`
- Test: `tests/unit/harvestUiFormat.test.ts`, `tests/ui/harvestPage.test.ts`

- [ ] **Step 1:** Tests: `purgeSentence` exact copy per Global Constraints for (2,0,0), (2,3,1), (1 photo singular? — sets have ≥2 shown; copies/videos singular "1 hidden copy", "1 Live Photo video"); MB with one decimal. Playwright: `Purge set…` button present when no choice made, not bound to any key; dialog shows the sentence; Cancel → same set still shown, no request; Purge → next set loads; 409 → notice and reload; Purged section lists the purge; Unblock → confirm → row shows `unblocked — returns on next harvest`.
- [ ] **Step 2:** FAIL. **Step 3:** Implement with a native `<dialog>` (focus trapped, Escape = Cancel, Purge button not default-focused). **Step 4:** PASS; `npm run test:ui` all green.
- [ ] **Step 5:** Commit `feat(picks-ui): purge a set and unblock from the Purged list`.

### Task 9: End-to-end

**Files:**
- Modify: `tests/e2e/standard.test.ts`, `tests/lib/expectations.ts` / `tests/lib/invariant.ts` as needed
- Test: the standard tier

- [ ] **Step 1:** In the standard tier, after the first full pass: pick one expected similar set, call `purgeSet` + start `PurgeSyncWorkflow` (as the API does), run two staging builds and a copy sweep; assert its files are gone from the vault and staging, `purged.csv` has its lines, doctor `purgeMismatched` 0. Re-drop the set's original files into the drop zone; assert each ends `files.state = 'BLOCKED'` and the vault count is unchanged. Truncate `purged` (simulated wipe), restart the worker, assert reconcile re-imported every sha and a further re-drop is still blocked.
- [ ] **Step 2:** Run `make test-smoke`, `make test-standard`, `make chaos` — PASS.
- [ ] **Step 3:** Commit `test(e2e): purged sets stay out of the vault, also after a wiped block list`.
