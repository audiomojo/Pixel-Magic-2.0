# Purge a similar set — design

Date: 2026-10-28. Status: approved in conversation (approach A, parts 1–3); this document is the
written spec.

## 1. Problem

The Picks screen shows similar sets whose photos the user does not want at all. Today the only
choices are a pick, "No clear best" or "Skip"; the photos stay in the vault and come back on every
harvest of their source drive.

## 2. Goal

From the Picks screen the user purges a whole set in one confirmed step. Every photo of the set
leaves the vault, a record of each file (checksum, size, names) is kept, and a purged file that
arrives again — by harvest or by a manual drop, even into a freshly wiped database — never
re-enters the vault. A purge can be unblocked; the next harvest then brings the photo back.

Non-goals: restoring a purged photo without its source drive; deleting anything on the source
drives; blocking by file name or by look-alike (only exact bytes are blocked); training the AI on
purges.

## 3. Definitions

- **Purge closure of a set:** the copy groups of the shown candidates (shown stills and their
  hidden lesser copies; groups merged into them per `merged_into`) plus Live motions of those
  stills (`live_content_id`). A copy group of the set that was never shown (an undescribed
  candidate, a group linked to the set after the last staging build) is not in it. Computed on the
  server from the current database; the shown list from the token only names which groups, and a
  purge is refused as stale when a shown sha is no longer a `HASHED`/`CLUSTERED` still in a copy
  group of the set.
- **Blocked sha:** a `purged` row with `unblocked_at IS NULL`.

## 4. Data model — migration `0011_purge.sql`

- Table `purged`: `sha256 char(64) PRIMARY KEY`, `size_bytes bigint NOT NULL`,
  `names text[] NOT NULL`, `purged_at timestamptz NOT NULL`, `unblocked_at timestamptz`,
  `set_id bigint` (the set it was purged from, for the list; null for entries reloaded without it).
  A purge after an unblock sets `purged_at = now()`, `unblocked_at = NULL`.
- `contents`: state check gains `PURGED`; new columns `purged_at timestamptz`,
  `purge_removed_at timestamptz`, `purge_reclaimed_bytes bigint`,
  `purge_mismatch boolean NOT NULL DEFAULT false`.
- `files`: state check gains `BLOCKED` (a dropped file turned away; its name is kept).
- `harvest_items`: state check gains `BLOCKED`.

## 5. Purging (server, `POST /api/picks/purge`)

`GET /api/picks/purge-preview?token=…` (called when the dialog opens) returns the closure's
counts — shown photos, hidden copies, Live videos, total bytes — with the same staleness check.

`POST` input: the set's pick token (the same signed token `next` issues). The server:

1. Verifies the token; recomputes the set's shown candidates. If they differ from the token's
   (staging rebuilt, a photo already purged) → 409 `set changed`; the UI reloads the next set.
2. In one transaction: lock the closure's content rows; set each to `PURGED`,
   `purged_at = now()`; upsert a `purged` row per content (names from `files.original_name`).
3. After commit: append one `purge` line per sha to `data/purged.csv` and fsync it (§7); request
   a staging build; run removal (§6) for each sha. The response is sent after the commit; removal
   continues in the background.

A purge is not a pick: no `picks` row, no change to today's count, no training trigger. Existing
picks that name a purged sha stay; `loadPickInputs` and the Picks API ignore purged shas.

## 6. Removing purged vault files

Generalize `removeRetired` (src/activities/identical.ts) into a shared removal used for both
`DUPLICATE` and `PURGED` contents. For `PURGED` there is no keeper check; everything else is the
same: DEFERRED while the vault file's nlink > 1 (a staging generation links it), re-hash before
unlink, mismatch → `purge_mismatch = true` and keep the file, missing file → removed with 0 bytes,
fsync the directory before recording `purge_removed_at` and `purge_reclaimed_bytes`.

The copy sweep (`pm-copies`, every 15 minutes) retries pending purged rows as it does retired
ones. `make doctor` reports `purged` and `purgeMismatched`.

Staging, the similar-set build and the Picks screen skip `PURGED` contents and their files, as
they skip `DUPLICATE`.

## 7. The durable record — `DATA_DIR/purged.csv`

Append-only, one line per event: `action,sha256,size_bytes,purged_or_unblocked_at,names`
(`action` is `purge` or `unblock`; `names` a JSON array of strings; fields CSV-quoted as needed). Lines are appended and
fsynced after the database commit.

On worker start, before the scanner or any ingest runs, a reconcile step merges the two both ways:
- replay the file in order into `purged` (latest event per sha wins), adding entries the database
  lacks;
- append a line for every `purged` row whose latest state the file lacks (a crash between commit
  and append).

So a wiped database rebuilds its block list from the file before any file is taken in. A missing
file is an empty list (first run); an unreadable line is logged and skipped, never fatal. A raw
line break always ends a line, even inside quotes (JSON-escaped names never contain one), so a
line torn by a crash cannot swallow the event appended after it. Besides worker start and
`PurgeSyncWorkflow`, the copy sweep also runs the reconcile, so a purge whose sync never started
reaches the file within one sweep period.

## 8. Blocking returning files

- **Ingest (drop zone):** in `ingestFile`, after hashing, if the sha is blocked: delete the
  claimed file, set the `files` row to `BLOCKED`, log the names. It is never linked into the vault
  and no content row is created.
- **Harvester:** in `known.ts`, the contents match ignores `PURGED` contents, and a blocked sha (by size + sha, like the contents match) returns a
  new outcome `BLOCKED`; `send.ts` settles the item as `BLOCKED` and does not send it. The run
  summary counts blocked files.

## 9. Unblocking

UI: the Purged list (§10) row's **Unblock** button, behind a confirmation.
`POST /api/picks/unblock {sha256s}`:

- set `purged.unblocked_at = now()`; append `unblock` lines to `purged.csv`;
- reset `harvest_items` in state `BLOCKED` or `KNOWN` with that sha to `PLANNED`, so the next run
  of its source sends it again; likewise `SENT` items whose copy was purged (sent before the
  still-`PURGED` content's `purged_at`), clearing their drop-zone copy and `file_id`. Only shas
  that are unblocked are reset. The known-check never counts such a `SENT` item as the winner
  of a duplicate.

On ingest of a sha whose content is `PURGED` and unblocked: link the file into the vault, reset
the content to `INGESTED` (clearing the purge columns) and run the normal content workflow.
Purging it again later blocks it again.

## 10. UI (src/harvest/ui/picks.js)

- **Purge set…** button beside "No clear best" and "Skip"; no keyboard shortcut.
- Dialog: "Delete these N photos, M hidden copies and K Live Photo videos (X MB) from the vault?
  Your originals on the source drives are not touched. These files will be blocked from coming
  back." Buttons **Purge** / **Cancel**. Cancel changes nothing.
- On success the next set loads. On 409 the screen reloads with a short notice.
- **Purged** section on the Picks tab (`GET /api/picks/purged`): one row per purge event (set),
  newest first: names, date, size, file count, **Unblock** (confirmed). Unblocked rows show
  "unblocked — returns on next harvest".

## 11. Testing

- **Unit / integration (testcontainers Postgres, temp vault):** closure (copies, Live video,
  merged groups); stale token → 409; purge transaction marks rows and writes `purged`; removal
  deferred while staged, then removed; mismatch flagged and kept; CSV append and both-way
  reconcile (wiped database, crash between commit and append, bad line); ingest blocks a purged
  sha (`files.BLOCKED`, no vault file, no content); harvester known → `BLOCKED`, not sent;
  unblock resets ledger items and ingest restores the photo through the pipeline; purge after
  unblock blocks again; picks naming a purged sha are ignored.
- **UI (Playwright):** button, dialog counts, Cancel does nothing, Purge loads the next set,
  Purged list and Unblock.
- **End-to-end:** purge a set, drop the same files again: none re-enter the vault; reconcile
  after recreating the database from the CSV.
- Existing tiers stay green.
