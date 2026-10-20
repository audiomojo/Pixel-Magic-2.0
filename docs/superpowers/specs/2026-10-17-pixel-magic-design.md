# Pixel Magic 2.0 — Design Spec

> Status: **DRAFT for review** · 2026-10-17
> Supersedes `architecturte/01–05` (kept for history). Where they disagree, this spec wins.

## 1. Purpose

Clean up three decades of personal photos (hundreds of thousands of files, mostly from iPhones) on a Mac. Then keep running with light, periodic use. A future AI stage will score images at about 20–30 s each, so the pipeline will run for **weeks to months**. It must survive restarts, sleep, reboots and redeploys **without losing its place, losing a file, or doing a file's work twice**.

Success means:
1. Every dropped file ends in exactly one final state, and its bytes still exist.
2. True copies of the same shot are grouped, with one best copy picked.
3. Different shots that look alike (bursts, retakes) are **all kept**, grouped together as input for the future AI.
4. A crash at any moment changes nothing about the final result.
5. Progress is visible live.

## 2. Requirements

| ID | Requirement |
|----|-------------|
| F1 | A **drop zone** folder is the only way in. Files may arrive one at a time or 100k+ at once, in nested folders. |
| F2 | **Copy** = the same shot: byte-identical, or a re-encode/resize/format change of it. **Similar** = a *different* shot that looks alike (burst frame, retake). A burst frame is never a copy. |
| F3 | Staging buckets: `unique/`, `similar/`, `copies/`, `quarantine/`, `other/`. |
| F4 | Each copy group has one **best copy**, chosen by the rule in §6.4. |
| F5 | The original relative path, name, size and mtime of every dropped file are kept. |
| F6 | Live Photo motion clips (MOV) are staged beside their still, in the same folder, with the same base name. |
| N1 | **Resumable:** a crash, `docker compose down`, power loss or Mac sleep at any point → processing resumes where it stopped, with no manual steps and no double effects. |
| N2 | **Distributed:** workers are stateless and poll task queues. A worker can run in Docker, natively on macOS (needed for Metal/ANE AI), or on another machine. |
| N3 | **Never destroys image bytes.** The vault is append-only. Deleting anything is always a human decision outside the pipeline, with one exception the user decided (2026-10-18, R9): a dropped file whose bytes the vault already holds is removed once a fresh re-hash of it **and** of the vault file both equal its SHA-256 (§6.5). The vault holds each distinct file once. |
| N4 | **Observable:** live drop-zone count, backlog and in-flight work per stage, throughput, ETA, copy/similar counts, errors. |
| N5 | Docker Compose, open-source components only, ports bound to `127.0.0.1`. |

**Out of scope for v1:** the AI scoring itself (§9 only defines the hook), deleting anything other than confirmed byte-identical duplicates (§6.5), a review UI, video near-dedupe (videos get exact dedupe by SHA-256 only), detecting crops beyond about 5%, and multi-host shared storage.

## 3. Principles

1. **Temporal owns progress; Postgres owns facts.** Workflow history records which steps finished. The `pixelmagic` database records what is known about files and contents. Every activity is idempotent, keyed by file or SHA-256, so Temporal re-running a step is always safe.
2. **When in doubt, similar.** The only harmful classification error is a false *copy*. Every ambiguous case resolves to *similar* (everything kept) or *distinct*.
3. **Derived things are not stored.** Representatives, Live Photo pairs and staging paths are computed when staging is built. Staging can be deleted and rebuilt at any time.
4. **One filesystem.** `dropzone/`, `work/`, `vault/` and `staging/` live under one `DATA_DIR` on one device, so `rename` and `link` are atomic and hardlinks cost no space. Workers check this at startup and refuse to run otherwise.

## 4. Architecture

### 4.1 Containers

| Container | Role |
|-----------|------|
| `postgres` (16) | Databases `pixelmagic` (app) and `temporal` (+ `temporal_visibility`). Named volume, defaults for `fsync` and `synchronous_commit`. |
| `temporal` | Temporal server with Postgres persistence and SQL visibility. The schema is created by a one-shot admin-tools container (`init.sql` creates the databases), and a second one-shot (`temporal-ns`) registers the `default` namespace once the server is healthy. |
| `temporal-ui` | Workflow history UI at `127.0.0.1:8080`. |
| `migrate` | One-shot `node-pg-migrate up` (plain SQL migrations) for `pixelmagic`. |
| `worker` | TypeScript on Node 24 LTS + Temporal TypeScript SDK. Serves the workflow queue `main` plus `io`, `cpu` and `cluster`. Bind-mounts `DATA_DIR` at `/data`. |
| `worker-extra` | The same image with `WORKER_QUEUES=main,io,cpu` (no `cluster`), so its in-memory index never flips between replicas. Scale with `docker compose up --scale worker-extra=N`. |
| `grafana` | Dashboard over a Postgres datasource at `127.0.0.1:3000`. |
| harvester (native, not a container) | A Node 24 process on the Mac under launchd (`make harvest-install`), outside compose because only the host sees `/Volumes`. Runs the Temporal worker for queue `harvest`, the local page at `127.0.0.1:3020` and the host heartbeat. Copies photos from drives into `DATA_DIR/dropzone/harvest/`; see the harvest spec (`2026-10-19-harvest-design.md`). |

### 4.2 Task queues

| Queue | Activities | Concurrency per worker |
|-------|-----------|------------------------|
| `io` | `scan_and_start`, `ingest_file`, `reclaim_copy`, `sweep_copies`, `record_hash`, `mark_failed`, `maybe_build_staging` | `IO_CONCURRENCY` = 8 |
| `cpu` | `decode_content` | `CPU_CONCURRENCY` = number of CPU cores |
| `cluster` | `cluster_content` | **1**, plus a Postgres advisory lock held for the whole activity, so only one runs system-wide even with several workers |
| `harvest` | `HarvestWorkflow` and its activities (detect, scan, send, link), served only by the native harvester, never by the worker containers (one active run at a time). |
| `ai` (future) | `score_content` | Native macOS worker |

### 4.3 Workflows

- **`ScanWorkflow`** runs on a Temporal Schedule every `SCAN_INTERVAL_S` = 30 s, with overlap policy *skip*. Its activity takes two snapshots of the drop zone `STABLE_SECONDS` = 5 s apart. A file is *stable* if its size and mtime are unchanged between them. The scan skips names matching `.part`, `.crdownload`, `.DS_Store`, `._*` and `~$*`, and empty folders. For each stable file it starts `FileWorkflow` with:
  - workflow ID `file-<sha1(rel_path|size|mtime_ns)>`
  - ID-reuse policy *allow duplicate*, conflict policy *fail*: a start while the file's workflow is still running is ignored, while a re-drop after it has finished starts a new run.
  - **Backpressure:** the scan starts no new workflows while running `FileWorkflow`s ≥ `MAX_IN_FLIGHT` = 5000 (checked with a visibility count query).
  - It always writes `scan_stats` (pending files and bytes in the drop zone).
  - **Empty folders:** after starting workflows it removes drop-zone folders (never the drop zone itself) that hold nothing but Finder litter (`.DS_Store`, AppleDouble `._*`) and were unchanged for `EMPTY_DIR_GRACE_S` = 60 s, deepest first. Only litter files are unlinked; any other entry, including an ignored partial download, a link or a subfolder, keeps the folder, and `rmdir` refuses a folder that filled meanwhile.
- **`FileWorkflow(rel_path, size, mtime_ns)`** runs `ingest_file`. If the result is `COPY`, it runs `reclaim_copy` (io, §6.5) and ends, whatever that returns. If the result is new still-image content, it then runs `decode_content` (cpu) → `record_hash` (io) → `cluster_content` (cluster). If the content is quarantined, the workflow ends there. If decode attempts run out (§7), it runs `mark_failed` and ends. **`ContentWorkflow(sha256)`** runs the same content steps and is used by `retry-failed`.
- **`StagingWorkflow`** runs on a Schedule every 5 min, or on demand via `make stage`. It runs `build_staging` when the data has changed since the last build **and** either no `FileWorkflow` is running or ≥ `STAGE_MAX_STALENESS_S` = 900 s have passed since the last build. `make stage FORCE=1` always rebuilds.
- **`CopySweepWorkflow`** runs on a Schedule (`pm-copies`) every 15 min, overlap *skip*, or on demand via `make sweep-copies`. It calls `sweep_copies` in batches of 200 COPY rows, at most 50 per run, resuming at a persisted cursor (§6.5).

## 5. Data model (`pixelmagic`)

```
files(id bigserial PK, rel_path text, original_name text, size_bytes bigint, mtime_ns bigint,
      ext text NULL, sha256 char(64) NULL, state text, workflow_run_id text UNIQUE NULL,
      attempts int default 0, last_error text NULL, discovered_at, updated_at,
      copy_removed_at timestamptz NULL, copy_reclaimed_bytes bigint NULL,
      copy_mismatch bool default false)                                        -- 0004, §6.5
  state ∈ DISCOVERED | CLAIMED | INGESTED | COPY | QUARANTINED
  CHECK copy_removed_at only on COPY rows, never together with copy_mismatch
  UNIQUE (rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED')   -- partial: a re-drop gets a new row

contents(sha256 char(64) PK, ext text, kind text, mime text, size_bytes bigint,
         width int, height int, phash bigint, dhash bigint,
         captured_at timestamptz NULL, subsec text NULL, camera text NULL, burst_uuid text NULL,
         live_content_id text NULL, duration_s real NULL,
         state text, quarantine_reason text NULL, copy_group_id bigint NULL,
         copy_ambiguous bool default false, last_error text NULL,
         ingested_at, hashed_at, clustered_at, updated_at)
  kind  ∈ still | motion | other
  state ∈ INGESTED | HASHED | CLUSTERED | QUARANTINED | FAILED (motion/other stop at INGESTED)

groups(id bigserial PK, kind text, merged_into bigint NULL, similar_set_id bigint NULL)
  kind ∈ copy | similar;  a copy group's similar_set_id points at a similar group (or NULL)

cluster_state(id int PK = 1, version bigint)            -- bumped by every cluster transaction
scan_stats(id int PK = 1, pending_files bigint, pending_bytes bigint, scanned_at)
staging_builds(gen bigint PK, watermark timestamptz, cluster_version bigint, built_at, entries bigint)
copy_sweep_state(id int PK = 1, after_id bigint)        -- where the duplicate sweep resumes (0004)
```

- `phash` and `dhash` are stored as signed int64; `toSigned` / `toUnsigned` (`BigInt.asIntN` / `asUintN`) convert. In TypeScript, hashes are always `bigint`.
- Indexes: `files(state)`, `contents(state)`, `contents(live_content_id)`, `contents(copy_group_id)`.
- Every path is stored **relative to `DATA_DIR`**, so a native macOS worker with a different mount point sees the same rows.
- `updated_at` triggers use `clock_timestamp()`, not `now()`, so a long transaction can't commit behind a recorded staging watermark.

## 6. Pipeline steps

### 6.1 `ingest_file` (queue `io`, heartbeats)

Each step can be retried, and the state left by a crash at any point is handled:

1. Upsert the `files` row by `(rel_path, size, mtime_ns)` among non-final rows, giving `file_id`.
2. **Claim:** `rename(dropzone/rel_path, work/claimed/<file_id>)`.
   - If the source is missing but the claimed file exists, continue.
   - If both are missing and `files.sha256` is set, the file is never marked vanished: if `vault/.copies/<file_id>.<ext>` exists, commit `COPY`; else if the vault file exists, commit as its owner (step 6); otherwise fail the attempt as retryable (an anomaly is retried, never hidden). This path sees only non-final rows; `.copies` files are removed (§6.5) only after `COPY` is committed, so a parked copy is never gone here.
   - If both are missing and there is no hash, mark `QUARANTINED(vanished)`.
3. Stream SHA-256 in 1 MiB chunks, heartbeating. **Commit** `state=CLAIMED, sha256`.
4. Detect the type by content using exiftool `FileType` and `MIMEType`, never by extension. This `MIMEType` is the only source of `contents.mime`. This handles `wide`-atom MOVs and the JPEG named `IMG_E8719.HEIC`. Read metadata through `exiftool-vendored` (one long-lived `-stay_open` exiftool per worker, raw tags with `-G -n`):
   - `DateTimeOriginal`, `SubSecTimeOriginal`, `Make`/`Model`, Apple `BurstUUID` and `ContentIdentifier` (stills)
   - QuickTime `ContentIdentifier` and `Duration` (MOVs)
5. **Vault:** `link(work/claimed/<file_id>, vault/<ab>/<cd>/<sha256>.<ext>)`.
   - `ext` comes from the detected type, lowercased.
   - If the link fails with EEXIST and the existing file is the same inode (`samefile`), a previous attempt linked it.
   - If EEXIST with a different inode, the file is a byte-identical copy: `rename` it to `vault/.copies/<file_id>.<ext>`.
6. **Commit** `files.state = COPY` exactly when step 5 hit EEXIST with a different inode; otherwise (this file owns the vault inode) `INSERT contents … ON CONFLICT DO NOTHING` and `files.state = INGESTED`. Every state update is guarded by the allowed predecessor states, so a stale attempt can never roll a final row back. Then `unlink(work/claimed/<file_id>)` if it still exists.
7. `chmod 0444` the vault file. Staging hardlinks share the inode, so an editor saving in place cannot change the original.

Returns `IngestResult(file_id, sha256, is_new, kind)`.

**Crash states after `COPY` is committed** (with §6.5). A replay of a final row returns the stored outcome; for `COPY` it re-runs the tail of step 6 only if `.copies/<file_id>.<ext>` still exists. Each of these is normal:
- `COPY`, `.copies` file present, not marked removed: removal is still to come (FileWorkflow or the sweep).
- `COPY`, `.copies` file gone, `copy_removed_at` set: removed.
- `COPY`, `.copies` file gone, **not** marked removed: a crash between removal's unlink and its mark. `reclaim_copy` re-hashes the vault file; if it equals `files.sha256` the row is marked removed. If the vault file is missing or differs, that is an anomaly: the row is flagged `copy_mismatch` (reported by `make doctor` and Grafana), never hidden and never retried.

### 6.2 `decode_content` (queue `cpu`) and `record_hash` (queue `io`)

- `record_hash` skips if `state ≥ HASHED`. `decode_content` has no database access (§12 R4), so it always decodes.
- Decode in a child process (`workerpool`, process workers) with a timeout of `DECODE_TIMEOUT_S` = 60 s. A hang or segfault kills only the child:
  - **sharp** (libvips) with `limitInputPixels: 200_000_000` (larger is a decompression bomb, a permanent error), `failOn: 'truncated'`, and `.rotate()` to apply EXIF orientation. JPEG shrink-on-load makes the 32×32 resize fast.
  - **HEIC/HEIF** is decoded with `heic-decode` (libheif compiled to WebAssembly), because prebuilt sharp has no HEVC decoder. The raw RGBA is then handed to sharp. One code path works on the Mac and in Docker; Task 4 benchmarks it.
  - **RAW** uses the embedded preview, extracted with exiftool.
  - **pHash** (our own implementation, the standard algorithm): 32×32 grayscale → 2-D DCT-II → top-left 8×8 → bit = coefficient > median of those 64. **dHash:** 9×8 grayscale → bit = pixel[x+1] > pixel[x]. Both are 64-bit, row-major, first bit = most significant.
- Width and height are those of the **original**, not the draft.
- A broken pool (segfault) is recreated, and the activity fails as retryable (§7).
- Formats: JPEG, PNG, HEIC/HEIF, WebP, TIFF, GIF, BMP, RAW (embedded preview).

### 6.3 `cluster_content` (queue `cluster`, single-active)

**In-memory index.** The worker keeps typed arrays: `phash` and `dhash` as hi/lo `Uint32Array` pairs, plus `captured_at_ns`, `subsec`, `camera`, `burst_uuid`, `width`, `height`, `mime` and `copy_group_id`, plus the grouping state. It reloads them all whenever `cluster_state.version` differs from the version it cached (§12 R2).

**Matching.** A new hash is compared against **every** stored hash with XOR + 32-bit popcount over the typed arrays. Each candidate within `LOOK_ALIKE_PHASH` = 8 **and** `LOOK_ALIKE_DHASH` = 10 is classified by `classify(a, b) -> "copy" | "similar"`:

Rows are evaluated top to bottom; the first match wins.

| Condition | Result |
|-----------|--------|
| Both have `burst_uuid` and they are equal | similar |
| Both have `captured_at` and they differ (at whole-second precision) | similar |
| Both have `subsec` and they differ | similar |
| Both have `camera` and they differ | similar |
| Same `captured_at`, and both `subsec` present and equal | **copy** |
| Same `captured_at` with `subsec` missing on either side, pHash ≤ `COPY_PHASH` and dHash ≤ `COPY_DHASH`, and (different dimensions **or** different mime) | **copy** |
| Exactly one side has `captured_at`, pHash ≤ `COPY_PHASH` = 4 and dHash ≤ `COPY_DHASH` = 6 | **copy** |
| Neither has `captured_at`, pHash ≤ 4, dHash ≤ 6, and (different dimensions **or** different mime) | **copy** |
| Anything else | similar |

**Grouping (union-find, capture-consistent).**
- Every still content gets a copy group: a new singleton, or the union of the groups of all its *copy* matches.
- **A union is refused, and downgraded to *similar*, if the merged copy group would contain two different known `(captured_at, subsec)` values, two members of one burst, or two different non-null `camera` values.** This stops an EXIF-less export from bridging two burst frames (or two cameras' look-alikes) into one copy group.
- *Similar* matches union the copy groups' similar sets.
- On a merge, the smaller group gets `merged_into` the larger (ties broken by lower id), and its members are re-pointed.
- A content whose copy union was refused is flagged `copy_ambiguous` and is never chosen as a best copy.
- In one transaction: commit `state=CLUSTERED`, increment `cluster_state.version`, and write the group changes. Update the in-memory index only **after** the commit.

**Order independence.** Every new hash is compared with all members. As a result, the **similar-set partition** (the connected components of the look-alike graph) never depends on arrival order. The **copy-group partition** doesn't either, except for contents flagged `copy_ambiguous` in either run: which conflicting group such a content joins, and which key-less copies get flagged, depend on order (§12 R3).

### 6.4 `build_staging` (queue `io`, heartbeats)

Every staged link is a hardlink of a **vault** file (`vault/<ab>/<cd>/<sha256>.<ext>`). Byte-identical COPY files are never staged, removed or not (user choice (a), 2026-10-18); they only name contents (the lowest `(rel_path, file_id)` among all of a content's files) and pair Live Photos, and they are counted on the dashboard.

Takes a Postgres advisory lock. Computes the whole tree from the database into `staging/.gen-<n+1>/` using hardlinks from the vault. Then it:
1. creates the symlink `staging/.current.tmp → .gen-<n+1>`
2. renames it over `staging/current` (atomic)
3. inserts a `staging_builds` row
4. deletes generations older than `n`

A crash mid-build leaves an orphaned `.gen-*` directory, which is deleted at the start of the next build. Users browse `DATA_DIR/staging/current/`.

**Best copy** in a copy group, highest first:
0. not `copy_ambiguous`
1. pixels
2. has `captured_at`
3. has a paired Live Photo clip
4. format rank: RAW > TIFF > PNG > HEIC > JPEG > WebP > GIF > BMP
5. size in bytes
6. earliest `captured_at`
7. lowest `sha256` (deterministic tie-break)

Among the files that share the best content, the one with the lowest `(rel_path, file_id)` gives the name (`file_id` follows arrival order, so it is only the last tie-break).

**Layout.** Date = `captured_at`, else the file's `mtime`, formatted in UTC.
```
current/
  unique/YYYY/YYYY-MM/<YYYYMMDD_HHMMSS>_<original_name>      best copy of every copy group with no similar set
  similar/YYYY/YYYY-MM/s-<sha8>/<YYYYMMDD_HHMMSS>_<name>     best copy of every copy group in the set (all frames kept); sha8 of the set's best copy
  copies/g-<sha8>/<name>                                     every other content of the group (different files of the same shot); sha8 of the group's best copy
  quarantine/<reason>/<name>                                 includes quarantine/failed/ for FAILED contents
  other/<rel_dir>/<name>                                     videos, AAE sidecars, unknown types (canonical file)
```
- **Name collisions** within a folder add `_<sha8>` before the extension, then `_2`, `_3`, … in order of `(sha256, rel_path)`. A Live Photo MOV takes its still's final base name.
- **Live Photo pairing** happens here as a query:
  - Match a `motion` content to a `still` by `live_content_id`.
  - Fallback: some provenance file of each shares the source directory and the stem (case-insensitive, full stem, so `IMG_2834 1` ≠ `IMG_2834`), and `duration_s ≤ LIVE_MAX_DURATION_S` = 5.
  - A paired MOV goes beside **every** staged placement of its still's content. Unpaired MOVs go to `other/`.

### 6.5 Removing confirmed duplicates: `reclaim_copy` and `sweep_copies` (queue `io`, heartbeats)

User decision 2026-10-18: the vault holds each distinct file once, and re-dropping the same folders stores nothing new. Ingest is unchanged up to `COPY` (§6.1). Then, for a `COPY` row whose `vault/.copies/<file_id>.<ext>` exists:

1. Skip (return the outcome, change nothing) if the row is not `COPY`, is already marked removed, or is flagged `copy_mismatch` (never retried).
2. If the `.copies` file has another name (`nlink > 1`, e.g. a staging generation built before this change linked it), **defer**: unlinking would free nothing. A later sweep retries once that generation is gone.
3. Re-hash the `.copies` file **and** the vault file `vault/<ab>/<cd>/<sha256>.<ext>` (1 MiB chunks, heartbeating), and check both are unchanged since (size, mtime; not the inode number, which virtiofs may renumber) right before the unlink; a change fails the attempt as retryable.
4. Only if both hashes equal `files.sha256`: `unlink` the `.copies` file, fsync `vault/.copies`, then mark the row: `copy_removed_at`, `copy_reclaimed_bytes`.
5. On any mismatch, a missing vault file, or something other than a regular file: delete nothing, keep both files, set `copy_mismatch` with the observed hashes in `last_error`. Shown by `make doctor` and the Grafana "Copy mismatches" tile.

The order is crash-safe: `COPY` is committed before any unlink, and the mark follows it (§6.1, crash states). The step runs in **FileWorkflow** right after a `COPY` outcome, and in **`sweep_copies`** (`CopySweepWorkflow`), which takes the next 200 `COPY` rows not removed and not flagged after `copy_sweep_state.after_id`, in id order, and wraps to 0 at the end. The sweep covers crash leftovers, deferred copies and the one-off pass over data ingested before this change.

## 7. Errors and retries

| Class | Raised as | Behaviour |
|-------|-----------|-----------|
| Transient (DB or Temporal unavailable, `EIO`, `ENOSPC`, worker death) | Any ordinary exception | Retry policy: initial 1 s, backoff ×2, max interval 5 min, **unlimited attempts**. Disk-full stalls the work; it never fails it. |
| Permanent (undecodable, truncated, decompression bomb, unsupported) | Returned as an outcome, not raised (§12 R4) | `decode_content` returns the reason, and `record_hash` records `QUARANTINED(reason)`. The workflow ends normally. |
| Poison (decode timeout, broken pool) | `ApplicationError(type="Poison")`, retryable | Only `decode_content` (no database access) has `maximum_attempts = POISON_ATTEMPTS` = 5. After that, the workflow runs `mark_failed(sha256, error)` and sets `contents.state=FAILED`. `make retry-failed` resets those rows and starts a `ContentWorkflow` for each. |

Activity timeouts:
- `ingest_file`: start-to-close 1 h, heartbeat 30 s
- `decode_content`: start-to-close 2 min; `record_hash`: 1 min
- `cluster_content`, `mark_failed`: start-to-close 1 min
- `scan_and_start`: start-to-close 10 min, heartbeat 30 s
- `build_staging`: start-to-close 2 h, heartbeat 30 s
- `reclaim_copy`, `sweep_copies`: start-to-close 1 h, heartbeat 30 s (hashing a large video twice)

Workers handle `SIGTERM` with a graceful shutdown timeout of 60 s. `make verify-vault [SAMPLE=1000]` re-hashes vault files and reports mismatches. It skips `vault/.copies/` (named by file id, not hash) and reports how many files are still there and how many duplicates were removed (§6.5). It never repairs anything.

## 8. Observability

- **Temporal UI:** per-file history, retries and stack traces.
- **Grafana "Pipeline Live"** (provisioned JSON, 5 s refresh, SQL over `pixelmagic` and `temporal_visibility`; the home page, viewable without a login, Grafana bound to 127.0.0.1). Tiles:
  - drop zone pending files and bytes (`scan_stats`)
  - running `FileWorkflow`s
  - backlog per stage (`contents` by state)
  - throughput per stage per minute (from `ingested_at`, `hashed_at`, `clustered_at`)
  - ETA (backlog ÷ 10-minute rate)
  - vault: distinct files stored and their total bytes
  - copy groups with two or more contents; similar groups (sets of two or more copy groups)
  - duplicates removed (§6.5): the count, bytes reclaimed, and bytes of COPY files still pending removal
  - copy mismatches (duplicates kept because a re-hash disagreed or a file was missing): should read 0
  - quarantined by reason, FAILED count and the last 20 `last_error` values
  - age of the last staging build
- **Alert (optional):** "backlog > 0 and no stage throughput for 10 min" → an ntfy webhook.
- **Logs:** `pino` JSON to stdout, including `workflow_id`, `file_id` and `sha256`.

## 9. Future AI hook (design only)

- `ScoreWorkflow` runs on a schedule. Its backlog is `similar`-set members with no row in a future `scores(sha256, model_version, …)` table for the current `model_version`.
- It runs `score_content` on queue `ai` (a native macOS worker, heartbeating, about 30 s each). Results are idempotent, keyed by `(sha256, model_version)`.
- A new model version creates a new backlog. Existing workflows never change.

## 10. Testing

1. **Unit:**
   - `classify` (a table of cases, including every row in §6.3)
   - capture-consistent union-find and order independence (a property test over shuffled inputs)
   - vault paths, signed/unsigned conversion, the best-copy key, staging naming and collisions, Live Photo stem matching
   - exiftool parsing on real fixture files
2. **Workflow:** Temporal's time-skipping test environment with activities faked to fail at each boundary. Each workflow completes with each effect applied exactly once. Covers quarantine, poison → FAILED, and backpressure.
3. **Corpus end-to-end:**
   - The corpus is `test-automation/sample-images/`: read-only, git-ignored, never seen by any hosted service.
   - `tools/corpus` builds `tests/corpus/manifest.json` (git-ignored: it holds personal capture metadata) from an independent pass: full SHA-256, `sips` dimensions, exiftool capture fields, and relations (exact groups, HEIC/JPG twins, `IMG_E`/`IMG_O`, `"X 1"` copies, bursts, Live Photo pairs).
   - `tools/corpus/seed.ts` clones a subset with `cp -c` into `data-test/<run_id>/dropzone`, under Compose project `pmtest_<run_id>`.
   - Tiers: `smoke` (~150 files, ≤ 5 min, before every commit), `standard` (~2k files plus synthetic variants, at milestones), `full` (on demand).
   - Pinned files: `IMG_E8719.HEIC`, `IMG_0004.MOV`, `PSWU5412.MP4` + `"PSWU5412 1.MP4"`, `IMG_0034.HEIC/.JPG` (same name, different photos — kept apart), `"IMG_6941 1.HEIC"` + `IMG_E6941.JPG` (HEIC/JPG copy pair), `IMG_0433` + `IMG_E0433`, `IMG_2834.HEIC` + `"IMG_2834 1.HEIC"` with its MOV (`"IMG_2834 1.MOV"`; there is no `IMG_2834.MOV`), `VERP1022.TIF`, `AUXA4949.BMP`, ≥ 20 Live Photo pairs, ≥ 5 burst sets.
   - A **golden staging snapshot** (`tests/e2e/golden/smoke_tree.txt`) is regenerated only by `make update-golden`.
   - `make corpus-verify` runs before and after every run.
4. **Chaos (the resilience proof):**
   - During a `standard` run: `kill -9` random workers, restart `temporal` and `postgres`, and once `docker compose kill` the whole stack, then `up`.
   - The final `files`/`contents`/`groups` partition and the staging tree must equal the run with no chaos.
   - The **golden invariant** must hold:
     - every seeded file has exactly one final row, in a final state; any FAILED content has a `last_error`
     - `sha256(vault file) == name` for every vault file
     - every distinct seeded SHA-256 is in the vault exactly once, and after idle `.copies` holds no confirmed duplicate (every COPY row is marked removed, none flagged)
     - staging holds each non-motion content once (no byte-identical COPY is staged)
     - `work/claimed/` is empty
     - no copy group contains two different known capture times
   - "Equal" means: the similar-set partition and the copy-group partition are identical, and so is the staging tree snapshot, except for contents flagged `copy_ambiguous` in **either** run (and their staged entries), which both comparisons leave out.

## 11. Settled defaults (from the old open questions)

| Question | Decision |
|----------|----------|
| What "sorting" means | Filed by capture date, falling back to file mtime (§6.4) |
| Duplicate thresholds | §6.3, all configurable via env |
| Formats | §6.2. Videos and other files are exact-deduped only, then go to `other/` (MOVs paired with a still go beside the still) |
| Where it runs | Develop on the Mac in Docker Desktop. AI workers run natively on macOS. Stays portable to Linux |
| One volume | Yes. The worker refuses to start if `DATA_DIR` subfolders span devices |
| Empty the drop zone | Yes, files are moved into the vault. Nothing is ever deleted, except confirmed byte-identical duplicates (§6.5, R9) |

## 12. Revisions during planning

These changes were made while writing the implementation plan. The sections above already include them.

- **R1 Ingest idempotency key.** `files.workflow_run_id` (unique) identifies the row. If an ingest is re-run after its commit (a crash before Temporal recorded completion), it returns the stored outcome instead of reporting the file as vanished. `files.ext` is stored at claim time.
- **R2 Cluster cache.** `contents.cluster_seq` is replaced by a single-row `cluster_state.version`, so a cached index is never used after another worker has changed the groups. Only the `worker` service serves `cluster`; `worker-extra` replicas don't.
- **R3 Order independence, stated precisely.** Refusing a capture-conflicting union means an EXIF-less export that bridges two burst frames can join either frame's group, depending on arrival order. Such contents are flagged `copy_ambiguous`, are never a best copy, and are excluded when partitions are compared. Similar sets are always order-independent. Two frames of one second without `subsec` (older cameras) look like a re-encoding pair, so §6.3 row 6 also needs the tight copy distances, and a union that would mix two different cameras is refused like a capture conflict; a refusal flags only contents with neither a capture key nor a burst. With chains of key-less copies touching several bridges, *which* key-less contents end up flagged can depend on order. Keyed photos are never flagged. Partition and tree comparisons exclude contents flagged in **either** run.
- **R4 Decode and record are split.** `decode_content` (cpu, no database, 5 attempts) is separate from `record_hash` (io, unlimited retries). A long database outage therefore never marks content FAILED. FAILED moved from `files.state` to `contents.state`. Quarantine is returned as an activity outcome rather than raised.
- **R5 Deterministic staging names.** Group folders are named after their best copy's SHA-256 (`g-<sha8>`, `s-<sha8>`), not database ids, so a calm run and a chaos run produce identical trees.
- **R6 FAILED contents** are staged under `quarantine/failed/`.
- **R7 Language: TypeScript.** The pipeline is TypeScript on Node 24 LTS (ESM, strict). The future AI scorer may be any language Temporal supports (likely native Python or Swift for Metal/ANE). It only shares the `ai` task-queue name and JSON payloads with the pipeline.
- **R8 Preflight rulings.** Applied after a conflict scan of the plan:
  - §6.4: the staged name comes from the lowest `(rel_path, file_id)`, not the lowest `file_id`, so trees don't depend on arrival order. Dates in staged names are UTC.
  - §10.3: the pinned set lists only files that exist (`"IMG_2834 1.MOV"`; no `IMG_2834.MOV`). The seeder is `tools/corpus/seed.ts`. The manifest is git-ignored.
  - §6.2: only `record_hash` skips on `state ≥ HASHED`; `decode_content` always decodes (follows R4).
  - §6.1: a hashed file whose source and claimed copies are gone is never marked vanished (`.copies` → COPY, vault → owner, else retry). `COPY` means exactly "EEXIST with a different inode", and only the vault owner inserts `contents`. State updates are guarded by predecessor states. `contents.mime` comes only from exiftool.
  - §5: `staging_builds.cluster_version` stores the version next to the watermark; `updated_at` uses `clock_timestamp()`.
  - §7: `mark_failed` and `scan_and_start` get timeouts; `verify-vault` skips `.copies`. §8: "reclaimable" is an approximation. §4.1: a one-shot registers the `default` namespace.
- **R9 Remove confirmed duplicates (user decision, 2026-10-18).** A `COPY` file's `.copies` twin is unlinked once a fresh re-hash of it and of its vault file both equal `files.sha256` (§6.5; migration 0004). N3's unlink rule now reads: a file holding dropped bytes may be unlinked only (a) after another durable link exists, or (b) after it is confirmed byte-identical to its vault file by a fresh re-hash of both. Staging shows only different files of the same shot under `copies/` (choice (a)); `copies/v-` is gone. Rulings where the brief was silent, each the option that deletes nothing: a `.copies` file with another link is deferred, not unlinked; a missing vault file, a non-regular file or a hash disagreement flags the row and deletes nothing; a file that changes during the re-hash is retried, not removed. The dashboard's "≈ Reclaimable" tile became "Duplicates removed" (count, bytes reclaimed, bytes pending) plus "Copy mismatches".
