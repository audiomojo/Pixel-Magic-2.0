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
| N3 | **Never destroys image bytes.** The vault is append-only. Deleting anything is always a human decision outside the pipeline. |
| N4 | **Observable:** live drop-zone count, backlog and in-flight work per stage, throughput, ETA, copy/similar counts, errors. |
| N5 | Docker Compose, open-source components only, ports bound to `127.0.0.1`. |

**Out of scope for v1:** the AI scoring itself (§9 only defines the hook), deleting anything, a review UI, video near-dedupe (videos get exact dedupe by SHA-256 only), detecting crops beyond about 5%, and multi-host shared storage.

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
| `temporal` | Temporal server with Postgres persistence and SQL visibility. The schema is created by a one-shot admin-tools container. |
| `temporal-ui` | Workflow history UI at `127.0.0.1:8080`. |
| `migrate` | One-shot `alembic upgrade head` for `pixelmagic`. |
| `worker` | Python 3.12 + Temporal Python SDK. Serves queues `io`, `cpu` and `cluster`, plus the workflows. Bind-mounts `DATA_DIR` at `/data`. Scale up by adding replicas. |
| `grafana` | Dashboard over a Postgres datasource at `127.0.0.1:3000`. |

### 4.2 Task queues

| Queue | Activities | Concurrency per worker |
|-------|-----------|------------------------|
| `io` | `scan_dropzone`, `ingest_file`, `build_staging` | `IO_CONCURRENCY` = 8 |
| `cpu` | `hash_content` | `CPU_CONCURRENCY` = number of CPU cores |
| `cluster` | `cluster_content` | **1**, plus a Postgres advisory lock held for the whole activity, so only one runs system-wide even with several workers |
| `ai` (future) | `score_content` | Native macOS worker |

### 4.3 Workflows

- **`ScanWorkflow`** runs on a Temporal Schedule every `SCAN_INTERVAL_S` = 30 s, with overlap policy *skip*. Its activity takes two snapshots of the drop zone `STABLE_SECONDS` = 5 s apart. A file is *stable* if its size and mtime are unchanged between them. The scan skips names matching `.part`, `.crdownload`, `.DS_Store`, `._*` and `~$*`, and empty folders. For each stable file it starts `FileWorkflow` with:
  - workflow ID `file-<sha1(rel_path|size|mtime_ns)>`
  - ID-reuse policy *allow duplicate*, conflict policy *fail*: a start while the file's workflow is still running is ignored, while a re-drop after it has finished starts a new run.
  - **Backpressure:** the scan starts no new workflows while running `FileWorkflow`s ≥ `MAX_IN_FLIGHT` = 5000 (checked with a visibility count query).
  - It always writes `scan_stats` (pending files and bytes in the drop zone).
- **`FileWorkflow(rel_path, size, mtime_ns)`** runs `ingest_file`. If the result is new still-image content, it then runs `hash_content` and then `cluster_content`. If either returns *quarantined*, the workflow ends there. If attempts run out (§7), it runs `mark_failed` and ends.
- **`StagingWorkflow`** runs on a Schedule every 5 min, or on demand via `make stage`. It runs `build_staging` when the data has changed since the last build **and** either no `FileWorkflow` is running or ≥ `STAGE_MAX_STALENESS_S` = 900 s have passed since the last build. `make stage FORCE=1` always rebuilds.

## 5. Data model (`pixelmagic`)

```
files(id bigserial PK, rel_path text, original_name text, size_bytes bigint, mtime_ns bigint,
      sha256 char(64) NULL, state text, attempts int default 0, last_error text NULL,
      discovered_at, updated_at)
  state ∈ DISCOVERED | CLAIMED | INGESTED | COPY | QUARANTINED | FAILED
  UNIQUE (rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED')   -- partial: a re-drop gets a new row

contents(sha256 char(64) PK, ext text, kind text, mime text, size_bytes bigint,
         width int, height int, phash bigint, dhash bigint,
         captured_at timestamptz NULL, subsec text NULL, camera text NULL, burst_uuid text NULL,
         live_content_id text NULL, duration_s real NULL,
         state text, quarantine_reason text NULL, copy_group_id bigint NULL, cluster_seq bigint NULL,
         ingested_at, hashed_at, clustered_at, updated_at)
  kind  ∈ still | motion | other
  state ∈ INGESTED | HASHED | CLUSTERED | QUARANTINED          (motion/other stop at INGESTED)

groups(id bigserial PK, kind text, merged_into bigint NULL, similar_set_id bigint NULL)
  kind ∈ copy | similar;  a copy group's similar_set_id points at a similar group (or NULL)

scan_stats(id int PK = 1, pending_files bigint, pending_bytes bigint, scanned_at)
staging_builds(gen bigint PK, watermark timestamptz, built_at, entries bigint)
```

- `phash` and `dhash` are stored as signed int64; `pm.hashing.to_signed` / `to_unsigned` convert.
- Indexes: `files(state)`, `contents(state)`, `contents(live_content_id)`, `contents(cluster_seq)`.
- Every path is stored **relative to `DATA_DIR`**, so a native macOS worker with a different mount point sees the same rows.

## 6. Pipeline steps

### 6.1 `ingest_file` (queue `io`, heartbeats)

Each step can be retried, and the state left by a crash at any point is handled:

1. Upsert the `files` row by `(rel_path, size, mtime_ns)` among non-final rows, giving `file_id`.
2. **Claim:** `rename(dropzone/rel_path, work/claimed/<file_id>)`.
   - If the source is missing but the claimed file exists, continue.
   - If both are missing and `files.sha256` is set, go to step 5 (the vault step already ran).
   - If both are missing and there is no hash, mark `QUARANTINED(vanished)`.
3. Stream SHA-256 in 1 MiB chunks, heartbeating. **Commit** `state=CLAIMED, sha256`.
4. Detect the type by content using exiftool `FileType` and `MIMEType`, never by extension. This handles `wide`-atom MOVs and the JPEG named `IMG_E8719.HEIC`. Read metadata through one `exiftool -stay_open` process per worker:
   - `DateTimeOriginal`, `SubSecTimeOriginal`, `Make`/`Model`, Apple `BurstUUID` and `ContentIdentifier` (stills)
   - QuickTime `ContentIdentifier` and `Duration` (MOVs)
5. **Vault:** `link(work/claimed/<file_id>, vault/<ab>/<cd>/<sha256>.<ext>)`.
   - `ext` comes from the detected type, lowercased.
   - If the link fails with EEXIST and the existing file is the same inode (`samefile`), a previous attempt linked it.
   - If EEXIST with a different inode, the file is a byte-identical copy: `rename` it to `vault/.copies/<file_id>.<ext>`.
6. **Commit** `INSERT contents … ON CONFLICT DO NOTHING` and `files.state = INGESTED` (new content) or `COPY` (content already known). Then `unlink(work/claimed/<file_id>)` if it still exists.
7. `chmod 0444` the vault file. Staging hardlinks share the inode, so an editor saving in place cannot change the original.

Returns `IngestResult(file_id, sha256, is_new, kind)`.

### 6.2 `hash_content` (queue `cpu`)

- Skip if `state ≥ HASHED`.
- Decode in a `ProcessPoolExecutor` child with a timeout of `DECODE_TIMEOUT_S` = 60 s:
  - register `pillow-heif`; RAW uses `rawpy.extract_thumb()`
  - `Image.MAX_IMAGE_PIXELS = 200_000_000` (larger is a decompression bomb, a permanent error)
  - JPEG `draft('L', (256, 256))`, then `ImageOps.exif_transpose`
  - `imagehash.phash` / `dhash`, `hash_size=8`
- Width and height are those of the **original**, not the draft.
- A broken pool (segfault) is recreated, and the activity fails as retryable (§7).
- Formats: JPEG, PNG, HEIC/HEIF, WebP, TIFF, GIF, BMP, RAW (embedded preview).

### 6.3 `cluster_content` (queue `cluster`, single-active)

**In-memory index.** The worker keeps NumPy arrays of `phash`, `dhash` (uint64), `captured_at_ns`, `subsec`, `camera`, `burst_uuid`, `width`, `height`, `mime` and `copy_group_id`. On each call it refreshes them from rows with `cluster_seq > last_seq`. On startup it loads all of them.

**Matching.** A new hash is compared against **every** stored hash with `np.bitwise_count(index ^ h)`. Each candidate within `LOOK_ALIKE_PHASH` = 8 **and** `LOOK_ALIKE_DHASH` = 10 is classified by `classify(a, b) -> "copy" | "similar"`:

Rows are evaluated top to bottom; the first match wins.

| Condition | Result |
|-----------|--------|
| Both have `burst_uuid` and they are equal | similar |
| Both have `captured_at` and they differ (at whole-second precision) | similar |
| Both have `subsec` and they differ | similar |
| Both have `camera` and they differ | similar |
| Same `captured_at`, and both `subsec` present and equal | **copy** |
| Same `captured_at` with `subsec` missing on either side, and (different dimensions **or** different mime) | **copy** |
| Exactly one side has `captured_at`, pHash ≤ `COPY_PHASH` = 4 and dHash ≤ `COPY_DHASH` = 6 | **copy** |
| Neither has `captured_at`, pHash ≤ 4, dHash ≤ 6, and (different dimensions **or** different mime) | **copy** |
| Anything else | similar |

**Grouping (union-find, capture-consistent).**
- Every still content gets a copy group: a new singleton, or the union of the groups of all its *copy* matches.
- **A union is refused, and downgraded to *similar*, if the merged copy group would contain two different known `(captured_at, subsec)` values.** This stops an EXIF-less export from bridging two burst frames into one copy group.
- *Similar* matches union the copy groups' similar sets.
- On a merge, the smaller group gets `merged_into` the larger (ties broken by lower id), and its members are re-pointed.
- In one transaction: commit `state=CLUSTERED`, `cluster_seq = nextval`, and the group changes. Update the in-memory index only **after** the commit.

**Order independence.** Because every new hash is compared with all members, the final partitions (copy groups and similar sets, compared as sets of SHA-256 sets) do not depend on arrival order.

### 6.4 `build_staging` (queue `io`, heartbeats)

Takes a Postgres advisory lock. Computes the whole tree from the database into `staging/.gen-<n+1>/` using hardlinks from the vault. Then it:
1. creates the symlink `staging/.current.tmp → .gen-<n+1>`
2. renames it over `staging/current` (atomic)
3. inserts a `staging_builds` row
4. deletes generations older than `n`

A crash mid-build leaves an orphaned `.gen-*` directory, which is deleted at the start of the next build. Users browse `DATA_DIR/staging/current/`.

**Best copy** in a copy group, highest first:
1. pixels
2. has `captured_at`
3. has a paired Live Photo clip
4. format rank: RAW > TIFF > PNG > HEIC > JPEG > WebP > GIF > BMP
5. size in bytes
6. earliest `captured_at`
7. lowest `sha256` (deterministic tie-break)

Among the files that share the best content, the one with the lowest `file_id` gives the name.

**Layout.** Date = `captured_at`, else the file's `mtime`.
```
current/
  unique/YYYY/YYYY-MM/<YYYYMMDD_HHMMSS>_<original_name>      best copy of every copy group with no similar set
  similar/YYYY/YYYY-MM/s-<set_id>/<YYYYMMDD_HHMMSS>_<name>   best copy of every copy group in the set (all frames kept)
  copies/g-<group_id>/<name>                                 every other file of the group (other contents + byte copies)
  copies/v-<sha8>/<name>                                     byte copies of non-still files
  quarantine/<reason>/<name>
  other/<rel_dir>/<name>                                     videos, AAE sidecars, unknown types (canonical file)
```
- **Name collisions** within a folder add `_<sha8>` before the extension. A Live Photo MOV takes its still's final base name.
- **Live Photo pairing** happens here as a query:
  - Match a `motion` content to a `still` by `live_content_id`.
  - Fallback: some provenance file of each shares the source directory and the stem (case-insensitive, full stem, so `IMG_2834 1` ≠ `IMG_2834`), and `duration_s ≤ LIVE_MAX_DURATION_S` = 5.
  - A paired MOV goes beside **every** staged placement of its still's content. Unpaired MOVs go to `other/`.

## 7. Errors and retries

| Class | Raised as | Behaviour |
|-------|-----------|-----------|
| Transient (DB or Temporal unavailable, `EIO`, `ENOSPC`, worker death) | Any ordinary exception | Retry policy: initial 1 s, backoff ×2, max interval 5 min, **unlimited attempts**. Disk-full stalls the work; it never fails it. |
| Permanent (`UnidentifiedImageError`, truncated, decompression bomb, unsupported) | `ApplicationError(non_retryable=True, type="Quarantine", details=[reason])` | The activity records `QUARANTINED(reason)` before raising. The workflow ends normally. |
| Poison (decode timeout, broken pool) | `ApplicationError(type="Poison")`, retryable | `hash_content` uses `maximum_attempts = POISON_ATTEMPTS` = 5. After that, the workflow runs `mark_failed(file_id, error)` and sets `files.state=FAILED`. `make retry-failed` starts new `FileWorkflow`s for FAILED rows. |

Activity timeouts:
- `ingest_file`: start-to-close 1 h, heartbeat 30 s
- `hash_content`: start-to-close 2 min
- `cluster_content`: start-to-close 1 min
- `build_staging`: start-to-close 2 h, heartbeat 30 s

Workers handle `SIGTERM` with a graceful shutdown timeout of 60 s. `make verify-vault [SAMPLE=1000]` re-hashes vault files and reports mismatches. It never repairs anything.

## 8. Observability

- **Temporal UI:** per-file history, retries and stack traces.
- **Grafana "Pipeline Live"** (provisioned JSON, 5 s refresh, SQL over `pixelmagic` and `temporal_visibility`). Tiles:
  - drop zone pending files and bytes (`scan_stats`)
  - running `FileWorkflow`s
  - backlog per stage (`contents` by state)
  - throughput per stage per minute (from `ingested_at`, `hashed_at`, `clustered_at`)
  - ETA (backlog ÷ 10-minute rate)
  - copy groups, similar sets, bytes reclaimable (Σ size of non-best copies)
  - quarantined by reason, FAILED count and the last 20 `last_error` values
  - age of the last staging build
- **Alert (optional):** "backlog > 0 and no stage throughput for 10 min" → an ntfy webhook.
- **Logs:** `structlog` JSON to stdout, including `workflow_id`, `file_id` and `sha256`.

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
   - `tools/corpus` builds `tests/corpus/manifest.json` from an independent pass: full SHA-256, `sips` dimensions, exiftool capture fields, and relations (exact groups, HEIC/JPG twins, `IMG_E`/`IMG_O`, `"X 1"` copies, bursts, Live Photo pairs).
   - `seed.py` clones a subset with `cp -c` into `data-test/<run_id>/dropzone`, under Compose project `pmtest_<run_id>`.
   - Tiers: `smoke` (~150 files, ≤ 5 min, before every commit), `standard` (~2k files plus synthetic variants, at milestones), `full` (on demand).
   - Pinned files: `IMG_E8719.HEIC`, `IMG_0004.MOV`, `PSWU5412.MP4` + `"PSWU5412 1.MP4"`, `IMG_0034.HEIC/.JPG`, `IMG_0433` + `IMG_E0433`, `IMG_2834.HEIC` + `"IMG_2834 1.HEIC"` with both MOVs, `VERP1022.TIF`, `AUXA4949.BMP`, ≥ 20 Live Photo pairs, ≥ 5 burst sets.
   - A **golden staging snapshot** (`tests/e2e/golden/smoke_tree.txt`) is regenerated only by `make update-golden`.
   - `make corpus-verify` runs before and after every run.
4. **Chaos (the resilience proof):**
   - During a `standard` run: `kill -9` random workers, restart `temporal` and `postgres`, and once `docker compose kill` the whole stack, then `up`.
   - The final `files`/`contents`/`groups` partition and the staging tree must equal the run with no chaos.
   - The **golden invariant** must hold:
     - every seeded file has exactly one final row, in a final state (or FAILED with an error)
     - `sha256(vault file) == name` for every vault file
     - the vault plus `.copies` hold every seeded byte
     - `work/claimed/` is empty
     - no copy group contains two different known capture times

## 11. Settled defaults (from the old open questions)

| Question | Decision |
|----------|----------|
| What "sorting" means | Filed by capture date, falling back to file mtime (§6.4) |
| Duplicate thresholds | §6.3, all configurable via env |
| Formats | §6.2. Videos and other files are exact-deduped only, then go to `other/` (MOVs paired with a still go beside the still) |
| Where it runs | Develop on the Mac in Docker Desktop. AI workers run natively on macOS. Stays portable to Linux |
| One volume | Yes. The worker refuses to start if `DATA_DIR` subfolders span devices |
| Empty the drop zone | Yes, files are moved into the vault. Nothing is ever deleted |
