# Pixel Magic 2.0 — Harvest: design

Date: 2026-10-19. Status: written for review.
Parent spec: `docs/superpowers/specs/2026-10-17-pixel-magic-design.md` (the pipeline). Background, not instructions: the `find-all-images` project (`/Users/rich/Data/source-code/find-all-images`, its spec and plan) and `docs/superpowers/plans/2026-10-19-harvest-project-plan.md`, whose measurements and lessons this design uses.

## 1. Purpose

Find the real photos on external drives and NAS shares and feed them into the drop zone, so the pipeline stores each distinct photo once. The user chooses what to harvest: nothing scans a source unless the user selects it, and a source harvested before is scanned again only when the user selects it again.

## 2. Requirements

| # | Requirement |
|---|---|
| H1 | **Sources are never modified.** No write, rename, chmod, utimes, flag change or delete ever targets a source path. Sources are opened read-only. |
| H2 | **User-triggered.** A harvest runs only when started from the page. There is no schedule. |
| H3 | **Selectable sources.** The page lists every detected drive and share (and known ones not mounted) with its history. The user selects whole sources or folders within them. Each selected folder keeps its own last-harvest date. |
| H4 | **Only new work on a re-run.** Re-harvesting a folder sends only files that are new or changed since they were last seen. Re-running a finished harvest sends 0 files. |
| H5 | **Known content is not copied.** A file whose bytes are already in the vault, or already sent in this or an earlier harvest, is not copied. Where it was found is still recorded. |
| H6 | **Paced.** The Mac's disk stays at or below `HARVEST_MAX_DISK_PCT` (80 %), and the drop-zone backlog at or below `HARVEST_MAX_PENDING_FILES` (20,000). |
| H7 | **Durable.** A reboot, a harvester crash, Docker down, or an unplugged drive never loses progress, never sends a file twice into the vault, and never marks a file failed for good. |
| H8 | **Possible photos are reviewed, not sent.** Files that might be photos wait on the page; the user approves or skips them, and approved ones are sent like photos. |
| H9 | **Provenance.** For any vault photo, every source path where its bytes were found is queryable, including copies that were never sent. |
| H10 | **The pipeline is unchanged.** Harvest is one more producer for the drop zone. Ingest, vault, clustering, staging and duplicate removal keep their semantics. The only pipeline schema change is an index (§5). |

### User decisions (2026-10-19)

- The control surface is a **local web page**, no login.
- Selection is **a drive or share, plus optional folders** inside it.
- Possible photos are **reviewed on the page**, with thumbnails.
- The harvester **starts at login** (launchd), installed and removed by make targets.
- NAS photos already copied by `find-all-images` are **re-harvested from the NAS** (true provenance). Its copies folder is not a source; the user may delete it after NAS-Backup is harvested.

### Settled defaults (overridable)

- Page at `http://127.0.0.1:3020` (`HARVEST_UI_PORT`).
- The disk limit is measured with `statfs` on `DATA_DIR`. On APFS that reports the whole container (the internal disk), which is what fills up.
- RAW formats the pipeline does not decode (DCR, SRW) are harvested and land in the vault as kind `other`: stored, never clustered.
- Videos and Live Photo movies are out of scope for now (§13).
- One run is active at a time. Further selections queue behind it.

## 3. Architecture

```
 Mac                                                      Docker (unchanged)
 ┌──────────── pm-harvester (launchd, Node 24) ────────────┐
 │ HTTP page + JSON API  127.0.0.1:3020                     │
 │ Temporal worker, task queue "harvest"                    │──▶ temporal 127.0.0.1:7233
 │   HarvestWorkflow; activities detect/scan/send/...       │──▶ postgres 127.0.0.1:5432
 └──────┬──────────────────────────────────────────────────┘
  reads │ only            writes .part → rename
 /Volumes/* ─────▶ DATA_DIR/dropzone/harvest/<label>/<rel path> ──▶ existing ScanWorkflow → vault
```

- **Why on the Mac.** Only the host sees `/Volumes` (drives mounted later, SMB shares) and `diskutil`. The parent spec already plans native macOS workers (N2, §4.2); every path in Postgres is relative, so a host process shares the same rows.
- **One process, `src/harvest/host.ts`** (`npm run harvester`): a Temporal worker on queue `harvest` plus the HTTP server. It reads `DATA_DIR` (the host path the containers bind-mount; `make fs-check` already requires one filesystem), `DATABASE_URL` and `TEMPORAL_ADDRESS` (both localhost) from `.env`.
- **launchd.** `make harvest-install` writes `~/Library/LaunchAgents/com.pixelmagic.harvester.plist` (KeepAlive, RunAtLoad, logs under `DATA_DIR/logs/harvester*.log`, rotated by size) and loads it. `make harvest-uninstall` unloads and removes it. `make harvester` runs it in the foreground for development. Nothing else on the system changes.
- **Containers:** unchanged. The worker containers never poll `harvest`.
- **Code layout:** `src/harvest/` — `classify.ts`, `exifMake.ts`, `sources.ts`, `paths.ts` (path keys, the guarded writer), `activities.ts`, `workflow.ts` (exported from `src/workflows.ts` so one bundle serves both), `throttle.ts`, `server.ts`, `thumbs.ts`, `host.ts`; static page in `src/harvest/ui/`.

## 4. Sources

### 4.1 Detection (`detectSources`)

Parse `/sbin/mount`. A mount is a candidate when its mount point is under `/Volumes/`.

- **Network** (`smbfs`, `afpfs`, `nfs`, `webdav`): label `NAS-<share>` — share name URL-decoded, `/` → `_`; an empty share, `.` or `..` is rejected.
- **Local:** label = volume name.
- **Excluded:** `/Volumes/Recovery`; `nobrowse` mounts; a device containing `@` (Time Machine snapshots); a mount point whose first component under `/Volumes` starts with `.` or `com.apple.`; disk images (`diskutil info -plist <device>` reports `BusProtocol` = `Disk Image`; 15 s timeout; on failure or a missing key, the volume is kept); any volume that contains `DATA_DIR`, compared by device+inode and by casefolded realpath.
- Read-only volumes (for example NTFS) are kept.
- Labels `harvest`, `.copies`, `_logs` and any label starting with `.` are reserved and rejected.

### 4.2 Identity

A source's identity never depends on how it was mounted or named.

- **Local:** `local:<VolumeUUID>` from `diskutil info -plist`. Without a UUID: `local-name:<volume name>`.
- **Network:** `net:<server lower-cased, without `._smb._tcp.local`>/<share casefolded>`; the user part (`GUEST:@`) is dropped.
- A remounted drive (`/Volumes/Expansion 1`) or share (`/Volumes/Plex-1`) maps to its existing row and keeps its label. Two different volumes that would share a label get ` (2)`, ` (3)` suffixes on the later one, stored once.

### 4.3 Browsing

`GET /api/sources/:id/tree?path=` lists one directory of the live source: sub-folder names only, sorted, with symlinks and pruned folders (§6.2) hidden. It uses `readdir` only, never writes, and times out after 10 s per call.

## 5. Data model (migrations `0005_harvest.sql`, `0006_harvest_fixes.sql`)

```sql
harvest_sources (id bigserial PK, identity text UNIQUE NOT NULL, label text UNIQUE NOT NULL,
  kind text CHECK (kind IN ('network','local')), last_mount_path text,
  first_seen_at timestamptz, last_seen_at timestamptz)

harvest_folders (id bigserial PK, source_id FK, rel_path text NOT NULL /* '' = whole source */,
  last_harvested_at timestamptz, last_run_id bigint, last_counts jsonb,
  UNIQUE (source_id, rel_path))

harvest_runs (id bigserial PK, workflow_id text UNIQUE, dry_run boolean,
  send_only boolean NOT NULL DEFAULT false /* review approvals: send PLANNED items, no scan */,
  state text CHECK (state IN ('queued','scanning','sending','paused','done','cancelled','failed')),
  paused_reason text, paused_since timestamptz, created_at, started_at, finished_at, error text)

harvest_run_folders (run_id FK, folder_id FK, position int /* selection order */, state text /* queued|scanning|sending|waiting_source|done */,
  dirs bigint, photos bigint, maybes bigint, junk jsonb /* reason → count */, errors bigint,
  sent bigint, known bigint, dups bigint, bytes_sent bigint, scan_cursor text,
  PRIMARY KEY (run_id, folder_id))

harvest_items (id bigserial PK, source_id FK, rel_path text NOT NULL, rel_path_raw bytea,
  path_key text NOT NULL /* NFC + casefold */, size_bytes bigint, mtime_ns bigint,
  kind text CHECK (kind IN ('photo','maybe')),
  state text CHECK (state IN ('PLANNED','SENT','KNOWN','DUP','REVIEW','REJECTED','FAILED','VANISHED')),
  sha256 char(64), head_hash char(64), dropzone_path text,
  dropzone_mtime_ns bigint /* the copy's on-disk mtime (utimes is not ns-exact) */, file_id bigint,
  duplicate_of bigint REFERENCES harvest_items, attempts int DEFAULT 0, last_error text,
  first_seen_run bigint, last_seen_run bigint, sent_at timestamptz,
  UNIQUE (source_id, path_key, size_bytes, mtime_ns))
-- indexes: (state, source_id), (size_bytes), (sha256), (dropzone_path) WHERE file_id IS NULL,
--   (lower(dropzone_path)) WHERE file_id IS NULL      -- name choice: is this name held by an unlinked item?
--   (sent_at) WHERE state = 'SENT' AND file_id IS NULL -- the throttle gate's recent-unlinked count

harvest_host_state (id int PK CHECK (id = 1), heartbeat_at timestamptz, version text)
  -- one row, the host heartbeat (every 30 s) that the Grafana tile reads

CREATE INDEX contents_size_idx ON contents (size_bytes);   -- the one pipeline-table change

-- 0006_harvest_fixes.sql (0005 was already applied to the live database)
harvest_run_items (run_id FK ON DELETE CASCADE, item_id FK ON DELETE CASCADE,
  PRIMARY KEY (run_id, item_id))       -- a send-only run's approved items: it sends only these
harvest_run_folders.root_ok boolean NOT NULL DEFAULT true
                                       -- false: the scan could not read the folder's root → no history
-- indexes: harvest_items (duplicate_of) WHERE duplicate_of IS NOT NULL   -- promoteDuplicates
--   harvest_items (source_id, rel_path COLLATE "C", id) WHERE state = 'PLANNED'
--                                      -- sendBatch's keyset page over one run folder (send order)
--   harvest_items (source_id, path_key COLLATE "C", id) WHERE state = 'PLANNED'
--                                      -- the folder's items under another case spelling
```

- `rel_path` is relative to the source root, stored NFC. `rel_path_raw` holds the original bytes when the name is not valid UTF-8. `path_key` is NFC + casefold: APFS is normalization-insensitive and SMB can deliver NFD/NFC twins.
- **Same file, small mtime shift.** FAT/exFAT shifts mtimes by exactly one hour across DST changes, and SMB rounds them. When a scanned file has no exact row but a row with the same `(source_id, path_key, size_bytes)` exists whose mtime differs by ≤ 2 s or by 3600 ± 2 s, it is that row (its `last_seen_run` is updated, its key is not).
- **States.**

| State | Meaning |
|---|---|
| `PLANNED` | A photo (or an approved maybe) waiting to be sent. |
| `SENT` | Copied into the drop zone; `dropzone_path` is relative to the drop zone, as `files.rel_path` is. |
| `KNOWN` | Its sha256 and size match a `contents` row: not copied. |
| `DUP` | Same sha256 as `duplicate_of`, an item that is SENT or PLANNED: not copied. |
| `REVIEW` | A possible photo waiting for the user. |
| `REJECTED` | A possible photo the user skipped. Approving it later sets PLANNED. |
| `FAILED` | Copy or read failed `HARVEST_MAX_ATTEMPTS` (5) times. A later scan that sees it again unchanged makes it PLANNED with its attempts reset. Transient NAS errors cost no attempt (§9). |
| `VANISHED` | Gone from the source when its turn came. Seen again on a later scan → PLANNED. |

- **Link to the pipeline.** A periodic query (every 60 s, and at the end of a run) sets `file_id` by joining `files` on `rel_path = dropzone_path`, `size_bytes` and `mtime_ns`.
- **Provenance** (H9): `harvest_items` by `sha256` (KNOWN and DUP rows carry it; SENT rows get it from `files.sha256` via `file_id` when it was not hashed) plus `files.rel_path`.
- **Folder counts on the page** come from `harvest_folders.last_counts`, written when a run finishes that folder — never for a folder whose root the scan could not read (`root_ok` false: not found, or unreadable).

## 6. Classification (port of `find-all-images`)

### 6.1 Candidates

Only regular files (`lstat`, `S_ISREG`); symlinks are never followed. Extensions are matched case-insensitively.

- `RAW_EXTS`: cr2 cr3 crw nef nrw arw srf sr2 dng raf orf rw2 pef srw 3fr iiq x3f erf mos mrw kdc dcr
- `TAGGED_EXTS` (may carry camera EXIF): jpg jpeg jpe jfif heic heif tif tiff
- `FOLDER_ONLY_EXTS`: bmp
- `MAYBE_EXTS`: png webp gif avif jp2 j2k jpf jpx
- Anything else is not a candidate and is ignored without a count (svg, ai, eps, psd, psb, ico, icns, xcf, video, …).

### 6.2 Pruned directories (never walked)

- Names (case-insensitive): `caches`, `node_modules`, `.git`, `__macosx`, `.spotlight-v100`, `.fseventsd`, `.trashes`, `.temporaryitems`, `appdata`, `windows`, `program files`, `program files (x86)`, `$recycle.bin`, `system volume information`.
- Suffixes: `.app`, `.framework`, `.bundle`, `.plugin`, `.lrdata`, `.sparsebundle`.
- Inside a Photos or iPhoto library (`*.photoslibrary`, `iPhoto Library*`), only the `originals` and `Masters` subtrees. Files directly in the library root are classified normally.

### 6.3 Rules, in order

| Step | Rule | Result |
|---|---|---|
| 1 | `.DS_Store`, `Thumbs.db` (checked by name before the extension test) | junk `system-file` |
| 2 | Candidate extension and name starts with `._` | junk `appledouble` |
| 3 | Size < `HARVEST_MIN_SIZE` (20480 bytes) | junk `too-small` |
| 4 | Name starts with `screenshot` or `screen shot` | junk `screenshot` |
| 5 | Stem is `folder`, `cover`, `front` or `back`, or starts with `albumart` | junk `album-art` |
| 6 | RAW | **photo** |
| 7 | TAGGED in a photo folder | **photo** (the file is not read) |
| 8 | TAGGED with a non-empty EXIF Make | **photo** |
| 9 | BMP in a photo folder | **photo** |
| 10 | Otherwise, inside a source-code project | junk `in-project` |
| 11 | Otherwise, inside a graphics folder | junk `graphics-folder` |
| 12 | Otherwise | **maybe** |

- **Photo folder:** a directory component, relative to the source root, that equals `photos`, `pictures`, `my pictures`, `dcim` or `camera roll`; starts with `camera`; or contains `iphone`. Also `originals`/`masters` directly inside a Photos/iPhoto library. Not `pics`, and not the source root's own name.
- **Source-code project:** a directory whose own entries include a marker, and everything beneath it; a directory named `Source Code` counts too. Markers: files `package.json`, `pom.xml`, `build.gradle`, `build.gradle.kts`, `pyproject.toml`, `setup.py`, `Cargo.toml`, `go.mod`, `Gemfile`, `composer.json`, `CMakeLists.txt`, `Makefile`, `.gitignore`, `.git` (file), `*.sln`, `*.csproj`, `*.vcxproj`; directories `.git`, `.svn`, `node_modules`, `venv`, `.venv`, `*.xcodeproj`, `*.xcworkspace`. Markers are checked before pruning.
- **Graphics folder:** a component that equals `music`, `audio`, `itunes` or `ai images`; contains `comfyui`, `stable diffusion` or `midjourney`; or ends with `_files` or `.band`.

### 6.4 EXIF Make (`exifMake.ts`)

A bounded, read-only TypeScript port of the `find-all-images` reader:

- Reads at most the first 256 KiB. JPEG: APP1 `Exif`. TIFF/DNG: IFD0, and an IFD0 beyond the buffer by one capped seek (≤ 64 KiB, plus ≤ 4 KiB for an out-of-chunk Make value). HEIC/HEIF: the `Exif` item via `meta`/`iinf`/`iloc` (≤ 64 KiB), with box sizes, extent counts and item counts bounds-checked.
- Returns the stripped Make tag (0x010F, IFD0), or `null` for anything malformed. It never throws for bad data; an `OSError`-class read failure is a scan error for that file.
- No exiftool fallback: one process call per file over 600k files is too slow, and `null` only moves a file to *maybe*, where the user sees it.

## 7. The harvest run

### 7.1 Workflow

`HarvestWorkflow({ runId })`, id `harvest-<runId>`, task queue `harvest`. Started by `POST /api/runs`. One run is active at a time: a new run waits (state `queued`) until the active one ends; the server starts the next.

1. **Scan** each selected folder in order: `scanFolder(runId, folderId)`.
2. **Send** (skipped for a dry run): `sendBatch(runId)` in a loop, ≤ 500 items per call, until no PLANNED item of the run's folders remains. Continue-as-new every 50 batches.
3. **Finish:** `finishRun(runId)` writes `harvest_folders.last_harvested_at`, `last_run_id`, `last_counts`, links `file_id`s, and sets the run `done`.

- **Signals:** `pause`, `resume`, `itemsApproved` (approved maybes join the active run's send loop). **Cancel** is a Temporal cancellation: the current activity stops at its next heartbeat and deletes its own `.part`; the run is `cancelled`; unsent items stay PLANNED for a later run.
- **Activities** are idempotent; each item transition is one `UPDATE … WHERE id = $1 AND state = <expected>` (parent principle: Temporal owns progress, Postgres owns facts).
- **Which items a run sends:** PLANNED items whose `rel_path` is under one of the run's folders (`path_key` prefix match), including items left PLANNED by an earlier cancelled or paused run. An item under nested selected folders counts for the deepest one. A send-only run sends only its own approved items (`harvest_run_items`).

### 7.2 `scanFolder`

- Walks the folder top-down, sorted by name, without following symlinks, applying §6. It heartbeats every 5 s with dirs visited and the current directory. On a retry it resumes after the last fully processed directory (`scan_cursor`, in walk order).
- Upserts each photo as PLANNED, each maybe as REVIEW (or leaves an existing row's state alone), and sets `last_seen_run`. Rows in `FAILED` under the attempt cap and `VANISHED` rows seen again become PLANNED.
- Records counts (photos, maybes, junk by reason, errors) on `harvest_run_folders`. Unreadable directories and files are counted and logged, and the walk continues.
- Error counters are resume-stable: each unreadable directory is counted once, so a resumed scan reports the same counts as an uninterrupted one.
- If the mount itself is gone (no mount, the mount root cannot be read, or its device id changed since the scan started), it returns `SOURCE_GONE` (§9) and leaves the policy (waiting) to the workflow. A selected folder missing on a live drive is one error ("folder not found") and the folder is done, with no folder history.
- If the folder's root, an ancestor or the source root cannot be read for a permission reason (EPERM/EACCES: no Full Disk Access), it returns `UNREADABLE` ("cannot read <label>/<path>: permission denied"); the workflow pauses the run until the user resumes, and no history is written.
- Each selected folder component is resolved against the names on disk: when its NFC spelling does not open (an NFD name on a normalization-sensitive share), the directory entry whose NFC form matches is used, with its raw bytes.

### 7.3 `sendBatch`

Checks the gate once, then takes the run's next ≤ 500 PLANNED items, one run folder at a time in folder order, then by `rel_path` (byte order, as the scan walks), then id — items of the folder recorded under another case spelling come after, by `path_key` — as a keyset page from a cursor the workflow carries (with the consecutive-error count) between calls and across continue-as-new. A full batch is `MORE`; at the end of a pass, any PLANNED item still in scope (an error under the cap, an approval, a promoted duplicate) starts a new pass, else `DONE`. For each:

1. **Throttle gate** (§7.5).
2. **Known content** (§7.4) → `KNOWN` or `DUP`, or continue.
3. **Copy** (§7.6) → `SENT`.

Heartbeats after each item with running counts, which the page reads from `harvest_run_folders`.

### 7.4 Known content (pre-send dedupe)

Always on. A file is hashed only when its size could match:

1. **Size matches a `contents` row** (`contents_size_idx`): compute the full sha256 (1 MiB reads). If a `contents` row has that sha256 and size → `KNOWN`.
2. **Size matches another ledger item** that is SENT, or PLANNED in this run's send scope and earlier in its send order (photo before maybe, then folder position, `path_key`, id): compute a 64 KiB head hash for both (files ≤ 64 KiB: the head is the whole file), and the full sha256 where heads collide. Same sha256 as a KNOWN item → `KNOWN`; as a SENT or earlier PLANNED item → `DUP`, `duplicate_of` that item.
3. Otherwise the file is copied without hashing.

- A SENT candidate is never read from its source (it may have changed since): a linked one is compared by `files.sha256`; an unlinked one through its drop-zone copy, only while that copy still has the item's size and `dropzone_mtime_ns`.
- A PLANNED candidate is read only on the mount its source identity resolves to (detection, not `last_mount_path`), and only while its size and mtime (±2 s, ±1 h) still match the row; otherwise it is no match and nothing is stored.

- Hashes are stored on the row and never recomputed for an unchanged file.
- A hash read error leaves the item to be copied as-is (the pipeline dedupes it anyway) and records the error.
- **Lost winner.** When an item ends `FAILED` or `VANISHED` and DUP items point at it, the earliest (photo before maybe, then send order) becomes PLANNED and the others point at it. Nothing is skipped unless identical bytes are in the vault or on their way.
- A photo and a maybe with the same bytes: the photo is the one sent.

### 7.5 Throttle gate

Checked before every file.

- **Disk:** `statfs(DATA_DIR)`; shut if `(blocks − bavail + file_size_in_blocks) / blocks > HARVEST_MAX_DISK_PCT / 100`.
- **Backlog:** shut if `max(scan_stats.pending_files, harvest SENT items with file_id IS NULL and sent in the last hour) ≥ HARVEST_MAX_PENDING_FILES`, or if `HARVEST_MAX_PENDING_BYTES` is set and `scan_stats.pending_bytes` reaches it. (The pipeline's scan stats lag by up to one scan interval; the harvester's own count covers the gap.)
- **When shut,** the activity heartbeats every 30 s and writes `paused_reason` (`disk 80.4 %`, `backlog 20,113`) and `paused_since` on the run; the run state is `paused` until the gate opens. A user pause looks the same with reason `paused by you`.
- **Stalled:** if the disk is the reason **and** the drop zone has no pending files **and** no file is DISCOVERED/CLAIMED **and** no COPY row awaits reclaim (`copy_removed_at IS NULL AND NOT copy_mismatch`) for 10 minutes, waiting cannot help. Reason `stalled: the vault is filling the disk`; the page shows it red and suggests freeing space or raising the limit. The run keeps checking every 5 minutes.

### 7.6 Copy protocol

1. **Choose the destination** `harvest/<label>/<rel_path>` (relative to the drop zone). If that name, casefolded, exists in the drop zone or belongs to another SENT item not yet linked, add ` (2)`, ` (3)` … before the extension. A name reserved by another PLANNED item also counts as taken. Store it on the row (still PLANNED) **before** copying.
2. `mkdir -p` the parent. On `ENOENT` (the pipeline's empty-folder cleanup removed a parent at that moment), retry up to 3 times.
3. `lstat` the source and check that its size and mtime still match the row; otherwise the item becomes VANISHED (a rescan plans it again under its new key).
4. Open the source read-only (`O_RDONLY`). Write the bytes to `.<name>.h<item id>.part` beside the destination, which the pipeline ignores (`.part`).
5. Check the written size equals the source size, and the source's size and mtime are unchanged. Otherwise remove our `.part` and retry later.
6. `chmod` the `.part` to `(source mode & 0o777) | 0o600` and set its mtime (ns) to the source's. BSD flags are never copied.
7. Publish without replacing: `link` `.part` → destination, then `unlink` the `.part`. If the destination exists (`EEXIST`), go back to step 1 for a new suffix.
8. `UPDATE … SET state = 'SENT', sent_at = now()`.

- **After a crash between 7 and 8**, the retry finds its stored destination either still in the drop zone with the same size and mtime, or already claimed (a `files` row with that `rel_path`, size and mtime), and marks it SENT. An orphaned `.<name>.h<id>.part` of its own is deleted.
- **Errors:** `ENOSPC` → remove the `.part`, pause the run (`disk full`). Any other error → `attempts + 1`, `last_error`; at the cap → `FAILED`. 25 consecutive errors in one batch → pause the run with the last error as the reason.
- ~~**Fast path:** a source on the same APFS volume as `DATA_DIR` is cloned.~~ Struck (T7-a): see the changelog.

## 8. The page and its API

Static HTML, CSS and vanilla JS from `src/harvest/ui/`, served by Node's `http` module on `127.0.0.1:HARVEST_UI_PORT`. It polls `GET /api/state` every 2 s. No login.

### 8.1 Views

1. **Sources:** every known source (mounted ones first), with kind, used space, last harvest date and sent count; unmounted ones greyed out. **Refresh drives** reruns detection. The count of possible photos waiting for review links to the review view.
2. **Source:** the folder tree (§4.3). Each folder shows its last harvest date, or "covered by `<ancestor>` on `<date>`". Tick folders (or the whole source) → **Start harvest** or **Dry run**. Folders already harvested start unticked.
3. **Run panel** (on every view while a run is active): state, current folder, counts (dirs, photos, possible, junk), sent / already in vault / duplicates / failed, disk % and backlog against their limits, files per minute, the pause reason, **Pause / Resume / Cancel**, and an expandable error list. The queue of waiting runs, each removable.
4. **Review possible photos:** grouped by source and folder with counts and sizes; per group **Send all**, **Skip all**, **Review**. Review shows a thumbnail grid with per-file send/skip. Approved items become PLANNED and are sent by the active run (signal `itemsApproved`) or by a new send-only run.
5. **Find a photo:** paste a sha256 or a vault/staging file name; lists every source path where those bytes were found, with state and dates.
6. **History:** past runs with their counts; a dry run shows what would be sent, known, duplicate, possible and junk by reason.

### 8.2 API

| Method | Path | Effect |
|---|---|---|
| GET | `/api/state` | Sources, the active run and queue, gate status, review count. |
| POST | `/api/sources/refresh` | Run detection. |
| GET | `/api/sources/:id/tree?path=` | One folder level (§4.3). |
| POST | `/api/runs` | `{folders: [{sourceId, relPath}], dryRun}` → creates folders and a run, queues or starts it. |
| POST | `/api/runs/:id/pause` · `/resume` · `/cancel` | Signal or cancel. |
| GET | `/api/runs?limit=` · `/api/runs/:id` | History and detail, including errors. |
| GET | `/api/review?sourceId=&path=` | Maybe groups or files. |
| POST | `/api/review` | `{itemIds or {sourceId, path}, action: 'send' or 'skip'}`. |
| GET | `/api/thumb/:itemId` | A 256 px JPEG. |
| GET | `/api/where?q=` | Provenance lookup. |

- **Protection.** Every request must carry `Host: 127.0.0.1:<port>` or `localhost:<port>` (against DNS rebinding). Every POST must also carry `X-Pixel-Magic: 1` and, when present, a matching `Origin`. Without that, any web page open in the browser could start or cancel a harvest.
- **Thumbnails** are made on demand with sharp (heic-decode for HEIC), reading the source only. They are cached under `DATA_DIR/harvest-cache/thumbs/<itemId>.jpg`, evicted oldest-first above `HARVEST_THUMB_CACHE_MB` (1024). An unmounted source or a decode failure gives a placeholder.

## 9. Failures

| Event | Behaviour |
|---|---|
| Mac reboot, harvester crash or OOM | launchd restarts the harvester and Temporal resumes the run. Scans resume from `scan_cursor`; sends from the ledger. |
| Docker stack down | The page shows "pipeline stack down" (from the failed Postgres/Temporal health checks). The worker keeps reconnecting with backoff; nothing is sent. |
| Drive unplugged or asleep | The activity returns `SOURCE_GONE`; the run folder becomes `waiting_source` and the run `paused` (`Expansion unplugged`). The workflow re-runs detection every 60 s and resumes when the same identity is mounted, at any mount point. Items stay PLANNED. A long unplug continues the workflow as new before Temporal's history limit. |
| No permission to read a drive | The scan returns `UNREADABLE`; the run pauses with "cannot read <label>/<path>: permission denied" until the user grants access and resumes. Nothing is marked harvested. |
| NAS errors | Per-file retry with backoff. Transient errors on a live mount (`ETIMEDOUT`, `EAGAIN`, `ECONNRESET`, `EHOSTDOWN`, `EHOSTUNREACH`, `ENETDOWN`, `ENETUNREACH`, and `EIO` on a network source) cost no attempt; any 25 item errors in a row pause the run with the reason. |
| A share that hangs | Detection probes a mount once at a time (a pending probe is reused) and the host runs with `UV_THREADPOOL_SIZE=32`, so a hung share does not freeze the other file I/O. |
| A run's workflow ended without finishing its row | `startNextQueued` marks it failed ("workflow closed unexpectedly") and starts the next run; cancelling such a run finishes it as cancelled. |
| Crash mid-copy | §7.6: destination stored before copying; per-item `.part` names; the retry recognises an already sent file. |
| Source file changed mid-copy | Not sent this time; picked up as a new item at the next scan. |
| Disk full | The run pauses; the `.part` is removed. |
| The pipeline quarantines or fails a sent file | Not a harvest concern: the bytes are in the vault (QUARANTINED stays in the vault) and `file_id` shows the outcome. |

## 10. Safety invariants

Each is proven by a test (§12).

1. **H1.** All harvester file writes go through one function in `paths.ts` that refuses any path outside `DATA_DIR/dropzone/harvest/` and `DATA_DIR/harvest-cache/` (after `realpath` of the parent). Sources are opened only with `O_RDONLY`. A source containing `DATA_DIR` is refused.
2. No final drop-zone name is ever overwritten. No half-written file is ever visible under a name the pipeline ingests.
3. The disk gate holds before every file, given the readings it sees.
4. An item is `KNOWN` only if `contents` has its sha256 and size. It is `DUP` only while its `duplicate_of` is SENT or PLANNED.
5. Re-running a finished harvest sends 0 files.
6. Unplugging a source never sets an item `FAILED`.
7. Nothing runs without a user action (no schedule, no auto-start of runs at boot — only a resumption of a run already started).

## 11. Configuration (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `HARVEST_UI_PORT` | 3020 | Page port (127.0.0.1 only). |
| `HARVEST_MAX_DISK_PCT` | 80 | Disk limit. |
| `HARVEST_MAX_PENDING_FILES` | 20000 | Drop-zone backlog limit. |
| `HARVEST_MAX_PENDING_BYTES` | unset | Optional byte limit on the backlog. |
| `HARVEST_MIN_SIZE` | 20480 | The too-small threshold, in bytes. |
| `HARVEST_MAX_ATTEMPTS` | 5 | Per-item copy attempts. |
| `HARVEST_THUMB_CACHE_MB` | 1024 | Thumbnail cache cap. |

Parsed with the existing config rules (empty = unset, bounds enforced).

## 12. Testing

- **Unit:** classification (every rule in §6, ported from the `find-all-images` cases); the EXIF reader on synthetic JPEG, TIFF (far IFD0) and HEIC files, plus malformed and crafted inputs that must return `null` quickly; mount parsing against a captured sample (NAS shares, `Plex-1`, a DMG, a TM snapshot, an NTFS read-only volume, `%20` names); identity and labels; path keys (NFC/NFD, case); the mtime-shift rule; the throttle gate with fake `statfs` and stats; the guarded writer.
- **Activities** on real Postgres (testcontainers) with fixture folders as sources: scan idempotency and resume from the cursor; the copy protocol, including the crash points via the existing checkpoint hook; every known-content case (same size different bytes, head collision with a different tail, a lost winner, photo over maybe, no re-hash on a re-run); `SOURCE_GONE`.
- **Sources untouched:** the fixture source is made read-only (`chmod -R a-w`, plus `uchg` on one file) before a full harvest; bytes, mtimes, modes and flags are snapshotted before and after; a wrapper over `node:fs` fails the test on any mutating call whose path is not under the allowed roots.
- **Workflow** (TestWorkflowEnvironment): scan → send → finish; pause, resume and cancel; unplug and replug; a queued second run; approving maybes mid-run; continue-as-new.
- **HTTP:** every endpoint; Host/Origin/header protection; thumbnails and cache eviction.
- **Dashboard SQL** tests for the Harvest tiles (§13).
- **End-to-end tier (`make test-harvest`):** on a `pmtest_*` stack, a fixture source made of clones of the sample corpus in a temp folder (the corpus stays read-only; nothing leaves the Mac). Harvest it twice; assert each distinct sha256 is in the vault once, the second run sends 0, and the source is unchanged.
- **Gates:** `npm test`, lint, format:check, typecheck, build, coverage (≥ 85 % branches on new files), plus the existing smoke tier.

## 13. Observability and operations

- **Grafana:** a "Harvest" row: sent and known (today, total), the backlog, the active run's state and pause reason, and the harvester's last heartbeat (red after 5 minutes while a run is active). The row links to the page.
- **Make targets:** `harvest-install`, `harvest-uninstall`, `harvester` (foreground), `harvest-status` (terminal summary), `test-harvest`.
- **README:** what harvest does and never does, installing the agent, the page, limits, and the rollout below.

### Rollout on the user's data

1. Dry runs of NAS-Backup and Expansion, compared with `find-all-images`' measurements (NAS: 17,402 photos; Expansion: 622,804 photos, about 78.6k distinct).
2. Harvest NAS-Backup.
3. Harvest Expansion, watching the disk stay ≤ 80 %.
4. The user reviews possible photos when it suits them.

## 14. Out of scope

Videos and Live Photo movies; cloud sources; deleting or changing anything on a source (never); archives (zip, tgz); changes to ingest, clustering or staging; harvesting arbitrary Mac folders; widening the pipeline's still kinds (DCR, SRW).

## 15. Changelog

Decisions made during implementation that changed or refined this spec, in plain language.

- **R-a (gate):** while the gate is shut, `sendBatch` returns `GATED` with the reason and the workflow sleeps 30 s before calling it again. This replaces "the activity heartbeats every 30 s" (§7.5): the page behaves the same, and a user pause no longer has to cancel a running activity.
- **R-b (host heartbeat):** a one-row table `harvest_host_state(id = 1, heartbeat_at, version)` holds the host heartbeat the Grafana tile reads.
- **P1 → T7-a (no clone fast path):** the §7.6 "Fast path" bullet is struck. Node 24 on macOS cannot clone files, and same-volume sources are out of scope (§14). Every copy is a byte copy.
- **P1 (`copyInto`):** `GuardedFs.copyInto(src, dest)` was added so every write stays behind the guard (source opened `O_RDONLY`, destination guarded). Its clone option was later removed (T7-a).
- **P2 (`RunControl`):** `activities.ts` declares only the `RunControl` interface; `control.ts` and `runs.ts` implement `temporalRunControl` and `startNextQueued`, so no module references a later one.
- **P5 (make targets):** the make targets (`harvest-install` and the rest) are defined once, in Task 11. `test-harvest` comes with the end-to-end tier.
- **P6 (sources untouched test):** the "no mutating call on a source" test injects a recording `FsOps` into `GuardedFs` and the send code, and a static test checks that no file in `src/harvest/` except `paths.ts` imports a mutating fs function. ESM namespaces cannot be spied, so the source snapshot with read-only locking stays too.
- **T1-a (system-file rule):** `.DS_Store` and `Thumbs.db` are checked by name before the extension test, so the `system-file` rule is reachable; the `._` rule applies to candidate extensions only (§6.3).
- **P3 (gate signature):** `checkGate(deps, runId, fileSize, now?)`; the 10-minute stall is measured from that run's own `paused_since` and `paused_reason`.
- **P4 (page files):** no copy step. The host serves the page from `src/harvest/ui`, found relative to the compiled host file, so the harvester always runs from the repository.
- **T3-a (`dropzone_mtime_ns`):** `utimes` is only microsecond-exact, so the copy's real on-disk mtime is stored in `harvest_items.dropzone_mtime_ns`. Linking to `files` and crash recognition compare against it, not the source's mtime.
- **T3-b (no-replace publish):** `.part` → final uses `link` (on `EEXIST`, the next suffix) then `unlink`, never `rename`, which would silently overwrite.
- **T4-a (diskutil failure):** if `diskutil` fails for a local volume, no `local-name:` identity is minted. The existing row with the same last mount path keeps its identity; otherwise the volume is skipped this round with a warning, so history never forks into "Expansion (2)".
- **T4-b (hung mounts):** filesystem calls on a mount root during detection time out after 5 s, and that mount is skipped this round (stale SMB shares hang forever).
- **T5-a (`position`):** `harvest_run_folders.position` stores the selection order, because folder ids of previously harvested folders are older than new ones.
- **T5-b (scan and `SOURCE_GONE`):** the scan only reports `SOURCE_GONE`; the workflow sets the run folder to `waiting_source`, because the scan should not guess run policy.
- **T5-c (`SOURCE_GONE`):** only when the mount itself is gone (no mount, the root cannot be read, or its device id changed). A selected folder missing on a live drive counts as one error ("folder not found") and the folder is done, so a renamed folder never waits forever.
- **T5-d (resume-stable errors):** scan error counters count each unreadable directory once, skip ancestor-loop error counting on resume and honour the resume point, so resumed counts equal an uninterrupted run (§7.2).
- **T6-a (stall):** the stall test also requires no unlinked SENT harvest items in the last hour, since our own files in flight mean waiting can still help. "Stalled" can show up to an hour later.
- **T6-b (known check):** KNOWN ledger items are not candidates in §7.4 step 2. §10.4 (KNOWN only when `contents` holds the sha256 and size) wins, and step 1 already covers them.
- **T6-c (DUP race):** the DUP write is one conditional statement (the target must still be SENT or PLANNED, else the item is sent). A PLANNED item whose source changed or vanished at send time becomes VANISHED and promotes its duplicates. `paused_since` stays put while the reason stays in the same class (disk, backlog, stalled).
- **T7-c (changed during copy):** a source that changed during the copy becomes VANISHED (a rescan plans it under its new key), and a name reserved by another PLANNED item counts as taken (§7.6).
- **T7-b / T7-d (raw source paths):** a name that is not valid UTF-8, or whose on-disk bytes differ from its NFC spelling (NFD names on exFAT, SMB, NTFS), is stored in `rel_path_raw` and read through that raw path. The drop-zone name uses the NFC `rel_path`.
- **T7-e (copy hardening):** an `unlink` failure after the link still counts as published (a leftover `.part` is cleaned later); the `.part` is fsynced after its metadata and the parent directory after the link, before SENT; `chmod` and `utimes` go through the open write handle; a zero-row `dropzone_mtime_ns` update aborts and removes the part; known-content hashing heartbeats while it reads.
- **T9-a (review API):** a review `{sourceId, path}` means that exact parent directory. `/api/state` also returns extra fields (`coveredBy`, per-source counts, the gate). Cancelling a queued run marks it cancelled and asks the control to cancel it, best effort.
- **T9-b (send-only runs):** review approvals create a run with `send_only = true`. It skips the scan, its folders are the approved items' parent directories, it sends only PLANNED items, and it writes no folder history. If the active run is a dry run, approvals queue a send-only run instead of signalling it. One approval never triggers a drive scan.
- **T9-c (log rotation):** the harvester's log rotation writes through a `GuardedFs` confined to `DATA_DIR/logs`, with no allow-list exception.
- **T9-d (approval routing):** an approval signals the active run only if it is not a dry run and its folders cover every approved item; otherwise a send-only run is queued. The mount cache can be up to 10 s stale.
- **T10-a (unidentified drives):** a volume whose probe failed and that matches no row is returned separately as `unidentified` (no row, not selectable). `/api/state` and the page show it greyed as "couldn't identify this drive — Refresh drives". System tools (`/usr/sbin/diskutil`, `/usr/bin/plutil`, `/sbin/mount`) are called by absolute path because launchd's PATH is minimal.
- **Operations (Task 11):** the agent runs `process.execPath` as recorded at install time, so `make harvest-install` must be re-run after a Node upgrade. Its stdout and stderr go to `DATA_DIR/logs/harvester.launchd.log` (crash output only). The plist is the one write outside `DATA_DIR`, made through a `GuardedFs` on `~/Library/LaunchAgents`.
- **T11-a (operations):** `launchctl bootstrap` is retried on exit code 5 / "Input/output error" up to 3 times with a 1 s delay, because a bootout may still be tearing down. `make harvest-stop` (bootout, plist kept) and `make harvest-start` (bootstrap the existing plist) were added. The foreground `make harvester` needs `make harvest-stop` first.
- **F-1 (send-only items):** a send-only run records its approved item ids in `harvest_run_items` (migration 0006) and sends only those (still PLANNED); its folders are kept for display and counters. An approval covered by an active send-only run adds its items to that run; a normal run's folders already hold them. One approval never sends unapproved photos.
- **F-2 (unreadable folders):** when a folder's root, an ancestor or the source root cannot be read for a permission reason, `scanFolder` returns `UNREADABLE` ("cannot read <label>/<path>: permission denied"); the run pauses until resumed and writes no folder history. A folder not found keeps T5-c (one error, done) but writes no history (`harvest_run_folders.root_ok`, migration 0006). Folder components are resolved against the names on disk (NFC match). README: grant the node binary Full Disk Access; `make harvest-status` names it.
- **F-3 (migration 0006):** schema changes after 0005 (already applied to the live database) are in `0006_harvest_fixes.sql`: `harvest_run_items`, `root_ok`, and the `duplicate_of` and PLANNED keyset indexes (§5).
- **C1 (long unplug):** the scan retry loop continues as new before the history limit (done folders are skipped; the cursor is in the database). `RunControl.start` reports a duplicate whose workflow already closed, and `describe` whether a run's workflow is running; `startNextQueued` fails such runs ("workflow closed unexpectedly") — queued or active — and moves on. Cancelling a run whose workflow is gone finishes it as cancelled and starts the next.
- **Respell:** a rescan refreshes `rel_path` and `rel_path_raw` from the disk (matched row and insert-conflict path alike), so an NFC ↔ NFD respell with the same key no longer leaves stale bytes that make the item VANISHED every run.
- **I2 (known content):** a SENT candidate is never read from its source (linked: `files.sha256`; unlinked: its drop-zone copy while it is unchanged); a PLANNED candidate is read only on the mount its identity resolves to and only while size and mtime match; a PLANNED candidate qualifies only inside the current run's send scope and earlier in its send order (§7.4). `checkKnown` takes the run id and the item's path key.
- **I3 / F-2:** see F-2.
- **I4 (hung shares):** a pending containment probe per mount is reused by later detections, and `UV_THREADPOOL_SIZE=32` is set in the launchd agent, `make harvester` and `npm run harvester` (§9).
- **I5 (send paging):** the gate is checked before any item query; items are paged per run folder with a `(rel_path COLLATE "C", id)` keyset cursor carried by the workflow, over the folder's exact `rel_path` range, selecting only the needed columns; items under the folder's key but another case spelling (renamed since; a rescan respells what it sees) follow in a second part keyed by `path_key`. A full batch is `MORE` without another query, and the end of a pass is one `LIMIT 1` probe per folder and part. Send order (`rel_path`, as the scan walks) is unchanged. Indexes in 0006 (§5); EXPLAIN on 120 000 rows shows index range scans with no sort.
- **I6 (transient NAS errors):** transient errors on a live mount cost no attempt and count toward the 25-in-a-row pause (carried across batches); a rescan resets a FAILED item it sees again unchanged to PLANNED with 0 attempts (§5, §9).
- **M1 (folder states):** a run folder is `waiting_source` while its drive is unplugged (scan: set by the workflow through `setFolderState`, keeping T5-b; send: by `sendBatch`) and `sending` while its items are sent (§9).
- **M2 (pause during a scan):** a pause that arrives during a scan step shows at once as "pause requested — after the current folder step" and takes effect when the step ends.
- **M8 (startup):** a test pins that the host's startup `startNextQueued` starts only an already-queued run (§10.7).
