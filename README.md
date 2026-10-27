# Pixel Magic 2.0

[![CI](https://github.com/audiomojo/Pixel-Magic-2.0/actions/workflows/ci.yml/badge.svg)](https://github.com/audiomojo/Pixel-Magic-2.0/actions/workflows/ci.yml)

Photo de-duplication pipeline (TypeScript, Temporal, Postgres). Drop photos into
`DATA_DIR/dropzone`; unique photos and copies are organised under `DATA_DIR/staging`.

Everything under `DATA_DIR` must live on one filesystem (hardlinks and renames are
relied on). Verify with `make fs-check`.

The vault holds each distinct file once. A dropped file whose bytes are already in the vault
(a byte-identical duplicate, state `COPY`) is parked in `vault/.copies/` and then removed, but
only after a fresh re-hash of it **and** of the vault file both match. That is the one thing the
pipeline ever deletes (otherwise a file holding dropped bytes is unlinked only once another
durable link exists). On any disagreement both files are kept and the row is flagged: see
`make doctor` and the Grafana "Copy mismatches" tile, which should read 0. `make copies` lists
every duplicate drop (path, mtime, when it was dropped, the vault sha), so you can see what was
dropped twice; `make sweep-copies` runs the 15-minute duplicate sweep now. `staging/current/copies/`
shows only different files of the same shot (e.g. a resized JPG of a HEIC), never byte-identical
duplicates.

`staging/current/` is rebuilt from the database: don't keep files there. Anything saved inside
it that staging did not put there is moved to `staging/.rescued/<generation>/` when that
generation is cleaned up, never deleted.

## Development

    npm install
    npm test            # unit + workflow tests
    npm run test:ui     # the harvester page in Chromium (first: npx playwright install chromium)
    npm run lint && npm run format:check && npm run typecheck && npm run build

Configuration is via environment variables: copy `.env.example` to `.env` (compose reads it).
Every tunable there is forwarded to the workers; one left unset uses its default.

The dashboard is at <http://localhost:3000> (or `GRAFANA_PORT`), viewable without a login;
the `admin` login is only needed to edit.
Its top strip shows each step in order (① scan, ② check & copy, ③ drop zone, ④ ingest,
⑤ decode, ⑥ cluster): green while that step is working, orange while it waits or is paused,
grey when idle. ② gives the harvest's % done, photos left, rate and time left; it can be green
while the drop zone is empty, when the photos it checks are already in the vault.

Drop real files, not symlinks or aliases: links in the drop zone are never followed or
ingested (each scan logs a warning while any are there).

Once every file in a dropped folder has been ingested, the scan removes the empty folder
(and any `.DS_Store` / `._*` litter in it) after it has sat unchanged for 60 s, so the drop
zone stays clean. A folder still holding anything else, even a partial download, is kept.

Decoding is memory-bound: `CPU_CONCURRENCY` defaults to the CPU count, capped at the
container's memory ÷ `DECODE_MEM_BUDGET_MB` (1280 MB, about one ~60 MP panorama). Docker
Desktop's VM defaults to about 8 GB, so about 6 decoders run in parallel. For more, raise the
memory under Docker Desktop → Settings → Resources, or set `CPU_CONCURRENCY` (or a smaller
`DECODE_MEM_BUDGET_MB`) in `.env`.

Without a memory limit every worker container sizes itself from the whole VM, and `worker` and
each `worker-extra` replica share that VM with Postgres and Temporal. When you scale
`worker-extra`, either set `WORKER_MEM_LIMIT` / `WORKER_EXTRA_MEM_LIMIT` (compose `mem_limit`,
which the default then follows) or set `CPU_CONCURRENCY` so that the decoders of all
containers × `DECODE_MEM_BUDGET_MB` stay below the VM's memory minus about 2 GB. Otherwise the
OOM killer ends decodes, which retry and can end FAILED `decode_killed`.

Container logs use Docker's `local` driver, rotated at 5 × 20 MB per container, so a
months-long run never fills the Docker VM's disk (which Postgres shares).

## Harvest (external drives and NAS)

Harvest copies real photos from your external drives and network shares into the drop zone
(`DATA_DIR/dropzone/harvest/<drive>/…`), where the pipeline de-duplicates them as usual. It runs
natively on the Mac (only the Mac sees `/Volumes`) as a launchd agent that starts at login.

It **never** modifies a source (files are opened read-only, nothing is written, moved or
deleted there), **never** overwrites a drop-zone file (a name clash gets ` (2)`), and **never**
starts by itself: there is no schedule, and a run begins only when you press Start on the page.
Files whose bytes are already in the vault are not copied.

    make harvest-install     # build, write ~/Library/LaunchAgents/com.pixelmagic.harvester.plist, load it
    make harvest-uninstall   # unload and remove it
    make harvest-status      # active run, item counts, gate, host heartbeat
    make harvest-stop        # unload the agent but keep it installed
    make harvest-start       # load it again
    make harvester           # run in the foreground; needs `make harvest-stop` first

**Grant the harvester access to your drives** (least privilege). macOS grants drive access
per program, so the agent does not run your usual `node`: `make harvest-install` copies it to
`~/Library/Application Support/PixelMagic/harvester/pixel-magic-harvester`, which nothing else
runs. Give that copy **only** Removable Volumes and Network Volumes access, **not Full Disk
Access** (which would also open your home folder and other apps' data):

1. After installing, open the page, pick each drive and open a folder. macOS asks whether
   "pixel-magic-harvester" may access files on a removable volume (or a network volume):
   press **Allow**.
2. Check **System Settings → Privacy & Security → Files & Folders →
   pixel-magic-harvester**: **Removable Volumes** and **Network Volumes** should be on.
3. If no prompt appears and the page shows "cannot read <drive>/<folder>: permission
   denied", turn them on there (if pixel-magic-harvester is not listed yet, run
   `make harvest-stop` and `make harvest-start`, then open a folder again), then press
   **Resume**. A run without access pauses and marks nothing as harvested.

The `node` line of `make harvest-status` names the binary to grant. A reinstall with the same
Node keeps the copy (and its grant); after a Node upgrade `make harvest-status` says the
runtime is out of date: run `make harvest-install`, then grant access again (step 1).
`make harvest-uninstall` removes the copy. A foreground `make harvester` runs your usual
`node` from Terminal, which uses Terminal's own access.

The page is at <http://127.0.0.1:3020> (this Mac only). Pick a drive, tick folders (or the
whole drive) and press **Start harvest**, or **Dry run** to see what would be sent first. A
folder you harvested before shows its date; a rescan sends only what is new. Files with unusual
extensions or in project-like folders are "possible photos": they wait on the **Review** view
for you to send or skip them (the Grafana "Harvest" row counts them). Copying starts while the drive is still being scanned: photos
found so far go to the drop zone, so the pipeline works in parallel with the scan. Pause, resume and cancel
are on the page; a run waits by itself when the disk or the pipeline backlog is over its limit,
or when a drive is unplugged, and resumes when that clears.

Approving possible photos sends only those files: an approval never scans a drive. A drive the
harvester cannot identify shows greyed on the page as "couldn't identify this drive"; press
**Refresh drives** to retry.

A network share that stops answering can hang file calls until it returns. The harvester
probes such a share only once at a time and runs with `UV_THREADPOOL_SIZE=32` (set in the
launchd agent, `make harvester` and `npm run harvester`), so a hung share does not freeze the
page or the other drives. A foreground run started some other way should set it too.

The dev/compose stack (Postgres and Temporal) must be up and migrated (`make up`) before the
harvester can work; it retries until they answer.

| Variable                    | Default | Meaning                                                   |
| --------------------------- | ------- | --------------------------------------------------------- |
| `HARVEST_UI_PORT`           | 3020    | Page port (127.0.0.1 only).                               |
| `HARVEST_MAX_DISK_PCT`      | 80      | Pause sending when `DATA_DIR`'s disk would pass this.     |
| `HARVEST_MAX_PENDING_FILES` | 20000   | Pause when the drop-zone backlog reaches this many files. |
| `HARVEST_MAX_PENDING_BYTES` | unset   | Optional byte limit on the backlog.                       |
| `HARVEST_MIN_SIZE`          | 20480   | Smaller files are skipped as too small (bytes).           |
| `HARVEST_MAX_ATTEMPTS`      | 5       | Copy attempts per file before it is marked FAILED.        |
| `HARVEST_THUMB_CACHE_MB`    | 1024    | Thumbnail cache cap.                                      |

They are read from `.env` (see `.env.example`); restart the agent (`make harvest-install`) after
changing one.

Logs: `DATA_DIR/logs/harvester.log` (rotated by size) and, for crashes only,
`DATA_DIR/logs/harvester.launchd.log`. **After a Node upgrade (or moving the repo) run
`make harvest-install` again**: the agent runs its own copy of Node and the repo path it was
installed with.
Design: `docs/superpowers/specs/2026-10-19-harvest-design.md`.

## Upgrading

Up to `MAX_IN_FLIGHT` (5,000) FileWorkflows are always in flight, and a workflow-code change
that alters the order of its commands makes their histories fail replay. The default upgrade
is therefore a **drain**:

1. Pause the scan schedule:
   `docker compose run --rm temporal-ns temporal schedule toggle --schedule-id pm-scan --pause --reason upgrade --address temporal:7233`
2. Wait until Grafana's "In flight" tile (running FileWorkflows) reads 0, and no
   `make retry-failed` ContentWorkflows are still running (Temporal UI).
3. `make up` (rebuilds and restarts the workers; migrations run first).
4. Unpause: the same command with `--unpause`.

Rules for changes to `src/workflows.ts`:

- Every change in command order (an activity, timer or child added, removed or reordered) goes
  behind `patched('<change-id>')`, so running histories replay the old path. After a later
  drain, replace it with `deprecatePatch('<change-id>')`, then remove it.
- Activity implementations and activity options can change without a drain.

**Upgrading to duplicate removal (migration 0004).** Staging generations built before it hardlink
every `.copies` file, and removal defers a `.copies` file that has another link (it would free
nothing). After `make up`, run `make stage FORCE=1` **twice** (the build keeps the previous
generation), then `make sweep-copies` until Grafana's "Duplicates removed" tile shows 0 pending
bytes (each run handles at most 10,000 duplicates; `pm-copies` also runs it every 15 min).
Check `make doctor` for `copyMismatches: 0`.

A worker restart (upgrade, crash or Mac reboot) costs each in-flight decode one poison attempt.
A content whose attempts run out ends FAILED; `make retry-failed` starts it again, and is safe
to re-run. A workflow that fails with `UnexpectedState` (visible in the Temporal UI at
<http://localhost:8080>) means a database row is not in the state its step expects; check that
row by hand before restarting anything.
