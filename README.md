# Pixel Magic 2.0

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
    npm run lint && npm run format:check && npm run typecheck && npm run build

Configuration is via environment variables: copy `.env.example` to `.env` (compose reads it).
Every tunable there is forwarded to the workers; one left unset uses its default.

The dashboard is at <http://localhost:3000> (or `GRAFANA_PORT`), viewable without a login;
the `admin` login is only needed to edit.

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
