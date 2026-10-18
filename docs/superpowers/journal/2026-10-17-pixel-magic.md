# Pixel Magic 2.0: build journal

Dates: 2026-10-17 to 2026-10-18.
Spec: `docs/superpowers/specs/2026-10-17-pixel-magic-design.md`.
Plan: `docs/superpowers/plans/2026-10-17-pixel-magic.md`.

## Goal

Ingest three decades of photos on a Mac: hundreds of thousands of files, with runs that will last months once AI scoring arrives (20–30 s per image).

- Restarts must never lose progress.
- Only true copies of the same shot count as duplicates.
- Burst frames and look-alikes are kept, as "similar" sets.

## How it was built

- **Stack.** Temporal and Postgres 16, with TypeScript on Node 24 LTS (sharp, heic-decode, exiftool-vendored). Everything runs in Docker Compose.
- **Process.** superpowers subagent-driven development, applied to 13 plan tasks.
  - Each task got a fresh implementer, a task review covering the spec and quality, and fix rounds until the review was clean.
  - A whole-branch review closed the work, followed by one fix wave and one re-review.
- **Preflight.** A scan before the work began found 50 conflicts in the plan, F1–F50. Each was ruled on, and the spec and plan were amended where needed.
- **Rulings.** About 90 decisions were made without stopping to ask the user. Each one was logged with its reason and its cost if wrong. The ones that matter most are listed below.
- **Privacy.** The photo corpus (`test-automation/sample-images/`) is read-only and git-ignored, and nothing from it goes to hosted services.
  - Every end-to-end run checks it for drift before and after.
  - The manifest and golden tree derived from it are generated locally and never committed.

## What the tests found

The end-to-end and chaos tests ran on clones of the real corpus. They found defects that the unit tests did not:

- **Docker inodes.** Docker Desktop file sharing renumbers inodes, so same-file checks moved to a sha-verified rule.
- **Bridge flagging.** An EXIF-less export that linked two burst frames caused the *real* frame to be flagged `copy_ambiguous`. Grouping now flags the key-less bridge and its key-less copies, and never a photo with a capture key. A property test checks this over thousands of arrival orders.
- **Decode memory.** Eighteen parallel decodes of ~60 MP panoramas exhausted Docker's memory. Concurrency now follows available memory, and a decode killed by the OS is retried, with a total cap of 10 attempts.
- **Arrival-order staging.** A similar set that collapsed to one copy group was staged as `similar/` or `unique/` depending on arrival order. It now always stages as unique.
- **Weak chaos.** The disruptions first landed after the work was done. The chaos stack is now throttled, and the test asserts that every disruption overlaps decoding and clustering.

## Final review (whole branch)

The final review found 0 critical, 8 important and 22 minor issues. All were fixed. The main ones:

- **Clustering at scale.** Rebuilding the cluster cache cost O(n²) and would have stalled clustering at around 500k files. It is now linear, with indexed set merges.
- **Half-written files.** A file still being written could have been ingested. It is now re-checked and put back.
- **Staging cleanup.** Cleanup could delete a file the user had saved there. Such files now move to `staging/.rescued/`.
- **Settings.** `.env` tunables never reached the containers. They are now forwarded, empty values mean unset, and bounds are enforced.
- **Burst safety (user requirement).**
  - A same-second match without subseconds also needs tight hash distances.
  - A copy group never mixes camera models. The cost: some real exports from such cameras land as "similar".
- **Operations.** The README now documents redeploys mid-run, plus per-worker memory limits and log rotation.

Four more small fixes followed: an idle timeout on the cluster transaction, a staging cleanup race, fsync of vault directories on replay, and a floor under the "copy-if-tight" test oracle.

## Key rulings

- **Order dependence.** Partitions and staged trees are compared after excluding contents flagged `copy_ambiguous` in either run. The spec §12 R3 records this.
- **Unlink rule.** User bytes are unlinked only once another durable link exists.
- **Retries.** Transient errors retry. Poison decodes stop after 5 failures, and every decode after 10 attempts. `make retry-failed` recovers either.
- **Change detection.** Staging detects changes with Postgres snapshots (`pg_current_snapshot()`) rather than timestamps.
- **Corpus tooling.** The corpus oracle is independent of `src/`. Corpus seeding clones files and falls back to a byte copy only when cloning is unsupported.
- **Grafana port.** Grafana's port is configurable; locally it is 3001.

## Results

- **Branch.** 44 commits, fast-forwarded into `main` (HEAD `db3ef21`). Commit history was kept as is.
- **Unit tests.** 439 pass; coverage is about 99% of lines and 94% of branches.
- **End-to-end tiers.** Smoke and standard pass.
- **Chaos.** The chaos run kills, restarts and pauses the worker, Temporal and Postgres. It produces the same partitions and staged tree as a calm run.
- **Bursts.** In every run, no copy group contained two burst frames.

## Open items

- The "copy-if-tight" check is strict only for PNG variants. The other lossy kinds are counted and printed but do not fail the test.
- There is a narrow, already-failing path in staging cleanup where putting an entry back is not atomic.
- Run `make verify-vault` periodically. A writer that still holds a file open after ingest can change vault bytes, and only verification detects it.

## Follow-up: removing confirmed duplicates (2026-10-18)

The user decided the vault holds each distinct file once (spec §6.5, R9; migration 0004).

- **Removal.** A `COPY` file's `.copies` twin is unlinked only after a fresh re-hash of it and of its vault file both equal `files.sha256`. The unlink is fsynced before the row is marked. It runs in FileWorkflow (behind `patched('reclaim-copy')`) and in a bounded 15-minute sweep (`pm-copies`).
- **Deletes nothing otherwise.** A mismatch, a missing file or a non-regular file flags `copy_mismatch` and is never retried. A `.copies` file with another link (an old staging generation) is deferred. A file that changes during the re-hash is retried.
- **Staging choice (a).** `copies/` shows only different files of the same shot; `copies/v-` is gone. The smoke golden lost exactly one line.
- **Found while building.** Comparing inode numbers to detect change would retry forever on virtiofs, so the check uses size and mtime. Upgrading needs two forced staging builds before old-generation links let the sweep reclaim (README, Upgrading).
- **Results.** 479 unit and workflow tests pass. Smoke, standard and chaos pass, with corpus-verify showing 0 drift.
