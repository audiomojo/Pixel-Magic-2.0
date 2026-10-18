# Pixel Magic 2.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a resumable, Temporal-orchestrated photo-dedupe pipeline. It moves dropped files into a content-addressed vault, groups true copies of the same shot and keeps every similar shot, and rebuilds a hardlinked staging tree that can be regenerated at any time.

**Architecture:** One TypeScript package runs as Temporal workers on the task queues `main` (workflows), `io`, `cpu` and `cluster`. Postgres holds the facts (`files`, `contents`, `groups`). Temporal holds progress. Staging is computed from the database and swapped in with an atomic symlink rename. Compose runs postgres, temporal-admin, temporal, temporal-ns, temporal-ui, migrate, worker, worker-extra and grafana.

**Tech Stack:**
- **Runtime:** Node 24 LTS, TypeScript 5.x (strict, ESM, `module: NodeNext`), npm
- **Temporal:** `@temporalio/{client,worker,workflow,activity,testing}`
- **Data and config:** `pg` (with `BigInt` parsing for int8), `node-pg-migrate` (SQL migrations), `zod` (config)
- **Imaging:** `sharp`, `heic-decode`, `exiftool-vendored`, `workerpool` (process workers)
- **Logging:** `pino`
- **Tests:** vitest, `@vitest/coverage-v8`, fast-check, `@testcontainers/postgresql`
- **Lint and format:** eslint (typescript-eslint, strict-type-checked), prettier
- **Script runner:** `tsx` for `tools/`

**Spec:** `docs/superpowers/specs/2026-10-17-pixel-magic-design.md` (including §12 "Revisions during planning", R1–R8)

## Global Constraints

- **Build and run:**
  - Node ≥ 24 (`"engines"`), `"type": "module"`, and `tsconfig` with `strict`, `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes`.
  - Source lives in `src/`. `tsconfig.json` is `noEmit` (typecheck + lint over src, tests, tools and the root config files); `npm run build` runs `tsc -p tsconfig.build.json` (`rootDir: src`, `outDir: dist`), so entries land at `dist/<entry>.js`. Services run `node dist/<entry>.js`.
- **64-bit hashes are `bigint` everywhere.** Never use `number` for a hash, size or `mtime_ns`. Configure `pg` with `types.setTypeParser(20, BigInt)`. In Temporal payloads, bigints travel as decimal strings.
- **Config** comes from env vars with these exact names and defaults. They are parsed by `zod` into camelCase fields (`SCAN_INTERVAL_S` → `scanIntervalS`):
  - `DATA_DIR=/data`, `DATABASE_URL` (required), `TEMPORAL_ADDRESS=temporal:7233`, `TEMPORAL_NAMESPACE=default`, `WORKER_QUEUES=main,io,cpu,cluster`
  - `SCAN_INTERVAL_S=30`, `STABLE_SECONDS=5`, `MAX_IN_FLIGHT=5000`
  - `IO_CONCURRENCY=8`, `CPU_CONCURRENCY=<os.availableParallelism()>`, `DECODE_TIMEOUT_S=60`, `POISON_ATTEMPTS=5`
  - `LOOK_ALIKE_PHASH=8`, `LOOK_ALIKE_DHASH=10`, `COPY_PHASH=4`, `COPY_DHASH=6`
  - `LIVE_MAX_DURATION_S=5`, `STAGE_MAX_STALENESS_S=900`
- **The `DATA_DIR` layout** is `dropzone/`, `work/claimed/`, `vault/<ab>/<cd>/<sha256>.<ext>`, `vault/.copies/<fileId>.<ext>`, `staging/.gen-<n>/` and `staging/current` (symlink). Every path stored in the database is **relative to `DATA_DIR`**, except `files.rel_path`, which is the file's path **relative to `dropzone/`** (spec §6.1: `rename(dropzone/rel_path, …)`).
- **A file holding dropped bytes (`dropzone/`, `work/claimed/`, `vault/`, `vault/.copies/`) may be unlinked only (a) after another durable link to it exists, or (b) after it is confirmed byte-identical to its vault file by a fresh re-hash of both files** (user decision 2026-10-18, spec §6.5 / §12 R9). The only such unlinks allowed are: under (a), `work/claimed/<id>` after its vault link is committed, `work/claimed/<id>` after a stale attempt has successfully hardlinked it back to `dropzone/<rel_path>` (put-back), and old or orphaned `staging/.gen-*` directories; under (b), `vault/.copies/<id>.<ext>` of a committed `COPY` row, by `reclaimCopy`/`sweepCopies` only, when it has no other link, both hashes equal `files.sha256` and neither file changed since, followed by an fsync of `vault/.copies` before the row is marked removed. Anything less deletes nothing (a mismatch flags the row). Temp files the pipeline creates itself (fs-check probe dir, RAW preview temp under `os.tmpdir()`) may be removed.
- Vault files are mode `0o444`. Containers run as non-root (`UID`/`GID` build args, default 501/20; GID 20 already exists in Debian as `dialout`, so the Dockerfile reuses an existing group).
- **Workflow code** (`src/workflows.ts`) imports only `@temporalio/workflow` and **type-only** imports from `src/types.ts` and `src/activities/index.ts`.
- Published ports are `127.0.0.1` only: Grafana 3000, Temporal UI 8080, Postgres 5432, Temporal 7233.
- The corpus `test-automation/sample-images/` is never written to. It stays git-ignored, and tests reach it only by cloning with `cp -c`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Between-Task Gate (mandatory)

No task is complete, and no next task starts, until **all** of the following pass on the committed tree. The reviewer re-runs them itself; it does not trust the implementer's report. **This table is authoritative:** every gate whose "Applies from" ≤ the current task runs, whatever a task's step text lists (each task's gate step spells out the full command set).

| Gate | Command | Applies from |
|------|---------|--------------|
| G1 Red first | The task's new tests were seen failing before the implementation (the implementer pastes the failing output). An empty or comment-only test body fails G1. Waived for Task 13 (no implementation step; paste the first run's output instead). | Task 1 |
| G2 Full regression | `npm test` (= `vitest run --project unit --project workflow`) with **every** test from earlier tasks, 0 failures, 0 unexpected skips | Task 1 |
| G3 Static | `npm run lint && npm run format:check && npm run typecheck && npm run build` | Task 1 |
| G4 Coverage | `npm run test:coverage` (v8 coverage over `src/**` and `tools/corpus/lib.ts`, excluding the entry modules `src/fscheck.ts`, `src/worker.ts`, `src/decodeWorker.ts` and the CLI wrappers `tools/corpus/{buildManifest,seed,verify,synth}.ts`; thresholds lines 85 / branches 85), and every covered file the task touched is ≥ 85%. Keep logic out of entry files. | Task 2 |
| G5 Corpus unit | `npm run test:corpus` passes (real sample files, read-only) | Task 4 |
| G6 Live stack | `make down && make up && make fs-check && make trickle` (the self-checking trickle from Task 9: the `IMG_6941 1.HEIC` / `IMG_E6941.JPG` copy pair lands one in `unique/`, one in `copies/g-…/`) | Task 9 |
| G7 Corpus safety | `make corpus-verify` exits 0 | Task 11 |
| G8 E2E | `make test-smoke` (and `make test-standard` after Tasks 12 and 13) | Task 12 |

**If any gate fails,** fix it inside the current task with superpowers:systematic-debugging. Never weaken or skip a test to get past a gate. A test from an earlier task that fails now is a regression in the current task.

## Review Focus

1. **An EXIF-less export that looks like two different burst frames** must never put both frames into one copy group. The export is flagged `copyAmbiguous` and is never chosen as a best copy. Pinned by `bridge export never merges conflicting shots` (Task 3) and the e2e burst check (Task 12).
2. **Re-dropping a file that was already ingested** (same path, size and mtime) gets a new `files` row and ends as `COPY`. It must not be stuck in the drop zone or wrongly marked `vanished`. Pinned by `re-drop after ingest becomes COPY` (Task 6).
3. **An ingest activity re-run after its commit but before Temporal recorded completion** (a crash at that moment) returns the same outcome, without creating a second row. Pinned by `re-run after commit returns same outcome` (Task 6).
4. **A Postgres or Temporal outage of several minutes while images are being hashed** makes the work wait. It never marks content `FAILED`, because only `decodeContent` has an attempt limit. Pinned by `recordHash outage never fails content` (Task 9).
5. **Opening a staged file for writing** (an editor saving in place) must fail without changing the vault original. Pinned by `staged file is read-only` (Task 8).

---

## File Structure

```
package.json              scripts: build, typecheck, lint, format:check, test, test:coverage, test:corpus, test:e2e, test:chaos
tsconfig.json             strict ESM NodeNext, noEmit; include src, tests, tools, *.config.ts, eslint.config.js
tsconfig.build.json       extends tsconfig.json; rootDir src, outDir dist, noEmit false (used by npm run build)
vitest.config.ts          projects: unit (tests/unit, excluding *.corpus.test.ts), workflow (tests/workflow),
                          corpus (tests/**/*.corpus.test.ts), e2e (tests/e2e), chaos (tests/chaos)
eslint.config.js, .prettierrc
Dockerfile                node:24-bookworm-slim + perl (for exiftool-vendored); non-root; npm ci; tsc build
docker-compose.yml        postgres, temporal-admin, temporal, temporal-ns, temporal-ui, migrate, worker, worker-extra, grafana
docker-compose.test.yml   override: ephemeral 127.0.0.1 ports (ports: !override), DATA_DIR from env
Makefile                  up down logs lint test fs-check stage retry-failed verify-vault corpus-* test-smoke test-standard chaos update-golden
.env.example
infra/postgres/init.sql   creates DBs pixelmagic, temporal, temporal_visibility + read-only role pm_reader
infra/grafana/            provisioning/{datasources,dashboards}/*.yml, dashboards/pipeline-live.json (.gitkeep placeholders from Task 2)
migrations/0001_initial.sql
src/
  config.ts               Settings, loadSettings()
  logger.ts               pino instance
  types.ts                payload types shared by workflows and activities (type-only)
  ids.ts                  fileWorkflowId
  schedules.ts            ensureSchedules
  fsops.ts                DataLayout, checkSameDevice, vaultRel, sha256File, linkIntoVault, isIgnored
  fscheck.ts              entry: proves link/rename/symlink-swap on the mount
  db.ts                   createPool, withTx
  hashing.ts              toSigned/toUnsigned, hamming, phashFromGray, dhashFromGray (pure)
  decode.ts               computeImageFacts (runs inside a workerpool child), QuarantineError
  decodeWorker.ts         workerpool child entry
  metadata.ts             Metadata, parseMetadata (pure), MetadataReader (exiftool-vendored)
  classify.ts             Facts, Thresholds, looksAlike, captureConflict, classify
  grouping.ts             Grouper (capture-consistent union-find), Assignment
  hashIndex.ts            HashIndex (typed arrays + match query)
  stagingPlan.ts          StageContent, StageFile, Entry, bestCopyKey, pairLivePhotos, planStaging
  activities/
    index.ts              createActivities(deps) → the Activities object; Deps type
    ingest.ts, hashing.ts, cluster.ts, staging.ts, scan.ts
  workflows.ts            FileWorkflow, ContentWorkflow, ScanWorkflow, StagingWorkflow
  worker.ts               entry: workers per queue, ensureSchedules, startup checks
  cli.ts                  entry: stage | retry-failed | verify-vault (node:util parseArgs)
tools/corpus/             lib.ts, buildManifest.ts, seed.ts, verify.ts, synth.ts (run with tsx)
tests/
  helpers/                pg.ts (testcontainers + migrate + truncate), layout.ts (tmpDir), facts.ts (F/T/NOEXIF builders), images.ts (sharp-generated fixtures),
                          activityEnv.ts (runActivity, testDeps), hangWorker.ts (decode child that never resolves), corpus.ts
  unit/ …                 one file per module (*.test.ts); corpus-dependent ones are *.corpus.test.ts
  workflow/workflows.test.ts
  lib/                    stack.ts, invariant.ts, expectations.ts, snapshot.ts
  e2e/                    smoke.test.ts, standard.test.ts, golden/smoke_tree.txt
  chaos/chaos.test.ts
  corpus/manifest.json    generated by make corpus-manifest; git-ignored (holds personal capture metadata)
```

---

### Task 1: Scaffold, config and filesystem guarantees

**Files:**
- Create: `package.json`, `tsconfig.json`, `tsconfig.build.json`, `vitest.config.ts`, `eslint.config.js`, `.prettierrc`, `Dockerfile`, `.dockerignore`, `Makefile`, `.env.example`, `README.md`, `src/config.ts`, `src/logger.ts`, `src/fsops.ts`, `src/fscheck.ts`, `tests/helpers/layout.ts`
- Modify: `.gitignore` (add `node_modules/`, `dist/`, `coverage/`; `.superpowers/` is already there, don't add it twice)
- Test: `tests/unit/config.test.ts`, `tests/unit/fsops.test.ts`

**Interfaces:**
- Produces:
  - `type Settings` with the camelCase fields from Global Constraints; `workerQueues: string[]`
  - `loadSettings(env?: NodeJS.ProcessEnv): Settings`
  - `class DataLayout { constructor(root: string) }` with readonly `root`, `dropzone`, `claimed`, `vault`, `vaultCopies` and `staging`, a method `ensure(): Promise<void>`, and `rel(abs: string): string`
  - `checkSameDevice(layout: DataLayout): Promise<void>`, which throws `SameDeviceError`
  - `vaultRel(sha256: string, ext: string): string`
  - `sha256File(path: string, onChunk?: () => void): Promise<string>` (1 MiB chunks)
  - `type LinkOutcome = 'LINKED' | 'ALREADY_LINKED' | 'EXISTS_OTHER'`
  - `linkIntoVault(src: string, dest: string): Promise<LinkOutcome>`. It creates parent dirs, fsyncs the dest dir, and uses `fs.link`. On `EEXIST` it compares `dev` and `ino`.
  - `isIgnored(name: string): boolean`
  - Test helper `tmpDir(): Promise<string>` in `tests/helpers/layout.ts` (`mkdtemp` under `os.tmpdir()`; removed in `afterAll`)
  - `package.json` scripts: `build` (`tsc -p tsconfig.build.json`), `typecheck` (`tsc --noEmit`), `lint`, `format:check`, `test` (`vitest run --project unit --project workflow`), `test:coverage`, `test:corpus` (`vitest run --project corpus`). Tasks 12/13 add `test:e2e`/`test:chaos`. `node-pg-migrate` (v8, ESM-only) goes in `dependencies`, not `devDependencies`, because the `migrate` service runs it from the runtime image after `npm ci --omit=dev`.
  - `vitest.config.ts` coverage block: provider `v8`, `include: ['src/**', 'tools/corpus/lib.ts']`, `exclude: ['src/fscheck.ts', 'src/worker.ts', 'src/decodeWorker.ts', 'tools/corpus/{buildManifest,seed,verify,synth}.ts']`, thresholds lines 85 / branches 85

- [ ] **Step 1: Write the failing tests**

```ts
test('settings defaults', () => {
  const s = loadSettings({ DATABASE_URL: 'postgres://x' });
  expect([s.scanIntervalS, s.stableSeconds, s.maxInFlight]).toEqual([30, 5, 5000]);
  expect([s.lookAlikePhash, s.lookAlikeDhash, s.copyPhash, s.copyDhash]).toEqual([8, 10, 4, 6]);
  expect(s.workerQueues).toEqual(['main', 'io', 'cpu', 'cluster']);
});
test('missing DATABASE_URL throws', () => expect(() => loadSettings({})).toThrow(/DATABASE_URL/));

test('vaultRel', () => expect(vaultRel('abcd' + '0'.repeat(60), 'heic')).toBe(`vault/ab/cd/abcd${'0'.repeat(60)}.heic`));

test.each([['.DS_Store', true], ['._IMG_1.HEIC', true], ['a.part', true], ['a.crdownload', true], ['~$lock', true],
           ['IMG_2834 1.HEIC', false], ['café.jpg', false]])('isIgnored(%s)', (n, want) => expect(isIgnored(n)).toBe(want));

test('linkIntoVault outcomes', async () => {
  const d = await tmpDir(); await writeFile(`${d}/a`, '1'); await writeFile(`${d}/b`, '1');
  const dest = `${d}/v/ab/cd/x.jpg`;
  expect(await linkIntoVault(`${d}/a`, dest)).toBe('LINKED');
  expect(await linkIntoVault(`${d}/a`, dest)).toBe('ALREADY_LINKED');
  expect(await linkIntoVault(`${d}/b`, dest)).toBe('EXISTS_OTHER');
  expect((await stat(`${d}/a`)).ino).toBe((await stat(dest)).ino);
});

test('sha256File streams and calls onChunk per MiB', async () => {
  const p = `${await tmpDir()}/f`; const buf = Buffer.alloc(3 << 20, 'x'); await writeFile(p, buf);
  let calls = 0;
  expect(await sha256File(p, () => calls++)).toBe(createHash('sha256').update(buf).digest('hex'));
  expect(calls).toBe(3);
});

test('checkSameDevice passes on one fs', async () => {
  const l = new DataLayout(await tmpDir()); await l.ensure(); await expect(checkSameDevice(l)).resolves.toBeUndefined();
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm install && npx vitest run --project unit`
Expected: FAIL with `Cannot find module '../../src/config.js'`

- [ ] **Step 3: Implement the scaffold, `config.ts`, `logger.ts`, `fsops.ts` and `fscheck.ts`**

- **`fscheck.ts`:** in a temp dir under `DATA_DIR`, run `link`, `rename`, and a symlink swap (`symlink` to `.current.tmp`, then `rename` over an existing symlink). Check that the inode is shared. Print `fs-check OK` and exit 0, otherwise exit 1.
- **`tsconfig.json` / `tsconfig.build.json`:** see Global Constraints. ESLint's typed rules use `tsconfig.json` (`parserOptions.project`), which includes `eslint.config.js` and `vitest.config.ts`, so typed lint doesn't error on the config files.
- **`Dockerfile`:** two-stage. The build stage runs `npm ci && npm run build`. The runtime stage is `node:24-bookworm-slim` plus `apt-get install perl`, runs `npm ci --omit=dev`, copies `dist/` and `migrations/`, and creates the user with `getent group $GID || groupadd -g $GID pm` then `useradd -u $UID -g $GID -m pm` (GID 20 is `dialout` in Debian; `groupadd -g 20` would fail).
- **`Makefile`:** `test` = `npm test`; `fs-check` = `docker compose run --rm worker node dist/fscheck.js`. Other targets may be stubs until their task.

- [ ] **Step 4: Run gates G1–G3** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && test -f dist/fscheck.js`
Expected: PASS, no errors, and `dist/fscheck.js` exists (not `dist/src/…`)

- [ ] **Step 5: Verify the guarantees on the real Docker Desktop bind mount**

Run: `mkdir -p data && docker build -t pixelmagic . && docker run --rm -v "$PWD/data:/data" pixelmagic node dist/fscheck.js`
Expected: `fs-check OK`. If it fails, stop: the one-filesystem design does not hold on this mount.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: TypeScript scaffold, config, filesystem primitives and fs-check"
```

---

### Task 2: Infrastructure, schema and migrations

**Files:**
- Create: `docker-compose.yml`, `docker-compose.test.yml`, `infra/postgres/init.sql`, `migrations/0001_initial.sql`, `src/db.ts`, `tests/helpers/pg.ts`, `infra/grafana/provisioning/datasources/.gitkeep`, `infra/grafana/provisioning/dashboards/.gitkeep`, `infra/grafana/dashboards/.gitkeep` (so compose's Grafana mounts exist before Task 10 fills them; otherwise Docker creates empty root-owned dirs)
- Test: `tests/unit/schema.test.ts`

**Interfaces:**
- Consumes: `Settings.databaseUrl`
- Produces:
  - Schema (spec §5 with the §12 revisions; column names snake_case in SQL):
    - `files(id bigserial, rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state, workflow_run_id UNIQUE, attempts, last_error, discovered_at, updated_at)`
    - `contents(sha256 PK, ext, kind, mime, size_bytes, width, height, phash, dhash, captured_at, subsec, camera, burst_uuid, live_content_id, duration_s, state, quarantine_reason, copy_group_id, copy_ambiguous bool default false, last_error, ingested_at, hashed_at, clustered_at, updated_at)`
    - `groups(id, kind, merged_into, similar_set_id)`
    - `cluster_state(id=1, version bigint default 0)` (seeded)
    - `scan_stats(id=1, pending_files, pending_bytes, scanned_at)` (seeded)
    - `staging_builds(gen PK, watermark timestamptz, cluster_version bigint NOT NULL DEFAULT 0, built_at, entries)`
  - CHECK constraints for every state column. The partial unique index is on `files(rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED')`. An `updated_at` trigger on `files` and `contents` sets `updated_at = clock_timestamp()` (not `now()`, which is the transaction start and can hide a long transaction's commit behind a staging watermark).
  - The migration file starts with the `-- Up Migration` marker that node-pg-migrate requires for SQL migrations (optionally followed by `-- Down Migration`).
  - `createPool(url: string, max?: number): pg.Pool`, with the int8 → `BigInt` parser installed
  - `withTx<T>(pool: pg.Pool, fn: (c: pg.PoolClient) => Promise<T>): Promise<T>`
  - Test helper `startPg(): Promise<{ url: string; pool: pg.Pool; truncate(): Promise<void>; stop(): Promise<void> }>`: testcontainers `new PostgreSqlContainer('postgres:16')` with migrations applied through the node-pg-migrate v8 (ESM) API `runner({ databaseUrl, dir: 'migrations', direction: 'up', migrationsTable: 'pgmigrations', log: () => {} })`. It is started once per test file in `beforeAll`. `truncate()` runs `TRUNCATE files, contents, groups, staging_builds RESTART IDENTITY` and then `UPDATE`s the singleton rows back to their defaults (`cluster_state.version = 0`; `scan_stats` pending counts 0, `scanned_at` NULL). It never deletes the singleton rows.

- [ ] **Step 1: Write the failing tests**

```ts
const ins = `INSERT INTO files(rel_path, original_name, size_bytes, mtime_ns, state) VALUES ('a.jpg','a.jpg',1,1,$1)`;
test('partial unique allows re-drop after final', async () => {
  await pool.query(ins, ['INGESTED']);
  await pool.query(ins, ['DISCOVERED']);                                    // earlier row is final
  await expect(pool.query(ins, ['CLAIMED'])).rejects.toMatchObject({ code: '23505' });
});
test('state CHECK rejects unknown', async () =>
  expect(pool.query(ins, ['NOPE'])).rejects.toMatchObject({ code: '23514' }));
test('bigint holds signed phash and parses as BigInt', async () => {
  await pool.query(`INSERT INTO contents(sha256, ext, kind, mime, size_bytes, state, phash) VALUES ($1,'jpg','still','image/jpeg',1,'HASHED',$2)`,
                   ['0'.repeat(64), (-(2n ** 63n)).toString()]);
  expect((await pool.query('SELECT phash FROM contents')).rows[0].phash).toBe(-(2n ** 63n));
});
test('singletons seeded', async () =>
  expect((await pool.query('SELECT version FROM cluster_state WHERE id=1')).rows[0].version).toBe(0n));
test('updated_at trigger bumps', async () => {
  const { rows: [r] } = await pool.query(`${ins} RETURNING id, updated_at`, ['DISCOVERED']);
  await pool.query('SELECT pg_sleep(0.01)');
  const { rows: [u] } = await pool.query(`UPDATE files SET state = 'CLAIMED' WHERE id = $1 RETURNING updated_at`, [r.id]);
  expect(u.updated_at.getTime()).toBeGreaterThan(r.updated_at.getTime());
});
test('updated_at uses clock time, not transaction start', async () => {
  const [start, stamped] = await withTx(pool, async (c) => {
    const t0 = (await c.query('SELECT now() AS t')).rows[0].t as Date; await c.query('SELECT pg_sleep(0.05)');
    return [t0, (await c.query(`${ins} RETURNING updated_at`, ['DISCOVERED'])).rows[0].updated_at as Date];
  });
  expect(stamped.getTime() - start.getTime()).toBeGreaterThanOrEqual(40);
});
test('staging_builds has cluster_version', async () => {
  await pool.query(`INSERT INTO staging_builds(gen, watermark, built_at, entries) VALUES (1, now(), now(), 0)`);
  expect((await pool.query('SELECT cluster_version FROM staging_builds')).rows[0].cluster_version).toBe(0n);
});
test('truncate keeps singletons', async () => {
  await pool.query('UPDATE cluster_state SET version = 7'); await truncate();
  expect((await pool.query('SELECT version FROM cluster_state WHERE id=1')).rows[0].version).toBe(0n);
  expect((await pool.query('SELECT count(*)::int AS n FROM scan_stats WHERE id=1')).rows[0].n).toBe(1);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/schema.test.ts`
Expected: FAIL (`relation "files" does not exist`, or the helper is missing)

- [ ] **Step 3: Implement the migration, `db.ts`, the pg helper and compose**

- **`init.sql`** (runs only on an empty volume, before any table exists): creates DBs `pixelmagic`, `temporal`, `temporal_visibility` and the login role `pm_reader`. Then, for `pixelmagic` and `temporal_visibility` (each via `\connect`): `GRANT CONNECT ON DATABASE … TO pm_reader`, `GRANT USAGE ON SCHEMA public TO pm_reader`, and `ALTER DEFAULT PRIVILEGES FOR ROLE <POSTGRES_USER> IN SCHEMA public GRANT SELECT ON TABLES TO pm_reader` (a plain `GRANT SELECT ON ALL TABLES` would grant nothing here). `init.sql` alone owns database creation.
- **Compose:**
  - `postgres:16` with `init.sql`, a `pg_isready` healthcheck and the `pg-data` volume.
  - `temporal-admin` (`temporalio/admin-tools`, one-shot, depends on postgres healthy) runs only `temporal-sql-tool … setup-schema -v 0.0` and `update-schema -d <schema dir>` for `temporal` and `temporal_visibility` with the `postgres12` plugin (both are safe to re-run). It does **not** run `create-database`.
  - `temporal` (`temporalio/server`, pinned to the current stable 1.x tag, `DB=postgres12`) depends on `temporal-admin` completing successfully. Healthcheck: `temporal operator cluster health --address temporal:7233` (interval 5s, retries 30). If the pinned server tag ships no `temporal` CLI, use `tctl --address temporal:7233 cluster health` and note the tag in the report.
  - `temporal-ns` (`temporalio/admin-tools`, one-shot, depends on `temporal` healthy): `temporal operator namespace describe default --address temporal:7233 || temporal operator namespace create default --address temporal:7233`. The plain `temporalio/server` image does not register `default`.
  - `temporal-ui`.
  - `migrate` runs the app image with `npx node-pg-migrate up -m migrations` (`node-pg-migrate` is a runtime dependency).
  - `worker` (`WORKER_QUEUES=main,io,cpu,cluster`) and `worker-extra` (`WORKER_QUEUES=main,io,cpu`, `deploy.replicas: 0`) both depend on `temporal-ns` completing and `migrate` completing, mount `${DATA_DIR:-./data}:/data`, set `environment: DATA_DIR: /data` explicitly (the host `${DATA_DIR}` is used only on the volume's host side, so an `env_file` can't leak the host path), and set `stop_grace_period: 60s`.
  - `grafana` mounts `infra/grafana/provisioning` and `infra/grafana/dashboards`. Every long-running service gets `restart: unless-stopped`.
- **`docker-compose.test.yml`:** remaps every published port with `ports: !override ["127.0.0.1::<port>"]` (a plain `ports:` in an override is concatenated with the base file's fixed ports; `!override` needs Compose ≥ 2.24.4 — check `docker compose version`).

- [ ] **Step 4: Run gates G1–G4** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage`
Expected: PASS, coverage thresholds met

- [ ] **Step 5: Verify the stack comes up**

Run: `docker compose up -d --wait postgres temporal temporal-ui && docker compose up temporal-ns migrate --exit-code-from migrate && docker compose ps -a && docker compose run --rm temporal-ns temporal operator namespace describe default --address temporal:7233 && curl -sf http://127.0.0.1:8080 >/dev/null && echo UI-OK`
Expected: postgres and temporal are healthy, temporal-ns and migrate exited 0, namespace `default` is described, and `UI-OK` is printed

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: compose infrastructure, Temporal on Postgres, app schema"
```

---

### Task 3: Dedupe core — classify and capture-consistent grouping

**Files:**
- Create: `src/classify.ts`, `src/grouping.ts`, `src/hashing.ts` (only `hamming`, `toSigned` and `toUnsigned` here; Task 4 adds the rest), `tests/helpers/facts.ts`
- Test: `tests/unit/classify.test.ts`, `tests/unit/grouping.test.ts`, `tests/unit/hashing.test.ts`

**Interfaces:**
- Produces:
  - `hamming(a: bigint, b: bigint): number`, `toSigned(u: bigint): bigint`, `toUnsigned(s: bigint): bigint`
  - `type CaptureKey = { s: number; sub: string | null }`
  - `interface Facts`: `sha256: string`, `phash: bigint`, `dhash: bigint` (both unsigned), `width: number`, `height: number`, `mime: string`, `captured: CaptureKey | null`, `camera: string | null`, `burstUuid: string | null`
  - `interface Thresholds`: `lookAlikePhash`, `lookAlikeDhash`, `copyPhash`, `copyDhash`; `DEFAULT_THRESHOLDS = { 8, 10, 4, 6 }`
  - `looksAlike(a: Facts, b: Facts, t: Thresholds): boolean`
  - `captureConflict(a: CaptureKey, b: CaptureKey): boolean`: true if the seconds differ, or if both `sub`s are non-null and differ
  - `classify(a: Facts, b: Facts, t: Thresholds): 'copy' | 'similar'`, the spec §6.3 table, evaluated top to bottom
  - `class Grouper { constructor(newId: (kind: 'copy' | 'similar') => number) }` with methods:
    - `load(row: { sha256: string; copyGroup: number; similarSet: number | null; captured: CaptureKey | null; ambiguous: boolean }): void`
    - `add(f: Facts, matches: ReadonlyArray<readonly [sha256: string, verdict: 'copy' | 'similar']>): Assignment`
    - `copyPartition(): Set<string>`, where each element is a sorted, comma-joined set of SHA-256s
    - `similarPartition(): Set<string>`: the connected components of the look-alike graph, including singletons
  - `interface Assignment`: `copyGroup: number`, `ambiguous: boolean`, `created: Array<[id: number, kind: 'copy' | 'similar']>`, `copyMerges: Array<[loser: number, winner: number]>`, `similarMerges: Array<[loser: number, winner: number]>`, `similarLinks: Map<number, number>` (copy group → similar set)
  - One `add` creates at most one copy group and one similar set (Task 7 relies on this to prefetch two ids).
  - Test helper `tests/helpers/facts.ts` exporting `T` (= `DEFAULT_THRESHOLDS`), `F(sha, overrides?)` and `NOEXIF` exactly as below. Task 7's tests import it; don't redefine them.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/helpers/facts.ts
export const T = DEFAULT_THRESHOLDS;
export const F = (sha: string, o: Partial<Facts> = {}): Facts => ({ sha256: sha, phash: 0n, dhash: 0n, width: 4000, height: 3000,
  mime: 'image/heic', captured: { s: 1_600_000_000, sub: '123' }, camera: 'Apple iPhone 12', burstUuid: null, ...o });
export const NOEXIF = { captured: null, camera: null } as const;

// classify.test.ts / grouping.test.ts: import { F, T, NOEXIF } from '../helpers/facts.js'

test.each<[Facts, Facts, 'copy' | 'similar']>([
  [F('a', { burstUuid: 'B1' }), F('b', { phash: 1n, burstUuid: 'B1' }), 'similar'],                 // same burst → never copy
  [F('a'), F('b', { captured: { s: 1_600_000_001, sub: '123' } }), 'similar'],                     // different second
  [F('a'), F('b', { captured: { s: 1_600_000_000, sub: '456' } }), 'similar'],                     // different subsec
  [F('a'), F('b', { camera: 'Canon' }), 'similar'],                                                // different camera
  [F('a'), F('b', { phash: 0b111n, width: 2000, height: 1500, mime: 'image/jpeg' }), 'copy'],      // same capture incl. subsec
  [F('a', { captured: { s: 1, sub: null } }), F('b', { phash: 3n, captured: { s: 1, sub: null } }), 'similar'], // no subsec, same dims+mime
  [F('a', { captured: { s: 1, sub: null } }), F('b', { phash: 3n, width: 2000, height: 1500, captured: { s: 1, sub: null } }), 'copy'],
  [F('a'), F('b', { phash: 0b1111n, dhash: 0b111111n, ...NOEXIF }), 'copy'],                     // EXIF-less export, tight
  [F('a'), F('b', { phash: 0b11111n, ...NOEXIF }), 'similar'],                                     // EXIF-less, not tight
  [F('a', NOEXIF), F('b', { phash: 1n, ...NOEXIF }), 'similar'],                                   // both EXIF-less, same dims
  [F('a', NOEXIF), F('b', { phash: 1n, width: 10, height: 10, ...NOEXIF }), 'copy'],
])('classify %#', (a, b, want) => expect(classify(a, b, T)).toBe(want));

test('looksAlike bounds', () => {
  expect(looksAlike(F('a'), F('b', { phash: 0xffn, dhash: 0x3ffn }), T)).toBe(true);
  expect(looksAlike(F('a'), F('b', { phash: 0x1ffn }), T)).toBe(false);
});

test('toSigned/toUnsigned round-trip', () => fc.assert(fc.property(fc.bigInt({ min: 0n, max: 2n ** 64n - 1n }), (u) =>
  toUnsigned(toSigned(u)) === u && toSigned(u) >= -(2n ** 63n) && toSigned(u) < 2n ** 63n)));
test('hamming', () => { expect(hamming(0n, 2n ** 64n - 1n)).toBe(64); expect(hamming(0b1011n, 0b0001n)).toBe(2); });

test('bridge export never merges conflicting shots', () => {
  const facts = { A: F('A', { captured: { s: 1, sub: '1' } }), B: F('B', { captured: { s: 2, sub: '1' } }), X: F('X', NOEXIF) };
  const verdict = (p: string, q: string) => ([p, q].sort().join('') === 'AB' ? 'similar' : 'copy') as const;
  for (const order of permutations(['A', 'B', 'X'] as const)) {
    const g = new Grouper(counter()); const seen: string[] = [];
    for (const s of order) { g.add(facts[s], seen.map((o) => [o, verdict(s, o)] as const)); seen.push(s); }
    expect([...g.copyPartition()].some((grp) => grp.includes('A') && grp.includes('B'))).toBe(false);
    expect(g.similarPartition()).toEqual(new Set(['A,B,X']));
  }
});

test('partitions are order-independent without conflicts', () => fc.assert(fc.property(
  fc.shuffledSubarray(range(12), { minLength: 12, maxLength: 12 }), (perm) => {
    const g = runGrouper(EDGES, FACTS, perm);                    // fixture: 3 scenes × (2 burst frames + 1 resized copy of each frame) = 12
    expect(g.similarPartition()).toEqual(EXPECTED_SIMILAR);
    expect(g.copyPartition()).toEqual(EXPECTED_COPY);
  })));
```

`permutations`, `counter`, `range`, `runGrouper`, `EDGES`, `FACTS` and the `EXPECTED_*` constants are defined in the test module. `fc.bigInt({ min, max })` is used because fast-check v4 removed `fc.bigUintN`. Expected: `EXPECTED_COPY` = 6 copy groups (each frame with its resized copy), `EXPECTED_SIMILAR` = 3 sets (one per scene).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/classify.test.ts tests/unit/grouping.test.ts tests/unit/hashing.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement `classify.ts`, `grouping.ts` and the `hashing.ts` basics**

`Grouper.add` follows this algorithm:

```
copyTargets = distinct root copy groups of matches with verdict 'copy'
group = new copy group
for target of copyTargets sorted by (size desc, id asc):
    if captures(group) ∪ captures(target) ∪ {f.captured} holds two conflicting keys → refuse; ambiguous = true
    else union(group, target): smaller → larger, tie → lower id wins; record copyMerges
similarTargets = root copy groups of every match (copy or similar) not equal to `group`
if any: union their similar sets and group's into one (create if none); record similarMerges / similarLinks
```

Here `captures(group)` is the set of non-null capture keys of its members. `hamming` uses popcount on two 32-bit halves.

- [ ] **Step 4: Run gates G1–G4** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage`
Expected: PASS (fast-check runs 100 shuffles)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: copy/similar classifier and capture-consistent grouping"
```

---

### Task 4: Image decoding and perceptual hashes

**Files:**
- Create: `src/decode.ts`, `src/decodeWorker.ts`, `tests/helpers/images.ts` (sharp-generated JPEG/PNG/WebP fixtures, truncated and corrupt variants, a decompression-bomb PNG), `tests/helpers/corpus.ts` (`C(name)` path helper, `hasCorpus`, `corpusNames(filter)`)
- Modify: `src/hashing.ts` (add `phashFromGray` and `dhashFromGray`)
- Test: `tests/unit/hashing.test.ts`, `tests/unit/decode.test.ts`, `tests/unit/decode.corpus.test.ts`

**Interfaces:**
- Consumes: `hamming` (Task 3); `tmpDir` (Task 1). RAW preview extraction calls `exiftool-vendored` directly (`extractPreview(src, dest)`); `MetadataReader` (Task 5) is not used here.
- Produces:
  - `phashFromGray(px: Uint8Array /* 32×32 */): bigint`, `dhashFromGray(px: Uint8Array /* 9×8 */): bigint`. Pure, row-major, first bit = MSB.
  - `interface ImageFacts`: `width: number`, `height: number`, `phash: bigint`, `dhash: bigint`, `mime: string`
  - `class QuarantineError extends Error { constructor(readonly reason: 'decode_error' | 'truncated' | 'decompression_bomb' | 'unsupported') }`, which sets `this.name = 'QuarantineError'` (an own property, so it survives workerpool's error serialization; `instanceof` does not survive and callers across the pool must test `name`/`reason`)
  - `computeImageFacts(absPath: string, ext: string): Promise<ImageFacts>`
  - `type WireFacts = { width: number; height: number; phash: string; dhash: string; mime: string }` and `computeImageFactsWire(absPath: string, ext: string): Promise<WireFacts>` (hashes as unsigned decimal strings; workerpool process workers use JSON over `process.send`, which can't carry `bigint`)
  - `endDecodeResources(): Promise<void>`: ends the lazily created RAW-preview `ExifTool` if one exists
  - `decodeWorker.ts` (entry, no logic): `workerpool.worker({ computeImageFacts: computeImageFactsWire }, { onTerminate: endDecodeResources })`. The method is registered under the name `computeImageFacts`; the caller converts the strings back with `BigInt`.
  - In `tests/helpers/images.ts`: `photo(dir, ext, opts?: { scale?: number; size?: [number, number]; scene?: number; orientation?: 6 }): Promise<string>`, `truncatedJpeg`, `randomBytes`, `bombPng` (each `(dir) => Promise<string>`), and `JPEG_A: Buffer` (a small fixed JPEG used by Tasks 6 and 9). `orientation: 6` stores the pixels rotated 270° and sets EXIF Orientation 6, so the displayed image equals the unrotated photo.

- [ ] **Step 1: Write the failing tests**

```ts
test('phash of known gradient is stable (golden)', () => expect(phashFromGray(gradient32())).toBe(GOLDEN_PHASH));
test('dhash bit order', () => expect(dhashFromGray(rampRows9x8())).toBe(0xffffffffffffffffn));

test('hash stable across format and size', async () => {
  const d = await tmpDir(); const base = await computeImageFacts(await photo(d, 'jpg'), 'jpg');
  for (const [p, e] of [[await photo(d, 'png'), 'png'], [await photo(d, 'jpg', { scale: 0.5 }), 'jpg'],
                        [await photo(d, 'webp'), 'webp']] as const)
    expect(hamming(base.phash, (await computeImageFacts(p, e)).phash)).toBeLessThanOrEqual(4);
});
test('EXIF orientation applied', async () => {
  const d = await tmpDir(); const up = await computeImageFacts(await photo(d, 'jpg'), 'jpg');
  const tagged = await computeImageFacts(await photo(d, 'jpg', { orientation: 6 }), 'jpg');
  expect(hamming(up.phash, tagged.phash)).toBeLessThanOrEqual(4); expect([tagged.width, tagged.height]).toEqual([up.width, up.height]);
});
test('dimensions are oriented original, not the 32×32 resize', async () => {
  const f = await computeImageFacts(await photo(await tmpDir(), 'jpg', { size: [4032, 3024] }), 'jpg');
  expect([f.width, f.height]).toEqual([4032, 3024]);
});
test('different photos are far apart', async () => {
  const d = await tmpDir();
  const [a, b] = [await computeImageFacts(await photo(d, 'jpg', { scene: 1 }), 'jpg'), await computeImageFacts(await photo(d, 'jpg', { scene: 2 }), 'jpg')];
  expect(hamming(a.phash, b.phash)).toBeGreaterThan(16);
});
test('wire form carries hashes as decimal strings', async () => {
  const p = await photo(await tmpDir(), 'png'); const [f, w] = [await computeImageFacts(p, 'png'), await computeImageFactsWire(p, 'png')];
  expect(w).toEqual({ ...f, phash: f.phash.toString(), dhash: f.dhash.toString() });
});
test('QuarantineError keeps name and reason as own properties', () => {
  const e = new QuarantineError('truncated');
  expect(Object.keys(e)).toEqual(expect.arrayContaining(['name', 'reason'])); expect([e.name, e.reason]).toEqual(['QuarantineError', 'truncated']);
});
test.each([[truncatedJpeg, 'truncated'], [randomBytes, 'decode_error'], [bombPng, 'decompression_bomb']] as const)(
  'quarantine reason %#', async (make, reason) =>
    expect(computeImageFacts(await make(await tmpDir()), 'jpg')).rejects.toMatchObject({ reason }));

// decode.corpus.test.ts — describe.skipIf(!hasCorpus)
test('HEIC and its JPG twin hash within copy range', async () =>
  expect(hamming((await computeImageFacts(C('IMG_0034.HEIC'), 'heic')).phash,
                 (await computeImageFacts(C('IMG_0034.JPG'), 'jpg')).phash)).toBeLessThanOrEqual(4));
test('misnamed JPEG decodes', async () => expect((await computeImageFacts(C('IMG_E8719.HEIC'), 'jpg')).mime).toBe('image/jpeg'));
test.each([['VERP1022.TIF', 'tif'], ['AUXA4949.BMP', 'bmp']])('%s decodes', async (n, e) => {
  const f = await computeImageFacts(C(n), e); expect(f.width).toBeGreaterThan(0); expect(f.height).toBeGreaterThan(0);
});
test('HEIC decode benchmark', async () => {
  const names = corpusNames((n) => n.endsWith('.HEIC') && !n.startsWith('IMG_E')).slice(0, 20); const ms: number[] = [];
  for (const n of names) { const t = performance.now(); await computeImageFacts(C(n), 'heic'); ms.push(performance.now() - t); }
  console.log(`median HEIC ms: ${median(ms)}`);
  expect(names).toHaveLength(20); expect(median(ms)).toBeLessThanOrEqual(1500);
}, 120_000);
```

`gradient32`, `rampRows9x8` and `median` are defined in the test modules. `afterAll(endDecodeResources)` in both decode test files.

`GOLDEN_PHASH` is computed once from the first correct implementation, cross-checked by hand against the algorithm in spec §6.2, and committed. It locks the bit layout.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/hashing.test.ts tests/unit/decode.test.ts`
Expected: FAIL with `Cannot find module` / `phashFromGray is not a function`

- [ ] **Step 3: Implement `phashFromGray`, `dhashFromGray` and `computeImageFacts`**

Follow spec §6.2:
- `sharp(input, { limitInputPixels: 200_000_000, failOn: 'truncated' }).rotate()`, then `.removeAlpha().toColourspace('b-w').resize(32, 32, { fit: 'fill' }).raw()` for pHash and the same with `resize(9, 8, { fit: 'fill' })` for dHash. (`greyscale()` alone keeps an alpha channel, so RGBA input — always the case for heic-decode — yields 2 channels; `resize` without `fit: 'fill'` defaults to `cover` and crops.) Assert `info.channels === 1` and `data.length === w * h`, else throw. Compute `GOLDEN_PHASH` only after this pipeline is in place.
- HEIC/HEIF: `heic-decode` → `sharp(data, { raw: { width, height, channels: 4 } })`.
- RAW extensions: `extractPreview` into a temp file under `os.tmpdir()` (not `DATA_DIR`), removed afterwards. The `ExifTool` instance is created lazily on the first RAW file as `new ExifTool({ maxProcs: 1 })`, so non-RAW decode children start no perl process; `endDecodeResources()` ends it.
- BMP: sharp's prebuilt libvips has no BMP loader. Decode uncompressed BMP in-process (a small header parser or a pure-JS BMP decoder dependency) and hand the raw pixels to sharp; anything it can't parse is `unsupported`.
- Map sharp errors by message: "pixel limit" → `decompression_bomb`; "Premature end" or "truncated" → `truncated`; "unsupported image format" → `decode_error`. Unknown extensions/MIME → `unsupported`.
- DCT: a straightforward separable 2-D DCT-II over 32×32 is fine.

- [ ] **Step 4: Run gates G1–G5** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus`
Expected: PASS. Record the benchmark's median HEIC ms in the report.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: sharp/heic decode with pHash/dHash and quarantine reasons"
```

---

### Task 5: Metadata via exiftool

**Files:**
- Create: `src/metadata.ts`
- Test: `tests/unit/metadata.test.ts`, `tests/unit/metadata.corpus.test.ts`

**Interfaces:**
- Produces:
  - `interface Metadata`: `fileType: string`, `mime: string`, `ext: string`, `kind: 'still' | 'motion' | 'other'`, `capturedAt: Date | null`, `subsec: string | null`, `camera: string | null`, `burstUuid: string | null`, `liveContentId: string | null`, `durationS: number | null`
  - `parseMetadata(tags: Record<string, unknown>): Metadata`, which is pure. Its keys are exiftool `-G -n` names: `File:FileType`, `File:MIMEType`, `File:FileTypeExtension`, `EXIF:DateTimeOriginal`, `EXIF:OffsetTimeOriginal`, `EXIF:SubSecTimeOriginal`, `EXIF:Make`, `EXIF:Model`, `MakerNotes:BurstUUID`, `MakerNotes:ContentIdentifier`, `QuickTime:ContentIdentifier`, `QuickTime:Duration` and `QuickTime:CreateDate`.
  - `class MetadataReader` with methods `read(absPath: string): Promise<Metadata>` and `end(): Promise<void>`. `read` uses `exiftool.readRaw` with `-G -n` (not `exiftool.read()`, which returns post-processed tags with no `Group:` prefixes and `ExifDateTime` objects). Check the argument form of the installed exiftool-vendored major: `readRaw(path, ['-G', '-n'])` in older majors, `readRaw(path, { readArgs: ['-G', '-n'] })` in newer ones.
  - `Metadata.mime` (exiftool `MIMEType`) is the **only** source of `contents.mime` (Task 6 writes it at ingest; sharp's mime is never stored).
- Consumes: `C`, `hasCorpus` from `tests/helpers/corpus.ts` (Task 4)

- [ ] **Step 1: Write the failing tests**

```ts
const heic = { 'File:FileType': 'HEIC', 'File:MIMEType': 'image/heic', 'File:FileTypeExtension': 'HEIC',
  'EXIF:DateTimeOriginal': '2021:10:03 14:02:11', 'EXIF:OffsetTimeOriginal': '-07:00', 'EXIF:SubSecTimeOriginal': 482,
  'EXIF:Make': 'Apple', 'EXIF:Model': 'iPhone 12', 'MakerNotes:BurstUUID': 'B-1', 'MakerNotes:ContentIdentifier': 'C-1' };
test('HEIC still', () => {
  const m = parseMetadata(heic);
  expect([m.kind, m.ext, m.subsec, m.camera, m.burstUuid, m.liveContentId]).toEqual(['still', 'heic', '482', 'Apple iPhone 12', 'B-1', 'C-1']);
  expect(m.capturedAt?.toISOString()).toBe('2021-10-03T21:02:11.000Z');
});
test('camera dedupes make', () => expect(parseMetadata({ ...heic, 'EXIF:Model': 'Apple iPhone 12' }).camera).toBe('Apple iPhone 12'));
test('misnamed JPEG uses content type', () =>
  expect(parseMetadata({ 'File:FileType': 'JPEG', 'File:MIMEType': 'image/jpeg', 'File:FileTypeExtension': 'JPG' }).ext).toBe('jpg'));
test('Live Photo MOV', () => {
  const m = parseMetadata({ 'File:FileType': 'MOV', 'File:MIMEType': 'video/quicktime', 'File:FileTypeExtension': 'MOV',
                            'QuickTime:ContentIdentifier': 'C-1', 'QuickTime:Duration': 2.97 });
  expect([m.kind, m.liveContentId, m.durationS]).toEqual(['motion', 'C-1', 2.97]);
});
test('AAE sidecar and empty tags are other', () => {
  expect(parseMetadata({ 'File:FileType': 'XMP', 'File:MIMEType': 'application/rdf+xml', 'File:FileTypeExtension': 'XMP' }).kind).toBe('other');
  expect(parseMetadata({}).kind).toBe('other');
});
test('no offset is treated as UTC', () => expect(parseMetadata({ 'File:FileType': 'JPEG', 'File:MIMEType': 'image/jpeg',
  'File:FileTypeExtension': 'JPG', 'EXIF:DateTimeOriginal': '2000:01:01 00:00:00' }).capturedAt?.toISOString()).toBe('2000-01-01T00:00:00.000Z'));
test.each(['0000:00:00 00:00:00', 'garbage', ''])('invalid date %j becomes null', (d) =>
  expect(parseMetadata({ ...heic, 'EXIF:DateTimeOriginal': d }).capturedAt).toBeNull());

// metadata.corpus.test.ts
test.each([['IMG_E8719.HEIC', 'still', 'jpg'], ['IMG_0004.MOV', 'motion', 'mov'], ['VERP1022.TIF', 'still', 'tif'],
           ['AUXA4949.BMP', 'still', 'bmp']])('real %s', async (n, kind, ext) => {
  const m = await reader.read(C(n)); expect([m.kind, m.ext]).toEqual([kind, ext]);
});
test('AAE is other', async () => expect((await reader.read(C('IMG_nnnn.AAE'))).kind).toBe('other'));
test('a Live Photo pair shares a content id', async () => {
  const [h, m] = [await reader.read(C('IMG_2834 1.HEIC')), await reader.read(C('IMG_2834 1.MOV'))];
  expect(h.liveContentId).not.toBeNull(); expect(m.liveContentId).toBe(h.liveContentId);
});
test('HEIC/JPG twin share capturedAt', async () => {
  // IMG_0034.HEIC/.JPG are NOT twins (different phones, years apart) — ruling after Task 4
  const [h, j] = [await reader.read(C('IMG_6941 1.HEIC')), await reader.read(C('IMG_E6941.JPG'))];
  expect(h.capturedAt).not.toBeNull(); expect(j.capturedAt?.getTime()).toBe(h.capturedAt?.getTime());
});
```

The corpus tests run under `describe.skipIf(!hasCorpus)` with one `reader` created in `beforeAll` and `reader.end()` in `afterAll`.

**If `HEIC/JPG twin share capturedAt` fails on the real corpus** (e.g. the JPG has no `OffsetTimeOriginal`), the offset would split the twin (classify row 2 → similar), blocking G6. Fix it by having `parseMetadata` ignore `OffsetTimeOriginal` and read `DateTimeOriginal` as UTC wall-clock time; update the `HEIC still` expectation to `'2021-10-03T14:02:11.000Z'`, and say so in the report. **If `a Live Photo pair shares a content id` fails** because the real files carry no content id, report it; Task 8's stem fallback still pairs them.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/metadata.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement `metadata.ts`**

- `kind` is `still` when the MIME type is `image/*` and FileType ∈ {JPEG, PNG, HEIC, HEIF, WEBP, TIFF, GIF, BMP, DNG, CR2, CR3, NEF, ARW, ORF, RW2, RAF}. It is `motion` when the MIME type is `video/*`. Otherwise it is `other`.
- `subsec` is the string form of the tag.
- `ext` is `FileTypeExtension` lowercased, with `jpeg` normalised to `jpg`.

- [ ] **Step 4: Run gates G1–G5** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: exiftool metadata with content-based type detection"
```

---

### Task 6: `ingestFile` activity — the vault crash protocol

**Files:**
- Create: `src/types.ts`, `src/activities/index.ts`, `src/activities/ingest.ts`, `tests/helpers/activityEnv.ts`
- Modify: `tests/helpers/images.ts` (add `MOV_BYTES`)
- Test: `tests/unit/ingest.test.ts`

**Interfaces:**
- Consumes: `DataLayout`, `linkIntoVault`, `sha256File`, `vaultRel`, `loadSettings`, `tmpDir` (Task 1); `withTx`, `startPg` (Task 2); `JPEG_A` (Task 4); `MetadataReader`, `Metadata.mime` (Task 5)
- Produces:
  - In `types.ts`:
    - `interface FileRef { relPath: string; size: string; mtimeNs: string }`
    - `interface IngestResult { fileId: string; sha256: string | null; ext: string | null; state: 'INGESTED' | 'COPY' | 'QUARANTINED'; isNew: boolean; kind: 'still' | 'motion' | 'other' | null }`
  - In `activities/index.ts`:
    - `interface Deps { pool: pg.Pool; settings: Settings; layout: DataLayout; metadata: MetadataReader; temporal?: Client; checkpoint?: (name: string) => void | Promise<void> }` (`ingestFile` awaits `checkpoint`)
    - `createActivities(deps: Deps)`, which returns `{ ingestFile, … }` and gains members in later tasks
    - `type Activities = ReturnType<typeof createActivities>`
  - `ingestFile(ref: FileRef): Promise<IngestResult>`. It calls `heartbeat()` on every chunk, reads `Context.current().info.workflowExecution.runId`, and calls `deps.checkpoint?.(name)` at `'claim'`, `'link'` and `'copies_rename'` (a test seam).
  - **Outcome rule:** `state = 'COPY'` exactly when `linkIntoVault` returned `EXISTS_OTHER`; `isNew = outcome ∈ {LINKED, ALREADY_LINKED}`. Only the vault owner (`isNew`) inserts `contents` (still `ON CONFLICT DO NOTHING`, for retries). So a byte copy that commits before the owner never leaves the content un-hashed, and staging never points at a missing `.copies` file.
  - **Guarded transitions:** every `UPDATE files SET state = …` includes `AND state IN (<allowed predecessors>)` (`CLAIMED ← DISCOVERED|CLAIMED`; final states `← CLAIMED`). If no row is updated, the attempt throws `StaleAttemptError` (an ordinary, retryable error), so a paused old attempt that resumes can never roll a final row back.
  - `class StaleAttemptError extends Error`, `class MissingBytesError extends Error` (both exported from `ingest.ts`)
  - In `tests/helpers/activityEnv.ts`:
    - `runActivity<A extends unknown[], R>(fn: (...a: A) => Promise<R>, args: A, opts?: { runId?: string; onHeartbeat?: (d: unknown) => void }): Promise<R>`, using `MockActivityEnvironment` with the overridden `workflowExecution`; `runId` defaults to `'test-run'`, so `runActivity(fn, args)` is valid
    - `testDeps(pg: { url: string; pool: pg.Pool }, overrides?: Partial<Deps>): Promise<{ deps: Deps; layout: DataLayout; close(): Promise<void> }>`: a fresh `DataLayout` under `tmpDir()` (ensured), `loadSettings({ DATABASE_URL: pg.url })`, one real `MetadataReader`; `close()` ends what it created. Later tasks extend it.
  - In `tests/helpers/images.ts`: `MOV_BYTES: Buffer`, a minimal QuickTime file (a `ftyp` box with major brand `qt  ` followed by an empty `mdat` box) that exiftool reports as `FileType MOV`, `MIMEType video/quicktime`. No binary fixture is committed.

- [ ] **Step 1: Write the failing tests**

Defined in the test module: `drop(name, bytes, mtimeNs?)` writes into the drop zone and returns its `FileRef`; `crashAt(name)` returns `{ ...deps, checkpoint }` whose `checkpoint` throws `SimulatedCrash` once at that name; `count(table, where?)` returns a row count. `acts = createActivities(deps)`; `beforeEach` truncates the DB and empties the layout.

```ts
test('new file lands in vault, read-only, drop zone and claimed empty', async () => {
  const r = await runActivity(acts.ingestFile, [await drop('IMG 1.jpg', JPEG_A)], { runId: 'r1' });
  expect([r.state, r.isNew, r.kind]).toEqual(['INGESTED', true, 'still']);
  const v = join(layout.root, vaultRel(r.sha256!, 'jpg'));
  expect((await stat(v)).mode & 0o777).toBe(0o444);
  expect(await readdir(layout.claimed)).toEqual([]); expect(existsSync(join(layout.dropzone, 'IMG 1.jpg'))).toBe(false);
});
test('byte-identical second file is COPY', async () => {
  const a = await runActivity(acts.ingestFile, [await drop('a.jpg', JPEG_A)], { runId: 'r1' });
  const b = await runActivity(acts.ingestFile, [await drop('b.jpg', JPEG_A)], { runId: 'r2' });
  expect([b.state, b.isNew, b.sha256]).toEqual(['COPY', false, a.sha256]);
  expect(existsSync(join(layout.vaultCopies, `${b.fileId}.jpg`))).toBe(true);
});
test.each(['claim', 'link', 'copies_rename'])('resume after crash at %s', async (point) => {
  if (point === 'copies_rename') await runActivity(acts.ingestFile, [await drop('first.jpg', JPEG_A)], { runId: 'r0' });
  const ref = await drop('a.jpg', JPEG_A); const crashing = createActivities(crashAt(point));
  await expect(runActivity(crashing.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(SimulatedCrash);
  const r = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(r.state).toBe(point === 'copies_rename' ? 'COPY' : 'INGESTED');
  expect(await count('files', `rel_path = 'a.jpg'`)).toBe(1); expect(await readdir(layout.claimed)).toEqual([]);
});
test('re-run after commit returns same outcome', async () => {
  const ref = await drop('a.jpg', JPEG_A); const first = await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  expect(await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).toEqual(first); expect(await count('files')).toBe(1);
});
test('re-drop after ingest becomes COPY', async () => {
  const ref = await drop('a.jpg', JPEG_A); await runActivity(acts.ingestFile, [ref], { runId: 'r1' });
  const ref2 = await drop('a.jpg', JPEG_A, BigInt(ref.mtimeNs));
  expect((await runActivity(acts.ingestFile, [ref2], { runId: 'r2' })).state).toBe('COPY'); expect(await count('files')).toBe(2);
});
test('vanished file is quarantined', async () =>
  expect(await runActivity(acts.ingestFile, [{ relPath: 'gone.jpg', size: '1', mtimeNs: '1' }], { runId: 'r1' }))
    .toMatchObject({ state: 'QUARANTINED', sha256: null }));
test('MOV is motion', async () => expect((await runActivity(acts.ingestFile, [await drop('IMG_1.MOV', MOV_BYTES)], { runId: 'r1' })).kind).toBe('motion'));
test('heartbeats during hashing', async () => {
  let beats = 0; const ref = await drop('big.jpg', Buffer.concat([JPEG_A, Buffer.alloc(3 << 20)]));
  await runActivity(acts.ingestFile, [ref], { runId: 'r1', onHeartbeat: () => beats++ });
  expect(beats).toBeGreaterThanOrEqual(3);
});
test('vault owner is INGESTED even when a byte copy commits first', async () => {
  const ra = await drop('a.jpg', JPEG_A), rb = await drop('b.jpg', JPEG_A);
  await expect(runActivity(createActivities(crashAt('link')).ingestFile, [ra], { runId: 'r1' })).rejects.toThrow(SimulatedCrash);
  const b = await runActivity(acts.ingestFile, [rb], { runId: 'r2' });
  expect([b.state, b.isNew]).toEqual(['COPY', false]); expect(await count('contents')).toBe(0);
  const a = await runActivity(acts.ingestFile, [ra], { runId: 'r1' });
  expect([a.state, a.isNew]).toEqual(['INGESTED', true]); expect(await count('contents')).toBe(1);
  expect(existsSync(join(layout.vaultCopies, `${b.fileId}.jpg`))).toBe(true);
});
test('a resumed stale attempt cannot regress a final row', async () => {
  const ref = await drop('a.jpg', JPEG_A); let second: IngestResult | undefined;
  const stale = createActivities({ ...deps, checkpoint: async (n) => {                  // attempt 1 "pauses" after linking
    if (n === 'link' && !second) second = await runActivity(acts.ingestFile, [ref], { runId: 'r1' }); } });   // attempt 2 finishes meanwhile
  await expect(runActivity(stale.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(StaleAttemptError);
  expect(second?.state).toBe('INGESTED');
  expect((await pool.query(`SELECT state FROM files`)).rows.map((r) => r.state)).toEqual(['INGESTED']);
});
test('hashed file with claimed gone but vault present commits INGESTED', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  await expect(runActivity(createActivities(crashAt('link')).ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(SimulatedCrash);
  for (const f of await readdir(layout.claimed)) await rm(join(layout.claimed, f));                    // test-only anomaly
  expect((await runActivity(acts.ingestFile, [ref], { runId: 'r1' })).state).toBe('INGESTED');
});
test('hashed file whose bytes are all gone is retried, never vanished', async () => {
  const ref = await drop('a.jpg', JPEG_A);
  await expect(runActivity(createActivities(crashAt('link')).ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(SimulatedCrash);
  const { sha256 } = (await pool.query(`SELECT sha256 FROM files`)).rows[0];
  for (const f of await readdir(layout.claimed)) await rm(join(layout.claimed, f));
  await rm(join(layout.root, vaultRel(sha256, 'jpg')), { force: true });                                // test-only anomaly
  await expect(runActivity(acts.ingestFile, [ref], { runId: 'r1' })).rejects.toThrow(MissingBytesError);
  expect(await count('files', `state = 'QUARANTINED'`)).toBe(0);
});
test('contents.mime comes from exiftool', async () => {
  const r = await runActivity(acts.ingestFile, [await drop('IMG_1.HEIC', JPEG_A)], { runId: 'r1' });   // JPEG bytes named .HEIC
  expect(r.ext).toBe('jpg'); expect((await pool.query('SELECT mime FROM contents')).rows[0].mime).toBe('image/jpeg');
});
```

`a resumed stale attempt …` reaches step 5 after attempt 2 committed `INGESTED`; its guarded update matches 0 rows and throws.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/ingest.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement `ingestFile`** following spec §6.1 and §12 R1, in this order

1. Look up the row by `workflow_run_id`. If it is final, return the stored outcome. Otherwise `INSERT … ON CONFLICT (rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED') DO UPDATE SET workflow_run_id = EXCLUDED.workflow_run_id RETURNING id`.
2. **Claim** with `rename`. If the source is missing:
   - claimed exists → continue;
   - else if `files.sha256` is **not** set → QUARANTINED `vanished`;
   - else (sha set; never mark it `vanished`): `.copies/<id>.<ext>` exists → commit COPY; else the vault file `vaultRel(sha, ext)` exists → commit INGESTED as the vault owner (insert `contents`, step 5); else throw `MissingBytesError` (retryable; the bytes must be somewhere, so an anomaly is retried, never hidden).
   Run `checkpoint('claim')`. Hash, then commit `CLAIMED, sha256`.
3. Read metadata from the claimed file. Commit `ext`.
4. `linkIntoVault`, then `checkpoint('link')`. On `EXISTS_OTHER`, `rename` to `.copies/<id>.<ext>`, then `checkpoint('copies_rename')`.
5. In one transaction: if the outcome is `LINKED`/`ALREADY_LINKED`, `INSERT INTO contents … ON CONFLICT DO NOTHING` (state `INGESTED`, `mime` = exiftool `Metadata.mime`, plus the other metadata columns); then the guarded update of `files.state` (`INGESTED` for the owner, `COPY` for `EXISTS_OTHER`).
6. `unlink` the claimed file if it still exists. `chmod 0o444` the vault file.

- [ ] **Step 4: Run gates G1–G5** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: idempotent ingest with crash-safe vault protocol"
```

---

### Task 7: Hash and cluster activities

**Files:**
- Create: `src/hashIndex.ts`, `src/activities/hashing.ts`, `src/activities/cluster.ts`, `tests/helpers/hangWorker.ts` (`workerpool.worker({ computeImageFacts: () => new Promise(() => {}) })`)
- Modify: `src/activities/index.ts` (register the new activities; add `decodePool` to `Deps`), `src/types.ts`, `tests/helpers/activityEnv.ts` (`testDeps` creates a test decode pool and terminates it in `close()`)
- Test: `tests/unit/hashIndex.test.ts`, `tests/unit/hashClusterActivities.test.ts`

**Interfaces:**
- Consumes: the pool method `computeImageFacts` (returns `WireFacts`, hashes as decimal strings), `QuarantineError` (`name === 'QuarantineError'`, `reason`) (Task 4); `classify`, `Grouper`, `Facts`, `Thresholds`, `CaptureKey` (Task 3); `F`, `T` from `tests/helpers/facts.ts` (Task 3); `vaultRel` (Task 1); `Deps`, `createActivities`, `runActivity`, `testDeps` (Task 6); `photo`, `truncatedJpeg` (Task 4)
- Produces:
  - `createDecodePool(workerPath: string, maxWorkers: number, forkOpts?: { execArgv?: string[] }): WorkerPool` in `src/activities/hashing.ts` = `workerpool.pool(workerPath, { workerType: 'process', maxWorkers, forkOpts })`. `Deps` gains `decodePool: WorkerPool` (required). Whoever creates the pool owns `.terminate()`: `worker.ts` (Task 9) with `dist/decodeWorker.js`; tests via `testDeps`, which uses `createDecodePool('src/decodeWorker.ts', 2, { execArgv: ['--import', 'tsx'] })` because a child can't load `.ts` (with NodeNext `.js` specifiers) under plain Node. `createActivities` never creates a pool.
  - In `types.ts`: `interface DecodeResult { facts: { width: number; height: number; phash: string; dhash: string; mime: string } | null; quarantineReason: string | null }`
  - `decodeContent(sha256: string, ext: string): Promise<DecodeResult>`. Queue `cpu`, **no database access and no skip check** (it always decodes; only `recordHash` skips). It runs `deps.decodePool.exec('computeImageFacts', [abs, ext]).timeout(decodeTimeoutS * 1000)`. A timeout or child crash throws `ApplicationFailure.retryable(msg, 'Poison')`. An error with `name === 'QuarantineError'` returns `{ facts: null, quarantineReason: err.reason }` (`instanceof` doesn't survive the process boundary).
  - `recordHash(sha256: string, r: DecodeResult): Promise<'HASHED' | 'QUARANTINED'>`. Idempotent: `UPDATE … WHERE sha256 = $1 AND state = 'INGESTED'` sets `phash`/`dhash` (via `toSigned`), `width`, `height`, `hashed_at = now()`, state `HASHED` (or `QUARANTINED` + `quarantine_reason`). It never writes `mime` (exiftool's value from ingest is the only source). When no row changed it maps the current state: `HASHED`/`CLUSTERED` → `'HASHED'`, `QUARANTINED` → `'QUARANTINED'`, anything else throws.
  - `markFailed(sha256: string, error: string): Promise<void>`, which sets `contents.state = 'FAILED'` and `last_error` with `AND state = 'INGESTED'` (0 rows = already handled, a no-op)
  - `class HashIndex` with methods `clear(): void`, `append(rows: readonly Facts[], groups: readonly number[]): void` and `matches(f: Facts, t: Thresholds): Facts[]`. Hashes are stored as hi/lo `Uint32Array` pairs that grow by doubling.
  - `captureKey(capturedAt: Date | null, subsec: string | null): CaptureKey | null` in `src/activities/cluster.ts`, with `s = Math.floor(capturedAt.getTime() / 1000)`: the one place that turns DB rows into `Facts.captured`
  - `clusterContent(sha256: string): Promise<number>` (returns the copy group id). Queue `cluster`. It holds `pg_advisory_xact_lock(hashtext('pm.cluster'))`. It reloads `HashIndex` and `Grouper` from the database when `cluster_state.version` ≠ the cached version, and increments `version` in the same transaction. If the state is already `CLUSTERED`, it returns the existing group. `Thresholds` are built from `deps.settings` (`lookAlikePhash`, `lookAlikeDhash`, `copyPhash`, `copyDhash`); `DEFAULT_THRESHOLDS` is for tests only. **Group ids:** inside the lock, prefetch `SELECT nextval('groups_id_seq') FROM generate_series(1, 2)` (one `add` creates at most one copy group and one similar set) and feed `Grouper`'s synchronous `newId` from that queue; convert ids with `Number()` at the DB boundary (ids < 2^53), so `Grouper` and the return value are `number`. Unused prefetched ids are simply skipped. The final `UPDATE contents SET state = 'CLUSTERED' …` includes `AND state = 'HASHED'`.

- [ ] **Step 1: Write the failing tests**

```ts
test('index matches within thresholds', () => {
  const idx = new HashIndex(); idx.append([F('a'), F('b', { phash: 0x1ffn })], [1, 2]);
  expect(idx.matches(F('n'), T).map((m) => m.sha256)).toEqual(['a']);
});
test('index grows past initial capacity', () => {
  const rows = range(10_000).map((i) => F(`s${i}`, { phash: mix(i), dhash: mix(i + 20_000) }));
  const idx = new HashIndex(); idx.append(rows, rows.map((_, i) => i));
  expect(idx.matches(F('q', { phash: mix(5000), dhash: mix(25_000) }), T).map((m) => m.sha256)).toEqual(['s5000']);
});
test('decodeContent: timeout is retryable Poison', async () => {
  const hang = createDecodePool('tests/helpers/hangWorker.ts', 1, { execArgv: ['--import', 'tsx'] });
  try {
    const acts = createActivities({ ...deps, decodePool: hang, settings: { ...deps.settings, decodeTimeoutS: 1 } });
    await expect(runActivity(acts.decodeContent, await vaultFile((d) => photo(d, 'jpg')))).rejects.toMatchObject({ type: 'Poison', nonRetryable: false });
  } finally { await hang.terminate(true); }
});
test('decodeContent needs no database', async () => {
  const offline = createActivities({ ...deps, pool: new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/x' }) });
  const r = await runActivity(offline.decodeContent, await vaultFile((d) => photo(d, 'jpg')));
  expect(r.quarantineReason).toBeNull(); expect(typeof r.facts?.phash).toBe('string'); expect(BigInt(r.facts!.phash)).toBeGreaterThanOrEqual(0n);
});
test('decodeContent: corrupt file returns quarantine reason', async () =>
  expect(await runActivity(acts.decodeContent, await vaultFile(truncatedJpeg))).toEqual({ facts: null, quarantineReason: 'truncated' }));
test('recordHash is idempotent', async () => {
  const sha = await seedContent({ state: 'INGESTED' });
  expect(await runActivity(acts.recordHash, [sha, OK])).toBe('HASHED'); expect(await runActivity(acts.recordHash, [sha, OK])).toBe('HASHED');
});
test('recordHash quarantines', async () =>
  expect(await runActivity(acts.recordHash, [await seedContent({ state: 'INGESTED' }), { facts: null, quarantineReason: 'truncated' }])).toBe('QUARANTINED'));
test('recordHash sets hashed_at and never writes mime', async () => {
  const sha = await seedContent({ state: 'INGESTED', mime: 'image/heic' });
  await runActivity(acts.recordHash, [sha, { ...OK, facts: { ...OK.facts!, mime: 'image/jpeg' } }]);
  const r = (await pool.query('SELECT hashed_at, mime FROM contents WHERE sha256 = $1', [sha])).rows[0];
  expect(r.hashed_at).not.toBeNull(); expect(r.mime).toBe('image/heic');
});
test('recordHash after clustering still returns HASHED', async () =>
  expect(await runActivity(acts.recordHash, [await seedContent({ state: 'CLUSTERED' }), OK])).toBe('HASHED'));
test('captureKey floors to the UTC second', () =>
  expect(captureKey(new Date('2021-10-03T21:02:11.900Z'), '482')).toEqual({ s: 1_633_294_931, sub: '482' }));
test('cluster: copy joins, burst frame gets own group in same similar set', async () => {
  const [a, c, b] = await seedHashed(F('a'), F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
                                     F('b', { phash: 1n, captured: { s: 1_600_000_000, sub: '999' } }));
  const [ga, gc, gb] = [await cluster(a), await cluster(c), await cluster(b)];
  expect(typeof ga).toBe('number'); expect(ga).toBe(gc); expect(gb).not.toBe(ga);
  expect(await similarSetOf(ga)).not.toBeNull(); expect(await similarSetOf(ga)).toBe(await similarSetOf(gb));
});
test('cluster reloads when another worker wrote', async () => {
  const [z, a, c] = await seedHashed(F('z', { phash: mix(1), dhash: mix(2) }), F('a'), F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }));
  await runActivity(acts.clusterContent, [z]);                                        // warm acts' cache
  const ga = await runActivity(createActivities(deps).clusterContent, [a]);          // another worker writes
  expect(await runActivity(acts.clusterContent, [c])).toBe(ga);                       // acts' cache is stale and must reload
});
test('cluster is idempotent', async () => { const [a] = await seedHashed(F('a')); expect(await cluster(a)).toBe(await cluster(a)); });
test('concurrent clusterContent calls serialise', async () => {
  const [a, c] = await seedHashed(F('a'), F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }));
  const [ga, gc] = await Promise.all([runActivity(createActivities(deps).clusterContent, [a]), runActivity(createActivities(deps).clusterContent, [c])]);
  expect(ga).toBe(gc); expect(await count('groups', `kind = 'copy' AND merged_into IS NULL`)).toBe(1);
});
test('cluster uses thresholds from settings', async () => {
  const strict = createActivities({ ...deps, settings: { ...deps.settings, lookAlikePhash: 0, lookAlikeDhash: 0 } });
  const [a, c] = await seedHashed(F('a'), F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }));
  expect(await runActivity(strict.clusterContent, [a])).not.toBe(await runActivity(strict.clusterContent, [c]));
});
```

Defined in the test modules: `range`, `mix(i) = BigInt.asUintN(64, BigInt(i + 1) * 0x9e3779b97f4a7c15n)` (well-spread 64-bit values), `OK` (a valid `DecodeResult`), `seedContent(o)` (inserts one `contents` row, returns its sha), `seedHashed(...facts)` (inserts `HASHED` rows from `Facts`, returns their shas), `cluster(sha)` (= `runActivity(acts.clusterContent, [sha])`), `similarSetOf(groupId)`, `count(table, where?)`, and `vaultFile(make)`, which writes `make(dir)`'s file into the vault and returns `[sha256, ext]`. `deps` comes from `testDeps` (Task 6, extended here).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/hashIndex.test.ts tests/unit/hashClusterActivities.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement the activities**

`clusterContent` persists the `Assignment` from Task 3 like this:
- insert the `created` groups
- `UPDATE groups SET merged_into` for losers
- `UPDATE contents SET copy_group_id = winner WHERE copy_group_id = loser`
- `UPDATE groups SET similar_set_id` per entry in `similarLinks`, re-pointing groups whose set was merged
- set `contents.copy_ambiguous`, `state = 'CLUSTERED'`, `clustered_at`

Update the in-memory index only after `COMMIT`.

- [ ] **Step 4: Run gates G1–G5** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: decode/record/cluster activities with single-active clustering"
```

---

### Task 8: Staging plan and atomic build

**Files:**
- Create: `src/stagingPlan.ts`, `src/activities/staging.ts`
- Modify: `src/activities/index.ts`, `src/types.ts`
- Test: `tests/unit/stagingPlan.test.ts`, `tests/unit/stagingBuild.test.ts`

**Interfaces:**
- Consumes: the schema incl. `staging_builds.cluster_version` (Task 2); `vaultRel`, `tmpDir` (Task 1); `Deps`, `createActivities`, `runActivity`, `testDeps` (Tasks 6–7)
- Produces:
  - `interface StageContent`: `sha256`, `kind`, `ext`, `mime`, `width`, `height`, `sizeBytes: bigint`, `capturedAt: Date | null`, `liveContentId`, `durationS`, `state`, `quarantineReason`, `copyGroup: number | null`, `similarSet: number | null`, `ambiguous: boolean`
  - `interface StageFile`: `fileId: bigint`, `relPath`, `originalName`, `mtimeNs: bigint`, `sha256`, `state`, `srcRel`
  - `interface Entry { dest: string; src: string }`
  - `type BestCopyKey = readonly [notAmbiguous: number, pixels: number, hasCaptured: number, hasLive: number, formatRank: number, sizeBytes: bigint, negEpochMs: number, sha256: string]`
  - `bestCopyKey(c: StageContent, hasLive: boolean): BestCopyKey`, in spec §6.4 order. "Earliest `captured_at`" is encoded as `-capturedAt.getTime()` (0 when null; `hasCaptured` already ranks those).
  - `compareBestCopy(a: BestCopyKey, b: BestCopyKey): number`: negative when `a` is the better copy. Every element compares **descending** except the final `sha256`, which compares **ascending** (lowest sha wins). Sorting with it puts the best copy first.
  - `pairLivePhotos(contents: readonly StageContent[], files: readonly StageFile[], maxDurationS: number): Map<string, string>` (still sha → motion sha)
  - `planStaging(contents: readonly StageContent[], files: readonly StageFile[], maxDurationS: number): Entry[]`, deterministic and sorted by `dest`
  - In `types.ts`: `interface BuildResult { gen: number | null; entries: number; skipped: boolean }`
  - `maybeBuildStaging(force: boolean): Promise<BuildResult>` (queue `io`, heartbeats every 1,000 links)

- [ ] **Step 1: Write the failing tests**

Here `C(sha, overrides?)` and `Fl(fileId, relPath, content, overrides?)` are builders with defaults, defined in the test module: `C` defaults to a `CLUSTERED` `still` HEIC, 4032×3024, `capturedAt: new Date('2021-10-03T14:02:11Z')` (a UTC instant), `copyGroup: 1`, `similarSet: null`, `ambiguous: false`; `Fl` sets `originalName` = basename, `state` `INGESTED` and `srcRel = vaultRel(sha, ext)`, or `COPY` and `.copies/<fileId>.<ext>` when overridden. `dests(entries)` returns the sorted `dest` strings. `seedPipelineRows()`, `build(force)` (= `runActivity(acts.maybeBuildStaging, [force])`), `tree(dir)`, `genDir(n)`, `walk(dir)` and `fakeTemporal(running)` (a client whose `workflow.count` resolves `{ count: running }`) live in `stagingBuild.test.ts`.

```ts
test('unique best copy and content-named copies folder', () => {
  const heic = C('h'.repeat(64), { ext: 'heic', width: 4032, height: 3024, copyGroup: 1 });
  const jpg = C('j'.repeat(64), { ext: 'jpg', width: 2016, height: 1512, copyGroup: 1 });
  expect(dests(planStaging([heic, jpg], [Fl(1n, 'IMG_0034.HEIC', heic), Fl(2n, 'IMG_0034.JPG', jpg)], 5)))
    .toEqual(['copies/g-hhhhhhhh/IMG_0034.JPG', 'unique/2021/2021-10/20211003_140211_IMG_0034.HEIC']);
});
test('burst set keeps every shot in similar/', () => {
  const a = C('a'.repeat(64), { copyGroup: 1, similarSet: 9 }), b = C('b'.repeat(64), { copyGroup: 2, similarSet: 9 });
  expect(dests(planStaging([a, b], [Fl(1n, 'A.HEIC', a), Fl(2n, 'B.HEIC', b)], 5)).every((d) => d.startsWith('similar/2021/2021-10/s-'))).toBe(true);
});
test('Live MOV beside still with same base name', () => {
  const s = C('5'.repeat(64), { liveContentId: 'L1' }), m = C('6'.repeat(64), { kind: 'motion', ext: 'mov', liveContentId: 'L1', durationS: 2, copyGroup: null, state: 'INGESTED' });
  expect(dests(planStaging([s, m], [Fl(1n, 'x/IMG_9.HEIC', s), Fl(2n, 'y/OTHER.MOV', m)], 5)))
    .toEqual(['unique/2021/2021-10/20211003_140211_IMG_9.HEIC', 'unique/2021/2021-10/20211003_140211_IMG_9.MOV']);   // MOV keeps its own extension
});
test('stem fallback is exact and duration-bounded', () => {
  const s1 = C('1'.repeat(64)), s2 = C('2'.repeat(64));
  const m1 = C('3'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 3 }), long = C('4'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 30 });
  const files = [Fl(1n, 'd/IMG_2834.HEIC', s1), Fl(2n, 'd/IMG_2834 1.HEIC', s2), Fl(3n, 'd/IMG_2834 1.MOV', m1), Fl(4n, 'd/IMG_2834.MOV', long)];
  expect(pairLivePhotos([s1, s2, m1, long], files, 5)).toEqual(new Map([['2'.repeat(64), '3'.repeat(64)]]));
});
test('collision gets _sha8, then _2', () => {
  const best = C('a'.repeat(64)), j = C('b'.repeat(64), { ext: 'jpg', width: 10, height: 10 });
  const files = [Fl(1n, 'IMG_1.HEIC', best), Fl(2n, 'x/IMG_1.JPG', j), Fl(3n, 'y/IMG_1.JPG', j, { state: 'COPY' }), Fl(4n, 'z/IMG_1.JPG', j, { state: 'COPY' })];
  expect(dests(planStaging([best, j], files, 5)).filter((d) => d.startsWith('copies/'))).toEqual(
    ['copies/g-aaaaaaaa/IMG_1.JPG', 'copies/g-aaaaaaaa/IMG_1_bbbbbbbb.JPG', 'copies/g-aaaaaaaa/IMG_1_bbbbbbbb_2.JPG']);
});
test('ambiguous content is never best', () => {
  const amb = C('c'.repeat(64), { width: 8000, height: 6000, ambiguous: true }), ok = C('d'.repeat(64));
  expect(dests(planStaging([amb, ok], [Fl(1n, 'AMB.HEIC', amb), Fl(2n, 'OK.HEIC', ok)], 5)))
    .toEqual(['copies/g-dddddddd/AMB.HEIC', 'unique/2021/2021-10/20211003_140211_OK.HEIC']);
});
test('best-copy ties: earliest capture, then lowest sha', () => {
  const early = C('9'.repeat(64), { capturedAt: new Date('2021-10-03T14:02:10Z') }), late = C('1'.repeat(64));
  expect(compareBestCopy(bestCopyKey(early, false), bestCopyKey(late, false))).toBeLessThan(0);
  expect(compareBestCopy(bestCopyKey(C('1'.repeat(64)), false), bestCopyKey(C('2'.repeat(64)), false))).toBeLessThan(0);
  expect(compareBestCopy(bestCopyKey(C('2'.repeat(64)), true), bestCopyKey(C('1'.repeat(64)), false))).toBeLessThan(0);   // hasLive beats sha
});
test('quarantine, failed and other buckets', () => {
  const q = C('e'.repeat(64), { state: 'QUARANTINED', quarantineReason: 'truncated', copyGroup: null });
  const f = C('f'.repeat(64), { state: 'FAILED', copyGroup: null });
  const aae = C('0'.repeat(64), { kind: 'other', ext: 'aae', mime: 'application/xml', state: 'INGESTED', copyGroup: null });
  expect(dests(planStaging([q, f, aae], [Fl(1n, 'd/BAD.JPG', q), Fl(2n, 'd/HANG.JPG', f), Fl(3n, 'd/e/IMG_1.AAE', aae)], 5)))
    .toEqual(['other/d/e/IMG_1.AAE', 'quarantine/failed/HANG.JPG', 'quarantine/truncated/BAD.JPG']);
});
test('names are independent of group ids', () => {
  const mk = (g: number, set: number) => [C('a'.repeat(64), { copyGroup: g, similarSet: set }), C('b'.repeat(64), { copyGroup: g + 1, similarSet: set })];
  const files = (cs: StageContent[]) => cs.map((c, i) => Fl(BigInt(i + 1), `${c.sha256[0]}.HEIC`, c));
  const [p, q] = [mk(1, 9), mk(500, 77)];
  expect(planStaging(p, files(p), 5)).toEqual(planStaging(q, files(q), 5));
});
test('name comes from lowest (relPath, fileId), not arrival order', () => {
  const m = C('1'.repeat(64), { kind: 'motion', ext: 'mp4', copyGroup: null, state: 'INGESTED' });
  const a = planStaging([m], [Fl(1n, 'PSWU5412 1.MP4', m), Fl(2n, 'PSWU5412.MP4', m, { state: 'COPY' })], 5);
  const b = planStaging([m], [Fl(1n, 'PSWU5412.MP4', m), Fl(2n, 'PSWU5412 1.MP4', m, { state: 'COPY' })], 5);
  expect(dests(a)).toEqual(dests(b)); expect(dests(a)).toEqual(['copies/v-11111111/PSWU5412.MP4', 'other/PSWU5412 1.MP4']);
});
test('dated names are UTC', () => {
  const c = C('a'.repeat(64), { capturedAt: new Date('2021-12-31T23:30:00Z') });
  expect(dests(planStaging([c], [Fl(1n, 'N.HEIC', c)], 5))).toEqual(['unique/2021/2021-12/20211231_233000_N.HEIC']);
});
test('unclustered stills are not staged; INGESTED motion/other are', () => {
  const still = C('2'.repeat(64), { state: 'HASHED', copyGroup: null });
  const mov = C('3'.repeat(64), { kind: 'motion', ext: 'mov', state: 'INGESTED', copyGroup: null, durationS: 30 });
  const aae = C('4'.repeat(64), { kind: 'other', ext: 'aae', state: 'INGESTED', copyGroup: null });
  expect(dests(planStaging([still, mov, aae], [Fl(1n, 'S.HEIC', still), Fl(2n, 'V.MOV', mov), Fl(3n, 'X.AAE', aae)], 5)))
    .toEqual(['other/V.MOV', 'other/X.AAE']);
});

test('build swaps atomically and is repeatable', async () => {
  await seedPipelineRows(); const r1 = await build(true); const r2 = await build(true);
  expect(r2.gen).toBe(r1.gen! + 1); expect(await tree(join(layout.staging, 'current'))).toEqual(await tree(genDir(r1.gen!)));
  expect(await readlink(join(layout.staging, 'current'))).toBe(`.gen-${r2.gen}`);
  expect((await readdir(layout.staging)).sort()).toEqual([`.gen-${r1.gen}`, `.gen-${r2.gen}`, 'current']);
});
test('staged files share inode with vault', async () => {
  await seedPipelineRows(); await build(true);
  const vaultInodes = new Set(await Promise.all((await walk(layout.vault)).map(async (f) => (await stat(f)).ino)));
  const staged = await walk(join(layout.staging, 'current')); expect(staged.length).toBeGreaterThan(0);
  for (const f of staged) expect(vaultInodes.has((await stat(f)).ino), f).toBe(true);
});
test('orphan gen from a crash is removed', async () => {
  await seedPipelineRows(); await mkdir(join(layout.staging, '.gen-99'), { recursive: true });
  const r = await build(true);
  expect(existsSync(join(layout.staging, '.gen-99'))).toBe(false); expect(r.gen).not.toBe(99);
});
test('skips when unchanged; a cluster_state bump counts as a change', async () => {
  await seedPipelineRows(); await build(true);
  expect(await build(false)).toMatchObject({ skipped: true, gen: null });
  await pool.query('UPDATE cluster_state SET version = version + 1');
  expect((await build(false)).skipped).toBe(false);
});
test('waits while FileWorkflows run, unless stale', async () => {
  await seedPipelineRows(); await build(true); await pool.query(`UPDATE files SET state = state`);   // trigger bumps updated_at
  const busy = (stale: number) => createActivities({ ...deps, temporal: fakeTemporal(3), settings: { ...deps.settings, stageMaxStalenessS: stale } });
  expect((await runActivity(busy(900).maybeBuildStaging, [false])).skipped).toBe(true);
  expect((await runActivity(busy(0).maybeBuildStaging, [false])).skipped).toBe(false);
});
test.skipIf(process.getuid?.() === 0)('staged file is read-only', async () => {
  await seedPipelineRows(); await build(true);
  const f = (await walk(join(layout.staging, 'current', 'unique')))[0]!;
  await expect(open(f, 'r+')).rejects.toMatchObject({ code: 'EACCES' });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/stagingPlan.test.ts tests/unit/stagingBuild.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement `stagingPlan.ts` and `maybeBuildStaging`**

**Rules** (spec §6.4 + §12 R5 and R6):
- `g-<sha8>` is the group's best content's sha. `s-<sha8>` is the best content across the whole set.
- Collisions within a folder, in order of `(sha256, relPath)`: the first keeps `name`, then `name_<sha8>`, then `name_<sha8>_2`, `name_<sha8>_3`, … (suffix before the extension).
- **Naming file:** among the files that share a placed content, the one with the lowest `(relPath, fileId)` gives the name (relPath is the only order-free key; `fileId` follows arrival order, which differs between runs and under chaos).
- **Dates:** `YYYY/YYYY-MM/YYYYMMDD_HHMMSS` are formatted in **UTC** (`getUTC*`), from `capturedAt`, else the naming file's `mtimeNs`. Deterministic on the Mac and in Docker.
- A Live MOV takes its still's final base name and keeps its own original extension.
- A content's placement (`unique/`, `similar/`, `other/`, `quarantine/`) links from `vaultRel(sha, ext)`; every other file in `copies/g-…`/`copies/v-…` links from its own `srcRel`.

**Build:**
- Take the advisory lock `hashtext('pm.stage')`.
- The watermark is `greatest(max(files.updated_at), max(contents.updated_at))`, stored in `staging_builds.watermark`, plus `cluster_state.version`, stored in `staging_builds.cluster_version`. "Unchanged" = current watermark ≤ the last build's watermark **and** current version = its `cluster_version`.
- When `force` is false, skip if unchanged, or if there are running `FileWorkflow`s (via `deps.temporal.workflow.count(...)`) and the time since the last build is less than `stageMaxStalenessS`.
- Delete any `.gen-*` that has no `staging_builds` row. Link every entry, `symlink` to `.current.tmp`, then `rename` over `current`. Insert `staging_builds`. Delete generations older than the previous one.

- [ ] **Step 4: Run gates G1–G5** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: deterministic staging plan with atomic generation swap"
```

---

### Task 9: Workflows, scanner and worker

**Files:**
- Create: `src/workflows.ts`, `src/ids.ts`, `src/schedules.ts`, `src/activities/scan.ts`, `src/worker.ts`, `src/cli.ts` (only `stage [--force]` here; Task 10 completes the CLI)
- Modify: `src/activities/index.ts`, `src/types.ts`, `Makefile` (`up`, `down`, `stage`, `trickle`)
- Test: `tests/workflow/workflows.test.ts`, `tests/unit/scan.test.ts`, `tests/unit/schedules.test.ts`

**Interfaces:**
- Consumes: every activity from Tasks 6–8 (`ingestFile`, `decodeContent`, `recordHash`, `markFailed`, `clusterContent`, `maybeBuildStaging`); `createDecodePool` (Task 7); `checkSameDevice`, `DataLayout`, `isIgnored`, `loadSettings` (Task 1); `createPool` (Task 2); `MetadataReader` (Task 5); `runActivity`, `testDeps` (Task 6; `runActivity(fn, args)` without options is valid); `JPEG_A` (Task 4)
- Produces:
  - Workflows (`@temporalio/workflow` only, type-only imports):
    - `FileWorkflow(ref: FileRef, opts?: { poisonAttempts?: number }): Promise<string>` (the final state)
    - `ContentWorkflow(sha256: string, ext: string, opts?): Promise<string>`
    - `ScanWorkflow(): Promise<ScanResult>`
    - `StagingWorkflow(force?: boolean): Promise<BuildResult>`
  - `runContentSteps(sha256, ext, poisonAttempts)`, a workflow-side function: `decodeContent` → `recordHash` → `clusterContent`, with `markFailed` on decode exhaustion. It returns `'CLUSTERED' | 'QUARANTINED' | 'FAILED'`.
  - In `types.ts`: `interface ScanResult { started: number; pendingFiles: number; pendingBytes: string }`
  - `scanAndStart(): Promise<ScanResult>` (queue `io`)
  - `fileWorkflowId(ref: FileRef): string` in `src/ids.ts`: `'file-' + sha1(`${relPath}|${size}|${mtimeNs}`)`
  - `ensureSchedules(client: Client, s: Settings): Promise<void>` in `src/schedules.ts`: `pm-scan` every `scanIntervalS` and `pm-stage` every 300 s (intervals as `` `${n}s` `` strings), overlap `SKIP`. On `ScheduleAlreadyRunning` it calls `client.schedule.getHandle(id).update(() => <the current options>)`, so a changed `SCAN_INTERVAL_S` takes effect.
  - Entrypoint: `node dist/worker.js` (startup wiring only; logic lives in tested modules)
- Activity options, as constants in `workflows.ts`:

| Activity | Queue | Timeouts | Retry |
|----------|-------|----------|-------|
| `ingestFile` | `io` | startToClose `1h`, heartbeat `30s` | unlimited, `1s` ×2 → `5m` |
| `decodeContent` | `cpu` | startToClose `2m` | `maximumAttempts: poisonAttempts` (default 5) |
| `recordHash` | `io` | startToClose `1m` | unlimited |
| `clusterContent` | `cluster` | startToClose `1m` | unlimited |
| `markFailed` | `io` | startToClose `1m` | unlimited |
| `maybeBuildStaging` | `io` | startToClose `2h`, heartbeat `30s` | unlimited |
| `scanAndStart` | `io` | startToClose `10m`, heartbeat `30s` | unlimited |

`proxyActivities` requires a `startToCloseTimeout` (or `scheduleToCloseTimeout`) for every activity; none may be left out.

- [ ] **Step 1: Write the failing tests**

These use `TestWorkflowEnvironment.createTimeSkipping()`, with a `Worker` registering fake activity implementations under the real names. Defined in the workflow test module: `fakes(overrides)` (records `calls`), `runFile(f)` / `runContent(f, sha, ext)` (start the workflow and return its result), `alwaysPoison`, `failNTimes(n, value)`, and the `INGESTED_STILL` / `INGESTED_MOTION` / `COPY` results. Defined in `scan.test.ts`: `fakeClient` (records starts; `setRunning(n)` sets what `workflow.count` returns; `failStartFor(relPath, err)`), `actsWithFakeClient` (activities built from `testDeps` with `temporal: fakeClient`), `growFileDuringScan(path)`, `layout`, `pool`, `settings`.

```ts
test('new still runs all steps', async () => expect(await runFile(fakes({ ingest: INGESTED_STILL }))).toBe('CLUSTERED'));
test('COPY stops after ingest', async () => { const f = fakes({ ingest: COPY }); expect(await runFile(f)).toBe('COPY'); expect(f.calls).toEqual(['ingestFile']); });
test('motion stops after ingest', async () => expect(await runFile(fakes({ ingest: INGESTED_MOTION }))).toBe('INGESTED'));
test('quarantine stops before cluster', async () => {
  const f = fakes({ ingest: INGESTED_STILL, record: 'QUARANTINED' });
  expect(await runFile(f)).toBe('QUARANTINED'); expect(f.calls).not.toContain('clusterContent');
});
test('decode poison marks FAILED after 5', async () => {
  const f = fakes({ ingest: INGESTED_STILL, decode: alwaysPoison });
  expect(await runFile(f)).toBe('FAILED'); expect(f.calls.filter((c) => c === 'decodeContent')).toHaveLength(5); expect(f.calls.at(-1)).toBe('markFailed');
});
test('recordHash outage never fails content', async () =>
  expect(await runFile(fakes({ ingest: INGESTED_STILL, record: failNTimes(40, 'HASHED') }))).toBe('CLUSTERED'));
test('ingest retries through outage', async () => expect(await runFile(fakes({ ingest: failNTimes(20, INGESTED_STILL) }))).toBe('CLUSTERED'));
test('ContentWorkflow runs content steps only', async () => {
  const f = fakes({}); expect(await runContent(f, 'a'.repeat(64), 'heic')).toBe('CLUSTERED');
  expect(f.calls).toEqual(['decodeContent', 'recordHash', 'clusterContent']);
});

test('fileWorkflowId is stable and sensitive', () => {
  const r = { relPath: 'a b/é.jpg', size: '1', mtimeNs: '2' };
  expect(fileWorkflowId(r)).toBe(fileWorkflowId({ ...r })); expect(fileWorkflowId(r)).not.toBe(fileWorkflowId({ ...r, mtimeNs: '3' }));
});
test('scan starts only stable, non-ignored files', async () => {
  growFileDuringScan(join(layout.dropzone, 'big.mov'));
  await writeFile(join(layout.dropzone, 'ok.jpg'), JPEG_A); await writeFile(join(layout.dropzone, '.DS_Store'), 'x');
  const r = await runActivity(actsWithFakeClient.scanAndStart, []);
  expect(fakeClient.startedRelPaths()).toEqual(['ok.jpg']); expect(r.pendingFiles).toBe(2);
});
test('scan tolerates WorkflowExecutionAlreadyStartedError', async () => {
  await writeFile(join(layout.dropzone, 'a.jpg'), JPEG_A); await writeFile(join(layout.dropzone, 'b.jpg'), JPEG_A);
  fakeClient.failStartFor('a.jpg', new WorkflowExecutionAlreadyStartedError('already running', 'file-x', 'FileWorkflow'));
  const r = await runActivity(actsWithFakeClient.scanAndStart, []);
  expect(r.started).toBe(1); expect(fakeClient.startedRelPaths()).toEqual(['b.jpg']);
});
test('scan backpressure', async () => {
  fakeClient.setRunning(settings.maxInFlight); await writeFile(join(layout.dropzone, 'ok.jpg'), JPEG_A);
  const r = await runActivity(actsWithFakeClient.scanAndStart, []);
  expect(r.started).toBe(0); expect(fakeClient.startedRelPaths()).toEqual([]);
  expect((await pool.query('SELECT pending_files FROM scan_stats WHERE id = 1')).rows[0].pending_files).toBe(1n);
});

// schedules.test.ts — fakeScheduleClient({ existing }) makes create() throw ScheduleAlreadyRunning for those ids and records update() results
test('ensureSchedules creates both schedules', async () => {
  const sc = fakeScheduleClient({ existing: [] }); await ensureSchedules(sc.client, settings);
  expect([...sc.created.keys()].sort()).toEqual(['pm-scan', 'pm-stage']);
});
test('ensureSchedules updates an existing schedule to the current interval', async () => {
  const sc = fakeScheduleClient({ existing: ['pm-scan', 'pm-stage'] }); await ensureSchedules(sc.client, { ...settings, scanIntervalS: 45 });
  expect(sc.updated.get('pm-scan')).toMatchObject({ spec: { intervals: [{ every: '45s' }] }, policies: { overlap: ScheduleOverlapPolicy.SKIP } });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run --project workflow && npx vitest run tests/unit/scan.test.ts tests/unit/schedules.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement the workflows, scan, worker and minimal `cli stage`**

- **`scanAndStart`:** take two recursive `readdir` snapshots `stableSeconds` apart, heartbeating between them. Write `scan_stats`. Get `running` from `client.workflow.count("WorkflowType='FileWorkflow' AND ExecutionStatus='Running'")`. Start at most `maxInFlight - running` workflows on queue `main`, with `workflowIdReusePolicy: ALLOW_DUPLICATE` and `workflowIdConflictPolicy: FAIL`.
- **`worker.ts`:**
  - Startup checks: `checkSameDevice`, `layout.ensure()`, `metadata.read` on a probe file.
  - Create one `Worker` per entry in `WORKER_QUEUES`. Workflows (`workflowsPath`) run on `main`. Each other queue gets `maxConcurrentActivityTaskExecutions`: `ioConcurrency`, `cpuConcurrency`, or 1 for `cluster`.
  - Create the decode pool with `createDecodePool('dist/decodeWorker.js', cpuConcurrency)` (only when `cpu` is served) and pass it in `Deps`.
  - Call `ensureSchedules` when `cluster` is served.
  - `shutdownGraceTime: '60s'`, and on shutdown close the pg pool, `metadata.end()`, and `decodePool.terminate()` (which runs the child's `onTerminate`, ending its RAW exiftool). Log with pino.

- [ ] **Step 4: Run gates G1–G6** (G1: paste the Step 2 failing output; G6 also needs Step 5)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus && make down && make up && make fs-check`
Expected: PASS, `fs-check OK`

- [ ] **Step 5: Verify a real end-to-end trickle (G6)**

Add the Makefile target `trickle`: `cp -c "test-automation/sample-images/IMG_6941 1.HEIC" test-automation/sample-images/IMG_E6941.JPG data/dropzone/ && sleep 60 && $(MAKE) stage FORCE=1 && find data/staging/current/ -type f && test -n "$$(find data/staging/current/unique -name '*6941*')" && test -n "$$(find data/staging/current/copies -path '*/g-*/*6941*')"` (which of the two is the best copy depends on pixels/format rank; the check only requires one in each place). Later tasks' G6 runs it as-is (a re-drop adds `COPY` files but keeps both checks true).

Run: `make trickle`
Expected: exit 0; one of `IMG_6941 1.HEIC` / `IMG_E6941.JPG` under `unique/…`, the other under `copies/g-…/`, and the workflows Completed in the Temporal UI

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: Temporal workflows, scanner with backpressure, worker entrypoint"
```

---

### Task 10: Operator CLI and Grafana dashboard

**Files:**
- Create: `infra/grafana/provisioning/datasources/pg.yml`, `infra/grafana/provisioning/dashboards/pm.yml`, `infra/grafana/dashboards/pipeline-live.json`
- Modify: `src/cli.ts`, `Makefile` (`retry-failed`, `verify-vault` via `docker compose exec worker node dist/cli.js …`)
- Test: `tests/unit/cli.test.ts`, `tests/unit/dashboardSql.test.ts`

**Interfaces:**
- Consumes: `StagingWorkflow`, `ContentWorkflow` (Task 9); `sha256File`, `DataLayout` (Task 1); `startPg` (Task 2); `testDeps` (Task 6)
- Produces:
  - `main(argv: string[], deps?): Promise<number>` (the exit code) in `cli.ts`
  - `stage [--force]` (from Task 9)
  - `retry-failed`: resets FAILED contents to `INGESTED` and starts `ContentWorkflow` with ID `content-<sha>-retry-<epoch>`; prints the count
  - `verify-vault [--sample N]`: exits 1 and lists any vault file whose hash doesn't match its name. It **skips `vault/.copies/`** (those are named by file id, not hash) and prints `skipped <n> .copies files`.
  - Dashboard titled "Pixel Magic: Pipeline Live", with `refresh: "5s"` and these panels: `Drop zone`, `In flight`, `Backlog by stage`, `Throughput / min`, `ETA`, `Copy groups`, `Similar sets`, `≈ Reclaimable`, `Quarantined by reason`, `Failed`, `Recent errors`, `Last staging build`
  - `≈ Reclaimable` = Σ over root copy groups of (Σ `contents.size_bytes` − max `size_bytes`) + Σ `files.size_bytes` of `COPY` files, returned as column `value`. It is an approximation on purpose: it does **not** re-implement the §6.4 best-copy ranking in SQL (derived data isn't stored, and the tile is informational).

- [ ] **Step 1: Write the failing tests**

```ts
test('every pixelmagic panel query runs against a seeded DB', async () => {
  await seedPipelineRowsSql(pool);
  for (const t of loadTargets('infra/grafana/dashboards/pipeline-live.json').filter((t) => t.datasource.uid === 'pixelmagic'))
    await expect(pool.query(t.rawSql)).resolves.toBeDefined();               // no Grafana macros
});
test('visibility panel queries run against a stub executions_visibility', async () => {
  const vis = loadTargets(DASH).filter((t) => t.datasource.uid === 'temporal_visibility'); expect(vis.length).toBeGreaterThan(0);
  const c = await pool.connect();
  try {
    await c.query(`CREATE SCHEMA vis; SET search_path = vis; CREATE TABLE executions_visibility (namespace_id text, run_id text, workflow_id text,
                   workflow_type_name text, status int, start_time timestamptz, close_time timestamptz)`);
    for (const t of vis) await expect(c.query(t.rawSql)).resolves.toBeDefined();
  } finally { await c.query('RESET search_path; DROP SCHEMA vis CASCADE'); c.release(); }
});
test('≈ Reclaimable uses the approximate formula', async () => {
  await seedReclaimable(pool);              // one copy group with contents of 100 and 40 bytes, plus one COPY file of 100 bytes
  expect(Number((await pool.query(targetByTitle('≈ Reclaimable').rawSql)).rows[0].value)).toBe(140);
});
test('dashboard has required panels and 5s refresh', () => {
  const d = loadDashboard(); expect(d.panels.map((p) => p.title)).toEqual(expect.arrayContaining(REQUIRED)); expect(d.refresh).toBe('5s');
});
test('verify-vault detects mismatch', async () => {
  await writeVault(layout, 'a'); await writeVault(layout, 'b', { nameSha: '0'.repeat(64) });
  const out = captureStdout(); expect(await main(['verify-vault'], deps)).toBe(1); expect(out()).toContain('0'.repeat(64));
});
test('verify-vault skips .copies and reports the count', async () => {
  await writeVault(layout, 'a'); await writeFile(join(layout.vaultCopies, '7.jpg'), 'named by file id');
  const out = captureStdout(); expect(await main(['verify-vault'], deps)).toBe(0); expect(out()).toContain('skipped 1 .copies files');
});
test('retry-failed resets and starts', async () => {
  await seedContent({ state: 'FAILED', lastError: 'x' }); await seedContent({ state: 'FAILED', lastError: 'y' });
  const out = captureStdout(); expect(await main(['retry-failed'], { ...deps, temporal: fakeClient })).toBe(0);
  expect((await pool.query(`SELECT state FROM contents`)).rows.map((r) => r.state)).toEqual(['INGESTED', 'INGESTED']);
  expect(fakeClient.started.map((w) => w.workflowType)).toEqual(['ContentWorkflow', 'ContentWorkflow']);
  expect(fakeClient.started.every((w) => /^content-[0-9a-f]{64}-retry-\d+$/.test(w.workflowId))).toBe(true);
  expect(out()).toContain('2');
});
test('unknown command exits 2 with usage', async () => {
  const err = captureStderr(); expect(await main(['nope'], deps)).toBe(2); expect(err()).toMatch(/usage/i);
});
```

Defined in the test modules: `DASH` (the dashboard path), `loadTargets`, `loadDashboard`, `targetByTitle`, `REQUIRED` (the panel titles above), `seedPipelineRowsSql`, `seedReclaimable`, `seedContent`, `writeVault(layout, bytes, { nameSha? })`, `captureStdout`/`captureStderr` (return a getter for what was written), and `fakeClient` (records `started` `{ workflowType, workflowId }`). `deps` comes from `testDeps` plus a `startPg` pool.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/cli.test.ts tests/unit/dashboardSql.test.ts`
Expected: FAIL

- [ ] **Step 3: Implement the CLI, datasource provisioning and dashboard JSON**

- The datasources are `pixelmagic` and `temporal_visibility`, both through the read-only role `pm_reader`.
- ETA = backlog ÷ the rate of `clustered_at` over the last 10 minutes.

- [ ] **Step 4: Run gates G1–G6** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus && make down && make up && make fs-check && make trickle`
Expected: PASS, `fs-check OK`, `make trickle` exits 0

- [ ] **Step 5: Verify in the browser**

Run: `open http://127.0.0.1:3000`
Expected: "Pixel Magic: Pipeline Live" loads with no panel errors

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: operator CLI and Postgres-backed Grafana dashboard"
```

---

### Task 11: Corpus manifest, seeder and verifier

**Files:**
- Create: `tools/corpus/lib.ts`, `tools/corpus/buildManifest.ts`, `tools/corpus/seed.ts`, `tools/corpus/verify.ts`, `tools/corpus/synth.ts` (the four CLIs are thin wrappers; logic lives in `lib.ts`, which G4 covers). `tests/corpus/manifest.json` is generated and **git-ignored** (it holds capture times, burst/content UUIDs and dimensions; `make corpus-manifest` rebuilds it from the hash cache).
- Modify: `Makefile` (`corpus-manifest`, `corpus-verify`), `.gitignore` (`tests/corpus/.hash-cache.json`, `tests/corpus/manifest.json`)
- Test: `tests/unit/corpusTools.test.ts`

**Interfaces:**
- Consumes: nothing from `src/` except `isIgnored`. The oracle must stay independent: it uses the `sips` CLI and the `exiftool` CLI bundled with exiftool-vendored, called directly.
- Produces (in `lib.ts`):
  - `buildManifest(corpusDir: string, opts?: { cachePath?: string }): Promise<Manifest>` (the `buildManifest.ts` CLI wraps it)
  - `buildRelations(recs: ManifestRecord[]): Relations`
  - `PINNED: readonly string[]`
  - `selectSubsets(m: Manifest, seed: number): { smoke: string[]; standard: string[] }`, which throws (naming the file) if any `PINNED` file is missing from `m`
  - `seed(opts: SeedOptions): Promise<SeedManifest>`, where `interface SeedOptions { corpusDir: string; names: string[]; dest: string; layout?: 'flat' | 'nested'; shuffle?: number; extras?: Array<'junk' | 'corrupt'>; synthetic?: boolean; prefix?: string }` and `interface SeedManifest { dest: string; files: SeedFile[] }`, `interface SeedFile { relPath: string; name: string; sha256: string; size: string; mtimeNs: string; origin: 'corpus' | 'synthetic' | 'junk' | 'corrupt'; seedName?: string; variant?: 'resize50' | 'q60-noexif' | 'png' | 'bridge'; expect?: 'copy' | 'similar'; reason?: string }` (`reason` = the expected quarantine reason of a corrupt extra; `expect` = synth ground truth vs `seedName`)
  - `verify(corpusDir: string, m: Manifest, deep: boolean): Promise<string[]>` (the drift list)
- `interface ManifestRecord`: `name: string`, `size: string` and `mtimeNs: string` (**decimal strings**: JSON can't hold `bigint`, and sizes/mtimes are never `number`), `sha256: string`, `kind: 'still' | 'motion' | 'other'`, `extMismatch: boolean`, `width: number | null`, `height: number | null`, `capturedAt: string | null` (ISO), `subsec: string | null`, `camera: string | null`, `burstUuid: string | null`, `contentId: string | null`, `durationS: number | null`
- Manifest relations (each a list of sorted name pairs, `exactGroups` a list of name lists): `exactGroups`, `heicJpgTwin`, `editOf`, `spaceSuffixCopy`, `burst` (same `BurstUUID`, or captures ≤ 2 s apart from the same camera with different subsec), `livePhoto` (content ID, or same stem with a MOV ≤ 5 s). `spaceSuffixCopy` is a **name-only** relation (`"X 1"` vs `"X"`); it never implies "copy" (`IMG_2834.HEIC` 8116×3808 and `"IMG_2834 1.HEIC"` 4032×3024 are different shots).
- `PINNED` (both subsets; spec §10.3, only files that exist): `IMG_E8719.HEIC`, `IMG_0004.MOV`, `PSWU5412.MP4`, `PSWU5412 1.MP4`, `IMG_0034.HEIC`, `IMG_0034.JPG` (same name, different photos — must stay separate), `IMG_6941 1.HEIC`, `IMG_E6941.JPG` (the real HEIC/JPG copy pair), `IMG_0433.HEIC`, `IMG_E0433.HEIC`, `IMG_2834.HEIC`, `IMG_2834 1.HEIC`, `IMG_2834 1.MOV`, `VERP1022.TIF`, `AUXA4949.BMP` (there is no `IMG_2834.MOV`), plus ≥ 20 Live Photo pairs and ≥ 5 burst sets chosen by `selectSubsets`
- CLIs:
  - `tsx tools/corpus/seed.ts --subset smoke|standard|full --dest DIR [--layout flat|nested] [--shuffle N] [--extras junk,corrupt] [--synthetic]` writes `DIR/../seed-manifest.json`
  - `synth.ts` generates variants from real seeds with sharp: resize 50% keeping EXIF (`withMetadata`), re-encode q60 with EXIF stripped, PNG conversion, and an EXIF-stripped export of one burst frame (the bridge case). Ground truth lands in the seed manifest.

- [ ] **Step 1: Write the failing tests** (tiny fixtures in a tmp dir, no corpus needed)

```ts
test('relations from names', () => {
  const r = buildRelations(['IMG_0034.HEIC', 'IMG_0034.JPG', 'IMG_E0433.HEIC', 'IMG_0433.HEIC', 'IMG_2834 1.HEIC', 'IMG_2834.HEIC'].map(rec));
  expect(r.heicJpgTwin).toContainEqual(['IMG_0034.HEIC', 'IMG_0034.JPG']);
  expect(r.editOf).toContainEqual(['IMG_E0433.HEIC', 'IMG_0433.HEIC']);
  expect(r.spaceSuffixCopy).toContainEqual(['IMG_2834 1.HEIC', 'IMG_2834.HEIC']);
});
test('subsets are deterministic and include pins', () => {
  const m = fakeManifest(500); expect(selectSubsets(m, 7)).toEqual(selectSubsets(m, 7));
  expect(selectSubsets(m, 7).smoke).toEqual(expect.arrayContaining(PINNED));
});
test('seed clones without touching source', async () => {
  const src = await makeCorpus(); const before = await snapshot(src);
  await seed({ corpusDir: src, names: ['a.jpg'], dest: `${tmp}/dz` });
  expect(await snapshot(src)).toEqual(before); expect(existsSync(`${tmp}/dz/a.jpg`)).toBe(true);
});
test('selectSubsets throws when a pinned file is missing', () =>
  expect(() => selectSubsets(fakeManifest(500, { omit: ['IMG_2834 1.MOV'] }), 7)).toThrow(/IMG_2834 1\.MOV/));
test('verify detects drift', async () => {
  const src = await makeCorpus(); const m = await manifestOf(src);      // records with the real size/mtimeNs/sha256 of each tmp file
  expect(await verify(src, m, true)).toEqual([]);
  await appendFile(join(src, 'a.jpg'), 'x'); await rm(join(src, 'b.jpg'));     // tmp copies only, never the real corpus
  const drift = await verify(src, m, true);
  expect(drift).toHaveLength(2); expect(drift).toEqual(expect.arrayContaining([expect.stringContaining('a.jpg'), expect.stringContaining('b.jpg')]));
});
test('burst relation needs same camera and different subsec', () => {
  const at = (s: number) => `2021-10-03T14:02:${s}.000Z`, cam = 'Apple iPhone 12';
  const r = buildRelations([rec('A.HEIC', { capturedAt: at(11), subsec: '1', camera: cam }), rec('B.HEIC', { capturedAt: at(12), subsec: '5', camera: cam }),
                            rec('C.HEIC', { capturedAt: at(11), subsec: '7', camera: 'Canon' }), rec('D.HEIC', { capturedAt: at(11), subsec: '1', camera: cam })]);
  expect(r.burst).toContainEqual(['A.HEIC', 'B.HEIC']);
  expect(r.burst.flat()).not.toContain('C.HEIC');                       // different camera
  expect(r.burst).not.toContainEqual(['A.HEIC', 'D.HEIC']);             // same capture incl. subsec: same shot, not a burst
});
test('manifest numbers are decimal strings', async () => {
  const rec0 = (await manifestOf(await makeCorpus())).records[0]!;
  expect(rec0.size).toMatch(/^\d+$/); expect(rec0.mtimeNs).toMatch(/^\d+$/);
});
```

Defined in the test module: `rec(name, overrides?)` (a `ManifestRecord` with defaults), `fakeManifest(n, { omit? })` (n synthetic records plus every `PINNED` name not omitted), `makeCorpus()` (a tmp dir with `a.jpg`, `b.jpg`, … from sharp), `manifestOf(dir)` (= `buildManifest(dir, { cachePath: <tmp> })`), `snapshot(dir)`, `tmp`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/corpusTools.test.ts`
Expected: FAIL

- [ ] **Step 3: Implement the tools**

- SHA-256 runs in parallel with `worker_threads` (`availableParallelism()`), with a cache keyed by `(name, size, mtimeNs)`.
- Cloning uses `cp -c -p` via `execFile` and falls back to `copyFile` + `utimes`. Check free space first.

- [ ] **Step 4: Run gates G1–G7, then build the real manifest** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus && make down && make up && make fs-check && make trickle && make corpus-manifest && make corpus-verify`
Expected:
- PASS
- the summary prints about 14,880 files
- `burst` count ≥ 5. If it is lower, report it; the `synth.ts` bridge cases then carry the burst tests.
- verify exits 0

- [ ] **Step 5: Commit**

```bash
git add tools tests/unit/corpusTools.test.ts Makefile .gitignore && git commit -m "feat: corpus manifest, deterministic subsets, CoW seeder, verifier"
```

---

### Task 12: End-to-end harness, invariant and smoke/standard suites

**Files:**
- Create: `tests/lib/stack.ts`, `tests/lib/invariant.ts`, `tests/lib/expectations.ts`, `tests/lib/snapshot.ts`, `tests/e2e/smoke.test.ts`, `tests/e2e/standard.test.ts`, `tests/e2e/golden/smoke_tree.txt`
- Modify: `Makefile` (`test-smoke`, `test-standard`, `update-golden`; each runs `corpus-verify` before and after), `package.json` (`test:e2e`)
- Test: the files above

**Interfaces:**
- Consumes: `seed`, `selectSubsets`, `Manifest`, `SeedManifest`, `SeedFile` and the manifest file (Task 11); the compose files incl. `docker-compose.test.yml` (Task 2); `cli stage` (Task 9); the order-free staging names (Task 8)
- Produces:
  - `class Stack { constructor(runId: string, dataDir: string) }` with methods `up()`, `down(keep: boolean)`, `kill(svc)`, `restart(svc)`, `pause(svc, ms)`, `killAll()`, `waitIdle(timeoutMs)`, `stage()`, and the property `dbUrl`
  - In `tests/lib/stack.ts`, the one shared runner (Task 13 reuses it): `runTier(tier: 'smoke' | 'standard', opts?: { shuffle?: number; layout?: 'flat' | 'nested'; runId?: string }): Promise<TierRun>`. It loads the manifest, picks names with `selectSubsets(manifest, 7)`, `seed`s into `data-test/<runId>/dropzone` (standard adds `extras: ['junk', 'corrupt']` and `synthetic: true`), and runs `stack.up()`; it does not wait.
  - `interface TierRun { tier; runId: string; stack: Stack; dataDir: string; dbUrl: string; manifest: Manifest; seedManifest: SeedManifest; ctx: [string, string, SeedManifest]; snap: [string, string]; exp: [string, SeedManifest, Manifest]; runToIdle(timeoutMs?: number): Promise<void>; reseed(prefix: string): Promise<SeedManifest> }`. `ctx`/`snap`/`exp` are the argument tuples for `checkInvariant`/`treeSnapshot`/`diffExpectations`; `runToIdle` = `stack.waitIdle` then `stack.stage()`; `reseed` seeds the same names again under `dropzone/<prefix>` and merges the files into `seedManifest`.
  - `queries(dbUrl: string)` in `tests/lib/expectations.ts`, returning `kindOf(name)`, `mimeOf(name)`, `sameCopyGroup(a, b)`, `states(names)`, `stateOf(relPath)`, `quarantineReasonOf(relPath)`, `fileRowCount(relPaths)` and `beside(movName, stillName)` (some staged path of the MOV has the same dirname and stem as a staged path of the still's content)
  - `checkInvariant(dbUrl: string, dataDir: string, seedManifest: SeedManifest): Promise<string[]>` (violations; empty means it passed)
  - `treeSnapshot(dataDir: string, dbUrl: string): Promise<string[]>`: lines of `dest\tsha256`, with entries of `copy_ambiguous` contents shown as `AMBIGUOUS\t<sha256>`
  - `readGolden(name: string): Promise<string[]>` in `tests/lib/snapshot.ts` (reads `tests/e2e/golden/<name>`; with `UPDATE_GOLDEN=1`, which `make update-golden` sets, the smoke test writes the snapshot there instead of comparing)
  - `diffExpectations(dbUrl: string, seedManifest: SeedManifest, manifest: Manifest): Promise<string[]>`. Expected copy-ness comes **only** from sha256 equality and synth ground truth (`SeedFile.expect`), never from the name-only `spaceSuffixCopy` relation (a false copy is the one harmful error; `IMG_2834` vs `"IMG_2834 1"` are different shots).
  - `partitions(dbUrl: string): Promise<{ similar: Set<string>; copy: Set<string> }>` (the copy partition excludes ambiguous contents)

- [ ] **Step 1: Write the e2e tests** (vitest project `e2e`, tier selected by `TIER` env)

`smoke.test.ts` and `standard.test.ts` each do `run = await runTier(tier); await run.runToIdle()` in `beforeAll` (timeout ≥ 30 min), bind `ctx = run.ctx`, `exp = run.exp`, `snap = run.snap`, `manifest = run.manifest` and the `queries(run.dbUrl)` helpers, and `run.stack.down(keep = failed)` in `afterAll`. The first two tests below live in `smoke.test.ts`; the rest in `standard.test.ts`, whose first test repeats the invariant and expectations checks.

```ts
test('smoke: invariant, expectations, golden tree', async () => {
  expect(await checkInvariant(...ctx)).toEqual([]); expect(await diffExpectations(...exp)).toEqual([]);
  expect(await treeSnapshot(...snap)).toEqual(await readGolden('smoke_tree.txt'));
});
test('pinned cases', async () => {
  expect(await kindOf('IMG_E8719.HEIC')).toBe('still'); expect(await mimeOf('IMG_E8719.HEIC')).toBe('image/jpeg');
  expect(await kindOf('IMG_0004.MOV')).toBe('motion');
  expect(await sameCopyGroup('IMG_6941 1.HEIC', 'IMG_E6941.JPG')).toBe(true);
  expect(await sameCopyGroup('IMG_0034.HEIC', 'IMG_0034.JPG')).toBe(false); // same name, different photos
  expect(await sameCopyGroup('IMG_2834.HEIC', 'IMG_2834 1.HEIC')).toBe(false);
  expect(new Set(await states(['PSWU5412.MP4', 'PSWU5412 1.MP4']))).toEqual(new Set(['INGESTED', 'COPY']));
  expect(await beside('IMG_2834 1.MOV', 'IMG_2834 1.HEIC')).toBe(true);
});
test('standard: no burst pair shares a copy group', async () => {
  for (const [a, b] of manifest.relations.burst) expect(await sameCopyGroup(a, b), `${a} ${b}`).toBe(false);
});
test('standard: synthetic resized/re-encoded variants join their seed’s copy group', async () => {
  const copies = run.seedManifest.files.filter((f) => f.origin === 'synthetic' && f.expect === 'copy'); expect(copies.length).toBeGreaterThan(0);
  for (const v of copies) expect(await sameCopyGroup(v.name, v.seedName!), v.name).toBe(true);
});
test('every Live Photo pair staged together', async () => {
  const seeded = new Set(run.seedManifest.files.map((f) => f.name));
  const pairs = manifest.relations.livePhoto.filter(([a, b]) => seeded.has(a) && seeded.has(b)); expect(pairs.length).toBeGreaterThanOrEqual(20);
  for (const [still, mov] of pairs) expect(await beside(mov, still), `${still} ${mov}`).toBe(true);
});
test('junk never discovered', async () => {
  const junk = run.seedManifest.files.filter((f) => f.origin === 'junk'); expect(junk.length).toBeGreaterThan(0);
  expect(await fileRowCount(junk.map((f) => f.relPath))).toBe(0);
  for (const f of junk) expect(existsSync(join(run.dataDir, 'dropzone', f.relPath)), f.relPath).toBe(true);
});
test('corrupt extras quarantined with expected reasons', async () => {
  const bad = run.seedManifest.files.filter((f) => f.origin === 'corrupt'); expect(bad.length).toBeGreaterThan(0);
  for (const f of bad) expect(await quarantineReasonOf(f.relPath), f.relPath).toBe(f.reason);
});
test('nested Unicode layout works', async () => {
  const nested = await runTier('standard', { layout: 'nested', runId: `${run.runId}-nested` });
  try {
    await nested.runToIdle();
    expect(nested.seedManifest.files.some((f) => f.relPath.includes('/') && /[^\x00-\x7f]/.test(f.relPath))).toBe(true);
    expect(await checkInvariant(...nested.ctx)).toEqual([]); expect(await partitions(nested.dbUrl)).toEqual(await partitions(run.dbUrl));
  } finally { await nested.stack.down(false); }
});
test('re-seed under new paths is all COPY', async () => {                 // last: it mutates the run
  const again = (await run.reseed('again/')).files.filter((f) => f.relPath.startsWith('again/') && f.origin === 'corpus');
  await run.runToIdle(); expect(again.length).toBeGreaterThan(0);
  for (const f of again) expect(await stateOf(f.relPath), f.relPath).toBe('COPY');
});
```

- [ ] **Step 2: Run them and watch them fail**

Run: `make test-smoke`
Expected: FAIL (the golden file doesn't exist yet), with a readable per-file diff report for any other mismatch

- [ ] **Step 3: Implement the harness, generate the golden file and review it by hand**

- Isolation: `COMPOSE_PROJECT_NAME=pmtest_<runId>`, `DATA_DIR=data-test/<runId>`, and `docker-compose.test.yml`. The test reads the mapped ports via `docker compose port`.
- `waitIdle`: two consecutive 5 s polls in which running workflows = 0 and the drop zone has no eligible files. Then call `stage()`.
- On failure: keep the stack and dump per-state counts and the last 200 log lines from each service.

Run `make update-golden`, then **read `smoke_tree.txt` and confirm every pinned case lands in the expected bucket before committing it.**

- [ ] **Step 4: Run gates G1–G8** (G1: paste the Step 2 failing output)

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus && make down && make up && make fs-check && make trickle && make corpus-verify && make test-smoke && make test-standard`
Expected: PASS. Smoke takes ≤ 5 min. `corpus-verify` passes before and after. Run `make test-smoke` twice: the golden tree must match both times (staging names don't depend on arrival order).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "test: corpus-driven e2e harness, golden invariant, smoke/standard suites"
```

---

### Task 13: Chaos proof

**Files:**
- Create: `tests/chaos/chaos.test.ts`
- Modify: `tests/lib/stack.ts` (add `runWithChaos` to `TierRun`), `Makefile` (`chaos`), `package.json` (`test:chaos`)
- Test: `tests/chaos/chaos.test.ts`

**Interfaces:**
- Consumes: `Stack`, `runTier`, `TierRun` (with `ctx`/`snap`/`exp`, `runToIdle`), `checkInvariant`, `treeSnapshot`, `diffExpectations`, `partitions` (Task 12)
- Produces (in `tests/lib/stack.ts`): `TierRun.runWithChaos(opts: { seed: number; everyS: [min: number, max: number]; untilProgress: number; actions: ChaosAction[] }): Promise<string[]>`, where `type ChaosAction = ['kill', svc] | ['restart', svc] | ['pause', svc, ms] | ['killAll']`. It first scales `worker-extra` to 1 (so killing it is meaningful), then, with a seeded PRNG, waits a random `everyS` interval and applies a random action via `Stack`, until the fraction of seeded files in a final state reaches `untilProgress`. `killAll` is followed by `up()`. Every action is applied at least once. It returns the applied action log, which the test prints.

**G1 is waived for this task** (no implementation step; the assertions reuse Task 12's). Paste the first run's output instead.

- [ ] **Step 1: Write the chaos test**

```ts
test('chaos equals calm', async () => {
  const calm = await runTier('standard', { shuffle: 1 }); await calm.runToIdle();
  const storm = await runTier('standard', { shuffle: 1 });
  await storm.runWithChaos({ seed: 42, everyS: [15, 45], untilProgress: 0.9, actions: [
    ['kill', 'worker'], ['kill', 'worker-extra'], ['restart', 'temporal'], ['restart', 'postgres'],
    ['killAll'], ['pause', 'worker', 120_000]] });                         // pause ≈ Mac sleep
  await storm.runToIdle();
  expect(await checkInvariant(...storm.ctx)).toEqual([]);
  expect(await partitions(storm.dbUrl)).toEqual(await partitions(calm.dbUrl));
  expect(await treeSnapshot(...storm.snap)).toEqual(await treeSnapshot(...calm.snap));
  expect(await diffExpectations(...storm.exp)).toEqual([]);
}, 3 * 60 * 60 * 1000);
```

- [ ] **Step 2: Run it**

Run: `make chaos`
Expected: PASS. Any failure is a real defect: stop, use superpowers:systematic-debugging, add a regression unit test in the owning module, fix it, and re-run.

- [ ] **Step 3: Confirm it passes three times in a row, then run gates G2–G8**

Run: `(for i in 1 2 3; do make chaos || exit 1; done) && npm test && npm run lint && npm run format:check && npm run typecheck && npm run build && npm run test:coverage && npm run test:corpus && make down && make up && make fs-check && make trickle && make corpus-verify && make test-smoke && make test-standard`
Expected: 3 chaos PASS, then every gate green

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test: chaos proof that crashes never change the outcome"
```
