# Pixel Magic 2.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a resumable, Temporal-orchestrated photo-dedupe pipeline. It moves dropped files into a content-addressed vault, groups true copies of the same shot and keeps every similar shot, and rebuilds a hardlinked staging tree that can be regenerated at any time.

**Architecture:** One TypeScript package runs as Temporal workers on the task queues `main` (workflows), `io`, `cpu` and `cluster`. Postgres holds the facts (`files`, `contents`, `groups`). Temporal holds progress. Staging is computed from the database and swapped in with an atomic symlink rename. Compose runs postgres, temporal-admin, temporal, temporal-ui, migrate, worker, worker-extra and grafana.

**Tech Stack:**
- **Runtime:** Node 24 LTS, TypeScript 5.x (strict, ESM, `module: NodeNext`), npm
- **Temporal:** `@temporalio/{client,worker,workflow,activity,testing}`
- **Data and config:** `pg` (with `BigInt` parsing for int8), `node-pg-migrate` (SQL migrations), `zod` (config)
- **Imaging:** `sharp`, `heic-decode`, `exiftool-vendored`, `workerpool` (process workers)
- **Logging:** `pino`
- **Tests:** vitest, `@vitest/coverage-v8`, fast-check, `@testcontainers/postgresql`
- **Lint and format:** eslint (typescript-eslint, strict-type-checked), prettier
- **Script runner:** `tsx` for `tools/`

**Spec:** `docs/superpowers/specs/2026-10-17-pixel-magic-design.md` (including §12 "Revisions during planning", R1–R7)

## Global Constraints

- **Build and run:**
  - Node ≥ 24 (`"engines"`), `"type": "module"`, and `tsconfig` with `strict`, `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes`.
  - Source lives in `src/`, compiled by `tsc` to `dist/`. Services run `node dist/<entry>.js`.
- **64-bit hashes are `bigint` everywhere.** Never use `number` for a hash, size or `mtime_ns`. Configure `pg` with `types.setTypeParser(20, BigInt)`. In Temporal payloads, bigints travel as decimal strings.
- **Config** comes from env vars with these exact names and defaults. They are parsed by `zod` into camelCase fields (`SCAN_INTERVAL_S` → `scanIntervalS`):
  - `DATA_DIR=/data`, `DATABASE_URL` (required), `TEMPORAL_ADDRESS=temporal:7233`, `TEMPORAL_NAMESPACE=default`, `WORKER_QUEUES=main,io,cpu,cluster`
  - `SCAN_INTERVAL_S=30`, `STABLE_SECONDS=5`, `MAX_IN_FLIGHT=5000`
  - `IO_CONCURRENCY=8`, `CPU_CONCURRENCY=<os.availableParallelism()>`, `DECODE_TIMEOUT_S=60`, `POISON_ATTEMPTS=5`
  - `LOOK_ALIKE_PHASH=8`, `LOOK_ALIKE_DHASH=10`, `COPY_PHASH=4`, `COPY_DHASH=6`
  - `LIVE_MAX_DURATION_S=5`, `STAGE_MAX_STALENESS_S=900`
- **The `DATA_DIR` layout** is `dropzone/`, `work/claimed/`, `vault/<ab>/<cd>/<sha256>.<ext>`, `vault/.copies/<fileId>.<ext>`, `staging/.gen-<n>/` and `staging/current` (symlink). Every path stored in the database is **relative to `DATA_DIR`**.
- **Nothing in the pipeline ever unlinks a file it hasn't first hardlinked or renamed somewhere else.** The only unlinks allowed are `work/claimed/<id>` after its vault link is committed, and old `staging/.gen-*` directories.
- Vault files are mode `0o444`. Containers run as non-root (`UID`/`GID` build args, default 501/20).
- **Workflow code** (`src/workflows.ts`) imports only `@temporalio/workflow` and **type-only** imports from `src/types.ts` and `src/activities/index.ts`.
- Published ports are `127.0.0.1` only: Grafana 3000, Temporal UI 8080, Postgres 5432, Temporal 7233.
- The corpus `test-automation/sample-images/` is never written to. It stays git-ignored, and tests reach it only by cloning with `cp -c`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Between-Task Gate (mandatory)

No task is complete, and no next task starts, until **all** of the following pass on the committed tree. The reviewer re-runs them itself; it does not trust the implementer's report.

| Gate | Command | Applies from |
|------|---------|--------------|
| G1 Red first | The task's new tests were seen failing before the implementation (the implementer pastes the failing output) | Task 1 |
| G2 Full regression | `npm test` (= `vitest run --project unit --project workflow`) with **every** test from earlier tasks, 0 failures, 0 unexpected skips | Task 1 |
| G3 Static | `npm run lint && npm run format:check && npm run typecheck && npm run build` | Task 1 |
| G4 Coverage | `npm run test:coverage` (v8 coverage, thresholds lines 85 / branches 85), and every file the task touched is ≥ 85% | Task 2 |
| G5 Corpus unit | `npm run test:corpus` passes (real sample files, read-only) | Task 4 |
| G6 Live stack | `make down && make up && make fs-check`, then the Task 9 Step 5 trickle (IMG_0034 HEIC/JPG lands in `unique/` + `copies/`) | Task 9 |
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
tsconfig.json             strict ESM NodeNext; include src, tests, tools
vitest.config.ts          projects: unit (tests/unit, excluding *.corpus.test.ts), workflow (tests/workflow),
                          corpus (tests/**/*.corpus.test.ts), e2e (tests/e2e), chaos (tests/chaos)
eslint.config.js, .prettierrc
Dockerfile                node:24-bookworm-slim + perl (for exiftool-vendored); non-root; npm ci; tsc build
docker-compose.yml        postgres, temporal-admin, temporal, temporal-ui, migrate, worker, worker-extra, grafana
docker-compose.test.yml   override: ephemeral 127.0.0.1 ports, DATA_DIR from env
Makefile                  up down logs lint test fs-check stage retry-failed verify-vault corpus-* test-smoke test-standard chaos update-golden
.env.example
infra/postgres/init.sql   creates DBs pixelmagic, temporal, temporal_visibility + read-only role pm_reader
infra/grafana/            provisioning/{datasources,dashboards}/*.yml, dashboards/pipeline-live.json
migrations/0001_initial.sql
src/
  config.ts               Settings, loadSettings()
  logger.ts               pino instance
  types.ts                payload types shared by workflows and activities (type-only)
  ids.ts                  fileWorkflowId
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
  helpers/                pg.ts (testcontainers + migrate + truncate), layout.ts, images.ts (sharp-generated fixtures), activityEnv.ts, corpus.ts
  unit/ …                 one file per module (*.test.ts); corpus-dependent ones are *.corpus.test.ts
  workflow/workflows.test.ts
  lib/                    stack.ts, invariant.ts, expectations.ts, snapshot.ts
  e2e/                    smoke.test.ts, standard.test.ts, golden/smoke_tree.txt
  chaos/chaos.test.ts
  corpus/manifest.json    committed: names + hashes only
```

---

### Task 1: Scaffold, config and filesystem guarantees

**Files:**
- Create: `package.json`, `tsconfig.json`, `vitest.config.ts`, `eslint.config.js`, `.prettierrc`, `Dockerfile`, `.dockerignore`, `Makefile`, `.env.example`, `README.md`, `src/config.ts`, `src/logger.ts`, `src/fsops.ts`, `src/fscheck.ts`
- Modify: `.gitignore` (add `node_modules/`, `dist/`, `coverage/`, `.superpowers/`)
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
- **`Dockerfile`:** two-stage. The build stage runs `npm ci && npm run build`. The runtime stage is `node:24-bookworm-slim` plus `apt-get install perl`, runs `npm ci --omit=dev`, copies `dist/` and `migrations/`, and creates the user from `UID`/`GID`.
- **`Makefile`:** `test` = `npm test`; `fs-check` = `docker compose run --rm worker node dist/fscheck.js`. Other targets may be stubs until their task.

- [ ] **Step 4: Run gates G2 and G3**

Run: `npm test && npm run lint && npm run format:check && npm run typecheck && npm run build`
Expected: PASS, no errors

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
- Create: `docker-compose.yml`, `docker-compose.test.yml`, `infra/postgres/init.sql`, `migrations/0001_initial.sql`, `src/db.ts`, `tests/helpers/pg.ts`
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
    - `staging_builds(gen PK, watermark, built_at, entries)`
  - CHECK constraints for every state column. The partial unique index is on `files(rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED')`. An `updated_at` trigger is added on `files` and `contents`.
  - `createPool(url: string, max?: number): pg.Pool`, with the int8 → `BigInt` parser installed
  - `withTx<T>(pool: pg.Pool, fn: (c: pg.PoolClient) => Promise<T>): Promise<T>`
  - Test helper `startPg(): Promise<{ url: string; pool: pg.Pool; truncate(): Promise<void>; stop(): Promise<void> }>`: testcontainers `postgres:16` with migrations applied through the node-pg-migrate API. It is started once per test file in `beforeAll`.

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
test('updated_at trigger bumps', async () => { /* insert, update state, assert updated_at increased */ });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/schema.test.ts`
Expected: FAIL (`relation "files" does not exist`, or the helper is missing)

- [ ] **Step 3: Implement the migration, `db.ts`, the pg helper and compose**

- **Compose:**
  - `postgres:16` with `init.sql`, a `pg_isready` healthcheck and the `pg-data` volume.
  - `temporal-admin` (`temporalio/admin-tools`, one-shot) runs `temporal-sql-tool` create, setup-schema and update-schema for `temporal` and `temporal_visibility` using the `postgres12` plugin.
  - `temporal` (`temporalio/server`, pinned to the current stable 1.x tag, `DB=postgres12`) depends on `temporal-admin` completing successfully.
  - `temporal-ui`.
  - `migrate` runs the app image with `npx node-pg-migrate up -m migrations`.
  - `worker` (`WORKER_QUEUES=main,io,cpu,cluster`) and `worker-extra` (`WORKER_QUEUES=main,io,cpu`, `deploy.replicas: 0`) both mount `${DATA_DIR:-./data}:/data` and set `stop_grace_period: 60s`.
  - `grafana`. Every service gets `restart: unless-stopped`.
- **`docker-compose.test.yml`:** remaps every port to `127.0.0.1::<port>`.

- [ ] **Step 4: Run gates G2–G4**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage`
Expected: PASS, coverage thresholds met

- [ ] **Step 5: Verify the stack comes up**

Run: `docker compose up -d postgres temporal temporal-ui migrate && docker compose ps && curl -sf http://127.0.0.1:8080 >/dev/null && echo UI-OK`
Expected: postgres and temporal are healthy, migrate exited 0, and `UI-OK` is printed

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: compose infrastructure, Temporal on Postgres, app schema"
```

---

### Task 3: Dedupe core — classify and capture-consistent grouping

**Files:**
- Create: `src/classify.ts`, `src/grouping.ts`, and `src/hashing.ts` (only `hamming`, `toSigned` and `toUnsigned` here; Task 4 adds the rest)
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

- [ ] **Step 1: Write the failing tests**

```ts
const T = DEFAULT_THRESHOLDS;
const F = (sha: string, o: Partial<Facts> = {}): Facts => ({ sha256: sha, phash: 0n, dhash: 0n, width: 4000, height: 3000,
  mime: 'image/heic', captured: { s: 1_600_000_000, sub: '123' }, camera: 'Apple iPhone 12', burstUuid: null, ...o });
const NOEXIF = { captured: null, camera: null } as const;

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

test('toSigned/toUnsigned round-trip', () => fc.assert(fc.property(fc.bigUintN(64), (u) =>
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
    const g = runGrouper(EDGES, FACTS, perm);                    // fixture: 3 scenes × (burst pair + resized copy)
    expect(g.similarPartition()).toEqual(EXPECTED_SIMILAR);
    expect(g.copyPartition()).toEqual(EXPECTED_COPY);
  })));
```

`permutations`, `counter`, `range`, `runGrouper`, `EDGES`, `FACTS` and the `EXPECTED_*` constants are defined in the test module.

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

- [ ] **Step 4: Run gates G2–G4**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage`
Expected: PASS (fast-check runs 100 shuffles)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: copy/similar classifier and capture-consistent grouping"
```

---

### Task 4: Image decoding and perceptual hashes

**Files:**
- Create: `src/decode.ts`, `src/decodeWorker.ts`, `tests/helpers/images.ts` (sharp-generated JPEG/PNG/WebP fixtures, truncated and corrupt variants, a decompression-bomb PNG), `tests/helpers/corpus.ts` (`C(name)` path helper and `hasCorpus`)
- Modify: `src/hashing.ts` (add `phashFromGray` and `dhashFromGray`)
- Test: `tests/unit/hashing.test.ts`, `tests/unit/decode.test.ts`, `tests/unit/decode.corpus.test.ts`

**Interfaces:**
- Consumes: `hamming` (Task 3). RAW preview extraction calls `exiftool-vendored` directly (`extractPreview`); `MetadataReader` (Task 5) is not used here.
- Produces:
  - `phashFromGray(px: Uint8Array /* 32×32 */): bigint`, `dhashFromGray(px: Uint8Array /* 9×8 */): bigint`. Pure, row-major, first bit = MSB.
  - `interface ImageFacts`: `width: number`, `height: number`, `phash: bigint`, `dhash: bigint`, `mime: string`
  - `class QuarantineError extends Error { constructor(readonly reason: 'decode_error' | 'truncated' | 'decompression_bomb' | 'unsupported') }`
  - `computeImageFacts(absPath: string, ext: string): Promise<ImageFacts>`
  - `decodeWorker.ts` registers `{ computeImageFacts }` with `workerpool.worker`. Bigints cross the process boundary as decimal strings, and the caller converts them back.

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
test('EXIF orientation applied', async () => { /* same pixels saved rotated + orientation tag → hamming ≤ 4 */ });
test('dimensions are oriented original, not the 32×32 resize', async () => {
  const f = await computeImageFacts(await photo(await tmpDir(), 'jpg', { size: [4032, 3024] }), 'jpg');
  expect([f.width, f.height]).toEqual([4032, 3024]);
});
test('different photos are far apart', async () => { /* two unrelated synthetic scenes → hamming > 16 */ });
test.each([[truncatedJpeg, 'truncated'], [randomBytes, 'decode_error'], [bombPng, 'decompression_bomb']] as const)(
  'quarantine reason %#', async (make, reason) =>
    expect(computeImageFacts(await make(await tmpDir()), 'jpg')).rejects.toMatchObject({ reason }));

// decode.corpus.test.ts — describe.skipIf(!hasCorpus)
test('HEIC and its JPG twin hash within copy range', async () =>
  expect(hamming((await computeImageFacts(C('IMG_0034.HEIC'), 'heic')).phash,
                 (await computeImageFacts(C('IMG_0034.JPG'), 'jpg')).phash)).toBeLessThanOrEqual(4));
test('misnamed JPEG decodes', async () => expect((await computeImageFacts(C('IMG_E8719.HEIC'), 'jpg')).mime).toBe('image/jpeg'));
test('TIF and BMP decode', async () => { /* VERP1022.TIF, AUXA4949.BMP */ });
test('HEIC decode benchmark', async () => { /* 20 corpus HEICs, log ms/image; fail if median > 1500 ms */ });
```

`GOLDEN_PHASH` is computed once from the first correct implementation, cross-checked by hand against the algorithm in spec §6.2, and committed. It locks the bit layout.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/hashing.test.ts tests/unit/decode.test.ts`
Expected: FAIL with `Cannot find module` / `phashFromGray is not a function`

- [ ] **Step 3: Implement `phashFromGray`, `dhashFromGray` and `computeImageFacts`**

Follow spec §6.2:
- `sharp(input, { limitInputPixels: 200_000_000, failOn: 'truncated' }).rotate()`, then `.greyscale().resize(32, 32, { fit: 'fill' }).raw()` for pHash and `resize(9, 8)` for dHash.
- HEIC/HEIF: `heic-decode` → `sharp(data, { raw: { width, height, channels: 4 } })`.
- RAW extensions: `exiftool.extractPreview` into a temp file.
- Map sharp errors by message: "pixel limit" → `decompression_bomb`; "Premature end" or "truncated" → `truncated`; "unsupported image format" → `decode_error`. Unknown extensions/MIME → `unsupported`.
- DCT: a straightforward separable 2-D DCT-II over 32×32 is fine.

- [ ] **Step 4: Run gates G2–G5**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus`
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
  - `class MetadataReader` with methods `read(absPath: string): Promise<Metadata>` (exiftool-vendored raw read with `-G -n`) and `end(): Promise<void>`

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
test('zero/invalid date becomes null', () => { /* '0000:00:00 00:00:00' → null */ });

// metadata.corpus.test.ts
test.each([['IMG_E8719.HEIC', 'still', 'jpg'], ['IMG_0004.MOV', 'motion', 'mov'], ['VERP1022.TIF', 'still', 'tif'],
           ['AUXA4949.BMP', 'still', 'bmp']])('real %s', async (n, kind, ext) => {
  const m = await reader.read(C(n)); expect([m.kind, m.ext]).toEqual([kind, ext]);
});
test('AAE is other', async () => expect((await reader.read(C('IMG_0066.AAE'))).kind).toBe('other'));
test('a Live Photo pair shares a content id', async () => { /* IMG_2834 1.HEIC and IMG_2834 1.MOV */ });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/metadata.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement `metadata.ts`**

- `kind` is `still` when the MIME type is `image/*` and FileType ∈ {JPEG, PNG, HEIC, HEIF, WEBP, TIFF, GIF, BMP, DNG, CR2, CR3, NEF, ARW, ORF, RW2, RAF}. It is `motion` when the MIME type is `video/*`. Otherwise it is `other`.
- `subsec` is the string form of the tag.
- `ext` is `FileTypeExtension` lowercased, with `jpeg` normalised to `jpg`.

- [ ] **Step 4: Run gates G2–G5**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: exiftool metadata with content-based type detection"
```

---

### Task 6: `ingestFile` activity — the vault crash protocol

**Files:**
- Create: `src/types.ts`, `src/activities/index.ts`, `src/activities/ingest.ts`, `tests/helpers/activityEnv.ts`
- Test: `tests/unit/ingest.test.ts`

**Interfaces:**
- Consumes: `DataLayout`, `linkIntoVault`, `sha256File`, `vaultRel` (Task 1); `withTx` (Task 2); `MetadataReader` (Task 5)
- Produces:
  - In `types.ts`:
    - `interface FileRef { relPath: string; size: string; mtimeNs: string }`
    - `interface IngestResult { fileId: string; sha256: string | null; ext: string | null; state: 'INGESTED' | 'COPY' | 'QUARANTINED'; isNew: boolean; kind: 'still' | 'motion' | 'other' | null }`
  - In `activities/index.ts`:
    - `interface Deps { pool: pg.Pool; settings: Settings; layout: DataLayout; metadata: MetadataReader; temporal?: Client; checkpoint?: (name: string) => void }`
    - `createActivities(deps: Deps)`, which returns `{ ingestFile, … }` and gains members in later tasks
    - `type Activities = ReturnType<typeof createActivities>`
  - `ingestFile(ref: FileRef): Promise<IngestResult>`. It calls `heartbeat()` on every chunk, reads `Context.current().info.workflowExecution.runId`, and calls `deps.checkpoint?.(name)` at `'claim'`, `'link'` and `'copies_rename'` (a test seam).
  - Helper `runActivity(fn, args, { runId })`, using `MockActivityEnvironment` with the overridden `workflowExecution`

- [ ] **Step 1: Write the failing tests**

`drop(name, bytes, mtimeNs?)` writes into the drop zone and returns its `FileRef`. `crashAt(name)` builds Deps whose `checkpoint` throws `SimulatedCrash` once at that name.

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
test('heartbeats during hashing', async () => { /* 3 MiB file → MockActivityEnvironment heartbeat listener fires ≥ 3× */ });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/ingest.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement `ingestFile`** following spec §6.1 and §12 R1, in this order

1. Look up the row by `workflow_run_id`. If it is final, return the stored outcome. Otherwise `INSERT … ON CONFLICT (rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED') DO UPDATE SET workflow_run_id = EXCLUDED.workflow_run_id RETURNING id`.
2. **Claim** with `rename`. If the source is missing: claimed exists → continue; `.copies/<id>.*` exists → commit COPY; neither → QUARANTINED `vanished`. Run `checkpoint('claim')`. Hash, then commit `CLAIMED, sha256`.
3. Read metadata from the claimed file. Commit `ext`.
4. `linkIntoVault`, then `checkpoint('link')`. On `EXISTS_OTHER`, `rename` to `.copies/<id>.<ext>`, then `checkpoint('copies_rename')`.
5. In one transaction, `INSERT INTO contents … ON CONFLICT DO NOTHING` (state `INGESTED`, including the metadata columns) and update `files.state`.
6. `unlink` the claimed file if it still exists. `chmod 0o444` the vault file.

- [ ] **Step 4: Run gates G2–G5**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: idempotent ingest with crash-safe vault protocol"
```

---

### Task 7: Hash and cluster activities

**Files:**
- Create: `src/hashIndex.ts`, `src/activities/hashing.ts`, `src/activities/cluster.ts`
- Modify: `src/activities/index.ts` (register the new activities; create the decode pool `workerpool.pool(decodeWorkerPath, { workerType: 'process', maxWorkers: settings.cpuConcurrency })`), `src/types.ts`
- Test: `tests/unit/hashIndex.test.ts`, `tests/unit/hashClusterActivities.test.ts`

**Interfaces:**
- Consumes: `computeImageFacts` via the pool, `QuarantineError` (Task 4); `classify`, `Grouper`, `DEFAULT_THRESHOLDS` (Task 3); `vaultRel` (Task 1)
- Produces:
  - In `types.ts`: `interface DecodeResult { facts: { width: number; height: number; phash: string; dhash: string; mime: string } | null; quarantineReason: string | null }`
  - `decodeContent(sha256: string, ext: string): Promise<DecodeResult>`. Queue `cpu`, no database access. It runs `pool.exec('computeImageFacts', [abs, ext]).timeout(decodeTimeoutS * 1000)`. A timeout or child crash throws `ApplicationFailure.retryable(msg, 'Poison')`. A `QuarantineError` returns `{ facts: null, quarantineReason }`.
  - `recordHash(sha256: string, r: DecodeResult): Promise<'HASHED' | 'QUARANTINED'>`. Idempotent (`UPDATE … WHERE state = 'INGESTED'`; returns the current state when nothing changed).
  - `markFailed(sha256: string, error: string): Promise<void>`, which sets `contents.state = 'FAILED'` and `last_error`
  - `class HashIndex` with methods `clear(): void`, `append(rows: readonly Facts[], groups: readonly number[]): void` and `matches(f: Facts, t: Thresholds): Facts[]`. Hashes are stored as hi/lo `Uint32Array` pairs that grow by doubling.
  - `clusterContent(sha256: string): Promise<number>` (returns the copy group id). Queue `cluster`. It holds `pg_advisory_xact_lock(hashtext('pm.cluster'))`. It reloads `HashIndex` and `Grouper` from the database when `cluster_state.version` ≠ the cached version, and increments `version` in the same transaction. If the state is already `CLUSTERED`, it returns the existing group.

- [ ] **Step 1: Write the failing tests**

```ts
test('index matches within thresholds', () => {
  const idx = new HashIndex(); idx.append([F('a'), F('b', { phash: 0x1ffn })], [1, 2]);
  expect(idx.matches(F('n'), T).map((m) => m.sha256)).toEqual(['a']);
});
test('index grows past initial capacity', () => { /* append 10_000 rows; matches still exact */ });
test('decodeContent: timeout is retryable Poison', async () => {
  const acts = createActivities({ ...deps, settings: { ...deps.settings, decodeTimeoutS: 1 } });
  await expect(runActivity(acts.decodeContent, await vaultHangingFile())).rejects.toMatchObject({ type: 'Poison', nonRetryable: false });
});
test('decodeContent: corrupt file returns quarantine reason', async () =>
  expect(await runActivity(acts.decodeContent, await vaultFile(truncatedJpeg))).toEqual({ facts: null, quarantineReason: 'truncated' }));
test('recordHash is idempotent', async () => {
  const sha = await seedContent({ state: 'INGESTED' });
  expect(await runActivity(acts.recordHash, [sha, OK])).toBe('HASHED'); expect(await runActivity(acts.recordHash, [sha, OK])).toBe('HASHED');
});
test('recordHash quarantines', async () =>
  expect(await runActivity(acts.recordHash, [await seedContent({ state: 'INGESTED' }), { facts: null, quarantineReason: 'truncated' }])).toBe('QUARANTINED'));
test('cluster: copy joins, burst frame gets own group in same similar set', async () => {
  const [a, c, b] = await seedHashed(F('a'), F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }),
                                     F('b', { phash: 1n, captured: { s: 1_600_000_000, sub: '999' } }));
  const [ga, gc, gb] = [await cluster(a), await cluster(c), await cluster(b)];
  expect(ga).toBe(gc); expect(gb).not.toBe(ga);
  expect(await similarSetOf(ga)).not.toBeNull(); expect(await similarSetOf(ga)).toBe(await similarSetOf(gb));
});
test('cluster reloads when another worker wrote', async () => {
  const [a, c] = await seedHashed(F('a'), F('c', { phash: 3n, width: 2000, height: 1500, mime: 'image/jpeg' }));
  expect(await runActivity(acts.clusterContent, [a])).toBe(await runActivity(createActivities(deps).clusterContent, [c]));
});
test('cluster is idempotent', async () => { const [a] = await seedHashed(F('a')); expect(await cluster(a)).toBe(await cluster(a)); });
test('concurrent clusterContent calls serialise', async () => { /* Promise.all of 2 look-alike copies on 2 Activities objects → one group */ });
```

`vaultHangingFile()` uses an injected `decodeWorkerPath` pointing to a test worker whose `computeImageFacts` never resolves.

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

- [ ] **Step 4: Run gates G2–G5**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus`
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
- Consumes: the schema (Task 2); `vaultRel` (Task 1)
- Produces:
  - `interface StageContent`: `sha256`, `kind`, `ext`, `mime`, `width`, `height`, `sizeBytes: bigint`, `capturedAt: Date | null`, `liveContentId`, `durationS`, `state`, `quarantineReason`, `copyGroup: number | null`, `similarSet: number | null`, `ambiguous: boolean`
  - `interface StageFile`: `fileId: bigint`, `relPath`, `originalName`, `mtimeNs: bigint`, `sha256`, `state`, `srcRel`
  - `interface Entry { dest: string; src: string }`
  - `bestCopyKey(c: StageContent, hasLive: boolean): readonly (number | bigint | string)[]`, compared lexicographically, highest first: `!ambiguous`, then the spec §6.4 order. The final tie-break is the **lowest** `sha256`.
  - `pairLivePhotos(contents: readonly StageContent[], files: readonly StageFile[], maxDurationS: number): Map<string, string>` (still sha → motion sha)
  - `planStaging(contents: readonly StageContent[], files: readonly StageFile[], maxDurationS: number): Entry[]`, deterministic and sorted by `dest`
  - In `types.ts`: `interface BuildResult { gen: number | null; entries: number; skipped: boolean }`
  - `maybeBuildStaging(force: boolean): Promise<BuildResult>` (queue `io`, heartbeats every 1,000 links)

- [ ] **Step 1: Write the failing tests**

Here `C(...)` and `Fl(...)` are builders with defaults; `dests(entries)` returns the sorted `dest` strings.

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
test('Live MOV beside still with same base name', () => { /* liveContentId match → same dirname, same stem */ });
test('stem fallback is exact and duration-bounded', () => {
  const s1 = C('1'.repeat(64)), s2 = C('2'.repeat(64));
  const m1 = C('3'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 3 }), long = C('4'.repeat(64), { kind: 'motion', ext: 'mov', durationS: 30 });
  const files = [Fl(1n, 'd/IMG_2834.HEIC', s1), Fl(2n, 'd/IMG_2834 1.HEIC', s2), Fl(3n, 'd/IMG_2834 1.MOV', m1), Fl(4n, 'd/IMG_2834.MOV', long)];
  expect(pairLivePhotos([s1, s2, m1, long], files, 5)).toEqual(new Map([['2'.repeat(64), '3'.repeat(64)]]));
});
test('collision gets _sha8, then _2', () => { /* three contents with the same dated name */ });
test('ambiguous content is never best', () => { /* bigger ambiguous content still lands in copies/ */ });
test('quarantine, failed and other buckets', () => { /* QUARANTINED → quarantine/<reason>/; FAILED → quarantine/failed/; AAE → other/<rel_dir>/ */ });
test('names are independent of group ids', () => { /* same contents under different copyGroup/similarSet numbers → identical dests */ });
test('unclustered stills are not staged; INGESTED motion/other are', () => {});

test('build swaps atomically and is repeatable', async () => {
  await seedPipelineRows(); const r1 = await build(true); const r2 = await build(true);
  expect(r2.gen).toBe(r1.gen! + 1); expect(await tree(join(layout.staging, 'current'))).toEqual(await tree(genDir(r1.gen!)));
  expect(await readlink(join(layout.staging, 'current'))).toBe(`.gen-${r2.gen}`);
  expect((await readdir(layout.staging)).sort()).toEqual([`.gen-${r1.gen}`, `.gen-${r2.gen}`, 'current']);
});
test('staged files share inode with vault', async () => {});
test('orphan gen from a crash is removed', async () => { /* pre-create .gen-99 with no staging_builds row */ });
test('skips when unchanged', async () => { /* force=false, no newer updated_at → skipped: true */ });
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
- Collisions get `_<sha8>`, then `_2`, `_3`, … in order of `(sha256, relPath)`.

**Build:**
- Take the advisory lock `hashtext('pm.stage')`.
- The watermark is `greatest(max(files.updated_at), max(contents.updated_at))` combined with `cluster_state.version`.
- When `force` is false, skip if unchanged, or if there are running `FileWorkflow`s (via `deps.temporal.workflow.count(...)`) and the time since the last build is less than `stageMaxStalenessS`.
- Delete any `.gen-*` that has no `staging_builds` row. Link every entry, `symlink` to `.current.tmp`, then `rename` over `current`. Insert `staging_builds`. Delete generations older than the previous one.

- [ ] **Step 4: Run gates G2–G5**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: deterministic staging plan with atomic generation swap"
```

---

### Task 9: Workflows, scanner and worker

**Files:**
- Create: `src/workflows.ts`, `src/ids.ts`, `src/activities/scan.ts`, `src/worker.ts`, `src/cli.ts` (only `stage [--force]` here; Task 10 completes the CLI)
- Modify: `src/activities/index.ts`, `src/types.ts`, `Makefile` (`up`, `down`, `stage`)
- Test: `tests/workflow/workflows.test.ts`, `tests/unit/scan.test.ts`

**Interfaces:**
- Consumes: every activity from Tasks 6–8
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
  - `ensureSchedules(client: Client, s: Settings): Promise<void>`: `pm-scan` every `scanIntervalS` and `pm-stage` every 300 s, overlap `SKIP`. It ignores `ScheduleAlreadyRunning`.
  - Entrypoint: `node dist/worker.js`
- Activity options, as constants in `workflows.ts`:

| Activity | Queue | Timeouts | Retry |
|----------|-------|----------|-------|
| `ingestFile` | `io` | startToClose `1h`, heartbeat `30s` | unlimited, `1s` ×2 → `5m` |
| `decodeContent` | `cpu` | startToClose `2m` | `maximumAttempts: poisonAttempts` (default 5) |
| `recordHash` | `io` | startToClose `1m` | unlimited |
| `clusterContent` | `cluster` | startToClose `1m` | unlimited |
| `markFailed` | `io` | — | unlimited |
| `maybeBuildStaging` | `io` | startToClose `2h`, heartbeat `30s` | unlimited |

- [ ] **Step 1: Write the failing tests**

These use `TestWorkflowEnvironment.createTimeSkipping()`, with a `Worker` registering fake activity implementations under the real names.

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
test('ContentWorkflow runs content steps only', async () => {});

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
test('scan tolerates WorkflowExecutionAlreadyStartedError', async () => {});
test('scan backpressure', async () => { /* count() returns maxInFlight → started 0, scan_stats still written */ });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run --project workflow && npx vitest run tests/unit/scan.test.ts`
Expected: FAIL with `Cannot find module`

- [ ] **Step 3: Implement the workflows, scan, worker and minimal `cli stage`**

- **`scanAndStart`:** take two recursive `readdir` snapshots `stableSeconds` apart, heartbeating between them. Write `scan_stats`. Get `running` from `client.workflow.count("WorkflowType='FileWorkflow' AND ExecutionStatus='Running'")`. Start at most `maxInFlight - running` workflows on queue `main`, with `workflowIdReusePolicy: ALLOW_DUPLICATE` and `workflowIdConflictPolicy: FAIL`.
- **`worker.ts`:**
  - Startup checks: `checkSameDevice`, `layout.ensure()`, `metadata.read` on a probe file.
  - Create one `Worker` per entry in `WORKER_QUEUES`. Workflows (`workflowsPath`) run on `main`. Each other queue gets `maxConcurrentActivityTaskExecutions`: `ioConcurrency`, `cpuConcurrency`, or 1 for `cluster`.
  - Call `ensureSchedules` when `cluster` is served.
  - `shutdownGraceTime: '60s'`, and close the pg pool, the exiftool process and the decode pool on shutdown. Log with pino.

- [ ] **Step 4: Run gates G2–G6**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus && make down && make up && make fs-check`
Expected: PASS, `fs-check OK`

- [ ] **Step 5: Verify a real end-to-end trickle (G6)**

Run: `cp -c test-automation/sample-images/IMG_0034.* data/dropzone/ && sleep 60 && make stage FORCE=1 && find data/staging/current/ -type f`
Expected: `IMG_0034.HEIC` under `unique/…`, `IMG_0034.JPG` under `copies/g-…/`, and both workflows Completed in the Temporal UI

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
- Consumes: `StagingWorkflow`, `ContentWorkflow` (Task 9); `sha256File` (Task 1)
- Produces:
  - `main(argv: string[], deps?): Promise<number>` (the exit code) in `cli.ts`
  - `stage [--force]` (from Task 9)
  - `retry-failed`: resets FAILED contents to `INGESTED` and starts `ContentWorkflow` with ID `content-<sha>-retry-<epoch>`; prints the count
  - `verify-vault [--sample N]`: exits 1 and lists any vault file whose hash doesn't match its name
  - Dashboard titled "Pixel Magic: Pipeline Live", with `refresh: "5s"` and these panels: `Drop zone`, `In flight`, `Backlog by stage`, `Throughput / min`, `ETA`, `Copy groups`, `Similar sets`, `Reclaimable`, `Quarantined by reason`, `Failed`, `Recent errors`, `Last staging build`

- [ ] **Step 1: Write the failing tests**

```ts
test('every pixelmagic panel query runs against a seeded DB', async () => {
  await seedPipelineRowsSql(pool);
  for (const t of loadTargets('infra/grafana/dashboards/pipeline-live.json').filter((t) => t.datasource.uid === 'pixelmagic'))
    await expect(pool.query(t.rawSql)).resolves.toBeDefined();               // no Grafana macros
});
test('visibility panel query runs against a stub executions_visibility', async () => { /* create minimal table in scratch schema */ });
test('dashboard has required panels and 5s refresh', () => {
  const d = loadDashboard(); expect(d.panels.map((p) => p.title)).toEqual(expect.arrayContaining(REQUIRED)); expect(d.refresh).toBe('5s');
});
test('verify-vault detects mismatch', async () => {
  await writeVault(layout, 'a'); await writeVault(layout, 'b', { nameSha: '0'.repeat(64) });
  const out = captureStdout(); expect(await main(['verify-vault'], deps)).toBe(1); expect(out()).toContain('0'.repeat(64));
});
test('retry-failed resets and starts', async () => { /* 2 FAILED rows → state INGESTED, fakeClient started 2 ContentWorkflows */ });
test('unknown command exits 2 with usage', async () => {});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/cli.test.ts tests/unit/dashboardSql.test.ts`
Expected: FAIL

- [ ] **Step 3: Implement the CLI, datasource provisioning and dashboard JSON**

- The datasources are `pixelmagic` and `temporal_visibility`, both through the read-only role `pm_reader`.
- ETA = backlog ÷ the rate of `clustered_at` over the last 10 minutes.

- [ ] **Step 4: Run gates G2–G6**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && npm run test:corpus && make down && make up`
Expected: PASS

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
- Create: `tools/corpus/lib.ts`, `tools/corpus/buildManifest.ts`, `tools/corpus/seed.ts`, `tools/corpus/verify.ts`, `tools/corpus/synth.ts`, `tests/corpus/manifest.json` (generated)
- Modify: `Makefile` (`corpus-manifest`, `corpus-verify`), `.gitignore` (`tests/corpus/.hash-cache.json`)
- Test: `tests/unit/corpusTools.test.ts`

**Interfaces:**
- Consumes: nothing from `src/` except `isIgnored`. The oracle must stay independent: it uses the `sips` CLI and the `exiftool` CLI bundled with exiftool-vendored, called directly.
- Produces (in `lib.ts`):
  - `buildRelations(recs: ManifestRecord[]): Relations`
  - `selectSubsets(m: Manifest, seed: number): { smoke: string[]; standard: string[] }`
  - `seed(opts: SeedOptions): Promise<SeedManifest>`
  - `verify(corpusDir: string, m: Manifest, deep: boolean): Promise<string[]>` (the drift list)
- Manifest records: `name`, `size`, `mtimeNs`, `sha256`, `kind`, `extMismatch`, `width`, `height`, `capturedAt`, `subsec`, `burstUuid`, `contentId`, `durationS`
- Manifest relations: `exactGroups`, `heicJpgTwin`, `editOf`, `spaceSuffixCopy`, `burst` (same `BurstUUID`, or captures ≤ 2 s apart from the same camera with different subsec), `livePhoto` (content ID, or same stem with a MOV ≤ 5 s)
- Pinned files in both subsets: everything in spec §10.3
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
test('verify detects drift', async () => {});
test('burst relation needs same camera and different subsec', () => {});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/unit/corpusTools.test.ts`
Expected: FAIL

- [ ] **Step 3: Implement the tools**

- SHA-256 runs in parallel with `worker_threads` (`availableParallelism()`), with a cache keyed by `(name, size, mtimeNs)`.
- Cloning uses `cp -c -p` via `execFile` and falls back to `copyFile` + `utimes`. Check free space first.

- [ ] **Step 4: Run gates G2–G5 and G7, then build the real manifest**

Run: `npm test && npm run lint && npm run typecheck && npm run test:coverage && make corpus-manifest && make corpus-verify`
Expected:
- PASS
- the summary prints about 14,880 files
- `burst` count ≥ 5. If it is lower, report it; the `synth.ts` bridge cases then carry the burst tests.
- verify exits 0

- [ ] **Step 5: Commit**

```bash
git add tools tests/unit/corpusTools.test.ts tests/corpus/manifest.json Makefile .gitignore && git commit -m "feat: corpus manifest, deterministic subsets, CoW seeder, verifier"
```

---

### Task 12: End-to-end harness, invariant and smoke/standard suites

**Files:**
- Create: `tests/lib/stack.ts`, `tests/lib/invariant.ts`, `tests/lib/expectations.ts`, `tests/lib/snapshot.ts`, `tests/e2e/smoke.test.ts`, `tests/e2e/standard.test.ts`, `tests/e2e/golden/smoke_tree.txt`
- Modify: `Makefile` (`test-smoke`, `test-standard`, `update-golden`; each runs `corpus-verify` before and after), `package.json` (`test:e2e`)
- Test: the files above

**Interfaces:**
- Consumes: `seed` and the manifest (Task 11); the compose files (Task 2); `cli stage` (Task 9)
- Produces:
  - `class Stack { constructor(runId: string, dataDir: string) }` with methods `up()`, `down(keep: boolean)`, `kill(svc)`, `restart(svc)`, `pause(svc, ms)`, `killAll()`, `waitIdle(timeoutMs)`, `stage()`, and the property `dbUrl`
  - `checkInvariant(dbUrl: string, dataDir: string, seedManifest: SeedManifest): Promise<string[]>` (violations; empty means it passed)
  - `treeSnapshot(dataDir: string, dbUrl: string): Promise<string[]>`: lines of `dest\tsha256`, with entries of `copy_ambiguous` contents shown as `AMBIGUOUS\t<sha256>`
  - `diffExpectations(dbUrl: string, seedManifest: SeedManifest, manifest: Manifest): Promise<string[]>`
  - `partitions(dbUrl: string): Promise<{ similar: Set<string>; copy: Set<string> }>` (the copy partition excludes ambiguous contents)

- [ ] **Step 1: Write the e2e tests** (vitest project `e2e`, tier selected by `TIER` env)

```ts
test('smoke: invariant, expectations, golden tree', async () => {
  expect(await checkInvariant(...ctx)).toEqual([]); expect(await diffExpectations(...exp)).toEqual([]);
  expect(await treeSnapshot(...snap)).toEqual(await readGolden('smoke_tree.txt'));
});
test('pinned cases', async () => {
  expect(await kindOf('IMG_E8719.HEIC')).toBe('still'); expect(await mimeOf('IMG_E8719.HEIC')).toBe('image/jpeg');
  expect(await kindOf('IMG_0004.MOV')).toBe('motion');
  expect(await sameCopyGroup('IMG_0034.HEIC', 'IMG_0034.JPG')).toBe(true);
  expect(await sameCopyGroup('IMG_2834.HEIC', 'IMG_2834 1.HEIC')).toBe(false);
  expect(new Set(await states(['PSWU5412.MP4', 'PSWU5412 1.MP4']))).toEqual(new Set(['INGESTED', 'COPY']));
  expect(await beside('IMG_2834 1.MOV', 'IMG_2834 1.HEIC')).toBe(true);
});
test('standard: no burst pair shares a copy group', async () => {
  for (const [a, b] of manifest.relations.burst) expect(await sameCopyGroup(a, b), `${a} ${b}`).toBe(false);
});
test('standard: synthetic resized/re-encoded variants join their seed’s copy group', async () => {});
test('every Live Photo pair staged together', async () => {});
test('junk never discovered', async () => {});
test('corrupt extras quarantined with expected reasons', async () => {});
test('re-seed under new paths is all COPY', async () => {});
test('nested Unicode layout works', async () => {});
```

- [ ] **Step 2: Run them and watch them fail**

Run: `make test-smoke`
Expected: FAIL (the golden file doesn't exist yet), with a readable per-file diff report for any other mismatch

- [ ] **Step 3: Implement the harness, generate the golden file and review it by hand**

- Isolation: `COMPOSE_PROJECT_NAME=pmtest_<runId>`, `DATA_DIR=data-test/<runId>`, and `docker-compose.test.yml`. The test reads the mapped ports via `docker compose port`.
- `waitIdle`: two consecutive 5 s polls in which running workflows = 0 and the drop zone has no eligible files. Then call `stage()`.
- On failure: keep the stack and dump per-state counts and the last 200 log lines from each service.

Run `make update-golden`, then **read `smoke_tree.txt` and confirm every pinned case lands in the expected bucket before committing it.**

- [ ] **Step 4: Run gates G2–G8**

Run: `npm test && npm run test:coverage && npm run test:corpus && make corpus-verify && make test-smoke && make test-standard`
Expected: PASS. Smoke takes ≤ 5 min. `corpus-verify` passes before and after.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "test: corpus-driven e2e harness, golden invariant, smoke/standard suites"
```

---

### Task 13: Chaos proof

**Files:**
- Create: `tests/chaos/chaos.test.ts`
- Modify: `Makefile` (`chaos`), `package.json` (`test:chaos`)
- Test: `tests/chaos/chaos.test.ts`

**Interfaces:**
- Consumes: `Stack`, `checkInvariant`, `treeSnapshot`, `diffExpectations`, `partitions` (Task 12)

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

- [ ] **Step 3: Confirm it passes three times in a row (G8)**

Run: `for i in 1 2 3; do make chaos || break; done && make test-standard`
Expected: 3 PASS, and standard still green

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test: chaos proof that crashes never change the outcome"
```
