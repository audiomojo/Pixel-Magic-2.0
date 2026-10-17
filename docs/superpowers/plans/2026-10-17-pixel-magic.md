# Pixel Magic 2.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a resumable, Temporal-orchestrated photo-dedupe pipeline. It moves dropped files into a content-addressed vault, groups true copies of the same shot and keeps every similar shot, and rebuilds a hardlinked staging tree that can be regenerated at any time.

**Architecture:** One Python package (`pixelmagic`) runs as Temporal workers on the task queues `main` (workflows), `io`, `cpu` and `cluster`. Postgres holds the facts (`files`, `contents`, `groups`). Temporal holds progress. Staging is computed from the database and swapped in with an atomic symlink rename. Compose runs postgres, temporal, temporal-ui, migrate, worker, worker-extra and grafana.

**Tech Stack:** Python 3.12, uv, temporalio, psycopg 3 (async + pool), Alembic/SQLAlchemy (migrations only), pydantic-settings, Pillow, pillow-heif, rawpy, imagehash, NumPy ≥ 2.0, PyExifTool (+ exiftool binary), pebble, structlog. Tests use pytest, pytest-asyncio, hypothesis and testcontainers[postgres]. Lint and types use ruff and mypy (strict on `src/`).

**Spec:** `docs/superpowers/specs/2026-10-17-pixel-magic-design.md` (including §12 "Revisions during planning")

## Global Constraints

- Python 3.12. The package lives at `src/pixelmagic/`, and every command runs through `uv run`.
- All config comes from env vars with the exact names below. Defaults: `DATA_DIR=/data`, `DATABASE_URL`, `TEMPORAL_ADDRESS=temporal:7233`, `TEMPORAL_NAMESPACE=default`, `WORKER_QUEUES=main,io,cpu,cluster`, `SCAN_INTERVAL_S=30`, `STABLE_SECONDS=5`, `MAX_IN_FLIGHT=5000`, `IO_CONCURRENCY=8`, `CPU_CONCURRENCY=<cpu count>`, `DECODE_TIMEOUT_S=60`, `POISON_ATTEMPTS=5`, `LOOK_ALIKE_PHASH=8`, `LOOK_ALIKE_DHASH=10`, `COPY_PHASH=4`, `COPY_DHASH=6`, `LIVE_MAX_DURATION_S=5`, `STAGE_MAX_STALENESS_S=900`.
- The `DATA_DIR` layout is `dropzone/`, `work/claimed/`, `vault/<ab>/<cd>/<sha256>.<ext>`, `vault/.copies/<file_id>.<ext>`, `staging/.gen-<n>/` and `staging/current` (symlink). Every path stored in the database is **relative to `DATA_DIR`**.
- **Nothing in the pipeline ever unlinks a file it hasn't first hardlinked or renamed somewhere else.** The only unlinks allowed are `work/claimed/<id>` after its vault link is committed, and old `staging/.gen-*` directories.
- Vault files are mode `0444`. Containers run as non-root (`UID`/`GID` build args, default 501/20).
- Published ports are `127.0.0.1` only: Grafana 3000, Temporal UI 8080, Postgres 5432, Temporal 7233.
- The corpus `test-automation/sample-images/` is never written to. It stays git-ignored, and tests reach it only by cloning with `cp -c`.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **An EXIF-less export that looks like two different burst frames** must never put both frames into one copy group. The export is flagged `copy_ambiguous` and is never chosen as a best copy. Pinned by `test_bridge_export_never_merges_conflicting_shots` (Task 3) and the e2e burst check (Task 12).
2. **Re-dropping a file that was already ingested** (same path, size and mtime) gets a new `files` row and ends as `COPY`. It must not be stuck in the drop zone or wrongly marked `vanished`. Pinned by `test_redrop_after_ingest_becomes_copy` (Task 6).
3. **An ingest activity re-run after its commit but before Temporal recorded completion** (a crash at that moment) returns the same outcome, without creating a second row. Pinned by `test_rerun_after_commit_returns_same_outcome` (Task 6).
4. **A Postgres or Temporal outage of several minutes while images are being hashed** makes the work wait. It never marks content `FAILED`, because only `decode_content` has an attempt limit. Pinned by `test_record_hash_outage_never_fails_content` (Task 9).
5. **Opening a staged file for writing** (an editor saving in place) must fail without changing the vault original. Pinned by `test_staged_file_is_read_only` (Task 8).

---

## File Structure

```
pyproject.toml            uv project; deps; ruff/mypy/pytest config (markers: corpus, e2e, chaos)
Dockerfile                python:3.12-slim + exiftool (libimage-exiftool-perl) + libheif; non-root; uv sync
docker-compose.yml        postgres, temporal-admin (schema), temporal, temporal-ui, migrate, worker, worker-extra, grafana
docker-compose.test.yml   override: ephemeral 127.0.0.1 ports, DATA_DIR from env
Makefile                  up down logs lint fmt test fs-check stage retry-failed verify-vault corpus-* test-smoke test-standard chaos update-golden
.env.example
infra/postgres/init.sql   creates DBs pixelmagic, temporal, temporal_visibility
infra/grafana/            provisioning/{datasources,dashboards}/*.yml, dashboards/pipeline-live.json
alembic.ini, migrations/  Alembic env + 0001_initial.py
src/pixelmagic/
  config.py               Settings, get_settings()
  types.py                dataclasses shared by workflows and activities
  fsops.py                DataLayout, check_same_device, vault_rel, sha256_stream, link_into_vault, is_ignored
  fscheck.py              `python -m pixelmagic.fscheck`: proves link/rename/symlink-swap on the mount
  db.py                   async pool factory, transaction helper
  hashing.py              to_signed/to_unsigned, ImageFacts, QuarantineError, compute_image_facts
  metadata.py             Metadata, parse_metadata (pure), ExifTool reader
  classify.py             Facts, Thresholds, hamming, looks_alike, capture_conflict, classify
  grouping.py             Grouper (capture-consistent union-find), Assignment
  index.py                HashIndex (NumPy arrays + match query)
  staging_plan.py         StageContent, StageFile, Entry, best_copy_key, pair_live_photos, plan_staging
  activities/
    ingest.py             ingest_file
    hashing.py            decode_content, record_hash, mark_failed
    cluster.py            cluster_content
    staging.py            maybe_build_staging
    scan.py               scan_and_start
  workflows.py            FileWorkflow, ContentWorkflow, ScanWorkflow, StagingWorkflow, run_content_steps
  worker.py               `python -m pixelmagic.worker`: workers per queue, ensure_schedules, startup checks
  cli.py                  `python -m pixelmagic.cli {stage,retry-failed,verify-vault}`
tools/corpus/             build_manifest.py, seed.py, verify.py, synth.py
tests/
  conftest.py             pg (testcontainers, migrated), data_layout (tmp DATA_DIR), image fixtures
  unit/ …                 one file per module
  workflow/test_workflows.py
  lib/                    stack.py, invariant.py, expectations.py, snapshot.py
  e2e/                    test_smoke.py, test_standard.py, golden/smoke_tree.txt
  chaos/test_chaos.py
  corpus/manifest.json    committed: names + hashes only
```

---

### Task 1: Scaffold, config and filesystem guarantees

**Files:**
- Create: `pyproject.toml`, `Dockerfile`, `Makefile`, `.env.example`, `src/pixelmagic/__init__.py`, `src/pixelmagic/config.py`, `src/pixelmagic/fsops.py`, `src/pixelmagic/fscheck.py`, `README.md`
- Modify: `.gitignore` (already ignores the corpus, `data/` and `data-test/`)
- Test: `tests/unit/test_config.py`, `tests/unit/test_fsops.py`

**Interfaces:**
- Produces:
  - `Settings` (pydantic-settings, field names = lower-case env names from Global Constraints) and `get_settings() -> Settings`
  - `DataLayout(root: Path)` with properties `dropzone`, `claimed`, `vault`, `vault_copies`, `staging`, a method `ensure() -> None`, and `rel(p: Path) -> str`
  - `check_same_device(layout: DataLayout) -> None`, which raises `SameDeviceError`
  - `vault_rel(sha256: str, ext: str) -> str`
  - `sha256_stream(path: Path, on_chunk: Callable[[], None] | None = None, chunk: int = 1 << 20) -> str`
  - `LinkOutcome` enum: `LINKED | ALREADY_LINKED | EXISTS_OTHER`
  - `link_into_vault(src: Path, dest: Path) -> LinkOutcome` (creates parent dirs; fsyncs the dest dir)
  - `is_ignored(name: str) -> bool`

- [ ] **Step 1: Write the failing tests**

```python
def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    s = get_settings()
    assert (s.scan_interval_s, s.stable_seconds, s.max_in_flight) == (30, 5, 5000)
    assert (s.look_alike_phash, s.look_alike_dhash, s.copy_phash, s.copy_dhash) == (8, 10, 4, 6)
    assert s.worker_queues == ["main", "io", "cpu", "cluster"]

def test_vault_rel():
    assert vault_rel("ab" + "cd" + "0" * 60, "heic") == "vault/ab/cd/abcd" + "0" * 60 + ".heic"

@pytest.mark.parametrize("name,ignored", [(".DS_Store", True), ("._IMG_1.HEIC", True), ("a.part", True),
    ("a.crdownload", True), ("~$lock", True), ("IMG_2834 1.HEIC", False), ("café.jpg", False)])
def test_is_ignored(name, ignored): assert is_ignored(name) is ignored

def test_link_into_vault_outcomes(tmp_path):
    a, b, dest = tmp_path/"a", tmp_path/"b", tmp_path/"v/ab/cd/x.jpg"
    a.write_bytes(b"1"); b.write_bytes(b"1")
    assert link_into_vault(a, dest) is LinkOutcome.LINKED
    assert link_into_vault(a, dest) is LinkOutcome.ALREADY_LINKED
    assert link_into_vault(b, dest) is LinkOutcome.EXISTS_OTHER
    assert os.stat(a).st_ino == os.stat(dest).st_ino

def test_sha256_stream_calls_on_chunk(tmp_path):
    p = tmp_path/"f"; p.write_bytes(b"x" * (3 << 20)); calls = []
    assert sha256_stream(p, on_chunk=lambda: calls.append(1)) == hashlib.sha256(b"x" * (3 << 20)).hexdigest()
    assert len(calls) == 3

def test_check_same_device_passes_on_one_fs(tmp_path):
    layout = DataLayout(tmp_path); layout.ensure(); check_same_device(layout)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py tests/unit/test_fsops.py -v`
Expected: FAIL with `ModuleNotFoundError: pixelmagic`

- [ ] **Step 3: Implement the scaffold, `config.py` and `fsops.py`**

- **`pyproject.toml`:** list every dependency from Tech Stack. Configure ruff (line length 100). Set mypy `strict = true` for `src`. Declare pytest markers `corpus`, `e2e` and `chaos`, with `asyncio_mode = "auto"`.
- **`link_into_vault`:** use `os.link`. On `FileExistsError`, compare with `os.path.samefile`.
- **`check_same_device`:** compare `st_dev` across the four roots.
- **`fscheck.main()`:** in a temp dir under `DATA_DIR`, run link, rename, and a `symlink` + `os.replace` swap over an existing symlink, and check that the inode is shared. Exit 0 and print `fs-check OK`, otherwise exit 1.
- **`Dockerfile`:** `apt-get install libimage-exiftool-perl`; create the user from `UID`/`GID` args; `uv sync --frozen --no-dev`.
- **`Makefile`:** add the targets listed in File Structure. They may be stubs until their task: `lint`, `fmt`, `test` (= `uv run pytest tests/unit tests/workflow`) and `fs-check` (= `docker compose run --rm worker python -m pixelmagic.fscheck`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit -v && uv run ruff check . && uv run mypy src`
Expected: PASS, no lint or type errors

- [ ] **Step 5: Verify the guarantees on the real Docker Desktop bind mount**

Run: `docker build -t pixelmagic . && docker run --rm -v "$PWD/data:/data" pixelmagic python -m pixelmagic.fscheck`
Expected: `fs-check OK`. If it fails, stop: the one-filesystem design does not hold on this mount.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: scaffold, config, filesystem primitives and fs-check"
```

---

### Task 2: Infrastructure, schema and migrations

**Files:**
- Create: `docker-compose.yml`, `docker-compose.test.yml`, `infra/postgres/init.sql`, `alembic.ini`, `migrations/env.py`, `migrations/versions/0001_initial.py`, `src/pixelmagic/db.py`, `tests/conftest.py`
- Test: `tests/unit/test_schema.py`

**Interfaces:**
- Consumes: `Settings.database_url`
- Produces:
  - Schema (spec §5 with the §12 revisions):
    - `files(id, rel_path, original_name, size_bytes, mtime_ns, ext, sha256, state, workflow_run_id UNIQUE, attempts, last_error, discovered_at, updated_at)`
    - `contents(sha256 PK, ext, kind, mime, size_bytes, width, height, phash, dhash, captured_at, subsec, camera, burst_uuid, live_content_id, duration_s, state, quarantine_reason, copy_group_id, copy_ambiguous bool default false, last_error, ingested_at, hashed_at, clustered_at, updated_at)`
    - `groups(id, kind, merged_into, similar_set_id)`
    - `cluster_state(id=1, version bigint)`
    - `scan_stats(id=1, pending_files, pending_bytes, scanned_at)`
    - `staging_builds(gen PK, watermark, built_at, entries)`
  - CHECK constraints for every state column. The partial unique index is on `files(rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED')`.
  - `db.create_pool(url: str, max_size: int = 10) -> AsyncConnectionPool`
  - Fixture `pg` (session scope): a testcontainers Postgres 16 with `alembic upgrade head` applied, which yields a URL. Fixture `conn` (function scope) truncates every table.

- [ ] **Step 1: Write the failing tests**

```python
async def test_partial_unique_allows_redrop_after_final(conn):
    ins = "INSERT INTO files(rel_path, original_name, size_bytes, mtime_ns, state) VALUES ('a.jpg','a.jpg',1,1,%s)"
    await conn.execute(ins, ("INGESTED",))
    await conn.execute(ins, ("DISCOVERED",))           # allowed: the earlier row is final
    with pytest.raises(psycopg.errors.UniqueViolation):
        await conn.execute(ins, ("CLAIMED",))           # two non-final rows for one drop

async def test_state_check_rejects_unknown(conn):
    with pytest.raises(psycopg.errors.CheckViolation):
        await conn.execute("INSERT INTO files(rel_path, original_name, size_bytes, mtime_ns, state) VALUES ('a','a',1,1,'NOPE')")

async def test_bigint_holds_signed_phash(conn):
    await conn.execute("INSERT INTO contents(sha256, ext, kind, mime, size_bytes, state, phash) VALUES (%s,'jpg','still','image/jpeg',1,'HASHED',%s)", ("0"*64, -(2**63)))
    assert (await (await conn.execute("SELECT phash FROM contents")).fetchone())[0] == -(2**63)

async def test_singletons_seeded(conn):
    assert (await (await conn.execute("SELECT version FROM cluster_state WHERE id=1")).fetchone())[0] == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_schema.py -v`
Expected: FAIL (`relation "files" does not exist`, or the fixture fails because migrations are missing)

- [ ] **Step 3: Implement the migration, `db.py` and compose**

- **Compose:**
  - `postgres:16` with `init.sql`, a `pg_isready` healthcheck and the `pg-data` volume.
  - `temporalio/admin-tools` one-shot that runs `temporal-sql-tool` setup and update-schema for `temporal` and `temporal_visibility` (Postgres plugin `postgres12`).
  - `temporalio/server` (pin the current stable 1.x tag) with `DB=postgres12`, depending on the admin step completing successfully.
  - `temporalio/ui`.
  - `migrate` (the app image, `alembic upgrade head`).
  - `worker` (`WORKER_QUEUES=main,io,cpu,cluster`) and `worker-extra` (`WORKER_QUEUES=main,io,cpu`, `deploy.replicas: 0`), both mounting `${DATA_DIR:-./data}:/data` and with `stop_grace_period: 60s`.
  - `grafana`. Every service gets `restart: unless-stopped`.
- **`docker-compose.test.yml`:** remaps every port to `127.0.0.1::<port>`, which assigns ephemeral ports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_schema.py -v`
Expected: PASS

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
- Create: `src/pixelmagic/classify.py`, `src/pixelmagic/grouping.py`
- Test: `tests/unit/test_classify.py`, `tests/unit/test_grouping.py`

**Interfaces:**
- Produces:
  - `Facts`, a frozen dataclass: `sha256: str`, `phash: int`, `dhash: int` (both unsigned), `width: int`, `height: int`, `mime: str`, `captured_s: int | None` (epoch seconds), `subsec: str | None`, `camera: str | None`, `burst_uuid: str | None`
  - `Thresholds`, a frozen dataclass with `look_alike_phash=8, look_alike_dhash=10, copy_phash=4, copy_dhash=6`
  - `hamming(a: int, b: int) -> int`
  - `looks_alike(a: Facts, b: Facts, t: Thresholds) -> bool`
  - `capture_conflict(a: tuple[int, str | None], b: tuple[int, str | None]) -> bool`: true if the seconds differ, or if both subsecs are present and differ
  - `classify(a: Facts, b: Facts, t: Thresholds) -> Literal["copy", "similar"]`, the spec §6.3 table, evaluated top to bottom
  - `Grouper(new_id: Callable[[Literal["copy", "similar"]], int])` with methods:
    - `load(sha256: str, copy_group: int, similar_set: int | None, capture: tuple[int, str | None] | None, ambiguous: bool) -> None`
    - `add(f: Facts, matches: Sequence[tuple[str, Literal["copy", "similar"]]]) -> Assignment`
    - `copy_partition() -> set[frozenset[str]]`
    - `similar_partition() -> set[frozenset[str]]`: the connected components of the look-alike graph, including singleton copy groups
  - `Assignment`, a frozen dataclass: `copy_group: int`, `ambiguous: bool`, `created: list[tuple[int, str]]`, `copy_merges: list[tuple[int, int]]` (loser, winner), `similar_merges: list[tuple[int, int]]`, `similar_links: dict[int, int]` (copy group → similar set)

- [ ] **Step 1: Write the failing tests**

```python
T = Thresholds()
def F(sha, ph=0, dh=0, w=4000, h=3000, mime="image/heic", t=1_600_000_000, sub="123", cam="Apple iPhone 12", burst=None):
    return Facts(sha, ph, dh, w, h, mime, t, sub, cam, burst)

@pytest.mark.parametrize("a,b,want", [
    (F("a", burst="B1"), F("b", ph=1, burst="B1"), "similar"),                     # same burst → never copy
    (F("a"), F("b", t=1_600_000_001), "similar"),                                 # different second
    (F("a"), F("b", sub="456"), "similar"),                                       # different subsec
    (F("a"), F("b", cam="Canon"), "similar"),                                     # different camera
    (F("a"), F("b", ph=3, w=2000, h=1500, mime="image/jpeg"), "copy"),            # same capture incl. subsec
    (F("a", sub=None), F("b", sub=None, ph=2), "similar"),                        # same second, no subsec, same dims+mime
    (F("a", sub=None), F("b", sub=None, ph=2, w=2000, h=1500), "copy"),           # same second, no subsec, resized
    (F("a"), F("b", ph=4, dh=6, t=None, sub=None, cam=None), "copy"),             # EXIF-less export, tight match
    (F("a"), F("b", ph=5, t=None, sub=None, cam=None), "similar"),                # EXIF-less, not tight enough
    (F("a", t=None, sub=None, cam=None), F("b", ph=1, t=None, sub=None, cam=None), "similar"),  # both EXIF-less, same dims
    (F("a", t=None, sub=None, cam=None), F("b", ph=1, w=10, h=10, t=None, sub=None, cam=None), "copy"),
])
def test_classify_table(a, b, want): assert classify(a, b, T) == want

def test_looks_alike_bounds():
    assert looks_alike(F("a"), F("b", ph=0b11111111, dh=0b1111111111), T)
    assert not looks_alike(F("a"), F("b", ph=0b111111111), T)

def ids():
    n = itertools.count(1); return lambda kind: next(n)

def test_bridge_export_never_merges_conflicting_shots():
    for order in itertools.permutations(["A", "B", "X"]):
        g = Grouper(ids()); facts = {"A": F("A", t=1), "B": F("B", t=2), "X": F("X", t=None, sub=None, cam=None)}
        verdict = {frozenset("AX"): "copy", frozenset("BX"): "copy", frozenset("AB"): "similar"}
        seen = []
        for s in order:
            a = g.add(facts[s], [(o, verdict[frozenset(s + o)]) for o in seen]); seen.append(s)
        assert not any({"A", "B"} <= grp for grp in g.copy_partition())
        assert g.similar_partition() == {frozenset("ABX")}

@given(st.permutations(range(12)))
def test_similar_partition_is_order_independent(perm):
    # fixture graph: 12 contents, a fixed edge list with mixed copy/similar verdicts and no conflicts
    assert run_grouper(EDGES, FACTS, perm).similar_partition() == EXPECTED_SIMILAR
    assert run_grouper(EDGES, FACTS, perm).copy_partition() == EXPECTED_COPY
```

`run_grouper`, `EDGES`, `FACTS` and the `EXPECTED_*` constants are defined in the test module: 12 shots in 3 scenes, where every scene has one burst pair and one resized copy.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_classify.py tests/unit/test_grouping.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `classify.py` and `grouping.py`**

`Grouper.add` follows this algorithm:

```
copy_targets = distinct root copy groups of matches with verdict "copy"
group = new copy group (or the first target if any)
accepted = []
for target in sorted(copy_targets, key=(-size, id)):        # deterministic: largest first
    if captures(group) ∪ captures(target) ∪ {f.capture} has two conflicting keys: refuse → ambiguous = True; treat as "similar"
    else: union(group, target) (smaller → larger; ties → lower id is winner) and record copy_merges
similar_targets = root copy groups of every match (copy or similar) that is not in `group`
if similar_targets: union all of their similar sets plus group's into one (creating one if none), record merges/links
```

Here `captures(group)` is the set of non-null capture keys of its members.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_classify.py tests/unit/test_grouping.py -v`
Expected: PASS (hypothesis runs 100 permutations)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: copy/similar classifier and capture-consistent grouping"
```

---

### Task 4: Image decoding and perceptual hashes

**Files:**
- Create: `src/pixelmagic/hashing.py`, `tests/fixtures.py` (generates images in `tmp_path` with Pillow and pillow-heif)
- Test: `tests/unit/test_hashing.py`

**Interfaces:**
- Produces:
  - `to_signed(u: int) -> int`, `to_unsigned(s: int) -> int`
  - `ImageFacts`, a frozen dataclass: `width: int`, `height: int`, `phash: int`, `dhash: int` (both unsigned), `mime: str`
  - `QuarantineError(reason: str)` with reasons `decode_error | truncated | decompression_bomb | unsupported`
  - `compute_image_facts(path: str) -> ImageFacts`. It is top-level and picklable, and runs in a pebble child.

- [ ] **Step 1: Write the failing tests**

```python
@given(st.integers(0, 2**64 - 1))
def test_signed_roundtrip(u): assert to_unsigned(to_signed(u)) == u and -(2**63) <= to_signed(u) < 2**63

def test_hash_stable_across_format_and_size(tmp_path):
    jpg, png, heic, small = make_photo(tmp_path, "jpg"), make_photo(tmp_path, "png"), make_photo(tmp_path, "heic"), make_photo(tmp_path, "jpg", scale=0.5)
    base = compute_image_facts(str(jpg))
    for other in (png, heic, small):
        assert hamming(base.phash, compute_image_facts(str(other)).phash) <= 4

def test_exif_orientation_is_applied(tmp_path):
    assert hamming(compute_image_facts(str(make_photo(tmp_path, "jpg"))).phash,
                   compute_image_facts(str(make_photo(tmp_path, "jpg", rotate_with_exif=True))).phash) <= 4

def test_dimensions_are_original_not_draft(tmp_path):
    f = compute_image_facts(str(make_photo(tmp_path, "jpg", size=(4032, 3024))))
    assert (f.width, f.height) == (4032, 3024)

@pytest.mark.parametrize("maker,reason", [(make_truncated_jpeg, "truncated"), (make_random_bytes, "decode_error"),
                                          (make_bomb_png, "decompression_bomb")])
def test_quarantine_reasons(tmp_path, maker, reason):
    with pytest.raises(QuarantineError) as e: compute_image_facts(str(maker(tmp_path)))
    assert e.value.reason == reason
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_hashing.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `compute_image_facts`** following spec §6.2

- `Image.MAX_IMAGE_PIXELS = 200_000_000`. Map `DecompressionBombError` to `decompression_bomb`.
- Map an `OSError` containing "truncated" to `truncated`, and `UnidentifiedImageError` to `decode_error`.
- For RAW extensions use `rawpy.imread(...).extract_thumb()`.
- Read the original size **before** `draft()`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_hashing.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: draft-mode decode with pHash/dHash and quarantine reasons"
```

---

### Task 5: Metadata via exiftool

**Files:**
- Create: `src/pixelmagic/metadata.py`
- Test: `tests/unit/test_metadata.py`

**Interfaces:**
- Produces:
  - `Metadata`, a frozen dataclass: `file_type: str`, `mime: str`, `ext: str`, `kind: Literal["still", "motion", "other"]`, `captured_at: datetime | None` (tz-aware), `subsec: str | None`, `camera: str | None`, `burst_uuid: str | None`, `live_content_id: str | None`, `duration_s: float | None`
  - `parse_metadata(tags: dict[str, Any]) -> Metadata`, which is pure. Its keys are exiftool `-G -n` names: `File:FileType`, `File:MIMEType`, `File:FileTypeExtension`, `EXIF:DateTimeOriginal`, `EXIF:OffsetTimeOriginal`, `EXIF:SubSecTimeOriginal`, `EXIF:Make`, `EXIF:Model`, `MakerNotes:BurstUUID`, `MakerNotes:ContentIdentifier`, `QuickTime:ContentIdentifier`, `QuickTime:Duration` and `QuickTime:CreateDate`.
  - `ExifTool`, a context manager wrapping one `exiftool -stay_open` process, with `read(path: Path) -> Metadata`

- [ ] **Step 1: Write the failing tests**

```python
def test_heic_still():
    m = parse_metadata({"File:FileType": "HEIC", "File:MIMEType": "image/heic", "File:FileTypeExtension": "HEIC",
        "EXIF:DateTimeOriginal": "2021:10:03 14:02:11", "EXIF:OffsetTimeOriginal": "-07:00",
        "EXIF:SubSecTimeOriginal": "482", "EXIF:Make": "Apple", "EXIF:Model": "iPhone 12",
        "MakerNotes:BurstUUID": "B-1", "MakerNotes:ContentIdentifier": "C-1"})
    assert (m.kind, m.ext, m.subsec, m.camera, m.burst_uuid, m.live_content_id) == ("still", "heic", "482", "Apple iPhone 12", "B-1", "C-1")
    assert m.captured_at == datetime(2021, 10, 3, 21, 2, 11, tzinfo=UTC)

def test_misnamed_jpeg_uses_content_type():
    assert parse_metadata({"File:FileType": "JPEG", "File:MIMEType": "image/jpeg", "File:FileTypeExtension": "JPG"}).ext == "jpg"

def test_live_mov():
    m = parse_metadata({"File:FileType": "MOV", "File:MIMEType": "video/quicktime", "File:FileTypeExtension": "MOV",
                        "QuickTime:ContentIdentifier": "C-1", "QuickTime:Duration": 2.97})
    assert (m.kind, m.live_content_id, m.duration_s) == ("motion", "C-1", 2.97)

def test_aae_and_unknown_are_other():
    assert parse_metadata({"File:FileType": "XMP", "File:MIMEType": "application/rdf+xml", "File:FileTypeExtension": "XMP"}).kind == "other"
    assert parse_metadata({}).kind == "other"

def test_no_offset_is_treated_as_utc():
    assert parse_metadata({"File:FileType": "JPEG", "File:MIMEType": "image/jpeg", "File:FileTypeExtension": "JPG",
        "EXIF:DateTimeOriginal": "2000:01:01 00:00:00"}).captured_at == datetime(2000, 1, 1, tzinfo=UTC)

@pytest.mark.corpus
@pytest.mark.parametrize("name,kind,ext", [("IMG_E8719.HEIC", "still", "jpg"), ("IMG_0004.MOV", "motion", "mov"),
                                            ("VERP1022.TIF", "still", "tif"), ("AUXA4949.BMP", "still", "bmp")])
def test_real_files(corpus_dir, name, kind, ext):
    with ExifTool() as et: m = et.read(corpus_dir / name)
    assert (m.kind, m.ext) == (kind, ext)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_metadata.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `metadata.py`**

- `kind` is `still` when the MIME type is `image/*` and FileType ∈ {JPEG, PNG, HEIC, HEIF, WEBP, TIFF, GIF, BMP, DNG, CR2, CR3, NEF, ARW, ORF, RW2, RAF}. It is `motion` when the MIME type is `video/*`. Otherwise it is `other`.
- `camera` is `f"{Make} {Model}"` with the Make de-duplicated.
- The `corpus_dir` fixture skips the test when `test-automation/sample-images` is absent.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_metadata.py -v`
Expected: PASS (corpus tests pass on this Mac)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: exiftool metadata with content-based type detection"
```

---

### Task 6: `ingest_file` activity — the vault crash protocol

**Files:**
- Create: `src/pixelmagic/types.py`, `src/pixelmagic/activities/__init__.py` (an `Activities` class holding `pool`, `settings`, `layout`, `exiftool`, `client`), `src/pixelmagic/activities/ingest.py`
- Test: `tests/unit/test_ingest.py`

**Interfaces:**
- Consumes: `DataLayout`, `link_into_vault`, `sha256_stream`, `vault_rel` (Task 1); `ExifTool.read` (Task 5); the schema (Task 2)
- Produces:
  - `FileRef(rel_path: str, size: int, mtime_ns: int)`
  - `IngestResult(file_id: int, sha256: str | None, state: Literal["INGESTED", "COPY", "QUARANTINED"], is_new: bool, kind: str | None)`
  - `Activities.ingest_file(ref: FileRef) -> IngestResult`, an `@activity.defn` that heartbeats on every chunk. It reads `activity.info().workflow_run_id`.

- [ ] **Step 1: Write the failing tests**

The tests call `acts.ingest_file` through `ActivityEnvironment`, with `info` overridden to set `workflow_run_id`. A `drop(name, data)` helper writes into the drop zone and returns its `FileRef`.

```python
async def test_new_file_lands_in_vault(acts, layout, conn):
    r = await run(acts.ingest_file, drop("IMG 1.jpg", JPEG_A), run_id="r1")
    assert (r.state, r.is_new, r.kind) == ("INGESTED", True, "still")
    v = layout.root / vault_rel(r.sha256, "jpg")
    assert v.exists() and oct(v.stat().st_mode)[-3:] == "444" and not any(layout.claimed.iterdir())
    assert not (layout.dropzone / "IMG 1.jpg").exists()

async def test_byte_identical_second_file_is_copy(acts, layout):
    a = await run(acts.ingest_file, drop("a.jpg", JPEG_A), run_id="r1")
    b = await run(acts.ingest_file, drop("b.jpg", JPEG_A), run_id="r2")
    assert (b.state, b.is_new, b.sha256) == ("COPY", False, a.sha256)
    assert (layout.vault_copies / f"{b.file_id}.jpg").exists()

async def test_resume_after_claim_before_commit(acts, layout, conn):
    ref = drop("a.jpg", JPEG_A); crash_after(acts, "claim")           # raises after rename, before CLAIMED commit
    with pytest.raises(SimulatedCrash): await run(acts.ingest_file, ref, run_id="r1")
    r = await run(acts.ingest_file, ref, run_id="r1")
    assert r.state == "INGESTED" and await count(conn, "files") == 1

async def test_resume_after_vault_link_before_commit(acts, layout, conn): ...  # crash_after("link") → ALREADY_LINKED path → INGESTED
async def test_resume_after_copies_rename_before_commit(acts, layout, conn): ...  # crash_after("copies_rename") → COPY

async def test_rerun_after_commit_returns_same_outcome(acts, conn):
    ref = drop("a.jpg", JPEG_A)
    first = await run(acts.ingest_file, ref, run_id="r1")
    assert await run(acts.ingest_file, ref, run_id="r1") == first and await count(conn, "files") == 1

async def test_redrop_after_ingest_becomes_copy(acts, conn):
    ref = drop("a.jpg", JPEG_A); await run(acts.ingest_file, ref, run_id="r1")
    ref2 = drop("a.jpg", JPEG_A, mtime_ns=ref.mtime_ns)               # same path, size, mtime
    r = await run(acts.ingest_file, ref2, run_id="r2")
    assert r.state == "COPY" and await count(conn, "files") == 2

async def test_vanished_file_is_quarantined(acts):
    r = await run(acts.ingest_file, FileRef("gone.jpg", 1, 1), run_id="r1")
    assert (r.state, r.sha256) == ("QUARANTINED", None)

async def test_mov_is_motion_and_not_new_still(acts):
    r = await run(acts.ingest_file, drop("IMG_1.MOV", MOV_BYTES), run_id="r1")
    assert (r.state, r.kind) == ("INGESTED", "motion")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_ingest.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `ingest_file`** following spec §6.1 and §12 R1, in this order

1. Look up the row by `workflow_run_id`. If it is final, return its stored outcome. Otherwise `INSERT … ON CONFLICT (rel_path, size_bytes, mtime_ns) WHERE state IN ('DISCOVERED','CLAIMED') DO UPDATE SET workflow_run_id = EXCLUDED.workflow_run_id RETURNING id`.
2. Claim with `rename` (resume branches as in §6.1: claimed exists → continue; `.copies/<id>.*` exists → commit COPY; neither → QUARANTINED `vanished`). Hash, then commit `CLAIMED, sha256, ext`.
3. Read metadata from the claimed file.
4. `link_into_vault`. On `EXISTS_OTHER`, rename to `vault/.copies/<id>.<ext>`.
5. In one transaction, `INSERT contents ON CONFLICT DO NOTHING` (state `INGESTED`, kind from the metadata) and update `files.state`.
6. Unlink the claimed file. `chmod 0444` the vault file.

`crash_after` is a test seam: `Activities._checkpoint(name)`, which is a no-op in production.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_ingest.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: idempotent ingest with crash-safe vault protocol"
```

---

### Task 7: Hash and cluster activities

**Files:**
- Create: `src/pixelmagic/index.py`, `src/pixelmagic/activities/hashing.py`, `src/pixelmagic/activities/cluster.py`
- Test: `tests/unit/test_index.py`, `tests/unit/test_hash_cluster_activities.py`

**Interfaces:**
- Consumes: `compute_image_facts`, `QuarantineError`, `to_signed` (Task 4); `classify`, `Grouper`, `Thresholds` (Task 3); `vault_rel` (Task 1)
- Produces:
  - `DecodeResult(facts: ImageFacts | None, quarantine_reason: str | None)`
  - `Activities.decode_content(sha256: str, ext: str) -> DecodeResult`. Queue `cpu`, no database access. It uses a pebble `ProcessPool(max_workers=cpu_concurrency)` with `timeout=decode_timeout_s`. A `TimeoutError` or `ProcessExpired` raises `ApplicationError("…", type="Poison")`, which is retryable.
  - `Activities.record_hash(sha256: str, result: DecodeResult) -> str`. Queue `io`; returns `"HASHED"` or `"QUARANTINED"`. It is idempotent: `UPDATE … WHERE state = 'INGESTED'`.
  - `Activities.mark_failed(sha256: str, error: str) -> None`, which sets `contents.state = 'FAILED'` and `last_error`
  - `HashIndex` with methods `clear()`, `append(rows: Sequence[Facts], groups: Sequence[int])` and `matches(f: Facts, t: Thresholds) -> list[Facts]` (uses `np.bitwise_count`)
  - `Activities.cluster_content(sha256: str) -> int` (returns the copy group id). Queue `cluster`. It holds `pg_advisory_xact_lock(hashtext('pm.cluster'))`. It reloads `HashIndex` and `Grouper` from the database when `cluster_state.version` ≠ the cached version, and increments the version in the same transaction as its writes. It is idempotent: if the state is already `CLUSTERED`, it returns the existing group.

- [ ] **Step 1: Write the failing tests**

```python
def test_index_matches_within_thresholds():
    idx = HashIndex(); idx.append([F("a", ph=0), F("b", ph=0b111111111)], [1, 2])
    assert [m.sha256 for m in idx.matches(F("n", ph=0), T)] == ["a"]

async def test_decode_poison_on_timeout(acts, monkeypatch):
    monkeypatch.setattr(acts.settings, "decode_timeout_s", 1)
    with pytest.raises(ApplicationError) as e: await run(acts.decode_content, *vault_hanging_file(acts))  # fixture: an fn that sleeps
    assert e.value.type == "Poison" and not e.value.non_retryable

async def test_record_hash_is_idempotent(acts, conn):
    sha = await seed_content(conn, state="INGESTED")
    assert await run(acts.record_hash, sha, DecodeResult(FACTS, None)) == "HASHED"
    assert await run(acts.record_hash, sha, DecodeResult(FACTS, None)) == "HASHED"

async def test_record_hash_quarantines(acts, conn):
    sha = await seed_content(conn, state="INGESTED")
    assert await run(acts.record_hash, sha, DecodeResult(None, "truncated")) == "QUARANTINED"

async def test_cluster_copy_and_burst(acts, conn):
    a, copy, burst = await seed_hashed(conn, F("a"), F("c", ph=2, w=2000, h=1500, mime="image/jpeg"), F("b", ph=1, sub="999"))
    ga, gc, gb = [await run(acts.cluster_content, s) for s in (a, copy, burst)]
    assert ga == gc != gb
    assert await similar_set_of(conn, ga) == await similar_set_of(conn, gb) is not None

async def test_cluster_reloads_when_another_worker_wrote(acts, acts2, conn):
    a, b = await seed_hashed(conn, F("a"), F("c", ph=2, w=2000, h=1500, mime="image/jpeg"))
    assert await run(acts.cluster_content, a) == await run(acts2.cluster_content, b)      # acts2 has a stale cache

async def test_cluster_is_idempotent(acts, conn):
    (a,) = await seed_hashed(conn, F("a"))
    assert await run(acts.cluster_content, a) == await run(acts.cluster_content, a)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_index.py tests/unit/test_hash_cluster_activities.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement the activities**

`cluster_content` persists the `Assignment` from Task 3 like this:
- insert the created groups
- `UPDATE groups SET merged_into` for losers
- `UPDATE contents SET copy_group_id = winner WHERE copy_group_id = loser`
- `UPDATE groups SET similar_set_id` per entry in `similar_links`, re-pointing groups whose set was merged
- set `contents.copy_ambiguous`, `state = 'CLUSTERED'`, `clustered_at`

Update the in-memory index only after the commit.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_index.py tests/unit/test_hash_cluster_activities.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: decode/record/cluster activities with single-active clustering"
```

---

### Task 8: Staging plan and atomic build

**Files:**
- Create: `src/pixelmagic/staging_plan.py`, `src/pixelmagic/activities/staging.py`
- Test: `tests/unit/test_staging_plan.py`, `tests/unit/test_staging_build.py`

**Interfaces:**
- Consumes: the schema (Task 2); `vault_rel` (Task 1)
- Produces:
  - `StageContent`, a frozen dataclass: `sha256`, `kind`, `ext`, `mime`, `width`, `height`, `size_bytes`, `captured_at`, `live_content_id`, `duration_s`, `state`, `quarantine_reason`, `copy_group`, `similar_set`, `ambiguous`
  - `StageFile`, a frozen dataclass: `file_id`, `rel_path`, `original_name`, `mtime_ns`, `sha256`, `state`, `src_rel`
  - `Entry(dest: str, src: str)`
  - `best_copy_key(c: StageContent, has_live: bool) -> tuple`: `not ambiguous`, then the spec §6.4 order, then `sha256` reversed so the lowest sha wins. Used as `max(key=…)`.
  - `pair_live_photos(contents: Sequence[StageContent], files: Sequence[StageFile], max_duration_s: float) -> dict[str, str]` (still sha → motion sha)
  - `plan_staging(contents: Sequence[StageContent], files: Sequence[StageFile], max_duration_s: float) -> list[Entry]`, deterministic and sorted by `dest`
  - `BuildResult(gen: int | None, entries: int, skipped: bool)`
  - `Activities.maybe_build_staging(force: bool) -> BuildResult` (queue `io`, heartbeats every 1,000 links)

- [ ] **Step 1: Write the failing tests**

Here `C(...)` and `Fl(...)` are builders with defaults.

```python
def test_unique_best_copy_and_copies_folder():
    heic = C("h"*64, ext="heic", width=4032, height=3024, copy_group=1); jpg = C("j"*64, ext="jpg", width=2016, height=1512, copy_group=1)
    e = plan_staging([heic, jpg], [Fl(1, "IMG_0034.HEIC", heic), Fl(2, "IMG_0034.JPG", jpg)], 5)
    assert dests(e) == ["copies/g-hhhhhhhh/IMG_0034.JPG", "unique/2021/2021-10/20211003_140211_IMG_0034.HEIC"]

def test_burst_set_keeps_every_shot_in_similar():
    a, b = C("a"*64, copy_group=1, similar_set=9), C("b"*64, copy_group=2, similar_set=9)
    assert all(d.startswith("similar/2021/2021-10/s-") for d in dests(plan_staging([a, b], [Fl(1, "A.HEIC", a), Fl(2, "B.HEIC", b)], 5)))

def test_live_mov_beside_still_with_same_base_name():
    s, m = C("s"*64, live_content_id="L"), C("m"*64, kind="motion", ext="mov", live_content_id="L", duration_s=2.9)
    d = dests(plan_staging([s, m], [Fl(1, "IMG_1.HEIC", s), Fl(2, "IMG_1.MOV", m)], 5))
    assert {Path(x).parent for x in d} == {Path(d[0]).parent} and {Path(x).stem for x in d} == {Path(d[0]).stem}

def test_stem_fallback_is_exact_and_duration_bounded():
    s1, s2 = C("1"*64), C("2"*64)
    m1, long = C("3"*64, kind="motion", ext="mov", duration_s=3), C("4"*64, kind="motion", ext="mov", duration_s=30)
    files = [Fl(1, "d/IMG_2834.HEIC", s1), Fl(2, "d/IMG_2834 1.HEIC", s2), Fl(3, "d/IMG_2834 1.MOV", m1), Fl(4, "d/IMG_2834.MOV", long)]
    assert pair_live_photos([s1, s2, m1, long], files, 5) == {"2"*64: "3"*64}

def test_collision_gets_sha8_suffix(): ...                 # two different contents with the same dated name
def test_ambiguous_content_is_never_best(): ...
def test_quarantine_failed_and_other_buckets(): ...        # QUARANTINED → quarantine/<reason>/, FAILED → quarantine/failed/, AAE → other/<rel_dir>/
def test_group_folder_names_are_content_derived(): ...      # same plan under different group ids → identical dests

async def test_build_swaps_atomically_and_is_idempotent(acts, layout, conn):
    await seed_pipeline_rows(conn, layout)
    r1 = await run(acts.maybe_build_staging, True); r2 = await run(acts.maybe_build_staging, True)
    assert r2.gen == r1.gen + 1 and tree(layout.staging / "current") == tree_of_gen(layout, r1.gen)
    assert os.readlink(layout.staging / "current") == f".gen-{r2.gen}"
    assert sorted(p.name for p in layout.staging.iterdir()) == [f".gen-{r1.gen}", f".gen-{r2.gen}", "current"]

async def test_staged_files_share_inode_with_vault(acts, layout, conn): ...
async def test_orphan_gen_from_crash_is_removed(acts, layout, conn): ...   # pre-create .gen-99 with no staging_builds row
async def test_skips_when_unchanged(acts, conn): ...                        # force=False, no newer updated_at → skipped=True

async def test_staged_file_is_read_only(acts, layout, conn):
    await seed_pipeline_rows(conn, layout); await run(acts.maybe_build_staging, True)
    f = next((layout.staging / "current" / "unique").rglob("*.*"))
    with pytest.raises(PermissionError): open(f, "r+b")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_staging_plan.py tests/unit/test_staging_build.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `staging_plan.py` and `maybe_build_staging`**

**Rules** (spec §6.4 + §12 R5 and R6):
- `g-<sha8>` is the group's best content's sha. `s-<sha8>` is the best content across the whole set.
- Collisions get `_<sha8>`, then `_2`, `_3`, … in order of `(sha256, rel_path)`.
- Contents that are not yet `CLUSTERED` (stills) are skipped. Motion and other contents with state `INGESTED` are staged.

**Build:**
- Take the advisory lock `hashtext('pm.stage')`.
- The watermark is `greatest(max(files.updated_at), max(contents.updated_at), max(groups changes via cluster_state))`.
- When `force=False`, skip if the watermark ≤ the last build's watermark, or if running `FileWorkflow`s > 0 and the time since the last build < `stage_max_staleness_s`.
- Delete any `.gen-*` directory that has no `staging_builds` row. Link every entry, `os.symlink` to `.current.tmp`, then `os.replace` over `current`. Insert `staging_builds`. Delete generations older than the previous one.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_staging_plan.py tests/unit/test_staging_build.py -v`
Expected: PASS (`test_staged_file_is_read_only` is skipped when run as root, with a reason message)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: deterministic staging plan with atomic generation swap"
```

---

### Task 9: Workflows, scanner and worker

**Files:**
- Create: `src/pixelmagic/workflows.py`, `src/pixelmagic/activities/scan.py`, `src/pixelmagic/worker.py`
- Test: `tests/workflow/test_workflows.py`, `tests/unit/test_scan.py`

**Interfaces:**
- Consumes: every `Activities.*` method from Tasks 6–8
- Produces:
  - Workflows:
    - `FileWorkflow.run(ref: FileRef) -> str` (the final content or file state)
    - `ContentWorkflow.run(sha256: str, ext: str) -> str`
    - `ScanWorkflow.run() -> ScanResult`
    - `StagingWorkflow.run(force: bool = False) -> BuildResult`
  - `run_content_steps(sha256: str, ext: str) -> str`: a workflow-side helper that runs decode → record → cluster and calls `mark_failed` on decode exhaustion
  - `ScanResult(started: int, pending_files: int, pending_bytes: int)`
  - `Activities.scan_and_start() -> ScanResult` (queue `io`)
  - `file_workflow_id(ref: FileRef) -> str`, which is `"file-" + sha1(f"{rel_path}|{size}|{mtime_ns}")`
  - `ensure_schedules(client: Client) -> None`: schedule `pm-scan` every `scan_interval_s` and `pm-stage` every 300 s, both with overlap `SKIP`
  - Entrypoint: `python -m pixelmagic.worker`
- Activity options, as module constants in `workflows.py`:

| Activity | Queue | Timeouts | Retry |
|----------|-------|----------|-------|
| `ingest_file` | `io` | start-to-close 1 h, heartbeat 30 s | unlimited, 1 s ×2 → 300 s |
| `decode_content` | `cpu` | start-to-close 2 min | max attempts `poison_attempts` |
| `record_hash` | `io` | start-to-close 1 min | unlimited |
| `cluster_content` | `cluster` | start-to-close 1 min | unlimited |
| `mark_failed` | `io` | — | unlimited |
| `maybe_build_staging` | `io` | start-to-close 2 h, heartbeat 30 s | unlimited |

- [ ] **Step 1: Write the failing tests**

These use `WorkflowEnvironment.start_time_skipping()`, with fake activities registered under the real activity names.

```python
async def test_new_still_runs_all_steps(env): assert await run_file(env, fakes(ingest=INGESTED_STILL)) == "CLUSTERED"
async def test_copy_stops_after_ingest(env):
    f = fakes(ingest=COPY_RESULT); assert await run_file(env, f) == "COPY" and f.calls == ["ingest_file"]
async def test_motion_stops_after_ingest(env): assert await run_file(env, fakes(ingest=INGESTED_MOTION)) == "INGESTED"
async def test_quarantine_stops_before_cluster(env):
    f = fakes(ingest=INGESTED_STILL, record="QUARANTINED"); assert await run_file(env, f) == "QUARANTINED" and "cluster_content" not in f.calls
async def test_decode_poison_marks_failed_after_5(env):
    f = fakes(ingest=INGESTED_STILL, decode=always_poison); assert await run_file(env, f) == "FAILED"
    assert f.calls.count("decode_content") == 5 and f.calls[-1] == "mark_failed"
async def test_record_hash_outage_never_fails_content(env):
    f = fakes(ingest=INGESTED_STILL, record=fail_n_times(40, then="HASHED")); assert await run_file(env, f) == "CLUSTERED"
async def test_ingest_retries_through_outage(env): assert await run_file(env, fakes(ingest=fail_n_times(20, then=INGESTED_STILL))) == "CLUSTERED"

def test_file_workflow_id_is_stable():
    assert file_workflow_id(FileRef("a b/é.jpg", 1, 2)) == file_workflow_id(FileRef("a b/é.jpg", 1, 2)) != file_workflow_id(FileRef("a b/é.jpg", 1, 3))

async def test_scan_only_starts_stable_files(acts_with_fake_client, layout):
    write_growing_file(layout.dropzone / "big.mov")                   # appends during the 5 s window
    (layout.dropzone / "ok.jpg").write_bytes(JPEG_A); (layout.dropzone / ".DS_Store").write_bytes(b"x")
    r = await run(acts_with_fake_client.scan_and_start)
    assert started_rel_paths(acts_with_fake_client) == ["ok.jpg"] and r.pending_files == 2

async def test_scan_ignores_already_started(acts_with_fake_client, layout): ...  # client raises WorkflowAlreadyStartedError → counted, no error
async def test_scan_backpressure(acts_with_fake_client, layout): ...           # count_workflows returns MAX_IN_FLIGHT → started == 0, scan_stats still written
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/workflow tests/unit/test_scan.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement the workflows, scan and worker**

- **`scan_and_start`:** take two `os.walk` snapshots `stable_seconds` apart (heartbeat between them). Write `scan_stats`. Get `running = client.count_workflows("WorkflowType='FileWorkflow' AND ExecutionStatus='Running'")`. Start up to `max_in_flight - running` workflows with `id_reuse_policy=ALLOW_DUPLICATE` and `id_conflict_policy=FAIL`, on task queue `main`.
- **`worker.main()`:**
  - Startup checks: `check_same_device`, an exiftool version check, `layout.ensure()`.
  - Create one `temporalio.worker.Worker` per entry in `WORKER_QUEUES`. Workflows run on `main`. Each other queue gets its own activities and `max_concurrent_activities`: `io_concurrency`, `cpu_concurrency`, or 1 for `cluster`.
  - Call `ensure_schedules` when the `cluster` queue is served.
  - Graceful shutdown on SIGTERM with a 60 s timeout.
  - Logs: structlog JSON.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/workflow tests/unit/test_scan.py -v`
Expected: PASS

- [ ] **Step 5: Verify a real end-to-end trickle**

Run: `make up && cp -c test-automation/sample-images/IMG_0034.* data/dropzone/ && sleep 60 && make stage FORCE=1 && find data/staging/current -type f`
Expected: `IMG_0034.HEIC` under `unique/…`, `IMG_0034.JPG` under `copies/g-…/`, and both workflows Completed in the Temporal UI

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: Temporal workflows, scanner with backpressure, worker entrypoint"
```

---

### Task 10: Operator CLI and Grafana dashboard

**Files:**
- Create: `src/pixelmagic/cli.py`, `infra/grafana/provisioning/datasources/pg.yml`, `infra/grafana/provisioning/dashboards/pm.yml`, `infra/grafana/dashboards/pipeline-live.json`
- Modify: `Makefile` (wire up `stage`, `retry-failed` and `verify-vault`)
- Test: `tests/unit/test_cli.py`, `tests/unit/test_dashboard_sql.py`

**Interfaces:**
- Consumes: `StagingWorkflow`, `ContentWorkflow` (Task 9); `sha256_stream` (Task 1)
- Produces:
  - `cli stage [--force]`: runs `StagingWorkflow` with ID `stage-manual-<ts>` and waits
  - `cli retry-failed`: resets FAILED contents to `INGESTED` and starts `ContentWorkflow` with ID `content-<sha>-retry-<epoch>`; prints the count
  - `cli verify-vault [--sample N]`: exits 1 and lists any vault file whose hash doesn't match its name
  - Dashboard panels with these titles: `Drop zone`, `In flight`, `Backlog by stage`, `Throughput / min`, `ETA`, `Copy groups`, `Similar sets`, `Reclaimable`, `Quarantined by reason`, `Failed`, `Recent errors`, `Last staging build`

- [ ] **Step 1: Write the failing tests**

```python
async def test_every_panel_query_runs(conn):
    await seed_pipeline_rows_sql(conn)
    for panel in load_panels("infra/grafana/dashboards/pipeline-live.json"):
        for target in panel["targets"]:
            await conn.execute(target["rawSql"])                     # no Grafana macros allowed

def test_dashboard_has_required_panels():
    assert {p["title"] for p in load_panels(...)} >= REQUIRED_TITLES and load_json(...)["refresh"] == "5s"

def test_verify_vault_detects_mismatch(tmp_layout, capsys):
    good, bad = write_vault(tmp_layout, b"a"), write_vault(tmp_layout, b"b", name_sha="0"*64)
    assert cli_main(["verify-vault"]) == 1 and "0"*64 in capsys.readouterr().out

async def test_retry_failed_resets_and_starts(conn, fake_client): ...
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_cli.py tests/unit/test_dashboard_sql.py -v`
Expected: FAIL

- [ ] **Step 3: Implement the CLI, datasource provisioning and dashboard JSON**

- The datasource points at the `pixelmagic` DB with a read-only role created in `init.sql`.
- The "In flight" panel uses `temporal_visibility.executions_visibility` (`status = 1`), grouped by `workflow_type_name`.
- ETA = backlog ÷ the rate of `clustered_at` over the last 10 minutes.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_cli.py tests/unit/test_dashboard_sql.py -v`
Expected: PASS

- [ ] **Step 5: Verify in the browser**

Run: `make up && open http://127.0.0.1:3000`
Expected: "Pixel Magic: Pipeline Live" loads with no panel errors

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: operator CLI and Postgres-backed Grafana dashboard"
```

---

### Task 11: Corpus manifest, seeder and verifier

**Files:**
- Create: `tools/corpus/build_manifest.py`, `tools/corpus/seed.py`, `tools/corpus/verify.py`, `tools/corpus/synth.py`, `tests/corpus/manifest.json` (generated)
- Modify: `Makefile` (`corpus-manifest`, `corpus-verify`)
- Test: `tests/unit/test_corpus_tools.py`

**Interfaces:**
- Consumes: nothing from `pixelmagic` except `is_ignored`. The oracle must stay independent: it uses `sips` and the exiftool CLI directly.
- Produces:
  - Manifest records: `name`, `size`, `mtime_ns`, `sha256`, `kind`, `ext_mismatch`, `width`, `height`, `captured_at`, `subsec`, `burst_uuid`, `content_id`, `duration_s`
  - Manifest relations: `exact_groups`, `heic_jpg_twin`, `edit_of`, `space_suffix_copy`, `burst` (same `BurstUUID`, or captures ≤ 2 s apart with different subsec from the same camera), `live_photo` (content ID, or same stem with a MOV ≤ 5 s)
  - Manifest `subsets`: `smoke` and `standard`, seeded and stratified, including the pinned files from spec §10.3
  - `seed.py --subset {smoke,standard,full} --dest DIR [--layout flat|nested] [--shuffle SEED] [--extras junk,corrupt] [--synthetic]` writes `DIR/../seed-manifest.json`
  - `synth.py`, which generates variants of real seeds: resize 50% keeping EXIF, re-encode q60 with EXIF stripped, PNG conversion, and an EXIF-stripped export of one burst frame (the bridge case). Ground truth lands in the seed manifest.
  - `verify.py [--deep]`, which exits non-zero on any drift

- [ ] **Step 1: Write the failing tests** (tiny fixtures under `tmp_path`, no corpus needed)

```python
def test_relations_from_names():
    r = build_relations([rec("IMG_0034.HEIC"), rec("IMG_0034.JPG"), rec("IMG_E0433.HEIC"), rec("IMG_0433.HEIC"),
                         rec("IMG_2834 1.HEIC"), rec("IMG_2834.HEIC")])
    assert ("IMG_0034.HEIC", "IMG_0034.JPG") in r["heic_jpg_twin"] and ("IMG_E0433.HEIC", "IMG_0433.HEIC") in r["edit_of"]
    assert ("IMG_2834 1.HEIC", "IMG_2834.HEIC") in r["space_suffix_copy"]

def test_subsets_are_deterministic_and_include_pins():
    m = fake_manifest(500); assert select_subsets(m, seed=7) == select_subsets(m, seed=7)
    assert PINNED <= set(select_subsets(m, seed=7)["smoke"])

def test_seed_clones_without_touching_source(tmp_path):
    src = make_corpus(tmp_path); before = snapshot(src)
    seed(src, subset=["a.jpg"], dest=tmp_path / "dz"); assert snapshot(src) == before and (tmp_path / "dz/a.jpg").exists()

def test_verify_detects_drift(tmp_path): ...
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_corpus_tools.py -v`
Expected: FAIL

- [ ] **Step 3: Implement the tools**

- SHA-256 runs in parallel with a `ProcessPoolExecutor`, with a cache at `tests/corpus/.hash-cache.json` (git-ignored) keyed by `(name, size, mtime_ns)`.
- Cloning uses `cp -c -p` and falls back to `shutil.copy2`.

- [ ] **Step 4: Run the tests, then build the real manifest**

Run: `uv run pytest tests/unit/test_corpus_tools.py -v && make corpus-manifest && make corpus-verify`
Expected:
- tests PASS
- the manifest summary prints about 14,880 files
- `burst` count ≥ 5. If it is lower, report it and rely on `synth.py` bridge cases.
- verify exits 0

- [ ] **Step 5: Commit**

```bash
git add tools tests/unit/test_corpus_tools.py tests/corpus/manifest.json Makefile && git commit -m "feat: corpus manifest, deterministic subsets, CoW seeder, verifier"
```

---

### Task 12: End-to-end harness, invariant and smoke/standard suites

**Files:**
- Create: `tests/lib/stack.py`, `tests/lib/invariant.py`, `tests/lib/expectations.py`, `tests/lib/snapshot.py`, `tests/e2e/conftest.py`, `tests/e2e/test_smoke.py`, `tests/e2e/test_standard.py`, `tests/e2e/golden/smoke_tree.txt`
- Modify: `Makefile` (`test-smoke`, `test-standard`, `update-golden`; each runs `corpus-verify` before and after)
- Test: the files above

**Interfaces:**
- Consumes: `seed.py`, the manifest (Task 11); the compose files (Task 2); `cli stage` (Task 10)
- Produces:
  - `Stack(run_id: str, data_dir: Path)` with methods `up()`, `down(keep: bool)`, `kill(service)`, `restart(service)`, `kill_all()`, `wait_idle(timeout_s) -> None`, `stage()`, `db_url`
  - `check_invariant(db_url: str, data_dir: Path, seed_manifest: dict) -> list[str]` (violations; empty means it passed)
  - `tree_snapshot(data_dir: Path, db_url: str) -> list[str]`: lines of `dest\tsha256`, with entries of `copy_ambiguous` contents shown as `AMBIGUOUS\t<sha256>`
  - `diff_expectations(db_url: str, seed_manifest: dict, manifest: dict) -> list[str]`

- [ ] **Step 1: Write the e2e tests** (markers `e2e`, plus `smoke` or `standard`)

```python
def test_smoke_invariant_and_golden(stack_smoke):
    assert check_invariant(*stack_smoke.ctx) == []
    assert diff_expectations(*stack_smoke.exp_ctx) == []
    assert tree_snapshot(*stack_smoke.snap_ctx) == read_golden("smoke_tree.txt")

def test_pinned_cases(stack_smoke):
    db = stack_smoke.db
    assert kind_of(db, "IMG_E8719.HEIC") == "still" and mime_of(db, "IMG_E8719.HEIC") == "image/jpeg"
    assert kind_of(db, "IMG_0004.MOV") == "motion"
    assert same_copy_group(db, "IMG_0034.HEIC", "IMG_0034.JPG")
    assert not same_copy_group(db, "IMG_2834.HEIC", "IMG_2834 1.HEIC")
    assert states(db, ["PSWU5412.MP4", "PSWU5412 1.MP4"]) == {"INGESTED", "COPY"}
    assert beside(stack_smoke.staging, "IMG_2834 1.MOV", "IMG_2834 1.HEIC")

def test_no_burst_pair_shares_a_copy_group(stack_standard):
    for a, b in stack_standard.manifest["relations"]["burst"]:
        assert not same_copy_group(stack_standard.db, a, b), (a, b)

def test_every_live_pair_staged_together(stack_standard): ...
def test_junk_never_discovered(stack_smoke): ...           # --extras junk
def test_corrupt_extras_quarantined(stack_smoke): ...      # --extras corrupt → expected reasons
def test_reseed_under_new_paths_is_all_copy(stack_smoke): ...
def test_nested_unicode_layout(stack_smoke_nested): ...
```

- [ ] **Step 2: Run them and watch them fail**

Run: `make test-smoke`
Expected: FAIL (the golden file doesn't exist yet), with a readable per-file diff report for any other mismatch

- [ ] **Step 3: Implement the harness, generate the golden file and review it by hand**

- Isolation: `COMPOSE_PROJECT_NAME=pmtest_<run_id>`, `DATA_DIR=data-test/<run_id>`, and `docker-compose.test.yml`.
- `wait_idle`: two consecutive 5 s polls in which running workflows = 0 and the drop zone has no eligible files. Then call `stage()`.
- On failure: keep the stack and dump per-state counts and the last 200 log lines from each service.

Run `make update-golden`, then **read `smoke_tree.txt` and confirm every pinned case lands in the expected bucket before committing it.**

- [ ] **Step 4: Run the suites to verify they pass**

Run: `make test-smoke && make test-standard`
Expected: PASS. Smoke takes ≤ 5 min. `corpus-verify` passes before and after.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "test: corpus-driven e2e harness, golden invariant, smoke/standard suites"
```

---

### Task 13: Chaos proof

**Files:**
- Create: `tests/chaos/test_chaos.py`
- Modify: `Makefile` (`chaos`)
- Test: `tests/chaos/test_chaos.py`

**Interfaces:**
- Consumes: `Stack`, `check_invariant`, `tree_snapshot`, `diff_expectations` (Task 12)

- [ ] **Step 1: Write the chaos test** (marker `chaos`)

```python
def test_chaos_equals_calm(tmp_run_factory):
    calm = tmp_run_factory("standard", shuffle=1); calm.run_to_idle()
    storm = tmp_run_factory("standard", shuffle=1)
    storm.run_with_chaos(seed=42, actions=[
        ("kill", "worker"), ("kill", "worker-extra"), ("restart", "temporal"), ("restart", "postgres"),
        ("kill_all", None), ("sleep_mac_like_pause", "worker")],   # docker pause 120 s, then unpause
        every_s=(15, 45), until_progress=0.9)
    storm.run_to_idle()
    assert check_invariant(*storm.ctx) == []
    assert partitions(storm.db) == partitions(calm.db)         # similar sets exactly; copy groups excluding ambiguous
    assert tree_snapshot(*storm.snap_ctx) == tree_snapshot(*calm.snap_ctx)
    assert diff_expectations(*storm.exp_ctx) == []
```

- [ ] **Step 2: Run it**

Run: `make chaos`
Expected: PASS. Any failure is a real defect: stop, use superpowers:systematic-debugging, add a regression unit test in the owning module, fix it, and re-run.

- [ ] **Step 3: Confirm it passes three times in a row**

Run: `for i in 1 2 3; do make chaos || break; done`
Expected: 3 PASS

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test: chaos proof that crashes never change the outcome"
```
