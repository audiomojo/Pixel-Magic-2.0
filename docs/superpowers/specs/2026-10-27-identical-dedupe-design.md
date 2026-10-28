# Identical-picture dedupe — design

Date: 2026-10-27. Status: approved in conversation (sections 1–3); this document is the written spec.

## 1. Problem

Exact dedupe works on the sha256 of a file's bytes. Two files holding the same picture with different
metadata (an app re-saved the EXIF, e.g. `DSCF2223.JPG` re-saved by Picasa 3.0 and the camera's own
`DSCF2223.JPG`, staged as `DSCF2223_7f9848a7.JPG`) have different sha256s, so both enter the vault.
The classifier then refuses to call them copies (same second, same encoding looks like a burst,
`src/classify.ts:70`), so they land in a similar set as two copy groups whose AI scores tie exactly.
On the live library this is 791 of 6,625 similar sets.

## 2. Goal

The vault never holds two files whose decoded pictures are pixel-for-pixel identical. Of each such
pair only the better file stays; the other is deleted from the vault, its record kept. This applies to
new drops and, once, to the existing vault.

Non-goals: merging metadata between files; treating resized or re-compressed versions as duplicates
(they stay copy groups, unchanged); RAW files (only their embedded previews can be decoded, so two
RAWs cannot be proven identical); motion files (videos).

## 3. Definitions

- **Identical:** two non-RAW stills whose full-resolution decodes, with EXIF orientation applied
  (the same decode path as hashing: sharp `.rotate()`, heic-decode for HEVC, the BMP parser), have
  equal width, height, channel count and every pixel byte equal. Formats may differ (a PNG exported
  from a JPEG is identical to it).
- **Suspect:** an existing content in state `HASHED` or `CLUSTERED`, kind `still`, not RAW, with
  equal `phash`, `dhash`, `width` and `height`. Identical pictures always have equal values for all
  four, so only suspects are decoded and compared.
- **Keeper rule** (first rule that separates the two decides):
  1. **Camera original** — a file whose `EXIF:Software` or `XMP:CreatorTool` names an editor loses to
     one that does not. Editor list (case-insensitive substring): `picasa`, `photoshop`, `lightroom`,
     `gimp`, `iphoto`, `aperture`, `photos` (Apple Photos, "Photos 9.0"), `windows photo`,
     `photo gallery`, `acdsee`, `snapseed`, `google`. A phone OS version or camera firmware is not an
     editor.
  2. **More metadata** — count of present fields among `DateTimeOriginal`, `Make`, `Model`,
     `GPSLatitude`; more wins.
  3. **Larger file** (`size_bytes`).
  4. **Earlier ingested** (`ingested_at`, then sha256 for determinism).

  The tags are read with exiftool from the two vault files at comparison time; nothing new is stored
  per photo. The deciding rule is recorded (`camera-original`, `more-metadata`, `larger`, `earlier`).

## 4. Flow for a new photo

A new activity `dedupeIdentical(sha256)` runs in `runContentSteps` after `recordHash` and before
`clusterContent`, on the cpu queue (it decodes), only for a content now `HASHED`.

1. Find suspects. None → return `unique` (the common case; one indexed query).
2. Under a transaction-scoped advisory lock keyed on `(phash, dhash, width, height)` (so two identical
   newcomers in flight serialize), for each suspect not already compared (see `pixel_compares`):
   decode both and compare; record the verdict in `pixel_compares`.
3. All identical contents form one family with the newcomer; the keeper rule picks one keeper; every
   other member is retired (§5). If the newcomer is retired it is never clustered and the workflow
   ends. If a `CLUSTERED` content is retired, `cluster_state.version` is bumped in the same
   transaction so every worker's cluster cache reloads without it; the newcomer is then clustered
   normally (pixel-identical, so it joins the groups the retired one was in).
4. Any decode failure, missing vault file or checksum mismatch → no deletion; the pair is recorded as
   `error` in `pixel_compares` with the message, and the newcomer continues as today.

## 5. Retiring a content

In one transaction:
- `contents.state = 'DUPLICATE'`, `duplicate_of = <keeper sha>`, `duplicate_rule = <rule>`,
  `duplicate_at = now()`.
- If the retired content has `live_content_id` and the keeper has none, the keeper takes it (the
  Live Photo video stays paired).
- Retired members already pointing at another retired content are re-pointed to the final keeper
  (`duplicate_of` is always a non-duplicate).

After commit: re-hash the vault file and the keeper's vault file; only if they match
`contents.sha256` and `duplicate_of` unlink the loser and set `duplicate_removed_at`,
`duplicate_reclaimed_bytes`. A mismatch (or a missing keeper) sets `duplicate_mismatch = true` and
deletes nothing. A missing file sets `duplicate_removed_at` with 0 bytes (already gone). While the
vault file has another link (a staging generation still holds it) the removal is DEFERRED: nothing
unlinked or recorded, since staging's reaper would otherwise rescue the last link as a user's file
(ruling R15). The copy sweep (`pm-copies`) and `dedupe-identical --apply` retry pending rows; once
staging builds have dropped the generations that linked it, the unlink frees the space.
Each removal logs both names, the keeper and the rule.

## 6. Everything else that sees contents

- **Staging** places nothing for a `DUPLICATE` content (it already reads only `CLUSTERED` stills;
  quarantine/other listings must also skip `DUPLICATE`). Its `files` rows are not staged.
- **Picks:** `loadPickInputs` maps a pick of a retired sha to its keeper; the Picks API resolves the
  same way when checking "still a candidate".
- **AI:** retired contents leave `similar_candidates` at the next build; their scores are pruned at
  the next rank (existing R44 behaviour).
- **Re-drop of a retired file:** in `ingestFile`, after hashing, if `contents.sha256` exists with state
  `DUPLICATE`, the file becomes `COPY` of that content and the claimed file is deleted (its hash
  equals the recorded sha, which is the check) — it is never linked back into the vault.
- **Harvester:** `known.ts` matches by sha256 and size against `contents`, so a retired file stays
  KNOWN and is not re-sent. No change.
- **Copy sweep / reclaimCopy:** a `.copies` file whose content was retired is deleted as today if its
  bytes match; it must not be compared against the (deleted) vault file — compare against the
  recorded sha256 instead.

## 7. Data model — migration `0009_identical_dedupe.sql`

- `contents`: add `duplicate_of char(64)`, `duplicate_rule text`, `duplicate_at timestamptz`,
  `duplicate_removed_at timestamptz`, `duplicate_reclaimed_bytes bigint`,
  `duplicate_mismatch boolean NOT NULL DEFAULT false`; replace `contents_state_check` to allow
  `DUPLICATE`; CHECK `(state = 'DUPLICATE') = (duplicate_of IS NOT NULL)`.
- Index `contents_suspect_idx ON contents (phash, dhash, width, height) WHERE kind = 'still'`.
- `pixel_compares (sha_a char(64), sha_b char(64), verdict text CHECK (verdict IN
  ('identical','different','error')), detail text, compared_at timestamptz DEFAULT now(),
  PRIMARY KEY (sha_a, sha_b), CHECK (sha_a < sha_b))` — `different` verdicts are never re-decoded;
  `error` verdicts are retried on the next run.

## 8. The one-time cleanup

CLI `node dist/cli.js dedupe-identical [--apply]`, Make target `make dedupe-identical [APPLY=1]`,
run in the worker container.

- Finds every suspect family among `HASHED`/`CLUSTERED` non-RAW stills (group by the four columns,
  size > 1), compares pairs (reusing `pixel_compares`), applies the keeper rule.
- **Dry run (default):** deletes nothing; writes `DATA_DIR/reports/dedupe-identical-<timestamp>.csv`
  (keeper path and original names, loser path and original names, rule, bytes) and prints totals:
  families, files to remove, bytes to free, errors.
- **`--apply`:** retires and deletes as in §5, family by family, each family its own transaction plus
  post-commit unlink. Re-runnable; an interrupted run leaves every family either untouched or fully
  retired (the unlink step is resumed by re-running: retired contents with no `duplicate_removed_at`
  are unlinked first).
- Afterwards `make stage FORCE=1` refreshes staging; the AI re-ranks on the set-membership change.

The apply step runs only after the user has reviewed the dry-run report.

## 9. Testing

- **Unit:** keeper rule (each rule deciding, full tie → earlier/sha); editor list (Picasa, Photos 9.0,
  iOS "17.1", camera firmware strings); suspect key; pixel compare (equal, one pixel off, different
  channel count, orientation applied).
- **Integration (testcontainers Postgres, temp vault):** newcomer loses; newcomer wins over a
  CLUSTERED content (cache version bumped, newcomer clustered into the same groups); three identical
  files; RAW skipped; one-pixel-different kept as similar; re-drop of a retired file becomes COPY and
  is not re-vaulted; Live link moved to keeper; pick of the loser counts for the keeper; decode
  failure / checksum mismatch deletes nothing; dry run deletes nothing; apply is idempotent and
  resumes an interrupted unlink.
- **End-to-end:** the test corpus gains a metadata-only duplicate pair (camera original + an
  editor-re-saved copy); the golden staging tree is updated so only the camera original is staged.
  The chaos tier includes a worker kill during `dedupeIdentical`.
- Existing tiers stay green.
