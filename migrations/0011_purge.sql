-- Up Migration

-- Purge a similar set: the chosen contents are deleted from the vault and their exact
-- sha256 is blocked from coming back. Blocking is by sha256 + size only.
CREATE TABLE purged (
  sha256 char(64) PRIMARY KEY,
  size_bytes bigint NOT NULL,
  names text[] NOT NULL DEFAULT '{}',
  purged_at timestamptz NOT NULL,
  unblocked_at timestamptz,
  set_id bigint
);

ALTER TABLE contents
  ADD COLUMN purged_at timestamptz,
  ADD COLUMN purge_removed_at timestamptz,
  ADD COLUMN purge_reclaimed_bytes bigint,
  ADD COLUMN purge_mismatch boolean NOT NULL DEFAULT false;

ALTER TABLE contents DROP CONSTRAINT contents_state_check;
ALTER TABLE contents ADD CONSTRAINT contents_state_check
  CHECK (state IN ('INGESTED','HASHED','CLUSTERED','QUARANTINED','FAILED','DUPLICATE','PURGED'));
ALTER TABLE contents ADD CONSTRAINT contents_purge_check
  CHECK ((state = 'PURGED') = (purged_at IS NOT NULL));

ALTER TABLE files DROP CONSTRAINT files_state_check;
ALTER TABLE files ADD CONSTRAINT files_state_check
  CHECK (state IN ('DISCOVERED','CLAIMED','INGESTED','COPY','QUARANTINED','BLOCKED'));

ALTER TABLE harvest_items DROP CONSTRAINT harvest_items_state_check;
ALTER TABLE harvest_items ADD CONSTRAINT harvest_items_state_check
  CHECK (state IN ('PLANNED','SENT','KNOWN','DUP','REVIEW','REJECTED','FAILED','VANISHED','BLOCKED'));

ALTER TABLE harvest_run_folders ADD COLUMN blocked bigint NOT NULL DEFAULT 0;

-- Where the copy sweep's pass over purged contents resumes ('' = from the start).
ALTER TABLE copy_sweep_state ADD COLUMN purged_after text NOT NULL DEFAULT '';

-- Down Migration

ALTER TABLE copy_sweep_state DROP COLUMN purged_after;
ALTER TABLE harvest_run_folders DROP COLUMN blocked;
ALTER TABLE harvest_items DROP CONSTRAINT harvest_items_state_check;
ALTER TABLE harvest_items ADD CONSTRAINT harvest_items_state_check
  CHECK (state IN ('PLANNED','SENT','KNOWN','DUP','REVIEW','REJECTED','FAILED','VANISHED'));
ALTER TABLE files DROP CONSTRAINT files_state_check;
ALTER TABLE files ADD CONSTRAINT files_state_check
  CHECK (state IN ('DISCOVERED','CLAIMED','INGESTED','COPY','QUARANTINED'));
ALTER TABLE contents DROP CONSTRAINT contents_purge_check;
ALTER TABLE contents DROP CONSTRAINT contents_state_check;
ALTER TABLE contents ADD CONSTRAINT contents_state_check
  CHECK (state IN ('INGESTED','HASHED','CLUSTERED','QUARANTINED','FAILED','DUPLICATE'));
ALTER TABLE contents
  DROP COLUMN purge_mismatch,
  DROP COLUMN purge_reclaimed_bytes,
  DROP COLUMN purge_removed_at,
  DROP COLUMN purged_at;
DROP TABLE purged;
