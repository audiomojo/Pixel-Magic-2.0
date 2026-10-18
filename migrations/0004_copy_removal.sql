-- Up Migration

-- A COPY file's `vault/.copies/<id>.<ext>` is removed once it is confirmed byte-identical to its
-- vault file by a fresh re-hash of both (spec §6.5). The row records when and how many bytes;
-- a failed confirmation sets `copy_mismatch` (observed hashes in `last_error`) and deletes nothing.
ALTER TABLE files
  ADD COLUMN copy_removed_at      timestamptz,
  ADD COLUMN copy_reclaimed_bytes bigint,
  ADD COLUMN copy_mismatch        boolean NOT NULL DEFAULT false,
  ADD CONSTRAINT files_copy_removed_check CHECK (copy_removed_at IS NULL OR state = 'COPY'),
  ADD CONSTRAINT files_copy_flag_check CHECK (NOT (copy_mismatch AND copy_removed_at IS NOT NULL));

-- The sweep's work list: COPY rows neither removed nor flagged.
CREATE INDEX files_copy_pending_idx ON files (id)
  WHERE state = 'COPY' AND copy_removed_at IS NULL AND NOT copy_mismatch;

-- Where the bounded sweep resumes (the last file id it examined; 0 = from the start).
CREATE TABLE copy_sweep_state (
  id       int PRIMARY KEY,
  after_id bigint NOT NULL DEFAULT 0,
  CONSTRAINT copy_sweep_state_singleton CHECK (id = 1)
);
INSERT INTO copy_sweep_state (id) VALUES (1);

-- Down Migration

DROP TABLE copy_sweep_state;
DROP INDEX files_copy_pending_idx;
ALTER TABLE files
  DROP CONSTRAINT files_copy_flag_check,
  DROP CONSTRAINT files_copy_removed_check,
  DROP COLUMN copy_mismatch,
  DROP COLUMN copy_reclaimed_bytes,
  DROP COLUMN copy_removed_at;
