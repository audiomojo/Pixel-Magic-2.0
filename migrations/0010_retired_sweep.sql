-- Up Migration

-- Where the copy sweep's pass over retired identical duplicates resumes (the last sha256 it
-- examined; '' = from the start). Those rows wait while a staging generation still links them.
ALTER TABLE copy_sweep_state ADD COLUMN retired_after text NOT NULL DEFAULT '';

-- Down Migration

ALTER TABLE copy_sweep_state DROP COLUMN retired_after;
