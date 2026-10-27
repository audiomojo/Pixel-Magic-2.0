-- Up Migration

-- Identical-picture dedupe: a pixel-identical still is retired to state DUPLICATE and
-- points at its survivor via duplicate_of. The vault file is reclaimed later.
ALTER TABLE contents
  ADD COLUMN duplicate_of char(64),
  ADD COLUMN duplicate_rule text,
  ADD COLUMN duplicate_at timestamptz,
  ADD COLUMN duplicate_removed_at timestamptz,
  ADD COLUMN duplicate_reclaimed_bytes bigint,
  ADD COLUMN duplicate_mismatch boolean NOT NULL DEFAULT false;

ALTER TABLE contents DROP CONSTRAINT contents_state_check;
ALTER TABLE contents ADD CONSTRAINT contents_state_check
  CHECK (state IN ('INGESTED','HASHED','CLUSTERED','QUARANTINED','FAILED','DUPLICATE'));
ALTER TABLE contents ADD CONSTRAINT contents_duplicate_check
  CHECK ((state = 'DUPLICATE') = (duplicate_of IS NOT NULL));

-- Finds suspect pairs: stills sharing perceptual hashes and dimensions.
CREATE INDEX contents_suspect_idx ON contents (phash, dhash, width, height) WHERE kind = 'still';

-- Cache of pixel comparisons so a pair is decoded at most once.
CREATE TABLE pixel_compares (
  sha_a char(64) NOT NULL,
  sha_b char(64) NOT NULL,
  verdict text NOT NULL,
  detail text,
  compared_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (sha_a, sha_b),
  CONSTRAINT pixel_compares_order_check CHECK (sha_a < sha_b),
  CONSTRAINT pixel_compares_verdict_check CHECK (verdict IN ('identical','different','error'))
);

-- Down Migration

DROP TABLE pixel_compares;
DROP INDEX contents_suspect_idx;
ALTER TABLE contents DROP CONSTRAINT contents_duplicate_check;
ALTER TABLE contents DROP CONSTRAINT contents_state_check;
ALTER TABLE contents ADD CONSTRAINT contents_state_check
  CHECK (state IN ('INGESTED','HASHED','CLUSTERED','QUARANTINED','FAILED'));
ALTER TABLE contents
  DROP COLUMN duplicate_mismatch,
  DROP COLUMN duplicate_reclaimed_bytes,
  DROP COLUMN duplicate_removed_at,
  DROP COLUMN duplicate_at,
  DROP COLUMN duplicate_rule,
  DROP COLUMN duplicate_of;
