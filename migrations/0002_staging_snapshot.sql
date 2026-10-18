-- Up Migration

-- The MVCC snapshot a staging build read its facts under. A files/contents row whose xmin is not
-- visible in it was committed after (or concurrently with) that build: the facts changed.
ALTER TABLE staging_builds ADD COLUMN snapshot pg_snapshot;

-- Down Migration

ALTER TABLE staging_builds DROP COLUMN snapshot;
