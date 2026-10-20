-- Up Migration

-- When the send phase decided an item (SENT, KNOWN or DUP): the dashboard's harvest rate and ETA.
-- Only sent_at existed, so a stretch of photos already in the vault showed no activity at all.
ALTER TABLE harvest_items ADD COLUMN decided_at timestamptz;
CREATE INDEX harvest_items_decided_at_idx ON harvest_items (decided_at)
  WHERE decided_at IS NOT NULL;

-- Down Migration

DROP INDEX harvest_items_decided_at_idx;
ALTER TABLE harvest_items DROP COLUMN decided_at;
