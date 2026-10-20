-- Up Migration

-- Harvest fixes after the final review (spec §5, §15). 0005 is already applied: changes go here.

-- F-1: the items a send-only run (review approvals) sends; it sends only these.
CREATE TABLE harvest_run_items (
  run_id  bigint NOT NULL REFERENCES harvest_runs(id) ON DELETE CASCADE,
  item_id bigint NOT NULL REFERENCES harvest_items(id) ON DELETE CASCADE,
  PRIMARY KEY (run_id, item_id)
);

-- F-2: false when the scan could not read the folder's root (not found, unreadable): no history.
ALTER TABLE harvest_run_folders ADD COLUMN root_ok boolean NOT NULL DEFAULT true;

-- I5: promoteDuplicates looks DUP rows up by their target.
CREATE INDEX harvest_items_duplicate_of_idx ON harvest_items (duplicate_of)
  WHERE duplicate_of IS NOT NULL;
-- I5: sendBatch pages through one run folder's PLANNED items in send order, a keyset on
-- (rel_path COLLATE "C", id) over the folder's rel_path range; items of the folder under another
-- case spelling are found by their path_key range.
CREATE INDEX harvest_items_planned_rel_idx ON harvest_items (source_id, (rel_path COLLATE "C"), id)
  WHERE state = 'PLANNED';
CREATE INDEX harvest_items_planned_key_idx ON harvest_items (source_id, (path_key COLLATE "C"), id)
  WHERE state = 'PLANNED';

-- Down Migration

DROP INDEX harvest_items_planned_key_idx;
DROP INDEX harvest_items_planned_rel_idx;
DROP INDEX harvest_items_duplicate_of_idx;
ALTER TABLE harvest_run_folders DROP COLUMN root_ok;
DROP TABLE harvest_run_items;
