-- Up Migration

-- A similar-set merge re-points every copy group of the losing set
-- (`UPDATE groups … WHERE similar_set_id = $1`); without an index that scans all of `groups`.
CREATE INDEX groups_similar_set_id_idx ON groups (similar_set_id);

-- Down Migration

DROP INDEX groups_similar_set_id_idx;
