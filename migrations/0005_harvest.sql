-- Up Migration

-- Harvest: copies real photos from external drives into the drop zone (spec §5).

CREATE TABLE harvest_sources (
  id              bigserial PRIMARY KEY,
  identity        text NOT NULL UNIQUE,
  label           text NOT NULL UNIQUE,
  kind            text,
  last_mount_path text,
  first_seen_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_seen_at    timestamptz,
  CONSTRAINT harvest_sources_kind_check CHECK (kind IN ('network','local'))
);

CREATE TABLE harvest_folders (
  id                bigserial PRIMARY KEY,
  source_id         bigint NOT NULL REFERENCES harvest_sources(id),
  rel_path          text NOT NULL,          -- '' = the whole source
  last_harvested_at timestamptz,
  last_run_id       bigint,
  last_counts       jsonb,
  UNIQUE (source_id, rel_path)
);

CREATE TABLE harvest_runs (
  id           bigserial PRIMARY KEY,
  workflow_id  text UNIQUE,
  dry_run      boolean NOT NULL DEFAULT false,
  state        text NOT NULL,
  paused_reason text,
  paused_since timestamptz,
  created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  started_at   timestamptz,
  finished_at  timestamptz,
  error        text,
  CONSTRAINT harvest_runs_state_check
    CHECK (state IN ('queued','scanning','sending','paused','done','cancelled','failed'))
);

CREATE TABLE harvest_run_folders (
  run_id     bigint NOT NULL REFERENCES harvest_runs(id) ON DELETE CASCADE,
  folder_id  bigint NOT NULL REFERENCES harvest_folders(id) ON DELETE CASCADE,
  state      text NOT NULL DEFAULT 'queued',
  dirs       bigint NOT NULL DEFAULT 0,
  photos     bigint NOT NULL DEFAULT 0,
  maybes     bigint NOT NULL DEFAULT 0,
  junk       jsonb NOT NULL DEFAULT '{}',   -- reason -> count
  errors     bigint NOT NULL DEFAULT 0,
  sent       bigint NOT NULL DEFAULT 0,
  known      bigint NOT NULL DEFAULT 0,
  dups       bigint NOT NULL DEFAULT 0,
  bytes_sent bigint NOT NULL DEFAULT 0,
  scan_cursor text,
  PRIMARY KEY (run_id, folder_id),
  CONSTRAINT harvest_run_folders_state_check
    CHECK (state IN ('queued','scanning','sending','waiting_source','done'))
);

CREATE TABLE harvest_items (
  id             bigserial PRIMARY KEY,
  source_id      bigint NOT NULL REFERENCES harvest_sources(id),
  rel_path       text NOT NULL,             -- relative to the source root, NFC
  rel_path_raw   bytea,                     -- original bytes when not valid UTF-8
  path_key       text NOT NULL,             -- NFC + casefold
  size_bytes     bigint NOT NULL,
  mtime_ns       bigint NOT NULL,
  kind           text NOT NULL,
  state          text NOT NULL,
  sha256         char(64),
  head_hash      char(64),
  dropzone_path  text,
  file_id        bigint,
  duplicate_of   bigint REFERENCES harvest_items(id),
  attempts       int NOT NULL DEFAULT 0,
  last_error     text,
  first_seen_run bigint,
  last_seen_run  bigint,
  sent_at        timestamptz,
  UNIQUE (source_id, path_key, size_bytes, mtime_ns),
  CONSTRAINT harvest_items_kind_check CHECK (kind IN ('photo','maybe')),
  CONSTRAINT harvest_items_state_check
    CHECK (state IN ('PLANNED','SENT','KNOWN','DUP','REVIEW','REJECTED','FAILED','VANISHED'))
);
CREATE INDEX harvest_items_state_source_idx ON harvest_items (state, source_id);
CREATE INDEX harvest_items_size_idx ON harvest_items (size_bytes);
CREATE INDEX harvest_items_sha256_idx ON harvest_items (sha256);
CREATE INDEX harvest_items_dropzone_unlinked_idx ON harvest_items (dropzone_path) WHERE file_id IS NULL;

-- The host heartbeat the Grafana tile reads (one row).
CREATE TABLE harvest_host_state (
  id           int PRIMARY KEY,
  heartbeat_at timestamptz,
  version      text,
  CONSTRAINT harvest_host_state_singleton CHECK (id = 1)
);
INSERT INTO harvest_host_state (id) VALUES (1);

-- The one pipeline-table change: the known-content check looks contents up by size.
CREATE INDEX contents_size_idx ON contents (size_bytes);

-- Down Migration

DROP INDEX contents_size_idx;
DROP TABLE harvest_host_state, harvest_items, harvest_run_folders, harvest_runs,
  harvest_folders, harvest_sources;
