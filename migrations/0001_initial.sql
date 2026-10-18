-- Up Migration

CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE files (
  id              bigserial PRIMARY KEY,
  rel_path        text NOT NULL,
  original_name   text NOT NULL,
  size_bytes      bigint NOT NULL,
  mtime_ns        bigint NOT NULL,
  ext             text,
  sha256          char(64),
  state           text NOT NULL,
  workflow_run_id text UNIQUE,
  attempts        int NOT NULL DEFAULT 0,
  last_error      text,
  discovered_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT files_state_check CHECK (state IN ('DISCOVERED','CLAIMED','INGESTED','COPY','QUARANTINED'))
);
CREATE UNIQUE INDEX files_active_uniq ON files (rel_path, size_bytes, mtime_ns)
  WHERE state IN ('DISCOVERED','CLAIMED');
CREATE INDEX files_state_idx ON files (state);
CREATE TRIGGER files_updated_at BEFORE UPDATE ON files
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE contents (
  sha256            char(64) PRIMARY KEY,
  ext               text NOT NULL,
  kind              text NOT NULL,
  mime              text,
  size_bytes        bigint NOT NULL,
  width             int,
  height            int,
  phash             bigint,
  dhash             bigint,
  captured_at       timestamptz,
  subsec            text,
  camera            text,
  burst_uuid        text,
  live_content_id   text,
  duration_s        real,
  state             text NOT NULL,
  quarantine_reason text,
  copy_group_id     bigint,
  copy_ambiguous    boolean NOT NULL DEFAULT false,
  last_error        text,
  ingested_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
  hashed_at         timestamptz,
  clustered_at      timestamptz,
  updated_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT contents_kind_check CHECK (kind IN ('still','motion','other')),
  CONSTRAINT contents_state_check CHECK (state IN ('INGESTED','HASHED','CLUSTERED','QUARANTINED','FAILED'))
);
CREATE INDEX contents_state_idx ON contents (state);
CREATE INDEX contents_live_content_id_idx ON contents (live_content_id);
CREATE INDEX contents_copy_group_id_idx ON contents (copy_group_id);
CREATE TRIGGER contents_updated_at BEFORE UPDATE ON contents
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE groups (
  id             bigserial PRIMARY KEY,
  kind           text NOT NULL,
  merged_into    bigint,
  similar_set_id bigint,
  CONSTRAINT groups_kind_check CHECK (kind IN ('copy','similar'))
);

CREATE TABLE cluster_state (
  id      int PRIMARY KEY,
  version bigint NOT NULL DEFAULT 0,
  CONSTRAINT cluster_state_singleton CHECK (id = 1)
);
INSERT INTO cluster_state (id) VALUES (1);

CREATE TABLE scan_stats (
  id            int PRIMARY KEY,
  pending_files bigint NOT NULL DEFAULT 0,
  pending_bytes bigint NOT NULL DEFAULT 0,
  scanned_at    timestamptz,
  CONSTRAINT scan_stats_singleton CHECK (id = 1)
);
INSERT INTO scan_stats (id) VALUES (1);

CREATE TABLE staging_builds (
  gen             bigint PRIMARY KEY,
  watermark       timestamptz NOT NULL,
  cluster_version bigint NOT NULL DEFAULT 0,
  built_at        timestamptz NOT NULL,
  entries         bigint NOT NULL
);

-- Down Migration

DROP TABLE staging_builds, scan_stats, cluster_state, groups, contents, files;
DROP FUNCTION set_updated_at();
