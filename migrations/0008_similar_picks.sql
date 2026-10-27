-- Up Migration

-- Similar picks: a local taste model marks the user's pick in each similar set.
-- The AI tables are written by a separate worker, so sha256 columns deliberately have no
-- foreign key to contents: they must never block pipeline deletes or ordering.

CREATE TABLE photo_features (
  sha256 text NOT NULL,
  feature_version int NOT NULL,
  embedding real[] NOT NULL,
  quality jsonb NOT NULL,
  faces jsonb NOT NULL,
  vlm jsonb,
  failed text,
  described_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (sha256, feature_version)
);

CREATE TABLE picks (
  id bigserial PRIMARY KEY,
  set_id bigint NOT NULL,
  chosen_sha256 text,
  shown_sha256 text[] NOT NULL,
  no_clear_best boolean NOT NULL,
  random_draw boolean NOT NULL DEFAULT false,
  model_version int,
  model_pick_sha256 text,
  reasons text[] NOT NULL DEFAULT '{}',
  decided_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT picks_no_clear_best_chk CHECK (no_clear_best = (chosen_sha256 IS NULL))
);
CREATE INDEX picks_decided_at_idx ON picks (decided_at);
CREATE INDEX picks_chosen_sha256_idx ON picks (chosen_sha256);

CREATE TABLE photo_scores (
  sha256 text NOT NULL,
  model_version int NOT NULL,
  score real NOT NULL,
  PRIMARY KEY (sha256, model_version)
);
CREATE INDEX photo_scores_model_version_idx ON photo_scores (model_version);

CREATE TABLE ranker_models (
  version serial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  feature_version int NOT NULL,
  picks_used int NOT NULL,
  cv_agreement real,
  promoted boolean NOT NULL DEFAULT false,
  path text NOT NULL,
  ranked_at timestamptz
);

-- Written by staging: the photos of each similar set, so the worker can find sets to rank.
CREATE TABLE similar_candidates (
  set_id bigint NOT NULL,
  sha256 text NOT NULL,
  PRIMARY KEY (set_id, sha256)
);
CREATE INDEX similar_candidates_sha256_idx ON similar_candidates (sha256);

-- Down Migration

DROP INDEX similar_candidates_sha256_idx;
DROP TABLE similar_candidates;
DROP TABLE ranker_models;
DROP INDEX photo_scores_model_version_idx;
DROP TABLE photo_scores;
DROP INDEX picks_chosen_sha256_idx;
DROP INDEX picks_decided_at_idx;
DROP TABLE picks;
DROP TABLE photo_features;
