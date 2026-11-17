-- Up Migration

-- The scorecard's baselines, recorded with every model on the same held-out picks as its
-- cv_agreement. All nullable: older versions get them from the one-off backfill
-- (`python -m pixelmagic_ai.scorecard backfill`), and v1 (trained on no picks) stays null.
ALTER TABLE ranker_models
  ADD COLUMN cv_n int,               -- held-out picks behind the figures
  ADD COLUMN chance_agreement real,  -- mean of 1/candidates over those picks
  ADD COLUMN rule_agreement real,    -- the rule score's tie-credit agreement on them
  ADD COLUMN rule_ties int,          -- picks the rule score tied on
  ADD COLUMN model_wins int,         -- picks where the model's held-out credit beat the rule's
  ADD COLUMN rule_wins int;          -- picks where the rule's credit beat the model's

-- Down Migration

ALTER TABLE ranker_models
  DROP COLUMN rule_wins,
  DROP COLUMN model_wins,
  DROP COLUMN rule_ties,
  DROP COLUMN rule_agreement,
  DROP COLUMN chance_agreement,
  DROP COLUMN cv_n;
