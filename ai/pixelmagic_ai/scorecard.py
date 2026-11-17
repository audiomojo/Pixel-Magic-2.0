"""The scorecard backfill: `python -m pixelmagic_ai.scorecard backfill` fills the baseline
columns of older ranker_models rows (see migration 0012) so the learning curve has a rule and
chance line under every version. Each version is re-measured on the picks that existed when it
was created, with today's sets and features (sets merge and photos get described over time; the
picks themselves are exact). Only null columns are filled, so running it twice changes nothing,
and cv_agreement and picks_used are never touched."""

import logging
import os
import sys
from pathlib import Path

import psycopg

from . import db, ranker
from .config import load_settings

REPO_ROOT = Path(__file__).resolve().parents[2]

log = logging.getLogger(__name__)


def backfill(conn: psycopg.Connection) -> None:
    for row in db.model_history(conn):
        b = row.baselines
        if None not in (b.cv_n, b.chance_agreement, b.rule_agreement, b.rule_ties,
                        b.model_wins, b.rule_wins):
            continue
        picks = db.load_picks(conn, before=row.created_at)
        sets = db.load_sets(conn, row.feature_version)
        lam, readable = None, True  # the rule score for picks_used = 0
        if row.picks_used > 0:
            try:
                lam = ranker.load(row.path).lam
            except Exception as e:  # noqa: BLE001 — missing, truncated, not a model file
                log.warning("version %d: model file %r unreadable (%s); paired counts left null",
                            row.version, row.path, e)
                readable = False
        r = ranker.evaluate(sets, picks, lam)
        # The rule and chance need no model file; the paired counts do.
        filled = db.Baselines(r.cv_n, r.chance, r.rule_agreement, r.rule_ties,
                              r.model_wins if readable else None, r.rule_wins if readable else None)
        if readable and row.picks_used > 0:
            if r.cv_n != row.picks_used:
                log.warning("version %d: %d picks now, picks_used was %d",
                            row.version, r.cv_n, row.picks_used)
            # A stored strict figure can sit below the tie-credit one by up to ties/n.
            slack = (r.rule_ties or 0) / r.cv_n if r.cv_n else 0.0
            if row.cv_agreement is None or abs(r.cv - row.cv_agreement) > slack + 1e-6:
                log.warning("version %d: recomputed cv %.3f, stored cv %s (tie slack %.3f)",
                            row.version, r.cv, row.cv_agreement, slack)
        db.fill_baselines(conn, row.version, filled)
        log.info("version %d: n %d, chance %s, rule %s (ties %s), model wins %s, rule wins %s",
                 row.version, filled.cv_n, filled.chance_agreement, filled.rule_agreement,
                 filled.rule_ties, filled.model_wins, filled.rule_wins)


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    if argv != ["backfill"]:
        print("usage: python -m pixelmagic_ai.scorecard backfill", file=sys.stderr)
        return 2
    settings = load_settings(os.environ, REPO_ROOT)
    with psycopg.connect(settings.database_url) as conn:
        backfill(conn)
        conn.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
