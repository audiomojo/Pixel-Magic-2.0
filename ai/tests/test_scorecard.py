import logging

import numpy as np
import pytest
from test_train import SIZE, add_sets, add_smile_picks

from pixelmagic_ai import db, ranker, scorecard


def rows(conn) -> list[tuple]:
    return conn.execute(
        "SELECT version, picks_used, cv_agreement, cv_n, chance_agreement, rule_agreement,"
        " rule_ties, model_wins, rule_wins FROM ranker_models ORDER BY version"
    ).fetchall()


def add_model(conn, created: str, picks_used: int, cv: float, path: str) -> int:
    (version,) = conn.execute(
        "INSERT INTO ranker_models (created_at, feature_version, picks_used, cv_agreement,"
        " promoted, path) VALUES (%s, 1, %s, %s, true, %s) RETURNING version",
        (created, picks_used, cv, path),
    ).fetchone()
    return version


@pytest.fixture
def history(conn, tmp_path):
    """Picks on two days; four model versions created before, between and after them."""
    sets = add_sets(conn)
    add_smile_picks(conn, sets, 30)
    conn.execute("UPDATE picks SET decided_at = '2026-01-01' WHERE id <= 10")
    conn.execute("UPDATE picks SET decided_at = '2026-02-01' WHERE id > 10")
    pca = ranker.fit_pca(db.load_sets(conn, 1))
    rule = tmp_path / "rule.npz"
    ranker.save(ranker.RULE_ONLY(pca), rule)
    fitted = tmp_path / "fitted.npz"
    ranker.save(ranker.fit(db.load_sets(conn, 1), db.load_picks(conn), pca, 1.0), fitted)
    add_model(conn, "2025-12-01", 0, 0.0, str(rule))  # v1: no picks yet
    add_model(conn, "2026-01-15", 0, 0.4, str(rule))  # v2: 10 picks, below MIN_TRAIN_PICKS
    add_model(conn, "2026-03-01", 30, 0.7, str(fitted))  # v3: fitted on all 30
    add_model(conn, "2026-03-02", 30, 0.7, str(tmp_path / "gone.npz"))  # v4: file lost
    conn.execute("UPDATE ranker_models SET rule_ties = 99 WHERE version = 2")  # already set
    return sets


def test_backfill_fills_each_version_from_the_picks_before_it(history, conn, caplog):
    with caplog.at_level(logging.INFO):
        scorecard.backfill(conn)
    v1, v2, v3, v4 = rows(conn)
    assert v1 == (1, 0, 0.0, 0, None, None, None, 0, 0)
    assert v2[:4] == (2, 0, pytest.approx(0.4), 10)
    assert v2[4] == pytest.approx(1 / SIZE) and 0 <= v2[5] <= 1
    assert v2[6] == 99  # only nulls are filled
    assert v2[7:] == (0, 0)  # the rule is the model: no paired counts
    assert v3[:4] == (3, 30, pytest.approx(0.7), 30)  # cv_agreement and picks_used untouched
    assert v3[4] == pytest.approx(1 / SIZE) and 0 <= v3[5] <= 1 and 0 <= v3[6] <= 30
    assert v3[7] + v3[8] > 0 and v3[7] + v3[8] <= 30
    assert v4[3:7] == v3[3:7]  # the rule and chance need no model file
    assert v4[7:] == (None, None)  # the paired counts do
    assert "version 4" in caplog.text and "unreadable" in caplog.text
    # v3's stored 0.7 is made up, so the recomputed CV is reported as a mismatch.
    assert "version 3" in caplog.text and "cv" in caplog.text


def test_backfill_twice_changes_nothing(history, conn):
    scorecard.backfill(conn)
    before = rows(conn)
    scorecard.backfill(conn)
    assert rows(conn) == before


def test_load_picks_before_a_time(history, conn):
    assert len(db.load_picks(conn)) == 30
    assert len(db.load_picks(conn, before="2026-01-15")) == 10
    assert db.load_picks(conn, before="2025-01-01") == []


def test_evaluate_at_a_lam_matches_train_on_the_same_picks(history, conn):
    sets, picks = db.load_sets(conn, 1), db.load_picks(conn)
    r = ranker.train(sets, picks, None)
    e = ranker.evaluate(sets, picks, r.model.lam)
    assert e.cv == pytest.approx(r.cv) and e.cv_n == r.cv_n == 30
    assert (e.model_wins, e.rule_wins) == (r.model_wins, r.rule_wins)
    assert np.allclose(e.rule_credits, r.rule_credits)
    rule = ranker.evaluate(sets, picks, None)
    assert rule.cv == pytest.approx(rule.rule_agreement) and (rule.model_wins, rule.rule_wins) == (0, 0)
