import asyncio
import logging
import threading
import time
from pathlib import Path

import numpy as np
import psycopg
import pytest
from temporalio import activity
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from pixelmagic_ai import activities as acts_mod
from pixelmagic_ai import db, ranker
from pixelmagic_ai.activities import Activities, Deps
from pixelmagic_ai.config import Settings
from pixelmagic_ai.embed import FakeEmbedder
from pixelmagic_ai.faces import FakeDetector
from pixelmagic_ai.workflows import TRAIN_WORKFLOW_ID, TrainWorkflow

N_SETS, SIZE, DIM = 40, 4, 8


def sha(set_id: int, j: int) -> str:
    return f"{set_id:032x}{j:032x}"


def add_sets(conn, seed: int = 3, feature_version: int = 1) -> dict[int, list[str]]:
    """N_SETS described sets. Each candidate's smile is distinct; the rule score ignores it."""
    rng = np.random.default_rng(seed)
    sets: dict[int, list[str]] = {}
    for s in range(1, N_SETS + 1):
        sets[s] = []
        for j in range(SIZE):
            h = sha(s, j)
            conn.execute(
                "INSERT INTO contents (sha256, ext, kind, size_bytes, state)"
                " VALUES (%s, 'jpg', 'still', 1, 'CLUSTERED') ON CONFLICT DO NOTHING",
                (h,),
            )
            conn.execute(
                "INSERT INTO similar_candidates (set_id, sha256) VALUES (%s, %s)"
                " ON CONFLICT DO NOTHING",
                (s, h),
            )
            quality = {
                "sharpness": float(rng.uniform(50, 500)),
                "brightness": float(rng.uniform(0.3, 0.7)),
                "highlights": float(rng.uniform(0, 0.05)),
                "shadows": float(rng.uniform(0, 0.05)),
                "noise": float(rng.uniform(0, 0.1)),
                "aesthetic": float(rng.uniform(3, 7)),
            }
            faces = [
                {"eyes_open": float(rng.uniform()), "smile": float(0.1 + 0.2 * j + rng.uniform(0, 0.05)),
                 "quality": float(rng.uniform(0.4, 0.6)), "size": float(rng.uniform(0.05, 0.3))}
            ]
            emb = [float(x) for x in rng.normal(size=DIM)]
            db.upsert_features(conn, db.FeatureRow(h, feature_version, emb, quality, faces, None))
            sets[s].append(h)
    return sets


def add_smile_picks(conn, sets: dict[int, list[str]], n: int) -> None:
    """The user always picks the broadest smile (candidate SIZE-1), shown in shuffled order."""
    rng = np.random.default_rng(11)
    for s in list(sets)[:n]:
        shown = list(rng.permutation(sets[s]))
        conn.execute(
            "INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best)"
            " VALUES (%s, %s, %s, false)",
            (s, sets[s][SIZE - 1], shown),
        )


def add_unsure_pick(conn) -> None:
    conn.execute(
        "INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best)"
        " VALUES (1, NULL, %s, true)",
        ([sha(1, 0), sha(1, 1)],),
    )


def models(conn) -> list[tuple]:
    return conn.execute(
        "SELECT version, picks_used, promoted, path, ranked_at FROM ranker_models ORDER BY version"
    ).fetchall()


def scores(conn, version: int) -> dict[str, float]:
    rows = conn.execute(
        "SELECT sha256, score FROM photo_scores WHERE model_version = %s", (version,)
    ).fetchall()
    return dict(rows)


@pytest.fixture
def acts(tmp_path: Path, database_url: str, conn) -> Activities:
    settings = Settings(tmp_path, database_url, "unused:7233", "default", 2)
    return Activities(Deps(settings, FakeDetector([]), FakeEmbedder()))


def test_first_run_without_picks_promotes_and_ranks_rule_only(acts, conn, tmp_path):
    add_sets(conn)
    assert acts.train_ranker() == 1
    ((version, picks_used, promoted, path, ranked_at),) = models(conn)
    assert (version, picks_used, promoted) == (1, 0, True)
    assert path == str(tmp_path / "ai" / "models" / "1.npz")
    model = ranker.load(path)
    assert np.array_equal(model.weights, ranker.RULE_ONLY(model.pca).weights)
    assert ranked_at is None
    assert not list((tmp_path / "ai" / "models").glob("*.tmp*"))  # atomic write left no temp

    assert acts.rank_sets() == N_SETS * SIZE
    assert len(scores(conn, 1)) == N_SETS * SIZE
    assert models(conn)[0][4] is not None


def test_consistent_picks_promote_version_2_and_rank_it(acts, conn):
    sets = add_sets(conn)
    acts.train_ranker()
    acts.rank_sets()
    add_smile_picks(conn, sets, 30)
    assert acts.train_ranker() == 2
    v2 = models(conn)[1]
    assert v2[:3] == (2, 30, True)
    assert acts.rank_sets() == N_SETS * SIZE
    s2 = scores(conn, 2)
    assert len(s2) == N_SETS * SIZE
    assert models(conn)[1][4] is not None
    # The learnt taste: the broadest smile scores highest in the unpicked sets too.
    for s in range(31, N_SETS + 1):
        assert max(sets[s], key=s2.__getitem__) == sets[s][SIZE - 1]


def test_train_skips_without_new_picks_and_runs_after_one(acts, conn, tmp_path):
    sets = add_sets(conn)
    acts.train_ranker()
    add_smile_picks(conn, sets, 30)
    assert acts.train_ranker() == 2
    files = sorted((tmp_path / "ai" / "models").iterdir())
    assert acts.train_ranker() is None
    assert len(models(conn)) == 2
    assert sorted((tmp_path / "ai" / "models").iterdir()) == files
    add_unsure_pick(conn)  # a no_clear_best pick counts as a change
    assert acts.train_ranker() == 3
    assert len(models(conn)) == 3


def test_ranking_keeps_only_the_ranked_versions_scores(acts, conn):
    sets = add_sets(conn)
    acts.train_ranker()
    acts.rank_sets()
    add_smile_picks(conn, sets, 30)
    acts.train_ranker()
    acts.rank_sets()
    versions = conn.execute("SELECT DISTINCT model_version FROM photo_scores").fetchall()
    assert versions == [(2,)]
    assert len(models(conn)) == 2 and Path(models(conn)[0][3]).is_file()


def test_rerank_of_the_same_version_moves_ranked_at(acts, conn):
    add_sets(conn)
    acts.train_ranker()
    acts.rank_sets()
    first = models(conn)[0][4]
    time.sleep(0.01)
    acts.rank_sets()
    assert models(conn)[0][4] > first


def test_worse_candidate_is_kept_unpromoted_and_scores_not_rewritten(acts, conn, monkeypatch):
    sets = add_sets(conn)
    acts.train_ranker()
    add_smile_picks(conn, sets, 30)
    acts.train_ranker()
    acts.rank_sets()
    before_scores = scores(conn, 2)
    before_ranked = models(conn)[1][4]

    add_unsure_pick(conn)  # something new, so training runs
    real = ranker.train
    monkeypatch.setattr(ranker, "train", lambda *a: (real(*a)[0], 0.1, False))
    assert acts.train_ranker() is None
    v3 = models(conn)[2]
    assert v3[0] == 3 and v3[2] is False and v3[4] is None
    assert Path(v3[3]).is_file()  # unpromoted models are kept
    assert db.current_model(conn).version == 2
    assert scores(conn, 3) == {}
    assert scores(conn, 2) == before_scores
    assert models(conn)[1][4] == before_ranked


def test_missing_model_file_falls_back_to_previous_file_under_current_version(acts, conn, caplog):
    sets = add_sets(conn)
    acts.train_ranker()
    add_smile_picks(conn, sets, 30)
    acts.train_ranker()
    Path(models(conn)[1][3]).unlink()
    with caplog.at_level(logging.WARNING):
        assert acts.rank_sets() == N_SETS * SIZE
    s2 = scores(conn, 2)
    assert len(s2) == N_SETS * SIZE
    assert scores(conn, 1) == {}
    assert models(conn)[0][4] is None and models(conn)[1][4] is not None
    assert "version 1 file" in caplog.text
    with psycopg.connect(acts.deps.settings.database_url) as c:
        v1 = ranker.score(ranker.load(models(conn)[0][3]), db.load_sets(c, 1))
    assert s2 == pytest.approx(v1, rel=1e-5)  # the version 1 model produced them


def test_no_readable_model_file_ranks_rule_only_under_current_version(acts, conn):
    sets = add_sets(conn)
    acts.train_ranker()
    add_smile_picks(conn, sets, 30)
    acts.train_ranker()
    Path(models(conn)[0][3]).unlink()
    Path(models(conn)[1][3]).write_bytes(b"garbage")
    assert acts.rank_sets() == N_SETS * SIZE
    s2 = scores(conn, 2)
    assert len(s2) == N_SETS * SIZE
    with psycopg.connect(acts.deps.settings.database_url) as c:
        rule = ranker.score(ranker.RULE_ONLY(ranker.fit_pca(db.load_sets(c, 1))), db.load_sets(c, 1))
    assert s2 == pytest.approx(rule, rel=1e-5)


def test_rank_without_any_model_bootstraps_the_rule_score_once(acts, conn, tmp_path):
    add_sets(conn)
    assert acts.rank_sets() == N_SETS * SIZE
    ((version, picks_used, promoted, path, ranked_at),) = models(conn)
    assert (version, picks_used, promoted) == (1, 0, True) and ranked_at is not None
    assert path == str(tmp_path / "ai" / "models" / "1.npz")
    model = ranker.load(path)
    assert np.array_equal(model.weights, ranker.RULE_ONLY(model.pca).weights)
    assert len(scores(conn, 1)) == N_SETS * SIZE
    assert acts.rank_sets() == N_SETS * SIZE
    assert len(models(conn)) == 1


def test_bootstrap_waits_for_a_running_training(acts, conn):
    """rank_sets takes the train lock before the rank lock; a training in progress is waited
    for and its model used, not a second bootstrap."""
    add_sets(conn)
    conn.execute("SELECT pg_advisory_lock(hashtext('pm.ai.train'))")
    t = threading.Thread(target=acts.rank_sets)
    t.start()
    time.sleep(0.3)
    assert t.is_alive() and models(conn) == []
    acts_other = Activities(acts.deps)
    with psycopg.connect(acts.deps.settings.database_url) as c:
        acts_other._train_locked(c)  # the training that held the lock
    conn.execute("SELECT pg_advisory_unlock(hashtext('pm.ai.train'))")
    t.join(5)
    assert not t.is_alive()
    assert len(models(conn)) == 1 and models(conn)[0][4] is not None


def test_two_rank_sets_at_once_serialise(acts, conn, monkeypatch):
    add_sets(conn)
    acts.train_ranker()
    in_flight, overlap = [0], [False]
    real = ranker.score

    def slow_score(model, sets):
        in_flight[0] += 1
        overlap[0] |= in_flight[0] > 1
        time.sleep(0.3)
        in_flight[0] -= 1
        return real(model, sets)

    monkeypatch.setattr(ranker, "score", slow_score)
    results: list[int] = []
    threads = [threading.Thread(target=lambda: results.append(acts.rank_sets())) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == [N_SETS * SIZE] * 2
    assert not overlap[0]


def test_rank_waits_for_the_lock_and_releases_it_on_error(acts, conn, monkeypatch):
    add_sets(conn)
    acts.train_ranker()
    conn.execute("SELECT pg_advisory_lock(hashtext('pm.ai.rank'))")
    t = threading.Thread(target=acts.rank_sets)
    t.start()
    time.sleep(0.3)
    assert t.is_alive() and models(conn)[0][4] is None
    conn.execute("SELECT pg_advisory_unlock(hashtext('pm.ai.rank'))")
    t.join(5)
    assert not t.is_alive() and models(conn)[0][4] is not None

    def boom(model, sets):
        raise RuntimeError("boom")

    monkeypatch.setattr(ranker, "score", boom)
    with pytest.raises(RuntimeError):
        acts.rank_sets()
    assert conn.execute("SELECT pg_try_advisory_lock(hashtext('pm.ai.rank'))").fetchone() == (True,)
    conn.execute("SELECT pg_advisory_unlock(hashtext('pm.ai.rank'))")


def test_ranks_on_the_newest_completely_described_feature_version(acts, conn, monkeypatch):
    add_sets(conn, feature_version=1)
    assert db.ranking_feature_version(conn, 2) == 1  # version 2 has a backlog
    acts.train_ranker()
    monkeypatch.setattr(acts_mod, "FEATURE_VERSION", 2)
    assert acts.rank_sets() == N_SETS * SIZE  # still the version 1 features
    add_sets(conn, feature_version=2)
    assert db.ranking_feature_version(conn, 2) == 2
    conn.execute("TRUNCATE photo_features")
    assert db.ranking_feature_version(conn, 2) == 2  # none complete: the latest anyway


# --- TrainWorkflow -----------------------------------------------------------------------------

QUEUE = "ai-train-test"


class FakeTrain:
    def __init__(self, result: int | None) -> None:
        self.result = result
        self.trained = 0
        self.ranked = 0
        self.release = asyncio.Event()

    @activity.defn(name="train_ranker")
    async def train_ranker(self) -> int | None:
        self.trained += 1
        await self.release.wait()
        return self.result

    @activity.defn(name="rank_sets")
    async def rank_sets(self) -> int:
        self.ranked += 1
        return 7


async def _train(fake: FakeTrain, starts: int):
    async with await WorkflowEnvironment.start_time_skipping() as env, Worker(
        env.client, task_queue=QUEUE, workflows=[TrainWorkflow],
        activities=[fake.train_ranker, fake.rank_sets],
    ):
        handles = []
        for _ in range(starts):
            handles.append(await env.client.start_workflow(
                TrainWorkflow.run, id=TRAIN_WORKFLOW_ID, task_queue=QUEUE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            ))
            await asyncio.sleep(0.2)
        fake.release.set()
        results = [await h.result() for h in handles]
        return results, {h.run_id for h in handles}


def test_train_workflow_ranks_only_when_promoted():
    fake = FakeTrain(2)
    assert asyncio.run(_train(fake, 1))[0] == [2]
    assert (fake.trained, fake.ranked) == (1, 1)
    fake = FakeTrain(None)
    assert asyncio.run(_train(fake, 1))[0] == [None]
    assert (fake.trained, fake.ranked) == (1, 0)


def test_two_train_starts_with_the_same_id_run_once():
    fake = FakeTrain(4)
    results, run_ids = asyncio.run(_train(fake, 2))
    assert results == [4, 4]
    assert len(run_ids) == 1
    assert (fake.trained, fake.ranked) == (1, 1)


def test_train_workflow_id():
    assert TRAIN_WORKFLOW_ID == "ai-train"


def test_train_skips_after_a_losing_training_without_newer_picks(acts, conn, tmp_path, monkeypatch):
    """The skip compares against the newest model of any kind: a kept-unpromoted model already
    learnt from these picks, so training again would only store another identical loser."""
    sets = add_sets(conn)
    acts.train_ranker()
    add_smile_picks(conn, sets, 30)
    real = ranker.train
    monkeypatch.setattr(ranker, "train", lambda *a: (real(*a)[0], 0.1, False))
    assert acts.train_ranker() is None  # v2 kept unpromoted
    assert len(models(conn)) == 2
    files = sorted((tmp_path / "ai" / "models").iterdir())
    assert acts.train_ranker() is None
    assert len(models(conn)) == 2
    assert sorted((tmp_path / "ai" / "models").iterdir()) == files


def stamp_candidates_changed(conn) -> None:
    conn.execute("UPDATE similar_state SET candidates_changed_at = clock_timestamp() WHERE id = 1")


def test_rank_needed(acts, conn):
    assert acts.rank_needed() is False  # nothing described: nothing to rank
    sets = add_sets(conn)
    assert acts.rank_needed() is True  # described candidates, no ranked model
    acts.train_ranker()
    assert acts.rank_needed() is True  # promoted but not ranked yet
    acts.rank_sets()
    assert acts.rank_needed() is False
    stamp_candidates_changed(conn)  # staging changed set membership after the ranking
    assert acts.rank_needed() is True
    acts.rank_sets()
    assert acts.rank_needed() is False
    add_smile_picks(conn, sets, 30)
    acts.train_ranker()  # v2 promoted, not ranked
    assert acts.rank_needed() is True
    acts.rank_sets()
    assert acts.rank_needed() is False
