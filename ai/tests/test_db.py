import numpy as np
import psycopg

from pixelmagic_ai import db

V = 1
A, B, C, D = ("a" * 64, "b" * 64, "c" * 64, "d" * 64)


def add_content(conn: psycopg.Connection, sha: str, ext: str = "jpg") -> None:
    conn.execute(
        "INSERT INTO contents (sha256, ext, kind, size_bytes, state)"
        " VALUES (%s, %s, 'still', 1, 'CLUSTERED')",
        (sha, ext),
    )


def add_candidates(conn: psycopg.Connection, set_id: int, *shas: str) -> None:
    for sha in shas:
        conn.execute("INSERT INTO similar_candidates (set_id, sha256) VALUES (%s, %s)", (set_id, sha))


def feature(sha: str, failed: str | None = None, version: int = V) -> db.FeatureRow:
    if failed:
        return db.FeatureRow(sha, version, None, None, None, failed)
    return db.FeatureRow(sha, version, [0.5, 0.25], {"aesthetic": 4.5}, [{"area": 0.1}], None)


def test_describe_backlog_returns_only_candidates_without_features(conn):
    for sha, ext in ((A, "jpg"), (B, "heic"), (C, "png")):
        add_content(conn, sha, ext)
    add_candidates(conn, 1, A, B)
    add_candidates(conn, 2, B, C)
    db.upsert_features(conn, feature(A))
    db.upsert_features(conn, feature(C, version=2))  # another version does not count
    assert db.describe_backlog(conn, V, 10) == [(B, "heic"), (C, "png")]
    assert db.describe_backlog(conn, V, 1) == [(B, "heic")]


def test_describe_backlog_counts_failed_rows_as_described(conn):
    add_content(conn, A)
    add_candidates(conn, 1, A)
    db.upsert_features(conn, feature(A, failed="bad file"))
    assert db.describe_backlog(conn, V, 10) == []


def test_upsert_features_twice_keeps_one_row(conn):
    db.upsert_features(conn, feature(A, failed="boom"))
    db.upsert_features(conn, feature(A))
    rows = conn.execute("SELECT failed, embedding, quality FROM photo_features").fetchall()
    assert rows == [(None, [0.5, 0.25], {"aesthetic": 4.5})]


def test_load_sets_skips_failed_rows(conn):
    add_candidates(conn, 1, A, B, C)
    add_candidates(conn, 2, D)
    db.upsert_features(conn, feature(A))
    db.upsert_features(conn, feature(B, failed="boom"))
    db.upsert_features(conn, feature(D, version=2))
    sets = db.load_sets(conn, V)
    assert list(sets) == [1]
    assert [d.sha256 for d in sets[1]] == [A]
    d = sets[1][0]
    assert isinstance(d.embedding, np.ndarray)
    assert d.embedding.tolist() == [0.5, 0.25]
    assert d.quality == {"aesthetic": 4.5}
    assert d.faces == [{"area": 0.1}]


def test_load_picks_skips_no_clear_best(conn):
    conn.execute(
        "INSERT INTO picks (set_id, chosen_sha256, shown_sha256, no_clear_best)"
        " VALUES (1, %s, %s, false), (2, NULL, %s, true)",
        (A, [A, B], [C, D]),
    )
    assert db.load_picks(conn) == [db.Pick(chosen=A, shown=[A, B])]


def test_current_model_ignores_unpromoted_rows(conn):
    assert db.current_model(conn) is None
    v1 = db.insert_model(conn, V, 40, 0.8, True, "m1.pkl")
    db.insert_model(conn, V, 60, 0.9, False, "m2.pkl")
    model = db.current_model(conn)
    assert model is not None and model.version == v1 and model.path == "m1.pkl"
    v3 = db.insert_model(conn, V, 80, 0.85, True, "m3.pkl")
    assert db.current_model(conn).version == v3


def test_write_scores_upserts_and_mark_ranked(conn):
    v = db.insert_model(conn, V, 40, None, True, "m.pkl")
    db.write_scores(conn, v, {A: 1.0, B: 2.0})
    db.write_scores(conn, v, {A: 3.0})
    rows = conn.execute("SELECT sha256, score FROM photo_scores ORDER BY sha256").fetchall()
    assert rows == [(A, 3.0), (B, 2.0)]
    assert db.current_model(conn).ranked_at is None
    db.mark_ranked(conn, v)
    assert db.current_model(conn).ranked_at is not None


def test_describe_backlog_after_cursor(conn):
    for sha in (A, B, C):
        add_content(conn, sha)
    add_candidates(conn, 1, A, B, C)
    assert db.describe_backlog(conn, V, 10, after_sha=A) == [(B, "jpg"), (C, "jpg")]
    assert db.describe_backlog(conn, V, 1, after_sha=A) == [(B, "jpg")]
    assert db.describe_backlog(conn, V, 10, after_sha=C) == []
    assert db.describe_backlog(conn, V, 10, after_sha=None) == [(A, "jpg"), (B, "jpg"), (C, "jpg")]
