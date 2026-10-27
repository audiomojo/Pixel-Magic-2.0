"""Database access for the worker. Functions take a connection and never commit: the caller
owns the transaction (use `with conn.transaction():` or an autocommit connection)."""

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import psycopg
from psycopg.types.json import Jsonb


@dataclass
class FeatureRow:
    sha256: str
    feature_version: int
    embedding: list[float] | None
    quality: dict | None
    faces: list[dict] | None
    failed: str | None


@dataclass
class Described:
    sha256: str
    embedding: np.ndarray
    quality: dict
    faces: list[dict]


@dataclass
class Pick:
    chosen: str
    shown: list[str]


@dataclass
class ModelRow:
    version: int
    feature_version: int
    picks_used: int
    cv_agreement: float | None
    promoted: bool
    path: str
    ranked_at: datetime | None


def describe_backlog(conn: psycopg.Connection, feature_version: int, limit: int) -> list[tuple[str, str]]:
    """(sha256, ext) of candidates with no photo_features row (failed or not) for the version."""
    rows = conn.execute(
        """
        SELECT c.sha256, c.ext
        FROM contents c
        WHERE EXISTS (SELECT 1 FROM similar_candidates sc WHERE sc.sha256 = c.sha256)
          AND NOT EXISTS (
            SELECT 1 FROM photo_features f
            WHERE f.sha256 = c.sha256 AND f.feature_version = %s)
        ORDER BY c.sha256
        LIMIT %s
        """,
        (feature_version, limit),
    ).fetchall()
    return [(sha, ext) for sha, ext in rows]


def upsert_features(conn: psycopg.Connection, row: FeatureRow) -> None:
    """A failed row stores an empty embedding/quality/faces (the columns are NOT NULL)."""
    conn.execute(
        """
        INSERT INTO photo_features (sha256, feature_version, embedding, quality, faces, failed)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (sha256, feature_version) DO UPDATE SET
          embedding = EXCLUDED.embedding, quality = EXCLUDED.quality, faces = EXCLUDED.faces,
          failed = EXCLUDED.failed, described_at = now()
        """,
        (
            row.sha256,
            row.feature_version,
            row.embedding if row.embedding is not None else [],
            Jsonb(row.quality if row.quality is not None else {}),
            Jsonb(row.faces if row.faces is not None else []),
            row.failed,
        ),
    )


def load_sets(conn: psycopg.Connection, feature_version: int) -> dict[int, list[Described]]:
    """Set id -> its described, non-failed candidates."""
    rows = conn.execute(
        """
        SELECT sc.set_id, f.sha256, f.embedding, f.quality, f.faces
        FROM similar_candidates sc
        JOIN photo_features f ON f.sha256 = sc.sha256 AND f.feature_version = %s
        WHERE f.failed IS NULL
        ORDER BY sc.set_id, f.sha256
        """,
        (feature_version,),
    ).fetchall()
    sets: dict[int, list[Described]] = {}
    for set_id, sha, emb, quality, faces in rows:
        sets.setdefault(set_id, []).append(
            Described(sha, np.asarray(emb, dtype=np.float32), quality, faces)
        )
    return sets


def load_picks(conn: psycopg.Connection) -> list[Pick]:
    rows = conn.execute(
        "SELECT chosen_sha256, shown_sha256 FROM picks WHERE NOT no_clear_best ORDER BY id"
    ).fetchall()
    return [Pick(chosen, list(shown)) for chosen, shown in rows]


def insert_model(
    conn: psycopg.Connection,
    feature_version: int,
    picks_used: int,
    cv_agreement: float | None,
    promoted: bool,
    path: str,
) -> int:
    row = conn.execute(
        """
        INSERT INTO ranker_models (feature_version, picks_used, cv_agreement, promoted, path)
        VALUES (%s, %s, %s, %s, %s) RETURNING version
        """,
        (feature_version, picks_used, cv_agreement, promoted, path),
    ).fetchone()
    assert row is not None
    return row[0]


def current_model(conn: psycopg.Connection) -> ModelRow | None:
    """The highest-version promoted model."""
    row = conn.execute(
        """
        SELECT version, feature_version, picks_used, cv_agreement, promoted, path, ranked_at
        FROM ranker_models WHERE promoted ORDER BY version DESC LIMIT 1
        """
    ).fetchone()
    return ModelRow(*row) if row else None


def write_scores(conn: psycopg.Connection, version: int, scores: dict[str, float]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO photo_scores (sha256, model_version, score) VALUES (%s, %s, %s)
            ON CONFLICT (sha256, model_version) DO UPDATE SET score = EXCLUDED.score
            """,
            [(sha, version, score) for sha, score in scores.items()],
        )


def mark_ranked(conn: psycopg.Connection, version: int) -> None:
    conn.execute("UPDATE ranker_models SET ranked_at = now() WHERE version = %s", (version,))
