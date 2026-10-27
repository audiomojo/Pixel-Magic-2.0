"""Activities on task queue `ai`. Sync functions: the worker runs them on a thread pool.

Decoding and the cheap measures run in parallel; the face detector and the embedder share one
lock (one model call at a time, so the GPU and Vision are never driven from two threads).
"""

import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from temporalio import activity

from . import db, ranker
from .config import Settings
from .embed import FEATURE_VERSION, Embedder
from .faces import FaceDetector
from .imageio import UndecodableError, load_rgb
from .quality import measure

log = logging.getLogger(__name__)

RANK_LOCK = "pm.ai.rank"
TRAIN_LOCK = "pm.ai.train"

def model_path(data_dir: Path, version: int) -> Path:
    return data_dir / "ai" / "models" / f"{version}.npz"


def save_atomic(model: ranker.Model, path: Path) -> None:
    """A temp file in the same directory, then rename: a reader never sees half a model."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    try:
        ranker.save(model, tmp)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def load_current(
    conn: psycopg.Connection,
) -> tuple[db.ModelRow, ranker.Model | None, int] | None:
    """(current row, the model to use, the version it is from), or None with no promoted model.
    An unreadable current file falls back to the previous promoted versions (newest first);
    with none readable the model is None and the version the current one."""
    rows = db.promoted_models(conn)
    if not rows:
        return None
    for row in rows:
        try:
            return rows[0], ranker.load(row.path), row.version
        except Exception as e:  # noqa: BLE001 — missing, truncated, not a model file
            log.warning("model version %d file %r unreadable: %s", row.version, row.path, e)
    return rows[0], None, rows[0].version


def vault_path(data_dir: Path, sha256: str, ext: str) -> Path:
    return data_dir / "vault" / sha256[0:2] / sha256[2:4] / f"{sha256}.{ext}"


@dataclass
class Deps:
    """Built once per worker process and shared by every activity thread."""

    settings: Settings
    detector: FaceDetector
    embedder: Embedder
    model_lock: threading.Lock = field(default_factory=threading.Lock)


class Activities:
    def __init__(self, deps: Deps) -> None:
        self.deps = deps

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self.deps.settings.database_url)

    @activity.defn(name="describe_photo")
    def describe_photo(self, sha256: str, ext: str) -> str:
        """"ok" or "failed". An undecodable file gets a final failed row; any other error
        (a missing or unreadable file, the database) propagates so Temporal retries."""
        d = self.deps
        path = vault_path(d.settings.data_dir, sha256, ext)
        try:
            img = load_rgb(path, ext)  # opens the vault file read-only
        except UndecodableError as e:
            row = db.FeatureRow(sha256, FEATURE_VERSION, None, None, None, e.reason)
        else:
            quality = measure(img)
            with d.model_lock:
                faces = d.detector.detect(img)
                embedding = d.embedder.embed(img)
                quality["aesthetic"] = d.embedder.aesthetic(img)
            row = db.FeatureRow(
                sha256, FEATURE_VERSION, [float(x) for x in embedding], quality, faces, None
            )
        with self._connect() as conn:
            db.upsert_features(conn, row)
            conn.commit()
        return "failed" if row.failed else "ok"

    @activity.defn(name="describe_backlog_page")
    def describe_backlog_page(
        self, limit: int, after_sha: str | None = None
    ) -> list[tuple[str, str]]:
        with self._connect() as conn:
            return db.describe_backlog(conn, FEATURE_VERSION, limit, after_sha=after_sha)

    @activity.defn(name="train_ranker")
    def train_ranker(self) -> int | None:
        """Fits a model on the picks and stores it (file and ranker_models row, kept whether
        promoted or not). The promoted version, else None. The first run promotes the rule
        score."""
        with self._connect() as conn:
            conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (TRAIN_LOCK,))
            try:
                fv = db.ranking_feature_version(conn, FEATURE_VERSION)
                sets = db.load_sets(conn, fv)
                picks = db.load_picks(conn)
                found = load_current(conn)
                current = None
                if found is not None:  # no readable file: compare against the rule score
                    current = found[1] or ranker.RULE_ONLY(ranker.fit_pca(sets))
                model, cv, promoted = ranker.train(sets, picks, current)
                used = ranker.picks_used(model, sets, picks)
                version = db.insert_model(conn, fv, used, cv, promoted, "")
                path = model_path(self.deps.settings.data_dir, version)
                save_atomic(model, path)
                db.set_model_path(conn, version, str(path))
                conn.commit()
                log.info(
                    "model version %d: %d picks, cv %.3f, %s",
                    version, used, cv, "promoted" if promoted else "kept unpromoted",
                )
                return version if promoted else None
            finally:
                conn.rollback()  # no-op after the commit; clears a failed transaction
                conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (TRAIN_LOCK,))
                conn.commit()

    @activity.defn(name="rank_sets")
    def rank_sets(self) -> int:
        """Scores every described candidate with the current model; candidates scored.

        Scores go under the version whose file was used: an unreadable current file falls back
        to the previous promoted version's file. When no file is readable the rule score is
        written under the current version (its row exists; the rule score needs no file)."""
        with self._connect() as conn:
            conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (RANK_LOCK,))
            try:
                found = load_current(conn)
                if found is None:
                    log.info("rank_sets: no promoted model yet")
                    return 0
                current, model, version = found
                fv = db.ranking_feature_version(conn, FEATURE_VERSION)
                sets = db.load_sets(conn, fv)
                if model is None:
                    model = ranker.RULE_ONLY(ranker.fit_pca(sets))
                    log.warning("rank_sets: rule score under version %d", version)
                elif version != current.version:
                    log.warning(
                        "rank_sets: version %d instead of %d", version, current.version
                    )
                scores = ranker.score(model, sets)
                db.write_scores(conn, version, scores)
                db.mark_ranked(conn, version)
                conn.commit()
                log.info(
                    "rank_sets: %d candidates scored by version %d (features v%d)",
                    len(scores), version, fv,
                )
                return len(scores)
            finally:
                conn.rollback()
                conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (RANK_LOCK,))
                conn.commit()

    def all(self) -> list:
        return [
            self.describe_photo,
            self.describe_backlog_page,
            self.train_ranker,
            self.rank_sets,
        ]
