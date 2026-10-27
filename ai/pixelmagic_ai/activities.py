"""Activities on task queue `ai`. Sync functions: the worker runs them on a thread pool.

Decoding and the cheap measures run in parallel; the face detector and the embedder share one
lock (one model call at a time, so the GPU and Vision are never driven from two threads).
"""

import threading
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from temporalio import activity

from . import db
from .config import Settings
from .embed import FEATURE_VERSION, Embedder
from .faces import FaceDetector
from .imageio import UndecodableError, load_rgb
from .quality import measure


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

    @activity.defn(name="rank_sets")
    def rank_sets(self) -> int:
        """Placeholder until the ranker lands (Task 10): sets ranked."""
        return 0

    def all(self) -> list:
        return [self.describe_photo, self.describe_backlog_page, self.rank_sets]
