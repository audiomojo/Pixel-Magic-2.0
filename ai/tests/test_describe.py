import os
from pathlib import Path

import pytest
from PIL import Image

from pixelmagic_ai import db
from pixelmagic_ai.activities import Activities, Deps, vault_path
from pixelmagic_ai.config import Settings
from pixelmagic_ai.embed import FEATURE_VERSION, FakeEmbedder
from pixelmagic_ai.faces import FakeDetector

A, B = "a1" + "0" * 62, "b2" + "0" * 62
FACE = {"eyes_open": 1.0, "smile": 0.5, "quality": 0.7, "size": 0.05}


def add_candidate(conn, sha: str, ext: str) -> None:
    conn.execute(
        "INSERT INTO contents (sha256, ext, kind, size_bytes, state)"
        " VALUES (%s, %s, 'still', 1, 'CLUSTERED')",
        (sha, ext),
    )
    conn.execute("INSERT INTO similar_candidates (set_id, sha256) VALUES (1, %s)", (sha,))


@pytest.fixture
def acts(tmp_path: Path, database_url: str, conn) -> Activities:
    settings = Settings(tmp_path, database_url, "unused:7233", "default", 2)
    return Activities(Deps(settings, FakeDetector([FACE]), FakeEmbedder()))


def put(tmp_path: Path, sha: str, ext: str, data: bytes | None = None) -> Path:
    p = vault_path(tmp_path, sha, ext)
    p.parent.mkdir(parents=True, exist_ok=True)
    if data is None:
        Image.new("RGB", (64, 48), (200, 120, 40)).save(p, format="JPEG")
    else:
        p.write_bytes(data)
    p.chmod(0o444)  # the vault is read-only for the worker
    return p


def test_vault_path_layout(tmp_path):
    assert vault_path(tmp_path, A, "jpg") == tmp_path / "vault" / "a1" / "00" / f"{A}.jpg"


def test_jpeg_candidate_gets_a_full_row(acts, conn, tmp_path):
    add_candidate(conn, A, "jpg")
    put(tmp_path, A, "jpg")
    assert acts.describe_photo(A, "jpg") == "ok"
    emb, quality, faces, failed = conn.execute(
        "SELECT embedding, quality, faces, failed FROM photo_features"
        " WHERE sha256 = %s AND feature_version = %s",
        (A, FEATURE_VERSION),
    ).fetchone()
    assert failed is None
    assert len(emb) == 8
    assert {"sharpness", "brightness", "highlights", "shadows", "noise", "aesthetic"} <= set(quality)
    assert 1.0 <= quality["aesthetic"] <= 10.0
    assert faces == [FACE]


def test_corrupt_file_gets_failed_row_without_raising(acts, conn, tmp_path):
    add_candidate(conn, B, "jpg")
    put(tmp_path, B, "jpg", b"not a jpeg at all")
    assert acts.describe_photo(B, "jpg") == "failed"
    (failed,) = conn.execute("SELECT failed FROM photo_features WHERE sha256 = %s", (B,)).fetchone()
    assert failed


def test_missing_file_raises_for_temporal_to_retry(acts, conn):
    add_candidate(conn, A, "jpg")
    with pytest.raises(FileNotFoundError):
        acts.describe_photo(A, "jpg")
    assert conn.execute("SELECT count(*) FROM photo_features").fetchone() == (0,)


def test_vault_file_is_unchanged(acts, conn, tmp_path):
    add_candidate(conn, A, "jpg")
    p = put(tmp_path, A, "jpg")
    before = (p.read_bytes(), os.stat(p).st_mtime_ns)
    acts.describe_photo(A, "jpg")
    assert (p.read_bytes(), os.stat(p).st_mtime_ns) == before


def test_rerun_is_a_noop(acts, conn, tmp_path):
    add_candidate(conn, A, "jpg")
    add_candidate(conn, B, "jpg")
    put(tmp_path, A, "jpg")
    put(tmp_path, B, "jpg", b"garbage")
    page = acts.describe_backlog_page(200)
    assert sorted(page) == [(A, "jpg"), (B, "jpg")]
    for sha, ext in page:
        acts.describe_photo(sha, ext)
    assert acts.describe_backlog_page(200) == []
    assert db.describe_backlog(conn, FEATURE_VERSION, 10) == []


def test_rank_sets_placeholder_returns_zero(acts):
    assert acts.rank_sets() == 0
