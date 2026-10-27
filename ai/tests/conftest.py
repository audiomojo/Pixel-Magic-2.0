import re
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"

# Mirrors truncate() in tests/helpers/pg.ts.
TRUNCATES = [
    "TRUNCATE files, contents, groups, staging_builds RESTART IDENTITY",
    "UPDATE cluster_state SET version = 0 WHERE id = 1",
    "UPDATE scan_stats SET pending_files = 0, pending_bytes = 0, scanned_at = NULL WHERE id = 1",
    "TRUNCATE harvest_run_folders, harvest_items, harvest_runs, harvest_folders, harvest_sources"
    " RESTART IDENTITY CASCADE",
    "UPDATE harvest_host_state SET heartbeat_at = NULL, version = NULL WHERE id = 1",
    "TRUNCATE photo_features, picks, photo_scores, ranker_models, similar_candidates"
    " RESTART IDENTITY",
    "UPDATE similar_state SET candidates_changed_at = NULL WHERE id = 1",
    "UPDATE copy_sweep_state SET after_id = 0 WHERE id = 1",
]


def up_section(sql: str) -> str:
    m = re.search(r"-- Up Migration(.*?)(?:-- Down Migration|\Z)", sql, re.S)
    assert m, "migration without an Up section"
    return m.group(1)


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:16", driver=None) as pg:
        url = pg.get_connection_url()
        with psycopg.connect(url, autocommit=True) as conn:
            for path in sorted(MIGRATIONS.glob("*.sql")):
                conn.execute(up_section(path.read_text()))
        yield url


@pytest.fixture
def conn(database_url: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(database_url, autocommit=True) as c:
        for stmt in TRUNCATES:
            c.execute(stmt)
        yield c
