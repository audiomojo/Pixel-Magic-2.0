from pathlib import Path

from pixelmagic_ai.config import load_settings


def test_defaults(tmp_path: Path):
    s = load_settings({}, tmp_path)
    assert s.data_dir == tmp_path / "data"
    assert s.database_url == "postgres://postgres:pixelmagic@127.0.0.1:5432/pixelmagic"
    assert s.temporal_address == "127.0.0.1:7233"
    assert s.temporal_namespace == "default"
    assert s.describe_concurrency == 2


def test_env_file_and_precedence(tmp_path: Path):
    (tmp_path / ".env").write_text(
        "# comment\nPOSTGRES_PASSWORD='s3cret'\nDATA_DIR=\"vault\"\nAI_DESCRIBE_CONCURRENCY=4\n"
        "TEMPORAL_NAMESPACE=fromfile\n\n"
    )
    s = load_settings({"TEMPORAL_NAMESPACE": "fromenv"}, tmp_path)
    assert s.data_dir == tmp_path / "vault"
    assert s.database_url == "postgres://postgres:s3cret@127.0.0.1:5432/pixelmagic"
    assert s.describe_concurrency == 4
    assert s.temporal_namespace == "fromenv"


def test_empty_value_means_default(tmp_path: Path):
    s = load_settings({"DATABASE_URL": "", "AI_DESCRIBE_CONCURRENCY": ""}, tmp_path)
    assert s.describe_concurrency == 2
    assert s.database_url.startswith("postgres://postgres:")
