"""Worker settings: the same variables and defaults as src/harvest/config.ts."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    database_url: str
    temporal_address: str
    temporal_namespace: str
    describe_concurrency: int


def parse_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines; `#` comments and blank lines skipped; matching quotes stripped."""
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        out[key] = value
    return out


def load_settings(env: Mapping[str, str], repo_root: Path) -> Settings:
    """Environment variables win over `<repo_root>/.env`; an empty value means the default."""
    merged = {**parse_env_file(repo_root / ".env"), **env}

    def get(key: str, default: str) -> str:
        return merged.get(key) or default

    password = get("POSTGRES_PASSWORD", "pixelmagic")
    return Settings(
        data_dir=repo_root / get("DATA_DIR", "data"),
        database_url=get(
            "DATABASE_URL", f"postgres://postgres:{password}@127.0.0.1:5432/pixelmagic"
        ),
        temporal_address=get("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
        temporal_namespace=get("TEMPORAL_NAMESPACE", "default"),
        describe_concurrency=int(get("AI_DESCRIBE_CONCURRENCY", "2")),
    )
