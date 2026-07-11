from __future__ import annotations

import re
from pathlib import Path

import yaml


HOST_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = (
    HOST_REPOSITORY_ROOT
    if (HOST_REPOSITORY_ROOT / "docker-compose.yml").is_file()
    else Path("/workspace/release-contract")
)


def test_compose_publishes_only_gateway_and_restarts_services() -> None:
    """Gateway以外をホスト公開せず、全サービスを再起動可能にする。"""

    compose = yaml.safe_load((CONTRACT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    assert set(services) == {"postgres", "redis", "api", "worker", "beat", "frontend", "gateway"}
    assert services["gateway"]["ports"] == ["${KEIBA_APP_PORT:-18080}:80"]
    assert all("ports" not in config for name, config in services.items() if name != "gateway")
    assert all(config.get("restart") == "unless-stopped" for config in services.values())


def test_local_secret_files_are_ignored_and_examples_are_placeholders() -> None:
    """秘密ファイルを追跡せず、サンプルには実キーを置かない。"""

    gitignore = (CONTRACT_ROOT / ".gitignore").read_text(encoding="utf-8")
    env_example = (CONTRACT_ROOT / ".env.example").read_text(encoding="utf-8")

    assert re.search(r"(?m)^\.env$", gitignore)
    assert re.search(r"(?m)^\.env\.\*$", gitignore)
    assert "OPENAI_API_KEY=" in env_example
    assert "CHANGE_THIS_TO_A_LONG_RANDOM_PASSWORD" in env_example
    assert not re.search(r"sk-(?:proj-)?[A-Za-z0-9_-]{20,}", env_example)


def test_application_sources_do_not_embed_common_secret_patterns() -> None:
    """アプリ本体へ代表的な実秘密値形式が混入していないことを検査する。"""

    backend_app = HOST_REPOSITORY_ROOT / "backend" / "app"
    targets = [backend_app, CONTRACT_ROOT / "frontend" / "app"]
    patterns = [
        re.compile(r"sk-(?:proj-)?[A-Za-z0-9_-]{20,}"),
        re.compile(r"AKIA[0-9A-Z]{16}"),
        re.compile(r"ghp_[A-Za-z0-9]{30,}"),
    ]

    for directory in targets:
        for path in directory.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".ts", ".tsx", ".js"}:
                continue
            source = path.read_text(encoding="utf-8")
            assert all(pattern.search(source) is None for pattern in patterns), path


def test_purchase_execution_is_disabled_in_database_and_service() -> None:
    """外部購入クライアントを持たず、DBと生成処理の双方で自動購入を無効にする。"""

    backend_app = HOST_REPOSITORY_ROOT / "backend" / "app"
    models = (backend_app / "db" / "models.py").read_text(encoding="utf-8")
    betting = (backend_app / "services" / "betting.py").read_text(
        encoding="utf-8"
    )

    assert "purchase_execution_enabled = false" in models
    assert betting.count("purchase_execution_enabled=False") >= 2
    assert "ipat" not in betting.lower()
    assert "jra.go.jp" not in betting.lower()
