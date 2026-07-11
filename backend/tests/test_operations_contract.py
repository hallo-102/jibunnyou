from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = REPOSITORY_ROOT if (REPOSITORY_ROOT / "README.md").is_file() else Path("/workspace/release-contract")


def test_api_documentation_is_available_below_gateway_api_path() -> None:
    """GatewayのAPI routing配下でOpenAPIとSwaggerを提供する。"""

    with TestClient(app) as client:
        assert client.get("/api/openapi.json").status_code == 200
        assert client.get("/api/docs").status_code == 200
        assert client.get("/docs").status_code == 404


def test_operations_documents_cover_required_procedures() -> None:
    """初心者向け運用文書に完成条件の手順が揃っていることを固定する。"""

    readme = (CONTRACT_ROOT / "README.md").read_text(encoding="utf-8")
    operations = (CONTRACT_ROOT / "docs" / "operations" / "運用手順.md").read_text(encoding="utf-8")
    troubleshooting = (CONTRACT_ROOT / "docs" / "operations" / "障害対応・FAQ.md").read_text(
        encoding="utf-8"
    )
    combined = "\n".join((readme, operations, troubleshooting))

    for required in (
        "インストール",
        "起動",
        "停止",
        "更新",
        "バックアップ",
        "復元",
        "ログ",
        "設定",
        "モデル",
        "APIキー",
        "FAQ",
    ):
        assert required in combined

    assert "docker compose up -d --build" in readme
    assert "Backup-KeibaAiStudio.ps1" in operations
    assert "Restore-KeibaAiStudio.ps1" in operations
    assert "down -v" in combined
    assert "自動投票" in combined
