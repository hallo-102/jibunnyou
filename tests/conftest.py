"""候補専用workspaceを各テストで新規初期化する。"""
import pytest

from nas.workspace import initialize


@pytest.fixture
def workspace(tmp_path):
    production = tmp_path / "production"
    production.mkdir()
    return initialize(tmp_path / "candidate", production)
