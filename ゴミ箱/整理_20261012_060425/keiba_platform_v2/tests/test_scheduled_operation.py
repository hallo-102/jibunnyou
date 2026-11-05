from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from keiba_v2.collectors import netkeiba


def test_official_ids_include_only_formal_race_cells():
    # 中止注記・別ページへのリンクを混ぜ、正式な番組表のレースだけを取得することを確認します。
    html = """<table class='basic'><tr><th><div class='main'>4回東京2日</div></th></tr>
    <tr><td>1レース</td></tr><tr><td>2レース</td></tr>
    <tr><td>第8レース以降取りやめ</td></tr></table>
    <a href='?race_id=202605040101'>前日結果</a>"""
    assert netkeiba._race_ids_from_official_program(html, "20261004") == ["202605040201", "202605040202"]


@pytest.mark.parametrize("html", ["<html>HTTP error</html>", "<table class='basic'><div class='main'>東京</div></table>"])
def test_official_program_fails_closed_when_unparseable(html):
    # 空ページや開催回・日が欠けたページから開催IDを捏造しないことを確認します。
    with pytest.raises(RuntimeError):
        netkeiba._race_ids_from_official_program(html, "20261004")


def test_official_fetch_propagates_http_failure(monkeypatch):
    # HTTP障害を開催なしとして扱わず、上位の処理を止めます。
    response = Mock()
    response.raise_for_status.side_effect = RuntimeError("HTTP 400")
    requests = Mock()
    requests.get.return_value = response
    monkeypatch.setattr(netkeiba, "_require_collection_deps", lambda: (requests, None, None, None))
    with pytest.raises(RuntimeError, match="HTTP 400"):
        netkeiba.collect_race_ids("20261004")


def test_entries_rejects_one_missing_race(monkeypatch):
    # 一部レースだけの収集成功を、全開催の成功として返さないことを確認します。
    import pandas as pd
    requests, soup, _, _ = netkeiba._require_collection_deps()
    session = Mock()
    session.get.return_value.content = b"<html></html>"
    monkeypatch.setattr(requests, "Session", lambda: session)
    monkeypatch.setattr(netkeiba, "collect_race_ids", lambda *a, **k: ["202605040201", "202605040202"])
    frames = iter([pd.DataFrame([{"race_id": "first"}]), pd.DataFrame()])
    monkeypatch.setattr(netkeiba, "_parse_runner_rows", lambda *a: next(frames))
    monkeypatch.setattr(netkeiba.time, "sleep", lambda *a: None)
    with pytest.raises(RuntimeError, match="race_id=202605040202"):
        netkeiba.collect_entries("20261004")


@pytest.mark.parametrize("script,stage,expected_calls", [
    ("run_raceday.ps1", "collect", ["collect"]),
    ("run_raceday.ps1", "validate", ["collect", "validate"]),
    ("run_raceday.ps1", "run", ["collect", "validate", "run"]),
    ("run_t5.ps1", "t5-runtime", ["t5-runtime"]),
])
def test_powershell_stops_on_native_failure(tmp_path, script, stage, expected_calls):
    # 実データ・DBに触れず、失敗したネイティブ段階以降を実行しないことをWindowsで検証します。
    shell = shutil.which("powershell.exe")
    if shell is None:
        pytest.skip("Windows PowerShell is required")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    calls = tmp_path / "calls.txt"
    stub = tmp_path / "stub.cmd"
    stub.write_text(f'@echo off\necho %3>>"{calls}"\nif "%3" == "{stage}" exit /b 7\nexit /b 0\n', encoding="ascii")
    source = Path(__file__).resolve().parents[1] / "scripts" / script
    text = source.read_text(encoding="utf-8").replace('$Python = ".\\.venv\\Scripts\\python.exe"', f'$Python = "{stub}"')
    (scripts / script).write_text(text, encoding="utf-8-sig")
    for folder in ("data/input", "data/raw"):
        (tmp_path / folder).mkdir(parents=True)
    (tmp_path / "data/input/races_20261004.csv").touch()
    (tmp_path / "data/raw/race_schedule_20261004.csv").touch()
    result = subprocess.run([shell, "-NoProfile", "-File", str(scripts / script), "-RaceDate", "20261004"], capture_output=True, timeout=30)
    assert result.returncode != 0
    assert b"completed" not in result.stdout
    assert calls.read_text().splitlines() == expected_calls
