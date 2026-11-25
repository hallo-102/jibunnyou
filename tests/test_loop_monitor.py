"""監視画面で未実行や古い記録を正常完了と誤表示しないための検証。"""
from datetime import datetime
import json

import pytest

from desktop_monitor.state import JST, collect, present, read_json


NOW = datetime(2026, 11, 14, 6, 30, tzinfo=JST)


def snapshot():
    # 実際の状態JSONと同じ構造の最小観測値を用意する。
    return {"loop": {"checked_at": NOW.isoformat(), "days": {"20261114": {}}},
            "daily": {}, "ready": {}, "day": "20261114", "daily_log": ""}


def test_before_scheduled_start_is_waiting():
    view = present(snapshot(), NOW)
    assert view["tone"] == "wait"
    assert "06:45" in view["headline"]
    assert view["stages"][0]["status"] == "done"
    assert all(s["status"] == "wait" for s in view["stages"][1:])


def test_absent_start_after_grace_is_warning():
    value = snapshot()
    value["loop"]["checked_at"] = NOW.replace(minute=51).isoformat()
    view = present(value, NOW.replace(minute=51))
    assert "開始記録がありません" in view["headline"]
    assert view["tone"] == "warn"


def test_no_race_is_not_failed():
    value = snapshot()
    value["daily"] = {"status": "NO_RACE"}
    view = present(value, NOW)
    assert "非開催日" in view["headline"]
    assert all(s["status"] == "skip" for s in view["stages"][1:])


def test_failure_is_visible():
    value = snapshot()
    value["daily"] = {"status": "FAILED", "error": "マスタを読み込めません"}
    view = present(value, NOW)
    assert view["tone"] == "error"
    assert "マスタ" in view["issues"][0]


def test_ready_is_not_same_as_frozen():
    value = snapshot()
    value["ready"] = {"status": "READY"}
    view = present(value, NOW)
    assert view["stages"][1]["status"] == "done"
    assert view["stages"][2]["status"] == "wait"


def test_running_log_is_used_without_inventing_progress():
    value = snapshot()
    value["daily"] = {"status": "RUNNING"}
    value["daily_log"] = "INFO stage_start odds\nINFO stage_end odds\nINFO stage_start prediction"
    view = present(value, NOW)
    assert view["tone"] == "active"
    assert "stage_start prediction" in view["stages"][1]["detail"]


def test_stale_heartbeat_overrides_done_state():
    value = snapshot()
    value["loop"]["days"]["20261114"] = {"frozen": "/pre", "shadow": "/shadow"}
    view = present(value, NOW.replace(hour=8))
    assert view["tone"] == "warn"
    assert "更新" in view["headline"]


def test_recovered_errors_are_not_current_failures():
    value = snapshot()
    value["loop"]["days"]["20261114"] = {"frozen": "/pre", "freeze_error": "古いエラー",
        "shadow": "/shadow", "shadow_error": "古いエラー", "review": "/post", "error": "古いエラー",
        "masters_updated": NOW.isoformat(), "master_update_error": "古いエラー",
        "uploaded": "post", "upload_error": "古いエラー"}
    view = present(value, NOW)
    assert view["tone"] == "done"
    assert "まで完了" in view["headline"]
    assert view["issues"] == []


def test_shadow_missed_is_never_marked_done():
    value = snapshot()
    value["loop"]["days"]["20261114"] = {"frozen": "/pre", "shadow_missed": True}
    view = present(value, NOW)
    assert view["tone"] == "warn"
    assert "除外" in view["headline"]


def test_missing_day_does_not_reuse_yesterday():
    value = snapshot()
    value["day"] = "20261113"
    assert "記録はありません" in present(value, NOW)["headline"]


def test_read_json_does_not_hide_invalid_data(tmp_path):
    path = tmp_path / "status.json"
    assert read_json(path) == {}
    with pytest.raises(FileNotFoundError):
        read_json(path, required=True)
    path.write_text("{", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        read_json(path)


def test_collection_is_read_only_and_missing_daily_is_wait(tmp_path):
    path = tmp_path / "data/output/continuous_improvement/status.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(snapshot()["loop"]), encoding="utf-8")
    before = {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    result = collect(str(tmp_path), "20261114")
    assert result["daily"] == {}
    assert {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


def test_observer_publishes_new_names_without_changing_business_files(tmp_path):
    from desktop_monitor.publisher import publish
    root = tmp_path / "nas"
    state_file = root / "data/output/continuous_improvement/status.json"
    state_file.parent.mkdir(parents=True)
    state_file.write_text(json.dumps(snapshot()["loop"]), encoding="utf-8")
    original = state_file.read_bytes()
    output = tmp_path / "observations"
    first = publish(root, output)
    second = publish(root, output)
    assert first != second
    assert first.read_bytes() == second.read_bytes() or json.loads(first.read_text())["day"] == json.loads(second.read_text())["day"]
    assert "snapshot_published_at" in json.loads(second.read_text())
    assert state_file.read_bytes() == original
