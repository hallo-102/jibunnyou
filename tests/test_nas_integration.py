"""合成入力で隔離、改変停止、頭数契約、非開催日を最低限検証する。"""
from __future__ import annotations

import importlib
import json
from pathlib import Path

import pandas as pd
import pytest

from continuous_improvement.archive import sha256, verify_archive, write_json
from nas.candidate import forward_audit, run, separate_paths, validate_population, verify_candidate
from nas.compare import compare, fixed_tickets


def workbook(path: Path, rows: list[dict], *, source: bool = False) -> None:
    """実データを使わず必要なシート契約だけを作る。"""
    frame = pd.DataFrame(rows)
    if not source:
        for column, default in (("risk_score", 0), ("extra_penalty", 0), ("data_confidence", 100)):
            if column not in frame:
                frame[column] = default
    with pd.ExcelWriter(path) as writer:
        frame.to_excel(writer, sheet_name="今走レース情報" if source else "TARGET", index=False)
        if not source:
            pd.DataFrame({"レースID": ["R1"], "1位馬番": [1], "予想状態": ["PREDICTED"], "予想可否": ["数値予想済み"]}).to_excel(writer, sheet_name="買い目_レース別1行", index=False)


@pytest.mark.parametrize("relative", [".", "child", "../"])
def test_output_must_be_independent(tmp_path: Path, relative: str) -> None:
    snapshot = tmp_path / "snapshot"
    with pytest.raises(ValueError, match="独立"):
        separate_paths(snapshot, snapshot / relative)


def test_fixed_archive_rejects_tampering_and_escape(tmp_path: Path) -> None:
    item = tmp_path / "input.xlsx"
    item.write_bytes(b"fixture")
    write_json(tmp_path / "manifest.json", {"files": {"input.xlsx": sha256(item)}})
    item.write_bytes(b"changed")
    with pytest.raises(ValueError, match="SHA"):
        verify_archive(tmp_path)
    write_json(tmp_path / "manifest.json", {"files": {"../outside": "0" * 64}})
    with pytest.raises(ValueError):
        verify_archive(tmp_path)


def test_model_and_forward_policy_are_not_silently_relaxed() -> None:
    assert verify_candidate()["effective_sha256"] == "8325f5c0748aa49076631b02424f2104a7924d0b39a4148d4703f687a937a789"
    audit = forward_audit()
    assert audit["active_generation"] == "field_size_adjusted_finish_20260905"
    assert audit["status"] == "BLOCKED"
    assert audit["candidate_scope"] == "offline"


def test_population_missing_scores_and_scratches(tmp_path: Path) -> None:
    source, pred = tmp_path / "input.xlsx", tmp_path / "prediction.xlsx"
    # 取消を入力に残した場合も、順位のあるレースから一部馬だけ黙って落とせない。
    rows = [{"レースID": "R1", "馬番": 1}, {"レースID": "R1", "馬番": 2, "状態": "取消"}]
    workbook(source, rows, source=True)
    output = [{"rid_str": "R1", "馬番": 1, "rank": 1, "score_raw": 50.0001},
              {"rid_str": "R1", "馬番": 2, "rank": 2, "score_raw": 50.0}]
    workbook(pred, output)
    assert validate_population(source, pred)["predicted_horses"] == 2
    workbook(pred, output[:1])
    with pytest.raises(ValueError, match="頭数"):
        validate_population(source, pred)
    output[1]["score_raw"] = None
    workbook(pred, output)
    with pytest.raises(ValueError, match="丸め前"):
        validate_population(source, pred)


def test_non_raceday_never_publishes_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workspace) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    names = ["input.xlsx", "odds.csv", "masters/race_levels.xlsx", "masters/場所_馬場_タイム.xlsx", "calendar.txt"]
    for name in names:
        target = snapshot / name
        target.parent.mkdir(exist_ok=True)
        target.write_text("20260808\n", encoding="utf-8")
    write_json(snapshot / "manifest.json", {"day": "20260809", "mode": "offline", "files": {n: sha256(snapshot / n) for n in names}})
    # 呼ばれたら失敗する偽pipelineで、非開催時の予想非実行を確認する。
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    monkeypatch.setattr(pipeline, "run_pipeline", lambda **_: pytest.fail("非開催日に予想した"))
    result = run(snapshot, workspace / "runs", workspace=workspace, calendar=snapshot / "calendar.txt")
    assert json.loads((result / "STATUS.json").read_text())["status"] == "NO_RACE"
    assert not (result / "READY.json").exists()


def test_compare_keeps_original_dense_rank_and_exact_top5(tmp_path: Path, workspace) -> None:
    tmp_path = tmp_path / "inputs"
    tmp_path.mkdir()
    left, right = tmp_path / "old.xlsx", tmp_path / "new.xlsx"
    old = [{"rid_str": "R1", "馬番": i, "rank": min(i, 5), "score": 60.0, "feature": 1.0} for i in range(1, 7)]
    new = [{**r, "rank": i, "feature": 1.0 + 1e-12} for i, r in enumerate(old, 1)]
    workbook(left, old)
    workbook(right, new)
    before = sha256(left)
    report = compare(left, right, workspace / "comparisons/comparison.json", {"features": ["feature"], "feature_names": {}}, workspace=workspace, provenance={})
    assert sha256(left) == before
    assert len(report["races"][0]["left"]["top5"]) == 6
    assert len(report["races"][0]["right"]["top5"]) == 5
    assert report["horse_differences"][0]["changes"] == {"rank": {"left": 5.0, "right": 6.0}}
    assert "score_raw" in report["unavailable_columns"]["left"]


def test_pipeline_failure_leaves_failed_status_without_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workspace) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    names = ["input.xlsx", "odds.csv", "masters/race_levels.xlsx", "masters/場所_馬場_タイム.xlsx"]
    for name in names:
        path = snapshot / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"fixture")
    write_json(snapshot / "manifest.json", {"day": "20260809", "mode": "offline", "files": {n: sha256(snapshot / n) for n in names}})
    pipeline = importlib.import_module("1_keibayosou_pipeline")

    def fail(**kwargs: object) -> None:
        raise RuntimeError("fixture failure")  # READYを発行してはいけない処理失敗を注入する。

    monkeypatch.setattr(pipeline, "run_pipeline", fail)
    with pytest.raises(RuntimeError, match="fixture failure"):
        run(snapshot, workspace / "runs", workspace=workspace)
    result = next((workspace / "runs").iterdir())
    assert json.loads((result / "STATUS.json").read_text())["status"] == "FAILED"
    assert not (result / "READY.json").exists()


def test_pr11_review_does_not_use_raw_total_to_reorder_saved_tie(tmp_path: Path) -> None:
    from continuous_improvement.review import predictions
    path = tmp_path / "prediction.xlsx"
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame({"rid_str": ["R1", "R1"], "馬番": [13, 16], "rank": [1, 1],
                      "score": [60.0, 60.0], "total": [1.0, 2.0]}).to_excel(writer, sheet_name="TARGET", index=False)
        pd.DataFrame({"レースID": ["R1"], "場所": ["京都"], "コース": ["芝1600"], "クラス": ["3勝"], "馬場": ["良"]}).to_excel(writer, sheet_name="今走レース情報", index=False)
    before = sha256(path)
    frame, _ = predictions(path, {"features": [], "feature_names": {}})
    assert frame["original_pred_rank"].tolist() == [1, 1]
    assert frame.set_index("馬番")["pred_rank"].to_dict() == {13: 1, 16: 2}
    assert sha256(path) == before


def test_fixed_ticket_comparison_separates_score_from_purchase() -> None:
    roi = pd.DataFrame({"レースID": ["R1"], "score1": [60.0], "3連複1点目_馬番1": [1],
                        "3連複1点目_馬番2": [3], "3連複1点目_馬番3": [2], "3連複_金額": [100]})
    before = fixed_tickets(roi, "R1")
    roi["score1"] = 70.0  # score変更だけで固定買い目差が出ないことを確認する。
    assert fixed_tickets(roi, "R1") == before
    roi["3連複1点目_馬番3"] = 4
    assert fixed_tickets(roi, "R1") != before
