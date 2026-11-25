"""PR #17の重大指摘を、小さな失敗再現で固定する。"""
from dataclasses import dataclass
import importlib
import json
from pathlib import Path

import pandas as pd
import pytest

from continuous_improvement.archive import sha256, write_json
from nas import candidate
from nas.compare import compare


def sheets(path, *, ranks=(1, 2), predicted=True):
    # 本物のExcel境界で分類と順位の不整合を再現する。
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame({"rid_str": ["R1", "R1"], "馬番": [1, 2], "rank": ranks,
                      "score_raw": [60.0, 50.0], "risk_score": [0, 0],
                      "extra_penalty": [0, 0], "data_confidence": [100, 100]}).to_excel(writer, sheet_name="TARGET", index=False)
        pd.DataFrame({"レースID": ["R1", "R1"], "馬番": [1, 2]}).to_excel(writer, sheet_name="今走レース情報", index=False)
        pd.DataFrame({"レースID": ["R1"], "予想状態": ["PREDICTED" if predicted else "NO_PREDICTION"],
                      "予想可否": ["数値予想済み" if predicted else "予想対象外"], "購入金額": [0]}).to_excel(writer, sheet_name="買い目_レース別1行", index=False)


def snapshot(tmp_path, *, calendar=False):
    root = tmp_path / "snapshot"
    root.mkdir()
    names = ["input.xlsx", "odds.csv", "masters/race_levels.xlsx", "masters/場所_馬場_タイム.xlsx"]
    if calendar:
        names.append("calendar.txt")
    for name in names:
        path = root / name
        path.parent.mkdir(exist_ok=True)
        path.write_text("20260808\n", encoding="utf-8")
    write_json(root / "manifest.json", {"day": "20260809", "mode": "offline", "files": {n: sha256(root / n) for n in names}})
    return root


def test_s1_compare_cannot_replace_existing_json(tmp_path, workspace):
    tmp_path = tmp_path / "inputs"
    tmp_path.mkdir()
    left, right = tmp_path / "old.xlsx", tmp_path / "new.xlsx"
    sheets(left)
    sheets(right)
    output = workspace / "comparisons/manifest.json"
    output.parent.mkdir()
    output.write_text('{"protected":true}', encoding="utf-8")
    before = output.read_bytes()
    with pytest.raises((ValueError, FileExistsError)):
        compare(left, right, output, {"features": [], "feature_names": {}}, workspace=workspace, provenance={})
    assert output.read_bytes() == before


@pytest.mark.parametrize("ranks,predicted,reason", [((1, 2), False, "分類"), ((2, 1), True, "再計算")])
def test_s3_classification_and_reversed_order_rejected(tmp_path, ranks, predicted, reason):
    path = tmp_path / "prediction.xlsx"
    sheets(path, ranks=ranks, predicted=predicted)
    with pytest.raises(ValueError, match=reason):
        candidate.validate_population(path, path)


def test_a2_fixed_calendar_is_used_without_cli_option(tmp_path, monkeypatch, workspace):
    root = snapshot(tmp_path, calendar=True)
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    monkeypatch.setattr(pipeline, "run_pipeline", lambda **_: pytest.fail("固定カレンダーの非開催を無視した"))
    result = candidate.run(root, workspace / "runs", workspace=workspace)
    assert json.loads((result / "STATUS.json").read_text())["status"] == "NO_RACE"


def test_s2_final_status_failure_cannot_leave_ready(tmp_path, monkeypatch, workspace):
    root = snapshot(tmp_path)
    pipeline = importlib.import_module("1_keibayosou_pipeline")

    @dataclass
    class Result:
        count: int = 0

    def predict(**kwargs):
        Path(kwargs["OUT_EXCEL"]).write_bytes(b"fixture prediction")  # 終盤の公開順だけを検証する。

    monkeypatch.setattr(pipeline, "run_pipeline", predict)
    monkeypatch.setattr(candidate, "verify_candidate", lambda: {})
    monkeypatch.setattr(candidate, "complete_market_columns", lambda *args: None)
    monkeypatch.setattr(candidate, "complete_unsupported_races", lambda *args: Result())
    monkeypatch.setattr(candidate, "validate_population", lambda *args: {})
    monkeypatch.setattr(candidate, "validate_excel", lambda *args, **kwargs: Result())
    monkeypatch.setattr(candidate, "collect", lambda *args: {"runtime_id": "fixture", "code_hashes": {}})
    monkeypatch.setattr(candidate, "forward_audit", lambda: {})

    def fail_completed(path, payload):
        if path.name == "STATUS.json" and payload.get("status") == "COMPLETED":
            raise OSError("final status write failure")
        write_json(path, payload)

    monkeypatch.setattr(candidate, "write_json", fail_completed)
    with pytest.raises(OSError, match="final status"):
        candidate.run(root, workspace / "runs", workspace=workspace)
    run = next((workspace / "runs").iterdir())
    assert not (run / "READY.json").exists()


@pytest.mark.parametrize("target", ["production", "code", "snapshot", "outside"])
def test_s1_host_wrapper_rejects_unsafe_rw_mount(tmp_path, workspace, target):
    from nas.launch import checked_environment
    from nas.workspace import CODE_ROOT
    root = snapshot(tmp_path)
    paths = {"production": tmp_path / "production/data/output", "code": CODE_ROOT / "data/output",
             "snapshot": root / "output", "outside": tmp_path / "other"}
    with pytest.raises(ValueError):
        checked_environment(workspace, root, paths[target], tmp_path / "production", "a" * 40)


def test_s1_workspace_cannot_be_initialized_under_code_or_production(tmp_path):
    from nas.workspace import CODE_ROOT, initialize
    production = tmp_path / "production"
    production.mkdir()
    for target in (production / "data/output/candidate", CODE_ROOT / "unused-review-workspace"):
        with pytest.raises(ValueError):
            initialize(target, production)
        assert not target.exists()


def test_s1_compare_preserves_predictable_tmp_and_new_result(workspace):
    from nas.workspace import Workspace
    boundary = Workspace(workspace)
    target = workspace / "comparisons/new.json"
    target.parent.mkdir()
    sentinel = target.with_suffix(".tmp")
    sentinel.write_text("keep", encoding="utf-8")
    boundary.new_json(target, {"valid": True})
    with pytest.raises(FileExistsError):
        boundary.new_json(target, {"valid": False})
    assert json.loads(target.read_text())["valid"] is True
    assert sentinel.read_text() == "keep"


def test_a1_user_notes_cannot_claim_identical_inputs(tmp_path, workspace):
    folder = tmp_path / "inputs"
    folder.mkdir()
    left, right = folder / "left.xlsx", folder / "right.xlsx"
    sheets(left)
    sheets(right)
    result = compare(left, right, workspace / "comparisons/result.json", {"features": [], "feature_names": {}},
                     workspace=workspace, provenance={"model": "SAME", "implementation": "SAME", "data": "SAME"})
    assert result["attribution"]["data"] == "UNVERIFIED"
    assert result["attribution"]["model"] == "UNVERIFIED"


def test_a1_saved_input_mismatch_and_runtime_tampering(tmp_path):
    from nas.provenance import attribution
    from model_registry import normalized_weights_sha256
    left, right = tmp_path / "legacy", tmp_path / "candidate"
    left.mkdir()
    right.mkdir()
    for folder in (left, right):
        (folder / "prediction.xlsx").write_bytes(b"prediction")
    (left / "app").mkdir()
    (left / "app/code.py").write_bytes(b"# fixture source")
    (left / "yosou_py").mkdir()
    weight = "yosou_py/best_feature_weights_20260730.py"
    (left / weight).write_bytes(b"# fixture weight")
    (left / "raw_scores.json").write_text("[]", encoding="utf-8")
    weights = {"__default__": {"x": 1.0}}
    model = {"weight_file": weight, "file_sha256": sha256(left / weight), "effective_sha256": normalized_weights_sha256(weights)}
    write_json(left / "source_manifest.json", {"input_manifest_sha256": "a" * 64,
               "files": {"app/code.py": sha256(left / "app/code.py"), weight: sha256(left / weight)}})
    write_json(left / "runtime.json", {"prediction_sha256": sha256(left / "prediction.xlsx"), "input_manifest_sha256": "a" * 64,
               "model_identity": model, "weights": {"common": weights, "surface": {}}})
    write_json(left / "trace_manifest.json", {"prediction_sha256": sha256(left / "prediction.xlsx"), "raw_scores_sha256": sha256(left / "raw_scores.json"),
               "runtime_sha256": sha256(left / "runtime.json"), "source_manifest_sha256": sha256(left / "source_manifest.json")})
    write_json(right / "runtime.json", {"input_manifest_sha256": "b" * 64, "code_hashes": {"code.py": "c" * 64}})
    write_json(right / "manifest.json", {"files": {"prediction.xlsx": sha256(right / "prediction.xlsx"), "runtime.json": sha256(right / "runtime.json")}})
    result = attribution(left / "prediction.xlsx", right / "prediction.xlsx")
    assert result["data"] == "DIFFERENT"
    assert result["model"] == "UNVERIFIED"
    assert result["left"]["status"] == "VERIFIED_SAVED_RECORDS"
    (left / "runtime.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="固定SHA"):
        attribution(left / "prediction.xlsx", right / "prediction.xlsx")


def test_s3_missing_target_race_is_rejected(tmp_path):
    path = tmp_path / "prediction.xlsx"
    sheets(path)
    with pd.ExcelWriter(path, mode="a", if_sheet_exists="replace", engine="openpyxl") as writer:
        pd.DataFrame(columns=["rid_str", "馬番", "rank", "score_raw"]).to_excel(writer, sheet_name="TARGET", index=False)
    with pytest.raises(ValueError, match="分類"):
        candidate.validate_population(path, path)


def test_s3_excluded_race_cannot_keep_roi_ticket(tmp_path):
    path = tmp_path / "prediction.xlsx"
    sheets(path, predicted=False)
    with pd.ExcelWriter(path, mode="a", if_sheet_exists="replace", engine="openpyxl") as writer:
        pd.DataFrame(columns=["rid_str", "馬番", "rank", "score_raw"]).to_excel(writer, sheet_name="TARGET", index=False)
        pd.DataFrame({"レースID": ["R1"], "3連複1点目_馬番1": [1], "3連複1点目_馬番2": [2],
                      "3連複1点目_馬番3": [3], "3連複_金額": [100]}).to_excel(writer, sheet_name="回収率重視_買い目候補", index=False)
    with pytest.raises(ValueError, match="購入券"):
        candidate.validate_population(path, path)


def test_s1_legacy_replay_also_rejects_outside_workspace(tmp_path, workspace):
    from nas.legacy_replay import replay
    root = snapshot(tmp_path)
    with pytest.raises(ValueError, match="workspace"):
        replay(tmp_path / "production", root, tmp_path / "elsewhere", workspace=workspace)
    assert not (tmp_path / "elsewhere").exists()
