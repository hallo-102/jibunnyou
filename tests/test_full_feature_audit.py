import importlib
import json
import sys
from io import StringIO

import pandas as pd
import pytest

from scripts import full_feature_audit as audit_module


DAY = pd.Timestamp("2026-08-09")
RACE = "202608090101"


@pytest.fixture
def audit():
    features = importlib.import_module("1_keibayosou_features")
    config = importlib.import_module("1_keibayosou_config")
    instance = audit_module.LiveAudit(StringIO(), StringIO(), [], features, config)
    instance.export_horses = 1
    return instance


@pytest.mark.parametrize(
    "race_name, race_class, reason, starts, history_category",
    [
        ("２歳新馬", None, "debut", None, "expected_no_history"),
        (None, "3歳未勝利", "maiden", 0, "expected_no_history"),
        (None, "3歳未勝利", "maiden", 2, "confirmed_history_gaps"),
        (None, "3歳未勝利", "maiden", None, "unverified_prior_starts"),
        ("障害３歳以上未勝利", "未勝利", "obstacle", None, "unverified_prior_starts"),
        ("特別競走", "１勝クラス", None, None, "suspected_history_gaps"),
    ],
)
def test_membership_and_history_classification(
    audit, monkeypatch, tmp_path, race_name, race_class, reason, starts, history_category
):
    # Use a numeric Excel ID to exercise normalization before either comparison.
    now = pd.DataFrame({"レースID": [int(RACE)], "馬番": [1], "馬名": ["テスト馬"],
                        "レース名": [race_name], "クラス": [race_class], "通算出走数": [starts]})
    optimized = pd.DataFrame({"rid_str": [RACE], "馬番": [1], "馬名": ["テスト馬"]})
    empty = pd.DataFrame()
    monkeypatch.setattr(audit_module, "run_feature_paths",
                        lambda *args: (empty, empty, optimized, {"optimizer_common": empty}))
    audit.input_book(tmp_path / "input.xlsx", DAY, {}, now, empty, empty, empty, [])
    audit.tabulate(tmp_path)
    summary = pd.read_csv(tmp_path / "real_audit_summary.csv").set_index(["requirement", "metric"])
    membership = summary.loc[("D-12", "wrapper_horse_membership")]
    assert membership["expected_exclusions"] == bool(reason)
    assert membership["membership_needs_review"] == (reason is None)
    assert membership["issues"] == (reason is None)
    assert membership["expected_other"] == 0
    if reason:
        assert membership[f"expected_{reason}"] == 1
    assert summary.loc[("D-12", "wrapper_rows"), "issues"] == (reason is None)
    history = summary.loc[("D-01", "history_rows")]
    for category in ("expected_no_history", "confirmed_history_gaps", "unverified_prior_starts", "suspected_history_gaps"):
        assert history[category] == (category == history_category)
    missing = history_category in {"confirmed_history_gaps", "suspected_history_gaps"}
    assert history["missing"] == missing
    assert history["issues"] == missing
    assert history["unverified"] == (history_category == "unverified_prior_starts")
    assert audit.coverage_status() == ("observed_with_limits" if history_category == "expected_no_history" else "partial")
    report = "\n".join(audit.classification_report())
    assert f"正常除外 {int(bool(reason))}件" in report
    assert f"履歴欠落 {int(history_category == 'confirmed_history_gaps')}件" in report
    assert f"出走経験未確認 {int(history_category == 'unverified_prior_starts')}件" in report


def test_legacy_debut_without_history_sheet(audit, tmp_path):
    path = tmp_path / "input_20260809.xlsx"
    now = pd.DataFrame({"レースID": [int(RACE)], "日付": [20260809], "頭数": [12],
                        "レース名": ["２歳新馬"]})
    now.to_excel(path, sheet_name="今走レース情報", index=False)
    empty = pd.DataFrame()
    quality = []
    audit.input_book(path, DAY, pd.read_excel(path, sheet_name=None), now, empty, empty, empty, quality)
    assert audit.requirements["D-01", "history_rows"]["正常な履歴なし"] == 1
    assert audit.coverage_status() == "observed_with_limits", quality


def test_production_only_membership_is_reviewed(audit, monkeypatch, tmp_path):
    now = pd.DataFrame({"レースID": [int(RACE)], "馬番": [1], "馬名": ["テスト馬"], "レース名": ["新馬"]})
    production = pd.DataFrame({"rid_str": [RACE], "馬番": [1], "馬名": ["テスト馬"]})
    empty = pd.DataFrame()
    monkeypatch.setattr(audit_module, "run_feature_paths",
                        lambda *args: (empty, empty, empty, {"frame": production, "optimizer_common": empty}))
    monkeypatch.setattr(audit, "feature_table", lambda *args, **kwargs: None)
    audit.input_book(tmp_path / "input.xlsx", DAY, {}, now, empty, empty, empty, [])
    counts = audit.requirements["D-12", "wrapper_horse_membership"]
    assert counts["要調査"] == 1
    assert counts["正常除外"] == 0


@pytest.mark.parametrize("race_class, expected", [("新馬", True), ("１勝クラス", False)])
def test_unmatched_horse_in_existing_history_sheet(audit, monkeypatch, tmp_path, race_class, expected):
    now = pd.DataFrame({"レースID": [int(RACE)], "馬番": [1], "馬名": ["テスト馬"], "クラス": [race_class]})
    book = {RACE: pd.DataFrame({"馬名": ["別の馬"], "日付": [20260701]})}
    empty = pd.DataFrame()
    monkeypatch.setattr(audit_module, "run_feature_paths", lambda *args: (empty, empty, empty, {}))
    audit.input_book(tmp_path / "input.xlsx", DAY, book, now, empty, empty, empty, [])
    counts = audit.requirements["D-01", "history_rows"]
    assert counts["正常な履歴なし"] == expected
    assert counts["履歴欠落の疑い"] == (not expected)


@pytest.mark.parametrize(
    "source, asof, starts, expected_status",
    [
        ("ratings_history", "2026-08-01", 1, "履歴欠落"),
        ("ratings_history", "2026-08-09", 1, "出走経験未確認"),
        ("initial_rating", None, 0, "出走経験未確認"),
    ],
)
def test_maiden_with_no_matched_horse_uses_only_prior_master_evidence(
    audit, monkeypatch, tmp_path, source, asof, starts, expected_status
):
    now = pd.DataFrame({"レースID": [int(RACE)], "馬番": [1], "馬名": ["テスト馬"], "クラス": ["未勝利"]})
    book = {RACE: pd.DataFrame({"馬名": ["別の馬"], "日付": [20260701]})}
    levels = pd.DataFrame()
    levels.attrs["horse_ratings"] = pd.DataFrame({"name_norm": ["テスト馬"], "rating_source": [source],
                                                "rating_asof_date": [asof], "start_count": [starts]})
    empty = pd.DataFrame()
    monkeypatch.setattr(audit_module, "run_feature_paths", lambda *args: (empty, empty, empty, {"optimizer_common": empty}))
    audit.input_book(tmp_path / "input.xlsx", DAY, book, now, levels, empty, empty, [])
    assert audit.requirements["D-01", "history_rows"][expected_status] == 1


@pytest.mark.parametrize("revenue_status", ["not_provided", "read_error", "no_in_period_rows", "completed"])
def test_cli_reports_feature_and_revenue_status_independently(monkeypatch, tmp_path, capsys, revenue_status):
    output = tmp_path / "audit"
    argv = ["full_feature_audit.py", "--data-dir", str(tmp_path / "data"), "--output-dir", str(output)]
    if revenue_status != "not_provided":
        path = tmp_path / "bets.csv"
        argv.extend(["--settled-bets", str(path)])
        if revenue_status != "read_error":
            pd.DataFrame([{
                "race_id": RACE, "horse_number": 1,
                "date": "2026-08-09" if revenue_status == "completed" else "2024-01-01",
                "venue": "新潟", "surface": "芝", "distance": 1600, "class": "1勝",
                "going": "良", "popularity": 1, "reliability": "高", "bet_type": "単勝",
                "stake": 100, "payout": 200,
            }]).to_csv(path, index=False)

    def covered_workbook(args, names, add, audit, manifest, loaders):
        audit.input_horses = audit.recomputed_horses = audit.export_horses = 1
        return [], [], [], []

    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(sys, "path", sys.path.copy())
    monkeypatch.setattr(sys, "dont_write_bytecode", sys.dont_write_bytecode)
    monkeypatch.setattr(audit_module, "workbook_audit", covered_workbook)
    assert audit_module.main() == 0
    manifest = json.loads((output / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == manifest["feature_audit_status"] == "observed_with_limits"
    assert manifest["revenue_evaluation_status"] == revenue_status
    report = (output / "full_feature_audit.md").read_text(encoding="utf-8")
    assert "特徴量監査 `observed_with_limits`" in report
    assert f"収益評価 `{revenue_status}`" in report
    assert f"revenue_evaluation_status={revenue_status}" in capsys.readouterr().out
