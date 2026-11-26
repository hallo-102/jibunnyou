import importlib
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
    "race_name, race_class, reason, expected_no_history",
    [
        ("２歳新馬", None, "debut", True),
        (None, "3歳未勝利", "maiden", True),
        ("障害３歳以上未勝利", "未勝利", "obstacle", False),
        ("特別競走", "１勝クラス", None, False),
    ],
)
def test_membership_and_history_classification(
    audit, monkeypatch, tmp_path, race_name, race_class, reason, expected_no_history
):
    # Use a numeric Excel ID to exercise normalization before either comparison.
    now = pd.DataFrame({"レースID": [int(RACE)], "馬番": [1], "馬名": ["テスト馬"],
                        "レース名": [race_name], "クラス": [race_class]})
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
    assert history["expected_no_history"] == expected_no_history
    assert history["suspected_history_gaps"] == (not expected_no_history)
    assert history["missing"] == (not expected_no_history)
    assert history["issues"] == (not expected_no_history)
    stats = pd.DataFrame({"bets": [1]})
    assert audit.coverage_status(stats) == ("observed_with_limits" if expected_no_history else "partial")
    assert audit.coverage_status(empty) == "partial"
    report = "\n".join(audit.classification_report())
    assert f"正常除外 {int(bool(reason))}件" in report
    assert f"履歴欠落の疑い {int(not expected_no_history)}件" in report


def test_legacy_debut_without_history_sheet(audit, tmp_path):
    path = tmp_path / "input_20260809.xlsx"
    now = pd.DataFrame({"レースID": [int(RACE)], "日付": [20260809], "頭数": [12],
                        "レース名": ["２歳新馬"]})
    now.to_excel(path, sheet_name="今走レース情報", index=False)
    empty = pd.DataFrame()
    quality = []
    audit.input_book(path, DAY, pd.read_excel(path, sheet_name=None), now, empty, empty, empty, quality)
    assert audit.requirements["D-01", "history_rows"]["正常な履歴なし"] == 1
    assert audit.coverage_status(pd.DataFrame({"bets": [1]})) == "observed_with_limits", quality


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
