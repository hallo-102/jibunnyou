"""Issue #53 の合成 Excel による方式分離・A/B 一致・再開試験。"""

from collections import Counter
from datetime import date
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from model_registry import sha256_file
from tools import compare_period_models as period
from tools.ab_comparison_evidence import timestamp
from tools.ab_comparison_inputs import features, load_snapshot, score
from tools.compare_ab_models import FEAT_COLS, PROJECT_ROOT, baselines_from, read_json, save_json
from tools.period_comparison_features import METHODS, period_builders
from tools.period_comparison_inputs import AuditInputs, audited_result, day_features


NAMES = [f"試験馬{i}" for i in range(1, 9)]
PAYOUTS = {"単勝": {"1": 250}, "複勝": {"1": 150, "2": 160, "3": 170}, "3連複": {"1-2-3": 3000}}


def race_workbook(path, day, rid):
    now = pd.DataFrame({
        "レースID": rid, "馬番": range(1, 9), "馬名": NAMES, "頭数": 8,
        "場所": "東京", "コース": "芝1600", "クラス": "試験GII", "日付": day,
        "人気": [8, 7, 6, 5, 4, 3, 2, 1], "着順": [99] * 8,
    })
    past = pd.Timestamp(day) - pd.Timedelta(days=2)
    dates = [(past - pd.Timedelta(days=offset)).date() for offset in range(6)]
    history = pd.DataFrame([
        {"馬名": NAMES[0], "日付": text, "着順": finish, "競走状態": status,
         "レース名": race_class, "上り": last, "人気": 3, "タイム指数": time_index,
         "ラップタイム": laps, "場所": "東京", "芝・ダ": "芝", "距離": 1600}
        for text, finish, status, race_class, last, time_index, laps in (
            (dates[0].isoformat(), 1, "", "試験GII", 33, 90, "11.0-11.0-11.0-12.0-12.0-12.0"),
            (dates[1].isoformat(), None, "", "試験GI", None, None, "12.0-12.0-12.0-11.0-11.0-11.0"),
            (dates[2].isoformat(), 4, "中止", "試験GII", 36, 70, "12.0-12.0-12.0-11.0-11.0-11.0"),
            (dates[3].isoformat(), 5, "取消", "試験GI", 37, 50, "11.0-11.0-11.0-12.0-12.0-12.0"),
            (dates[4].isoformat(), 8, "", "試験GI", 39, 40, "12.0-12.0-12.0-11.0-11.0-11.0"),
            (f"{past.year}/1/11", 2, "", "試験GII", 35, 65, ""),
            (day, 99, "", "試験GII", 99, 99, ""),
        )
    ])
    cache = pd.DataFrame({"rid_str": rid, "馬番": range(1, 9), "master_rating": range(8, 0, -1)})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        now.to_excel(writer, sheet_name=features.NOW_SHEET, index=False)
        history.to_excel(writer, sheet_name=rid, index=False)
        cache.to_excel(writer, sheet_name=features.HORSES_SHEET, index=False)


def result_workbook(path, rid, *, pay=True):
    finish = pd.DataFrame({"レースID": rid, "馬名": NAMES, "馬番": range(1, 9), "着順": range(1, 9)})
    payout = pd.DataFrame({
        "レースID": rid, "払戻種別": ["単勝", "複勝", "複勝", "複勝", "3連複"],
        "組番": ["1", "1", "2", "3", "1-2-3"], "払戻金": [250, 150, 160, 170, 3000],
    })
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        finish.to_excel(writer, sheet_name="着順", index=False)
        if pay:
            payout.to_excel(writer, sheet_name="払戻", index=False)


@pytest.fixture
def audit(tmp_path):
    days = []
    combinations = [
        ("2025-06-14", "B", "C", True), ("2026-07-09", "A", "A", True),
        ("2026-08-09", "A", "B", True), ("2026-08-15", "A", "C", True),
        ("2026-08-16", "A", "unverified", True), ("2026-10-23", "A", "A", False),
        ("2026-10-24", "C", "A", True), ("2026-11-23", "D", "unverified", True),
    ]
    for i, (day, left, right, payout) in enumerate(combinations, 1):
        rid = f"2026050501{i:02}"
        races = tmp_path / f"馬の競走成績_{day}.xlsx"
        result = tmp_path / f"結果_{day}.xlsx"
        race_workbook(races, day, rid)
        result_workbook(result, rid, pay=payout)
        snapshot = {"input_evidence_grade": left}
        if left in "AB":
            snapshot.update({
                "input_evidence_source": f"NAS保存証跡 {day}", "start_at": f"{day}T12:00:00+09:00",
                "observed_at": f"{day}T11:55:00+09:00", "popularity": {str(i): i for i in range(1, 9)},
            })
        if left == "D":
            snapshot.update({
                "input_evidence_grade": "A", "input_evidence_source": "独立保存記録",
                "start_at": f"{day}T12:00:00+09:00", "observed_at": f"{day}T12:01:00+09:00",
            })
        proof = {"evidence_grade": right, "evidence_source": f"NAS確定結果 {day}"}
        if right == "C":
            proof.update({
                "evidence_source": f"JRA公式確定結果 {day}",
                "jra_official_result": {
                    "race_id": rid, "race_date": day, "finish_top3": [1, 2, 3], "payouts": PAYOUTS,
                },
            })
        if right == "unverified":
            proof = {}
        if right == "A":
            proof["finalized_at"] = f"{day}T12:30:00+09:00"
        if right == "B":
            proof["available_by_date"] = day
        days.append({
            "date": day, "workbook": races.name, "workbook_sha256": sha256_file(races),
            "results_workbook": result.name, "results_workbook_sha256": sha256_file(result),
            "races": {rid: {"input": snapshot, "result": proof}},
        })
    path = tmp_path / "inventory.json"
    save_json(path, {"schema_version": 1, "days": days})
    return path, days


@pytest.mark.parametrize(("input_grade", "result_grade", "cohort"), [
    ("B", "C", "reference"), ("A", "B", "formal"), ("A", "C", "formal"),
    ("A", "unverified", None), ("B", "unverified", None), ("C", "A", None), ("D", "A", None),
])
def test_result_cohort(input_grade, result_grade, cohort):
    record = {"input_grade": input_grade, "result": {"evidence": {"grade": result_grade}}}
    assert period.result_cohort(record) == cohort


def test_official_result_requires_matching_payout():
    day = date(2026, 8, 9)
    raw = {"race_id": "202605050101", "finish_top3": [1, 2, 3], "payouts": PAYOUTS}
    proof = {"evidence_grade": "C", "evidence_source": "JRA公式確定結果"}
    result, issues = audited_result(day, None, raw, proof, None)
    assert result["evidence"]["grade"] == "unverified"
    assert "unverified_result_evidence" in issues
    proof["jra_official_result"] = {**raw, "race_date": day.isoformat(),
                                    "payouts": {**PAYOUTS, "3連複": {"1-2-3": 3100}}}
    with pytest.raises(ValueError, match="JRA公式確定結果との照合に失敗"):
        audited_result(day, None, raw, proof, None)


@pytest.mark.parametrize(("observed_at", "grade"), [(None, "C"), ("12:00:00", "D"), ("12:01:00", "D")])
def test_input_timestamp_classification(audit, observed_at, grade):
    inventory, days = audit
    item = days[-1]
    rid, proof = next(iter(item["races"].items()))
    if observed_at is None:
        proof["input"].pop("observed_at")
    else:
        proof["input"]["observed_at"] = f"{item['date']}T{observed_at}+09:00"
    _, records, reasons = AuditInputs(inventory.parent).day(date.fromisoformat(item["date"]), item)
    assert records[rid]["input_grade"] == grade
    if grade == "D":
        assert records[rid]["observed_at"] == proof["input"]["observed_at"]
        assert reasons["input_at_or_after_start"] == 1


def test_period_isolated_feature_changes(audit):
    inventory, days = audit
    day = days[1]
    rid = next(iter(day["races"]))
    builders, metadata = period_builders(PROJECT_ROOT)
    _, races, reasons = day_features(AuditInputs(inventory.parent), date.fromisoformat(day["date"]), day, builders)
    assert reasons["history_same_or_future_rows"] == 1
    frames = {name: frame.set_index("馬番")[FEAT_COLS] for name, frame in races[rid].items()}
    legacy = frames["legacy"]
    changed = {}
    for name in ("M01", "M02", "M03", "M05", "M06", "M07", "M08"):
        different = ~np.isclose(legacy, frames[name], equal_nan=True, atol=1e-9, rtol=0)
        changed[name] = {column for column, col_changed in zip(FEAT_COLS, different.any(axis=0)) if col_changed}

    # 変更を実際に観測できるプローブを置き、各方式の非対象の列も比較する。
    assert frames["M01"].loc[1, "cond_pace_fast_last3f"] != legacy.loc[1, "cond_pace_fast_last3f"]
    assert frames["M02"].loc[1, "last3f_class_diff"] != legacy.loc[1, "last3f_class_diff"]
    assert frames["M05"].loc[1, "ta_n"] == legacy.loc[1, "ta_n"] + 1
    assert frames["M06"].loc[1, "ta_n"] == legacy.loc[1, "ta_n"] - 1
    assert frames["M07"].loc[1, "recent3_finish"] == 2.5
    assert frames["M08"].loc[1, "win_rate"] == pytest.approx(1 / 4)
    pace = {"cond_pace_fast_last3f", "cond_pace_slow_last3f"}
    grade = {"last3f_class_diff"}
    recent = {"recent3_finish", "recent3_last3f", "recent3_time_idx", "recent_distance_time_idx"}
    trends = {"recent_finish_trend", "recent_time_idx_maintained", "recent_time_idx_trend"}
    history = {"ta_n", "win_rate", "avg_score", "avg_finish", "avg_last3f", "avg_time_idx", "distance_time_idx",
               "distance_range_top3_rate", "distance_exact_top3_rate", "distance_range_reliability",
               "condition_match_reliability", "rating_now", "lap_match_bonus", "condition_change_score", "fast_score"}
    assert changed == {
        "M01": pace, "M02": grade, "M03": pace | grade, "M05": history | trends,
        "M06": (history - {"avg_finish"}) | trends | recent | {"cond_pace_slow_last3f"},
        "M07": recent | trends, "M08": {"win_rate", "avg_score"},
    }
    assert all(frames[name].loc[1, "master_rating"] == legacy.loc[1, "master_rating"] for name in changed)
    assert pd.isna(frames["corrected"].loc[1, "master_rating"])
    assert frames["corrected_with_cache"].loc[1, "master_rating"] == legacy.loc[1, "master_rating"]
    assert metadata["D-12"]["after_matches"] and not metadata["D-12"]["before_matches"]


def test_period_resume_and_ab_parity(audit, monkeypatch):
    inventory, days = audit
    output = inventory.parent / "out"
    count = Counter()
    save_features, score_day = period.save_features, period.score_day

    def fail_feature(path, inputs, day, item, builders):
        if day.isoformat() == days[1]["date"]:
            raise InterruptedError("features")
        count["features"] += 1
        save_features(path, inputs, day, item, builders)

    monkeypatch.setattr(period, "save_features", fail_feature)
    with pytest.raises(InterruptedError, match="features"):
        period.run(inventory, output)
    assert count["features"] == 1
    assert not (output / "features" / days[1]["date"]).exists()
    first_workbook = inventory.parent / days[0]["workbook"]
    first_workbook.rename(first_workbook.with_suffix(".off"))

    def count_features(*args):
        count["features"] += 1
        save_features(*args)

    def fail_score(path, features_path, day, stage, weights, assessment):
        if day.isoformat() == days[1]["date"]:
            raise InterruptedError("scores")
        count["scores"] += 1
        score_day(path, features_path, day, stage, weights, assessment)

    monkeypatch.setattr(period, "save_features", count_features)
    monkeypatch.setattr(period, "score_day", fail_score)
    with pytest.raises(InterruptedError, match="scores"):
        period.run(inventory, output)
    assert count == {"features": len(days), "scores": 1}
    assert read_json(output / "coverage.json")["totals"] == {
        "inventory_races": 8, "feature_races": 7, "finish_and_ticket_payout_matched": 6,
        "all_reported_payouts_matched": 6, "formal_revenue_races": 3, "reference_revenue_races": 1,
    }

    def count_scores(*args):
        count["scores"] += 1
        score_day(*args)

    monkeypatch.setattr(period, "score_day", count_scores)
    manifest = period.run(inventory, output)
    assert count == {"features": len(days), "scores": len(days)}
    assert period.run(inventory, output) == read_json(output / "manifest.json") == manifest
    assert count == {"features": len(days), "scores": len(days)}
    assert manifest["purchase"]["type"] == "simulated_tickets"
    assert manifest["forward_status"] == "previously_evaluated_reconciliation"
    rows = read_json(output / "races.json")
    assert len(rows) == len({(r["race_id"], r["method"]) for r in rows}) == 7 * len(period.LABELS)
    comparison = read_json(output / "comparison.json")
    formal = next(r for r in comparison if (r["method"], r["stage"], r["cohort"]) == ("M00", "validation", "formal"))
    assert formal["all_purchase_races"] == 3
    assert formal["all_investment_yen"] == 3000
    assert formal["win_hit_sample"] == 4
    reference_train = next(r for r in comparison if (r["method"], r["stage"], r["cohort"]) == ("M00", "train", "reference"))
    assert reference_train["all_settled_races"] == 1
    assert all(r["ticket_payout"] is None for r in rows
               if r["input_evidence_grade"] == "C" or r["result_evidence_grade"] == "unverified")
    assert not any(r["date"] == days[-1]["date"] for r in rows)
    assert {r["method"] for r in rows if not r["diagnostic_only"]} == METHODS.keys()
    for label in ("corrected_with_cache", "legacy_no_cache"):
        diagnostic = next(r for r in comparison if (r["method"], r["stage"], r["cohort"])
                          == (label, "validation", "formal"))
        assert diagnostic["diagnostic_only"]
        assert diagnostic["all_settled_races"] == formal["all_settled_races"]
    diagnostics = read_json(output / "cache_diagnostics.json")
    assert {(row["candidate"], row["reference"]) for row in diagnostics} == set(period.CACHE_PAIRS.values())

    day = next(day for day in days if day["date"] == "2026-08-09")
    rid, proof = next(iter(day["races"].items()))
    snap = {"race_id": rid, "workbook": day["workbook"], "workbook_sha256": day["workbook_sha256"], **proof["input"]}
    load = load_snapshot(inventory.parent, snap, timestamp(snap["start_at"]), Counter(), {
        "legacy": (features.build_features_from_excel, False, True),
        "corrected": (features.build_features_from_excel, True, False),
    })
    weights = baselines_from({}, PROJECT_ROOT)["github"][0]
    for ab_label, method in (("legacy", "M00"), ("corrected", "M04")):
        expected = load[ab_label].reset_index(drop=True)
        actual = period.read_frame(output / "features" / day["date"] / f"{method}.json.gz")
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        expected_scores = score(expected, weights).sort_values("rank")[["rid_str", "馬番", "score_raw", "rank"]]
        actual_scores = period.read_frame(output / "scores" / day["date"] / f"{method}.json.gz")
        pd.testing.assert_frame_equal(actual_scores, expected_scores.reset_index(drop=True), check_exact=True)

    result = {"race_id": rid, "race_date": day["date"], **proof["result"], "finish_top3": [1, 2, 3],
              "payouts": PAYOUTS}
    for filename, payload in (("snap.json", [snap]), ("result.json", [result]), ("ab.json", {
        "schema_version": 2, "snapshots": "snap.json", "results": "result.json", "evaluation_date": day["date"],
    })):
        save_json(inventory.parent / filename, payload)
    completed = subprocess.run(
        [sys.executable, "-m", "tools.compare_ab_models", "--config", str(inventory.parent / "ab.json"),
         "--output", str(inventory.parent / "ab")], cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    ab_rows = read_json(inventory.parent / "ab" / "races.json")
    for ab_label, method in (("A", "M00"), ("B", "M04")):
        ab = next(row for row in ab_rows if row["method"] == ab_label)
        actual = next(r for r in rows if r["race_id"] == rid and r["method"] == method)
        for field in ("top5", "purchase_decision", "win_hit", "place_hit", "winner_in_top5",
                      "ticket_investment", "ticket_payout"):
            assert actual[field] == ab[field]
