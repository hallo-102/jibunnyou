"""合成Excelから4方式の最適化、照合、集計、出力まで実行する。"""

import json
import subprocess
import sys

import pandas as pd
import pytest
from datetime import date

from model_registry import sha256_file
from tools.ab_comparison_inputs import features
from tools.compare_ab_models import PROJECT_ROOT, fixed_candidates
from tools.ab_comparison_metrics import summarize


def test_ab_comparison_cli(tmp_path):
    records, results = [], []
    names = [f"試験馬{i}" for i in range(1, 7)]
    days = ["2025-06-14", "2026-06-30", "2026-08-09", "2026-10-23", "2026-10-24"]
    for i, day in enumerate(days, 1):
        rid = f"2026050501{i:02}"
        path = tmp_path / f"{rid}.xlsx"
        previous = pd.Timestamp(day) - pd.Timedelta(days=4)
        history = []
        for number, name in enumerate(names, 1):
            history.extend([
                {"馬名": name, "日付": f"{previous.year}/{previous.month}/{previous.day}", "着順": number,
                 "レース名": "試験GII", "上り": 32 + number, "ラップタイム": "11-11-12-12-12-12"},
                {"馬名": name, "日付": (previous - pd.Timedelta(days=10)).date().isoformat(), "着順": 1,
                 "レース名": "試験GI", "上り": 32, "ラップタイム": "12-12-12-11-11-12"},
                {"馬名": name, "日付": (previous - pd.Timedelta(days=12)).date().isoformat(),
                 "着順": "中止", "競走状態": "競走中止"},
                {"馬名": name, "日付": (previous - pd.Timedelta(days=13)).date().isoformat(), "着順": "取消"},
                {"馬名": name, "日付": day, "着順": 99},
            ])
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            pd.DataFrame({
                "レースID": rid, "馬番": range(1, 7), "馬名": names, "頭数": 6,
                "場所": "東京", "コース": "芝1600", "クラス": "試験GII",
                "人気": [6, 5, 4, 3, 2, 1], "着順": [99] * 6, "最終オッズ": [999] * 6,
            }).to_excel(writer, sheet_name=features.NOW_SHEET, index=False)
            pd.DataFrame(history).to_excel(writer, sheet_name=rid, index=False)
            pd.DataFrame({"rid_str": [rid] * 6, "馬番": range(1, 7),
                          "master_rating": [40, 30, 20, 10, 5, 0]}).to_excel(
                writer, sheet_name=features.HORSES_SHEET, index=False,
            )
        records.append({"race_id": rid, "workbook": path.name, "workbook_sha256": sha256_file(path),
                        "start_at": f"{day}T12:00:00+09:00", "observed_at": f"{day}T11:55:00+09:00",
                        "popularity": {str(n): n for n in range(1, 7)}})
        result = {"race_id": rid, "finalized_at": f"{day}T12:20:00+09:00", "finish_top3": [1, 2, 3],
                  "payouts": {"単勝": {"1": 250}, "複勝": {"1": 150, "2": 160},
                              "3連複": {"1-2-3": 3000 if i == 3 else 500}}}
        if i == 2:
            result["finalized_at"] = "2026-07-01T00:00:00+09:00"
        if i == 4:
            del result["payouts"]["3連複"]
        results.append(result)
    records.append({"race_id": "202605050199", "start_at": "2026-07-01T15:00:00+09:00",
                    "observed_at": "2026-07-01T14:55:00+09:00"})
    config = {"schema_version": 1, "snapshots": "snapshots.json", "results": "results.json",
              "optimizer_iterations": {"N_ITER_DEFAULT": 2, "N_ITER_PLACE": 2, "N_ITER_PLACE_SURFACE": 2}}
    for filename, payload in (("config.json", config), ("snapshots.json", records), ("results.json", results)):
        (tmp_path / filename).write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "out"
    completed = subprocess.run(
        [sys.executable, "-m", "tools.compare_ab_models", "--config", str(tmp_path / "config.json"),
         "--output", str(output)], cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr

    def read(name):
        return json.loads((output / f"{name}.json").read_text(encoding="utf-8"))

    summary = read("comparison")
    assert {(row["method"], row["stage"]) for row in summary} == {
        (method, stage) for method in "ABCD" for stage in ("validation", "forward")
    }
    valid = [row for row in summary if row["stage"] == "validation"]
    assert all(row["races"] == 2 and row["horses"] == 12 for row in valid)
    assert all(row["filtered_purchase_races"] == 2 and row["filtered_settled_races"] == 1 for row in valid)
    assert all(row["filtered_investment_yen"] == 1000 and row["filtered_roi_pct"] is not None for row in valid)
    assert all(row["win_roi_sample"] == 2 and row["place_roi_sample"] == 2 for row in valid)
    assert all(row["top5_slots_captured_sample"] == 2 for row in valid)
    assert len(pd.read_csv(output / "comparison.csv")) == 8
    assert {r["dimension"] for r in read("breakdown")} == {"month", "place", "distance", "race_class", "popularity_band"}

    manifest = read("manifest")
    modes = manifest["methods"]["github"]
    assert modes["A"]["weight_sha256"] == modes["B"]["weight_sha256"]
    for method in "CD":
        assert modes[method]["training_race_ids"] == [records[0]["race_id"]]
        assert modes[method]["weight_sha256"] == sha256_file(output / f"weights_{method}.json")
    assert manifest["diagnostics"]["main_parity_counts"] == {"equal": 5}
    assert manifest["diagnostics"]["variants"]["D-12"]["after_matches"]
    assert manifest["coverage"]["train"]["missing"]["history_same_or_future_rows"] == 12
    assert {"missing_workbook", "training_result_not_final_in_train_period", "missing_payout_3連複"} <= {
        row["reason"] for row in read("exclusions")
    }
    diagnostics = read("diagnostics")
    assert any(r["comparison"] == "corrected_cache_effect" and r["changed_feature_cells"].get("master_rating")
               for r in diagnostics)
    assert any(r["comparison"] == "single_D-04" and r["changed_feature_cells"].get("ta_n") for r in diagnostics)
    assert any(r["comparison"] == "single_D-09" and r["changed_feature_cells"].get("win_rate") for r in diagnostics)


    trial = tmp_path / "trial"
    trial.mkdir()
    snapshot = {**records[2], "workbook": str(tmp_path / records[2]["workbook"])}
    ledger = [{"baseline": "github", "method": "A", "race_id": snapshot["race_id"],
               "purchase_decision": "購入候補", "investment_yen": 400, "payout_yen": 725}]
    for filename, payload in (("snapshots.json", [snapshot]), ("results.json", [results[2]]),
                              ("actual.json", ledger)):
        (trial / filename).write_text(json.dumps(payload), encoding="utf-8")

    def single_run(name, **overrides):
        config_path = trial / f"{name}.json"
        config_path.write_text(json.dumps({
            "schema_version": 1, "snapshots": "snapshots.json", "results": "results.json",
            "evaluation_date": "2026-08-09", "operational_results": "actual.json", **overrides,
        }), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-m", "tools.compare_ab_models", "--config", str(config_path),
             "--output", str(trial / name)], cwd=PROJECT_ROOT, capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return trial / name

    old_output = output
    output = single_run("AB")
    single = read("manifest")
    assert single["weight_mode"] == "ab_only" and set(single["methods"]["github"]) == {"A", "B"}
    assert single["optimizer_config"] is None and not list(output.glob("weights_*.json"))
    assert single["validation_selection"]["status"] == "disabled_single_day"
    assert single["validation_selection"]["min_settled_races"] == 30
    assert "証明にはなりません" in single["notes"]["input_timestamp_evidence"]
    assert "完全再現を証明しません" in single["notes"]["production_reproduction"]
    assert "十分条件ではありません" in single["notes"]["selection_minimum"]
    box = read("comparison")
    assert {(r["method"], r["stage"]) for r in box} == {("A", "validation"), ("B", "validation")}
    assert box[0]["own_filtered_investment_yen"] == 1000
    assert box[0]["filtered_investment_yen"] == 1000
    actual_a, actual_b = read("operational_comparison")
    assert (actual_a["investment_yen"], actual_a["payout_yen"], actual_a["profit_yen"]) == (400, 725, 325)
    assert actual_a["roi_pct"] == 181.25
    assert actual_b["status"] == "no_matching_actual_purchase_records"
    assert "investment_yen" not in actual_b

    source_manifest = old_output / "manifest.json"
    output = single_run("ABCD", weight_mode="fixed", candidate_manifest=str(source_manifest))
    assert set(read("manifest")["methods"]["github"]) == set("ABCD")
    assert read("manifest")["methods"]["github"]["C"]["training_race_ids"] == [records[0]["race_id"]]
    assert read("manifest")["methods"]["github"]["D"]["weight_sha256"] == modes["D"]["weight_sha256"]
    assert not list(output.glob("optimizer_*.csv"))
    with pytest.raises(ValueError, match="評価より前"):
        fixed_candidates(source_manifest, "github", date(2026, 6, 30), set())
    with pytest.raises(ValueError, match="評価より前"):
        fixed_candidates(source_manifest, "github", date(2026, 8, 9), {records[0]["race_id"]})


def test_method_specific_purchases_are_distinct_from_common_box():
    row = dict(race_id="202605050101", horses=6, win_hit=True, place_hit=True,
               winner_in_top5=True, top5_slots_captured=3, top5_complete=True,
               win_payout=150, place_payout=110, ticket_investment=1000, ticket_payout=3000,
               purchase_decision="購入候補", common_purchase=False)
    result = summarize([row], 100, 3)
    assert result["filtered_purchase_races"] == 0
    assert result["own_filtered_purchase_races"] == 1
    assert result["own_filtered_investment_yen"] == 1000
    assert result["own_filtered_payout_yen"] == 3000
    assert result["own_filtered_profit_yen"] == 2000
    assert result["own_filtered_roi_pct"] == 300
