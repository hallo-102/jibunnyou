"""合成Excelを使い、予想・最適化の特徴量と現行値を比較する。"""

import importlib

import pandas as pd
import pytest

from tokutyouryou_keisann import features as optimizer

prediction = importlib.import_module("1_keibayosou_pipeline")
config = importlib.import_module("1_keibayosou_config")
shared = importlib.import_module("1_keibayosou_features")

RACEDAY = "20261105"
RACE_ID = "202605050101"


@pytest.fixture(params=["prediction", "optimizer"])
def calculate(request, tmp_path, monkeypatch):
    monkeypatch.setattr(optimizer, "_load_pipeline_master_data", lambda *_: (None, None))
    monkeypatch.setattr(optimizer, "load_odds_csv", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(optimizer, "_attach_dl_features", lambda merged, feat: feat)

    def calculate_with_history(history, *, now_date="2026/11/05", race_class="", cached=None):
        path = tmp_path / f"history_{RACEDAY}.xlsx"
        now = pd.DataFrame({
            "レースID": [RACE_ID], "馬番": [1], "馬名": ["馬A"], "頭数": [10],
            "場所": ["東京"], "コース": ["芝1600"], "クラス": [race_class], "日付": [now_date],
        })
        history = pd.DataFrame(history).assign(馬名="馬A")
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            now.to_excel(writer, sheet_name=config.NOW_SHEET, index=False)
            history.to_excel(writer, sheet_name=RACE_ID, index=False)
            if cached is not None:
                pd.DataFrame(
                    {"rid_str": [RACE_ID], "馬番": [1], **cached},
                ).to_excel(writer, sheet_name=config.HORSES_SHEET, index=False)

        def build(**kwargs):
            if request.param == "optimizer":
                return optimizer.build_features_from_one_file(str(path), **kwargs)
            return prediction.build_features_from_excel(str(path), raceday=RACEDAY, **kwargs)[1]

        return build(), build(validation=True)

    return calculate_with_history


def test_d01_dates_filter_and_sort(calculate):
    legacy, validation = calculate({
        "日付": ["2026/11/1", "2026/10/31", "2026/11/5", "2026/11/6", 2026112, "2026/1/12"],
        "着順": [1, 2, 7, 8, 9, 3],
    })
    assert legacy.iloc[0]["days_off"] == 5
    assert validation.iloc[0]["days_off"] == 4
    assert validation.iloc[0]["ta_n"] == 3
    assert validation.iloc[0]["avg_finish"] == 2


def test_d01_non_padded_current_day_and_unverified_history(calculate):
    legacy, validation = calculate(
        {"日付": ["2026/11/5", "2026-11-06", "2026/2/30"], "着順": [1, 2, 3]},
        now_date="2026/11/5",
        cached={"avg_finish": [7]},
    )
    assert legacy.empty
    assert validation.iloc[0]["ta_n"] == 0
    assert pd.isna(validation.iloc[0]["avg_finish"])


def test_d01_validation_requires_cutoff():
    with pytest.raises(ValueError, match="開催日"):
        shared._compute_horse_features_from_race_sheets(
            {}, pd.DataFrame({"rid_str": [], "馬名": []}), None, validation=True,
        )


@pytest.mark.parametrize(("laps", "legacy", "corrected"), [
    ("11.0-11.0-12.0-12.0-12.0-12.0", "slow", "fast"),
    ("12.0-12.0-12.0-11.0-11.0-12.0", "fast", "slow"),
    ("11.0-12.0-12.0-11.0-12.0-12.0", "mid", "mid"),
])
def test_d02_pace_direction(laps, legacy, corrected):
    assert shared._parse_pace_from_laps(laps) == legacy
    assert shared._parse_pace_from_laps(laps, validation=True) == corrected


def test_d02_pace_features(calculate):
    legacy, validation = calculate({
        "日付": ["2026/11/03", "2026/11/02", "2026/11/01"],
        "上り": [36.0, 34.0, 35.0],
        "ラップタイム": [
            "11.0-11.0-12.0-12.0-12.0-12.0",
            "12.0-12.0-12.0-11.0-11.0-12.0",
            "11.0-12.0-12.0-11.0-12.0-12.0",
        ],
    })
    assert legacy.iloc[0][["cond_pace_fast_last3f", "cond_pace_slow_last3f"]].tolist() == [34, 36]
    assert validation.iloc[0][["cond_pace_fast_last3f", "cond_pace_slow_last3f"]].tolist() == [36, 34]


@pytest.mark.parametrize(("race_class", "legacy", "corrected"), [
    ("GI", "G1", "G1"),
    ("GII", "G1", "G2"),
    ("GIII", "G1", "G3"),
    ("レース（ＧⅢ）", "G3", "G3"),
    ("Jpn2", "G2", "G2"),
    ("G10", "G1", ""),
    ("GIFT", "G1", ""),
])
def test_d03_grade_tokens(race_class, legacy, corrected):
    assert shared._parse_class_name_from_text(race_class) == legacy
    assert shared._parse_class_name_from_text(race_class, validation=True) == corrected


def test_d03_class_specific_finish_section(calculate):
    legacy, validation = calculate(
        {
            "日付": ["2026/11/03", "2026/11/02", "2026/11/01"],
            "レース名": ["レース(GI)", "レース(GII)", "レース(GIII)"],
            "上り": [33, 34, 38],
        },
        race_class="GII",
    )
    assert legacy.iloc[0]["last3f_class_diff"] == 0
    assert validation.iloc[0]["last3f_class_diff"] == 1


def test_d04_non_starters_and_unfinished_runs(calculate):
    legacy, validation = calculate({
        "日付": ["2026/11/04", "2026/11/03", "2026/11/02", "2026/11/01"],
        "着順": [1, "競走中止", "競走除外", "出走取消"],
    })
    assert legacy.iloc[0][["ta_n", "avg_finish"]].tolist() == [4, 1]
    assert validation.iloc[0][["ta_n", "avg_finish"]].tolist() == [2, 1]


def test_d04_unfinished_run_with_numeric_placeholder(calculate):
    legacy, validation = calculate({
        "日付": ["2026/11/03", "2026/11/02"],
        "着順": [1, 99],
        "競走状態": ["完走", "中止"],
    })
    assert legacy.iloc[0][["ta_n", "avg_finish"]].tolist() == [2, 50]
    assert validation.iloc[0][["ta_n", "avg_finish"]].tolist() == [2, 1]


def test_d05_recent_window_precedes_missing_value_removal(calculate):
    values = [9, 1, None, 3]
    legacy, validation = calculate({
        "日付": ["2026/11/01", "2026/11/04", "2026/11/03", "2026/11/02"],
        "距離": ["芝1600"] * 4,
        "着順": values,
        "人気": values,
        "上り": values,
        "タイム指数": values,
        "着差": values,
    })
    recent_columns = [
        "recent3_finish", "recent3_pop", "recent3_last3f", "recent3_time_idx",
        "recent_distance_time_idx",
    ]
    assert legacy.iloc[0][recent_columns].tolist() == pytest.approx([13 / 3] * 5)
    assert validation.iloc[0][recent_columns].tolist() == [2] * 5
    assert legacy.iloc[0]["recent_margin_improvement"] == 0
    assert validation.iloc[0]["recent_margin_improvement"] == pytest.approx(13 / 3 - 2)
