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
