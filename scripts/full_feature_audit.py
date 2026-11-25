#!/usr/bin/env python3
"""Read-only production feature audit. Run with the repository's Python environment."""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import importlib
from importlib.metadata import PackageNotFoundError, version
import json
import math
import re
import sys
import unicodedata
from unittest.mock import patch
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "outputs/full_feature_audit_20261125"
START = pd.Timestamp("2025-06-14")
END = {"input": pd.Timestamp("2026-11-01"), "output": pd.Timestamp("2026-10-25")}
DIMENSIONS = ["venue", "surface", "distance", "class", "going", "popularity_band", "reliability"]
CONDITION_COLUMNS = ["dimension", "value", "bet_type", "bets", "stake", "payout", "profit", "roi_pct"]


def audit_date(value):
    if isinstance(value, (date, datetime)) and pd.notna(value):
        return pd.Timestamp(value.date() if isinstance(value, datetime) else value)
    text = str(value).strip()
    if re.fullmatch(r"\d{8}(?:\.0)?", text):
        return pd.to_datetime(text[:8], format="%Y%m%d", errors="coerce")
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}(?: 00:00:00)?", text):
        return pd.to_datetime(text, errors="coerce")
    return pd.NaT


def read_weights(path):
    names = {"FEATURE_WEIGHTS", "FEATURE_WEIGHTS_BY_PLACE_SURFACE"}
    mappings = {}
    for node in ast.parse(path.read_text(encoding="utf-8-sig")).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in names:
                    mappings.update(ast.literal_eval(node.value))
    return mappings


def production_probes(f, loaders, add):
    rid = "202611050101"
    now = pd.DataFrame({"rid_str": [rid], "馬番": [1], "馬名": ["監査馬"],
                        "頭数": [16], "場所": ["東京"], "コース": ["芝1600"],
                        "クラス": ["G1"], "馬場": ["良"]})

    def compute(rows, levels=None, raceday="20261105"):
        history = pd.DataFrame(rows).assign(**{"馬名": "監査馬"})
        return f._compute_horse_features_from_race_sheets(
            {rid: history}, now, levels, raceday=raceday
        ).iloc[0]

    dates = ["2026/11/1", "2026/1/8", "2026/10/31", "2026/11/5",
             "2026/11/05", "2026/11/06", "invalid"]
    date_result = compute({"日付": dates, "着順": list(range(1, 8))})
    add("D-01", "過去走採用件数", date_result.ta_n, 3,
        "7行中、有効な過去3行。同日2・未来1・不明1は対象外")
    add("D-01", "最後の出走からの日数", date_result.days_off, 4,
        "採用された非ゼロ埋め11月1日を並べ替え用日付ヘルパーで再解析")
    ambiguous = compute({"日付": ["2026/1/12"]}, raceday="20260201")
    add("D-01", "区切り除去で曖昧になる過去走の採用件数", ambiguous.ta_n, 1,
        "1月12日の区切りを除くと2026112となり、11月2日と解釈され得る")
    leaked = compute({"日付": ["2026/12/3"]}, raceday="20260301")
    add("D-01", "非ゼロ埋め未来日付の混入件数", leaked.ta_n, 0,
        "非ゼロ埋め12月3日を開催日3月1日で検査")
    add("D-01", "同日・未来の混入件数", compute({"日付": dates[3:6]}).ta_n, 0,
        "開催日指定の同日2行・翌日1行")
    add("D-01", "非ゼロ埋め日付の解析", f._parse_yyyymmdd("2026/11/5"), "2026-11-05",
        "特徴量の日付ヘルパー")
    pace_cases = [(34, 36, "fast"), (35, 35, "mid"), (36, 34, "slow")]
    for early, late, expected in pace_cases:
        laps = f"11.0-11.0-{early-22}.0-11.0-11.0-{late-22}.0"
        add("D-02", f"前半/後半 {early}/{late}", f._parse_pace_from_laps(laps), expected, "前後半各3F")
    for label, expected in [("GI", "G1"), ("GII", "G2"), ("GIII", "G3")]:
        add("D-03", label, f._parse_class_name_from_text(label), expected, "ASCII表記")

    history = {"日付": ["2026/10/30", "2026/10/29", "2026/10/28", "2026/10/27"],
               "着順": [1, "除外", "中止", "取消"]}
    actual = compute(history)
    add("D-04", "過去走数", actual.ta_n, 2, "完走1・中止1を出走として計数")
    add("D-04", "平均着順", actual.avg_finish, 1.0, "数値着順1件のみ。中止への数値付与は仕様未確定")
    add("D-09", "勝率", actual.win_rate, 1.0, "1勝÷有効着順1件")
    recent = compute({**history, "着順": [1, None, 3, 9]})
    add("D-05", "直近3走の平均着順", recent.recent3_finish, 2.0,
        "直近3行 [1, 欠損, 3]。4走目は9")
    for metric in ("人気", "上り", "タイム指数", "着差"):
        values = pd.Series([1.0, None, 3.0, 9.0])
        add("D-05", f"直近3走の{metric}に使う共通平均", f._safe_recent_mean(values), 2.0,
            "直近3行 [1, 欠損, 3]。4走目は9")
    for size in (8, 16, 18):
        fields = {"日付": ["2026/10/30"], "通過": ["8-8"]}
        known = compute({**fields, "頭数": [size]})
        missing = compute(fields)
        add("D-06", f"過去{size}頭の脚質コードを頭数欠損で再計算",
            missing.running_style_code, known.running_style_code, "過去頭数ありとなしの脚質分類")
        add("D-06", f"過去{size}頭の8番手を頭数欠損で再計算",
            missing.early_position_rate, known.early_position_rate, "欠損時は今回の16頭で代用")
    dist = compute({**history, "着順": [1, 7, 8, 9],
                    "距離": ["芝1600", "芝1800", "芝1800", "芝1800"], "場所": ["東京"] * 4})
    add("D-07", "平均との差 m", dist.dist_diff, 150.0, "同距離の勝利1件がある合成履歴")
    add("D-07", "同場同距離の3着内率", dist.distance_exact_top3_rate, 1.0, "現行コードに独立指標あり")

    ids = [str(202610010001 + idx) for idx in range(6)]
    values = pd.Series([20, 30, 40, 50, 60, 100])
    finishes = pd.Series([1, 4, 2, 5, 6, 8])
    levels = pd.DataFrame({"rid_str": ids, "race_level": values})
    level_run = compute({"日付": pd.date_range("2026-10-25", periods=6)[::-1],
                         "race_id": ids, "着順": finishes}, levels)
    add("D-08", "上位5件平均", level_run.past_racelevel_top5_avg3, 56.0, "古い最高値100を含む")
    alternatives = {
        "直近3走平均": values.head(3).mean(),
        "直近5走平均": values.head(5).mean(),
        "新しさによる線形加重平均": sum(values * pd.Series([6, 5, 4, 3, 2, 1])) / 21,
        "本人着順による加重平均": sum(values / finishes) / sum(1 / finishes),
    }
    for name, value in alternatives.items():
        add("D-08", name, value, None, "合成履歴で現行上位5件平均56と比較。採用判定には時系列再検証が必要")

    master = {
        "horses": pd.DataFrame({"id": [1, 2], "name": ["監査馬", "比較馬"], "initial_rating": [80, 80]}),
        "races": pd.DataFrame({"race_id": list("abcd"),
                               "date": ["2026/10/28", "2026/10/29", "2026/11/05", "2026/11/06"]}),
        "ratings_history": pd.DataFrame({"race_id": list("abcda"), "horse_id": [1, 1, 1, 1, 2],
                                         "post_rating": [100, 120, 900, 999, 80]}),
    }
    ratings, excluded = loaders._build_historical_rating_master(master, "20261105")
    add("D-10", "当日・未来rating除外行", excluded, 2, "rating履歴から除外")
    add("D-10", "採用rating", ratings.iloc[0].rating, 120.0, "最新の開催日前rating")
    add("D-10", "信頼度", ratings.iloc[0].rating_confidence, 0.55, "0.25 + 0.15×2走")
    with TemporaryDirectory(prefix="full-feature-audit-") as directory:
        path = Path(directory) / "race_levels.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, frame in master.items():
                frame.to_excel(writer, sheet_name=name, index=False)
            master["ratings_history"][["race_id", "horse_id"]].to_excel(writer, sheet_name="entries", index=False)
            pd.DataFrame({"race_id": list("abcd"), "race_level_score": [10.5, 20.5, 30.5, 40.5]}).to_excel(
                writer, sheet_name="race_levels", index=False)
        try:
            level_frame = loaders.load_race_levels(str(path), raceday="20261105")
        except TypeError as exc:
            add("D-10", "レースレベルExcelの読込", type(exc).__name__, 2,
                f"pandas {pd.__version__}。本番ローダーで失敗。補正計算は下記の個別呼び出しで確認")
            level_frame = pd.DataFrame()
            names = master["horses"][["id", "name"]].set_axis(["horse_id", "horse_name"], axis=1)
            level_frame.attrs["horse_ratings"] = names.merge(ratings, on="horse_id", how="left")
        else:
            add("D-10", "レースレベルExcelの読込", len(level_frame), 2, "4行のうち当日・翌日を除外")
    field = pd.DataFrame({"馬名": ["監査馬", "比較馬"], "rid_str": [rid, rid]})
    attached = f._attach_master_rating_features(field, level_frame)
    add("D-10", "信頼度補正済み相対rating", attached.iloc[0].master_rating_confidence_adjusted,
        11.0, "(120−100)×0.55")

    add("D-11", "先行圧 4頭・脚質既知8頭・全16頭", f._calc_front_pressure(4, 8, 16), 0.85,
        "先行圧1.0に脚質カバレッジ補正0.85")
    add("D-11", "位置上昇率 8→4番手・16頭", f._pass_position_rates("8-4", 16)[2], 0.25,
        "(8−4)÷16")
    context = pd.DataFrame({"__place__": ["東京", "中山"], "__surface__": ["芝", "芝"],
                            "__dist__": [1600, 1600], "__class__": ["G1", "G1"],
                            "__last3f__": [34, 38], "__time_idx__": [90, 70]})
    result = f._calc_contextual_last3f_features(context, "東京", "芝", 1600, "G1")
    add("D-11", "上がり3F補正", result["last3f_context_value"], 2.0, "全体平均36−条件一致平均34")
    add("D-11", "タイム指数補正", result["time_idx_context_value"], 10.0, "条件一致平均90−全体平均80")
    finish = compute({**history, "着順": [1, 8, None, "中止"], "頭数": [8, 8, 8, 8]})
    add("D-11", "頭数補正平均着順", finish.field_size_adjusted_finish, 0.5, "8頭の1着と8着の平均")
    return pd.DataFrame([recent, dist, level_run, finish])


def finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def numeric_series(values):
    return pd.to_numeric(values, errors="coerce").map(finite).astype(float)


def difference(actual, expected):
    if pd.isna(actual) or pd.isna(expected):
        return "未検証", None
    if isinstance(actual, bool) or isinstance(expected, bool):
        return ("一致" if actual == expected else "差異"), None
    if isinstance(actual, str) or isinstance(expected, str):
        return ("一致" if actual == expected else "差異"), None
    delta = float(actual) - float(expected)
    return ("一致" if math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9)
            else "差異"), delta


def expected_class(value):
    text = unicodedata.normalize("NFKC", str(value)).upper()
    grade = re.search(r"(?<![A-Z])(?:JPN|G)(III|II|I|[123])(?![A-Z0-9])", text)
    if grade:
        return "G" + {"I": "1", "II": "2", "III": "3"}.get(grade[1], grade[1])
    return None


def expected_pace(value):
    parts = re.split(r"\s*[-−－–]\s*", str(value).strip())
    if len(parts) < 6 or any(not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", part) or float(part) <= 0 for part in parts):
        return None
    laps = list(map(Decimal, parts))
    gap = sum(laps[:3]) - sum(laps[-3:])
    return "fast" if gap <= Decimal("-0.6") else "slow" if gap >= Decimal("0.6") else "mid"


def history_checks(f, frame, entries, day, levels, emit):
    """Compare the production history selection with calendar- and start-based references."""
    column = lambda *names: f._pick_col(frame, list(names))
    name_key = column("馬名")
    day_key = column("日付")
    finish_key = column("着順", "着 順")
    field_key = column("頭数", "頭 数", "出走頭数")
    pass_key = f._pick_value_pattern_col(
        frame, ["通過", "通過順位", "通過順", "コーナー 通過順"], r"^\d{1,2}(?:-\d{1,2}){1,4}$")
    class_key = column("レース名", "クラス", "条件")
    lap_key = column("ラップタイム")
    past_rid_key = column("race_id", "レースID")
    dates = frame[day_key] if day_key else pd.Series(None, index=frame.index, dtype=object)
    expected_dates = dates.map(audit_date)
    production_dates = pd.to_datetime(
        dates.astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8],
        format="%Y%m%d", errors="coerce")
    sort_dates = dates.map(f._parse_yyyymmdd)
    if name_key is None:
        for entry in entries:
            emit(entry, "D-01", "history_horse_key", None, None, "過去走シートの馬名列なし")
        return
    names = frame[name_key].astype(str).str.strip()
    grouped = names.groupby(names).groups
    compact = names.str.replace(r"\s+", "", regex=True)
    fallback = compact.groupby(compact).groups

    for entry in entries:
        horse = entry["horse"]
        indices = grouped.get(horse, fallback.get(re.sub(r"\s+", "", horse), pd.Index([])))
        rows = frame.loc[indices]
        if rows.empty:
            emit(entry, "D-01", "history_rows", None, None, "今走馬に対応する過去走行なし")
            continue
        for index, row in rows.iterrows():
            item = {**entry, "history_row": index + 2, "past_race_id": row.get(past_rid_key),
                    "source_value": dates.at[index]}
            expected_date = expected_dates.at[index]
            filter_date = production_dates.at[index]
            sort_date = sort_dates.at[index]
            emit(item, "D-01", "date_parsing", display_date(filter_date), display_date(expected_date), "過去走フィルターの日付")
            emit(item, "D-01", "include_before_raceday", pd.notna(filter_date) and filter_date < day,
                 expected_date < day if pd.notna(expected_date) else None, "開催日より前の行を採用")
            if pd.notna(filter_date) and filter_date < day:
                emit(item, "D-01", "date_for_sorting", display_date(sort_date), display_date(expected_date),
                     "採用行の並べ替え用日付")
            if pd.isna(expected_date) or expected_date >= day:
                continue
            if lap_key:
                lap = row[lap_key]
                emit({**item, "source_value": lap}, "D-02", "pace", f._parse_pace_from_laps(lap) or None,
                     expected_pace(lap), "前半が0.6秒以上速い場合fast、遅い場合slow")
            else:
                emit(item, "D-02", "pace", None, None, "ラップ列なし")
            cls = row.get(class_key)
            expected = expected_class(cls)
            actual = f._parse_class_name_from_text(cls) or None
            # Non-graded conditions are outside this check.
            if expected or actual in {"G1", "G2", "G3"} or pd.isna(cls):
                emit({**item, "source_value": cls}, "D-03", "graded_class", actual, expected,
                     "G/Jpnのローマ数字・算用数字を正規化")

        legacy = rows.loc[production_dates.loc[indices].lt(day)].sort_index()
        ordered = legacy.assign(audit_sort=sort_dates.loc[legacy.index]).sort_values(
            "audit_sort", ascending=False, kind="stable").drop(columns="audit_sort")
        chronological = rows.loc[expected_dates.loc[indices].lt(day)]
        chronological = chronological.assign(audit_sort=expected_dates.loc[chronological.index]).sort_values(
            "audit_sort", ascending=False, kind="stable").drop(columns="audit_sort")
        if chronological.empty and legacy.empty:
            for code, metric in (("D-04", "starts"), ("D-05", "recent3_finish"), ("D-06", "running_style"),
                                 ("D-09", "win_rate")):
                emit(entry, code, metric, None, None, "開催日前の履歴なし")
            continue

        statuses = chronological[finish_key].astype(str) if finish_key else pd.Series("", index=chronological.index)
        non_starts = statuses.str.contains("除外|取消|取り消", regex=True)
        eligible = chronological.loc[~non_starts]
        unknown_date = bool(expected_dates.loc[indices].isna().any())
        trustworthy_starts = bool(finish_key and not unknown_date
                                  and chronological[finish_key].notna().all())
        emit(entry, "D-04", "starts", len(legacy), len(eligible) if trustworthy_starts else None,
             "除外・取消を除き、中止を含む。日付または着順欄が欠損すると期待件数は保留")
        legacy_finish = f._to_float_series(legacy[finish_key]) if finish_key else pd.Series(dtype=float)
        expected_finish = f._to_float_series(eligible[finish_key]) if finish_key else pd.Series(dtype=float)
        expected_finish = expected_finish.where(expected_finish.gt(0) & expected_finish.mod(1).eq(0))
        emit(entry, "D-04", "avg_finish_valid_finishes", legacy_finish.mean(), expected_finish.mean(),
             "数値着順の平均を比較。中止を数値に換算する方針は未確定")
        for label, pattern in (("excluded", "除外"), ("withdrawn", "取消|取り消"), ("did_not_finish", "中止")):
            matches = statuses.str.contains(pattern, regex=True)
            for idx in statuses[matches].index:
                included = idx in legacy.index
                emit({**entry, "history_row": idx + 2, "past_race_id": rows.loc[idx].get(past_rid_key),
                      "source_value": statuses.at[idx]}, "D-04", f"counts_as_start_{label}", included,
                     label == "did_not_finish", "中止を数値着順へ換算する方針は未定")

        for feature, aliases in {
            "recent3_finish": ("着順", "着 順"), "recent3_pop": ("人気", "人 気"),
            "recent3_last3f": ("上り", "上り3F", "上り３F", "後3F"),
            "recent3_time_idx": ("ﾀｲﾑ指数", "タイム指数", "ﾀｲﾑ 指数"),
        }.items():
            key = column(*aliases)
            actual = f._safe_recent_mean(f._to_float_series(ordered[key])) if key else None
            # Keep production's admitted and sorted rows here to isolate the window-order defect.
            expected = f._to_float_series(ordered[key].head(3)).mean() if key else None
            emit(entry, "D-05", feature, actual, expected,
                 "同一の本番履歴順で、先に3行を取った後に有効値を平均",
                 force_difference=bool(key and pd.notna(actual) and pd.isna(expected)))
        valid_finish = legacy_finish.where(legacy_finish.gt(0) & legacy_finish.mod(1).eq(0))
        emit(entry, "D-09", "win_rate", (legacy_finish == 1).mean() if legacy_finish.notna().any() else None,
             (valid_finish.dropna() == 1).mean() if valid_finish.notna().any() else None,
             "同一本番履歴。期待値の分母は正の整数着順")
        if pass_key:
            current_size = finite(entry.get("field_size"))
            production_sizes = (numeric_series(legacy[field_key]) if field_key
                                else pd.Series(current_size, index=legacy.index, dtype=float))
            ref_sizes = (numeric_series(legacy[field_key]) if field_key
                         else pd.Series(float("nan"), index=legacy.index))
            actual = f._build_running_style_profile(legacy[pass_key], production_sizes)
            expected = f._build_running_style_profile(legacy[pass_key], ref_sizes)
            for metric in ("running_style", "early_position_rate", "position_gain_rate"):
                emit(entry, "D-06", metric, actual[metric] if actual["running_style"] else None,
                     expected[metric] if expected["running_style"] else None,
                     "過去頭数のある行のみが比較基準。頭数不明の過去走の正しい脚質は未検証")
            for idx, passing in legacy[pass_key].items():
                size = ref_sizes.at[idx]
                current = f._pass_position_rates(passing, production_sizes.at[idx])
                reference = f._pass_position_rates(passing, size)
                item = {**entry, "history_row": idx + 2, "past_race_id": legacy.loc[idx].get(past_rid_key),
                        "source_value": passing}
                if pd.notna(passing) and re.fullmatch(r"\d{1,2}(?:-\d{1,2}){1,4}", str(passing)):
                    emit(item, "D-06", "current_field_size_substitution", bool(current is not None and not field_key),
                         False, "今回頭数を代用した件数。脚質の誤判定件数は過去頭数不明のため別途未検証")
                emit(item, "D-06", "past_field_size_position",
                     current[0] if current else None, reference[0] if reference else None,
                     f"過去頭数={display(size)}、今回頭数={display(current_size)}。不明時の代用値は観測")
        else:
            emit(entry, "D-06", "running_style", None, None, "通過順列なし")

        finish = valid_finish
        sizes = numeric_series(legacy[field_key]) if field_key else pd.Series(float("nan"), index=legacy.index)
        matches = legacy_finish.notna() & sizes.gt(1)
        production_adjusted = (1 - (legacy_finish[matches] - 1) / (sizes[matches] - 1)).clip(0, 1)
        admissible = matches & finish.le(sizes)
        reference = (1 - (finish[admissible] - 1) / (sizes[admissible] - 1))
        emit(entry, "D-11", "field_size_adjusted_finish", production_adjusted.mean(), reference.mean(),
             "期待値の分母は頭数内の有効着順。実時計の補正基準は別途必要")

        distance_key = column("距離")
        surface_key = column("芝・ダ", "芝ダ", "コース") or distance_key
        if distance_key and finish_key and entry.get("distance") and entry.get("surface"):
            distances = legacy[distance_key].map(f._parse_distance_m)
            mean_distance = numeric_series(distances).mean()
            same = distances.eq(entry["distance"]) & legacy[surface_key].map(f._parse_surface).eq(entry["surface"])
            same_finish = finish[same].dropna()
            place_key = column("場所", "競馬場", "場名", "開催")
            exact = same & legacy[place_key].map(f._normalize_place_text).eq(entry["venue"]) if place_key else None
            current = legacy_finish[exact].le(3).mean() if exact is not None and exact.any() else None
            emit(entry, "D-07", "same_distance_top3_rate", current, same_finish.le(3).mean(),
                 f"現行の同場同芝ダ同距離率と、同芝ダ同距離の有効着順 {len(same_finish)}件を比較。"
                 f"本番平均距離差={display(abs(entry['distance'] - mean_distance))}", hypothesis=True)
        else:
            emit(entry, "D-07", "same_distance_top3_rate", None, None, "距離・芝ダ・着順が必要", hypothesis=True)
        if past_rid_key and not levels.empty and {"rid_str", "race_level"} <= set(levels):
            level_map = levels.drop_duplicates("rid_str", keep="last").set_index("rid_str")["race_level"]
            current_keys = f._normalize_rid_series(ordered[past_rid_key])
            current = numeric_series(current_keys.map(level_map)).nlargest(5).mean()
            keys = f._normalize_rid_series(eligible[past_rid_key])
            available = numeric_series(keys.map(level_map))
            for limit in (3, 5):
                emit(entry, "D-08", f"recent{limit}_level", current, available.head(limit).mean(),
                     "現行のレベル上位5件平均と日付順の候補を比較", hypothesis=True)
            weights = pd.Series(range(len(available), 0, -1), index=available.index).where(available.notna())
            valid_places = expected_finish.where(available.notna()).dropna()
            emit(entry, "D-08", "recency_weighted_level", current,
                 (available * weights).sum() / weights.sum() if weights.sum() else None,
                 "日付順に線形加重", hypothesis=True)
            emit(entry, "D-08", "finish_weighted_level", current,
                 (available.loc[valid_places.index] / valid_places).sum() / (1 / valid_places).sum()
                 if not valid_places.empty else None, "本人着順の逆数で加重", hypothesis=True)
        else:
            emit(entry, "D-08", "recent3_level", None, None, "過去レースIDとレースレベルマスターが必要", hypothesis=True)


def display_date(value):
    return value.strftime("%Y-%m-%d") if pd.notna(value) else None



KEYS = ["rid_str", "馬番"]
DETAIL_COLUMNS = ["file", "date", "race_id", "horse_number", "horse", "history_row", "past_race_id",
                  "source_value", "requirement", "metric", "current", "expected", "difference",
                  "status", "missing", "evidence"]
FEATURE_COLUMNS = ["file", "date", "race_id", "horse_number", "horse", "feature", "stage", "venue",
                   "surface", "distance", "going", "class", "production_value", "optimizer_value",
                   "difference", "comparison", "anomaly", "weight", "legacy_raw_contribution",
                   "calculation_source"]
MISSING = float("nan")


def feature_sources(names):
    """Point each feature to its producer, including shared context calculations."""
    files = ["1_keibayosou_features.py", "1_keibayosou_pipeline.py", "tokutyouryou_keisann/features.py"]
    locations = defaultdict(set)
    for file in files:
        tree = ast.parse((ROOT / file).read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                candidates = (key for key, val in zip(node.keys, node.values)
                              if val and not isinstance(val, ast.Constant))
            elif isinstance(node, ast.Assign):
                candidates = (target.slice for target in node.targets if isinstance(target, ast.Subscript))
            else:
                continue
            for key in candidates:
                if isinstance(key, ast.Constant) and key.value in names:
                    locations[key.value].add((file, key.lineno))
    return {name: ", ".join(f"{file}:{line}" for file, line in sorted(locations[name]))
            or "動的生成・補完。対応する入力列と欠損状態を確認" for name in names}


def index_horses(frame):
    if frame.empty:
        return pd.DataFrame(index=pd.MultiIndex.from_tuples([], names=KEYS))
    if frame[KEYS].isna().any().any() or frame.duplicated(KEYS).any():
        raise ValueError("レースID・馬番の欠損または重複")
    return frame.set_index(KEYS)


class ScoringInputCaptured(Exception):
    """Stop the production path once its feature inputs reach scoring."""


def run_feature_paths(path, day, pipeline, optimizer, levels, base, odds):
    raceday = day.strftime("%Y%m%d")
    merged, common = pipeline.build_features_from_excel(str(path), levels, base, odds, raceday=raceday)
    captured = {}

    def capture(frame, place_map, surface_map, calc_fav_risk):
        captured.update(frame=frame, place=place_map, surface=surface_map, calculator=calc_fav_risk)
        raise ScoringInputCaptured

    # Supply the same in-memory inputs and stop before any production writer is reached.
    with (TemporaryDirectory(prefix="full-audit-pipeline-") as scratch,
          patch.object(pipeline, "load_race_levels", return_value=levels),
          patch.object(pipeline, "load_base_time", return_value=base),
          patch.object(pipeline, "load_odds_csv", return_value=odds),
          patch.object(pipeline, "build_features_from_excel", return_value=(merged.copy(), common.copy())),
          patch.object(pipeline, "write_features_to_excel"),
          patch.object(pipeline, "append_success_report"),
          patch.object(pipeline, "compute_scores_with_pipeline_logic", side_effect=capture)):
        try:
            pipeline.run_pipeline(str(path), str(Path(scratch) / "unwritten.xlsx"), RACEDAY=raceday, ENABLE_SHADOW=False)
        except ScoringInputCaptured:
            pass
        except Exception as exc:
            captured["production_error"] = f"{type(exc).__name__}: {exc}"

    def optimizer_common(source, level_frame, base_frame, odds_frame, *, raceday):
        result = optimizer_builder(source, level_frame, base_frame, odds_frame, raceday=day.strftime("%Y%m%d"))
        captured["optimizer_common"] = result[1].copy()
        return result

    optimizer_builder = optimizer.build_features_from_excel
    try:
        with (patch.object(optimizer, "_load_pipeline_master_data", return_value=(levels, base)),
              patch.object(optimizer, "load_odds_csv", return_value=odds),
              patch.object(optimizer, "build_features_from_excel", side_effect=optimizer_common),
              patch.object(optimizer, "_load_dl_model_cache", side_effect=RuntimeError("監査ではDL学習を実行しない"))):
            optimized = optimizer.build_features_from_one_file(str(path))
    except Exception as exc:
        optimized = pd.DataFrame()
        captured["optimizer_error"] = f"{type(exc).__name__}: {exc}"
    return merged, common, optimized, captured


class LiveAudit:
    def __init__(self, detail_file, feature_file, names, f, config):
        self.details = csv.DictWriter(detail_file, fieldnames=DETAIL_COLUMNS, lineterminator="\n")
        self.values = csv.DictWriter(feature_file, fieldnames=FEATURE_COLUMNS, lineterminator="\n")
        self.details.writeheader()
        self.values.writeheader()
        self.requirements = defaultdict(Counter)
        self.features = defaultdict(Counter)
        self.names = names
        self.f = f
        self.config = config
        self.pipeline = importlib.import_module("1_keibayosou_pipeline")
        self.optimizer = importlib.import_module("tokutyouryou_keisann.features")
        self.sources = feature_sources(set(names))
        self.input_horses = 0
        self.recomputed_horses = 0
        self.export_horses = 0
        self.scoring_horses = 0
        self.partial = False

    def emit(self, item, requirement, metric, actual, expected, evidence, *, hypothesis=False, force_difference=False):
        status, delta = difference(actual, expected)
        missing = bool(pd.isna(actual) or pd.isna(expected))
        if force_difference:
            status = "差異"
        if hypothesis:
            status = "改善仮説" if not pd.isna(expected) else "未検証"
        elif requirement in {"D-01", "D-02", "D-03", "D-12"} and pd.isna(actual) and not pd.isna(expected):
            status = "差異"
        counts = self.requirements[requirement, metric]
        counts["total"] += 1
        counts["missing"] += missing
        counts[status] += 1
        row = {key: value for key, value in item.items() if key in DETAIL_COLUMNS}
        row.update(requirement=requirement, metric=metric, current=actual, expected=expected,
                   difference=delta, status=status, missing=int(missing), evidence=evidence)
        self.details.writerow({key: value if pd.notna(value) else None for key, value in row.items()})

    def tabulate(self, output):
        rows = []
        for (req, metric), count in sorted(self.requirements.items()):
            evaluated = count["一致"] + count["差異"]
            rows.append((req, metric, count["total"], evaluated, count["差異"],
                         100 * count["差異"] / evaluated if evaluated else None, count["missing"],
                         count["未検証"], count["改善仮説"]))
        seen = {row[0] for row in rows}
        rows.extend((req, "未検証", 0, 0, 0, None, 0, 0, 0)
                    for req in (f"D-{i:02d}" for i in range(1, 13)) if req not in seen)
        pd.DataFrame(sorted(rows), columns=["requirement", "metric", "targets", "evaluated", "issues", "issue_pct",
                                            "missing", "unverified", "hypothesis"]).to_csv(
            output / "real_audit_summary.csv", index=False)
        feature_rows = []
        total_abs_contribution = sum(item["abs_contribution"] for item in self.features.values())
        for stage in ("shared_generator", "legacy_scoring_input", "export_target"):
            for feature in self.names:
                item = self.features[stage, feature]
                total = item["total"]
                valid = item["valid"]
                contribution_n = item["contribution_n"]
                feature_rows.append((stage, feature, total, valid, total - valid,
                                     100 * (total - valid) / total if total else None,
                                     item["outliers"], item["iqr_evaluated"], item["compared"], item["different"],
                                     contribution_n, item["contribution"] if contribution_n else None,
                                     item["abs_contribution"] if contribution_n else None,
                                     100 * item["abs_contribution"] / total_abs_contribution
                                     if contribution_n and total_abs_contribution else None,
                                     "観測" if valid else "未検証", self.sources[feature]))
        pd.DataFrame(feature_rows, columns=["stage", "feature", "targets", "valid", "missing", "missing_pct",
                                            "anomaly_candidates", "iqr_evaluated", "compared", "different",
                                            "contribution_n", "signed_contribution_sum", "absolute_contribution_sum",
                                            "absolute_contribution_share_pct", "status", "calculation_source"]).to_csv(
            output / "feature_audit_summary.csv", index=False)

    def feature_table(self, path, day, production, optimized, metadata, stage, captured=None):
        prod = index_horses(production)
        opt = index_horses(optimized)
        meta = index_horses(metadata)
        keys = prod.index.union(opt.index) if stage == "shared_generator" else prod.index
        if not len(keys):
            return
        id_base = {"file": str(path), "date": display_date(day)}
        context = []
        for race, number in keys:
            data = meta.loc[(race, number)] if (race, number) in meta.index else {}
            raw_place = captured["place"].get(race) if captured else data.get("場所")
            raw_surface = captured["surface"].get(race) if captured else data.get("芝・ダ", data.get("芝ダ"))
            place = self.pipeline._normalize_place(raw_place)
            surface = self.pipeline._normalize_surface(raw_surface)
            group = self.f._select_weights(place, surface)
            context.append((dict(id_base, race_id=race, horse_number=number, horse=data.get("馬名"),
                                 venue=place, surface=surface, distance=data.get("距離"), going=data.get("馬場"),
                                 **{"class": data.get("クラス", data.get("レース名"))}), group))
        for feature in self.names:
            raw = prod[feature].reindex(keys) if feature in prod else pd.Series(None, index=keys, dtype=object)
            actual = numeric_series(raw)
            expected = numeric_series(opt[feature].reindex(keys)) if feature in opt else pd.Series(MISSING, index=keys)
            counts = self.features[stage, feature]
            counts["total"] += len(keys)
            counts["valid"] += int(actual.count())
            anomalous = raw.notna() & actual.isna()
            if stage == "export_target":
                anomalous |= actual.lt(0) | actual.gt(100)
            valid = actual.dropna()
            if len(valid) >= 4:
                q1, q3 = valid.quantile([0.25, 0.75])
                iqr = q3 - q1
                anomalous |= actual.lt(q1 - 1.5 * iqr) | actual.gt(q3 + 1.5 * iqr)
                counts["iqr_evaluated"] += len(valid)
            counts["outliers"] += int(anomalous.sum())
            for i, (actual_val, expected_val, anomaly) in enumerate(zip(actual, expected, anomalous)):
                key = keys[i]
                identity, weights = context[i]
                compare, gap = difference(actual_val, expected_val)
                if stage == "shared_generator" and len(opt):
                    if ((key in prod.index) != (key in opt.index) or
                            pd.isna(actual_val) != pd.isna(expected_val)):
                        compare = "差異"
                if stage == "legacy_scoring_input":
                    if feature.startswith("dl_") or feature in {"favorite_risk", "extra_penalty", "rest_dist_risk"}:
                        compare, gap, expected_val = "未検証", None, MISSING
                counts["compared"] += compare in {"一致", "差異"}
                counts["different"] += compare == "差異"
                weight = weights.get(feature, 0.0) if stage == "legacy_scoring_input" and feature in self.config.FEAT_COLS else None
                contribution = float(actual_val) * weight if weight is not None and pd.notna(actual_val) else None
                if contribution is not None:
                    counts["contribution_n"] += 1
                    counts["contribution"] += contribution
                    counts["abs_contribution"] += abs(contribution)
                self.values.writerow(dict(identity, feature=feature, stage=stage,
                                          production_value=actual_val if pd.notna(actual_val) else None,
                                          optimizer_value=expected_val if pd.notna(expected_val) else None,
                                          difference=gap, comparison=compare,
                                          anomaly=int(anomaly), weight=weight, legacy_raw_contribution=contribution,
                                          calculation_source=self.sources[feature]))
                if stage == "shared_generator":
                    if compare == "差異":
                        self.emit({k: identity[k] for k in ("file", "date", "race_id", "horse_number", "horse")},
                                  "D-12", "shared_feature_comparison", actual_val, expected_val,
                                  feature + "。馬キーまたは値・欠損状態の差", force_difference=True)
                    else:
                        counts = self.requirements["D-12", "shared_feature_comparison"]
                        counts["total"] += 1
                        counts["missing"] += bool(pd.isna(actual_val) or pd.isna(expected_val))
                        counts[compare] += 1

    def input_book(self, path, day, book, now, levels, base, odds, quality):
        f = self.f
        now = f._expand_legacy_now_rows_from_race_sheets(book, now.copy())
        rows = {str(f._normalize_rid_series(pd.Series([name])).iloc[0]): (name, frame)
                for name, frame in book.items() if re.fullmatch(r"\d{10,13}", str(name))}
        rid_column = next((key for key in ("rid_str", "レースID", "race_id")
                           if key in now and now[key].notna().any()), None)
        if not rid_column or "馬名" not in now:
            quality.append(f"- 今走のレースIDまたは馬名列なし `{path}`。")
            self.partial = True
            return
        now["rid_str"] = f._normalize_rid_series(now[rid_column])
        identified = now[now["rid_str"].isin(rows) & now["馬名"].notna() & now["馬名"].astype(str).str.strip().ne("")]
        if len(identified) != len(now):
            self.partial = True
            quality.append(f"- 今走から履歴シートに照合できない行 {len(now) - len(identified)}件 `{path}`。")
        self.input_horses += len(identified)
        ratings = levels.attrs.get("horse_ratings")
        for race, entries in identified.groupby("rid_str"):
            _, frame = rows[race]
            current_size = entries["頭数"].dropna().iloc[0] if "頭数" in entries and entries["頭数"].notna().any() else None
            items = []
            for _, row in entries.iterrows():
                item = {"file": str(path), "date": display_date(day), "race_id": race,
                        "horse_number": row.get("馬番"), "horse": str(row["馬名"]).strip(),
                        "field_size": current_size, "distance": f._parse_distance_m(row.get("コース", row.get("距離"))),
                        "surface": f._parse_surface(row.get("コース", row.get("芝・ダ"))),
                        "venue": f._normalize_place_text(row.get("場所"))}
                items.append(item)
                raw_class = row.get("クラス", row.get("レース名"))
                current_grade = f._parse_class_name_from_text(raw_class) or None
                grade = expected_class(raw_class)
                if grade or current_grade in {"G1", "G2", "G3"} or pd.isna(raw_class):
                    self.emit({**item, "source_value": raw_class}, "D-03", "current_graded_class",
                              current_grade, grade, "今走の重賞表記")
                selected = (ratings.loc[ratings["name_norm"].eq(f._normalize_horse_name(item["horse"]))]
                            if isinstance(ratings, pd.DataFrame) and "name_norm" in ratings else pd.DataFrame())
                rating = selected.iloc[-1] if not selected.empty else {}
                asof = audit_date(rating.get("rating_asof_date"))
                self.emit(item, "D-10", "rating_before_raceday", asof < day if pd.notna(asof) else None,
                          True if pd.notna(asof) else None, "マスターが返したratingの参照日")
                starts = finite(rating.get("start_count"))
                expected = (min(1.0, 0.25 + 0.15 * starts) if starts is not None and rating.get("rating_source") == "ratings_history"
                            else 0.15 if rating.get("rating_source") == "initial_rating" else None)
                self.emit(item, "D-10", "rating_confidence", finite(rating.get("rating_confidence")), expected,
                          "時点付き出走数または初期ratingから信頼度を検証")
            history_checks(f, frame, items, day, levels, self.emit)
        try:
            merged, shared, optimized, actual = run_feature_paths(
                path, day, self.pipeline, self.optimizer, levels, base, odds)
            paired = actual.get("optimizer_common", pd.DataFrame())
            self.feature_table(path, day, shared, paired, merged, "shared_generator")
            self.recomputed_horses += len(shared)
            for field, title in (("optimizer_error", "最適化ラッパー"), ("production_error", "本番ラッパー")):
                if field in actual:
                    self.partial = True
                    quality.append(f"- {title}の計算失敗 `{path}` ({actual[field]})。")
            if "frame" in actual:
                self.scoring_horses += len(actual["frame"])
                self.feature_table(path, day, actual["frame"], optimized, merged, "legacy_scoring_input", captured=actual)
            elif "production_error" not in actual:
                self.partial = True
                quality.append(f"- 本番の予測対象条件を通過した馬なし `{path}`。スコア寄与は未検証。")
            if "production_error" not in actual and "optimizer_error" not in actual:
                prod_keys = index_horses(actual.get("frame", pd.DataFrame()))
                opt_keys = index_horses(optimized)
                for key in prod_keys.index.union(opt_keys.index):
                    horse_row = prod_keys.loc[key] if key in prod_keys.index else opt_keys.loc[key]
                    item = {"file": str(path), "date": display_date(day), "race_id": key[0],
                            "horse_number": key[1], "horse": horse_row.get("馬名")}
                    self.emit(item, "D-12", "wrapper_horse_membership", key in prod_keys.index, key in opt_keys.index,
                              "一致条件は同じ馬キーでの出力。差はラッパーの対象馬抽出条件を含む")
            for race, batch in shared.groupby("rid_str"):
                rating_values = numeric_series(batch["master_rating"])
                confidences = numeric_series(batch["master_rating_confidence"])
                adjusted = (rating_values - rating_values.mean()) * confidences.fillna(0)
                for index, row in batch.iterrows():
                    item = {"file": str(path), "date": display_date(day), "race_id": race,
                            "horse_number": row["馬番"], "horse": row["馬名"]}
                    self.emit(item, "D-10", "rating_confidence_adjusted",
                              finite(row["master_rating_confidence_adjusted"]), adjusted.at[index],
                              "(rating−レース内平均)×信頼度。欠損信頼度は現行仕様の0")
                    for feature, raw, conditional, sign in (
                        ("last3f_context_value", "avg_last3f", "cond_avg_last3f", -1),
                        ("time_idx_context_value", "avg_time_idx", "cond_avg_time_idx", 1),
                    ):
                        overall, subset = finite(row.get(raw)), finite(row.get(conditional))
                        expected = sign * (subset - overall) if overall is not None and subset is not None else None
                        self.emit(item, "D-11", feature, finite(row.get(feature)), expected,
                                  "条件一致平均と全履歴平均の差。時計の補正精度には別途基準が必要")
                    actual_pressure = finite(row.get("front_pressure"))
                    known_ratio = finite(row.get("known_style_ratio"))
                    front_ratio = finite(row.get("front_style_ratio"))
                    front = finite(row.get("front_candidate_count"))
                    escape = finite(row.get("escape_candidate_count"))
                    if all(value is not None for value in (known_ratio, front_ratio, front, escape)):
                        pressure = (0.7 * min((front + escape) / 4, 1) + 0.3 * min(front_ratio / 0.4, 1))
                        reference = pressure * (0.7 + 0.3 * known_ratio) if known_ratio else 0.5
                    else:
                        reference = None
                    self.emit(item, "D-11", "front_pressure", actual_pressure, reference,
                              "先行・逃げ頭数、前脚質割合、脚質カバレッジから算出")
            base_item = {"file": str(path), "date": display_date(day)}
            self.emit(base_item, "D-12", "shared_generator_rows", len(shared), len(paired) if "optimizer_common" in actual else None,
                      "同一Excel・マスター・オッズ・開催日で両入口を比較")
            self.emit(base_item, "D-12", "wrapper_rows",
                      len(actual.get("frame", [])) if "production_error" not in actual else None,
                      len(optimized) if "optimizer_error" not in actual else None,
                      "本番のスコア入力と最適化の出力。対象馬の違いは抽出条件を含む")
        except Exception as exc:
            self.partial = True
            quality.append(f"- 特徴量の再計算・比較失敗 `{path}` ({type(exc).__name__}: {exc})。")

    def target(self, path, day, target):
        f = self.f
        target = target.copy()
        for key in ("rid_str", "レースID", "race_id"):
            if key in target:
                target["rid_str"] = f._normalize_rid_series(target[key])
                break
        if not set(KEYS) <= set(target):
            raise ValueError("TARGET のレースID・馬番列なし")
        target["馬番"] = f._normalize_umaban_series(target["馬番"])
        rename = {label: feature for feature, label in self.config.JAPANESE_FEATURE_NAMES.items()
                  if feature not in target and label in target}
        target = target.rename(columns=rename)
        self.feature_table(path, day, target, pd.DataFrame(), target, "export_target")
        self.export_horses += len(target)



def file_digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


class RunManifest:
    def __init__(self):
        self.files = {}

    def mark(self, path, role, status, reason=""):
        path = Path(path).resolve()
        key = (str(path), role)
        if key not in self.files:
            try:
                digest = file_digest(path)
            except OSError as exc:
                digest = None
                reason = f"{reason} ({type(exc).__name__}: {exc})".strip()
            self.files[key] = {"path": str(path), "role": role, "sha256": digest}
        history = self.files[key].setdefault("events", [])
        event = {"status": status, "reason": reason}
        if event not in history:
            history.append(event)
        self.files[key].update(status=status, reason=reason)

    def write(self, output, args, config, audit, status):
        packages = {}
        for name in ("pandas", "numpy", "openpyxl", "xlrd", "requests", "torch", "scikit-learn"):
            try:
                packages[name] = version(name)
            except PackageNotFoundError:
                packages[name] = None
        spec = config.PRODUCTION_MODEL_SPEC
        payload = {
            "status": status, "python": sys.version.split()[0], "dependencies": packages,
            "data_dir": str(args.data_dir.resolve()), "output_dir": str(output),
            "period": {kind: [display_date(args.start_date), display_date(end)] for kind, end in args.ends.items()},
            "model": {
                "name": spec.model_name, "score": config.SCORING_MODEL_VERSION,
                "weight_file": spec.weight_file, "configured_file_sha256": spec.file_sha256,
                "actual_file_sha256": file_digest(spec.resolved_weight_path),
                "configured_effective_sha256": spec.effective_sha256,
                "actual_effective_sha256": importlib.import_module("model_registry").normalized_weights_sha256(
                    config._production_effective_weights),
            },
            "observations": {
                "matched_input_horse_rows": audit.input_horses, "recomputed_horse_rows": audit.recomputed_horses,
                "export_horse_rows": audit.export_horses, "legacy_scoring_horse_rows": audit.scoring_horses,
                "historical_20261012": "issue_reference_only",
            },
            "files": list(self.files.values()),
        }
        (output / "run_manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                                                  encoding="utf-8")


def find_master(data_dir, explicit, candidates):
    if explicit:
        return explicit
    return next((data_dir / folder / name for name in candidates for folder in ("input", "master")
                 if (data_dir / folder / name).is_file()), None)


def odds_for_day(path, day):
    if path is None or not path.is_dir():
        return path
    date_text = day.strftime("%Y%m%d")
    candidates = [file for file in path.iterdir()
                  if file.is_file() and file.suffix.lower() == ".csv" and date_text in file.name]
    if not candidates:
        raise FileNotFoundError(f"{date_text} に対応するオッズCSVなし")
    return max(candidates, key=lambda file: (file.stat().st_mtime_ns, file.name))


def load_day_sources(args, loaders, day, manifest, quality, audit):
    frames = []
    for role, path, loader in (
            ("race_levels", args.race_levels, loaders.load_race_levels),
            ("base_time", args.base_time, loaders.load_base_time),
            ("odds", args.odds_csv, loaders.load_odds_csv)):
        try:
            if role == "odds":
                path = odds_for_day(path, day)
            if path is None or not path.is_file():
                raise FileNotFoundError("監査入力にファイル指定なし、またはファイル不存在")
            frame = loader(str(path), raceday=day.strftime("%Y%m%d")) if role != "base_time" else loader(str(path))
            if frame.empty and not isinstance(frame.attrs.get("horse_ratings"), pd.DataFrame):
                raise ValueError("ローダーが有効行を返さない")
        except Exception as exc:
            message = f"{display_date(day)} {role}: {type(exc).__name__}: {exc}"
            quality.append(f"- {message}。関連項目は未検証。")
            if path and not path.is_dir():
                manifest.mark(path, role, "unavailable", message)
            audit.partial = True
            frame = pd.DataFrame()
        else:
            manifest.mark(path, role, "used")
        frames.append(frame)
    return frames


def workbook_audit(args, feature_names, add, audit, manifest, loaders):
    """Inspect recognized columns without joining outcomes or inferring missing dates."""
    quality, schedule, time_tables, jockey_tables = [], [], [], []
    metrics = {
        "走破時計": ("タイム", "走破時計", "走破タイム"),
        "タイム指数": ("タイム指数", "ﾀｲﾑ指数", "avg_time_idx"),
        "先行負荷": ("front_pressure", "先行圧", "先行負荷"),
        "レース上り3F": ("レース上り3F", "レース上がり3F", "レース上り", "後3F"),
        "馬場": ("馬場", "馬場状態"),
        "位置取り補正": ("position_gain_rate", "位置取り補正"),
    }
    for kind, end in args.ends.items():
        directory = args.data_dir / kind
        books = sorted(p for p in directory.rglob("*")
                       if p.suffix.lower() in {".xlsx", ".xlsm", ".xls"} and not p.name.startswith("~$")
                       and p.resolve() not in {item.resolve() for item in (args.race_levels, args.base_time) if item})
        quality.append(f"- `{kind}` のExcel {len(books)}冊。")
        dates = set()
        counts = {key: 0 for key in ["date_missing", "invalid", "unpadded", "same", "future", "lost", "leak", "misread"]}
        feature_tables = 0
        for file in books:
            try:
                book = pd.read_excel(file, sheet_name=None)
            except Exception as exc:
                manifest.mark(file, kind, "read_error", f"{type(exc).__name__}: {exc}")
                audit.partial = True
                quality.append(f"- 読み取り失敗 `{file}` ({type(exc).__name__}: {exc})。")
                continue
            now = book.get("今走レース情報")
            if now is None:
                manifest.mark(file, kind, "unsupported", "今走シートなし")
                audit.partial = True
                quality.append(f"- 今走シートなし `{file.name}`。開催日の特定と履歴比較は未実施。")
                continue
            date_col = next((key for key in ("日付", "開催日", "日付(月日)", "年月日", "raceday") if key in now), None)
            if date_col:
                found = set(now[date_col].map(audit_date).dropna())
            else:
                match = re.search(r"(?<!\d)(20\d{6})(?!\d)", file.stem)
                found = {audit_date(match[1])} if match and pd.notna(audit_date(match[1])) else set()
            if len(found) != 1:
                manifest.mark(file, kind, "unsupported", "開催日を一意に特定できない")
                audit.partial = True
                quality.append(f"- 開催日を一意に特定できないExcel `{file.name}`。")
                continue
            day = found.pop()
            if not args.start_date <= day <= end:
                manifest.mark(file, kind, "out_of_period")
                continue
            dates.add(day)
            manifest.mark(file, kind, "used")
            if kind == "input":
                try:
                    audit.input_book(file, day, book, now, *load_day_sources(args, loaders, day, manifest, quality, audit), quality)
                except Exception as exc:
                    audit.partial = True
                    manifest.mark(file, kind, "audit_error", f"{type(exc).__name__}: {exc}")
                    quality.append(f"- 過去走の明細監査失敗 `{file}` ({type(exc).__name__}: {exc})。")
            elif "TARGET" in book:
                try:
                    audit.target(file, day, book["TARGET"])
                except (ValueError, KeyError) as exc:
                    audit.partial = True
                    manifest.mark(file, kind, "audit_error", str(exc))
                    quality.append(f"- TARGETの監査失敗 `{file}` ({exc})。")
            else:
                audit.partial = True
                manifest.mark(file, kind, "unsupported", "TARGETなし")
                quality.append(f"- TARGETなし `{file}`。")
            for name, frame in book.items():
                if kind == "output" and name == "TARGET":
                    feature_tables += 1
                    for feature in feature_names:
                        label = audit.config.JAPANESE_FEATURE_NAMES.get(feature)
                        key = feature if feature in frame else label
                        if key in frame:
                            values = pd.to_numeric(frame[key], errors="coerce")
                            add("feature_data", f"{file.name}/{feature}", int(values.count()), None,
                                f"TARGET 全{len(values)}行中の数値件数", "Excel")
                        else:
                            add("feature_data", f"{file.name}/{feature}", 0, None,
                                "TARGET に列なし", "Excel")
                historical = kind == "input" and bool(re.fullmatch(r"\d{10,13}", str(name)))
                if historical:
                    date_key = next((key for key in ("日付", "開催日") if key in frame), None)
                    if date_key is None:
                        counts["date_missing"] += len(frame)
                    else:
                        raw = frame[date_key]
                        parsed = raw.map(audit_date)
                        counts["invalid"] += int(parsed.isna().sum())
                        counts["unpadded"] += int(raw.astype(str).str.contains(
                            r"^\d{4}[/\-](?:\d[/\-]|\d{2}[/\-]\d$)", regex=True).sum())
                        counts["same"] += int(parsed.eq(day).sum())
                        counts["future"] += int(parsed.gt(day).sum())
                        production_dates = pd.to_datetime(
                            raw.astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8],
                            format="%Y%m%d", errors="coerce")
                        counts["lost"] += int((parsed.lt(day) & ~production_dates.lt(day)).sum())
                        counts["leak"] += int((parsed.ge(day) & production_dates.lt(day)).sum())
                        counts["misread"] += int((parsed.notna() & production_dates.notna() & parsed.ne(production_dates)).sum())
                if historical or (kind == "output" and name == "TARGET"):
                    extracted = {label: frame[key] for label, candidates in metrics.items()
                                 for key in candidates if key in frame}
                    if extracted:
                        time_tables.append(pd.DataFrame(extracted, index=frame.index))
                if historical:
                    going = next((key for key in ("馬場", "馬場状態") if key in frame), None)
                    if going and {"騎手", "着順"} <= set(frame):
                        selected = [key for key in ("race_id", "レースID") if key in frame]
                        if selected and "馬名" in frame:
                            jockey_tables.append(frame[[selected[0], "馬名", "騎手", going, "着順"]]
                                                 .set_axis(["race", "horse", "jockey", "going", "finish"], axis=1))
        coverage = ", ".join(day.strftime("%Y-%m-%d") for day in sorted(dates)) or "確認済み日なし"
        schedule.append(f"- `{kind}` ({args.start_date.date()}〜{end.date()})。確認 {len(dates)}日。{coverage}。")
        if kind == "input" and dates:
            quality.append(
                "- 過去走シート行数（複数Excel間で重複あり） "
                f"日付列なし {counts['date_missing']}、解析不能 {counts['invalid']}、"
                f"非ゼロ埋め {counts['unpadded']}、同日 {counts['same']}、未来 {counts['future']}、"
                f"本番日付処理で除外される有効な過去行 {counts['lost']}、"
                f"過去と誤認する当日・未来行 {counts['leak']}、"
                f"別日として解析される行 {counts['misread']}。"
                "日付フィルター単体の比較で、馬名照合・予測採用の実績は含まない。"
            )
        if kind == "output":
            quality.append(f"- 特徴量の列・有効件数を調べたTARGET {feature_tables}シート。")
    master_files = [p for p in (args.data_dir / "master").rglob("*") if p.is_file()]
    quality.append(f"- `master` のファイル {len(master_files)}件。実際のrating時点・寄与率の再現にはレース単位の履歴とスコアが必要。")
    return quality, schedule, time_tables, jockey_tables


def time_report(tables):
    if not any(len(table.columns) for table in tables):
        return ["対象期間内の読取可能な明細がなく、実データの異常値検査は未実施。"]
    frame = pd.concat(tables, ignore_index=True)
    lines = ["| 項目 | 有効数 | 欠損 | 形式・範囲外 | 最小 | 最大 | IQR外れ候補 |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for key, series in frame.items():
        if key == "馬場":
            good = series.astype(str).str.strip().isin(["良", "稍", "稍重", "重", "不", "不良"])
            lines.append(f"| {key} | {good.sum()} | {series.isna().sum()} | {(~good & series.notna()).sum()} | | | |")
            continue
        numeric = pd.to_numeric(series, errors="coerce")
        if key == "走破時計":
            split = series.astype(str).str.extract(r"^(\d+):([0-5]?\d(?:\.\d+)?)$")
            numeric = numeric.fillna(pd.to_numeric(split[0], errors="coerce") * 60 + pd.to_numeric(split[1], errors="coerce"))
        valid = numeric.map(math.isfinite)
        if key in {"走破時計", "レース上り3F"}:
            valid &= numeric.gt(0)
        if key == "先行負荷":
            valid &= numeric.between(0, 1)
        if key == "位置取り補正":
            valid &= numeric.between(-1, 1)
        values = numeric[valid]
        q1, q3 = values.quantile([0.25, 0.75])
        iqr = q3 - q1
        outliers = ((values < q1 - 1.5 * iqr) | (values > q3 + 1.5 * iqr)).sum() if len(values) >= 4 else ""
        lines.append(f"| {key} | {len(values)} | {series.isna().sum()} | {(~valid & series.notna()).sum()} | "
                     f"{values.min():g} | {values.max():g} | {outliers} |")
    return lines


def jockey_report(tables):
    if not tables:
        return ["騎手・馬場・着順・レースID・馬名を揃えた実データを取得できず、馬場別成績は未集計。"]
    entries = pd.concat(tables, ignore_index=True).dropna().drop_duplicates()
    entries = entries[~entries.duplicated(["race", "horse"], keep=False)]
    entries["finish"] = pd.to_numeric(entries["finish"], errors="coerce")
    entries = entries[entries["finish"].gt(0)].assign(win=lambda rows: rows["finish"].eq(1),
                                                      top3=lambda rows: rows["finish"].le(3))
    lines = ["| 騎手 | 馬場 | 有効着順件数 | 勝率 % | 3着内率 % |", "|---|---|---:|---:|---:|"]
    for (jockey, going), group in entries.groupby(["jockey", "going"]):
        lines.append(f"| {jockey} | {going} | {len(group)} | {100 * group.win.mean():.2f} | {100 * group.top3.mean():.2f} |")
    return lines


def condition_stats(path, start=START, end=END["output"]):
    if path is None:
        return pd.DataFrame(columns=CONDITION_COLUMNS)
    frame = pd.read_csv(path, dtype={"race_id": str, "horse_number": str})
    required = {"race_id", "horse_number", "date", "popularity", "bet_type", "stake", "payout",
                *DIMENSIONS} - {"popularity_band"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"購入明細の必須列不足 {sorted(missing)}")
    dates = frame["date"].map(audit_date)
    if dates.isna().any():
        raise ValueError("購入明細の日付が解析できません")
    frame = frame.loc[dates.between(start, end)].copy()
    if frame[["race_id", "horse_number", "bet_type"]].isna().any().any():
        raise ValueError("購入明細のキー欠損")
    if frame.duplicated(["race_id", "horse_number", "bet_type"]).any():
        raise ValueError("重複購入行は馬・券種単位に事前集約してください")
    for key in ("stake", "payout"):
        frame[key] = pd.to_numeric(frame[key], errors="raise")
        if not frame[key].map(math.isfinite).all():
            raise ValueError(f"金額の欠損・非有限値 {key}")
    if not (frame.stake.gt(0) & frame.payout.ge(0) & frame.bet_type.isin(["単勝", "複勝"])).all():
        raise ValueError("投資額・払戻額・券種が不正です")
    popularity = pd.to_numeric(frame.popularity, errors="coerce")
    frame["popularity_band"] = pd.cut(popularity, [0, 3, 6, 9, math.inf], labels=["1–3", "4–6", "7–9", "10+"])
    frame[DIMENSIONS] = frame[DIMENSIONS].astype("string").fillna("不明")
    frame["class × popularity_band"] = frame["class"] + " / " + frame["popularity_band"]
    rows = []
    for dim in [*DIMENSIONS, "class × popularity_band"]:
        for (value, bet), group in frame.groupby([dim, "bet_type"], sort=True):
            stake, payout = group.stake.sum(), group.payout.sum()
            rows.append((dim, value, bet, len(group), stake, payout, payout - stake, 100 * payout / stake))
    return pd.DataFrame(rows, columns=CONDITION_COLUMNS)


def integration_checks(f, config, groups, production, add):
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    optimizer = importlib.import_module("tokutyouryou_keisann.features")
    registry = importlib.import_module("model_registry")
    add("D-12", "本番・最適化の特徴量生成関数の同一性",
        pipeline.build_features_from_excel is optimizer.build_features_from_excel is f.build_features_from_excel,
        True, "ラッパーが参照する共通関数。同一Excelから各パイプラインの最終出力を比較するには実データが必要", "コード")
    add("D-12", "本番設定のファイルSHA-256", registry.sha256_file(ROOT / production["weight_file"]),
        production["file_sha256"], "config/production_model.json の固定値", "コード")
    add("D-12", "本番実効SHA-256", registry.normalized_weights_sha256(config._production_effective_weights),
        production["effective_sha256"], "実行時の補完・符号ガード適用後", "コード")
    add("D-12", "本番選択ファイル", config.ACTIVE_FEATURE_WEIGHTS_FILE,
        production["weight_file"], "本番設定と実行時選択", "コード")
    add("D-12", "スコア版", config.SCORING_MODEL_VERSION, production["scoring_model_version"],
        "本番設定と実行時選択", "コード")
    for label, key in (("グループ数", "expected_group_count"), ("重み数", "expected_element_count")):
        measured = len(groups) if key == "expected_group_count" else sum(map(len, groups.values()))
        add("D-12", label, measured, production[key], "重みファイルの実測", "コード")
    shadow = json.loads((ROOT / "config/shadow_model.json").read_text(encoding="utf-8"))
    flags = {key: value for key, value in shadow.items() if key.startswith("allow_")}
    add("D-12", "検証設定の権限制限", len(flags) == 7 and all(value is False for value in flags.values()),
        True, "順位・買い目・購入・メール・本番採用・最適化基準・重み最適化の7フラグ", "コード")
    add("D-12", "検証設定のファイルSHA-256", registry.sha256_file(ROOT / shadow["weight_file"]),
        shadow["file_sha256"], "設定は分離。現行の検証設定は同一パスの重みを指すがSHAが異なる", "コード")


def write_md(output, filename, title, lines):
    (output / filename).write_text(f"# {title}\n\n" + "\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    parser.add_argument("--start-date", type=audit_date, default=START)
    parser.add_argument("--end-date", type=audit_date, help="両入力区分の終了日。省略時は仕様の区分別終了日")
    parser.add_argument("--race-levels", type=Path)
    parser.add_argument("--base-time", type=Path)
    parser.add_argument("--odds-csv", type=Path, help="CSVファイル、または開催日入りCSVを含むディレクトリ")
    parser.add_argument("--settled-bets", type=Path, help="購入明細のUTF-8 CSV。列定義は概要参照")
    args = parser.parse_args()
    args.ends = dict.fromkeys(END, args.end_date) if args.end_date is not None else END
    if pd.isna(args.start_date) or any(pd.isna(end) or end < args.start_date for end in args.ends.values()):
        parser.error("対象期間には有効な開始日・終了日を指定してください")
    output = args.output_dir.resolve()
    args.data_dir = args.data_dir.resolve()
    if output.is_relative_to(args.data_dir) or args.data_dir.is_relative_to(output):
        parser.error("監査出力とデータ入力は独立したディレクトリを指定してください")
    if output.is_relative_to(ROOT) and not output.is_relative_to(OUTPUT.resolve()):
        parser.error("リポジトリ内の出力は監査専用ディレクトリ配下を指定してください")
    output.mkdir(parents=True, exist_ok=True)
    args.race_levels = find_master(args.data_dir, args.race_levels, ("race_levels.xlsx",))
    args.base_time = find_master(args.data_dir, args.base_time, ("場所_馬場_タイム.xlsx", "base_time.xlsx"))
    if args.odds_csv is None and (args.data_dir / "ozzu_csv").is_dir():
        args.odds_csv = args.data_dir / "ozzu_csv"
    manifest = RunManifest()

    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT))
    manifest.mark(ROOT / "config/production_model.json", "production_model_config", "used")
    manifest.mark(ROOT / "config/shadow_model.json", "shadow_model_config", "inspected")
    try:
        config = importlib.import_module("1_keibayosou_config")
        f = importlib.import_module("1_keibayosou_features")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        (output / "run_manifest.json").write_text(json.dumps(
            {"status": "startup_error", "error": error, "files": list(manifest.files.values())},
            ensure_ascii=False, indent=2), encoding="utf-8")
        print(error, file=sys.stderr)
        return 2
    loaders = importlib.import_module("1_keibayosou_loaders")
    production = json.loads((ROOT / "config/production_model.json").read_text(encoding="utf-8"))
    manifest.mark(ROOT / production["weight_file"], "production_weights", "used")
    groups = read_weights(ROOT / production["weight_file"])
    features = sorted(set().union(*(set(group) for group in groups.values())))
    records = []

    def add(requirement, metric, observed, expected, evidence, source="合成"):
        if expected is None:
            status = "観測"
        elif isinstance(expected, bool) or not isinstance(expected, (float, int)) or isinstance(observed, str):
            status = "一致" if observed == expected else "差異"
        else:
            status = "一致" if math.isclose(observed, expected, rel_tol=1e-9, abs_tol=1e-12) else "差異"
        records.append((requirement, source, metric, observed, expected, status, evidence))

    synthetic = production_probes(f, loaders, add)
    integration_checks(f, config, groups, production, add)
    for feature in features:
        raw_values = [group[feature] for group in groups.values() if feature in group]
        applied = [group.get(feature, 0) for group in config._production_effective_weights.values()]
        sample_n = int(synthetic[feature].count()) if feature in synthetic else 0
        add("feature", feature, len(raw_values), len(groups),
            f"設定非ゼロ {sum(v != 0 for v in raw_values)}、実効非ゼロ {sum(v != 0 for v in applied)}、"
            f"設定重み範囲 {min(raw_values):g}〜{max(raw_values):g}、合成数値 {sample_n}/{len(synthetic)}",
            "重み・合成")
    with ((output / "real_audit_details.csv").open("w", encoding="utf-8", newline="") as details,
          (output / "feature_audit_values.csv").open("w", encoding="utf-8", newline="") as values):
        audit = LiveAudit(details, values, features, f, config)
        quality, schedule, time_tables, jockey_tables = workbook_audit(args, features, add, audit, manifest, loaders)
    audit.tabulate(output)
    try:
        stats = condition_stats(args.settled_bets, args.start_date, args.ends["output"])
    except (OSError, ValueError) as exc:
        audit.partial = True
        manifest.mark(args.settled_bets, "settled_bets", "read_error", str(exc))
        quality.append(f"- 購入明細を利用できない `{args.settled_bets}` ({exc})。")
        stats = condition_stats(None)
    else:
        if args.settled_bets:
            manifest.mark(args.settled_bets, "settled_bets", "used" if not stats.empty else "no_in_period_rows")
    status = ("no_real_data" if not (audit.input_horses or audit.recomputed_horses or audit.export_horses)
              else "partial" if audit.partial or stats.empty or not (audit.recomputed_horses and audit.export_horses)
              else "observed_with_limits")
    manifest.write(output, args, config, audit, status)
    results = pd.DataFrame(records, columns=["requirement", "source", "metric", "observed", "expected", "status", "evidence"])
    results.to_csv(output / "full_feature_audit_results.csv", index=False)
    stats.to_csv(output / "condition_stats.csv", index=False)

    def table(*requirements):
        selected = results[results.requirement.isin(requirements)]
        return ["| 要件 | 指標 | 観測 | 比較値 | 判定 | 根拠 |", "|---|---|---|---|---|---|"] + [
            f"| {r.requirement} | {r.metric} | {display(r.observed)} | {display(r.expected)} | {r.status} | {r.evidence} |"
            for r in selected.itertuples()
        ]

    write_md(output, "data_quality_audit.md", "データ品質監査",
             quality + ["", *table("D-01", "D-04", "D-05", "D-09")])
    write_md(output, "race_schedule_audit.md", "レース開催日程監査", schedule + [
        "", "日付は今走シート、日付列が無い場合はファイル名中のYYYYMMDDから抽出。"
        "対象期間内の開催日リスト原本が無いため、日付単位の欠落照合は未実施。"])
    write_md(output, "time_speed_audit.md", "タイムスピード監査", time_report(time_tables) + [
        "", "上り3Fの補正例は馬個体の上りを使用。IQRは開催条件を統合した探索用の候補。"
        "分布の偏りや異常確定には距離・馬場別の確認が必要。", "", *table("D-11")])
    write_md(output, "jockey_condition_audit.md", "騎手成績と馬場状態の監査", jockey_report(jockey_tables) + [
        "", "過去走の競走IDと馬名で一意化し、競合する行は集計から除外する。"
        "共通特徴量生成関数に騎手名の直接入力はない。騎手と各特徴量・順位の関係を定量化するには"
        "同一レース・馬で結合した実データと交絡を考慮した分析が必要。"])
    write_md(output, "integration_audit.md", "本番環境との統合と隔離", table("D-12") + [
        "", "本番・検証の設定ファイルと用途は別。両設定の重みパスは共通。"
        "検証の固定SHAが現行ファイルと異なるため、当該設定での検証モデル読み込みは失敗する。"
        "本プログラムは予想パイプラインの実行、購入、配信、重みの更新を行わない。"])
    summary(output, len(features), len(groups), sum(map(len, groups.values())), stats, table, status, audit)
    print(f"{status} {output}")
    return 2 if status == "no_real_data" else 0


def display(value):
    if pd.isna(value):
        return "—"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def summary(output, count, groups, elements, stats, table, status, audit):
    lines = [
        f"本番重みの実測は **{count}特徴量・{groups}グループ・{elements:,}要素**。"
        "詳細CSVに全特徴量の設定カバレッジ、非ゼロ重み数、重み範囲、合成値の観測件数を記録した。",
        "",
        f"**実行状態 `{status}`**。履歴照合 {audit.input_horses}馬行、"
        f"特徴量再計算 {audit.recomputed_horses}馬行、保存済みTARGET {audit.export_horses}馬行。"
        "0件時は合成・コード監査の成果物を出力し、終了コード2を返す。"
        "`partial` は一部未検証、`observed_with_limits` も以下の適用範囲内での観測。",
        "",
        "`python scripts/full_feature_audit.py --data-dir /nas/data --output-dir /nas/audit/run "
        "--start-date 2025-06-14 --end-date 2026-10-25`",
        "",
        "`--data-dir` は input/output/master を含むディレクトリ。"
        "終了日を省略すると原本は11月1日、TARGETは10月25日まで。"
        "マスターは `--race-levels` と `--base-time`、オッズは `--odds-csv` で指定できる。"
        "オッズにディレクトリを指定するとファイル名に開催日YYYYMMDDを含むCSVを選ぶ。"
        "単体CSVは対象期間と整合する入力を用意する。日付の根拠がないオッズの時点は未検証。"
        "期間・使用モデル・実測/設定SHA・依存版・入力パスとファイルSHA・読込状態は `run_manifest.json` に記録。",
        "",
        "## 実データの明細と101特徴量",
        "",
        "- `real_audit_details.csv` は今走レースID・馬番・馬名・過去走のExcel行/レースID、現行値、"
        "比較値、数値差（現行−比較）、欠損フラグを記録。文字列・真偽値の差は判定を参照。",
        "- `real_audit_summary.csv` は指標ごとの対象・評価可能・差異・欠損・未検証件数。"
        "発生率の分母は評価可能件数。異なるExcelに同一過去走が載る場合は今走対象単位で数える。",
        "- `feature_audit_values.csv` と `feature_audit_summary.csv` は共通生成、legacy加重入力、"
        "保存済みTARGETの段階別。計算条件は場・芝ダ・距離・馬場・クラスとコード位置を参照。"
        "TARGETは日本語列名を解決する。保存値は本番出力上0〜100尺度で、再計算した生値との直接比較は未実施。",
        "- 非数値・無限値、TARGETの0〜100範囲外、各ブック・段階の有効値が4件以上の1.5×IQR超過を"
        "異常候補とする。欠損率は値を取得できた馬キー集合内の率。異常候補の競馬上の妥当性は実データで確認する。",
        "- legacy寄与は本番が使用する場・芝ダ別の実効重み×観測値。絶対寄与率は観測できた加重項の"
        "絶対値総和を分母とする。減点・DL加算・最終正規化・順位は対象外。取得できない寄与は未検証。",
        "- 本番と最適化の共通生成を同一のExcel・マスター・オッズ・開催日で再実行し、"
        "馬キー・欠損状態・値を比較する。ラッパー間では本番のスコア入力と最適化の出力を観測する。"
        "監査中の学習・書き込みを防ぐためDL学習を抑止する。DL・減点系のラッパー間一致は未検証。",
        "- D-07・D-08は改善仮説。差異件数と発生率には加えない。",
        "",
        "## 仕様対応",
        "",
        "| 要件 | 実装 | 実走での未検証事項 |", "|---|---|---|",
        "| D-01 | 本番過去走フィルターを合成実行し、Excel履歴の日付表記・未来行・取りこぼしを計数 | 実際の予測入力・rating時点の突合 |",
        "| D-02 | 本番ペース判定で前半／後半の3パターンを比較 | 表記別の頻度、スコア差 |",
        "| D-03 | 本番クラス判定でGI・GII・GIIIを比較 | 誤判定件数、条件一致への影響 |",
        "| D-04 | 除外・中止・取消・完走を含む過去走数と数値着順平均を計算 | 発生件数、中止の着順評価方針 |",
        "| D-05 | 欠損を含む直近3走の結果と3走を先に抽出した結果を比較 | 他指標を含む欠損頻度と順位差 |",
        "| D-06 | 過去8・16・18頭の位置率と今回16頭での代用結果を比較 | 頭数欠損率と脚質・先行圧の変化 |",
        "| D-07 | 同場同距離3着内率と同芝ダ同距離の候補を比較する改善仮説 | どちらを優先すると予測が改善するか |",
        "| D-08 | 上位5件平均と直近3／5走・新しさ・本人着順による集計を比較する改善仮説 | 候補方式の時系列評価 |",
        "| D-09 | 有効着順を分母とした勝率との比較 | 実データの勝率差と順位差 |",
        "| D-10 | 履歴の時点制御、出走数信頼度、レース内相対ratingの補正を確認 | スコア寄与率22.67%の再測定 |",
        "| D-11 | 先行圧・通過位置・上がり・指数・頭数補正着順を合成で検証し、時計等の列を監査 | 時計の補正精度と条件別の分布 |",
        "| D-12 | 共通生成関数の同一性、本番の固定SHAと実効重み、検証側フラグ・SHAを確認 | 実データでのラッパー間最終出力の一致と検証モデルの稼働 |",
        "",
        "## 合成実行・コード監査の結果",
        "",
        *table(*(f"D-{i:02d}" for i in range(1, 13))),
        "",
        "D-01〜D-06およびD-09の差異は学習入力やスコアに影響し得る。"
        "実データでの影響量の確定、再最適化、全期間の回収率比較は後続工程。",
        "",
        "## 条件別回収率",
        "",
        "`--settled-bets` に、購入条件を適用済みのUTF-8 CSVを指定する。"
        "必須列は `race_id, horse_number, date, venue, surface, distance, class, going, popularity, reliability, bet_type, stake, payout`。"
        "確定払戻ゼロは負け、取消・返還は実額を指定する。単勝・複勝別に投資額・払戻額・利益額（払戻−投資）・払戻総額÷投資総額を集計する。"
        "データ信頼度の区分は明細側で指定する。購入条件の適用や精算は入力側で行う。",
        "",
    ]
    if stats.empty:
        lines.append("対象期間の確定購入明細が無く、実データの条件別回収率は未集計。CSVはヘッダーのみ。")
    else:
        lines.extend(["各棒の1ブロックは約10ポイント（表示上限300%）。", "",
                      "| クラス・人気帯 | 券種 | 購入件数 | 回収率 |", "|---|---|---:|---|"])
        for row in stats[stats.dimension.eq("class × popularity_band")].itertuples():
            bar = "█" * min(30, round(row.roi_pct / 10))
            lines.append(f"| {row.value} | {row.bet_type} | {row.bets} | {row.roi_pct:.1f}% {bar} |")
    lines.extend([
        "",
        "## 2026-10-12 レースレベル監査との比較",
        "",
        "以下はIssue #25に記載された参照値。独立再測定は未実施。実行時の条件別回収率は別の集計で、"
        "同一レース・同一時点での除外モデル再計算は後続工程。記載による対象は170レース・2,329頭、評価147レース、未来rating参照0件。",
        "",
        "| 指標 | 現行 % | レースレベル除外 % | 差 pt |", "|---|---:|---:|---:|",
        "| 1位単勝回収率 | 109.9 | 62.1 | 47.8 |",
        "| 1位複勝回収率 | 100.8 | 73.4 | 27.4 |",
        "| 1位複勝的中率 | 53.7 | 42.2 | 11.5 |",
        "| TOP5勝馬包含率 | 76.2 | 64.6 | 11.6 |",
        "| データ時点限定の単勝回収率 | 57.3 | 53.0 | 4.3 |",
        "",
        "記載上の寄与率はレースレベル関連26.4%、その他73.6%。内訳は信頼度補正済み能力rating 22.67%、"
        "過去走レースレベル上位平均2.13%、過去走最高レースレベル1.52%、直近rating百分位0.09%、巻き返し評価0.02%。",
        "",
        "開発前後の既存テストと10件の分類はPR本文に記録。",
    ])
    write_md(output, "full_feature_audit.md", "全特徴量監査", lines)


if __name__ == "__main__":
    raise SystemExit(main())
