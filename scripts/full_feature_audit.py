#!/usr/bin/env python3
"""Read-only production feature audit. Run with the repository's Python environment."""
from __future__ import annotations

import argparse
import ast
import importlib
import json
import math
import re
import sys
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "outputs/full_feature_audit_20261125"
START = pd.Timestamp("2025-06-14")
END = {"input": pd.Timestamp("2026-11-01"), "output": pd.Timestamp("2026-10-25")}
DIMENSIONS = ["venue", "surface", "distance", "class", "going", "popularity_band", "reliability"]
CONDITION_COLUMNS = ["dimension", "value", "bet_type", "bets", "stake", "payout", "roi_pct"]


def audit_date(value):
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


def workbook_audit(data_dir, feature_names, add):
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
    for kind, end in END.items():
        directory = data_dir / kind
        books = sorted(p for p in directory.rglob("*")
                       if p.suffix.lower() in {".xlsx", ".xlsm", ".xls"} and not p.name.startswith("~$"))
        quality.append(f"- `{kind}` のExcel {len(books)}冊。")
        dates = set()
        counts = {key: 0 for key in ["date_missing", "invalid", "unpadded", "same", "future", "lost", "leak", "misread"]}
        feature_tables = 0
        for file in books:
            try:
                book = pd.read_excel(file, sheet_name=None)
            except (OSError, ImportError, ValueError) as exc:
                quality.append(f"- 読み取り失敗 `{file.name}` ({type(exc).__name__})。")
                continue
            now = book.get("今走レース情報")
            if now is None:
                quality.append(f"- 今走シートなし `{file.name}`。開催日の特定と履歴比較は未実施。")
                continue
            date_col = next((key for key in ("日付", "開催日", "日付(月日)", "年月日", "raceday") if key in now), None)
            if date_col:
                found = set(now[date_col].map(audit_date).dropna())
            else:
                match = re.search(r"(?<!\d)(20\d{6})(?!\d)", file.stem)
                found = {audit_date(match[1])} if match and pd.notna(audit_date(match[1])) else set()
            if len(found) != 1:
                quality.append(f"- 開催日を一意に特定できないExcel `{file.name}`。")
                continue
            day = found.pop()
            if not START <= day <= end:
                continue
            dates.add(day)
            for name, frame in book.items():
                if kind == "output" and name == "TARGET":
                    feature_tables += 1
                    for feature in feature_names:
                        if feature in frame:
                            values = pd.to_numeric(frame[feature], errors="coerce")
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
                    time_tables.append(pd.DataFrame(extracted, index=frame.index))
                if historical:
                    going = next((key for key in ("馬場", "馬場状態") if key in frame), None)
                    if going and {"騎手", "着順"} <= set(frame):
                        selected = [key for key in ("race_id", "レースID") if key in frame]
                        if selected and "馬名" in frame:
                            jockey_tables.append(frame[[selected[0], "馬名", "騎手", going, "着順"]]
                                                 .set_axis(["race", "horse", "jockey", "going", "finish"], axis=1))
        expected = {"input": 151, "output": 145}[kind]
        coverage = ", ".join(day.strftime("%Y-%m-%d") for day in sorted(dates)) or "確認済み日なし"
        schedule.append(f"- `{kind}` ({START.date()}〜{end.date()})。確認 {len(dates)}/{expected}日。{coverage}。")
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
    master_files = [p for p in (data_dir / "master").rglob("*") if p.is_file()]
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


def condition_stats(path):
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
    frame = frame.loc[dates.between(START, END["output"])].copy()
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
            rows.append((dim, value, bet, len(group), stake, payout, 100 * payout / stake))
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
    parser.add_argument("--settled-bets", type=Path, help="購入明細のUTF-8 CSV。列定義は概要参照")
    args = parser.parse_args()

    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT))
    f = importlib.import_module("1_keibayosou_features")
    config = importlib.import_module("1_keibayosou_config")
    loaders = importlib.import_module("1_keibayosou_loaders")
    production = json.loads((ROOT / "config/production_model.json").read_text(encoding="utf-8"))
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
    quality, schedule, time_tables, jockey_tables = workbook_audit(args.data_dir, features, add)
    stats = condition_stats(args.settled_bets)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    results = pd.DataFrame(records, columns=["requirement", "source", "metric", "observed", "expected", "status", "evidence"])
    results.to_csv(OUTPUT / "full_feature_audit_results.csv", index=False)
    stats.to_csv(OUTPUT / "condition_stats.csv", index=False)

    def table(*requirements):
        selected = results[results.requirement.isin(requirements)]
        return ["| 要件 | 指標 | 観測 | 比較値 | 判定 | 根拠 |", "|---|---|---|---|---|---|"] + [
            f"| {r.requirement} | {r.metric} | {display(r.observed)} | {display(r.expected)} | {r.status} | {r.evidence} |"
            for r in selected.itertuples()
        ]

    write_md(OUTPUT, "data_quality_audit.md", "データ品質監査",
             quality + ["", *table("D-01", "D-04", "D-05", "D-09")])
    write_md(OUTPUT, "race_schedule_audit.md", "レース開催日程監査", schedule + [
        "", "日付は今走シート、日付列が無い場合はファイル名中のYYYYMMDDから抽出。"
        "対象期間内の開催日リスト原本が無いため、日付単位の欠落照合は未実施。"])
    write_md(OUTPUT, "time_speed_audit.md", "タイムスピード監査", time_report(time_tables) + [
        "", "上り3Fの補正例は馬個体の上りを使用。IQRは開催条件を統合した探索用の候補。"
        "分布の偏りや異常確定には距離・馬場別の確認が必要。", "", *table("D-11")])
    write_md(OUTPUT, "jockey_condition_audit.md", "騎手成績と馬場状態の監査", jockey_report(jockey_tables) + [
        "", "過去走の競走IDと馬名で一意化し、競合する行は集計から除外する。"
        "共通特徴量生成関数に騎手名の直接入力はない。騎手と各特徴量・順位の関係を定量化するには"
        "同一レース・馬で結合した実データと交絡を考慮した分析が必要。"])
    write_md(OUTPUT, "integration_audit.md", "本番環境との統合と隔離", table("D-12") + [
        "", "本番・検証の設定ファイルと用途は別。両設定の重みパスは共通。"
        "検証の固定SHAが現行ファイルと異なるため、当該設定での検証モデル読み込みは失敗する。"
        "本プログラムは予想パイプラインの実行、購入、配信、重みの更新を行わない。"])
    summary(OUTPUT, len(features), len(groups), sum(map(len, groups.values())), stats, table)
    print(OUTPUT.relative_to(ROOT))


def display(value):
    if pd.isna(value):
        return "—"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def summary(output, count, groups, elements, stats, table):
    lines = [
        f"本番重みの実測は **{count}特徴量・{groups}グループ・{elements:,}要素**。"
        "詳細CSVに全特徴量の設定カバレッジ、非ゼロ重み数、重み範囲、合成値の観測件数を記録した。",
        "",
        "`python scripts/full_feature_audit.py` で再生成できる。"
        "`--data-dir` は input/output/master を含むディレクトリ。既定値は `data`。",
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
        "| D-07 | 平均距離との差と同場同距離3着内率を確認 | どちらを優先すると予測が改善するか |",
        "| D-08 | 上位5件平均と直近3／5走・新しさ・本人着順による集計を比較 | 候補方式の時系列評価 |",
        "| D-09 | 有効着順を分母とした勝率との比較 | 実データの勝率差と順位差 |",
        "| D-10 | 履歴の時点制御、出走数信頼度、レース内相対ratingの補正を確認 | スコア寄与率22.67%の再測定 |",
        "| D-11 | 先行圧・通過位置・上がり・指数・頭数補正着順を合成で検証し、時計等の列を監査 | 時計の補正精度と条件別の分布 |",
        "| D-12 | 共通生成関数の同一性、本番の固定SHAと実効重み、検証側フラグ・SHAを確認 | 実データでのラッパー間最終出力の一致と検証モデルの稼働 |",
        "",
        "## 合成実行・コード監査の結果",
        "",
        *table(*(f"D-{i:02d}" for i in range(1, 13))),
        "",
        "D-01〜D-06およびD-09の差異は学習入力やスコアに影響し得る。特徴量定義を修正する場合、"
        "現行重みと再最適化後の重みを同一の時系列分割で比較し、保留期間で改善を判定する。"
        "合成ケースから実データの影響量や再最適化の効果は判定できない。",
        "",
        "## 条件別回収率",
        "",
        "`--settled-bets` に、購入条件を適用済みのUTF-8 CSVを指定する。"
        "必須列は `race_id, horse_number, date, venue, surface, distance, class, going, popularity, reliability, bet_type, stake, payout`。"
        "確定払戻ゼロは負け、取消・返還は実額を指定する。単勝・複勝別に払戻総額÷投資総額を集計する。"
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
        "以下はIssue #25に記載された参照値。過去監査結果のファイルと対応する入力・スコアが"
        "環境に無いため、独立再現は未実施。記載による対象は170レース・2,329頭、評価147レース、未来rating参照0件。",
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
    main()
