"""A〜D比較用の発走前スナップショットと確定結果の読込。"""

from collections import Counter
from datetime import datetime
import importlib
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pandas as pd

from model_registry import sha256_file
from race_purchase_filter.evaluation_utils import integer
from tools.ab_comparison_evidence import available_by_cutoff, result_evidence, snapshot_grade, timestamp
from tokutyouryou_keisann.common import _norm_name
from tokutyouryou_keisann.config import FEAT_COLS
from tokutyouryou_keisann.features import _parse_distance_to_int


pipeline = importlib.import_module("1_keibayosou_pipeline")
features = importlib.import_module("1_keibayosou_features")

NOW_COLUMNS = (
    "レースID", "馬番", "馬名", "頭数", "場所", "コース", "芝・ダ", "距離", "馬場", "クラス", "レース名", "備考",
)
HISTORY_COLUMNS = (
    "馬名", "日付", "着順", "着 順", "人気", "人 気", "距離", "上り", "上り3F", "上り３F", "後3F",
    "着差", "ﾀｲﾑ指数", "タイム指数", "ﾀｲﾑ 指数", "通過", "通過順位", "通過順", "コーナー 通過順",
    "race_id", "レースID", "ラップタイム", "場所", "競馬場", "場名", "開催", "芝・ダ", "芝ダ",
    "コース", "レース名", "クラス", "条件", "頭数", "頭 数", "出走頭数", "馬場", "馬 場", "馬場状態",
    "備考", "脚注", "コメント", "競走状態", "出走状態", "状態", "異常区分",
)
BET_TYPES = {"単勝": 1, "複勝": 1, "3連複": 3}


class RosterUnavailable(ValueError):
    """A〜D で照合可能な完全な出走表を作れない。"""


def checked_path(root, record, key):
    path = (root / record[key]).resolve()
    if sha256_file(path) != record[key + "_sha256"]:
        raise ValueError(f"SHA-256不一致: {path}")
    return path


def load_snapshot(root, record, start, counted, builders, *, book=None):
    """同じ安全な入力を共通特徴量生成関数で2回処理する。"""
    rid = record["race_id"]
    if book is None:
        path = (root / record["workbook"]).resolve()
        content = path.read_bytes()
        if sha256(content).hexdigest() != record["workbook_sha256"]:
            raise ValueError(f"SHA-256不一致: {path}")
        book = pd.read_excel(BytesIO(content), sheet_name=None, engine="openpyxl")
    now = book.get(features.NOW_SHEET)
    if now is None or not {"レースID", "馬番", "馬名", "頭数"}.issubset(now.columns):
        raise RosterUnavailable("今走シートまたは必須列がありません")
    ids = features._normalize_rid_series(now["レースID"])
    now = now.loc[ids.eq(rid), list(now.columns.intersection(NOW_COLUMNS))].copy()
    day = start.date() if isinstance(start, datetime) else start
    now["日付"] = day.strftime("%Y/%m/%d")
    now["馬番"] = now["馬番"].map(integer)
    count = len(now)
    if (not count or now["馬番"].isna().any() or now["馬番"].duplicated().any()
            or now["馬番"].le(0).any() or not now["頭数"].map(integer).eq(count).all()
            or now["馬名"].isna().any() or now["馬名"].astype(str).str.strip().eq("").any()):
        raise RosterUnavailable("発走前の馬番・馬名・頭数が不完全です")
    names = now["馬名"].map(_norm_name)
    if names.eq("").any() or names.duplicated().any():
        raise RosterUnavailable("馬名を一意に照合できません")
    history = book.get(rid, pd.DataFrame(columns=["馬名", "日付"]))
    history = history.loc[:, list(history.columns.intersection(HISTORY_COLUMNS))].copy()
    raw_dates = history.get("日付", pd.Series(None, index=history.index, dtype=object))
    dates = pd.to_datetime(raw_dates.map(features._parse_validation_date))
    past = dates.lt(pd.Timestamp(day))
    counted["history_invalid_date_rows"] += int(dates.isna().sum())
    counted["history_same_or_future_rows"] += int((dates.notna() & ~past).sum())
    history = history.loc[past].copy()
    if "馬名" not in history:
        history["馬名"] = ""
    if history.empty:
        counted["races_without_history"] += 1

    cached = book.get(features.HORSES_SHEET)
    if cached is not None and not cached.empty:
        cached = cached.rename(columns={"umaban": "馬番", "馬 番": "馬番", "馬番 ": "馬番"})
        id_col = "rid_str" if "rid_str" in cached else "レースID"
        cached = cached.loc[features._normalize_rid_series(cached[id_col]).eq(rid)].copy()
        cached["馬番"] = cached["馬番"].map(integer)
        if cached["馬番"].duplicated().any() or not set(cached["馬番"]).issubset(now["馬番"]):
            raise ValueError(f"{rid}: horses シートの馬番が不正です")
        cached = cached[["馬番", *[c for c in FEAT_COLS if c in cached]]].assign(rid_str=rid)
    if cached is None or cached.empty:
        counted["races_without_horses_cache"] += 1

    day_text = day.strftime("%Y%m%d")
    master_frames = []
    sources = (
        ("race_levels", lambda path: pipeline.load_race_levels(str(path), raceday=day_text)),
        ("base_time", lambda path: pipeline.load_base_time(str(path))),
        ("odds_csv", lambda path: pipeline.load_odds_csv(str(path), raceday=day_text)),
    )
    for key, loader in sources:
        if key not in record:
            counted["missing_" + key] += 1
            master_frames.append(None)
            continue
        if timestamp(record[key + "_observed_at"]) > timestamp(record["observed_at"]):
            raise ValueError(f"{rid}: {key} が予想スナップショットより後です")
        frame = loader(checked_path(root, record, key))
        if frame is None or (frame.empty and not frame.attrs):
            counted["empty_" + key] += 1
        master_frames.append(frame)

    # 過去走の元の日付表記を残し、日付の解釈に関する差も再生する。
    with TemporaryDirectory(prefix="jibunnyou-ab-") as tmp:
        workbooks = {}
        for use_cache in (False, True):
            clean = Path(tmp) / f"snapshot_{use_cache}.xlsx"
            with pd.ExcelWriter(clean, engine="openpyxl") as writer:
                now.to_excel(writer, sheet_name=features.NOW_SHEET, index=False)
                history.to_excel(writer, sheet_name=rid, index=False)
                if use_cache and cached is not None:
                    cached.to_excel(writer, sheet_name=features.HORSES_SHEET, index=False)
            workbooks[use_cache] = str(clean)
        frames = {}
        for label, (build, mode, use_cache) in builders.items():
            kwargs = {} if mode is None else {"validation": mode}
            frames[label] = build(workbooks[use_cache], *master_frames, raceday=day_text, **kwargs)

    primary = [frames["legacy"], frames["corrected"]]
    if any(merged.empty for merged, _ in primary):
        if not all(merged.empty for merged, _ in primary):
            raise RosterUnavailable("特徴量方式間で対象レースが異なります")
        return None

    popularity = record.get("popularity", {})
    observed = record.get("observed_at")
    pop_observed = record.get("popularity_observed_at", observed)
    if (popularity or pop_observed) and timestamp(pop_observed) > timestamp(observed):
        raise ValueError(f"{rid}: 人気の取得が予想スナップショットより後です")
    if set(popularity) - {str(n) for n in now["馬番"]}:
        raise ValueError(f"{rid}: 人気の馬番が出走表にありません")
    pop_ranks = {n: integer(popularity.get(str(n))) for n in now["馬番"]}
    ranked = [n for n in pop_ranks.values() if n is not None]
    if len(ranked) != len(set(ranked)) or any(not 1 <= n <= count for n in ranked):
        raise ValueError(f"{rid}: 人気順位が重複または範囲外です")
    counted["horses_without_popularity"] += count - len(ranked)

    dl = record.get("dl", {})
    if dl:
        dl_observed = timestamp(record["dl_observed_at"])
        if not timestamp(record["dl_model_training_end_at"]) < dl_observed <= timestamp(record["observed_at"]):
            raise ValueError(f"{rid}: DL 学習終了・取得時刻が不正です")
        if not re.fullmatch(r"[0-9a-f]{64}", record["dl_model_sha256"]):
            raise ValueError(f"{rid}: DL モデルSHAが不正です")
        if set(dl) != {str(n) for n in now["馬番"]}:
            raise ValueError(f"{rid}: DL スナップショットと出走馬が一致しません")
        if any(type(item["probability"]) not in (int, float) or not 0 <= item["probability"] <= 1
               for item in dl.values()) or sorted(item["rank"] for item in dl.values()) != list(range(1, count + 1)):
            raise ValueError(f"{rid}: DLの確率または順位が不正です")
    else:
        counted["missing_dl"] += 1

    prepared = {}
    for label, (merged, feat) in frames.items():
        if sorted(feat["馬番"].tolist()) != sorted(now["馬番"].tolist()):
            if label in {"legacy", "corrected"}:
                raise RosterUnavailable("特徴量作成後の出走馬が一致しません")
            counted["diagnostic_roster_mismatch_" + label] += 1
            continue
        feat["name_norm"] = feat["馬名"].map(_norm_name)
        first = merged.iloc[0].fillna("")
        feat["place_name"] = pipeline._normalize_place(first["場所"])
        feat["surface_name"] = pipeline._normalize_surface(first["芝・ダ"])
        feat["distance_m"] = _parse_distance_to_int(first["距離"])
        feat["race_class"] = str(first.get("クラス") or first.get("レース名") or "")
        feat["人気"] = feat["馬番"].map(pop_ranks).astype(float)
        feat["頭数"] = count
        feat["馬場"] = str(first["馬場"])
        feat["備考"] = now.set_index("馬番").get("備考", pd.Series(dtype=object)).reindex(feat["馬番"]).fillna("").to_numpy()
        if dl:
            probs = feat["馬番"].map(lambda n: dl[str(n)]["probability"])
            span = probs.max() - probs.min()
            prob_score = (probs - probs.min()) / span if span else 0.5
            ranks = feat["馬番"].map(lambda n: dl[str(n)]["rank"])
            rank_score = (count - ranks) / (count - 1) if count > 1 else 0.5
        else:
            prob_score = rank_score = 0.5
        feat["dl_score"] = prob_score * pipeline.DL_PROB_BLEND + rank_score * pipeline.DL_RANK_BLEND
        prepared[label] = feat

    # 修正式の再計算値を保持し、欠損セルにだけ従来の補完を適用。
    corrected_cache = prepared["corrected"].copy()
    if cached is not None and not cached.empty:
        values = cached.set_index("馬番").reindex(corrected_cache["馬番"]).reset_index(drop=True)
        columns = [col for col in FEAT_COLS if col in values]
        corrected_cache[columns] = corrected_cache[columns].fillna(values[columns].apply(pd.to_numeric, errors="coerce"))
    prepared["corrected_with_cache"] = corrected_cache
    return prepared


def read_results(rows, races, evidence=None, *, starts=None):
    """結果は予想側へ結合せず、集計用の別マップへ格納する。"""
    results = {}
    if evidence is None:
        evidence = {}
    starts = starts if starts is not None else {rid: race["start"] for rid, race in races.items()}
    for row in rows:
        rid = row["race_id"]
        if rid in evidence:
            raise ValueError(f"{rid}: 結果が重複しています")
        proof = evidence[rid] = result_evidence(row, starts.get(rid))
        if proof["grade"] == "D":
            continue
        roster = races[rid]["frames"]["legacy"]["馬番"] if rid in races else None
        top, payouts = result_values(row, roster)
        results[rid] = {"top": top, "payouts": payouts, "evidence": proof}
    return results


def result_values(row, roster=None):
    """確定着順と券種別の払戻が、出走表・組番と整合することを確認する。"""
    rid = row["race_id"]
    top = row.get("finish_top3")
    valid_top = (isinstance(top, list) and len(top) == 3
                 and all(type(n) is int and n > 0 for n in top) and len(set(top)) == 3)
    if roster is not None:
        if valid_top and set(top) - set(roster):
            raise ValueError(f"{rid}: 確定着順に出走表にない馬番があります")
    payouts = {}
    for kind, prices in row.get("payouts", {}).items():
        if (kind not in BET_TYPES or not isinstance(prices, dict)
                or any(not re.fullmatch(r"[1-9]\d*(?:-[1-9]\d*)*", key)
                       or len(set(key.split("-"))) != BET_TYPES[kind]
                       or len(key.split("-")) != BET_TYPES[kind]
                       or tuple(sorted(map(int, key.split("-")))) != tuple(map(int, key.split("-")))
                       or type(value) is not int or value <= 0 for key, value in prices.items())):
            raise ValueError(f"{rid}: 払戻形式が不正です ({kind})")
        if not valid_top:
            continue
        expected = {"単勝": {str(top[0])}, "3連複": {"-".join(map(str, sorted(top)))}}
        if kind == "複勝":
            possible = [{str(n) for n in top[:2]}, {str(n) for n in top}]
            if roster is not None:
                field = len(roster)
                possible = [set() if field < 5 else possible[0 if field < 8 else 1]]
            if set(prices) not in possible:
                raise ValueError(f"{rid}: 複勝払戻の対象馬が不完全です")
        elif set(prices) != expected[kind]:
            raise ValueError(f"{rid}: {kind}払戻の組番が不完全です")
        payouts[kind] = prices
    return top if valid_top else None, payouts


def load_races(root, rows, periods, builders, evaluation_date=None, include_training=True,
               *, legacy=False, reference=False):
    builders = {
        "legacy": (features.build_features_from_excel, False, True),
        "corrected": (features.build_features_from_excel, True, False),
        **builders,
    }
    races = {}
    coverage = Counter()
    missing = {stage: Counter() for stage in periods}
    start_times, excluded = {}, []
    for row in rows:
        rid = row["race_id"]
        if not re.fullmatch(r"\d{10,13}", rid) or rid in start_times:
            raise ValueError(f"レースIDが不正または重複しています: {rid}")
        start = start_times[rid] = timestamp(row["start_at"])
        stage = next((label for label, (lo, hi) in periods.items()
                      if lo <= start.date() and (hi is None or start.date() <= hi)), None)
        if stage is None:
            coverage["outside_periods"] += 1
            excluded.append({"race_id": rid, "reason": "outside_periods"})
            continue
        if (stage == "train" and not include_training) or (
                stage != "train" and evaluation_date and start.date() != evaluation_date):
            coverage["outside_run_scope"] += 1
            excluded.append({"race_id": rid, "stage": stage, "reason": "outside_run_scope"})
            continue
        coverage[stage] += 1
        grade = snapshot_grade(row, legacy=legacy)
        if grade != "A" and not (reference and stage != "train" and grade == "B"):
            reason = f"input_evidence_grade_{grade}"
            missing[stage][reason] += 1
            excluded.append({"race_id": rid, "stage": stage, "reason": reason})
            continue
        observed = timestamp(row["observed_at"])
        if observed >= start:
            raise ValueError(f"{rid}: 発走前スナップショットが必要です")
        if "workbook" not in row:
            missing[stage]["missing_workbook"] += 1
            excluded.append({"race_id": rid, "stage": stage, "reason": "missing_workbook"})
            continue
        try:
            frames = load_snapshot(root, row, start, missing[stage], builders)
        except RosterUnavailable as exc:
            missing[stage]["unusable_roster"] += 1
            excluded.append({"race_id": rid, "stage": stage, "reason": "unusable_roster", "detail": str(exc)})
            continue
        except FileNotFoundError as exc:
            missing[stage]["missing_snapshot_file"] += 1
            excluded.append({"race_id": rid, "stage": stage, "reason": "missing_snapshot_file", "file": str(exc.filename)})
            continue
        if frames is None:
            missing[stage]["excluded_obstacle_races"] += 1
            excluded.append({"race_id": rid, "stage": stage, "reason": "obstacle_race"})
            continue
        races[rid] = {"start": start, "observed": observed, "stage": stage, "frames": frames,
                      "input_evidence_grade": grade,
                      "pop_observed": timestamp(row.get("popularity_observed_at", row["observed_at"]))}
    return races, coverage, missing, excluded


def score(feat, weights):
    race_meta = feat.drop_duplicates("rid_str").set_index("rid_str")
    return pipeline.compute_scores_with_pipeline_logic(
        feat, race_meta["place_name"].to_dict(), race_meta["surface_name"].to_dict(),
        features.build_calc_favorite_risk(), weights_map=weights,
    )


def training_set(races, results, train_end, mode):
    frames, entries = [], []
    for rid, race in races.items():
        result = results.get(rid)
        if (race["stage"] != "train" or not result or not result["top"]
                or not available_by_cutoff(result["evidence"], train_end)):
            continue
        frame = race["frames"][mode]
        frames.append(frame)
        names = frame.set_index("馬番")["name_norm"]
        entries.extend((rid, names[n], i, n) for i, n in enumerate(result["top"], 1))
    if not frames:
        raise ValueError("学習期間内に確定した学習レースがありません")
    return pd.concat(frames, ignore_index=True), pd.DataFrame(
        entries, columns=["rid_str", "name_norm", "着順_num", "馬番_int"],
    )


def feature_missingness(races, stage, mode):
    counts = Counter()
    for race in races.values():
        if race["stage"] == stage:
            counts.update(race["frames"][mode][FEAT_COLS].isna().sum().astype(int).to_dict())
    return dict(counts)
