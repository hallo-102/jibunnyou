# -*- coding: utf-8 -*-
"""ファイル読み込み系の関数。"""

from __future__ import annotations

import importlib
import os
import re
import sys
import unicodedata
from functools import lru_cache
from typing import Optional, Dict, List, Tuple, Any

import pandas as pd


@lru_cache(maxsize=4)
def _read_race_level_book_cached(
    path_abs: str,
    mtime_ns: int,
) -> Dict[str, pd.DataFrame]:
    """更新時刻単位でマスターExcelをキャッシュする。返却側では必ずcopyする。"""
    del mtime_ns  # キャッシュキーとしてのみ使用する。
    return pd.read_excel(path_abs, sheet_name=None, engine="openpyxl")


def _register_renamed_keibayosou_modules() -> None:
    """1_ 始まりへリネームした自作モジュールを、旧import名でも参照できるようにする。"""
    if "keibayosou_utils" not in sys.modules:
        sys.modules["keibayosou_utils"] = importlib.import_module("1_keibayosou_utils")


_register_renamed_keibayosou_modules()

from keibayosou_utils import _normalize_place, _normalize_surface, _to_int


def _normalize_master_date(value: Any) -> pd.Timestamp:
    """マスター内の日付を時系列比較できるTimestampへ正規化する。"""
    if pd.isna(value):
        return pd.NaT
    digits = re.sub(r"\D", "", str(value))
    if len(digits) >= 8:
        return pd.to_datetime(digits[:8], format="%Y%m%d", errors="coerce")
    return pd.to_datetime(value, errors="coerce")


def _build_historical_rating_master(
    book: Dict[str, pd.DataFrame],
    raceday: str,
) -> Tuple[Optional[pd.DataFrame], int]:
    """
    `race_date < RACEDAY` の履歴だけで、予想対象日時点の馬ratingを復元する。

    履歴なし馬は horses.initial_rating へフォールバックし、最新ratingsは
    RACEDAYが未指定の場合に限って従来互換として使用する。
    """
    horses_df = book.get("horses")
    ratings_df = book.get("ratings")
    history_df = book.get("ratings_history")
    races_df = book.get("races")
    target_date = _normalize_master_date(raceday)

    horse_master = pd.DataFrame(columns=["horse_id", "horse_name", "name_norm", "initial_rating"])
    if horses_df is not None and not horses_df.empty:
        horse_master = horses_df.rename(
            columns={"id": "horse_id", "name": "horse_name"}
        ).copy()
        for col in ["horse_id", "horse_name", "initial_rating"]:
            if col not in horse_master.columns:
                horse_master[col] = pd.NA
        horse_master["name_norm"] = horse_master["horse_name"].map(
            lambda s: (
                ""
                if pd.isna(s)
                else unicodedata.normalize("NFKC", str(s))
                .replace("　", "")
                .replace(" ", "")
                .strip()
            )
        )
        horse_master["initial_rating"] = pd.to_numeric(
            horse_master["initial_rating"], errors="coerce"
        )
        horse_master = horse_master[
            ["horse_id", "horse_name", "name_norm", "initial_rating"]
        ].copy()

    # RACEDAY未指定時だけ、従来の最新ratingsを利用する。
    if pd.isna(target_date):
        if ratings_df is None or ratings_df.empty:
            return horse_master, 0
        latest = ratings_df.copy()
        for col in [
            "rating",
            "start_count",
            "recent_rating",
            "rating_confidence",
            "recent_start_count_180d",
            "rating_volatility",
        ]:
            if col not in latest.columns:
                latest[col] = pd.NA
            latest[col] = pd.to_numeric(latest[col], errors="coerce")
        latest["rating_source"] = "latest_ratings_no_raceday"
        latest["rating_asof_date"] = pd.NaT
        return latest, 0

    if (
        history_df is None
        or history_df.empty
        or races_df is None
        or races_df.empty
        or "race_id" not in history_df.columns
        or "race_id" not in races_df.columns
    ):
        fallback = horse_master.copy()
        fallback["rating"] = fallback["initial_rating"]
        fallback["start_count"] = 0
        fallback["recent_rating"] = fallback["initial_rating"]
        fallback["rating_confidence"] = 0.15
        fallback["recent_start_count_180d"] = 0
        fallback["rating_volatility"] = pd.NA
        fallback["rating_source"] = "initial_rating"
        fallback["rating_asof_date"] = pd.NaT
        return fallback[
            [
                "horse_id",
                "rating",
                "start_count",
                "recent_rating",
                "rating_confidence",
                "recent_start_count_180d",
                "rating_volatility",
                "rating_source",
                "rating_asof_date",
            ]
        ], 0

    races = races_df[["race_id", "date"]].copy()
    races["race_date"] = races["date"].map(_normalize_master_date)
    history = history_df.merge(
        races[["race_id", "race_date"]], on="race_id", how="left"
    )
    future_or_same = history["race_date"].notna() & (
        history["race_date"] >= target_date
    )
    future_excluded_count = int(future_or_same.sum())
    history = history[
        history["race_date"].notna() & (history["race_date"] < target_date)
    ].copy()

    rating_candidates = [
        "post_overall_rating",
        "post_rating",
        "post_surface_rating",
    ]
    for col in rating_candidates:
        if col not in history.columns:
            history[col] = pd.NA
        history[col] = pd.to_numeric(history[col], errors="coerce")
    history["historical_rating"] = history[rating_candidates].bfill(axis=1).iloc[:, 0]
    history = history.sort_values(
        ["horse_id", "race_date", "race_id"], kind="mergesort"
    )

    rows: List[Dict[str, Any]] = []
    recent_cutoff = target_date - pd.Timedelta(days=180)
    for horse_id, group in history.groupby("horse_id", sort=False):
        valid = group.dropna(subset=["historical_rating"])
        if valid.empty:
            continue
        latest = valid.iloc[-1]
        recent_values = valid.tail(3)["historical_rating"]
        rating_deltas = valid["historical_rating"].diff().dropna()
        start_count = int(valid["race_id"].nunique())
        rows.append(
            {
                "horse_id": horse_id,
                "rating": float(latest["historical_rating"]),
                "start_count": start_count,
                "recent_rating": float(recent_values.mean()),
                "rating_confidence": min(1.0, 0.25 + 0.15 * start_count),
                "recent_start_count_180d": int(
                    valid.loc[valid["race_date"] >= recent_cutoff, "race_id"].nunique()
                ),
                "rating_volatility": (
                    float(rating_deltas.std(ddof=0))
                    if not rating_deltas.empty
                    else 0.0
                ),
                "rating_source": "ratings_history",
                "rating_asof_date": latest["race_date"],
            }
        )

    historical = pd.DataFrame(rows)
    merged = horse_master.merge(historical, on="horse_id", how="left")
    no_history = merged["rating"].isna()
    merged.loc[no_history, "rating"] = merged.loc[no_history, "initial_rating"]
    merged.loc[no_history, "recent_rating"] = merged.loc[
        no_history, "initial_rating"
    ]
    merged.loc[no_history, "start_count"] = 0
    merged.loc[no_history, "recent_start_count_180d"] = 0
    merged.loc[no_history, "rating_confidence"] = 0.15
    merged.loc[no_history, "rating_source"] = "initial_rating"
    return merged[
        [
            "horse_id",
            "rating",
            "start_count",
            "recent_rating",
            "rating_confidence",
            "recent_start_count_180d",
            "rating_volatility",
            "rating_source",
            "rating_asof_date",
        ]
    ], future_excluded_count


def load_race_levels(path: str, raceday: str = "") -> pd.DataFrame:
    """
    race_levels.xlsx 読み込み（現行フォーマットに合わせて拡張）

    期待シート:
      - race_levels: race_id, race_level_score, pre_mean, pre_p50, pre_top5_mean
      - entries: race_id, horse_id
      - horses: id, name
      - ratings: horse_id, rating

    仕様:
      - horses と ratings を JOIN して horse_id→name→name_norm→rating を解決
      - race_level は race_levels.race_level_score を優先し、無ければ pre_top5_mean や ratings 上位5頭平均を使用
      - 戻り値は rid_str, race_level（race_level_score優先→pre_top5_mean→pre_mean→ratingsベース）を含む DataFrame
    """
    if not os.path.exists(path):
        print("[INFO] race_levels.xlsx が見つからないため、全て NaN 扱いにします")
        return pd.DataFrame(columns=["rid_str", "race_level"])

    try:
        path_abs = os.path.abspath(path)
        mtime_ns = int(os.stat(path_abs).st_mtime_ns)
        cached_book = _read_race_level_book_cached(path_abs, mtime_ns)
        book = {name: frame.copy() for name, frame in cached_book.items()}
    except Exception as e:
        print(f"[WARN] race_levels.xlsx の読み込みに失敗しました: {e}")
        return pd.DataFrame(columns=["rid_str", "race_level"])

    def _norm_name(s):
        if pd.isna(s):
            return ""
        return (
            unicodedata.normalize("NFKC", str(s))
            .replace("　", "")
            .replace(" ", "")
            .strip()
        )

    race_levels_df = book.get("race_levels")
    entries_df = book.get("entries")
    horses_df = book.get("horses")
    ratings_df = book.get("ratings")
    races_df = book.get("races")
    rating_master, future_history_excluded_count = _build_historical_rating_master(
        book, raceday
    )

    # horse_id -> name_norm, rating
    horse_master = None
    if horses_df is not None:
        tmp = horses_df.rename(columns={"id": "horse_id", "name": "horse_name"})
        tmp["horse_id"] = tmp.get("horse_id")
        tmp["horse_name"] = tmp.get("horse_name")
        tmp["name_norm"] = tmp["horse_name"].map(_norm_name)
        horse_master = tmp[["horse_id", "horse_name", "name_norm"]]

    if rating_master is None and ratings_df is not None:
        tmp = ratings_df.rename(columns={"horse_id": "horse_id", "rating": "rating"}).copy()
        rating_cols = [
            "horse_id",
            "rating",
            "start_count",
            "recent_rating",
            "rating_confidence",
            "recent_start_count_180d",
            "rating_volatility",
        ]
        for col in rating_cols:
            if col not in tmp.columns:
                tmp[col] = pd.NA
        for col in rating_cols:
            if col != "horse_id":
                tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
        rating_master = tmp[rating_cols].copy()

    # 過去バックテストでは、予想日以降のrace_level自体も参照対象から除外する。
    target_date = _normalize_master_date(raceday)
    allowed_race_ids: Optional[set] = None
    if (
        pd.notna(target_date)
        and races_df is not None
        and not races_df.empty
        and {"race_id", "date"}.issubset(races_df.columns)
    ):
        race_dates = races_df[["race_id", "date"]].copy()
        race_dates["_race_date"] = race_dates["date"].map(_normalize_master_date)
        allowed_race_ids = set(
            race_dates.loc[
                race_dates["_race_date"].notna()
                & (race_dates["_race_date"] < target_date),
                "race_id",
            ].tolist()
        )
        if entries_df is not None and "race_id" in entries_df.columns:
            entries_df = entries_df[entries_df["race_id"].isin(allowed_race_ids)].copy()
        if race_levels_df is not None and "race_id" in race_levels_df.columns:
            race_levels_df = race_levels_df[
                race_levels_df["race_id"].isin(allowed_race_ids)
            ].copy()

    entries_with_rating = None
    if entries_df is not None and rating_master is not None:
        entries_with_rating = entries_df.merge(rating_master, on="horse_id", how="left")
        if horse_master is not None:
            entries_with_rating = entries_with_rating.merge(horse_master, on="horse_id", how="left")

    rating_mean_map = {}
    rating_top5_map = {}
    if entries_with_rating is not None and not entries_with_rating.empty:
        grp = entries_with_rating.groupby("race_id")["rating"]
        rating_mean_map = grp.mean().to_dict()

        # 上位5頭平均
        def _top5_mean(s: pd.Series):
            s = s.dropna().sort_values(ascending=False)
            if s.empty:
                return None
            return s.head(5).mean()

        rating_top5_map = grp.apply(_top5_mean).to_dict()

    if race_levels_df is None:
        # race_levels シートが無い場合は ratings 由来の情報のみで構築
        if not rating_top5_map and not rating_mean_map:
            print("[WARN] race_levels シートが無く ratings 由来のレベルも算出できません（未使用で続行）")
            return pd.DataFrame(columns=["rid_str", "race_level"])

        keys = list(rating_top5_map.keys()) if rating_top5_map else list(rating_mean_map.keys())
        df = pd.DataFrame(
            {
                "rid_str": [str(k) for k in keys],
                "race_level": [
                    rating_top5_map.get(k) if rating_top5_map.get(k) is not None else rating_mean_map.get(k)
                    for k in keys
                ],
            }
        )
        if horse_master is not None and rating_master is not None:
            df.attrs["horse_ratings"] = horse_master.merge(rating_master, on="horse_id", how="left")
        df.attrs["rating_raceday"] = str(raceday or "")
        df.attrs["future_history_excluded_count"] = future_history_excluded_count
        return df

    rl = race_levels_df.copy()
    if "race_id" not in rl.columns:
        print("[WARN] race_levels シートに race_id 列が無いため、未使用で続行します")
        return pd.DataFrame(columns=["rid_str", "race_level"])

    rl["rid_str"] = rl["race_id"].astype(str)
    rl["race_level_score"] = pd.to_numeric(rl.get("race_level_score"), errors="coerce")
    rl["pre_mean"] = pd.to_numeric(rl.get("pre_mean"), errors="coerce")
    rl["pre_top5_mean"] = pd.to_numeric(rl.get("pre_top5_mean"), errors="coerce")

    # race_level 優先順位: race_level_score -> pre_top5_mean -> pre_mean -> ratings上位5平均 -> ratings平均
    rl["race_level"] = rl["race_level_score"]
    rl.loc[rl["race_level"].isna(), "race_level"] = rl.loc[rl["race_level"].isna(), "pre_top5_mean"]
    rl.loc[rl["race_level"].isna(), "race_level"] = rl.loc[rl["race_level"].isna(), "pre_mean"]
    rl.loc[rl["race_level"].isna(), "race_level"] = rl.loc[rl["race_level"].isna(), "rid_str"].map(rating_top5_map)
    rl.loc[rl["race_level"].isna(), "race_level"] = rl.loc[rl["race_level"].isna(), "rid_str"].map(rating_mean_map)

    out = rl[["rid_str", "race_level", "race_level_score", "pre_mean", "pre_top5_mean"]].copy()
    if horse_master is not None and rating_master is not None:
        out.attrs["horse_ratings"] = horse_master.merge(rating_master, on="horse_id", how="left")
    out.attrs["rating_raceday"] = str(raceday or "")
    out.attrs["future_history_excluded_count"] = future_history_excluded_count
    return out


def load_base_time(path: str) -> pd.DataFrame:
    """
    基準タイムファイルを読み込み。
    優先: 「場所_馬場_タイム.xlsx」
    フォールバック: 「base_time.xlsx」
    カラム構成は2パターンをサポート：
      1) place, surface, distance, base_time
      2) 場所, 馬場, 距離, タイム
    それ以外の場合は警告を出して未使用。
    """
    # path が存在しない場合は base_time.xlsx をフォールバック
    if not os.path.exists(path):
        alt = os.path.join(os.path.dirname(path), "base_time.xlsx")
        if not os.path.exists(alt):
            print("[INFO] 基準タイムファイルが見つからないため、全て NaN 扱いにします")
            return pd.DataFrame(columns=["place", "surface", "distance", "base_time"])
        else:
            print(f"[INFO] {os.path.basename(path)} が無いので base_time.xlsx を使用します")
            path = alt

    df = pd.read_excel(path, engine="openpyxl")
    cols = set(df.columns)

    # パターン1: すでに place/surface/distance/base_time がある
    if {"place", "surface", "distance", "base_time"}.issubset(cols):
        out = df[["place", "surface", "distance", "base_time"]].copy()
        out["place"] = out["place"].map(_normalize_place)
        out["surface"] = out["surface"].map(_normalize_surface)
        return out

    # パターン2: 日本語列名（場所, 馬場, 距離, タイム）
    if {"場所", "馬場", "距離", "タイム"}.issubset(cols):
        tmp = df[["場所", "馬場", "距離", "タイム"]].copy()
        tmp = tmp.rename(
            columns={
                "場所": "place",
                "馬場": "surface",
                "距離": "distance",
                "タイム": "base_time",
            }
        )
        tmp["place"] = tmp["place"].map(_normalize_place)
        tmp["surface"] = tmp["surface"].map(_normalize_surface)
        return tmp[["place", "surface", "distance", "base_time"]]

    # パターン3: 「場所/コース/タイム」形式（例: コース="ダ1200", "芝1800"）
    if {"場所", "コース", "タイム"}.issubset(cols):
        tmp = df[["場所", "コース", "タイム"]].copy()
        tmp = tmp.rename(columns={"場所": "place", "コース": "course", "タイム": "base_time"})

        def _parse_distance_from_course(course: object) -> float:
            if pd.isna(course):
                return float("nan")
            m = re.search(r"(\d{3,4})", str(course))
            return float(m.group(1)) if m else float("nan")

        tmp["place"] = tmp["place"].map(_normalize_place)
        tmp["surface"] = tmp["course"].map(_normalize_surface)
        tmp["distance"] = tmp["course"].map(_parse_distance_from_course)
        tmp["base_time"] = pd.to_numeric(tmp["base_time"], errors="coerce")

        out = tmp[["place", "surface", "distance", "base_time"]].copy()
        out = out.dropna(subset=["place", "surface", "distance", "base_time"])
        out["distance"] = pd.to_numeric(out["distance"], errors="coerce").astype("int64")
        out = out.groupby(["place", "surface", "distance"], as_index=False)["base_time"].median()
        return out

    print("[WARN] 基準タイムファイルの列構成が想定外のため未使用で続行します")
    return pd.DataFrame(columns=["place", "surface", "distance", "base_time"])


# =========================
# odds CSV 読み込み強化
# =========================
def _read_csv_with_fallback(csv_path: str) -> pd.DataFrame:
    """CSVを文字コードゆらぎ込みで読む（utf-8 / utf-8-sig / cp932 など）"""
    last_err: Optional[Exception] = None
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis", "utf-16"):
        try:
            return pd.read_csv(csv_path, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    if last_err is None:
        raise RuntimeError("CSV読み込みに失敗しました（原因不明）")
    raise last_err


def _norm_header(x: object) -> str:
    """ヘッダ名の比較用正規化（全角/半角・空白ゆらぎに強くする）"""
    s = "" if x is None else str(x)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\s\u3000]+", "", s)
    s = s.replace("：", ":")
    return s


def _rename_odds_columns_if_possible(df: pd.DataFrame) -> pd.DataFrame:
    """
    すでに rid_str/umaban/tansho が無いが、日本語列名等で入っている場合に寄せる。
    """
    norm_to_orig = {_norm_header(c): c for c in df.columns}

    rid_alias = ["rid_str", "rid", "raceid", "race_id", "レースID", "レースId", "レースＩＤ"]
    uma_alias = ["umaban", "馬番", "馬 番", "馬_番"]
    tan_alias = ["tansho", "単勝", "単勝オッズ", "単勝 ｵｯｽﾞ", "単勝オッズ(倍)", "単勝 オッズ"]

    def pick(alias_list: List[str]) -> Optional[str]:
        for a in alias_list:
            na = _norm_header(a)
            if na in norm_to_orig:
                return norm_to_orig[na]
        # 部分一致（最後の保険）
        for na, orig in norm_to_orig.items():
            for a in alias_list:
                if _norm_header(a) in na:
                    return orig
        return None

    rid_col = pick(rid_alias)
    uma_col = pick(uma_alias)
    tan_col = pick(tan_alias)

    rename_map: Dict[str, str] = {}
    if rid_col is not None and rid_col != "rid_str":
        rename_map[rid_col] = "rid_str"
    if uma_col is not None and uma_col != "umaban":
        rename_map[uma_col] = "umaban"
    if tan_col is not None and tan_col != "tansho":
        rename_map[tan_col] = "tansho"

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _race_to_no(r: Any) -> str:
    """'11R' -> '11'（2桁ゼロ埋め）"""
    m = re.search(r"(\d+)", str(r))
    return m.group(1).zfill(2) if m else ""


def _odds_to_float(x: object) -> Optional[float]:
    """オッズ文字列を float に変換する。"""
    s = str(x).replace(",", "")
    if not re.search(r"\d", s):
        return None
    try:
        return float(s)
    except Exception:
        return None


def _fukusho_to_lower_float(x: object) -> Optional[float]:
    """複勝オッズが範囲表記の場合は下限を返す。"""
    s = str(x).replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _combination_to_umaban(x: object) -> Optional[int]:
    """OZZUの組番から単勝・複勝用の馬番を取り出す。"""
    m = re.search(r"(\d+)", str(x))
    return int(m.group(1)) if m else None


def _normalize_horse_name_for_key(x: object) -> str:
    """オッズ照合キー用に馬名の全角半角・空白ゆらぎを吸収する。"""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = unicodedata.normalize("NFKC", str(x))
    s = re.sub(r"[\s\u3000]+", "", s)
    return s.strip()


def _format_duplicate_sample(df: pd.DataFrame, cols: List[str], limit: int = 10) -> List[Dict[str, object]]:
    """重複エラーに表示するサンプル行を作る。"""
    sample_cols = [c for c in cols if c in df.columns]
    return df[sample_cols].head(limit).to_dict("records")


def _raise_on_duplicate_keys(
    df: pd.DataFrame,
    key_cols: List[str],
    context: str,
    horse_name_col: Optional[str] = None,
) -> None:
    """同一キーが複数ある場合は、黙って先勝ちにせず例外で停止する。"""
    if df.empty:
        return

    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{context} の重複検査に必要な列が不足しています: {missing}")

    dup = df[df.duplicated(subset=key_cols, keep=False)].copy()
    if dup.empty:
        return

    if horse_name_col is not None and horse_name_col in dup.columns:
        work = dup.copy()
        work["_horse_name_norm_for_key"] = work[horse_name_col].map(_normalize_horse_name_for_key)
        bad_name_keys = (
            work.groupby(key_cols)["_horse_name_norm_for_key"]
            .nunique(dropna=True)
            .loc[lambda s: s > 1]
        )
        if not bad_name_keys.empty:
            sample = _format_duplicate_sample(work, key_cols + [horse_name_col, "_horse_name_norm_for_key"])
            raise ValueError(
                f"{context} で同一キーに複数の馬名を検知しました。"
                f" key_cols={key_cols}"
                f" sample={sample}"
            )

    sample_cols = key_cols + ([horse_name_col] if horse_name_col and horse_name_col in dup.columns else [])
    sample = _format_duplicate_sample(dup, sample_cols)
    raise ValueError(
        f"{context} で重複データを検知しました。keep='first' で処理せず中止します。"
        f" key_cols={key_cols}"
        f" sample={sample}"
    )


def _convert_ozzu_to_odds(df: pd.DataFrame, raceday: Optional[str] = None) -> pd.DataFrame:
    """
    OZZU形式:
      date,racecourse,race,name,bet_type,combination,odds
    を
      date, place, race_no, umaban, name, name_norm, tansho, fukusho, ozzu_key
    に変換する。

    OZZU CSVはJRA公式由来で netkeiba race_id を持たないため、
    rid_str は作らず、日付・場所・R番号・馬番・馬名で照合できる形にする。
    """
    out_cols = ["date", "place", "race_no", "umaban", "name", "name_norm", "tansho", "fukusho", "ozzu_key"]
    need = {"date", "racecourse", "race", "name", "bet_type", "combination", "odds"}
    if not need.issubset(set(df.columns)):
        raise ValueError(f"OZZU形式として必要列が不足: need={sorted(need)} actual={list(df.columns)}")

    d = df.copy()

    d["date"] = d["date"].astype(str).str.replace(r"\D+", "", regex=True)
    raceday_digits = re.sub(r"\D+", "", str(raceday)) if raceday else ""
    if re.fullmatch(r"\d{8}", raceday_digits):
        d = d[d["date"] == raceday_digits].copy()
    if d.empty:
        return pd.DataFrame(columns=out_cols)

    d["place"] = d["racecourse"].map(_normalize_place)
    d["race_no"] = d["race"].apply(_race_to_no)
    d["umaban"] = d["combination"].apply(_combination_to_umaban)
    d["bet_type"] = d["bet_type"].fillna("").astype(str).str.strip()
    d["name"] = d["name"].fillna("").astype(str).str.strip()
    d["name_norm"] = d["name"].map(_normalize_horse_name_for_key)
    d = d[
        d["date"].astype(str).str.fullmatch(r"\d{8}", na=False)
        & d["place"].astype(str).str.strip().ne("")
        & d["race_no"].astype(str).str.fullmatch(r"\d{2}", na=False)
        & d["umaban"].notna()
        & d["name_norm"].astype(str).str.strip().ne("")
    ].copy()
    if d.empty:
        return pd.DataFrame(columns=out_cols)

    _raise_on_duplicate_keys(
        d,
        # 過去のOZZU CSVには、同一日・場・R・馬番へ別レースの馬名が
        # 混在したファイルがある。後段の照合は馬名もキーに使うため、
        # 馬名まで同一なのに重複する場合だけ矛盾として停止する。
        ["date", "place", "race_no", "umaban", "bet_type", "name_norm"],
        "OZZU CSV",
        horse_name_col="name",
    )

    base_cols = ["date", "place", "race_no", "umaban", "name_norm"]
    tansho = d[d["bet_type"].astype(str).str.contains("単勝", na=False)].copy()
    tansho["tansho"] = tansho["odds"].apply(_odds_to_float)
    tansho = tansho.dropna(subset=base_cols + ["tansho"])
    _raise_on_duplicate_keys(tansho, base_cols, "OZZU単勝オッズ", horse_name_col="name")
    tansho = tansho[base_cols + ["name", "tansho"]].copy()

    fukusho = d[d["bet_type"].astype(str).str.contains("複勝", na=False)].copy()
    fukusho["fukusho"] = fukusho["odds"].apply(_fukusho_to_lower_float)
    fukusho = fukusho.dropna(subset=base_cols + ["fukusho"])
    _raise_on_duplicate_keys(fukusho, base_cols, "OZZU複勝オッズ", horse_name_col="name")
    fukusho = fukusho[base_cols + ["name", "fukusho"]].copy()

    out = pd.merge(tansho, fukusho, on=base_cols, how="outer", suffixes=("_tansho", "_fukusho"))
    if out.empty:
        return pd.DataFrame(columns=out_cols)

    out["name"] = out.get("name_tansho", pd.Series(pd.NA, index=out.index)).combine_first(
        out.get("name_fukusho", pd.Series(pd.NA, index=out.index))
    )
    out["date"] = out["date"].astype(str)
    out["place"] = out["place"].astype(str)
    out["race_no"] = out["race_no"].astype(str).str.zfill(2)
    out["umaban"] = out["umaban"].astype(int)
    out["tansho"] = pd.to_numeric(out.get("tansho"), errors="coerce")
    out["fukusho"] = pd.to_numeric(out.get("fukusho"), errors="coerce")
    out["ozzu_key"] = (
        out["date"]
        + "_"
        + out["place"]
        + "_"
        + out["race_no"]
        + "_"
        + out["umaban"].astype(str)
        + "_"
        + out["name_norm"].astype(str)
    )
    return out[out_cols]


def load_odds_csv(path: str, raceday: Optional[str] = None) -> pd.DataFrame:
    """
    オッズCSVを読み込む。

    対応形式:
      A) すでに rid_str, umaban, tansho がある（標準形式）
      B) 日本語列名など（例：レースID/馬番/単勝オッズ）→自動リネーム
      C) OZZU形式（date,racecourse,race,name,bet_type,combination,odds）
         → date/place/race_no/umaban/tansho/fukusho/ozzu_key に変換

    path がディレクトリの場合:
      - raceday(YYYYMMDD) を含むCSVがあれば優先
      - 無ければ更新日時が最新のCSVを使う
    """
    if not os.path.exists(path):
        print("[INFO] オッズCSVが見つからないため、オッズ系特徴量は一部NaNになります")
        return pd.DataFrame(columns=["rid_str", "umaban", "tansho"])

    csv_path = path
    if os.path.isdir(path):
        csv_files = [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".csv")
        ]
        if not csv_files:
            print("[WARN] オッズCSVが見つからないため、空のDataFrameで続行します")
            return pd.DataFrame(columns=["rid_str", "umaban", "tansho"])

        preferred: List[str] = []
        if raceday and re.fullmatch(r"\d{8}", str(raceday)):
            preferred = [p for p in csv_files if str(raceday) in os.path.basename(p)]

        pick_list = preferred if preferred else csv_files
        csv_path = max(pick_list, key=lambda p: os.path.getmtime(p))

    # 1) 文字コードゆらぎ込みで読む
    df = _read_csv_with_fallback(csv_path)

    # 2) 標準形式へ寄せられるなら寄せる
    df2 = _rename_odds_columns_if_possible(df)

    need = {"rid_str", "umaban", "tansho"}
    if need.issubset(df2.columns):
        out = df2[["rid_str", "umaban", "tansho"]].copy()
        out["rid_str"] = out["rid_str"].map(lambda x: re.sub(r"\D+", "", str(x)) if pd.notna(x) else "")
        out["umaban"] = out["umaban"].map(lambda x: _to_int(x) if pd.notna(x) else None)
        out["tansho"] = out["tansho"].map(
            lambda x: float(str(x).replace(",", "")) if pd.notna(x) and re.search(r"\d", str(x)) else None
        )
        out = out.dropna(subset=["rid_str", "umaban", "tansho"])
        out["umaban"] = out["umaban"].astype(int)
        out["tansho"] = pd.to_numeric(out["tansho"], errors="coerce")
        out = out.dropna(subset=["tansho"])
        out = out[out["rid_str"].astype(str).str.strip().ne("")]
        _raise_on_duplicate_keys(out, ["rid_str", "umaban"], "標準オッズCSV")
        return out

    # 3) OZZU形式なら変換（あなたの 1_02_scrape_jra_odds_2.py の出力に対応）
    ozzu_need = {"date", "racecourse", "race", "bet_type", "combination", "odds"}
    if ozzu_need.issubset(set(df.columns)):
        return _convert_ozzu_to_odds(df, raceday=raceday)

    # 4) ここまで来たら形式不明 → 列名とファイルを出してエラー
    raise ValueError(
        "オッズCSVの形式が想定外です。"
        " / 必要: rid_str, umaban, tansho（標準形式）または OZZU形式"
        f" / 実際の列={list(df.columns)}"
        f" / 読み込んだファイル={csv_path}"
    )
