# -*- coding: utf-8 -*-
"""ファイル読み込み系の関数。"""

from __future__ import annotations

import os
import re
import unicodedata
from typing import Optional, Dict, List, Tuple, Any

import pandas as pd

from keibayosou_utils import _normalize_place, _normalize_surface, _to_int


def load_race_levels(path: str) -> pd.DataFrame:
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
        book = pd.read_excel(path, sheet_name=None, engine="openpyxl")
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

    # horse_id -> name_norm, rating
    horse_master = None
    if horses_df is not None:
        tmp = horses_df.rename(columns={"id": "horse_id", "name": "horse_name"})
        tmp["horse_id"] = tmp.get("horse_id")
        tmp["horse_name"] = tmp.get("horse_name")
        tmp["name_norm"] = tmp["horse_name"].map(_norm_name)
        horse_master = tmp[["horse_id", "horse_name", "name_norm"]]

    rating_master = None
    if ratings_df is not None:
        tmp = ratings_df.rename(columns={"horse_id": "horse_id", "rating": "rating"})
        tmp["rating"] = pd.to_numeric(tmp.get("rating"), errors="coerce")
        rating_master = tmp[["horse_id", "rating"]]

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

    return rl[["rid_str", "race_level", "race_level_score", "pre_mean", "pre_top5_mean"]]


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


def _convert_ozzu_to_odds(df: pd.DataFrame) -> pd.DataFrame:
    """
    OZZU形式:
      date,racecourse,race,name,bet_type,combination,odds
    を
      rid_str, umaban, tansho
    に変換する（単勝のみ利用）
    """
    need = {"date", "racecourse", "race", "bet_type", "combination", "odds"}
    if not need.issubset(set(df.columns)):
        raise ValueError(f"OZZU形式として必要列が不足: need={sorted(need)} actual={list(df.columns)}")

    # 競馬場コード（JRA一般のコード）
    place_code_map = {
        "札幌": "01",
        "函館": "02",
        "福島": "03",
        "新潟": "04",
        "東京": "05",
        "中山": "06",
        "中京": "07",
        "京都": "08",
        "阪神": "09",
        "小倉": "10",
    }

    d = df.copy()
    d = d[d["bet_type"].astype(str).str.contains("単勝", na=False)].copy()

    # date: 数字だけ
    d["date"] = d["date"].astype(str).str.replace(r"\D+", "", regex=True)

    # racecourse: 空白除去
    d["racecourse"] = d["racecourse"].astype(str).str.replace(r"[\s\u3000]+", "", regex=True)

    d["race_no"] = d["race"].apply(_race_to_no)
    d["place_code"] = d["racecourse"].map(place_code_map)

    if d["place_code"].isna().any():
        bad = d[d["place_code"].isna()]["racecourse"].dropna().unique().tolist()
        raise ValueError(f"競馬場名→コード変換に失敗: {bad} / place_code_map に追記してください")

    d["rid_str"] = d["date"] + d["place_code"] + d["race_no"]
    d["umaban"] = d["combination"].apply(lambda x: _to_int(x))
    d["tansho"] = d["odds"].apply(
        lambda x: float(str(x).replace(",", "")) if re.search(r"\d", str(x)) else None
    )

    out = d[["rid_str", "umaban", "tansho"]].copy()
    out = out.dropna(subset=["rid_str", "umaban", "tansho"])
    out["rid_str"] = out["rid_str"].astype(str).str.replace(r"\D+", "", regex=True)
    out["umaban"] = out["umaban"].astype(int)
    out["tansho"] = pd.to_numeric(out["tansho"], errors="coerce")
    out = out.dropna(subset=["tansho"])
    return out


def load_odds_csv(path: str, raceday: Optional[str] = None) -> pd.DataFrame:
    """
    オッズCSVを読み込み、最低限必要な列（rid_str, umaban, tansho）を返す。

    対応形式:
      A) すでに rid_str, umaban, tansho がある（標準形式）
      B) 日本語列名など（例：レースID/馬番/単勝オッズ）→自動リネーム
      C) OZZU形式（date,racecourse,race,name,bet_type,combination,odds）→単勝だけ変換

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
        return out

    # 3) OZZU形式なら変換（あなたの 02_scrape_jra_odds_2.py の出力に対応）
    ozzu_need = {"date", "racecourse", "race", "bet_type", "combination", "odds"}
    if ozzu_need.issubset(set(df.columns)):
        return _convert_ozzu_to_odds(df)

    # 4) ここまで来たら形式不明 → 列名とファイルを出してエラー
    raise ValueError(
        "オッズCSVの形式が想定外です。"
        " / 必要: rid_str, umaban, tansho（または OZZU形式）"
        f" / 実際の列={list(df.columns)}"
        f" / 読み込んだファイル={csv_path}"
    )
