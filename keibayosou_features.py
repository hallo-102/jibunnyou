# -*- coding: utf-8 -*-
from __future__ import annotations

"""
keibayosou_features.py（完全版）

提供する関数（pipeline が import しているもの）
- apply_weights
- score_sum
- normalize_score
- build_calc_favorite_risk
- build_features_from_excel

今回の修正ポイント
- apply_weights の weights を省略可能にして、place/surface から自動で重みを選ぶ
- 条件一致の新特徴量を追加
  - 同距離だけで比較
  - 同競馬場だけで比較
  - 芝/ダ別で比較
  - クラス別で比較
  - ラップから簡易ペース(slow/mid/fast)を判定
  - その条件一致の中で「上がり3Fの価値」を作る
- 今走のペース列がある場合はそれも条件一致に使う
修正必要箇所以外は既存のまま
"""

from typing import Any, Callable, Dict, Optional, Tuple

import re
import numpy as np
import pandas as pd

from keibayosou_config import (
    NOW_SHEET,
    TARGET_SHEET,
    HORSES_SHEET,
    FEAT_COLS,
    OUT_COLS,
    FEATURE_WEIGHTS,
    FEATURE_WEIGHTS_BY_PLACE_SURFACE,
)
from keibayosou_utils import _ensure_rid_str


# =============================================================================
# 小さな共通関数
# =============================================================================
def _to_str(x: Any) -> str:
    return "" if x is None else str(x)


def _to_float(x: Any) -> float:
    try:
        if x is None:
            return float("nan")
        s = str(x).strip()
        if s == "":
            return float("nan")
        return float(s)
    except Exception:
        return float("nan")


def _safe_z(x: pd.Series) -> pd.Series:
    """平均との差 / 標準偏差（標準偏差=0やNaNなら0にする）"""
    m = x.mean(skipna=True)
    s = x.std(skipna=True)
    if pd.isna(s) or s == 0:
        return pd.Series([0.0] * len(x), index=x.index)
    return (x - m) / s


def _normalize_rid_series(s: pd.Series) -> pd.Series:
    """
    rid_str の dtype ゆれ対策。
    - Excel 由来で float64（末尾 .0）になるケースを Int64 → 文字列に寄せる
    - 文字列の場合も数字以外を除去して寄せる
    """
    s_digits = s.astype(str).str.strip().str.replace(r"\D", "", regex=True)
    num = pd.to_numeric(s, errors="coerce")
    num_str = num.astype("Int64").astype(str).replace("<NA>", pd.NA)
    out = s_digits.where(s_digits.notna() & (s_digits != ""), num_str).fillna("").astype(str)
    out = out.map(lambda x: x[-12:] if isinstance(x, str) and len(x) > 12 else x)
    return out


def _normalize_umaban_series(s: pd.Series) -> pd.Series:
    """馬番の dtype ゆれ対策（Int64に寄せる）。"""
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def _clean_colname(col: Any) -> str:
    """列名の空白ゆらぎ（例: '人 気', '着 順'）を吸収するための正規化。"""
    s = "" if col is None else str(col)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    return s


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """df.columns から候補のどれかに一致する列名を返す（空白ゆらぎを吸収）。"""
    norm_map = {_clean_colname(c): str(c) for c in df.columns}
    for cand in candidates:
        key = _clean_colname(cand)
        if key in norm_map:
            return norm_map[key]
    return None


def _to_float_series(s: pd.Series) -> pd.Series:
    """数字以外が混ざる列も数値化する。"""
    t = s.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(t, errors="coerce")


def _parse_distance_m(x: Any) -> Optional[int]:
    """'ダ1400' / '芝1800' / '1400' のような距離表記からmを抽出する。"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    m = re.search(r"(\d{3,4})", str(x))
    return int(m.group(1)) if m else None


def _parse_surface(x: Any) -> str:
    """'芝1800' / 'ダ1400' / '芝' / 'ダ' から芝ダを抽出する。"""
    if x is None:
        return ""
    s = str(x)
    if "芝" in s:
        return "芝"
    if "ダ" in s:
        return "ダ"
    if "障" in s:
        return "障害"
    return ""


def _parse_yyyymmdd(x: Any) -> Optional[pd.Timestamp]:
    """日付（YYYY/MM/DD 等）を Timestamp にする。"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, pd.Timestamp):
        return x
    s = re.sub(r"[^\d]", "", str(x))
    if len(s) == 8:
        try:
            return pd.to_datetime(s, format="%Y%m%d")
        except Exception:
            return None
    return None


def _parse_laps_to_min_1f(lap_text: Any) -> Optional[float]:
    """
    '12.6 - 10.9 - 11.9 ...' のようなラップ文字列から
    そのレースの最速1F（最小値）を返す。
    """
    if lap_text is None or (isinstance(lap_text, float) and np.isnan(lap_text)):
        return None
    nums = re.findall(r"\d+\.\d+", str(lap_text))
    if not nums:
        return None
    try:
        laps = [float(x) for x in nums]
        return float(min(laps)) if laps else None
    except Exception:
        return None


def _normalize_place_text(x: Any) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    s = re.sub(r"競馬場", "", s)
    return s


def _bucket_distance(dist: Optional[int]) -> str:
    if dist is None or (isinstance(dist, float) and np.isnan(dist)):
        return ""
    try:
        d = int(dist)
    except Exception:
        return ""
    if d <= 1400:
        return "short"
    if d <= 1800:
        return "mile_mid"
    if d <= 2200:
        return "middle"
    return "long"


def _parse_class_name_from_text(text_val: Any) -> str:
    if text_val is None or (isinstance(text_val, float) and np.isnan(text_val)):
        return ""
    s = _to_str(text_val)
    s = s.replace("　", "").replace(" ", "")
    s = s.upper()

    if "G1" in s or "GI" in s or "ＧⅠ" in s or "Ｇ1" in s or "JPN1" in s:
        return "G1"
    if "G2" in s or "GII" in s or "ＧⅡ" in s or "Ｇ2" in s or "JPN2" in s:
        return "G2"
    if "G3" in s or "GIII" in s or "ＧⅢ" in s or "Ｇ3" in s or "JPN3" in s:
        return "G3"
    if "リステッド" in s:
        return "L"
    if re.search(r"(^|[^A-Z])L([^A-Z]|$)", s):
        return "L"
    if "オープン" in s or "OP" in s:
        return "OP"
    if "3勝" in s:
        return "3勝"
    if "2勝" in s:
        return "2勝"
    if "1勝" in s:
        return "1勝"
    if "新馬" in s:
        return "新馬"
    if "未勝利" in s:
        return "未勝利"
    if "障害" in s:
        return "障害"
    return ""


def _parse_pace_from_laps(lap_text: Any) -> str:
    """
    前半3F と 後半3F の差で pace を簡易判定
    - diff <= -0.6 : slow
    - diff >=  0.6 : fast
    - それ以外     : mid
    """
    if lap_text is None or (isinstance(lap_text, float) and np.isnan(lap_text)):
        return ""
    nums = re.findall(r"\d+\.\d+", str(lap_text))
    if len(nums) < 6:
        return ""
    try:
        laps = [float(x) for x in nums]
    except Exception:
        return ""
    first3 = sum(laps[:3])
    last3 = sum(laps[-3:])
    diff = first3 - last3
    if diff <= -0.6:
        return "slow"
    if diff >= 0.6:
        return "fast"
    return "mid"


def _pick_now_pace(row: pd.Series) -> str:
    """
    今走シートに想定ペース列がある場合だけ利用する。
    無ければ空文字を返し、ペース一致条件は使わない。
    """
    for col in ["想定ペース", "ペース", "レースペース", "pace"]:
        if col in row.index:
            val = str(row.get(col) or "").strip().lower()
            if val in {"slow", "mid", "fast"}:
                return val
    return ""


def _build_context_match_mask(
    place_s: pd.Series,
    surface_s: pd.Series,
    dist_s: pd.Series,
    class_s: pd.Series,
    pace_s: pd.Series,
    now_place: str,
    now_surface: str,
    now_dist: Optional[int],
    now_class: str,
    now_pace: str = "",
) -> pd.Series:
    idx = place_s.index
    mask = pd.Series([True] * len(idx), index=idx)

    if now_place:
        mask &= place_s.fillna("").astype(str) == now_place
    if now_surface:
        mask &= surface_s.fillna("").astype(str) == now_surface

    now_bucket = _bucket_distance(now_dist)
    if now_bucket:
        mask &= dist_s.map(_bucket_distance) == now_bucket

    if now_class:
        mask &= class_s.fillna("").astype(str) == now_class

    # 今走シートに想定ペースがある場合だけ一致条件に使う
    if now_pace:
        mask &= pace_s.fillna("").astype(str) == now_pace

    return mask


def _safe_series_mean(s: pd.Series) -> float:
    s_num = pd.to_numeric(s, errors="coerce")
    return float(s_num.mean()) if not s_num.dropna().empty else np.nan


def _calc_contextual_last3f_features(
    one: pd.DataFrame,
    now_place: str,
    now_surface: str,
    now_dist: Optional[int],
    now_class: str,
    now_pace: str = "",
) -> Dict[str, float]:
    if one.empty:
        return {
            "cond_match_count": 0.0,
            "cond_avg_last3f": np.nan,
            "cond_avg_time_idx": np.nan,
            "cond_pace_fast_last3f": np.nan,
            "cond_pace_slow_last3f": np.nan,
            "last3f_place_surface_diff": np.nan,
            "last3f_dist_diff": np.nan,
            "last3f_class_diff": np.nan,
            "last3f_context_value": np.nan,
            "time_idx_context_value": np.nan,
        }

    place_s = one.get("__place__", pd.Series([""] * len(one), index=one.index)).fillna("").astype(str)
    surface_s = one.get("__surface__", pd.Series([""] * len(one), index=one.index)).fillna("").astype(str)
    dist_s = pd.to_numeric(one.get("__dist__", pd.Series([np.nan] * len(one), index=one.index)), errors="coerce")
    class_s = one.get("__class__", pd.Series([""] * len(one), index=one.index)).fillna("").astype(str)
    pace_s = one.get("__pace__", pd.Series([""] * len(one), index=one.index)).fillna("").astype(str)
    last3_s = pd.to_numeric(one.get("__last3f__", pd.Series([np.nan] * len(one), index=one.index)), errors="coerce")
    time_idx_s = pd.to_numeric(one.get("__time_idx__", pd.Series([np.nan] * len(one), index=one.index)), errors="coerce")

    mask_place_surface = pd.Series([True] * len(one), index=one.index)
    if now_place:
        mask_place_surface &= place_s == now_place
    if now_surface:
        mask_place_surface &= surface_s == now_surface

    now_bucket = _bucket_distance(now_dist)
    mask_dist = dist_s.map(_bucket_distance) == now_bucket if now_bucket else pd.Series([True] * len(one), index=one.index)
    mask_class = class_s == now_class if now_class else pd.Series([True] * len(one), index=one.index)

    mask_context = _build_context_match_mask(
        place_s=place_s,
        surface_s=surface_s,
        dist_s=dist_s,
        class_s=class_s,
        pace_s=pace_s,
        now_place=now_place,
        now_surface=now_surface,
        now_dist=now_dist,
        now_class=now_class,
        now_pace=now_pace,
    )

    cond_avg_last3f = _safe_series_mean(last3_s[mask_context])
    cond_avg_time_idx = _safe_series_mean(time_idx_s[mask_context])

    all_avg_last3f = _safe_series_mean(last3_s)
    all_avg_time_idx = _safe_series_mean(time_idx_s)

    last3f_place_surface = _safe_series_mean(last3_s[mask_place_surface])
    last3f_dist = _safe_series_mean(last3_s[mask_dist])
    last3f_class = _safe_series_mean(last3_s[mask_class])

    pace_fast_last3f = _safe_series_mean(last3_s[pace_s == "fast"])
    pace_slow_last3f = _safe_series_mean(last3_s[pace_s == "slow"])

    last3f_context_value = np.nan
    if not np.isnan(cond_avg_last3f) and not np.isnan(all_avg_last3f):
        # 上がり3Fは小さいほど良いので、平均との差で価値化
        last3f_context_value = all_avg_last3f - cond_avg_last3f

    time_idx_context_value = np.nan
    if not np.isnan(cond_avg_time_idx) and not np.isnan(all_avg_time_idx):
        # タイム指数は大きいほど良い想定なので、条件一致平均との差
        time_idx_context_value = cond_avg_time_idx - all_avg_time_idx

    return {
        "cond_match_count": float(int(mask_context.sum())),
        "cond_avg_last3f": cond_avg_last3f,
        "cond_avg_time_idx": cond_avg_time_idx,
        "cond_pace_fast_last3f": pace_fast_last3f,
        "cond_pace_slow_last3f": pace_slow_last3f,
        "last3f_place_surface_diff": (all_avg_last3f - last3f_place_surface) if not np.isnan(last3f_place_surface) and not np.isnan(all_avg_last3f) else np.nan,
        "last3f_dist_diff": (all_avg_last3f - last3f_dist) if not np.isnan(last3f_dist) and not np.isnan(all_avg_last3f) else np.nan,
        "last3f_class_diff": (all_avg_last3f - last3f_class) if not np.isnan(last3f_class) and not np.isnan(all_avg_last3f) else np.nan,
        "last3f_context_value": last3f_context_value,
        "time_idx_context_value": time_idx_context_value,
    }


def _course_bias(place: str, surface: str, dist: Optional[int]) -> str:
    """
    コース傾向（代理）
    - ダ: 先行(front)寄り
    - 東京芝1400-2000: 差し(rear)寄り
    - 中山芝1600以下: 先行(front)寄り
    """
    p = _to_str(place).strip()
    s = _to_str(surface).strip()
    if "ダ" in s:
        return "front"
    if p == "中山" and "芝" in s and dist is not None and dist <= 1600:
        return "front"
    if p == "東京" and "芝" in s and dist is not None and 1400 <= dist <= 2000:
        return "rear"
    return "neutral"


def _infer_style_from_pass(pass_str: Any, field_size: Optional[int]) -> Optional[str]:
    """通過（例: '3-5-5-5'）から front/mid/rear を推定する。"""
    if pass_str is None or (isinstance(pass_str, float) and np.isnan(pass_str)):
        return None
    nums = re.findall(r"\d+", str(pass_str))
    if not nums:
        return None
    try:
        pos = int(nums[-1])  # 4角相当（最後の数字）
    except Exception:
        return None
    if field_size and field_size > 0:
        frac = pos / field_size
        if frac <= 0.33:
            return "front"
        if frac <= 0.66:
            return "mid"
        return "rear"
    if pos <= 3:
        return "front"
    if pos <= 8:
        return "mid"
    return "rear"


def _compute_horse_features_from_race_sheets(
    book: Dict[Any, pd.DataFrame],
    now_df: pd.DataFrame,
    levels_df: Optional[pd.DataFrame],
    raceday: str = "",
) -> pd.DataFrame:
    """
    馬の競走成績ブック内の「レースIDシート」から、horse-level の特徴量を計算する。
    - 1行=1頭（rid_str, 馬番, 馬名）
    - 過去走明細の列名ゆらぎ（'人 気', '着 順' 等）を吸収する
    """
    race_sheets: Dict[str, pd.DataFrame] = {}
    for sh_name, df in book.items():
        if str(sh_name) in {NOW_SHEET, "README", HORSES_SHEET}:
            continue
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        rid_norm = _normalize_rid_series(pd.Series([sh_name])).iloc[0]
        if re.fullmatch(r"\d{10,13}", str(rid_norm or "")):
            race_sheets[str(rid_norm)] = df

    race_level_map: Dict[str, float] = {}
    if levels_df is not None and isinstance(levels_df, pd.DataFrame) and not levels_df.empty:
        tmp = levels_df.copy()
        tmp = _ensure_rid_str(tmp, label="levels_df")
        if "rid_str" in tmp.columns and "race_level" in tmp.columns:
            tmp["rid_str"] = _normalize_rid_series(tmp["rid_str"])
            tmp["race_level"] = pd.to_numeric(tmp["race_level"], errors="coerce")
            race_level_map = tmp.set_index("rid_str")["race_level"].to_dict()

    now = now_df.copy()
    now = _ensure_rid_str(now, label="NOW")
    now["rid_str"] = _normalize_rid_series(now["rid_str"])
    now["馬番"] = _normalize_umaban_series(now.get("馬番", pd.Series([pd.NA] * len(now))))
    now["馬名"] = now.get("馬名", "").astype(str).str.strip()

    if "コース" in now.columns:
        now["now_surface"] = now["コース"].map(_parse_surface)
        now["now_dist"] = now["コース"].map(_parse_distance_m)
    else:
        now["now_surface"] = ""
        now["now_dist"] = pd.NA

    field_size_map = now.groupby("rid_str")["頭数"].first().map(lambda x: int(_to_float(x) or 0) if pd.notna(x) else None).to_dict() if "頭数" in now.columns else {}

    now_date = None
    if raceday and re.fullmatch(r"\d{8}", raceday):
        now_date = pd.to_datetime(raceday, format="%Y%m%d")

    rows: list[dict[str, Any]] = []
    for rid, g in now.groupby("rid_str", dropna=False):
        rid_str = str(rid)
        df_race = race_sheets.get(rid_str)
        if df_race is None:
            print(f"[WARN] レースIDシートが見つかりません: rid_str={rid_str}（特徴量はNaNになります）")
            continue

        df = df_race.copy()
        df.columns = [str(c) for c in df.columns]

        c_name = _pick_col(df, ["馬名"])
        c_date = _pick_col(df, ["日付"])
        c_finish = _pick_col(df, ["着順", "着 順"])
        c_pop = _pick_col(df, ["人気", "人 気"])
        c_dist = _pick_col(df, ["距離"])
        c_last3f = _pick_col(df, ["上り", "上り3F", "上り３F", "後3F"])
        c_margin = _pick_col(df, ["着差"])
        c_time_idx = _pick_col(df, ["ﾀｲﾑ指数", "タイム指数", "ﾀｲﾑ 指数"])
        c_pass = _pick_col(df, ["通過", "通過順位", "通過順", "コーナー 通過順"])
        c_race_id = _pick_col(df, ["race_id", "レースID"])
        c_lap = _pick_col(df, ["ラップタイム"])
        c_place = _pick_col(df, ["場所", "競馬場", "場名", "開催"])
        c_surface = _pick_col(df, ["芝・ダ", "芝ダ", "コース"])
        c_race_name = _pick_col(df, ["レース名", "クラス", "条件"])

        if c_name is None:
            print(f"[WARN] レースIDシートに馬名列がありません: rid_str={rid_str}（特徴量はNaNになります）")
            continue

        df["__horse_name__"] = df[c_name].astype(str).str.strip()

        for _, r_now in g.iterrows():
            umaban = r_now.get("馬番")
            horse_name = str(r_now.get("馬名") or "").strip()
            if not horse_name:
                continue

            one = df[df["__horse_name__"] == horse_name].copy()
            if one.empty:
                key = re.sub(r"\s+", "", horse_name)
                one = df[df["__horse_name__"].map(lambda x: re.sub(r"\s+", "", str(x))) == key].copy()

            finish_s = _to_float_series(one[c_finish]) if c_finish else pd.Series(dtype=float)
            pop_s = _to_float_series(one[c_pop]) if c_pop else pd.Series(dtype=float)
            dist_s = one[c_dist].map(_parse_distance_m) if c_dist else pd.Series(dtype=float)
            last3_s = _to_float_series(one[c_last3f]) if c_last3f else pd.Series(dtype=float)
            margin_s = _to_float_series(one[c_margin]) if c_margin else pd.Series(dtype=float)
            time_idx_s = _to_float_series(one[c_time_idx]) if c_time_idx else pd.Series(dtype=float)

            one["__place__"] = one[c_place].map(_normalize_place_text) if c_place else ""
            one["__surface__"] = one[c_surface].map(_parse_surface) if c_surface else ""
            one["__dist__"] = one[c_dist].map(_parse_distance_m) if c_dist else np.nan
            one["__class__"] = one[c_race_name].map(_parse_class_name_from_text) if c_race_name else ""
            one["__pace__"] = one[c_lap].map(_parse_pace_from_laps) if c_lap else ""
            one["__last3f__"] = last3_s
            one["__time_idx__"] = time_idx_s

            if c_date and not one.empty:
                one["__date__"] = one[c_date].map(_parse_yyyymmdd)
                one = one.sort_values("__date__", ascending=False, kind="mergesort")

            ta_n = float(len(one)) if not one.empty else 0.0
            avg_finish = float(finish_s.mean()) if not finish_s.dropna().empty else np.nan
            avg_pop = float(pop_s.mean()) if not pop_s.dropna().empty else np.nan
            win_rate = float((finish_s == 1).mean()) if not finish_s.dropna().empty else np.nan
            avg_last3f = float(last3_s.mean()) if not last3_s.dropna().empty else np.nan
            avg_margin = float(margin_s.mean()) if not margin_s.dropna().empty else np.nan
            avg_time_idx = float(time_idx_s.mean()) if not time_idx_s.dropna().empty else np.nan

            now_dist = _parse_distance_m(r_now.get("コース")) if "コース" in now.columns else None
            if now_dist is None or (isinstance(now_dist, float) and np.isnan(now_dist)):
                now_dist = _parse_distance_m(r_now.get("距離")) if "距離" in now.columns else None

            past_mean_dist = float(pd.to_numeric(dist_s, errors="coerce").mean()) if not pd.to_numeric(dist_s, errors="coerce").dropna().empty else np.nan
            dist_diff = float(abs(now_dist - past_mean_dist)) if (now_dist is not None and not np.isnan(past_mean_dist)) else np.nan

            days_off = np.nan
            if now_date is not None and c_date and not one.empty:
                d = one[c_date].map(_parse_yyyymmdd).dropna()
                if not d.empty:
                    last_date = d.max()
                    days_off = float((now_date - last_date).days)

            now_place = _normalize_place_text(r_now.get("場所"))
            now_surface = _parse_surface(r_now.get("コース")) if "コース" in now.columns else _parse_surface(r_now.get("芝・ダ"))
            now_class = _parse_class_name_from_text(r_now.get("クラス", r_now.get("レース名", "")))
            now_pace = _pick_now_pace(r_now)

            context_feats = _calc_contextual_last3f_features(
                one=one,
                now_place=now_place,
                now_surface=now_surface,
                now_dist=now_dist,
                now_class=now_class,
                now_pace=now_pace,
            )

            spkm_vals = pd.Series(dtype=float)
            if c_lap and not one.empty:
                spkm_vals = one[c_lap].map(_parse_laps_to_min_1f)
                spkm_vals = pd.to_numeric(spkm_vals, errors="coerce")

            ta_spkm_best = float(spkm_vals.min()) if not spkm_vals.dropna().empty else np.nan
            ta_spkm_avg3 = float(spkm_vals.head(3).mean()) if not spkm_vals.dropna().empty else np.nan

            place = str(r_now.get("場所") or "")
            surface = _parse_surface(r_now.get("コース")) if "コース" in now.columns else ""
            field_size = field_size_map.get(rid_str)
            style = None
            if c_pass and not one.empty:
                styles = []
                for v in one[c_pass].dropna().tolist():
                    st = _infer_style_from_pass(v, field_size)
                    if st:
                        styles.append(st)
                if styles:
                    style = pd.Series(styles).mode().iloc[0]

            bias = _course_bias(place, surface, now_dist)
            if bias == "neutral" or style is None:
                leg_type_suitability = 0.2
            elif bias == style:
                leg_type_suitability = 1.0
            else:
                leg_type_suitability = -1.0

            if not last3_s.dropna().empty and len(last3_s.dropna()) >= 2:
                var = float(np.nanvar(last3_s))
                lap_match_bonus = float(1.0 / (1.0 + var))
            else:
                lap_match_bonus = 0.5

            fast_score = (40.0 - float(avg_last3f)) if not np.isnan(avg_last3f) else np.nan

            avg_score = np.nan
            if not np.isnan(avg_finish) or not np.isnan(avg_pop) or not np.isnan(win_rate) or not np.isnan(fast_score):
                avg_score = (
                    (-avg_finish if not np.isnan(avg_finish) else 0.0)
                    + (-avg_pop * 0.3 if not np.isnan(avg_pop) else 0.0)
                    + (win_rate * 5.0 if not np.isnan(win_rate) else 0.0)
                    + (fast_score * 0.05 if not np.isnan(fast_score) else 0.0)
                )

            rating_now = avg_time_idx if not np.isnan(avg_time_idx) else avg_score

            past_levels = pd.Series(dtype=float)
            if c_race_id and not one.empty and race_level_map:
                rids = one[c_race_id].astype(str)
                rids = _normalize_rid_series(rids)
                past_levels = rids.map(race_level_map)
                past_levels = pd.to_numeric(past_levels, errors="coerce").dropna()

            past_racelevel_top5_avg3 = float(past_levels.sort_values(ascending=False).head(5).mean()) if not past_levels.empty else np.nan
            past_racelevel_top5_best = float(past_levels.max()) if not past_levels.empty else np.nan

            rows.append(
                {
                    "rid_str": rid_str,
                    "馬番": int(umaban) if pd.notna(umaban) else pd.NA,
                    "馬名": horse_name,
                    "avg_finish": avg_finish,
                    "avg_pop": avg_pop,
                    "dist_diff": dist_diff,
                    "days_off": days_off,
                    "avg_last3f": avg_last3f,
                    "avg_margin": avg_margin,
                    "avg_time_idx": avg_time_idx,
                    "win_rate": win_rate,
                    "fast_score": fast_score,
                    "avg_score": avg_score,
                    "leg_type_suitability": leg_type_suitability,
                    "lap_match_bonus": lap_match_bonus,
                    "ta_spkm_best": ta_spkm_best,
                    "ta_spkm_avg3": ta_spkm_avg3,
                    "ta_n": ta_n,
                    "rating_now": rating_now,
                    "past_racelevel_top5_avg3": past_racelevel_top5_avg3,
                    "past_racelevel_top5_best": past_racelevel_top5_best,
                    # ここから追加
                    "cond_match_count": context_feats["cond_match_count"],
                    "cond_avg_last3f": context_feats["cond_avg_last3f"],
                    "cond_avg_time_idx": context_feats["cond_avg_time_idx"],
                    "cond_pace_fast_last3f": context_feats["cond_pace_fast_last3f"],
                    "cond_pace_slow_last3f": context_feats["cond_pace_slow_last3f"],
                    "last3f_place_surface_diff": context_feats["last3f_place_surface_diff"],
                    "last3f_dist_diff": context_feats["last3f_dist_diff"],
                    "last3f_class_diff": context_feats["last3f_class_diff"],
                    "last3f_context_value": context_feats["last3f_context_value"],
                    "time_idx_context_value": context_feats["time_idx_context_value"],
                }
            )

    feat_df = pd.DataFrame(rows)
    if feat_df.empty:
        return pd.DataFrame(columns=["rid_str", "馬番", "馬名"] + FEAT_COLS)

    feat_df["rid_str"] = _normalize_rid_series(feat_df["rid_str"])
    feat_df["馬番"] = _normalize_umaban_series(feat_df["馬番"])
    feat_df["馬名"] = feat_df["馬名"].astype(str)

    if "rating_now" in feat_df.columns:
        feat_df["rating_vs_field_mean"] = feat_df["rating_now"] - feat_df.groupby("rid_str")["rating_now"].transform("mean")
        feat_df["rating_field_percentile"] = feat_df.groupby("rid_str")["rating_now"].rank(pct=True, method="average")
    else:
        feat_df["rating_vs_field_mean"] = np.nan
        feat_df["rating_field_percentile"] = np.nan

    for c in FEAT_COLS:
        if c not in feat_df.columns:
            feat_df[c] = np.nan

    feat_df = feat_df[["rid_str", "馬番", "馬名"] + FEAT_COLS].copy()
    return feat_df


def _select_weights(place: str = "", surface: str = "") -> Dict[str, float]:
    """
    place/surface から重み辞書を選ぶ
    優先順位：
      1) FEATURE_WEIGHTS_BY_PLACE_SURFACE[(place, surface)]
      2) FEATURE_WEIGHTS[place]
      3) FEATURE_WEIGHTS["__default__"]
      4) {}
    """
    p = _to_str(place).strip()
    s = _to_str(surface).strip()

    if p and s:
        ws = FEATURE_WEIGHTS_BY_PLACE_SURFACE.get((p, s))
        if isinstance(ws, dict) and ws:
            return ws

    if p:
        wp = FEATURE_WEIGHTS.get(p)
        if isinstance(wp, dict) and wp:
            return wp

    wd = FEATURE_WEIGHTS.get("__default__")
    if isinstance(wd, dict) and wd:
        return wd

    return {}


# =============================================================================
# 重み適用・スコア計算
# =============================================================================
def apply_weights(
    feats: Dict[str, Any],
    weights: Optional[Dict[str, float]] = None,
    place: str = "",
    surface: str = "",
) -> Dict[str, float]:
    """
    feats:   {特徴量名: 値}
    weights: {特徴量名: 重み}（省略可）
      - 省略された場合は place/surface から自動選択する

    返り値: {特徴量名: (数値化した値) * 重み}
    """
    if weights is None:
        weights = _select_weights(place=place, surface=surface)

    out: Dict[str, float] = {}
    for k, v in feats.items():
        w = float(weights.get(k, 0.0))
        fv = _to_float(v)
        if pd.isna(fv):
            fv = 0.0
        out[k] = fv * w
    return out


def score_sum(weighted_feats: Dict[str, float]) -> float:
    """重み付き特徴量を足し合わせて raw スコアにする"""
    s = 0.0
    for v in weighted_feats.values():
        try:
            s += float(v)
        except Exception:
            pass
    return float(s)


def normalize_score(x: pd.Series) -> pd.Series:
    """
    レース内で正規化（Z→0〜100の雰囲気にする）
    ※keibayosou_pipeline.py 側で groupby(transform) される想定
    """
    z = _safe_z(pd.to_numeric(x, errors="coerce").fillna(0.0))
    return (50.0 + 10.0 * z).clip(lower=0.0, upper=100.0)


# =============================================================================
# favorite_risk（リスク補正）
# =============================================================================
def build_calc_favorite_risk(
    place_map: Optional[Dict[str, str]] = None,
    surface_map: Optional[Dict[str, str]] = None,
    dist_map: Optional[Dict[str, Optional[int]]] = None,
    field_size_map: Optional[Dict[str, Optional[int]]] = None,
    pop_map: Optional[Dict[Tuple[str, int], Optional[int]]] = None,
    baba_map: Optional[Dict[str, str]] = None,
) -> Callable[[pd.Series], float]:
    """
    行（Series）を受け取って favorite_risk を返す関数（クロージャ）を作る。

    ※簡易版：
      - 備考（出遅れ等） + 人気 + 頭数 + 馬場で軽くリスク化
    """
    place_map = place_map or {}
    surface_map = surface_map or {}
    dist_map = dist_map or {}
    field_size_map = field_size_map or {}
    pop_map = pop_map or {}
    baba_map = baba_map or {}

    def _to_int(v: Any) -> Optional[int]:
        try:
            if v is None:
                return None
            s = str(v).strip()
            if s == "":
                return None
            m = re.search(r"(-?\d+)", s)
            return int(m.group(1)) if m else None
        except Exception:
            return None

    bad_words = [
        "出遅れ",
        "出脚鈍",
        "スタート",
        "出負け",
        "躓",
        "ゲート",
        "行き脚",
        "二の脚",
    ]

    def calc(row: pd.Series) -> float:
        rid = str(row.get("rid_str", ""))
        uma = _to_int(row.get("馬番"))

        risk = 0.0

        biko = str(row.get("備考", "") or "")
        if any(w in biko for w in bad_words):
            risk += 1.0

        pop = None
        if uma is not None:
            pop = pop_map.get((rid, int(uma)))
        if pop is None:
            pop = _to_int(row.get("人気"))
        if pop is not None:
            if pop <= 1:
                risk += 0.30
            elif pop <= 3:
                risk += 0.20
            elif pop <= 5:
                risk += 0.10

        fs = field_size_map.get(rid)
        if fs is None:
            fs = _to_int(row.get("頭数"))
        if fs is not None:
            if fs >= 18:
                risk += 0.15
            elif fs >= 16:
                risk += 0.10

        baba = str(baba_map.get(rid, "") or row.get("馬場", "") or "")
        if baba:
            if "不良" in baba:
                risk += 0.20
            elif "重" in baba:
                risk += 0.15
            elif "稍" in baba or "稍重" in baba:
                risk += 0.10

        _ = place_map.get(rid)
        _ = surface_map.get(rid)
        _ = dist_map.get(rid)

        return float(max(0.0, risk))

    return calc


# =============================================================================
# Excel 読み込み・特徴量生成（最小限の互換版）
# =============================================================================
def _read_excel_sheet(path: str, sheet: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet)
    except Exception as e:
        raise RuntimeError(f"Excel読み込み失敗: path={path} sheet={sheet} err={e}")


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def build_features_from_excel(
    src_excel_path: str,
    levels_df: Optional[pd.DataFrame] = None,
    base_time_df: Optional[pd.DataFrame] = None,
    odds_df: Optional[pd.DataFrame] = None,
    raceday: str = "",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    返り値：
      merged_df : 今走レース情報（NOW_SHEET）に horses を必要に応じて結合したもの
      feat_df   : rid_str/馬番/馬名 + FEAT_COLS を含む特徴量テーブル
    """
    try:
        book = pd.read_excel(src_excel_path, sheet_name=None, engine="openpyxl")
    except Exception as e:
        raise RuntimeError(f"Excel読み込み失敗: path={src_excel_path} err={e}")

    if NOW_SHEET not in book:
        raise RuntimeError(f"今走シートが見つかりません: sheet={NOW_SHEET} path={src_excel_path}")
    now_df = book[NOW_SHEET].copy()
    if "dl_rank" not in now_df.columns:
        now_df["dl_rank"] = np.nan
    now_df["dl_rank"] = pd.to_numeric(now_df["dl_rank"], errors="coerce")

    horses_df = book.get(HORSES_SHEET)

    if raceday:
        date_cols = ["日付", "開催日", "日付(月日)", "年月日", "raceday"]
        date_col = next((c for c in date_cols if c in now_df.columns), None)
        if date_col:
            s = now_df[date_col].astype(str).str.replace("/", "", regex=False).str.replace("-", "", regex=False)
            now_df = now_df.loc[s.str.contains(str(raceday), na=False)].copy()

    merged = now_df.copy()

    if "芝・ダ" not in merged.columns and "コース" in merged.columns:
        merged["芝・ダ"] = merged["コース"].map(_parse_surface)
    if "距離" not in merged.columns and "コース" in merged.columns:
        merged["距離"] = merged["コース"].map(_parse_distance_m)

    merged = _ensure_cols(merged, ["rid_str", "馬番", "馬名", "頭数", "人気", "備考", "場所", "芝・ダ", "距離", "馬場"])

    merged = _ensure_rid_str(merged, label="build_features_from_excel(NOW)")
    if "rid_str" in merged.columns:
        rid_ser = merged["rid_str"]
        rid_empty = rid_ser.isna() | rid_ser.astype(str).str.strip().isin(["", "nan", "<NA>"])
        if rid_empty.all() and "レースID" in merged.columns:
            merged["rid_str"] = merged["レースID"]
        merged["rid_str"] = _normalize_rid_series(merged["rid_str"])
    if "馬番" in merged.columns:
        merged["馬番"] = _normalize_umaban_series(merged["馬番"])

    computed_feat = _compute_horse_features_from_race_sheets(book, merged, levels_df, raceday=raceday)

    if horses_df is not None and isinstance(horses_df, pd.DataFrame) and not horses_df.empty:
        h = horses_df.copy()
        h = _ensure_rid_str(h, label="HORSES")
        if "馬番" not in h.columns:
            for cand in ["umaban", "馬 番", "馬番 "]:
                if cand in h.columns:
                    h["馬番"] = h[cand]
                    break
        if "rid_str" in h.columns:
            h["rid_str"] = _normalize_rid_series(h["rid_str"])
        if "馬番" in h.columns:
            h["馬番"] = _normalize_umaban_series(h["馬番"])

        if {"rid_str", "馬番"}.issubset(h.columns) and not computed_feat.empty:
            computed_feat = pd.merge(
                computed_feat,
                h,
                on=["rid_str", "馬番"],
                how="left",
                suffixes=("", "_h"),
            )
            for col in FEAT_COLS:
                alt = f"{col}_h"
                if col in computed_feat.columns and alt in computed_feat.columns:
                    computed_feat[col] = computed_feat[col].where(computed_feat[col].notna(), computed_feat[alt])
            drop_cols = [c for c in computed_feat.columns if c.endswith("_h")]
            if drop_cols:
                computed_feat = computed_feat.drop(columns=drop_cols)

    merged = pd.merge(
        merged,
        computed_feat[["rid_str", "馬番"] + FEAT_COLS],
        on=["rid_str", "馬番"],
        how="left",
        suffixes=("", "_feat"),
    )

    feat = pd.DataFrame()
    feat["rid_str"] = merged["rid_str"].astype(str)
    feat["馬番"] = _normalize_umaban_series(merged["馬番"])
    feat["馬名"] = merged["馬名"].astype(str)

    for col in FEAT_COLS:
        if col in merged.columns:
            feat[col] = pd.to_numeric(merged[col], errors="coerce")

    feat = _ensure_cols(feat, FEAT_COLS)

    return merged, feat