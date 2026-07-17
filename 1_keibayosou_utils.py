# -*- coding: utf-8 -*-
"""
1_keibayosou_utils.py（完全版）

いまの状況：
- 1_keibayosou_features.py から
  _ensure_rid_str と _normalize_place_surface を import しようとしている
- しかし、あなたが上書きした utils.py には
  _normalize_place_surface が無くて ImportError になっていました

このファイルは、あなたがアップロードした 1_keibayosou_utils.py をベースに
✅ _normalize_place_surface を「追記」して
✅ 既存の関数は消さずに
“完全な全体コード”としてまとめたものです。
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _retry_session(total: int = 3, backoff: float = 0.3) -> requests.Session:
    """requests 用のリトライ付きセッションを作成"""
    session = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=backoff,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _to_int(x, default=None):
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default


def _to_float(x, default=None):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _safe_div(a, b, default=0.0):
    try:
        if b == 0:
            return default
        return a / b
    except Exception:
        return default


def _normalize_place(name: str) -> str:
    """「札幌」「札幌競馬場」などを「札幌」に正規化"""
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFKC", name)
    name = name.replace("競馬場", "").strip()
    return name


def _normalize_surface(s: str) -> str:
    """芝／ダート／障害 の表記ゆれを正規化"""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s)
    if "芝" in s:
        return "芝"
    if "ダ" in s:
        return "ダ"
    if "障" in s:
        return "障害"
    return s


def _extract_delay_flag(text: Any, delay_keywords: list[str]) -> int:
    """horsesシートの備考から出遅れ系キーワードを検出して 1/0 を返す"""
    if pd.isna(text):
        return 0
    s = str(text)
    for kw in delay_keywords:
        if kw in s:
            return 1
    return 0


def _normalize_0_100(series: pd.Series) -> pd.Series:
    """
    series を 0〜100 に正規化する（min==max の場合は全て50）
    """
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return pd.Series([50.0] * len(series), index=series.index)
    mn, mx = s.min(), s.max()
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([50.0] * len(series), index=series.index)
    return (s - mn) / (mx - mn) * 100


def normalize_features_within_race(
    df: pd.DataFrame,
    feature_cols: Sequence[str],
    *,
    race_col: str = "rid_str",
    lower_is_better: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """特徴量をレース内百分位へ変換し、元値と欠損フラグを残す。"""
    if race_col not in df.columns:
        raise ValueError(f"レース内尺度統一に必要な列がありません: {race_col}")

    out = df.copy()
    lower_set = set(lower_is_better or [])
    for feature in feature_cols:
        if feature not in out.columns:
            out[feature] = np.nan

        raw = pd.to_numeric(out[feature], errors="coerce")
        out[f"{feature}_raw"] = raw
        out[f"{feature}_missing"] = raw.isna().astype(int)

        race_median = raw.groupby(out[race_col]).transform("median")
        global_median = raw.median(skipna=True)
        if pd.isna(global_median):
            global_median = 0.0
        filled = raw.fillna(race_median).fillna(float(global_median))
        percentile = filled.groupby(out[race_col]).rank(pct=True, method="average")
        if feature in lower_set:
            percentile = 1.0 - percentile
        out[feature] = percentile.clip(lower=0.0, upper=1.0)

    return out


def build_feature_health_diagnostics(
    df: pd.DataFrame,
    feature_cols: Sequence[str],
    *,
    weights: Optional[Mapping[str, float]] = None,
    race_col: str = "rid_str",
    correlation_threshold: float = 0.90,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """特徴量の健康状態と重複相関を、計画書の診断定義で集計する。"""
    if race_col not in df.columns:
        raise ValueError(f"特徴量診断に必要な列がありません: {race_col}")

    work = df.copy()
    present_features = [feature for feature in feature_cols if feature in work.columns]
    numeric = pd.DataFrame(index=work.index)
    for feature in present_features:
        numeric[feature] = pd.to_numeric(work[feature], errors="coerce")

    correlations = numeric.corr(method="pearson", min_periods=20) if not numeric.empty else pd.DataFrame()
    correlation_rows: list[dict[str, object]] = []
    duplicate_map: dict[str, list[str]] = {feature: [] for feature in feature_cols}
    for left_index, left in enumerate(present_features):
        for right in present_features[left_index + 1 :]:
            corr = correlations.at[left, right] if left in correlations.index and right in correlations.columns else np.nan
            if pd.isna(corr):
                continue
            is_duplicate = abs(float(corr)) >= float(correlation_threshold)
            correlation_rows.append(
                {
                    "特徴量1": left,
                    "特徴量2": right,
                    "相関係数": float(corr),
                    "重複疑い": "あり" if is_duplicate else "なし",
                }
            )
            if is_duplicate:
                duplicate_map[left].append(right)
                duplicate_map[right].append(left)

    weights = weights or {}
    total_count = len(work)
    race_count = int(work[race_col].nunique(dropna=True))
    diagnostic_rows: list[dict[str, object]] = []
    for feature in feature_cols:
        values = numeric[feature] if feature in numeric.columns else pd.Series(np.nan, index=work.index, dtype=float)
        valid = values.dropna()
        valid_count = int(valid.size)
        missing_count = int(values.isna().sum())
        missing_rate = float(missing_count / total_count) if total_count else 1.0
        zero_count = int(values.eq(0.0).sum())
        zero_rate = float(zero_count / valid_count) if valid_count else 0.0
        unique_count = int(valid.nunique(dropna=True))
        std = float(valid.std(ddof=0)) if valid_count else np.nan

        grouped = pd.DataFrame({race_col: work[race_col], "value": values}).groupby(race_col, dropna=True)["value"]
        race_unique = grouped.nunique(dropna=True)
        all_equal_count = int(race_unique.le(1).sum())
        all_equal_rate = float(all_equal_count / race_count) if race_count else 1.0
        race_std = grouped.std(ddof=0)
        race_mean_std = float(race_std.mean(skipna=True)) if race_std.notna().any() else np.nan

        if valid_count >= 4 and pd.notna(std) and std > 0:
            z = (valid - float(valid.mean())) / std
            outlier_count = int(z.abs().gt(4.0).sum())
        else:
            outlier_count = 0

        weight = float(weights.get(feature, 0.0))
        contribution = values * weight
        contribution_grouped = pd.DataFrame(
            {race_col: work[race_col], "value": contribution}
        ).groupby(race_col, dropna=True)["value"]
        contribution_std = contribution_grouped.std(ddof=0)
        practical_contribution = (
            float(contribution_std.mean(skipna=True)) if contribution_std.notna().any() else 0.0
        )

        duplicates = duplicate_map.get(feature, [])
        if valid_count == 0 or unique_count <= 1 or (pd.notna(std) and std == 0.0):
            classification = "算出不良"
        elif missing_rate >= 0.50:
            classification = "データ不足"
        elif duplicates:
            classification = "重複"
        else:
            classification = "正常"

        if classification == "正常" and (missing_rate >= 0.50 or all_equal_rate >= 0.50):
            automatic_judgment = "要注意"
        elif classification == "算出不良":
            automatic_judgment = "異常"
        elif classification == "重複":
            automatic_judgment = "重複疑い"
        elif classification == "データ不足":
            automatic_judgment = "要注意"
        else:
            automatic_judgment = "正常"

        diagnostic_rows.append(
            {
                "特徴量名": feature,
                "分類": classification,
                "自動判定": automatic_judgment,
                "全データ数": total_count,
                "有効頭数": valid_count,
                "欠損数": missing_count,
                "欠損率": missing_rate,
                "ゼロ件数": zero_count,
                "ゼロ率": zero_rate,
                "ユニーク数": unique_count,
                "平均値": float(valid.mean()) if valid_count else np.nan,
                "中央値": float(valid.median()) if valid_count else np.nan,
                "標準偏差": std,
                "最小値": float(valid.min()) if valid_count else np.nan,
                "最大値": float(valid.max()) if valid_count else np.nan,
                "全頭同値レース数": all_equal_count,
                "全頭同値レース率": all_equal_rate,
                "レース内平均標準偏差": race_mean_std,
                "異常値件数": outlier_count,
                "現在の重み": weight,
                "ランキングへの実質寄与": practical_contribution,
                "相関0.90以上の特徴量": ", ".join(sorted(duplicates)),
            }
        )

    diagnostic_df = pd.DataFrame(diagnostic_rows)
    correlation_df = pd.DataFrame(correlation_rows)
    if not correlation_df.empty:
        correlation_df = correlation_df.sort_values("相関係数", key=lambda s: s.abs(), ascending=False)
    return diagnostic_df, correlation_df


def _build_feature_sheet_for_export(
    feat_df: pd.DataFrame,
    feat_cols: list[str],
    japanese_feature_names: dict[str, str],
) -> pd.DataFrame:
    """
    TARGETシート用の出力DataFrameを作成
    - FEAT_COLS は 0-100 正規化
    - delay_rate など FEAT_COLS 以外はそのまま
    - 列名は JAPANESE_FEATURE_NAMES で日本語化
    """
    df = feat_df.copy()
    for col in feat_cols:
        if col in df.columns:
            df[col] = _normalize_0_100(df[col])
    rename_map = {k: v for k, v in japanese_feature_names.items() if k in df.columns}
    return df.rename(columns=rename_map)


def _ensure_rid_str(df: pd.DataFrame, label: str = "") -> pd.DataFrame:
    """
    rid_str 列が無い場合に、候補列（例: 'レースID'）から rid_str を生成する。

    返り値:
      - rid_str が存在する DataFrame（見つからない場合は何もせずそのまま返す）
    """
    if "rid_str" in df.columns:
        df["rid_str"] = df["rid_str"].astype(str)
        return df

    # よくある列名候補（英語/日本語）
    candidates = [
        "レースID",
        "race_id",
        "raceid",
        "rid",
        "RID",
        "RaceID",
        "RaceId",
    ]

    # 空白ゆらぎ対策用に「正規化した列名→元の列名」を作る
    norm_to_raw = {}
    for c in df.columns:
        raw = str(c)
        norm = re.sub(r"\s+", "", raw).lower()
        norm_to_raw[norm] = raw

    for cand in candidates:
        cand_norm = re.sub(r"\s+", "", str(cand)).lower()
        if cand_norm in norm_to_raw:
            col = norm_to_raw[cand_norm]
            df["rid_str"] = df[col].astype(str)
            prefix = f"{label} の" if label else ""
            print(f"[INFO] {prefix}rid_str 列が無かったため、'{col}' 列から生成しました")
            return df

    # 候補が無ければ何もしない（呼び出し元で rid_str 存在チェックして止める設計）
    return df


# =========================
# ★今回追加：ImportError 対策（features.py が import してくる想定）
# =========================
def _normalize_place_surface(place, surface) -> Tuple[Optional[str], Optional[str]]:
    """
    place: 例 "中京" "中京競馬場" "東京"
    surface: 例 "芝" "ダ" "ダート" "芝1800" "ダ1200"

    return:
      (place_norm, surface_norm)
        place_norm: 競馬場名の正規化（例 "中京"）
        surface_norm: "芝" / "ダ" / "障害" / None
    """
    # place 正規化（競馬場名）
    place_norm = None
    if isinstance(place, str) and place.strip():
        p = unicodedata.normalize("NFKC", place).strip()
        p = p.replace("競馬場", "").strip()
        place_norm = p if p else None

    # surface 正規化（芝・ダ・障害）
    surface_norm = None
    s = ""
    if isinstance(surface, str) and surface.strip():
        s = unicodedata.normalize("NFKC", surface).strip()

    if "芝" in s:
        surface_norm = "芝"
    elif "ダ" in s:
        surface_norm = "ダ"
    elif "障" in s:
        surface_norm = "障害"
    else:
        surface_norm = None

    return place_norm, surface_norm
