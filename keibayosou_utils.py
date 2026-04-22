# -*- coding: utf-8 -*-
"""
keibayosou_utils.py（完全版）

いまの状況：
- keibayosou_features.py から
  _ensure_rid_str と _normalize_place_surface を import しようとしている
- しかし、あなたが上書きした utils.py には
  _normalize_place_surface が無くて ImportError になっていました

このファイルは、あなたがアップロードした keibayosou_utils.py をベースに
✅ _normalize_place_surface を「追記」して
✅ 既存の関数は消さずに
“完全な全体コード”としてまとめたものです。
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional, Tuple

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
