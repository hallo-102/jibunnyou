# =========================
# keibayosou_penalties.py
# =========================
# 目的：
# - 「条件付きペナルティ（extra_penalty）」と
# - 「休養×距離差の危険馬スコア（rest_dist_risk）」
# を計算するための関数をまとめたファイルです。
#
# ★重要：このファイルの中で自分自身を import しないでください。
#   NG例：from keibayosou_penalties import calc_extra_penalty ...
#   → 循環importになって ImportError になります。
#
# 使い方（他ファイルから）：
#   from keibayosou_penalties import calc_extra_penalty, calc_rest_dist_risk
#
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Optional

import pandas as pd

from keibayosou_config import (
    # A-1) 人気常連
    PEN_FAV_POP_TH,
    PEN_FAV_POP_K,
    PEN_FAV_POP_APPLY_WINRATE_MAX,
    # A-2) 善戦マン
    PEN_GOOD_LOSER_L,
    PEN_GOOD_LOSER_H,
    PEN_GOOD_LOSER_K,
    PEN_GOOD_LOSER_APPLY_WINRATE_MAX,
    # A-3) ベテラン過大評価
    PEN_TA_N_CAP,
    PEN_TA_N_K,
    # A-4) 惜敗マン
    PEN_CLOSE_LOSS_FINISH_TH,
    PEN_CLOSE_LOSS_MARGIN_TH,
    PEN_CLOSE_LOSS_K,
    PEN_CLOSE_LOSS_APPLY_WINRATE_MAX,
    # 追加：休養×距離差リスク
    REST_RISK_TH_DAYS,
    REST_RISK_MAX_DAYS,
    DIST_RISK_TH_M,
    DIST_RISK_MAX_M,
    REST_DIST_RISK_K,
)

__all__ = [
    "calc_rest_dist_risk",
    "calc_extra_penalty",
]


# ================================================================
# 内部ユーティリティ（落ちない変換）
# ================================================================
def _safe_float(x, default: Optional[float] = None) -> Optional[float]:
    """None / NaN / 空文字でも落ちずに float を返す（失敗時は default）。"""
    try:
        if x is None:
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        # pandas.NA 対策
        if x is pd.NA:
            return default
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return default
        return float(s)
    except Exception:
        return default


def _clip01(v: float) -> float:
    """0〜1に丸める。"""
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def _first_present_float(row: pd.Series, keys: list[str], default: Optional[float] = None) -> Optional[float]:
    """候補列を順に見て、最初に数値化できた値を返す。"""
    for key in keys:
        val = _safe_float(row.get(key), default=None)
        if val is not None:
            return val
    return default


# ================================================================
# 追加：休養×距離差 の危険馬スコア（掛け算）
# ================================================================
def calc_rest_dist_risk(row: pd.Series) -> float:
    """
    休養(days_off) と 距離差(dist_diff) から「危険度」を作る（0以上）。

    ざっくり：
    - 休養が長い（閾値超え）ほど危険
    - 距離差が大きい（閾値超え）ほど危険
    - 2つを掛け算して、両方が揃ったときに強く効く

    設定値は keibayosou_config.py の以下を使います：
    - REST_RISK_TH_DAYS / REST_RISK_MAX_DAYS
    - DIST_RISK_TH_M / DIST_RISK_MAX_M
    - REST_DIST_RISK_K
    """
    days_off = _first_present_float(row, ["days_off", "f_rest_days"], default=None)
    dist_diff = _first_present_float(row, ["dist_diff", "f_diff_distance"], default=None)

    if days_off is None or dist_diff is None:
        return 0.0

    # 休養：TH〜MAXで 0→1
    if float(REST_RISK_MAX_DAYS) <= float(REST_RISK_TH_DAYS):
        rest_norm = 0.0
    else:
        rest_norm = (float(days_off) - float(REST_RISK_TH_DAYS)) / (
            float(REST_RISK_MAX_DAYS) - float(REST_RISK_TH_DAYS)
        )
        rest_norm = _clip01(float(rest_norm))

    # 距離差：TH〜MAXで 0→1（距離差は絶対値で扱う）
    dist_abs = abs(float(dist_diff))
    if float(DIST_RISK_MAX_M) <= float(DIST_RISK_TH_M):
        dist_norm = 0.0
    else:
        dist_norm = (dist_abs - float(DIST_RISK_TH_M)) / (float(DIST_RISK_MAX_M) - float(DIST_RISK_TH_M))
        dist_norm = _clip01(float(dist_norm))

    risk = rest_norm * dist_norm * float(REST_DIST_RISK_K)
    return float(max(0.0, risk))


# ================================================================
# 条件付きペナルティ（extra_penalty）
# ================================================================
def calc_extra_penalty(row: pd.Series, rest_dist_risk: Optional[float] = None) -> float:
    """
    extra_penalty を計算します（0以上）。

    使う列（rowに無くても落ちない）：
    - avg_pop（平均人気）
    - avg_finish（平均着順）
    - ta_n（過去走数など）
    - avg_margin（平均着差）
    - win_rate（勝率）
    - days_off / dist_diff（休養・距離差：rest_dist_risk計算用）

    rest_dist_risk を外から渡すと、二重計算を避けられます。
    （pipeline側で先に calc_rest_dist_risk を計算して渡す想定）
    """
    avg_pop = _first_present_float(row, ["avg_pop", "f_pop_mean"], default=None)
    avg_finish = _first_present_float(row, ["avg_finish", "f_finish_mean"], default=None)
    ta_n = _first_present_float(row, ["ta_n", "f_race_count"], default=None)
    avg_margin = _safe_float(row.get("avg_margin"), default=None)
    win_rate = _first_present_float(row, ["win_rate", "f_win_rate"], default=None)

    p = 0.0

    # -------------------------
    # A-1) 人気常連（人気のわりに勝てない）
    # avg_pop が小さいほど人気
    # 「勝率が低いときだけ」発動
    # -------------------------
    if avg_pop is not None:
        cond_win = (win_rate is None) or (float(win_rate) <= float(PEN_FAV_POP_APPLY_WINRATE_MAX))
        if cond_win and (float(avg_pop) <= float(PEN_FAV_POP_TH)):
            p += (float(PEN_FAV_POP_TH) - float(avg_pop)) * float(PEN_FAV_POP_K)

    # -------------------------
    # A-2) 善戦マン（3.5〜6.5着あたりに偏る）
    # 「勝率が低いときだけ」発動
    # -------------------------
    if avg_finish is not None:
        cond_win = (win_rate is None) or (float(win_rate) <= float(PEN_GOOD_LOSER_APPLY_WINRATE_MAX))
        if cond_win and (float(PEN_GOOD_LOSER_L) <= float(avg_finish) <= float(PEN_GOOD_LOSER_H)):
            center = (float(PEN_GOOD_LOSER_L) + float(PEN_GOOD_LOSER_H)) / 2.0
            width = (float(PEN_GOOD_LOSER_H) - float(PEN_GOOD_LOSER_L)) / 2.0
            if width > 0:
                closeness = 1.0 - min(1.0, abs(float(avg_finish) - center) / width)  # 0〜1
                if closeness < 0.0:
                    closeness = 0.0
                p += float(closeness) * float(PEN_GOOD_LOSER_K)

    # -------------------------
    # A-3) ベテラン過大評価（走りすぎ）
    # -------------------------
    if ta_n is not None and float(ta_n) > float(PEN_TA_N_CAP):
        p += (float(ta_n) - float(PEN_TA_N_CAP)) * float(PEN_TA_N_K)

    # -------------------------
    # A-4) 惜敗マン（着順は悪いのに着差が小さい）
    # 「勝率が低いときだけ」発動
    # -------------------------
    if avg_finish is not None and avg_margin is not None:
        cond_win = (win_rate is None) or (float(win_rate) <= float(PEN_CLOSE_LOSS_APPLY_WINRATE_MAX))
        if cond_win and (float(avg_finish) >= float(PEN_CLOSE_LOSS_FINISH_TH)) and (
            float(avg_margin) <= float(PEN_CLOSE_LOSS_MARGIN_TH)
        ):
            p += float(PEN_CLOSE_LOSS_K)

    # -------------------------
    # 追加：休養×距離差リスク（掛け算）
    # -------------------------
    if rest_dist_risk is None:
        rest_dist_risk = calc_rest_dist_risk(row)
    p += float(rest_dist_risk)

    return float(max(0.0, p))
