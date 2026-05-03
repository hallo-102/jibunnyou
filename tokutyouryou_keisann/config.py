# config.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List


def _detect_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _detect_excel_dir(project_root: Path) -> Path:
    env = os.getenv("KEIBA_EXCEL_DIR", "").strip()
    if env:
        p = Path(env)
        if p.exists():
            return p

    p1 = project_root / "xlsx"
    if p1.exists():
        return p1

    p2 = project_root / "data" / "input"
    if p2.exists():
        return p2

    p1.mkdir(parents=True, exist_ok=True)
    return p1


def _detect_py_dir(project_root: Path) -> Path:
    env = os.getenv("KEIBA_PY_DIR", "").strip()
    if env:
        p = Path(env)
        p.mkdir(parents=True, exist_ok=True)
        return p

    p1 = project_root / "yosou_py"
    if p1.exists():
        return p1

    p2 = project_root / "py"
    if p2.exists():
        return p2

    p1.mkdir(parents=True, exist_ok=True)
    return p1


PROJECT_ROOT = Path(os.getenv("KEIBA_ROOT", str(_detect_project_root()))).resolve()
EXCEL_DIR = _detect_excel_dir(PROJECT_ROOT)
PY_DIR = _detect_py_dir(PROJECT_ROOT)
RACE_LEVEL_XLSX = PROJECT_ROOT / "data" / "master" / "race_levels.xlsx"

try:
    from keibayosou_penalties import calc_rest_dist_risk, calc_extra_penalty
except Exception:
    calc_rest_dist_risk = None
    calc_extra_penalty = None

try:
    from keibayosou_config import (
        ALPHA as PIPE_ALPHA,
        BASE_TIME_XLSX,
        ODDS_CSV,
        EXTRA_ALPHA as PIPE_EXTRA_ALPHA,
        PEN_FAV_POP_TH,
        PEN_FAV_POP_K,
        PEN_FAV_POP_APPLY_WINRATE_MAX,
        PEN_GOOD_LOSER_L,
        PEN_GOOD_LOSER_H,
        PEN_GOOD_LOSER_K,
        PEN_GOOD_LOSER_APPLY_WINRATE_MAX,
        PEN_TA_N_CAP,
        PEN_TA_N_K,
        PEN_CLOSE_LOSS_FINISH_TH,
        PEN_CLOSE_LOSS_MARGIN_TH,
        PEN_CLOSE_LOSS_K,
        PEN_CLOSE_LOSS_APPLY_WINRATE_MAX,
        EMPIRICAL_WEIGHT_SIGN_GUARD,
        FEAT_COLS as PIPE_FEAT_COLS,
        FEATURE_WEIGHTS_BASE as PIPE_FEATURE_WEIGHTS_BASE,
    )
except Exception:
    BASE_TIME_XLSX = PROJECT_ROOT / "data" / "master" / "base_time.xlsx"
    ODDS_CSV = PROJECT_ROOT / "csv"
    EMPIRICAL_WEIGHT_SIGN_GUARD = {}
    PIPE_FEAT_COLS = None
    PIPE_FEATURE_WEIGHTS_BASE = {}
    PIPE_ALPHA = 20.0
    PIPE_EXTRA_ALPHA = 10.0
    PEN_FAV_POP_TH, PEN_FAV_POP_K, PEN_FAV_POP_APPLY_WINRATE_MAX = 3.0, 0.6, 0.14
    PEN_GOOD_LOSER_L, PEN_GOOD_LOSER_H, PEN_GOOD_LOSER_K, PEN_GOOD_LOSER_APPLY_WINRATE_MAX = 3.5, 6.5, 0.8, 0.12
    PEN_TA_N_CAP, PEN_TA_N_K = 9.0, 0.18
    PEN_CLOSE_LOSS_FINISH_TH, PEN_CLOSE_LOSS_MARGIN_TH, PEN_CLOSE_LOSS_K, PEN_CLOSE_LOSS_APPLY_WINRATE_MAX = 4.0, 0.30, 0.7, 0.16


CONFIG: Dict[str, Any] = {
    "DATA_GLOB": str(EXCEL_DIR / "*馬の競走成績_*.xlsx"),
    "EXCLUDE_KEYWORDS": [
        "_with_topN",
    ],
    "RESULTS_FILE": str(
        (EXCEL_DIR / "racedata_results.xlsx")
        if (EXCEL_DIR / "racedata_results.xlsx").exists()
        else (PROJECT_ROOT / "data" / "master" / "racedata_results.xlsx")
    ),
    "TARGET_SHEET": "今走レース情報",
    "TOP_K": 5,
    "N_RECENT": 5,
    "BET_TYPE": "3連複",
    "BET_UNIT_YEN": 100,
    "BOX_SIZE": 5,
    "SKIP_IF_PAYOUT_MISSING": False,
    "PAYOUT_CAP_YEN": 100000,

    "SCORE_GAP_MIN": 1.2,

    "RANDOM_SEED": 13,
    "OPTIMIZER_SEEDS": [13, 17, 23],
    "MULTISEED_MEDIAN_ACCEPT_RATIO": 0.98,
    "N_ITER_DEFAULT": 10000,
    "N_ITER_PLACE": 5000,
    "N_ITER_PLACE_SURFACE": 4000,
    "PERTURB_P": 0.28,
    "LOGN_SIGMA": 0.18,
    "ADD_EPS_SD": 0.06,

    "WEIGHT_MIN": -5.0,
    "WEIGHT_MAX": 5.0,
    "RACELEVEL_WEIGHT_MIN": -2.5,
    "RACELEVEL_WEIGHT_MAX": 2.5,

    "TRAIN_START_DATE": "20250524",
    "TRAIN_END_DATE": "20260228",
    "TEST_START_DATE": "20260301",
    #"TRAIN_START_DATE": "20251213",
    #"TRAIN_END_DATE": "20260228",
    #"TEST_START_DATE": "20260301",

    "MIN_PLACE_RACES": 60,
    "MIN_PLACE_BETS": 30,

    "MIN_PLACE_SURFACE_RACES": 35,
    "MIN_PLACE_SURFACE_BETS": 15,

    "OBJECTIVE_MODE": "TOP5_HIT",
    "TOP5_HIT_W_FIRST": 3.0,
    "TOP5_HIT_W_SECOND": 2.0,
    "TOP5_HIT_W_THIRD": 1.0,

    "OBJ_W_TOP5_POINT_RATE": 0.85,
    "OBJ_W_TOP3_COMPLETE_RATE": 1.55,
    "OBJ_W_WIN_IN_TOP5_RATE": 0.50,
    "OBJ_W_PLACE_IN_TOP5_RATE": 0.55,
    "OBJ_W_RANK1_PLACE_RATE": 1.00,
    "OBJ_W_RANK1_WIN_RATE": 0.10,
    "OBJ_W_COVERAGE": 0.10,

    "OBJ_MIN_BETS": 120,
    "OBJ_MAX_BETS": 1400,
    "OBJ_LOW_BETS_PENALTY": 0.20,
    "OBJ_HIGH_BETS_PENALTY": 0.10,

    "PLACE_BLEND_WITH_DEFAULT": 0.30,
    "PLACE_SURFACE_BLEND_WITH_PLACE": 0.50,
    "MIN_EVAL_ROWS_PER_RID": 6,
    "MIN_WEAKNESS_GROUP_RACES": 20,
}

# 本番側 keibayosou_config.py の FEAT_COLS と揃える
FEAT_COLS: List[str] = [
    "avg_finish",
    "avg_pop",
    "dist_diff",
    "days_off",
    "avg_last3f",
    "avg_margin",
    "avg_time_idx",
    "recent3_finish",
    "recent3_pop",
    "recent3_last3f",
    "recent3_time_idx",
    "recent_finish_trend",
    "recent_pop_trend",
    "recent_time_idx_trend",
    "win_rate",
    "fast_score",
    "avg_score",
    "leg_type_suitability",
    "lap_match_bonus",
    "ta_spkm_best",
    "ta_spkm_avg3",
    "ta_n",
    "rating_now",
    "rating_vs_field_mean",
    "rating_field_percentile",
    "past_racelevel_top5_avg3",
    "past_racelevel_top5_best",
    "cond_match_count",
    "cond_avg_last3f",
    "cond_avg_time_idx",
    "cond_pace_fast_last3f",
    "cond_pace_slow_last3f",
    "last3f_place_surface_diff",
    "last3f_dist_diff",
    "last3f_class_diff",
    "last3f_context_value",
    "time_idx_context_value",
    "dl_rank_score",
]

# 本番側に寄せた初期シード
# 値は本番の初期重みをベースに、そのまま最適化開始点として使う
FEATURE_WEIGHTS_SEED: Dict[str, float] = {
    "avg_finish": -0.5,
    "avg_pop": -0.3,
    "dist_diff": -0.2,
    "days_off": -0.1,
    "avg_last3f": 0.4,
    "avg_margin": -0.3,
    "avg_time_idx": -0.2,
    "recent3_finish": -0.4,
    "recent3_pop": -0.2,
    "recent3_last3f": -0.3,
    "recent3_time_idx": 0.5,
    "recent_finish_trend": 0.4,
    "recent_pop_trend": 0.2,
    "recent_time_idx_trend": 0.5,
    "win_rate": 1.0,
    "fast_score": 0.5,
    "avg_score": 0.8,
    "leg_type_suitability": 0.6,
    "lap_match_bonus": 0.7,
    "ta_spkm_best": 0.5,
    "ta_spkm_avg3": -0.3,
    "ta_n": 0.2,
    "rating_now": 1.2,
    "rating_vs_field_mean": 0.8,
    "rating_field_percentile": 0.5,
    "past_racelevel_top5_avg3": 0.4,
    "past_racelevel_top5_best": 0.6,
    "cond_match_count": 0.2,
    "cond_avg_last3f": 0.4,
    "cond_avg_time_idx": 0.3,
    "cond_pace_fast_last3f": 0.2,
    "cond_pace_slow_last3f": 0.1,
    "last3f_place_surface_diff": 0.5,
    "last3f_dist_diff": 0.4,
    "last3f_class_diff": 0.5,
    "last3f_context_value": 0.8,
    "time_idx_context_value": 0.6,
    "dl_rank_score": -1.0,
}

# 本番側の特徴量定義を最優先する。
# ここを同期しておくことで、予想側に追加した特徴量が最適化対象から漏れるのを防ぐ。
if PIPE_FEAT_COLS:
    FEAT_COLS = list(PIPE_FEAT_COLS)
    for _col in FEAT_COLS:
        if _col not in FEATURE_WEIGHTS_SEED:
            FEATURE_WEIGHTS_SEED[_col] = float(PIPE_FEATURE_WEIGHTS_BASE.get(_col, 0.0))

PLACE_MAP = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}

RACELEVEL_COLS = {
    "past_racelevel_top5_avg3",
    "past_racelevel_top5_best",
}
