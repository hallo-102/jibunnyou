# ============================================================
# keibayosou_config.py（完全コード）
# 修正内容：
# - race_levels.xlsx / 場所_馬場_タイム.xlsx を複数候補から自動検出
# - 既存 pipeline 互換用に DL_PROB_BLEND などを追加
# - 条件一致の新特徴量を FEAT_COLS / 初期重み / 日本語名に追加
# - 外部重みファイルを「丸ごと置換」ではなく「既存重みに上書きマージ」
# 修正必要箇所以外は既存のまま
# ============================================================

# -*- coding: utf-8 -*-
"""共通の定数・設定・特徴量重みをまとめたモジュール。"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import importlib.util  # best_feature_weights_YYYYMMDD.py 動的読込用

# ================================================================
# パス等の基本設定
# ================================================================

TARGET_SHEET = "TARGET"           # 特徴量を出力するシート名
NOW_SHEET = "今走レース情報"      # 今走用シート名

# horses シート名（keibayosou_features.py が参照）
HORSES_SHEET = "horses"

# 出力用カラム（互換用：存在しても未使用なら問題なし）
OUT_COLS = [
    "rid_str",
    "馬番",
    "馬名",
    "score",
    "rank",
    "favorite_risk",
    "extra_penalty",
    "rest_dist_risk",
]

CODE_DIR = Path(__file__).resolve().parent

# データ配置ルート（あなたの環境に合わせて固定）
DATA_ROOT = Path(r"C:\Users\okino\OneDrive\ドキュメント\my_python_cursor")

EXCEL_DIR = CODE_DIR / "data" / "input"
CSV_DIR = DATA_ROOT / "csv"
EXE_DIR = DATA_ROOT / "exe"
INI_DIR = DATA_ROOT / "ini"
PY_DIR = CODE_DIR / "yosou_py"
JSON_DIR = DATA_ROOT / "json"

BASE_DIR = CODE_DIR
HORSE_RESULTS_DIR = EXCEL_DIR


# ================================================================
# ここを修正：複数候補からファイルを自動検出
# ================================================================
def _pick_existing_file(*candidates: Path) -> Path:
    """
    候補を順番に見て、最初に存在するファイルを返す。
    どれも無ければ先頭候補を返す。
    """
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            pass
    return candidates[0]


RACE_LEVEL_XLSX = _pick_existing_file(
    CODE_DIR / "data" / "input" / "race_levels.xlsx",
    CODE_DIR / "data" / "master" / "race_levels.xlsx",
    CODE_DIR / "xlsx" / "race_levels.xlsx",
    CODE_DIR / "race_levels.xlsx",
)

BASE_TIME_XLSX = _pick_existing_file(
    CODE_DIR / "data" / "input" / "場所_馬場_タイム.xlsx",
    CODE_DIR / "data" / "master" / "場所_馬場_タイム.xlsx",
    CODE_DIR / "xlsx" / "場所_馬場_タイム.xlsx",
    CODE_DIR / "場所_馬場_タイム.xlsx",
    CODE_DIR / "data" / "input" / "base_time.xlsx",
    CODE_DIR / "data" / "master" / "base_time.xlsx",
    CODE_DIR / "xlsx" / "base_time.xlsx",
    CODE_DIR / "base_time.xlsx",
)

ODDS_CSV = CODE_DIR / "data" / "ozzu_csv"
SUCCESS_REPORT = EXCEL_DIR / "success_report.xlsx"


# ================================================================
# 既存 pipeline 互換設定
# ================================================================
# keibayosou_pipeline.py が import しているため必須
DL_PROB_BLEND = 0.35
DL_RANK_BLEND = 0.00
DL_SCORE_BONUS = 10.0

# 互換用で残しておく
USE_DL_PROB = True
USE_DL_RANK = True


# ================================================================
# 特徴量重み（デフォルト）  ※外部ファイルが無いとき用フォールバック
# ================================================================

FEATURE_WEIGHTS_BASE: Dict[str, float] = {
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
    "master_rating": 0.003,
    "master_start_count": 0.02,
    "master_recent_rating": 0.004,
    "master_rating_confidence": 0.8,
    "master_recent_start_count_180d": 0.04,
    "master_rating_volatility": -0.01,
    "master_rating_vs_field_mean": 0.03,
    "master_recent_rating_vs_field_mean": 0.04,
    "master_rating_field_percentile": 0.8,
    "master_recent_rating_field_percentile": 1.0,
    "master_rating_confidence_adjusted": 0.04,
    "past_racelevel_top5_avg3": 0.4,
    "past_racelevel_top5_best": 0.6,
    # ここから追加
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
    # 既存
    "dl_rank_score": -1.0,
}

FEATURE_WEIGHTS_SAPPORO = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_HAKODATE = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_FUKUSHIMA = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_NIIGATA = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_TOKYO = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_NAKAYAMA = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_CHUKYO = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_KYOTO = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_HANSHIN = FEATURE_WEIGHTS_BASE
FEATURE_WEIGHTS_KOKURA = FEATURE_WEIGHTS_BASE

FEATURE_WEIGHTS: Dict[str, Dict[str, float]] = {
    "__default__": dict(FEATURE_WEIGHTS_BASE),
    "札幌": dict(FEATURE_WEIGHTS_SAPPORO),
    "函館": dict(FEATURE_WEIGHTS_HAKODATE),
    "福島": dict(FEATURE_WEIGHTS_FUKUSHIMA),
    "新潟": dict(FEATURE_WEIGHTS_NIIGATA),
    "東京": dict(FEATURE_WEIGHTS_TOKYO),
    "中山": dict(FEATURE_WEIGHTS_NAKAYAMA),
    "中京": dict(FEATURE_WEIGHTS_CHUKYO),
    "京都": dict(FEATURE_WEIGHTS_KYOTO),
    "阪神": dict(FEATURE_WEIGHTS_HANSHIN),
    "小倉": dict(FEATURE_WEIGHTS_KOKURA),
}

FEATURE_WEIGHTS_BY_PLACE_SURFACE: Dict[Tuple[str, str], Dict[str, float]] = {}

# 実績相関と重みの符号が逆になりやすい特徴量の符号ガード。
# data/output/current_weight_backtest/feature_diagnostics_current_weight_backtest.xlsx
# の weight_corr_mismatch シートで確認した corr_top3 の向きを採用する。
EMPIRICAL_WEIGHT_SIGN_GUARD: Dict[str, int] = {
    "avg_pop": -1,
    "avg_score": 1,
    "rating_field_percentile": 1,
    "master_rating": 1,
    "master_start_count": 1,
    "master_recent_rating": 1,
    "master_rating_confidence": 1,
    "master_recent_start_count_180d": 1,
    "master_rating_volatility": -1,
    "master_rating_vs_field_mean": 1,
    "master_recent_rating_vs_field_mean": 1,
    "master_rating_field_percentile": 1,
    "master_recent_rating_field_percentile": 1,
    "master_rating_confidence_adjusted": 1,
    "avg_margin": -1,
    "recent_pop_trend": 1,
    "rating_now": 1,
    "leg_type_suitability": 1,
    "ta_spkm_best": 1,
    "recent_time_idx_trend": 1,
    "dl_rank_score": -1,
    "fast_score": 1,
}

# 使う特徴量（列名）
FEAT_COLS = [
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
    "master_rating",
    "master_start_count",
    "master_recent_rating",
    "master_rating_confidence",
    "master_recent_start_count_180d",
    "master_rating_volatility",
    "master_rating_vs_field_mean",
    "master_recent_rating_vs_field_mean",
    "master_rating_field_percentile",
    "master_recent_rating_field_percentile",
    "master_rating_confidence_adjusted",
    "past_racelevel_top5_avg3",
    "past_racelevel_top5_best",
    # ここから追加
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
    # 既存
    "dl_rank_score",
]

# 人気馬リスク補正係数
ALPHA = 20.0

# ================================================================
# 条件付きペナルティ
# ================================================================
EXTRA_ALPHA = 10.0

PEN_FAV_POP_TH = 3.0
PEN_FAV_POP_K = 0.6
PEN_FAV_POP_APPLY_WINRATE_MAX = 0.14
PEN_FAV_POP_APPLY_PLACEHOLDER = True

PEN_GOOD_LOSER_L = 3.5
PEN_GOOD_LOSER_H = 6.5
PEN_GOOD_LOSER_K = 0.8
PEN_GOOD_LOSER_APPLY_WINRATE_MAX = 0.12

PEN_TA_N_CAP = 9.0
PEN_TA_N_K = 0.18

PEN_CLOSE_LOSS_FINISH_TH = 4.0
PEN_CLOSE_LOSS_MARGIN_TH = 0.30
PEN_CLOSE_LOSS_K = 0.7
PEN_CLOSE_LOSS_APPLY_WINRATE_MAX = 0.16

# ================================================================
# 追加：休養×距離差 の危険馬スコア設定
# ================================================================
REST_RISK_TH_DAYS = 50.0
REST_RISK_MAX_DAYS = 100.0

DIST_RISK_TH_M = 50.0
DIST_RISK_MAX_M = 100.0

REST_DIST_RISK_K = 0.8

# ================================================================
# 日本語名（TARGET出力用）
# ================================================================
JAPANESE_FEATURE_NAMES: Dict[str, str] = {
    "avg_finish": "平均着順",
    "avg_pop": "平均人気",
    "dist_diff": "距離差",
    "days_off": "休養日数",
    "avg_last3f": "平均上がり3F",
    "avg_margin": "平均着差",
    "avg_time_idx": "タイム指数",
    "recent3_finish": "近3走平均着順",
    "recent3_pop": "近3走平均人気",
    "recent3_last3f": "近3走平均上がり3F",
    "recent3_time_idx": "近3走平均タイム指数",
    "recent_finish_trend": "近走着順上昇度",
    "recent_pop_trend": "近走人気上昇度",
    "recent_time_idx_trend": "近走タイム上昇度",
    "win_rate": "勝率",
    "fast_score": "高速適性",
    "avg_score": "平均指数",
    "leg_type_suitability": "脚質適性",
    "lap_match_bonus": "ラップ適合",
    "ta_spkm_best": "1F最速",
    "ta_spkm_avg3": "1F平均3走",
    "ta_n": "レース数",
    "rating_now": "現在レーティング",
    "rating_vs_field_mean": "レーティング差",
    "rating_field_percentile": "レーティング百分位",
    "master_rating": "マスタ総合rating",
    "master_start_count": "マスタ出走数",
    "master_recent_rating": "マスタ近走rating",
    "master_rating_confidence": "マスタrating信頼度",
    "master_recent_start_count_180d": "マスタ近180日出走数",
    "master_rating_volatility": "マスタrating変動幅",
    "master_rating_vs_field_mean": "マスタrating平均との差",
    "master_recent_rating_vs_field_mean": "マスタ近走rating平均との差",
    "master_rating_field_percentile": "マスタrating百分位",
    "master_recent_rating_field_percentile": "マスタ近走rating百分位",
    "master_rating_confidence_adjusted": "マスタ信頼度補正rating差",
    "past_racelevel_top5_avg3": "過去レベル平均",
    "past_racelevel_top5_best": "過去レベル最高",
    # ここから追加
    "cond_match_count": "条件一致レース数",
    "cond_avg_last3f": "条件一致平均上がり3F",
    "cond_avg_time_idx": "条件一致平均タイム指数",
    "cond_pace_fast_last3f": "ハイペース時平均上がり3F",
    "cond_pace_slow_last3f": "スローペース時平均上がり3F",
    "last3f_place_surface_diff": "競馬場芝ダ補正上がり差",
    "last3f_dist_diff": "距離補正上がり差",
    "last3f_class_diff": "クラス補正上がり差",
    "last3f_context_value": "条件文脈上がり価値",
    "time_idx_context_value": "条件文脈タイム価値",
    # 既存
    "dl_rank_score": "DL順位スコア",
    "delay_rate": "出遅れ率",
    "rest_dist_risk": "休養×距離差リスク",
}

DELAY_KEYWORDS = [
    "出遅れ",
    "スタート悪",
    "スタートで後手",
    "ゲートで後手",
    "ゲートで出負け",
    "出負け",
    "出脚鈍",
    "ダッシュつかず",
    "二の脚つかず",
    "行き脚がつかず",
]

# ================================================================
# 外部特徴量重みファイル(best_feature_weights_YYYYMMDD.py) 読み込み
# ================================================================
def _normalize_weight_object(obj: Any) -> Dict[str, float]:
    """FEATURE_WEIGHTS の値を {feat: weight} 形式の dict に揃える"""
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, (list, tuple)):
        if not obj:
            return {}
        first = obj[0]
        if isinstance(first, dict):
            return dict(first)
    try:
        return dict(obj)  # type: ignore[arg-type]
    except Exception:
        print(f"[WARN] FEATURE_WEIGHTS の値の型が想定外です: {type(obj)} -> 無視します")
        return {}


def _normalize_weights_mapping(raw_fw: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    fixed: Dict[str, Dict[str, float]] = {}
    for key, val in raw_fw.items():
        fixed[key] = _normalize_weight_object(val)
    return fixed


def _find_latest_weights_module(base_dir: str) -> Optional[Tuple[str, str]]:
    pattern = re.compile(r"best_feature_weights_(\d{8})\.py$")
    latest: Optional[Tuple[str, str]] = None

    base = Path(base_dir)
    if not base.exists():
        return None

    for p in base.iterdir():
        if not p.is_file():
            continue
        m = pattern.match(p.name)
        if not m:
            continue
        day = m.group(1)
        if (latest is None) or (day > latest[0]):
            latest = (day, str(p))
    return latest


def _merge_feature_weights(
    base_fw: Dict[str, Dict[str, float]],
    ext_fw: Dict[str, Dict[str, float]],
) -> Dict[str, Dict[str, float]]:
    """
    外部重みを丸ごと置換せず、既存重みに上書きマージする。
    新特徴量が外部ファイルに無い場合でも、既存の初期重みを残す。
    """
    merged: Dict[str, Dict[str, float]] = {}
    for key, weights in base_fw.items():
        merged[key] = dict(weights)

    default_weights = dict(base_fw.get("__default__", {}))

    for key, ext_weights in ext_fw.items():
        if key in merged:
            tmp = dict(merged[key])
            tmp.update(ext_weights)
            merged[key] = tmp
        else:
            tmp = dict(default_weights)
            tmp.update(ext_weights)
            merged[key] = tmp

    if "__default__" not in merged:
        merged["__default__"] = dict(default_weights)

    return merged


def _merge_feature_weights_by_place_surface(
    base_fw_by_ps: Dict[Tuple[str, str], Dict[str, float]],
    ext_fw_by_ps: Dict[Tuple[str, str], Dict[str, float]],
    default_weights: Dict[str, float],
) -> Dict[Tuple[str, str], Dict[str, float]]:
    """
    place×surface別重みも既存を残しつつ上書きマージする。
    """
    merged: Dict[Tuple[str, str], Dict[str, float]] = {}
    for key, weights in base_fw_by_ps.items():
        merged[key] = dict(weights)

    for key, ext_weights in ext_fw_by_ps.items():
        tmp = dict(merged.get(key, default_weights))
        tmp.update(ext_weights)
        merged[key] = tmp

    return merged


def _enforce_empirical_weight_signs(
    weights_map: Dict[Any, Dict[str, float]],
) -> Dict[Any, Dict[str, float]]:
    """
    実績相関の向きと逆符号になっている重みを読み込み時点で補正する。
    値の絶対値は最適化結果を尊重し、符号だけを corr_top3 の向きへ揃える。
    """
    fixed: Dict[Any, Dict[str, float]] = {}
    for key, weights in weights_map.items():
        new_weights = dict(weights)
        for feat, expected_sign in EMPIRICAL_WEIGHT_SIGN_GUARD.items():
            if feat not in new_weights:
                continue
            try:
                val = float(new_weights[feat])
            except Exception:
                continue
            if val == 0:
                continue
            if (val > 0 and expected_sign < 0) or (val < 0 and expected_sign > 0):
                new_weights[feat] = abs(val) * expected_sign
        fixed[key] = new_weights
    return fixed


def _load_external_feature_weights(base_dir: str):
    """
    best_feature_weights_YYYYMMDD.py を動的 import して FEATURE_WEIGHTS を取得する。
    戻り値: (FEATURE_WEIGHTS, FEATURE_WEIGHTS_BY_PLACE_SURFACE)
    見つからなければ (None, None)
    """
    latest = _find_latest_weights_module(base_dir)
    if latest is None:
        print("[INFO] best_feature_weights_YYYYMMDD.py が見つからなかったので組み込み重みを使用します")
        return None, None

    day, path = latest
    module_name = f"_best_feature_weights_{day}"

    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"spec_from_file_location が None を返しました: {path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        raw_fw = getattr(module, "FEATURE_WEIGHTS", None)
        raw_fw_by_ps = getattr(module, "FEATURE_WEIGHTS_BY_PLACE_SURFACE", None)

        if raw_fw is None:
            print(f"[WARN] {Path(path).name} に FEATURE_WEIGHTS が定義されていないため無視します")
            return None, None

        fw = _normalize_weights_mapping(dict(raw_fw))

        fw_by_ps_norm: Optional[Dict[Tuple[str, str], Dict[str, float]]] = None
        if isinstance(raw_fw_by_ps, dict):
            fw_by_ps_norm = {}
            for k, v in raw_fw_by_ps.items():
                if not isinstance(k, tuple) or len(k) != 2:
                    continue
                fw_by_ps_norm[(str(k[0]), str(k[1]))] = _normalize_weight_object(v)

        print(f"[INFO] 外部重みファイル {Path(path).name} を読み込みました（最新日付={day}）")
        return fw, fw_by_ps_norm

    except Exception as e:
        print(f"[WARN] 外部重みファイル {Path(path).name} の読み込みに失敗しました: {e}")
        return None, None


try:
    _ext_fw, _ext_fw_by_ps = _load_external_feature_weights(str(PY_DIR))
    if _ext_fw is not None:
        FEATURE_WEIGHTS = _merge_feature_weights(FEATURE_WEIGHTS, _ext_fw)
    FEATURE_WEIGHTS = _enforce_empirical_weight_signs(FEATURE_WEIGHTS)
    if _ext_fw_by_ps is not None:
        FEATURE_WEIGHTS_BY_PLACE_SURFACE = _merge_feature_weights_by_place_surface(
            FEATURE_WEIGHTS_BY_PLACE_SURFACE,
            _ext_fw_by_ps,
            FEATURE_WEIGHTS.get("__default__", {}),
        )
    FEATURE_WEIGHTS_BY_PLACE_SURFACE = _enforce_empirical_weight_signs(FEATURE_WEIGHTS_BY_PLACE_SURFACE)
except Exception as e:
    print(f"[WARN] 外部重み読込時にエラーが発生しました: {e}")
