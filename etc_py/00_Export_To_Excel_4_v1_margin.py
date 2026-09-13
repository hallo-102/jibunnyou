# -*- coding: utf-8 -*-
"""
v1 の着差学習だけを強化した正式アブレーション候補。

目的:
- 現行 v1 の設計・初期値・market prior・surface rating・K係数等はそのまま使う。
- compute_performance_score だけ差し替え、今回レースの着差を post_rating 更新へ
  より強く反映する。
- 今回レースの着差は今回の pre_rating には一切使わないため、予測リークはない。
- 効果は次走以降の pre_rating にのみ伝播する。

使い方:
  python -u etc_py/00_Export_To_Excel_4_v1_margin.py \
      --excel data/master/racedata_results_clean_v3.xlsx \
      --out data/master/race_levels_v1_margin.xlsx

このファイルは sibling の 00_Export_To_Excel_4.py を動的importし、関数だけpatchする。
本番v1ファイルは変更しない。
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import List, Optional


def _load_base():
    base_path = Path(__file__).with_name("00_Export_To_Excel_4.py")
    spec = importlib.util.spec_from_file_location("race_level_v1_base_margin_ablation", base_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"v1本体を読み込めません: {base_path}")
    mod = importlib.util.module_from_spec(spec)
    # dataclass は class の __module__ を sys.modules から解決するため、
    # exec_module() より前に登録しておく必要がある。
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


base = _load_base()

# ---- 正式候補の着差パラメータ ---------------------------------------------
# 現行v1より着差を重視するが、1レースの極端値に支配されないよう上限を持たせる。
WIN_MARGIN_COEF = 0.28
WIN_MARGIN_CAP = 0.18
LOSER_GAP_COEF = 0.18
LOSER_GAP_CAP = 0.38
CLOSE_FINISH_BONUS_MAX = 0.08
CLOSE_FINISH_GAP_LIMIT = 0.60


def compute_performance_score_margin(
    rank_score: float,
    rank: Optional[int],
    field: int,
    race_class: Optional[str],
    race_name: Optional[str],
    odds,
    pop,
    margin_sec: Optional[float],
    gap_from_winner_sec: Optional[float],
    winner_margin_sec: Optional[float],
    time_sec: Optional[float],
    best_time_sec: Optional[float],
    median_time_sec: Optional[float],
    cond_best_time_sec: Optional[float],
    cond_median_time_sec: Optional[float],
    last3f: Optional[float],
    best_last3f: Optional[float],
    median_last3f: Optional[float],
    passing_positions: List[int],
) -> float:
    """v1互換のactual_score。変更点は着差部分だけ。"""
    score = float(rank_score)
    rc = base.normalize_race_class_key(race_class)
    pop_v = base.to_int_or_none(pop)
    odds_v = base.to_float_or_none(odds)

    # --- 着差強化部分 ------------------------------------------------------
    if rank == 1:
        win_margin = winner_margin_sec if winner_margin_sec is not None else margin_sec
        if win_margin is not None:
            score += base.clamp(WIN_MARGIN_COEF * float(win_margin), 0.0, WIN_MARGIN_CAP)
        else:
            score += 0.02
    elif gap_from_winner_sec is not None:
        gap = max(float(gap_from_winner_sec), 0.0)
        score += base.clamp(-LOSER_GAP_COEF * gap, -LOSER_GAP_CAP, 0.0)
        if rank is not None and rank <= 5 and gap <= CLOSE_FINISH_GAP_LIMIT:
            # 0秒差で最大+0.08、0.60秒で0へ線形減衰。
            bonus = CLOSE_FINISH_BONUS_MAX * (1.0 - gap / CLOSE_FINISH_GAP_LIMIT)
            score += base.clamp(bonus, 0.0, CLOSE_FINISH_BONUS_MAX)

    # --- 以下は現行v1と同じ ------------------------------------------------
    if rc == "Ｇ１" and rank == 1:
        score += 0.10
        if base.is_classic_generation_g1(race_name):
            score += 0.04
        if pop_v is not None and pop_v <= 3:
            score += 0.03
        if odds_v is not None and odds_v <= 4.0:
            score += 0.02
    elif rc in ("Ｇ２", "Ｇ３") and rank == 1:
        score += 0.04

    if rank is not None and pop_v is not None:
        pop_gap = pop_v - rank
        if pop_gap >= 4:
            score += base.clamp(0.01 * pop_gap, 0.0, 0.05)
        elif pop_gap <= -5:
            score -= base.clamp(0.008 * abs(pop_gap), 0.0, 0.04)

    ref_best = cond_best_time_sec if cond_best_time_sec is not None else best_time_sec
    ref_median = cond_median_time_sec if cond_median_time_sec is not None else median_time_sec

    if time_sec is not None:
        if ref_best is not None:
            score += base.clamp(-0.05 * (time_sec - ref_best), -0.18, 0.10)
        if ref_median is not None:
            score += base.clamp(0.025 * (ref_median - time_sec), -0.05, 0.07)

    if last3f is not None:
        if best_last3f is not None:
            score += base.clamp(-0.04 * (last3f - best_last3f), -0.06, 0.05)
        if median_last3f is not None:
            score += base.clamp(0.015 * (median_last3f - last3f), -0.03, 0.03)

    score += base.calc_style_bonus(rank, field, passing_positions)
    return base.clamp(score, 0.0, 1.0)


def run(input_xlsx: Path, output_xlsx: Path) -> None:
    # process_excel_to_memory がglobal名 compute_performance_score を参照するため、
    # base module側の関数を差し替えれば残りの処理は完全にv1と同じ経路を通る。
    base.compute_performance_score = compute_performance_score_margin

    print("[v1-margin] formal margin ablation")
    print(f"[v1-margin] input={input_xlsx}")
    print(f"[v1-margin] output={output_xlsx}")
    print(
        "[v1-margin] params "
        f"win_coef={WIN_MARGIN_COEF} win_cap={WIN_MARGIN_CAP} "
        f"loser_coef={LOSER_GAP_COEF} loser_cap={LOSER_GAP_CAP} "
        f"close_bonus={CLOSE_FINISH_BONUS_MAX}"
    )

    store = base.process_excel_to_memory(input_xlsx)
    # v1本体の実際の書き出し関数名は write_store_to_excel。
    base.write_store_to_excel(store, output_xlsx, input_xlsx)
    print("[v1-margin] done")


def main() -> None:
    parser = argparse.ArgumentParser(description="formal v1 + margin-weighted rating candidate")
    parser.add_argument(
        "--excel",
        type=Path,
        default=Path("data/master/racedata_results_clean_v3.xlsx"),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/master/race_levels_v1_margin.xlsx"),
    )
    args = parser.parse_args()

    src = args.excel.resolve()
    out = args.out.resolve()
    if not src.exists():
        raise FileNotFoundError(src)
    out.parent.mkdir(parents=True, exist_ok=True)
    run(src, out)


if __name__ == "__main__":
    main()
