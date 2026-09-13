# -*- coding: utf-8 -*-
"""
v1 + formal margin + normalized time candidate.

Purpose:
- Keep v1 structure, initial ratings, market prior, surface ratings and K-factor unchanged.
- Keep the already-promising stronger margin learning.
- Replace raw-second time adjustments inside actual_score with relative time-rate adjustments.
  This reduces the distortion where the same 0.5 sec gap is treated equally at 1200m and 2400m.
- Current-race result information is used only for post-race rating updates, never for that race's pre_rating.

This is an ablation candidate only. Production v1 is not modified.
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import List, Optional


def _load_base():
    base_path = Path(__file__).with_name("00_Export_To_Excel_4.py")
    spec = importlib.util.spec_from_file_location("race_level_v1_base_margin_time_ablation", base_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"v1本体を読み込めません: {base_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


base = _load_base()

# Margin parameters: identical to formal margin candidate.
WIN_MARGIN_COEF = 0.28
WIN_MARGIN_CAP = 0.18
LOSER_GAP_COEF = 0.18
LOSER_GAP_CAP = 0.38
CLOSE_FINISH_BONUS_MAX = 0.08
CLOSE_FINISH_GAP_LIMIT = 0.60

# Time parameters: use relative rate rather than raw seconds.
# Example: 1% slower than reference => approx -0.06 on best-time term.
TIME_BEST_RATE_COEF = 6.0
TIME_MEDIAN_RATE_COEF = 3.0
TIME_BEST_NEG_CAP = -0.18
TIME_BEST_POS_CAP = 0.10
TIME_MEDIAN_NEG_CAP = -0.05
TIME_MEDIAN_POS_CAP = 0.07


def _time_rate(value: Optional[float], ref: Optional[float]) -> Optional[float]:
    if value is None or ref is None:
        return None
    try:
        value_f = float(value)
        ref_f = float(ref)
    except Exception:
        return None
    if ref_f <= 0:
        return None
    return (value_f - ref_f) / ref_f


def compute_performance_score_margin_time(
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
    score = float(rank_score)
    rc = base.normalize_race_class_key(race_class)
    pop_v = base.to_int_or_none(pop)
    odds_v = base.to_float_or_none(odds)

    # ---- stronger margin learning: same as formal margin candidate ---------
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
            bonus = CLOSE_FINISH_BONUS_MAX * (1.0 - gap / CLOSE_FINISH_GAP_LIMIT)
            score += base.clamp(bonus, 0.0, CLOSE_FINISH_BONUS_MAX)

    # ---- grade / market-surprise terms: identical to v1 -------------------
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

    # ---- normalized time learning -----------------------------------------
    ref_best = cond_best_time_sec if cond_best_time_sec is not None else best_time_sec
    ref_median = cond_median_time_sec if cond_median_time_sec is not None else median_time_sec

    if time_sec is not None:
        best_rate = _time_rate(time_sec, ref_best)
        if best_rate is not None:
            # faster than ref => negative rate => positive contribution
            score += base.clamp(
                -TIME_BEST_RATE_COEF * best_rate,
                TIME_BEST_NEG_CAP,
                TIME_BEST_POS_CAP,
            )

        median_rate = _time_rate(time_sec, ref_median)
        if median_rate is not None:
            score += base.clamp(
                -TIME_MEDIAN_RATE_COEF * median_rate,
                TIME_MEDIAN_NEG_CAP,
                TIME_MEDIAN_POS_CAP,
            )

    # ---- last3f / style: identical to v1 ----------------------------------
    if last3f is not None:
        if best_last3f is not None:
            score += base.clamp(-0.04 * (last3f - best_last3f), -0.06, 0.05)
        if median_last3f is not None:
            score += base.clamp(0.015 * (median_last3f - last3f), -0.03, 0.03)

    score += base.calc_style_bonus(rank, field, passing_positions)
    return base.clamp(score, 0.0, 1.0)


def run(input_xlsx: Path, output_xlsx: Path) -> None:
    base.compute_performance_score = compute_performance_score_margin_time

    print("[v1-margin-time] formal margin + normalized-time ablation")
    print(f"[v1-margin-time] input={input_xlsx}")
    print(f"[v1-margin-time] output={output_xlsx}")
    print(
        "[v1-margin-time] margin params "
        f"win_coef={WIN_MARGIN_COEF} win_cap={WIN_MARGIN_CAP} "
        f"loser_coef={LOSER_GAP_COEF} loser_cap={LOSER_GAP_CAP}"
    )
    print(
        "[v1-margin-time] time params "
        f"best_rate_coef={TIME_BEST_RATE_COEF} median_rate_coef={TIME_MEDIAN_RATE_COEF}"
    )

    store = base.process_excel_to_memory(input_xlsx)
    base.write_store_to_excel(store, output_xlsx, input_xlsx)
    print("[v1-margin-time] done")


def main() -> None:
    p = argparse.ArgumentParser(description="formal v1 + margin + normalized-time candidate")
    p.add_argument("--excel", type=Path, default=Path("data/master/racedata_results_clean_v3.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/race_levels_v1_margin_time.xlsx"))
    args = p.parse_args()

    src = args.excel.resolve()
    out = args.out.resolve()
    if not src.exists():
        raise FileNotFoundError(src)
    out.parent.mkdir(parents=True, exist_ok=True)
    run(src, out)


if __name__ == "__main__":
    main()
