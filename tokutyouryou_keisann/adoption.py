# -*- coding: utf-8 -*-
"""時系列分割と候補重みの採用ゲート。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

import pandas as pd


METRIC_NAMES = (
    "rank1_place_rate",
    "top5_point_rate",
    "top3_complete_rate",
    "place_in_top5_rate",
)


@dataclass(frozen=True)
class PeriodSplit:
    """重複のないTRAIN・VALID・TESTデータと診断情報。"""

    train: pd.DataFrame
    valid: pd.DataFrame
    test: pd.DataFrame
    unknown: pd.DataFrame
    outside: pd.DataFrame
    summary: Dict[str, Any]


def split_train_valid_test(
    frame: pd.DataFrame,
    train_start: str,
    train_end: str,
    valid_start: str,
    valid_end: str,
    test_start: str,
    test_end: str = "",
    excluded_train_files: Iterable[str] = (),
) -> PeriodSplit:
    """日付不明を除外し、境界を含む3期間へ厳密に分割する。"""
    work = frame.copy()
    if "date" not in work.columns:
        work["date"] = ""
    normalized = (
        work["date"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str[:8]
    )
    known_mask = normalized.str.fullmatch(r"\d{8}", na=False)
    work["_split_date"] = normalized

    unknown = work.loc[~known_mask].drop(columns=["_split_date"]).copy()
    known = work.loc[known_mask].copy()
    train_mask = known["_split_date"].between(train_start, train_end, inclusive="both")
    valid_mask = known["_split_date"].between(valid_start, valid_end, inclusive="both")
    test_mask = known["_split_date"].ge(test_start)
    if test_end:
        test_mask &= known["_split_date"].le(test_end)

    train = known.loc[train_mask].copy()
    if excluded_train_files and "source_file_name" in train.columns:
        train = train.loc[
            ~train["source_file_name"].astype(str).isin(set(excluded_train_files))
        ].copy()
    valid = known.loc[valid_mask].copy()
    test = known.loc[test_mask].copy()
    assigned_mask = train_mask | valid_mask | test_mask
    outside = known.loc[~assigned_mask].drop(columns=["_split_date"]).copy()

    def _ids(df: pd.DataFrame, col: str) -> set[str]:
        if col not in df.columns:
            return set()
        values = df[col].dropna().astype(str).str.strip()
        return set(values[values.ne("")])

    train_rids = _ids(train, "rid_str")
    valid_rids = _ids(valid, "rid_str")
    test_rids = _ids(test, "rid_str")
    train_files = _ids(train, "source_file_name")
    valid_files = _ids(valid, "source_file_name")
    test_files = _ids(test, "source_file_name")

    summary = {
        "train": {
            "rows": int(len(train)),
            "horses": int(len(train)),
            "races": int(len(train_rids)),
            "files": int(len(train_files)),
        },
        "valid": {
            "rows": int(len(valid)),
            "horses": int(len(valid)),
            "races": int(len(valid_rids)),
            "files": int(len(valid_files)),
        },
        "test": {
            "rows": int(len(test)),
            "horses": int(len(test)),
            "races": int(len(test_rids)),
            "files": int(len(test_files)),
        },
        "race_id_overlap": {
            "train_valid": int(len(train_rids & valid_rids)),
            "train_test": int(len(train_rids & test_rids)),
            "valid_test": int(len(valid_rids & test_rids)),
        },
        "file_overlap": {
            "train_valid": int(len(train_files & valid_files)),
            "train_test": int(len(train_files & test_files)),
            "valid_test": int(len(valid_files & test_files)),
        },
        "unknown_date_rows": int(len(unknown)),
        "outside_period_rows": int(len(outside)),
    }

    for df in (train, valid, test):
        df.drop(columns=["_split_date"], inplace=True)
    return PeriodSplit(train, valid, test, unknown, outside, summary)


def _safe_ratio(candidate: float, baseline: float) -> float:
    """baseline=0を安全に扱う。両方0なら同等、候補のみ正なら改善とする。"""
    if baseline <= 0:
        return 1.0 if candidate <= 0 else float("inf")
    return candidate / baseline


def evaluate_valid_gate(
    baseline_valid: Dict[str, float],
    candidate_valid: Dict[str, float],
    train_candidate: Dict[str, float],
    config: Dict[str, Any],
    baseline_ready: bool = True,
    baseline_error_code: str = "",
) -> Tuple[bool, list[str], Dict[str, Any]]:
    """TRAINとVALIDだけでTEST実行可否を決める。"""
    reasons: list[str] = []
    gates: Dict[str, Any] = {}
    valid_races = int(candidate_valid.get("n_bets", 0) or 0)
    min_valid = int(config.get("ADOPTION_MIN_VALID_RACES", 100) or 100)
    gates["valid_sample"] = valid_races >= min_valid
    if not gates["valid_sample"]:
        reasons.append(f"VALID母数不足: {valid_races} < {min_valid}")

    if not baseline_ready:
        code = baseline_error_code or "baseline_weight_load_failed"
        reasons.append(f"{code}: 既存best重みを安全に読み込めません")
        gates["baseline_ready"] = False
    else:
        gates["baseline_ready"] = True

    valid_thresholds = {
        "rank1_place_rate": float(
            config.get("ADOPTION_RANK1_PLACE_RATIO_MIN", 0.98)
        ),
        "top5_point_rate": float(
            config.get("ADOPTION_TOP5_POINT_RATIO_MIN", 1.00)
        ),
        "top3_complete_rate": float(
            config.get("ADOPTION_TOP3_COMPLETE_RATIO_MIN", 0.98)
        ),
        "place_in_top5_rate": float(
            config.get("ADOPTION_PLACE_IN_TOP5_RATIO_MIN", 1.00)
        ),
    }
    for metric, threshold in valid_thresholds.items():
        ratio = _safe_ratio(
            float(candidate_valid.get(metric, 0.0) or 0.0),
            float(baseline_valid.get(metric, 0.0) or 0.0),
        )
        passed = ratio >= threshold
        gates[f"valid_{metric}"] = {"ratio": ratio, "threshold": threshold, "passed": passed}
        if not passed:
            reasons.append(f"VALID {metric}比率不足: {ratio:.4f} < {threshold:.4f}")

    max_gap = float(config.get("ADOPTION_MAX_TRAIN_VALID_GAP", 0.10) or 0.10)
    gaps = {
        metric: abs(
            float(train_candidate.get(metric, 0.0) or 0.0)
            - float(candidate_valid.get(metric, 0.0) or 0.0)
        )
        for metric in METRIC_NAMES
    }
    max_observed_gap = max(gaps.values(), default=0.0)
    gates["train_valid_gap"] = {
        "values": gaps,
        "max": max_observed_gap,
        "threshold": max_gap,
        "passed": max_observed_gap <= max_gap,
    }
    if max_observed_gap > max_gap:
        reasons.append(
            f"TRAIN・VALID差が上限超過: {max_observed_gap:.4f} > {max_gap:.4f}"
        )

    valid_passed = not reasons
    gates["valid_gate_passed"] = valid_passed
    return valid_passed, reasons, gates


def evaluate_test_gate(
    baseline_test: Dict[str, float],
    candidate_test: Dict[str, float],
    config: Dict[str, Any],
) -> Tuple[bool, list[str], Dict[str, Any]]:
    """VALID通過後に限りTESTの重大悪化を確認する。"""
    reasons: list[str] = []
    gates: Dict[str, Any] = {}
    test_thresholds = {
        "rank1_place_rate": float(
            config.get("ADOPTION_TEST_RANK1_PLACE_RATIO_MIN", 0.95)
        ),
        "top5_point_rate": float(
            config.get("ADOPTION_TEST_TOP5_POINT_RATIO_MIN", 0.95)
        ),
        "top3_complete_rate": float(
            config.get("ADOPTION_TEST_TOP3_COMPLETE_RATIO_MIN", 0.90)
        ),
    }
    for metric, threshold in test_thresholds.items():
        ratio = _safe_ratio(
            float(candidate_test.get(metric, 0.0) or 0.0),
            float(baseline_test.get(metric, 0.0) or 0.0),
        )
        passed = ratio >= threshold
        gates[f"test_{metric}"] = {"ratio": ratio, "threshold": threshold, "passed": passed}
        if not passed:
            reasons.append(f"TEST {metric}重大悪化: {ratio:.4f} < {threshold:.4f}")

    baseline_n = int(baseline_test.get("n_bets", 0) or 0)
    candidate_n = int(candidate_test.get("n_bets", 0) or 0)
    min_race_ratio = float(
        config.get("ADOPTION_TEST_RACE_COUNT_RATIO_MIN", 0.95) or 0.95
    )
    race_ratio = _safe_ratio(float(candidate_n), float(baseline_n))
    race_count_passed = race_ratio >= min_race_ratio
    gates["test_race_count"] = {
        "ratio": race_ratio,
        "threshold": min_race_ratio,
        "passed": race_count_passed,
    }
    if not race_count_passed:
        reasons.append(
            f"TEST対象レース数減少: {candidate_n}/{baseline_n} ({race_ratio:.4f})"
        )

    passed = not reasons
    gates["test_gate_passed"] = passed
    return passed, reasons, gates


def evaluate_weight_adoption(
    baseline_valid: Dict[str, float],
    candidate_valid: Dict[str, float],
    baseline_test: Dict[str, float] | None,
    candidate_test: Dict[str, float] | None,
    train_candidate: Dict[str, float],
    config: Dict[str, Any],
    baseline_exists: bool = True,
) -> Tuple[str, list[str], Dict[str, Any]]:
    """後方互換API。VALID通過時だけ、呼出元が渡したTEST結果を判定する。"""
    valid_passed, reasons, gates = evaluate_valid_gate(
        baseline_valid=baseline_valid,
        candidate_valid=candidate_valid,
        train_candidate=train_candidate,
        config=config,
        baseline_ready=baseline_exists,
    )
    if not valid_passed:
        return "rejected", reasons, gates
    if baseline_test is None or candidate_test is None:
        reasons.append("TEST評価がないため採用不可")
        gates["test_gate_passed"] = False
        return "rejected", reasons, gates
    test_passed, test_reasons, test_gates = evaluate_test_gate(
        baseline_test, candidate_test, config
    )
    reasons.extend(test_reasons)
    gates.update(test_gates)
    return ("adopted" if test_passed else "rejected"), reasons, gates
