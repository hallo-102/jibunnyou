from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.legacy_bridge.normalization import (
    normalize_horse_name,
    normalize_race_id,
    pick_value,
    row_to_jsonable,
    safe_float,
    safe_int,
    safe_str,
)


TARGET_SHEET = "TARGET"
NOW_RACE_SHEET = "今走レース情報"
NUMERIC_FIELDS: dict[str, list[str]] = {
    "prediction_rank": ["予想順位", "rank"],
    "prediction_score": ["score", "total"],
    "dl_rank": ["dl_rank"],
    "dl_prob": ["dl_prob"],
    "dl_score": ["dl_score", "DL順位スコア"],
    "favorite_risk": ["favorite_risk"],
    "extra_penalty": ["extra_penalty"],
    "rest_dist_risk": ["rest_dist_risk"],
    "estimated_in3_rate": ["推定馬券内率_オッズ補正後", "推定馬券内率"],
    "expected_value": ["期待値"],
}


class PredictionGoldenMasterError(RuntimeError):
    """Raised when a prediction workbook cannot be compared safely."""


@dataclass(frozen=True)
class GoldenMasterComparison:
    baseline_rows: int
    candidate_rows: int
    compared_fields: tuple[str, ...]
    mismatch_count: int
    diagnostics: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.mismatch_count == 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["passed"] = self.passed
        return payload


def compare_prediction_workbooks(
    baseline_path: Path,
    candidate_path: Path,
    *,
    absolute_tolerance: float = 1e-8,
    max_diagnostics: int = 100,
) -> GoldenMasterComparison:
    """Compare prediction-critical values without relying on workbook byte equality."""

    if absolute_tolerance < 0:
        raise ValueError("absolute_tolerance must be non-negative")
    if max_diagnostics < 1:
        raise ValueError("max_diagnostics must be positive")

    baseline = _read_rows(baseline_path)
    candidate = _read_rows(candidate_path)
    diagnostics: list[str] = []
    mismatch_count = 0

    baseline_keys = set(baseline)
    candidate_keys = set(candidate)
    for key in sorted(baseline_keys - candidate_keys):
        mismatch_count += 1
        _append_diagnostic(diagnostics, max_diagnostics, f"candidate missing row: {key}")
    for key in sorted(candidate_keys - baseline_keys):
        mismatch_count += 1
        _append_diagnostic(diagnostics, max_diagnostics, f"candidate has extra row: {key}")

    for key in sorted(baseline_keys & candidate_keys):
        expected = baseline[key]
        actual = candidate[key]
        if expected["horse_name"] != actual["horse_name"]:
            mismatch_count += 1
            _append_diagnostic(
                diagnostics,
                max_diagnostics,
                f"horse identity differs: {key}, baseline={expected['horse_name']}, "
                f"candidate={actual['horse_name']}",
            )
        for field in NUMERIC_FIELDS:
            expected_value = expected[field]
            actual_value = actual[field]
            tolerance = 0.0 if field in {"prediction_rank", "dl_rank"} else absolute_tolerance
            if not _numeric_values_match(expected_value, actual_value, tolerance):
                mismatch_count += 1
                _append_diagnostic(
                    diagnostics,
                    max_diagnostics,
                    f"value differs: {key}, field={field}, baseline={expected_value}, "
                    f"candidate={actual_value}",
                )

    return GoldenMasterComparison(
        baseline_rows=len(baseline),
        candidate_rows=len(candidate),
        compared_fields=("horse_name", *NUMERIC_FIELDS.keys()),
        mismatch_count=mismatch_count,
        diagnostics=tuple(diagnostics),
    )


def _read_rows(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    path = path.resolve(strict=True)
    excel = pd.ExcelFile(path)
    sheet_name = TARGET_SHEET if TARGET_SHEET in excel.sheet_names else NOW_RACE_SHEET
    if sheet_name not in excel.sheet_names:
        raise PredictionGoldenMasterError(
            f"prediction workbook has no comparable sheet: {path}"
        )

    frame = pd.read_excel(path, sheet_name=sheet_name)
    rows: dict[tuple[str, int], dict[str, Any]] = {}
    for row_number, record in enumerate(frame.to_dict(orient="records"), start=2):
        raw = row_to_jsonable(record)
        race_id = normalize_race_id(pick_value(raw, ["レースID", "rid_str", "race_id"]))
        horse_no = safe_int(pick_value(raw, ["馬番", "馬 番", "umaban"]))
        horse_name = safe_str(pick_value(raw, ["馬名", "horse_name", "name"]))
        rank = safe_int(pick_value(raw, NUMERIC_FIELDS["prediction_rank"]))
        score = safe_float(pick_value(raw, NUMERIC_FIELDS["prediction_score"]))
        if race_id is None or horse_no is None or horse_name is None:
            raise PredictionGoldenMasterError(
                f"invalid prediction identity: {path.name}, sheet={sheet_name}, row={row_number}"
            )
        if rank is None or score is None:
            raise PredictionGoldenMasterError(
                f"missing prediction rank or score: {path.name}, sheet={sheet_name}, "
                f"row={row_number}"
            )

        key = (race_id, horse_no)
        if key in rows:
            raise PredictionGoldenMasterError(f"duplicate prediction identity: {path.name}, {key}")
        values: dict[str, Any] = {
            "horse_name": normalize_horse_name(horse_name),
            "prediction_rank": rank,
            "prediction_score": score,
        }
        for field, aliases in NUMERIC_FIELDS.items():
            if field in values:
                continue
            values[field] = safe_float(pick_value(raw, aliases))
        rows[key] = values

    if not rows:
        raise PredictionGoldenMasterError(f"prediction workbook contains no rows: {path}")
    return rows


def _numeric_values_match(
    expected: float | int | None,
    actual: float | int | None,
    tolerance: float,
) -> bool:
    if expected is None or actual is None:
        return expected is actual
    return abs(float(expected) - float(actual)) <= tolerance


def _append_diagnostic(diagnostics: list[str], limit: int, message: str) -> None:
    if len(diagnostics) < limit:
        diagnostics.append(message)
