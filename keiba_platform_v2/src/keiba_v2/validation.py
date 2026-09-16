from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .contracts import REQUIRED_COLUMNS, canonicalize


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    def raise_for_error(self) -> None:
        if not self.ok:
            raise ValueError("; ".join(self.errors))


def validate_races(df: pd.DataFrame, settings: dict) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return ValidationResult(False, (f"missing columns: {missing}",), ())

    data = canonicalize(df)
    if data.empty:
        errors.append("input is empty")
        return ValidationResult(False, tuple(errors), tuple(warnings))

    if (data["race_id"] == "").any():
        errors.append("race_id contains blanks")
    if data["horse_no"].isna().any():
        errors.append("horse_no contains invalid values")
    if data["horse_name"].eq("").any():
        errors.append("horse_name contains blanks")
    if data.duplicated(["race_id", "horse_no"]).any():
        errors.append("duplicate race_id + horse_no")

    valid_horse_no = data["horse_no"].dropna().between(1, 18)
    if not valid_horse_no.all():
        errors.append("horse_no must be between 1 and 18")

    if bool(settings.get("require_positive_odds", True)):
        if data["win_odds"].isna().any() or (data["win_odds"] <= 0).any():
            errors.append("win_odds must be positive")

    min_horses = int(settings.get("min_horses_per_race", 5))
    max_horses = int(settings.get("max_horses_per_race", 18))
    counts = data.groupby("race_id")["horse_no"].nunique()
    too_small = counts[counts < min_horses]
    too_large = counts[counts > max_horses]
    if not too_small.empty:
        errors.append(f"too few horses: {too_small.to_dict()}")
    if not too_large.empty:
        errors.append(f"too many horses: {too_large.to_dict()}")

    if data["win_odds"].max(skipna=True) > 1000:
        warnings.append("extremely large win_odds detected")

    return ValidationResult(not errors, tuple(errors), tuple(warnings))
