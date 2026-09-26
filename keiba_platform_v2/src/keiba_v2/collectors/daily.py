from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .jra_odds import collect_jra_odds, save_odds
from .netkeiba import collect_entries, save_entries
from .schedule import collect_start_times


def _norm_name(value: object) -> str:
    return re.sub(r"[\s\u3000]+", "", str(value or "").strip())


def merge_entries_and_odds(entries: pd.DataFrame, odds: pd.DataFrame) -> pd.DataFrame:
    required_entries = {"race_id", "horse_no", "horse_name"}
    required_odds = {"race_id", "horse_no", "win_odds"}
    if required_entries - set(entries.columns):
        raise ValueError(f"entries missing: {sorted(required_entries - set(entries.columns))}")
    if required_odds - set(odds.columns):
        raise ValueError(f"odds missing: {sorted(required_odds - set(odds.columns))}")

    left = entries.copy().drop(columns=["win_odds", "place_odds"], errors="ignore")
    right = odds.copy().rename(columns={"horse_name": "odds_horse_name"})
    merged = left.merge(
        right[[c for c in ["race_id", "horse_no", "odds_horse_name", "win_odds", "place_odds"] if c in right.columns]],
        on=["race_id", "horse_no"], how="left", validate="one_to_one",
    )
    if "win_odds" not in merged.columns:
        raise RuntimeError("JRA win_odds column disappeared during merge")
    merged["win_odds"] = pd.to_numeric(merged["win_odds"], errors="coerce")
    if merged["win_odds"].isna().any() or (merged["win_odds"] <= 0).any():
        missing = merged.loc[merged["win_odds"].isna() | (merged["win_odds"] <= 0), ["race_id", "horse_no", "horse_name"]]
        raise RuntimeError(f"JRA odds missing/invalid for entry rows: {missing.head(10).to_dict('records')}")

    if "odds_horse_name" in merged.columns:
        mismatch = merged.apply(
            lambda r: bool(_norm_name(r["odds_horse_name"])) and _norm_name(r["horse_name"]) != _norm_name(r["odds_horse_name"]),
            axis=1,
        )
        if mismatch.any():
            rows = merged.loc[mismatch, ["race_id", "horse_no", "horse_name", "odds_horse_name"]]
            raise RuntimeError(f"horse name mismatch between netkeiba and JRA: {rows.head(10).to_dict('records')}")
        merged = merged.drop(columns=["odds_horse_name"])
    return merged.sort_values(["race_id", "horse_no"]).reset_index(drop=True)


def collect_daily_dataset(race_date: str, project_root: str | Path, *, headless: bool = True) -> dict:
    root = Path(project_root)
    raw_dir = root / "data" / "raw"
    input_dir = root / "data" / "input"
    raw_dir.mkdir(parents=True, exist_ok=True)
    input_dir.mkdir(parents=True, exist_ok=True)

    entries = collect_entries(race_date, headless=headless)
    schedule = collect_start_times(entries)
    entries = entries.merge(schedule[["race_id", "start_time"]], on="race_id", how="left", validate="many_to_one")
    runners, combinations = collect_jra_odds(race_date, headless=headless)
    if combinations.empty and len(combinations.columns) == 0:
        combinations = pd.DataFrame(columns=[
            "race_id", "race_date", "racecourse", "race_no",
            "bet_type", "selection", "odds",
        ])
    merged = merge_entries_and_odds(entries, runners)

    entry_path = save_entries(entries, raw_dir / f"netkeiba_entries_{race_date}.csv")
    schedule_path = raw_dir / f"race_schedule_{race_date}.csv"
    schedule.to_csv(schedule_path, index=False, encoding="utf-8-sig")
    runner_odds_path, combo_path = save_odds(runners, combinations, raw_dir, race_date)
    canonical_path = input_dir / f"races_{race_date}.csv"
    merged.to_csv(canonical_path, index=False, encoding="utf-8-sig")

    return {
        "entries": entries,
        "schedule": schedule,
        "runner_odds": runners,
        "combination_odds": combinations,
        "canonical": merged,
        "entry_path": entry_path,
        "schedule_path": schedule_path,
        "runner_odds_path": runner_odds_path,
        "combination_odds_path": combo_path,
        "canonical_path": canonical_path,
    }
