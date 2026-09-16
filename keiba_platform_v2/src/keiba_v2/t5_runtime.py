from __future__ import annotations

import hashlib
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .collectors.jra_odds import collect_jra_race_odds
from .collectors.schedule import collect_one_start_time
from .config import load_settings
from .orchestrator import run_pipeline
from .storage import RunStore

JST = ZoneInfo("Asia/Tokyo")


def _target_datetime(race_date: str, start_time: str) -> datetime:
    return datetime.strptime(f"{race_date} {start_time}", "%Y%m%d %H:%M").replace(tzinfo=JST) - timedelta(minutes=5)


def _output_tag(race_date: str, race_no: int, race_id: str) -> str:
    digest = hashlib.sha1(str(race_id).encode("utf-8")).hexdigest()[:8]
    return f"{race_date}_{int(race_no):02d}R_{digest}_T5"


def _merge_t5_odds(base_race: pd.DataFrame, runner_odds: pd.DataFrame) -> pd.DataFrame:
    base = base_race.copy().drop(columns=["win_odds", "place_odds"], errors="ignore")
    odds = runner_odds.copy().rename(columns={"horse_name": "odds_horse_name", "win_odds": "t5_win_odds"})
    merged = base.merge(
        odds[["race_id", "horse_no", "odds_horse_name", "t5_win_odds"]],
        on=["race_id", "horse_no"],
        how="inner",
        validate="one_to_one",
    )
    if merged.empty:
        raise RuntimeError("T-5 odds did not match any entry rows")
    if len(merged) != len(base_race) or len(merged) != len(runner_odds):
        raise RuntimeError("T-5 runner/entry count mismatch")
    merged["win_odds"] = pd.to_numeric(merged["t5_win_odds"], errors="coerce")
    if merged["win_odds"].isna().any() or (merged["win_odds"] <= 0).any():
        raise RuntimeError("T-5 odds contain invalid win odds")
    return merged.drop(columns=["t5_win_odds", "odds_horse_name"], errors="ignore")


def process_t5_race(
    race_date: str,
    race_id: str,
    racecourse: str,
    race_no: int,
    input_path: str | Path,
    settings_path: str | Path | None = None,
    *,
    headless: bool = True,
) -> dict:
    settings = load_settings(settings_path)
    app_cfg = settings.section("app")
    store = RunStore(settings.project_root / str(app_cfg.get("runtime_db", "data/runtime/keiba_v2.sqlite3")))
    if store.is_t5_done(race_id):
        return {"race_id": race_id, "status": "ALREADY_DONE"}

    base = pd.read_csv(input_path, encoding="utf-8-sig", dtype={"race_id": str, "race_date": str})
    base_race = base[base["race_id"].astype(str) == str(race_id)].copy()
    if base_race.empty:
        raise RuntimeError(f"race not found in canonical input: {race_id}")

    last_error: Exception | None = None
    runners = combos = None
    for delay in (0, 5, 10):
        if delay:
            time.sleep(delay)
        try:
            runners, combos = collect_jra_race_odds(race_date, racecourse, int(race_no), headless=headless)
            break
        except Exception as exc:
            last_error = exc
    if runners is None or combos is None:
        raise RuntimeError(f"T-5 odds failed after 3 attempts: {race_id}: {last_error}")

    t5_input = _merge_t5_odds(base_race, runners)
    runtime_dir = settings.project_root / "data" / "runtime" / "t5" / hashlib.sha1(str(race_id).encode("utf-8")).hexdigest()[:12]
    runtime_dir.mkdir(parents=True, exist_ok=True)
    input_snapshot = runtime_dir / "runners.csv"
    combo_snapshot = runtime_dir / "combination_odds.csv"
    runners_snapshot = runtime_dir / "jra_runner_odds.csv"
    t5_input.to_csv(input_snapshot, index=False, encoding="utf-8-sig")
    runners.to_csv(runners_snapshot, index=False, encoding="utf-8-sig")

    combo_path: Path | None = None
    if not combos.empty:
        combos.to_csv(combo_snapshot, index=False, encoding="utf-8-sig")
        combo_path = combo_snapshot

    existing_daily_stake = store.successful_t5_stake(race_date)
    result = run_pipeline(
        input_snapshot,
        race_date,
        settings_path,
        combo_path,
        output_tag=_output_tag(race_date, int(race_no), race_id),
        existing_daily_stake_yen=existing_daily_stake,
    )
    store.mark_t5_result(race_id, "SUCCESS", run_id=result["run_id"])
    return {
        "race_id": race_id,
        "status": "SUCCESS",
        "run_id": result["run_id"],
        "existing_daily_stake_yen": existing_daily_stake,
        "metrics": result["metrics"],
        "strategy_path": str(result["strategy_path"]),
    }


def run_t5_runtime(
    input_path: str | Path,
    schedule_path: str | Path,
    race_date: str,
    settings_path: str | Path | None = None,
    *,
    headless: bool = True,
    poll_seconds: float = 1.0,
) -> list[dict]:
    settings = load_settings(settings_path)
    app_cfg = settings.section("app")
    store = RunStore(settings.project_root / str(app_cfg.get("runtime_db", "data/runtime/keiba_v2.sqlite3")))
    schedule = pd.read_csv(schedule_path, encoding="utf-8-sig", dtype={"race_id": str, "race_date": str, "source_race_id": str})
    schedule = schedule[schedule["race_date"].astype(str) == str(race_date)].copy().reset_index(drop=True)
    if schedule.empty:
        raise RuntimeError(f"empty schedule for {race_date}")
    required = {"race_id", "racecourse", "race_no", "start_time", "source_race_id"}
    missing = required - set(schedule.columns)
    if missing:
        raise ValueError(f"T-5 schedule missing columns: {sorted(missing)}")

    for _, race in schedule.iterrows():
        target = _target_datetime(str(race_date), str(race["start_time"]))
        store.mark_t5_scheduled(str(race["race_id"]), str(race_date), target.isoformat())

    finished: list[dict] = []
    terminal: set[str] = set()
    refreshed: set[str] = set()
    next_refresh_attempt: dict[str, datetime] = {}

    while len(terminal) < len(schedule):
        now = datetime.now(JST)
        for idx, race in schedule.iterrows():
            race_id = str(race["race_id"])
            if race_id in terminal:
                continue
            if store.is_t5_done(race_id):
                terminal.add(race_id)
                continue

            target = _target_datetime(str(race_date), str(schedule.at[idx, "start_time"]))
            refresh_from = target - timedelta(minutes=25)  # start time - 30 minutes
            if race_id not in refreshed and now >= refresh_from:
                retry_at = next_refresh_attempt.get(race_id)
                if retry_at is None or now >= retry_at:
                    try:
                        latest_start = collect_one_start_time(str(race["source_race_id"]))
                        schedule.at[idx, "start_time"] = latest_start
                        target = _target_datetime(str(race_date), latest_start)
                        store.mark_t5_scheduled(race_id, str(race_date), target.isoformat())
                        refreshed.add(race_id)
                    except Exception as exc:
                        next_refresh_attempt[race_id] = now + timedelta(seconds=60)
                        window_start_old = target - timedelta(seconds=30)
                        if now >= window_start_old:
                            store.mark_t5_result(race_id, "FAILED", error=f"start-time refresh failed: {exc}")
                            terminal.add(race_id)
                            finished.append({"race_id": race_id, "status": "FAILED", "error": f"start-time refresh failed: {exc}"})
                            continue

            target = _target_datetime(str(race_date), str(schedule.at[idx, "start_time"]))
            window_start = target - timedelta(seconds=30)
            window_end = target + timedelta(seconds=30)
            if now < window_start:
                continue
            if race_id not in refreshed:
                store.mark_t5_result(race_id, "FAILED", error="start time was not reconfirmed before T-5 window")
                terminal.add(race_id)
                finished.append({"race_id": race_id, "status": "FAILED", "error": "start time was not reconfirmed before T-5 window"})
                continue
            if now > window_end:
                store.mark_t5_result(race_id, "MISSED", error=f"T-5 window missed at {now.isoformat()}")
                terminal.add(race_id)
                finished.append({"race_id": race_id, "status": "MISSED"})
                continue

            try:
                result = process_t5_race(
                    str(race_date), race_id, str(race["racecourse"]), int(race["race_no"]),
                    input_path, settings_path, headless=headless,
                )
                finished.append(result)
            except Exception as exc:
                store.mark_t5_result(race_id, "FAILED", error=str(exc))
                finished.append({"race_id": race_id, "status": "FAILED", "error": str(exc)})
            terminal.add(race_id)
        if len(terminal) < len(schedule):
            time.sleep(max(0.2, poll_seconds))
    return finished
