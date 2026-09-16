from __future__ import annotations

from pathlib import Path

import pandas as pd

from .collectors.daily import collect_daily_dataset
from .collectors.netkeiba_results import collect_results_for_entries, save_results
from .config import load_settings
from .history import HistoryStore
from .orchestrator import run_pipeline


def _prefect():
    try:
        from prefect import flow, task
    except ImportError as exc:
        raise RuntimeError("Prefect is not installed. Install with: pip install -e .[ml]") from exc
    return flow, task


def build_morning_flow():
    flow, task = _prefect()

    @task(retries=2, retry_delay_seconds=15)
    def collect(race_date: str, settings_path: str | None = None) -> dict:
        settings = load_settings(settings_path)
        result = collect_daily_dataset(race_date, settings.project_root, headless=True)
        return {
            "canonical_path": str(result["canonical_path"]),
            "combination_odds_path": str(result["combination_odds_path"]),
            "entry_path": str(result["entry_path"]),
        }

    @task(retries=1, retry_delay_seconds=10)
    def predict(paths: dict, race_date: str, settings_path: str | None = None) -> dict:
        result = run_pipeline(
            paths["canonical_path"],
            race_date,
            settings_path,
            paths["combination_odds_path"],
        )
        return {
            **paths,
            "run_id": result["run_id"],
            "metrics": result["metrics"],
            "prediction_path": str(result["prediction_path"]),
            "strategy_path": str(result["strategy_path"]),
        }

    @flow(name="keiba-platform-v2-morning")
    def morning_flow(race_date: str, settings_path: str | None = None) -> dict:
        paths = collect(race_date, settings_path)
        return predict(paths, race_date, settings_path)

    return morning_flow


def build_result_flow():
    flow, task = _prefect()

    @task(retries=2, retry_delay_seconds=30)
    def collect_results(entries_path: str, race_date: str, settings_path: str | None = None) -> dict:
        settings = load_settings(settings_path)
        entries = pd.read_csv(entries_path, encoding="utf-8-sig", dtype={"source_race_id": str, "race_date": str})
        results, payouts = collect_results_for_entries(entries)
        result_path, payout_path = save_results(results, payouts, settings.project_root / "data" / "results", race_date)
        history_path = settings.project_root / str(settings.section("app").get("history_db", "data/runtime/history.sqlite3"))
        inserted = HistoryStore(history_path).upsert_runs(results)
        return {
            "result_path": str(result_path),
            "payout_path": str(payout_path),
            "history_rows_upserted": inserted,
        }

    @flow(name="keiba-platform-v2-results")
    def result_flow(entries_path: str, race_date: str, settings_path: str | None = None) -> dict:
        return collect_results(entries_path, race_date, settings_path)

    return result_flow


def run_morning_prefect(race_date: str, settings_path: str | None = None) -> dict:
    return build_morning_flow()(race_date, settings_path)


def run_result_prefect(entries_path: str, race_date: str, settings_path: str | None = None) -> dict:
    return build_result_flow()(entries_path, race_date, settings_path)
