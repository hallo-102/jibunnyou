from __future__ import annotations

from pathlib import Path

from .orchestrator import run_pipeline


def build_flow():
    try:
        from prefect import flow, task
    except ImportError as exc:
        raise RuntimeError("Prefect is not installed. Install with: pip install -e .[ml]") from exc

    @task(retries=2, retry_delay_seconds=10)
    def execute(input_path: str, race_date: str, settings_path: str | None = None) -> dict:
        result = run_pipeline(Path(input_path), race_date, settings_path)
        return {
            "metrics": result["metrics"],
            "prediction_path": str(result["prediction_path"]),
            "odds_path": str(result["odds_path"]),
        }

    @flow(name="keiba-platform-v2-daily")
    def daily_flow(input_path: str, race_date: str, settings_path: str | None = None) -> dict:
        return execute(input_path, race_date, settings_path)

    return daily_flow


def run_prefect(input_path: str, race_date: str, settings_path: str | None = None) -> dict:
    return build_flow()(input_path, race_date, settings_path)
