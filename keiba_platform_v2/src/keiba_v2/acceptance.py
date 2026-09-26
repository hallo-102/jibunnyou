from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from .config import load_settings


@dataclass(frozen=True)
class AcceptanceCheck:
    name: str
    ok: bool
    detail: str

    def to_dict(self) -> dict:
        return asdict(self)


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def run_acceptance(race_date: str, settings_path: str | Path | None = None) -> dict:
    settings = load_settings(settings_path)
    root = settings.project_root
    app = settings.section("app")
    strategy = settings.section("strategy")
    race_selection_cfg = settings.section("race_selection")

    runtime_db = root / str(app.get("runtime_db", "data/runtime/keiba_v2.sqlite3"))
    schedule_path = root / "data" / "raw" / f"race_schedule_{race_date}.csv"
    results_path = root / "data" / "results" / f"results_{race_date}.csv"
    payouts_path = root / "data" / "results" / f"payouts_{race_date}.csv"
    report_path = root / "data" / "output" / "performance_report.xlsx"
    output_dir = root / "data" / "output"

    checks: list[AcceptanceCheck] = []

    schedule = _safe_read_csv(schedule_path)
    checks.append(AcceptanceCheck(
        "schedule_exists",
        not schedule.empty,
        f"{len(schedule)} races" if not schedule.empty else str(schedule_path),
    ))

    statuses = pd.DataFrame()
    daily_stake = 0
    bet_races = 0
    if runtime_db.exists():
        with sqlite3.connect(runtime_db) as conn:
            statuses = pd.read_sql_query(
                "SELECT race_id,status,run_id,error FROM t5_snapshots WHERE race_date=? ORDER BY race_id",
                conn,
                params=(str(race_date),),
            )
            stake_row = conn.execute(
                """
                SELECT COALESCE(SUM(b.stake_yen),0)
                FROM t5_snapshots t
                JOIN strategy_bets b ON b.run_id=t.run_id
                WHERE t.race_date=? AND t.status='SUCCESS'
                """,
                (str(race_date),),
            ).fetchone()
            daily_stake = int(stake_row[0] if stake_row else 0)
            race_row = conn.execute(
                """
                SELECT COUNT(DISTINCT t.race_id)
                FROM t5_snapshots t
                WHERE t.race_date=? AND t.status='SUCCESS'
                  AND EXISTS (SELECT 1 FROM strategy_bets b WHERE b.run_id=t.run_id AND b.race_id=t.race_id)
                """,
                (str(race_date),),
            ).fetchone()
            bet_races = int(race_row[0] if race_row else 0)

    checks.append(AcceptanceCheck("runtime_db_exists", runtime_db.exists(), str(runtime_db)))
    terminal_ok = not statuses.empty and statuses["status"].isin(["SUCCESS", "NO_BET"]).all()
    checks.append(AcceptanceCheck(
        "all_t5_terminal_without_failure",
        terminal_ok,
        statuses["status"].value_counts().to_dict().__str__() if not statuses.empty else "no T-5 statuses",
    ))

    if not schedule.empty and not statuses.empty:
        schedule_ids = set(schedule["race_id"].astype(str))
        status_ids = set(statuses["race_id"].astype(str))
        complete = schedule_ids == status_ids
        detail = f"scheduled={len(schedule_ids)} status_rows={len(status_ids)} missing={sorted(schedule_ids-status_ids)} extra={sorted(status_ids-schedule_ids)}"
    else:
        complete = False
        detail = "schedule or T-5 status is empty"
    checks.append(AcceptanceCheck("all_scheduled_races_accounted_for", complete, detail))

    daily_limit = int(strategy.get("daily_stake_limit_yen", 5000))
    checks.append(AcceptanceCheck(
        "daily_stake_limit",
        daily_stake <= daily_limit,
        f"stake={daily_stake} limit={daily_limit}",
    ))

    max_buy_races = int(race_selection_cfg.get("max_buy_races_per_day", 5))
    checks.append(AcceptanceCheck(
        "daily_bet_race_limit",
        bet_races <= max_buy_races,
        f"bet_races={bet_races} limit={max_buy_races}",
    ))

    results = _safe_read_csv(results_path)
    payouts = _safe_read_csv(payouts_path)
    checks.append(AcceptanceCheck("results_exist", not results.empty, f"rows={len(results)} path={results_path}"))
    checks.append(AcceptanceCheck("payouts_exist", not payouts.empty, f"rows={len(payouts)} path={payouts_path}"))

    settled_files = sorted(output_dir.glob(f"strategy_bets_{race_date}*_T5_settled.csv"))
    successful_bet_runs = int((statuses["status"] == "SUCCESS").sum()) if not statuses.empty else 0
    checks.append(AcceptanceCheck(
        "t5_bets_settled",
        len(settled_files) == successful_bet_runs,
        f"settled_files={len(settled_files)} successful_bet_runs={successful_bet_runs}",
    ))
    checks.append(AcceptanceCheck("performance_report_exists", report_path.exists(), str(report_path)))

    ok = all(check.ok for check in checks)
    return {
        "ok": ok,
        "race_date": str(race_date),
        "daily_stake_yen": daily_stake,
        "bet_races": bet_races,
        "checks": [c.to_dict() for c in checks],
    }


def write_acceptance_report(report: dict, destination: str | Path) -> Path:
    dst = Path(destination)
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst
