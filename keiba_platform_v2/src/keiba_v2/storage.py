from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    race_date TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    metrics_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS strategy_bets (
    run_id TEXT NOT NULL,
    race_id TEXT NOT NULL,
    bet_type TEXT NOT NULL,
    selection TEXT NOT NULL,
    stake_yen INTEGER NOT NULL,
    reason TEXT,
    expected_value REAL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, race_id, bet_type, selection)
);
CREATE TABLE IF NOT EXISTS strategy_results (
    run_id TEXT NOT NULL,
    race_id TEXT NOT NULL,
    bet_type TEXT NOT NULL,
    selection TEXT NOT NULL,
    return_yen INTEGER NOT NULL,
    profit_yen INTEGER NOT NULL,
    settled_at TEXT NOT NULL,
    PRIMARY KEY (run_id, race_id, bet_type, selection)
);
CREATE TABLE IF NOT EXISTS t5_snapshots (
    race_id TEXT PRIMARY KEY,
    race_date TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    captured_at TEXT,
    status TEXT NOT NULL,
    run_id TEXT,
    error TEXT
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def start_run(self, run_id: str, race_date: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO runs(run_id,race_date,started_at,finished_at,status,metrics_json) VALUES(?,?,?,?,?,?)",
                (run_id, race_date, utc_now(), None, "RUNNING", "{}"),
            )

    def finish_run(self, run_id: str, status: str, metrics: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE runs SET finished_at=?, status=?, metrics_json=? WHERE run_id=?",
                (utc_now(), status, json.dumps(metrics, ensure_ascii=False), run_id),
            )

    def save_strategy_bets(self, run_id: str, bets: list[dict]) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO strategy_bets(run_id,race_id,bet_type,selection,stake_yen,reason,expected_value,created_at) VALUES(?,?,?,?,?,?,?,?)",
                [
                    (
                        run_id,
                        str(b["race_id"]),
                        str(b["bet_type"]),
                        str(b["selection"]),
                        int(b["stake_yen"]),
                        str(b.get("reason", "")),
                        None if b.get("expected_value") is None else float(b["expected_value"]),
                        now,
                    )
                    for b in bets
                ],
            )

    def save_strategy_results(self, run_id: str, rows: list[dict]) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO strategy_results(run_id,race_id,bet_type,selection,return_yen,profit_yen,settled_at) VALUES(?,?,?,?,?,?,?)",
                [
                    (
                        run_id,
                        str(r["race_id"]),
                        str(r["bet_type"]),
                        str(r["selection"]),
                        int(r.get("return_yen", 0)),
                        int(r.get("profit_yen", 0)),
                        now,
                    )
                    for r in rows
                ],
            )

    def is_t5_done(self, race_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT status FROM t5_snapshots WHERE race_id=?",
                (str(race_id),),
            ).fetchone()
        return bool(row and row["status"] == "SUCCESS")

    def successful_t5_stake(self, race_date: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(b.stake_yen), 0) AS total
                FROM t5_snapshots t
                JOIN strategy_bets b ON b.run_id = t.run_id
                WHERE t.race_date=? AND t.status='SUCCESS'
                """,
                (str(race_date),),
            ).fetchone()
        return int(row["total"] if row else 0)

    def mark_t5_scheduled(self, race_id: str, race_date: str, scheduled_at: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO t5_snapshots(race_id,race_date,scheduled_at,status) VALUES(?,?,?,?) "
                "ON CONFLICT(race_id) DO UPDATE SET race_date=excluded.race_date, scheduled_at=excluded.scheduled_at "
                "WHERE t5_snapshots.status != 'SUCCESS'",
                (str(race_id), str(race_date), str(scheduled_at), "SCHEDULED"),
            )

    def mark_t5_result(self, race_id: str, status: str, *, run_id: str | None = None, error: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE t5_snapshots SET captured_at=?, status=?, run_id=?, error=? WHERE race_id=?",
                (utc_now(), str(status), run_id, error, str(race_id)),
            )
