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
