from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS horse_runs (
    source_race_id TEXT NOT NULL,
    race_id TEXT NOT NULL,
    race_date TEXT NOT NULL,
    racecourse TEXT,
    race_no INTEGER,
    horse_id TEXT NOT NULL,
    horse_name TEXT,
    horse_no INTEGER,
    finish_position REAL,
    popularity REAL,
    win_odds REAL,
    last3f REAL,
    distance REAL,
    surface TEXT,
    carried_weight REAL,
    body_weight REAL,
    PRIMARY KEY (source_race_id, horse_id)
);
CREATE INDEX IF NOT EXISTS idx_horse_runs_horse_date ON horse_runs(horse_id, race_date);
"""

HISTORY_COLUMNS = [
    "source_race_id", "race_id", "race_date", "racecourse", "race_no",
    "horse_id", "horse_name", "horse_no", "finish_position", "popularity",
    "win_odds", "last3f", "distance", "surface", "carried_weight", "body_weight",
]


class HistoryStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(HISTORY_SCHEMA)

    def upsert_runs(self, runs: pd.DataFrame) -> int:
        if runs.empty:
            return 0
        missing = {"source_race_id", "race_id", "race_date", "horse_id"} - set(runs.columns)
        if missing:
            raise ValueError(f"history rows missing columns: {sorted(missing)}")
        x = runs.copy()
        for col in HISTORY_COLUMNS:
            if col not in x.columns:
                x[col] = None
        x = x[HISTORY_COLUMNS]
        x = x[x["horse_id"].fillna("").astype(str).str.strip() != ""].copy()
        if x.empty:
            return 0
        rows = [tuple(None if pd.isna(v) else v for v in row) for row in x.itertuples(index=False, name=None)]
        placeholders = ",".join(["?"] * len(HISTORY_COLUMNS))
        updates = ",".join(f"{c}=excluded.{c}" for c in HISTORY_COLUMNS if c not in {"source_race_id", "horse_id"})
        sql = (
            f"INSERT INTO horse_runs({','.join(HISTORY_COLUMNS)}) VALUES({placeholders}) "
            f"ON CONFLICT(source_race_id,horse_id) DO UPDATE SET {updates}"
        )
        with sqlite3.connect(self.path) as conn:
            conn.executemany(sql, rows)
        return len(rows)

    def load_all(self) -> pd.DataFrame:
        with sqlite3.connect(self.path) as conn:
            return pd.read_sql_query("SELECT * FROM horse_runs ORDER BY race_date, source_race_id, horse_no", conn)

    def load_for_horses(self, horse_ids: list[str]) -> pd.DataFrame:
        ids = [str(x) for x in horse_ids if str(x)]
        if not ids:
            return pd.DataFrame(columns=HISTORY_COLUMNS)
        placeholders = ",".join(["?"] * len(ids))
        with sqlite3.connect(self.path) as conn:
            return pd.read_sql_query(
                f"SELECT * FROM horse_runs WHERE horse_id IN ({placeholders}) ORDER BY race_date",
                conn,
                params=ids,
            )


def attach_history_features(entries: pd.DataFrame, history: pd.DataFrame, n_recent: int = 5) -> pd.DataFrame:
    out = entries.copy()
    if "horse_id" not in out.columns or history.empty:
        for col, default in {
            "feature_avg_finish": 99.0,
            "feature_avg_pop": 99.0,
            "feature_avg_last3f": 99.0,
            "feature_win_rate": 0.0,
            "feature_top3_rate": 0.0,
            "feature_recent_count": 0.0,
            "feature_days_off_log": 0.0,
        }.items():
            if col not in out.columns:
                out[col] = default
        return out

    hist = history.copy()
    hist["race_date"] = pd.to_datetime(hist["race_date"], errors="coerce")
    current_dates = pd.to_datetime(out.get("race_date"), errors="coerce")
    feature_rows: list[dict] = []

    for idx, row in out.iterrows():
        horse_id = str(row.get("horse_id", "") or "")
        current_date = current_dates.loc[idx] if idx in current_dates.index else pd.NaT
        h = hist[hist["horse_id"].astype(str) == horse_id].copy()
        if pd.notna(current_date):
            h = h[h["race_date"] < current_date]
        h = h.sort_values("race_date").tail(n_recent)

        finish = pd.to_numeric(h.get("finish_position"), errors="coerce")
        pop = pd.to_numeric(h.get("popularity"), errors="coerce")
        last3f = pd.to_numeric(h.get("last3f"), errors="coerce")
        count = int(len(h))
        last_date = h["race_date"].max() if count else pd.NaT
        days_off = int((current_date - last_date).days) if pd.notna(current_date) and pd.notna(last_date) else 0

        feature_rows.append({
            "_index": idx,
            "feature_avg_finish": float(finish.mean()) if finish.notna().any() else 99.0,
            "feature_avg_pop": float(pop.mean()) if pop.notna().any() else 99.0,
            "feature_avg_last3f": float(last3f.mean()) if last3f.notna().any() else 99.0,
            "feature_win_rate": float(finish.eq(1).mean()) if count else 0.0,
            "feature_top3_rate": float(finish.le(3).mean()) if count else 0.0,
            "feature_recent_count": float(count),
            "feature_days_off_log": float(np.log1p(max(days_off, 0))),
        })

    features = pd.DataFrame(feature_rows).set_index("_index")
    for col in features.columns:
        out[col] = features[col].reindex(out.index)
    return out
