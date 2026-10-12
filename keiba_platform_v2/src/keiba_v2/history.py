from __future__ import annotations

import re
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
CREATE INDEX IF NOT EXISTS idx_horse_runs_name_date ON horse_runs(horse_name, race_date);
"""

HISTORY_COLUMNS = [
    "source_race_id", "race_id", "race_date", "racecourse", "race_no",
    "horse_id", "horse_name", "horse_no", "finish_position", "popularity",
    "win_odds", "last3f", "distance", "surface", "carried_weight", "body_weight",
]


def normalize_horse_name(value: object) -> str:
    return re.sub(r"[\s\u3000]+", "", str(value or "").strip())


def legacy_horse_id(horse_name: object) -> str:
    name = normalize_horse_name(horse_name)
    return f"NAME:{name}" if name else ""


class HistoryStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(HISTORY_SCHEMA)

    def upsert_runs(self, runs: pd.DataFrame) -> int:
        if runs.empty:
            return 0
        missing = {"source_race_id", "race_id", "race_date"} - set(runs.columns)
        if missing:
            raise ValueError(f"history rows missing columns: {sorted(missing)}")
        x = runs.copy()
        for col in HISTORY_COLUMNS:
            if col not in x.columns:
                x[col] = None
        x["horse_name"] = x["horse_name"].fillna("").astype(str).str.strip()
        x["horse_id"] = x["horse_id"].fillna("").astype(str).str.strip()
        missing_id = x["horse_id"].eq("")
        x.loc[missing_id, "horse_id"] = x.loc[missing_id, "horse_name"].map(legacy_horse_id)
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

    def load_for_horses(self, horse_ids: list[str], horse_names: list[str] | None = None) -> pd.DataFrame:
        ids = [str(x) for x in horse_ids if str(x)]
        names = [normalize_horse_name(x) for x in (horse_names or []) if normalize_horse_name(x)]
        clauses: list[str] = []
        params: list[str] = []
        if ids:
            clauses.append(f"horse_id IN ({','.join(['?'] * len(ids))})")
            params.extend(ids)
        if names:
            legacy_ids = [f"NAME:{name}" for name in names]
            clauses.append(f"horse_id IN ({','.join(['?'] * len(legacy_ids))})")
            params.extend(legacy_ids)
        if not clauses:
            return pd.DataFrame(columns=HISTORY_COLUMNS)
        with sqlite3.connect(self.path) as conn:
            return pd.read_sql_query(
                f"SELECT * FROM horse_runs WHERE {' OR '.join(clauses)} ORDER BY race_date",
                conn,
                params=params,
            )


def _select_horse_history(hist: pd.DataFrame, horse_id: str, horse_name: str) -> pd.DataFrame:
    if hist.empty:
        return hist.copy()
    h = hist[hist["horse_id"].astype(str) == str(horse_id)].copy() if horse_id else hist.iloc[0:0].copy()
    if h.empty and horse_name:
        legacy_id = legacy_horse_id(horse_name)
        h = hist[hist["horse_id"].astype(str) == legacy_id].copy()
    if h.empty and "horse_name" in hist.columns and horse_name:
        normalized = hist["horse_name"].map(normalize_horse_name)
        h = hist[normalized == normalize_horse_name(horse_name)].copy()
    return h


def attach_history_features(entries: pd.DataFrame, history: pd.DataFrame, n_recent: int = 5) -> pd.DataFrame:
    out = entries.copy()
    defaults = {
        "feature_avg_finish": 99.0,
        "feature_avg_pop": 99.0,
        "feature_avg_last3f": 99.0,
        "feature_win_rate": 0.0,
        "feature_top3_rate": 0.0,
        "feature_recent_count": 0.0,
        "feature_days_off_log": 0.0,
    }
    if history.empty:
        for col, default in defaults.items():
            if col not in out.columns:
                out[col] = default
        return out

    hist = history.copy()
    hist["race_date"] = pd.to_datetime(hist["race_date"], errors="coerce")
    if "race_date" in out.columns:
        current_dates = pd.to_datetime(out["race_date"], errors="coerce")
    else:
        current_dates = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")
    feature_rows: list[dict] = []

    for idx, row in out.iterrows():
        horse_id = str(row.get("horse_id", "") or "")
        horse_name = normalize_horse_name(row.get("horse_name", ""))
        current_date = current_dates.loc[idx] if idx in current_dates.index else pd.NaT
        h = _select_horse_history(hist, horse_id, horse_name)
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

    features = pd.DataFrame(feature_rows).set_index("_index") if feature_rows else pd.DataFrame()
    for col, default in defaults.items():
        if col in features.columns:
            out[col] = features[col].reindex(out.index).fillna(default)
        elif col not in out.columns:
            out[col] = default
    return out


def build_training_dataset(history: pd.DataFrame, n_recent: int = 5) -> pd.DataFrame:
    """Create labeled rows using only races strictly before each target race date."""
    if history.empty:
        return pd.DataFrame()
    required = {"race_id", "race_date", "horse_id", "horse_no", "horse_name", "finish_position", "win_odds"}
    missing = required - set(history.columns)
    if missing:
        raise ValueError(f"history missing training columns: {sorted(missing)}")

    targets = history.copy()
    targets["race_date"] = pd.to_datetime(targets["race_date"], errors="coerce")
    targets = targets.dropna(subset=["race_date", "horse_id", "finish_position"]).copy()
    targets["race_date"] = targets["race_date"].dt.strftime("%Y%m%d")
    featured = attach_history_features(targets, history, n_recent=n_recent)
    featured["is_winner"] = pd.to_numeric(featured["finish_position"], errors="coerce").eq(1).astype(int)
    featured["is_top3"] = pd.to_numeric(featured["finish_position"], errors="coerce").le(3).astype(int)
    return featured.sort_values(["race_date", "race_id", "horse_no"]).reset_index(drop=True)
