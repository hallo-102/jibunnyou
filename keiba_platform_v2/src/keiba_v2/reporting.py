from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_settled_files(
    directory: str | Path,
    pattern: str = "strategy_bets_*_T5_settled.csv",
) -> pd.DataFrame:
    """Load only T-5 settled tickets by default; morning preview tickets are excluded."""
    root = Path(directory)
    files = sorted(root.glob(pattern)) if root.exists() else []
    frames: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_csv(path, encoding="utf-8-sig")
        df["source_file"] = path.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_performance_report(settled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    if settled.empty:
        empty = pd.DataFrame()
        return empty, empty, {
            "bets": 0, "stake_yen": 0, "return_yen": 0, "profit_yen": 0,
            "roi": 0.0, "hit_rate": 0.0,
        }

    x = settled.copy()
    for col in ("stake_yen", "return_yen", "profit_yen"):
        x[col] = pd.to_numeric(x.get(col, 0), errors="coerce").fillna(0)
    x["bet_type"] = x["bet_type"].astype(str).str.upper()
    x["hit"] = x["return_yen"] > 0

    by_type = (
        x.groupby("bet_type", as_index=False)
        .agg(
            bets=("bet_type", "size"),
            hits=("hit", "sum"),
            stake_yen=("stake_yen", "sum"),
            return_yen=("return_yen", "sum"),
            profit_yen=("profit_yen", "sum"),
            avg_expected_value=("expected_value", "mean"),
        )
    )
    by_type["hit_rate"] = by_type["hits"] / by_type["bets"].clip(lower=1)
    by_type["roi"] = by_type["return_yen"] / by_type["stake_yen"].replace(0, pd.NA)
    by_type["roi"] = by_type["roi"].fillna(0.0)

    by_race = (
        x.groupby("race_id", as_index=False)
        .agg(
            bets=("race_id", "size"),
            stake_yen=("stake_yen", "sum"),
            return_yen=("return_yen", "sum"),
            profit_yen=("profit_yen", "sum"),
        )
    )
    by_race["roi"] = by_race["return_yen"] / by_race["stake_yen"].replace(0, pd.NA)
    by_race["roi"] = by_race["roi"].fillna(0.0)

    stake = int(x["stake_yen"].sum())
    ret = int(x["return_yen"].sum())
    summary = {
        "bets": int(len(x)),
        "hits": int(x["hit"].sum()),
        "stake_yen": stake,
        "return_yen": ret,
        "profit_yen": ret - stake,
        "roi": float(ret / stake) if stake else 0.0,
        "hit_rate": float(x["hit"].mean()) if len(x) else 0.0,
        "races": int(x["race_id"].astype(str).nunique()),
    }
    return by_type, by_race, summary


def write_report(settled: pd.DataFrame, destination: str | Path) -> Path:
    by_type, by_race, summary = build_performance_report(settled)
    dst = Path(destination)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(dst, engine="openpyxl") as writer:
        pd.DataFrame([summary]).to_excel(writer, index=False, sheet_name="summary")
        by_type.to_excel(writer, index=False, sheet_name="by_bet_type")
        by_race.to_excel(writer, index=False, sheet_name="by_race")
        settled.to_excel(writer, index=False, sheet_name="tickets")
    return dst
