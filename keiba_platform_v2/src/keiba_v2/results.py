from __future__ import annotations

from pathlib import Path

import pandas as pd


RESULT_ALIASES: dict[str, tuple[str, ...]] = {
    "race_id": ("race_id", "レースID", "RID", "rid"),
    "horse_no": ("horse_no", "馬番", "馬番号"),
    "finish_position": ("finish_position", "着順", "着順_num"),
    "win_payout_yen_per_100": ("win_payout_yen_per_100", "単勝払戻", "単勝払戻金"),
}


def _rename(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    lookup = {str(c).strip(): c for c in out.columns}
    ren: dict[object, str] = {}
    for canonical, aliases in RESULT_ALIASES.items():
        if canonical in out.columns:
            continue
        for alias in aliases:
            if alias in lookup:
                ren[lookup[alias]] = canonical
                break
    return out.rename(columns=ren)


def load_results(path: str | Path, sheet_name: str | int | None = 0) -> pd.DataFrame:
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(src)
    if src.suffix.lower() in {".xlsx", ".xls", ".xlsm"}:
        df = pd.read_excel(src, sheet_name=sheet_name)
    else:
        df = pd.read_csv(src, encoding="utf-8-sig")
    if isinstance(df, dict):
        df = pd.concat(df.values(), ignore_index=True) if df else pd.DataFrame()
    out = _rename(df)
    if "race_id" in out.columns:
        out["race_id"] = out["race_id"].fillna("").astype(str).str.strip()
    if "horse_no" in out.columns:
        out["horse_no"] = pd.to_numeric(out["horse_no"], errors="coerce").astype("Int64")
    if "finish_position" in out.columns:
        out["finish_position"] = pd.to_numeric(out["finish_position"], errors="coerce")
    if "win_payout_yen_per_100" in out.columns:
        out["win_payout_yen_per_100"] = pd.to_numeric(out["win_payout_yen_per_100"], errors="coerce").fillna(0)
    return out


def normalize_combination(selection: str) -> str:
    values = [int(x) for x in str(selection).replace(" ", "").split("-") if x]
    return "-".join(str(v) for v in sorted(values))


def evaluate_strategy_bets(bets: pd.DataFrame, results: pd.DataFrame, payouts: pd.DataFrame | None = None) -> pd.DataFrame:
    """Attach returns to WIN/QUINELLA/TRIO strategy tickets.

    Preferred payout format: race_id, bet_type, selection, payout_yen_per_100.
    For WIN only, embedded result-column payouts remain supported as fallback.
    """
    if bets.empty:
        return bets.assign(return_yen=pd.Series(dtype=int), profit_yen=pd.Series(dtype=int))
    out = bets.copy()
    out["return_yen"] = 0.0

    payout_map: dict[tuple[str, str, str], float] = {}
    if payouts is not None and not payouts.empty:
        required = {"race_id", "bet_type", "selection", "payout_yen_per_100"}
        missing = required - set(payouts.columns)
        if missing:
            raise ValueError(f"payouts missing columns: {sorted(missing)}")
        p = payouts.copy()
        p["race_id"] = p["race_id"].astype(str)
        p["bet_type"] = p["bet_type"].astype(str).str.upper()
        p["selection"] = p["selection"].astype(str).map(normalize_combination)
        p["payout_yen_per_100"] = pd.to_numeric(p["payout_yen_per_100"], errors="coerce").fillna(0.0)
        payout_map = {
            (str(r["race_id"]), str(r["bet_type"]), str(r["selection"])): float(r["payout_yen_per_100"])
            for _, r in p.iterrows()
        }

    if "win_payout_yen_per_100" in results.columns:
        winners = results[pd.to_numeric(results.get("finish_position"), errors="coerce").eq(1)].copy()
        for _, r in winners.dropna(subset=["horse_no"]).iterrows():
            key = (str(r["race_id"]), "WIN", normalize_combination(str(int(r["horse_no"]))))
            payout_map.setdefault(key, float(r.get("win_payout_yen_per_100", 0) or 0))

    for idx, row in out.iterrows():
        race_id = str(row["race_id"])
        bet_type = str(row["bet_type"]).upper()
        selection = normalize_combination(str(row["selection"]))
        stake = float(row.get("stake_yen", 0) or 0)
        payout = payout_map.get((race_id, bet_type, selection), 0.0)
        out.at[idx, "return_yen"] = payout * (stake / 100.0)

    out["return_yen"] = out["return_yen"].round().astype(int)
    out["stake_yen"] = pd.to_numeric(out["stake_yen"], errors="coerce").fillna(0).astype(int)
    out["profit_yen"] = out["return_yen"] - out["stake_yen"]
    return out
