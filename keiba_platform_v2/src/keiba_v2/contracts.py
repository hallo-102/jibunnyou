from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = ("race_id", "horse_no", "horse_name", "win_odds")


def load_race_table(path: str | Path) -> pd.DataFrame:
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(src)
    suffix = src.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        df = pd.read_excel(src)
    elif suffix == ".csv":
        last_error: Exception | None = None
        for enc in ("utf-8-sig", "cp932", "utf-8"):
            try:
                df = pd.read_csv(src, encoding=enc)
                break
            except UnicodeDecodeError as exc:
                last_error = exc
        else:
            raise last_error or RuntimeError("CSV encoding error")
    else:
        raise ValueError(f"unsupported input type: {suffix}")

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("race_id", "horse_name"):
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).str.strip()
    if "horse_no" in out.columns:
        out["horse_no"] = pd.to_numeric(out["horse_no"], errors="coerce").astype("Int64")
    if "win_odds" in out.columns:
        out["win_odds"] = pd.to_numeric(out["win_odds"], errors="coerce")
    return out
