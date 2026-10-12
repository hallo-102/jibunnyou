from __future__ import annotations

from pathlib import Path

import pandas as pd


ALIASES: dict[str, tuple[str, ...]] = {
    "race_id": ("race_id", "レースID", "RID", "rid"),
    "horse_no": ("horse_no", "馬番", "馬番号"),
    "horse_name": ("horse_name", "馬名"),
    "win_odds": ("win_odds", "単勝オッズ", "単勝", "odds"),
    "finish_avg": ("finish_avg", "avg_finish", "平均着順"),
    "pop_avg": ("pop_avg", "avg_pop", "平均人気"),
    "last3f_avg": ("last3f_avg", "avg_last3f", "平均上がり3F"),
    "win_rate": ("win_rate", "勝率"),
    "fast_score": ("fast_score",),
    "avg_score": ("avg_score",),
    "leg_type_suitability": ("leg_type_suitability", "脚質適性"),
    "days_off": ("days_off", "休養日数"),
    "distance": ("distance", "距離"),
    "last_distance": ("last_distance", "前走距離"),
    "body_weight_change": ("body_weight_change", "馬体重増減"),
}


def _rename_aliases(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    lookup = {str(c).strip(): c for c in out.columns}
    rename: dict[object, str] = {}
    for canonical, aliases in ALIASES.items():
        if canonical in out.columns:
            continue
        for alias in aliases:
            if alias in lookup:
                rename[lookup[alias]] = canonical
                break
    return out.rename(columns=rename)


def load_legacy_excel(path: str | Path, sheet_name: str | int | None = 0) -> pd.DataFrame:
    """Read a legacy workbook without modifying it and map common Japanese columns."""
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(src)
    df = pd.read_excel(src, sheet_name=sheet_name)
    if isinstance(df, dict):
        frames = []
        for name, frame in df.items():
            x = frame.copy()
            x["source_sheet"] = str(name)
            frames.append(x)
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return _rename_aliases(df)


def export_canonical_csv(df: pd.DataFrame, destination: str | Path) -> Path:
    dst = Path(destination)
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False, encoding="utf-8-sig")
    return dst
