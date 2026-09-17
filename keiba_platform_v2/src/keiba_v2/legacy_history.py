from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .history import legacy_horse_id

ALIASES: dict[str, tuple[str, ...]] = {
    "race_id": ("race_id", "レースID", "RID", "rid", "rid_str"),
    "source_race_id": ("source_race_id",),
    "horse_id": ("horse_id", "馬ID", "馬id"),
    "horse_name": ("horse_name", "馬名", "馬 名", "name"),
    "horse_no": ("horse_no", "馬番", "馬番号"),
    "finish_position": ("finish_position", "着順", "着順_num"),
    "popularity": ("popularity", "人気", "人気順"),
    "win_odds": ("win_odds", "単勝", "単勝オッズ"),
    "last3f": ("last3f", "上り", "上がり", "上り3F", "後3F"),
    "distance": ("distance", "距離"),
    "surface": ("surface", "芝ダ", "芝・ダート"),
    "racecourse": ("racecourse", "競馬場", "場所"),
    "race_no": ("race_no", "R", "レース番号"),
    "carried_weight": ("carried_weight", "斤量"),
    "body_weight": ("body_weight", "馬体重"),
    "race_date": ("race_date", "日付", "開催日", "date"),
}


def _flatten_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [" ".join(str(x) for x in c if str(x) != "nan").strip() for c in out.columns]
    else:
        out.columns = [str(c).strip() for c in out.columns]
    return out


def _rename(frame: pd.DataFrame) -> pd.DataFrame:
    out = _flatten_columns(frame)
    lookup = {str(c).replace(" ", ""): c for c in out.columns}
    rename: dict[object, str] = {}
    claimed_sources: set[object] = set()
    for canonical, aliases in ALIASES.items():
        if canonical in out.columns:
            continue
        for alias in aliases:
            key = str(alias).replace(" ", "")
            source = lookup.get(key)
            if source is not None and source not in claimed_sources:
                rename[source] = canonical
                claimed_sources.add(source)
                break
    return out.rename(columns=rename)


def _date_from_text(text: str) -> str:
    m = re.search(r"(20\d{6})", str(text))
    return m.group(1) if m else ""


def _date_from_filename(path: Path) -> str:
    return _date_from_text(path.name)


def load_legacy_history(path: str | Path, *, race_date: str | None = None) -> pd.DataFrame:
    """Read an existing CSV/Excel result file without modifying it.

    Workbook sheets that do not contain horse result columns are ignored. When
    race_date is absent from rows, precedence is: explicit ``--date``, an
    8-digit date in the sheet name, then an 8-digit date in the filename.
    Horse IDs are optional; legacy name identities are generated.
    """
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(src)

    explicit_date = str(race_date or "")
    filename_date = _date_from_filename(src)

    if src.suffix.lower() in {".xlsx", ".xls", ".xlsm"}:
        raw = pd.read_excel(src, sheet_name=None)
        candidates = [(str(sheet_name), _rename(df)) for sheet_name, df in raw.items()]
    else:
        candidates = [("", _rename(pd.read_csv(src, encoding="utf-8-sig")))]

    frames: list[pd.DataFrame] = []
    for sheet_name, frame in candidates:
        required = {"race_id", "horse_name", "finish_position"}
        if not required.issubset(frame.columns):
            continue
        x = frame.copy()
        if "source_race_id" not in x.columns:
            x["source_race_id"] = x["race_id"]

        sheet_date = _date_from_text(sheet_name)
        fallback_date = explicit_date or sheet_date or filename_date
        if "race_date" not in x.columns:
            x["race_date"] = fallback_date
        else:
            x["race_date"] = x["race_date"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str[:8]
            if fallback_date:
                x.loc[x["race_date"].eq(""), "race_date"] = fallback_date
        if not x["race_date"].fillna("").astype(str).str.fullmatch(r"20\d{6}").all():
            location = f" sheet={sheet_name!r}" if sheet_name else ""
            raise ValueError(f"race_date could not be resolved for every result row: {src}{location}")

        if "horse_id" not in x.columns:
            x["horse_id"] = x["horse_name"].map(legacy_horse_id)
        else:
            x["horse_id"] = x["horse_id"].fillna("").astype(str)
            missing = x["horse_id"].str.strip().eq("")
            x.loc[missing, "horse_id"] = x.loc[missing, "horse_name"].map(legacy_horse_id)
        if "horse_no" not in x.columns:
            x["horse_no"] = pd.NA
        frames.append(x)

    if not frames:
        raise ValueError(f"no result sheets/rows recognized in {src}")
    out = pd.concat(frames, ignore_index=True)
    out["race_id"] = out["race_id"].fillna("").astype(str).str.strip()
    out["source_race_id"] = out["source_race_id"].fillna(out["race_id"]).astype(str).str.strip()
    out["finish_position"] = pd.to_numeric(out["finish_position"], errors="coerce")
    out = out.dropna(subset=["finish_position"])
    return out
