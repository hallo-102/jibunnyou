"""xlsx loader with optional openpyxl dependency.

If openpyxl is not installed, callers get OpenpyxlMissingError with a clear
message so the GUI can show it and continue with manual input.
Unknown/missing columns are reported as '不明' - never guessed.
"""
from __future__ import annotations

import os

try:
    from storage import compute_sha256
except ImportError:  # when imported as part of a package / different cwd
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
    from storage import compute_sha256  # noqa: E402

UNKNOWN = "不明"

COLUMN_CANDIDATES = {
    "date": ["開催日", "日付", "date"],
    "place": ["競馬場", "場", "place"],
    "race_no": ["R", "レース番号", "race_no"],
    "race_id": ["race_id", "レースID"],
    "horse_no": ["馬番"],
    "horse_name": ["馬名"],
    "race_name": ["レース名", "クラス", "レース名・クラス"],
}


class OpenpyxlMissingError(Exception):
    pass


def _find_column(headers, candidates):
    for cand in candidates:
        if cand in headers:
            return headers.index(cand)
    # partial match fallback: header containing candidate (still explicit header, not data guessing)
    for idx, h in enumerate(headers):
        if h is None:
            continue
        hs = str(h)
        for cand in candidates:
            if cand in hs:
                return idx
    return None


def load_races_from_xlsx(path):
    """Return dict: {file_name, sha256, races: [...]}

    Each race: {date, place, race_no, race_id, race_name, horses:[{horse_no, horse_name}]}
    Missing items are '不明'.
    """
    try:
        import openpyxl  # type: ignore
    except ImportError as exc:
        raise OpenpyxlMissingError(
            "openpyxl がインストールされていないため xlsx を読み取れません。"
            "手動入力で継続できます。必要なら pip install openpyxl を実行してください。"
        ) from exc

    sha = compute_sha256(path)
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return {"file_name": os.path.basename(path), "sha256": sha, "races": []}
    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    col_idx = {}
    for key, candidates in COLUMN_CANDIDATES.items():
        col_idx[key] = _find_column(headers, candidates)

    def cell(row, key):
        idx = col_idx.get(key)
        if idx is None or idx >= len(row):
            return UNKNOWN
        v = row[idx]
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return UNKNOWN
        return v

    races_map = {}
    order = []
    for row in rows[1:]:
        if row is None or all(v is None for v in row):
            continue
        date = cell(row, "date")
        place = cell(row, "place")
        race_no = cell(row, "race_no")
        race_id = cell(row, "race_id")
        race_name = cell(row, "race_name")
        # Group key: race_id if known, else date/place/race_no
        key = str(race_id) if race_id != UNKNOWN else f"{date}|{place}|{race_no}"
        if key not in races_map:
            races_map[key] = {
                "date": str(date) if not isinstance(date, str) else date,
                "place": place,
                "race_no": race_no,
                "race_id": race_id,
                "race_name": race_name,
                "horses": [],
            }
            order.append(key)
        horse_no = cell(row, "horse_no")
        horse_name = cell(row, "horse_name")
        if horse_no != UNKNOWN or horse_name != UNKNOWN:
            races_map[key]["horses"].append({"horse_no": horse_no, "horse_name": horse_name})
    return {
        "file_name": os.path.basename(path),
        "sha256": sha,
        "races": [races_map[k] for k in order],
    }
