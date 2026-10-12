from __future__ import annotations

import re
import time

import pandas as pd

from .netkeiba import UA, _require_collection_deps


def _parse_start_time(soup, *, source_race_id: str) -> str:
    texts: list[str] = []
    for selector in ("div.RaceData01", ".RaceData01", "div.RaceData02", ".RaceData02"):
        for node in soup.select(selector):
            texts.append(node.get_text(" ", strip=True))
    if not texts:
        texts.append(soup.get_text(" ", strip=True))

    for text in texts:
        m = re.search(r"(\d{1,2}):(\d{2})\s*発走", text)
        if not m:
            m = re.search(r"発走\s*(\d{1,2}):(\d{2})", text)
        if m:
            hour = int(m.group(1))
            minute = int(m.group(2))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return f"{hour:02d}:{minute:02d}"
    raise RuntimeError(f"start time not found: source_race_id={source_race_id}")


def collect_one_start_time(source_race_id: str) -> str:
    requests, BeautifulSoup, _, _ = _require_collection_deps()
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={source_race_id}"
    response = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    return _parse_start_time(soup, source_race_id=source_race_id)


def collect_start_times(entries: pd.DataFrame) -> pd.DataFrame:
    """Fetch race start times for canonical entries using source netkeiba race IDs."""
    required = {"race_id", "race_date", "racecourse", "race_no", "source_race_id"}
    missing = required - set(entries.columns)
    if missing:
        raise ValueError(f"entries missing schedule columns: {sorted(missing)}")

    races = entries[list(required)].drop_duplicates().sort_values(["race_date", "racecourse", "race_no"])
    rows: list[dict] = []
    for _, race in races.iterrows():
        source_race_id = str(race["source_race_id"])
        start_time = collect_one_start_time(source_race_id)
        rows.append({
            "race_id": str(race["race_id"]),
            "race_date": str(race["race_date"]),
            "racecourse": str(race["racecourse"]),
            "race_no": int(race["race_no"]),
            "source_race_id": source_race_id,
            "start_time": start_time,
        })
        time.sleep(0.2)
    return pd.DataFrame(rows)
