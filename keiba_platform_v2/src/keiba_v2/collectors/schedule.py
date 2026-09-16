from __future__ import annotations

import re
import time

import pandas as pd

from .netkeiba import UA, _require_collection_deps


def collect_start_times(entries: pd.DataFrame) -> pd.DataFrame:
    """Fetch race start times for canonical entries using source netkeiba race IDs."""
    required = {"race_id", "race_date", "racecourse", "race_no", "source_race_id"}
    missing = required - set(entries.columns)
    if missing:
        raise ValueError(f"entries missing schedule columns: {sorted(missing)}")

    requests, BeautifulSoup, _, _ = _require_collection_deps()
    races = entries[list(required)].drop_duplicates().sort_values(["race_date", "racecourse", "race_no"])
    session = requests.Session()
    rows: list[dict] = []
    for _, race in races.iterrows():
        source_race_id = str(race["source_race_id"])
        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={source_race_id}"
        response = session.get(url, headers={"User-Agent": UA}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        text = soup.get_text(" ", strip=True)
        m = re.search(r"(?:発走\s*)?(\d{1,2}):(\d{2})", text)
        if not m:
            raise RuntimeError(f"start time not found: source_race_id={source_race_id}")
        start_time = f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
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
