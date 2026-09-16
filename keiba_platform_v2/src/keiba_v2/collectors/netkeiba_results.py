from __future__ import annotations

import io
import re
import time
from pathlib import Path

import pandas as pd

from .netkeiba import PLACE_NAMES, UA, _canonical_race_id, _horse_id_from_href, _require_collection_deps


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [str(c[-1]).strip() for c in out.columns]
    else:
        out.columns = [str(c).strip() for c in out.columns]
    return out


def _find_col(df: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    normalized = {str(c).replace(" ", ""): c for c in df.columns}
    for name in names:
        if name in normalized:
            return normalized[name]
    return None


def _place_from_text(text: str) -> str:
    for place in PLACE_NAMES:
        if place in text:
            return place
    return ""


def _race_meta_from_text(text: str) -> tuple[str, float | None]:
    surface = ""
    if "芝" in text:
        surface = "芝"
    elif "ダ" in text or "ダート" in text:
        surface = "ダート"
    m = re.search(r"(?:芝|ダ|ダート)\s*(\d{3,4})m", text)
    distance = float(m.group(1)) if m else None
    return surface, distance


def _horse_ids_by_number(soup) -> dict[int, str]:
    result: dict[int, str] = {}
    for tr in soup.select("tr.HorseList"):
        no_cell = tr.select_one("td.Umaban")
        horse = tr.select_one("td.HorseInfo a")
        if no_cell is None or horse is None:
            continue
        no_text = re.sub(r"\D", "", no_cell.get_text(" ", strip=True))
        if not no_text:
            continue
        result[int(no_text)] = _horse_id_from_href(str(horse.get("href", "")))
    return result


def collect_one_result(race_date: str, source_race_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    requests, BeautifulSoup, _, _ = _require_collection_deps()
    url = f"https://race.netkeiba.com/race/result.html?race_id={source_race_id}&rf=race_list"
    response = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    text = soup.get_text(" ", strip=True)
    place = _place_from_text(text)
    race_no = int(source_race_id[-2:]) if source_race_id[-2:].isdigit() else 0
    canonical_id = _canonical_race_id(race_date, place or "UNKNOWN", race_no)
    surface, distance = _race_meta_from_text(text)
    horse_ids = _horse_ids_by_number(soup)

    tables = pd.read_html(io.StringIO(response.text))
    if not tables:
        raise RuntimeError(f"result table missing: {source_race_id}")
    result_table = _flatten_columns(tables[0])
    c_finish = _find_col(result_table, ("着順",))
    c_no = _find_col(result_table, ("馬番",))
    c_name = _find_col(result_table, ("馬名",))
    c_pop = _find_col(result_table, ("人気",))
    c_odds = _find_col(result_table, ("単勝",))
    c_last3f = _find_col(result_table, ("上り", "上がり"))
    c_weight = _find_col(result_table, ("斤量",))
    c_body = _find_col(result_table, ("馬体重",))
    if c_finish is None or c_no is None or c_name is None:
        raise RuntimeError(f"essential result columns missing: {source_race_id} columns={list(result_table.columns)}")

    rows: list[dict] = []
    for _, r in result_table.iterrows():
        no = pd.to_numeric(r.get(c_no), errors="coerce")
        finish = pd.to_numeric(str(r.get(c_finish, "")).replace("中", ""), errors="coerce")
        if pd.isna(no):
            continue
        body_value = str(r.get(c_body, "")) if c_body else ""
        body_match = re.search(r"(\d+)", body_value)
        rows.append({
            "source_race_id": source_race_id,
            "race_id": canonical_id,
            "race_date": race_date,
            "racecourse": place,
            "race_no": race_no,
            "horse_id": horse_ids.get(int(no), ""),
            "horse_name": str(r.get(c_name, "")).strip(),
            "horse_no": int(no),
            "finish_position": finish,
            "popularity": pd.to_numeric(r.get(c_pop), errors="coerce") if c_pop else None,
            "win_odds": pd.to_numeric(r.get(c_odds), errors="coerce") if c_odds else None,
            "last3f": pd.to_numeric(r.get(c_last3f), errors="coerce") if c_last3f else None,
            "distance": distance,
            "surface": surface,
            "carried_weight": pd.to_numeric(r.get(c_weight), errors="coerce") if c_weight else None,
            "body_weight": float(body_match.group(1)) if body_match else None,
        })

    payout_rows: list[dict] = []
    pay_targets = {
        "Tansho": "WIN",
        "Umaren": "QUINELLA",
        "Fuku3": "TRIO",
    }
    for css_class, bet_type in pay_targets.items():
        tr = soup.find("tr", class_=css_class)
        if not tr:
            continue
        nums = [n.get_text(strip=True) for n in tr.select("td.Result span") if n.get_text(strip=True)]
        payout_cell = tr.find("td", class_="Payout")
        payouts = payout_cell.get_text("|", strip=True).split("|") if payout_cell else []
        step = 1 if bet_type == "WIN" else 2 if bet_type == "QUINELLA" else 3
        for i in range(0, len(nums), step):
            selection_nums = nums[i:i + step]
            if len(selection_nums) != step:
                continue
            selection = "-".join(str(v) for v in sorted(int(x) for x in selection_nums if x.isdigit()))
            payout_text = payouts[i // step] if i // step < len(payouts) else ""
            payout = pd.to_numeric(re.sub(r"\D", "", payout_text), errors="coerce")
            payout_rows.append({
                "race_id": canonical_id,
                "bet_type": bet_type,
                "selection": selection,
                "payout_yen_per_100": int(payout) if pd.notna(payout) else 0,
            })

    return pd.DataFrame(rows), pd.DataFrame(payout_rows)


def collect_results_for_entries(entries: pd.DataFrame, *, pause_sec: float = 0.25) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {"race_date", "source_race_id"}
    missing = required - set(entries.columns)
    if missing:
        raise ValueError(f"entries missing result collection columns: {sorted(missing)}")
    races = entries[["race_date", "source_race_id"]].drop_duplicates().sort_values(["race_date", "source_race_id"])
    result_frames: list[pd.DataFrame] = []
    payout_frames: list[pd.DataFrame] = []
    for _, row in races.iterrows():
        result, payout = collect_one_result(str(row["race_date"]), str(row["source_race_id"]))
        if not result.empty:
            result_frames.append(result)
        if not payout.empty:
            payout_frames.append(payout)
        time.sleep(pause_sec)
    results = pd.concat(result_frames, ignore_index=True) if result_frames else pd.DataFrame()
    payouts = pd.concat(payout_frames, ignore_index=True) if payout_frames else pd.DataFrame()
    return results, payouts


def save_results(results: pd.DataFrame, payouts: pd.DataFrame, output_dir: str | Path, race_date: str) -> tuple[Path, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    result_path = out / f"results_{race_date}.csv"
    payout_path = out / f"payouts_{race_date}.csv"
    results.to_csv(result_path, index=False, encoding="utf-8-sig")
    payouts.to_csv(payout_path, index=False, encoding="utf-8-sig")
    return result_path, payout_path
