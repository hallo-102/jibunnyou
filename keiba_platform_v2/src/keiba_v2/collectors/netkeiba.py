from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

PLACE_NAMES = ("札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉")
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class CollectedRace:
    source_race_id: str
    canonical_race_id: str
    racecourse: str
    race_no: int


def _require_collection_deps():
    try:
        import requests
        from bs4 import BeautifulSoup
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError as exc:
        raise RuntimeError("install collection dependencies: pip install -e .[collection]") from exc
    return requests, BeautifulSoup, webdriver, Options


def _canonical_race_id(race_date: str, place: str, race_no: int) -> str:
    return f"{race_date}_{place}_{race_no:02d}R"


def _extract_place(text: str) -> str:
    for place in PLACE_NAMES:
        if place in text:
            return place
    return ""


def _horse_id_from_href(href: str) -> str:
    m = re.search(r"/horse/(?:result/)?(\d+)", str(href or ""))
    return m.group(1) if m else ""


def collect_race_ids(race_date: str, *, headless: bool = True, timeout_sec: int = 40) -> list[str]:
    _, BeautifulSoup, webdriver, Options = _require_collection_deps()
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={race_date}"
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument(f"--user-agent={UA}")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(timeout_sec)
    try:
        driver.get(url)
        time.sleep(1.0)
        soup = BeautifulSoup(driver.page_source, "html.parser")
    finally:
        driver.quit()

    ids: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = str(a.get("href", ""))
        if "race_id=" not in href:
            continue
        rid = parse_qs(urlparse(href).query).get("race_id", [""])[0]
        if rid.isdigit():
            ids.add(rid)
    return sorted(ids)


def _parse_runner_rows(soup, race_date: str, source_race_id: str) -> pd.DataFrame:
    page_text = soup.get_text(" ", strip=True)
    place = _extract_place(page_text)
    race_no = int(source_race_id[-2:]) if source_race_id[-2:].isdigit() else 0
    canonical_id = _canonical_race_id(race_date, place or "UNKNOWN", race_no)

    rows: list[dict] = []
    for tr in soup.select("tr.HorseList"):
        no_cell = tr.select_one("td.Umaban")
        horse = tr.select_one("td.HorseInfo a")
        if no_cell is None or horse is None:
            continue
        no_text = re.sub(r"\D", "", no_cell.get_text(" ", strip=True))
        if not no_text:
            continue
        horse_name = horse.get_text(" ", strip=True)
        horse_id = _horse_id_from_href(str(horse.get("href", "")))
        age_cell = tr.select_one("td.Barei")
        weight_cell = tr.select_one("td.Weight")
        jockey_cell = tr.select_one("td.Jockey a")
        trainer_cell = tr.select_one("td.Trainer a")
        odds_cell = tr.select_one("td.Popular span") or tr.select_one("td.Popular")

        sex_age = age_cell.get_text(" ", strip=True) if age_cell else ""
        age_match = re.search(r"(\d+)", sex_age)
        win_odds = None
        if odds_cell is not None:
            odds_match = re.search(r"\d+(?:\.\d+)?", odds_cell.get_text(" ", strip=True))
            if odds_match:
                win_odds = float(odds_match.group(0))

        rows.append({
            "race_id": canonical_id,
            "source_race_id": source_race_id,
            "race_date": race_date,
            "racecourse": place,
            "race_no": race_no,
            "horse_id": horse_id,
            "horse_no": int(no_text),
            "horse_name": horse_name,
            "sex_age": sex_age,
            "age": int(age_match.group(1)) if age_match else None,
            "carried_weight": pd.to_numeric(weight_cell.get_text(" ", strip=True), errors="coerce") if weight_cell else None,
            "jockey": jockey_cell.get_text(" ", strip=True) if jockey_cell else "",
            "trainer": trainer_cell.get_text(" ", strip=True) if trainer_cell else "",
            "win_odds": win_odds,
        })
    return pd.DataFrame(rows)


def collect_entries(race_date: str, *, headless: bool = True) -> pd.DataFrame:
    requests, BeautifulSoup, _, _ = _require_collection_deps()
    race_ids = collect_race_ids(race_date, headless=headless)
    if not race_ids:
        raise RuntimeError(f"no netkeiba races found: {race_date}")

    session = requests.Session()
    frames: list[pd.DataFrame] = []
    for rid in race_ids:
        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={rid}"
        response = session.get(url, headers={"User-Agent": UA}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        frame = _parse_runner_rows(soup, race_date, rid)
        if not frame.empty:
            frames.append(frame)
        time.sleep(0.25)
    if not frames:
        raise RuntimeError(f"netkeiba entry rows were empty: {race_date}")
    out = pd.concat(frames, ignore_index=True)
    if out["racecourse"].eq("").any():
        raise RuntimeError("failed to identify racecourse from netkeiba page")
    return out.sort_values(["race_id", "horse_no"]).reset_index(drop=True)


def save_entries(df: pd.DataFrame, destination: str | Path) -> Path:
    dst = Path(destination)
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False, encoding="utf-8-sig")
    return dst
