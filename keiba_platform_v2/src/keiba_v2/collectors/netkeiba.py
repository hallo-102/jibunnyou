from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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


def _race_ids_from_official_program(html: bytes | str, race_date: str) -> list[str]:
    """JRA公式番組表の開催回・開催日・正式R番号からレースIDを取得する。"""
    _, BeautifulSoup, _, _ = _require_collection_deps()
    soup = BeautifulSoup(html, "html.parser")
    ids: set[str] = set()
    for table in soup.select("table.basic"):
        heading = table.select_one("div.main")
        if heading is None:
            continue
        label = heading.get_text(" ", strip=True)
        place = _extract_place(label)
        meeting = re.search(r"(\d+)\s*回", label)
        day = re.search(r"(\d+)\s*日", label)
        if not place or meeting is None or day is None:
            raise RuntimeError(f"invalid JRA meeting heading: {label}")
        # 注記中の「第8レース」などをR番号として誤認せず、正式な先頭セルだけを採用します。
        for row in table.select("tr"):
            cell = row.find(["th", "td"])
            number = re.fullmatch(r"\s*(\d+)\s*レース\s*", cell.get_text(" ", strip=True)) if cell else None
            if number is None:
                continue
            race_no = int(number.group(1))
            if not 1 <= race_no <= 12:
                raise RuntimeError(f"invalid JRA race number: {race_no}")
            code = PLACE_NAMES.index(place) + 1
            rid = f"{race_date[:4]}{code:02d}{int(meeting.group(1)):02d}{int(day.group(1)):02d}{race_no:02d}"
            if rid in ids:
                raise RuntimeError(f"duplicate JRA race ID: {rid}")
            ids.add(rid)
    if not ids:
        raise RuntimeError(f"JRA official race program is empty/unparseable: {race_date}")
    return sorted(ids)


def collect_race_ids(race_date: str, *, headless: bool = True, timeout_sec: int = 40) -> list[str]:
    """ブラウザ表示指定は互換性のため保持し、開催IDはJRA公式情報で確定する。"""
    date = datetime.strptime(race_date, "%Y%m%d")
    requests, _, _, _ = _require_collection_deps()
    url = f"https://www.jra.go.jp/keiba/calendar{date.year}/{date.year}/{date.month}/{date:%m%d}.html"
    response = requests.get(url, headers={"User-Agent": UA}, timeout=timeout_sec)
    # HTTP失敗を「レースなし」として正常扱いせず、呼び出し元まで伝播させます。
    response.raise_for_status()
    return _race_ids_from_official_program(response.content, race_date)


def _parse_runner_rows(soup, race_date: str, source_race_id: str) -> pd.DataFrame:
    page_text = soup.get_text(" ", strip=True)
    race_data = soup.select_one(".RaceData02")
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    race_data_text = race_data.get_text(" ", strip=True) if race_data else ""
    place = _extract_place(race_data_text) or _extract_place(title_text) or _extract_place(page_text)
    race_no = int(source_race_id[-2:]) if source_race_id[-2:].isdigit() else 0
    canonical_id = _canonical_race_id(race_date, place or "UNKNOWN", race_no)

    rows: list[dict] = []
    for tr in soup.select("tr.HorseList"):
        no_cell = tr.select_one('td[class^="Umaban"]')
        horse = tr.select_one("td.HorseInfo a")
        if no_cell is None or horse is None:
            continue
        no_text = re.sub(r"\D", "", no_cell.get_text(" ", strip=True))
        if not no_text:
            continue
        horse_name = horse.get_text(" ", strip=True)
        horse_id = _horse_id_from_href(str(horse.get("href", "")))
        age_cell = tr.select_one("td.Barei")
        carried_weight_cell = tr.select_one("td.Dredging")
        if carried_weight_cell is None and age_cell is not None:
            carried_weight_cell = age_cell.find_next_sibling("td")
        jockey_cell = tr.select_one("td.Jockey a")
        trainer_cell = tr.select_one("td.Trainer a")
        popular_cell = (
            tr.select_one('span[id^="ninki-"]')
            or tr.select_one("td.Popular_Ninki span")
            or tr.select_one("td.Popular_Ninki")
        )

        sex_age = age_cell.get_text(" ", strip=True) if age_cell else ""
        age_match = re.search(r"(\d+)", sex_age)
        popularity = None
        if popular_cell is not None:
            pop_match = re.search(r"\d+", popular_cell.get_text(" ", strip=True))
            if pop_match:
                popularity = int(pop_match.group(0))

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
            "carried_weight": pd.to_numeric(
                carried_weight_cell.get_text(" ", strip=True), errors="coerce"
            ) if carried_weight_cell else None,
            "jockey": jockey_cell.get_text(" ", strip=True) if jockey_cell else "",
            "trainer": trainer_cell.get_text(" ", strip=True) if trainer_cell else "",
            "netkeiba_popularity": popularity,
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
        # 1レースでも欠けた部分出馬表を全開催の入力として保存しないようにします。
        if frame.empty:
            raise RuntimeError(f"netkeiba entry rows were empty: {race_date} race_id={rid}")
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
