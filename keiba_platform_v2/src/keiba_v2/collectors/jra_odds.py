from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

_NUM = re.compile(r"[^\d.\-]")
_PLACE = re.compile(r"回([^\d]+?)\d")


def _require_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("install collection dependencies and browser: pip install -e .[collection]; python -m playwright install chromium") from exc
    return sync_playwright


def _num(text: str) -> str:
    return _NUM.sub("", str(text or ""))


def _place(raw: str) -> str:
    m = _PLACE.search(str(raw))
    return m.group(1) if m else re.sub(r"[0-9回日]", "", str(raw)).strip()


def _race_id(race_date: str, racecourse: str, race_no: int) -> str:
    return f"{race_date}_{racecourse}_{race_no:02d}R"


def _parse_tanpuku(page) -> list[dict]:
    rows = page.locator("table.tanpuku tr")
    result: list[dict] = []
    for i in range(rows.count()):
        row = rows.nth(i)
        num_cell = row.locator("td.num")
        if num_cell.count() == 0:
            continue
        horse_no = _num(num_cell.inner_text().strip())
        if not horse_no:
            continue
        links = row.locator("a")
        horse_name = links.first.inner_text().strip() if links.count() else ""
        tan = row.locator("td.odds_tan")
        fuku = row.locator("td.odds_fuku")
        result.append({
            "horse_no": int(horse_no),
            "horse_name": horse_name,
            "win_odds": pd.to_numeric(_num(tan.inner_text().strip()), errors="coerce") if tan.count() else None,
            "place_odds": fuku.inner_text().strip() if fuku.count() else "",
        })
    return result


def _parse_trio(page) -> dict[str, float]:
    result: dict[str, float] = {}
    tables = page.locator("ul.fuku3_list table.basic")
    for t in range(tables.count()):
        table = tables.nth(t)
        cap = table.locator("caption")
        if not cap.count():
            continue
        prefix = cap.inner_text().strip()
        rows = table.locator("tr")
        for r in range(rows.count()):
            th = rows.nth(r).locator("th")
            td = rows.nth(r).locator("td")
            if not th.count() or not td.count():
                continue
            third = th.inner_text().strip()
            odd = pd.to_numeric(_num(td.inner_text().strip()), errors="coerce")
            if pd.notna(odd):
                nums = sorted(int(x) for x in f"{prefix}-{third}".split("-") if str(x).isdigit())
                if len(nums) == 3:
                    result["-".join(map(str, nums))] = float(odd)
    return result


def collect_jra_odds(race_date: str, *, headless: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collect same-day JRA win/place/trio odds from the public odds pages.

    Returns (runner_odds, combination_odds). Website changes are treated as hard
    failures; downstream prediction must not silently continue with stale odds.
    """
    sync_playwright = _require_playwright()
    runner_rows: list[dict] = []
    combo_rows: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            page.goto("https://www.jra.go.jp/", timeout=60_000)
            page.locator("#quick_menu a[onclick*='accessO.html']").first.click(force=True)
            page.wait_for_load_state("domcontentloaded")

            place_links = page.locator("div.link_list a[onclick*='accessO.html']")
            link_labels = [place_links.nth(i).inner_text().strip() for i in range(place_links.count())]
            if not link_labels:
                raise RuntimeError("JRA odds place links not found")

            for label in link_labels:
                place_links = page.locator("div.link_list a[onclick*='accessO.html']")
                match_index = None
                for i in range(place_links.count()):
                    if place_links.nth(i).inner_text().strip() == label:
                        match_index = i
                        break
                if match_index is None:
                    continue
                racecourse = _place(label)
                place_links.nth(match_index).click()
                page.wait_for_load_state("domcontentloaded")

                for race_no in range(1, 13):
                    row = page.locator(f"tr:has(th.race_num img[alt='{race_no}レース'])").first
                    if row.count() == 0:
                        continue
                    canonical = _race_id(race_date, racecourse, race_no)

                    tan_btn = row.locator("div.tanpuku a").first
                    if tan_btn.count():
                        tan_btn.click()
                        page.wait_for_selector("table.tanpuku td.num", timeout=10_000)
                        for h in _parse_tanpuku(page):
                            runner_rows.append({
                                "race_id": canonical,
                                "race_date": race_date,
                                "racecourse": racecourse,
                                "race_no": race_no,
                                **h,
                            })
                        page.go_back()
                        page.wait_for_load_state("domcontentloaded")

                    row = page.locator(f"tr:has(th.race_num img[alt='{race_no}レース'])").first
                    trio_btn = row.locator("div.trio a").first if row.count() else None
                    if trio_btn is not None and trio_btn.count():
                        trio_btn.click()
                        page.wait_for_selector("ul.fuku3_list", timeout=10_000)
                        for selection, odds in _parse_trio(page).items():
                            combo_rows.append({
                                "race_id": canonical,
                                "race_date": race_date,
                                "racecourse": racecourse,
                                "race_no": race_no,
                                "bet_type": "TRIO",
                                "selection": selection,
                                "odds": odds,
                            })
                        page.go_back()
                        page.wait_for_load_state("domcontentloaded")

                page.go_back()
                page.wait_for_load_state("domcontentloaded")
        finally:
            browser.close()

    runners = pd.DataFrame(runner_rows)
    combos = pd.DataFrame(combo_rows)
    if runners.empty:
        raise RuntimeError(f"JRA win odds were empty: {race_date}")
    duplicates = runners.duplicated(["race_id", "horse_no"], keep=False)
    if duplicates.any():
        raise RuntimeError("duplicate JRA runner odds detected")
    return runners.sort_values(["race_id", "horse_no"]).reset_index(drop=True), combos


def save_odds(runners: pd.DataFrame, combinations: pd.DataFrame, output_dir: str | Path, race_date: str) -> tuple[Path, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runner_path = out / f"jra_runner_odds_{race_date}.csv"
    combo_path = out / f"jra_combination_odds_{race_date}.csv"
    runners.to_csv(runner_path, index=False, encoding="utf-8-sig")
    combinations.to_csv(combo_path, index=False, encoding="utf-8-sig")
    return runner_path, combo_path
