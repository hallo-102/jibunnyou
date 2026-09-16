from __future__ import annotations

import datetime as dt
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


def _target_date_tokens(race_date: str) -> tuple[str, ...]:
    d = dt.datetime.strptime(race_date, "%Y%m%d").date()
    return (
        race_date,
        f"{d.year}年{d.month}月{d.day}日",
        f"{d.month}月{d.day}日",
    )


def _page_matches_date(page, race_date: str) -> bool:
    tokens = _target_date_tokens(race_date)
    texts = [page.url]
    try:
        texts.append(page.title())
    except Exception:
        pass
    try:
        texts.append(page.locator("body").inner_text(timeout=5_000))
    except Exception:
        pass
    joined = " ".join(texts)
    return any(token in joined for token in tokens)


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


def _open_odds_top(page) -> None:
    page.goto("https://www.jra.go.jp/", timeout=60_000)
    link = page.locator("#quick_menu a[onclick*='accessO.html']").first
    if not link.count():
        raise RuntimeError("JRA odds menu not found")
    link.click(force=True)
    page.wait_for_load_state("domcontentloaded")


def _goto_place(page, race_date: str, racecourse: str) -> None:
    candidates = page.locator("div.link_list a[onclick*='accessO.html']")
    labels = [candidates.nth(i).inner_text().strip() for i in range(candidates.count())]
    indexes = [i for i, label in enumerate(labels) if racecourse in _place(label)]
    if not indexes:
        raise RuntimeError(f"JRA racecourse link not found: {racecourse}")

    for idx in indexes:
        candidates = page.locator("div.link_list a[onclick*='accessO.html']")
        if idx >= candidates.count():
            continue
        candidates.nth(idx).click()
        page.wait_for_load_state("domcontentloaded")
        if _page_matches_date(page, race_date):
            return
        page.go_back()
        page.wait_for_load_state("domcontentloaded")
    raise RuntimeError(f"JRA page date mismatch: date={race_date} racecourse={racecourse}")


def _collect_race_from_place_page(page, race_date: str, racecourse: str, race_no: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    canonical = _race_id(race_date, racecourse, race_no)
    row = page.locator(f"tr:has(th.race_num img[alt='{race_no}レース'])").first
    if row.count() == 0:
        raise RuntimeError(f"JRA race row not found: {canonical}")

    runner_rows: list[dict] = []
    combo_rows: list[dict] = []

    tan_btn = row.locator("div.tanpuku a").first
    if not tan_btn.count():
        raise RuntimeError(f"JRA win odds link not found: {canonical}")
    tan_btn.click()
    page.wait_for_selector("table.tanpuku td.num", timeout=10_000)
    if not _page_matches_date(page, race_date):
        raise RuntimeError(f"JRA win odds date mismatch: {canonical}")
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
        if not _page_matches_date(page, race_date):
            raise RuntimeError(f"JRA trio odds date mismatch: {canonical}")
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

    runners = pd.DataFrame(runner_rows)
    combos = pd.DataFrame(combo_rows)
    if runners.empty:
        raise RuntimeError(f"JRA runner odds empty: {canonical}")
    if runners.duplicated(["race_id", "horse_no"], keep=False).any():
        raise RuntimeError(f"duplicate JRA runner odds: {canonical}")
    return runners, combos


def collect_jra_race_odds(
    race_date: str,
    racecourse: str,
    race_no: int,
    *,
    headless: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sync_playwright = _require_playwright()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            _open_odds_top(page)
            _goto_place(page, race_date, racecourse)
            return _collect_race_from_place_page(page, race_date, racecourse, int(race_no))
        finally:
            browser.close()


def collect_jra_odds(race_date: str, *, headless: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collect same-day JRA win/place/trio odds for every race currently listed."""
    sync_playwright = _require_playwright()
    runner_frames: list[pd.DataFrame] = []
    combo_frames: list[pd.DataFrame] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            _open_odds_top(page)
            links = page.locator("div.link_list a[onclick*='accessO.html']")
            labels = list(dict.fromkeys(links.nth(i).inner_text().strip() for i in range(links.count())))
            racecourses = list(dict.fromkeys(_place(label) for label in labels if _place(label)))
            if not racecourses:
                raise RuntimeError("JRA odds place links not found")

            for racecourse in racecourses:
                _open_odds_top(page)
                try:
                    _goto_place(page, race_date, racecourse)
                except RuntimeError:
                    continue
                for race_no in range(1, 13):
                    row = page.locator(f"tr:has(th.race_num img[alt='{race_no}レース'])").first
                    if row.count() == 0:
                        continue
                    runners, combos = _collect_race_from_place_page(page, race_date, racecourse, race_no)
                    runner_frames.append(runners)
                    if not combos.empty:
                        combo_frames.append(combos)
        finally:
            browser.close()

    runners = pd.concat(runner_frames, ignore_index=True) if runner_frames else pd.DataFrame()
    combos = pd.concat(combo_frames, ignore_index=True) if combo_frames else pd.DataFrame()
    if runners.empty:
        raise RuntimeError(f"JRA win odds were empty: {race_date}")
    return runners.sort_values(["race_id", "horse_no"]).reset_index(drop=True), combos


def save_odds(runners: pd.DataFrame, combinations: pd.DataFrame, output_dir: str | Path, race_date: str) -> tuple[Path, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runner_path = out / f"jra_runner_odds_{race_date}.csv"
    combo_path = out / f"jra_combination_odds_{race_date}.csv"
    runners.to_csv(runner_path, index=False, encoding="utf-8-sig")
    combinations.to_csv(combo_path, index=False, encoding="utf-8-sig")
    return runner_path, combo_path
