# -*- coding: utf-8 -*-
"""
JRA オッズ一括スクレイパー － 馬名付き単勝・複勝対応版
────────────────────────────────────────────
■ 追加仕様
    ● 単勝ページ遷移時に《馬名》も同時取得
    ● CSV 列を   date, racecourse, race, name, bet_type, combination, odds
      の順に統一
■ インストール
    pip install playwright
    playwright install
────────────────────────────────────────────
"""

import csv
import datetime
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from playwright.sync_api import TimeoutError, sync_playwright

HEADLESS = True        # デバッグ時 False
DATE_STR = datetime.datetime.now().strftime("%Y%m%d")
#DATE_STR="20251227"


#────────────────────────────────────────────
# ユーティリティ
#────────────────────────────────────────────
_re_num = re.compile(r"[^\d.\-]")
_re_place = re.compile(r"回([^\d]+?)\d")
_re_yyyymmdd = re.compile(r"(?<!\d)(20\d{2})[\/\-.年]?(\d{1,2})[\/\-.月]?(\d{1,2})日?(?!\d)")
_re_md = re.compile(r"(\d{1,2})月\s*(\d{1,2})日")

def num(text: str) -> str:
    """数値・ピリオド・ハイフンのみ残す"""
    return _re_num.sub("", text)

def normalize_place(raw: str) -> str:
    """2回函館9日 → 函館"""
    m = _re_place.search(raw)
    return m.group(1) if m else re.sub(r"[0-9回日]", "", raw)

def _format_yyyymmdd(year: int, month: int, day: int) -> Optional[str]:
    """年月日を YYYYMMDD に正規化する。存在しない日付は None を返す。"""
    try:
        return datetime.date(int(year), int(month), int(day)).strftime("%Y%m%d")
    except ValueError:
        return None

def _date_candidates_from_text(text: object, base_date: str = DATE_STR) -> List[str]:
    """テキストやURLから開催日候補（YYYYMMDD）を抽出する。"""
    src = "" if text is None else str(text)
    if not src:
        return []

    base_year = int(str(base_date)[:4])
    dates: List[str] = []

    for m in _re_yyyymmdd.finditer(src):
        ymd = _format_yyyymmdd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if ymd:
            dates.append(ymd)

    for m in _re_md.finditer(src):
        ymd = _format_yyyymmdd(base_year, int(m.group(1)), int(m.group(2)))
        if ymd:
            dates.append(ymd)

    return list(dict.fromkeys(dates))

def _date_candidates_from_link(link, target_date: str) -> List[str]:
    """開催場リンクの表示テキスト・属性から開催日候補を抽出する。"""
    texts: List[str] = []
    for getter in (
        lambda: link.inner_text(timeout=3_000),
        lambda: link.get_attribute("href") or "",
        lambda: link.get_attribute("onclick") or "",
        lambda: link.evaluate("el => Array.from(el.attributes).map(a => a.value).join(' ')"),
    ):
        try:
            texts.append(getter())
        except Exception:
            continue

    dates: List[str] = []
    for text in texts:
        dates.extend(_date_candidates_from_text(text, base_date=target_date))
    return list(dict.fromkeys(dates))

def _collect_today_place_link_indexes(page, target_date: str) -> List[int]:
    """TOPの開催場リンクから、当日開催と判断できるリンクだけを返す。"""
    place_links = page.locator("div.link_list a[onclick*='accessO.html']")
    today_indexes: List[int] = []
    undated_indexes: List[int] = []

    for idx in range(place_links.count()):
        link = place_links.nth(idx)
        dates = _date_candidates_from_link(link, target_date)
        if target_date in dates:
            today_indexes.append(idx)
        elif not dates:
            undated_indexes.append(idx)

    if today_indexes:
        print(f"[INFO] 当日開催リンクを検出しました: {len(today_indexes)}件 / date={target_date}")
        return today_indexes

    if undated_indexes:
        print("[WARN] 開催場リンクから日付を判定できないため、遷移後のページ日付確認で絞り込みます")
        return undated_indexes

    return []

def _page_date_status(page, target_date: str, context: str) -> Optional[bool]:
    """現在ページの開催日が実行日と一致するか確認する。日付不明は None。"""
    primary_texts: List[str] = [page.url]
    try:
        primary_texts.append(page.title())
    except Exception:
        pass

    for selector in ("h1", "h2", "h3", ".race_header", ".kaisai", ".date", ".thisweek"):
        try:
            loc = page.locator(selector)
            for i in range(min(loc.count(), 5)):
                primary_texts.append(loc.nth(i).inner_text(timeout=1_000))
        except Exception:
            continue

    primary_dates: List[str] = []
    for text in primary_texts:
        primary_dates.extend(_date_candidates_from_text(text, base_date=target_date))
    primary_dates = list(dict.fromkeys(primary_dates))

    if len(primary_dates) == 1:
        if primary_dates[0] == target_date:
            return True
        print(f"[WARN] {context}: ページ開催日が実行日と違うためスキップします / page_dates={primary_dates} / target={target_date}")
        return False
    if len(primary_dates) > 1:
        print(
            f"[WARN] {context}: 見出し・URLなどから複数日付が見つかり、開催日を確定できないためスキップします "
            f"/ page_dates={primary_dates} / target={target_date}"
        )
        return None

    body_dates: List[str] = []
    try:
        body_text = page.locator("body").inner_text(timeout=5_000)
        body_dates.extend(_date_candidates_from_text(body_text, base_date=target_date))
    except Exception:
        pass
    body_dates = list(dict.fromkeys(body_dates))

    if len(body_dates) == 1 and body_dates[0] == target_date:
        return True
    if len(body_dates) == 1:
        print(f"[WARN] {context}: ページ開催日が実行日と違うためスキップします / page_dates={body_dates} / target={target_date}")
        return False
    if len(body_dates) > 1:
        print(f"[WARN] {context}: ページ内に複数日付があるため開催日を特定できません / page_dates={body_dates} / target={target_date}")
        return None

    print(f"[WARN] {context}: ページ開催日を確認できないためスキップ候補にします / target={target_date}")
    return None

def _normalize_name_for_duplicate_check(name: object) -> str:
    """重複検査用に馬名の空白ゆらぎを吸収する。"""
    return re.sub(r"[\s\u3000]+", "", "" if name is None else str(name).strip())

def _validate_rows_before_save(rows: Sequence[Sequence[object]]) -> None:
    """CSV保存前に、日付・場所・R・馬番・式別の重複を検査する。"""
    grouped: Dict[tuple, List[Sequence[object]]] = {}
    for row in rows:
        key = (str(row[0]), str(row[1]), str(row[2]), str(row[5]), str(row[4]))
        grouped.setdefault(key, []).append(row)

    for key, vals in grouped.items():
        if len(vals) <= 1:
            continue

        names = {_normalize_name_for_duplicate_check(v[3]) for v in vals}
        names.discard("")
        if len(names) > 1:
            raise RuntimeError(
                "CSV保存前の重複検査で馬名不一致を検知しました。"
                f" key(date,place,race,combination,bet_type)={key}"
                f" names={sorted(names)}"
            )

        odds_values = {str(v[6]).strip() for v in vals}
        if len(odds_values) > 1:
            raise RuntimeError(
                "CSV保存前の重複検査でオッズ不一致を検知しました。"
                f" key(date,place,race,combination,bet_type)={key}"
                f" odds={sorted(odds_values)}"
            )

        raise RuntimeError(
            "CSV保存前の重複検査で同一キーの重複を検知しました。"
            f" key(date,place,race,combination,bet_type)={key}"
        )

#────────────────────────────────────────────
# 単勝・複勝ページパーサ
#────────────────────────────────────────────
def parse_tanpuku(page) -> List[Dict]:
    """
    単勝・複勝ページから
        [{"num":'3', "name":'セイウン', "tan":'4.2', "fuku":'1.8-2.6'}, …]
    を返す
    """
    rows = page.locator("table.tanpuku tr")
    result = []

    for i in range(rows.count()):
        row = rows.nth(i)
        # 馬番
        num_cell = row.locator("td.num")
        if num_cell.count() == 0:
            continue
        horse_no = num_cell.inner_text().strip()

        # 馬名（リンク or class 無しセル → 最初の <a> を優先）
        name_cell = row.locator("a").first
        if name_cell.count() == 0:
            # fallback: num, odds以外のセルを探す
            cells = row.locator("td")
            name_candidates = [
                cells.nth(j).inner_text().strip()
                for j in range(cells.count())
                if not cells.nth(j).evaluate("el => el.className").startswith(("num","odds_tan","odds_fuku"))
            ]
            horse_name = name_candidates[0] if name_candidates else ""
        else:
            horse_name = name_cell.inner_text().strip()

        tan_odds  = row.locator("td.odds_tan").inner_text().strip()
        fuku_odds = row.locator("td.odds_fuku").inner_text().strip()

        result.append(
            {"num": horse_no, "name": horse_name,
             "tan": num(tan_odds), "fuku": num(fuku_odds)}
        )
    return result

#────────────────────────────────────────────
# ３連複ページパーサ
#────────────────────────────────────────────
def parse_trio(page) -> Dict[str, str]:
    trio = {}
    tables = page.locator("ul.fuku3_list table.basic")
    for t in range(tables.count()):
        cap = tables.nth(t).locator("caption").inner_text().strip()        # 例 1-2
        rows = tables.nth(t).locator("tr")
        for r in range(rows.count()):
            third = rows.nth(r).locator("th").inner_text().strip()
            odd   = rows.nth(r).locator("td").inner_text().strip()
            trio[f"{cap}-{third}"] = num(odd)
    return trio

#────────────────────────────────────────────
# メインスクレイパー
#────────────────────────────────────────────
def scrape_odds(headless: bool = HEADLESS, out_dir: str = r"C:\\Users\\okino\\OneDrive\\ドキュメント\\my_python_cursor\\keiba_yosou_2026\\data\\ozzu_csv") -> Path:
    out_csv = Path(out_dir) / f"OZZU_{DATE_STR}.csv"
    headers = ["date", "racecourse", "race", "name", "bet_type", "combination", "odds"]
    rows_to_save: List[List[str]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})

            # 1▶ TOP → オッズ
            page.goto("https://www.jra.go.jp/", timeout=60_000)
            page.locator("#quick_menu a[onclick*='accessO.html']").first.click(force=True)
            page.wait_for_load_state("domcontentloaded")

            # 2▶ 開催場リンク（当日分だけを対象にする）
            today_link_indexes = _collect_today_place_link_indexes(page, DATE_STR)
            if not today_link_indexes:
                raise RuntimeError(f"当日開催場リンクが見つかりませんでした: date={DATE_STR}")

            for idx in today_link_indexes:
                place_links = page.locator("div.link_list a[onclick*='accessO.html']")
                if idx >= place_links.count():
                    print(f"[WARN] 開催場リンクの再取得時に index={idx} が存在しないためスキップします")
                    continue

                link = place_links.nth(idx)
                place_label = normalize_place(link.inner_text().strip())
                link.click()
                page.wait_for_load_state("domcontentloaded")

                place_date_status = _page_date_status(page, DATE_STR, f"{place_label} 開催場ページ")
                if place_date_status is not True:
                    page.go_back()
                    page.wait_for_load_state("domcontentloaded")
                    continue

                # 3▶ レース行ループ
                for r in range(1, 13):
                    row = page.locator(f"tr:has(th.race_num img[alt='{r}レース'])").first
                    if row.count() == 0:
                        continue
                    race_lbl = f"{r}R"

                    # 単勝＋複勝
                    tan_btn = row.locator("div.tanpuku a").first
                    if tan_btn.count():
                        tan_btn.click()
                        page.wait_for_selector("table.tanpuku td.num", timeout=10_000)

                        detail_date_status = _page_date_status(page, DATE_STR, f"{place_label} {race_lbl} 単勝複勝ページ")
                        if detail_date_status is not True:
                            page.go_back()
                            page.wait_for_load_state("domcontentloaded")
                            continue

                        horses = parse_tanpuku(page)
                        # 単勝
                        for h in horses:
                            rows_to_save.append([DATE_STR, place_label, race_lbl, h["name"], "単勝", h["num"], h["tan"]])
                        # 複勝
                        for h in horses:
                            rows_to_save.append([DATE_STR, place_label, race_lbl, h["name"], "複勝", h["num"], h["fuku"]])

                        page.go_back()
                        page.wait_for_load_state("domcontentloaded")

                    # ３連複
                    trio_btn = row.locator("div.trio a").first
                    if trio_btn.count():
                        trio_btn.click()
                        page.wait_for_selector("ul.fuku3_list", timeout=10_000)

                        detail_date_status = _page_date_status(page, DATE_STR, f"{place_label} {race_lbl} 3連複ページ")
                        if detail_date_status is not True:
                            page.go_back()
                            page.wait_for_load_state("domcontentloaded")
                            continue

                        trio = parse_trio(page)
                        for comb, odd in trio.items():
                            rows_to_save.append([DATE_STR, place_label, race_lbl, "", "3連複", comb, odd])
                        page.go_back()
                        page.wait_for_load_state("domcontentloaded")

                page.go_back()
                page.wait_for_load_state("domcontentloaded")
        finally:
            browser.close()

    if not rows_to_save:
        raise RuntimeError(f"保存対象の当日オッズがありませんでした: date={DATE_STR}")

    _validate_rows_before_save(rows_to_save)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(headers)
        writer.writerows(rows_to_save)

    return out_csv

#────────────────────────────────────────────
# 実行
#────────────────────────────────────────────
if __name__ == "__main__":
    debug = len(sys.argv) > 1 and sys.argv[1].lower() == "show"
    try:
        csv_file = scrape_odds(headless=not debug)
        print(f"✅ 収集完了: {csv_file}")
    except TimeoutError as te:
        print("🛑 タイムアウト:", te)
        sys.exit(1)
    except Exception as e:
        print("🛑 予期せぬエラー:", e)
        sys.exit(1)

