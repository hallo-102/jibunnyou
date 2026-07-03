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
from typing import List, Dict

from playwright.sync_api import TimeoutError, sync_playwright

HEADLESS = True        # デバッグ時 False
DATE_STR = datetime.datetime.now().strftime("%Y%m%d")
#DATE_STR="20251227"


#────────────────────────────────────────────
# ユーティリティ
#────────────────────────────────────────────
_re_num = re.compile(r"[^\d.\-]")
_re_place = re.compile(r"回([^\d]+?)\d")

def num(text: str) -> str:
    """数値・ピリオド・ハイフンのみ残す"""
    return _re_num.sub("", text)

def normalize_place(raw: str) -> str:
    """2回函館9日 → 函館"""
    m = _re_place.search(raw)
    return m.group(1) if m else re.sub(r"[0-9回日]", "", raw)

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

    with sync_playwright() as pw, open(out_csv, "w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(headers)

        browser = pw.chromium.launch(headless=headless)
        page    = browser.new_page(viewport={"width": 1400, "height": 900})

        # 1▶ TOP → オッズ
        page.goto("https://www.jra.go.jp/", timeout=60_000)
        page.locator("#quick_menu a[onclick*='accessO.html']").first.click(force=True)
        page.wait_for_load_state("domcontentloaded")

        # 2▶ 開催場リンク
        place_links = page.locator("div.link_list a[onclick*='accessO.html']")
        for idx in range(place_links.count()):
            link = place_links.nth(idx)
            place_label = normalize_place(link.inner_text().strip())
            link.click(); page.wait_for_load_state("domcontentloaded")

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

                    horses = parse_tanpuku(page)
                    # 単勝
                    for h in horses:
                        writer.writerow([DATE_STR, place_label, race_lbl, h["name"],
                                         "単勝", h["num"], h["tan"]])
                    # 複勝
                    for h in horses:
                        writer.writerow([DATE_STR, place_label, race_lbl, h["name"],
                                         "複勝", h["num"], h["fuku"]])

                    page.go_back(); page.wait_for_load_state("domcontentloaded")

                # ３連複
                trio_btn = row.locator("div.trio a").first
                if trio_btn.count():
                    trio_btn.click()
                    page.wait_for_selector("ul.fuku3_list", timeout=10_000)
                    trio = parse_trio(page)
                    for comb, odd in trio.items():
                        writer.writerow([DATE_STR, place_label, race_lbl, "",
                                         "3連複", comb, odd])
                    page.go_back(); page.wait_for_load_state("domcontentloaded")

            # 戻って開催場リンクを再取得
            page.go_back(); page.wait_for_load_state("domcontentloaded")
            place_links = page.locator("div.link_list a[onclick*='accessO.html']")

        browser.close()
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
    except Exception as e:
        print("🛑 予期せぬエラー:", e)

